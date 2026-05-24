import base64
import json
import logging
import copy
import asyncio
from typing import Dict, Any, Tuple, Optional
from fastapi import HTTPException

from services.fabric.deploy_service import FabricDeployService
from services.fabric.auth_service import execute_fabric_request, resolve_fabric_token

logger = logging.getLogger(__name__)
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


async def _execute_with_retry(service, method: str, url: str, **kwargs) -> Any:
    max_retries = 5
    delays = [5, 10, 20, 40, 80]
    
    for attempt in range(max_retries + 1):
        resp = await execute_fabric_request(service, method, url, **kwargs)
        if resp.is_success:
            return resp
            
        # Parse error
        is_cicd_in_progress = False
        try:
            body = resp.json()
            if isinstance(body, dict):
                err_code = body.get("errorCode")
                if not err_code and isinstance(body.get("error"), dict):
                    err_code = body["error"].get("code") or body["error"].get("errorCode")
                if err_code == "ActiveCiCdOperationInProgress":
                    is_cicd_in_progress = True
        except Exception:
            pass
            
        if is_cicd_in_progress and attempt < max_retries:
            delay = delays[attempt]
            logger.warning(f"[Fabric Retry] ActiveCiCdOperationInProgress (attempt {attempt + 1}/{max_retries}). Retrying in {delay}s...")
            await asyncio.sleep(delay)
            continue
            
        return resp


class FabricMutationService(FabricDeployService):
    def __init__(self, token: str | None = None):
        access_token = resolve_fabric_token(token) or token or ""
        super().__init__(access_token)

    async def _get_pipeline_definition(self, workspace_id: str, pipeline_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # Try /dataPipelines first, and fall back to /items if needed
        urls = [
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/getDefinition",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition"
        ]
        
        last_resp = None
        for url in urls:
            resp = await _execute_with_retry(self, "POST", url)
            last_resp = resp
            if resp.status_code in (200, 201, 202):
                break
                
        if not last_resp:
            raise HTTPException(status_code=500, detail="Failed to fetch pipeline definition: No response received.")
            
        if last_resp.status_code == 202:
            location = last_resp.headers.get("Location")
            if not location:
                raise HTTPException(status_code=502, detail="getDefinition returned 202 Accepted but Location header was missing.")
            while True:
                poll_resp = await _execute_with_retry(self, "GET", location)
                if poll_resp.status_code == 200:
                    raw_def = poll_resp.json()
                    break
                elif poll_resp.status_code in (202, 204):
                    await asyncio.sleep(2)
                    continue
                else:
                    raise HTTPException(status_code=poll_resp.status_code, detail=f"Failed to poll definition LRO: {poll_resp.text}")
        elif last_resp.status_code in (200, 201):
            raw_def = last_resp.json()
        else:
            raise HTTPException(status_code=last_resp.status_code, detail=f"Failed to get pipeline definition: {last_resp.text}")
            
        parts = raw_def.get("definition", {}).get("parts", []) or raw_def.get("parts", [])
        for part in parts:
            if part.get("path") == "pipeline-content.json":
                decoded = base64.b64decode(part["payload"]).decode("utf-8")
                return raw_def, json.loads(decoded)
        raise HTTPException(status_code=400, detail="pipeline-content.json not found in definition")

    async def _update_pipeline_definition(self, workspace_id: str, pipeline_id: str, content: dict):
        definition_b64 = base64.b64encode(json.dumps(content).encode("utf-8")).decode("utf-8")
        update_url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition"
        payload = {
            "definition": {
                "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
            }
        }
        resp = await _execute_with_retry(self, "POST", update_url, json=payload)
        if not resp.is_success:
            raise HTTPException(status_code=resp.status_code, detail=f"Fabric API error during updateDefinition: {resp.text}")

    async def _extract_rest_connection_id(self, workspace_id: str, pipeline_id: str, preferred_role: str) -> Optional[str]:
        try:
            _, definition = await self._get_pipeline_definition(workspace_id, pipeline_id)
            refs = []
            for activity in definition.get("properties", {}).get("activities", []):
                type_props = activity.get("typeProperties", {})
                for role in ("source", "sink"):
                    endpoint = type_props.get(role) or {}
                    conn = ((endpoint.get("datasetSettings", {}) or {}).get("externalReferences", {}) or {}).get("connection")
                    if conn:
                        refs.append({"role": role, "connection_id": conn, "connector_type": endpoint.get("type", "")})
            preferred = [ref for ref in refs if ref["role"] == preferred_role and "Rest" in ref["connector_type"]]
            return (preferred or refs or [{}])[0].get("connection_id")
        except Exception as exc:
            logger.debug(f"Failed to extract REST connection from pipeline {pipeline_id}: {exc}")
            return None

    async def _create_rest_connection(self, display_name: str, base_url: str) -> str:
        from services.fabric.connection_service import FabricConnectionService

        token = self.headers["Authorization"].replace("Bearer ", "")
        connection_service = FabricConnectionService(token)
        return await connection_service.get_or_create_rest_connection(
            base_url,
            display_name,
        )

    async def _resolve_rest_connection_id(
        self,
        workspace_id: str,
        params: dict,
        connection_name: str | None = None,
        template_pipeline_id: str | None = None,
        preferred_role: str = "source",
        require_existing: bool = False,
    ) -> str:
        # 1. params["connection_id"] supplied -> use directly
        if params.get("connection_id"):
            return params["connection_id"]

        # 2. params["ref_pipeline_id"] -> extract UUID from that pipeline
        ref_pipeline = template_pipeline_id or params.get("ref_pipeline_id")
        if ref_pipeline:
            connection_id = await self._extract_rest_connection_id(workspace_id, ref_pipeline, preferred_role)
            if connection_id:
                return connection_id
            if require_existing:
                raise HTTPException(status_code=400, detail=f"No connection UUID found in pipeline {ref_pipeline}.")

        # 3. params["base_url"] -> create new connection
        base_url = params.get("base_url") or params.get("url")
        if base_url and not require_existing:
            return await self._create_rest_connection(connection_name or "Dynamic_REST_Connection", base_url)

        raise HTTPException(
            status_code=400,
            detail="Cannot resolve Fabric REST connection UUID. Provide connection_id, ref_pipeline_id, or base_url."
        )

    async def _build_rest_endpoint(self, params: dict, role: str, workspace_id: str) -> dict:
        connection_id = params.get("connection_id") or params.get("connection_reference")
        if not connection_id and params.get("ref_pipeline_id"):
            connection_id = await self._extract_rest_connection_id(workspace_id, params["ref_pipeline_id"], role)
        if not connection_id and (params.get("base_url") or params.get("url")):
            connection_id = await self._create_rest_connection(
                params.get("connection_name") or f"Dynamic_REST_{role.title()}_Connection",
                params.get("base_url") or params.get("url"),
            )
        if not connection_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{role}_params.connection_id is required unless ref_pipeline_id "
                    "or base_url is provided for dynamic REST connection resolution"
                ),
            )
        return {
            "type": "RestSource" if role == "source" else "RestSink",
            "httpRequestTimeout": params.get("request_timeout", "00:01:40"),
            "requestMethod": params.get("request_method") or params.get("method") or ("GET" if role == "source" else "POST"),
            "datasetSettings": {
                "type": "RestResource",
                "typeProperties": {"relativeUrl": params.get("relative_url", "")},
                "externalReferences": {"connection": connection_id},
            },
        }

    async def inspect(self, workspace_id: str, pipeline_id: str, token: str | None = None) -> Dict[str, Any]:
        self.access_token = resolve_fabric_token(token) or token or ""
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        raw_def, content = await self._get_pipeline_definition(workspace_id, pipeline_id)
        
        idx, activity = self._get_copy_activity_with_index(content)
        type_props = activity.get("typeProperties", {})
        
        source_type = self._detect_connector_type(activity, "source")
        sink_type = self._detect_connector_type(activity, "sink")
        
        source_conn = ((type_props.get("source", {}).get("datasetSettings", {}) or {}).get("externalReferences", {}) or {}).get("connection")
        sink_conn = ((type_props.get("sink", {}).get("datasetSettings", {}) or {}).get("externalReferences", {}) or {}).get("connection")
        
        return {
            "source_type": source_type,
            "sink_type": sink_type,
            "source_connection_id": source_conn,
            "sink_connection_id": sink_conn,
        }

    async def modify_source(
        self,
        workspace_id: str,
        pipeline_id: str,
        source_type: str,
        source_params: dict,
        token: str | None = None,
        connection_name: str | None = None,
        template_pipeline_id: str | None = None,
    ) -> Dict[str, Any]:
        self.access_token = resolve_fabric_token(token) or token or ""
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        raw_def, content = await self._get_pipeline_definition(workspace_id, pipeline_id)
        idx, original_activity = self._get_copy_activity_with_index(content)
        
        mutated = copy.deepcopy(original_activity)
        resolved_resources = {}
        
        source_type_upper = source_type.upper()
        if source_type_upper in ("REST", "REST_API"):
            connection_id = await self._resolve_rest_connection_id(
                workspace_id,
                source_params,
                connection_name,
                template_pipeline_id,
                preferred_role="source",
            )
            mutated["typeProperties"]["source"] = self._build_rest_source(
                connection_id,
                source_params.get("relative_url", ""),
                source_params.get("request_method") or source_params.get("method", "GET"),
                source_params.get("request_timeout", "00:01:40"),
            )
            old_inputs = original_activity.get("inputs", [])
            if old_inputs and old_inputs[0].get("referenceName"):
                resolved_resources["remove_src_ds"] = old_inputs[0]["referenceName"]
            mutated.pop("inputs", None)
        else:
            endpoint, resources = await self._build_connector_endpoint(
                source_params,
                "source",
                source_type,
                workspace_id,
                key_prefix="src",
            )
            mutated["typeProperties"]["source"] = endpoint
            resolved_resources.update(resources)
            old_inputs = original_activity.get("inputs", [])
            if old_inputs and old_inputs[0].get("referenceName"):
                resolved_resources["remove_src_ds"] = old_inputs[0]["referenceName"]
                
        final_activity = self._preserve_activity_properties(original_activity, mutated)
        cloned = copy.deepcopy(content)
        cloned["properties"]["activities"][idx] = final_activity
        
        self._inject_reference_resources(cloned, resolved_resources)
        
        await self._update_pipeline_definition(workspace_id, pipeline_id, cloned)
        
        return {
            "status": "success",
            "pipeline_id": pipeline_id,
            "source_type": source_type,
        }

    async def modify_sink(
        self,
        workspace_id: str,
        pipeline_id: str,
        sink_type: str,
        sink_params: dict,
        token: str | None = None,
        connection_name: str | None = None,
        template_pipeline_id: str | None = None,
    ) -> Dict[str, Any]:
        self.access_token = resolve_fabric_token(token) or token or ""
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        raw_def, content = await self._get_pipeline_definition(workspace_id, pipeline_id)
        idx, original_activity = self._get_copy_activity_with_index(content)
        
        mutated = copy.deepcopy(original_activity)
        resolved_resources = {}
        
        sink_type_upper = sink_type.upper()
        if sink_type_upper in ("REST", "REST_API"):
            connection_id = await self._resolve_rest_connection_id(
                workspace_id,
                sink_params,
                connection_name,
                template_pipeline_id,
                preferred_role="sink",
                require_existing=True,
            )
            mutated["typeProperties"]["sink"] = self._build_rest_sink(
                connection_id,
                sink_params.get("relative_url", ""),
                sink_params.get("request_method") or sink_params.get("method", "POST"),
                sink_params.get("request_timeout", "00:01:40"),
            )
            old_outputs = original_activity.get("outputs", [])
            if old_outputs and old_outputs[0].get("referenceName"):
                resolved_resources["remove_snk_ds"] = old_outputs[0]["referenceName"]
            mutated.pop("outputs", None)
        else:
            original_sink = (original_activity.get("typeProperties", {}) or {}).get("sink", {})
            mutated["typeProperties"]["sink"] = self._mutate_existing_sink(
                original_sink,
                sink_params,
                sink_type,
            )
            old_outputs = original_activity.get("outputs", [])
            if old_outputs and old_outputs[0].get("referenceName"):
                resolved_resources["remove_snk_ds"] = old_outputs[0]["referenceName"]
                
        final_activity = self._preserve_activity_properties(original_activity, mutated)
        cloned = copy.deepcopy(content)
        cloned["properties"]["activities"][idx] = final_activity
        
        self._inject_reference_resources(cloned, resolved_resources)
        
        await self._update_pipeline_definition(workspace_id, pipeline_id, cloned)
        
        return {
            "status": "success",
            "pipeline_id": pipeline_id,
            "sink_type": sink_type,
        }
