import httpx
import json
import asyncio
import base64
from fastapi import HTTPException
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricPipelineService:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    def _network_error(self, action: str, exc: httpx.RequestError) -> HTTPException:
        target = str(exc.request.url) if exc.request else FABRIC_API_BASE
        return HTTPException(
            status_code=503,
            detail={
                "code": "FabricNetworkError",
                "message": f"Unable to reach Microsoft Fabric API while {action}.",
                "target": target,
                "reason": exc.__class__.__name__,
                "hint": "Check internet/proxy/VPN/SSL inspection settings and retry.",
            },
        )

    async def list_pipelines(self, workspace_id: str):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Note: Fabric items API
                resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=DataPipeline", headers=self.headers)
                resp.raise_for_status()
                return resp.json().get("value", [])
        except httpx.RequestError as exc:
            raise self._network_error("listing pipelines", exc) from exc

    async def get_pipeline(self, workspace_id: str, pipeline_id: str) -> Dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}", headers=self.headers)
                resp.raise_for_status()
                return resp.json()
        except httpx.RequestError as exc:
            raise self._network_error("loading pipeline metadata", exc) from exc

    async def run_pipeline(self, workspace_id: str, pipeline_id: str, pipeline_name: Optional[str] = None, owner_upn: Optional[str] = None, owner_object_id: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"executionData": {}}
        if pipeline_name:
            payload["executionData"]["pipelineName"] = pipeline_name
        if owner_upn:
            payload["executionData"]["OwnerUserPrincipalName"] = owner_upn
        if owner_object_id:
            payload["executionData"]["OwnerUserObjectId"] = owner_object_id

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
                    headers=self.headers,
                    json=payload,
                )
                if resp.status_code not in (200, 201, 202):
                    raise HTTPException(status_code=resp.status_code, detail=f"Pipeline execution failed: {resp.text}")
                location = resp.headers.get("Location", "")
                job_instance_id = location.rstrip("/").split("/")[-1] if location else None
                try:
                    body = resp.json()
                except Exception:
                    body = {}
                return {
                    "job_instance_id": job_instance_id or body.get("id"),
                    "location": location,
                    "body": body,
                    "status_code": resp.status_code,
                }
        except httpx.RequestError as exc:
            raise self._network_error("starting pipeline execution", exc) from exc

    async def get_pipeline_job_instance(self, workspace_id: str, pipeline_id: str, job_instance_id: str) -> Dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_instance_id}",
                    headers=self.headers,
                )
                if not resp.is_success:
                    raise HTTPException(status_code=resp.status_code, detail=f"Get pipeline job instance failed: {resp.text}")
                return resp.json()
        except httpx.RequestError as exc:
            raise self._network_error("polling pipeline job status", exc) from exc

    async def query_activity_runs(
        self,
        workspace_id: str,
        job_instance_id: str,
        last_updated_after: str,
        last_updated_before: str,
    ) -> List[Dict[str, Any]]:
        payload = {
            "filters": [],
            "orderBy": [{"orderBy": "ActivityRunStart", "order": "ASC"}],
            "lastUpdatedAfter": last_updated_after,
            "lastUpdatedBefore": last_updated_before,
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    f"{FABRIC_API_BASE}/workspaces/{workspace_id}/datapipelines/pipelineruns/{job_instance_id}/queryactivityruns",
                    headers=self.headers,
                    json=payload,
                )
                if not resp.is_success:
                    raise HTTPException(status_code=resp.status_code, detail=f"Query activity runs failed: {resp.text}")
                body = resp.json()
                return body if isinstance(body, list) else body.get("value", [])
        except httpx.RequestError as exc:
            raise self._network_error("querying pipeline activity runs", exc) from exc

    async def bulk_export_definitions(self, workspace_id: str, pipeline_ids: list):
        """Polls LRO and returns a dict of {pipeline_id: {filename: content}}"""
        logger.info(
            "bulk_export_definitions request workspace_id=%s pipeline_item_ids=%s",
            workspace_id,
            pipeline_ids,
        )
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/bulkExportDefinitions?beta=true"
        payload = {
            "mode": "Selective",
            "items": [{"id": pid, "type": "DataPipeline"} for pid in pipeline_ids]
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=self.headers, json=payload)
            if not resp.is_success:
                raise HTTPException(status_code=resp.status_code, detail=f"Export failed: {resp.text}")
            
            location_url = resp.headers.get("Location")
            if not location_url:
                raise Exception("Location header missing")
                
            # Poll
            while True:
                poll_resp = await client.get(location_url, headers=self.headers)
                poll_resp.raise_for_status()
                if poll_resp.status_code == 200:
                    status_data = poll_resp.json()
                    if status_data.get("status") == "Succeeded":
                        break
                    elif status_data.get("status") in ("Failed", "Canceled"):
                        raise Exception(f"Export failed: {status_data}")
                await asyncio.sleep(2)

            # Result
            res_resp = await client.get(f"{location_url}/result", headers=self.headers)
            res_resp.raise_for_status()
            result_data = res_resp.json()
            
            results = {}
            item_index = result_data.get("itemDefinitionsIndex", [])
            definition_parts = result_data.get("definitionParts", [])
            
            for idx_entry in item_index:
                pid = idx_entry.get("id")
                root_path = idx_entry.get("rootPath")
                files = {}
                for part in definition_parts:
                    path = part.get("path", "")
                    if path.startswith(root_path):
                        rel_path = path[len(root_path):].lstrip("/")
                        payload_b64 = part.get("payload", "")
                        content = base64.b64decode(payload_b64)
                        
                        if rel_path == "pipeline-content.json":
                            files["pipeline.json"] = content
                        elif rel_path == "item.metadata.json":
                            try:
                                metadata = json.loads(content)
                                manifest = {
                                    "name": metadata.get("displayName", "Pipeline"),
                                    "type": "DataPipeline",
                                    "properties": metadata
                                }
                                files["manifest.json"] = json.dumps(manifest, indent=2).encode('utf-8')
                            except:
                                files["manifest.json"] = content
                        else:
                            files[rel_path] = content
                results[pid] = files
            return results

    def analyze_pipeline_json(self, pipeline_json: dict, client_name: str):
        """Analyzes a Fabric pipeline JSON and returns intelligence-style data"""
        activities = pipeline_json.get("properties", {}).get("activities", [])
        
        # Simple extraction logic similar to DiscoveryEngine
        ingestion_support = {
            "file_based": any("S3" in str(a) or "ADLS" in str(a) or "File" in str(a) for a in activities),
            "api": any("Rest" in str(a) or "Http" in str(a) for a in activities),
            "database": any("Sql" in str(a) or "Jdbc" in str(a) for a in activities),
            "streaming": "EventHub" in str(pipeline_json),
            "batch": True
        }
        
        file_types = []
        raw_str = json.dumps(pipeline_json).lower()
        if "csv" in raw_str: file_types.append("CSV")
        if "json" in raw_str: file_types.append("JSON")
        if "parquet" in raw_str: file_types.append("Parquet")
        
        return {
            "framework": "Microsoft Fabric",
            "scan_status": "success",
            "auth_mode": "sso",
            "is_fallback": False,
            "source_systems": [{"type": "Fabric", "name": client_name}],
            "discovered_assets": [{"type": "Pipeline", "name": pipeline_json.get("name", "Unknown")}],
            "data_pipelines": [{"name": pipeline_json.get("name", "Fabric Pipeline"), "type": "Fabric"}],
            "ingestion_support": ingestion_support,
            "ingestion_details": {"source_type": "FABRIC", "target": "fabric"},
            "pipeline_capabilities": {"discovery": True, "export": True},
            "file_types": file_types or ["Not Available"],
            "original_config": pipeline_json,
            "reformatted_config": {
                "client": client_name,
                "source_type": "FABRIC",
                "activities_count": len(activities)
            },
            "interactive_flow": ["Connect to Fabric", "Extract Pipeline", "Analyze Logic", "Generate Config"]
        }
