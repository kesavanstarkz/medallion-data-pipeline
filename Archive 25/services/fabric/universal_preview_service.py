import asyncio
import base64
import json
import time
from typing import Any, Dict, List, Optional

import httpx
import logging
from fastapi import HTTPException

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
UNIVERSAL_PREVIEW_NOTEBOOK_NAME = "RuntimeUniversalPreviewNotebook"


def _first_key(data: Any, candidates: List[str]) -> Any:
    wanted = {item.lower() for item in candidates}
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).lower() in wanted:
                return value
            found = _first_key(value, candidates)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _first_key(item, candidates)
            if found is not None:
                return found
    return None


def _normalize_format(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "csv"
    aliases = {
        "text": "txt",
        "delimitedtext": "csv",
    }
    return aliases.get(text, text)


def build_universal_preview_notebook_source(preview_request: Dict[str, Any]) -> str:
    # Extract dynamic values from the preview_request
    spec = preview_request.get("resolved_source") or preview_request
    meta = spec.get("connection_metadata") or {}
    read_options = spec.get("read_options") or {}
    
    # Resolve path
    source_type = str(spec.get("source_type") or "").lower()
    connector_type = str(spec.get("connector_type") or "").lower()
    explicit_path = meta.get("previewPath") or meta.get("fullPath") or meta.get("path")
    if not explicit_path and ("lakehouse" in source_type or "lakehouse" in connector_type or "onelake" in source_type):
        workspace_id = meta.get("workspaceId") or meta.get("workspace_id")
        artifact_id = meta.get("artifactId") or meta.get("artifact_id")
        resolved_path = meta.get("resolvedPath") or meta.get("resolved_path")
        if workspace_id and artifact_id and resolved_path:
            item_type = meta.get("itemType") or "Lakehouse"
            explicit_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{artifact_id}.{item_type}/{resolved_path.lstrip('/')}"
    if not explicit_path:
        explicit_path = meta.get("abfssPath") or meta.get("s3Path") or meta.get("blobPath") or meta.get("resolvedPath") or meta.get("resolved_path") or ""

    # Resolve CSV options
    header_enabled = bool(read_options.get("header", True))
    delimiter = str(read_options.get("delimiter") or ",")
    quote_char = str(read_options.get("quote") or '"')
    escape_char = str(read_options.get("escape") or '\\')

    path_lit = json.dumps(explicit_path)
    delim_lit = json.dumps(delimiter)
    quote_lit = json.dumps(quote_char)
    escape_lit = json.dumps(escape_char)
    header_lit = "True" if header_enabled else "False"

    # Construct strict PySpark CSV snippet as requested
    return f"""# Fabric notebook source

# METADATA ********************

# CELL ********************

from pyspark.sql import SparkSession
import json
import traceback

# In Microsoft Fabric, mssparkutils is built-in but can be imported if needed
try:
    from notebookutils import mssparkutils
except ImportError:
    pass

path = {path_lit}

try:
    df = spark.read \\
        .option("header", {header_lit}) \\
        .option("delimiter", {delim_lit}) \\
        .option("quote", {quote_lit}) \\
        .option("escape", {escape_lit}) \\
        .csv(path)

    sample_rows = df.limit(20).toPandas().to_dict(orient="records")

    schema = [
        {{
            "name": f.name,
            "type": str(f.dataType),
            "nullable": f.nullable
        }}
        for f in df.schema.fields
    ]

    result = {{
        "sample_rows": sample_rows,
        "schema": schema
    }}
except Exception as e:
    result = {{
        "error": str(e),
        "traceback": traceback.format_exc()
    }}

mssparkutils.notebook.exit(json.dumps(result))
"""


class FabricUniversalPreviewService:
    _NOTEBOOK_CACHE: Dict[str, Dict[str, Any]] = {}

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.request(method, url, headers=self.headers, **kwargs)
            return response

    @staticmethod
    def _json_object(response: httpx.Response) -> Dict[str, Any]:
        try:
            body = response.json()
        except Exception:
            return {}
        return body if isinstance(body, dict) else {}

    @classmethod
    def _cache_key(cls, workspace_id: str, display_name: str = UNIVERSAL_PREVIEW_NOTEBOOK_NAME) -> str:
        return f"{workspace_id}:{display_name}"

    @classmethod
    def _get_cached_notebook(cls, workspace_id: str, display_name: str = UNIVERSAL_PREVIEW_NOTEBOOK_NAME) -> Optional[Dict[str, Any]]:
        cached = cls._NOTEBOOK_CACHE.get(cls._cache_key(workspace_id, display_name))
        if not cached or not cached.get("resolved") or not cached.get("notebookId"):
            return None
        return cached

    @classmethod
    def _set_cached_notebook(cls, workspace_id: str, notebook: Dict[str, Any]) -> Dict[str, Any]:
        cached = {
            "workspaceId": workspace_id,
            "notebookId": notebook.get("id"),
            "displayName": notebook.get("displayName") or notebook.get("name") or UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
            "resolved": bool(notebook.get("id")),
            "cachedAt": time.time(),
        }
        cls._NOTEBOOK_CACHE[cls._cache_key(workspace_id, cached["displayName"])] = cached
        return cached

    @classmethod
    def _clear_cached_notebook(cls, workspace_id: str, display_name: str = UNIVERSAL_PREVIEW_NOTEBOOK_NAME) -> None:
        cache_key = cls._cache_key(workspace_id, display_name)
        if cache_key in cls._NOTEBOOK_CACHE:
            del cls._NOTEBOOK_CACHE[cache_key]

    async def _wait_lro(self, response: httpx.Response) -> Dict[str, Any]:
        final_body: Dict[str, Any] = {}
        if response.status_code != 202:
            return self._json_object(response)
        location = response.headers.get("Location")
        if not location:
            return final_body
            
        backoff_intervals = [3, 6, 12, 20, 30]
        start_time = time.time()
        timeout = 300  # 5 minutes
        attempt = 0
        
        while time.time() - start_time < timeout:
            sleep_time = backoff_intervals[attempt] if attempt < len(backoff_intervals) else 30
            await asyncio.sleep(sleep_time)
            attempt += 1
            
            poll = await self._request("GET", location)
            poll_body = self._json_object(poll)
            if poll.status_code in (200, 201, 204):
                status = str(poll_body.get("status") or "").lower()
                if status in ("failed", "canceled", "cancelled"):
                    raise HTTPException(status_code=500, detail=f"Fabric LRO failed with status {status}: {poll.text}")
                if status in ("notstarted", "running"):
                    continue
                final_body = poll_body or final_body
                break
            if poll.status_code == 202:
                continue
            if poll.is_error:
                raise HTTPException(status_code=poll.status_code, detail=f"Fabric LRO failed: {poll.text}")
        
        # Try to fetch result if applicable, but often ResourceLocation is in the poll_body
        result_url = f"{location.rstrip('/')}/result"
        result = await self._request("GET", result_url)
        if result.is_success:
            result_body = self._json_object(result)
            if result_body:
                final_body.update(result_body)
        return final_body

    async def list_notebooks(self, workspace_id: str) -> List[Dict[str, Any]]:
        response = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks")
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"List notebooks failed: {response.text}")
        try:
            body = response.json()
        except Exception:
            return []
        if isinstance(body, list):
            return [item for item in body if isinstance(item, dict)]
        if isinstance(body, dict):
            value = body.get("value")
            return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        return []

    async def list_workspace_items(self, workspace_id: str, item_type: Optional[str] = None) -> List[Dict[str, Any]]:
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        if item_type:
            url = f"{url}?type={item_type}"
        response = await self._request("GET", url)
        if not response.is_success:
            return []
        try:
            body = response.json()
        except Exception:
            return []
        if isinstance(body, list):
            return [item for item in body if isinstance(item, dict)]
        if isinstance(body, dict):
            value = body.get("value")
            return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        return []

    async def get_notebook(self, workspace_id: str, notebook_id: str) -> Optional[Dict[str, Any]]:
        response = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}")
        if response.status_code == 404:
            return None
        if not response.is_success:
            return None
        body = self._json_object(response)
        return body or None

    async def create_notebook(self, workspace_id: str, display_name: str, source_code: str) -> Dict[str, Any]:
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        payload = {
            "displayName": display_name,
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            },
        }
        response = await self._request("POST", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks", json=payload)
        if response.status_code not in (200, 201, 202):
            raise HTTPException(status_code=response.status_code, detail=f"Create notebook failed: {response.text}")
        lro_body = await self._wait_lro(response)
        body = lro_body or self._json_object(response)
        notebook_id = body.get("id") or _first_key(body, ["itemId", "artifactId", "notebookId", "workspaceObjectId"])
        if not notebook_id and body.get("resourceLocation"):
            notebook_id = body.get("resourceLocation").rstrip("/").split("/")[-1]
            
        return {
            "body": body,
            "operationId": response.headers.get("x-ms-operation-id"),
            "requestId": response.headers.get("request-id") or response.headers.get("x-ms-request-id"),
            "location": response.headers.get("Location"),
            "notebookId": notebook_id,
            "displayName": body.get("displayName") or body.get("name") or display_name,
        }

    async def delete_notebook(self, workspace_id: str, notebook_id: str) -> None:
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}"
        logger.info(f"Deleting stale notebook: {notebook_id} at {url}")
        response = await self._request("DELETE", url)
        if response.status_code not in (200, 202, 204, 404):
            logger.warning(f"Failed to delete old notebook {notebook_id}: {response.text}")

    async def update_notebook_definition(self, workspace_id: str, notebook_id: str, source_code: str) -> None:
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        payload = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        response = await self._request(
            "POST",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/updateDefinition?updateMetadata=false",
            json=payload,
        )
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=response.status_code, detail=f"Update notebook definition failed: {response.text}")
        await self._wait_lro(response)

    async def _resolve_notebook_by_name(self, workspace_id: str, display_name: str) -> Optional[Dict[str, Any]]:
        notebooks = await self.list_notebooks(workspace_id)
        exact = next((item for item in notebooks if item.get("displayName") == display_name and str(item.get("type") or "Notebook").lower() == "notebook"), None)
        if exact and exact.get("id"):
            return exact
        items = await self.list_workspace_items(workspace_id, item_type="Notebook")
        exact = next((item for item in items if item.get("displayName") == display_name and str(item.get("type") or "Notebook").lower() == "notebook"), None)
        if exact and exact.get("id"):
            return exact
        recent = next((item for item in reversed(items) if item.get("displayName") == display_name), None)
        if recent and recent.get("id"):
            return recent
        return None

    async def _poll_for_notebook_resolution(
        self,
        workspace_id: str,
        display_name: str,
        notebook_id: Optional[str],
        diagnostics: List[Dict[str, Any]],
        max_retries: int = 30,
        base_delay_seconds: int = 4,
    ) -> Optional[Dict[str, Any]]:
        delay_seconds = base_delay_seconds
        for attempt in range(1, max_retries + 1):
            diagnostics.append({
                "state": "notebook_polling",
                "attempt": attempt,
                "workspaceId": workspace_id,
                "notebookId": notebook_id,
                "displayName": display_name,
                "delaySeconds": delay_seconds,
            })
            if notebook_id:
                by_id = await self.get_notebook(workspace_id, notebook_id)
                if by_id and by_id.get("id"):
                    diagnostics.append({
                        "state": "notebook_visible",
                        "resolution": "itemId",
                        "workspaceId": workspace_id,
                        "notebookId": by_id.get("id"),
                        "displayName": by_id.get("displayName"),
                    })
                    return by_id
            by_name = await self._resolve_notebook_by_name(workspace_id, display_name)
            if by_name and by_name.get("id"):
                diagnostics.append({
                    "state": "notebook_visible",
                    "resolution": "displayName",
                    "workspaceId": workspace_id,
                    "notebookId": by_name.get("id"),
                    "displayName": by_name.get("displayName"),
                })
                return by_name
            await asyncio.sleep(delay_seconds)
            delay_seconds = min(delay_seconds * 2, 12)
        return None

    async def ensure_preview_notebook(self, workspace_id: str, source_code: str, diagnostics: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        diagnostics = diagnostics if diagnostics is not None else []
        cached = self._get_cached_notebook(workspace_id)
        if cached:
            diagnostics.append({
                "state": "notebook_cached",
                "workspaceId": workspace_id,
                "notebookId": cached.get("notebookId"),
                "displayName": cached.get("displayName"),
            })
            existing = await self.get_notebook(workspace_id, cached["notebookId"])
            if existing and existing.get("id"):
                diagnostics.append({"state": "notebook_delete_started", "notebookId": existing["id"]})
                await self.delete_notebook(workspace_id, existing["id"])
                self._clear_cached_notebook(workspace_id)

        existing = await self._resolve_notebook_by_name(workspace_id, UNIVERSAL_PREVIEW_NOTEBOOK_NAME)
        if existing and existing.get("id"):
            diagnostics.append({"state": "notebook_delete_started", "notebookId": existing["id"]})
            await self.delete_notebook(workspace_id, existing["id"])
            self._clear_cached_notebook(workspace_id)

        diagnostics.append({
            "state": "notebook_create_started",
            "workspaceId": workspace_id,
            "displayName": UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
        })
        created = await self.create_notebook(workspace_id, UNIVERSAL_PREVIEW_NOTEBOOK_NAME, source_code)
        diagnostics.append({
            "state": "notebook_create_completed",
            "workspaceId": workspace_id,
            "displayName": UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
            "operationId": created.get("operationId"),
            "requestId": created.get("requestId"),
            "notebookId": created.get("notebookId"),
            "location": created.get("location"),
        })
        resolved = await self._poll_for_notebook_resolution(
            workspace_id=workspace_id,
            display_name=UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
            notebook_id=created.get("notebookId"),
            diagnostics=diagnostics,
        )
        if not resolved or not resolved.get("id"):
            raise HTTPException(
                status_code=408,
                detail={
                    "message": f"Unable to resolve the runtime universal preview notebook after creation. workspaceId={workspace_id} operationId={created.get('operationId') or ''}",
                    "workspaceId": workspace_id,
                    "operationId": created.get("operationId"),
                    "requestId": created.get("requestId"),
                    "createdNotebookId": created.get("notebookId"),
                    "location": created.get("location"),
                    "lifecycle": diagnostics,
                },
            )
        self._set_cached_notebook(workspace_id, resolved)
        diagnostics.append({
            "state": "notebook_cached_saved",
            "workspaceId": workspace_id,
            "notebookId": resolved.get("id"),
            "displayName": resolved.get("displayName"),
        })
        return resolved

    async def run_notebook(self, workspace_id: str, notebook_id: str) -> str:
        payload = {
            "executionData": {
                "parameters": {}
            }
        }
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
        
        logger.info(f"Notebook Execution Endpoint: {url} | Method: POST | JobType: RunNotebook | Payload: {json.dumps(payload)}")
        
        response = await self._request("POST", url, json=payload)
        if response.status_code not in (200, 201, 202):
            if "InvalidJobType" in response.text:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "message": "Fabric returned InvalidJobType error.",
                        "endpoint": url,
                        "payload": payload,
                        "error_response": response.text
                    }
                )
            raise HTTPException(status_code=response.status_code, detail=f"Run notebook failed: {response.text}")
            
        location = response.headers.get("Location", "")
        job_instance_id = location.rstrip("/").split("/")[-1] if location else None
        body = self._json_object(response)
        job_instance_id = job_instance_id or body.get("id")
        
        logger.info(f"Notebook Run ID: {job_instance_id} | Location: {location}")
        
        if not job_instance_id:
            raise HTTPException(status_code=500, detail="Notebook run did not return a job instance id.")
        return job_instance_id

    async def get_notebook_job_instance(self, workspace_id: str, notebook_id: str, job_instance_id: str) -> Dict[str, Any]:
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_instance_id}"
        response = await self._request("GET", url)
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"Get notebook job instance failed: {response.text}")
        body = self._json_object(response)
        return body

    async def execute_preview(self, workspace_id: str, preview_request: Dict[str, Any], timeout_seconds: int = 600) -> Dict[str, Any]:
        source_code = build_universal_preview_notebook_source(preview_request)
        notebook_diagnostics: List[Dict[str, Any]] = []
        try:
            notebook = await self.ensure_preview_notebook(workspace_id, source_code, diagnostics=notebook_diagnostics)
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
            detail.setdefault("notebook_diagnostics", notebook_diagnostics)
            return {
                "preview_error": detail.get("message") or "Notebook provisioning failed.",
                "notebook_diagnostics": detail.get("notebook_diagnostics") or notebook_diagnostics,
                "provisioning_detail": detail,
            }
        notebook_id = notebook.get("id")
        if not notebook_id:
            return {
                "preview_error": "Preview notebook id could not be resolved.",
                "notebook_diagnostics": notebook_diagnostics,
            }

        notebook_diagnostics.append({
            "state": "notebook_execution_started",
            "workspaceId": workspace_id,
            "notebookId": notebook_id,
            "displayName": notebook.get("displayName"),
        })
        job_instance_id = await self.run_notebook(workspace_id, notebook_id)
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            instance = await self.get_notebook_job_instance(workspace_id, notebook_id, job_instance_id)
            if not isinstance(instance, dict):
                return {
                    "preview_error": "Notebook job instance API returned a non-object response.",
                    "job_instance": instance,
                    "notebook_diagnostics": notebook_diagnostics,
                }
            status = str(_first_key(instance, ["status", "state"]) or "")
            logger.info(f"Notebook Run Poll Status: {status} | Run ID: {job_instance_id}")
            
            if status.lower() in {"completed", "succeeded", "success"}:
                notebook_diagnostics.append({
                    "state": "notebook_execution_completed",
                    "workspaceId": workspace_id,
                    "notebookId": notebook_id,
                    "jobInstanceId": job_instance_id,
                    "status": status,
                })
                exit_value = _first_key(instance, ["exitValue"])
                logger.info(f"Notebook Run Output (truncated): {str(exit_value)[:500] if exit_value else 'None'}")
                logger.info(json.dumps(instance, indent=2))
                
                if not exit_value:
                    logger.error(
                        f"Missing exitValue! Notebook ID: {notebook_id} | Job Instance ID: {job_instance_id} | "
                        f"Instance Payload: {json.dumps(instance)} | "
                        f"Stdout: {str(_first_key(instance, ['stdout', 'logs']) or 'N/A')} | "
                        f"Stderr: {str(_first_key(instance, ['stderr', 'error']) or 'N/A')}"
                    )
                    return {
                        "message": "Notebook completed but exit value missing",
                        "raw_execution_response": instance,
                        "stdout": _first_key(instance, ["stdout", "logs"]) or "N/A",
                        "stderr": _first_key(instance, ["stderr", "error"]) or "N/A",
                        "jobInstanceId": job_instance_id,
                        "notebook_diagnostics": notebook_diagnostics,
                    }
                try:
                    payload = json.loads(exit_value)
                    if "error" in payload:
                        return {
                            "preview_error": f"Spark Exception: {payload['error']} | Traceback: {payload.get('traceback', '')}",
                            "notebook_diagnostics": notebook_diagnostics,
                        }
                    return {
                        "sample_rows": payload.get("sample_rows", []),
                        "schema": payload.get("schema", []),
                        "notebook_diagnostics": notebook_diagnostics,
                    }
                except Exception:
                    return {
                        "preview_error": "Notebook returned a non-JSON exit value.",
                        "exit_value": exit_value,
                        "job_instance": instance,
                        "notebook_diagnostics": notebook_diagnostics,
                    }
            if status.lower() in {"failed", "cancelled", "canceled"}:
                raise HTTPException(status_code=500, detail=f"Universal preview notebook failed with status {status}.")
            await asyncio.sleep(5)
        raise HTTPException(status_code=504, detail="Universal preview notebook timed out.")
