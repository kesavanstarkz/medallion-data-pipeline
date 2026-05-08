"""
Fabric Spark Preview Service
=============================
Uses Fabric Spark notebook execution to read source files, writes preview data
to a temporary Delta table, then reads the table via the Lakehouse SQL Analytics
endpoint. No direct OneLake access. No exitValue dependency.

Flow:
  1. Create/update notebook → Spark reads source, writes to __runtime_preview_cache table
  2. Execute notebook via Fabric Jobs API
  3. Discover SQL endpoint via GET /lakehouses/{id}
  4. Query __runtime_preview_cache via pyodbc (service principal TDS auth)
  5. Clean up: drop the preview table via a second notebook execution
  6. Return schema_discovery
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
import pyodbc
from fastapi import HTTPException

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
PREVIEW_TABLE_NAME = "__runtime_preview_cache"
PREVIEW_NOTEBOOK_NAME = "RuntimePreviewNotebook"


# ─────────────────────────────────────────────────────────────
# Notebook source generation
# ─────────────────────────────────────────────────────────────

def _build_preview_notebook_source(
    resolved_path: str,
    source_format: str = "csv",
    delimiter: str = ",",
    quote_char: str = '"',
    escape_char: str = "\\",
    header_enabled: bool = True,
    workspace_id: str = "",
    artifact_id: str = "",
) -> str:
    """Generate PySpark notebook source that reads the source file and
    writes schema + sample rows to a Delta table."""

    path_lit = json.dumps(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{artifact_id}/{resolved_path}")
    delim_lit = json.dumps(delimiter)
    quote_lit = json.dumps(quote_char)
    escape_lit = json.dumps(escape_char)
    header_lit = "True" if header_enabled else "False"
    table_name = PREVIEW_TABLE_NAME

    if source_format in ("json",):
        read_block = f"df = spark.read.json({path_lit})"
    elif source_format in ("parquet",):
        read_block = f"df = spark.read.parquet({path_lit})"
    elif source_format in ("delta",):
        read_block = f'df = spark.read.format("delta").load({path_lit})'
    else:
        # CSV / delimited text (default)
        read_block = f"""df = spark.read \\
    .option("header", {header_lit}) \\
    .option("delimiter", {delim_lit}) \\
    .option("quote", {quote_lit}) \\
    .option("escape", {escape_lit}) \\
    .csv({path_lit})"""

    return f"""# Fabric notebook source

# METADATA ********************

# META {{
# META   "kernel_info": {{
# META     "name": "synapse_pyspark"
# META   }},
# META   "dependencies": {{
# META     "lakehouse": {{
# META       "default_lakehouse": "{artifact_id}",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "{workspace_id}"
# META     }}
# META   }}
# META }}

# CELL ********************

import json

{read_block}

columns = [
    {{
        "column_name": f.name,
        "data_type": str(f.dataType),
        "nullable": f.nullable,
        "ordinal_position": idx + 1
    }}
    for idx, f in enumerate(df.schema.fields)
]

nullable_columns = [f.name for f in df.schema.fields if f.nullable]
timestamp_columns = [
    f.name for f in df.schema.fields
    if "timestamp" in str(f.dataType).lower() or "date" in str(f.dataType).lower()
    or any(t in f.name.lower() for t in ("date", "time", "timestamp"))
]

sample_rows = df.limit(20).toPandas().to_dict(orient="records")

# Serialize each row to JSON string for safe Delta storage
import pandas as pd

rows_data = []
for i, row in enumerate(sample_rows):
    rows_data.append((i, json.dumps(row, default=str)))

for col in columns:
    col["sample_value"] = str(sample_rows[0].get(col["column_name"])) if sample_rows else None

schema_json = json.dumps({{
    "columns": columns,
    "nullable_columns": nullable_columns,
    "timestamp_columns": timestamp_columns,
    "primary_key_candidates": [],
    "row_count": len(sample_rows),
}}, default=str)

# Write schema as one row, sample rows as additional rows
preview_data = [(0, "schema", schema_json)]
for idx, row_json in rows_data:
    preview_data.append((idx + 1, "row", row_json))

preview_df = spark.createDataFrame(preview_data, ["row_index", "row_type", "payload"])
preview_df.write.mode("overwrite").saveAsTable("{table_name}")

print("PREVIEW_TABLE_WRITTEN")
"""


def _build_cleanup_notebook_source(artifact_id: str, workspace_id: str) -> str:
    """Generate PySpark source to drop the preview cache table."""
    return f"""# Fabric notebook source

# METADATA ********************

# META {{
# META   "kernel_info": {{
# META     "name": "synapse_pyspark"
# META   }},
# META   "dependencies": {{
# META     "lakehouse": {{
# META       "default_lakehouse": "{artifact_id}",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "{workspace_id}"
# META     }}
# META   }}
# META }}

# CELL ********************

spark.sql("DROP TABLE IF EXISTS {PREVIEW_TABLE_NAME}")
print("PREVIEW_TABLE_DROPPED")
"""


# ─────────────────────────────────────────────────────────────
# Format detection
# ─────────────────────────────────────────────────────────────

def _detect_format(source_connection: Dict[str, Any]) -> str:
    fmt = str(source_connection.get("format") or "").lower()
    source_type = str(source_connection.get("source_type") or "").lower()
    connector_type = str(source_connection.get("connector_type") or "").lower()
    file_name = str(source_connection.get("file_name") or "").lower()
    combined = f"{fmt} {source_type} {connector_type} {file_name}"
    if any(t in combined for t in ("delimited", "csv", "txt", "text")):
        return "csv"
    if "parquet" in combined:
        return "parquet"
    if "json" in combined:
        return "json"
    if "delta" in combined:
        return "delta"
    if file_name.endswith(".csv") or file_name.endswith(".txt") or file_name.endswith(".tsv"):
        return "csv"
    if file_name.endswith(".json"):
        return "json"
    if file_name.endswith(".parquet"):
        return "parquet"
    return "csv"


# ─────────────────────────────────────────────────────────────
# Fabric API helpers
# ─────────────────────────────────────────────────────────────

class FabricSparkPreviewService:
    """
    Spark-based preview engine.
    Executes a notebook to read source data via Spark,
    writes to a Delta table, reads via SQL analytics endpoint.
    """

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        self.azure_tenant_id = os.getenv("AZURE_TENANT_ID")
        self.azure_client_id = os.getenv("AZURE_CLIENT_ID")
        self.azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")

    async def _request(self, method: str, url: str, max_retries: int = 3, **kwargs) -> httpx.Response:
        backoff = [2, 5, 10]
        for attempt in range(max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    resp = await client.request(method, url, headers=self.headers, **kwargs)
                if resp.status_code in (429, 502, 503, 504) and attempt < max_retries:
                    await asyncio.sleep(backoff[min(attempt, len(backoff) - 1)])
                    continue
                return resp
            except (httpx.RequestError, httpx.TimeoutException):
                if attempt < max_retries:
                    await asyncio.sleep(backoff[min(attempt, len(backoff) - 1)])
                    continue
                raise
        raise HTTPException(status_code=502, detail="Fabric API retries exhausted.")

    # ── Notebook lifecycle ──

    @staticmethod
    def _extract_notebook_id(obj: Dict[str, Any]) -> Optional[str]:
        """Safely extract notebook ID from any Fabric API response shape."""
        for key in ("id", "itemId", "notebookId", "artifactId"):
            val = obj.get(key)
            if val and isinstance(val, str):
                return val
        return None

    async def _find_notebook(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        resp = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks")
        if not resp.is_success:
            logger.warning("Notebook list failed: HTTP %s", resp.status_code)
            return None
        items = resp.json().get("value") or []
        for nb in items:
            if nb.get("displayName") == PREVIEW_NOTEBOOK_NAME:
                return nb
        return None

    async def _poll_notebook_visibility(
        self,
        workspace_id: str,
        hint_id: Optional[str] = None,
        timeout: int = 90,
    ) -> Dict[str, Any]:
        """Poll workspace until the preview notebook becomes visible.

        Handles Fabric eventual consistency: notebook creation may succeed
        but not appear in the workspace list immediately.
        """
        backoff = [2, 4, 8, 12, 20, 30]
        start = time.time()
        attempt = 0

        while time.time() - start < timeout:
            sleep_secs = backoff[attempt] if attempt < len(backoff) else 30
            logger.info(
                "Polling notebook visibility: attempt=%d hint_id=%s sleeping=%ds",
                attempt + 1, hint_id, sleep_secs,
            )
            await asyncio.sleep(sleep_secs)

            resp = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks")
            if resp.is_success:
                items = resp.json().get("value") or []
                logger.info("Workspace notebook list: %d items on attempt %d", len(items), attempt + 1)
                for nb in items:
                    nb_id = self._extract_notebook_id(nb)
                    # Match by hint_id first, then fall back to display name
                    if hint_id and nb_id == hint_id:
                        logger.info("Notebook visible by id match: id=%s", nb_id)
                        return nb
                    if nb.get("displayName") == PREVIEW_NOTEBOOK_NAME:
                        logger.info("Notebook visible by name match: id=%s", nb_id)
                        return nb
            else:
                logger.warning("Notebook list returned HTTP %s on attempt %d", resp.status_code, attempt + 1)

            attempt += 1

        raise HTTPException(
            status_code=500,
            detail="Notebook creation succeeded but notebook did not become visible within timeout (90s). Fabric eventual consistency exceeded.",
        )

    async def _create_notebook(self, workspace_id: str, source_code: str) -> Dict[str, Any]:
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        body = {
            "displayName": PREVIEW_NOTEBOOK_NAME,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {"path": "notebook-content.py", "payload": payload_b64, "payloadType": "InlineBase64"}
                ],
            },
        }
        resp = await self._request("POST", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks", json=body)

        if resp.status_code == 201:
            result = resp.json()
            logger.info("Notebook created (201): keys=%s", list(result.keys()))
            nb_id = self._extract_notebook_id(result)
            if nb_id:
                return result
            # 201 but no id — wait for visibility
            logger.warning("201 response has no id, polling for visibility. keys=%s", list(result.keys()))
            return await self._poll_notebook_visibility(workspace_id, hint_id=None)

        if resp.status_code == 202:
            lro_result = await self._wait_lro(resp)
            logger.info(
                "Notebook creation LRO completed: keys=%s payload=%s",
                list(lro_result.keys()) if lro_result else "empty",
                json.dumps(lro_result, default=str)[:300] if lro_result else "{}",
            )
            # Extract any hint ID from the LRO result
            hint_id = self._extract_notebook_id(lro_result) if lro_result else None
            if hint_id:
                logger.info("LRO returned id=%s, verifying visibility", hint_id)
                # Still poll briefly to confirm visibility before proceeding
                return await self._poll_notebook_visibility(workspace_id, hint_id=hint_id, timeout=30)
            # No id in LRO result — full visibility poll
            logger.info("LRO result has no id, polling workspace for notebook visibility")
            return await self._poll_notebook_visibility(workspace_id, hint_id=None)

        raise HTTPException(status_code=resp.status_code, detail=f"Notebook creation failed: {resp.text}")

    async def _update_notebook(self, workspace_id: str, notebook_id: str, source_code: str) -> None:
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        body = {
            "definition": {
                "format": "ipynb",
                "parts": [
                    {"path": "notebook-content.py", "payload": payload_b64, "payloadType": "InlineBase64"}
                ],
            },
        }
        resp = await self._request(
            "POST",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition",
            json=body,
        )
        if resp.status_code == 200:
            return
        if resp.status_code == 202:
            await self._wait_lro(resp)
            return
        raise HTTPException(status_code=resp.status_code, detail=f"Notebook update failed: {resp.text}")

    async def _ensure_notebook(self, workspace_id: str, source_code: str) -> str:
        """Ensure the preview notebook exists with current source. Returns notebook_id."""
        existing = await self._find_notebook(workspace_id)
        if existing:
            notebook_id = self._extract_notebook_id(existing)
            if not notebook_id:
                logger.error(
                    "Existing notebook has no id. keys=%s full=%s",
                    list(existing.keys()),
                    json.dumps(existing, default=str)[:500],
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Existing notebook object has no id field. Keys: {list(existing.keys())}",
                )
            await self._update_notebook(workspace_id, notebook_id, source_code)
            logger.info("Notebook updated: id=%s", notebook_id)
            return notebook_id

        created = await self._create_notebook(workspace_id, source_code)
        notebook_id = self._extract_notebook_id(created)
        if not notebook_id:
            logger.error(
                "Created notebook has no id. keys=%s full=%s",
                list(created.keys()),
                json.dumps(created, default=str)[:500],
            )
            raise HTTPException(
                status_code=500,
                detail=f"Created notebook object has no id field. Keys: {list(created.keys())}",
            )
        logger.info("Notebook ensured: id=%s", notebook_id)
        return notebook_id


    # ── Execution ──

    async def _run_notebook(self, workspace_id: str, notebook_id: str) -> str:
        """Trigger notebook execution. Returns job instance ID."""
        resp = await self._request(
            "POST",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook",
        )
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            # Extract job instance ID from Location header
            parts = location.rstrip("/").split("/")
            return parts[-1] if parts else ""
        raise HTTPException(status_code=resp.status_code, detail=f"Notebook execution trigger failed: {resp.text}")

    async def _poll_job(self, workspace_id: str, notebook_id: str, job_id: str, timeout: int = 600) -> str:
        """Poll job until terminal state. Returns final status."""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_id}"
        start = time.time()
        backoff_intervals = [3, 6, 12, 20, 30]
        attempt = 0
        while time.time() - start < timeout:
            resp = await self._request("GET", url)
            if resp.is_success:
                status = (resp.json().get("status") or "").lower()
                logger.info("Notebook job poll: status=%s job_id=%s", status, job_id)
                if status in ("completed", "succeeded", "success"):
                    return "completed"
                if status in ("failed", "cancelled", "canceled"):
                    failure = resp.json().get("failureReason") or ""
                    raise HTTPException(status_code=500, detail=f"Notebook execution failed: {status}. {failure}")
            sleep_time = backoff_intervals[attempt] if attempt < len(backoff_intervals) else 30
            await asyncio.sleep(sleep_time)
            attempt += 1
        raise HTTPException(status_code=408, detail="Notebook execution timed out.")

    async def _wait_lro(self, response: httpx.Response) -> Dict[str, Any]:
        location = response.headers.get("Location")
        if not location:
            return {}
        for _ in range(30):
            await asyncio.sleep(5)
            poll = await self._request("GET", location)
            if poll.status_code in (200, 201):
                body = poll.json() if poll.text else {}
                status = str(body.get("status", "")).lower()
                if status not in ("notstarted", "running"):
                    return body
            if poll.status_code == 202:
                continue
        return {}

    # ── SQL Analytics endpoint ──

    async def _get_sql_endpoint(self, workspace_id: str, lakehouse_id: str) -> str:
        """Discover the SQL analytics endpoint for the Lakehouse."""
        resp = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
        if not resp.is_success:
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to get Lakehouse info: {resp.text}")
        props = resp.json().get("properties", {})
        sql_props = props.get("sqlEndpointProperties", {})
        conn_string = sql_props.get("connectionString", "")
        if not conn_string:
            raise HTTPException(status_code=500, detail="Lakehouse SQL analytics endpoint not available. It may still be provisioning.")
        return conn_string

    def _query_preview_table(self, sql_server: str, database: str) -> Dict[str, Any]:
        """Query the __runtime_preview_cache table via pyodbc."""
        if not self.azure_tenant_id or not self.azure_client_id or not self.azure_client_secret:
            raise HTTPException(status_code=500, detail="Azure credentials not configured for SQL endpoint access.")

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={sql_server};"
            f"DATABASE={database};"
            f"UID={self.azure_client_id}@{self.azure_tenant_id};"
            f"PWD={self.azure_client_secret};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )

        try:
            conn = pyodbc.connect(conn_str, timeout=30)
            cursor = conn.cursor()
            cursor.execute(f"SELECT row_type, payload FROM [{PREVIEW_TABLE_NAME}] ORDER BY row_index")
            rows = cursor.fetchall()
            conn.close()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"SQL endpoint query failed: {str(exc)}")

        schema_info = {}
        sample_rows = []
        for row_type, payload in rows:
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if row_type == "schema":
                schema_info = data
            elif row_type == "row":
                sample_rows.append(data)

        return {
            "columns": schema_info.get("columns", []),
            "nullable_columns": schema_info.get("nullable_columns", []),
            "timestamp_columns": schema_info.get("timestamp_columns", []),
            "primary_key_candidates": schema_info.get("primary_key_candidates", []),
            "sample_rows": sample_rows,
        }

    # ── Main orchestration ──

    async def execute_preview(self, source_connection: Dict[str, Any]) -> Dict[str, Any]:
        """
        Full preview flow:
        1. Build notebook source
        2. Ensure notebook exists
        3. Execute notebook (Spark reads source → writes Delta table)
        4. Read table via SQL analytics endpoint
        5. Clean up preview table
        6. Return schema_discovery
        """
        workspace_id = source_connection.get("workspace_id")
        artifact_id = source_connection.get("artifact_id")
        resolved_path = source_connection.get("resolved_path")
        diagnostics: List[Dict[str, Any]] = []

        if not workspace_id or not artifact_id or not resolved_path:
            raise HTTPException(status_code=400, detail={
                "error": "Incomplete source metadata",
                "details": f"workspace_id={workspace_id}, artifact_id={artifact_id}, resolved_path={resolved_path}",
            })

        detected_format = _detect_format(source_connection)
        diagnostics.append({
            "step": "format_detection",
            "status": "success",
            "format": detected_format,
        })

        # 1. Build notebook source
        source_code = _build_preview_notebook_source(
            resolved_path=resolved_path,
            source_format=detected_format,
            delimiter=source_connection.get("delimiter") or ",",
            quote_char=source_connection.get("quote_char") or '"',
            escape_char=source_connection.get("escape_char") or "\\",
            header_enabled=source_connection.get("header_enabled", True),
            workspace_id=workspace_id,
            artifact_id=artifact_id,
        )
        logger.info("Preview notebook source generated (%d chars)", len(source_code))

        # 2. Ensure notebook
        try:
            notebook_id = await self._ensure_notebook(workspace_id, source_code)
            diagnostics.append({"step": "notebook_provision", "status": "success", "notebook_id": notebook_id})
        except HTTPException as exc:
            diagnostics.append({"step": "notebook_provision", "status": "failed", "error": str(exc.detail)})
            raise

        # 3. Execute notebook
        try:
            job_id = await self._run_notebook(workspace_id, notebook_id)
            diagnostics.append({"step": "notebook_execute", "status": "started", "job_id": job_id})
        except HTTPException as exc:
            diagnostics.append({"step": "notebook_execute", "status": "failed", "error": str(exc.detail)})
            raise

        # 4. Wait for completion
        try:
            await self._poll_job(workspace_id, notebook_id, job_id)
            diagnostics.append({"step": "notebook_execute", "status": "completed", "job_id": job_id})
        except HTTPException as exc:
            diagnostics.append({"step": "notebook_execute", "status": "failed", "error": str(exc.detail)})
            raise

        # 5. Discover SQL endpoint
        try:
            sql_server = await self._get_sql_endpoint(workspace_id, artifact_id)
            diagnostics.append({"step": "sql_endpoint", "status": "success", "server": sql_server})
        except HTTPException as exc:
            diagnostics.append({"step": "sql_endpoint", "status": "failed", "error": str(exc.detail)})
            raise

        # 6. Get Lakehouse display name for database name
        try:
            lh_resp = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses/{artifact_id}")
            lh_name = lh_resp.json().get("displayName", artifact_id) if lh_resp.is_success else artifact_id
        except Exception:
            lh_name = artifact_id

        # 7. Query the preview table
        try:
            schema_discovery = self._query_preview_table(sql_server, lh_name)
            diagnostics.append({
                "step": "sql_query",
                "status": "success",
                "columns": len(schema_discovery.get("columns", [])),
                "sample_rows": len(schema_discovery.get("sample_rows", [])),
            })
        except HTTPException as exc:
            diagnostics.append({"step": "sql_query", "status": "failed", "error": str(exc.detail)})
            raise

        # 8. Validate
        if not schema_discovery.get("columns") or not schema_discovery.get("sample_rows"):
            raise HTTPException(status_code=500, detail={
                "error": "Preview extraction failed: Spark read the source but no preview data was extracted.",
                "diagnostics": diagnostics,
            })

        # 9. Clean up (best-effort, don't block on failure)
        try:
            cleanup_source = _build_cleanup_notebook_source(artifact_id, workspace_id)
            await self._update_notebook(workspace_id, notebook_id, cleanup_source)
            cleanup_job_id = await self._run_notebook(workspace_id, notebook_id)
            # Don't wait for cleanup to finish — fire and forget
            diagnostics.append({"step": "cleanup", "status": "triggered", "job_id": cleanup_job_id})
        except Exception as exc:
            diagnostics.append({"step": "cleanup", "status": "skipped", "reason": str(exc)})

        logger.info(
            "Spark Preview Success | workspace=%s artifact=%s path=%s columns=%d rows=%d",
            workspace_id, artifact_id, resolved_path,
            len(schema_discovery["columns"]), len(schema_discovery["sample_rows"]),
        )

        return {
            "schema_discovery": schema_discovery,
            "preview_mode": "spark_sql_endpoint",
            "diagnostics": diagnostics,
            "resolved_path": resolved_path,
            "source_name": source_connection.get("file_name") or resolved_path,
        }
