"""
Fabric Spark Preview Service
=============================
Universal Dynamic Preview Engine with SQL Staging.

Flow:
  1. Dynamic Ingestion: Notebook reads ANY source (CSV, JSON, JDBC, etc.)
  2. SQL Staging: Data is loaded into a session-specific SQL table (runtime_preview_{session_id})
  3. Interactive Query: Backend executes dynamic SQL queries against the staging table via TDS/ODBC
  4. Lifecycle: Staging tables are dropped after session expiration

Features:
- Universal Source Connector Logic
- Read-only SQL Validation
- Direct Lakehouse SQL Analytics Endpoint Integration
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS_BASE = "https://onelake.dfs.fabric.microsoft.com"
PREVIEW_TABLE_PREFIX = "runtime_preview_"
PREVIEW_NOTEBOOK_NAME = "RuntimePreviewNotebook"
NEON_DB_URL = "postgresql://neondb_owner:npg_EyGsgV7kAKC4@ep-dark-morning-aqz49q4z-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require"
NEON_JDBC_URL = "jdbc:postgresql://ep-dark-morning-aqz49q4z-pooler.c-8.us-east-1.aws.neon.tech/neondb"

# Module-level cache: workspace_id → notebook_id.
_NOTEBOOK_ID_CACHE: Dict[str, str] = {}

# Session store: session_id → {workspace_id, artifact_id, staging_table}
_PREVIEW_SESSION_STORE: Dict[str, Dict[str, str]] = {}


# ─────────────────────────────────────────────────────────────
# Notebook source generation
# ─────────────────────────────────────────────────────────────

def _build_preview_notebook_source(source_connection: Dict[str, Any], session_id: str) -> str:
    """Generate PySpark notebook source with granular error handling and SQL staging."""

    workspace_id = source_connection.get("workspace_id", "")
    artifact_id = source_connection.get("artifact_id", "")
    resolved_path = source_connection.get("resolved_path", "")
    source_format = _detect_format(source_connection)
    source_type = str(source_connection.get("source_type") or "").lower()
    table_name = f"{PREVIEW_TABLE_PREFIX}{session_id.replace('-', '_')}"

    # Use fully-qualified ABFSS path for maximum reliability across environments
    abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{artifact_id}/{resolved_path}"
    path_lit = json.dumps(abfss_path)

    # ── Source-specific Spark reader ──
    if source_type in ("lakehousetable", "lakehouse_table", "table"):
        src_table = source_connection.get("table_name") or resolved_path
        read_block = f"df = spark.table({json.dumps(src_table)})"
    elif source_type in ("jdbc", "sql_server", "oracle", "postgresql", "mysql"):
        connection_options = source_connection.get("connection_options", {})
        options_str = ",\n        ".join([f".option({json.dumps(k)}, {json.dumps(v)})" for k, v in connection_options.items()])
        read_block = f"""df = spark.read \\
    .format("jdbc") \\
    {options_str} \\
    .load()"""
    elif source_type in ("rest", "rest_api", "web"):
        url = source_connection.get("url")
        method = source_connection.get("method", "GET")
        headers = json.dumps(source_connection.get("headers", {}))
        read_block = f"""import requests
response = requests.request({json.dumps(method)}, {json.dumps(url)}, headers={headers})
data = response.json()
if isinstance(data, dict): data = [data]
df = spark.createDataFrame(data)"""
    elif source_format == "json":
        read_block = f'df = spark.read.option("multiLine", True).json({path_lit})'
    elif source_format == "parquet":
        read_block = f"df = spark.read.parquet({path_lit})"
    elif source_format == "delta":
        read_block = f'df = spark.read.format("delta").load({path_lit})'
    else:
        delimiter = source_connection.get("delimiter") or ","
        header = source_connection.get("header_enabled", True)
        quote_char = source_connection.get("quote_char") or '"'
        escape_char = source_connection.get("escape_char") or "\\\\"
        header_lit = "True" if header else "False"
        read_block = f"""df = (spark.read
    .option("header", {header_lit})
    .option("inferSchema", True)
    .option("delimiter", {json.dumps(delimiter)})
    .option("quote", {json.dumps(quote_char)})
    .option("escape", {json.dumps(escape_char)})
    .csv({path_lit}))"""

    # Indent the read_block to fit into the try: block (4 spaces)
    indented_read_block = "\n".join(["    " + line for line in read_block.split("\n")])

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
import traceback
import os
import sys

def emit_error(stage, err):
    print("PREVIEW_ERROR_START")
    print(json.dumps({{
        "stage": stage,
        "error": str(err),
        "traceback": traceback.format_exc()
    }}, default=str))
    print("PREVIEW_ERROR_END")
    # Soft fail - don't sys.exit(1) as it can crash the API-initiated session
    return False

_preview_ok = True

import json
import traceback
from notebookutils import mssparkutils

try:
    print("PREVIEW_STATUS: Initializing ingestion...")
{indented_read_block}
    print("PREVIEW_STATUS: Data ingestion successful.")
    
    # 2. Save to Neon Postgres Staging
    staging_table = "{table_name}"
    print(f"PREVIEW_STATUS: Saving to Neon Postgres table {{staging_table}}...")
    
    url = "{NEON_JDBC_URL}"
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", staging_table) \
        .option("user", "neondb_owner") \
        .option("password", "npg_EyGsgV7kAKC4") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
        
    print("PREVIEW_STATUS: Staging successful.")

    # 3. Output basic success marker
    print("PREVIEW_RESULT_START")
    print(json.dumps({{
        "staging_table": staging_table,
        "success": True
    }}, default=str))
    print("PREVIEW_RESULT_END")

except Exception as e:
    print("PREVIEW_ERROR_START")
    print(json.dumps({{
        "stage": "spark_ingestion_or_staging",
        "error": str(e),
        "traceback": traceback.format_exc()
    }}, default=str))
    print("PREVIEW_ERROR_END")
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
# .py → .ipynb conversion
# ─────────────────────────────────────────────────────────────

def _to_ipynb_json(
    raw_py_source: str,
    workspace_id: str,
    artifact_id: str,
) -> str:
    """Convert Fabric .py notebook source to valid .ipynb JSON.

    The Fabric API 'format: ipynb' endpoint requires a proper Jupyter
    notebook JSON structure — NOT raw Python code.  The existing source
    generators produce the Fabric .py format with '# METADATA' and '# CELL'
    markers.  This function strips those markers, extracts the executable
    code, and wraps it in a minimal .ipynb structure.
    """
    # Extract the code cell content (everything after '# CELL **')
    cell_marker = "# CELL "
    if cell_marker in raw_py_source:
        cell_code = raw_py_source.split(cell_marker, 1)[1]
        # Skip the rest of the marker line (the asterisks + newline)
        newline_pos = cell_code.find("\n")
        cell_code = cell_code[newline_pos + 1:].strip() if newline_pos != -1 else cell_code.strip()
    else:
        # No marker found — use the entire source, stripping comment headers
        lines = raw_py_source.strip().splitlines()
        code_lines = []
        in_metadata = False
        for line in lines:
            if line.startswith("# METADATA") or line.startswith("# META"):
                in_metadata = True
                continue
            if in_metadata and not line.startswith("# META"):
                in_metadata = False
            if not in_metadata and line != "# Fabric notebook source":
                code_lines.append(line)
        cell_code = "\n".join(code_lines).strip()

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {
                "name": "python"
            },
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "python",
                "name": "synapse_pyspark"
            }
        },
        "cells": [
            {
                "cell_type": "code",
                "source": cell_code.splitlines(True),
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            }
        ],
    }
    return json.dumps(notebook)


# ─────────────────────────────────────────────────────────────
# Fabric API helpers
# ─────────────────────────────────────────────────────────────

class FabricSparkPreviewService:
    """
    Universal Spark-based preview engine.
    Ingests source data into a SQL staging table via Fabric Spark notebooks,
    allowing users to run dynamic SQL queries against real source data.
    """

    def __init__(self, access_token: str):
        self.access_token = access_token
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

    # ── Notebook lifecycle (LRO-based, no workspace listing) ──

    @staticmethod
    def _extract_notebook_id(obj: Dict[str, Any]) -> Optional[str]:
        """Safely extract notebook ID from any Fabric API response shape."""
        for key in ("id", "itemId", "notebookId", "artifactId"):
            val = obj.get(key)
            if val and isinstance(val, str):
                return val
        return None

    @staticmethod
    def _extract_operation_id_from_url(location_url: str) -> Optional[str]:
        """Extract operation ID from a Fabric Location header URL.
        Example: https://api.fabric.microsoft.com/v1/operations/{opId}
        """
        if not location_url:
            return None
        parts = location_url.rstrip("/").split("/")
        # The operation ID is the last path segment
        candidate = parts[-1] if parts else None
        if candidate and len(candidate) > 8:
            return candidate
        return None

    async def _poll_operation_status(
        self,
        operation_url: str,
        timeout: int = 600,
        interval: int = 10,
    ) -> Dict[str, Any]:
        """Poll a Fabric long-running operation URL until terminal state.

        Returns the operation status body (NOT the created resource — use
        _get_operation_result for that).
        """
        start = time.time()
        attempt = 0
        while time.time() - start < timeout:
            if attempt > 0:
                wait = min(interval * (1.5 ** min(attempt - 1, 5)), 30)
                await asyncio.sleep(wait)

            resp = await self._request("GET", operation_url)
            if resp.status_code == 202:
                attempt += 1
                continue
            if resp.is_success:
                body = resp.json() if resp.text else {}
                status = str(body.get("status", "")).lower()
                logger.info(
                    "LRO poll attempt=%d status=%s url=%s",
                    attempt + 1, status, operation_url[:120],
                )
                if status in ("succeeded", "completed"):
                    return body
                if status in ("failed", "cancelled", "canceled"):
                    error_msg = body.get("error", {}).get("message", "") if isinstance(body.get("error"), dict) else str(body.get("error", ""))
                    raise HTTPException(
                        status_code=500,
                        detail=f"Fabric operation failed: {status}. {error_msg}",
                    )
                # Still running
            else:
                logger.warning("LRO poll returned HTTP %s", resp.status_code)

            attempt += 1

        raise HTTPException(
            status_code=408,
            detail=f"Fabric long-running operation timed out after {timeout}s.",
        )

    async def _get_operation_result(self, operation_url: str) -> Optional[Dict[str, Any]]:
        """GET /operations/{opId}/result — returns the created/modified resource.

        This is the CRITICAL call that the previous implementation was missing.
        The operation status endpoint only returns {status, createdDateTime, ...}
        but NOT the created resource.  The /result sub-path returns the actual
        notebook object with its ID.
        """
        result_url = operation_url.rstrip("/") + "/result"
        resp = await self._request("GET", result_url)
        if resp.is_success:
            body = resp.json() if resp.text else {}
            logger.info(
                "Operation result retrieved: keys=%s",
                list(body.keys()) if body else "empty",
            )
            return body
        logger.warning(
            "Operation result GET returned HTTP %s for %s",
            resp.status_code, result_url[:120],
        )
        return None

    async def _track_lro(self, response: httpx.Response, timeout: int = 600) -> Dict[str, Any]:
        """Full Fabric LRO lifecycle: poll operation → fetch result.

        1. Extract Location header from 202 response
        2. Poll operation status until Succeeded
        3. GET /operations/{opId}/result to retrieve the created resource
        4. Return the created resource dict (with ID)

        This replaces the old _wait_lro which only did step 1-2 and returned
        the operation status (which never contains a notebook ID).
        """
        location = response.headers.get("Location", "")
        operation_id = response.headers.get("x-ms-operation-id", "")
        retry_after = response.headers.get("Retry-After", "")

        logger.info(
            "Tracking LRO: Location=%s operation_id=%s retry_after=%s",
            location[:120] if location else "none",
            operation_id or "none",
            retry_after or "none",
        )

        if not location:
            # No Location header — try to build operation URL from operation_id
            if operation_id:
                location = f"{FABRIC_API_BASE}/operations/{operation_id}"
                logger.info("Built operation URL from x-ms-operation-id: %s", location)
            else:
                logger.warning("202 response has no Location or operation-id header")
                return {}

        # Step 1-2: Poll until terminal state
        await self._poll_operation_status(location, timeout=timeout)

        # Step 3: Fetch the actual created resource
        result = await self._get_operation_result(location)
        if result:
            return result

        # Fallback: try extracting the operation ID and building alternative result URL
        op_id = self._extract_operation_id_from_url(location) or operation_id
        if op_id and f"/operations/{op_id}" not in location:
            alt_url = f"{FABRIC_API_BASE}/operations/{op_id}"
            result = await self._get_operation_result(alt_url)
            if result:
                return result

        logger.warning("LRO completed but /result returned no data")
        return {}

    async def _get_notebook_by_id(
        self,
        workspace_id: str,
        notebook_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Direct GET by notebook ID — used to verify cached IDs are still valid."""
        resp = await self._request(
            "GET",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}",
        )
        if resp.is_success:
            return resp.json()
        return None

    async def _create_notebook(self, workspace_id: str, source_code: str) -> Dict[str, Any]:
        """Create a notebook via the Fabric API.  Returns a dict with the notebook ID.

        Handles:
        - 201: Synchronous creation, body contains the notebook.
        - 202: Async LRO — polls operation, then GETs /result to get the notebook.
        - 409: Notebook already exists — extracts ID from error body.
        """
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        body = {
            "displayName": PREVIEW_NOTEBOOK_NAME,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {"path": "notebook-content.ipynb", "payload": payload_b64, "payloadType": "InlineBase64"}
                ],
            },
        }

        logger.info("Creating notebook in workspace=%s", workspace_id)
        resp = await self._request(
            "POST",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks",
            json=body,
        )

        # ── 201: Synchronous creation ──
        if resp.status_code == 201:
            result = resp.json()
            nb_id = self._extract_notebook_id(result)
            logger.info("Notebook created synchronously (201): id=%s", nb_id)
            if nb_id:
                _NOTEBOOK_ID_CACHE[workspace_id] = nb_id
            return result

        # ── 202: Async LRO ──
        if resp.status_code == 202:
            logger.info("Notebook creation is async (202), tracking LRO...")
            result = await self._track_lro(resp, timeout=600)
            nb_id = self._extract_notebook_id(result)
            if nb_id:
                logger.info("Notebook created via LRO: id=%s", nb_id)
                _NOTEBOOK_ID_CACHE[workspace_id] = nb_id
                return result

            # _track_lro returned {} — the operation succeeded but /result
            # didn't return the notebook.  Extract operation ID from the
            # Location header and try a workspace items query as last resort.
            logger.warning(
                "LRO succeeded but /result had no notebook ID. "
                "Falling back to workspace items lookup."
            )
            fallback = await self._find_notebook_by_name(workspace_id)
            if fallback:
                return fallback
            raise HTTPException(
                status_code=500,
                detail=(
                    "Notebook LRO completed successfully but the created notebook "
                    "could not be retrieved from /operations/{opId}/result or the "
                    "workspace items list.  Please retry."
                ),
            )

        # ── 409: Already exists ──
        if resp.status_code == 409:
            logger.warning("Notebook creation returned 409 — already exists.")
            # Try to extract the conflicting item's ID from the error body
            try:
                conflict_body = resp.json() if resp.text else {}
                conflict_id = self._extract_notebook_id(conflict_body)
                if conflict_id:
                    logger.info("Extracted notebook ID from 409 body: %s", conflict_id)
                    _NOTEBOOK_ID_CACHE[workspace_id] = conflict_id
                    return {"id": conflict_id, "displayName": PREVIEW_NOTEBOOK_NAME}
            except Exception:
                pass
            # Fall back to workspace lookup
            fallback = await self._find_notebook_by_name(workspace_id)
            if fallback:
                return fallback
            raise HTTPException(
                status_code=500,
                detail="Notebook already exists (409) but could not retrieve its ID.",
            )

        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Notebook creation failed: {resp.text}",
        )

    async def _find_notebook_by_name(self, workspace_id: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        """Last-resort lookup: search workspace items for the preview notebook by name.
        Only used when LRO /result and 409 body both fail to provide an ID.
        Includes retries to handle Fabric API eventual consistency.
        """
        for attempt in range(retries):
            # Try /notebooks endpoint
            resp = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks")
            if resp.is_success:
                for nb in resp.json().get("value") or []:
                    if nb.get("displayName") == PREVIEW_NOTEBOOK_NAME:
                        nb_id = self._extract_notebook_id(nb)
                        if nb_id:
                            _NOTEBOOK_ID_CACHE[workspace_id] = nb_id
                            logger.info("Found notebook by name (fallback): id=%s", nb_id)
                            return nb

            # Try /items endpoint
            resp_items = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Notebook")
            if resp_items.is_success:
                for item in resp_items.json().get("value") or []:
                    if item.get("displayName") == PREVIEW_NOTEBOOK_NAME:
                        nb_id = self._extract_notebook_id(item)
                        if nb_id:
                            _NOTEBOOK_ID_CACHE[workspace_id] = nb_id
                            logger.info("Found notebook by name in /items (fallback): id=%s", nb_id)
                            return item

            if attempt < retries - 1:
                logger.warning(
                    "Notebook %s not found in listing on attempt %d. Waiting for consistency...",
                    PREVIEW_NOTEBOOK_NAME, attempt + 1
                )
                await asyncio.sleep(2 ** attempt)

        return None

    async def _update_notebook(self, workspace_id: str, notebook_id: str, source_code: str) -> None:
        """Update an existing notebook's definition."""
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        body = {
            "definition": {
                "format": "ipynb",
                "parts": [
                    {"path": "notebook-content.ipynb", "payload": payload_b64, "payloadType": "InlineBase64"}
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
            await self._poll_operation_status(
                resp.headers.get("Location", ""),
                timeout=120,
            ) if resp.headers.get("Location") else None
            return
        # 404 means notebook was deleted externally — caller should re-create
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Notebook not found — will re-create.")
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Notebook update failed: {resp.text}",
        )

    async def _delete_notebook(self, workspace_id: str, notebook_id: str) -> None:
        """Delete a notebook from the workspace (best-effort). Wait for LRO if 202."""
        resp = await self._request(
            "DELETE",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}",
        )
        if resp.status_code == 200 or resp.status_code == 404:
            logger.info("Notebook deleted: id=%s (status=%s)", notebook_id, resp.status_code)
            return
        if resp.status_code == 202:
            logger.info("Notebook delete accepted (202), waiting for LRO: id=%s", notebook_id)
            location = resp.headers.get("Location")
            if location:
                await self._poll_operation_status(location, timeout=120)
            return
        logger.warning("Notebook delete failed: id=%s status=%s", notebook_id, resp.status_code)

    async def _ensure_notebook(self, workspace_id: str, source_code: str) -> str:
        """Ensure the preview notebook exists with current source.  Returns notebook_id.

        Strategy (no workspace listing dependency):
        1. Check module-level cache for a known notebook ID.
        2. If cached: try to update definition.
        3. If cache miss: look up by name. If found, try to update.
        4. If update fails (404, stale metadata, etc.): delete old notebook and re-create.
        5. If completely missing: create new notebook via LRO.
        6. Cache the ID for subsequent calls.
        """
        diagnostics: List[Dict[str, str]] = []

        # ── Step 1: Try cached notebook ID ──
        cached_id = _NOTEBOOK_ID_CACHE.get(workspace_id)
        
        # ── Step 2: If not in cache, try to find existing by name ──
        if not cached_id:
            existing = await self._find_notebook_by_name(workspace_id, retries=1)
            if existing:
                cached_id = self._extract_notebook_id(existing)
                if cached_id:
                    logger.info("Found existing notebook after server restart: id=%s", cached_id)
                    _NOTEBOOK_ID_CACHE[workspace_id] = cached_id

        # ── Step 3: Try to update existing notebook ──
        if cached_id:
            try:
                await self._update_notebook(workspace_id, cached_id, source_code)
                logger.info("Existing notebook updated successfully: id=%s", cached_id)
                return cached_id
            except HTTPException as exc:
                logger.warning(
                    "Notebook %s update failed (status=%s), deleting and recreating.",
                    cached_id, exc.status_code,
                )
                _NOTEBOOK_ID_CACHE.pop(workspace_id, None)
                # Delete the stale notebook and wait for completion so a clean create succeeds
                await self._delete_notebook(workspace_id, cached_id)

        # ── Step 4: Create new notebook with correct .ipynb definition ──
        logger.info("Creating fresh notebook for workspace=%s", workspace_id)
        created = await self._create_notebook(workspace_id, source_code)
        notebook_id = self._extract_notebook_id(created)
        if not notebook_id:
            raise HTTPException(
                status_code=500,
                detail=f"Notebook creation succeeded but no ID in response. Keys: {list(created.keys())}",
            )

        _NOTEBOOK_ID_CACHE[workspace_id] = notebook_id
        logger.info("Notebook ensured and cached: id=%s", notebook_id)
        return notebook_id

    # ── Execution ──

    async def _run_notebook(self, workspace_id: str, notebook_id: str) -> str:
        """Trigger notebook execution using the notebook-specific endpoint."""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/instances?jobType=RunNotebook"
        
        # Explicit configuration often required for RunNotebook jobs via API
        body = {
            "executionData": {
                "parameters": {}
            }
        }
        
        resp = await self._request(
            "POST",
            url,
            json=body,
        )
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            # Extract job instance ID from Location header
            parts = location.rstrip("/").split("/")
            return parts[-1] if parts else ""
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Notebook execution trigger failed: {resp.text}",
        )

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
                    failure = resp.json().get("failureReason") or {}
                    
                    # Try to fetch detailed error from job output if available
                    detailed_error = await self._fetch_detailed_notebook_error(workspace_id, notebook_id, job_id)
                    
                    error_msg = detailed_error if detailed_error else f"Notebook execution failed: {status}. {failure}"
                    raise HTTPException(
                        status_code=500,
                        detail=error_msg,
                    )
            sleep_time = backoff_intervals[attempt] if attempt < len(backoff_intervals) else 30
            await asyncio.sleep(sleep_time)
            attempt += 1
        raise HTTPException(status_code=408, detail="Notebook execution timed out.")

    async def _fetch_notebook_output(self, workspace_id: str, notebook_id: str, job_id: str) -> Optional[Dict[str, Any]]:
        """Fetch notebook execution output with retries for eventual consistency."""
        # Try both 'items' and 'notebooks' path segments as Fabric APIs can be inconsistent
        paths = [
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_id}/results",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/instances/{job_id}/results"
        ]
        
        for url in paths:
            attempt = 0
            max_attempts = 3
            while attempt < max_attempts:
                resp = await self._request("GET", url)
                
                if resp.status_code == 200:
                    full_text = self._extract_all_text(resp.json())
                    if "PREVIEW_RESULT_START" in full_text:
                        match = re.search(r"PREVIEW_RESULT_START\s*(.*?)\s*PREVIEW_RESULT_END", full_text, re.DOTALL)
                        if match:
                            try:
                                json_str = match.group(1).strip()
                                if json_str.startswith('"') and json_str.endswith('"'):
                                    json_str = json.loads(json_str)
                                return json.loads(json_str)
                            except:
                                pass
                    
                if resp.status_code == 404:
                    # Resource might not be ready yet
                    attempt += 1
                    if attempt < max_attempts:
                        await asyncio.sleep(2 * attempt)
                        continue
                break # Not a 404 or max attempts reached
                
        logger.warning("Markers not found in job output or API returned 404.")
        return None

    def _extract_all_text(self, obj: Any) -> str:
        """Recursively extract all 'text' fields from a JSON structure (standard Jupyter/Fabric result)."""
        texts = []
        if isinstance(obj, dict):
            # Check for 'text' field (common in stream outputs)
            if "text" in obj:
                val = obj["text"]
                if isinstance(val, list):
                    texts.append("".join(val))
                else:
                    texts.append(str(val))
            # Check for 'data' field (common in execute_result)
            if "data" in obj and isinstance(obj["data"], dict):
                for mime, val in obj["data"].items():
                    if "text/plain" in mime:
                        if isinstance(val, list):
                            texts.append("".join(val))
                        else:
                            texts.append(str(val))
            # Recurse
            for k, v in obj.items():
                if k not in ("text", "data"):
                    texts.append(self._extract_all_text(v))
        elif isinstance(obj, list):
            for item in obj:
                texts.append(self._extract_all_text(item))
        
        return "\n".join(texts)

    async def _fetch_detailed_notebook_error(self, workspace_id: str, notebook_id: str, job_id: str) -> Optional[str]:
        """Fetch notebook execution output to find PREVIEW_ERROR markers with retries."""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_id}/results"
        
        attempt = 0
        while attempt < 3:
            resp = await self._request("GET", url)
            if resp.status_code == 200:
                full_text = self._extract_all_text(resp.json())
                if "PREVIEW_ERROR_START" in full_text:
                    match = re.search(r"PREVIEW_ERROR_START\s*(.*?)\s*PREVIEW_ERROR_END", full_text, re.DOTALL)
                    if match:
                        try:
                            json_str = match.group(1).strip()
                            # Handle potential double-encoding or extra quotes from Fabric logs
                            if json_str.startswith('"') and json_str.endswith('"'):
                                try:
                                    json_str = json.loads(json_str)
                                except: pass
                            
                            error_data = json.loads(json_str)
                            return (
                                f"Notebook Error in stage '{error_data.get('stage', 'unknown')}': "
                                f"{error_data.get('error')}\n\n"
                                f"Traceback:\n{error_data.get('traceback')}"
                            )
                        except Exception as e:
                            logger.warning("Failed to parse PREVIEW_ERROR JSON: %s", str(e))
                            return f"Notebook failed with raw error: {json_str[:500]}..."
            
            if resp.status_code == 404:
                attempt += 1
                await asyncio.sleep(2 * attempt)
                continue
            break
            
        return None

    # ── SQL Analytics Endpoint ──

    async def _get_sql_endpoint(self, workspace_id: str, lakehouse_id: str) -> str:
        """Discover the SQL analytics endpoint connection string for the Lakehouse."""
        resp = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
        if not resp.is_success:
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to get Lakehouse info: {resp.text}")
        props = resp.json().get("properties", {})
        sql_props = props.get("sqlEndpointProperties", {})
        conn_string = sql_props.get("connectionString", "")
        if not conn_string:
            raise HTTPException(status_code=500, detail="Lakehouse SQL analytics endpoint not available. It may still be provisioning.")
        return conn_string

    def _validate_sql(self, sql_query: str) -> None:
        """Restrict queries to read-only SELECT commands for security."""
        blocked_keywords = [
            "DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT", "EXEC", 
            "CREATE", "GRANT", "REVOKE", "MERGE"
        ]
        query_upper = sql_query.upper()
        
        # Check for blocked keywords
        for kw in blocked_keywords:
            if re.search(rf"\b{kw}\b", query_upper):
                raise HTTPException(status_code=403, detail=f"Destructive SQL command '{kw}' is blocked.")
        
        # Must contain SELECT
        if not re.search(r"\bSELECT\b", query_upper):
            raise HTTPException(status_code=403, detail="Only SELECT queries are allowed.")

    async def execute_sql_query(
        self, 
        workspace_id: str, 
        lakehouse_id: str, 
        sql_query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute a SQL query against the Neon Postgres staging database."""
        self._validate_sql(sql_query)
        
        try:
            # Wrap synchronous psycopg2 in a thread
            def _query():
                conn = psycopg2.connect(NEON_DB_URL)
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                cursor.execute(sql_query)
                
                # Fetch results
                rows = cursor.fetchall()
                # Extract column names from description
                columns = [desc[0] for desc in cursor.description]
                
                # Convert RealDictCursor results to standard dicts
                standard_rows = [dict(r) for r in rows]
                
                conn.close()
                return {"columns": columns, "rows": standard_rows}

            return await asyncio.to_thread(_query)
        except Exception as exc:
            logger.error("Postgres execution failed: %s", str(exc))
            raise HTTPException(status_code=500, detail=f"Postgres execution failed: {str(exc)}")
        
    # ── Main Orchestration ──

    async def execute_preview(self, source_connection: Dict[str, Any]) -> Dict[str, Any]:
        """
        Universal SQL-Staging Flow:
        1. Build notebook that ingests source and saves to Delta table.
        2. Execute notebook.
        3. Capture staging table name and initial schema from stdout.
        4. Return session info for interactive SQL.
        """
        workspace_id = source_connection.get("workspace_id")
        artifact_id = source_connection.get("artifact_id")
        session_id = str(uuid.uuid4())
        diagnostics: List[Dict[str, Any]] = []

        if not workspace_id or not artifact_id:
            raise HTTPException(status_code=400, detail="Missing workspace_id or artifact_id")

        # 1. Provision Notebook
        raw_source = _build_preview_notebook_source(source_connection, session_id)
        ipynb_json = _to_ipynb_json(raw_source, workspace_id, artifact_id)
        
        try:
            # 1. Clear cache to ensure we're using a fresh ID if the previous one failed
            if workspace_id in _NOTEBOOK_ID_CACHE:
                del _NOTEBOOK_ID_CACHE[workspace_id]
            
            notebook_id = await self._ensure_notebook(workspace_id, ipynb_json)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Notebook provisioning failed: {str(exc)}")

        # 2. Run Job
        try:
            job_id = await self._run_notebook(workspace_id, notebook_id)
            await self._poll_job(workspace_id, notebook_id, job_id)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Execution trigger failed: {str(exc)}")

        # 3. Capture Metadata (Optional if staging table name is deterministic)
        # We try to get metadata from stdout, but if empty, we fall back to the deterministic table name
        result_data = await self._fetch_notebook_output(workspace_id, notebook_id, job_id)
        staging_table = f"{PREVIEW_TABLE_PREFIX}{session_id.replace('-', '_')}"
        
        # Store session mapping
        _PREVIEW_SESSION_STORE[session_id] = {
            "workspace_id": workspace_id,
            "artifact_id": artifact_id,
            "staging_table": staging_table
        }

        # 4. Fetch initial sample rows via SQL Analytics Endpoint
        columns = []
        sample_rows = []
        sql_success = False
        
        try:
            logger.info("Fetching initial preview rows from SQL endpoint for table: %s", staging_table)
            sql_data = await self.execute_sql_query(
                workspace_id, artifact_id, f"SELECT * FROM {staging_table} LIMIT 20"
            )
            sample_rows = sql_data.get("rows", [])
            # Map SQL column names to our expected format
            columns = [{"column_name": c, "data_type": "unknown"} for c in sql_data.get("columns", [])]
            sql_success = True
        except Exception as exc:
            logger.warning("Failed to fetch initial rows via SQL: %s", str(exc))
            # If SQL fails AND result_data is missing, we must show the notebook error
            if not result_data:
                detailed_error = await self._fetch_detailed_notebook_error(workspace_id, notebook_id, job_id)
                if detailed_error:
                    raise HTTPException(status_code=500, detail=detailed_error)
                raise HTTPException(status_code=500, detail=f"Notebook succeeded but staging table could not be queried: {str(exc)}")

        # Merge metadata from notebook stdout if available (it has data types), otherwise use SQL columns
        final_columns = result_data.get("columns") if (result_data and result_data.get("columns")) else columns

        # 5. Reshape for Frontend Expectations
        # The UI expects top-level 'rows' and 'columns' for the preview table,
        # and a detailed 'schema_discovery' object for the metadata table.
        
        display_columns = [c["column_name"] for c in final_columns]
        
        # Build enriched schema columns for the Discovery table
        enriched_columns = []
        for i, col in enumerate(final_columns):
            col_name = col["column_name"]
            # Extract a sample value from the first row if available
            sample_val = sample_rows[0].get(col_name) if sample_rows else None
            
            enriched_columns.append({
                "column_name": col_name,
                "data_type": col.get("data_type", "unknown"),
                "nullable": True, # Assume nullable for dynamic preview
                "ordinal_position": i + 1,
                "sample_value": sample_val
            })

        return {
            "success": True,
            "session_id": session_id,
            "staging_table": staging_table,
            "preview_mode": "fabric_spark",
            "columns": display_columns,
            "rows": sample_rows,
            "schema_discovery": {
                "columns": enriched_columns,
                "sample_rows": sample_rows,
                "nullable_columns": [],
                "primary_key_candidates": [],
                "timestamp_columns": []
            },
            "diagnostics": diagnostics
        }

    async def cleanup_session(self, workspace_id: str, artifact_id: str, session_id: str):
        """Drop the Neon Postgres staging table."""
        staging_table = f"{PREVIEW_TABLE_PREFIX}{session_id.replace('-', '_')}"
        try:
            def _drop():
                conn = psycopg2.connect(NEON_DB_URL)
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                conn.commit()
                conn.close()
            
            await asyncio.to_thread(_drop)
            logger.info("Cleaned up Neon preview session: %s", session_id)
        except Exception as exc:
            logger.warning("Failed to cleanup Neon session %s: %s", session_id, str(exc))
