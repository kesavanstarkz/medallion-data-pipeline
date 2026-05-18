from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import logging
import json
import urllib.request
import urllib.error
import base64
from io import BytesIO
from datetime import datetime
from sqlalchemy.orm import Session
import pandas as pd
import httpx
from services.pipeline_intelligence_service import analyze_pipeline_live
from services.fabric_bundle_analysis_service import analyze_fabric_bundle
from services.fabric_runtime_intelligence_service import execute_and_capture_runtime_intelligence
from services.fabric.auth_service import FabricAuthService, ONELAKE_STORAGE_SCOPE
from api.storage import preview_file
from core.database import SessionLocal
from core.database import get_db
from core.utils import generate_dataset_id, DirectSparkPreviewService
from services.fabric.universal_preview_service import FabricSparkPreviewService
from models.api_source_config import APISourceConfig
from models.master_config_authoritative import MasterConfigAuthoritative
from models.master_config import MasterConfig
from core.settings import settings
import psycopg2
from core.mcp_connector import get_mcp_connector
from core.azure_storage import get_storage_client
import io

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/discovery", tags=["Pipeline Intelligence"])

class AnalyzeRequest(BaseModel):
    client_name: str
    # platform identifies the execution layer (FABRIC, DATABRICKS, AWS, AZURE)
    # it is NOT a source connector and must never be validated against registered sources
    platform: Optional[str] = None
    target: Optional[str] = None
    auth_mode: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    use_cloud_llm: bool = True
    llm_provider: str = "gpt"
    use_local_llm: bool = False
    scan_mode: str = "live"
    providers: Optional[str] = None
    source_type: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class ApiScanRequest(BaseModel):
    client_name: str
    source_name: Optional[str] = None


class RuntimeIntelligenceRequest(BaseModel):
    client_name: str
    workspace_id: str
    pipeline_id: str
    existing_analysis: Optional[Dict[str, Any]] = None


class RuntimeSourcePreviewRequest(BaseModel):
    source_connection: Optional[Dict[str, Any]] = None
    schema_discovery: Optional[Dict[str, Any]] = None
    workspaceId: Optional[str] = None
    artifactId: Optional[str] = None
    rootFolder: Optional[str] = None
    folderPath: Optional[str] = None
    fileName: Optional[str] = None
    format: Optional[str] = None
    header: Optional[bool] = None
    delimiter: Optional[str] = None


class RuntimeSourceSaveRequest(BaseModel):
    client_name: str
    pipeline_id: Optional[str] = None
    runtime_source_discovery: Dict[str, Any]


class PreviewQueryRequest(BaseModel):
    session_id: str
    query: Optional[str] = None
    sql_query: Optional[str] = None


def _runtime_source_path(source_connection: Dict[str, Any]) -> str:
    return (
        source_connection.get("preview_path")
        or source_connection.get("resolved_path")
        or source_connection.get("full_path")
        or "/".join(
            part.strip("/")
            for part in [str(source_connection.get("folder_path") or "").strip("/"), str(source_connection.get("file_name") or "").strip("/")]
            if part
        )
    )


def _runtime_dataset_name(source_connection: Dict[str, Any]) -> str:
    # Prioritize actual file names over artifact IDs (which are often pipeline names)
    return (
        source_connection.get("file_name")
        or source_connection.get("source_object")
        or source_connection.get("artifact_id")
        or "source_export.csv"
    )


def _decode_token_audience(token: Optional[str]) -> Optional[str]:
    if not token or token.count(".") < 2:
        return None
    try:
        payload = token.split(".")[1]
        padding = "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload + padding).decode("utf-8"))
        return claims.get("aud")
    except Exception:
        return None


def _normalize_preview_strategy(source_connection: Dict[str, Any]) -> str:
    source_type = str(source_connection.get("source_type") or source_connection.get("storage_type") or "").lower()
    connector_type = str(source_connection.get("connector_type") or "").lower()
    format_type = str(source_connection.get("format") or "").lower()
    resolved_path = str(source_connection.get("resolved_path") or "").lower()
    metadata = source_connection.get("connection_metadata") or {}

    if any(token in connector_type for token in ("rest", "http", "graphql", "soap")) or any(key in metadata for key in ("endpoint", "url")):
        return "api_preview"
    if any(token in connector_type for token in ("sql", "jdbc", "oracle", "mysql", "postgres", "snowflake", "db2")) or any(key in metadata for key in ("jdbc_url", "connectionString", "table", "query")):
        return "sql_preview"
    if format_type in {"csv", "txt", "tsv"} or "delimited" in connector_type:
        return "direct_csv"
    if format_type == "json":
        return "direct_json"
    if format_type == "parquet":
        return "parquet_preview"
    if format_type == "delta":
        return "delta_preview"
    if resolved_path.startswith(("s3://", "az://", "https://", "abfss://", "abfs://", "wasbs://")):
        return "direct_file"
    if "lakehouse" in source_type or "onelake" in source_type:
        return "onelake_direct"
    return "spark_fallback"


def _is_lightweight_preview_strategy(strategy: str, source_connection: Dict[str, Any]) -> bool:
    format_type = str(source_connection.get("format") or "").lower()
    return strategy in {"direct_csv", "direct_json", "onelake_direct"} or (
        strategy == "direct_file" and format_type in {"csv", "txt", "tsv", "json"}
    )


def _schema_from_dataframe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    schema: List[Dict[str, Any]] = []
    for index, column in enumerate(df.columns):
        dtype = str(df[column].dtype)
        schema.append({
            "column_name": str(column),
            "data_type": dtype,
            "nullable": bool(df[column].isnull().any()),
            "ordinal_position": index + 1,
        })
    return schema


def _preview_payload_from_dataframe(df: pd.DataFrame, resolved_path: str, preview_mode: str, source_name: str) -> Dict[str, Any]:
    normalized = df.head(25).copy()
    normalized = normalized.where(pd.notnull(normalized), None)
    preview_rows = normalized.to_dict(orient="records")
    schema = _schema_from_dataframe(normalized)
    return {
        "type": "csv",
        "preview_rows": preview_rows,
        "rows": [[row.get(column["column_name"]) for column in schema] for row in preview_rows],
        "schema": schema,
        "row_count_estimate": len(preview_rows),
        "total_rows_approx": f"First {len(preview_rows)} rows shown",
        "columns": [column["column_name"] for column in schema],
        "datatypes": [column["data_type"] for column in schema],
        "nullable_columns": [column["column_name"] for column in schema if column["nullable"]],
        "resolved_path": resolved_path,
        "preview_mode": preview_mode,
        "source_name": source_name,
    }


def _direct_preview_from_bytes(content: bytes, source_connection: Dict[str, Any], resolved_path: str, preview_mode: str) -> Dict[str, Any]:
    format_type = str(source_connection.get("format") or "").lower()
    delimiter = source_connection.get("delimiter") or ","
    header = source_connection.get("header_enabled")

    if preview_mode in {"direct_csv", "direct_file"} or format_type in {"csv", "txt", "tsv"}:
        read_kwargs: Dict[str, Any] = {"nrows": 25, "sep": delimiter}
        if header is False:
            read_kwargs["header"] = None
        df = pd.read_csv(BytesIO(content), **read_kwargs)
        return _preview_payload_from_dataframe(df, resolved_path, preview_mode, _runtime_dataset_name(source_connection))
    if preview_mode == "direct_json" or format_type == "json":
        payload = json.loads(content.decode("utf-8"))
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            df = pd.DataFrame(payload[:25])
            return _preview_payload_from_dataframe(df, resolved_path, preview_mode, _runtime_dataset_name(source_connection))
        if isinstance(payload, dict):
            df = pd.DataFrame([payload])
            return _preview_payload_from_dataframe(df, resolved_path, preview_mode, _runtime_dataset_name(source_connection))
        return {
            "type": "json",
            "preview_rows": [{"value": payload}],
            "rows": [[payload]],
            "schema": [{"column_name": "value", "data_type": type(payload).__name__, "nullable": payload is None, "ordinal_position": 1}],
            "row_count_estimate": 1,
            "columns": ["value"],
            "datatypes": [type(payload).__name__],
            "nullable_columns": ["value"] if payload is None else [],
            "resolved_path": resolved_path,
            "preview_mode": preview_mode,
            "source_name": _runtime_dataset_name(source_connection),
        }
    if preview_mode == "parquet_preview" or format_type == "parquet":
        df = pd.read_parquet(BytesIO(content)).head(25)
        return _preview_payload_from_dataframe(df, resolved_path, preview_mode, _runtime_dataset_name(source_connection))
    return {
        "type": "text",
        "preview_rows": [{"value": content.decode("utf-8", errors="ignore")[:5000]}],
        "rows": [[content.decode("utf-8", errors="ignore")[:5000]]],
        "schema": [{"column_name": "value", "data_type": "text", "nullable": False, "ordinal_position": 1}],
        "row_count_estimate": 1,
        "columns": ["value"],
        "datatypes": ["text"],
        "nullable_columns": [],
        "resolved_path": resolved_path,
        "preview_mode": preview_mode,
        "source_name": _runtime_dataset_name(source_connection),
    }


import re
import urllib.parse

def _onelake_candidate_urls(source_connection: Dict[str, Any]) -> List[str]:
    workspace_id = source_connection.get("workspace_id")
    artifact_id = source_connection.get("artifact_id")
    resolved_path = str(source_connection.get("resolved_path") or "").lstrip("/")
    if not workspace_id or not artifact_id or not resolved_path:
        return []
    
    encoded_path = urllib.parse.quote(resolved_path, safe="/")
    
    if re.match(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", artifact_id):
        return [f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{artifact_id}/{encoded_path}"]
        
    item_type = str((source_connection.get("connection_metadata") or {}).get("itemType") or "Lakehouse")
    return [
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{artifact_id}/{encoded_path}",
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{artifact_id}.{item_type}/{encoded_path}",
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{artifact_id}.Lakehouse/{encoded_path}",
    ]


async def _execute_direct_preview_strategy(
    strategy: str,
    source_connection: Dict[str, Any],
    storage_token: Optional[str],
    db: Session,
    diagnostics: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    resolved_path = str(source_connection.get("resolved_path") or _runtime_source_path(source_connection) or "")
    source_name = _runtime_dataset_name(source_connection)

    if strategy in {"direct_csv", "direct_json", "parquet_preview", "direct_file"}:
        path = _runtime_source_path(source_connection)
        if path.startswith(("az://", "s3://", "https://")):
            payload = preview_file(path=path, db=db)
            payload["preview_mode"] = strategy
            payload["resolved_path"] = resolved_path or path
            payload["source_name"] = source_name
            payload["preview_rows"] = [
                {column: row[index] if isinstance(row, list) and index < len(row) else None for index, column in enumerate(payload.get("columns") or [])}
                for row in (payload.get("rows") or [])[:25]
            ]
            payload["schema"] = payload.get("schema") or [
                {"column_name": column, "data_type": "string", "nullable": True, "ordinal_position": index + 1}
                for index, column in enumerate(payload.get("columns") or [])
            ]
            payload["row_count_estimate"] = len(payload.get("preview_rows") or [])
            return payload
        if storage_token:
            for candidate in _onelake_candidate_urls(source_connection):
                logger.info(
                    "Runtime preview direct OneLake read | strategy=%s url=%s token_scope=%s token_audience=%s",
                    strategy,
                    candidate,
                    ONELAKE_STORAGE_SCOPE,
                    _decode_token_audience(storage_token),
                )
                diagnostics.append({
                    "mode": "onelake_direct",
                    "status": "attempted",
                    "url": candidate,
                    "strategy": strategy,
                    "token_scope": ONELAKE_STORAGE_SCOPE,
                    "token_audience": _decode_token_audience(storage_token),
                    "token_source": "backend_client_credentials",
                })
                try:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        response = await client.get(candidate, headers={"Authorization": f"Bearer {storage_token}"})
                    if response.is_success:
                        payload = _direct_preview_from_bytes(
                            response.content,
                            source_connection,
                            source_connection.get("resolved_path") or candidate,
                            strategy,
                        )
                        payload["source_name"] = source_name
                        return payload
                    diagnostics.append({
                        "mode": "onelake_direct",
                        "status": "failed",
                        "url": candidate,
                        "strategy": strategy,
                        "http_status": response.status_code,
                        "response_excerpt": response.text[:200] if response.text else "",
                        "token_scope": ONELAKE_STORAGE_SCOPE,
                        "token_audience": _decode_token_audience(storage_token),
                    })
                except Exception as exc:
                    diagnostics.append({
                        "mode": "onelake_direct",
                        "status": "failed",
                        "url": candidate,
                        "strategy": strategy,
                        "error": str(exc),
                        "token_scope": ONELAKE_STORAGE_SCOPE,
                        "token_audience": _decode_token_audience(storage_token),
                    })
        return None

    if strategy == "onelake_direct" and storage_token:
        for candidate in _onelake_candidate_urls(source_connection):
            logger.info(
                "Runtime preview direct OneLake read | strategy=%s url=%s token_scope=%s token_audience=%s",
                strategy,
                candidate,
                ONELAKE_STORAGE_SCOPE,
                _decode_token_audience(storage_token),
            )
            diagnostics.append({
                "mode": "onelake_direct",
                "status": "attempted",
                "url": candidate,
                "token_scope": ONELAKE_STORAGE_SCOPE,
                "token_audience": _decode_token_audience(storage_token),
                "token_source": "backend_client_credentials",
            })
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.get(candidate, headers={"Authorization": f"Bearer {storage_token}"})
                if response.is_success:
                    return _direct_preview_from_bytes(response.content, source_connection, resolved_path or candidate, strategy)
                diagnostics.append({
                    "mode": "onelake_direct",
                    "status": "failed",
                    "url": candidate,
                    "http_status": response.status_code,
                    "response_excerpt": response.text[:200] if response.text else "",
                    "token_scope": ONELAKE_STORAGE_SCOPE,
                    "token_audience": _decode_token_audience(storage_token),
                })
            except Exception as exc:
                diagnostics.append({
                    "mode": "onelake_direct",
                    "status": "failed",
                    "url": candidate,
                    "error": str(exc),
                    "token_scope": ONELAKE_STORAGE_SCOPE,
                    "token_audience": _decode_token_audience(storage_token),
                })
        return None

    if strategy == "api_preview":
        metadata = source_connection.get("connection_metadata") or {}
        endpoint = metadata.get("endpoint") or metadata.get("url")
        headers = metadata.get("headers") or {}
        if not endpoint:
            return None
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(endpoint, headers=headers)
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"API preview failed: {response.text}")
        return _direct_preview_from_bytes(response.content, source_connection, endpoint, strategy)

    if strategy == "sql_preview":
        return None

    if strategy == "delta_preview":
        return None

    return None


def _normalize_preview_source(request: RuntimeSourcePreviewRequest) -> Dict[str, Any]:
    source_connection = dict(request.source_connection or {})
    if request.workspaceId:
        source_connection.setdefault("workspace_id", request.workspaceId)
    if request.artifactId:
        source_connection.setdefault("artifact_id", request.artifactId)
    if request.rootFolder:
        source_connection.setdefault("root_folder", request.rootFolder)
    if request.folderPath:
        source_connection.setdefault("folder_path", request.folderPath)
    if request.fileName:
        source_connection.setdefault("file_name", request.fileName)
    if request.format:
        source_connection.setdefault("format", str(request.format).lower())
    if request.header is not None:
        source_connection.setdefault("header_enabled", request.header)
    if request.delimiter:
        source_connection.setdefault("delimiter", request.delimiter)
    if not source_connection.get("resolved_path"):
        root = str(source_connection.get("root_folder") or "Files").strip("/\\") or "Files"
        folder = str(source_connection.get("folder_path") or "").strip("/\\")
        file_name = str(source_connection.get("file_name") or "").strip("/\\")
        parts = [root]
        if folder:
            parts.append(folder)
        if file_name:
            parts.append(file_name)
        source_connection["resolved_path"] = "/".join(part for part in parts if part)
    return source_connection


def _structured_runtime_sample_preview(source_connection: Dict[str, Any], schema_discovery: Dict[str, Any], diagnostics: List[Dict[str, Any]]) -> Dict[str, Any]:
    sample_rows = schema_discovery.get("sample_rows") or []
    columns = schema_discovery.get("columns") or []
    column_names = [column.get("column_name") for column in columns if column.get("column_name")]
    schema = [
        {
            "column_name": column.get("column_name"),
            "data_type": column.get("data_type"),
            "nullable": column.get("nullable"),
            "ordinal_position": column.get("ordinal_position"),
        }
        for column in columns
    ]
    preview_rows = []
    for row in sample_rows[:25]:
        if isinstance(row, dict):
            preview_rows.append({column: row.get(column) for column in column_names})
    return {
        "type": "csv",
        "preview_rows": preview_rows,
        "rows": [[row.get(column) for column in column_names] for row in preview_rows],
        "schema": schema,
        "row_count_estimate": len(sample_rows),
        "total_rows_approx": "Runtime metadata sample",
        "columns": column_names,
        "datatypes": [column.get("data_type") for column in columns],
        "resolved_path": source_connection.get("resolved_path"),
        "preview_mode": "runtime_sample",
        "source_name": _runtime_dataset_name(source_connection),
        "diagnostics": diagnostics,
    }


@router.post("/fabric-bundle-analysis")
async def run_fabric_bundle_analysis(
    http_request: Request,
    client_name: str = Form(...),
    workspace_id: Optional[str] = Form(None),
    pipeline_id: Optional[str] = Form(None),
    use_cloud_llm: bool = Form(True),
    existing_analysis_json: Optional[str] = Form(None),
    file: UploadFile = File(...),
):
    if not (file.filename or "").lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only Microsoft Fabric exported ZIP bundles are supported.")

    try:
        existing_analysis = json.loads(existing_analysis_json) if existing_analysis_json else None
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"existing_analysis_json is not valid JSON: {exc}")

    authorization = http_request.headers.get("authorization")
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()

    try:
        payload = await analyze_fabric_bundle(
            client_name=client_name,
            file_bytes=await file.read(),
            filename=file.filename or "fabric-export.zip",
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            use_cloud_llm=use_cloud_llm,
            authorization_token=bearer_token,
            existing_analysis=existing_analysis,
        )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fabric bundle analysis failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/fabric-runtime-intelligence")
async def run_fabric_runtime_intelligence(request: RuntimeIntelligenceRequest, http_request: Request):
    authorization = http_request.headers.get("authorization")
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()
    if not bearer_token:
        raise HTTPException(status_code=401, detail="Fabric runtime capture requires a bearer token.")

    try:
        return await execute_and_capture_runtime_intelligence(
            client_name=request.client_name,
            workspace_id=request.workspace_id,
            pipeline_id=request.pipeline_id,
            access_token=bearer_token,
            existing_analysis=request.existing_analysis,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fabric runtime intelligence failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/fabric-runtime-source-preview")
async def preview_runtime_source(request: RuntimeSourcePreviewRequest, http_request: Request, db: Session = Depends(get_db)):
    source_connection = _normalize_preview_source(request)
    source_path = _runtime_source_path(source_connection)
    diagnostics: List[Dict[str, Any]] = []

    diagnostics.append({
        "mode": "metadata_resolution",
        "status": "success" if source_connection.get("workspace_id") or source_connection.get("artifact_id") else "partial",
        "workspace_id": source_connection.get("workspace_id"),
        "artifact_id": source_connection.get("artifact_id"),
        "root_folder": source_connection.get("root_folder"),
        "folder_path": source_connection.get("folder_path"),
        "file_name": source_connection.get("file_name"),
        "resolved_path": source_connection.get("resolved_path"),
    })

    if not source_connection.get("workspace_id") or not source_connection.get("artifact_id") or not source_connection.get("resolved_path"):
        raise HTTPException(status_code=400, detail={
            "error": "Incomplete source metadata for preview",
            "details": "workspace_id, artifact_id, and resolved_path are required.",
            "diagnostics": diagnostics,
        })

    # Extract bearer token for notebook-based fallback
    authorization = http_request.headers.get("authorization")
    bearer_token: Optional[str] = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()

    try:
        # ── Attempt 1: DirectSparkPreviewService (in-process Spark, only available
        #               inside Fabric / Databricks environments)
        direct_service = DirectSparkPreviewService()
        result = await direct_service.execute_preview(source_connection)

        # Check whether the service returned a Spark-unavailable error rather than
        # raising, and fall back to the notebook-based approach in that case.
        spark_unavailable = (
            result.get("error")
            and "spark not available" in str(result.get("error", "")).lower()
        )

        if spark_unavailable:
            logger.info(
                "DirectSparkPreviewService: Spark not available locally — "
                "falling back to FabricSparkPreviewService (notebook-based). "
                "workspace_id=%s artifact_id=%s",
                source_connection.get("workspace_id"),
                source_connection.get("artifact_id"),
            )

            if not bearer_token:
                raise HTTPException(
                    status_code=401,
                    detail={
                        "error": "Preview execution failed: Spark not available in this environment and no bearer token was provided for notebook-based fallback.",
                        "diagnostics": diagnostics,
                        "schema_discovery": {
                            "columns": [],
                            "sample_rows": [],
                            "nullable_columns": [],
                            "primary_key_candidates": [],
                            "timestamp_columns": [],
                        },
                        "source_name": _runtime_dataset_name(source_connection),
                    },
                )

            # ── Attempt 2: FabricSparkPreviewService (notebook-based, remote Spark)
            notebook_service = FabricSparkPreviewService(bearer_token)
            result = await notebook_service.execute_preview(source_connection)

        result["source_name"] = _runtime_dataset_name(source_connection)
        result.setdefault("diagnostics", []).extend(diagnostics)

        schema_discovery = result.get("schema_discovery", {})
        logger.info(
            "Preview Success | mode=%s workspace_id=%s artifact_id=%s resolved_path=%s columns=%s sample_rows=%s",
            result.get("preview_mode", "unknown"),
            source_connection.get("workspace_id"),
            source_connection.get("artifact_id"),
            source_connection.get("resolved_path"),
            len(schema_discovery.get("columns") or []),
            len(schema_discovery.get("sample_rows") or []),
        )

        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Runtime source preview failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={
            "error": "Preview execution failed",
            "details": str(exc),
            "diagnostics": diagnostics,
        })


@router.post("/fabric-runtime-source-query")
async def execute_preview_query(request: PreviewQueryRequest, http_request: Request):
    """Execute a dynamic SQL query against a preview session staging table."""
    # Extract bearer token
    authorization = http_request.headers.get("authorization")
    bearer_token: Optional[str] = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()
    
    if not bearer_token:
        raise HTTPException(status_code=401, detail="Authentication required for preview query.")

    from services.fabric.universal_preview_service import _PREVIEW_SESSION_STORE
    
    session = _PREVIEW_SESSION_STORE.get(request.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Preview session {request.session_id} not found or expired.")
    
    workspace_id = session["workspace_id"]
    artifact_id = session["artifact_id"]
    
    sql_to_run = request.query or request.sql_query
    if not sql_to_run:
        raise HTTPException(status_code=400, detail="SQL query is required.")

    notebook_service = FabricSparkPreviewService(bearer_token)
    try:
        result = await notebook_service.execute_sql_query(
            workspace_id=workspace_id,
            lakehouse_id=artifact_id,
            sql_query=sql_to_run
        )
        # Normalize result for UI
        if "rows" in result and isinstance(result["rows"], list):
            result["row_count"] = len(result["rows"])
        if "columns" in result and isinstance(result["columns"], list):
            result["column_count"] = len(result["columns"])
            
        return result
    except Exception as exc:
        logger.error("Preview query failed: %s", str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/fabric-runtime-metadata-discovery")
async def discover_runtime_metadata(session_id: str, http_request: Request):
    """Fetch schema, table, and column metadata for the staging database."""
    authorization = http_request.headers.get("authorization")
    bearer_token: Optional[str] = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()
    
    if not bearer_token:
        raise HTTPException(status_code=401, detail="Authentication required for metadata discovery.")

    from services.fabric.universal_preview_service import _PREVIEW_SESSION_STORE
    
    session = _PREVIEW_SESSION_STORE.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Preview session {session_id} not found or expired.")
    
    notebook_service = FabricSparkPreviewService(bearer_token)
    try:
        # Discover all tables in the staging DB (Neon Postgres)
        # We look for tables starting with our prefix or the current staging table
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        tables_res = await notebook_service.execute_sql_query(
            workspace_id=session["workspace_id"],
            lakehouse_id=session["artifact_id"],
            sql_query=tables_query
        )
        tables = [row["table_name"] for row in tables_res.get("rows", [])]
        
        # Discover columns for the primary staging table
        staging_table = session["staging_table"]
        columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{staging_table}'"
        columns_res = await notebook_service.execute_sql_query(
            workspace_id=session["workspace_id"],
            lakehouse_id=session["artifact_id"],
            sql_query=columns_query
        )
        columns = columns_res.get("rows", [])
        
        return {
            "tables": tables,
            "columns": columns,
            "active_table": staging_table
        }
    except Exception as exc:
        logger.error("Metadata discovery failed: %s", str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/fabric-runtime-source-save")
def save_runtime_source(request: RuntimeSourceSaveRequest, db: Session = Depends(get_db)):
    runtime_source_discovery = request.runtime_source_discovery or {}
    source_connection = runtime_source_discovery.get("source_connection") or {}
    target_connection = runtime_source_discovery.get("target_connection") or {}
    schema_discovery = runtime_source_discovery.get("schema_discovery") or {}
    dq_recommendations = runtime_source_discovery.get("dq_recommendations") or []
    runtime_statistics = runtime_source_discovery.get("runtime_statistics") or {}

    source_path = _runtime_source_path(source_connection)
    if not source_path:
        source_path = f"fabric://{source_connection.get('workspace_id') or 'workspace'}/{source_connection.get('artifact_id') or _runtime_dataset_name(source_connection)}"

    source_type = str(source_connection.get("storage_type") or source_connection.get("source_type") or "FABRIC").upper()
    dataset_id = generate_dataset_id(request.client_name, source_type, source_path)
    source_object = _runtime_dataset_name(source_connection)
    file_format = source_connection.get("format") or target_connection.get("format") or "UNKNOWN"

    authoritative = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == dataset_id).first()
    if not authoritative:
        authoritative = MasterConfigAuthoritative(dataset_id=dataset_id)
        db.add(authoritative)
    authoritative.pipeline_id = request.pipeline_id
    authoritative.client_name = request.client_name
    authoritative.source_type = source_type
    authoritative.source_folder = source_connection.get("folder_path") or source_path
    authoritative.source_object = source_object
    authoritative.file_format = file_format
    authoritative.raw_layer_path = source_path
    authoritative.target_layer_bronze = target_connection.get("folder_path") or target_connection.get("full_path")
    authoritative.is_active = True
    authoritative.updated_at = datetime.utcnow()

    legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == dataset_id).first()
    if not legacy:
        legacy = MasterConfig(dataset_id=dataset_id)
        db.add(legacy)
    legacy.client_name = request.client_name
    legacy.source_system = source_type
    legacy.source_object = source_object
    legacy.source_schema = source_connection.get("folder_path")
    legacy.file_format = file_format
    legacy.target_schema = target_connection.get("folder_path")
    legacy.target_table = target_connection.get("file_name")
    legacy.is_active = True
    legacy.validation_rules = {
        "schema": schema_discovery,
        "dq_rules": dq_recommendations,
        "runtime_metrics": runtime_statistics,
        "source_connection": source_connection,
        "target_connection": target_connection,
    }
    legacy.rows_read = runtime_statistics.get("rows_read") or 0
    legacy.rows_written = runtime_statistics.get("rows_written") or 0
    legacy.updated_at = datetime.utcnow()

    db.commit()
    return {
        "status": "SUCCESS",
        "dataset_id": dataset_id,
        "source_path": source_path,
        "source_object": source_object,
        "file_format": file_format,
    }


def _api_headers(config: APISourceConfig) -> Dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "DEA-Agent/1.0"}
    auth_type = (config.auth_type or "none").lower()
    token = config.auth_token or ""
    if auth_type == "bearer" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type in {"api_key", "apikey"} and token:
        headers[config.api_key_header or "X-Api-Key"] = token
    elif auth_type == "basic" and token:
        headers["Authorization"] = f"Basic {base64.b64encode(token.encode()).decode()}"
    return headers


def _extract_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        if len(payload) == 2 and isinstance(payload[1], list):
            payload = payload[1]
        records = payload
    elif isinstance(payload, dict):
        records = None
        for key in ("data", "results", "items", "records", "value", "articles", "sources", "hits", "entries", "content", "list"):
            if isinstance(payload.get(key), list):
                records = payload[key]
                break
        if records is None:
            records = [payload]
    else:
        records = [{"value": payload}]

    normalized = []
    for row in records[:100]:
        normalized.append(row if isinstance(row, dict) else {"value": row})
    return normalized


def _infer_schema(records: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    schema = []
    seen = set()
    for row in records:
        for key, value in row.items():
            if key in seen:
                continue
            seen.add(key)
            if isinstance(value, bool):
                dtype = "BOOLEAN"
            elif isinstance(value, int):
                dtype = "INTEGER"
            elif isinstance(value, float):
                dtype = "DOUBLE"
            elif isinstance(value, (dict, list)):
                dtype = "JSON"
            else:
                dtype = "STRING"
            schema.append({"column_name": str(key), "data_type": dtype})
    return schema


def _dq_rules_for_schema(schema: List[Dict[str, str]]) -> Dict[str, Any]:
    return {
        column["column_name"]: {
            "rules": ["NOT_NULL"] + (["VALID_JSON"] if column["data_type"] == "JSON" else []),
            "severity": "WARN",
        }
        for column in schema
    }


def _endpoint_url(base_url: str, endpoint: str) -> str:
    if endpoint.lower().startswith(("http://", "https://")):
        return endpoint
    return f"{base_url.rstrip('/')}/{endpoint.strip('/')}" if endpoint else base_url.rstrip("/")


# Canonical identifiers that represent EXECUTION PLATFORMS.
PLATFORM_IDENTIFIERS: set[str] = {"FABRIC", "DATABRICKS", "AWS", "AZURE"}

# Canonical identifiers that represent DATA-SOURCE CONNECTORS.
SOURCE_CONNECTOR_IDENTIFIERS: set[str] = {"REST_API", "S3", "ADLS", "LOCAL", "AWS", "AZURE"}


def _canonical_source_type(value: Optional[str]) -> Optional[str]:
    """Normalise a data-source connector name to its canonical upper-case form."""
    raw = (value or "").strip().upper()
    if raw in {"AWS", "S3"}:
        return "AWS"
    if raw in {"AZURE", "ADLS"}:
        return "AZURE"
    if raw in {"API", "REST", "REST_API"}:
        return "REST_API"
    if raw in {"FABRIC", "MICROSOFT_FABRIC"}:
        return "FABRIC"
    if raw == "LOCAL":
        return "LOCAL"
    return raw or None


def _canonical_platform(value: Optional[str]) -> Optional[str]:
    """Normalise a platform name to its canonical upper-case form."""
    raw = (value or "").strip().upper()
    mapping = {
        "FABRIC": "FABRIC",
        "MICROSOFT_FABRIC": "FABRIC",
        "MSFABRIC": "FABRIC",
        "DATABRICKS": "DATABRICKS",
        "AWS": "AWS",
        "AMAZON": "AWS",
        "AZURE": "AZURE",
        "MICROSOFT": "AZURE",
    }
    return mapping.get(raw)


def _is_platform(value: Optional[str]) -> bool:
    """Return True when the value is a platform identifier, not a source connector."""
    canonical = _canonical_source_type(value)
    return canonical in PLATFORM_IDENTIFIERS and canonical not in SOURCE_CONNECTOR_IDENTIFIERS



def _target_source_type(target: Optional[str]) -> Optional[str]:
    raw = (target or "").strip().lower()
    if raw in {"aws", "s3"}:
        return "AWS"
    if raw in {"azure", "adls"}:
        return "AZURE"
    if raw in {"fabric", "microsoft fabric", "msfabric"}:
        return "FABRIC"
    if raw in {"api", "rest", "rest_api"}:
        return "REST_API"
    if raw == "local":
        return "LOCAL"
    return None


def _configured_source_types(client_name: str) -> set[str]:
    db = SessionLocal()
    try:
        types = set()
        for cfg in db.query(APISourceConfig).filter(APISourceConfig.client_name == client_name, APISourceConfig.is_active == True).all():
            mapped = _canonical_source_type(cfg.source_type)
            if mapped:
                types.add(mapped)
        for row in db.query(MasterConfigAuthoritative.source_type).filter(MasterConfigAuthoritative.client_name == client_name, MasterConfigAuthoritative.is_active == True).distinct().all():
            mapped = _canonical_source_type(row[0])
            if mapped:
                types.add(mapped)
        for row in db.query(MasterConfig.source_system).filter(MasterConfig.client_name == client_name, MasterConfig.is_active == True).distinct().all():
            mapped = _canonical_source_type(row[0])
            if mapped:
                types.add(mapped)
        return types
    finally:
        db.close()

@router.post("/analyze")
async def run_discovery_analyze(request: AnalyzeRequest, http_request: Request):
    """
    Analyzes the live cloud environment or configs for a client.

    Architectural contract:
      - `platform`    = execution layer (FABRIC / DATABRICKS / AWS / AZURE)
                        Never registered as a source; never validated against configured_types.
      - `source_type` = data-source connector (REST_API / S3 / ADLS / LOCAL)
                        Registered by users; validated only when present and not a platform.

    Valid combinations (examples):
      FABRIC + REST_API  ✓
      FABRIC + LOCAL     ✓
      FABRIC + S3        ✓
      REST_API (legacy)  ✓
      S3 (legacy)        ✓
    """
    logger.info(f"Running live pipeline intelligence for {request.client_name}")
    try:
        # ── 1. Resolve platform and source-type separately ───────────────────
        requested_platform = _canonical_platform(request.platform)

        # source_type from request; also accept target/providers as a legacy fallback.
        requested_source_type = _canonical_source_type(request.source_type)
        
        # Legacy: derive source type from target/providers when no explicit source_type given
        if not requested_source_type:
            derived = _target_source_type(request.target or request.providers)
            if derived:
                requested_source_type = derived

        # ── 2. Fetch configured source types for this client ──────────────────
        configured_types = _configured_source_types(request.client_name)

        # ── 3. Debug logging ──────────────────────────────────────────────────
        logger.info(
            "Discovery validation | platform=%s source_type=%s configured_types=%s",
            requested_platform,
            requested_source_type,
            sorted(configured_types),
        )

        # ── 4. Validate ONLY the data-source connector ────────────────────────
        # We skip validation if:
        # a) No sources are configured yet for the client
        # b) The requested source type is FABRIC (which is a platform, not a connector)
        # c) The requested source type is None
        if (
            configured_types
            and requested_source_type
            and requested_source_type != "FABRIC"
            and requested_source_type not in configured_types
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Discovery target '{requested_source_type}' is not configured for client "
                    f"'{request.client_name}'. Configured source types: {sorted(configured_types)}"
                ),
            )

        # ── 5. Build the bearer token from the Authorization header ───────────
        authorization = http_request.headers.get("authorization")
        bearer_token = None
        if authorization and authorization.lower().startswith("bearer "):
            bearer_token = authorization.split(" ", 1)[1].strip()

        # ── 6. Determine providers/target for the scanner ─────────────────────
        # When a platform is given, use it as the scan target so the intelligence
        # service routes to the correct scanner (fabric / aws / azure).
        scan_target = request.target or (requested_platform.lower() if requested_platform else None)
        providers = request.providers or scan_target

        result = await analyze_pipeline_live(
            client_name=request.client_name,
            providers=providers,
            target=scan_target,
            auth_mode=request.auth_mode,
            credentials=request.credentials,
            use_cloud_llm=request.use_cloud_llm,
            llm_provider=request.llm_provider,
            use_local_llm=request.use_local_llm,
            scan_mode=request.scan_mode,
            authorization_token=bearer_token,
            payload=request.payload,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing live pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api-scan")
def run_api_scan(request: ApiScanRequest):
    """
    Scans registered REST API endpoints for a client.
    REST API configs without base_url or endpoints are intentionally skippable and return 400 here;
    the UI should allow Continue without scan for that case.
    """
    db = SessionLocal()
    try:
        query = db.query(APISourceConfig).filter(
            APISourceConfig.client_name == request.client_name,
            APISourceConfig.is_active == True,
        )
        if request.source_name:
            query = query.filter(APISourceConfig.source_name == request.source_name)
        configs = [
            cfg for cfg in query.all()
            if _canonical_source_type(cfg.source_type) == "REST_API"
        ]

        scan_configs = [
            cfg for cfg in configs
            if cfg.base_url and [ep.strip() for ep in (cfg.endpoints or "").split(",") if ep.strip()]
        ]
        if not scan_configs:
            raise HTTPException(status_code=400, detail="Provide API details to enable scanning")

        datasets = []
        discovered_assets = []
        warnings = []
        errors = []
        all_rules = {}

        for cfg in scan_configs:
            endpoints = [ep.strip() for ep in (cfg.endpoints or "").split(",") if ep.strip()]
            for endpoint in endpoints:
                url = _endpoint_url(cfg.base_url, endpoint)
                try:
                    req = urllib.request.Request(url, headers=_api_headers(cfg))
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        raw_text = resp.read().decode("utf-8-sig").strip()
                    payload = json.loads(raw_text)
                    records = _extract_records(payload)
                    schema = _infer_schema(records)
                    dq_rules = _dq_rules_for_schema(schema)
                    all_rules[endpoint] = dq_rules

                    dataset_id = generate_dataset_id(request.client_name, "API", f"{endpoint}/{endpoint.replace('/', '_')}.csv")
                    dataset = {
                        "dataset_id": dataset_id,
                        "source_name": cfg.source_name,
                        "endpoint": endpoint,
                        "full_url": url,
                        "file_name": f"{endpoint.replace('/', '_')}.csv",
                        "file_path": f"{endpoint}/{endpoint.replace('/', '_')}.csv",
                        "file_format": "CSV",
                        "record_sample_size": len(records),
                        "schema": schema,
                        "dq_rules": dq_rules,
                    }
                    datasets.append(dataset)
                    discovered_assets.append({
                        "type": "REST_API_ENDPOINT",
                        "name": endpoint,
                        "source_name": cfg.source_name,
                        "url": url,
                        "columns": [column["column_name"] for column in schema],
                    })

                    existing = db.query(MasterConfigAuthoritative).filter(
                        MasterConfigAuthoritative.dataset_id == dataset_id
                    ).first()
                    if not existing:
                        existing = MasterConfigAuthoritative(dataset_id=dataset_id)
                        db.add(existing)
                    existing.client_name = request.client_name
                    existing.source_type = "API"
                    existing.source_folder = endpoint
                    existing.source_object = dataset["file_name"]
                    existing.file_format = "CSV"
                    existing.raw_layer_path = f"Raw/{request.client_name}/API/{dataset['file_name']}"
                    existing.target_layer_bronze = f"Bronze/{request.client_name}/API/{dataset['file_name']}"
                    existing.target_layer_silver = f"Silver/{request.client_name}/API/{dataset['file_name']}"
                    existing.is_active = True
                except urllib.error.HTTPError as exc:
                    errors.append(f"{endpoint}: HTTP {exc.code} {exc.reason}")
                except Exception as exc:
                    errors.append(f"{endpoint}: {exc}")

        if not datasets:
            raise HTTPException(status_code=400, detail="REST API scan did not produce any datasets. " + "; ".join(errors))

        db.commit()
        source_path = ",".join(dataset["endpoint"] for dataset in datasets)
        return {
            "framework": "REST API",
            "scan_status": "partial" if errors else "success",
            "auth_mode": "credentials" if any((cfg.auth_type or "none").lower() != "none" for cfg in scan_configs) else "none",
            "is_fallback": False,
            "source_systems": [{"type": "REST_API", "source_name": cfg.source_name, "base_url": cfg.base_url} for cfg in scan_configs],
            "discovered_assets": discovered_assets,
            "datasets": datasets,
            "data_pipelines": [{"name": "REST API ingestion", "source_type": "API", "endpoint_count": len(datasets)}],
            "ingestion_support": {"file_based": False, "api": True, "database": False, "streaming": False, "batch": True},
            "ingestion_details": {"source_type": "API", "source_path": source_path, "target": "api"},
            "reformatted_config": {"source_type": "API", "source_path": source_path, "datasets": datasets},
            "pipeline_capabilities": {"scan_mode": "live", "api": True, "batch": True},
            "dq_rules": all_rules,
            "file_types": ["CSV"],
            "warnings": warnings,
            "errors": errors,
            "interactive_flow": ["REST API", "Infer schema", "Generate datasets", "Suggest DQ rules"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"REST API scan failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

class FabricRuntimePromoteRequest(BaseModel):
    client_name: str
    pipeline_name: str
    staging_table: str
    file_format: Optional[str] = "CSV"

@router.post("/fabric-runtime-promote-v2")
async def promote_fabric_runtime_to_adls(request: FabricRuntimePromoteRequest):
    """
    Exports staged NeonDB data to ADLS and returns the ADLS source metadata.
    """
    if not request.staging_table:
        raise HTTPException(status_code=400, detail="staging_table is required")
        
    try:
        # 1. Read directly from NeonDB staging table (Intermediate Staging)
        # The preview data was already extracted and staged in NeonDB by the preview step.
        conn = psycopg2.connect(settings.NEON_DB_URL)
        df = pd.read_sql(f"SELECT * FROM {request.staging_table}", conn)
        conn.close()
        
        if df is None or df.empty:
            raise HTTPException(status_code=400, detail="Extracted dataset is empty or not found in NeonDB.")
            
        # 2. Export to ADLS
        storage = get_storage_client()
        clean_pipeline = request.pipeline_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        folder_path = f"runtime_pipeline_sources/{request.client_name}/{clean_pipeline}"
        file_name = "source_export.csv"
        full_key = f"{folder_path}/{file_name}"
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        storage.put_object(Key=full_key, Body=csv_buffer.getvalue().encode('utf-8'))
        
        # 3. Return ADLS metadata - this allows the UI to pivot to a standard ADLS orchestration
        return {
            "status": "SUCCESS",
            "source_type": "ADLS",
            "source_path": f"az://{storage.default_container}/{full_key}",
            "folder_path": f"az://{storage.default_container}/{folder_path}",
            "source_object": file_name,
            "file_name": file_name,
            "file_format": "CSV",
            "total_rows": len(df)
        }
    except Exception as e:
        logger.error(f"Promotion to ADLS failed: {e}")
        raise HTTPException(status_code=500, detail=f"Promotion failed [V4_DIRECT]: {str(e)}")
