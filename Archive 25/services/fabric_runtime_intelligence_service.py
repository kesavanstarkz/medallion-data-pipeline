import asyncio
import json
import logging
import time
import base64
import re
from pathlib import PurePosixPath
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


from fastapi import HTTPException
from core.database import SessionLocal
from models.metadata import PipelineRunHistory
from services.fabric.pipeline_service import FabricPipelineService
from services.fabric.artifact_resolver import FabricArtifactResolver
from services.fabric_bundle_analysis_service import _extract_auto_discovered_config, merge_pipeline_configs

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"Completed", "Succeeded", "Failed", "Cancelled", "Canceled"}
FABRIC_REQUIRED_EXECUTION_SCOPES = {"Workspace.ReadWrite.All", "Item.ReadWrite.All", "Item.Execute.All"}
FABRIC_OPTIONAL_EXECUTION_SCOPES = {"DataPipeline.ReadWrite.All", "DataPipeline.Execute.All"}


def _iso_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _parse_fabric_datetime(value: Optional[str]) -> Optional[datetime]:
    raw = (value or "").strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    if "." in normalized:
        match = re.match(r"^(.*?\.)(\d+)([+-]\d{2}:\d{2})?$", normalized)
        if match:
            prefix, fraction, suffix = match.groups()
            normalized = f"{prefix}{fraction[:6]}{suffix or ''}"

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        if "+" not in normalized[10:] and "-" not in normalized[10:]:
            try:
                return datetime.fromisoformat(normalized + "+00:00")
            except ValueError:
                return None
        return None


def _decode_jwt_unverified(token: str) -> Dict[str, Any]:
    parts = (token or "").split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        data = json.loads(decoded.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _base_scope_name(scope: str) -> str:
    value = (scope or "").strip()
    if not value:
        return value
    if "/" in value:
        return value.rsplit("/", 1)[-1]
    return value


def validate_fabric_execution_token(access_token: str) -> Dict[str, Any]:
    claims = _decode_jwt_unverified(access_token)
    scopes = sorted({_base_scope_name(scope) for scope in str(claims.get("scp") or "").split(" ") if scope.strip()})
    roles = claims.get("roles") if isinstance(claims.get("roles"), list) else []
    aud = claims.get("aud")
    tenant = claims.get("tid")
    exp = claims.get("exp")
    now = int(time.time())
    expired = bool(exp and int(exp) <= now)
    missing_scopes = sorted(scope for scope in FABRIC_REQUIRED_EXECUTION_SCOPES if scope not in scopes)
    return {
        "aud": aud,
        "tenant": tenant,
        "exp": exp,
        "expired": expired,
        "scp": scopes,
        "roles": roles,
        "required_scopes": sorted(FABRIC_REQUIRED_EXECUTION_SCOPES),
        "optional_scopes": sorted(FABRIC_OPTIONAL_EXECUTION_SCOPES),
        "missing_scopes": missing_scopes,
        "has_required_scopes": not missing_scopes and aud == "https://api.fabric.microsoft.com" and not expired,
    }


def _fabric_permission_error(validation: Dict[str, Any]) -> HTTPException:
    return HTTPException(
        status_code=403,
        detail={
            "code": "InsufficientScopes",
            "message": "Your Microsoft login does not currently include Fabric execution permissions.",
            "current_scopes": validation.get("scp") or [],
            "required_scopes": validation.get("required_scopes") or [],
            "missing_scopes": validation.get("missing_scopes") or [],
            "token_audience": validation.get("aud"),
            "tenant": validation.get("tenant"),
            "expired": validation.get("expired"),
            "roles": validation.get("roles") or [],
            "admin_instructions": "Please grant delegated Microsoft Fabric API permissions:\n- Workspace.ReadWrite.All\n- Item.ReadWrite.All\n- Item.Execute.All\n\nThen provide admin consent.",
        },
    )


def _safe_get(data: Any, path: List[str]) -> Any:
    current = data
    for part in path:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        return value
    return None


def _find_first_key(data: Any, keys: List[str]) -> Any:
    wanted = {key.lower() for key in keys}
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).lower() in wanted:
                return value
            found = _find_first_key(value, keys)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_first_key(item, keys)
            if found is not None:
                return found
    return None


def _collect_strings(data: Any, limit: int = 200) -> List[str]:
    values: List[str] = []

    def visit(value: Any) -> None:
        if len(values) >= limit:
            return
        if isinstance(value, dict):
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif value is not None:
            text = str(value).strip()
            if text:
                values.append(text)

    visit(data)
    return values


def _to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        digits = value.strip().replace(",", "")
        if re.fullmatch(r"-?\d+", digits):
            return int(digits)
    return None


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
            return float(text)
    return None


def _infer_format(file_name: Optional[str], source_payload: Any, sink_payload: Any) -> Optional[str]:
    for candidate in (
        _find_first_key(source_payload, ["format", "fileFormat", "formatSettings", "type"]),
        _find_first_key(sink_payload, ["format", "fileFormat", "formatSettings", "type"]),
    ):
        if isinstance(candidate, str):
            text = candidate.strip()
            if text and text.lower() not in {"source", "sink"}:
                return text.upper()
    if file_name and "." in file_name:
        return file_name.rsplit(".", 1)[-1].upper()
    return None


def _normalize_format(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    mapping = {
        "csv": "csv",
        "json": "json",
        "txt": "txt",
        "text": "txt",
        "parquet": "parquet",
        "delta": "delta",
    }
    return mapping.get(text, text)


def _build_lakehouse_resolved_path(
    storage_type: Optional[str],
    folder_path: Optional[str],
    file_name: Optional[str],
    root_folder: Optional[str] = None,
) -> Optional[str]:
    if "lakehouse" not in str(storage_type or "").lower():
        return None
    root = str(root_folder or "Files").strip("/\\") or "Files"
    parts = [root]
    if folder_path:
        parts.append(str(folder_path).strip("/\\"))
    if file_name:
        parts.append(str(file_name).strip("/\\"))
    return "/".join(part for part in parts if part)


def _resolve_preview_strategy(source: Dict[str, Any]) -> str:
    connector = str(source.get("connector_type") or "").lower()
    fmt = str(source.get("format") or "").lower()
    resolved_path = str(source.get("resolved_path") or source.get("full_path") or "").lower()
    connection_metadata = source.get("connection_metadata") or {}

    if any(key in connection_metadata for key in ("endpoint", "url")) or connector in {"rest", "restapi", "webactivity", "http", "graphql", "soap"}:
        return "rest_api"
    if any(key in connection_metadata for key in ("jdbc_url", "table", "query", "schema")) or connector in {"jdbc", "sql", "sqlserver", "snowflake", "oracle", "postgresql", "mysql", "db2"}:
        return "spark_jdbc"
    if fmt in {"delta"}:
        return "spark_delta"
    if fmt in {"parquet", "orc", "avro", "excel", "xml", "json"}:
        return f"spark_{fmt}"
    if fmt in {"csv", "txt", "tsv"}:
        return "spark_delimited"
    if resolved_path.startswith(("abfss://", "abfs://", "wasbs://", "s3://", "ftp://", "sftp://")):
        return "spark_file"
    if str(source.get("storage_type") or "").lower() in {"lakehouse", "onelake"}:
        return "spark_file"
    return "metadata_only"


def _build_universal_source_model(source_connection: Dict[str, Any]) -> Dict[str, Any]:
    connection_metadata = _clean_object({
        "workspaceId": source_connection.get("workspace_id"),
        "artifactId": source_connection.get("artifact_id"),
        "rootFolder": source_connection.get("root_folder"),
        "folderPath": source_connection.get("folder_path"),
        "fileName": source_connection.get("file_name"),
        "resolvedPath": source_connection.get("resolved_path"),
        "previewPath": source_connection.get("preview_path"),
        "fullPath": source_connection.get("full_path"),
    })
    runtime_metadata = _clean_object({
        "activityName": source_connection.get("activity_name"),
        "linkedServiceName": source_connection.get("linked_service_name"),
        "headerEnabled": source_connection.get("header_enabled"),
        "delimiter": source_connection.get("delimiter"),
        "quoteChar": source_connection.get("quote_char"),
        "escapeChar": source_connection.get("escape_char"),
    })
    model = {
        "source_type": source_connection.get("storage_type") or source_connection.get("source_type"),
        "connector_type": source_connection.get("connector_type") or source_connection.get("source_type"),
        "format": _normalize_format(source_connection.get("format")),
        "workspace_id": source_connection.get("workspace_id"),
        "artifact_id": source_connection.get("artifact_id"),
        "connection_metadata": connection_metadata,
        "runtime_metadata": runtime_metadata,
        "preview_strategy": "",
    }
    model["preview_strategy"] = _resolve_preview_strategy({**source_connection, **model})
    return _clean_object(model)


def _split_path(path_value: Optional[str], file_name: Optional[str]) -> Dict[str, Optional[str]]:
    raw = str(path_value or "").strip()
    if not raw and file_name:
        return {"folder_path": None, "file_name": file_name, "full_path": file_name}
    if not raw:
        return {"folder_path": None, "file_name": file_name, "full_path": None}

    normalized = raw.replace("\\", "/")
    path = PurePosixPath(normalized)
    guessed_file = file_name
    if not guessed_file and path.suffix:
        guessed_file = path.name
    if guessed_file and normalized.endswith(guessed_file):
        folder = normalized[: -len(guessed_file)].rstrip("/")
    elif path.suffix:
        folder = str(path.parent).rstrip("/")
    else:
        folder = normalized.rstrip("/")
    return {
        "folder_path": folder or None,
        "file_name": guessed_file,
        "full_path": normalized,
    }


def _clean_object(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned = {key: _clean_object(item) for key, item in value.items()}
        return {
            key: item for key, item in cleaned.items()
            if item not in (None, "", [], {})
        }
    if isinstance(value, list):
        return [item for item in (_clean_object(item) for item in value) if item not in (None, "", [], {})]
    return value


def _pick_primary_copy_activity(activity_runs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    copy_runs = [run for run in activity_runs if str(run.get("activityType") or "").lower() == "copy"]
    if not copy_runs:
        return None

    def sort_key(run: Dict[str, Any]) -> tuple:
        output_payload = run.get("output") or {}
        rows = _to_int(output_payload.get("rowsRead")) or 0
        written = _to_int(output_payload.get("rowsCopied") or output_payload.get("rowsWritten")) or 0
        duration = _to_int(run.get("durationInMs")) or 0
        return (max(rows, written), duration)

    return max(copy_runs, key=sort_key)


def _extract_runtime_connection(connection_role: str, activity_run: Dict[str, Any], pipeline_summary: Dict[str, Any]) -> Dict[str, Any]:
    input_payload = activity_run.get("input") or {}
    output_payload = activity_run.get("output") or {}
    payload = input_payload.get(connection_role) or {}
    location = _find_first_key(payload, ["location"]) or {}
    strings = " ".join(_collect_strings([payload, location, input_payload, output_payload])[:80]).lower()

    file_name = _first_non_empty(
        _find_first_key(payload, ["fileName", "filename", "wildcardFileName", "objectName", "table"]),
        _find_first_key(location, ["fileName", "filename", "wildcardFileName", "objectName", "table"]),
    )
    path_value = _first_non_empty(
        _find_first_key(payload, ["path", "folderPath", "folder", "directory", "objectPath", "tablePath"]),
        _find_first_key(location, ["path", "folderPath", "folder", "directory", "objectPath", "tablePath"]),
    )
    split_path = _split_path(str(path_value) if path_value is not None else None, str(file_name) if file_name is not None else None)
    storage_type = _first_non_empty(
        _find_first_key(payload, ["store", "storeType", "storageType"]),
        "Lakehouse" if "lakehouse" in strings else None,
        "Warehouse" if "warehouse" in strings else None,
    )
    connector_type = _first_non_empty(
        _find_first_key(payload, ["type", "datasetType", "connectorType"]),
        _find_first_key(location, ["type", "datasetType", "connectorType"]),
        storage_type,
    )
    role_key = "source_type" if connection_role == "source" else "target_type"
    root_folder = _first_non_empty(
        _find_first_key(payload, ["rootFolder", "rootPath"]),
        "Files" if "lakehouse" in str(storage_type or "").lower() else None,
    )
    resolved_path = _build_lakehouse_resolved_path(
        storage_type=storage_type,
        folder_path=split_path.get("folder_path"),
        file_name=split_path.get("file_name"),
        root_folder=root_folder,
    )
    file_format = _normalize_format(_infer_format(str(split_path.get("file_name") or ""), payload, payload))

    connection = {
        role_key: _first_non_empty(
            _find_first_key(payload, ["type", "datasetType"]),
            storage_type,
        ),
        "connector_type": connector_type,
        "storage_type": storage_type,
        "format": file_format,
        "file_name": split_path.get("file_name"),
        "folder_path": split_path.get("folder_path"),
        "full_path": split_path.get("full_path"),
        "root_folder": root_folder,
        "resolved_path": resolved_path,
        "workspace_id": _first_non_empty(
            _find_first_key([payload, location, output_payload], ["workspaceId", "workspace_id"]),
            pipeline_summary.get("workspace_id"),
        ),
        "artifact_id": _find_first_key([payload, location, output_payload], ["artifactId", "itemId", "lakehouseId", "warehouseId", "artifact_id"]),
        "delimiter": _find_first_key(payload, ["delimiter", "columnDelimiter", "fieldDelimiter"]),
        "escape_char": _find_first_key(payload, ["escapeChar", "escapeCharacter"]),
        "quote_char": _find_first_key(payload, ["quoteChar", "quoteCharacter"]),
        "header_enabled": _to_bool(_find_first_key(payload, ["firstRowAsHeader", "header", "hasHeader", "headerEnabled"])),
        "linked_service_name": activity_run.get("linkedServiceName"),
        "activity_name": activity_run.get("activityName"),
        "preview_path": split_path.get("full_path") if str(split_path.get("full_path") or "").startswith(("az://", "s3://", "https://")) else None,
        "preview_supported": bool(split_path.get("full_path") or resolved_path),
    }
    connection["connection_metadata"] = _clean_object({
        "workspaceId": connection.get("workspace_id"),
        "artifactId": connection.get("artifact_id"),
        "rootFolder": root_folder,
        "folderPath": split_path.get("folder_path"),
        "fileName": split_path.get("file_name"),
        "resolvedPath": resolved_path,
        "path": split_path.get("full_path"),
    })
    connection["runtime_metadata"] = _clean_object({
        "activityName": activity_run.get("activityName"),
        "activityType": activity_run.get("activityType"),
        "linkedServiceName": activity_run.get("linkedServiceName"),
        "headerEnabled": connection.get("header_enabled"),
        "delimiter": connection.get("delimiter"),
        "quoteChar": connection.get("quote_char"),
        "escapeChar": connection.get("escape_char"),
    })
    connection["preview_strategy"] = _resolve_preview_strategy(connection)
    return _clean_object(connection)


def _sample_rows_from_runtime(actual_metadata_rows: List[Dict[str, Any]], activity_outputs: Dict[str, Any]) -> List[Dict[str, Any]]:
    for row in actual_metadata_rows:
        rows = row.get("rows") or []
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[:25]
        first_row = row.get("first_row")
        if isinstance(first_row, dict):
            return [first_row]
    for payload in activity_outputs.values():
        output_payload = (payload or {}).get("output") or {}
        for key in ("value", "rows", "data", "records"):
            rows = output_payload.get(key)
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                return rows[:25]
        first_row = output_payload.get("firstRow")
        if isinstance(first_row, dict):
            return [first_row]
    return []


def _infer_value_type(value: Any) -> str:
    if value in (None, ""):
        return "STRING"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, (dict, list)):
        return "JSON"
    text = str(value).strip()
    if re.fullmatch(r"-?\d+", text):
        return "INTEGER"
    if re.fullmatch(r"-?\d+\.\d+", text):
        return "DOUBLE"
    if _parse_fabric_datetime(text):
        return "TIMESTAMP"
    return "STRING"


def _infer_schema_profile(sample_rows: List[Dict[str, Any]], actual_primary_keys: List[Any]) -> Dict[str, Any]:
    if not sample_rows:
        return {
            "columns": [],
            "nullable_columns": [],
            "timestamp_columns": [],
            "primary_key_candidates": [str(item) for item in actual_primary_keys if item],
            "sample_rows": [],
        }

    column_order: List[str] = []
    for row in sample_rows:
        for key in row.keys():
            if key not in column_order:
                column_order.append(str(key))

    columns = []
    timestamp_columns: List[str] = []
    nullable_columns: List[str] = []
    primary_key_candidates: List[str] = []
    row_count = len(sample_rows)
    for column_name in column_order:
        values = [row.get(column_name) for row in sample_rows]
        non_empty = [value for value in values if value not in (None, "")]
        inferred = _infer_value_type(non_empty[0]) if non_empty else "STRING"
        nullable = len(non_empty) != len(values)
        if inferred == "TIMESTAMP" or any(token in column_name.lower() for token in ["date", "time", "timestamp"]):
            timestamp_columns.append(column_name)
        if nullable:
            nullable_columns.append(column_name)
        distinct_values = {json.dumps(value, sort_keys=True, default=str) for value in non_empty}
        if non_empty and len(distinct_values) == row_count and any(token in column_name.lower() for token in ["id", "key", "code"]):
            primary_key_candidates.append(column_name)
        columns.append({
            "column_name": column_name,
            "data_type": inferred,
            "nullable": nullable,
            "ordinal_position": len(columns) + 1,
            "sample_value": non_empty[0] if non_empty else None,
        })

    for key in actual_primary_keys:
        if key and str(key) not in primary_key_candidates:
            primary_key_candidates.append(str(key))

    return {
        "columns": columns,
        "nullable_columns": nullable_columns,
        "timestamp_columns": timestamp_columns,
        "primary_key_candidates": primary_key_candidates,
        "sample_rows": sample_rows[:25],
    }


def _build_runtime_statistics(activity_run: Dict[str, Any], runtime_metrics: Dict[str, Any]) -> Dict[str, Any]:
    output_payload = activity_run.get("output") or {}
    duration_ms = _to_int(activity_run.get("durationInMs")) or runtime_metrics.get("pipeline_duration_ms") or 0
    rows_read = _to_int(output_payload.get("rowsRead"))
    rows_written = _to_int(output_payload.get("rowsCopied") or output_payload.get("rowsWritten"))
    files_read = _to_int(output_payload.get("filesRead")) or (1 if rows_read is not None else None)
    files_written = _to_int(output_payload.get("filesWritten")) or (1 if rows_written is not None else None)
    bytes_read = _to_float(output_payload.get("dataRead") or output_payload.get("bytesRead"))
    throughput = _to_float(output_payload.get("throughput"))
    if throughput is None and bytes_read and duration_ms:
        throughput = round(bytes_read / max(duration_ms / 1000.0, 0.001) / (1024 * 1024), 3)
    return _clean_object({
        "rows_read": rows_read,
        "rows_written": rows_written,
        "files_read": files_read,
        "files_written": files_written,
        "throughput": throughput,
        "duration_ms": duration_ms or None,
    })


def _build_dq_recommendations(
    source_connection: Dict[str, Any],
    target_connection: Dict[str, Any],
    schema_profile: Dict[str, Any],
    runtime_statistics: Dict[str, Any],
) -> List[Dict[str, Any]]:
    recommendations: List[Dict[str, Any]] = []
    if source_connection.get("delimiter"):
        recommendations.append({
            "rule": "DELIMITER_CONSISTENCY",
            "severity": "WARN",
            "reason": f"Validate that every inbound row uses delimiter '{source_connection['delimiter']}'.",
        })
    if source_connection.get("header_enabled") is True:
        recommendations.append({
            "rule": "HEADER_VALIDATION",
            "severity": "WARN",
            "reason": "Confirm the header row matches the inferred schema and column order.",
        })
    if runtime_statistics.get("rows_read") is not None and runtime_statistics.get("rows_written") is not None:
        severity = "INFO" if runtime_statistics["rows_read"] == runtime_statistics["rows_written"] else "ERROR"
        recommendations.append({
            "rule": "ROW_COUNT_RECONCILIATION",
            "severity": severity,
            "reason": f"Rows read ({runtime_statistics['rows_read']}) should match rows written ({runtime_statistics['rows_written']}).",
        })
    for column in schema_profile.get("columns", []):
        if not column.get("nullable"):
            recommendations.append({
                "rule": "NOT_NULL",
                "severity": "WARN",
                "column": column.get("column_name"),
                "reason": f"Column '{column.get('column_name')}' was populated in sampled runtime rows and is a candidate for not-null enforcement.",
            })
    if schema_profile.get("timestamp_columns"):
        recommendations.append({
            "rule": "TIMESTAMP_PARSE",
            "severity": "WARN",
            "reason": f"Normalize timestamp columns: {', '.join(schema_profile['timestamp_columns'])}.",
        })
    return recommendations


def _build_activity_intelligence(activity_run: Dict[str, Any], source_connection: Dict[str, Any], target_connection: Dict[str, Any]) -> List[str]:
    activity_name = activity_run.get("activityName") or "Copy activity"
    source_type = source_connection.get("storage_type") or source_connection.get("source_type") or "source"
    target_type = target_connection.get("storage_type") or target_connection.get("target_type") or "target"
    source_folder = source_connection.get("folder_path") or source_connection.get("file_name") or "runtime source"
    target_folder = target_connection.get("folder_path") or target_connection.get("file_name") or "runtime target"
    source_format = source_connection.get("format") or source_connection.get("source_type") or "source payload"
    target_format = target_connection.get("format") or target_connection.get("target_type") or "target payload"
    return [
        f"{activity_name} copied {source_format} data from {source_type} '{source_folder}' to {target_type} '{target_folder}'.",
        f"Source file '{source_connection.get('file_name') or 'runtime-discovered object'}' was written as {target_format} into '{target_connection.get('file_name') or target_folder}'.",
    ]


def _build_reusable_ingestion_config(
    pipeline_summary: Dict[str, Any],
    source_connection: Dict[str, Any],
    target_connection: Dict[str, Any],
    schema_profile: Dict[str, Any],
    dq_recommendations: List[Dict[str, Any]],
    runtime_statistics: Dict[str, Any],
) -> Dict[str, Any]:
    return _clean_object({
        "source": source_connection,
        "target": target_connection,
        "schema": {
            "columns": schema_profile.get("columns", []),
            "primary_key_candidates": schema_profile.get("primary_key_candidates", []),
            "timestamp_columns": schema_profile.get("timestamp_columns", []),
        },
        "dq_rules": dq_recommendations,
        "runtime_metrics": runtime_statistics,
        "pipeline_context": pipeline_summary,
    })



def _build_runtime_source_discovery(
    job_instance: Dict[str, Any],
    activity_runs: List[Dict[str, Any]],
    actual_metadata_rows: List[Dict[str, Any]],
    activity_outputs: Dict[str, Any],
    actual_primary_keys: List[Any],
    runtime_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    pipeline_summary = {
        "workspace_id": job_instance.get("workspaceId"),
        "pipeline_run_id": job_instance.get("id"),
    }
    primary_copy = _pick_primary_copy_activity(activity_runs)
    if not primary_copy:
        return {
            "source_connection": {},
            "target_connection": {},
            "runtime_statistics": runtime_metrics,
            "schema_discovery": {
                "columns": [],
                "sample_rows": [],
                "primary_key_candidates": [str(item) for item in actual_primary_keys if item],
            },
            "ingestion_config": {},
            "dq_recommendations": [],
            "activity_intelligence": [],
            "lineage_summary": [],
        }

    source_connection = _extract_runtime_connection("source", primary_copy, pipeline_summary)
    target_connection = _extract_runtime_connection("sink", primary_copy, pipeline_summary)
    sample_rows = _sample_rows_from_runtime(actual_metadata_rows, activity_outputs)
    schema_profile = _infer_schema_profile(sample_rows, actual_primary_keys)
    runtime_statistics = _build_runtime_statistics(primary_copy, runtime_metrics)
    dq_recommendations = _build_dq_recommendations(source_connection, target_connection, schema_profile, runtime_statistics)
    activity_intelligence = _build_activity_intelligence(primary_copy, source_connection, target_connection)
    lineage_summary = [{
        "source_label": source_connection.get("storage_type") or source_connection.get("source_type") or "Source",
        "activity_label": primary_copy.get("activityName") or "Copy",
        "target_label": target_connection.get("storage_type") or target_connection.get("target_type") or "Target",
    }]
    return {
        "source_connection": source_connection,
        "target_connection": target_connection,
        "resolved_source": _build_universal_source_model(source_connection),
        "runtime_statistics": runtime_statistics,
        "schema_discovery": schema_profile,
        "ingestion_config": _build_reusable_ingestion_config(
            {
                "pipeline_run_id": job_instance.get("id"),
                "status": job_instance.get("status"),
                "workspace_id": source_connection.get("workspace_id") or target_connection.get("workspace_id"),
            },
            source_connection,
            target_connection,
            schema_profile,
            dq_recommendations,
            runtime_statistics,
        ),
        "dq_recommendations": dq_recommendations,
        "activity_intelligence": activity_intelligence,
        "lineage_summary": lineage_summary,
    }


def _status_bucket(status: str) -> str:
    normalized = (status or "").lower()
    if normalized in {"completed", "succeeded", "success"}:
        return "SUCCESS"
    if normalized in {"failed", "cancelled", "canceled"}:
        return "FAILURE"
    return "RUNNING"


def _extract_runtime_activity_payload(activity_run: Dict[str, Any]) -> Dict[str, Any]:
    input_payload = activity_run.get("input") or {}
    output_payload = activity_run.get("output") or {}
    error_payload = activity_run.get("error") or {}
    return {
        "activity_name": activity_run.get("activityName"),
        "activity_type": activity_run.get("activityType"),
        "status": activity_run.get("status"),
        "start_time": activity_run.get("activityRunStart"),
        "end_time": activity_run.get("activityRunEnd"),
        "duration_ms": activity_run.get("durationInMs"),
        "retry_attempt": activity_run.get("retryAttempt"),
        "iteration_hash": activity_run.get("iterationHash"),
        "input": input_payload,
        "output": output_payload,
        "error": error_payload,
        "linked_service_name": activity_run.get("linkedServiceName"),
    }


def _resolve_runtime_expressions(static_config: Dict[str, Any], activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    static_expressions = static_config.get("resolvedExpressions") or []
    run_by_name = {run.get("activityName"): run for run in activity_runs if run.get("activityName")}
    resolved = []
    for expr in static_expressions:
        activity_name = expr.get("activity")
        run = run_by_name.get(activity_name)
        runtime_value = None
        resolved_from = None
        if run:
            field_path = expr.get("field_path", "")
            lowered = field_path.lower()
            if "url" in lowered:
                runtime_value = _safe_get(run.get("input") or {}, ["url"]) or _safe_get(run.get("input") or {}, ["relativeUrl"])
                resolved_from = f"{activity_name} input"
            elif "items" in lowered:
                runtime_value = _safe_get(run.get("input") or {}, ["items"])
                resolved_from = f"{activity_name} input"
            elif "parameters" in lowered:
                runtime_value = _safe_get(run.get("input") or {}, ["parameters"])
                resolved_from = f"{activity_name} input parameters"
            elif "sqlreaderquery" in lowered or "source.query" in lowered:
                runtime_value = _safe_get(run.get("input") or {}, ["source", "sqlReaderQuery"]) or _safe_get(run.get("input") or {}, ["sqlReaderQuery"])
                resolved_from = f"{activity_name} input SQL"
            if runtime_value is None:
                runtime_value = _safe_get(run.get("output") or {}, ["value"]) or _safe_get(run.get("output") or {}, ["firstRow"])
                if runtime_value is not None:
                    resolved_from = f"{activity_name} output"
        if runtime_value is not None:
            resolved.append({
                "expression": expr.get("expression"),
                "resolved_value": runtime_value,
                "resolved_from": resolved_from or expr.get("resolved_from"),
                "activity": activity_name,
                "semantic_meaning": expr.get("semantic_meaning"),
            })
    return resolved


def _actual_api_endpoints(activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    endpoints = []
    for run in activity_runs:
        if str(run.get("activityType") or "").lower() != "webactivity":
            continue
        input_payload = run.get("input") or {}
        output_payload = run.get("output") or {}
        endpoints.append({
            "activity": run.get("activityName"),
            "method": input_payload.get("method"),
            "url": input_payload.get("url") or input_payload.get("relativeUrl"),
            "headers": input_payload.get("headers"),
            "status_code": output_payload.get("statusCode") or output_payload.get("statuscode"),
            "response": output_payload.get("body") or output_payload,
        })
    return endpoints


def _actual_metadata_rows(activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for run in activity_runs:
        if str(run.get("activityType") or "").lower() != "lookup":
            continue
        output_payload = run.get("output") or {}
        rows.append({
            "activity": run.get("activityName"),
            "executed_sql": _safe_get(run.get("input") or {}, ["source", "sqlReaderQuery"]) or _safe_get(run.get("input") or {}, ["sqlReaderQuery"]),
            "rows": output_payload.get("value") or output_payload.get("rows") or [],
            "first_row": output_payload.get("firstRow"),
            "schema": output_payload.get("structure") or output_payload.get("schema"),
        })
    return rows


def _runtime_lineage(activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lineage = []
    for run in activity_runs:
        if str(run.get("activityType") or "").lower() != "copy":
            continue
        input_payload = run.get("input") or {}
        output_payload = run.get("output") or {}
        source = input_payload.get("source") or {}
        sink = input_payload.get("sink") or {}
        lineage.append({
            "activity": run.get("activityName"),
            "source_type": source.get("type"),
            "sink_type": sink.get("type"),
            "source_path": source.get("path") or source.get("fileName") or source.get("table"),
            "target_path": sink.get("path") or sink.get("fileName") or sink.get("table"),
            "rows_read": output_payload.get("rowsRead"),
            "rows_written": output_payload.get("rowsCopied") or output_payload.get("rowsWritten"),
            "format": source.get("format") or sink.get("format"),
        })
    return lineage


def _runtime_notebook_params(activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    notebooks = []
    for run in activity_runs:
        if "notebook" not in str(run.get("activityType") or "").lower():
            continue
        input_payload = run.get("input") or {}
        output_payload = run.get("output") or {}
        notebooks.append({
            "activity": run.get("activityName"),
            "notebook_id": input_payload.get("notebookId") or input_payload.get("notebook"),
            "parameters": input_payload.get("parameters") or {},
            "duration_ms": run.get("durationInMs"),
            "output": output_payload,
        })
    return notebooks


def _runtime_sql_queries(activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    queries = []
    for run in activity_runs:
        input_payload = run.get("input") or {}
        sql = (
            _safe_get(input_payload, ["source", "sqlReaderQuery"])
            or input_payload.get("sqlReaderQuery")
            or input_payload.get("query")
        )
        if str(run.get("activityType") or "").lower() == "script":
            sql = sql or input_payload.get("script") or input_payload.get("text")
        if sql:
            queries.append({
                "activity": run.get("activityName"),
                "activity_type": run.get("activityType"),
                "sql": sql,
                "affected_rows": (run.get("output") or {}).get("rowsAffected"),
            })
    return queries


def _runtime_dq_observations(activity_runs: List[Dict[str, Any]], lineage: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    observations = []
    for row in lineage:
        if row.get("rows_read") is not None and row.get("rows_written") is not None and row["rows_read"] != row["rows_written"]:
            observations.append({
                "activity": row.get("activity"),
                "observation": "Row count mismatch",
                "evidence": f"rowsRead={row.get('rows_read')} rowsWritten={row.get('rows_written')}",
            })
    for run in activity_runs:
        if (run.get("retryAttempt") or 0) > 0:
            observations.append({
                "activity": run.get("activityName"),
                "observation": "Runtime retry detected",
                "evidence": f"retryAttempt={run.get('retryAttempt')}",
            })
        if str(run.get("status") or "").lower() == "failed":
            observations.append({
                "activity": run.get("activityName"),
                "observation": "Activity failure captured",
                "evidence": (run.get("error") or {}).get("message"),
            })
    return observations


def _execution_graph(static_config: Dict[str, Any], activity_runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    static_graph = static_config.get("activity_graph") or {}
    run_map = {run.get("activityName"): run for run in activity_runs if run.get("activityName")}
    nodes = []
    for node in static_graph.get("nodes", []):
        run = run_map.get(node.get("label")) or run_map.get(node.get("id"))
        nodes.append({
            **node,
            "status": run.get("status") if run else None,
            "duration_ms": run.get("durationInMs") if run else None,
            "start_time": run.get("activityRunStart") if run else None,
            "end_time": run.get("activityRunEnd") if run else None,
        })
    if not nodes:
        nodes = [
            {
                "id": run.get("activityName"),
                "label": run.get("activityName"),
                "type": run.get("activityType"),
                "status": run.get("status"),
                "duration_ms": run.get("durationInMs"),
                "start_time": run.get("activityRunStart"),
                "end_time": run.get("activityRunEnd"),
            }
            for run in activity_runs
        ]
    return {
        "nodes": nodes,
        "edges": static_graph.get("edges", []),
        "success_paths": static_graph.get("success_paths", []),
        "failure_paths": static_graph.get("failure_paths", []),
        "execution_paths": static_graph.get("execution_paths", []),
    }


def _activity_output_explorer(activity_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [_extract_runtime_activity_payload(run) for run in activity_runs]


def _persist_runtime_capture(
    client_name: str,
    job_instance_id: str,
    workspace_id: str,
    pipeline_id: str,
    execution_status: str,
    start_time: Optional[str],
    end_time: Optional[str],
    payload: Dict[str, Any],
) -> None:
    db = SessionLocal()
    try:
        record = db.query(PipelineRunHistory).filter_by(run_id=job_instance_id).first()
        if not record:
            record = PipelineRunHistory(run_id=job_instance_id, batch_id=datetime.utcnow().strftime("%m-%d-%H"))
            db.add(record)
        record.client_name = client_name
        record.source_type = "FABRIC_RUNTIME"
        record.folder_path = f"fabric://{workspace_id}/{pipeline_id}"
        record.status = execution_status
        if start_time:
            start_dt = _parse_fabric_datetime(start_time)
            if start_dt:
                record.start_time = start_dt.replace(tzinfo=None)
        if end_time:
            end_dt = _parse_fabric_datetime(end_time)
            if end_dt:
                record.end_time = end_dt.replace(tzinfo=None)
        record.total_datasets = len(payload.get("activity_outputs") or {})
        record.success_count = sum(1 for item in payload.get("runtime_activity_tracker", []) if str(item.get("status") or "").lower() in {"completed", "succeeded"})
        record.failure_count = sum(1 for item in payload.get("runtime_activity_tracker", []) if str(item.get("status") or "").lower() == "failed")
        record.pipeline_results = payload
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Failed to persist runtime intelligence capture")
    finally:
        db.close()


async def execute_and_capture_runtime_intelligence(
    client_name: str,
    workspace_id: str,
    pipeline_id: str,
    access_token: str,
    existing_analysis: Optional[Dict[str, Any]] = None,
    poll_interval_seconds: int = 10,
    timeout_seconds: int = 900,
) -> Dict[str, Any]:
    token_validation = validate_fabric_execution_token(access_token)
    if not token_validation.get("has_required_scopes"):
        raise _fabric_permission_error(token_validation)

    pipeline_service = FabricPipelineService(access_token)
    try:
        pipeline_item = await pipeline_service.get_pipeline(workspace_id, pipeline_id)
    except HTTPException as exc:
        if exc.status_code == 403:
            raise _fabric_permission_error(token_validation)
        raise
    run_start_requested_at = datetime.utcnow()
    pipeline_name = pipeline_item.get("displayName") or pipeline_item.get("name") or "pipeline"
    try:
        run_response = await pipeline_service.run_pipeline(workspace_id, pipeline_id, pipeline_name=pipeline_name)
    except HTTPException as exc:
        if exc.status_code == 403:
            raise _fabric_permission_error(token_validation)
        raise
    job_instance_id = run_response.get("job_instance_id")
    if not job_instance_id:
        raise HTTPException(status_code=500, detail="Fabric run endpoint did not return a job instance id.")

    job_instance = {}
    activity_runs: List[Dict[str, Any]] = []
    
    # PHASE 1: Wait for Pipeline Completion
    deadline = datetime.utcnow() + timedelta(seconds=timeout_seconds)
    pipeline_completed = False
    
    while datetime.utcnow() < deadline:
        try:
            job_instance = await pipeline_service.get_pipeline_job_instance(workspace_id, pipeline_id, job_instance_id)
            status = str(job_instance.get("status") or "")
            if status in TERMINAL_STATUSES:
                logger.info(f"Pipeline {job_instance_id} reached terminal status: {status}")
                pipeline_completed = True
                break
        except HTTPException as exc:
            if exc.status_code == 403:
                raise _fabric_permission_error(token_validation)
            raise
        await asyncio.sleep(poll_interval_seconds)

    # PHASE 2: Wait for Activity Metadata (Eventual Consistency)
    # Fabric completion does not guarantee immediate activity metadata availability.
    root_activity_id = job_instance.get("rootActivityId")
    logger.info(f"Starting activity polling for job {job_instance_id}. rootActivityId: {root_activity_id}")
    
    max_activity_retries = 20
    activity_attempt = 0
    backoff_intervals = [3, 3, 5, 5, 8, 8, 10, 10] # Custom backoff sequence
    
    while activity_attempt < max_activity_retries:
        query_after = (run_start_requested_at - timedelta(minutes=5)).isoformat() + "Z"
        query_before = _iso_now()
        
        try:
            activity_runs = await pipeline_service.query_activity_runs(workspace_id, job_instance_id, query_after, query_before)
            
            logger.info(
                f"Activity polling attempt {activity_attempt + 1}/{max_activity_retries}: "
                f"Found {len(activity_runs)} activities for job {job_instance_id}. "
                f"Status: {job_instance.get('status')}"
            )
            
            if activity_runs:
                # We found activities, stop polling
                break
                
        except Exception as e:
            logger.error(f"Error querying activity runs during polling: {e}")
            
        activity_attempt += 1
        if activity_attempt >= max_activity_retries:
            break
            
        # Determine wait time
        wait_time = backoff_intervals[min(activity_attempt, len(backoff_intervals)-1)]
        await asyncio.sleep(wait_time)

    if not activity_runs and pipeline_completed and job_instance.get("status") == "Completed":
        logger.warning(f"Fabric returned Completed status for {job_instance_id} but no activity metadata became available after {max_activity_retries} retries.")
        # We continue anyway to return the job instance state, but discovery will be empty.

    static_config = _extract_auto_discovered_config(existing_analysis or {}) if existing_analysis else {}
    final_pipeline_config = merge_pipeline_configs(static_config, static_config) if static_config else {}
    runtime_values = {
        item["activity_name"]: {
            "input": item.get("input"),
            "output": item.get("output"),
            "status": item.get("status"),
        }
        for item in _activity_output_explorer(activity_runs)
    }
    resolved_expressions = _resolve_runtime_expressions(static_config, activity_runs)
    actual_api_endpoints = _actual_api_endpoints(activity_runs)
    actual_metadata_rows = _actual_metadata_rows(activity_runs)
    lineage = _runtime_lineage(activity_runs)
    runtime_notebook_parameters = _runtime_notebook_params(activity_runs)
    runtime_sql_queries = _runtime_sql_queries(activity_runs)
    runtime_dq_observations = _runtime_dq_observations(activity_runs, lineage)
    execution_graph = _execution_graph(static_config, activity_runs)
    activity_outputs = {
        item["activity_name"]: item
        for item in _activity_output_explorer(activity_runs)
        if item.get("activity_name")
    }
    runtime_metrics = {
        "total_activities": len(activity_runs),
        "success_count": sum(1 for run in activity_runs if str(run.get("status") or "").lower() in {"completed", "succeeded"}),
        "failure_count": sum(1 for run in activity_runs if str(run.get("status") or "").lower() == "failed"),
        "retry_count": sum((run.get("retryAttempt") or 0) for run in activity_runs),
        "pipeline_duration_ms": None,
    }
    if job_instance.get("startTimeUtc") and job_instance.get("endTimeUtc"):
        start_dt = _parse_fabric_datetime(job_instance.get("startTimeUtc"))
        end_dt = _parse_fabric_datetime(job_instance.get("endTimeUtc"))
        if start_dt and end_dt:
            runtime_metrics["pipeline_duration_ms"] = int((end_dt - start_dt).total_seconds() * 1000)
    runtime_source_discovery = _build_runtime_source_discovery(
        job_instance=job_instance,
        activity_runs=activity_runs,
        actual_metadata_rows=actual_metadata_rows,
        activity_outputs=activity_outputs,
        actual_primary_keys=[
            row.get("first_row", {}).get("PrimaryKey")
            for row in actual_metadata_rows
            if isinstance(row.get("first_row"), dict) and row.get("first_row", {}).get("PrimaryKey")
        ],
        runtime_metrics=runtime_metrics,
    )
    
    # Resolve missing artifact IDs dynamically from Fabric workspace items
    artifact_resolver = FabricArtifactResolver(access_token)
    resolution_diagnostics = []
    
    source_conn = runtime_source_discovery.get("source_connection")
    if source_conn and not source_conn.get("artifact_id"):
        resolved_id = await artifact_resolver.resolve_artifact_id(
            workspace_id, 
            source_conn, 
            diagnostics=resolution_diagnostics
        )
        if resolved_id:
            source_conn["artifact_id"] = resolved_id
            if "connection_metadata" in source_conn:
                source_conn["connection_metadata"]["artifactId"] = resolved_id

    target_conn = runtime_source_discovery.get("target_connection")
    if target_conn and not target_conn.get("artifact_id"):
        resolved_id = await artifact_resolver.resolve_artifact_id(
            workspace_id, 
            target_conn, 
            diagnostics=resolution_diagnostics
        )
        if resolved_id:
            target_conn["artifact_id"] = resolved_id
            if "connection_metadata" in target_conn:
                target_conn["connection_metadata"]["artifactId"] = resolved_id
    
    runtime_source_discovery["resolution_diagnostics"] = resolution_diagnostics

    payload = {
        "pipeline_run_id": job_instance_id,
        "execution_status": job_instance.get("status"),
        "runtime_values": runtime_values,
        "resolved_expressions": resolved_expressions,
        "actual_api_endpoints": actual_api_endpoints,
        "actual_metadata_rows": actual_metadata_rows,
        "actual_tables": [row.get("first_row", {}).get("TableName") for row in actual_metadata_rows if isinstance(row.get("first_row"), dict) and row.get("first_row", {}).get("TableName")],
        "actual_primary_keys": [row.get("first_row", {}).get("PrimaryKey") for row in actual_metadata_rows if isinstance(row.get("first_row"), dict) and row.get("first_row", {}).get("PrimaryKey")],
        "lineage": lineage,
        "execution_graph": execution_graph,
        "runtime_dq_observations": runtime_dq_observations,
        "activity_outputs": activity_outputs,
        "runtime_metrics": runtime_metrics,
        "runtime_execution_summary": {
            "pipeline_name": pipeline_name,
            "pipeline_run_id": job_instance_id,
            "workspace_id": workspace_id,
            "pipeline_id": pipeline_id,
            "status": job_instance.get("status"),
            "start_time": job_instance.get("startTimeUtc"),
            "end_time": job_instance.get("endTimeUtc"),
            "failure_reason": job_instance.get("failureReason"),
            "root_activity_id": job_instance.get("rootActivityId"),
        },
        "runtime_activity_tracker": [
            {
                "activity_name": run.get("activityName"),
                "activity_type": run.get("activityType"),
                "status": run.get("status"),
                "start_time": run.get("activityRunStart"),
                "end_time": run.get("activityRunEnd"),
                "duration_ms": run.get("durationInMs"),
                "retry_attempt": run.get("retryAttempt"),
                "iteration_hash": run.get("iterationHash"),
                "error": run.get("error"),
            }
            for run in activity_runs
        ],
        "runtime_payload_viewer": {
            "job_instance": job_instance,
            "activity_runs": activity_runs,
        },
        "actual_source_to_target_mapping": lineage,
        "runtime_notebook_parameters": runtime_notebook_parameters,
        "runtime_source_discovery": runtime_source_discovery,
    }
    
    # Build dynamic reformatted_config for runtime intelligence
    reformatted_config = _build_runtime_reformatted_config(payload, client_name)
    if reformatted_config.get("activities"):
        logger.info("Runtime config generated dynamically")
    else:
        logger.warning("Using fallback config because runtime response is empty or missing activities")
    
    payload["reformatted_config"] = reformatted_config

    _persist_runtime_capture(
        client_name=client_name,
        job_instance_id=job_instance_id,
        workspace_id=workspace_id,
        pipeline_id=pipeline_id,
        execution_status=_status_bucket(str(job_instance.get("status") or "")),
        start_time=job_instance.get("startTimeUtc"),
        end_time=job_instance.get("endTimeUtc"),
        payload=payload,
    )

    if not activity_runs and pipeline_completed and job_instance.get("status") == "Completed":
        # Fail loudly after persistence to notify the user of the eventual consistency issue.
        raise HTTPException(
            status_code=408, 
            detail="Fabric returned completed pipeline status but no activity metadata became available after exhaustive polling. This is likely an eventual consistency delay in the Fabric Items API. Please wait a moment and try the scan again."
        )

    return payload


def _build_runtime_reformatted_config(payload: Dict[str, Any], client_name: str) -> Dict[str, Any]:
    runtime_metrics = payload.get("runtime_metrics") or {}
    runtime_tracker = payload.get("runtime_activity_tracker") or []
    runtime_source = payload.get("runtime_source_discovery") or {}
    source_conn = runtime_source.get("source_connection") or {}
    target_conn = runtime_source.get("target_connection") or {}
    exec_summary = payload.get("runtime_execution_summary") or {}

    activity_count = runtime_metrics.get("total_activities") or len(runtime_tracker)
    activity_names = [a.get("activity_name") for a in runtime_tracker if a.get("activity_name")]
    copy_activities = [a.get("activity_name") for a in runtime_tracker if str(a.get("activity_type") or "").lower() == "copy"]
    
    # linked_services -> Extract from multiple sources
    linked_services = []
    
    def extract_ls(obj):
        if not isinstance(obj, dict): return
        # Extract from runtime_values / activity_outputs structure
        # path: input.source.datasetSettings.linkedService.name
        ls_name = _safe_get(obj, ["input", "source", "datasetSettings", "linkedService", "name"])
        if ls_name: linked_services.append(ls_name)
        ls_name = _safe_get(obj, ["input", "sink", "datasetSettings", "linkedService", "name"])
        if ls_name: linked_services.append(ls_name)
        # Also check simple linkedServiceName
        ls_name = obj.get("linkedServiceName") or _safe_get(obj, ["input", "linkedServiceName"])
        if ls_name: linked_services.append(ls_name)

    # 1. runtime_values
    for val in payload.get("runtime_values", {}).values():
        extract_ls(val)
    # 2. activity_outputs
    for out in payload.get("activity_outputs", {}).values():
        extract_ls(out)
    # 3. runtime_payload_viewer.activity_runs
    for run in payload.get("runtime_payload_viewer", {}).get("activity_runs", []):
        extract_ls(run)
    
    linked_services = sorted(list(set(linked_services)))

    # file_types -> infer from extension
    file_types = []
    source_file = source_conn.get("file_name") or ""
    if "." in source_file:
        ext = source_file.split(".")[-1].lower()
        if ext in ["csv", "json", "parquet", "tsv", "txt"]:
            file_types.append(ext)
    
    if not file_types and source_conn.get("format"):
        file_types.append(str(source_conn["format"]).lower())
    
    # delimiter_config
    delimiter_config = {
        "column_delimiter": source_conn.get("delimiter", ","),
        "quote_char": source_conn.get("quote_char", "\""),
        "escape_char": source_conn.get("escape_char", "\\\\"),
        "header": source_conn.get("header_enabled", True)
    }

    return {
        "client_name": client_name,
        "source_type": source_conn.get("source_type", "DelimitedTextSource"),
        "discovery_mode": "FABRIC_RUNTIME",
        "source_path": source_conn.get("resolved_path") or source_conn.get("full_path") or f"fabric://{exec_summary.get('pipeline_name', 'Fabric')}",
        "pipeline_name": exec_summary.get("pipeline_name", "Fabric Runtime Pipeline"),
        
        "source": {
            "workspace_id": source_conn.get("workspace_id"),
            "artifact_id": source_conn.get("artifact_id")
        },
        
        "activity_count": activity_count,
        "activities": activity_names,
        "copy_activities": copy_activities,
        "linked_services": linked_services,
        "file_types": [ft.upper() for ft in file_types],
        
        "delimiter_config": delimiter_config,
        
        "targets": {
            "lakehouse": str(target_conn.get("storage_type") or "").lower() == "lakehouse",
            "warehouse": str(target_conn.get("storage_type") or "").lower() == "warehouse",
            "target_type": target_conn.get("target_type") or target_conn.get("sink_type") or "DelimitedTextSink",
            "connector_type": target_conn.get("connector_type") or "DelimitedTextSink",
            "storage_type": target_conn.get("storage_type"),
            "workspace_id": target_conn.get("workspace_id"),
            "artifact_id": target_conn.get("artifact_id"),
            "folder_path": target_conn.get("folder_path"),
            "file_name": target_conn.get("file_name"),
            "full_path": target_conn.get("full_path"),
            "resolved_path": target_conn.get("resolved_path")
        }
    }
