import logging
import os
import json
from typing import Optional, Dict, Any, List
from urllib import request as urlrequest

from engine.scanner.manager import scanner_manager
from core.credential_registry import put_aws_credentials

logger = logging.getLogger(__name__)

class DummySettings:
    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        creds = credentials or {}
        self.aws_access_key_id = creds.get("access_key") or creds.get("aws_access_key_id") or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = creds.get("secret_key") or creds.get("aws_secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_session_token = creds.get("session_token") or creds.get("aws_session_token") or os.getenv("AWS_SESSION_TOKEN")
        self.aws_region = creds.get("region") or os.getenv("AWS_REGION")
        self.aws_role_arn = creds.get("role_arn") or os.getenv("AWS_ROLE_ARN")
        self.azure_client_id = creds.get("client_id") or os.getenv("AZURE_CLIENT_ID")
        self.azure_client_secret = creds.get("client_secret") or os.getenv("AZURE_CLIENT_SECRET")
        self.azure_tenant_id = creds.get("tenant_id") or os.getenv("AZURE_TENANT_ID")
        self.azure_subscription_id = creds.get("subscription_id") or os.getenv("AZURE_SUBSCRIPTION_ID")
        self.azure_resource_group = creds.get("resource_group") or os.getenv("AZURE_RESOURCE_GROUP")
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")

def _normalize_target(target: Optional[str], providers: Optional[str]) -> str:
    raw = (target or providers or "aws").split(",")[0].strip().lower()
    if raw in {"amazon", "s3", "glue"}:
        return "aws"
    if raw in {"adls", "adf"}:
        return "azure"
    if raw in {"microsoft fabric", "msfabric"}:
        return "fabric"
    return raw if raw in {"aws", "azure", "fabric"} else "aws"


def _fallback_raw_assets(target: str) -> Dict[str, List[Any]]:
    if target == "azure":
        return {
            "raw_cloud_dump": [{
                "storage_accounts": [{
                    "id": "azure || demo-landing-adls",
                    "configuration": {
                        "Kind": "StorageV2",
                        "IsHnsEnabled": True,
                        "DataFormats": ["CSV", "JSON", "Parquet"],
                    },
                }],
                "datafactory": [{
                    "id": "azure || DEA-Ingestion-ADF",
                    "configuration": {"ProvisioningState": "Succeeded", "Activities": ["Copy", "Validation"]},
                }],
            }]
        }
    if target == "fabric":
        return {
            "raw_cloud_dump": [{
                "fabric_workspaces": [{
                    "id": "fabric || DEA-Analytics",
                    "configuration": {"Type": "Workspace"},
                }],
                "fabric_items": [
                    {"id": "fabric || Bronze-Lakehouse", "configuration": {"Type": "Lakehouse"}},
                    {"id": "fabric || DEA-Ingestion-Pipeline", "configuration": {"Type": "Pipeline", "DataFormats": ["CSV", "Parquet"]}},
                ],
            }]
        }
    return {
        "raw_cloud_dump": [{
            "s3": [{
                "id": "aws || dea-demo-landing",
                "configuration": {
                    "StorageClass": "Standard",
                    "DataFormats": ["CSV", "JSON", "Parquet"],
                    "IngestionTargets": ["s3://dea-demo-landing/raw"],
                },
            }],
            "glue": [{
                "id": "aws || dea-bronze-ingestion",
                "configuration": {"GlueVersion": "4.0", "CommandScript": "s3://scripts/dea_bronze.py"},
            }],
        }]
    }


def _flatten_assets(raw: Any) -> List[Dict[str, Any]]:
    assets: List[Dict[str, Any]] = []

    def visit(value: Any, group: str = "asset"):
        if isinstance(value, dict):
            if group in {"warnings", "errors", "_scan_meta", "framework", "source", "ingestion", "dq_rules"}:
                return
            if "id" in value or "configuration" in value:
                assets.append({
                    "type": group,
                    "name": str(value.get("id") or value.get("name") or group),
                    "configuration": value.get("configuration", value),
                })
                return
            for key, nested in value.items():
                visit(nested, key)
        elif isinstance(value, list):
            for item in value:
                visit(item, group)
        elif value:
            if group in {"warnings", "errors", "_scan_meta", "framework", "source", "ingestion", "dq_rules"}:
                return
            assets.append({"type": group, "name": str(value), "configuration": {}})

    visit(raw)
    return assets


def _collect_list(raw: Any, key_name: str) -> List[Any]:
    values: List[Any] = []

    def visit(value: Any):
        if isinstance(value, dict):
            for key, nested in value.items():
                if key == key_name and isinstance(nested, list):
                    values.extend(nested)
                else:
                    visit(nested)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(raw)
    return values


def _first_s3_source_path(assets: List[Dict[str, Any]]) -> str:
    for asset in assets:
        if asset.get("type") != "s3":
            continue
        config = asset.get("configuration", {}) or {}
        path = config.get("SuggestedPath")
        if path:
            return path
        bucket = config.get("BucketName")
        prefixes = config.get("Prefixes") or []
        if bucket:
            prefix = prefixes[0] if prefixes else ""
            return f"s3://{bucket}/{prefix}".rstrip("/")
    return ""


def _s3_bucket_from_path(path: str) -> str:
    if not path or not path.startswith("s3://"):
        return ""
    return path.split("s3://", 1)[1].split("/", 1)[0]


def _aws_region_from_assets(assets: List[Dict[str, Any]]) -> str:
    for asset in assets:
        if asset.get("type") == "s3":
            region = (asset.get("configuration") or {}).get("Region")
            if region:
                return region
    return "us-east-1"


def _build_config(client_name: str, target: str, file_types: List[str], delimiter_config: Dict[str, Any], assets: List[Dict[str, Any]], allow_demo_defaults: bool = True) -> Dict[str, Any]:
    source_type = "S3" if target == "aws" else "ADLS" if target == "azure" else "LOCAL"
    if target == "aws":
        source_path = _first_s3_source_path(assets)
        if not source_path and allow_demo_defaults:
            source_path = "s3://dea-demo-landing/raw"
    elif target == "azure":
        source_path = "az://demo-landing-adls/raw" if allow_demo_defaults else ""
    else:
        source_path = f"upload/{client_name}" if allow_demo_defaults else ""
    return {
        "client_name": client_name,
        "source_type": source_type,
        "source_path": source_path,
        "file_types": file_types,
        "delimiter_config": delimiter_config,
        "asset_count": len(assets),
    }


def _find_first_key(value: Any, wanted: str) -> Any:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key == wanted:
                return nested
            found = _find_first_key(nested, wanted)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_first_key(item, wanted)
            if found is not None:
                return found
    return None


def _fabric_item_name(item: Dict[str, Any]) -> str:
    raw = str(item.get("id") or item.get("name") or "FabricPipeline")
    return raw.split(" || ", 1)[1] if " || " in raw else raw


def _extract_fabric_definition_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    config = item.get("configuration") or {}
    definition = config.get("Definition") or config.get("definition") or {}
    if not isinstance(definition, dict):
        return {}

    # Fabric getDefinition usually returns decoded parts keyed by path, such as
    # pipeline-content.json. Keep this generic because tenant responses vary.
    candidates: List[Any] = []
    for key, value in definition.items():
        key_l = str(key).lower()
        if any(token in key_l for token in ["pipeline-content", "pipeline", "content"]):
            candidates.append(value)
    candidates.extend(definition.values())
    candidates.append(definition)

    for candidate in candidates:
        if isinstance(candidate, dict):
            if isinstance(candidate.get("properties"), dict):
                return candidate
            nested_props = _find_first_key(candidate, "properties")
            if isinstance(nested_props, dict):
                return {"properties": nested_props}
            activities = _find_first_key(candidate, "activities")
            if isinstance(activities, list):
                return {"properties": {"activities": activities}}
    return {}


def _collect_fabric_pipeline_items(raw_cloud_scan: Any) -> List[Dict[str, Any]]:
    pipelines: List[Dict[str, Any]] = []
    for item in _collect_list(raw_cloud_scan, "fabric_items"):
        if not isinstance(item, dict):
            continue
        config = item.get("configuration") or {}
        item_type = str(config.get("Type") or item.get("type") or "").lower()
        if item_type in {"pipeline", "datapipeline", "data pipeline"} or "pipeline" in item_type:
            pipelines.append(item)
    return pipelines


def _fabric_original_config_from_scan(raw_cloud_scan: Any) -> Dict[str, Any]:
    resources: List[Dict[str, Any]] = []
    for item in _collect_fabric_pipeline_items(raw_cloud_scan):
        definition = _extract_fabric_definition_from_item(item)
        if not definition:
            continue

        config = item.get("configuration") or {}
        pipeline_name = (
            definition.get("name")
            or config.get("Name")
            or config.get("DisplayName")
            or _fabric_item_name(item)
        )
        properties = definition.get("properties") if isinstance(definition.get("properties"), dict) else {}
        if not properties:
            properties = {k: v for k, v in definition.items() if k not in {"name", "type"}}

        resources.append({
            "name": pipeline_name,
            "type": "pipelines",
            "apiVersion": "2018-06-01",
            "properties": properties,
            "metadata": {
                "workspaceId": config.get("WorkspaceId"),
                "itemId": config.get("ItemId"),
                "fabricItemType": config.get("Type"),
            },
        })

    if not resources:
        return {}

    return {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {},
        "variables": {},
        "resources": resources,
    }


def _flatten_fabric_activities(activities: Any) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    if not isinstance(activities, list):
        return flat
    for activity in activities:
        if not isinstance(activity, dict):
            continue
        flat.append(activity)
        props = activity.get("typeProperties") or {}
        for nested_key in ["activities", "ifTrueActivities", "ifFalseActivities"]:
            flat.extend(_flatten_fabric_activities(props.get(nested_key)))
    return flat


def _fabric_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


def _fabric_activity_flow(resources: List[Dict[str, Any]]) -> List[str]:
    names: List[str] = []
    seen = set()
    for resource in resources:
        activities = _flatten_fabric_activities((resource.get("properties") or {}).get("activities"))
        for activity in activities:
            name = str(activity.get("name") or activity.get("type") or "").strip()
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    return names


def _fabric_rule_extract(client_name: str, original_config: Dict[str, Any], raw_cloud_scan: Any, delimiter_config: Dict[str, Any]) -> Dict[str, Any]:
    # Check if this is a runtime intelligence payload
    is_runtime = isinstance(original_config, dict) and "runtime_metrics" in original_config and "runtime_activity_tracker" in original_config
    
    if is_runtime:
        logger.info("Generated config populated from runtime intelligence")
        runtime_metrics = original_config.get("runtime_metrics") or {}
        runtime_tracker = original_config.get("runtime_activity_tracker") or []
        runtime_source = original_config.get("runtime_source_discovery") or {}
        source_conn = runtime_source.get("source_connection") or {}
        target_conn = runtime_source.get("target_connection") or {}
        exec_summary = original_config.get("runtime_execution_summary") or {}

        activity_count = runtime_metrics.get("total_activities") or len(runtime_tracker)
        activity_names = [a.get("activity_name") for a in runtime_tracker if a.get("activity_name")]
        copy_activities = [a.get("activity_name") for a in runtime_tracker if str(a.get("activity_type") or "").lower() == "copy"]
        notebooks = [a.get("activity_name") for a in runtime_tracker if "notebook" in str(a.get("activity_type") or "").lower()]
        
        # linked_services -> dataset linkedService names
        linked_services = []
        if source_conn.get("linked_service_name"):
            linked_services.append(source_conn["linked_service_name"])
        for a in runtime_tracker:
            ls = a.get("linked_service_name")
            if ls and ls not in linked_services:
                linked_services.append(ls)
        
        # file_types -> infer from source filename extension
        file_types = []
        source_file = source_conn.get("file_name") or ""
        if "." in source_file:
            ext = source_file.split(".")[-1].upper()
            if ext in ["CSV", "JSON", "PARQUET", "TSV", "TXT"]:
                file_types.append(ext)
        if not file_types and source_conn.get("format"):
            file_types.append(str(source_conn["format"]).upper())
        if not file_types:
            file_types = ["CSV"] # Safe default if nothing else found
            
        # targets.lakehouse -> true if target storage_type == "Lakehouse"
        has_lakehouse = str(target_conn.get("storage_type") or "").lower() == "lakehouse"
        has_warehouse = str(target_conn.get("storage_type") or "").lower() == "warehouse"
        
        # source_path -> resolved source path
        pipeline_name = exec_summary.get("pipeline_name", "Fabric Runtime Pipeline")
        source_path = source_conn.get("resolved_path") or source_conn.get("full_path") or f"fabric://{pipeline_name}"
        
        # delimiter_config update
        if source_conn.get("delimiter"):
            delimiter_config["column_delimiter"] = source_conn["delimiter"]
        if source_conn.get("header_enabled") is not None:
            delimiter_config["header"] = source_conn["header_enabled"]

        reformatted_config = {
            "client_name": client_name,
            "source_type": "FABRIC_RUNTIME",
            "source_path": source_path,
            "pipeline_name": pipeline_name,
            "activity_count": activity_count,
            "activities": activity_names,
            "copy_activities": copy_activities,
            "notebooks": notebooks,
            "linked_services": sorted(list(set(linked_services))),
            "file_types": sorted(list(set(file_types))),
            "delimiter_config": delimiter_config,
            "targets": {
                "lakehouse": has_lakehouse,
                "warehouse": has_warehouse,
            },
        }

        return {
            "source_systems": [], # Runtime discovery usually has its own source panel
            "ingestion_support": {
                "file_based": True,
                "api": any("web" in str(a.get("activity_type") or "").lower() for a in runtime_tracker),
                "database": any("lookup" in str(a.get("activity_type") or "").lower() or "sql" in str(a.get("activity_type") or "").lower() for a in runtime_tracker),
                "streaming": False,
                "batch": True,
            },
            "ingestion_types": ["file_based", "batch"],
            "file_types": reformatted_config["file_types"],
            "dq_rules": {
                "row_count_check": True,
                "failure_status_tracking": True,
                "schema_validation": True,
            },
            "pipeline_capabilities": {
                "copy_activity": len(copy_activities) > 0,
                "web_activity": any("web" in str(a.get("activity_type") or "").lower() for a in runtime_tracker),
                "lookup_activity": any("lookup" in str(a.get("activity_type") or "").lower() for a in runtime_tracker),
                "notebook_activity": len(notebooks) > 0,
                "lakehouse": has_lakehouse,
                "warehouse": has_warehouse,
                "bronze": True,
                "silver": True,
                "gold": True,
            },
            "interactive_flow": activity_names or ["Fabric Runtime Execution"],
            "reformatted_config": reformatted_config,
            "data_pipelines": [{
                "name": pipeline_name,
                "type": "DataPipeline",
                "platform": "Microsoft Fabric",
                "activities": activity_names,
                "activity_count": activity_count,
                "configuration": exec_summary,
            }],
            "ingestion_details": {
                "target": "fabric",
                "source_type": "FABRIC_RUNTIME",
                "source_path": source_path,
                "supported_modes": ["file_based", "batch"],
            },
            "llm_summary": f"Fabric runtime intelligence captured for pipeline '{pipeline_name}'. Actual metrics used for configuration.",
        }

    # ORIGINAL RULE-BASED EXTRACTION FOR STATIC SCANS
    resources = original_config.get("resources") if isinstance(original_config, dict) else []
    resources = resources if isinstance(resources, list) else []
    activities = []
    for resource in resources:
        activities.extend(_flatten_fabric_activities((resource.get("properties") or {}).get("activities")))

    all_text = json.dumps(original_config, default=str).lower()
    activity_types = [str(a.get("type") or "").lower() for a in activities]
    activity_names = [str(a.get("name") or a.get("type") or "") for a in activities]

    has_web = any("web" in t or "rest" in _fabric_text(a).lower() or "baseurl" in _fabric_text(a).lower() for t, a in zip(activity_types, activities))
    has_copy = any("copy" in t for t in activity_types)
    has_notebook = any("notebook" in t for t in activity_types)
    has_script = any("script" in t for t in activity_types)
    has_lookup = any("lookup" in t for t in activity_types)
    has_email = any("email" in t or "mail" in _fabric_text(a).lower() for t, a in zip(activity_types, activities))
    has_lakehouse = "lakehouse" in all_text
    has_warehouse = "warehouse" in all_text or "datawarehouse" in all_text or "sql" in all_text
    has_json = "json" in all_text

    linked_services = sorted(set(str(v) for v in _collect_list(original_config, "linkedServiceName") if v))
    source_systems = []
    if has_web:
        source_systems.append({"name": "Fabric pipeline REST API source", "type": "REST API", "configuration": {"detected_from": "Web activity"}})
    if has_lookup or has_warehouse:
        source_systems.append({"name": "Fabric DataWarehouse lookup", "type": "DataWarehouse", "configuration": {"detected_from": "Lookup/SQL activity"}})
    if has_lakehouse:
        source_systems.append({"name": "Fabric Lakehouse", "type": "Lakehouse", "configuration": {"detected_from": "Lakehouse dataset/activity"}})

    ingestion_support = {
        "file_based": has_lakehouse or has_json,
        "api": has_web,
        "database": has_lookup or has_warehouse,
        "streaming": "eventstream" in all_text or "stream" in all_text,
        "batch": True,
    }
    ingestion_types = [key for key, enabled in ingestion_support.items() if enabled]
    file_types = ["JSON"] if has_json else []
    if "csv" in all_text:
        file_types.append("CSV")
    if "parquet" in all_text:
        file_types.append("Parquet")
    file_types = list(dict.fromkeys(file_types))

    dq_rules = {
        "row_count_check": "row_count" in all_text or "rowcount" in all_text,
        "failure_status_tracking": "failed" in all_text or "failure" in all_text,
        "conditional_validation": any("ifcondition" in t or "if condition" in t for t in activity_types),
        "schema_validation": "schema" in all_text,
    }

    flow = _fabric_activity_flow(resources) or []
    pipeline_name = next((r.get("name") for r in resources if r.get("name")), "Unknown Fabric Pipeline")
    copy_activities = [a.get("name") for a in activities if "copy" in str(a.get("type") or "").lower()]
    notebooks = [a.get("activity_name") for a in activities if "notebook" in str(a.get("activity_type") or "").lower()]

    reformatted_config = {
        "client_name": client_name,
        "source_type": "FABRIC",
        "source_path": f"fabric://{pipeline_name}" if pipeline_name != "Unknown Fabric Pipeline" else "",
        "pipeline_name": pipeline_name,
        "activity_count": len(activities),
        "activities": activity_names,
        "copy_activities": [x for x in copy_activities if x],
        "notebooks": [x for x in notebooks if x],
        "linked_services": linked_services,
        "file_types": file_types,
        "delimiter_config": delimiter_config,
        "targets": {
            "lakehouse": has_lakehouse,
            "warehouse": has_warehouse,
        },
    }

    data_pipelines = [{
        "name": resource.get("name") or "Fabric Pipeline",
        "type": "DataPipeline",
        "platform": "Microsoft Fabric",
        "activities": activity_names,
        "activity_count": len(activity_names),
        "configuration": resource,
    } for resource in resources]

    return {
        "source_systems": source_systems,
        "ingestion_support": ingestion_support,
        "ingestion_types": ingestion_types,
        "file_types": file_types,
        "dq_rules": dq_rules,
        "pipeline_capabilities": {
            "copy_activity": has_copy,
            "web_activity": has_web,
            "lookup_activity": has_lookup,
            "notebook_activity": has_notebook,
            "script_activity": has_script,
            "email_notifications": has_email,
            "lakehouse": has_lakehouse,
            "warehouse": has_warehouse,
            "bronze": True,
            "silver": True,
            "gold": True,
        },
        "interactive_flow": flow,
        "reformatted_config": reformatted_config,
        "data_pipelines": data_pipelines,
        "ingestion_details": {
            "target": "fabric",
            "source_type": "FABRIC",
            "source_path": f"fabric://{pipeline_name}" if pipeline_name != "Unknown Fabric Pipeline" else "",
            "supported_modes": ingestion_types,
        },
        "llm_summary": "Fabric pipeline definition extracted from live Fabric API and normalized for DEA.",
    }


def _safe_json_loads(text: str) -> Optional[Dict[str, Any]]:
    import re
    match = re.search(r"(\{[\s\S]*\})", text or "")
    if match:
        text = match.group(1)
    text = (text or "").strip().replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(text)
    except Exception:
        return None


def _cloud_llm_extract(scan_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    azure_key = os.getenv("AZURE_OPENAI_API_KEY", "")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    if not azure_key or not azure_endpoint:
        logger.info("GPT extraction skipped because Azure OpenAI env vars are not configured.")
        return None

    prompt = {
        "task": "Normalize cloud framework discovery into DEA pipeline intelligence JSON.",
        "required_keys": [
            "source_systems",
            "ingestion_support",
            "ingestion_types",
            "file_types",
            "delimiter_config",
            "dq_rules",
            "pipeline_capabilities",
            "reformatted_config",
            "llm_summary",
        ],
        "scan_result": scan_result,
    }
    body = {
        "messages": [
            {
                "role": "system",
                "content": "You extract data engineering pipeline facts from cloud inventory. Respond with only valid JSON. Do not invent secrets or credentials.",
            },
            {"role": "user", "content": json.dumps(prompt, default=str)},
        ],
        "max_tokens": 3000,
        "temperature": 0,
    }
    headers = {"Content-Type": "application/json", "api-key": azure_key}

    try:
        req = urlrequest.Request(azure_endpoint, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
        with urlrequest.urlopen(req, timeout=90) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = _safe_json_loads(content)
        if not parsed:
            logger.warning("GPT extraction returned non-JSON content; using rule-based discovery response.")
        return parsed
    except Exception as exc:
        logger.warning(f"GPT extraction failed; using rule-based discovery response. Reason: {exc}")
        return None


def _merge_llm_overlay(base: Dict[str, Any], overlay: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not overlay:
        base["llm_summary"] = base.get("llm_summary") or "Rule-based extraction used; GPT extraction was unavailable."
        return base

    for key in [
        "source_systems",
        "ingestion_support",
        "ingestion_types",
        "file_types",
        "delimiter_config",
        "dq_rules",
        "pipeline_capabilities",
        "reformatted_config",
        "llm_summary",
    ]:
        value = overlay.get(key)
        if value not in (None, "", [], {}):
            base[key] = value
    base["llm_summary"] = base.get("llm_summary") or "GPT extraction completed."
    return base


async def analyze_pipeline_live(
    client_name: str,
    providers: Optional[str] = None,
    target: Optional[str] = None,
    auth_mode: Optional[str] = None,
    credentials: Optional[Dict[str, Any]] = None,
    use_cloud_llm: bool = True,
    llm_provider: str = "gpt",
    use_local_llm: bool = False,
    scan_mode: str = "live",
    authorization_token: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
):
    """
    Runs live cloud scan and extracts DEA capabilities.
    Fallback to local analysis if cloud scan fails or returns empty.
    """
    settings = DummySettings(credentials)
    target_key = _normalize_target(target, providers)
    provider_list = [p.strip().lower() for p in providers.split(",")] if providers else [target_key]
    has_request_credentials = bool(credentials) or bool(authorization_token)
    role_arn = (credentials or {}).get("role_arn") or getattr(settings, "aws_role_arn", None)
    resolved_auth_mode = auth_mode or ("sso" if target_key == "fabric" and authorization_token else "assumed_role" if target_key == "aws" and role_arn else "credentials" if has_request_credentials else "none")
    if target_key == "aws" and role_arn and has_request_credentials:
        resolved_auth_mode = "assumed_role"
    scan_status = "success"
    is_fallback = False
    warnings: List[str] = []
    errors: List[str] = []

    live_data = None
    try:
        live_data = await scanner_manager.scan_all(
            settings,
            providers=provider_list,
            azure_token=authorization_token,
            azure_token_fabric=authorization_token,
        )
    except Exception as e:
        logger.warning(f"Live scan failed: {e}. Using rule-based fallback.")
        scan_status = "partial"
        is_fallback = True
        errors.append(f"{target_key.upper()} scan failed: {e.__class__.__name__}")

    warnings = [str(w) for w in _collect_list(live_data, "warnings")]
    errors.extend([str(e) for e in _collect_list(live_data, "errors")])
    scan_meta = _collect_list(live_data, "_scan_meta")
    auth_failed = any(isinstance(m, dict) and m.get("auth_failed") for m in scan_meta)

    if target_key == "aws" and has_request_credentials and auth_failed:
        scan_status = "failed"
        is_fallback = False
        live_data = live_data or {"raw_cloud_dump": [{}]}
    elif (not live_data or not _flatten_assets(live_data)) and not has_request_credentials:
        live_data = _fallback_raw_assets(target_key)
        scan_status = "partial"
        is_fallback = True
    elif not live_data or not _flatten_assets(live_data):
        scan_status = "partial"
        is_fallback = False
        live_data = live_data or {"raw_cloud_dump": [{}]}
        if not warnings and not errors:
            warnings.append(f"No accessible {target_key.upper()} resources were discovered with the provided credentials.")
    elif warnings and scan_status == "success":
        scan_status = "partial"

    raw_cloud_scan = live_data or {}
    extracted_original_config: Dict[str, Any] = {}
    if target_key == "fabric":
        if payload:
            extracted_original_config = payload
            scan_status = "success"
            is_fallback = False
        else:
            extracted_original_config = _fabric_original_config_from_scan(raw_cloud_scan)
        
        if not extracted_original_config:
            warnings.append("Pipeline definition could not be extracted from Fabric API")
            if scan_status == "success":
                scan_status = "partial"

    extraction_source = extracted_original_config if target_key == "fabric" and extracted_original_config else raw_cloud_scan
    combined_text = json.dumps(extraction_source).lower() if extraction_source else ""
    
    # 1. Framework detection
    detected_framework = "Microsoft Fabric" if target_key == "fabric" else "AWS Glue" if target_key == "aws" else "Azure Data Factory"
    if "fabric" in combined_text or "lakehouse" in combined_text:
        detected_framework = "Microsoft Fabric"
    elif "databricks" in combined_text or "spark" in combined_text:
        detected_framework = "Databricks"
    elif "adf" in combined_text or "typeproperties" in combined_text:
        detected_framework = "Azure Data Factory"
    elif "glue" in combined_text or "lambda" in combined_text:
        detected_framework = "AWS Glue"

    # 2. Ingestion Support
    file_based = any(ext in combined_text for ext in ["csv", "json", "parquet", "blob", "s3", "adls", "delimitedtext", "storage"])
    api_based = any(ext in combined_text for ext in ["webactivity", "http", "rest", "graphql", "apigateway"])
    database = any(ext in combined_text for ext in ["sql", "jdbc", "datawarehouse", "table", "warehousetable", "database"])
    streaming = any(ext in combined_text for ext in ["kafka", "eventhub", "stream", "kinesis"])
    batch = any(ext in combined_text for ext in ["foreach", "until", "loop", "batch", "lambda"])
    
    if scan_status == "failed":
        file_based = api_based = database = streaming = batch = False
    elif not any([file_based, api_based, database]):
        file_based = True 
        batch = True

    # 3. File Types
    file_types = []
    if "csv" in combined_text or "delimited" in combined_text: file_types.append("CSV")
    if "json" in combined_text: file_types.append("JSON")
    if "parquet" in combined_text: file_types.append("Parquet")
    if "excel" in combined_text: file_types.append("Excel")
    if scan_status == "failed":
        file_types = []
    elif not file_types and (is_fallback or not has_request_credentials):
        file_types = ["CSV", "JSON", "Parquet"] # Provide common defaults
        
    # 4. Delimiter Config
    col_delim = ","
    quote_char = "\""
    escape_char = "\\\\"
    header = True
    
    # 5. DQ Rules
    schema_val = "schema" in combined_text or "validation" in combined_text or "datatype" in combined_text
    null_check = "null" in combined_text or "notnull" in combined_text
    dup_check = "duplicate" in combined_text or "distinct" in combined_text
    datatype = "datatype" in combined_text or "cast" in combined_text
    
    # 6. Loading Flow
    flow = ["Source", "Raw", "Bronze", "DQ Validation", "Silver", "Gold"]
    discovered_assets = _flatten_assets(raw_cloud_scan)
    data_pipelines = [
        asset for asset in discovered_assets
        if any(token in f"{asset.get('type')} {asset.get('name')} {asset.get('configuration')}".lower() for token in ["pipeline", "glue", "factory", "lambda", "function"])
    ]
    delimiter_config = {
        "column_delimiter": col_delim,
        "quote_char": quote_char,
        "escape_char": escape_char,
        "header": header,
    }
    ingestion_support = {
        "file_based": file_based,
        "api": api_based,
        "database": database,
        "streaming": streaming,
        "batch": batch,
    }
    ingestion_types = [k for k, v in ingestion_support.items() if v]
    source_systems = [
        {
            "name": asset.get("name"),
            "type": asset.get("type"),
            "configuration": asset.get("configuration", {}),
        }
        for asset in discovered_assets
        if any(token in f"{asset.get('type')} {asset.get('name')} {asset.get('configuration')}".lower() for token in ["s3", "storage", "api", "lakehouse", "blob", "adls"])
    ]
    dq_rules = {
        "schema_validation": schema_val or True,
        "null_check": null_check or True,
        "duplicate_check": dup_check,
        "datatype_check": datatype or True,
    }
    pipeline_capabilities = {
        "bronze": True,
        "silver": True,
        "gold": True,
        "cloud_llm_requested": bool(use_cloud_llm),
        "llm_provider": llm_provider,
        "scan_mode": scan_mode,
    }
    reformatted_config = _build_config(client_name, target_key, file_types, delimiter_config, discovered_assets, allow_demo_defaults=is_fallback or not has_request_credentials)
    ingestion_details = {
        "target": target_key,
        "source_type": reformatted_config["source_type"],
        "source_path": reformatted_config["source_path"],
        "supported_modes": [k for k, v in ingestion_support.items() if v],
    }

    # 7. Fabric-specific discovery promotion
    fabric_workspaces = []
    fabric_items = []
    if target_key == "fabric":
        # Extract from raw_cloud_dump lists
        for dump in raw_cloud_scan.get("raw_cloud_dump", []):
            if isinstance(dump, dict):
                fabric_workspaces.extend(dump.get("fabric_workspaces", []))
                fabric_items.extend(dump.get("fabric_items", []))
        
        # Log final structured results for debugging
        logger.info("FINAL SCAN RESULTS | Workspaces: %d | Items: %d", len(fabric_workspaces), len(fabric_items))

    result = {
        "framework": detected_framework,
        "auth_mode": resolved_auth_mode,
        "scan_status": scan_status,
        "is_fallback": is_fallback,
        "warnings": warnings,
        "errors": errors,
        "discovered_assets": discovered_assets,
        "data_pipelines": data_pipelines,
        "fabric_workspaces": fabric_workspaces,
        "fabric_items": fabric_items,
        "source_systems": source_systems,
        "ingestion_support": ingestion_support,
        "ingestion_types": ingestion_types,
        "file_types": file_types,
        "delimiter_config": delimiter_config,
        "dq_rules": dq_rules,
        "pipeline_capabilities": pipeline_capabilities,
        "interactive_flow": flow,
        "ingestion_details": ingestion_details,
        "original_config": extracted_original_config or {},
        "raw_cloud_scan": raw_cloud_scan,
        "raw_cloud_dump": raw_cloud_scan.get("raw_cloud_dump", []) if isinstance(raw_cloud_scan, dict) else [],
        "reformatted_config": reformatted_config,
        "llm_summary": "",
        "loading_flow": flow,
        "raw_analysis": {"scanned_cloud_assets": len(discovered_assets)}
    }

    if target_key == "fabric" and extracted_original_config:
        fabric_overlay = _fabric_rule_extract(client_name, extracted_original_config, raw_cloud_scan, delimiter_config)
        for key, value in fabric_overlay.items():
            if value not in (None, "", [], {}):
                if key == "pipeline_capabilities":
                    result[key] = {**result.get(key, {}), **value}
                else:
                    result[key] = value

    if use_cloud_llm and llm_provider == "gpt":
        llm_input = dict(result)
        if target_key == "fabric":
            llm_input["scan_result_for_extraction"] = {
                "original_config": extracted_original_config,
                "raw_cloud_scan": raw_cloud_scan,
            }
        result = _merge_llm_overlay(result, _cloud_llm_extract(llm_input))
    else:
        result["llm_summary"] = "GPT extraction not requested; rule-based extraction used."

    if target_key == "aws" and has_request_credentials and not is_fallback:
        real_source_path = reformatted_config.get("source_path") or ""
        result.setdefault("reformatted_config", {})
        result["reformatted_config"]["source_type"] = "S3"
        result["reformatted_config"]["source_path"] = real_source_path
        result.setdefault("ingestion_details", {})
        result["ingestion_details"]["source_type"] = "S3"
        result["ingestion_details"]["source_path"] = real_source_path
        bucket = _s3_bucket_from_path(real_source_path)
        if bucket:
            put_aws_credentials(client_name, bucket, credentials or {}, _aws_region_from_assets(discovered_assets))
            logger.info("Cached transient AWS scan credentials for client=%s bucket=%s", client_name, bucket)

    return result
