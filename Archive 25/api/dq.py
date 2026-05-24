from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from core.database import get_db
from models.master_config_authoritative import MasterConfigAuthoritative
from models.master_config import MasterConfig
from models.dq_schema_config import DQSchemaConfig, ExpectedDataType, DQRule, Severity
from core.settings import settings
from core.azure_storage import get_storage_client
import pandas as pd
from io import BytesIO
from datetime import datetime
from loguru import logger
from core.mcp_connector import get_mcp_connector
from fastapi import Query
import json
import os
from sqlalchemy.exc import IntegrityError

router = APIRouter(prefix="/dq", tags=["DQ & Schema Configuration"])

class DQColumnConfig(BaseModel):
    column_name: str = Field(..., min_length=1)
    expected_data_type: ExpectedDataType
    dq_rules: List[DQRule]
    rule_value: Optional[str] = None
    severity: Severity
    is_active: bool = True

class DQConfigureRequest(BaseModel):
    dataset_id: str
    columns: List[DQColumnConfig]

class SyncRequest(BaseModel):
    client_name: str
    source_type: Optional[str] = None


@router.post("/configure")
def configure_dq(request: DQConfigureRequest, db: Session = Depends(get_db)):
    mc = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == request.dataset_id).first()
    if not mc:
        logger.warning(f"Configure DQ: dataset_id {request.dataset_id} not found in DB yet. Proceeding in ad-hoc mode.")
        # We allow save even if not in DB, because user might Sync later or we might handle it in the flow.
        # This keeps Step 4 working for new/dynamic folder selections.
    db.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == request.dataset_id, DQSchemaConfig.is_active == True).update({"is_active": False, "updated_at": datetime.utcnow()})
    inserted = 0
    for col in request.columns:
        if not col.column_name:
            raise HTTPException(status_code=400, detail="column_name must be non-empty")
        for rule in col.dq_rules:
            obj = DQSchemaConfig(
                dataset_id=request.dataset_id,
                column_name=col.column_name,
                expected_data_type=col.expected_data_type,
                dq_rule=rule,
                rule_value=col.rule_value,
                severity=col.severity,
                is_active=col.is_active
            )
            db.add(obj)
            inserted += 1
    db.commit()
    return get_dq_config(request.dataset_id, db)

def _resolve_master_config_row(
    dataset_id: str,
    db: Session,
    client_name: Optional[str] = None,
) -> tuple[Optional[MasterConfigAuthoritative], Optional[MasterConfig]]:
    """Resolve authoritative/legacy rows by dataset_id or common display-name aliases."""
    from api.discovery import _is_temporary_artifact_name

    authoritative = (
        db.query(MasterConfigAuthoritative)
        .filter(MasterConfigAuthoritative.dataset_id == dataset_id)
        .first()
    )
    legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == dataset_id).first()

    def check_and_pivot(auth_row, leg_row):
        if not auth_row and not leg_row:
            return auth_row, leg_row
        
        src_type = (auth_row.source_type if auth_row else None) or (leg_row.source_type if leg_row else None)
        src_obj = (auth_row.source_object if auth_row else None) or (leg_row.source_object if leg_row else None)
        cli_name = (auth_row.client_name if auth_row else None) or (leg_row.client_name if leg_row else None) or client_name

        if (src_type == "FABRIC" or (src_obj and _is_temporary_artifact_name(src_obj))) and cli_name:
            # Query for another active non-FABRIC row for this client
            pivot_row = (
                db.query(MasterConfigAuthoritative)
                .filter(
                    MasterConfigAuthoritative.client_name == cli_name,
                    MasterConfigAuthoritative.source_type != "FABRIC",
                    MasterConfigAuthoritative.is_active == True,
                )
                .first()
            )
            if pivot_row:
                logger.info(f"DQ Resolver Dynamic Pivot: from Fabric/temporary row (dataset_id={dataset_id}) to non-FABRIC active row (dataset_id={pivot_row.dataset_id}) for client={cli_name}")
                pivot_legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == pivot_row.dataset_id).first()
                return pivot_row, pivot_legacy
        return auth_row, leg_row

    if authoritative or legacy:
        return check_and_pivot(authoritative, legacy)

    if not client_name and " " in str(dataset_id):
        # Display names like "Fabric Pipeline" won't match hashed ids; require client scope.
        return None, None

    filters = []
    if client_name:
        filters.append(MasterConfigAuthoritative.client_name == client_name)
    authoritative_candidates = db.query(MasterConfigAuthoritative).filter(*filters).all() if filters else []
    for row in authoritative_candidates:
        if (
            row.source_object == dataset_id
            or row.pipeline_id == dataset_id
            or (row.raw_layer_path and dataset_id in str(row.raw_layer_path))
        ):
            legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == row.dataset_id).first()
            return check_and_pivot(row, legacy)

    if client_name:
        legacy_candidates = (
            db.query(MasterConfig).filter(MasterConfig.client_name == client_name).all()
        )
        for row in legacy_candidates:
            if row.source_object == dataset_id or row.pipeline_id == dataset_id:
                authoritative = (
                    db.query(MasterConfigAuthoritative)
                    .filter(MasterConfigAuthoritative.dataset_id == row.dataset_id)
                    .first()
                )
                return check_and_pivot(authoritative, row)

    return None, None


def _columns_from_stored_schema(schema_payload) -> List[Dict]:
    if not schema_payload:
        return []
    if isinstance(schema_payload, list):
        raw_columns = schema_payload
    elif isinstance(schema_payload, dict):
        raw_columns = schema_payload.get("columns") or []
    else:
        return []

    cols = []
    for col in raw_columns:
        if isinstance(col, str):
            cols.append(
                {
                    "column_name": col,
                    "expected_data_type": "STRING",
                    "dq_rules": [],
                    "rule_value": None,
                    "severity": "ERROR",
                    "is_active": False,
                }
            )
            continue
        if not isinstance(col, dict):
            continue
        name = col.get("column_name") or col.get("name") or col.get("displayName")
        if not name:
            continue
        inferred = str(col.get("data_type") or col.get("inferred_type") or "STRING").upper()
        expected = "STRING"
        if "INT" in inferred:
            expected = "INTEGER"
        elif "FLOAT" in inferred or "DOUBLE" in inferred or "DECIMAL" in inferred:
            expected = "FLOAT"
        elif "DATE" in inferred or "TIME" in inferred:
            expected = "TIMESTAMP"
        cols.append(
            {
                "column_name": name,
                "expected_data_type": expected,
                "dq_rules": [],
                "rule_value": None,
                "severity": "ERROR",
                "is_active": False,
            }
        )
    return cols


@router.get("/config/{dataset_id}")
def get_dq_config(
    dataset_id: str,
    client_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    mc, legacy = _resolve_master_config_row(dataset_id, db, client_name=client_name)
    resolved_dataset_id = mc.dataset_id if mc else (legacy.dataset_id if legacy else dataset_id)

    rows = db.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == resolved_dataset_id).all()
    if not rows:
        if mc and getattr(mc, "schema", None):
            stored_cols = _columns_from_stored_schema(mc.schema)
            if stored_cols:
                return {"dataset_id": resolved_dataset_id, "columns": stored_cols}

        if not legacy:
            legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == resolved_dataset_id).first()
        if legacy:
            if getattr(legacy, "schema", None):
                stored_cols = _columns_from_stored_schema(legacy.schema)
                if stored_cols:
                    return {"dataset_id": resolved_dataset_id, "columns": stored_cols}
            validation_rules = legacy.validation_rules or {}
            if isinstance(validation_rules, dict):
                stored_cols = _columns_from_stored_schema(validation_rules.get("schema"))
                if stored_cols:
                    return {"dataset_id": resolved_dataset_id, "columns": stored_cols}

        try:
            discovery = get_dataset_columns(
                resolved_dataset_id,
                client_name=client_name or (mc.client_name if mc else None),
                db=db,
            )
            cols = []
            for c in discovery.get("columns", []):
                inferred = c.get("inferred_type", "STRING").lower()
                expected = "STRING"
                if "int" in inferred: expected = "INTEGER"
                elif "float" in inferred: expected = "FLOAT"
                elif "date" in inferred: expected = "TIMESTAMP" # or DATE
                
                cols.append({
                    "column_name": c.get("name"),
                    "expected_data_type": expected,
                    "dq_rules": [],
                    "rule_value": None,
                    "severity": "ERROR",
                    "is_active": False
                })
            return {"dataset_id": resolved_dataset_id, "columns": cols}
        except Exception as e:
            logger.warning(f"Auto-discovery fallback failed for {resolved_dataset_id}: {e}")
            return {"dataset_id": resolved_dataset_id, "columns": []}
    by_col: Dict[str, Dict] = {}
    for r in rows:
        name = r.column_name or "__dataset__"
        entry = by_col.get(name)
        if not entry:
            entry = {
                "column_name": name,
                "expected_data_type": (r.expected_data_type.value if r.expected_data_type else None),
                "dq_rules": [],
                "rule_value": None,
                "severity": (r.severity.value if r.severity else None),
                "is_active": False
            }
            by_col[name] = entry
        # Always prefer values from active rows over placeholder rows
        if r.expected_data_type:
            entry["expected_data_type"] = r.expected_data_type.value
        if r.severity:
            entry["severity"] = r.severity.value
        # Active rules contribute to dq_rules, rule_value and active flag
        if r.is_active and r.dq_rule:
            entry["dq_rules"].append(r.dq_rule.value)
            entry["is_active"] = True
            # Capture rule_value from active rows (may differ per rule)
            if r.rule_value is not None:
                entry["rule_value"] = r.rule_value
    return {"dataset_id": resolved_dataset_id, "columns": list(by_col.values())}

class DQSuggestRequest(BaseModel):
    dataset_id: str
    mode: str = Field(..., description="life_science or general")

def _enum_safe(name: str, enum_cls):
    if not name:
        return None
    try:
        return enum_cls[name]
    except KeyError:
        try:
            return enum_cls(name)
        except Exception:
            return None

@router.post("/suggest")
def suggest_dq(request: DQSuggestRequest, db: Session = Depends(get_db)):
    """
    AI-driven Data Quality suggestion engine.
    Resolution Tiers:
    0. Metadata Discovery (if not in MasterConfig DB)
    1. Local Registry (DQSchemaConfig)
    2. Cloud-Direct Native Headers (S3/ADLS via MCPSourceConnector)
    3. Azure Blob Fallback (Direct SDK)
    """
    from core.utils import generate_dataset_id
    from core.azure_storage import get_storage_client
    import pandas as pd
    from io import BytesIO
    
    canonical = "UNKNOWN"
    mc = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == request.dataset_id).first()
    
    # Tier 0: Fallback to Master Config CSV or APISource Scan if not in DB
    if not mc:
        logger.info(f"Dataset {request.dataset_id} not in DB; attempting fallback resolution")
        storage = get_storage_client()
        bucket = settings.AZURE_CONTAINER_NAME or "datalake"
        found_meta = None

        # A) Try Master Config CSVs in Azure Landing Zone
        try:
            resp = storage.list_objects_v2(Prefix="Master_Configuration/", Delimiter="/", Container=bucket)
            client_names = [p["Prefix"].split("/")[-2] for p in resp.get("CommonPrefixes", [])]
            for cn in client_names:
                key_path = f"Master_Configuration/{cn}/master_config.csv"
                try:
                    obj = storage.get_object(Key=key_path, Container=bucket)
                    df_mc = pd.read_csv(BytesIO(obj["Body"].read()))
                    row = df_mc[df_mc["dataset_id"] == request.dataset_id]
                    if not row.empty:
                        found_meta = row.iloc[0].to_dict()
                        found_meta["client_name"] = cn
                        break
                except: continue
            if found_meta: logger.info(f"Matched {request.dataset_id} via CSV fallback for client {found_meta['client_name']}")
        except: pass

        # B) Try Zero-Touch APISource Scan (Dynamic cloud discovery)
        if not found_meta:
            from models.api_source_config import APISourceConfig
            all_sources = db.query(APISourceConfig).all()
            for s in all_sources:
                 try:
                      scan_path = f"s3://{s.aws_bucket_name}/" if s.source_type == "S3" else (s.azure_container_name or "")
                      connector = get_mcp_connector(s.source_type)
                      data = connector.list_datasets(s.client_name, scan_path)
                      for d in data:
                           d_id = generate_dataset_id(s.client_name, s.source_type, d.file_path)
                           if d_id == request.dataset_id:
                                found_meta = {
                                    "dataset_id": d_id, "client_name": s.client_name, "source_type": s.source_type,
                                    "source_folder": scan_path, "source_object": d.file_name,
                                    "file_format": d.file_format, "raw_layer_path": d.file_path
                                }
                                break
                      if found_meta: break
                 except: continue
        
        if not found_meta:
             logger.error(f"Suggest DQ failed: dataset_id {request.dataset_id} not found in any registry or scan.")
             raise HTTPException(
                status_code=404, 
                detail=f"Dataset {request.dataset_id} not found. Ensure 'Sync Master Config' is successful."
             )
        
        from types import SimpleNamespace
        mc = SimpleNamespace(**found_meta)

    # Resolution Tiers for Column Discovery
    col_defs = []

    # Tier 1: Existing Rules in DB (Metadata Cache)
    cols = db.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == request.dataset_id).all()
    for r in cols:
        if r.column_name and r.column_name not in [c["column_name"] for c in col_defs]:
            col_defs.append({
                "column_name": r.column_name,
                "expected_data_type": r.expected_data_type.value if r.expected_data_type else "STRING"
            })

    # Tier 2: Cloud-Direct Native Header Resolution (S3/ADLS)
    if not col_defs:
        try:
            connector = get_mcp_connector(mc.source_type)
            if getattr(mc, 'raw_layer_path', None):
                canonical = mc.raw_layer_path.strip("/")
            else:
                source_path = getattr(mc, 'source_folder', '')
                if not (source_path.startswith("az://") or source_path.startswith("s3://")):
                    canonical = f"{source_path.strip('/')}/{mc.source_object}".strip("/")
                else:
                    canonical = f"{source_path.strip('/')}/{mc.source_object}"

            logger.info(f"Tier 2 Resolution: Reading {mc.source_type} at {canonical}")
            raw_content = connector.get_file_content(canonical, mc.client_name)
            df_sample = None
            try: df_sample = pd.read_csv(BytesIO(raw_content), nrows=50)
            except: 
                try: df_sample = pd.read_parquet(BytesIO(raw_content))
                except: df_sample = None
            
            if df_sample is not None and not df_sample.empty:
                for col_name in df_sample.columns:
                    col_defs.append({"column_name": str(col_name), "expected_data_type": "STRING"})
                logger.info(f"Tier 2 Success: Discovered {len(col_defs)} columns for {request.dataset_id}")
        except Exception as e:
            logger.warning(f"Tier 2 Metadata Resolution Failed: {e}")

    # Tier 3: Azure Blob SDK Fallback (Legacy/Last Resort)
    if not col_defs:
        try:
            from azure.storage.blob import BlobServiceClient
            from core.azure_storage import AzureStorageClient
            conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            if conn_str:
                blob_svc = BlobServiceClient.from_connection_string(conn_str)
                container = settings.AZURE_CONTAINER_NAME or "ag-de-agent"
                key_prefix = getattr(mc, 'raw_layer_path', None) or mc.source_folder
                possible_paths = [
                    f"{key_prefix.strip('/')}/{mc.source_object}", 
                    f"Raw/{mc.client_name}/{mc.source_object}",
                    f"landing/{mc.client_name}/{mc.source_object}"
                ]
                for bp in possible_paths:
                    try:
                        data = blob_svc.get_blob_client(container=container, blob=bp).download_blob().readall()
                        df_fb = pd.read_csv(BytesIO(data), nrows=5)
                        if df_fb is not None and not df_fb.empty:
                            for cname in df_fb.columns:
                                col_defs.append({"column_name": str(cname), "expected_data_type": "STRING"})
                            logger.info(f"Tier 3 Success: Discovered {len(col_defs)} columns from blob {bp}")
                            break
                    except: continue
        except: pass

    if not col_defs:
        diag = {
            "dataset_id": request.dataset_id,
            "mc_found": mc is not None,
            "source_type": getattr(mc, 'source_type', 'UNKNOWN'),
            "canonical_path": canonical
        }
        raise HTTPException(
            status_code=400, 
            detail=f"Column resolution failed. Diag: {diag}. Verify cloud credentials and file path."
        )

    # --- AI Rule Generation Block ---
    functions = [r.value for r in DQRule]
    domain_hint = "life_science" if request.mode.lower().startswith("life") else "general"

    system_prompt = (
        "You are an AI data quality assistant. Generate DQ rules in JSON format. "
        "The JSON MUST have a 'columns' list. NO other text or markdown."
    )
    col_text = "\n".join([f"- {c['column_name']}" for c in col_defs])
    user_prompt = f"""Generate rules for:
Domain: {domain_hint}
Rules: {", ".join(functions)}
Columns:
{col_text}

Respond with ONLY JSON."""

    try:
        import urllib.request
        anthropic_key  = os.getenv("ANTHROPIC_API_KEY", "")
        azure_key      = os.getenv("AZURE_OPENAI_API_KEY", "")
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "")
        use_azure = bool(azure_key and azure_endpoint and "openai.azure.com" in azure_endpoint)

        if use_azure:
            endpoint = azure_endpoint
            headers = {"Content-Type": "application/json", "api-key": azure_key}
            payload = {
                "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                "max_tokens": 2000, "temperature": 0
            }
        else:
            endpoint = "https://api.anthropic.com/v1/messages"
            headers = {"Content-Type": "application/json", "x-api-key": anthropic_key, "anthropic-version": "2023-06-01"}
            payload = {
                "model": "claude-3-5-sonnet-20241022", "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}], "max_tokens": 2000
            }

        req = urllib.request.Request(endpoint, data=json.dumps(payload).encode(), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            resp_data = json.loads(resp.read().decode())
        
        gen_text = resp_data["choices"][0]["message"]["content"] if use_azure else resp_data["content"][0]["text"]
        gen_text = gen_text.strip().replace("```json", "").replace("```", "").strip()
        suggestions = json.loads(gen_text)

        # Normalise AI response
        if "columns" not in suggestions:
            for v in suggestions.values():
                if isinstance(v, list) and len(v) > 0 and "column_name" in v[0]:
                    suggestions = {"columns": v}; break

        normalised = []
        for col in suggestions.get("columns", []):
            rules_raw = col.get("dq_rules") or []
            flat_rules = []
            for r in rules_raw:
                if isinstance(r, str): flat_rules.append(r.upper())
                elif isinstance(r, dict): 
                    rname = r.get("rule") or r.get("name")
                    if rname: flat_rules.append(str(rname).upper())
            
            normalised.append({
                "column_name": col.get("column_name"),
                "expected_data_type": col.get("expected_data_type") or "STRING",
                "dq_rules": flat_rules,
                "rule_value": col.get("rule_value"),
                "severity": col.get("severity") or "ERROR"
            })
        suggestions = {"columns": normalised}
        logger.info(f"AI Suggestions generated: {len(normalised)} columns.")

    except Exception as e:
        logger.error(f"AI generation failed for {request.dataset_id}: {e}")
        # Static Fallback: NOT_NULL for all columns
        fallback_cols = []
        for c in col_defs:
            fallback_cols.append({
                "column_name": c["column_name"], "expected_data_type": "STRING",
                "dq_rules": ["NOT_NULL"], "rule_value": None, "severity": "ERROR"
            })
        suggestions = {"columns": fallback_cols}
        logger.info("Using static fallback DQ rules.")


    # Persist suggestions — insert as ACTIVE so they take effect immediately
    inserted = 0
    col_list = suggestions.get("columns", [])
    logger.info(f"AI returned {len(col_list)} column suggestions for dataset {request.dataset_id}")

    if not col_list:
        logger.warning(f"AI returned empty columns list. Raw response: {gen_text[:500]}")
        return {"status": "SUCCESS", "dataset_id": request.dataset_id, "inserted": 0, "kept_inactive": True,
                "warning": "AI returned no columns. Check that master config is synced and dataset has columns."}

    try:
        # Wipe ALL existing DQ rows for this dataset first (both active and inactive)
        # so we start fresh with AI suggestions
        db.query(DQSchemaConfig).filter(
            DQSchemaConfig.dataset_id == request.dataset_id
        ).delete()
        db.flush()

        for col in col_list:
            name = col.get("column_name")
            if not name:
                continue
            exp = _enum_safe(str(col.get("expected_data_type") or "").upper(), ExpectedDataType)
            sev = _enum_safe(str(col.get("severity") or "").upper(), Severity) or Severity.ERROR
            rules = col.get("dq_rules") or []
            rvalue = col.get("rule_value")

            for rule_name in rules:
                rule = _enum_safe(str(rule_name).upper(), DQRule)
                if not rule:
                    logger.warning(f"Unknown DQ rule '{rule_name}' for column '{name}' — skipped")
                    continue
                obj = DQSchemaConfig(
                    dataset_id=request.dataset_id,
                    column_name=name,
                    expected_data_type=exp,
                    dq_rule=rule,
                    rule_value=(str(rvalue) if rvalue is not None else None),
                    severity=sev,
                    is_active=True   # ← activate immediately so GET /dq/config shows them
                )
                db.add(obj)
                inserted += 1

        db.commit()
        logger.info(f"Inserted {inserted} DQ rules for dataset {request.dataset_id}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to persist suggestions: {e}")

    return get_dq_config(request.dataset_id, db)

def _normalize_is_active(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lower = value.lower().strip()
        if lower in {"true", "1", "yes", "y"}:
            return True
        if lower in {"false", "0", "no", "n", ""}:
            return False
    if isinstance(value, int):
        return bool(value)
    return False

def _is_non_empty_str(value) -> bool:
    return isinstance(value, str) and value.strip() != ""

@router.post("/sync_master_config")
def sync_master_config(request: SyncRequest, db: Session = Depends(get_db)):
    storage = get_storage_client()
    bucket = settings.AZURE_CONTAINER_NAME or "datalake"
    key = f"Master_Configuration/{request.client_name}/master_config.csv"

    try:
        obj = storage.get_object(Key=key, Container=bucket)
    except Exception as e:
        logger.info(f"Master config blob not found for {key} (new client {request.client_name}). Attempting automated discovery...")
        from models.api_source_config import APISourceConfig
        from core.mcp_connector import get_mcp_connector
        from core.utils import generate_dataset_id
        
        sources = db.query(APISourceConfig).filter(APISourceConfig.client_name == request.client_name).all()
        
        # Check for Fabric Runtime rows in DB as a backup discovery source
        fabric_runtime_rows = db.query(MasterConfigAuthoritative).filter(
            MasterConfigAuthoritative.client_name == request.client_name,
            MasterConfigAuthoritative.source_type.in_(["FABRIC", "FABRIC_RUNTIME"])
        ).all()

        if not sources and not fabric_runtime_rows:
            return {
                "status": "no_source_found",
                "message": f"No Master Config, Fabric Runtime, or registered Source Config found for {request.client_name}. Register a source first.",
                "synced": 0,
                "created": 0
            }
        
        all_discovered = []
        
        # Add Fabric Runtime rows first
        for fr in fabric_runtime_rows:
            all_discovered.append({
                "dataset_id": fr.dataset_id,
                "pipeline_id": fr.pipeline_id,
                "client_name": fr.client_name,
                "source_type": fr.source_type,
                "source_folder": fr.source_folder,
                "source_object": fr.source_object,
                "file_format": fr.file_format,
                "raw_layer_path": fr.raw_layer_path,
                "target_layer_bronze": fr.target_layer_bronze,
                "target_layer_silver": fr.target_layer_silver,
                "is_active": fr.is_active,
                "load_type": fr.load_type or "full",
                "upsert_key": fr.upsert_key or "",
                "watermark_column": fr.watermark_column or "",
                "partition_column": fr.partition_column or ""
            })
        
        for s in sources:
            try:
                # If it's a bucket, the folder_path we use for discovery is the bucket name or a root-level Scan
                # For S3, folder_path starts with s3://
                scan_path = f"s3://{s.aws_bucket_name}/" if s.source_type == "S3" else (s.azure_container_name or "")
                connector = get_mcp_connector(s.source_type)
                datasets = connector.list_datasets(request.client_name, scan_path)
                for d in datasets:
                    d_id = generate_dataset_id(request.client_name, s.source_type, d.file_path)
                    all_discovered.append({
                        "dataset_id": d_id,
                        "pipeline_id": request.client_name.upper(),
                        "client_name": request.client_name,
                        "source_type": s.source_type,
                        "source_folder": scan_path,
                        "source_object": d.file_name,
                        "file_format": d.file_format,
                        "raw_layer_path": d.file_path,
                        "target_layer_bronze": f"Bronze/{request.client_name}/{d.file_name.rsplit('.', 1)[0] if '.' in d.file_name else d.file_name}",
                        "target_layer_silver": f"Silver/{request.client_name}/{d.file_name.rsplit('.', 1)[0] if '.' in d.file_name else d.file_name}",
                        "is_active": True,
                        "load_type": "full",
                        "upsert_key": "",
                        "watermark_column": "",
                        "partition_column": ""
                    })
            except Exception as se:
                logger.warning(f"Auto-discovery failed for source {s.source_name}: {se}")
        
        if not all_discovered:
            return {
                "status": "no_datasets_found",
                "message": f"No datasets discovered in any registered sources for {request.client_name}.",
                "synced": 0,
                "created": 0
            }
            
        df = pd.DataFrame(all_discovered)
        csv_bytes = df.to_csv(index=False, encoding="utf-8").encode("utf-8")
        storage.put_object(Container=bucket, Key=key, Body=csv_bytes, ContentType="text/csv")
        logger.info(f"Created initial Master Config for {request.client_name} at az://{bucket}/{key}")
        # Now proceed with the sync using this new 'df'


    
    if 'df' not in locals():
        try:
            df = pd.read_csv(BytesIO(obj["Body"].read()))
        except Exception as e:
            logger.error(f"CSV parse failed for {key}: {e}")
            raise HTTPException(status_code=400, detail=f"CSV parse failed: {e}")
    
    required_cols = [
        "dataset_id", "pipeline_id", "client_name", "source_type",
        "source_folder", "source_object", "file_format",
        "target_layer_bronze", "target_layer_silver"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")
    
    inserts = 0
    updates = 0
    skips = 0
    
    for _, row in df.iterrows():
        dsid = str(row.get("dataset_id")).strip() if pd.notna(row.get("dataset_id")) else ""
        if not dsid:
            skips += 1
            continue
        
        system_fields = {
            "pipeline_id": str(row.get("pipeline_id")).strip() if pd.notna(row.get("pipeline_id")) else None,
            "client_name": str(row.get("client_name")).strip() if pd.notna(row.get("client_name")) else None,
            "source_type": str(row.get("source_type")).strip() if pd.notna(row.get("source_type")) else None,
            "source_folder": str(row.get("source_folder")).strip() if pd.notna(row.get("source_folder")) else None,
            "source_object": str(row.get("source_object")).strip() if pd.notna(row.get("source_object")) else None,
            "file_format": str(row.get("file_format")).strip() if pd.notna(row.get("file_format")) else None,
            "raw_layer_path": str(row.get("raw_layer_path")).strip() if pd.notna(row.get("raw_layer_path")) else None,
            "target_layer_bronze": str(row.get("target_layer_bronze")).strip() if pd.notna(row.get("target_layer_bronze")) else None,
            "target_layer_silver": str(row.get("target_layer_silver")).strip() if pd.notna(row.get("target_layer_silver")) else None,
            "last_seen_batch": str(row.get("batch_id")).strip() if pd.notna(row.get("batch_id")) and "batch_id" in df.columns else None,
            "staging_table": str(row.get("staging_table")).strip() if pd.notna(row.get("staging_table")) else None,
        }

        csv_load_type = str(row.get("load_type")).strip() if pd.notna(row.get("load_type")) else ""
        csv_upsert_key = str(row.get("upsert_key")).strip() if pd.notna(row.get("upsert_key")) else ""
        csv_watermark = str(row.get("watermark_column")).strip() if pd.notna(row.get("watermark_column")) else ""
        csv_partition = str(row.get("partition_column")).strip() if pd.notna(row.get("partition_column")) else ""
        csv_is_active_raw = row.get("is_active")
        csv_is_active = _normalize_is_active(csv_is_active_raw) if pd.notna(csv_is_active_raw) else None

        existing = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == dsid).first()
        
        if existing:
            for k, v in system_fields.items():
                setattr(existing, k, v)
            
            if _is_non_empty_str(csv_load_type):
                existing.load_type = csv_load_type
            if _is_non_empty_str(csv_upsert_key):
                existing.upsert_key = csv_upsert_key
            if _is_non_empty_str(csv_watermark):
                existing.watermark_column = csv_watermark
            if _is_non_empty_str(csv_partition):
                existing.partition_column = csv_partition
            if csv_is_active is not None:
                existing.is_active = csv_is_active
            
            existing.updated_at = datetime.utcnow()
            updates += 1
        else:
            mc = MasterConfigAuthoritative(
                dataset_id=dsid,
                **system_fields,
                load_type=csv_load_type if _is_non_empty_str(csv_load_type) else None,
                upsert_key=csv_upsert_key if _is_non_empty_str(csv_upsert_key) else None,
                watermark_column=csv_watermark if _is_non_empty_str(csv_watermark) else None,
                partition_column=csv_partition if _is_non_empty_str(csv_partition) else None,
                is_active=csv_is_active if csv_is_active is not None else False,
                created_at=datetime.utcnow()
            )
            # Use merge instead of add to handle in-session duplicates gracefully (though they shouldn't exist here)
            db.merge(mc)
            inserts += 1
    
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        logger.warning(f"Concurrent sync conflict (IntegrityError), data likely already synced: {e}")
        return {"status": "SUCCESS", "client_name": request.client_name, "message": "Conflict detected but data persisted."}
    except Exception as e:
        db.rollback()
        logger.error(f"DB commit failed during sync: {e}")
        raise HTTPException(status_code=500, detail=f"DB commit failed: {e}")
    
    # Hard Sync: Collect all dataset_ids from CSV to deactivate stale DB records
    active_dsids = set(df["dataset_id"].astype(str).str.strip().tolist())
    
    # Deactivate records in DB that are not in CSV for this client
    db.query(MasterConfigAuthoritative).filter(
        MasterConfigAuthoritative.client_name == request.client_name,
        ~MasterConfigAuthoritative.dataset_id.in_(list(active_dsids))
    ).update({"is_active": False, "updated_at": datetime.utcnow()}, synchronize_session=False)
    
    db.commit()
    
    logger.info(f"Sync completed for {request.client_name}: inserts={inserts}, updates={updates}, skips={skips}")
    return {"status": "SUCCESS", "client_name": request.client_name, "inserts": inserts, "updates": updates, "skips": skips}

@router.get("/columns/{dataset_id}")
def get_dataset_columns(dataset_id: str, client_name: Optional[str] = None, db: Session = Depends(get_db)):
    mc, legacy_row = _resolve_master_config_row(dataset_id, db, client_name=client_name)
    resolved_dataset_id = mc.dataset_id if mc else (legacy_row.dataset_id if legacy_row else dataset_id)
    if not mc and not client_name:
        raise HTTPException(status_code=404, detail="dataset_id not found; run /dq/sync_master_config or provide client_name")

    resolved_client = mc.client_name if mc else client_name
    source_type = mc.source_type if mc else None
    source_folder = mc.source_folder if mc else None
    source_object = mc.source_object if mc else None

    if not mc:
        storage = get_storage_client()
        bucket = settings.AZURE_CONTAINER_NAME or "datalake"
        clean_client = resolved_client.strip().replace(" ", "_")
        key = f"Master_Configuration/{clean_client}/master_config.csv"
        try:
            obj = storage.get_object(Key=key, Container=bucket)
            df = pd.read_csv(BytesIO(obj["Body"].read()))
        except Exception as e:
            logger.error(f"Read failed for {key}: {e}")
            raise HTTPException(status_code=404, detail=f"Unable to read master config for client {resolved_client}: {e}")
        if "dataset_id" not in df.columns:
            raise HTTPException(status_code=400, detail="Missing dataset_id in master config CSV")
        row = df[df["dataset_id"] == dataset_id]
        if row.empty:
            raise HTTPException(status_code=404, detail="dataset_id not present in client master config")
        source_type = str(row.iloc[0].get("source_type")) if pd.notna(row.iloc[0].get("source_type")) else None
        source_folder = str(row.iloc[0].get("source_folder")) if pd.notna(row.iloc[0].get("source_folder")) else None
        source_object = str(row.iloc[0].get("source_object")) if pd.notna(row.iloc[0].get("source_object")) else None

    # If schema was previously persisted in authoritative master config, return it directly
    if mc and getattr(mc, "schema", None):
        try:
            cols = []
            for col in mc.schema.get("columns", []) if isinstance(mc.schema, dict) else (mc.schema or []):
                # Normalize to expected output for DQ
                cols.append({"name": col.get("column_name") or col.get("name"), "inferred_type": col.get("data_type") or col.get("inferred_type")})
            return {"dataset_id": resolved_dataset_id, "client_name": resolved_client, "columns": cols}
        except Exception:
            logger.warning("Failed returning stored schema from MasterConfigAuthoritative; falling back to live inference")

    if not (resolved_client and source_type and source_folder and source_object):
        raise HTTPException(status_code=400, detail="Insufficient metadata to resolve dataset columns")

    raw_layer_path = getattr(mc, "raw_layer_path", None) if mc else (str(row.iloc[0].get("raw_layer_path")) if pd.notna(row.iloc[0].get("raw_layer_path")) else None)

    from api.orchestrate import _canonical_source_type
    can_src = _canonical_source_type(source_type)

    if raw_layer_path and str(raw_layer_path).startswith("s3://"):
        can_src = "S3"
        canonical = str(raw_layer_path).strip("/")
    elif raw_layer_path and str(raw_layer_path).startswith("az://"):
        can_src = "ADLS"
        canonical = str(raw_layer_path).strip("/")
    elif source_folder and str(source_folder).startswith("s3://"):
        can_src = "S3"
        canonical = f"{str(source_folder).rstrip('/')}/{source_object}".strip("/")
    elif source_folder and str(source_folder).startswith("az://"):
        can_src = "ADLS"
        canonical = f"{str(source_folder).rstrip('/')}/{source_object}".strip("/")
    elif can_src == "FABRIC":
        canonical = (
            getattr(mc, "staging_table", None)
            or (str(row.iloc[0].get("staging_table")) if "row" in locals() and pd.notna(row.iloc[0].get("staging_table")) else None)
            or source_object
        )
    elif raw_layer_path:
        canonical = str(raw_layer_path).strip("/")
    else:
        canonical = f"{(source_folder or '').strip('/')}/{source_object}".strip("/")

    try:
        connector = get_mcp_connector(can_src or source_type)
        content = connector.get_file_content(canonical, resolved_client)
    except Exception as e:
        logger.error(f"MCP content read failed for {canonical}: {e}")
        raise HTTPException(status_code=404, detail=f"Unable to read dataset content: {e}")

    try:
        buf = BytesIO(content)
        df_sample = pd.read_csv(buf, nrows=50)
        cols = []
        preview_rows = []
        for c in df_sample.columns:
            dtype = str(df_sample[c].dtype)
            if "int" in dtype:
                inferred = "integer"
            elif "float" in dtype:
                inferred = "float"
            elif "datetime" in dtype:
                inferred = "date"
            else:
                inferred = "string"
            cols.append({"name": str(c), "inferred_type": inferred})
        # capture up to 5 preview rows
        preview_rows = df_sample.head(5).to_dict(orient="records") if not df_sample.empty else []

        # Persist schema to MasterConfigAuthoritative if available
        try:
            if mc:
                schema_payload = {"columns": [{"name": c["name"], "data_type": c["inferred_type"], "nullable": True} for c in cols], "preview_rows": preview_rows}
                db_mc = db.query(MasterConfigAuthoritative).filter(
                    MasterConfigAuthoritative.dataset_id == resolved_dataset_id
                ).first()
                if db_mc:
                    db_mc.schema = schema_payload
                    db.commit()
        except Exception as persist_exc:
            logger.warning(f"Failed to persist inferred schema for {resolved_dataset_id}: {persist_exc}")

        return {"dataset_id": resolved_dataset_id, "client_name": resolved_client, "columns": cols}
    except Exception:
        try:
            buf = BytesIO(content)
            df_parq = pd.read_parquet(buf)
            cols = [{"name": str(c), "inferred_type": "unknown"} for c in df_parq.columns]
            preview_rows = []
            try:
                preview_rows = df_parq.head(5).to_dict(orient="records")
            except Exception:
                preview_rows = []

            try:
                if mc:
                    schema_payload = {"columns": [{"name": c["name"], "data_type": "unknown", "nullable": True} for c in cols], "preview_rows": preview_rows}
                    db_mc = db.query(MasterConfigAuthoritative).filter(
                        MasterConfigAuthoritative.dataset_id == resolved_dataset_id
                    ).first()
                    if db_mc:
                        db_mc.schema = schema_payload
                        db.commit()
            except Exception as persist_exc:
                logger.warning(f"Failed to persist inferred parquet schema for {resolved_dataset_id}: {persist_exc}")

            return {"dataset_id": resolved_dataset_id, "client_name": resolved_client, "columns": cols}
        except Exception as e2:
            logger.error(f"Schema inference failed: {e2}")
            raise HTTPException(status_code=400, detail=f"Unable to infer columns: {e2}")