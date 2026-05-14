import os
from typing import Dict, List
from langgraph.graph import StateGraph, END

# Set up Langfuse tracing
os.environ["LANGFUSE_SECRET_KEY"] = "sk-lf-acfa1a1f-a0a1-487d-8a3d-aa0b3b7fe488"
os.environ["LANGFUSE_PUBLIC_KEY"] = "pk-lf-cbd909ac-2fcd-4f52-9772-bd467e9e0410"
os.environ["LANGFUSE_BASE_URL"] = "https://cloud.langfuse.com"

try:
    from langfuse import observe
except ImportError:
    def observe(*args, **kwargs):
        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return args[0]

        def _decorator(fn):
            return fn

        return _decorator
from core.mcp_connector import get_mcp_connector
from core.notifications import NotificationService
from core.master_config_manager import MasterConfigManager
from core.pipeline_service import PipelineService
from core.settings import settings
from core.azure_storage import get_storage_client
from models.dq_schema_config import DQSchemaConfig, ExpectedDataType, DQRule, Severity
from core.database import SessionLocal
from core.utils import generate_dataset_id
import hashlib
import io
import pandas as pd
from zoneinfo import ZoneInfo
from loguru import logger
import time
import json
import psycopg2
from psycopg2.extras import RealDictCursor

class MockDataset:
    def __init__(self, d, client, src):
        if isinstance(d, dict):
            self.file_name = d.get("dataset_name") or d.get("source_object")
            folder = d.get("source_folder") or ""
            if folder and src == "API":
                # Handle comma-separated legacy paths (e.g., users,posts)
                if "," in folder:
                    # Isolate the component that likely corresponds to this file
                    _parts = [p.strip() for p in folder.split(",")]
                    # If file_name starts with part (e.g. users.csv starts with users), use part
                    _match = next((p for p in _parts if str(self.file_name).startswith(p)), _parts[0])
                    self.file_path = f"{_match.strip('/')}/{self.file_name}"
                elif folder not in (self.file_name, ""):
                    self.file_path = f"{folder.strip('/')}/{self.file_name}"
                else:
                    self.file_path = d.get("file_path") or self.file_name
            else:
                self.file_path = d.get("file_path") or d.get("source_object") or d.get("dataset_name")
            self.file_format = d.get("file_format") or (str(self.file_name).split(".")[-1].upper() if "." in str(self.file_name) else "UNKNOWN")
        else:
            self.file_name = d.source_object
            folder = d.source_folder or ""
            if folder and src == "API":
                if "," in folder:
                    _parts = [p.strip() for p in folder.split(",")]
                    _match = next((p for p in _parts if d.source_object.startswith(p)), _parts[0])
                    self.file_path = f"{_match.strip('/')}/{d.source_object}"
                elif folder not in (d.source_object, ""):
                    self.file_path = f"{folder.strip('/')}/{d.source_object}"
                else:
                    self.file_path = d.source_object
            else:
                self.file_path = d.source_object
            self.file_format = d.file_format or (d.source_object.split(".")[-1].upper() if "." in d.source_object else "UNKNOWN")
        self.file_size = 0
        self.client_name = client
        self.source_type = src

@observe()
def _discover(state: Dict) -> Dict:
    src = state.get("source_type", "API")
    client = state.get("client_name")
    folder = state.get("folder_path") or ""
    progress: Dict[str, Dict[str, Dict]] = state.get("progress", {})
    
    # Normalise folder path for DB searching
    search_targets = []
    if folder:
        for t in folder.split(","):
            t = t.strip()
            if t.startswith("az://") or t.startswith("s3://"):
                _rest = t.split("://", 1)[1]
                _parts = _rest.split("/", 2)
                if len(_parts) > 2:
                    t = _parts[2] # actual path
                else:
                    t = "" # root
            search_targets.append(t)

    from models.master_config_authoritative import MasterConfigAuthoritative
    sess = SessionLocal()
    db_ds = []
    if client:
        # 1. First search for precise matches
        query = sess.query(MasterConfigAuthoritative).filter(
            MasterConfigAuthoritative.client_name == client,
            MasterConfigAuthoritative.source_type == src,
            MasterConfigAuthoritative.is_active == True
        )
        if search_targets:
            from sqlalchemy import or_
            query = query.filter(or_(
                MasterConfigAuthoritative.dataset_id.in_(search_targets),
                MasterConfigAuthoritative.source_object.in_(search_targets),
                MasterConfigAuthoritative.source_folder.in_(search_targets)
            ))
        db_ds = query.all()
    sess.close()

    if db_ds:
        logger.info(f"Discovered {len(db_ds)} datasets from DB for client {client}")
        ds = [MockDataset(d, client, src) for d in db_ds]
        for d in db_ds:
            dsid = d.dataset_id
            if dsid not in progress:
                progress[dsid] = {
                    "dataset_id": dsid,
                    "client_name": client,
                    "dataset_name": d.source_object,
                    "file_format": d.file_format or (d.source_object.split(".")[-1].upper() if "." in d.source_object else "UNKNOWN"),
                    "source_folder": d.source_folder,
                    "source_object": d.source_object,
                    "raw_layer_path": d.raw_layer_path,
                    "steps": {
                        "Client Source": {"status": "PASSED", "detail": f"Source: {d.source_type} storage"},
                        "Validation": {"status": "PENDING"},
                        "Raw Layer": {"status": "PENDING"},
                        "Master Configuration": {"status": "PENDING"},
                        "DQ Configuration": {"status": "PENDING"},
                        "Bronze": {"status": "PENDING"},
                        "Silver": {"status": "PENDING"},
                        "Gold": {"status": "PENDING"}
                    }
                }
        return {**state, "datasets": ds, "progress": progress}

    # Ad-hoc discovery for Cloud/API sources if not explicitly using LOCAL upload registry
    if src != "LOCAL":
        connector = get_mcp_connector(src)
        ds = connector.list_datasets(client, folder)
        logger.info(f"Discovered {len(ds)} datasets via connector for client {client} from {src}")
        for item in ds:
            dsid = generate_dataset_id(client, src, item.file_path)
            if dsid not in progress:
                progress[dsid] = {
                    "dataset_id": dsid,
                    "client_name": client,
                    "dataset_name": item.file_name,
                    "file_format": getattr(item, 'file_format', (item.file_name.split(".")[-1].upper() if "." in item.file_name else "UNKNOWN")),
                    "source_folder": folder,
                    "source_object": item.file_name,
                    "steps": {
                        "Client Source": {"status": "PASSED", "detail": f"Format: {getattr(item, 'file_format', 'UNKNOWN')} | Size: {getattr(item, 'file_size', 0)} B"},
                        "Validation": {"status": "PENDING"},
                        "Raw Layer": {"status": "PENDING"},
                        "Master Configuration": {"status": "PENDING"},
                        "DQ Configuration": {"status": "PENDING"},
                        "Bronze": {"status": "PENDING"},
                        "Silver": {"status": "PENDING"},
                        "Gold": {"status": "PENDING"}
                    }
                }
        return {**state, "datasets": ds, "progress": progress}
    
    # If LOCAL/S3 and no DB, we just return empty
    logger.warning(f"No datasets found in DB for {src} client {client}")
    
    # FABRIC specific: If no datasets found, but it's FABRIC, we might be using a dynamic staging table
    if src == "AZURE":
        src = "ADLS"
    elif src == "FABRIC":
        src = "FABRIC"
        return {**state, "datasets": [MockDataset({"dataset_name": "fabric_extracted.csv", "source_object": "fabric_extracted.csv", "source_folder": folder}, client, src)], "progress": {}}

    return {**state, "datasets": [], "progress": {}}

@observe()
def _land(state: Dict) -> Dict:
    client = state["client_name"]
    folder = state.get("folder_path") or ""
    src = state["source_type"]
    bucket = settings.AZURE_CONTAINER_NAME or "datalake"
    batch_id = state.get("batch_id")
    if not batch_id:
        from datetime import datetime
        batch_id = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%b-%d-%H")

    prog = state.get("progress", {})
    success = []
    failed = []
    prepared = []

    # If LOCAL or S3/ADLS and the DB already has paths, we use them
    db_sourced = all(p.get("raw_layer_path") for p in prog.values()) if prog else False
    
    if src == "LOCAL" or (db_sourced and src != "API"):
        for p in prog.values():
            dsid = p["dataset_id"]
            if p.get("raw_layer_path"):
                p["steps"]["Validation"] = {"status": "PASSED", "detail": "Pre-validated at source/previous landing"}
                p["steps"]["Raw Layer"] = {"status": "PASSED", "detail": f"Path: {p.get('raw_layer_path', 'N/A')}"}
                
                # Create Mock object for reporting
                ds_mock = MockDataset(p, client, src)
                success.append(ds_mock)
                
                prepared.append({
                    "dataset_id": dsid,
                    "file_name": p["dataset_name"], 
                    "file_path": p["dataset_name"], 
                    "file_format": p.get("file_format") or (p["dataset_name"].split(".")[-1].upper() if "." in p["dataset_name"] else "UNKNOWN"),
                    "client_name": client, 
                    "source_type": src,
                    "raw_layer_path": p.get("raw_layer_path")
                })
        logger.info(f"Datasets already landed (DB/Direct sourced) for {client}")
        return {**state, "batch_id": batch_id, "landed": prepared, "success": success, "failed": []}

    if src == "FABRIC":
        for ds in state.get("datasets", []):
            try:
                # 1. Find staging table from Master Config or state
                # 1. Find staging table - Priority: State -> Master Config
                staging_table = state.get("staging_table")
                
                if not staging_table:
                    from sqlalchemy.orm import Session
                    db = SessionLocal()
                    from models.master_config_authoritative import MasterConfigAuthoritative
                    mc = db.query(MasterConfigAuthoritative).filter(
                        MasterConfigAuthoritative.client_name == client,
                        MasterConfigAuthoritative.source_type == "FABRIC"
                    ).first()
                    if mc and hasattr(mc, "staging_table") and mc.staging_table:
                        staging_table = mc.staging_table
                    db.close()
                
                if not staging_table:
                    # Fallback to naming convention if possible
                    staging_table = folder # We passed staging_table as folder/endpoint in StepSources
                
                logger.info(f"Landing FABRIC data from Neon table: {staging_table}")
                
                # 2. Query Neon
                conn = psycopg2.connect(settings.NEON_DB_URL)
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(f"SELECT * FROM {staging_table}")
                rows = cur.fetchall()
                conn.close()
                
                if not rows:
                    raise RuntimeError(f"Staging table {staging_table} is empty or missing.")
                
                # 3. Convert to CSV
                df = pd.DataFrame([dict(r) for r in rows])
                csv_buf = io.BytesIO()
                df.to_csv(csv_buf, index=False)
                content = csv_buf.getvalue()
                
                # 4. Upload to Raw
                raw_key = f"Raw/{client}/{batch_id}/fabric/{ds.file_name}"
                s3 = get_storage_client()
                s3.put_object(Container=bucket, Key=raw_key, Body=content)
                
                dsid = generate_dataset_id(client, src, ds.file_path)
                prepared.append({
                    "dataset_id": dsid,
                    "file_path": ds.file_path, 
                    "file_name": ds.file_name, 
                    "file_format": "CSV", 
                    "client_name": client, 
                    "source_type": src, 
                    "raw_layer_path": f"az://{bucket}/{raw_key}", 
                    "content": content
                })
                success.append(ds)
                if dsid not in prog:
                    prog[dsid] = {"dataset_id": dsid, "steps": {}}
                prog[dsid]["steps"]["Validation"] = {"status": "PASSED", "detail": f"Extracted {len(rows)} rows from Fabric staging."}
                prog[dsid]["steps"]["Raw Layer"] = {"status": "PASSED", "detail": f"Landed to Raw: az://{bucket}/{raw_key}"}
                
            except Exception as e:
                logger.error(f"Fabric landing failed: {e}")
                failed.append({"file": ds.file_name, "reason": str(e)})
        
        return {**state, "batch_id": batch_id, "landed": prepared, "success": success, "failed": failed}

    s3 = get_storage_client()
    connector = get_mcp_connector(src)
    for ds in state.get("datasets", []):
        try:
            # Reconstruct the full URI so the connector knows which container to read from
            if folder.startswith("az://") or folder.startswith("s3://"):
                canonical_request = folder.rstrip("/") + "/" + ds.file_name
            else:
                canonical_request = ds.file_path
                
            content = connector.get_file_content(canonical_request, client)
            logger.info(f"Landing dataset: {ds.file_name} from {src} (Extracted: {len(content)} bytes)")
            from core.validation import ValidationService
            ValidationService.validate_content(content, ds.file_name, expected_size=ds.file_size)
            
            if folder.startswith("az://") or folder.startswith("s3://"):
                _rest = folder.split("://", 1)[1]
                _parts = _rest.split("/", 1)
                _rel = _parts[1] if len(_parts) > 1 else _parts[0]
                fp = _rel.strip("/").replace("/", "_") or "root"
            else:
                fp = folder.replace("/", "_") or "root"
                
            raw_key = f"Raw/{client}/{batch_id}/{fp}/{ds.file_name}"
            s3.put_object(Container=bucket, Key=raw_key, Body=content)
            
            dsid = generate_dataset_id(client, src, ds.file_path)
            
            prepared.append({
                "dataset_id": dsid,
                "file_path": ds.file_path, 
                "file_name": ds.file_name, 
                "file_format": ds.file_format, 
                "client_name": client, 
                "source_type": src, 
                "raw_layer_path": f"az://{bucket}/{raw_key}", 
                "content": content
            })
            
            logger.info(f"Successfully uploaded {ds.file_name} (ID: {dsid}) to Raw as {raw_key}")
            success.append(ds)
            if dsid in prog:
                prog[dsid]["steps"]["Validation"] = {"status": "PASSED", "detail": f"Checksum & content validation SUCCESS ({len(content)} bytes)"}
                prog[dsid]["steps"]["Raw Layer"] = {"status": "PASSED", "detail": f"Ingested to Raw: az://{bucket}/{raw_key}"}
        except Exception as e:
            logger.error(f"Failed to land {ds.file_name}: {e}")
            failed.append({"file": ds.file_name, "reason": str(e)})
            try:
                dsid = generate_dataset_id(client, src, ds.file_path)
                if dsid in prog:
                    prog[dsid]["steps"]["Validation"] = {"status": "FAILED", "detail": f"Validation Error: {str(e)}"}
            except: pass
    return {**state, "batch_id": batch_id, "landed": prepared, "success": success, "failed": failed}

@observe()
def _report_ingestion(state: Dict) -> Dict:
    notifier = NotificationService()
    client = state["client_name"]
    batch_id = state.get("batch_id")
    success = state.get("success", [])
    failed = state.get("failed", [])
    notifier.send_ingestion_report(client_name=client, batch_id=batch_id, success_list=success, failure_list=failed)
    logger.info(f"Ingestion report sent for {client} (Success: {len(success)}, Failed: {len(failed)})")
    return state

@observe()
def _configure(state: Dict) -> Dict:
    client = state["client_name"]
    src = state["source_type"]
    folder = state.get("folder_path") or ""
    prog = state.get("progress", {})

    if src in ("LOCAL", "S3"):
        for p in prog.values():
            p["steps"]["Master Configuration"] = {"status": "PASSED", "detail": "Authoritative metadata pre-registered in Database"}
        logger.info(f"Master Configuration already set for LOCAL/S3 sources")
        return state

    mgr = MasterConfigManager()
    datasets = []
    for i in state.get("landed", []):
        # Determine the specific endpoint/folder for this dataset
        ds_endpoint = i["file_path"].strip("/") if "file_path" in i else folder
        datasets.append({
            "file_path": i["file_path"], 
            "file_name": i["file_name"], 
            "file_format": i["file_format"], 
            "client_name": client, 
            "source_type": src, 
            "source_folder": ds_endpoint, # Use the specific endpoint folder
            "raw_layer_path": i["raw_layer_path"]
        })
        
    if datasets:
        logger.info(f"Configuring {len(datasets)} datasets for {client}")
        # mgr.update_master_config still needs the global client/src
        mgr.update_master_config({
            "client_name": client, 
            "source_type": src, 
            "source_folder": folder, 
            "batch_id": state.get("batch_id"), # Pass batch_id
            "datasets": datasets
        })
        # Mark progress for Master Configuration
        mc_key = mgr._get_config_key(client)
        bucket = settings.AZURE_CONTAINER_NAME or "datalake"
        mc_path = f"az://{bucket}/{mc_key}"
        
        for i in state.get("landed", []):
            dsid = generate_dataset_id(client, src, i['file_path'] if 'file_path' in i else i['file_name'])
            logger.info(f"Metadata Registered: {i['file_name']} -> {dsid} in {mc_path}")
            if dsid in prog:
                prog[dsid]["steps"]["Master Configuration"] = {"status": "PASSED", "detail": f"Metadata registered in Master Config Registry: {mc_path}"}
        
        # --- NEW: CRITICAL SYNC TO DB ---
        try:
            from api.dq import sync_master_config, SyncRequest
            from core.database import SessionLocal
            db = SessionLocal()
            sync_master_config(SyncRequest(client_name=client), db)
            db.close()
            logger.info(f"DB Sync automatically completed during configuration node for client: {client}")
        except Exception as e:
            logger.warning(f"Configuration node failed to auto-sync DB: {e}")
            
    return state

@observe()
def _prepare_dq(state: Dict) -> Dict:
    client = state["client_name"]
    src = state["source_type"]
    prog = state.get("progress", {})

    if src in ("LOCAL", "S3"):
        for p in prog.values():
            p["steps"]["DQ Configuration"] = {"status": "PASSED", "detail": "DQ Schema pre-defined in Database"}
        logger.info(f"DQ Configuration already set for LOCAL/S3 sources")
        return state

    sess = SessionLocal()
    items = state.get("landed", [])
    for i in items:
        try:
            content = i["content"]
            df = None
            fn = i["file_name"].lower()
            if fn.endswith(".csv"):
                try:
                    df = pd.read_csv(io.BytesIO(content), nrows=50, on_bad_lines="skip")
                except Exception:
                    df = pd.read_csv(io.BytesIO(content), nrows=20)
            elif fn.endswith((".xlsx", ".xls")):
                df = pd.read_excel(io.BytesIO(content), nrows=50)
            elif fn.endswith(".json"):
                try:
                    df = pd.read_json(io.BytesIO(content), lines=False)
                except Exception:
                    df = pd.read_json(io.BytesIO(content), lines=True)
                if isinstance(df, list):
                    df = pd.DataFrame(df)
            cols = list(df.columns) if df is not None else []
            types = {}
            if df is not None:
                for c in cols:
                    d = str(df[c].dtype).lower()
                    if "int" in d:
                        types[c] = ExpectedDataType.INTEGER
                    elif "float" in d:
                        types[c] = ExpectedDataType.FLOAT
                    elif "datetime" in d or "date" in d:
                        types[c] = ExpectedDataType.DATE
                    elif "bool" in d:
                        types[c] = ExpectedDataType.BOOLEAN
                    else:
                        types[c] = ExpectedDataType.STRING
            dsid = generate_dataset_id(client, src, i['file_path'])
            for c in cols:
                exists = sess.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == dsid, DQSchemaConfig.column_name == c).first()
                if exists:
                    continue
                et = types.get(c, ExpectedDataType.STRING)
                sess.add(DQSchemaConfig(dataset_id=dsid, column_name=c, expected_data_type=et, dq_rule=DQRule.NOT_NULL, rule_value=None, severity=Severity.WARN, is_active=True))
            sess.commit()
        except Exception:
            pass
    # Update progress for DQ Configuration
    prog = state.get("progress", {})
    for i in state.get("landed", []):
        dsid = generate_dataset_id(client, src, i['file_path'])
        if dsid in prog:
            # We need to know which columns were profiled
            # Re-running the logic slightly just to get the column count for the log
            # In a real app we'd save this in the state
            content = i.get("content", b"")
            col_count = 0
            cols = []
            if content:
                try:
                    df = None
                    fn = i["file_name"].lower()
                    if fn.endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(content), nrows=1)
                    elif fn.endswith((".xlsx", ".xls")):
                        df = pd.read_excel(io.BytesIO(content), nrows=1)
                    elif fn.endswith(".json"):
                        df = pd.read_json(io.BytesIO(content), lines=True)
                    if df is not None:
                        col_count = len(df.columns)
                        cols = list(df.columns)
                except Exception:
                    pass
            
            logger.info(f"DQ Configuration complete for {i['file_name']} ({col_count} columns)")
            col_list = f": {', '.join(cols[:5])}..." if cols else ""
            prog[dsid]["steps"]["DQ Configuration"] = {"status": "PASSED", "detail": f"Auto-profiled {col_count} columns{col_list} and registered DQ schema with default types."}
    sess.close()
    return state

@observe()
def _transform(state: Dict) -> Dict:
    svc = PipelineService()
    results = []
    client = state["client_name"]
    src = state["source_type"]
    for i in state.get("landed", []):
        dsid = i.get("dataset_id")
        if not dsid:
             logger.warning(f"Transform node: dataset_id is missing for {i.get('file_name')}. Falling back to dynamic gen.")
             dsid = generate_dataset_id(client, src, i['file_path'])

        try:
            logger.info(f"Running transform for dataset {dsid}")
            start_t = time.time()
            r = svc.run(dsid, suppress_email=True)
            duration = max(time.time() - start_t, 0.001)
            ds_name = state.get("progress", {}).get(dsid, {}).get("dataset_name") or i.get("file_name")
            result_status = "FAILURE" if str(r.get("gold", {}).get("status", "")).upper() == "FAILED" else "SUCCESS"
            result_reason = r.get("gold", {}).get("reason", "") if result_status == "FAILURE" else ""
            results.append({"dataset_id": dsid, "dataset_name": ds_name, "status": result_status, "metrics": r, "reason": result_reason})
            prog = state.get("progress", {})
            if dsid in prog:
                # DQ Details
                dq = r.get("dq_details", {})
                v_list = dq.get("violations", [])
                w_list = dq.get("warnings", [])
                
                # Bronze
                b_rows = int(r.get("bronze", {}).get("rows_written", 0))
                raw_rows = int(r.get("raw", {}).get("rows_read", 0))
                b_key = r.get("paths", {}).get("bronze", "")
                bucket = settings.AZURE_CONTAINER_NAME or "datalake"
                b_path = f"az://{bucket}/{b_key}"
                
                # Applied rules summary
                applied_rules = "Default Standard (Schema Alignment)"
                if v_list or w_list:
                    all_rules = set([v['rule'] for v in v_list] + [w['rule'] for w in w_list])
                    applied_rules = ", ".join(all_rules)
                
                b_detail = (
                    f"Path: {b_path} | "
                    f"Metrics: {raw_rows} read -> {b_rows} written | "
                    f"Perf: {duration:.2f}s ({raw_rows/duration:.1f} rec/s) | "
                    f"Rules: {applied_rules}"
                )
                prog[dsid]["steps"]["Bronze"] = {
                    "status": "PASSED", 
                    "detail": b_detail
                }
                
                # Silver
                s_rows = int(r.get("silver", {}).get("rows_written", 0))
                s_keys = r.get("paths", {}).get("silver", [])
                rej_keys = r.get("paths", {}).get("rejected", [])
                
                s_paths_list = [f"az://{bucket}/{k}" for k in s_keys]
                s_paths_str = ", ".join(s_paths_list) if s_paths_list else "N/A (All rows isolated)"
                rej_paths_str = ", ".join([f"az://{bucket}/{k}" for k in rej_keys]) if rej_keys else "None"
                
                # Build rejection summary
                rej_reason = "None"
                if v_list:
                    items = [f"{v['column']} ({v['rule']}): {v['count']} rows" for v in v_list[:3]]
                    rej_reason = ", ".join(items)
                    if len(v_list) > 3:
                        rej_reason += " ..."
                
                s_detail = (
                    f"Path: {s_paths_str} | "
                    f"Metrics: {s_rows} clean rows upserted | "
                    f"Isolation Path: {rej_paths_str} | "
                    f"Isolation Reason: {rej_reason}"
                )
                prog[dsid]["steps"]["Silver"] = {
                    "status": "PASSED", 
                    "detail": s_detail
                }
                
                # Gold
                g_rows = int(r.get("gold", {}).get("rows_written", 0))
                g_status = str(r.get("gold", {}).get("status", "")).upper()
                g_keys = r.get("paths", {}).get("gold", [])
                g_paths_list = [f"az://{bucket}/{k}" for k in g_keys]
                if g_status == "SKIPPED":
                    prog[dsid]["steps"]["Gold"] = {
                        "status": "SKIPPED",
                        "detail": r.get("gold", {}).get("reason", "Gold layer skipped")
                    }
                elif g_status == "FAILED":
                    prog[dsid]["steps"]["Gold"] = {
                        "status": "FAILED",
                        "detail": r.get("gold", {}).get("reason", "Gold layer failed")
                    }
                else:
                    g_paths_str = ", ".join(g_paths_list) if g_paths_list else "N/A"
                    prog[dsid]["steps"]["Gold"] = {
                        "status": "PASSED",
                        "detail": f"Path: {g_paths_str} | Metrics: {g_rows} rows published"
                    }
                logger.info(f"Transform SUCCESS for {ds_name} ({dsid}): Bronze={b_rows}, Silver={s_rows}, Gold={g_rows} ({g_status or 'PASSED'}), DQ_Violations={len(v_list)} in {duration:.2f}s")
        except Exception as e:
            logger.error(f"Transform failure for {dsid}: {e}")
            ds_name = state.get("progress", {}).get(dsid, {}).get("dataset_name") or i.get("file_name")
            results.append({"dataset_id": dsid, "dataset_name": ds_name, "status": "FAILURE", "reason": str(e)})
            prog = state.get("progress", {})
            if dsid in prog:
                prog[dsid]["steps"]["Bronze"] = {"status": "FAILED", "detail": f"Processing Halted: {str(e)}"}
                prog[dsid]["steps"]["Silver"] = {"status": "FAILED", "detail": "Skipped due to Bronze processing failure"}
                prog[dsid]["steps"]["Gold"] = {"status": "SKIPPED", "detail": "Skipped due to upstream processing failure"}
    # Return progress as map for node state consistency; the API handles list conversion
    return {**state, "pipeline_results": results}

@observe()
def _report(state: Dict) -> Dict:
    notifier = NotificationService()
    client = state["client_name"]
    batch_id = state.get("batch_id")
    succ = [r for r in state.get("pipeline_results", []) if r["status"] == "SUCCESS"]
    fail = [r for r in state.get("pipeline_results", []) if r["status"] == "FAILURE"]
    total = len(succ) + len(fail)
    # Build CSV rows
    csv_rows = []
    html = []
    html.append("<html><head><meta charset='utf-8'><style>body{font-family:Arial,Helvetica,sans-serif;color:#222}h1{margin:0 0 8px}h2{margin:18px 0 8px}table{border-collapse:collapse;width:100%;margin:8px 0}th,td{border:1px solid #ddd;padding:8px;font-size:13px}th{background:#f5f5f5;text-align:left}tr:nth-child(even){background:#fafafa}.ok{color:#0a7}.bad{color:#c00}.muted{color:#666;font-size:12px}.pill{display:inline-block;padding:2px 6px;border-radius:10px;background:#eef;color:#334;margin-left:6px}</style></head><body>")
    html.append(f"<h1>Pipeline Batch Report<span class='pill'>{client}</span><span class='pill'>Batch {batch_id}</span></h1>")
    html.append(f"<div class='muted'>Total datasets: {total} • Success: {len(succ)} • Failure: {len(fail)}</div>")
    if succ:
        html.append("<h2>Successful Datasets</h2>")
        html.append("<table><thead><tr><th>Dataset</th><th>Raw Rows</th><th>Bronze Rows</th><th>Silver Rows</th><th>Gold Rows</th><th>Gold Status</th><th>Bronze Path</th><th>Silver Paths</th><th>Gold Paths</th><th>Rejected Paths</th><th>DQ Violations</th><th>DQ Warnings</th></tr></thead><tbody>")
        for r in succ:
            m = r.get("metrics", {})
            paths = m.get("paths", {})
            dq = m.get("dq_details", {"violations": [], "warnings": []})
            v_count = sum(int(d.get("count", 0)) for d in dq.get("violations", []))
            w_count = sum(int(d.get("count", 0)) for d in dq.get("warnings", []))
            gold = m.get("gold", {})
            html.append(
                f"<tr><td>{r.get('dataset_name') or r['dataset_id']}</td><td>{m.get('raw',{}).get('rows_read')}</td><td>{m.get('bronze',{}).get('rows_written')}</td><td>{m.get('silver',{}).get('rows_written')}</td><td>{gold.get('rows_written', 0)}</td><td>{gold.get('status', '')}</td><td>{paths.get('bronze') or ''}</td><td>{', '.join(paths.get('silver', []) or [])}</td><td>{', '.join(paths.get('gold', []) or [])}</td><td>{', '.join(paths.get('rejected', []) or [])}</td><td>{v_count}</td><td>{w_count}</td></tr>"
            )
            csv_rows.append({
                "status": "SUCCESS",
                "dataset": r.get("dataset_name") or r["dataset_id"],
                "dataset_id": r["dataset_id"],
                "raw_rows": m.get("raw",{}).get("rows_read"),
                "bronze_rows": m.get("bronze",{}).get("rows_written"),
                "silver_rows": m.get("silver",{}).get("rows_written"),
                "gold_rows": gold.get("rows_written", 0),
                "gold_status": gold.get("status", ""),
                "bronze_path": paths.get("bronze") or "",
                "silver_paths": ", ".join(paths.get("silver", []) or []),
                "gold_paths": ", ".join(paths.get("gold", []) or []),
                "rejected_paths": ", ".join(paths.get("rejected", []) or []),
                "dq_violations": v_count,
                "dq_warnings": w_count,
                "reason": ""
            })
        html.append("</tbody></table>")
    if fail:
        html.append("<h2>Failed Datasets</h2>")
        html.append("<table><thead><tr><th>Dataset</th><th>Reason</th></tr></thead><tbody>")
        for r in fail:
            html.append(f"<tr><td>{r.get('dataset_name') or r['dataset_id']}</td><td class='bad'>{r.get('reason','')}</td></tr>")
            csv_rows.append({
                "status": "FAILURE",
                "dataset": r.get("dataset_name") or r["dataset_id"],
                "dataset_id": r["dataset_id"],
                "raw_rows": "",
                "bronze_rows": "",
                "silver_rows": "",
                "gold_rows": "",
                "gold_status": "",
                "bronze_path": "",
                "silver_paths": "",
                "gold_paths": "",
                "rejected_paths": "",
                "dq_violations": "",
                "dq_warnings": "",
                "reason": r.get("reason", "")
            })
        html.append("</tbody></table>")
    # Upload CSV to S3 and include a download link
    try:
        import csv
        from io import StringIO
        bucket = settings.AZURE_CONTAINER_NAME or "datalake"
        key = f"Reports/{client}/{batch_id}/pipeline_report.csv"
        buf = StringIO()
        fieldnames = ["status","dataset","dataset_id","raw_rows","bronze_rows","silver_rows","gold_rows","gold_status","bronze_path","silver_paths","gold_paths","rejected_paths","dq_violations","dq_warnings","reason"]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)
        az = get_storage_client()
        az.put_object(Container=bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
        url = az.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=7*24*3600)
        html.append(f"<p><a href='{url}' style='display:inline-block;padding:8px 12px;background:#1976d2;color:#fff;text-decoration:none;border-radius:4px'>Download CSV Report</a></p>")
    except Exception:
        pass
    html.append("</body></html>")
    notifier.send_email_html(f"Pipeline Batch Report: {client} - {batch_id}", "".join(html))
    logger.info(f"Final pipeline report sent for {client} Batch {batch_id}")
    return state

def build_graph() -> StateGraph:
    g = StateGraph(dict)
    g.add_node("discover", _discover)
    g.add_node("land", _land)
    g.add_node("report_ingestion", _report_ingestion)
    g.add_node("configure", _configure)
    g.add_node("prepare_dq", _prepare_dq)
    g.add_node("transform", _transform)
    g.add_node("report", _report)
    g.set_entry_point("discover")
    g.add_edge("discover", "land")
    g.add_edge("land", "report_ingestion")
    g.add_edge("report_ingestion", "configure")
    g.add_edge("configure", "prepare_dq")
    g.add_edge("prepare_dq", "transform")
    g.add_edge("transform", "report")
    g.add_edge("report", END)
    return g
