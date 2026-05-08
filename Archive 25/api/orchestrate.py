from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import json
import math
import numbers
from sqlalchemy.orm import Session
from core.database import get_db
from orchestration.flow import build_graph
from core.pipeline_service import PipelineService
from models.master_config_authoritative import MasterConfigAuthoritative
from models.metadata import PipelineRunHistory
from loguru import logger
from datetime import datetime

router = APIRouter(prefix="/orchestrate", tags=["LangGraph Orchestration"])


def clean_json(obj):
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, numbers.Real):
        value = float(obj)
        if not math.isfinite(value):
            return None
        return obj.item() if hasattr(obj, "item") else obj
    if isinstance(obj, dict):
        return {k: clean_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean_json(v) for v in obj]
    if obj.__class__.__name__ in {"NAType", "NaTType"}:
        return None
    return obj


def _json_line(payload: dict) -> str:
    return json.dumps(clean_json(payload), allow_nan=False) + "\n"


def _canonical_source_type(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip().upper()
    if raw in {"S3", "AWS"}:
        return "AWS"
    if raw in {"ADLS", "AZURE"}:
        return "AZURE"
    if raw in {"API", "REST", "REST_API"}:
        return "REST_API"
    if raw in {"FABRIC", "MICROSOFT_FABRIC"}:
        return "FABRIC"
    if raw == "LOCAL":
        return "LOCAL"
    return raw or None


def _configured_source_types(client_name: str, db: Session) -> set[str]:
    from models.api_source_config import APISourceConfig
    from models.master_config import MasterConfig

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


try:
    from langfuse import observe
except ImportError:
    def observe(*args, **kwargs):
        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return args[0]

        def _decorator(fn):
            return fn

        return _decorator

@observe()
def _run_pipeline_for_client(client_name: str, db: Session) -> dict:
    """Runs Bronze+Silver pipeline for ALL datasets of a client already in DB."""
    datasets = db.query(MasterConfigAuthoritative).filter(
        MasterConfigAuthoritative.client_name == client_name,
        MasterConfigAuthoritative.is_active == True,
    ).all()

    if not datasets:
        raise ValueError(f"No active datasets found for client '{client_name}'. "
                         f"Run POST /upload/ingest or POST /s3/ingest first.")

    svc     = PipelineService()
    results = []
    for mc in datasets:
        try:
            metrics = svc.run(mc.dataset_id, suppress_email=True)
            results.append({
                "dataset_id":   mc.dataset_id,
                "dataset_name": mc.source_object,
                "status":       "SUCCESS",
                "metrics":      metrics,
            })
            logger.info(f"Pipeline OK: {mc.source_object} ({mc.dataset_id})")
        except Exception as e:
            results.append({
                "dataset_id":   mc.dataset_id,
                "dataset_name": mc.source_object,
                "status":       "FAILURE",
                "reason":       str(e),
            })
            logger.error(f"Pipeline FAILED: {mc.source_object}: {e}")

    return {
        "status":          "SUCCESS" if all(r["status"] == "SUCCESS" for r in results) else "PARTIAL",
        "client_name":     client_name,
        "success_count":   sum(1 for r in results if r["status"] == "SUCCESS"),
        "failure_count":   sum(1 for r in results if r["status"] == "FAILURE"),
        "pipeline_results": results,
    }


@router.post("/run")
def run(
    source_type:  str            = "ADLS",
    client_name:  str            = "",
    folder_path:  Optional[str]  = None,
    batch_id:     Optional[str]  = None,
    stream:       bool           = True,
    require_real_scan: bool      = False,
    db:           Session        = Depends(get_db),
):
    """
    Universal orchestration endpoint — works for ALL source types:

    ADLS  → source_type=ADLS,  client_name=AMGEN, folder_path=clinical
    API   → source_type=API,   client_name=CDC,   folder_path=countries
    LOCAL → source_type=LOCAL, client_name=AMGEN  (folder_path not needed — files already in Raw)
    S3    → source_type=S3,    client_name=AWSDATA (folder_path not needed — files already in Raw)

    For LOCAL and S3: run POST /upload/ingest or POST /s3/ingest FIRST to upload files.
    Then call this endpoint — it picks up all uploaded files and runs Raw→Bronze→Silver.
    """
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")

    src = source_type.upper().strip()
    if src == "REST_API":
        src = "API"
    elif src == "AWS":
        src = "S3"
    elif src == "AZURE":
        src = "ADLS"
    configured_types = _configured_source_types(client_name, db)
    requested_type = _canonical_source_type(src)
    if configured_types and requested_type not in configured_types:
        logger.warning(f"Execution rejected for client={client_name}: requested={requested_type}, configured={sorted(configured_types)}")
        raise HTTPException(
            status_code=400,
            detail=f"Source type {requested_type} is not configured for client '{client_name}'. Configured source types: {sorted(configured_types)}"
        )

    scan_required = require_real_scan
    if scan_required:
        from models.master_config import MasterConfig
        configs = db.query(MasterConfig).filter(
            MasterConfig.client_name == client_name,
            MasterConfig.is_active == True,
        ).all()
        logger.info(f"Execution validation: client={client_name}, require_real_scan={scan_required}, config_rows={len(configs)}")
        if not configs:
            raise HTTPException(status_code=400, detail="Configuration must be saved before execution.")
        has_real_scan = False
        for cfg in configs:
            rules = cfg.validation_rules or {}
            caps = rules.get("pipeline_capabilities") or {}
            if (
                rules.get("scan_status") in ("success", "partial")
                and (rules.get("framework") == "REST API" or caps.get("api") or rules.get("auth_mode") not in (None, "", "none"))
                and not rules.get("is_fallback")
                and caps.get("scan_mode") != "mock"
            ):
                has_real_scan = True
                break
        if not has_real_scan:
            raise HTTPException(status_code=400, detail="Please perform a real scan using credentials before execution.")

    # All sources — go through full LangGraph orchestration
    if not folder_path and src not in ("LOCAL", "S3"):
        raise HTTPException(
            status_code=400,
            detail=f"folder_path is required for source_type={src}. "
                   f"For LOCAL or S3 sources, folder_path is not needed."
        )
    try:
        graph = build_graph().compile()
        state = {
            "source_type": src,
            "client_name": client_name,
            "folder_path": folder_path,
        }
        if batch_id:
            state["batch_id"] = batch_id
        
        # Determine the batch_id and create a history record
        active_batch = state.get("batch_id") or datetime.now().strftime("%b-%d-%H")
        run_record = PipelineRunHistory(
            batch_id=active_batch,
            client_name=client_name,
            source_type=src,
            folder_path=folder_path,
            status="RUNNING"
        )
        db.add(run_record)
        db.commit()
        db.refresh(run_record)
        run_id = run_record.run_id

        if not stream:
            # Single JSON response mode
            res = graph.invoke(state)
            progress_data = res.get("progress", [])
            if isinstance(progress_data, dict):
                progress_data = list(progress_data.values())
                
            return clean_json({
                "status":           "SUCCESS",
                "source_type":      src,
                "client_name":      client_name,
                "batch_id":         res.get("batch_id"),
                "success_count":    len([r for r in res.get("pipeline_results", []) if r["status"] == "SUCCESS"]),
                "failure_count":    len([r for r in res.get("pipeline_results", []) if r["status"] == "FAILURE"]),
                "pipeline_results": res.get("pipeline_results", []),
                "progress":         progress_data,
                "master_config":    res.get("master_config", [])
            })
            
        def event_stream():
            accumulated_results = []
            last_progress = []
            try:
                for output in graph.stream(state):
                    for node_name, node_state in output.items():
                        progress_data = node_state.get("progress", [])
                        if isinstance(progress_data, dict):
                            progress_data = list(progress_data.values())
                        
                        if progress_data:
                            last_progress = progress_data
                        
                        # Dynamically include master_config if node provided it
                        payload = {
                            "node": node_name, 
                            "progress": progress_data,
                            "timestamp": datetime.now().isoformat()
                        }
                        if "master_config" in node_state:
                            payload["master_config"] = node_state["master_config"]
                        if "pipeline_results" in node_state:
                            payload["pipeline_results"] = node_state["pipeline_results"]
                            accumulated_results = clean_json(node_state["pipeline_results"])
                            
                        yield _json_line(payload)
                
                # Final state after END - use accumulated results
                total = len(accumulated_results)
                succ = sum(1 for r in accumulated_results if r["status"] == "SUCCESS")
                fail = sum(1 for r in accumulated_results if r["status"] == "FAILURE")
                
                # Update DB history
                try:
                    db_final = next(get_db()) # Get a fresh session for the stream's end
                    hist = db_final.query(PipelineRunHistory).filter_by(run_id=run_id).first()
                    if hist:
                        hist.status = "SUCCESS" if fail == 0 else ("PARTIAL" if succ > 0 else "FAILURE")
                        hist.end_time = datetime.utcnow()
                        hist.total_datasets = total
                        hist.success_count = succ
                        hist.failure_count = fail
                        hist.pipeline_results = clean_json(accumulated_results)
                        db_final.commit()
                except Exception as e:
                    logger.error(f"Failed to update run history: {e}")

                yield _json_line({"status": "SUCCESS", "completed": True, "pipeline_results": accumulated_results, "progress": last_progress})
            except Exception as e:
                logger.error(f"Stream error: {e}")
                # Update DB with failure
                try:
                    db_fail = next(get_db())
                    hist = db_fail.query(PipelineRunHistory).filter_by(run_id=run_id).first()
                    if hist:
                        hist.status = "FAILURE"
                        hist.error_message = str(e)
                        hist.end_time = datetime.utcnow()
                        db_fail.commit()
                except: pass
                yield _json_line({"status": "FAILURE", "error": str(e), "completed": True})
                
        return StreamingResponse(event_stream(), media_type="application/x-ndjson")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@router.get("/master-config")
def get_master_config(client_name: str, source_type: str = None, dataset_ids: str = None, stream: bool = False, db: Session = Depends(get_db)):
    """
    Retrieves the Master Configuration CSV for a given client.
    Optionally filters by a comma-separated list of dataset_ids.
    If no config found, attempts ad-hoc discovery for storage sources.
    """
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")
        
    from core.master_config_manager import MasterConfigManager
    mgr = MasterConfigManager()
    key = mgr._get_config_key(client_name)
    
    try:
        import pandas as pd
        import numpy as np
        import hashlib
        

        df = mgr._get_existing_config(key)
        
        # Determine if we should perform ad-hoc discovery for new paths
        is_storage_path = dataset_ids and (dataset_ids.startswith("az://") or dataset_ids.startswith("s3://"))
        
        # If we have a path/endpoint, try ad-hoc discovery and merge with existing config
        if dataset_ids and (is_storage_path or source_type in ("API", "LOCAL")):
            logger.info(f"Ad-hoc discovery triggered for {client_name} @ {dataset_ids}")
            from core.utils import generate_dataset_id
            try:
                new_rows = []
                if dataset_ids.startswith("az://") or dataset_ids.startswith("s3://"):
                    from core.mcp_connector import get_mcp_connector
                    from core.utils import generate_dataset_id
                    
                    actual_src = "S3" if dataset_ids.startswith("s3://") else "ADLS"
                    connector = get_mcp_connector(actual_src)
                    
                    logger.info(f"Direct {actual_src} discovery: path={dataset_ids}")
                    # list_children returns folders and files
                    children = connector.list_children(client_name, dataset_ids)
                    
                    for item in children.get("files", []):
                        file_name = item["file_name"]
                        file_path = item["file_path"] # Canonical e.g. s3://bucket/key or key
                        
                        d_id = generate_dataset_id(client_name, actual_src, file_path)
                        
                        # Only add if it doesn't already exist in the dataframe
                        if df.empty or d_id not in df["dataset_id"].values:
                            new_rows.append({
                                "dataset_id": d_id,
                                "pipeline_id": client_name.upper(),
                                "client_name": client_name,
                                "source_type": actual_src,
                                "source_folder": dataset_ids,
                                "source_object": file_name,
                                "file_format": item.get("file_format", "UNKNOWN"),
                                "raw_layer_path": file_path, # Landed path will be updated during _land in flow.py
                                "target_layer_bronze": f"Bronze/{client_name}/{file_name.rsplit('.', 1)[0] if '.' in file_name else file_name}",
                                "target_layer_silver": f"Silver/{client_name}/{file_name.rsplit('.', 1)[0] if '.' in file_name else file_name}",
                                "is_active": True,
                                "load_type": "full",
                            })

                elif source_type and source_type.upper() == "API":
                    from core.mcp_connector import get_mcp_connector
                    from core.utils import generate_dataset_id
                    connector = get_mcp_connector(source_type)
                    discovered = connector.list_datasets(client_name, dataset_ids)
                    discovered_ids_for_filter = []
                    if discovered:
                        existing_ids = [str(v).lower() for v in df["dataset_id"].values] if not df.empty else []
                        for ds in discovered:
                            d_id = generate_dataset_id(client_name, source_type, ds.file_path)
                            discovered_ids_for_filter.append(d_id.lower())
                            if d_id.lower() not in existing_ids:
                                new_rows.append({
                                    "dataset_id": d_id,
                                    "pipeline_id": client_name.upper(),
                                    "client_name": client_name,
                                    "source_type": source_type,
                                    "source_folder": ds.file_path.rsplit('/', 1)[0] if '/' in ds.file_path else "",
                                    "source_object": ds.file_name,
                                    "file_format": ds.file_format,
                                    "raw_layer_path": "",
                                    "target_layer_bronze": f"Bronze/{client_name}/{ds.file_name.rsplit('.', 1)[0] if '.' in ds.file_name else ds.file_name}",
                                    "target_layer_silver": f"Silver/{client_name}/{ds.file_name.rsplit('.', 1)[0] if '.' in ds.file_name else ds.file_name}",
                                    "is_active": True,
                                    "load_type": "full",
                                })
                elif source_type and source_type.upper() == "LOCAL":
                    # Local files are already in Raw/client_name/ dataset_ids is a comma-separated list of filenames
                    ids = [i.strip() for i in dataset_ids.split(",") if i.strip()]
                    for d_id in ids:
                        # Check if already in config
                        if df.empty or d_id not in df["dataset_id"].values:
                             # Try to derive name from ID (replace extension if needed)
                             file_name = d_id
                             new_rows.append({
                                "dataset_id": d_id,
                                "pipeline_id": client_name.upper(),
                                "client_name": client_name,
                                "source_type": "LOCAL",
                                "source_folder": f"upload/{client_name}",
                                "source_object": file_name,
                                "file_format": file_name.split('.')[-1].upper() if '.' in file_name else "CSV",
                                "raw_layer_path": f"Raw/{client_name}/{file_name}",
                                "target_layer_bronze": f"Bronze/{client_name}/{file_name.rsplit('.', 1)[0] if '.' in file_name else file_name}",
                                "target_layer_silver": f"Silver/{client_name}/{file_name.rsplit('.', 1)[0] if '.' in file_name else file_name}",
                                "is_active": True,
                                "load_type": "full",
                            })
                
                if new_rows:
                    new_df = pd.DataFrame(new_rows)
                    df = pd.concat([df, new_df], ignore_index=True) if not df.empty else new_df
                    # Persist immediately so /dq/suggest finds it
                    mgr._save_config(df, key)
                    logger.info(f"Ad-hoc discovery found and saved {len(new_rows)} NEW files in {dataset_ids}")
                    
                    # IMMEDIATELY SYNC TO DB so Step 4/6 finds the records
                    try:
                        from api.dq import sync_master_config, SyncRequest
                        from core.database import SessionLocal
                        db_session = SessionLocal()
                        try:
                            sync_master_config(SyncRequest(client_name=client_name), db_session)
                        finally:
                            db_session.close()
                    except Exception as sync_err:
                        logger.warning(f"Ad-hoc DB sync failed: {sync_err}")

            except Exception as adhoc_err:
                logger.warning(f"Ad-hoc discovery failed for {client_name}: {adhoc_err}")


        # Skip filtering if we already scoped via ad-hoc discovery (path was the scope)
        is_storage_path = dataset_ids and (dataset_ids.startswith("az://") or dataset_ids.startswith("s3://"))
        
        # Filter storage paths to the active source/path so previous scan/manual rows do not leak in.
        if dataset_ids and is_storage_path and not df.empty:
            actual_src = "S3" if dataset_ids.startswith("s3://") else "ADLS"
            target_path = dataset_ids.rstrip("/").lower()
            mask = df["source_type"].astype(str).str.upper().eq(actual_src)
            if "source_folder" in df.columns:
                mask &= df["source_folder"].astype(str).str.rstrip("/").str.lower().apply(
                    lambda v: v == target_path or v.startswith(target_path + "/") or target_path.startswith(v + "/")
                )
            if "raw_layer_path" in df.columns:
                mask |= (
                    df["source_type"].astype(str).str.upper().eq(actual_src)
                    & df["raw_layer_path"].astype(str).str.lower().str.startswith(target_path)
                )
            df = df[mask]

        # Filter by dataset_ids if provided and it's NOT an already-scoped storage path
        if dataset_ids and not is_storage_path:
            target_ids = [tid.strip().lower() for tid in dataset_ids.split(",") if tid.strip()]
            if not df.empty:
                # Check for matches in multiple columns to be robust
                mask = df["dataset_id"].astype(str).str.lower().isin(target_ids)
                
                # IMPORTANT: If any IDs were discovered ad-hoc in THIS request, they MUST be included in the mask
                if 'discovered_ids_for_filter' in locals() and discovered_ids_for_filter:
                    mask |= df["dataset_id"].astype(str).str.lower().isin(discovered_ids_for_filter)

                if "source_object" in df.columns:
                    # Match filename with or without extension
                    mask |= df["source_object"].astype(str).str.lower().isin(target_ids)
                    mask |= df["source_object"].astype(str).str.lower().apply(lambda x: x.rsplit(".", 1)[0] if "." in x else x).isin(target_ids)
                
                if "source_folder" in df.columns:
                    # Match endpoint names strictly or if the stored folder is a sub-path of the search term
                    # OR if the search term is a parent of the stored folder
                    def is_path_match(stored_folder, target_id):
                        s = str(stored_folder).lower().strip('/')
                        t = str(target_id).lower().strip('/')
                        if s == t: return True
                        # If user asked for a parent, show children
                        if s.startswith(t + '/'): return True
                        # If user asked for a deep specific endpoint, show it even if stored as parent
                        if t.startswith(s + '/'): return True
                        return False

                    mask |= df["source_folder"].astype(str).apply(lambda x: any(is_path_match(x, t) for t in target_ids))

                df = df[mask]
        elif source_type and not df.empty:
            df = df[df["source_type"].astype(str).str.upper() == source_type.upper()]

        
        if df.empty:
             # LAST RESORT: Check for dynamic Fabric Runtime Config in DB (MasterConfigAuthoritative)
             from models.master_config import MasterConfigAuthoritative
             runtime_results = db.query(MasterConfigAuthoritative).filter(
                 MasterConfigAuthoritative.client_name == client_name,
                 MasterConfigAuthoritative.is_active == True,
                 MasterConfigAuthoritative.source_type.in_(["FABRIC", "FABRIC_RUNTIME"])
             ).all()
             
             if runtime_results:
                 logger.info(f"Found {len(runtime_results)} runtime config rows for {client_name} in DB.")
                 records = []
                 for r in runtime_results:
                     records.append({
                         "dataset_id": r.dataset_id,
                         "pipeline_id": r.pipeline_id,
                         "client_name": r.client_name,
                         "source_type": r.source_type,
                         "source_folder": r.source_folder,
                         "source_object": r.source_object,
                         "file_format": r.file_format,
                         "raw_layer_path": r.raw_layer_path,
                         "target_layer_bronze": r.target_layer_bronze,
                         "target_layer_silver": r.target_layer_silver,
                         "is_active": r.is_active,
                         "load_type": r.load_type,
                     })
                 
                 if stream:
                     def runtime_stream():
                         for rec in records: yield _json_line(rec)
                         yield _json_line({"status": "SUCCESS", "completed": True})
                     return StreamingResponse(runtime_stream(), media_type="application/x-ndjson")
                 return {"client_name": client_name, "config": records}

             if stream:
                 def empty_gen(): yield _json_line({"client_name": client_name, "message": "No config found"})
                 return StreamingResponse(empty_gen(), media_type="application/x-ndjson")
             return {"client_name": client_name, "config": [], "message": "No master configuration found for this client."}

        # Replace NaN/NaT with None for JSON compliance
        df = df.replace({np.nan: None})
        records = df.to_dict(orient="records")

        if stream:
            def record_stream():
                for r in records:
                    yield _json_line(r)
                yield _json_line({"status": "SUCCESS", "completed": True})
            return StreamingResponse(record_stream(), media_type="application/x-ndjson")

        return clean_json({
            "client_name": client_name, 
            "location": f"az://{mgr.bucket_name}/{key}",
            "config": records
        })
    except Exception as e:
        logger.error(f"Failed to fetch master config for {client_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/save-master-config")
def save_master_config(request: dict, db: Session = Depends(get_db)):
    """
    Explicit endpoint to persist Fabric runtime intelligence config.
    Upsert by client_name + artifact_id.
    """
    client_name = request.get("client_name")
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")
    
    reformatted = request.get("reformatted_config") or request
    source_meta = reformatted.get("source") or {}
    # Use artifact_id as dataset_id if possible, otherwise pipeline_name
    dataset_id = source_meta.get("artifact_id") or reformatted.get("pipeline_name") or "fabric_pipeline"
    
    # Map to Master Config Schema
    from core.master_config_manager import MasterConfigManager, MASTER_CONFIG_COLUMNS
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    mgr = MasterConfigManager()
    key = mgr._get_config_key(client_name)
    df = mgr._get_existing_config(key)
    
    # Construct a row
    new_row = {
        "dataset_id": dataset_id,
        "pipeline_id": client_name.upper(),
        "client_name": client_name,
        "source_type": reformatted.get("source_type") or "FABRIC_RUNTIME",
        "source_folder": reformatted.get("source_path") or "",
        "source_object": reformatted.get("pipeline_name") or "Fabric Pipeline",
        "file_format": (reformatted.get("file_types") or ["CSV"])[0],
        "target_layer_bronze": f"Bronze/{client_name}/{reformatted.get('pipeline_name')}",
        "target_layer_silver": f"Silver/{client_name}/{reformatted.get('pipeline_name')}",
        "is_active": True,
        "load_type": "full",
        "created_at": datetime.utcnow().isoformat()
    }
    
    # Upsert logic for CSV
    if not df.empty and "dataset_id" in df.columns:
        df = df[df["dataset_id"] != dataset_id]
    
    new_df = pd.DataFrame([new_row])
    # Ensure columns match MASTER_CONFIG_COLUMNS
    for col in MASTER_CONFIG_COLUMNS:
        if col not in new_df.columns:
            new_df[col] = None
    new_df = new_df[MASTER_CONFIG_COLUMNS]
    
    df = pd.concat([df, new_df], ignore_index=True) if not df.empty else new_df
    mgr._save_config(df, key)
    
    # IMMEDIATELY SYNC TO DB so Step 4/6 finds the records
    try:
        from api.dq import sync_master_config, SyncRequest
        sync_master_config(SyncRequest(client_name=client_name), db)
    except Exception as sync_err:
        logger.warning(f"Fabric runtime config DB sync failed: {sync_err}")
    
    return {"status": "SUCCESS", "message": "Master configuration persisted."}

class MasterConfigUpdateRequest(BaseModel):
    client_name: str
    config: list[dict]

@router.get("/preview")
def preview_data(s3_url: str):
    """
    Fetches the first 10-20 records of a file in Azure storage (CSV or Parquet)
    for interactive preview in the orchestration UI.
    """
    if not s3_url:
        raise HTTPException(status_code=400, detail="s3_url is required")
        
    from core.azure_storage import get_storage_client, AzureStorageClient
    import pandas as pd
    import io
    import numpy as np
    
    try:
        container, key = AzureStorageClient.parse_az_url(s3_url)
        az: AzureStorageClient = get_storage_client()
        
        # Try direct read
        content = None
        try:
            obj = az.get_object(Container=container, Key=key)
            content = obj["Body"].read()
            final_key = key
        except Exception as e:
            # If path is a folder (common for Silver/Bronze parquet), find first file
            if "BlobNotFound" in str(e) or "404" in str(e):
                logger.debug(f"Path {key} not found as single blob, listing children...")
                prefix = key.strip("/") + "/"
                list_res = az.list_objects_v2(Prefix=prefix, Container=container)

                all_blobs = [c["Key"] for c in list_res.get("Contents", [])]
                
                # Preferred: extensions
                files = [k for k in all_blobs if any(k.lower().endswith(ext) for ext in (".parquet", ".csv", ".json"))]
                
                if not files:
                    # Fallback: any file NOT metadata
                    files = [k for k in all_blobs if not any(m in k.lower() for m in ("_metadata", "_success", ".crc", ".metadata"))]
                
                if not files:
                    raise FileNotFoundError(f"No previewable files found in {s3_url}")
                    
                # Pick the first actual data file
                final_key = files[0]
                logger.info(f"Resolved folder {key} to file {final_key} for preview (Fallback mode)")
                obj = az.get_object(Container=container, Key=final_key)
                content = obj["Body"].read()

            else:
                raise
        
        df = None
        lower_key = final_key.lower()
        if lower_key.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        elif lower_key.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), nrows=50)
        elif lower_key.endswith(".json"):
            df = pd.read_json(io.BytesIO(content), lines=True, nrows=50)
        else:
            # Fallback: Try JSON, then CSV
            try:
                df = pd.read_json(io.BytesIO(content), lines=True, nrows=50)
                logger.info("Fallback: Decoded as JSON (lines=True)")
            except:
                try:
                    df = pd.read_json(io.BytesIO(content), lines=False, nrows=50)
                    logger.info("Fallback: Decoded as JSON (lines=False)")
                except:
                    try:
                        df = pd.read_csv(io.BytesIO(content), nrows=50)
                        logger.info("Fallback: Decoded as CSV")
                    except:
                        logger.warning("All fallbacks failed for file format detection.")
        
        if df is None:
            raise ValueError(f"Unsupported file format for preview: {final_key}")
            
        # Replace NaN/NaT for JSON compliance
        df = df.replace({np.nan: None})
        
        return clean_json({
            "path": s3_url,
            "resolved_file": final_key,
            "columns": list(df.columns),
            "rows": df.head(10).to_dict(orient="records"),
            "total_sample_rows": len(df)
        })
    except Exception as e:
        logger.error(f"Preview failed for {s3_url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class InitializeRequest(BaseModel):
    source_type:  str
    client_name:  str
    folder_path:  Optional[str] = None

@router.post("/initialize")
def initialize_orchestration(
    request: InitializeRequest,
    db: Session = Depends(get_db)
):
    """
    Runs Discovery, Landing, and Configuration nodes to generate master_config.csv.
    Useful for brand new clients/sources before the user starts the full pipeline.
    """
    if not request.client_name:
        raise HTTPException(status_code=400, detail="client_name is required")
        
    from orchestration.flow import _discover, _land, _configure
    
    try:
        # 1. Start with initial state
        state = {
            "source_type": request.source_type.upper().strip(),
            "client_name": request.client_name,
            "folder_path": request.folder_path,
            "progress": {}
        }
        
        # 2. Sequential node execution
        state = _discover(state)
        state = _land(state)
        state = _configure(state)
        
        # 3. Sync to DB so it shows up in MasterConfigAuthoritative
        from api.dq import sync_master_config, SyncRequest
        try:
            sync_master_config(SyncRequest(client_name=request.client_name), db)
        except Exception as e:
            logger.warning(f"Initial sync to DB failed (CSV was created but DB sync skipped): {e}")

        return {
            "status": "SUCCESS",
            "message": "Orchestration initialized. Master configuration generated and synced.",
            "datasets_found": len(state.get("datasets", [])),
            "batch_id": state.get("batch_id")
        }
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/master-config/update")
def update_master_config_file(request: MasterConfigUpdateRequest):
    """
    Updates the Master Configuration CSV for a client. 
    Accepts JSON records and persists them back to Azure Blob Storage.
    """
    if not request.client_name:
        raise HTTPException(status_code=400, detail="client_name is required")
    if not request.config:
         raise HTTPException(status_code=400, detail="config data is empty")
         
    from core.master_config_manager import MasterConfigManager, MASTER_CONFIG_COLUMNS
    import pandas as pd
    
    mgr = MasterConfigManager()
    key = mgr._get_config_key(request.client_name)
    
    try:
        # Convert JSON records to DataFrame
        df = pd.DataFrame(request.config)
        
        # Ensure only valid columns are saved
        # If any column is missing from input, add it as null
        for col in MASTER_CONFIG_COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        # Order columns correctly
        df = df[MASTER_CONFIG_COLUMNS]
        
        # Save back to Azure
        mgr._save_config(df, key)
        
        return {
            "status": "SUCCESS",
            "message": f"Master Configuration for {request.client_name} updated successfully.",
            "rows": len(df)
        }
    except Exception as e:
        logger.error(f"Failed to update master config for {request.client_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
def get_run_history(limit: int = 50, db: Session = Depends(get_db)):
    """Fetches the most recent pipeline execution runs from the history table."""
    try:
        runs = db.query(PipelineRunHistory).order_by(PipelineRunHistory.start_time.desc()).limit(limit).all()
        return {"status": "SUCCESS", "runs": runs}
    except Exception as e:
        logger.error(f"Failed to fetch run history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
