from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.health import router as health_router
from api.ingest import router as ingest_router
from api.configuration import router as configuration_router
from api.connect import router as connect_router
from api.config_workflow import router as config_workflow_router
from api.dq import router as dq_router
from api.pipeline import router as pipeline_router
from api.orchestrate import router as orchestrate_router
from api.api_source import router as api_source_router
from api.storage import router as storage_router
from api.upload import router as upload_router
from api.s3_injest import router as s3_router
from api.discovery import router as discovery_router
from api.auth import router as auth_router
from api.api_fabric import router as fabric_router
from core.logger import setup_logger
from dotenv import load_dotenv
from pathlib import Path
from fastapi.responses import FileResponse, JSONResponse
from fastapi import UploadFile, File, Form, HTTPException
from typing import Annotated, Optional
import json
import logging
from services.fabric.pipeline_parser import parse_pipeline_zip, remap_pipeline
from services.fabric.workspace_service import FabricWorkspaceService
from services.fabric.pipeline_service import FabricPipelineService
from services.fabric.deploy_service import FabricDeployService

# Load this backend's .env explicitly so running uvicorn from another working
# directory cannot accidentally pick up another project's environment.
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=False)

setup_logger()

# Main Application Entry Point
app = FastAPI(title="Data Engineer Agent (DEA)", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. Health & Status
app.include_router(health_router)

# 2. MCP Connector (Connectivity Test)
app.include_router(connect_router)

# 3. Ingestion (Execution: Source -> Raw)
app.include_router(ingest_router)

# 4. Configuration (Metadata Intelligence)
app.include_router(configuration_router)

# 5. Config Workflow (Human-in-the-Loop)
app.include_router(config_workflow_router)

# 6. Data Quality / Schema Configuration
app.include_router(dq_router)

# 7. Pipeline (Raw → Bronze → Silver)
app.include_router(pipeline_router)
app.include_router(orchestrate_router)
app.include_router(api_source_router)  # API Source Management
app.include_router(upload_router)       # Local File Upload
app.include_router(s3_router)           # S3 Bucket Ingestion
app.include_router(storage_router)     # Storage Explorer
app.include_router(discovery_router)   # Pipeline Intelligence
app.include_router(auth_router)        # Microsoft SSO
app.include_router(fabric_router)      # Microsoft Fabric Extraction

# Serve React build (if present) as a single-page app. We check a few
# common locations and mount the first existing build directory at '/'.
try:
    BASE_DIR = Path(__file__).parent
    build_candidates = [BASE_DIR / "build", BASE_DIR / "public"]
    for d in build_candidates:
        if d.exists():
            # Mount frontend at /orchestration-beta to avoid clashing with API root routes.
            app.mount("/orchestration-beta", StaticFiles(directory=str(d), html=True), name="frontend")
            # Also mount the build's static folder at /static so absolute asset paths
            # in index.html (e.g. /static/js/...) still resolve when the SPA is
            # served under /orchestration-beta.
            static_dir = d / "static"
            if static_dir.exists():
                app.mount("/static", StaticFiles(directory=str(static_dir)), name="frontend_static")
            index_file = d / "index.html"

            # Serve index.html for SPA routes under /orchestration-beta
            @app.get("/orchestration-beta", include_in_schema=False)
            def orchestration_index():
                return FileResponse(str(index_file))

            @app.get("/orchestration-beta/{full_path:path}", include_in_schema=False)
            def orchestration_spa(full_path: str):
                # If the requested file exists in build, serve it; otherwise return index.html
                requested = d / full_path
                if requested.exists() and requested.is_file():
                    return FileResponse(str(requested))
                return FileResponse(str(index_file))

            # Also serve the SPA at the root path '/' for local testing convenience.
            # This will only be used if no other API route matches the request.
            @app.get("/", include_in_schema=False)
            def root_index():
                # If the build/index.html exists, serve it. Otherwise return a small
                # health HTML so platforms (like Databricks) that probe '/' see a 200.
                if index_file.exists():
                    return FileResponse(str(index_file))
                from fastapi.responses import HTMLResponse
                return HTMLResponse("<html><body><h1>App running</h1></body></html>")

            @app.get("/{full_path:path}", include_in_schema=False)
            def root_spa(full_path: str):
                requested = d / full_path
                # Serve static files if they exist (css/js/media), otherwise return index.html
                if requested.exists() and requested.is_file():
                    return FileResponse(str(requested))
                if index_file.exists():
                    return FileResponse(str(index_file))
                from fastapi.responses import HTMLResponse
                return HTMLResponse("<html><body><h1>App running</h1></body></html>")

            break
except Exception:
    print("Warning: Failed to set up frontend static file serving. The API will still work, but the orchestration UI won't be available.")
    # If anything goes wrong here, don't block the app startup.
    pass


# Ensure Database Tables Exist
from core.database import engine, Base
# Import all models to ensure they are registered with Base metadata
from models.master_config import MasterConfig
from models.job import Job
from models.master_config_authoritative import MasterConfigAuthoritative
from models.dq_schema_config import DQSchemaConfig
from models.api_source_config import APISourceConfig
from models.metadata import IngestionMetadata, ConfigurationMetadata, PipelineRunHistory

# Create tables
Base.metadata.create_all(bind=engine)

try:
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE master_config_authoritative ADD COLUMN IF NOT EXISTS raw_layer_path TEXT"))
        
        # API Source Config Multi-Cloud Migrations
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'API'"))
        conn.execute(text("ALTER TABLE api_source_config ALTER COLUMN base_url DROP NOT NULL"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS aws_bucket_name TEXT"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS aws_region TEXT"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS aws_access_key TEXT"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS aws_secret_key TEXT"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS azure_account_name TEXT"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS azure_container_name TEXT"))
        conn.execute(text("ALTER TABLE api_source_config ADD COLUMN IF NOT EXISTS azure_account_key TEXT"))

        conn.execute(text("ALTER TABLE dq_schema_config ALTER COLUMN expected_data_type DROP NOT NULL"))
        conn.execute(text("ALTER TABLE dq_schema_config ALTER COLUMN dq_rule DROP NOT NULL"))
        conn.execute(text("ALTER TABLE dq_schema_config ALTER COLUMN severity DROP NOT NULL"))
        
        # Explicitly commit the migration transaction
        conn.commit()

        # Drop any existing foreign key constraints on dq_schema_config.dataset_id
        fk_rows = conn.execute(text(
            """
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = 'dq_schema_config'
              AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.column_name = 'dataset_id'
            """
        )).fetchall()
        for (cname,) in fk_rows:
            try:
                conn.execute(text(f"ALTER TABLE dq_schema_config DROP CONSTRAINT IF EXISTS {cname}"))
            except Exception:
                pass
except Exception:
    pass
# ─── Legacy Deployment Route (Ported from Discovery Agent) ──────────────────

@app.post("/deploy/execute", tags=["Fabric Deployment"])
async def deploy_pipeline_execute(
    zip_file: Annotated[UploadFile, File(description="Fabric pipeline ZIP")],
    access_token: Annotated[str, Form(description="Azure AD Token")],
    target_workspace_id: Annotated[str, Form(description="Target Fabric Workspace ID")],
    pipeline_name: Annotated[Optional[str], Form()] = None
):
    # 1. Initialize Services
    from services.fabric.workspace_service import FabricWorkspaceService
    from services.fabric.pipeline_service import FabricPipelineService
    from services.fabric.deploy_service import FabricDeployService
    import httpx
    
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

    # 2. Parse ZIP
    raw = await zip_file.read()
    try:
        parsed = parse_pipeline_zip(raw)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid ZIP structure: {str(e)}")

    pipeline_definition = parsed["raw_definition"]
    
    # 3. Resolve dependencies (Auto-Remapping)
    id_mappings = {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        # List items in target workspace
        items_resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{target_workspace_id}/items", headers=headers)
        target_items = items_resp.json().get("value", [])
        
        def auto_resolve(obj):
            if isinstance(obj, dict):
                if "type" in obj and obj["type"] == "Notebook":
                    nb = obj.get("typeProperties", {}).get("notebook", {})
                    ref_name = nb.get("referenceName")
                    if ref_name:
                        match = next((item for item in target_items if item.get("displayName") == ref_name and item.get("type") == "Notebook"), None)
                        if match:
                            id_mappings[nb.get("notebookId", "")] = match["id"]
                            id_mappings[nb.get("workspaceId", "")] = target_workspace_id
                for v in obj.values(): auto_resolve(v)
            elif isinstance(obj, list):
                for item in obj: auto_resolve(item)

        auto_resolve(pipeline_definition)
        final_definition = remap_pipeline(pipeline_definition, id_mappings)
        
        # 4. Versioning logic
        pipelines_resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{target_workspace_id}/items?type=DataPipeline", headers=headers)
        pipelines = pipelines_resp.json().get("value", [])
        
        original_name = (pipeline_name or "").strip() or parsed["pipeline_name"]
        final_name = original_name
        version = 1
        while any(p.get("displayName") == final_name for p in pipelines):
            final_name = f"{original_name}_v{version}"
            version += 1

        # 5. Create Pipeline
        import base64
        def_bytes = json.dumps(final_definition, indent=2).encode("utf-8")
        def_b64 = base64.b64encode(def_bytes).decode("utf-8")

        payload = {
            "displayName": final_name,
            "type": "DataPipeline",
            "definition": {
                "parts": [{"path": "pipeline-content.json", "payload": def_b64, "payloadType": "InlineBase64"}]
            }
        }
        
        create_resp = await client.post(f"{FABRIC_API_BASE}/workspaces/{target_workspace_id}/items", headers=headers, json=payload)
        
        # LOGGING (Requirement 9)
        logging.info(f"BACKEND ENDPOINT HIT: /deploy/execute")
        logging.info(f"DEPLOYMENT MODE: CREATE_OR_VERSION")
        logging.info(f"WORKSPACE ID: {target_workspace_id}")
        logging.info(f"TARGET WORKSPACE: {target_workspace_id}") # Usually the same in this simple flow
        logging.info(f"REQUESTED PIPELINE NAME: {final_name}")
        logging.info(f"ACTUAL FABRIC API CALLED: POST {FABRIC_API_BASE}/workspaces/{target_workspace_id}/items")
        
        if not create_resp.is_success:
            logging.error(f"FABRIC API ERROR RESPONSE: {create_resp.text}")
            raise HTTPException(status_code=create_resp.status_code, detail=f"Fabric API Error: {create_resp.text}")
            
        resp_json = create_resp.json()
        logging.info(f"RESPONSE PAYLOAD: {json.dumps(resp_json)}")
        logging.info(f"CREATED PIPELINE ITEM ID: {resp_json.get('id')}")

        return {
            "workspace_id": target_workspace_id,
            "pipeline_deployed": final_name,
            "status": "SUCCESS",
            "id": resp_json.get("id"),
            "remapped_ids": len(id_mappings),
            "fabric_response": resp_json,
            "pipeline_json": final_definition,
            "manifest_json": parsed.get("manifest", {})
        }
