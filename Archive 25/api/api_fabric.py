from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form, Header
from typing import List, Optional, Dict
from services.fabric.auth_service import FabricAuthService, resolve_fabric_token
from services.fabric.workspace_service import FabricWorkspaceService
from services.fabric.pipeline_service import FabricPipelineService
from services.fabric.deploy_service import FabricDeployService
import json

router = APIRouter(prefix="/fabric", tags=["fabric"])

def get_token(authorization: str):
    return resolve_fabric_token(authorization)

@router.get("/workspaces")
async def list_workspaces(authorization: str = Header(...)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    print("DEBUG: list_workspaces token =", token[:10] + "...")
    ws_service = FabricWorkspaceService(token)
    return await ws_service.list_workspaces()

@router.get("/pipelines")
async def list_pipelines(workspace_id: str, authorization: str = Header(...)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    print(f"DEBUG: list_pipelines ws={workspace_id} token={token[:10]}...")
    p_service = FabricPipelineService(token)
    return await p_service.list_pipelines(workspace_id)

@router.post("/deploy")
async def deploy(
    workspace_id: str = Form(...),
    file: UploadFile = File(...),
    authorization: str = Header(...)
):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    
    print(f"DEBUG: deploy ZIP to ws={workspace_id}")
    deploy_service = FabricDeployService(token)
    file_bytes = await file.read()
    
    return await deploy_service.deploy_pipeline(
        workspace_id=workspace_id,
        file_bytes=file_bytes
    )

@router.post("/extract")
async def extract(workspace_id: str, pipeline_id: str, authorization: str = Header(...)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    print(f"DEBUG: extract ws={workspace_id} pipe={pipeline_id} token={token[:10]}...")
    
    p_service = FabricPipelineService(token)
    results = await p_service.bulk_export_definitions(workspace_id, [pipeline_id])
    if pipeline_id in results:
        files = results[pipeline_id]
        return {
            "pipeline": json.loads(files.get("pipeline.json", b"{}").decode('utf-8')),
            "manifest": json.loads(files.get("manifest.json", b"{}").decode('utf-8'))
        }
    raise HTTPException(status_code=404, detail="Pipeline not found")
    
@router.post("/clone")
async def clone(
    source_workspace_id: str = Form(...),
    source_pipeline_id: str = Form(...),
    target_workspace_id: str = Form(...),
    new_name: str = Form(...),
    authorization: str = Header(...)
):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    
    print(f"DEBUG: clone pipe={source_pipeline_id} to ws={target_workspace_id} as {new_name}")
    deploy_service = FabricDeployService(token)
    
    return await deploy_service.clone_pipeline(
        source_workspace_id=source_workspace_id,
        source_pipeline_id=source_pipeline_id,
        target_workspace_id=target_workspace_id,
        new_name=new_name
    )
