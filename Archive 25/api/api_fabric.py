from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Header, Body
from typing import List, Optional, Dict, Any
from services.fabric.auth_service import FabricAuthService, resolve_fabric_token
from services.fabric.workspace_service import FabricWorkspaceService
from services.fabric.pipeline_service import FabricPipelineService
from services.fabric.deploy_service import FabricDeployService
from services.fabric.entity_resolver import (
    resolve_fabric_deployment_context,
    resolve_workspace_id,
    resolve_pipeline_item_id,
    log_export_context,
)
import json
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fabric", tags=["fabric"])

def get_token(authorization: Optional[str]):
    token = resolve_fabric_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Fabric access token missing or expired. Please sign in again via Microsoft SSO.")
    return token


async def _resolve_ids_from_payload(token: str, payload: Dict[str, Any]) -> Dict[str, str]:
    return await resolve_fabric_deployment_context(
        token,
        workspace_id=payload.get("workspace_id") or payload.get("workspace"),
        workspace_name=payload.get("workspace_name"),
        pipeline_id=payload.get("pipeline_id") or payload.get("pipeline"),
        pipeline_item_id=payload.get("pipeline_item_id"),
        pipeline_name=payload.get("pipeline_name"),
    )


async def _resolve_ids_from_form(
    token: str,
    *,
    workspace_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    workspace_name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    source_workspace_id: Optional[str] = None,
    source_pipeline_id: Optional[str] = None,
    target_workspace_id: Optional[str] = None,
) -> Dict[str, str]:
    ws_raw = workspace_id or source_workspace_id or target_workspace_id
    pipe_raw = pipeline_id or source_pipeline_id
    ws_id, ws_name = await resolve_workspace_id(token, workspace_id=ws_raw, workspace_name=workspace_name)
    ctx: Dict[str, str] = {
        "workspace_id": ws_id,
        "workspace_name": ws_name,
        "pipeline_item_id": "",
        "pipeline_id": "",
        "pipeline_name": pipeline_name or "",
    }
    if pipe_raw or pipeline_name:
        pipe_id, pipe_name = await resolve_pipeline_item_id(
            token,
            ws_id,
            pipeline_id=pipe_raw,
            pipeline_name=pipeline_name,
        )
        ctx["pipeline_item_id"] = pipe_id
        ctx["pipeline_id"] = pipe_id
        ctx["pipeline_name"] = pipe_name
    return ctx

@router.get("/workspaces")
async def list_workspaces(authorization: Optional[str] = Header(None)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    ws_service = FabricWorkspaceService(token)
    return await ws_service.list_workspaces()

@router.get("/pipelines")
async def list_pipelines(workspace_id: str, authorization: Optional[str] = Header(None)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    p_service = FabricPipelineService(token)
    return await p_service.list_pipelines(workspace_id)

@router.post("/deploy")
async def deploy(
    workspace_id: str = Form(...),
    file: UploadFile = File(...),
    workspace_name: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None)
):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")

    ctx = await _resolve_ids_from_form(token, workspace_id=workspace_id, workspace_name=workspace_name)
    deploy_service = FabricDeployService(token)
    file_bytes = await file.read()

    return await deploy_service.deploy_pipeline(
        workspace_id=ctx["workspace_id"],
        file_bytes=file_bytes
    )

@router.post("/extract")
async def extract(
    workspace_id: str,
    pipeline_id: str,
    workspace_name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    authorization: Optional[str] = Header(None),
):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")

    ctx = await resolve_fabric_deployment_context(
        token,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
    )
    log_export_context(
        workspace_id=ctx["workspace_id"],
        pipeline_item_id=ctx["pipeline_item_id"],
        workspace_name=ctx["workspace_name"],
        pipeline_name=ctx["pipeline_name"],
        operation="extract",
    )
    p_service = FabricPipelineService(token)
    results = await p_service.bulk_export_definitions(
        ctx["workspace_id"], [ctx["pipeline_item_id"]]
    )
    pipeline_id = ctx["pipeline_item_id"]
    workspace_id = ctx["workspace_id"]
    if pipeline_id in results:
        files = results[pipeline_id]
        return {
            "pipeline": json.loads(files.get("pipeline.json", b"{}").decode('utf-8')),
            "manifest": json.loads(files.get("manifest.json", b"{}").decode('utf-8'))
        }
    raise HTTPException(status_code=404, detail="Pipeline not found")

@router.post("/inspect")
async def inspect(payload: Dict[str, Any] = Body(...), authorization: Optional[str] = Header(None)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    if not (payload.get("workspace_id") or payload.get("workspace")):
        raise HTTPException(status_code=400, detail="workspace_id and pipeline_id are required")
    if not (payload.get("pipeline_id") or payload.get("pipeline_item_id") or payload.get("pipeline")):
        raise HTTPException(status_code=400, detail="workspace_id and pipeline_id are required")

    ctx = await _resolve_ids_from_payload(token, payload)
    workspace_id = ctx["workspace_id"]
    pipeline_id = ctx["pipeline_item_id"]
    log_export_context(
        workspace_id=workspace_id,
        pipeline_item_id=pipeline_id,
        workspace_name=ctx["workspace_name"],
        pipeline_name=ctx["pipeline_name"],
        operation="inspect",
    )

    p_service = FabricPipelineService(token)
    exported = await p_service.bulk_export_definitions(workspace_id, [pipeline_id])
    files = exported.get(pipeline_id, {})
    pipeline_json = files.get("pipeline.json")
    if not pipeline_json:
        raise HTTPException(status_code=404, detail="Pipeline definition not found")
    definition = json.loads(pipeline_json.decode("utf-8"))
    activities = definition.get("properties", {}).get("activities", [])
    copy_activities = [activity for activity in activities if activity.get("type") == "Copy"]

    # Attempt to map connector implementation types to friendly connector names
    from services.fabric.deploy_service import SOURCE_TYPE_MAP, SINK_TYPE_MAP

    connections = []
    activities_info = []
    detected_source_types = set()
    detected_sink_types = set()
    for activity in activities:
        info = {"name": activity.get("name"), "type": activity.get("type"), "roles": {}}
        type_props = activity.get("typeProperties") or {}
        for role in ("source", "sink"):
            endpoint = type_props.get(role) or {}
            ds_settings = endpoint.get("datasetSettings") or {}
            external_refs = ds_settings.get("externalReferences") or {}
            connection = external_refs.get("connection")
            raw_type = endpoint.get("type") or ds_settings.get("type") or ""
            mapped = (SOURCE_TYPE_MAP.get(raw_type) if role == "source" else SINK_TYPE_MAP.get(raw_type)) or raw_type
            info["roles"][role] = {
                "raw_type": raw_type,
                "connector_type": mapped,
                "datasetSettings": ds_settings,
                "connection_id": connection,
                "linkedServiceReference": ((ds_settings.get("linkedServiceName") or {}).get("referenceName") if isinstance(ds_settings, dict) else None),
            }
            if connection:
                connections.append({
                    "activity": activity.get("name"),
                    "role": role,
                    "connector_type": mapped,
                    "connection_id": connection,
                })
            if role == "source":
                detected_source_types.add(mapped)
            else:
                detected_sink_types.add(mapped)
        activities_info.append(info)

    return {
        "workspace_id": workspace_id,
        "pipeline_id": pipeline_id,
        "activity_count": len(activities),
        "copy_activity_count": len(copy_activities),
        "connections_found": len(connections),
        "connections": connections,
        "detected_source_types": list(detected_source_types),
        "detected_sink_types": list(detected_sink_types),
        "activities": activities_info,
        "metadata": {
            "activity_names": [activity.get("name") for activity in activities],
            "activity_types": [activity.get("type") for activity in activities],
        },
        "pipeline": definition,
    }
    
@router.post("/clone")
async def clone(
    source_workspace_id: str = Form(...),
    source_pipeline_id: str = Form(...),
    target_workspace_id: str = Form(...),
    new_name: str = Form(...),
    workspace_name: Optional[str] = Form(None),
    pipeline_name: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None)
):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")

    source_ctx = await resolve_fabric_deployment_context(
        token,
        workspace_id=source_workspace_id,
        workspace_name=workspace_name,
        pipeline_id=source_pipeline_id,
        pipeline_name=pipeline_name,
    )
    target_ws_id, target_ws_name = await resolve_workspace_id(
        token, workspace_id=target_workspace_id, workspace_name=workspace_name
    )
    log_export_context(
        workspace_id=source_ctx["workspace_id"],
        pipeline_item_id=source_ctx["pipeline_item_id"],
        workspace_name=source_ctx["workspace_name"],
        pipeline_name=source_ctx["pipeline_name"],
        operation="clone",
    )
    deploy_service = FabricDeployService(token)

    return await deploy_service.clone_pipeline(
        source_workspace_id=source_ctx["workspace_id"],
        source_pipeline_id=source_ctx["pipeline_item_id"],
        target_workspace_id=target_ws_id,
        new_name=new_name,
        workspace_name=source_ctx["workspace_name"],
        pipeline_name=source_ctx["pipeline_name"],
    )

@router.post("/reuse")
async def reuse_pipeline(payload: Dict[str, Any] = Body(...), authorization: Optional[str] = Header(None)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    ctx = await _resolve_ids_from_payload(token, payload)
    deploy_service = FabricDeployService(token)
    return await deploy_service.reuse_pipeline(
        workspace_id=ctx["workspace_id"],
        pipeline_id=ctx["pipeline_item_id"],
    )

@router.post("/modify-source")
async def modify_source(payload: Dict[str, Any] = Body(...), authorization: Optional[str] = Header(None)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    ctx = await _resolve_ids_from_payload(token, payload)
    new_name = (
        payload.get("clone_name")
        or payload.get("new_name")
        or f"{ctx['pipeline_name'] or 'Pipeline'}_source_updated"
    )
    template_pipeline_id = (
        payload.get("template_pipeline_id")
        or payload.get("ref_pipeline_id")
        or ctx["pipeline_item_id"]
    )
    log_export_context(
        workspace_id=ctx["workspace_id"],
        pipeline_item_id=ctx["pipeline_item_id"],
        workspace_name=ctx["workspace_name"],
        pipeline_name=ctx["pipeline_name"],
        operation="modify-source",
    )
    deploy_service = FabricDeployService(token)
    return await deploy_service.mutate_pipeline(
        workspace_id=ctx["workspace_id"],
        pipeline_id=ctx["pipeline_item_id"],
        new_name=new_name,
        mode="source",
        source_params={
            **(payload.get("source_params") or {}),
            **({"connector_type": payload.get("source_type")} if payload.get("source_type") else {}),
        },
        source_connection_name=payload.get("source_connection_name"),
        template_pipeline_id=template_pipeline_id,
        workspace_name=ctx["workspace_name"],
        pipeline_name=ctx["pipeline_name"],
    )

@router.post("/modify-sink")
async def modify_sink(payload: Dict[str, Any] = Body(...), authorization: Optional[str] = Header(None)):
    token = get_token(authorization)
    if not token: raise HTTPException(status_code=401, detail="Invalid Token")
    ctx = await _resolve_ids_from_payload(token, payload)
    new_name = payload.get("clone_name") or payload.get("new_name") or "Modified_Sink_Pipeline"
    template_pipeline_id = (
        payload.get("template_pipeline_id")
        or payload.get("ref_pipeline_id")
        or ctx["pipeline_item_id"]
    )
    log_export_context(
        workspace_id=ctx["workspace_id"],
        pipeline_item_id=ctx["pipeline_item_id"],
        workspace_name=ctx["workspace_name"],
        pipeline_name=ctx["pipeline_name"],
        operation="modify-sink",
    )
    deploy_service = FabricDeployService(token)
    return await deploy_service.mutate_pipeline(
        workspace_id=ctx["workspace_id"],
        pipeline_id=ctx["pipeline_item_id"],
        new_name=new_name,
        mode="sink",
        sink_params={
            **(payload.get("sink_params") or {}),
            **({"connector_type": payload.get("sink_type")} if payload.get("sink_type") else {}),
        },
        sink_connection_name=payload.get("sink_connection_name"),
        template_pipeline_id=template_pipeline_id,
        workspace_name=ctx["workspace_name"],
        pipeline_name=ctx["pipeline_name"],
    )
