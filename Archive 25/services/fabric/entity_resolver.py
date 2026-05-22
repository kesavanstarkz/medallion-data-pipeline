"""
Resolve Microsoft Fabric workspace and pipeline identifiers.

pipe_chg_src and Fabric bulkExportDefinitions require GUID item IDs, not display names.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Tuple

from fastapi import HTTPException

from services.fabric.pipeline_service import FabricPipelineService
from services.fabric.workspace_service import FabricWorkspaceService

logger = logging.getLogger(__name__)

_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def is_fabric_guid(value: Optional[str]) -> bool:
    return bool(value and _GUID_RE.match(str(value).strip()))


def _normalize_label(value: Optional[str]) -> str:
    return str(value or "").strip().casefold()


async def resolve_workspace_id(
    token: str,
    workspace_id: Optional[str] = None,
    workspace_name: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Return (workspace_id GUID, workspace display name).
    Resolves by GUID when valid; otherwise looks up workspace by display name.
    """
    candidate_id = (workspace_id or "").strip()
    candidate_name = (workspace_name or "").strip()

    if is_fabric_guid(candidate_id):
        name = candidate_name
        if not name:
            ws_service = FabricWorkspaceService(token)
            try:
                ws = await ws_service.get_workspace(candidate_id)
                name = ws.get("displayName") or ws.get("name") or candidate_id
            except Exception:
                name = candidate_id
        return candidate_id, name

    lookup_name = candidate_name or candidate_id
    if not lookup_name:
        raise HTTPException(
            status_code=400,
            detail="workspace_id (GUID) or workspace_name is required for Fabric operations.",
        )

    ws_service = FabricWorkspaceService(token)
    workspaces = await ws_service.list_workspaces()
    normalized = _normalize_label(lookup_name)
    matches = [
        ws
        for ws in workspaces
        if _normalize_label(ws.get("displayName")) == normalized
        or _normalize_label(ws.get("name")) == normalized
        or _normalize_label(ws.get("id")) == normalized
    ]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail=f"Fabric workspace not found for identifier '{lookup_name}'. Provide a workspace GUID.",
        )
    if len(matches) > 1:
        logger.warning(
            "Multiple Fabric workspaces matched name '%s'; using first match id=%s",
            lookup_name,
            matches[0].get("id"),
        )
    resolved = matches[0]
    resolved_id = resolved.get("id")
    if not is_fabric_guid(resolved_id):
        raise HTTPException(
            status_code=502,
            detail=f"Workspace lookup returned a non-GUID id for '{lookup_name}'.",
        )
    return resolved_id, resolved.get("displayName") or resolved.get("name") or lookup_name


async def resolve_pipeline_item_id(
    token: str,
    workspace_id: str,
    pipeline_id: Optional[str] = None,
    pipeline_item_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Return (pipeline item GUID, pipeline display name).
    Accepts pipeline_item_id or pipeline_id; resolves display names via workspace item list.
    """
    candidate_id = (pipeline_item_id or pipeline_id or "").strip()
    candidate_name = (pipeline_name or "").strip()

    if is_fabric_guid(candidate_id):
        name = candidate_name
        if not name:
            p_service = FabricPipelineService(token)
            try:
                item = await p_service.get_pipeline(workspace_id, candidate_id)
                name = item.get("displayName") or item.get("name") or candidate_id
            except Exception:
                name = candidate_id
        return candidate_id, name

    lookup_name = candidate_name or candidate_id
    if not lookup_name:
        raise HTTPException(
            status_code=400,
            detail="pipeline_item_id / pipeline_id (GUID) or pipeline_name is required.",
        )

    p_service = FabricPipelineService(token)
    pipelines = await p_service.list_pipelines(workspace_id)
    normalized = _normalize_label(lookup_name)
    matches = [
        p
        for p in pipelines
        if _normalize_label(p.get("displayName")) == normalized
        or _normalize_label(p.get("name")) == normalized
        or _normalize_label(p.get("id")) == normalized
    ]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Fabric pipeline '{lookup_name}' not found in workspace {workspace_id}. "
                "Provide a pipeline item GUID."
            ),
        )
    if len(matches) > 1:
        logger.warning(
            "Multiple pipelines matched name '%s' in workspace %s; using first id=%s",
            lookup_name,
            workspace_id,
            matches[0].get("id"),
        )
    resolved = matches[0]
    resolved_id = resolved.get("id")
    if not is_fabric_guid(resolved_id):
        raise HTTPException(
            status_code=502,
            detail=f"Pipeline lookup returned a non-GUID id for '{lookup_name}'.",
        )
    return resolved_id, resolved.get("displayName") or resolved.get("name") or lookup_name


async def resolve_fabric_deployment_context(
    token: str,
    *,
    workspace_id: Optional[str] = None,
    workspace_name: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    pipeline_item_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
) -> Dict[str, str]:
    """Resolve workspace and pipeline to GUIDs; return canonical context dict."""
    resolved_workspace_id, resolved_workspace_name = await resolve_workspace_id(
        token, workspace_id=workspace_id, workspace_name=workspace_name
    )
    resolved_pipeline_id, resolved_pipeline_name = await resolve_pipeline_item_id(
        token,
        resolved_workspace_id,
        pipeline_id=pipeline_id,
        pipeline_item_id=pipeline_item_id,
        pipeline_name=pipeline_name,
    )
    return {
        "workspace_id": resolved_workspace_id,
        "workspace_name": resolved_workspace_name,
        "pipeline_item_id": resolved_pipeline_id,
        "pipeline_id": resolved_pipeline_id,
        "pipeline_name": resolved_pipeline_name,
    }


def log_export_context(
    *,
    workspace_id: str,
    pipeline_item_id: str,
    workspace_name: str = "",
    pipeline_name: str = "",
    operation: str = "bulk_export_definitions",
) -> None:
    logger.info(
        "Fabric export preflight | operation=%s workspace_id=%s pipeline_item_id=%s "
        "workspace_name=%s pipeline_name=%s",
        operation,
        workspace_id,
        pipeline_item_id,
        workspace_name or "(unknown)",
        pipeline_name or "(unknown)",
    )
