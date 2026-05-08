import logging
from typing import Dict, Any, List, Optional
from .workspace_service import FabricWorkspaceService

logger = logging.getLogger(__name__)

class FabricArtifactResolver:
    def __init__(self, access_token: str):
        self.workspace_service = FabricWorkspaceService(access_token)

    async def resolve_artifact_id(
        self, 
        workspace_id: str, 
        source_metadata: Dict[str, Any],
        diagnostics: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[str]:
        """
        Dynamically resolve artifact_id from Fabric workspace items based on runtime metadata.
        """
        if not workspace_id:
            return None

        # 1. Check if already present
        artifact_id = source_metadata.get("artifact_id") or source_metadata.get("artifactId")
        if artifact_id:
            if diagnostics is not None:
                diagnostics.append({
                    "strategy": "metadata_lookup",
                    "status": "success",
                    "artifact_id": artifact_id
                })
            return artifact_id

        # 2. Extract potential names/identifiers from metadata
        potential_names = self._extract_potential_names(source_metadata)
        storage_type = source_metadata.get("storage_type") or source_metadata.get("source_type")
        
        if diagnostics is not None:
            diagnostics.append({
                "strategy": "context_detection",
                "potential_names": list(potential_names),
                "storage_type": storage_type
            })

        if not potential_names:
            logger.warning("No potential names found in source metadata for artifact resolution.")
            return None

        # 3. Query Fabric Items API
        try:
            items = await self.workspace_service.list_workspace_items(workspace_id)
        except Exception as e:
            logger.error(f"Failed to list workspace items for resolution: {e}")
            return None

        # 4. Match items
        # Priority 1: Exact name match for Lakehouse/Warehouse
        for item in items:
            item_name = item.get("displayName") or item.get("name")
            item_id = item.get("id")
            item_type = item.get("type")

            if not item_name or not item_id:
                continue

            if item_name in potential_names:
                # If we have a storage type hint, try to match it
                if storage_type:
                    if str(storage_type).lower() in str(item_type).lower():
                        if diagnostics is not None:
                            diagnostics.append({
                                "strategy": "fabric_items_api_match",
                                "status": "success",
                                "match_type": "name_and_type",
                                "item_name": item_name,
                                "item_type": item_type,
                                "artifact_id": item_id
                            })
                        return item_id
                
                # Fallback to name match if type doesn't strictly match or isn't provided
                if diagnostics is not None:
                    diagnostics.append({
                        "strategy": "fabric_items_api_match",
                        "status": "success",
                        "match_type": "name_only",
                        "item_name": item_name,
                        "item_type": item_type,
                        "artifact_id": item_id
                    })
                return item_id

        # 5. Fallback: Lakehouse name inference from path or other metadata
        # (This is already partially covered by _extract_potential_names)
        
        if diagnostics is not None:
            diagnostics.append({
                "strategy": "resolution_failed",
                "status": "not_found",
                "workspace_id": workspace_id,
                "potential_names": list(potential_names)
            })

        return None

    def _extract_potential_names(self, metadata: Dict[str, Any]) -> set[str]:
        names = set()
        
        # Check explicit name fields
        for key in ["lakehouseName", "lakehouse_name", "warehouseName", "warehouse_name", "artifactName", "name", "source_name"]:
            val = metadata.get(key)
            if val and isinstance(val, str):
                names.add(val)

        # Check connection_metadata
        conn_meta = metadata.get("connection_metadata") or {}
        for key in ["lakehouseName", "warehouseName", "artifactName", "name", "displayName"]:
            val = conn_meta.get(key)
            if val and isinstance(val, str):
                names.add(val)

        # Check runtime_metadata
        runtime_meta = metadata.get("runtime_metadata") or {}
        for key in ["lakehouseName", "warehouseName", "linkedServiceName"]:
            val = runtime_meta.get(key)
            if val and isinstance(val, str):
                names.add(val)

        # Check linked_service_name directly
        linked_service = metadata.get("linked_service_name")
        if linked_service and isinstance(linked_service, str):
            names.add(linked_service)

        # Try to infer from path if it looks like fabric://workspace/lakehouse/...
        resolved_path = metadata.get("resolved_path") or ""
        if resolved_path.startswith("fabric://"):
            parts = resolved_path.replace("fabric://", "").split("/")
            if len(parts) > 1:
                # parts[0] is workspace, parts[1] might be lakehouse name
                names.add(parts[1])

        # Try to infer from onelake path: https://onelake.dfs.fabric.microsoft.com/workspace/artifact/...
        full_path = str(metadata.get("full_path") or "")
        if "onelake.dfs.fabric.microsoft.com" in full_path:
            # Example: https://onelake.dfs.fabric.microsoft.com/8328-9323/MyLakehouse/Files/path
            parts = full_path.split("/")
            # Parts: ["https:", "", "onelake.dfs.fabric.microsoft.com", "workspace_id", "artifact_id_or_name", ...]
            if len(parts) > 4:
                names.add(parts[4])

        # Check for table names or object names which might be part of a warehouse
        for key in ["table", "objectName", "tableName"]:
            val = metadata.get(key)
            if val and isinstance(val, str):
                # Sometimes the table is in a warehouse, but the warehouse name is what we need for the artifact_id
                pass 

        return {n for n in names if n and len(n) > 1}
