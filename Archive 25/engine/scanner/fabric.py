import logging
import base64
import json
import requests
from typing import Dict, List, Any
from .base import CloudScanner

logger = logging.getLogger("pipeline_ie.scanner.fabric")

class FabricScanner(CloudScanner):
    def can_scan(self, settings: Any) -> bool:
        # Fabric REST calls use the delegated token from the session; scan() no-ops quickly without it.
        return True

    def scan(self, settings: Any, **kwargs) -> Dict[str, List[str]]:
        raw_assets: Dict[str, List[Any]] = {
            "fabric_workspaces": [],
            "fabric_items": [],
            "warnings": [],
            "errors": [],
        }

        try:
            # 1. Identity Resolution (Fabric API requires Power BI / Fabric scope, not ARM)
            azure_token = kwargs.get("azure_token_fabric") or kwargs.get("azure_token")
            if not azure_token:
                logger.info("No delegated token for Fabric, skipping deep scan.")
                raw_assets["warnings"].append("No Fabric delegated token provided; workspace scan skipped.")
                return {"raw_cloud_dump": [raw_assets]}

            # Microsoft Fabric API Scope
            headers = {"Authorization": f"Bearer {azure_token}"}
            logger.info("FABRIC TOKEN RESOLVED: %s...", azure_token[:10])
            
            # 2. List Workspaces
            logger.info("Triggering Microsoft Fabric Deep Discovery (GET /v1/workspaces)...")
            ws_response = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers, timeout=10)
            logger.info("FABRIC WORKSPACES RAW: %s", ws_response.text[:500])
            
            if ws_response.status_code == 200:
                workspaces = ws_response.json().get('value', [])
                for ws in workspaces:
                    ws_id = ws.get('id')
                    ws_name = ws.get('displayName')
                    
                    # Create structured workspace object for the Explorer UI
                    workspace_obj = {
                        "id": ws_id,
                        "workspace_id": ws_id,
                        "workspace_name": ws_name,
                        "name": ws_name,
                        "displayName": ws_name,
                        "type": ws.get('type'),
                        "pipelines": []
                    }
                    raw_assets["fabric_workspaces"].append(workspace_obj)
                    
                    # 3. List Items in Workspace (Fetch Pipelines)
                    logger.info("Fetching items for workspace %s (%s)...", ws_name, ws_id)
                    items_res = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items", headers=headers, timeout=10)
                    
                    if items_res.status_code == 200:
                        items_data = items_res.json()
                        logger.info("FABRIC ITEMS RAW FOR %s: %s", ws_name, items_res.text[:200])
                        items = items_data.get('value', [])
                        
                        for item in items:
                            item_id = item.get('id')
                            item_type = item.get('type')
                            item_name = item.get('displayName')
                            
                            # Only include DataPipelines in the explorer hierarchy
                            if str(item_type).lower() in {"pipeline", "datapipeline", "data pipeline"}:
                                workspace_obj["pipelines"].append({
                                    "id": item_id,
                                    "pipeline_item_id": item_id,
                                    "pipeline_id": item_id,
                                    "pipeline_name": item_name,
                                    "name": item_name,
                                    "displayName": item_name,
                                    "workspace_id": ws_id,
                                })
                                
                                # Fetch definition for the specific pipeline
                                definition = self._fetch_item_definition(headers, ws_id, item_id, item_type)
                                item_meta = {
                                    "id": f"fabric || {item_name}",
                                    "configuration": {
                                        "ItemId": item_id,
                                        "Type": item_type,
                                        "WorkspaceId": ws_id,
                                        "Definition": definition if definition else {"DefinitionFetchStatus": "unavailable"}
                                    }
                                }
                                raw_assets["fabric_items"].append(item_meta)
                            
                            elif item_type == 'Lakehouse':
                                try:
                                    lh_res = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/lakehouses/{item_id}", headers=headers, timeout=10)
                                    if lh_res.status_code == 200:
                                        lh_props = lh_res.json().get('properties', {})
                                        raw_assets["fabric_items"].append({
                                            "id": f"fabric || {item_name}",
                                            "configuration": {
                                                "ItemId": item_id,
                                                "Type": item_type,
                                                "WorkspaceId": ws_id,
                                                "OneLakeFilesPath": lh_props.get('oneLakeFilesPath'),
                                                "OneLakeTablesPath": lh_props.get('oneLakeTablesPath')
                                            }
                                        })
                                except Exception:
                                    pass
            elif ws_response.status_code == 401:
                logger.error("Fabric API 401 Unauthorized. The SSO token likely lacks the required PowerBI/Fabric scope.")
                raw_assets["errors"].append("Fabric API returned 401 Unauthorized. Token may lack Fabric/Power BI scopes.")
            else:
                logger.warning(f"Fabric API returned {ws_response.status_code}: {ws_response.text}")
                raw_assets["errors"].append(f"Fabric API returned {ws_response.status_code} while listing workspaces.")

        except Exception as e:
            logger.error(f"Fabric Scan failed: {e}")
            raw_assets["errors"].append(f"Fabric scan failed: {e.__class__.__name__}")

        return {"raw_cloud_dump": [raw_assets]}

    def _fetch_item_definition(self, headers: Dict[str, str], workspace_id: str, item_id: str, item_type: str) -> Dict[str, Any] | None:
        urls = [
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines/{item_id}/getDefinition",
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/{str(item_type).lower()}/items/{item_id}/getDefinition",
        ]

        for url in urls:
            try:
                response = requests.post(url, headers=headers, json={}, timeout=15)
                if response.status_code in (200, 201):
                    payload = response.json()
                    decoded = self._decode_definition_payload(payload)
                    if decoded:
                        return decoded
                elif response.status_code == 202:
                    location = response.headers.get("Location")
                    if location:
                        polled = self._poll_lro_definition(headers, location)
                        decoded = self._decode_definition_payload(polled) if polled else None
                        if decoded:
                            return decoded
            except Exception as exc:
                logger.debug(f"Fabric definition fetch failed for {item_id} via {url}: {exc}")
        return None

    def _poll_lro_definition(self, headers: Dict[str, str], location: str) -> Dict[str, Any] | None:
        for _ in range(5):
            try:
                response = requests.get(location, headers=headers, timeout=15)
                if response.status_code == 200:
                    return response.json()
                if response.status_code in (202, 204):
                    continue
            except Exception as exc:
                logger.debug(f"Fabric definition poll failed: {exc}")
                return None
        return None

    def _decode_definition_payload(self, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        parts = payload.get("definition", {}).get("parts") or payload.get("parts") or []
        decoded: Dict[str, Any] = {}
        if not isinstance(parts, list):
            return None

        for part in parts:
            if not isinstance(part, dict):
                continue
            path = part.get("path")
            if not path:
                continue
            raw_value = (
                part.get("payload")
                or part.get("content")
                or part.get("data")
                or part.get("payloadBase64")
            )
            if raw_value is None:
                continue

            text_value = None
            if isinstance(raw_value, str):
                text_value = raw_value
                try:
                    text_value = base64.b64decode(raw_value).decode("utf-8")
                except Exception:
                    text_value = raw_value

            if text_value is None:
                continue

            try:
                decoded[path] = json.loads(text_value)
            except Exception:
                decoded[path] = text_value

        return decoded or None

    def _simulate_fabric(self) -> Dict[str, List[Any]]:
        return {"raw_cloud_dump": [{
            "fabric_workspaces": [],
            "fabric_items": [],
            "warnings": ["Fabric simulation is disabled for pipeline-definition scans."],
            "errors": []
        }]}
