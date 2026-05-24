import httpx
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricWorkspaceService:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def list_workspaces(self):
        try:
            from services.fabric.auth_service import execute_fabric_request
            resp = await execute_fabric_request(self, "GET", f"{FABRIC_API_BASE}/workspaces")
            if resp.status_code == 401:
                raise HTTPException(status_code=401, detail="Unauthorized")
            resp.raise_for_status()
            data = resp.json()
            return data.get("value", [])
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Failed to list workspaces: {exc}")

    async def get_workspace(self, workspace_id: str):
        try:
            from services.fabric.auth_service import execute_fabric_request
            resp = await execute_fabric_request(self, "GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}")
            resp.raise_for_status()
            return resp.json()
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Failed to get workspace: {exc}")

    async def list_workspace_items(self, workspace_id: str):
        try:
            from services.fabric.auth_service import execute_fabric_request
            resp = await execute_fabric_request(self, "GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items")
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("value", [])
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Failed to list workspace items: {exc}")
