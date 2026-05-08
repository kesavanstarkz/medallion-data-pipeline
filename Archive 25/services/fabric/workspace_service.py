import httpx
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricWorkspaceService:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def list_workspaces(self):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{FABRIC_API_BASE}/workspaces", headers=self.headers)
            if resp.status_code == 401:
                raise HTTPException(status_code=401, detail="Unauthorized")
            resp.raise_for_status()
            data = resp.json()
            return data.get("value", [])

    async def get_workspace(self, workspace_id: str):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}", headers=self.headers)
            resp.raise_for_status()
            return resp.json()

    async def list_workspace_items(self, workspace_id: str):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items", headers=self.headers)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("value", [])
