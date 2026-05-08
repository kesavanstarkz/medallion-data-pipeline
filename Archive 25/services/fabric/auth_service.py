import httpx
import os
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"

# Global cache for Fabric tokens to persist across steps
# Keyed by a simple 'current' key for now, or could be keyed by client/user if needed
_FABRIC_TOKEN_CACHE = {
    "access_token": None,
    "validation": None,
    "timestamp": 0
}

def save_fabric_token(token: str, validation: dict = None):
    global _FABRIC_TOKEN_CACHE
    import time
    _FABRIC_TOKEN_CACHE["access_token"] = token
    _FABRIC_TOKEN_CACHE["validation"] = validation
    _FABRIC_TOKEN_CACHE["timestamp"] = time.time()

def get_cached_fabric_token():
    global _FABRIC_TOKEN_CACHE
    return _FABRIC_TOKEN_CACHE.get("access_token")

def resolve_fabric_token(request_or_header: str = None) -> str:
    """
    Resolves a Fabric token from:
    1. Provided header/string
    2. Global cache
    """
    # 1. Check provided token
    if request_or_header:
        token = request_or_header.replace("Bearer ", "").strip()
        if token and token != "null" and token != "undefined":
            # Update cache if it's a new token
            save_fabric_token(token)
            return token
            
    # 2. Check cache
    cached = get_cached_fabric_token()
    if cached:
        return cached
        
    return None

class FabricAuthService:
    def __init__(self, tenant_id=None, client_id=None, client_secret=None):
        self.tenant_id = tenant_id or os.getenv("AZURE_TENANT_ID")
        self.client_id = client_id or os.getenv("AZURE_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("AZURE_CLIENT_SECRET")

    async def get_client_token_for_scope(self, scope: str):
        """Get token using Client Credentials Flow for a specific resource scope."""
        if not self.tenant_id or not self.client_id or not self.client_secret:
            raise HTTPException(
                status_code=500,
                detail="Azure client credentials are not fully configured for backend token acquisition.",
            )
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": scope,
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, data=data)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get token for scope {scope}: {resp.text}",
                )
            return resp.json().get("access_token")

    async def get_client_token(self):
        """Get Fabric API token using Client Credentials Flow."""
        return await self.get_client_token_for_scope(FABRIC_API_SCOPE)

    async def get_storage_token(self):
        """Get OneLake DFS token using the Azure Storage resource scope."""
        return await self.get_client_token_for_scope(ONELAKE_STORAGE_SCOPE)

    async def get_obo_storage_token(self, user_token: str) -> str:
        """Get Azure Storage token on behalf of a user using OBO flow."""
        if not self.tenant_id or not self.client_id or not self.client_secret:
            raise HTTPException(
                status_code=500,
                detail="Azure client credentials are not fully configured for backend OBO token acquisition.",
            )
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": user_token,
            "scope": ONELAKE_STORAGE_SCOPE,
            "requested_token_use": "on_behalf_of",
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, data=data)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get OBO token for storage scope: {resp.text}",
                )
            return resp.json().get("access_token")

    def get_auth_url(self, redirect_uri):
        """Get URL for Interactive SSO login"""
        scope = "https://api.fabric.microsoft.com/DataPipeline.ReadWrite.All https://api.fabric.microsoft.com/Item.ReadWrite.All offline_access"
        return (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/authorize"
            f"?client_id={self.client_id}"
            f"&response_type=code"
            f"&redirect_uri={redirect_uri}"
            f"&response_mode=query"
            f"&scope={scope}"
        )
