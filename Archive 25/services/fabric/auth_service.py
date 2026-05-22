import httpx
import os
from fastapi import HTTPException
import time
import base64
import json
import requests
from loguru import logger

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
        try:
            if not _is_token_expired(cached):
                return cached
            # Token expired; try to refresh using client credentials as a fallback
            logger.info("Cached Fabric token expired, attempting client-credentials refresh")
            new = _get_client_token_sync()
            if new:
                save_fabric_token(new)
                return new
            logger.warning("Client-credentials refresh did not return a token")
        except Exception as exc:
            logger.exception("Error while attempting to refresh Fabric token: {}", exc)
    return None


def _decode_jwt_unverified(token: str) -> dict:
    parts = (token or "").split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        data = json.loads(decoded.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _is_token_expired(token: str) -> bool:
    claims = _decode_jwt_unverified(token)
    exp = claims.get("exp")
    if not exp:
        return False
    now = int(time.time())
    return int(exp) <= now


def _get_client_token_sync(scope: str = FABRIC_API_SCOPE) -> str:
    """Synchronous client credentials token acquisition used as a best-effort fallback when a cached delegated token expires."""
    tenant = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    if not tenant or not client_id or not client_secret:
        logger.debug("Client credentials not configured, cannot refresh token via client-credentials.")
        return None
    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": scope,
    }
    try:
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code != 200:
            logger.warning("Client-credentials token refresh failed: %s", resp.text)
            return None
        return resp.json().get("access_token")
    except Exception as exc:
        logger.exception("HTTP error during client-credentials token refresh: {}", exc)
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
