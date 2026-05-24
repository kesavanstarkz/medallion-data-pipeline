import httpx
import time
import uuid
import logging
from fastapi import HTTPException
from services.fabric.auth_service import (
    FABRIC_CONNECTION_READWRITE_SCOPE,
    fabric_token_has_permission,
)

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricConnectionService:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def get_connections(self) -> list:
        """Fetch all existing Fabric connections accessible by the user."""
        try:
            from services.fabric.auth_service import execute_fabric_request
            resp = await execute_fabric_request(self, "GET", f"{FABRIC_API_BASE}/connections")
            if not resp.is_success:
                logger.warning(f"Failed to fetch existing connections: {resp.text}")
                return []
            body = resp.json()
            return body.get("value", [])
        except Exception as e:
            logger.warning(f"Error fetching connections: {e}")
            return []

    async def get_or_create_rest_connection(self, base_url: str, preferred_name: str | None = None) -> str:
        """
        Robustly fetch or create a REST connection.
        If a connection for this base_url already exists, return its ID.
        Otherwise, create a new one with retry logic to avoid DuplicateConnectionName.
        """
        self._require_connection_permission("create a REST connection")

        # 1. Fetch existing connections to find a match
        existing_connections = await self.get_connections()
        
        # 2. Compare connections
        for conn in existing_connections:
            details = conn.get("connectionDetails", {})
            conn_type = details.get("type")
            
            if conn_type == "Web":
                params = details.get("parameters", [])
                url_param = next((p.get("value") for p in params if p.get("name") == "url"), None)
                
                if url_param == base_url:
                    logger.info(f"Reusing existing REST connection ID: {conn.get('id')} for URL: {base_url}")
                    return conn.get("id")

        # 3. If no match, generate unique name and create
        display_name = preferred_name or "Dynamic_REST_Connection"
        return await self._create_with_retry(display_name, base_url)

    async def _create_with_retry(self, initial_name: str, base_url: str) -> str:
        self._require_connection_permission("create a REST connection")

        # --- Deduplication: check if a connection with this displayName already exists ---
        existing_connections = await self.get_connections()
        for conn in existing_connections:
            if conn.get("displayName") == initial_name:
                logger.info(f"Connection with displayName '{initial_name}' already exists (id={conn.get('id')}). Reusing.")
                return conn.get("id")

        max_retries = 3
        current_name = initial_name
        
        from services.fabric.auth_service import execute_fabric_request
        for attempt in range(max_retries):
            payload = {
                "connectivityType": "ShareableCloud",
                "displayName": current_name,
                "connectionDetails": {
                    "type": "Web",
                    "creationMethod": "Web",
                    "parameters": [{"dataType": "Text", "name": "url", "value": base_url}],
                },
                "privacyLevel": "Organizational",
                "credentialDetails": {
                    "singleSignOnType": "None",
                    "connectionEncryption": "NotEncrypted",
                    "skipTestConnection": True,
                    "credentials": {"credentialType": "Anonymous"},
                },
            }
            
            resp = await execute_fabric_request(self, "POST", f"{FABRIC_API_BASE}/connections", json=payload)
            if resp.is_success:
                conn_id = resp.json().get("id")
                if conn_id:
                    logger.info(f"Successfully created REST connection '{current_name}' with ID: {conn_id}")
                    return conn_id
                raise HTTPException(status_code=502, detail="REST connection creation did not return an id")
            
            error_body = {}
            if resp.text:
                try:
                    error_body = resp.json()
                except ValueError:
                    pass
            
            error_code = error_body.get("errorCode", "")
            
            # Check for duplicate connection name issue
            if "DuplicateConnectionName" in error_code or "DuplicateConnectionName" in resp.text:
                logger.warning(f"Connection name '{current_name}' already exists. Attempt {attempt+1}/{max_retries}. Retrying with unique name...")
                timestamp = int(time.time())
                random_suffix = uuid.uuid4().hex[:4]
                current_name = f"rest_{timestamp}_{random_suffix}"
                continue
                
            raise HTTPException(
                status_code=resp.status_code,
                detail=(
                    "REST connection creation failed. The Fabric token must include "
                    f"{FABRIC_CONNECTION_READWRITE_SCOPE}, and the caller must be allowed "
                    f"to create Fabric connections. Fabric response: {resp.text}"
                ),
            )
                
        raise HTTPException(status_code=409, detail="Failed to create REST connection after multiple retries due to DuplicateConnectionName conflicts.")

    def _require_connection_permission(self, operation: str) -> None:
        if fabric_token_has_permission(self.access_token, FABRIC_CONNECTION_READWRITE_SCOPE):
            return

        raise HTTPException(
            status_code=401,
            detail=(
                f"Cannot {operation}: the active Fabric token is missing "
                f"{FABRIC_CONNECTION_READWRITE_SCOPE}. Sign in again after adding and "
                "admin-consenting this delegated Microsoft Fabric API permission on the "
                "app registration."
            ),
        )
