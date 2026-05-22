import os
import time
import json
import base64
import logging
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from services.fabric_client import InteractiveAuth, FabricClient

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

def get_session() -> requests.Session:
    """Create a requests Session with exponential backoff for 429 and 500s."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def poll_operation(session: requests.Session, location_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Poll the Long Running Operation (LRO) until Succeeded or Failed."""
    logger.info(f"Polling operation status from {location_url}...")
    while True:
        resp = session.get(location_url, headers=headers)
        resp.raise_for_status()
        
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "Unknown")
            logger.info(f"Operation status: {status}")
            
            if status == "Succeeded":
                return data
            elif status in ("Failed", "Canceled"):
                raise Exception(f"Operation failed: {json.dumps(data)}")
        
        retry_after = int(resp.headers.get("Retry-After", 5))
        time.sleep(retry_after)

def fetch_result(session: requests.Session, result_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Fetch the final result after the LRO succeeds."""
    logger.info(f"Fetching operation result from {result_url}...")
    resp = session.get(result_url, headers=headers)
    resp.raise_for_status()
    return resp.json()

async def extract_fabric_pipeline(
    workspace_id: str,
    pipeline_id: str,
    client_id: str,
    client_secret: str,
    tenant_id: str
) -> Dict[str, Any]:
    """
    Extracts the full pipeline JSON and generates/extracts manifest.json.
    Reuses logic from pipe_chg_src.
    """
    auth = InteractiveAuth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    fabric_client = FabricClient(auth=auth)

    # Fetch pipeline definition
    pipeline_definition = fabric_client.get_pipeline_definition(workspace_id, pipeline_id)

    # Process pipeline definition (similar to existing logic)
    pipeline_json = pipeline_definition.get("definition", {})
    manifest_json = {
        "name": pipeline_definition.get("displayName", "Exported Pipeline"),
        "type": "DataPipeline",
        "properties": pipeline_definition,
    }

    return {
        "pipeline_json": pipeline_json,
        "manifest_json": manifest_json,
    }
