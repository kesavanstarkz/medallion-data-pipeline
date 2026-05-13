from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from core.database import get_db
from models.target import Client, Target
from loguru import logger
import json
from datetime import datetime
import uuid

router = APIRouter(prefix="/config/targets", tags=["Target Configuration"])

class TargetRegisterRequest(BaseModel):
    client_name: str
    target_type: str
    target_name: str
    credential_config: Dict[str, Any]

class TargetUpdate(BaseModel):
    target_name: Optional[str] = None
    credential_config: Optional[Dict[str, Any]] = None

class SaveDataToTargetRequest(BaseModel):
    client_name: str
    source_name: Optional[str] = None
    target_id: str
    preview_data: Optional[Dict[str, Any]] = None
    save_schema: Optional[List[Dict[str, Any]]] = None
    schema: Optional[List[Dict[str, Any]]] = None # Direct schema
    rows: Optional[List[Any]] = None # Direct rows
    save_mode: str
    table_name: str
    schema_name: Optional[str] = None
    batch_size: Optional[int] = 1000
    primary_key: Optional[str] = None
    partition_key: Optional[str] = None
    options: Optional[Dict[str, Any]] = None

def _ensure_client(db: Session, client_name: str) -> str:
    client = db.query(Client).filter(Client.client_name == client_name).first()
    if not client:
        client = Client(client_name=client_name)
        db.add(client)
        db.commit()
        db.refresh(client)
    return client.client_id

@router.post("/test", summary="Test Connection to a Target System")
def test_target_connection(request: TargetRegisterRequest):
    """
    Mock connection test for various target systems.
    In a real implementation, this would use specific drivers for Fabric, SQL Server, etc.
    """
    logger.info(f"Testing connection for target type: {request.target_type} - {request.target_name}")
    
    # Simple validation of required fields for some types
    config = request.credential_config
    try:
        if request.target_type == "Fabric Warehouse":
            if not config.get("Workspace ID") or not config.get("Warehouse ID"):
                raise ValueError("Workspace ID and Warehouse ID are required.")
        elif request.target_type in ["SQL Server", "PostgreSQL", "MySQL", "Snowflake", "Redshift", "Azure Synapse"]:
            if not config.get("Host") and not config.get("Account") and not config.get("Server") and not config.get("Cluster Endpoint"):
                raise ValueError("Host/Endpoint is required.")
        
        # Mocking success for now
        return {"status": "SUCCESS", "message": f"Successfully validated connection to {request.target_type} '{request.target_name}'"}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

@router.post("", response_model=Dict[str, Any])
def register_target(request: TargetRegisterRequest, db: Session = Depends(get_db)):
    """
    Register a new target destination for a client.
    """
    client_id = _ensure_client(db, request.client_name)
    
    # Check if target name already exists for this client
    existing = db.query(Target).filter(Target.client_id == client_id, Target.target_name == request.target_name).first()
    if existing:
        # Update existing
        existing.target_type = request.target_type
        existing.credential_config_encrypted = json.dumps(request.credential_config)
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        logger.info(f"Updated target '{request.target_name}' for client '{request.client_name}'")
        return {"status": "UPDATED", "target_id": existing.target_id}
    else:
        # Create new
        new_target = Target(
            client_id=client_id,
            target_type=request.target_type,
            target_name=request.target_name,
            credential_config_encrypted=json.dumps(request.credential_config)
        )
        db.add(new_target)
        db.commit()
        db.refresh(new_target)
        logger.info(f"Registered new target '{request.target_name}' for client '{request.client_name}'")
        return {"status": "SUCCESS", "target_id": new_target.target_id}

@router.get("/{client_name}", response_model=List[Dict[str, Any]])
def list_targets(client_name: str, db: Session = Depends(get_db)):
    """
    List all registered targets for a given client.
    """
    client = db.query(Client).filter(Client.client_name == client_name).first()
    if not client:
        return []
    
    targets = db.query(Target).filter(Target.client_id == client.client_id).all()
    results = []
    for t in targets:
        results.append({
            "target_id": t.target_id,
            "client_id": t.client_id,
            "target_type": t.target_type,
            "target_name": t.target_name,
            "credential_config": json.loads(t.credential_config_encrypted) if t.credential_config_encrypted else {},
            "created_at": t.created_at,
            "updated_at": t.updated_at
        })
    return results

@router.delete("/{target_id}")
def delete_target(target_id: str, db: Session = Depends(get_db)):
    """
    Delete a target registration.
    """
    target = db.query(Target).filter(Target.target_id == target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")
    
    db.delete(target)
    db.commit()
    return {"status": "SUCCESS", "message": "Target deleted"}

@router.post("/save-data", summary="Save Discovered Data to a Target System")
async def save_data_to_target(request: SaveDataToTargetRequest, db: Session = Depends(get_db)):
    """
    Persist discovered data or schema into the configured target destination.
    """
    logger.info(f"Save to target requested: Client={request.client_name}, TargetID={request.target_id}, Mode={request.save_mode}")
    
    # 1. Fetch Target Configuration
    target = db.query(Target).filter(Target.target_id == request.target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target configuration not found.")
    
    config = json.loads(target.credential_config_encrypted) if target.credential_config_encrypted else {}
    
    # 2. Extract Data for Saving
    rows = request.rows or []
    columns = []
    
    # Priority 1: Explicitly passed schema
    if request.schema:
        columns = [c.get("name") or c.get("column_name") for c in request.schema if c.get("name") or c.get("column_name")]
    
    # Priority 2: Fallback to old preview_data/schema_discovery if rows/columns empty
    if not rows and request.preview_data:
        rows = request.preview_data.get("rows", [])
        
    if not columns:
        if request.preview_data and request.preview_data.get("columns"):
            columns_raw = request.preview_data.get("columns", [])
            if columns_raw and isinstance(columns_raw[0], dict):
                columns = [c.get("name") or c.get("column_name") or c.get("displayName") for c in columns_raw]
            else:
                columns = columns_raw
        elif request.schema_discovery:
            columns = [c.get("column_name") or c.get("name") for c in request.schema_discovery.get("columns", [])]
        elif request.save_schema:
            columns = [c.get("name") or c.get("column_name") for c in request.save_schema]
    
    if not columns and request.save_mode != "Create Table Only":
        raise HTTPException(status_code=400, detail="No columns found for saving.")

    # 3. Perform Save Action
    try:
        from services.target_save_service import TargetSaveService
        
        start_time = datetime.utcnow()
        
        result = await TargetSaveService.save_to_target(
            target_type=target.target_type,
            config=config,
            rows=rows,
            columns=columns,
            save_mode=request.save_mode,
            table_name=request.table_name,
            schema_name=request.schema_name,
            primary_key=request.primary_key,
            options=request.options
        )
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        result["execution_time"] = f"{execution_time:.2f}s"
        return result
        
    except Exception as e:
        logger.exception("Failed to save data to target")
        raise HTTPException(status_code=500, detail=f"Save failed: {str(e)}")
