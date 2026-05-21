from fastapi import APIRouter, HTTPException, Depends, Query
from core.pipeline_service import PipelineService
from core.utils import PreviewDataService
from sqlalchemy.orm import Session
from core.database import get_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

@router.post("/run/{dataset_id}")
def run_pipeline(dataset_id: str):
    try:
        service = PipelineService()
        metrics = service.run(dataset_id)
        return {"status": "SUCCESS", "dataset_id": dataset_id, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview/{dataset_id}")
def preview_data(
    dataset_id: str,
    db: Session = Depends(get_db),
    sample_rows: int = Query(20, description="Number of preview rows to fetch")
):
    """
    Preview data directly via Spark execution without notebook artifact creation.
    
    Eliminates:
    - Notebook artifact orchestration
    - Visibility polling and timeouts
    - Eventual consistency delays
    
    Instead:
    - Dynamically resolves workspace_id, lakehouse_id, and file path
    - Builds ABFSS OneLake path
    - Executes Spark read directly
    - Returns schema and preview rows as JSON
    
    Response format matches Fabric notebook table output.
    
    Parameters:
    - dataset_id: The dataset identifier to preview
    - sample_rows: Number of rows to fetch (default 20)
    
    Returns:
    - success: true/false
    - preview_supported: true if preview succeeded
    - columns: List of column definitions with name and type
    - rows: Array of preview rows as JSON objects
    - row_count: Total rows sampled
    - abfss_path: The OneLake ABFSS path used
    - source_type: Resolved source type (FABRIC, ADLS, S3, CSV, etc)
    """
    try:
        service = PreviewDataService()
        result = service.preview(dataset_id, sample_rows, db)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

