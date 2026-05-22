from sqlalchemy import Column, String, Integer, Boolean, DateTime, JSON, ForeignKey, func, Text, UUID
from core.database import Base
import uuid
from datetime import datetime

class IngestionMetadata(Base):
    """
    Append-only log of every file ingestion attempt.
    Tracks batch-level details and validation results.
    """
    __tablename__ = "ingestion_metadata"

    ingestion_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    client_name = Column(String, nullable=False, index=True)
    source_adls_path = Column(String, nullable=False)
    source_object = Column(String, nullable=False)
    batch_id = Column(String, nullable=False, index=True)  # Format: Month-Day-Hour
    
    validation_status = Column(String, nullable=False)  # PASS / FAIL
    failed_validation_rules = Column(JSON, default=list)  # Array of failed rules
    
    raw_storage_path = Column(String, nullable=True)  # Only populated if PASS
    
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow)
    job_status = Column(String, default="PROCESSING")  # PROCESSING, COMPLETED, FAILED
    error_message = Column(Text, nullable=True)

class ConfigurationMetadata(Base):
    """
    The Master Configuration Table.
    Logical identity of a dataset. Controlled updates only.
    """
    __tablename__ = "configuration_metadata"

    # Identity Columns (System Managed)
    dataset_id = Column(String, primary_key=True)  # hash(client + path + object)
    pipeline_id = Column(UUID(as_uuid=True), default=uuid.uuid4, index=True)
    
    client_name = Column(String, nullable=False, index=True)
    source_system = Column(String, nullable=False) # ADLS folder path
    source_object = Column(String, nullable=False) # filename
    
    file_format = Column(String, nullable=False)
    
    # Target Paths (System Managed)
    target_layer_bronze = Column(String, nullable=False)
    target_layer_silver = Column(String, nullable=False)
    
    # Operational Metadata (System Managed)
    latest_batch_id = Column(String, nullable=True)
    validation_rules = Column(JSON, default=dict)
    sensitive_data_flag = Column(Boolean, default=False)
    
    last_run_status = Column(String, default="PENDING")
    last_run_date = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    rows_read = Column(Integer, default=0)
    rows_written = Column(Integer, default=0)
    dependency_id = Column(String, nullable=True)
    
    # User Editable Columns (Domain Knowledge)
    load_type = Column(String, nullable=True)  # Full / Incremental
    upsert_key = Column(String, nullable=True)
    watermark_column = Column(String, nullable=True)
    partition_column = Column(String, nullable=True)
    is_active = Column(Boolean, default=False)  # Gatekeeper
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    last_edited_by = Column(String, nullable=True)

class PipelineRunHistory(Base):
    """
    Log of every batch-level pipeline execution.
    Tracks success/failure counts and store the result summary.
    """
    __tablename__ = "pipeline_run_history"

    run_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    batch_id = Column(String, nullable=False, index=True)
    client_name = Column(String, nullable=False, index=True)
    source_type = Column(String, nullable=False)
    folder_path = Column(String, nullable=True)
    platform = Column(String, nullable=True)
    workspace_id = Column(String, nullable=True)
    pipeline_id = Column(String, nullable=True)
    deployment_strategy = Column(String, nullable=True)
    
    status = Column(String, default="RUNNING")  # RUNNING, SUCCESS, FAILURE, PARTIAL
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    
    total_datasets = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    
    pipeline_results = Column(JSON, nullable=True)  # Detailed metrics / summary JSON
    error_message = Column(Text, nullable=True)
