from sqlalchemy import Column, String, Integer, Boolean, DateTime, JSON, Text
import uuid
from datetime import datetime
from core.database import Base

class MasterConfig(Base):
    __tablename__ = "master_configuration"

    # 1. Identity & System
    pipeline_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String, unique=True, index=True, nullable=False) # Hashed ID for idempotency/Identity
    batch_id = Column(String, nullable=True) # Latest Batch ID
    client_name = Column(String, nullable=True) # Adding client_name for filtering/export

    # 2. Source Details
    source_system = Column(String, nullable=False)
    source_schema = Column(String, nullable=True)
    source_object = Column(String, nullable=False) # Filename
    source_query = Column(Text, nullable=True)

    # 3. Target Details
    target_layer = Column(String, default="Bronze")
    target_schema = Column(String, nullable=True)
    target_table = Column(String, nullable=True)
    
    # 4. File Details
    file_format = Column(String, nullable=False)
    
    # 5. Load Logic (Human + AI)
    load_type = Column(String, nullable=True) # Full/Incremental
    upsert_key = Column(String, nullable=True)
    watermark_column = Column(String, nullable=True)
    watermark_value = Column(DateTime, nullable=True) # Last High Watermark
    partition_column = Column(String, nullable=True)
    
    # 6. Control Fields
    is_active = Column(Boolean, default=False) # Explicitly False as per requirements
    priority = Column(Integer, default=5)
    frequency = Column(String, nullable=True)
    
    # 7. Execution Status
    last_run_status = Column(String, default="PENDING")
    last_run_date = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    rows_read = Column(Integer, default=0)
    rows_written = Column(Integer, default=0)
    
    # 8. Validation & Metadata
    validation_rules = Column(JSON, nullable=True)
    # Persist discovered schema for DQ & downstream consumers
    schema = Column(JSON, nullable=True)
    sensitive_data_flag = Column(Boolean, default=False)
    dependency_id = Column(String(36), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
