from sqlalchemy import Column, String, Boolean, DateTime, Text
from core.database import Base
from datetime import datetime

class MasterConfigAuthoritative(Base):
    __tablename__ = "master_config_authoritative"

    dataset_id = Column(String(255), primary_key=True)
    pipeline_id = Column(String(255), nullable=True)
    client_name = Column(String(255), nullable=True)
    source_type = Column(String(50), nullable=True)
    source_folder = Column(Text, nullable=True)
    source_object = Column(Text, nullable=True)
    file_format = Column(String(50), nullable=True)
    raw_layer_path = Column(Text, nullable=True)
    target_layer_bronze = Column(Text, nullable=True)
    target_layer_silver = Column(Text, nullable=True)
    is_active = Column(Boolean, default=False)
    load_type = Column(String(50), nullable=True)
    upsert_key = Column(String(255), nullable=True)
    watermark_column = Column(String(255), nullable=True)
    partition_column = Column(String(255), nullable=True)
    last_seen_batch = Column(String(255), nullable=True)
    staging_table = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)