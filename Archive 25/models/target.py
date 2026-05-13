from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey, Text
from core.database import Base
import uuid
from datetime import datetime

class Client(Base):
    __tablename__ = "clients"

    client_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    client_name = Column(String, nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Target(Base):
    __tablename__ = "targets"

    target_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    client_id = Column(String(36), ForeignKey("clients.client_id"), nullable=False)
    target_type = Column(String, nullable=False)
    target_name = Column(String, nullable=False)
    credential_config_encrypted = Column(Text, nullable=True) # JSON string or encrypted text
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
