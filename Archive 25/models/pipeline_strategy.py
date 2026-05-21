"""
Pipeline Strategy Model

Defines the available strategies for pipeline manipulation in Microsoft Fabric:
- Reuse: Use existing pipeline as-is with updated metadata/parameters
- Clone: Duplicate the pipeline within workspace for this execution
- ModifySource: Replace pipeline source connector/configuration
- ModifySink: Replace pipeline sink connector/configuration
- CreateNew: Deploy a completely new pipeline from scratch (default Agentic DE flow)
"""

from enum import Enum
from pydantic import BaseModel
from typing import Optional, Dict, Any


class PipelineStrategy(str, Enum):
    """Available strategies for pipeline execution"""
    REUSE = "reuse"
    CLONE = "clone"
    MODIFY_SOURCE = "modify_source"
    MODIFY_SINK = "modify_sink"
    CREATE_NEW = "create_new"


class PipelineStrategyConfig(BaseModel):
    """Configuration for selected strategy"""
    strategy: PipelineStrategy
    workspace_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    clone_name: Optional[str] = None
    source_params: Optional[Dict[str, Any]] = None
    sink_params: Optional[Dict[str, Any]] = None
    source_type: Optional[str] = None
    sink_type: Optional[str] = None
    source_connection_name: Optional[str] = None
    sink_connection_name: Optional[str] = None
    template_pipeline_id: Optional[str] = None


class ConnectorType(str, Enum):
    """Supported connector types for pipeline operations"""
    DELIMITED_TEXT = "DelimitedText"
    CSV = "CSV"
    REST = "REST"
    SQL = "SQL"
    ADLS = "ADLS"
    LAKEHOUSE = "Lakehouse"
    JSON = "JSON"
    PARQUET = "Parquet"


class MutationMode(str, Enum):
    """Mode for pipeline mutation"""
    SOURCE = "source"
    SINK = "sink"
    BOTH = "both"


class PipelineCloneResult(BaseModel):
    """Result of cloning a pipeline"""
    pipeline_id: str
    display_name: str
    mode: str
    resolved_keys: list


class PipelineInspectionResult(BaseModel):
    """Result of inspecting a pipeline"""
    activity_name: str
    detected_source: str
    detected_sink: str
    source_connection_id: Optional[str] = None
    sink_connection_id: Optional[str] = None
    has_translator: bool
    has_mappings: bool
    policy: Dict[str, Any]
    depends_on: list
    parallel_copies: Optional[int] = None
    data_integration_units: Optional[int] = None


class ConnectionInfo(BaseModel):
    """Information about a Fabric connection"""
    connection_id: str
    activity: str
    role: str  # 'source' or 'sink'
    connector_type: str
