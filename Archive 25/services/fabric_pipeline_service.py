"""
Fabric Pipeline Service

Provides high-level operations for manipulating Microsoft Fabric pipelines:
- Clone pipelines with source/sink mutation
- Inspect pipelines to detect connector types
- Reuse existing pipelines
- Create new pipelines

This service integrates the mutation engine logic with the Agentic DE Platform.
"""

import base64
import copy
import json
import logging
import os
from typing import Optional, Dict, Any, Tuple
from enum import Enum
from services.fabric_client import FabricAPIError

logger = logging.getLogger(__name__)

# Map Fabric source types to canonical names
SOURCE_TYPE_MAP = {
    "DelimitedTextSource": "DelimitedText",
    "CsvSource": "CSV",
    "RestSource": "REST",
    "AzureSqlSource": "SQL",
    "SqlServerSource": "SQL",
    "AzureSqlDWSource": "SQL",
    "AzureBlobFSSource": "ADLS",
    "LakehouseTableSource": "Lakehouse",
    "LakehouseSource": "Lakehouse",
    "JsonSource": "JSON",
    "ParquetSource": "Parquet",
}

SINK_TYPE_MAP = {
    "DelimitedTextSink": "DelimitedText",
    "CsvSink": "CSV",
    "RestSink": "REST",
    "AzureSqlSink": "SQL",
    "SqlServerSink": "SQL",
    "AzureSqlDWSink": "SQL",
    "AzureBlobFSSink": "ADLS",
    "LakehouseTableSink": "Lakehouse",
    "LakehouseSink": "Lakehouse",
    "JsonSink": "JSON",
    "ParquetSink": "Parquet",
}

PRESERVE_TP_KEYS = [
    "translator", "columnMapping", "enableStaging", "stagingSettings",
    "parallelCopies", "dataIntegrationUnits", "enableSkipIncompatibleRow",
    "redirectIncompatibleRowSettings", "logSettings", "preserveRules", "preserve",
]

PRESERVE_ACTIVITY_KEYS = ["name", "description", "dependsOn", "policy", "userProperties"]


class MutationMode(str, Enum):
    """Mode for pipeline mutation"""
    SOURCE = "source"
    SINK = "sink"
    BOTH = "both"


class PipelineDefinitionError(Exception):
    """Error in pipeline definition"""
    pass


class FabricPipelineService:
    """
    Service for manipulating Microsoft Fabric pipelines
    
    Integrates with the Fabric API client to provide high-level
    pipeline operations including clone, inspect, and mutation.
    """
    
    def __init__(self, fabric_client):
        """Initialize with Fabric API client"""
        self.client = fabric_client
    
    # ======================== DECODE/ENCODE ========================
    
    def _decode_pipeline_definition(self, raw_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decode pipeline definition from Fabric API response
        
        Fabric stores pipeline JSON as base64 encoded in the 'payload' field
        within a 'pipeline-content.json' part.
        """
        for part in raw_def.get("definition", {}).get("parts", []):
            if part.get("path") == "pipeline-content.json":
                decoded = base64.b64decode(part["payload"]).decode("utf-8")
                return json.loads(decoded)
        raise PipelineDefinitionError("pipeline-content.json not found in definition")
    
    def _encode_pipeline_definition(self, raw_def: Dict[str, Any], content: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode pipeline definition for Fabric API
        
        Takes the modified pipeline content and re-encodes it as base64
        back into the raw definition structure.
        """
        definition = copy.deepcopy(raw_def.get("definition", {}))
        encoded = base64.b64encode(json.dumps(content).encode("utf-8")).decode("utf-8")
        
        for part in definition.get("parts", []):
            if part.get("path") == "pipeline-content.json":
                part["payload"] = encoded
                part.setdefault("payloadType", "InlineBase64")
                return definition
        
        raise PipelineDefinitionError("pipeline-content.json not found in definition")
    
    # ======================== COPY ACTIVITY HELPERS ========================
    
    def _get_copy_activity(self, content: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Find the Copy activity in the pipeline"""
        for i, activity in enumerate(content.get("properties", {}).get("activities", [])):
            if activity.get("type") == "Copy":
                return i, activity
        raise PipelineDefinitionError("No Copy activity found in pipeline")
    
    def _extract_connection_reference(self, endpoint: Dict[str, Any]) -> Optional[str]:
        """Extract connection UUID from endpoint datasetSettings"""
        ds = endpoint.get("datasetSettings")
        if not isinstance(ds, dict):
            return None
        ext = ds.get("externalReferences")
        if not isinstance(ext, dict):
            return None
        conn = ext.get("connection")
        return conn if isinstance(conn, str) else None
    
    def _extract_all_connection_refs(self, content: Dict[str, Any]) -> list:
        """Extract all connection UUIDs found in pipeline"""
        refs = []
        for activity in content.get("properties", {}).get("activities", []):
            tp = activity.get("typeProperties", {})
            name = activity.get("name", "?")
            
            for role in ("source", "sink"):
                cid = self._extract_connection_reference(tp.get(role, {}))
                if cid:
                    refs.append({
                        "activity": name,
                        "role": role,
                        "connector_type": tp.get(role, {}).get("type", "?"),
                        "connection_id": cid,
                    })
        return refs
    
    # ======================== REST SOURCE/SINK BUILDERS ========================
    
    def _build_rest_source(
        self,
        connection_id: str,
        relative_url: str,
        request_method: str = "GET",
        request_timeout: str = "00:01:40",
    ) -> Dict[str, Any]:
        """Build a REST source block for pipeline"""
        return {
            "type": "RestSource",
            "httpRequestTimeout": request_timeout,
            "requestMethod": request_method,
            "datasetSettings": {
                "type": "RestResource",
                "typeProperties": {
                    "relativeUrl": relative_url,
                },
                "externalReferences": {
                    "connection": connection_id,
                },
            },
        }
    
    def _build_rest_sink(
        self,
        connection_id: str,
        relative_url: str,
        request_method: str = "POST",
        request_timeout: str = "00:01:40",
    ) -> Dict[str, Any]:
        """Build a REST sink block for pipeline"""
        return {
            "type": "RestSink",
            "httpRequestTimeout": request_timeout,
            "requestMethod": request_method,
            "datasetSettings": {
                "type": "RestResource",
                "typeProperties": {
                    "relativeUrl": relative_url,
                },
                "externalReferences": {
                    "connection": connection_id,
                },
            },
        }
    
    # ======================== PRESERVATION & INJECTION ========================
    
    def _preserve_activity_properties(self, original: Dict[str, Any], mutated: Dict[str, Any]) -> Dict[str, Any]:
        """Preserve important activity properties from original"""
        result = copy.deepcopy(mutated)
        
        # Preserve activity-level keys
        for key in PRESERVE_ACTIVITY_KEYS:
            if key in original:
                result.setdefault(key, copy.deepcopy(original[key]))
        
        # Preserve typeProperties keys
        orig_tp = original.get("typeProperties", {})
        res_tp = result.setdefault("typeProperties", {})
        for key in PRESERVE_TP_KEYS:
            if key in orig_tp:
                res_tp.setdefault(key, copy.deepcopy(orig_tp[key]))
        
        # Preserve policy
        if "policy" in original:
            result.setdefault("policy", copy.deepcopy(original["policy"]))
        
        return result
    
    def _inject_resources(self, content: Dict[str, Any], resolved: Dict[str, Any]) -> Dict[str, Any]:
        """Inject resolved linked services and datasets into pipeline content"""
        props = content.setdefault("properties", {})
        
        # Collect dataset/linked service names to remove
        remove_ds_names = set()
        for key in ("remove_src_ds", "remove_snk_ds"):
            if key in resolved:
                remove_ds_names.add(resolved[key])
        
        remove_ls_names = set()
        if remove_ds_names:
            kept_ds = []
            for ds in props.get("datasets", []):
                if ds.get("name") in remove_ds_names:
                    # Track linked service to remove
                    ls_ref = (
                        ds.get("properties", {}).get("linkedServiceName", {}).get("referenceName")
                        or ds.get("linkedServiceName", {}).get("referenceName")
                    )
                    if ls_ref:
                        remove_ls_names.add(ls_ref)
                else:
                    kept_ds.append(ds)
            props["datasets"] = kept_ds
            
            # Remove associated linked services
            if remove_ls_names:
                props["linkedServices"] = [
                    ls for ls in props.get("linkedServices", [])
                    if ls.get("name") not in remove_ls_names
                ]
        
        # Add new linked services and datasets
        ls_list = props.setdefault("linkedServices", [])
        ds_list = props.setdefault("datasets", [])
        ls_names = {ls.get("name") for ls in ls_list}
        ds_names = {ds.get("name") for ds in ds_list}
        
        for prefix in ("src", "snk"):
            ls = resolved.get(f"{prefix}_ls")
            ls_name = resolved.get(f"{prefix}_ls_name")
            ds = resolved.get(f"{prefix}_ds")
            ds_name = resolved.get(f"{prefix}_ds_name")
            
            if ls and ls_name and ls_name not in ls_names:
                ls_list.append({"name": ls_name, **ls})
                ls_names.add(ls_name)
            
            if ds and ds_name and ds_name not in ds_names:
                ds_list.append({"name": ds_name, **ds})
                ds_names.add(ds_name)
        
        # Clean up stale connection metadata
        props.pop("connections", None)
        content.pop("connections", None)
        
        return content
    
    # ======================== PUBLIC OPERATIONS ========================
    
    def inspect(self, workspace_id: str, pipeline_id: str) -> Dict[str, Any]:
        """
        Inspect a pipeline to detect source/sink connector types
        and extract connection information
        """
        try:
            raw = self.client.get_pipeline_definition(workspace_id, pipeline_id)
            content = self._decode_pipeline_definition(raw)
            _, activity = self._get_copy_activity(content)
            tp = activity.get("typeProperties", {})
            
            source_type = self._detect_source_type(activity)
            sink_type = self._detect_sink_type(activity)
            
            return {
                "activity_name": activity.get("name"),
                "detected_source": source_type,
                "detected_sink": sink_type,
                "source_connection_id": self._extract_connection_reference(tp.get("source", {})),
                "sink_connection_id": self._extract_connection_reference(tp.get("sink", {})),
                "has_translator": "translator" in tp,
                "has_mappings": "columnMapping" in tp,
                "policy": activity.get("policy", {}),
                "depends_on": activity.get("dependsOn", []),
                "parallel_copies": tp.get("parallelCopies"),
                "data_integration_units": tp.get("dataIntegrationUnits"),
            }
        except FabricAPIError as e:
            if e.status_code == 403:
                client_id = os.environ.get("FABRIC_CLIENT_ID", "unknown")
                msg = (
                    f"403 Forbidden: Service Principal '{client_id}' lacks access to workspace '{workspace_id}'. "
                    f"Add it as Admin/Member in Fabric workspace settings and enable 'Allow service principals to use Power BI APIs' in tenant developer settings."
                )
                logger.error(msg)
                raise Exception(msg) from e
            logger.error(f"Error inspecting pipeline {pipeline_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inspecting pipeline {pipeline_id}: {e}")
            raise
    
    def extract_connections(self, workspace_id: str, pipeline_id: str) -> Dict[str, Any]:
        """
        Extract all connection UUIDs from a pipeline definition
        
        Does not require connection API permissions, only getDefinition access
        """
        try:
            raw = self.client.get_pipeline_definition(workspace_id, pipeline_id)
            content = self._decode_pipeline_definition(raw)
            refs = self._extract_all_connection_refs(content)
            
            return {
                "pipeline_id": pipeline_id,
                "connections_found": len(refs),
                "connections": refs,
            }
        except FabricAPIError as e:
            if e.status_code == 403:
                client_id = os.environ.get("FABRIC_CLIENT_ID", "unknown")
                msg = (
                    f"403 Forbidden: Service Principal '{client_id}' lacks access to workspace '{workspace_id}'. "
                    f"Add it as Admin/Member in Fabric workspace settings and enable 'Allow service principals to use Power BI APIs' in tenant developer settings."
                )
                logger.error(msg)
                raise Exception(msg) from e
            logger.error(f"Error extracting connections from {pipeline_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error extracting connections from {pipeline_id}: {e}")
            raise

