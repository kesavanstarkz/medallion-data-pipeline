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
from typing import Optional, Dict, Any, Tuple
from enum import Enum

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
        self.connector_mutator = ConnectorMutator(fabric_client)
    
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
        except Exception as e:
            logger.error(f"Error extracting connections from {pipeline_id}: {e}")
            raise
    
    def clone(
        self,
        workspace_id: str,
        pipeline_id: str,
        clone_name: str,
        mode: MutationMode = MutationMode.SOURCE,
        source_type: Optional[str] = None,
        sink_type: Optional[str] = None,
        source_params: Optional[Dict[str, Any]] = None,
        sink_params: Optional[Dict[str, Any]] = None,
        source_connection_name: Optional[str] = None,
        sink_connection_name: Optional[str] = None,
        template_pipeline_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Clone a pipeline with optional source/sink mutation
        
        Supports mutating source, sink, or both based on provided parameters
        """
        try:
            raw = self.client.get_pipeline_definition(workspace_id, pipeline_id)
            content = self._decode_pipeline_definition(raw)
            idx, original_activity = self._get_copy_activity(content)
            mutated = copy.deepcopy(original_activity)
            
            # Validate mutation mode requirements
            if mode in (MutationMode.SOURCE, MutationMode.BOTH) and not source_params:
                raise ValueError("source_params required for SOURCE or BOTH mode")
            if mode in (MutationMode.SINK, MutationMode.BOTH) and not sink_params:
                raise ValueError("sink_params required for SINK or BOTH mode")
            
            # Apply source mutation
            if mode in (MutationMode.SOURCE, MutationMode.BOTH):
                self._mutate_source(
                    mutated, original_activity, workspace_id,
                    source_type, source_params, source_connection_name, template_pipeline_id
                )
            
            # Apply sink mutation
            if mode in (MutationMode.SINK, MutationMode.BOTH):
                self._mutate_sink(
                    mutated, original_activity, workspace_id,
                    sink_type, sink_params, sink_connection_name, template_pipeline_id
                )
            
            # Finalize and create pipeline
            resolved = mutated.pop("_resolved", {})
            final_activity = self._preserve_activity_properties(original_activity, mutated)
            
            cloned_content = copy.deepcopy(content)
            cloned_content["properties"]["activities"][idx] = final_activity
            cloned_content = self._inject_resources(cloned_content, resolved)
            
            definition = self._encode_pipeline_definition(raw, cloned_content)
            result = self.client.create_pipeline(workspace_id, clone_name, definition)
            
            new_pipeline_id = result.get("id")
            if not new_pipeline_id:
                raise PipelineDefinitionError("Created pipeline ID not returned")
            
            self.client.update_pipeline_definition(workspace_id, new_pipeline_id, definition)
            
            return {
                "pipeline_id": new_pipeline_id,
                "display_name": result.get("displayName"),
                "mode": mode.value,
                "resolved_keys": list(resolved.keys()),
            }
        except Exception as e:
            logger.error(f"Error cloning pipeline {pipeline_id}: {e}")
            raise
    
    def reuse(self, workspace_id: str, pipeline_id: str) -> Dict[str, Any]:
        """
        Reuse an existing pipeline as-is
        
        Returns pipeline information without modification
        """
        try:
            result = self.client.get_pipeline(workspace_id, pipeline_id)
            return {
                "pipeline_id": result.get("id"),
                "display_name": result.get("displayName"),
                "mode": "reuse",
                "message": "Pipeline reused as-is",
            }
        except Exception as e:
            logger.error(f"Error reusing pipeline {pipeline_id}: {e}")
            raise
    
    # ======================== HELPER METHODS ========================
    
    def _detect_source_type(self, copy_activity: Dict[str, Any]) -> str:
        """Detect the type of source connector"""
        t = copy_activity.get("typeProperties", {}).get("source", {}).get("type", "")
        detected = SOURCE_TYPE_MAP.get(t)
        if not detected:
            raise ValueError(f"Unknown source type: {t}")
        return detected
    
    def _detect_sink_type(self, copy_activity: Dict[str, Any]) -> str:
        """Detect the type of sink connector"""
        t = copy_activity.get("typeProperties", {}).get("sink", {}).get("type", "")
        detected = SINK_TYPE_MAP.get(t)
        if not detected:
            raise ValueError(f"Unknown sink type: {t}")
        return detected
    
    def _mutate_source(
        self,
        mutated: Dict[str, Any],
        original: Dict[str, Any],
        workspace_id: str,
        source_type: Optional[str],
        source_params: Dict[str, Any],
        connection_name: Optional[str],
        template_pipeline_id: Optional[str],
    ) -> None:
        """Apply source mutation to activity"""
        stype = source_type or self._detect_source_type(original)
        
        if stype == "REST":
            connection_id = self._resolve_rest_connection_id(
                workspace_id, source_params, connection_name, template_pipeline_id
            )
            relative_url = source_params.get("relative_url", "")
            request_method = source_params.get("request_method") or source_params.get("method", "GET")
            request_timeout = source_params.get("request_timeout", "00:01:40")
            
            mutated["typeProperties"]["source"] = self._build_rest_source(
                connection_id, relative_url, request_method, request_timeout
            )
            
            # Track old dataset to remove
            old_inputs = original.get("inputs", [])
            if old_inputs:
                old_ds = old_inputs[0].get("referenceName")
                if old_ds:
                    mutated.setdefault("_resolved", {})["remove_src_ds"] = old_ds
            mutated.pop("inputs", None)
        else:
            # Non-REST connector mutation would go here
            # For now, REST is the primary focus
            logger.warning(f"Non-REST source type {stype} mutation not yet implemented")
    
    def _mutate_sink(
        self,
        mutated: Dict[str, Any],
        original: Dict[str, Any],
        workspace_id: str,
        sink_type: Optional[str],
        sink_params: Dict[str, Any],
        connection_name: Optional[str],
        template_pipeline_id: Optional[str],
    ) -> None:
        """Apply sink mutation to activity"""
        sktype = sink_type or self._detect_sink_type(original)
        
        if sktype == "REST":
            connection_id = self._resolve_rest_connection_id(
                workspace_id, sink_params, connection_name, template_pipeline_id
            )
            relative_url = sink_params.get("relative_url", "")
            request_method = sink_params.get("request_method") or sink_params.get("method", "POST")
            request_timeout = sink_params.get("request_timeout", "00:01:40")
            
            mutated["typeProperties"]["sink"] = self._build_rest_sink(
                connection_id, relative_url, request_method, request_timeout
            )
            
            # Track old dataset to remove
            old_outputs = original.get("outputs", [])
            if old_outputs:
                old_ds = old_outputs[0].get("referenceName")
                if old_ds:
                    mutated.setdefault("_resolved", {})["remove_snk_ds"] = old_ds
            mutated.pop("outputs", None)
        else:
            # Non-REST connector mutation would go here
            logger.warning(f"Non-REST sink type {sktype} mutation not yet implemented")
    
    def _resolve_rest_connection_id(
        self,
        workspace_id: str,
        params: Dict[str, Any],
        connection_name: Optional[str],
        template_pipeline_id: Optional[str],
    ) -> str:
        """
        Resolve a REST connection UUID without calling connection API
        
        Resolution order:
        1. params["connection_id"] - UUID supplied directly
        2. template_pipeline_id - extract from template pipeline definition
        3. params["ref_pipeline_id"] - extract from reference pipeline
        4. Create new connection dynamically if base_url provided
        """
        # Priority 1: Direct UUID
        if params.get("connection_id"):
            return params["connection_id"]
        
        # Priority 2/3: Extract from template or ref pipeline
        ref_pipeline = template_pipeline_id or params.get("ref_pipeline_id")
        if ref_pipeline:
            try:
                connections = self.extract_connections(workspace_id, ref_pipeline)
                if connections.get("connections"):
                    cid = connections["connections"][0]["connection_id"]
                    logger.info(f"Extracted connection_id={cid} from pipeline {ref_pipeline}")
                    return cid
            except Exception as e:
                logger.warning(f"Could not extract connection from {ref_pipeline}: {e}")
        
        # Priority 4: Create new connection
        base_url = params.get("base_url") or params.get("url")
        if not base_url:
            raise ValueError(
                "Cannot resolve REST connection. No connection_id, "
                "template_pipeline_id, or base_url provided."
            )
        
        display_name = connection_name or "Dynamic_REST_Connection"
        logger.info(f"Creating connection '{display_name}' for {base_url}")
        
        resp = self.client.create_rest_connection(
            display_name=display_name,
            base_url=base_url
        )
        cid = resp.get("id")
        if not cid:
            raise ValueError(f"Failed to create REST connection: {resp}")
        
        logger.info(f"Created connection_id={cid}")
        return cid


class ConnectorMutator:
    """Helper class for connector-specific mutations"""
    
    def __init__(self, fabric_client):
        self.client = fabric_client
    
    def resolve_connection(self, adapter, connection_name: str, params: Dict[str, Any]) -> str:
        """Resolve connection for non-REST connectors"""
        # This would handle ADLS, SQL, Lakehouse, etc.
        # For now, placeholder
        return connection_name
