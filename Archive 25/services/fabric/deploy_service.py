import httpx
import json
import base64
import zipfile
import io
import copy
import logging
from fastapi import HTTPException

from services.fabric.entity_resolver import log_export_context

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

PRESERVE_TP_KEYS = [
    "translator", "columnMapping", "enableStaging", "stagingSettings",
    "parallelCopies", "dataIntegrationUnits", "enableSkipIncompatibleRow",
    "redirectIncompatibleRowSettings", "logSettings", "preserveRules", "preserve",
]

PRESERVE_ACTIVITY_KEYS = ["name", "description", "dependsOn", "policy", "userProperties"]

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

class FabricDeployService:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def deploy_pipeline(self, workspace_id: str, file_bytes: bytes):
        """Automated ZIP-based deployment: Dynamically detects content and deploys"""
        
        # 1. Extract content from ZIP with flexible detection
        try:
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                json_files = [f for f in z.namelist() if f.endswith('.json')]
                
                pipeline_file = None
                manifest_file = None
                
                for f in json_files:
                    if 'manifest.json' in f.lower():
                        manifest_file = f
                    else:
                        # Assume the first other JSON is the pipeline content
                        pipeline_file = f
                
                if not pipeline_file:
                    raise HTTPException(status_code=400, detail="No pipeline JSON file found in ZIP")
                
                pipeline_content = z.read(pipeline_file).decode('utf-8')
                pipeline_name = "New Pipeline"
                
                if manifest_file:
                    manifest_data = json.loads(z.read(manifest_file).decode('utf-8'))
                    pipeline_name = manifest_data.get('displayName', pipeline_name)
                else:
                    # Fallback to filename (without .json)
                    pipeline_name = pipeline_file.split('/')[-1].replace('.json', '')
                
                definition_dict = json.loads(pipeline_content)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to process ZIP: {str(e)}")

        # 2. Check if pipeline exists in workspace
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        async with httpx.AsyncClient(timeout=30.0) as client:
            items_resp = await client.get(f"{url}?type=DataPipeline", headers=self.headers)
            items = items_resp.json().get("value", [])
            existing = next((i for i in items if i['displayName'] == pipeline_name), None)
            
            definition_b64 = base64.b64encode(json.dumps(definition_dict).encode('utf-8')).decode('utf-8')
            
            if existing:
                # Update Definition
                update_url = f"{url}/{existing['id']}/updateDefinition"
                payload = {
                    "definition": {
                        "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                    }
                }
                resp = await client.post(update_url, headers=self.headers, json=payload)
                pipeline_id = existing['id']
            else:
                # Create New
                payload = {
                    "displayName": pipeline_name,
                    "type": "DataPipeline",
                    "definition": {
                        "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                    }
                }
                resp = await client.post(url, headers=self.headers, json=payload)
                pipeline_id = resp.json().get('id') if resp.is_success else None

            if not resp.is_success:
                 raise HTTPException(status_code=resp.status_code, detail=f"Fabric API error: {resp.text}")
            
            return {
                "id": pipeline_id,
                "displayName": pipeline_name,
                "status": "Success"
            }

    async def clone_pipeline(
        self,
        source_workspace_id: str,
        source_pipeline_id: str,
        target_workspace_id: str,
        new_name: str,
        workspace_name: str | None = None,
        pipeline_name: str | None = None,
    ):
        """Clones a pipeline by exporting and then re-importing it with a new name"""
        from services.fabric.pipeline_service import FabricPipelineService
        p_service = FabricPipelineService(self.headers["Authorization"].replace("Bearer ", ""))

        log_export_context(
            workspace_id=source_workspace_id,
            pipeline_item_id=source_pipeline_id,
            workspace_name=workspace_name or "",
            pipeline_name=pipeline_name or "",
            operation="clone_pipeline",
        )
        # 1. Export
        results = await p_service.bulk_export_definitions(source_workspace_id, [source_pipeline_id])
        if source_pipeline_id not in results:
            raise HTTPException(status_code=404, detail="Source pipeline not found or export failed")
        
        files = results[source_pipeline_id]
        pipeline_json = files.get("pipeline.json")
        if not pipeline_json:
            raise HTTPException(status_code=400, detail="Pipeline content missing in export")
        
        definition_dict = json.loads(pipeline_json.decode('utf-8'))
        definition_b64 = base64.b64encode(json.dumps(definition_dict).encode('utf-8')).decode('utf-8')
        
        # 2. Check for name collisions in Target and resolve versioning
        url = f"{FABRIC_API_BASE}/workspaces/{target_workspace_id}/items"
        async with httpx.AsyncClient(timeout=30.0) as client:
            items_resp = await client.get(f"{url}?type=DataPipeline", headers=self.headers)
            pipelines = items_resp.json().get("value", [])
            
            final_name = new_name
            version = 1
            while any(p.get("displayName") == final_name for p in pipelines):
                final_name = f"{new_name}_v{version}"
                version += 1

            # 3. Create in Target
            payload = {
                "displayName": final_name,
                "type": "DataPipeline",
                "definition": {
                    "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                }
            }
            
            resp = await client.post(url, headers=self.headers, json=payload)
            if not resp.is_success:
                raise HTTPException(status_code=resp.status_code, detail=f"Fabric API error during clone: {resp.text}")
            
            resp_json = resp.json()
            return {
                "id": resp_json.get('id'),
                "displayName": final_name,
                "status": "Success"
            }

    async def reuse_pipeline(self, workspace_id: str, pipeline_id: str):
        """Select an existing pipeline for reuse without creating or mutating Fabric artifacts."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}",
                headers=self.headers,
            )
            if not resp.is_success:
                raise HTTPException(status_code=resp.status_code, detail=f"Fabric API error during reuse: {resp.text}")
            item = resp.json()
            return {
                "id": item.get("id") or pipeline_id,
                "displayName": item.get("displayName") or item.get("name"),
                "status": "Success",
                "mode": "reuse",
            }

    async def mutate_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        new_name: str,
        mode: str,
        source_params: dict | None = None,
        sink_params: dict | None = None,
        source_connection_name: str | None = None,
        sink_connection_name: str | None = None,
        template_pipeline_id: str | None = None,
        workspace_name: str | None = None,
        pipeline_name: str | None = None,
    ):
        """Clone a pipeline with source/sink mutation, matching pipe_chg_src MutationEngine.clone."""
        from services.fabric.pipeline_service import FabricPipelineService
        p_service = FabricPipelineService(self.headers["Authorization"].replace("Bearer ", ""))

        log_export_context(
            workspace_id=workspace_id,
            pipeline_item_id=pipeline_id,
            workspace_name=workspace_name or "",
            pipeline_name=pipeline_name or "",
            operation=f"mutate_pipeline_{mode}",
        )
        results = await p_service.bulk_export_definitions(workspace_id, [pipeline_id])
        if pipeline_id not in results:
            raise HTTPException(status_code=404, detail="Source pipeline not found or export failed")

        pipeline_json = results[pipeline_id].get("pipeline.json")
        if not pipeline_json:
            raise HTTPException(status_code=400, detail="Pipeline content missing in export")

        content = json.loads(pipeline_json.decode("utf-8"))
        logger.debug("Loaded exported pipeline content for mutation: workspace=%s pipeline=%s", workspace_id, pipeline_id)
        idx, original_activity = self._get_copy_activity_with_index(content)
        logger.debug("Original copy activity name=%s id=%s", original_activity.get("name"), original_activity.get("type"))
        mutated = copy.deepcopy(original_activity)
        resolved_resources = {}

        if mode in ("source", "both"):
            if not source_params:
                raise HTTPException(status_code=400, detail="source_params required")
            source_type = source_params.get("connector_type") or self._detect_connector_type(original_activity, "source")
            if source_type == "REST":
                connection_id = await self._resolve_rest_connection_id(
                    workspace_id,
                    source_params,
                    source_connection_name,
                    template_pipeline_id,
                    preferred_role="source",
                    require_existing=False,
                )
                mutated["typeProperties"]["source"] = self._build_rest_source(
                    connection_id,
                    source_params.get("relative_url", ""),
                    source_params.get("request_method") or source_params.get("method", "GET"),
                    source_params.get("request_timeout", "00:01:40"),
                )
                old_inputs = original_activity.get("inputs", [])
                if old_inputs and old_inputs[0].get("referenceName"):
                    resolved_resources["remove_src_ds"] = old_inputs[0]["referenceName"]
                mutated.pop("inputs", None)
            else:
                endpoint, resources = await self._build_connector_endpoint(
                    source_params,
                    "source",
                    source_type,
                    workspace_id,
                    key_prefix="src",
                )
                mutated["typeProperties"]["source"] = endpoint
                resolved_resources.update(resources)
                old_inputs = original_activity.get("inputs", [])
                if old_inputs and old_inputs[0].get("referenceName"):
                    resolved_resources["remove_src_ds"] = old_inputs[0]["referenceName"]

        if mode in ("sink", "both"):
            if not sink_params:
                raise HTTPException(status_code=400, detail="sink_params required")
            # Determine the selected sink type (from params) or detect actual from the exported pipeline
            selected_sink_type = (sink_params.get("connector_type") or "").strip()
            detected_sink_type = self._detect_connector_type(original_activity, "sink")
            sink_type = selected_sink_type or detected_sink_type

            # Debug logging: exported activities and detected connector types
            try:
                logger.debug("Exported pipeline activities: %s", [a.get("name") for a in content.get("properties", {}).get("activities", [])])
                logger.debug("Detected sink connector type: %s (selected: %s)", detected_sink_type, selected_sink_type)
                # Log linked services / dataset connection references and found connection IDs
                conns = []
                for activity in content.get("properties", {}).get("activities", []):
                    tp = activity.get("typeProperties", {})
                    for role in ("source", "sink"):
                        endpoint = tp.get(role) or {}
                        conn = ((endpoint.get("datasetSettings") or {}).get("externalReferences") or {}).get("connection")
                        if conn:
                            conns.append({"activity": activity.get("name"), "role": role, "connector_type": endpoint.get("type"), "connection_id": conn})
                logger.debug("Detected linked connections: %s", conns)
                logger.debug("Pipeline linkedServices: %s", content.get("properties", {}).get("linkedServices", []))
                logger.debug("Pipeline datasets: %s", content.get("properties", {}).get("datasets", []))
            except Exception:
                logger.debug("Failed logging exported activities")

            # If the caller explicitly selected a sink type, validate it matches the actual pipeline sink
            if selected_sink_type and selected_sink_type.upper() != detected_sink_type.upper():
                raise HTTPException(status_code=400, detail=f"Selected sink type {selected_sink_type} does not match actual pipeline sink type {detected_sink_type}")

            if sink_type == "REST":
                # For sink mutations we require an existing REST connection on the referenced/template pipeline
                connection_id = await self._resolve_rest_connection_id(
                    workspace_id,
                    sink_params,
                    sink_connection_name,
                    template_pipeline_id,
                    preferred_role="sink",
                    require_existing=True,
                )
                mutated["typeProperties"]["sink"] = self._build_rest_sink(
                    connection_id,
                    sink_params.get("relative_url", ""),
                    sink_params.get("request_method") or sink_params.get("method", "POST"),
                    sink_params.get("request_timeout", "00:01:40"),
                )
                old_outputs = original_activity.get("outputs", [])
                if old_outputs and old_outputs[0].get("referenceName"):
                    resolved_resources["remove_snk_ds"] = old_outputs[0]["referenceName"]
                mutated.pop("outputs", None)
            else:
                original_sink = (original_activity.get("typeProperties", {}) or {}).get("sink", {})
                mutated["typeProperties"]["sink"] = self._mutate_existing_sink(
                    original_sink,
                    sink_params,
                    sink_type,
                )
                logger.debug("Original sink definition: %s", json.dumps(original_sink, indent=2, default=str)[:4000])
                logger.debug("Mutated sink definition: %s", json.dumps(mutated["typeProperties"]["sink"], indent=2, default=str)[:4000])

        final_activity = self._preserve_activity_properties(original_activity, mutated)
        cloned = copy.deepcopy(content)
        cloned["properties"]["activities"][idx] = final_activity
        logger.debug("Final mutated pipeline activity: %s", json.dumps(final_activity, indent=2, default=str)[:4000])
        self._inject_reference_resources(cloned, resolved_resources)
        logger.debug("Final mutated pipeline payload for import: %s", json.dumps(cloned, indent=2, default=str)[:4000])

        result = await self._create_pipeline_from_definition(workspace_id, new_name, cloned)
        return {
            "status": "success",
            "cloned_pipeline_id": result.get("id"),
            "cloned_display_name": result.get("displayName"),
            "mutation_mode": mode,
            "resolved_resources": list(resolved_resources.keys()),
            **result,
        }

    def _get_copy_activity_with_index(self, definition_dict: dict) -> tuple[int, dict]:
        for idx, activity in enumerate(definition_dict.get("properties", {}).get("activities", [])):
            if activity.get("type") == "Copy":
                return idx, activity
        raise HTTPException(status_code=400, detail="No Copy activity found in pipeline")

    def _find_copy_activity(self, definition_dict: dict) -> dict:
        for activity in definition_dict.get("properties", {}).get("activities", []):
            if activity.get("type") == "Copy":
                return activity
        raise HTTPException(status_code=400, detail="No Copy activity found in pipeline")

    def _detect_connector_type(self, activity: dict, role: str) -> str:
        connector_type = activity.get("typeProperties", {}).get(role, {}).get("type", "")
        type_map = SOURCE_TYPE_MAP if role == "source" else SINK_TYPE_MAP
        return type_map.get(connector_type, "REST")

    async def _build_connector_endpoint(self, params: dict, role: str, fallback_type: str, workspace_id: str, key_prefix: str | None = None) -> tuple[dict, dict]:
        connector_type = str(params.get("connector_type") or params.get("type") or fallback_type or "REST").strip()
        connector_type_upper = connector_type.upper()
        if connector_type_upper in {"REST", "REST_API"}:
            return await self._build_rest_endpoint(params, role, workspace_id), {}
        if connector_type_upper in {"SQL", "SQLSERVER", "SQL_SERVER", "AZURESQL", "AZURE_SQL"}:
            return self._build_sql_endpoint(params, role), self._adapter_resources("sql", params, role, key_prefix)
        if connector_type_upper in {"ADLS", "AZURE", "AZURE_ADLS"}:
            return self._build_file_endpoint(params, role, "ADLS"), self._adapter_resources("adls", params, role, key_prefix)
        if connector_type_upper in {"S3", "AWS", "AMAZON_S3"}:
            return self._build_file_endpoint(params, role, "S3"), self._adapter_resources("s3", params, role, key_prefix)
        if connector_type_upper in {"LAKEHOUSE", "ONELAKE"}:
            return self._build_file_endpoint(params, role, "Lakehouse"), self._adapter_resources("lakehouse", params, role, key_prefix)
        if connector_type_upper in {"JSON"}:
            return self._build_file_endpoint(params, role, "JSON"), self._adapter_resources("json", params, role, key_prefix)
        if connector_type_upper in {"PARQUET"}:
            return self._build_file_endpoint(params, role, "Parquet"), self._adapter_resources("parquet", params, role, key_prefix)
        if connector_type_upper in {"CSV", "DELIMITEDTEXT", "DELIMITED_TEXT"}:
            return self._build_file_endpoint(params, role, "DelimitedText"), self._adapter_resources("delimitedtext", params, role, key_prefix)
        raise HTTPException(status_code=400, detail=f"Unsupported {role} connector_type: {connector_type}")

    def _mutate_existing_sink(self, original_sink: dict, sink_params: dict, sink_type: str) -> dict:
        """Mutate the existing exported sink definition without discarding pipeline structure."""
        mutated = copy.deepcopy(original_sink or {})
        dataset_settings = mutated.setdefault("datasetSettings", {})
        type_props = dataset_settings.setdefault("typeProperties", {})

        # Preserve sink type wrapper, only update fields that were explicitly passed.
        if sink_params.get("connection_id") or sink_params.get("connection_reference"):
            ext_refs = dataset_settings.setdefault("externalReferences", {})
            ext_refs["connection"] = sink_params.get("connection_id") or sink_params.get("connection_reference")

        location = type_props.setdefault("location", {})
        if sink_params.get("file_name") is not None:
            location["fileName"] = sink_params["file_name"]
        if sink_params.get("folder_path") is not None:
            location["folderPath"] = sink_params["folder_path"]
        if sink_params.get("file_system") is not None:
            location["fileSystem"] = sink_params["file_system"]
        if sink_params.get("bucket") is not None:
            location["bucketName"] = sink_params["bucket"]
        if sink_params.get("container") is not None:
            location["fileSystem"] = sink_params["container"]
        if sink_params.get("root_path") is not None:
            location["rootFolderPath"] = sink_params["root_path"]
        if sink_params.get("delimiter") is not None:
            type_props["columnDelimiter"] = sink_params["delimiter"]
        if sink_params.get("first_row_as_header") is not None:
            type_props["firstRowAsHeader"] = bool(sink_params["first_row_as_header"])
        if sink_params.get("encoding") is not None:
            type_props["encodingName"] = sink_params["encoding"]

        format_settings = mutated.setdefault("formatSettings", {})
        if sink_params.get("file_extension") is not None:
            format_settings["fileExtension"] = sink_params["file_extension"]
        if sink_params.get("quote_all") is not None:
            format_settings["quoteAllText"] = bool(sink_params["quote_all"])
        if sink_params.get("file_pattern") is not None:
            format_settings["filePattern"] = sink_params["file_pattern"]

        if format_settings:
            mutated["formatSettings"] = format_settings

        if sink_type.upper() == "SQL":
            if sink_params.get("write_behavior") is not None:
                mutated["writeBehavior"] = sink_params["write_behavior"]
            if sink_params.get("use_table_lock") is not None:
                mutated["sqlWriterUseTableLock"] = bool(sink_params["use_table_lock"])
            if sink_params.get("table_option") is not None:
                mutated["tableOption"] = sink_params["table_option"]
            if sink_params.get("disable_metrics") is not None:
                mutated["disableMetricsCollection"] = bool(sink_params["disable_metrics"])

        return mutated

    def _adapter_resources(self, prefix: str, params: dict, role: str, key_prefix: str | None) -> dict:
        resources = self._dataset_resources(prefix, params, role)
        if key_prefix not in {"src", "snk"}:
            return resources
        return {
            f"{key_prefix}_ls": resources.get(f"{role}_ls"),
            f"{key_prefix}_ds": resources.get(f"{role}_ds"),
            f"{key_prefix}_ls_name": resources.get(f"{role}_ls_name"),
            f"{key_prefix}_ds_name": resources.get(f"{role}_ds_name"),
        }

    def _build_rest_source(self, connection_id: str, relative_url: str, request_method: str = "GET", request_timeout: str = "00:01:40") -> dict:
        return {
            "type": "RestSource",
            "httpRequestTimeout": request_timeout,
            "requestMethod": request_method,
            "datasetSettings": {
                "type": "RestResource",
                "typeProperties": {"relativeUrl": relative_url},
                "externalReferences": {"connection": connection_id},
            },
        }

    def _build_rest_sink(self, connection_id: str, relative_url: str, request_method: str = "POST", request_timeout: str = "00:01:40") -> dict:
        return {
            "type": "RestSink",
            "httpRequestTimeout": request_timeout,
            "requestMethod": request_method,
            "datasetSettings": {
                "type": "RestResource",
                "typeProperties": {"relativeUrl": relative_url},
                "externalReferences": {"connection": connection_id},
            },
        }

    async def _resolve_rest_connection_id(
        self,
        workspace_id: str,
        params: dict,
        connection_name: str | None,
        template_pipeline_id: str | None,
        preferred_role: str = "source",
        require_existing: bool = False,
    ) -> str:
        # Prefer explicit connection id passed in params
        if params.get("connection_id"):
            return params["connection_id"]

        # Try to resolve from a referenced/template pipeline first
        ref_pipeline = template_pipeline_id or params.get("ref_pipeline_id")
        if ref_pipeline:
            # extract connection ids and prefer role matches (source/sink)
            connection_id = await self._extract_rest_connection_id(workspace_id, ref_pipeline, preferred_role)
            if connection_id:
                return connection_id
            # If caller requires an existing connection on the referenced pipeline, fail clearly
            if require_existing:
                raise HTTPException(status_code=400, detail=f"No connection UUID found in pipeline {ref_pipeline}.")

        # If an explicit base_url is provided, allow creating a dynamic REST connection
        base_url = params.get("base_url") or params.get("url")
        if base_url and not require_existing:
            return self._create_rest_connection(connection_name or "Dynamic_REST_Connection", base_url)

        # Nothing we can do: return clear error
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot resolve Fabric REST connection UUID. Provide connection_id or a reference pipeline with a REST connection."
            ),
        )

    async def _build_rest_endpoint(self, params: dict, role: str, workspace_id: str) -> dict:
        connection_id = params.get("connection_id") or params.get("connection_reference")
        if not connection_id and params.get("ref_pipeline_id"):
            connection_id = await self._extract_rest_connection_id(workspace_id, params["ref_pipeline_id"], role)
        if not connection_id and (params.get("base_url") or params.get("url")):
            connection_id = self._create_rest_connection(
                params.get("connection_name") or f"Dynamic_REST_{role.title()}_Connection",
                params.get("base_url") or params.get("url"),
            )
        if not connection_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{role}_params.connection_id is required unless ref_pipeline_id "
                    "or base_url is provided for dynamic REST connection resolution"
                ),
            )
        return {
            "type": "RestSource" if role == "source" else "RestSink",
            "httpRequestTimeout": params.get("request_timeout", "00:01:40"),
            "requestMethod": params.get("request_method") or params.get("method") or ("GET" if role == "source" else "POST"),
            "datasetSettings": {
                "type": "RestResource",
                "typeProperties": {"relativeUrl": params.get("relative_url", "")},
                "externalReferences": {"connection": connection_id},
            },
        }

    async def _extract_rest_connection_id(self, workspace_id: str, pipeline_id: str, preferred_role: str) -> str | None:
        # Inspect the exported definition directly so no connection-list permission is needed.
        from services.fabric.pipeline_service import FabricPipelineService

        token = self.headers["Authorization"].replace("Bearer ", "")
        service = FabricPipelineService(token)
        results = await service.bulk_export_definitions(workspace_id, [pipeline_id])
        pipeline_json = results.get(pipeline_id, {}).get("pipeline.json")
        if not pipeline_json:
            return None
        definition = json.loads(pipeline_json.decode("utf-8"))
        refs = []
        for activity in definition.get("properties", {}).get("activities", []):
            type_props = activity.get("typeProperties", {})
            for role in ("source", "sink"):
                endpoint = type_props.get(role) or {}
                conn = ((endpoint.get("datasetSettings") or {}).get("externalReferences") or {}).get("connection")
                if conn:
                    refs.append({"role": role, "connection_id": conn, "connector_type": endpoint.get("type", "")})
        preferred = [ref for ref in refs if ref["role"] == preferred_role and "Rest" in ref["connector_type"]]
        return (preferred or refs or [{}])[0].get("connection_id")

    def _create_rest_connection(self, display_name: str, base_url: str) -> str:
        payload = {
            "connectivityType": "ShareableCloud",
            "displayName": display_name,
            "connectionDetails": {
                "type": "Web",
                "creationMethod": "Web",
                "parameters": [{"dataType": "Text", "name": "url", "value": base_url}],
            },
            "privacyLevel": "Organizational",
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection": True,
                "credentials": {"credentialType": "Anonymous"},
            },
        }
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(f"{FABRIC_API_BASE}/connections", headers=self.headers, json=payload)
        if not resp.is_success:
            raise HTTPException(status_code=resp.status_code, detail=f"REST connection creation failed: {resp.text}")
        connection_id = resp.json().get("id")
        if not connection_id:
            raise HTTPException(status_code=502, detail="REST connection creation did not return an id")
        return connection_id

    def _build_sql_endpoint(self, params: dict, role: str) -> dict:
        if role == "source":
            endpoint = {
                "type": params.get("fabric_source_type", "AzureSqlSource"),
                "queryTimeout": params.get("query_timeout", "02:00:00"),
                "partitionOption": params.get("partition_option", "None"),
            }
            if params.get("query"):
                endpoint["sqlReaderQuery"] = params["query"]
            return endpoint
        return {
            "type": params.get("fabric_sink_type", "AzureSqlSink"),
            "writeBehavior": params.get("write_behavior", "insert"),
            "sqlWriterUseTableLock": bool(params.get("use_table_lock", False)),
            "tableOption": params.get("table_option", "none"),
            "disableMetricsCollection": bool(params.get("disable_metrics", False)),
        }

    def _build_file_endpoint(self, params: dict, role: str, format_name: str) -> dict:
        if format_name == "S3":
            normalized = params.get("format", "Parquet")
            endpoint_type = f"{normalized}Source" if role == "source" else f"{normalized}Sink"
            store_prefix = "AmazonS3"
            endpoint = {
                "type": endpoint_type,
                "datasetSettings": self._inline_dataset_settings(params, role, normalized, "AmazonS3Location"),
                "storeSettings": {"type": f"{store_prefix}{'Read' if role == 'source' else 'Write'}Settings"},
            }
            return endpoint
        normalized = "DelimitedText" if format_name in {"CSV", "DelimitedText"} else format_name
        if normalized == "Lakehouse" and params.get("root_path") == "Tables":
            return {
                "type": "LakehouseTableSource" if role == "source" else "LakehouseTableSink",
                "datasetSettings": self._inline_dataset_settings(params, role, "LakehouseTable", "LakehouseTableLocation"),
                **({"tableActionOption": params.get("table_action", "Append")} if role == "sink" else {}),
            }
        endpoint_type = f"{normalized}Source" if role == "source" else f"{normalized}Sink"
        store_prefix = "Lakehouse" if normalized == "Lakehouse" else "AzureBlobFS"
        if normalized == "Lakehouse":
            normalized = params.get("format", "Parquet")
            endpoint_type = f"{normalized}Source" if role == "source" else f"{normalized}Sink"
        endpoint = {"type": endpoint_type}
        dataset_type = {
            "DelimitedTextSource": "DelimitedText",
            "DelimitedTextSink": "DelimitedText",
            "JsonSource": "Json",
            "JsonSink": "Json",
            "ParquetSource": "Parquet",
            "ParquetSink": "Parquet",
        }.get(endpoint_type, normalized)
        endpoint["datasetSettings"] = self._inline_dataset_settings(
            params,
            role,
            dataset_type,
            "LakehouseLocation" if store_prefix == "Lakehouse" else "AzureBlobFSLocation",
        )
        if role == "source":
            endpoint["storeSettings"] = {
                "type": f"{store_prefix}ReadSettings",
                "recursive": bool(params.get("recursive", False)),
            }
            if params.get("wildcard"):
                endpoint["storeSettings"]["wildcardFileName"] = params["wildcard"]
            if endpoint_type == "DelimitedTextSource":
                endpoint["formatSettings"] = {"type": "DelimitedTextReadSettings"}
            elif endpoint_type == "JsonSource":
                endpoint["formatSettings"] = {"type": "JsonReadSettings"}
        else:
            endpoint["storeSettings"] = {"type": f"{store_prefix}WriteSettings"}
            if endpoint_type == "DelimitedTextSink":
                endpoint["formatSettings"] = {
                    "type": "DelimitedTextWriteSettings",
                    "quoteAllText": bool(params.get("quote_all", False)),
                    "fileExtension": params.get("file_extension", ".csv"),
                }
            elif endpoint_type == "JsonSink":
                endpoint["formatSettings"] = {"type": "JsonWriteSettings", "filePattern": params.get("file_pattern", "setOfObjects")}
            elif endpoint_type == "ParquetSink":
                endpoint["formatSettings"] = {"type": "ParquetWriteSettings"}
        return endpoint

    def _inline_dataset_settings(self, params: dict, role: str, dataset_type: str, location_type: str) -> dict:
        location = {
            "type": location_type,
            "fileName": params.get("file_name"),
            "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
            "fileSystem": params.get("file_system") or params.get("container") or params.get("bucket"),
            "rootFolderPath": params.get("root_path"),
        }
        location = {key: value for key, value in location.items() if value not in (None, "")}
        type_properties = {"location": location}
        if dataset_type == "DelimitedText":
            type_properties.update({
                "columnDelimiter": params.get("delimiter", ","),
                "firstRowAsHeader": bool(params.get("first_row_as_header", True)),
                "encodingName": params.get("encoding", "UTF-8"),
            })
        if dataset_type == "LakehouseTable":
            type_properties = {"table": params.get("table", ""), "schema": params.get("schema", "")}
            type_properties = {key: value for key, value in type_properties.items() if value not in (None, "")}

        external_refs = {}
        connection_id = params.get("connection_id") or params.get("connection_reference")
        if connection_id:
            external_refs["connection"] = connection_id

        settings = {
            "type": dataset_type,
            "typeProperties": type_properties,
        }
        if external_refs:
            settings["externalReferences"] = external_refs
        return settings

    def _dataset_resources(self, prefix: str, params: dict, role: str) -> dict:
        ls_name = params.get("linked_service_name") or f"ls_{prefix}_{role}"
        ds_name = params.get("dataset_name") or f"ds_{prefix}_{role}"
        connection_id = params.get("connection_id")
        resources = {
            f"{role}_ls_name": ls_name,
            f"{role}_ds_name": ds_name,
        }

        if prefix == "sql":
            resources[f"{role}_ls"] = {
                "type": params.get("ls_type", "AzureSqlDatabase"),
                "typeProperties": {
                    "connectionString": params.get("connection_string")
                    or f"Server={params.get('server', '')};Database={params.get('database', '')};",
                    **({"connectionId": connection_id} if connection_id else {}),
                },
            }
            resources[f"{role}_ds"] = {
                "type": params.get("dataset_type", "AzureSqlTable"),
                "linkedServiceName": {"referenceName": ls_name, "type": "LinkedServiceReference"},
                "typeProperties": {"schema": params.get("schema", "dbo"), "table": params.get("table", "")},
            }
            return resources

        if prefix == "lakehouse":
            resources[f"{role}_ls"] = {
                "type": "Lakehouse",
                "typeProperties": {
                    "workspaceId": params.get("workspace_id", ""),
                    "artifactId": params.get("lakehouse_id") or params.get("artifact_id", ""),
                    **({"connectionId": connection_id} if connection_id else {}),
                },
            }
            if params.get("root_path") == "Tables":
                resources[f"{role}_ds"] = {
                    "type": "LakehouseTable",
                    "linkedServiceName": {"referenceName": ls_name, "type": "LinkedServiceReference"},
                    "typeProperties": {"table": params.get("table", "")},
                }
            else:
                resources[f"{role}_ds"] = self._file_dataset(params, ls_name, params.get("format", "Parquet"), "LakehouseLocation")
            return resources

        format_type = {
            "json": "Json",
            "parquet": "Parquet",
            "delimitedtext": "DelimitedText",
        }.get(prefix, params.get("format", "Parquet"))
        is_s3 = prefix == "s3"
        resources[f"{role}_ls"] = {
            "type": "AmazonS3" if is_s3 else "AzureBlobFS",
            "typeProperties": {
                **(
                    {
                        "bucketName": params.get("bucket") or params.get("bucket_name") or "",
                        "region": params.get("region") or "",
                    }
                    if is_s3
                    else {"url": params.get("url") or params.get("account_url") or ""}
                ),
                **({"connectionId": connection_id} if connection_id else {}),
            },
        }
        resources[f"{role}_ds"] = self._file_dataset(params, ls_name, format_type, "AmazonS3Location" if is_s3 else "AzureBlobFSLocation")
        return resources

    def _file_dataset(self, params: dict, linked_service_name: str, dataset_type: str, location_type: str) -> dict:
        type_properties = {
            "location": {
                "type": location_type,
                "fileName": params.get("file_name"),
                "folderPath": params.get("folder_path"),
                "fileSystem": params.get("file_system") or params.get("container"),
                "rootFolderPath": params.get("root_path"),
            }
        }
        if dataset_type == "DelimitedText":
            type_properties.update({
                "columnDelimiter": params.get("delimiter", ","),
                "firstRowAsHeader": bool(params.get("first_row_as_header", True)),
                "encodingName": params.get("encoding", "UTF-8"),
            })
        if dataset_type == "Parquet" and params.get("compression"):
            type_properties["compressionCodec"] = params["compression"]
        return {
            "type": dataset_type,
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": type_properties,
        }

    def _preserve_copy_activity_settings(self, original: dict, mutated: dict) -> None:
        for key in PRESERVE_ACTIVITY_KEYS:
            if key in original and key not in mutated:
                mutated[key] = original[key]
        original_tp = original.get("typeProperties", {})
        mutated_tp = mutated.setdefault("typeProperties", {})
        for key in PRESERVE_TP_KEYS:
            if key in original_tp and key not in mutated_tp:
                mutated_tp[key] = original_tp[key]

    def _preserve_activity_properties(self, original: dict, mutated: dict) -> dict:
        result = copy.deepcopy(mutated)
        for key in PRESERVE_ACTIVITY_KEYS:
            if key in original:
                result.setdefault(key, copy.deepcopy(original[key]))
        original_tp = original.get("typeProperties", {})
        result_tp = result.setdefault("typeProperties", {})
        for key in PRESERVE_TP_KEYS:
            if key in original_tp:
                result_tp.setdefault(key, copy.deepcopy(original_tp[key]))
        if "policy" in original:
            result.setdefault("policy", copy.deepcopy(original["policy"]))
        return result

    def _inject_reference_resources(self, content: dict, resolved: dict) -> None:
        props = content.setdefault("properties", {})
        remove_ds_names = {resolved[key] for key in ("remove_src_ds", "remove_snk_ds") if resolved.get(key)}
        remove_ls_names = set()
        if remove_ds_names:
            kept_datasets = []
            for dataset in props.get("datasets", []):
                if dataset.get("name") in remove_ds_names:
                    linked_service = (
                        ((dataset.get("properties") or {}).get("linkedServiceName") or {}).get("referenceName")
                        or ((dataset.get("linkedServiceName") or {}).get("referenceName"))
                    )
                    if linked_service:
                        remove_ls_names.add(linked_service)
                else:
                    kept_datasets.append(dataset)
            props["datasets"] = kept_datasets
            if remove_ls_names:
                props["linkedServices"] = [
                    item for item in props.get("linkedServices", [])
                    if item.get("name") not in remove_ls_names
                ]

        linked_services = props.setdefault("linkedServices", [])
        datasets = props.setdefault("datasets", [])
        ls_names = {item.get("name") for item in linked_services}
        ds_names = {item.get("name") for item in datasets}
        for prefix in ("src", "snk"):
            ls = resolved.get(f"{prefix}_ls")
            ds = resolved.get(f"{prefix}_ds")
            ls_name = resolved.get(f"{prefix}_ls_name")
            ds_name = resolved.get(f"{prefix}_ds_name")
            if ls and ls_name and ls_name not in ls_names:
                linked_services.append({"name": ls_name, **ls})
                ls_names.add(ls_name)
            if ds and ds_name and ds_name not in ds_names:
                datasets.append({"name": ds_name, **ds})
                ds_names.add(ds_name)
        props.pop("connections", None)
        content.pop("connections", None)

    def _inject_resources(self, definition_dict: dict, resources: dict) -> None:
        props = definition_dict.setdefault("properties", {})
        linked_services = props.setdefault("linkedServices", [])
        datasets = props.setdefault("datasets", [])
        remove_dataset_names = {
            name for name in (resources.get("remove_source_ds"), resources.get("remove_sink_ds")) if name
        }
        if remove_dataset_names:
            remove_linked_services = set()
            kept_datasets = []
            for dataset in datasets:
                if dataset.get("name") in remove_dataset_names:
                    linked_service = (
                        ((dataset.get("properties") or {}).get("linkedServiceName") or {}).get("referenceName")
                        or ((dataset.get("linkedServiceName") or {}).get("referenceName"))
                    )
                    if linked_service:
                        remove_linked_services.add(linked_service)
                else:
                    kept_datasets.append(dataset)
            datasets[:] = kept_datasets
            if remove_linked_services:
                linked_services[:] = [
                    item for item in linked_services if item.get("name") not in remove_linked_services
                ]
        ls_names = {item.get("name") for item in linked_services}
        ds_names = {item.get("name") for item in datasets}
        for role in ("source", "sink"):
            ls = resources.get(f"{role}_ls")
            ls_name = resources.get(f"{role}_ls_name")
            ds = resources.get(f"{role}_ds")
            ds_name = resources.get(f"{role}_ds_name")
            if ls and ls_name and ls_name not in ls_names:
                linked_services.append({"name": ls_name, **ls})
                ls_names.add(ls_name)
            if ds and ds_name and ds_name not in ds_names:
                datasets.append({"name": ds_name, **ds})
                ds_names.add(ds_name)
        props.pop("connections", None)
        definition_dict.pop("connections", None)

    def _mark_old_dataset_for_removal(self, resources: dict, activity: dict, role: str) -> None:
        ref_key = "inputs" if role == "source" else "outputs"
        resource_key = f"remove_{role}_ds"
        refs = activity.get(ref_key) or []
        if refs and isinstance(refs[0], dict) and refs[0].get("referenceName"):
            resources[resource_key] = refs[0]["referenceName"]

    async def _create_pipeline_from_definition(self, workspace_id: str, requested_name: str, definition_dict: dict):
        definition_b64 = base64.b64encode(json.dumps(definition_dict).encode("utf-8")).decode("utf-8")
        items_url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        async with httpx.AsyncClient(timeout=30.0) as client:
            items_resp = await client.get(f"{items_url}?type=DataPipeline", headers=self.headers)
            pipelines = items_resp.json().get("value", [])
            final_name = requested_name
            version = 1
            while any(p.get("displayName") == final_name for p in pipelines):
                final_name = f"{requested_name}_v{version}"
                version += 1

            payload = {
                "displayName": final_name,
                "type": "DataPipeline",
                "definition": {
                    "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                },
            }
            resp = await client.post(items_url, headers=self.headers, json=payload)
            if not resp.is_success:
                raise HTTPException(status_code=resp.status_code, detail=f"Fabric API error during mutation: {resp.text}")
            body = resp.json()
            return {"id": body.get("id"), "displayName": final_name, "status": "Success", "fabric_response": body}
