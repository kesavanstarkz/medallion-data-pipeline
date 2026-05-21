import httpx
import json
import base64
import zipfile
import io
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

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

    async def clone_pipeline(self, source_workspace_id: str, source_pipeline_id: str, target_workspace_id: str, new_name: str):
        """Clones a pipeline by exporting and then re-importing it with a new name"""
        from services.fabric.pipeline_service import FabricPipelineService
        p_service = FabricPipelineService(self.headers["Authorization"].replace("Bearer ", ""))
        
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
    ):
        """Clone a pipeline while replacing the Copy activity source and/or sink blocks."""
        from services.fabric.pipeline_service import FabricPipelineService
        p_service = FabricPipelineService(self.headers["Authorization"].replace("Bearer ", ""))

        results = await p_service.bulk_export_definitions(workspace_id, [pipeline_id])
        if pipeline_id not in results:
            raise HTTPException(status_code=404, detail="Source pipeline not found or export failed")

        pipeline_json = results[pipeline_id].get("pipeline.json")
        if not pipeline_json:
            raise HTTPException(status_code=400, detail="Pipeline content missing in export")

        definition_dict = json.loads(pipeline_json.decode("utf-8"))
        activity = self._find_copy_activity(definition_dict)
        type_props = activity.setdefault("typeProperties", {})

        if mode in ("source", "both"):
            type_props["source"] = self._build_rest_endpoint(source_params or {}, "source")
            activity.pop("inputs", None)
        if mode in ("sink", "both"):
            type_props["sink"] = self._build_rest_endpoint(sink_params or {}, "sink")
            activity.pop("outputs", None)

        return await self._create_pipeline_from_definition(workspace_id, new_name, definition_dict)

    def _find_copy_activity(self, definition_dict: dict) -> dict:
        for activity in definition_dict.get("properties", {}).get("activities", []):
            if activity.get("type") == "Copy":
                return activity
        raise HTTPException(status_code=400, detail="No Copy activity found in pipeline")

    def _build_rest_endpoint(self, params: dict, role: str) -> dict:
        connection_id = params.get("connection_id") or params.get("connection_reference")
        if not connection_id:
            raise HTTPException(status_code=400, detail=f"{role}_params.connection_id is required")
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
