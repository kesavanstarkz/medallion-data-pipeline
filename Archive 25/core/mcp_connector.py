import os
import json
import base64
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from loguru import logger
from core.utils import parse_s3_url

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None


def _build_search_path(client_name: str, folder_path: str) -> str:
    """
    Builds the full ADLS search path without requiring the optional MCP server
    module to be importable during normal backend startup.
    """
    root = os.getenv("ADLS_ROOT_FOLDER", "").strip("/")
    fp = (folder_path or "").strip("/")

    if root and fp.startswith(root + "/"):
        return fp

    if client_name and fp.startswith(client_name + "/"):
        return "/".join([root, fp]) if root else fp

    parts = [p for p in [root, client_name, folder_path] if p and str(p).strip("/")]
    return "/".join(parts)


def _normalize_path(full_path: str, client_name: str, source_type: str) -> str:
    """
    Strips the ADLS root and client prefix from a full blob path. Kept local so
    direct S3/ADLS connectors do not depend on the optional MCP package.
    """
    root = os.getenv("ADLS_ROOT_FOLDER", "").strip("/")
    if root:
        root_prefix = root + "/"
        if full_path.startswith(root_prefix):
            full_path = full_path[len(root_prefix):]

    client_prefix = f"{client_name}/"
    if full_path.startswith(client_prefix):
        full_path = full_path[len(client_prefix):]
    return full_path

# ---------------------------------------------------------
# CONSTANTS & CONFIG
# ---------------------------------------------------------

class SourceType(str, Enum):
    ADLS = "ADLS"
    S3   = "S3"
    API  = "API"

@dataclass
class DatasetInfo:
    """
    Standard Standardized Output for all Connectors
    """
    file_name: str
    file_path: str 
    file_format: str
    file_size: int
    source_type: str
    client_name: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "file_format": self.file_format,
            "file_size": self.file_size,
            "source_type": self.source_type,
            "client_name": self.client_name
        }

# ---------------------------------------------------------
# INTERFACE
# ---------------------------------------------------------

class MCPSourceConnector(ABC):
    """
    Base Interface for MCP Source Connectors.
    Now acts as a Wrapper around the MCP Bridge.
    """
    @abstractmethod
    def list_datasets(self, client_name: str, folder_path: str) -> List[DatasetInfo]:
        pass

    @abstractmethod
    def get_file_content(self, file_path_canonical: str, client_name: str) -> bytes:
        pass

    @abstractmethod
    def list_children(self, client_name: str, folder_path: str) -> Dict[str, Any]:
        pass


# ---------------------------------------------------------
# MCP BRIDGE (Client Logic)
# ---------------------------------------------------------

class MCPBridge:
    """
    Manages the connection to the MCP Server process.
    """
    def __init__(self):
        if not (ClientSession and StdioServerParameters and stdio_client):
            raise RuntimeError(
                "MCP package is not installed. Install requirements.txt or use "
                "ADLS/S3/LOCAL connectors, which do not require MCP."
            )
        
        self.server_script = os.path.join(os.getcwd(), "core", "mcp_server.py")
        
        
        import sys
        self.server_params = StdioServerParameters(
            command=sys.executable,
            args=[self.server_script],
            env=os.environ.copy()
        )
    
    async def _call_tool(self, tool_name: str, arguments: dict) -> Any:
        """
        Connects, calls tool, disconnects. 
        """
        async with stdio_client(self.server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                result = await session.call_tool(tool_name, arguments=arguments)
                
                # FastMCP returns a list of TextContent/ImageContent
                if result.content and hasattr(result.content[0], "text"):
                     text_resp = result.content[0].text
                     try:
                         # Our tool returns JSON string
                         return json.loads(text_resp)
                     except json.JSONDecodeError:
                         return text_resp
                return None

    def run_tool_sync(self, tool_name: str, arguments: dict) -> Any:
        """
        Helper to run async MCP calls synchronously (since existing IngestionService is sync).
        """
        return asyncio.run(self._call_tool(tool_name, arguments))


# ---------------------------------------------------------
# IMPLEMENTATIONS (Proxies)
# ---------------------------------------------------------

class S3Connector(MCPSourceConnector):
    def __init__(self):
        self.source_type = SourceType.S3.value

    def _get_client_and_bucket(self, client_name: str, folder_path: str):
        """
        Helper to extract bucket from path and create a boto3 client 
        using credentials from APISourceConfig DB.
        """
        import boto3
        from core.database import SessionLocal
        from models.api_source_config import APISourceConfig
        from models.master_config_authoritative import MasterConfigAuthoritative
        from core.credential_registry import get_aws_credentials
        
        bucket, prefix = parse_s3_url(folder_path)

        db = SessionLocal()
        try:
            # First try finding by bucket name if we have it
            query = db.query(APISourceConfig).filter(
                APISourceConfig.source_type == "S3",
                APISourceConfig.is_active == True,
            )
            if bucket:
                config = query.filter(
                    APISourceConfig.client_name == client_name,
                    APISourceConfig.aws_bucket_name == bucket,
                ).first() or query.filter(APISourceConfig.aws_bucket_name == bucket).first()
            else:
                # Fallback to finding by client name
                config = query.filter(APISourceConfig.client_name == client_name).first()
                if config:
                    bucket = config.aws_bucket_name
                    if not prefix and config.endpoints:
                        prefix = config.endpoints.strip("/")
            
            if not config and bucket:
                mc = db.query(MasterConfigAuthoritative).filter(
                    MasterConfigAuthoritative.client_name == client_name,
                    MasterConfigAuthoritative.source_type == "S3",
                    MasterConfigAuthoritative.source_folder.like(f"s3://{bucket}%"),
                    MasterConfigAuthoritative.is_active == True,
                ).first()
                if mc:
                    logger.info(f"S3Connector registry fallback matched master config for client={client_name}, bucket={bucket}")
                    config = APISourceConfig(
                        client_name=client_name,
                        source_name=f"{bucket}-derived",
                        source_type="S3",
                        aws_bucket_name=bucket,
                        aws_region="us-east-1",
                        endpoints=prefix,
                        is_active=True,
                    )
            elif not config and not bucket:
                mc = db.query(MasterConfigAuthoritative).filter(
                    MasterConfigAuthoritative.client_name == client_name,
                    MasterConfigAuthoritative.source_type == "S3",
                    MasterConfigAuthoritative.is_active == True,
                ).first()
                if mc and mc.source_folder:
                    bucket, prefix = parse_s3_url(mc.source_folder)
                    logger.info(f"S3Connector derived bucket from master config. client={client_name}, bucket={bucket}, prefix={prefix}")
                    config = query.filter(
                        APISourceConfig.client_name == client_name,
                        APISourceConfig.aws_bucket_name == bucket,
                    ).first() or APISourceConfig(
                        client_name=client_name,
                        source_name=f"{bucket}-derived",
                        source_type="S3",
                        aws_bucket_name=bucket,
                        aws_region="us-east-1",
                        endpoints=prefix,
                        is_active=True,
                    )
            
            if not config:
                raise RuntimeError(f"No S3 configuration found for client '{client_name}' or bucket '{bucket}' in registry.")

            logger.info(f"S3Connector registry lookup: client={client_name}, bucket={bucket}, found_config={bool(config)}, auth_type={getattr(config, 'auth_type', None)}")

            transient = get_aws_credentials(client_name, bucket)
            if transient:
                logger.info(f"S3Connector using transient in-memory AWS credentials for client={client_name}, bucket={bucket}")
                if transient.get("role_arn"):
                    base = boto3.client(
                        'sts',
                        aws_access_key_id=transient.get("aws_access_key_id"),
                        aws_secret_access_key=transient.get("aws_secret_access_key"),
                        aws_session_token=transient.get("aws_session_token"),
                        region_name=transient.get("region_name") or config.aws_region or "us-east-1"
                    )
                    assumed = base.assume_role(RoleArn=transient["role_arn"], RoleSessionName="dea-s3-orchestration")
                    temp = assumed["Credentials"]
                    s3 = boto3.client(
                        's3',
                        aws_access_key_id=temp["AccessKeyId"],
                        aws_secret_access_key=temp["SecretAccessKey"],
                        aws_session_token=temp["SessionToken"],
                        region_name=transient.get("region_name") or config.aws_region or "us-east-1"
                    )
                    return s3, bucket, prefix
                s3 = boto3.client(
                    's3',
                    aws_access_key_id=transient.get("aws_access_key_id"),
                    aws_secret_access_key=transient.get("aws_secret_access_key"),
                    aws_session_token=transient.get("aws_session_token"),
                    region_name=transient.get("region_name") or config.aws_region or "us-east-1"
                )
                return s3, bucket, prefix

            if not config.aws_access_key or not config.aws_secret_key:
                raise RuntimeError(
                    f"S3 configuration found for client '{client_name}' and bucket '{bucket}', "
                    "but credentials are transient and not available in this backend process. Re-run the real AWS scan."
                )
            
            s3 = boto3.client(
                's3',
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_secret_key,
                region_name=config.aws_region or "us-east-1"
            )
            return s3, bucket, prefix
        finally:
            db.close()

    def list_datasets(self, client_name: str, folder_path: str) -> List[DatasetInfo]:
        logger.info(f"S3 Connector: Requesting list_datasets for {client_name} via Direct API")
        
        try:
            s3, bucket, prefix = self._get_client_and_bucket(client_name, folder_path)
            
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            datasets = []
            
            # Supported extensions
            SUPPORTED_EXTENSIONS = ('.csv', '.parquet', '.json', '.xlsx', '.xls', '.tsv')
            
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                # Skip the prefix itself if it's returned as an object (directory placeholder)
                if key.rstrip('/') == prefix.rstrip('/'):
                    continue
                
                name = key.split("/")[-1]
                if not name: 
                    continue
                
                # Filter by extension and size
                if not name.lower().endswith(SUPPORTED_EXTENSIONS):
                    continue
                if obj.get("Size", 0) == 0:
                    continue
                
                ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                canonical_path = f"s3://{bucket}/{key}"
                
                datasets.append(DatasetInfo(
                    file_name   = name,
                    file_path   = canonical_path,
                    file_format = ext.upper() if ext else "UNKNOWN",
                    file_size   = obj.get("Size", 0),
                    source_type = "S3",
                    client_name = client_name,
                ))
            return datasets
        except Exception as e:
            raise RuntimeError(f"S3Connector list_datasets failed: {e}")

    def get_file_content(self, file_path_canonical: str, client_name: str) -> bytes:
        logger.info(f"S3 Connector: Requesting content for {file_path_canonical} via Direct API")
        
        try:
            s3, bucket, key = self._get_client_and_bucket(client_name, file_path_canonical)
            
            obj = s3.get_object(Bucket=bucket, Key=key)
            return obj["Body"].read()
        except Exception as e:
            raise RuntimeError(f"S3Connector get_file_content failed for {file_path_canonical}: {e}")

    def list_children(self, client_name: str, folder_path: str) -> Dict[str, Any]:
        logger.info(f"S3 Connector: Requesting list_children for {folder_path} via Direct API")
        
        try:
            s3, bucket, prefix = self._get_client_and_bucket(client_name, folder_path)
            if prefix and not prefix.endswith("/"):
                prefix += "/"
                
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
            
            # Supported extensions
            SUPPORTED_EXTENSIONS = ('.csv', '.parquet', '.json', '.xlsx', '.xls', '.tsv')
            
            folders = []
            for p in resp.get("CommonPrefixes", []):
                folder_path_s3 = p.get("Prefix")
                name = folder_path_s3.strip("/").split("/")[-1]
                folders.append(name)
                
            files = []
            for c in resp.get("Contents", []):
                key = c.get("Key")
                # Skip the prefix itself (directory placeholder)
                if key.rstrip('/') == prefix.rstrip('/'):
                    continue
                if key == prefix: 
                    continue
                
                name = key.split("/")[-1]
                if not name: 
                    continue
                
                # Filter by extension and size
                if not name.lower().endswith(SUPPORTED_EXTENSIONS):
                    continue
                if c.get("Size", 0) == 0:
                    continue
                
                ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                canonical_path = f"s3://{bucket}/{key}"
                
                files.append({
                    "file_name": name,
                    "file_path": canonical_path,
                    "file_format": ext.upper() if ext else "UNKNOWN",
                    "file_size": c.get("Size", 0)
                })
                
            return {
                "folders": sorted(folders),
                "files": files
            }
        except Exception as e:
            raise RuntimeError(f"S3Connector list_children failed: {e}")


class ADLSConnector(MCPSourceConnector):
    def __init__(self):
        self.source_type = SourceType.ADLS.value

    def list_datasets(self, client_name: str, folder_path: str) -> List[DatasetInfo]:
        from core.azure_storage import AzureStorageClient
        storage = AzureStorageClient()
        
        logger.info(f"ADLS Connector: Requesting list_datasets for {client_name} via Direct API")
        
        container = None
        prefix = folder_path
        
        if folder_path and folder_path.startswith("az://"):
            container, prefix = storage.parse_az_url(folder_path)
            
        container = container or os.getenv("ADLS_CONTAINER_NAME", "ag-de-agent")
        
        try:
            resp = storage.list_objects_v2(Prefix=prefix, Container=container)
            datasets = []
            
            # Supported extensions
            SUPPORTED_EXTENSIONS = ('.csv', '.parquet', '.json', '.xlsx', '.xls', '.tsv')
            
            for obj in resp.get("Contents", []):
                key  = obj["Key"]
                
                # Skip the prefix itself if it's returned as an object (directory placeholder)
                if key.rstrip('/') == prefix.rstrip('/'):
                    continue

                name = key.split("/")[-1]
                if not name:
                    continue
                
                # Skip subdirectories if we only want immediate files
                if "/" in key[len(prefix):].lstrip("/"): 
                    continue 

                # Filter by extension and size
                if not name.lower().endswith(SUPPORTED_EXTENSIONS):
                    continue
                if obj.get("Size", 0) == 0:
                    continue
                
                ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                canonical_path = _normalize_path(key, client_name, "ADLS")
                
                datasets.append(DatasetInfo(
                    file_name   = name,
                    file_path   = canonical_path,
                    file_format = ext.upper() if ext else "UNKNOWN",
                    file_size   = obj.get("Size", 0),
                    source_type = "ADLS",
                    client_name = client_name,
                ))
            return datasets
        except Exception as e:
            raise RuntimeError(f"ADLSConnector list_datasets failed: {e}")

    def get_file_content(self, file_path_canonical: str, client_name: str) -> bytes:
        from core.azure_storage import AzureStorageClient
        storage = AzureStorageClient()
        
        logger.info(f"ADLS Connector: Requesting content for {file_path_canonical} via Direct API")
        
        container = None
        key = file_path_canonical
        
        if file_path_canonical and file_path_canonical.startswith("az://"):
            container, key = storage.parse_az_url(file_path_canonical)
        else:
            key = _build_search_path(client_name, file_path_canonical)
            
        container = container or os.getenv("ADLS_CONTAINER_NAME", "ag-de-agent")
        
        try:
            obj = storage.get_object(Key=key, Container=container)
            return obj["Body"].read()
        except Exception as e:
            raise RuntimeError(f"ADLSConnector get_file_content failed for {file_path_canonical}: {e}")

    def list_children(self, client_name: str, folder_path: str) -> Dict[str, Any]:
        from core.azure_storage import AzureStorageClient
        storage = AzureStorageClient()
        
        logger.info(f"ADLS Connector: Requesting list_children for {folder_path} via Direct API")
        
        container = None
        prefix = folder_path
        
        if folder_path and folder_path.startswith("az://"):
            container, prefix = storage.parse_az_url(folder_path)
            
        container = container or os.getenv("ADLS_CONTAINER_NAME", "ag-de-agent")
        
        if prefix and not prefix.endswith("/"):
            prefix += "/"
            
        try:
            resp = storage.list_objects_v2(Prefix=prefix, Container=container)
            folders_set = set()
            files = []
            
            # Supported extensions
            SUPPORTED_EXTENSIONS = ('.csv', '.parquet', '.json', '.xlsx', '.xls', '.tsv')
            
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                
                # Skip the prefix itself (directory placeholder)
                if key.rstrip('/') == prefix.rstrip('/'):
                    continue
                    
                rel = key[len(prefix):].lstrip("/") if prefix else key
                if not rel:
                    continue
                    
                if "/" in rel:
                    folders_set.add(rel.split("/")[0])
                else:
                    name = key.split("/")[-1]
                    if not name:
                        continue
                        
                    # Filter by extension and size
                    if not name.lower().endswith(SUPPORTED_EXTENSIONS):
                        continue
                    if obj.get("Size", 0) == 0:
                        continue
                        
                    canonical_path = _normalize_path(key, client_name, "ADLS")
                    
                    files.append({
                        "file_name": name,
                        "file_path": canonical_path,
                        "file_format": name.rsplit(".", 1)[-1].upper() if "." in name else "UNKNOWN",
                        "file_size": obj.get("Size", 0)
                    })
                    
            return {
                "folders": sorted(list(folders_set)),
                "files": files
            }
        except Exception as e:
            raise RuntimeError(f"ADLSConnector list_children failed: {e}")





class APIConnector(MCPSourceConnector):
    """
    Connector for REST API sources.
    Calls the API MCP tools (list_api_datasets, get_api_file_content, list_api_children).
    The API is configured entirely through .env variables:
      API_BASE_URL, API_AUTH_TYPE, API_AUTH_TOKEN, API_ENDPOINTS
    """
    def __init__(self):
        self.bridge = MCPBridge()
        self.source_type = SourceType.API.value

    def list_datasets(self, client_name: str, folder_path: str) -> List[DatasetInfo]:
        logger.info(f"APIConnector: list_datasets for {client_name}/{folder_path}")
        
        # Handle multiple endpoints separated by comma, but DON'T split if it's already a full URI
        if folder_path.startswith("az://") or folder_path.startswith("s3://") or folder_path.startswith("http"):
            endpoints = [folder_path.strip()]
        else:
            endpoints = [ep.strip() for ep in folder_path.split(",") if ep.strip()]
        
        all_datasets = []
        
        for ep in endpoints:
            try:
                resp = self.bridge.run_tool_sync(
                    "list_api_datasets",
                    {"source_type": self.source_type, "client_name": client_name, "folder_path": ep}
                )
                if isinstance(resp, dict) and "error" in resp:
                    logger.warning(f"MCP Server Error for endpoint {ep}: {resp['error']}")
                    continue
                if not isinstance(resp, list):
                    logger.warning(f"Unexpected response for {ep}: {resp}")
                    continue
                all_datasets.extend([DatasetInfo(**d) for d in resp])
            except Exception as e:
                logger.error(f"Failed to list datasets for API endpoint {ep}: {e}")
        
        return all_datasets

    def get_file_content(self, file_path_canonical: str, client_name: str) -> bytes:
        logger.info(f"APIConnector: get_file_content for {file_path_canonical}")
        resp = self.bridge.run_tool_sync(
            "get_api_file_content",
            {"source_type": self.source_type, "client_name": client_name, "file_path_canonical": file_path_canonical}
        )
        if isinstance(resp, dict) and "error" in resp:
            raise RuntimeError(f"MCP Server Error: {resp['error']}")
        if isinstance(resp, dict) and "content_base64" in resp:
            return base64.b64decode(resp["content_base64"])
        raise ValueError(f"Unexpected content response: {resp}")

    def list_children(self, client_name: str, folder_path: str) -> Dict[str, Any]:
        resp = self.bridge.run_tool_sync(
            "list_api_children",
            {"source_type": self.source_type, "client_name": client_name, "folder_path": folder_path}
        )
        if isinstance(resp, dict) and "error" in resp:
            raise RuntimeError(f"MCP Server Error: {resp['error']}")
        return resp


# ---------------------------------------------------------
# FACTORY
# ---------------------------------------------------------



# ---------------------------------------------------------
# LOCAL CONNECTOR — reads files already uploaded to Raw layer
# ---------------------------------------------------------

class LocalConnector(MCPSourceConnector):
    """
    Connector for locally uploaded files.
    Files are already in Azure Raw layer via POST /upload/ingest.
    Reads them back from blob so the agent can process them normally.
    """

    def list_datasets(self, client_name: str, folder_path: str) -> List[DatasetInfo]:
        from core.azure_storage import get_storage_client
        from core.settings import settings
        storage   = get_storage_client()
        container = settings.AZURE_CONTAINER_NAME or "ag-de-agent"
        # folder_path for LOCAL = "upload/<client_name>" or a specific batch prefix
        prefix    = folder_path.strip("/") + "/" if folder_path else f"Raw/{client_name}/"
        if not prefix.startswith("Raw/"):
            prefix = f"Raw/{client_name}/"

        try:
            resp = storage.list_objects_v2(Prefix=prefix, Container=container)
            datasets = []
            
            # Supported extensions
            SUPPORTED_EXTENSIONS = ('.csv', '.parquet', '.json', '.xlsx', '.xls', '.tsv')
            
            for obj in resp.get("Contents", []):
                key  = obj["Key"]
                # Skip the prefix itself if it's returned as an object (directory placeholder)
                if key.rstrip('/') == prefix.rstrip('/'):
                    continue

                name = key.split("/")[-1]
                if not name or "." not in name:
                    continue
                
                # Filter by extension and size
                if not name.lower().endswith(SUPPORTED_EXTENSIONS):
                    continue
                if obj.get("Size", 0) == 0:
                    continue

                ext = name.rsplit(".", 1)[-1].lower()
                datasets.append(DatasetInfo(
                    file_name   = name,
                    file_path   = key,
                    file_format = ext.upper(),
                    file_size   = obj.get("Size", 0),
                    source_type = "LOCAL",
                    client_name = client_name,
                ))
            return datasets
        except Exception as e:
            raise RuntimeError(f"LocalConnector list_datasets failed: {e}")

    def get_file_content(self, file_path_canonical: str, client_name: str) -> bytes:
        from core.azure_storage import get_storage_client
        from core.settings import settings
        storage   = get_storage_client()
        container = settings.AZURE_CONTAINER_NAME or "ag-de-agent"
        try:
            obj = storage.get_object(Key=file_path_canonical, Container=container)
            return obj["Body"].read()
        except Exception as e:
            raise RuntimeError(f"LocalConnector get_file_content failed for {file_path_canonical}: {e}")

    def list_children(self, client_name: str, folder_path: str) -> Dict[str, Any]:
        datasets = self.list_datasets(client_name, folder_path)
        return {
            "folders": [],
            "files": [{"file_name": d.file_name, "file_path": d.file_path,
                       "file_format": d.file_format, "file_size": d.file_size}
                      for d in datasets]
        }




class FabricConnector(MCPSourceConnector):
    """
    Connector for Fabric runtime sources.
    Reads from NeonDB staging tables to support DQ discovery and preview.
    """
    def list_datasets(self, client_name: str, folder_path: str) -> List[DatasetInfo]:
        # For Fabric, 'folder_path' is often the staging table name in this context
        return []

    def get_file_content(self, file_path_canonical: str, client_name: str) -> bytes:
        """
        Fabric connector reads either a staging table in Neon/Postgres or a file artifact
        exported to the raw layer. We determine which by probing the database for a table
        name first; if not found, we attempt to read via available cloud/local connectors.
        """
        import psycopg2
        import pandas as pd
        from io import BytesIO
        from core.settings import settings
        # Refer to connector classes defined below in this module (avoid circular import)
        LocalConnector = globals().get('LocalConnector')
        ADLSConnector = globals().get('ADLSConnector')
        S3Connector = globals().get('S3Connector')
        APIConnector = globals().get('APIConnector')

        # Normalize incoming identifier
        key = (file_path_canonical or "").split("/")[-1]
        staging_table = key
        logger.info(f"FabricConnector: resolving {file_path_canonical} (probe as table: {staging_table})")

        # 1) Probe Neon/Postgres for a real table name using to_regclass
        try:
            conn = psycopg2.connect(settings.NEON_DB_URL)
            cur = conn.cursor()
            cur.execute("SELECT to_regclass(%s)", (staging_table,))
            found = cur.fetchone()[0]
            if found:
                logger.info(f"FabricConnector: Detected existing table {staging_table} in Neon, reading via SQL")
                df = pd.read_sql(f"SELECT * FROM {staging_table} LIMIT 100", conn)
                conn.close()
                with BytesIO() as buffer:
                    df.to_csv(buffer, index=False)
                    return buffer.getvalue()
            conn.close()
        except Exception as e:
            # If the DB probe fails, log and continue to try file-based reads
            logger.debug(f"FabricConnector DB probe failed for {staging_table}: {e}")

        # 2) Try to resolve type-based routing by checking authoritative database configuration
        try:
            from core.database import SessionLocal
            from models.master_config_authoritative import MasterConfigAuthoritative
            db = SessionLocal()
            authoritative = None
            try:
                # Query for an authoritative row with matching client and dataset_id or raw path
                query = db.query(MasterConfigAuthoritative).filter(
                    MasterConfigAuthoritative.client_name == client_name,
                    MasterConfigAuthoritative.is_active == True
                )
                
                # Check for direct path match
                authoritative = query.filter(MasterConfigAuthoritative.raw_layer_path == file_path_canonical).first()
                if not authoritative:
                    # Fallback to finding any non-FABRIC active row for this client
                    authoritative = query.filter(MasterConfigAuthoritative.source_type != "FABRIC").first()
            finally:
                db.close()
                
            if authoritative:
                logger.info(f"FabricConnector type-based routing matched registered source_type '{authoritative.source_type}' for client '{client_name}'")
                from core.mcp_connector import get_mcp_connector
                resolved_connector = get_mcp_connector(authoritative.source_type)
                # Ensure we avoid circular invocation of FabricConnector itself
                if resolved_connector.__class__.__name__ != "FabricConnector":
                    target_path = authoritative.raw_layer_path or file_path_canonical
                    return resolved_connector.get_file_content(target_path, client_name)
        except Exception as routing_err:
            logger.debug(f"FabricConnector type-based routing resolution failed: {routing_err}")

        # 3) Fall back to safe type-based heuristics based on path prefixes
        try:
            cp = (file_path_canonical or "")
            if cp.startswith("s3://"):
                c = S3Connector()
                logger.info(f"FabricConnector routing to S3Connector for {file_path_canonical}")
                return c.get_file_content(file_path_canonical, client_name)
            elif cp.startswith("az://") or cp.startswith("azdls://"):
                c = ADLSConnector()
                logger.info(f"FabricConnector routing to ADLSConnector for {file_path_canonical}")
                return c.get_file_content(file_path_canonical, client_name)
            elif "Raw/" in cp or "/" not in cp:
                c = LocalConnector()
                logger.info(f"FabricConnector routing to LocalConnector for {file_path_canonical}")
                return c.get_file_content(file_path_canonical, client_name)
            
            # Absolute fallback: try ADLS and Local
            for c in [LocalConnector(), ADLSConnector()]:
                try:
                    logger.info(f"FabricConnector fallback trying {c.__class__.__name__} for {file_path_canonical}")
                    return c.get_file_content(file_path_canonical, client_name)
                except Exception:
                    continue
            
            raise RuntimeError(f"No connector could read file {file_path_canonical}")
        except Exception as e:
            logger.error(f"FabricConnector failed to read artifact {file_path_canonical}: {e}")
            raise RuntimeError(f"FabricConnector failed: {e}")


    def list_children(self, client_name: str, folder_path: str) -> Dict[str, Any]:
        return {"folders": [], "files": []}


def get_mcp_connector(source_type: str) -> MCPSourceConnector:
    """
    Factory method to get the appropriate connector.
    Supported: ADLS, API, LOCAL, S3, FABRIC
    """
    src = (source_type or "").upper().strip()
    if src in ["ADLS", "AZURE", "AZURE_STORAGE", "AZURESTORAGE"]:
        return ADLSConnector()
    elif src in ["API", "REST", "REST_API", "HTTP", "RESTAPI"]:
        return APIConnector()
    elif src == "LOCAL":
        return LocalConnector()
    elif src in ["S3", "AWS"]:
        return S3Connector()
    elif src in ["FABRIC", "NEON_STAGED_SOURCE"]:
        return FabricConnector()
    else:
        raise ValueError(f"Unsupported source type: {source_type}. Supported: ADLS, API, LOCAL, S3, FABRIC")
