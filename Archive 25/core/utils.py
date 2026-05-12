import hashlib
from typing import Tuple

def generate_dataset_id(client_name: str, source_type: str, file_path: str) -> str:
    """
    Standardized Dataset ID generation across the entire platform.
    Uses case-insensitive hashing of Client + Source Type + Path for high reliability.
    """
    client = client_name.lower().strip()
    src = source_type.upper().strip()
    path = file_path.lower().strip().replace(" ", "_")
    
    # Combined string: e.g. "lc1LOCALanalytics_data_2.csv"
    raw = f"{client}{src}{path}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def parse_s3_url(url: str) -> Tuple[str, str]:
    """
    Parses S3 URLs in multiple formats:
      1. s3://bucket/key
      2. s3://bucket
      3. https://bucket.s3.region.amazonaws.com/key
      4. https://s3.region.amazonaws.com/bucket/key
    
    Returns (bucket, key).
    """
    url = url.strip().replace("s3:// ", "s3://")
    
    if url.startswith("s3://"):
        rest = url[len("s3://"):]
        parts = rest.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    if "amazonaws.com" in url and "s3" in url:
        # Format 3: https://bucket.s3.region.amazonaws.com/key
        # OR Format 4: https://s3.region.amazonaws.com/bucket/key
        from urllib.parse import urlparse
        parsed = urlparse(url)
        netloc = parsed.netloc # bucket.s3.region.amazonaws.com OR s3.region.amazonaws.com
        path = parsed.path.lstrip("/") # bucket/key OR key
        
        if netloc.startswith("s3."):
            # Format 4
            parts = path.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        else:
            # Format 3
            bucket = netloc.split(".s3", 1)[0]
            key = path
        return bucket, key

    # Fallback: assume bucket/key
    parts = url.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


# ─── Preview Data Service ────────────────────────────────────────────────────
# Provides direct Spark preview without notebook artifact orchestration

from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from loguru import logger
import re
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None


class PreviewDataService:
    """
    Direct Spark preview execution via ABFSS without notebook artifacts.
    Dynamically resolves workspace/lakehouse IDs and executes Spark read.
    """
    
    def __init__(self):
        self.logger = logger
        self.spark = self._get_spark_session() if HAS_SPARK else None
    
    def _get_spark_session(self):
        """Get or create Spark session for preview."""
        try:
            if HAS_SPARK:
                spark = SparkSession.builder.appName("PreviewDataService").getOrCreate()
                return spark
        except Exception as e:
            self.logger.warning(f"Failed to get Spark session: {e}")
        return None
    
    def preview(self, dataset_id: str, sample_rows: int = 20, db: Session = None) -> Dict[str, Any]:
        """Execute preview without notebook artifacts."""
        try:
            metadata = self._resolve_metadata(dataset_id, db)
            workspace_id = self._resolve_workspace_id(metadata)
            lakehouse_id = self._resolve_lakehouse_id(metadata)
            resolved_path = self._resolve_file_path(metadata)
            source_type = metadata.get("source_type", "UNKNOWN").upper()
            
            self.logger.info(f"Preview: {dataset_id} -> {source_type}, ws={workspace_id}, lh={lakehouse_id}")
            
            if source_type not in ["FABRIC", "ADLS", "S3", "CSV", "PARQUET"]:
                return self._error_response(f"Unsupported source: {source_type}", "unsupported_source_type")
            
            abfss_path = self._build_abfss_path(workspace_id, lakehouse_id, resolved_path)
            delimiter = self._detect_delimiter(metadata)
            has_header = self._detect_header(metadata)
            file_format = self._detect_file_format(metadata, resolved_path)
            
            self.logger.info(f"ABFSS path: {abfss_path}, format: {file_format}, delimiter: {delimiter}")
            
            preview_result = self._execute_spark_preview(abfss_path, delimiter, has_header, sample_rows, file_format)
            
            if not preview_result.get("success"):
                return preview_result
            
            return {
                "success": True,
                "preview_supported": True,
                "dataset_id": dataset_id,
                "source_type": source_type,
                "abfss_path": abfss_path,
                "workspace_id": workspace_id,
                "lakehouse_id": lakehouse_id,
                "columns": preview_result.get("schema", []),
                "rows": preview_result.get("rows", []),
                "row_count": preview_result.get("row_count", 0),
                "total_sampled": len(preview_result.get("rows", [])),
                "delimiter": delimiter,
                "header": has_header
            }
            
        except ValueError as e:
            self.logger.error(f"Validation error: {e}")
            return self._error_response(str(e), "validation_error")
        except Exception as e:
            self.logger.error(f"Preview failed: {e}", exc_info=True)
            return self._error_response(str(e), "preview_execution_failed")
    
    def _resolve_metadata(self, dataset_id: str, db: Session) -> Dict[str, Any]:
        """Resolve dataset metadata from database."""
        if not db:
            raise ValueError("Database session required")
        
        try:
            from models.master_config_authoritative import MasterConfigAuthoritative
            mc = db.query(MasterConfigAuthoritative).filter(
                MasterConfigAuthoritative.dataset_id == dataset_id,
                MasterConfigAuthoritative.is_active == True
            ).first()
            
            if mc:
                return {
                    "dataset_id": dataset_id,
                    "source_type": mc.source_type,
                    "source_folder": mc.source_folder,
                    "source_object": mc.source_object,
                    "file_format": getattr(mc, 'file_format', 'CSV').upper(),
                    "raw_layer_path": getattr(mc, 'raw_layer_path', None),
                    "client_name": mc.client_name,
                    "workspace_id": getattr(mc, 'workspace_id', None),
                    "lakehouse_id": getattr(mc, 'lakehouse_id', None),
                }
        except Exception as e:
            self.logger.debug(f"MasterConfigAuthoritative lookup failed: {e}")
        
        try:
            from models.api_source_config import APISourceConfig
            asc = db.query(APISourceConfig).filter(APISourceConfig.is_active == True).first()
            
            if asc:
                return {
                    "dataset_id": dataset_id,
                    "source_type": asc.source_type or "API",
                    "client_name": asc.client_name,
                    "workspace_id": getattr(asc, 'workspace_id', None),
                    "lakehouse_id": getattr(asc, 'lakehouse_id', None),
                    "file_format": "CSV",
                }
        except Exception as e:
            self.logger.debug(f"APISourceConfig lookup failed: {e}")
        
        raise ValueError(f"Metadata not found for dataset_id: {dataset_id}")
    
    def _resolve_workspace_id(self, metadata: Dict[str, Any]) -> str:
        """Resolve workspace_id from metadata or environment."""
        if metadata.get("workspace_id"):
            return metadata["workspace_id"]
        
        import os
        env_workspace = os.getenv("FABRIC_WORKSPACE_ID")
        if env_workspace:
            return env_workspace
        
        raw_path = metadata.get("raw_layer_path", "")
        if raw_path and "@onelake.dfs.fabric.microsoft.com" in raw_path:
            match = re.search(r'([a-f0-9-]+)@onelake\.dfs\.fabric\.microsoft\.com', raw_path)
            if match:
                return match.group(1)
        
        raise ValueError("Cannot resolve workspace_id. Set FABRIC_WORKSPACE_ID or store in metadata.")
    
    def _resolve_lakehouse_id(self, metadata: Dict[str, Any]) -> str:
        """Resolve lakehouse_id from metadata or environment."""
        if metadata.get("lakehouse_id"):
            return metadata["lakehouse_id"]
        
        import os
        env_lakehouse = os.getenv("FABRIC_LAKEHOUSE_ID")
        if env_lakehouse:
            return env_lakehouse
        
        raw_path = metadata.get("raw_layer_path", "")
        if raw_path and "/Files/" in raw_path:
            match = re.search(r'/([a-f0-9-]+)/Files/', raw_path)
            if match:
                return match.group(1)
        
        raise ValueError("Cannot resolve lakehouse_id. Set FABRIC_LAKEHOUSE_ID or store in metadata.")
    
    def _resolve_file_path(self, metadata: Dict[str, Any]) -> str:
        """Resolve relative file path within lakehouse."""
        raw_path = metadata.get("raw_layer_path", "")
        if raw_path:
            if "@onelake.dfs.fabric.microsoft.com" in raw_path:
                match = re.search(r'/Files/(.*)', raw_path)
                if match:
                    return match.group(1)
            else:
                return raw_path
        
        source_folder = (metadata.get("source_folder") or "").strip("/").strip()
        source_object = metadata.get("source_object", "")
        
        if source_folder and source_object:
            return f"{source_folder}/{source_object}"
        elif source_object:
            return source_object
        
        raise ValueError("Cannot resolve file path. Ensure source_object or raw_layer_path is set.")
    
    def _build_abfss_path(self, workspace_id: str, lakehouse_id: str, resolved_path: str) -> str:
        """Build ABFSS path: abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Files/{path}"""
        if not workspace_id or not lakehouse_id or not resolved_path:
            raise ValueError(f"Missing parameters for ABFSS path")
        
        resolved_path = resolved_path.strip("/")
        resolved_path = re.sub(r'^Files/?', '', resolved_path)
        resolved_path = re.sub(r'/Files/?', '/', resolved_path)
        
        abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{resolved_path}"
        
        if not re.match(r'abfss://[a-f0-9-]+@onelake\.dfs\.fabric\.microsoft\.com/[a-f0-9-]+/Files/.+', abfss_path):
            raise ValueError(f"Invalid ABFSS path: {abfss_path}")
        
        return abfss_path
    
    def _detect_delimiter(self, metadata: Dict[str, Any]) -> str:
        """Detect file delimiter."""
        if metadata.get("delimiter"):
            return metadata["delimiter"]
        
        file_format = metadata.get("file_format", "CSV").upper()
        if file_format in ["TSV", "TAB"]:
            return "\t"
        elif file_format == "PIPE":
            return "|"
        elif file_format == "SEMICOLON":
            return ";"
        
        return ","
    
    def _detect_header(self, metadata: Dict[str, Any]) -> bool:
        """Detect if file has header."""
        if "header" in metadata:
            value = metadata["header"]
            if isinstance(value, bool):
                return value
            return str(value).lower() in ["true", "yes", "1"]
        
        return True
    
    def _detect_file_format(self, metadata: Dict[str, Any], file_path: str) -> str:
        """Detect file format."""
        if metadata.get("file_format"):
            return metadata["file_format"].upper()
        
        if "." in file_path:
            ext = file_path.rsplit(".", 1)[-1].upper()
            if ext in ["CSV", "PARQUET", "JSON", "DELTA", "ORC"]:
                return ext
        
        return "CSV"
    
    def _execute_spark_preview(self, abfss_path: str, delimiter: str, has_header: bool, sample_rows: int = 20, file_format: str = "CSV") -> Dict[str, Any]:
        """Execute Spark preview without notebooks."""
        if not self.spark:
            return self._error_response("Spark not available", "spark_unavailable")
        
        try:
            if file_format == "CSV":
                df = self.spark.read.option("header", "true" if has_header else "false").option("inferSchema", "true").option("delimiter", delimiter).csv(abfss_path)
            elif file_format == "PARQUET":
                df = self.spark.read.parquet(abfss_path)
            elif file_format == "JSON":
                df = self.spark.read.json(abfss_path)
            elif file_format == "DELTA":
                df = self.spark.read.format("delta").load(abfss_path)
            else:
                return self._error_response(f"Unsupported format: {file_format}", "unsupported_format")
            
            schema = [{"name": f.name, "type": str(f.dataType)} for f in df.schema.fields]
            preview_rows = df.limit(sample_rows).toPandas().to_dict(orient="records")
            preview_rows = self._sanitize_for_json(preview_rows)
            
            return {
                "success": True,
                "schema": schema,
                "rows": preview_rows,
                "row_count": len(preview_rows),
                "sample_limited": sample_rows
            }
            
        except Exception as e:
            self.logger.error(f"Spark execution failed: {e}", exc_info=True)
            return self._error_response(f"Spark preview failed: {str(e)}", "spark_execution_failed")
    
    def _sanitize_for_json(self, data: Any) -> Any:
        """Sanitize data for JSON serialization."""
        import numpy as np
        import pandas as pd
        from datetime import datetime
        
        if isinstance(data, dict):
            return {k: self._sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_for_json(v) for v in data]
        elif isinstance(data, float):
            if np.isnan(data) or np.isinf(data):
                return None
            return data
        elif isinstance(data, (np.integer, np.floating)):
            if np.isnan(data) or np.isinf(data):
                return None
            return data.item()
        elif isinstance(data, (pd.Timestamp, datetime)):
            return data.isoformat()
        elif pd.isna(data):
            return None
        else:
            return data
    
    def _error_response(self, message: str, error_code: str) -> Dict[str, Any]:
        """Build error response."""
        return {
            "success": False,
            "preview_supported": False,
            "error": message,
            "error_code": error_code,
            "columns": [],
            "rows": [],
            "row_count": 0
        }


# ─── Direct Spark Preview Service ────────────────────────────────────────────
# Executes direct Spark preview for runtime source discovery (NO notebook artifacts)

class DirectSparkPreviewService:
    """
    Direct Spark preview for runtime source discovery.
    Executes Spark read directly from ABFSS paths without creating notebook artifacts.
    Replaces the notebook-based FabricSparkPreviewService.
    """
    
    def __init__(self):
        self.logger = logger
        self.spark = self._get_spark_session() if HAS_SPARK else None
    
    def _get_spark_session(self):
        """Get or create Spark session for preview."""
        try:
            if HAS_SPARK:
                spark = SparkSession.builder.appName("DirectSparkPreview").getOrCreate()
                return spark
        except Exception as e:
            self.logger.warning(f"Failed to get Spark session: {e}")
        return None
    
    async def execute_preview(self, source_connection: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute preview directly via Spark without any notebook orchestration.
        
        Expected source_connection fields:
        - workspace_id: Fabric workspace ID
        - artifact_id: Lakehouse ID
        - resolved_path: File path within lakehouse
        - format: File format (csv, json, parquet, delta)
        - delimiter: For CSV files (default: ,)
        - header_enabled: Whether CSV has header (default: true)
        - quote_char: For CSV (default: ")
        - escape_char: For CSV (default: \\)
        
        Returns schema_discovery with columns and sample_rows
        """
        diagnostics: List[Dict[str, Any]] = []
        
        try:
            if not self.spark:
                raise ValueError("Spark not available in this environment")
            
            workspace_id = source_connection.get("workspace_id")
            artifact_id = source_connection.get("artifact_id")
            resolved_path = source_connection.get("resolved_path")
            
            if not all([workspace_id, artifact_id, resolved_path]):
                raise ValueError(f"Missing required fields: workspace_id={workspace_id}, artifact_id={artifact_id}, resolved_path={resolved_path}")
            
            diagnostics.append({"step": "metadata_validation", "status": "success"})
            
            # Build ABFSS path
            abfss_path = self._build_abfss_path(workspace_id, artifact_id, resolved_path)
            diagnostics.append({"step": "abfss_path_build", "status": "success", "path": abfss_path})
            
            # Detect format
            detected_format = self._detect_format(source_connection)
            diagnostics.append({"step": "format_detection", "status": "success", "format": detected_format})
            
            # Execute Spark read
            df = self._execute_spark_read(abfss_path, source_connection, detected_format)
            diagnostics.append({"step": "spark_read", "status": "success", "row_count": df.count()})
            
            # Extract schema and sample rows
            schema_discovery = self._extract_schema_discovery(df, source_connection)
            diagnostics.append({
                "step": "schema_extraction", 
                "status": "success",
                "columns": len(schema_discovery.get("columns", [])),
                "sample_rows": len(schema_discovery.get("sample_rows", []))
            })
            
            return {
                "schema_discovery": schema_discovery,
                "preview_mode": "direct_spark",
                "diagnostics": diagnostics,
                "resolved_path": resolved_path,
                "source_name": source_connection.get("file_name") or resolved_path,
            }
            
        except Exception as e:
            self.logger.error(f"Direct Spark preview failed: {e}", exc_info=True)
            diagnostics.append({"step": "execution", "status": "failed", "error": str(e)})
            return {
                "error": f"Preview execution failed: {str(e)}",
                "diagnostics": diagnostics,
                "schema_discovery": {
                    "columns": [],
                    "sample_rows": [],
                    "nullable_columns": [],
                    "primary_key_candidates": [],
                    "timestamp_columns": [],
                }
            }
    
    def _build_abfss_path(self, workspace_id: str, artifact_id: str, resolved_path: str) -> str:
        """Build ABFSS OneLake path."""
        resolved_path = resolved_path.strip("/")
        resolved_path = re.sub(r'^Files/?', '', resolved_path)
        resolved_path = re.sub(r'/Files/?', '/', resolved_path)
        
        abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{artifact_id}/Files/{resolved_path}"
        
        if not re.match(r'abfss://[a-f0-9-]+@onelake\.dfs\.fabric\.microsoft\.com/[a-f0-9-]+/Files/.+', abfss_path):
            raise ValueError(f"Invalid ABFSS path: {abfss_path}")
        
        return abfss_path
    
    def _detect_format(self, source_connection: Dict[str, Any]) -> str:
        """Detect file format."""
        if source_connection.get("format"):
            return source_connection["format"].upper()
        
        resolved_path = source_connection.get("resolved_path", "")
        if "." in resolved_path:
            ext = resolved_path.rsplit(".", 1)[-1].upper()
            if ext in ["CSV", "PARQUET", "JSON", "DELTA", "ORC"]:
                return ext
        
        return "CSV"
    
    def _execute_spark_read(self, abfss_path: str, source_connection: Dict[str, Any], file_format: str):
        """Execute Spark read with appropriate options."""
        try:
            if file_format == "CSV":
                delimiter = source_connection.get("delimiter", ",")
                header = source_connection.get("header_enabled", True)
                quote_char = source_connection.get("quote_char", '"')
                escape_char = source_connection.get("escape_char", "\\")
                
                df = self.spark.read \
                    .option("header", "true" if header else "false") \
                    .option("inferSchema", "true") \
                    .option("delimiter", delimiter) \
                    .option("quote", quote_char) \
                    .option("escape", escape_char) \
                    .csv(abfss_path)
            
            elif file_format == "PARQUET":
                df = self.spark.read.parquet(abfss_path)
            
            elif file_format == "JSON":
                df = self.spark.read.json(abfss_path)
            
            elif file_format == "DELTA":
                df = self.spark.read.format("delta").load(abfss_path)
            
            elif file_format == "ORC":
                df = self.spark.read.orc(abfss_path)
            
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Spark read failed for format {file_format}: {e}")
            raise
    
    def _extract_schema_discovery(self, df, source_connection: Dict[str, Any]) -> Dict[str, Any]:
        """Extract schema and sample rows from DataFrame."""
        # Get schema
        columns = []
        for field in df.schema.fields:
            columns.append({
                "column_name": field.name,
                "data_type": str(field.dataType),
                "nullable": field.nullable,
                "ordinal_position": len(columns) + 1,
            })
        
        # Get sample rows (limit to 25)
        df_limited = df.limit(25)
        pdf = df_limited.toPandas()
        pdf = pdf.where(pd.notnull(pdf), None)
        
        sample_rows = []
        for _, row in pdf.iterrows():
            row_dict = {}
            for col in columns:
                col_name = col["column_name"]
                val = row[col_name] if col_name in row.index else None
                # Sanitize for JSON
                if isinstance(val, (pd.Timestamp, datetime)):
                    val = val.isoformat()
                elif isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    val = None
                elif isinstance(val, (np.integer, np.floating)):
                    if np.isnan(val) or np.isinf(val):
                        val = None
                    else:
                        val = val.item()
                row_dict[col_name] = val
            sample_rows.append(row_dict)
        
        # Identify nullable columns
        nullable_columns = [col["column_name"] for col in columns if col["nullable"]]
        
        # Identify potential timestamp columns
        timestamp_columns = [
            col["column_name"] for col in columns 
            if "timestamp" in col["data_type"].lower() or "date" in col["data_type"].lower()
        ]
        
        return {
            "columns": columns,
            "sample_rows": sample_rows,
            "nullable_columns": nullable_columns,
            "timestamp_columns": timestamp_columns,
            "primary_key_candidates": [],  # Could be enhanced with constraints from Spark
        }

