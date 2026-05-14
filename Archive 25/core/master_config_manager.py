import hashlib
import io
import csv
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from loguru import logger
from core.settings import settings
from core.azure_storage import get_storage_client
from core.mcp_connector import DatasetInfo

# ---------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------
MASTER_CONFIG_S3_KEY = "config/master_config.csv"
MASTER_CONFIG_COLUMNS = [
    # System-filled
    "dataset_id",
    "pipeline_id",
    "client_name",
    "source_type",
    "source_folder",
    "source_object",
    "file_format",
    "raw_layer_path",
    "target_layer_bronze",
    "target_layer_silver",
    "is_active",
    "staging_table",
    "created_at",
    # Human-filled (reserved)
    "load_type",
    "upsert_key",
    "watermark_column",
    "partition_column"
]

from core.utils import generate_dataset_id

class MasterConfigManager:
    def __init__(self):
        self.s3_client = get_storage_client()   # AzureStorageClient (S3-compatible API)
        self.bucket_name = settings.AZURE_CONTAINER_NAME or "datalake"

    def _generate_dataset_id(self, client_name: str, source_type: str, file_path: str) -> str:
        """
        Generates deterministic dataset_id using centralized utility.
        """
        return generate_dataset_id(client_name, source_type, file_path)

    def _get_config_key(self, client_name: str) -> str:
        """
        Returns S3 Key: Blob key: Master_Configuration/<Client_Name>/master_config.csv
        """
        # Sanitize client name slightly?
        clean_client = client_name.strip().replace(" ", "_")
        return f"Master_Configuration/{clean_client}/master_config.csv"

    def _get_existing_config(self, key: str) -> pd.DataFrame:
        """
        Reads existing Master Config CSV from specific S3 Key.
        """
        try:
            logger.info(f"Fetching Master Config from: {key}")
            obj = self.s3_client.get_object(Key=key, Container=self.bucket_name)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()))
            # Ensure all columns exist
            for col in MASTER_CONFIG_COLUMNS:
                if col not in df.columns:
                    df[col] = None
            return df
        except Exception as not_found:
            if "BlobNotFound" in str(not_found) or "ResourceNotFound" in str(not_found) or "404" in str(not_found):
                logger.info(f"No existing Master Config found at {key}. Creating new.")
                return pd.DataFrame(columns=MASTER_CONFIG_COLUMNS)
            raise
        except Exception as e:
            logger.error(f"Failed to read Master Config at {key}: {e}")
            raise e

    def _save_config(self, df: pd.DataFrame, key: str):
        """
        Writes DataFrame to S3 as CSV at specific Key.
        """
        try:
            with io.BytesIO() as buffer:
                df.to_csv(buffer, index=False, encoding="utf-8")
                buffer.seek(0)
                self.s3_client.put_object(
                    Container=self.bucket_name,
                    Key=key,
                    Body=buffer.read(),
                    ContentType="text/csv"
                )
            logger.info(f"Successfully saved Master Config ({len(df)} rows) to az://{self.bucket_name}/{key}")
        except Exception as e:
            logger.error(f"Failed to save Master Config to S3: {e}")
            raise e

    def update_master_config(self, mcp_output: Dict):
        """
        Main entry point. Consumes MCP list -> Updates Client Specific S3 CSV.
        """
        datasets = mcp_output.get("datasets", [])
        if not datasets:
            logger.warning("No datasets provided to update logic.")
            raise ValueError("Input dataset list is empty.")
        
        # Determine Client Name - Critical for Folder Path
        client_name = mcp_output.get("client_name")
        if not client_name:
             raise ValueError("Client Name is mandatory for Master Config update.")

        # 1. Resolve Path & Load Existing
        s3_key = self._get_config_key(client_name)
        df_existing = self._get_existing_config(s3_key)
        
        # Convert existing DF to Dict of Records for easy upsert
        # Key = dataset_id, Status = Exists
        existing_records = {}
        if not df_existing.empty:
            existing_records = df_existing.set_index("dataset_id", drop=False).to_dict("index")

        is_dirty = False
        source_type = mcp_output.get("source_type")
        source_folder = mcp_output.get("source_folder")

        batch_id = mcp_output.get("batch_id")

        for ds in datasets:
            if isinstance(ds, dict):
                file_path = ds["file_path"]
                file_name = ds["file_name"]
                file_format = ds["file_format"]
                ds_client = ds.get("client_name", client_name)
                ds_source = ds.get("source_type", source_type)
                ds_folder = ds.get("source_folder", source_folder)
            else:
                file_path = ds.file_path
                file_name = ds.file_name
                file_format = ds.file_format
                ds_client = ds.client_name
                ds_source = ds.source_type
                ds_folder = source_folder
                
            d_id = self._generate_dataset_id(ds_client, ds_source, file_path)

            sanitized_path = file_path.replace("/", "_").replace(".", "_")
            pipeline_id = f"{ds_client}_{sanitized_path}"
            
            # System Fields that always overwrite
            system_update = {
                "dataset_id": d_id,
                "pipeline_id": pipeline_id,
                "client_name": ds_client,
                "source_type": ds_source,
                "source_folder": ds_folder,

                "source_object": file_name,
                "file_format": file_format,
                "batch_id": batch_id,
                "raw_layer_path": (ds.get("raw_layer_path") if isinstance(ds, dict) else None),
                "target_layer_bronze": f"az://{self.bucket_name}/Bronze/{ds_client}/{file_path}/",
                "target_layer_silver": f"az://{self.bucket_name}/Silver/{ds_client}/{file_path}/",
                # "is_active": False # DO NOT OVERWRITE THIS IF EXISTS
                # "created_at": ... # DO NOT OVERWRITE
            }

            if d_id in existing_records:
                # UPDATE Existing Record
                logger.info(f"Updating existing Dataset {d_id} ({file_name}).")
                # Merge: Update system fields, keep human fields
                
                rec = existing_records[d_id]
                rec.update(system_update)
                # Ensure updated_at? Not in columns currently but good practice
                is_dirty = True
            else:
                # INSERT New Record
                logger.info(f"Adding new Dataset {d_id} ({file_name}).")
                new_row = system_update.copy()
                # Set defaults for new record
                new_row["is_active"] = False
                new_row["created_at"] = datetime.utcnow().isoformat()
                new_row["load_type"] = None
                new_row["upsert_key"] = None
                new_row["watermark_column"] = None
                new_row["partition_column"] = None
                
                existing_records[d_id] = new_row
                is_dirty = True

        if not is_dirty:
            logger.info("Master Config is already up to date. No Azure Blob write needed.")
            return

        # 5. Save back to S3
        # Convert dict back to list -> DF
        final_rows = list(existing_records.values())
        df_final = pd.DataFrame(final_rows)
        
        # Ensure Valid Columns Order
        for col in MASTER_CONFIG_COLUMNS:
            if col not in df_final.columns:
                df_final[col] = None
        
        df_final = df_final[MASTER_CONFIG_COLUMNS]
        
        self._save_config(df_final, s3_key)

        