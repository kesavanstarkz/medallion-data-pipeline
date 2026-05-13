import pandas as pd
from sqlalchemy import create_engine, text
import json
import logging
from typing import Any, Dict, List, Optional
import io
from datetime import datetime

logger = logging.getLogger(__name__)

class TargetSaveService:
    @staticmethod
    def _get_conf(config: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
        """Helper to get a configuration value using multiple case-insensitive key fallbacks."""
        if not config: return default
        lower_config = {str(k).lower(): v for k, v in config.items()}
        for key in keys:
            if key.lower() in lower_config:
                return lower_config[key.lower()]
        return default

    @staticmethod
    def _get_engine(target_type: str, config: Dict[str, Any]):
        """Creates a SQLAlchemy engine for the target."""
        try:
            # Common parameters with fallbacks
            user = TargetSaveService._get_conf(config, ["User", "UserName", "UID", "user", "userid"])
            password = TargetSaveService._get_conf(config, ["Password", "PWD", "pwd", "password"])
            host = TargetSaveService._get_conf(config, ["Host", "Server", "server", "host"])
            database = TargetSaveService._get_conf(config, ["Database", "DatabaseName", "DB", "dbname"])
            
            if target_type == "PostgreSQL":
                port = TargetSaveService._get_conf(config, ["Port"], 5432)
                if not all([user, password, host, database]):
                    missing = [k for k, v in {"user":user, "password":password, "host":host, "database":database}.items() if not v]
                    raise ValueError(f"Missing mandatory PostgreSQL parameters: {', '.join(missing)}")
                url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
                return create_engine(url)
            elif target_type in ["SQL Server", "Azure Synapse", "Fabric Warehouse"]:
                driver = TargetSaveService._get_conf(config, ["Driver"], "{ODBC Driver 18 for SQL Server}")
                if not all([user, password, host, database]):
                    missing = [k for k, v in {"user":user, "password":password, "host":host, "database":database}.items() if not v]
                    raise ValueError(f"Missing mandatory SQL Server parameters: {', '.join(missing)}")
                conn_str = f"DRIVER={driver};SERVER={host};DATABASE={database};UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"
                url = f"mssql+pyodbc:///?odbc_connect={conn_str}"
                return create_engine(url)
            elif target_type == "MySQL":
                port = TargetSaveService._get_conf(config, ["Port"], 3306)
                url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
                return create_engine(url)
            elif target_type == "Snowflake":
                account = TargetSaveService._get_conf(config, ["Account", "AccountName"])
                schema = TargetSaveService._get_conf(config, ["Schema"], "PUBLIC")
                warehouse = TargetSaveService._get_conf(config, ["Warehouse"])
                role = TargetSaveService._get_conf(config, ["Role"])
                url = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
                return create_engine(url)
            elif target_type == "Redshift":
                port = TargetSaveService._get_conf(config, ["Port"], 5439)
                url = f"redshift+psycopg2://{user}:{password}@{host}:{port}/{database}"
                return create_engine(url)
        except Exception as e:
            logger.error(f"Failed to create engine for {target_type}: {str(e)}")
            raise e
        return None

    @staticmethod
    def _sanitize_identifier(name: str) -> str:
        """Sanitizes an identifier for SQL (replaces spaces/special chars with underscores)."""
        import re
        if not name:
            return "unknown"
        # Replace non-alphanumeric with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9]', '_', str(name).strip())
        # Collapse multiple underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Ensure it starts with a letter if possible
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
        return sanitized.lower()

    @staticmethod
    async def save_to_target(
        target_type: str,
        config: Dict[str, Any],
        rows: List[Dict[str, Any]],
        columns: List[str],
        save_mode: str,
        table_name: str,
        schema_name: Optional[str] = None,
        primary_key: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Orchestrates saving data to various target types.
        """
        logger.info(f"Saving to {target_type} in mode {save_mode} into table {table_name}")
        
        try:
            # Handle case where rows might be empty
            if not rows and save_mode not in ["Create Table Only", "Save Schema Only"]:
                return {"status": "SUCCESS", "message": "No data rows provided to save."}

            df = pd.DataFrame(rows)
            
            # Database targets using SQLAlchemy
            is_sql_target = target_type in ["PostgreSQL", "SQL Server", "MySQL", "Snowflake", "Azure Synapse", "Fabric Warehouse", "Redshift"]
            
            if columns:
                # Ensure all requested columns exist in DF
                for col in columns:
                    if col not in df.columns:
                        df[col] = None
                df = df[columns]
            
            if is_sql_target:
                # Sanitize column names for SQL targets
                sanitized_columns = [TargetSaveService._sanitize_identifier(c) for c in df.columns]
                df.columns = sanitized_columns
                if columns:
                    columns = [TargetSaveService._sanitize_identifier(c) for c in columns]
                if primary_key:
                    primary_key = TargetSaveService._sanitize_identifier(primary_key)
            
            full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
            
            if is_sql_target:
                engine = TargetSaveService._get_engine(target_type, config)
                if not engine:
                    raise ValueError(f"No driver or engine mapping available for target type: {target_type}")
                
                with engine.begin() as conn:
                    if save_mode == "Save Schema Only":
                        df.head(0).to_sql(table_name, conn, schema=schema_name, if_exists='replace', index=False)
                        return {"status": "SUCCESS", "message": f"Table schema created in {target_type}."}
                    
                    if save_mode == "Create Table Only":
                        df.head(0).to_sql(table_name, conn, schema=schema_name, if_exists='fail', index=False)
                        return {"status": "SUCCESS", "message": f"Empty table '{table_name}' created."}
                    
                    if_exists = 'append' if save_mode in ["Append to Existing Table", "Save Full Data"] else 'replace'
                    
                    if save_mode == "Upsert / Merge" and primary_key:
                        # Simple upsert logic: delete existing then insert
                        if primary_key in df.columns:
                            pk_vals = df[primary_key].dropna().unique().tolist()
                            if pk_vals:
                                pk_list = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in pk_vals])
                                try:
                                    conn.execute(text(f"DELETE FROM {full_table_name} WHERE {primary_key} IN ({pk_list})"))
                                except Exception as de:
                                    logger.warning(f"Delete before upsert failed (table might not exist): {str(de)}")
                    
                    df.to_sql(table_name, conn, schema=schema_name, if_exists=if_exists, index=False)
                
                return {
                    "status": "SUCCESS",
                    "message": "Data successfully saved to target.",
                    "rows_written": len(df),
                    "columns_mapped": len(columns),
                    "target_table": full_table_name
                }

            # File/Object Storage Targets
            elif target_type in ["AWS S3", "Azure ADLS", "OneLake"]:
                format_ext = options.get("format", "parquet").lower() if options else "parquet"
                
                output = io.BytesIO()
                if format_ext == "csv":
                    df.to_csv(output, index=False)
                elif format_ext == "json":
                    df.to_json(output, orient='records', indent=2)
                else:
                    df.to_parquet(output, index=False)
                
                output.seek(0)
                
                if target_type == "AWS S3":
                    import boto3
                    aws_key = TargetSaveService._get_conf(config, ["AWS Access Key", "aws_access_key_id", "access_key"])
                    aws_secret = TargetSaveService._get_conf(config, ["AWS Secret Key", "aws_secret_access_key", "secret_key"])
                    region = TargetSaveService._get_conf(config, ["Region", "region_name", "region"], "us-east-1")
                    bucket = TargetSaveService._get_conf(config, ["Bucket Name", "Bucket", "bucket", "bucket_name"])
                    
                    if not bucket:
                        raise ValueError("AWS S3 'Bucket Name' is required in configuration.")
                        
                    s3 = boto3.client(
                        's3',
                        aws_access_key_id=aws_key,
                        aws_secret_access_key=aws_secret,
                        region_name=region
                    )
                    key = f"{table_name}.{format_ext}"
                    s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue())
                    return {"status": "SUCCESS", "message": f"File saved to S3: s3://{bucket}/{key}"}
                
                elif target_type == "Azure ADLS":
                    from azure.storage.filedatalake import DataLakeServiceClient
                    acc_name = TargetSaveService._get_conf(config, ["Account Name", "account_name", "account"])
                    acc_key = TargetSaveService._get_conf(config, ["Account Key", "account_key", "key"])
                    container = TargetSaveService._get_conf(config, ["Container Name", "container_name", "container"])
                    
                    if not all([acc_name, acc_key, container]):
                        raise ValueError("Azure ADLS 'Account Name', 'Account Key', and 'Container Name' are required.")
                        
                    service_client = DataLakeServiceClient(
                        account_url=f"https://{acc_name}.dfs.core.windows.net",
                        credential=acc_key
                    )
                    file_system_client = service_client.get_file_system_client(file_system=container)
                    file_client = file_system_client.get_file_client(f"{table_name}.{format_ext}")
                    file_client.upload_data(output.getvalue(), overwrite=True)
                    return {"status": "SUCCESS", "message": f"File saved to ADLS: {table_name}.{format_ext}"}

            return {"status": "SUCCESS", "message": f"Operation completed for {target_type}."}
            
        except Exception as e:
            logger.exception(f"TargetSaveService error: {str(e)}")
            raise e
