from __future__ import annotations
import copy
from abc import ABC, abstractmethod
from typing import Any, Optional

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


def detect_source_type(copy_activity: dict) -> str:
    t = copy_activity.get("typeProperties", {}).get("source", {}).get("type", "")
    detected = SOURCE_TYPE_MAP.get(t)
    if not detected:
        raise ValueError(f"Unknown source type: {t}")
    return detected


def detect_sink_type(copy_activity: dict) -> str:
    t = copy_activity.get("typeProperties", {}).get("sink", {}).get("type", "")
    detected = SINK_TYPE_MAP.get(t)
    if not detected:
        raise ValueError(f"Unknown sink type: {t}")
    return detected


def get_adapter(connector_type: str) -> BaseConnectorAdapter:
    normalized = str(connector_type or "").strip().upper()
    aliases = {
        "CSV": "DELIMITEDTEXT",
        "DELIMITED_TEXT": "DELIMITEDTEXT",
        "REST_API": "REST",
        "SQLSERVER": "SQL",
        "SQL_SERVER": "SQL",
        "AZURESQL": "SQL",
        "AZURE_SQL": "SQL",
        "AZURE": "ADLS",
        "AZURE_ADLS": "ADLS",
        "AWS": "S3",
        "AMAZON_S3": "S3",
        "ONELAKE": "LAKEHOUSE",
    }
    normalized = aliases.get(normalized, normalized)

    registry: dict[str, type[BaseConnectorAdapter]] = {
        "CSV": DelimitedTextAdapter,
        "DELIMITEDTEXT": DelimitedTextAdapter,
        "REST": RESTAdapter,
        "SQL": SQLAdapter,
        "ADLS": ADLSAdapter,
        "S3": S3Adapter,
        "LAKEHOUSE": LakehouseAdapter,
        "JSON": JSONAdapter,
        "PARQUET": ParquetAdapter,
    }
    cls = registry.get(normalized)
    if not cls:
        raise ValueError(f"Unsupported connector type: {connector_type} (normalized: {normalized})")
    return cls()


class BaseConnectorAdapter(ABC):
    @abstractmethod
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        pass

    @abstractmethod
    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        pass

    @abstractmethod
    def connection_payload(self, params: dict) -> dict:
        pass

    def source_overrides(self, params: dict) -> dict:
        return {}

    def sink_overrides(self, params: dict) -> dict:
        return {}


class DelimitedTextAdapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {"url": params.get("url") or params.get("account_url")}
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "AzureBlobFS", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        return {
            "type": "DelimitedText",
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileName": params.get("file_name"),
                    "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
                    "fileSystem": params.get("file_system") or params.get("container") or params.get("bucket"),
                    "rootFolderPath": params.get("root_path"),
                },
                "columnDelimiter": params.get("delimiter", ","),
                "firstRowAsHeader": params.get("first_row_as_header", True),
                "encodingName": params.get("encoding", "UTF-8"),
            },
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "AzureDataLakeStorageGen2",
            "connectivitySettings": {"url": params.get("url") or params.get("account_url")},
        }

    def source_overrides(self, params: dict) -> dict:
        settings = {
            "type": "AzureBlobFSReadSettings",
            "recursive": params.get("recursive", False)
        }
        if params.get("wildcard"):
            settings["wildcardFileName"] = params["wildcard"]
        return {
            "type": "DelimitedTextSource",
            "storeSettings": settings,
            "formatSettings": {"type": "DelimitedTextReadSettings"},
        }

    def sink_overrides(self, params: dict) -> dict:
        return {
            "type": "DelimitedTextSink",
            "storeSettings": {"type": "AzureBlobFSWriteSettings"},
            "formatSettings": {
                "type": "DelimitedTextWriteSettings",
                "quoteAllText": params.get("quote_all", False),
                "fileExtension": params.get("file_extension", ".csv"),
            },
        }


class RESTAdapter(BaseConnectorAdapter):
    def _url(self, params: dict) -> str:
        url = params.get("url") or params.get("base_url")
        if not url:
            raise ValueError("REST params require url or base_url")
        return url

    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {
            "url": self._url(params),
            "authenticationType": params.get("auth_type", "Anonymous"),
        }
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "RestService", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        return {
            "type": "RestResource",
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {"relativeUrl": params.get("relative_url", "")},
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "RestService",
            "connectivitySettings": {"url": self._url(params)},
        }

    def source_overrides(self, params: dict) -> dict:
        relative_url = params.get("relative_url", "")
        # Connection reference is injected via datasetSettings in deploy_service or locally here
        connection_reference = params.get("connection_id") or params.get("connection_reference")
        
        ds_settings = {
            "type": "RestResource",
            "typeProperties": {
                "relativeUrl": relative_url,
            }
        }
        if connection_reference:
            ds_settings["externalReferences"] = {
                "connection": connection_reference,
            }

        return {
            "type": "RestSource",
            "httpRequestTimeout": params.get("request_timeout", "00:01:40"),
            "requestMethod": params.get("request_method") or params.get("method", "GET"),
            "datasetSettings": ds_settings,
        }

    def sink_overrides(self, params: dict) -> dict:
        relative_url = params.get("relative_url", "")
        connection_reference = params.get("connection_id") or params.get("connection_reference")
        
        ds_settings = {
            "type": "RestResource",
            "typeProperties": {
                "relativeUrl": relative_url,
            }
        }
        if connection_reference:
            ds_settings["externalReferences"] = {
                "connection": connection_reference,
            }

        return {
            "type": "RestSink",
            "httpRequestTimeout": params.get("request_timeout", "00:01:40"),
            "requestMethod": params.get("request_method") or params.get("method", "POST"),
            "datasetSettings": ds_settings,
        }


class SQLAdapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {
            "connectionString": params.get("connection_string")
            or f"Server={params.get('server', '')};Database={params.get('database', '')};"
        }
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": params.get("ls_type", "AzureSqlDatabase"), "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        return {
            "type": params.get("dataset_type", "AzureSqlTable"),
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {"schema": params.get("schema", "dbo"), "table": params.get("table", "")},
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "AzureSqlDatabase",
            "connectivitySettings": {"server": params.get("server"), "database": params.get("database")},
        }

    def source_overrides(self, params: dict) -> dict:
        src: dict[str, Any] = {
            "type": params.get("fabric_source_type", "AzureSqlSource"),
            "queryTimeout": params.get("query_timeout", "02:00:00"),
            "partitionOption": params.get("partition_option", "None"),
        }
        if params.get("query"):
            src["sqlReaderQuery"] = params["query"]
        return src

    def sink_overrides(self, params: dict) -> dict:
        return {
            "type": params.get("fabric_sink_type", "AzureSqlSink"),
            "writeBehavior": params.get("write_behavior", "insert"),
            "sqlWriterUseTableLock": bool(params.get("use_table_lock", False)),
            "tableOption": params.get("table_option", "none"),
            "disableMetricsCollection": bool(params.get("disable_metrics", False)),
        }


class ADLSAdapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {"url": params.get("url") or params.get("account_url", "")}
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "AzureBlobFS", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        fmt = params.get("format", "Parquet")
        return {
            "type": fmt,
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileName": params.get("file_name"),
                    "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
                    "fileSystem": params.get("file_system") or params.get("container") or params.get("bucket"),
                    "rootFolderPath": params.get("root_path"),
                }
            },
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "AzureDataLakeStorageGen2",
            "connectivitySettings": {"url": params.get("url") or params.get("account_url")},
        }

    def source_overrides(self, params: dict) -> dict:
        fmt = params.get("format", "Parquet")
        settings = {
            "type": "AzureBlobFSReadSettings",
            "recursive": bool(params.get("recursive", False)),
        }
        if params.get("wildcard"):
            settings["wildcardFileName"] = params["wildcard"]
        return {
            "type": f"{fmt}Source",
            "storeSettings": settings,
        }

    def sink_overrides(self, params: dict) -> dict:
        fmt = params.get("format", "Parquet")
        return {
            "type": f"{fmt}Sink",
            "storeSettings": {"type": "AzureBlobFSWriteSettings"},
            "formatSettings": {"type": f"{fmt}WriteSettings"},
        }


class S3Adapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {
            "bucketName": params.get("bucket") or params.get("bucket_name") or "",
            "region": params.get("region") or "",
        }
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "AmazonS3", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        fmt = params.get("format", "Parquet")
        return {
            "type": fmt,
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {
                "location": {
                    "type": "AmazonS3Location",
                    "fileName": params.get("file_name"),
                    "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
                    "bucketName": params.get("bucket") or params.get("bucket_name") or "",
                    "rootFolderPath": params.get("root_path"),
                }
            },
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "AmazonS3",
            "connectivitySettings": {
                "bucketName": params.get("bucket") or params.get("bucket_name") or "",
                "region": params.get("region") or "",
            },
        }

    def source_overrides(self, params: dict) -> dict:
        fmt = params.get("format", "Parquet")
        settings = {
            "type": "AmazonS3ReadSettings",
            "recursive": bool(params.get("recursive", False)),
        }
        if params.get("wildcard"):
            settings["wildcardFileName"] = params["wildcard"]
        return {
            "type": f"{fmt}Source",
            "storeSettings": settings,
        }

    def sink_overrides(self, params: dict) -> dict:
        fmt = params.get("format", "Parquet")
        return {
            "type": f"{fmt}Sink",
            "storeSettings": {"type": "AmazonS3WriteSettings"},
            "formatSettings": {"type": f"{fmt}WriteSettings"},
        }


class LakehouseAdapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {
            "workspaceId": params.get("workspace_id", ""),
            "artifactId": params.get("lakehouse_id") or params.get("artifact_id", ""),
        }
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "Lakehouse", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        root = params.get("root_path", "Files")
        if root == "Tables":
            return {
                "type": "LakehouseTable",
                "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
                "typeProperties": {"table": params.get("table", "")},
            }
        fmt = params.get("format", "Parquet")
        return {
            "type": fmt,
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {
                "location": {
                    "type": "LakehouseLocation",
                    "fileName": params.get("file_name"),
                    "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
                    "rootFolderPath": root,
                }
            },
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "Lakehouse",
            "connectivitySettings": {
                "workspaceId": params.get("workspace_id", ""),
                "artifactId": params.get("lakehouse_id") or params.get("artifact_id", ""),
            },
        }

    def source_overrides(self, params: dict) -> dict:
        if params.get("root_path") == "Tables":
            return {
                "type": "LakehouseTableSource",
                "timestampAsOf": params.get("timestamp_as_of"),
                "versionAsOf": params.get("version_as_of"),
            }
        fmt = params.get("format", "Parquet")
        return {
            "type": f"{fmt}Source",
            "storeSettings": {"type": "LakehouseReadSettings", "recursive": bool(params.get("recursive", False))},
        }

    def sink_overrides(self, params: dict) -> dict:
        if params.get("root_path") == "Tables":
            return {
                "type": "LakehouseTableSink",
                "tableActionOption": params.get("table_action", "Append"),
            }
        fmt = params.get("format", "Parquet")
        return {
            "type": f"{fmt}Sink",
            "storeSettings": {"type": "LakehouseWriteSettings"},
            "formatSettings": {"type": f"{fmt}WriteSettings"},
        }


class JSONAdapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {"url": params.get("url") or params.get("account_url", "")}
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "AzureBlobFS", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        return {
            "type": "Json",
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileName": params.get("file_name"),
                    "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
                    "fileSystem": params.get("file_system") or params.get("container") or params.get("bucket"),
                    "rootFolderPath": params.get("root_path"),
                },
                "encodingName": params.get("encoding", "UTF-8"),
            },
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "AzureDataLakeStorageGen2",
            "connectivitySettings": {"url": params.get("url") or params.get("account_url")},
        }

    def source_overrides(self, params: dict) -> dict:
        settings = {
            "type": "AzureBlobFSReadSettings",
            "recursive": bool(params.get("recursive", False)),
        }
        if params.get("wildcard"):
            settings["wildcardFileName"] = params["wildcard"]
        return {
            "type": "JsonSource",
            "storeSettings": settings,
            "formatSettings": {"type": "JsonReadSettings"},
        }

    def sink_overrides(self, params: dict) -> dict:
        return {
            "type": "JsonSink",
            "storeSettings": {"type": "AzureBlobFSWriteSettings"},
            "formatSettings": {
                "type": "JsonWriteSettings",
                "filePattern": params.get("file_pattern", "setOfObjects"),
            },
        }


class ParquetAdapter(BaseConnectorAdapter):
    def build_linked_service(self, params: dict, connection_id: Optional[str] = None) -> dict:
        props: dict[str, Any] = {"url": params.get("url") or params.get("account_url", "")}
        if connection_id:
            props["connectionId"] = connection_id
        return {"type": "AzureBlobFS", "typeProperties": props}

    def build_dataset(self, params: dict, linked_service_name: str) -> dict:
        props = {
            "location": {
                "type": "AzureBlobFSLocation",
                "fileName": params.get("file_name"),
                "folderPath": params.get("folder_path") or params.get("prefix") or params.get("folder"),
                "fileSystem": params.get("file_system") or params.get("container") or params.get("bucket"),
                "rootFolderPath": params.get("root_path"),
            }
        }
        if params.get("compression"):
            props["compressionCodec"] = params["compression"]
        return {
            "type": "Parquet",
            "linkedServiceName": {"referenceName": linked_service_name, "type": "LinkedServiceReference"},
            "typeProperties": props,
        }

    def connection_payload(self, params: dict) -> dict:
        return {
            "connectionKind": "Cloud",
            "connectionType": "AzureDataLakeStorageGen2",
            "connectivitySettings": {"url": params.get("url") or params.get("account_url")},
        }

    def source_overrides(self, params: dict) -> dict:
        settings = {
            "type": "AzureBlobFSReadSettings",
            "recursive": bool(params.get("recursive", False)),
        }
        if params.get("wildcard"):
            settings["wildcardFileName"] = params["wildcard"]
        return {
            "type": "ParquetSource",
            "storeSettings": settings,
        }

    def sink_overrides(self, params: dict) -> dict:
        return {
            "type": "ParquetSink",
            "storeSettings": {"type": "AzureBlobFSWriteSettings"},
            "formatSettings": {"type": "ParquetWriteSettings"},
        }
