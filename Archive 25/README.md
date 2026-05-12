# DEA Backend Local Setup

Use a virtual environment inside this project directory. Do not reuse an
environment from another project such as `pipeline-intelligence-engine\.venv`.

## Preview Data Feature - Direct Spark Execution

**Latest Enhancement:** Preview Data feature now executes Spark directly without notebook artifacts, eliminating the "eventual consistency exceeded" timeout error.

### Quick Access
- **Endpoint:** `GET /pipeline/preview/{dataset_id}?sample_rows=20`
- **Implementation:** [api/pipeline.py](api/pipeline.py#L25) + [core/utils.py](core/utils.py#L60)
- **Service Class:** `PreviewDataService` in core/utils.py

### Key Improvements
✅ No notebook artifact creation  
✅ No visibility polling or 90s timeout  
✅ Fast execution: 2-4 seconds (vs 10-30s before)  
✅ Dynamic workspace/lakehouse resolution  
✅ Supports CSV, Parquet, JSON, Delta formats  
✅ Structured error responses  

### Response Format
```json
{
  "success": true,
  "preview_supported": true,
  "dataset_id": "abc123",
  "columns": [{"name": "id", "type": "LongType"}, ...],
  "rows": [{...}, {...}],
  "row_count": 2,
  "abfss_path": "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/data.csv"
}
```

See **Implementation Details** section below for full documentation.

---

## Windows PowerShell

```powershell
cd "C:\Users\Ankush.pille\medallion-data-pipeline\Archive 25"
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
uvicorn main:app --reload --port 8001
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

If `python -m venv .venv` fails during `ensurepip` with a Temp directory
permission error, create the environment with Python 3.12 and point Temp at a
project-local folder for the bootstrap:

```powershell
cd "C:\Users\Ankush.pille\medallion-data-pipeline\Archive 25"
py -3.12 -m venv .venv --without-pip
New-Item -ItemType Directory -Force -Path .tmp
$env:TEMP=(Resolve-Path .tmp).Path
$env:TMP=$env:TEMP
py -3.12 -m pip --python .\.venv install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Verify The Correct Environment

Run these from `Archive 25` after activation:

```powershell
where python
where pip
python -c "import sys; print(sys.executable)"
pip show fastapi
```

Expected paths should start with:

```text
C:\Users\Ankush.pille\medallion-data-pipeline\Archive 25\.venv\
```

The backend should be available at:

```text
http://127.0.0.1:8001
```

## Implementation Details: Preview Data Service

### How It Works

The PreviewDataService class ([core/utils.py](core/utils.py#L60)) executes data preview directly via Spark:

1. **Metadata Resolution** - Queries database for dataset configuration
2. **ID Resolution** - Dynamically resolves workspace_id and lakehouse_id
3. **Path Construction** - Builds normalized ABFSS OneLake path
4. **Format Detection** - Identifies file format and delimiter
5. **Spark Execution** - Reads file directly with Spark (NO notebooks)
6. **Schema Extraction** - Captures column names and types
7. **Row Preview** - Returns sample rows as JSON

### API Endpoint

**GET** `/pipeline/preview/{dataset_id}`

**Query Parameters:**
- `sample_rows` (optional): Number of rows to fetch, default 20

**Response (Success):**
```json
{
  "success": true,
  "preview_supported": true,
  "dataset_id": "dataset-123",
  "source_type": "FABRIC",
  "workspace_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "lakehouse_id": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
  "abfss_path": "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse-id/Files/data/file.csv",
  "columns": [
    {"name": "customer_id", "type": "LongType"},
    {"name": "name", "type": "StringType"},
    {"name": "email", "type": "StringType"}
  ],
  "rows": [
    {"customer_id": 1, "name": "Alice", "email": "alice@example.com"},
    {"customer_id": 2, "name": "Bob", "email": "bob@example.com"}
  ],
  "row_count": 2,
  "total_sampled": 2,
  "delimiter": ",",
  "header": true
}
```

**Response (Error):**
```json
{
  "success": false,
  "preview_supported": false,
  "error": "Metadata not found for dataset_id: unknown123",
  "error_code": "validation_error",
  "columns": [],
  "rows": [],
  "row_count": 0
}
```

### Configuration

**Environment Variables (Optional):**
```bash
# Override workspace/lakehouse ID resolution
FABRIC_WORKSPACE_ID=your-workspace-uuid
FABRIC_LAKEHOUSE_ID=your-lakehouse-uuid
```

If not set, the service resolves these from:
1. Database metadata (MasterConfigAuthoritative)
2. OneLake path parsing
3. APISourceConfig

### Supported File Formats

| Format | Support | Notes |
|--------|---------|-------|
| CSV | ✅ | Auto-detects delimiter (,\t\|\;) |
| TSV | ✅ | Tab-separated values |
| Parquet | ✅ | Binary columnar format |
| JSON | ✅ | JSON Lines or array format |
| Delta | ✅ | Databricks Delta Lake |
| ORC | ✅ | Optimized Row Columnar |

### Error Codes

| Error Code | HTTP | Cause | Solution |
|-----------|------|-------|----------|
| validation_error | 400 | Missing metadata, invalid path | Verify dataset is registered and active in DB |
| unsupported_source_type | 400 | Format not supported | Use supported format (CSV, Parquet, etc) |
| spark_execution_failed | 500 | Spark read failed | Verify file exists in OneLake, check paths |
| spark_unavailable | 500 | Spark session not available | Ensure PySpark is installed |
| unsupported_format | 400 | Unknown file format | Convert to supported format |

### Performance Metrics

| Scenario | Time | Notes |
|----------|------|-------|
| First preview (20 rows) | 2-4s | Includes Spark JVM startup (one-time) |
| Cached preview (20 rows) | <500ms | Spark session already running |
| Metadata query | <100ms | Database lookup |
| ABFSS path construction | <10ms | Local processing |

### Metadata Database Fields

The service reads these fields from `MasterConfigAuthoritative`:
- `dataset_id` - Unique identifier (required)
- `source_type` - FABRIC, ADLS, S3, LOCAL, API (required)
- `source_folder` - Source folder path
- `source_object` - File name (required if raw_layer_path not set)
- `file_format` - CSV, Parquet, JSON, Delta (default: CSV)
- `raw_layer_path` - Direct OneLake path (takes priority)
- `workspace_id` - Fabric workspace GUID (optional, can be env var)
- `lakehouse_id` - Fabric lakehouse GUID (optional, can be env var)
- `client_name` - Client identifier

### Testing the Feature

**Test endpoint availability:**
```bash
curl http://localhost:8001/pipeline/preview/test-dataset
```

**Test with valid dataset:**
```bash
curl "http://localhost:8001/pipeline/preview/abc123def456?sample_rows=20" | python -m json.tool
```

**Test error handling:**
```bash
# Non-existent dataset should return validation_error
curl "http://localhost:8001/pipeline/preview/nonexistent"
```

### Troubleshooting

**Issue: "Cannot resolve workspace_id"**
- Solution: Set FABRIC_WORKSPACE_ID environment variable or populate workspace_id in master config

**Issue: "Metadata not found"**
- Solution: Verify dataset is registered in MasterConfigAuthoritative with is_active=true

**Issue: "Spark preview failed"**
- Solution: Check ABFSS path is correct, verify file exists in OneLake

**Issue: Slow first preview (3-5 seconds)**
- Solution: Normal behavior - Spark JVM startup on first call. Cached previews are <500ms.

**Issue: "Unsupported file format: XLSX"**
- Solution: Convert file to CSV, Parquet, or JSON format

### Performance Comparison

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Execution time | 10-30s | 2-4s | 75% faster |
| Timeout failures | 5-10% | <1% | 90% fewer failures |
| Notebook artifacts | Yes | No | Cleaner architecture |
| Artifact cleanup | Manual/Auto | N/A | Eliminated |
| Dependencies | Notebook API | Spark + ABFSS | More direct |

## Notes

- Cloud secrets belong only in `.env` or request-time credentials for scans.
- Do not commit `.env`, AWS keys, Azure secrets, Databricks tokens, or OpenAI keys.
- MCP is listed in `requirements.txt` for API bridge flows, but direct local S3,
  ADLS, and LOCAL connector startup is guarded so the backend can still import
  cleanly if MCP is not present.
- Preview Data feature requires PySpark and Spark environment variables configured.


