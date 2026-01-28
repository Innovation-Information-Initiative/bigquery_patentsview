# PatentsView to BigQuery

> Automated ETL pipeline for downloading USPTO PatentsView data and uploading to Google BigQuery

## Features

- 📥 Automated downloads from PatentsView (granted, pre-grant, beta)
- 🔄 Direct zip to Parquet conversion (no intermediate TSV files)
- ☁️ Google Cloud Storage integration
- 🗄️ BigQuery table creation from Parquet with schemas and descriptions
- 🎯 Task-based workflow with pytask

---

## Quick Start

```bash
# Setup
git clone https://github.com/yourusername/patentsview_gbq.git
cd patentsview_gbq
pixi install
pixi shell

# Configure
export VERSION=20250909
export CONFIG_TYPE=granted  # or pregrant, beta
export BQ_PROJECT_ID=your-project-id

# Authenticate with GCP
gcloud auth login

# Run pipeline
pytask
```

---

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `VERSION` | PatentsView data version | `20251209` |
| `CONFIG_TYPE` | Dataset: `granted`, `pregrant`, `beta` | `granted` |
| `BQ_PROJECT_ID` | Google Cloud project ID | `nber-i3` |

**BigQuery Datasets:**
- `granted` → `patentsview_granted`
- `pregrant` → `patentsview_pregrant`
- `beta` → `patentsview_beta`

**Data Scope:**
The pipeline downloads **metadata tables only** (patent details, locations, CPCs, etc.) from:
- Granted: `/data-download-tables`
- Pregrant: `/pg-download-tables`
- Beta: `/data-download-tables-beta`

**Text tables are NOT included** (claims, abstracts, descriptions). These are available at separate URLs (`/brf_sum_text`, `/claims`, `/detail_desc_text`, `/draw_desc_text`) and can be added as separate dataset configs if needed.

---

## Usage

```bash
# Run full pipeline
pytask

# Preview tasks
pytask --dry-run

# Run specific task
pytask -k task_01_download
pytask -k task_02_extract_to_parquet

# Limit tables for testing
MAX_TABLES=2 pytask -k task_06_create_bq
```

### Pipeline Tasks

1. `task_01_download` - Download data from PatentsView
2. `task_02_extract_to_parquet` - Extract zip files and convert directly to Parquet
3. `task_03_upload_gcs_zip` - Upload raw zips to GCS (not really needed).
4. `task_04_upload_gcs_parquet` - Upload Parquet to GCS
5. `task_05_metadata` - Collect table/variable descriptions from PatentsView website (no need to run this again, schemas are already in the resources folder).
6. `task_06_create_bq_from_gcs` - Create BigQuery tables from GCS parquet files
7. `task_07_apply_descriptions` - Apply descriptions to BigQuery tables

---

## Project Structure

```
patentsview_gbq/
├── src/patentsview_gbq/
│   ├── config.py
│   └── tasks/
│       ├── task_01_download.py ... task_07_apply_descriptions.py
│       └── archive/       # Deprecated tasks
├── bld/                   # Generated
│   ├── raw/{granted,pregrant,beta}/
│   ├── converted/{granted,pregrant,beta}/
│   └── metadata/{granted,pregrant,beta}/
└── resources/             # Manual schemas and metadata
    ├── patentsview_schemas/
    └── metadata/
```

**GCS Structure:** `gs://i3_raw/patentsview/{VERSION}/{DATASET}/`

**BigQuery Tables:** `{dataset}.{table_name}_{VERSION}`
- Example: `patentsview_granted.g_patent_20250909`

---

## Development

**Task Dependencies:**
```python
# Tasks use DirectoryNode for file dependencies
zip_files: Annotated[list[Path], DirectoryNode(root_dir=raw_dir, pattern="*.zip")]

# Non-file tasks use @task(after=...)
@pytask.task(after="task_upload_to_gcs")
```

**Manual Schemas:** Place JSON files in `resources/schemas/`

---

## Troubleshooting

**Download 403 errors:** Tool includes retry logic and browser headers. Check PatentsView availability.

**BigQuery failures:** Verify schema in `resources/schemas/` and GCS file exists with `gsutil ls`

**Task dependencies:** Run `pytask --dry-run` to verify task graph

---

## References

- [PatentsView Data](https://patentsview.org/download/data-download-tables)
- [pytask Documentation](https://pytask-dev.readthedocs.io/)
