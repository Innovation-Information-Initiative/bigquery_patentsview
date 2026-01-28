"""Task for creating BigQuery tables using pre-created schemas."""
import pytask
import json
import subprocess
from pathlib import Path
import os
from ...config import SCHEMAS, DATASETS, BLD, BQ_PROJECT_ID, VERSION, get_gcs_bucket

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
converted_dir = BLD / "converted" / DATASET
gcs_bucket = get_gcs_bucket(DATASET, suffix="_parquet")
bq_dataset = DATASETS[DATASET]["bq_dataset"]

@pytask.mark.skip()
@pytask.task(after="task_upload_to_gcs")
def task_create_bigquery_tables_manual() -> None:
    """Create BigQuery tables from uploaded data using pre-created schemas.
    
    Args:
        depends_on: Dictionary of dependencies
        produces: Output marker file
        
    Environment Variables:
        MANUAL_TABLES: Comma-separated list of tables to process (optional)
    """
    # Path to pre-created schemas
    schema_dir = SCHEMAS
    
    # Check if schema directory exists
    if not schema_dir.exists():
        raise FileNotFoundError(f"Schema directory not found: {schema_dir}")
    
    # Get list of schema files
    schema_files = list(schema_dir.glob("*.json"))
    if not schema_files:
        raise FileNotFoundError(f"No schema files found in {schema_dir}")
    
    print(f"Found {len(schema_files)} schema files in {schema_dir}")
    
    # Check for MANUAL_TABLES environment variable
    manual_tables = os.environ.get("MANUAL_TABLES")
    if manual_tables:
        manual_tables = [t.strip() for t in manual_tables.split(",")]
        print(f"Processing only these tables: {manual_tables}")
    
    # Create BigQuery tables
    for schema_file in schema_files:
        # Extract table name from schema file name and remove "schema_" prefix if present
        table_name = schema_file.stem
        if table_name.startswith("schema_"):
            table_name = table_name[7:]  # Remove "schema_" prefix
        
        # Skip if not in manual_tables (if specified)
        if manual_tables and table_name not in manual_tables:
            print(f"Skipping {table_name} (not in MANUAL_TABLES)")
            continue
        
        versioned_table_name = f"{table_name}_{VERSION}.parquet"
        gcs_file_path = f"gs://{gcs_bucket}/{versioned_table_name}"
        bq_table_name = f"{bq_dataset}.{table_name}_{VERSION}"

        # Check if file exists in GCS
        try:
            subprocess.run(['gsutil', 'ls', gcs_file_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            print(f"File '{gcs_file_path}' not found in GCS. Skipping.")
            continue

        # Load data into BigQuery with schema (Parquet format)
        bq_load_command = [
            'bq', 'load',
            '--source_format=PARQUET',
            '--replace',
            f'--project_id={BQ_PROJECT_ID}',
            f'--schema={schema_file}',  # Use pre-created schema for descriptions
            bq_table_name,
            gcs_file_path
        ]

        try:
            print(f"Creating table {bq_table_name} with schema {schema_file}")
            subprocess.run(bq_load_command, check=True)
            print(f"Data uploaded successfully to {bq_table_name}")
        except subprocess.CalledProcessError as e:
            print(f"Error uploading {table_name}: {e}")
            continue 