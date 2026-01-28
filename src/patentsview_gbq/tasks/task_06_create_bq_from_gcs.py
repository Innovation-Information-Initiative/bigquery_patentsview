"""Task for creating BigQuery tables from GCS parquet files."""
import pytask
import subprocess
from pathlib import Path
from typing import Annotated
import os

from pytask import Product, task
from ..config import BLD, BQ_PROJECT_ID, DATASETS, get_gcs_bucket, RESOURCES, VERSION

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
marker_dir = BLD / "gcs"
gcs_parquet_bucket = get_gcs_bucket(DATASET, suffix="_parquet")
bq_dataset = DATASETS[DATASET]["bq_dataset"]
schema_dir = RESOURCES / "patentsview_schemas" / DATASET

#@pytask.mark.skip()
@task(is_generator=True)
def task_create_bq_from_gcs() -> None:
    """Generate tasks to create BigQuery tables from uploaded GCS parquet files."""
    marker_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all marker files (indicates successful upload)
    marker_files = list(marker_dir.glob("*.parquet.uploaded"))
    
    for marker_file in marker_files:
        # Extract parquet filename from marker (remove .uploaded suffix)
        parquet_filename = marker_file.name.replace(".uploaded", "")
        # Table name is the filename without .parquet extension
        table_name = parquet_filename.replace(".parquet", "")
        
        # Extract base table name (remove version suffix if present)
        # e.g., g_patent_20250909 -> g_patent
        base_table_name = table_name
        version_suffix = f"_{VERSION}"
        if table_name.endswith(version_suffix):
            base_table_name = table_name[:-len(version_suffix)]
        
        # Look for schema file
        schema_file_path = schema_dir / f"schema_{base_table_name}.json"
        
        # GCS path to the parquet file
        gcs_file_path = f"gs://{gcs_parquet_bucket}/{parquet_filename}"
        # BigQuery table ID: dataset.table_name (project specified via --project_id flag)
        bq_table_id = f"{bq_dataset}.{table_name}"
        
        # Marker file to track successful table creation
        bq_marker_file = marker_dir / f"{parquet_filename}.bq_created"
        
        @task
        def create_bq_table(
            marker: Path = marker_file,
            gcs_path: str = gcs_file_path,
            table_id: str = bq_table_id,
            bq_marker: Annotated[Path, Product] = bq_marker_file,
            base_name: str = base_table_name,
        ) -> None:
            """Create a BigQuery table from a GCS parquet file."""
            # Skip if table already created
            if bq_marker.exists():
                print(f"Skipping {table_id} (already created)")
                return
            
            # Skip if marker file doesn't exist (file not uploaded yet)
            if not marker.exists():
                print(f"Skipping {table_id} (upload marker not found)")
                return
            
            # Check if file exists in GCS (skip if from different dataset)
            try:
                subprocess.run(
                    ['gsutil', 'ls', gcs_path],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            except subprocess.CalledProcessError:
                print(f"File '{gcs_path}' not found in GCS. Skipping (may be from different dataset).")
                return
            
            # Check for schema file
            schema_file = schema_dir / f"schema_{base_name}.json"
            use_schema = schema_file.exists()
            
            # Build BigQuery load command
            if use_schema:
                print(f"Creating BigQuery table {table_id} from {gcs_path} with schema {schema_file}")
                bq_load_command = [
                    'bq', 'load',
                    '--source_format=PARQUET',
                    '--replace',
                    f'--project_id={BQ_PROJECT_ID}',
                    f'--schema={schema_file}',
                    table_id,
                    gcs_path
                ]
            else:
                print(f"Creating BigQuery table {table_id} from {gcs_path} (auto-detecting schema)")
                bq_load_command = [
                    'bq', 'load',
                    '--source_format=PARQUET',
                    '--replace',
                    '--autodetect',
                    f'--project_id={BQ_PROJECT_ID}',
                    table_id,
                    gcs_path
                ]
            
            # Print command for manual debugging
            print(f"Running: {' '.join(str(x) for x in bq_load_command)}")

            try:
                subprocess.run(bq_load_command, check=True)
                # Create marker file to indicate successful table creation
                bq_marker.touch()
                print(f"Successfully created table {table_id}")
            except subprocess.CalledProcessError as e:
                print(f"Error creating table {table_id}: {e}")
                raise

