"""Task for creating BigQuery tables."""
import pytask
import json
import subprocess
from pathlib import Path
import os

from ...config import DATASETS, BLD, BQ_PROJECT_ID, VERSION, get_gcs_bucket

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
converted_dir = BLD / "converted" / DATASET
metadata_dir_manual = BLD / "metadata_manual" / DATASET
gcs_bucket = get_gcs_bucket(DATASET, suffix="_parquet")
bq_dataset = DATASETS[DATASET]["bq_dataset"]

@pytask.mark.skip()
@pytask.task(after="task_upload_to_gcs")
def task_create_bigquery_tables(
    variables_file: Path = metadata_dir_manual / "variable_descriptions.json",
) -> None:
    """Create BigQuery tables from uploaded data.

    Args:
        variables_file: Path to variable descriptions JSON file

    Environment Variables:
        MAX_TABLES: Maximum number of tables to process (optional)
    """
    # Load metadata
    with open(variables_file) as f:
        variable_descriptions = json.load(f)

    # Get list of tables
    tables = list(variable_descriptions.keys())
    print(tables)
    
    # Check for MAX_TABLES environment variable
    max_tables = None #os.environ.get("MAX_TABLES")
    if max_tables is not None:
        max_tables = int(max_tables)
        print(f"Processing only the first {max_tables} tables")
        tables = tables[:max_tables]
    
    # Create BigQuery tables
    for table_name in tables:
        versioned_table_name = f"{table_name}_{VERSION}.parquet"
        gcs_file_path = f"gs://{gcs_bucket}/{versioned_table_name}"
        bq_table_name = f"{bq_dataset}.{table_name}_{VERSION}"

        # Check if file exists in GCS
        try:
            subprocess.run(['gsutil', 'ls', gcs_file_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            print(f"File '{gcs_file_path}' not found in GCS. Skipping.")
            continue

        # Generate schema from variable descriptions
        schema_fields = []
        for var_name, var_info in variable_descriptions[table_name].items():
            field_type = var_info.get('type', 'STRING').upper()
            # Map common types to BigQuery types
            if field_type in ['INTEGER', 'INT']:
                bq_type = 'INTEGER'
            elif field_type in ['FLOAT', 'DOUBLE', 'NUMERIC']:
                bq_type = 'FLOAT'
            elif field_type in ['DATE']:
                bq_type = 'DATE'
            elif field_type in ['TIMESTAMP', 'DATETIME']:
                bq_type = 'TIMESTAMP'
            else:
                bq_type = 'STRING'
            
            schema_fields.append({
                'name': var_name,
                'type': bq_type,
                'description': var_info.get('description', '')
            })

        # Create temporary schema file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_file:
            json.dump(schema_fields, schema_file, indent=2)
            schema_path = schema_file.name

        # Load data into BigQuery from Parquet
        bq_load_command = [
            'bq', 'load',
            '--source_format=PARQUET',
            '--replace',
            f'--project_id={BQ_PROJECT_ID}',
            f'--schema={schema_path}',  # Provide schema with descriptions
            bq_table_name,
            gcs_file_path
        ]

        try:
            print(f"Creating table {bq_table_name} with schema from metadata")
            subprocess.run(bq_load_command, check=True)
            print(f"Data uploaded successfully to {bq_table_name}")
        except subprocess.CalledProcessError as e:
            print(f"Error uploading {table_name}: {e}")
            continue
        finally:
            # Clean up temporary schema file
            Path(schema_path).unlink()
