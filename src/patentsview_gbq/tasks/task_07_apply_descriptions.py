"""Task for applying table descriptions to BigQuery tables."""
import json
import subprocess
from pathlib import Path
import os
import re

from ..config import DATASETS, BQ_PROJECT_ID, VERSION, RESOURCES

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
metadata_dir = RESOURCES / "metadata" / DATASET
bq_dataset = DATASETS[DATASET]["bq_dataset"]


def clean_table_description(description):
    """Clean up table description for BigQuery."""
    # Replace newlines with spaces
    description = description.replace('\n', ' ')
    # Remove any problematic characters
    description = re.sub(r'[^\w\s.,;:()\-]', ' ', description)
    # Trim extra spaces
    description = re.sub(r'\s+', ' ', description).strip()
    return description


def task_apply_table_descriptions(
    tables_file: Path = metadata_dir / "table_descriptions.json",
) -> None:
    """Apply descriptions to BigQuery tables.

    Args:
        tables_file: Path to table descriptions JSON file

    Environment Variables:
        MAX_TABLES: Maximum number of tables to process (optional)
    """
    # Load metadata
    with open(tables_file) as f:
        raw_table_descriptions = json.load(f)

    # Parse table descriptions - keys contain "table_name\n\n\nzip:..." format
    table_descriptions = {}
    for key, description in raw_table_descriptions.items():
        # Extract table name (everything before first newline)
        table_name = key.split('\n')[0].strip()
        if table_name and not table_name.startswith('\\'):  # Skip malformed entries
            table_descriptions[table_name] = description

    # Get list of tables
    tables = list(table_descriptions.keys())

    # Check for MAX_TABLES environment variable
    max_tables = os.environ.get("MAX_TABLES")
    if max_tables is not None:
        max_tables = int(max_tables)
        print(f"Processing only the first {max_tables} tables")
        tables = tables[:max_tables]

    # Apply descriptions
    for table_name in tables:
        # Clean table description
        table_description = clean_table_description(table_descriptions.get(table_name, ""))

        # Get versioned table name
        versioned_table_name = f"{table_name}_{VERSION}"
        bq_table_name = f"{bq_dataset}.{versioned_table_name}"

        # Update table description
        if table_description:
            bq_update_command = [
                'bq', 'update',
                '--description', table_description,
                f'--project_id={BQ_PROJECT_ID}',
                bq_table_name
            ]

            try:
                print(f"Updating description for {bq_table_name}")
                subprocess.run(bq_update_command, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error updating {table_name}: {e}")
