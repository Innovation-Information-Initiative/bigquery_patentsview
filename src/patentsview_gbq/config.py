"""Pytask configuration settings."""

import os
from pathlib import Path

# Project paths
ROOT = Path(__file__).parent.parent.parent
SRC = ROOT / "src"
BLD = ROOT / "bld"
RESOURCES = ROOT / "resources"
SCHEMAS = RESOURCES / "schemas"

# Ensure directories exist
BLD.mkdir(exist_ok=True)
RESOURCES.mkdir(exist_ok=True)

# BigQuery configuration
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "nber-i3")

# Version configuration
VERSION = os.getenv("VERSION", "20251209")

# Dataset configurations - simple dictionaries
DATASETS = {
    "granted": {
        "base_url": "https://patentsview.org/download/data-download-tables",
        "dict_url": "https://patentsview.org/download/data-download-dictionary",
        "bq_dataset": "patentsview_granted",
        "gcs_prefix": "i3_raw/patentsview",
    },
    "pregrant": {
        "base_url": "https://patentsview.org/download/pg-download-tables",
        "dict_url": "https://patentsview.org/download/pg-data-download-dictionary",
        "bq_dataset": "patentsview_pregrant",
        "gcs_prefix": "i3_raw/patentsview",
    },
    "beta": {
        "base_url": "https://patentsview.org/download/data-download-tables-beta",
        "dict_url": "https://patentsview.org/download/data-download-dictionary_beta",
        "bq_dataset": "patentsview_beta",
        "gcs_prefix": "i3_raw/patentsview",
    },
}


def get_dataset_dirs(dataset_name: str) -> dict[str, Path]:
    """Get all directory paths for a dataset.

    Args:
        dataset_name: Name of the dataset (granted, pregrant, beta)

    Returns:
        Dictionary with keys: bld_dir, raw_dir, converted_dir,
        metadata_dir, metadata_dir_manual, schema_dir
    """
    return {
        "bld_dir": BLD,
        "raw_dir": BLD / "raw" / dataset_name,
        "converted_dir": BLD / "converted" / dataset_name,
        "metadata_dir": BLD / "metadata" / dataset_name,
        "metadata_dir_manual": BLD / "metadata_manual" / dataset_name,
        "schema_dir": BLD / "schemas" / dataset_name,
    }


def create_dataset_dirs(dataset_name: str) -> None:
    """Create all necessary directories for a dataset."""
    dirs = get_dataset_dirs(dataset_name)
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)


def get_gcs_bucket(dataset_name: str, suffix: str = "") -> str:
    """Get GCS bucket path for a dataset.

    Args:
        dataset_name: Name of the dataset
        suffix: Optional suffix (e.g., '_parquet', '_zip')

    Returns:
        Full GCS bucket path like 'i3_raw/patentsview/20250317/granted'
    """
    ds = DATASETS[dataset_name]
    return f"{ds['gcs_prefix']}/{VERSION}/{dataset_name}{suffix}"
