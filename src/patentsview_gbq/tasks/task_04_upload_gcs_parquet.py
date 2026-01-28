"""Task for uploading converted data to Google Cloud Storage."""
import pytask
import subprocess
from pathlib import Path
from typing import Annotated
import os

from pytask import Product, task
from ..config import BLD, get_gcs_bucket

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
converted_dir = BLD / "converted" / DATASET
gcs_parquet_bucket = get_gcs_bucket(DATASET, suffix="_parquet")
marker_dir = BLD / "gcs"

#@pytask.mark.skip()
@task(is_generator=True)
def task_upload_parquet_to_gcs() -> None:
    """Generate upload tasks for each parquet file."""
    gcs_path = f"gs://{gcs_parquet_bucket}/"

    # Create marker directory
    marker_dir.mkdir(parents=True, exist_ok=True)

    # Get all parquet files
    parquet_files = list(converted_dir.glob("*.parquet"))

    # Limit number of files if MAX_FILES is set
    max_files = os.environ.get("MAX_FILES")
    if max_files is not None:
        parquet_files = parquet_files[:int(max_files)]

    for parquet_file in parquet_files:
        marker_file = marker_dir / f"{parquet_file.name}.uploaded"

        @task
        def upload_file(
            local_file: Path = parquet_file,
            gcs_bucket_path: str = gcs_path,
            marker: Annotated[Path, Product] = marker_file,
        ) -> None:
            """Upload a single parquet file to GCS."""
            # Skip if already uploaded
            if marker.exists():
                print(f"Skipping {local_file} (already uploaded)")
                return

            print(f"Uploading {local_file} to {gcs_bucket_path}")
            subprocess.run(['gcloud', 'storage', 'cp', str(local_file), gcs_bucket_path], check=True)

            # Create marker file to indicate successful upload
            marker.touch()
            print(f"Upload complete. Created marker: {marker}")
