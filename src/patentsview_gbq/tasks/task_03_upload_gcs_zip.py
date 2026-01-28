"""Task for uploading raw zipped files to Google Cloud Storage."""
import pytask
import subprocess
from pathlib import Path
from typing import Annotated
import os

from pytask import DirectoryNode
from ..config import BLD, get_gcs_bucket

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
raw_dir = BLD / "raw" / DATASET
gcs_raw_bucket = get_gcs_bucket(DATASET, suffix="_zip")

@pytask.mark.skip()
def task_upload_raw_to_gcs(
    zip_files: Annotated[list[Path], DirectoryNode(root_dir=raw_dir, pattern="*.zip")],
) -> None:
    """Upload raw zipped files to Google Cloud Storage."""
    gcs_path = f"gs://{gcs_raw_bucket}/"

    for local_file_path in zip_files:
        print(f"Uploading {local_file_path} to {gcs_path}")
        subprocess.run(['gcloud', 'storage', 'cp', str(local_file_path), gcs_path], check=True)
