"""Task for making BigQuery tables publicly accessible."""
import json
import subprocess
import os

from ..config import DATASETS, BQ_PROJECT_ID, VERSION

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "pregrant")
bq_dataset = DATASETS[DATASET]["bq_dataset"]

ROLE = "roles/bigquery.dataViewer"
MEMBER = "allUsers"


def task_make_tables_public() -> None:
    """Grant allUsers dataViewer access on every table/view in the dataset.

    Environment Variables:
        CONFIG_TYPE: Dataset to process (granted, pregrant, beta). Default: granted.
    """
    # List tables in the dataset
    result = subprocess.run(
        [
            "bq",
            "ls",
            f"--project_id={BQ_PROJECT_ID}",
            "--format=prettyjson",
            bq_dataset,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    tables_json = json.loads(result.stdout)

    # Filter to tables, views, and materialized views matching the current VERSION
    table_ids = [
        entry["tableReference"]["tableId"]
        for entry in tables_json
        if entry.get("type") in ("TABLE", "VIEW", "MATERIALIZED_VIEW")
        and entry["tableReference"]["tableId"].endswith(f"_{VERSION}")
    ]

    if not table_ids:
        print(f"No tables/views found in {BQ_PROJECT_ID}:{bq_dataset}")
        return

    print(f"Found {len(table_ids)} tables/views in {BQ_PROJECT_ID}:{bq_dataset}")

    for table_id in table_ids:
        full = f"{BQ_PROJECT_ID}:{bq_dataset}.{table_id}"
        print(f"Applying IAM binding to {full} ...")
        try:
            subprocess.run(
                [
                    "bq",
                    "add-iam-policy-binding",
                    f"--member={MEMBER}",
                    f"--role={ROLE}",
                    full,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Error applying IAM binding to {full}: {e}")
            raise

    print(
        f"Done: applied {ROLE} for {MEMBER} on {len(table_ids)} resources "
        f"in {BQ_PROJECT_ID}:{bq_dataset}"
    )
