"""Task for extracting patent data and converting to Parquet."""
import sys
import csv

# MUST set this before pandas/csv readers are triggered
csv.field_size_limit(sys.maxsize)

import io
import json
import os
import zipfile
from pathlib import Path
from typing import Annotated, Optional, Set

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytask
from pytask import DirectoryNode, Product, task

from ..config import BLD, VERSION, DATASETS, RESOURCES

# Default N_COLUMNS - can be overridden per dataset if needed
N_COLUMNS = None

# Heuristic for "large" TSVs (uncompressed size in bytes)
# ZipInfo.file_size is the uncompressed size.
SIZE_THRESHOLD_BYTES = 200 * 1024 * 1024  # 200 MB

# Default chunk size for streaming
DEFAULT_CHUNKSIZE = 500_000


def _strip_outer_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """Strip surrounding double quotes from string columns."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: x[1:-1]
                if isinstance(x, str) and x.startswith('"') and x.endswith('"')
                else x
            )
    return df


def _apply_column_limit(df: pd.DataFrame) -> pd.DataFrame:
    """Optionally trim DataFrame to first N_COLUMNS."""
    if N_COLUMNS is not None:
        if df.shape[1] < N_COLUMNS:
            raise ValueError(
                f"Expected at least {N_COLUMNS} columns, but got {df.shape[1]}"
            )
        df = df.iloc[:, :N_COLUMNS]
    return df


def _get_column_types(table_name: str, dataset: str) -> tuple[Optional[Set[str]], Optional[Set[str]]]:
    """Get sets of date and integer column names from schema file if it exists.
    
    Returns:
        Tuple of (date_columns, integer_columns), either can be None if schema doesn't exist
    """
    schema_dir = RESOURCES / "patentsview_schemas" / dataset
    schema_file = schema_dir / f"schema_{table_name}.json"
    
    if not schema_file.exists():
        return None, None
    
    try:
        with open(schema_file) as f:
            schema = json.load(f)
        date_columns = {
            field["name"] for field in schema 
            if field.get("type") == "DATE"
        }
        integer_columns = {
            field["name"] for field in schema 
            if field.get("type") == "INTEGER"
        }
        return (
            date_columns if date_columns else None,
            integer_columns if integer_columns else None
        )
    except Exception:
        return None, None


def _parse_date_columns(df: pd.DataFrame, date_columns: Optional[Set[str]]) -> pd.DataFrame:
    """Parse date columns from string format to date32 for proper parquet serialization."""
    if date_columns is None:
        return df

    for col in date_columns:
        if col in df.columns:
            # Parse dates and convert to Python date objects
            # pyarrow will serialize these as DATE32 (INT32) in parquet
            df[col] = pd.to_datetime(
                df[col],
                format='%Y-%m-%d',
                errors='coerce'
            ).dt.date

    return df


def _parse_integer_columns(df: pd.DataFrame, integer_columns: Optional[Set[str]]) -> pd.DataFrame:
    """Parse integer columns from string format to int64 for proper parquet serialization."""
    if integer_columns is None:
        return df
    
    for col in integer_columns:
        if col in df.columns:
            # Convert to numeric, coercing errors to NaN (results in float64)
            numeric_series = pd.to_numeric(df[col], errors='coerce')
            
            # Check if all non-NaN values are whole numbers (integers)
            # If so, convert to Int64; otherwise keep as float64
            non_null_mask = numeric_series.notna()
            if non_null_mask.any():
                # Check if all non-null values are whole numbers
                # Use a small epsilon to handle floating point precision issues
                non_null_values = numeric_series[non_null_mask]
                # Check if values are close to their floor (within floating point precision)
                is_whole = (non_null_values - non_null_values.round()).abs() < 1e-10
                if is_whole.all():
                    # All values are integers (or very close), safe to convert to Int64
                    # Round first to ensure we have exact integers
                    df[col] = numeric_series.round().astype('Int64')
                else:
                    # Contains non-integer floats, keep as float64
                    # This shouldn't happen for INTEGER columns, but handle gracefully
                    df[col] = numeric_series
            else:
                # All NaN, can safely convert to Int64
                df[col] = numeric_series.astype('Int64')
    
    return df


def _write_parquet_streaming(
    tsv_stream,
    parquet_path: Path,
    log_prefix: str,
    chunksize: int = DEFAULT_CHUNKSIZE,
    date_columns: Optional[Set[str]] = None,
    integer_columns: Optional[Set[str]] = None,
) -> None:
    """Read a TSV stream in chunks and write a single Parquet file."""
    reader = pd.read_csv(
        tsv_stream,
        sep="\t",
        quoting=csv.QUOTE_MINIMAL,
        engine="python",
        dtype=str,
        keep_default_na=False,
        na_values=[],
        chunksize=chunksize,
    )

    writer = None
    try:
        for i, chunk in enumerate(reader):
            print(f"{log_prefix} – chunk {i}, shape={chunk.shape}")
            chunk = _apply_column_limit(chunk)
            chunk = _strip_outer_quotes(chunk)
            chunk = _parse_date_columns(chunk, date_columns)
            chunk = _parse_integer_columns(chunk, integer_columns)

            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def _convert_member_to_parquet(
    zip_ref: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    table_name: str,
    parquet_path: Path,
    log_file,
    dataset: str,
) -> None:
    """Convert a single TSV member inside a zip to Parquet (streaming or in-memory)."""
    log_prefix = f"{member.filename}"
    
    # Get column types from schema if available
    date_columns, integer_columns = _get_column_types(table_name, dataset)

    # Decide whether to stream based on uncompressed size
    use_streaming = member.file_size >= SIZE_THRESHOLD_BYTES

    with zip_ref.open(member) as tsv_file:
        tsv_data = io.TextIOWrapper(tsv_file, encoding="utf-8")

        if use_streaming:
            # For very wide / heavy text columns, smaller chunksize helps
            # Heuristic tweak for "abstract" columns (large strings)
            lower_chunksize = 100_000 if "abstract" in table_name else DEFAULT_CHUNKSIZE

            print(f"{log_prefix}: using streaming conversion (size={member.file_size} bytes).")
            log_file.write(
                f"{table_name}: STREAMING (size={member.file_size} bytes, chunksize={lower_chunksize})\n"
            )
            _write_parquet_streaming(
                tsv_stream=tsv_data,
                parquet_path=parquet_path,
                log_prefix=log_prefix,
                chunksize=lower_chunksize,
                date_columns=date_columns,
                integer_columns=integer_columns,
            )
            print(f"Converted {member.filename} (streaming) to {parquet_path}")
        else:
            # In-memory path for smaller TSVs
            df = pd.read_csv(
                tsv_data,
                sep="\t",
                quoting=csv.QUOTE_MINIMAL,
                engine="python",
                dtype=str,
                keep_default_na=False,
                na_values=[],
            )

            print(f"Loaded DataFrame with shape {df.shape}")
            print(f"Columns: {df.columns.tolist()}")

            df = _apply_column_limit(df)
            df = _strip_outer_quotes(df)
            df = _parse_date_columns(df, date_columns)
            df = _parse_integer_columns(df, integer_columns)

            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(parquet_path, index=False)
            log_file.write(
                f"{table_name}: IN_MEMORY (size={member.file_size} bytes, rows={df.shape[0]}, cols={df.shape[1]})\n"
            )
            print(f"Converted {member.filename} to {parquet_path} (in-memory)")

@task(is_generator=True)
def task_extract_to_parquet() -> None:
    """Generate tasks to extract zip files and convert to Parquet for all datasets."""
    for dataset_name in DATASETS.keys():
        raw_dir = BLD / "raw" / dataset_name
        converted_dir = BLD / "converted" / dataset_name
        
        converted_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Get all zip files for this dataset
        zip_files = list(raw_dir.glob("*.zip"))
        
        for zip_file in zip_files:
            # Get the table name from the zip file name (remove .zip and .tsv extensions)
            table_name = os.path.splitext(zip_file.name)[0]  # Remove .zip
            if table_name.endswith(".tsv"):
                table_name = os.path.splitext(table_name)[0]  # Remove .tsv
            
            versioned_name = f"{table_name}_{VERSION}"
            parquet_path = converted_dir / f"{versioned_name}.parquet"

            # Skip if already converted
            if parquet_path.exists():
                continue

            # Capture variables in default arguments to avoid closure issues
            @task
            def convert_zip(
                zip_path: Path = zip_file,
                table: str = table_name,
                parquet: Annotated[Path, Product] = parquet_path,
                dataset: str = dataset_name,
            ) -> None:
                """Convert a zip file to parquet, processing all TSV members."""
                # Skip if already converted
                if parquet.exists():
                    print(f"Skipping {parquet.name} (already converted)")
                    return
                
                dataset_converted_dir = BLD / "converted" / dataset
                dataset_converted_dir.mkdir(parents=True, exist_ok=True)
                
                log_path = dataset_converted_dir / "conversion_log.txt"
                with open(log_path, "a", encoding="utf-8") as log_file:
                    print(f"[{dataset}] Extracting and converting {zip_path.name}")
                    
                    try:
                        with zipfile.ZipFile(zip_path, "r") as zip_ref:
                            # Find all TSV members
                            members = [m for m in zip_ref.infolist() if m.filename.endswith(".tsv")]
                            
                            if not members:
                                raise ValueError(f"No TSV members found in {zip_path}")
                            
                            # Process each TSV member (typically just one)
                            for member in members:
                                _convert_member_to_parquet(
                                    zip_ref=zip_ref,
                                    member=member,
                                    table_name=table,
                                    parquet_path=parquet,
                                    log_file=log_file,
                                    dataset=dataset,
                                )
                        
                        print(f"Conversion complete: {parquet}")
                    
                    except Exception as e:
                        log_file.write(f"{zip_path.name}: ERROR - {e}\n")
                        print(f"[{dataset}] Failed to convert {zip_path}: {e}")
                        raise
