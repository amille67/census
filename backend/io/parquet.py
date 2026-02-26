"""Parquet I/O utilities for the ETL pipeline."""

from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backend.utils.logging import get_logger

logger = get_logger("io.parquet")


def write_parquet(
    df: pd.DataFrame,
    path: Path,
    schema: Optional[pa.Schema] = None,
    index: bool = False,
):
    """Write a DataFrame to Parquet with optional schema enforcement."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=index)
    pq.write_table(table, path, compression="snappy")
    logger.info("Wrote %d rows to %s", len(df), path)


def read_parquet(path: Path, columns: Optional[list] = None) -> pd.DataFrame:
    """Read a Parquet file into a DataFrame."""
    path = Path(path)
    df = pd.read_parquet(path, columns=columns)
    logger.info("Read %d rows from %s", len(df), path)
    return df


def read_parquet_schema(path: Path) -> pa.Schema:
    """Read only the schema from a Parquet file (no data loaded)."""
    return pq.read_schema(path)
