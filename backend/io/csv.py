"""CSV I/O utilities."""

from pathlib import Path
from typing import Optional

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("io.csv")


def read_csv_with_geoid(
    path: Path,
    geoid_column: str,
    geoid_length: int,
    dtype_overrides: Optional[dict] = None,
) -> pd.DataFrame:
    """Read a CSV file ensuring GEOID columns are preserved as zero-padded strings."""
    dtypes = {geoid_column: str}
    if dtype_overrides:
        dtypes.update(dtype_overrides)

    df = pd.read_csv(path, dtype=dtypes)
    df[geoid_column] = df[geoid_column].astype(str).str.zfill(geoid_length)
    logger.info("Read %d rows from %s", len(df), path)
    return df
