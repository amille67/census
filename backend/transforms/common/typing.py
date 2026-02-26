"""Type enforcement transforms."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.common.typing")


def enforce_string_geoids(df: pd.DataFrame, geoid_columns: dict) -> pd.DataFrame:
    """Enforce GEOID columns as zero-padded strings.

    Args:
        df: DataFrame to process
        geoid_columns: Mapping of column_name -> expected_length
    """
    df = df.copy()
    for col, length in geoid_columns.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str.zfill(length)
    return df


def enforce_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Coerce columns to numeric, setting errors to NaN."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
