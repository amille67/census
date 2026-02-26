"""Common data cleaning transforms."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.common.cleaning")


def strip_whitespace(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    """Strip leading/trailing whitespace from string columns."""
    df = df.copy()
    if columns is None:
        columns = df.select_dtypes(include=["object", "string"]).columns
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.strip()
    return df


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and underscore column names."""
    df = df.copy()
    df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df


def drop_duplicate_rows(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """Drop duplicate rows, logging the count."""
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    dropped = before - len(df)
    if dropped > 0:
        logger.info("Dropped %d duplicate rows", dropped)
    return df
