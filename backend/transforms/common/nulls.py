"""Null handling transforms."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.common.nulls")


def log_null_rates(df: pd.DataFrame, columns: list = None) -> dict:
    """Compute and log null rates for specified columns."""
    if columns is None:
        columns = df.columns.tolist()
    rates = {}
    for col in columns:
        if col in df.columns:
            rate = df[col].isna().mean()
            rates[col] = round(rate, 4)
            if rate > 0.05:
                logger.warning("High null rate for %s: %.1f%%", col, rate * 100)
    return rates


def fill_numeric_nulls(df: pd.DataFrame, value: float = 0.0, columns: list = None) -> pd.DataFrame:
    """Fill null values in numeric columns."""
    df = df.copy()
    if columns is None:
        columns = df.select_dtypes(include=["number"]).columns
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna(value)
    return df
