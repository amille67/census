"""Deduplication transforms."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.common.dedupe")


def dedupe_by_key(df: pd.DataFrame, key_col: str, sort_col: str = None, keep: str = "last") -> pd.DataFrame:
    """Deduplicate a DataFrame by a key column."""
    if sort_col and sort_col in df.columns:
        df = df.sort_values(sort_col)
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep=keep)
    logger.info("Deduped by %s: %d -> %d rows", key_col, before, len(df))
    return df
