"""Parquet partition and index helpers for geospatial data."""

import pandas as pd


def set_geoid_index(df: pd.DataFrame, geoid_col: str = "block_geoid") -> pd.DataFrame:
    """Set a GEOID column as index and sort for efficient merges."""
    return df.set_index(geoid_col).sort_index()


def partition_by_state(df: pd.DataFrame, state_col: str = "state_fips") -> dict:
    """Split a DataFrame into per-state partitions."""
    return {fips: group for fips, group in df.groupby(state_col)}
