"""Provenance metadata columns for ingest outputs."""

from datetime import datetime, timezone

import pandas as pd


def add_provenance_columns(
    df: pd.DataFrame,
    source_slug: str,
    source_vintage: str,
    geography_vintage: int = 2020,
    row_count_pre_agg: int = 0,
) -> pd.DataFrame:
    """Add standard provenance metadata columns to an ingest output."""
    df = df.copy()
    df["_source_slug"] = source_slug
    df["_ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["_source_vintage"] = str(source_vintage)
    df["_geography_vintage"] = geography_vintage
    df["_row_count_pre_agg"] = row_count_pre_agg
    return df
