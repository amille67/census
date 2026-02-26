"""Merge spine hierarchy into ingest output."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.merge_spine_hierarchy")


def merge_spine_hierarchy(
    ingest_df: pd.DataFrame,
    bg_index_df: pd.DataFrame,
    join_key: str = "bg_geoid",
) -> pd.DataFrame:
    """Attach full spine hierarchy columns to an ingest output.

    Ensures the standard hierarchy columns are present in the output.
    """
    hierarchy_cols = [
        "bg_geoid", "tract_geoid", "county_geoid",
        "state_fips", "msa_geoid",
    ]

    existing = [c for c in hierarchy_cols if c in ingest_df.columns and c != join_key]
    if existing:
        ingest_df = ingest_df.drop(columns=existing, errors="ignore")

    merged = ingest_df.merge(
        bg_index_df[hierarchy_cols].drop_duplicates(subset=["bg_geoid"]),
        on=join_key,
        how="left",
    )

    logger.info("Merged spine hierarchy: %d rows", len(merged))
    return merged
