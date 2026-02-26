"""Compress and write final spine outputs."""

from pathlib import Path

import pandas as pd

from backend.io.parquet import write_parquet
from backend.models.master_spine import (
    SPINE_SCHEMA,
    BG_INDEX_SCHEMA,
    derive_bg_index_from_spine,
)
from backend.utils.logging import get_logger

logger = get_logger("transforms.spine.compress_write")


SPINE_OUTPUT_COLUMNS = [
    "block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
    "state_fips", "state_abbr", "msa_geoid", "mega_region_id",
    "gross_land_area_sq_m", "water_area_sq_m",
    "protected_area_sq_m", "net_developable_area_sq_m",
]


def write_spine_outputs(
    spine_df: pd.DataFrame,
    spine_path: Path,
    bg_index_path: Path,
) -> dict:
    """Write both spine crosswalk and BG index to parquet.

    Returns:
        Dict with row counts and paths
    """
    # Ensure column order and presence
    for col in SPINE_OUTPUT_COLUMNS:
        if col not in spine_df.columns:
            spine_df[col] = None

    spine_out = spine_df[SPINE_OUTPUT_COLUMNS].copy()

    # Write block-grain spine
    write_parquet(spine_out, spine_path)
    logger.info("Wrote spine crosswalk: %d blocks", len(spine_out))

    # Derive and write BG-grain index
    bg_index = derive_bg_index_from_spine(spine_out)
    write_parquet(bg_index, bg_index_path)
    logger.info("Wrote BG index: %d block groups", len(bg_index))

    return {
        "spine_rows": len(spine_out),
        "bg_index_rows": len(bg_index),
        "spine_path": str(spine_path),
        "bg_index_path": str(bg_index_path),
    }
