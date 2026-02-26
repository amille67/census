"""Scenario B: Native block source -> Block Group ingest.

Join sequence:
  1. Normalize block_geoid (w_geocode for LODES)
  2. Merge to spine on block_geoid
  3. Aggregate to bg_geoid
"""

import pandas as pd

from backend.geo.constants import BLOCK_GEOID_LEN
from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.native_block_to_bg")


def ingest_native_block(
    block_df: pd.DataFrame,
    spine_df: pd.DataFrame,
    block_geoid_col: str = "block_geoid",
    aggregation_rules: dict = None,
) -> pd.DataFrame:
    """Ingest a native block-grain source to block group grain.

    Args:
        block_df: DataFrame with block-level data and block_geoid
        spine_df: Master spine crosswalk DataFrame
        block_geoid_col: Name of the block GEOID column
        aggregation_rules: Dict of {column: agg_func}

    Returns:
        DataFrame at block group grain
    """
    input_count = len(block_df)
    logger.info("Ingesting %d block rows via Scenario B", input_count)

    # Normalize block_geoid
    block_df = block_df.copy()
    block_df[block_geoid_col] = (
        block_df[block_geoid_col].astype(str).str.zfill(BLOCK_GEOID_LEN)
    )

    # Merge with spine
    merged = block_df.merge(
        spine_df[["block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
                   "state_fips", "msa_geoid", "net_developable_area_sq_m"]],
        left_on=block_geoid_col,
        right_on="block_geoid",
        how="inner",
    )

    match_rate = len(merged) / input_count if input_count > 0 else 0
    logger.info("Block-to-spine match rate: %.1f%%", match_rate * 100)

    # Aggregate to block group
    if aggregation_rules is None:
        numeric_cols = block_df.select_dtypes(include=["number"]).columns.tolist()
        aggregation_rules = {col: "sum" for col in numeric_cols if col != block_geoid_col}

    agg_dict = {col: func for col, func in aggregation_rules.items()
                if col in merged.columns}

    bg_agg = merged.groupby("bg_geoid").agg(agg_dict).reset_index()

    # Add hierarchy
    hierarchy = spine_df.groupby("bg_geoid")[
        ["tract_geoid", "county_geoid", "state_fips", "msa_geoid"]
    ].first().reset_index()
    bg_result = bg_agg.merge(hierarchy, on="bg_geoid", how="left")

    logger.info("Block ingest result: %d block groups", len(bg_result))
    return bg_result
