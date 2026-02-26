"""Scenario D: Native block group source -> Block Group ingest.

Join sequence:
  1. Compose bg_geoid from API components or normalize existing
  2. Merge with BG-grain spine index (NOT block-grain spine!)
  3. Write output at BG grain
"""

import pandas as pd

from backend.geo.constants import BG_GEOID_LEN
from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.native_bg_to_bg")


def ingest_native_bg(
    bg_df: pd.DataFrame,
    bg_index_df: pd.DataFrame,
    bg_geoid_col: str = "bg_geoid",
) -> pd.DataFrame:
    """Ingest a native block-group-grain source.

    Uses the BG-grain spine index for safe one-to-one joins,
    avoiding row duplication that would occur with block-grain spine.

    Args:
        bg_df: DataFrame with block-group-level data
        bg_index_df: BG-grain spine index DataFrame
        bg_geoid_col: Name of the block group GEOID column

    Returns:
        DataFrame at block group grain with spine hierarchy attached
    """
    input_count = len(bg_df)
    logger.info("Ingesting %d BG rows via Scenario D", input_count)

    bg_df = bg_df.copy()
    bg_df[bg_geoid_col] = bg_df[bg_geoid_col].astype(str).str.zfill(BG_GEOID_LEN)

    # Merge with BG index (one-to-one is the expected cardinality)
    merged = bg_df.merge(
        bg_index_df[["bg_geoid", "tract_geoid", "county_geoid",
                      "state_fips", "msa_geoid", "net_developable_area_sq_m"]],
        left_on=bg_geoid_col,
        right_on="bg_geoid",
        how="left",
        suffixes=("", "_spine"),
    )

    match_rate = merged["tract_geoid"].notna().mean()
    logger.info("BG-to-spine match rate: %.1f%%", match_rate * 100)

    # Verify no row multiplication
    if len(merged) != input_count:
        logger.warning(
            "Row count changed after merge: %d -> %d. "
            "Check for duplicate bg_geoid in spine index.",
            input_count, len(merged),
        )

    logger.info("BG ingest result: %d block groups", len(merged))
    return merged
