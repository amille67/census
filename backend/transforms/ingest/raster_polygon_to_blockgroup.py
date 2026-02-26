"""Scenario C: Raster/polygon source -> Block Group ingest.

Join sequence:
  1. Zonal stats or polygon overlay against 2020 BG polygons
  2. Output already keyed by bg_geoid
  3. Merge with BG projection of spine
"""

import geopandas as gpd
import pandas as pd

from backend.geo.constants import BG_GEOID_LEN
from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.raster_to_bg")


def ingest_raster_polygon(
    zonal_df: pd.DataFrame,
    bg_index_df: pd.DataFrame,
    bg_geoid_col: str = "bg_geoid",
) -> pd.DataFrame:
    """Ingest raster/polygon zonal statistics at block group grain.

    Expects zonal_df to already contain bg_geoid from the zonal stats
    computation (which operates against BG polygons).

    Args:
        zonal_df: DataFrame with zonal statistics keyed by bg_geoid
        bg_index_df: BG-grain spine index
        bg_geoid_col: Name of BG GEOID column

    Returns:
        DataFrame at block group grain with spine hierarchy
    """
    input_count = len(zonal_df)
    logger.info("Ingesting %d raster/polygon BG rows via Scenario C", input_count)

    zonal_df = zonal_df.copy()
    zonal_df[bg_geoid_col] = zonal_df[bg_geoid_col].astype(str).str.zfill(BG_GEOID_LEN)

    merged = zonal_df.merge(
        bg_index_df[["bg_geoid", "tract_geoid", "county_geoid",
                      "state_fips", "msa_geoid", "net_developable_area_sq_m"]],
        left_on=bg_geoid_col,
        right_on="bg_geoid",
        how="left",
        suffixes=("", "_spine"),
    )

    match_rate = merged["tract_geoid"].notna().mean()
    logger.info("Raster BG-to-spine match rate: %.1f%%", match_rate * 100)

    logger.info("Raster ingest result: %d block groups", len(merged))
    return merged
