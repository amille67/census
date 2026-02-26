"""Scenario A: Point source -> Block Group ingest.

Join sequence:
  1. Point -> Block polygon (spatial point-in-polygon)
  2. Block -> Master spine (merge on block_geoid)
  3. Aggregate to BG (groupby bg_geoid)
"""

import geopandas as gpd
import pandas as pd

from backend.geo.spatial_join import point_in_polygon
from backend.geo.constants import BLOCK_GEOID_LEN
from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.point_to_bg")


def ingest_point_source(
    points_gdf: gpd.GeoDataFrame,
    block_polygons: gpd.GeoDataFrame,
    spine_df: pd.DataFrame,
    aggregation_rules: dict,
    block_geoid_col: str = "block_geoid",
) -> pd.DataFrame:
    """Ingest a point source to block group grain.

    Args:
        points_gdf: GeoDataFrame with point geometries
        block_polygons: GeoDataFrame with block polygons and block_geoid
        spine_df: Master spine crosswalk DataFrame
        aggregation_rules: Dict of {output_col: agg_func} for BG aggregation
        block_geoid_col: Name of block GEOID column in block_polygons

    Returns:
        DataFrame at block group grain with aggregated metrics
    """
    input_count = len(points_gdf)
    logger.info("Ingesting %d points via Scenario A", input_count)

    # Step 1: Spatial join points to blocks
    points_with_block = point_in_polygon(
        points_gdf, block_polygons, block_geoid_col
    )
    match_rate = points_with_block[block_geoid_col].notna().mean()
    logger.info("Point-to-block match rate: %.1f%%", match_rate * 100)

    # Step 2: Merge with spine to get hierarchy
    points_with_spine = points_with_block.merge(
        spine_df[["block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
                   "state_fips", "msa_geoid"]],
        left_on=block_geoid_col,
        right_on="block_geoid",
        how="left",
    )

    # Step 3: Aggregate to block group
    agg_dict = {}
    for col, func in aggregation_rules.items():
        if func == "count":
            points_with_spine["_count"] = 1
            agg_dict["_count"] = "sum"
        elif col in points_with_spine.columns:
            agg_dict[col] = func

    bg_agg = points_with_spine.groupby("bg_geoid").agg(agg_dict).reset_index()

    if "_count" in agg_dict:
        bg_agg = bg_agg.rename(columns={"_count": "point_count"})

    # Add hierarchy back
    hierarchy = spine_df.groupby("bg_geoid")[
        ["tract_geoid", "county_geoid", "state_fips", "msa_geoid"]
    ].first().reset_index()
    bg_result = bg_agg.merge(hierarchy, on="bg_geoid", how="left")

    logger.info("Point ingest result: %d block groups", len(bg_result))
    return bg_result
