"""Attach MSA/CBSA assignment to spine via BG centroid spatial join."""

import geopandas as gpd
import pandas as pd

from backend.geo.spatial_join import centroid_to_polygon
from backend.utils.logging import get_logger

logger = get_logger("transforms.spine.attach_msa")


def attach_msa_to_spine(
    bg_polygons: gpd.GeoDataFrame,
    msa_polygons: gpd.GeoDataFrame,
    spine_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach msa_geoid to spine via BG centroid spatial join.

    Args:
        bg_polygons: GeoDataFrame with bg_geoid and geometry
        msa_polygons: GeoDataFrame with msa_geoid and geometry
        spine_df: Spine DataFrame to attach MSA assignment to

    Returns:
        Spine DataFrame with msa_geoid column added
    """
    bg_to_msa = centroid_to_polygon(
        source_gdf=bg_polygons,
        source_id_col="bg_geoid",
        target_gdf=msa_polygons,
        target_id_col="msa_geoid",
    )

    coverage = bg_to_msa["msa_geoid"].notna().mean()
    logger.info("MSA coverage: %.1f%% of block groups assigned", coverage * 100)

    spine = spine_df.merge(bg_to_msa, on="bg_geoid", how="left")
    return spine
