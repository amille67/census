"""Attach mega-region assignment to spine via BG centroid spatial join."""

import geopandas as gpd
import pandas as pd

from backend.geo.spatial_join import centroid_to_polygon
from backend.utils.logging import get_logger

logger = get_logger("transforms.spine.attach_mega_regions")


def attach_mega_regions_to_spine(
    bg_polygons: gpd.GeoDataFrame,
    mega_region_polygons: gpd.GeoDataFrame,
    spine_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach mega_region_id to spine via BG centroid spatial join."""
    bg_to_mega = centroid_to_polygon(
        source_gdf=bg_polygons,
        source_id_col="bg_geoid",
        target_gdf=mega_region_polygons,
        target_id_col="mega_region_id",
    )

    coverage = bg_to_mega["mega_region_id"].notna().mean()
    logger.info("Mega-region coverage: %.1f%% of BGs assigned", coverage * 100)

    spine = spine_df.merge(bg_to_mega, on="bg_geoid", how="left")
    return spine
