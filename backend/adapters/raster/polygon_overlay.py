"""Polygon overlay operations for raster-derived categorical data."""

import geopandas as gpd
import pandas as pd

from backend.geo.crs import to_area_crs
from backend.utils.logging import get_logger

logger = get_logger("adapters.raster.polygon_overlay")


def categorical_overlay(
    source_gdf: gpd.GeoDataFrame,
    zones_gdf: gpd.GeoDataFrame,
    zone_id_col: str,
    category_col: str,
) -> pd.DataFrame:
    """Compute area-weighted categorical overlay statistics.

    For each zone, computes the percentage of area covered by each category.
    """
    source_proj = to_area_crs(source_gdf)
    zones_proj = to_area_crs(zones_gdf)

    zones_proj["_zone_area"] = zones_proj.geometry.area

    intersection = gpd.overlay(
        zones_proj[[zone_id_col, "_zone_area", "geometry"]],
        source_proj[[category_col, "geometry"]],
        how="intersection",
    )
    intersection["_overlap_area"] = intersection.geometry.area
    intersection["_pct"] = intersection["_overlap_area"] / intersection["_zone_area"]

    pivot = intersection.pivot_table(
        index=zone_id_col,
        columns=category_col,
        values="_pct",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()

    pivot.columns = [
        f"pct_{c}" if c != zone_id_col else c
        for c in pivot.columns
    ]

    return pivot
