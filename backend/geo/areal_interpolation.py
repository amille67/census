"""Areal interpolation for vintage crosswalking (e.g., 2010 -> 2020 BGs)."""

import geopandas as gpd
import pandas as pd

from backend.geo.crs import to_area_crs


def areal_interpolation(
    source_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    source_id: str,
    target_id: str,
    value_columns: list,
) -> pd.DataFrame:
    """Apportion values from source polygons to target polygons by area overlap.

    Used for NHGIS 2010->2020 block group crosswalking.

    Args:
        source_gdf: GeoDataFrame with source vintage polygons and values
        target_gdf: GeoDataFrame with target vintage polygons
        source_id: ID column in source
        target_id: ID column in target
        value_columns: Columns to apportion

    Returns:
        DataFrame with target_id and apportioned value columns
    """
    source_proj = to_area_crs(source_gdf)
    target_proj = to_area_crs(target_gdf)

    source_proj["_source_area"] = source_proj.geometry.area

    intersection = gpd.overlay(
        source_proj[[source_id, "_source_area", "geometry"] + value_columns],
        target_proj[[target_id, "geometry"]],
        how="intersection",
    )
    intersection["_overlap_area"] = intersection.geometry.area
    intersection["_weight"] = intersection["_overlap_area"] / intersection["_source_area"]

    for col in value_columns:
        intersection[col] = intersection[col] * intersection["_weight"]

    result = intersection.groupby(target_id)[value_columns].sum().reset_index()
    return result
