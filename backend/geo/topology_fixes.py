"""Geometry topology repair utilities."""

import geopandas as gpd
from shapely.validation import make_valid


def fix_invalid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Repair invalid geometries using shapely make_valid."""
    gdf = gdf.copy()
    invalid_mask = ~gdf.geometry.is_valid
    if invalid_mask.any():
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)
    return gdf


def drop_empty_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Remove rows with null or empty geometries."""
    return gdf[~(gdf.geometry.is_empty | gdf.geometry.isna())].copy()
