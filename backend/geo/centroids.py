"""Centroid computation utilities."""

import geopandas as gpd

from backend.geo.crs import ensure_crs
from backend.geo.constants import CANONICAL_CRS


def add_centroid_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add centroid lat/lon columns to a GeoDataFrame."""
    gdf = ensure_crs(gdf, CANONICAL_CRS)
    centroids = gdf.geometry.centroid
    gdf = gdf.copy()
    gdf["centroid_lon"] = centroids.x
    gdf["centroid_lat"] = centroids.y
    return gdf
