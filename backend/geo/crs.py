"""CRS (Coordinate Reference System) utilities."""

import geopandas as gpd

from backend.geo.constants import CANONICAL_CRS, AREA_CRS


def ensure_crs(gdf: gpd.GeoDataFrame, target_crs: str = CANONICAL_CRS) -> gpd.GeoDataFrame:
    """Reproject a GeoDataFrame to the target CRS if needed."""
    if gdf.crs is None:
        gdf = gdf.set_crs(target_crs)
    elif gdf.crs.to_epsg() != int(target_crs.split(":")[1]):
        gdf = gdf.to_crs(target_crs)
    return gdf


def to_area_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to equal-area CRS for area calculations."""
    return ensure_crs(gdf, AREA_CRS)


def compute_area_sq_m(gdf: gpd.GeoDataFrame, geometry_col: str = "geometry") -> gpd.GeoSeries:
    """Compute area in square meters using equal-area projection."""
    projected = to_area_crs(gdf)
    return projected[geometry_col].area
