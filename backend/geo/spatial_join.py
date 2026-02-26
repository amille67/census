"""Spatial join utilities for point-in-polygon and centroid joins."""

import geopandas as gpd
import pandas as pd

from backend.geo.crs import ensure_crs
from backend.geo.constants import CANONICAL_CRS


def point_in_polygon(
    points_gdf: gpd.GeoDataFrame,
    polygons_gdf: gpd.GeoDataFrame,
    polygon_id_col: str,
    how: str = "left",
) -> gpd.GeoDataFrame:
    """Assign points to polygons via spatial join.

    Args:
        points_gdf: GeoDataFrame with point geometries
        polygons_gdf: GeoDataFrame with polygon geometries
        polygon_id_col: Column name in polygons_gdf to carry over
        how: Join type ('left' preserves all points)

    Returns:
        points_gdf with polygon_id_col added
    """
    points_gdf = ensure_crs(points_gdf, CANONICAL_CRS)
    polygons_gdf = ensure_crs(polygons_gdf, CANONICAL_CRS)

    result = gpd.sjoin(
        points_gdf,
        polygons_gdf[[polygon_id_col, "geometry"]],
        how=how,
        predicate="within",
    )
    result = result.drop(columns=["index_right"], errors="ignore")
    return result


def centroid_to_polygon(
    source_gdf: gpd.GeoDataFrame,
    source_id_col: str,
    target_gdf: gpd.GeoDataFrame,
    target_id_col: str,
) -> pd.DataFrame:
    """Assign source polygons to target polygons via centroid containment.

    Used for assigning BGs to MSAs or mega-regions.
    """
    source_gdf = ensure_crs(source_gdf, CANONICAL_CRS)
    target_gdf = ensure_crs(target_gdf, CANONICAL_CRS)

    centroids = source_gdf.copy()
    centroids["geometry"] = centroids.geometry.centroid

    joined = gpd.sjoin(
        centroids[[source_id_col, "geometry"]],
        target_gdf[[target_id_col, "geometry"]],
        how="left",
        predicate="within",
    )
    return joined[[source_id_col, target_id_col]].drop_duplicates(subset=[source_id_col])
