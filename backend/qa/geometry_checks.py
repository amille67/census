"""Geometry QA checks."""

import geopandas as gpd

from backend.utils.logging import get_logger

logger = get_logger("qa.geometry")


def check_valid_geometries(gdf: gpd.GeoDataFrame) -> dict:
    """Check for invalid geometries."""
    invalid_count = (~gdf.geometry.is_valid).sum()
    empty_count = gdf.geometry.is_empty.sum()
    null_count = gdf.geometry.isna().sum()

    return {
        "total": len(gdf),
        "invalid": int(invalid_count),
        "empty": int(empty_count),
        "null": int(null_count),
        "valid_rate": 1 - (invalid_count + empty_count + null_count) / len(gdf) if len(gdf) > 0 else 0,
    }
