"""Geometry overlay operations for water/protected land clipping."""

import geopandas as gpd
import numpy as np

from backend.geo.crs import to_area_crs


def compute_overlay_area(
    base_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    base_id_col: str,
    output_col: str,
) -> gpd.GeoDataFrame:
    """Compute the area of overlap between base polygons and overlay polygons.

    Returns base_gdf with a new column containing the overlay area in sq meters.
    """
    base_proj = to_area_crs(base_gdf)
    overlay_proj = to_area_crs(overlay_gdf)

    intersection = gpd.overlay(
        base_proj[[base_id_col, "geometry"]],
        overlay_proj[["geometry"]],
        how="intersection",
    )
    intersection["_overlay_area"] = intersection.geometry.area

    area_by_id = (
        intersection.groupby(base_id_col)["_overlay_area"]
        .sum()
        .rename(output_col)
    )

    result = base_gdf.merge(area_by_id, left_on=base_id_col, right_index=True, how="left")
    result[output_col] = result[output_col].fillna(0.0)
    return result


def compute_net_developable(
    df,
    gross_col: str = "gross_land_area_sq_m",
    water_col: str = "water_area_sq_m",
    protected_col: str = "protected_area_sq_m",
    output_col: str = "net_developable_area_sq_m",
):
    """Compute net developable area = gross - water - protected, floored at 0."""
    df = df.copy()
    df[output_col] = np.maximum(
        df[gross_col] - df[water_col] - df[protected_col],
        0.0,
    )
    return df
