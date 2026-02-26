"""Smoke test for point source ingest pipeline."""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, box

from backend.transforms.ingest.point_to_blockgroup import ingest_point_source


def make_synthetic_points(n=20):
    """Create synthetic point data."""
    import numpy as np
    np.random.seed(42)

    lats = np.random.uniform(33.0, 34.0, n)
    lons = np.random.uniform(-87.0, -86.0, n)

    return gpd.GeoDataFrame(
        {"id": range(n), "category": ["A", "B"] * (n // 2)},
        geometry=gpd.points_from_xy(lons, lats),
        crs="EPSG:4326",
    )


def make_synthetic_block_polygons():
    """Create synthetic block polygons covering the point area."""
    polys = []
    geoids = []
    idx = 0
    for lat in [33.0, 33.5]:
        for lon in [-87.0, -86.5]:
            polys.append(box(lon, lat, lon + 0.5, lat + 0.5))
            geoids.append(f"01001020100{idx % 3 + 1}{str(idx).zfill(3)}")
            idx += 1

    return gpd.GeoDataFrame(
        {"block_geoid": geoids},
        geometry=polys,
        crs="EPSG:4326",
    )


def make_synthetic_spine(block_polygons):
    from backend.geo.geoid import add_hierarchy_columns
    spine = pd.DataFrame({"block_geoid": block_polygons["block_geoid"]})
    spine = add_hierarchy_columns(spine)
    spine["msa_geoid"] = None
    spine["net_developable_area_sq_m"] = 50000.0
    return spine


class TestPointIngest:

    def test_point_to_blockgroup(self):
        points = make_synthetic_points()
        blocks = make_synthetic_block_polygons()
        spine = make_synthetic_spine(blocks)

        result = ingest_point_source(
            points_gdf=points,
            block_polygons=blocks,
            spine_df=spine,
            aggregation_rules={"_count": "count"},
        )

        assert "bg_geoid" in result.columns
        assert "point_count" in result.columns
        assert result["point_count"].sum() > 0
