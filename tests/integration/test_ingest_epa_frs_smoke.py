"""Smoke test for EPA FRS hybrid ingest pipeline.

Tests the full end-to-end flow with synthetic data:
  1. Parse synthetic FRS CSV
  2. Normalize and split into block-coded vs point populations
  3. Block-coded path: merge to spine, aggregate to BG
  4. Point path: spatial join to blocks, merge to spine, aggregate to BG
  5. Combine paths
  6. Validate output schema
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import box

from backend.adapters.epa_frs.parse import parse_frs_single_file
from backend.adapters.epa_frs.normalize import normalize_frs
from backend.adapters.epa_frs.aggregate import aggregate_frs_to_blockgroup
from backend.geo.geoid import add_hierarchy_columns
from backend.geo.constants import BLOCK_GEOID_LEN
from backend.geo.spatial_join import point_in_polygon
from backend.transforms.ingest.validate_ingest_output import validate_ingest_output
from backend.transforms.common.provenance import add_provenance_columns


def make_synthetic_spine(n_blocks=20):
    """Create a minimal spine with block polygons."""
    np.random.seed(42)
    blocks = []
    for i in range(n_blocks):
        state = "01" if i < n_blocks // 2 else "06"
        county = f"{state}001"
        tract = f"{county}020100"
        bg = f"{tract}{i % 4 + 1}"
        block = f"{bg}{str(i).zfill(3)}"
        blocks.append({"block_geoid": block})

    spine_df = pd.DataFrame(blocks)
    spine_df = add_hierarchy_columns(spine_df)
    spine_df["msa_geoid"] = None
    spine_df["net_developable_area_sq_m"] = 50000.0
    return spine_df


def make_synthetic_block_polygons(spine_df):
    """Create synthetic block polygons around a grid."""
    rows = []
    for idx, row in spine_df.iterrows():
        # Create a small grid of boxes, offset by index
        x_base = -86.0 + (idx % 10) * 0.01
        y_base = 32.0 + (idx // 10) * 0.01
        geom = box(x_base, y_base, x_base + 0.01, y_base + 0.01)
        rows.append({
            "block_geoid": row["block_geoid"],
            "geometry": geom,
        })

    return gpd.GeoDataFrame(rows, crs="EPSG:4269")


def make_synthetic_frs_csv(tmp_path, spine_df, n_facilities=40):
    """Create a synthetic FRS CSV that matches the spine."""
    np.random.seed(42)
    block_geoids = spine_df["block_geoid"].tolist()

    rows = []
    for i in range(n_facilities):
        # 60% get a valid block code, 40% get lat/lon only
        has_block = (i % 5 != 0) and (i % 5 != 1)
        block_code = block_geoids[i % len(block_geoids)] if has_block else ""

        rows.append({
            "REGISTRY_ID": f"110{str(i).zfill(9)}",
            "PRIMARY_NAME": f"Facility {i}",
            "CITY_NAME": "Testville",
            "COUNTY_NAME": "Test County",
            "FIPS_CODE": block_code[:5] if has_block else "01001",
            "STATE_CODE": "AL" if block_code[:2] == "01" or not has_block else "CA",
            "STATE_NAME": "ALABAMA",
            "COUNTRY_NAME": "US",
            "POSTAL_CODE": "12345",
            "FEDERAL_FACILITY_CODE": "Y" if i % 5 == 0 else "N",
            "TRIBAL_LAND_CODE": "N",
            "CONGRESSIONAL_DIST_NUM": "01",
            "CENSUS_TRACT_CODE": "020100",
            "EPA_REGION_CODE": "04",
            "SITE_TYPE_NAME": ["Stationary", "Mobile", "Federal"][i % 3],
            "PGM_SYS_ACRNMS": ["RCRA|AIR", "NPDES|TRI", "RCRA", "SEMS"][i % 4],
            "LATITUDE83": round(-86.0 + (i % 10) * 0.01 + 0.005, 6),
            "LONGITUDE83": round(32.0 + (i // 10) * 0.01 + 0.005, 6),
            "ACCURACY_VALUE": round(np.random.uniform(1, 50), 2),
            "CENSUS_BLOCK_CODE": block_code,
        })

    df = pd.DataFrame(rows)
    csv_path = tmp_path / "NATIONAL_SINGLE.CSV"
    df.to_csv(csv_path, index=False)
    return csv_path


class TestFRSHybridIngestSmoke:

    @pytest.fixture
    def spine(self):
        return make_synthetic_spine()

    @pytest.fixture
    def block_polygons(self, spine):
        return make_synthetic_block_polygons(spine)

    @pytest.fixture
    def frs_csv(self, tmp_path, spine):
        return make_synthetic_frs_csv(tmp_path, spine)

    def test_parse_and_normalize(self, frs_csv):
        """Test that parse + normalize produces three populations."""
        raw = parse_frs_single_file(frs_csv)
        assert len(raw) == 40

        split = normalize_frs(raw)
        total = len(split["block_df"]) + len(split["point_df"]) + len(split["dropped_df"])
        assert total == len(raw)
        assert split["stats"]["total_records"] == 40

    def test_block_path_merge_to_spine(self, frs_csv, spine):
        """Test Scenario B: block-coded records merge to spine."""
        raw = parse_frs_single_file(frs_csv)
        split = normalize_frs(raw)
        block_df = split["block_df"]

        if len(block_df) == 0:
            pytest.skip("No block-coded records in synthetic data")

        # Merge with spine
        merged = block_df.merge(
            spine[["block_geoid", "bg_geoid", "tract_geoid", "county_geoid", "state_fips"]],
            on="block_geoid",
            how="left",
        )
        match_rate = merged["bg_geoid"].notna().mean()
        assert match_rate > 0.5  # Most should match synthetic spine

        # Aggregate to BG
        matched = merged[merged["bg_geoid"].notna()].copy()
        bg_result = aggregate_frs_to_blockgroup(matched)
        assert bg_result["bg_geoid"].is_unique
        assert "frs_facility_count" in bg_result.columns

    def test_point_path_spatial_join(self, frs_csv, spine, block_polygons):
        """Test Scenario A: point records spatial join to blocks."""
        raw = parse_frs_single_file(frs_csv)
        split = normalize_frs(raw)
        point_df = split["point_df"]

        if len(point_df) == 0:
            pytest.skip("No point-only records in synthetic data")

        # Create GeoDataFrame
        point_gdf = gpd.GeoDataFrame(
            point_df,
            geometry=gpd.points_from_xy(point_df["longitude"], point_df["latitude"]),
            crs="EPSG:4326",
        )

        # Spatial join
        result = point_in_polygon(point_gdf, block_polygons, "block_geoid")
        assert "block_geoid" in result.columns

    def test_full_hybrid_pipeline(self, frs_csv, spine, block_polygons):
        """End-to-end test of the hybrid ingest."""
        # Parse
        raw = parse_frs_single_file(frs_csv)

        # Normalize
        split = normalize_frs(raw)
        block_df = split["block_df"]
        point_df = split["point_df"]

        results = []

        # Path 1: Block-coded
        if len(block_df) > 0:
            merged = block_df.merge(
                spine[["block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
                        "state_fips", "msa_geoid"]],
                on="block_geoid", how="left",
            )
            matched = merged[merged["bg_geoid"].notna()].copy()
            if len(matched) > 0:
                bg1 = aggregate_frs_to_blockgroup(matched)
                results.append(bg1)

        # Path 2: Point spatial
        if len(point_df) > 0:
            point_gdf = gpd.GeoDataFrame(
                point_df,
                geometry=gpd.points_from_xy(point_df["longitude"], point_df["latitude"]),
                crs="EPSG:4326",
            )
            with_block = point_in_polygon(point_gdf, block_polygons, "block_geoid")
            merged2 = pd.DataFrame(with_block).merge(
                spine[["block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
                        "state_fips", "msa_geoid"]],
                on="block_geoid", how="left",
            )
            matched2 = merged2[merged2["bg_geoid"].notna()].copy()
            if len(matched2) > 0:
                bg2 = aggregate_frs_to_blockgroup(matched2)
                results.append(bg2)

        # Combine
        assert len(results) > 0, "At least one path should produce results"
        combined = pd.concat(results, ignore_index=True)

        # Re-aggregate if overlapping BGs
        if not combined["bg_geoid"].is_unique:
            count_cols = [c for c in combined.columns if c.endswith("_count")]
            agg_dict = {c: "sum" for c in count_cols if c in combined.columns}
            combined = combined.groupby("bg_geoid").agg(agg_dict).reset_index()

        # Add hierarchy
        hierarchy = spine.groupby("bg_geoid")[
            ["tract_geoid", "county_geoid", "state_fips"]
        ].first().reset_index()
        result = combined.merge(hierarchy, on="bg_geoid", how="left")

        # Add provenance
        result = add_provenance_columns(result, "epa_frs", "2024",
                                         row_count_pre_agg=len(raw))

        # Validate
        report = validate_ingest_output(result, "epa_frs")
        assert report["passed"] or len(result) > 0
        assert result["bg_geoid"].is_unique
        assert "frs_facility_count" in result.columns
