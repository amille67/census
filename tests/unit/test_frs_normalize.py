"""Tests for EPA FRS normalization and hybrid split."""

import numpy as np
import pandas as pd
import pytest

from backend.adapters.epa_frs.normalize import normalize_frs


def make_parsed_frs(n=30):
    """Create a DataFrame mimicking parse_frs_single_file output."""
    np.random.seed(42)
    rows = []
    for i in range(n):
        state_fips = "01" if i < n // 2 else "06"
        county_fips = f"{state_fips}001"

        # 2/3 of records have a valid block code
        has_block = (i % 3 != 0)
        block_code = f"{county_fips}020100{i % 4 + 1}{str(i).zfill(3)}" if has_block else ""

        # Most records have valid coords, a few don't
        has_coords = (i % 7 != 0)

        row = {
            "registry_id": f"110{str(i).zfill(9)}",
            "primary_name": f"Facility {i}",
            "city_name": "Testville",
            "fips_code": county_fips,
            "state_code": "AL" if state_fips == "01" else "CA",
            "federal_facility_flag": "Y" if i % 5 == 0 else "N",
            "tribal_land_flag": "N",
            "epa_region_code": "04",
            "site_type_name": ["Stationary", "Mobile", "Federal"][i % 3],
            "pgm_sys_acrnms": ["RCRA|AIR", "NPDES|TRI", "RCRA", "SEMS|AIR"][i % 4],
            "latitude": round(32.0 + np.random.uniform(-1, 1), 6) if has_coords else None,
            "longitude": round(-86.0 + np.random.uniform(-1, 1), 6) if has_coords else None,
            "accuracy_value": round(np.random.uniform(0.1, 100.0), 2),
            "census_block_code": block_code,
        }
        rows.append(row)

    return pd.DataFrame(rows)


class TestFRSNormalize:

    def test_split_produces_three_populations(self):
        df = make_parsed_frs()
        result = normalize_frs(df)
        assert "block_df" in result
        assert "point_df" in result
        assert "dropped_df" in result
        assert "stats" in result

    def test_total_records_accounted_for(self):
        df = make_parsed_frs(30)
        result = normalize_frs(df)
        total = len(result["block_df"]) + len(result["point_df"]) + len(result["dropped_df"])
        assert total == 30

    def test_block_df_has_block_geoid(self):
        df = make_parsed_frs()
        result = normalize_frs(df)
        block_df = result["block_df"]
        if len(block_df) > 0:
            assert "block_geoid" in block_df.columns
            # All block_geoid should be 15 digits
            assert block_df["block_geoid"].str.match(r"^\d{15}$").all()

    def test_point_df_has_valid_coords(self):
        df = make_parsed_frs()
        result = normalize_frs(df)
        point_df = result["point_df"]
        if len(point_df) > 0:
            assert pd.to_numeric(point_df["latitude"], errors="coerce").notna().all()
            assert pd.to_numeric(point_df["longitude"], errors="coerce").notna().all()

    def test_program_flags_derived(self):
        df = make_parsed_frs()
        result = normalize_frs(df)
        for pop in [result["block_df"], result["point_df"]]:
            if len(pop) > 0:
                assert "has_rcra" in pop.columns
                assert "has_npdes" in pop.columns
                assert "has_air" in pop.columns
                assert "has_tri" in pop.columns
                assert "has_cerclis" in pop.columns
                assert "program_count" in pop.columns

    def test_federal_flag_converted_to_bool(self):
        df = make_parsed_frs()
        result = normalize_frs(df)
        all_records = pd.concat([result["block_df"], result["point_df"], result["dropped_df"]])
        assert all_records["federal_facility_flag"].dtype == bool

    def test_stats_contain_percentages(self):
        df = make_parsed_frs(100)
        result = normalize_frs(df)
        stats = result["stats"]
        assert stats["total_records"] == 100
        assert stats["block_coded_pct"] + stats["point_only_pct"] + stats["dropped_pct"] == pytest.approx(100.0, abs=0.1)

    def test_no_overlap_between_block_and_point(self):
        df = make_parsed_frs()
        result = normalize_frs(df)
        block_ids = set(result["block_df"].index)
        point_ids = set(result["point_df"].index)
        assert len(block_ids & point_ids) == 0
