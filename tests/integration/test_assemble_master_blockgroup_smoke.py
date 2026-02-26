"""Smoke test for master blockgroup assembly."""

import pandas as pd
import pytest

from backend.joins.contracts import execute_join
from backend.joins.assembly_joins import ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN


def make_synthetic_bg_index():
    return pd.DataFrame({
        "bg_geoid": ["010010201001", "010010201002", "060375277003"],
        "tract_geoid": ["01001020100", "01001020100", "06037527700"],
        "county_geoid": ["01001", "01001", "06037"],
        "state_fips": ["01", "01", "06"],
        "net_developable_area_sq_m": [50000, 60000, 100000],
    })


def make_synthetic_lodes_ingest():
    return pd.DataFrame({
        "bg_geoid": ["010010201001", "060375277003"],
        "total_jobs": [500, 25000],
        "simpson_diversity_index": [0.85, 0.92],
    })


def make_synthetic_acs_ingest():
    return pd.DataFrame({
        "bg_geoid": ["010010201001", "010010201002", "060375277003"],
        "total_population": [1000, 2000, 50000],
        "median_household_income": [45000, 55000, 75000],
    })


class TestAssembly:

    def test_assembly_preserves_all_bgs(self):
        base = make_synthetic_bg_index()
        lodes = make_synthetic_lodes_ingest()

        result = execute_join(ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN, base, lodes)

        # Left join should preserve all BGs
        assert len(result) == len(base)
        # BGs without LODES data should have NaN
        assert result.loc[result["bg_geoid"] == "010010201002", "total_jobs"].isna().all()

    def test_sequential_assembly(self):
        base = make_synthetic_bg_index()
        lodes = make_synthetic_lodes_ingest()
        acs = make_synthetic_acs_ingest()

        # Join LODES
        result = execute_join(ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN, base, lodes)
        # Join ACS
        result = execute_join(ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN, result, acs)

        assert len(result) == len(base)
        assert "total_jobs" in result.columns
        assert "total_population" in result.columns

    def test_no_row_multiplication_on_assembly(self):
        base = make_synthetic_bg_index()
        # Even with multiple joins, count should remain stable
        for _ in range(3):
            ingest = pd.DataFrame({
                "bg_geoid": ["010010201001"],
                f"metric_{_}": [100],
            })
            base = execute_join(ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN, base, ingest)

        assert len(base) == 3  # Original count
