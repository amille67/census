"""Tests for spine schema definitions."""

import pandas as pd
import pytest

from backend.models.master_spine import (
    SPINE_COLUMNS,
    BG_INDEX_COLUMNS,
    SPINE_SCHEMA,
    BG_INDEX_SCHEMA,
    derive_bg_index_from_spine,
)


class TestSpineSchema:

    def test_spine_columns_count(self):
        assert len(SPINE_COLUMNS) == 12

    def test_spine_has_required_columns(self):
        required = [
            "block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
            "state_fips", "state_abbr", "msa_geoid", "mega_region_id",
            "gross_land_area_sq_m", "water_area_sq_m",
            "protected_area_sq_m", "net_developable_area_sq_m",
        ]
        for col in required:
            assert col in SPINE_COLUMNS, f"Missing column: {col}"

    def test_bg_index_columns_count(self):
        assert len(BG_INDEX_COLUMNS) == 11

    def test_bg_index_has_bg_grain_areas(self):
        assert "gross_land_area_sq_m_bg" in BG_INDEX_COLUMNS
        assert "water_area_sq_m_bg" in BG_INDEX_COLUMNS
        assert "protected_area_sq_m_bg" in BG_INDEX_COLUMNS

    def test_derive_bg_index(self):
        spine_df = pd.DataFrame({
            "block_geoid": ["010010201001000", "010010201001001", "060375277003009"],
            "bg_geoid": ["010010201001", "010010201001", "060375277003"],
            "tract_geoid": ["01001020100", "01001020100", "06037527700"],
            "county_geoid": ["01001", "01001", "06037"],
            "state_fips": ["01", "01", "06"],
            "state_abbr": ["AL", "AL", "CA"],
            "msa_geoid": ["33860", "33860", "31080"],
            "mega_region_id": [None, None, "SoCal"],
            "gross_land_area_sq_m": [1000.0, 2000.0, 5000.0],
            "water_area_sq_m": [100.0, 200.0, 500.0],
            "protected_area_sq_m": [50.0, 50.0, 100.0],
            "net_developable_area_sq_m": [850.0, 1750.0, 4400.0],
        })

        bg_index = derive_bg_index_from_spine(spine_df)

        # Should have 2 unique block groups
        assert len(bg_index) == 2
        assert bg_index["bg_geoid"].is_unique

        # AL block group areas should be summed
        al_row = bg_index[bg_index["bg_geoid"] == "010010201001"].iloc[0]
        assert al_row["gross_land_area_sq_m_bg"] == 3000.0
        assert al_row["water_area_sq_m_bg"] == 300.0
        assert al_row["protected_area_sq_m_bg"] == 100.0

        # Hierarchy should be first value
        assert al_row["tract_geoid"] == "01001020100"
        assert al_row["state_abbr"] == "AL"
