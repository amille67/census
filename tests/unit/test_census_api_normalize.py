"""Tests for Census API normalization."""

import pandas as pd
import pytest

from backend.adapters.census_api.normalize import normalize_acs_blockgroup
from backend.geo.geoid import compose_bg_geoid


class TestACSNormalization:

    def test_compose_bg_geoid_from_api_components(self):
        assert compose_bg_geoid("01", "001", "020100", "1") == "010010201001"
        assert compose_bg_geoid("06", "037", "527700", "3") == "060375277003"

    def test_normalize_creates_bg_geoid(self):
        df = pd.DataFrame({
            "state": ["01", "06"],
            "county": ["001", "037"],
            "tract": ["020100", "527700"],
            "block group": ["1", "3"],
            "B01001_001E": [1000, 50000],
        })

        result = normalize_acs_blockgroup(df)

        assert "bg_geoid" in result.columns
        assert result["bg_geoid"].tolist() == ["010010201001", "060375277003"]

    def test_normalize_adds_hierarchy(self):
        df = pd.DataFrame({
            "state": ["01"],
            "county": ["001"],
            "tract": ["020100"],
            "block group": ["1"],
            "B01001_001E": [1000],
        })

        result = normalize_acs_blockgroup(df)

        assert "tract_geoid" in result.columns
        assert result["tract_geoid"].iloc[0] == "01001020100"
        assert "county_geoid" in result.columns
        assert result["county_geoid"].iloc[0] == "01001"

    def test_normalize_drops_raw_geo_columns(self):
        df = pd.DataFrame({
            "state": ["01"],
            "county": ["001"],
            "tract": ["020100"],
            "block group": ["1"],
            "B01001_001E": [1000],
        })

        result = normalize_acs_blockgroup(df)

        # Raw Census API geo columns should be dropped
        for col in ["state", "county", "tract", "block group"]:
            assert col not in result.columns

    def test_normalize_renames_known_variables(self):
        df = pd.DataFrame({
            "state": ["01"],
            "county": ["001"],
            "tract": ["020100"],
            "block group": ["1"],
            "B01001_001E": [1000],
            "B19013_001E": [55000],
        })

        result = normalize_acs_blockgroup(df)

        assert "total_population" in result.columns
        assert "median_household_income" in result.columns
