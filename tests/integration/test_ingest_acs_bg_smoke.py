"""Smoke test for ACS block group ingest pipeline."""

import pandas as pd
import pytest

from backend.adapters.census_api.normalize import normalize_acs_blockgroup
from backend.transforms.ingest.native_bg_to_blockgroup import ingest_native_bg


def make_synthetic_acs():
    """Create synthetic ACS API response data."""
    return pd.DataFrame({
        "state": ["01", "01", "06"],
        "county": ["001", "001", "037"],
        "tract": ["020100", "020100", "527700"],
        "block group": ["1", "2", "3"],
        "B01001_001E": [1000, 2000, 50000],
        "B19013_001E": [45000, 55000, 75000],
        "B25001_001E": [400, 800, 20000],
    })


def make_synthetic_bg_index():
    """Create a minimal BG spine index."""
    return pd.DataFrame({
        "bg_geoid": ["010010201001", "010010201002", "060375277003", "360610001001"],
        "tract_geoid": ["01001020100", "01001020100", "06037527700", "36061000100"],
        "county_geoid": ["01001", "01001", "06037", "36061"],
        "state_fips": ["01", "01", "06", "36"],
        "msa_geoid": [None, None, "31080", "35620"],
        "net_developable_area_sq_m": [50000, 60000, 100000, 200000],
    })


class TestACSIngest:

    def test_normalize_creates_bg_geoid(self):
        acs = make_synthetic_acs()
        result = normalize_acs_blockgroup(acs)
        assert "bg_geoid" in result.columns
        assert len(result) == 3

    def test_bg_to_spine_no_row_multiplication(self):
        acs = make_synthetic_acs()
        normalized = normalize_acs_blockgroup(acs)
        bg_index = make_synthetic_bg_index()

        result = ingest_native_bg(normalized, bg_index)

        # Should NOT increase row count (this is the key test!)
        assert len(result) == len(normalized)

    def test_spine_hierarchy_attached(self):
        acs = make_synthetic_acs()
        normalized = normalize_acs_blockgroup(acs)
        bg_index = make_synthetic_bg_index()

        result = ingest_native_bg(normalized, bg_index)

        # Should have hierarchy from spine
        al_row = result[result["bg_geoid"] == "010010201001"].iloc[0]
        assert al_row["county_geoid"] == "01001"
