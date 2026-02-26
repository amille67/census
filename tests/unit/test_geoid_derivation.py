"""Tests for GEOID derivation logic in backend/geo/geoid.py."""

import pandas as pd
import pytest

from backend.geo.geoid import (
    block_to_bg,
    block_to_tract,
    block_to_county,
    block_to_state_fips,
    state_fips_to_abbr,
    derive_all_from_block,
    compose_bg_geoid,
    add_hierarchy_columns,
    add_bg_hierarchy_columns,
)


class TestScalarDerivation:
    """Test single-value GEOID derivation functions."""

    def test_block_to_bg(self):
        assert block_to_bg("010010201001000") == "010010201001"

    def test_block_to_tract(self):
        assert block_to_tract("010010201001000") == "01001020100"

    def test_block_to_county(self):
        assert block_to_county("010010201001000") == "01001"

    def test_block_to_state_fips(self):
        assert block_to_state_fips("010010201001000") == "01"

    def test_state_fips_to_abbr(self):
        assert state_fips_to_abbr("01") == "AL"
        assert state_fips_to_abbr("06") == "CA"
        assert state_fips_to_abbr("36") == "NY"
        assert state_fips_to_abbr("99") == ""  # Unknown

    def test_derive_all_from_block(self):
        result = derive_all_from_block("060375277003009")
        assert result == {
            "block_geoid": "060375277003009",
            "bg_geoid": "060375277003",
            "tract_geoid": "06037527700",
            "county_geoid": "06037",
            "state_fips": "06",
            "state_abbr": "CA",
        }

    def test_compose_bg_geoid(self):
        assert compose_bg_geoid("01", "001", "020100", "1") == "010010201001"
        assert compose_bg_geoid("6", "037", "527700", "3") == "060375277003"

    def test_derive_all_preserves_15_digit_geoid(self):
        result = derive_all_from_block("720010100001000")
        assert result["state_fips"] == "72"  # Puerto Rico
        assert result["state_abbr"] == "PR"


class TestVectorizedDerivation:
    """Test DataFrame-level GEOID derivation."""

    def test_add_hierarchy_columns(self):
        df = pd.DataFrame({
            "block_geoid": ["010010201001000", "060375277003009", "360610001001000"]
        })
        result = add_hierarchy_columns(df)

        assert result["bg_geoid"].tolist() == ["010010201001", "060375277003", "360610001001"]
        assert result["tract_geoid"].tolist() == ["01001020100", "06037527700", "36061000100"]
        assert result["county_geoid"].tolist() == ["01001", "06037", "36061"]
        assert result["state_fips"].tolist() == ["01", "06", "36"]
        assert result["state_abbr"].tolist() == ["AL", "CA", "NY"]

    def test_add_hierarchy_columns_pads_short_geoids(self):
        df = pd.DataFrame({"block_geoid": ["10010201001000"]})  # 14 digits
        result = add_hierarchy_columns(df)
        assert result["block_geoid"].iloc[0] == "010010201001000"  # padded to 15
        assert result["state_fips"].iloc[0] == "01"

    def test_add_bg_hierarchy_columns(self):
        df = pd.DataFrame({"bg_geoid": ["010010201001", "060375277003"]})
        result = add_bg_hierarchy_columns(df)

        assert result["tract_geoid"].tolist() == ["01001020100", "06037527700"]
        assert result["county_geoid"].tolist() == ["01001", "06037"]
        assert result["state_fips"].tolist() == ["01", "06"]

    def test_original_df_not_modified(self):
        df = pd.DataFrame({"block_geoid": ["010010201001000"]})
        _ = add_hierarchy_columns(df)
        assert "bg_geoid" not in df.columns  # Original unchanged
