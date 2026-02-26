"""Tests for EPA FRS block group aggregation."""

import numpy as np
import pandas as pd
import pytest

from backend.adapters.epa_frs.aggregate import aggregate_frs_to_blockgroup


def make_facility_df(n=50):
    """Create facility-level DataFrame with bg_geoid (post spine-join)."""
    np.random.seed(42)
    rows = []
    bgs = [f"01001020100{i}" for i in range(1, 6)]  # 5 BGs

    for i in range(n):
        rows.append({
            "registry_id": f"110{str(i).zfill(9)}",
            "bg_geoid": bgs[i % len(bgs)],
            "site_type_name": ["Stationary", "Mobile", "Federal"][i % 3],
            "federal_facility_flag": (i % 5 == 0),
            "tribal_land_flag": (i % 10 == 0),
            "program_count": np.random.randint(1, 6),
            "has_rcra": i % 4 == 0,
            "has_npdes": i % 3 == 0,
            "has_air": i % 2 == 0,
            "has_tri": i % 5 == 0,
            "has_cerclis": i % 7 == 0,
            "accuracy_value": round(np.random.uniform(0.1, 100.0), 2),
        })

    return pd.DataFrame(rows)


class TestFRSAggregate:

    def test_output_at_bg_grain(self):
        df = make_facility_df()
        result = aggregate_frs_to_blockgroup(df)
        assert "bg_geoid" in result.columns
        assert result["bg_geoid"].is_unique

    def test_facility_count(self):
        df = make_facility_df(50)
        result = aggregate_frs_to_blockgroup(df)
        assert "frs_facility_count" in result.columns
        assert result["frs_facility_count"].sum() == df["registry_id"].nunique()

    def test_program_flags_are_int(self):
        df = make_facility_df()
        result = aggregate_frs_to_blockgroup(df)
        for col in ["frs_has_rcra", "frs_has_npdes", "frs_has_air", "frs_has_tri", "frs_has_cerclis"]:
            if col in result.columns:
                assert result[col].dtype in [int, np.int64, np.int32, np.int8]

    def test_federal_count(self):
        df = make_facility_df()
        result = aggregate_frs_to_blockgroup(df)
        assert "frs_federal_facility_count" in result.columns
        assert result["frs_federal_facility_count"].sum() == df["federal_facility_flag"].sum()

    def test_requires_bg_geoid(self):
        df = pd.DataFrame({"registry_id": ["001"]})
        with pytest.raises(ValueError, match="bg_geoid"):
            aggregate_frs_to_blockgroup(df)

    def test_site_type_diversity(self):
        df = make_facility_df()
        result = aggregate_frs_to_blockgroup(df)
        assert "frs_site_type_diversity" in result.columns
        # With 3 site types across 5 BGs, each BG should have at least 1
        assert (result["frs_site_type_diversity"] >= 1).all()
