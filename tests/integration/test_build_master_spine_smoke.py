"""Smoke test for build_master_spine script.

Uses small synthetic data to verify the spine build pipeline end-to-end.
"""

import pandas as pd
import pytest

from backend.transforms.spine.build_block_hierarchy import build_block_hierarchy
from backend.transforms.spine.compute_net_developable_area import compute_net_developable_area
from backend.transforms.spine.validate_spine import validate_spine
from backend.models.master_spine import derive_bg_index_from_spine


def make_synthetic_spine(n_blocks=100):
    """Create a synthetic block-level DataFrame for testing."""
    import numpy as np

    np.random.seed(42)
    state_fips = ["01", "06", "36"]
    blocks = []
    for state in state_fips:
        county = f"{state}001"
        tract = f"{county}020100"
        for i in range(n_blocks // len(state_fips)):
            bg = f"{tract}{i % 3 + 1}"
            block = f"{bg}{str(i).zfill(3)}"
            blocks.append({
                "block_geoid": block,
                "gross_land_area_sq_m": np.random.uniform(1000, 100000),
            })

    return pd.DataFrame(blocks)


class TestSpineBuildSmoke:

    def test_hierarchy_derivation(self):
        df = make_synthetic_spine()
        result = build_block_hierarchy(df)

        assert "bg_geoid" in result.columns
        assert "tract_geoid" in result.columns
        assert "county_geoid" in result.columns
        assert "state_fips" in result.columns
        assert "state_abbr" in result.columns

        # Check all derived correctly
        for _, row in result.head(5).iterrows():
            assert row["bg_geoid"] == row["block_geoid"][:12]
            assert row["tract_geoid"] == row["block_geoid"][:11]
            assert row["county_geoid"] == row["block_geoid"][:5]
            assert row["state_fips"] == row["block_geoid"][:2]

    def test_net_developable_area(self):
        df = make_synthetic_spine()
        df = build_block_hierarchy(df)
        df["water_area_sq_m"] = 100.0
        df["protected_area_sq_m"] = 50.0

        result = compute_net_developable_area(df)

        assert "net_developable_area_sq_m" in result.columns
        assert (result["net_developable_area_sq_m"] >= 0).all()
        expected = result["gross_land_area_sq_m"] - 100.0 - 50.0
        expected = expected.clip(lower=0)
        pd.testing.assert_series_equal(
            result["net_developable_area_sq_m"], expected, check_names=False
        )

    def test_bg_index_derivation(self):
        df = make_synthetic_spine()
        df = build_block_hierarchy(df)
        df["water_area_sq_m"] = 0.0
        df["protected_area_sq_m"] = 0.0
        df = compute_net_developable_area(df)
        df["msa_geoid"] = None
        df["mega_region_id"] = None

        bg_index = derive_bg_index_from_spine(df)

        assert bg_index["bg_geoid"].is_unique
        assert len(bg_index) < len(df)  # Fewer BGs than blocks
        assert "gross_land_area_sq_m_bg" in bg_index.columns

    def test_validation_on_synthetic(self):
        df = make_synthetic_spine()
        df = build_block_hierarchy(df)
        df["water_area_sq_m"] = 0.0
        df["protected_area_sq_m"] = 0.0
        df = compute_net_developable_area(df)
        df["msa_geoid"] = None
        df["mega_region_id"] = None

        report = validate_spine(df)
        # Hierarchy should be consistent
        hierarchy_check = [c for c in report["checks"] if c["name"] == "hierarchy_consistency"][0]
        assert hierarchy_check["passed"]
