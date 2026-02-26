"""Smoke test for LODES WAC ingest pipeline."""

import pandas as pd
import pytest

from backend.transforms.ingest.native_block_to_blockgroup import ingest_native_block
from backend.transforms.features.lodes_metrics import add_lodes_metrics, simpson_diversity_index


def make_synthetic_wac(n_blocks=50):
    """Create synthetic LODES WAC data."""
    import numpy as np
    np.random.seed(42)

    rows = []
    for i in range(n_blocks):
        state = "01" if i < 25 else "06"
        block = f"{state}001020100{i % 3 + 1}{str(i).zfill(3)}"
        row = {
            "block_geoid": block,
            "C000": np.random.randint(10, 1000),
        }
        for j in range(1, 21):
            row[f"CNS{j:02d}"] = np.random.randint(0, row["C000"] // 5 + 1)
        rows.append(row)

    return pd.DataFrame(rows)


def make_synthetic_spine(wac_df):
    """Create a minimal spine matching the WAC data."""
    from backend.geo.geoid import add_hierarchy_columns

    blocks = pd.DataFrame({"block_geoid": wac_df["block_geoid"].unique()})
    blocks = add_hierarchy_columns(blocks)
    blocks["msa_geoid"] = None
    blocks["net_developable_area_sq_m"] = 50000.0
    return blocks


class TestLODESWACIngest:

    def test_block_to_bg_aggregation(self):
        wac = make_synthetic_wac()
        spine = make_synthetic_spine(wac)

        agg_rules = {"C000": "sum", "CNS05": "sum"}
        result = ingest_native_block(wac, spine, "block_geoid", agg_rules)

        assert "bg_geoid" in result.columns
        assert result["bg_geoid"].is_unique
        assert result["C000"].sum() > 0

    def test_simpson_diversity(self):
        wac = make_synthetic_wac()
        sdi = simpson_diversity_index(wac, "C000")

        assert len(sdi) == len(wac)
        assert (sdi >= 0).all()
        assert (sdi <= 1).all()

    def test_lodes_metrics_added(self):
        wac = make_synthetic_wac()
        result = add_lodes_metrics(wac, "C000")

        assert "simpson_diversity_index" in result.columns
        assert "manufacturing_share" in result.columns
        assert "baumol_risk" in result.columns
