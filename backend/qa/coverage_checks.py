"""Coverage QA checks."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("qa.coverage")


def check_state_coverage(df: pd.DataFrame, state_col: str = "state_fips") -> dict:
    """Check how many states are represented."""
    n_states = df[state_col].nunique()
    return {
        "n_states": n_states,
        "expected_min": 50,
        "passed": n_states >= 50,
        "states": sorted(df[state_col].unique().tolist()),
    }


def check_bg_coverage(
    ingest_df: pd.DataFrame,
    bg_index_df: pd.DataFrame,
    bg_col: str = "bg_geoid",
) -> dict:
    """Check what fraction of total BGs have data."""
    total_bgs = bg_index_df[bg_col].nunique()
    covered_bgs = ingest_df[bg_col].nunique()
    return {
        "total_bgs": total_bgs,
        "covered_bgs": covered_bgs,
        "coverage_rate": covered_bgs / total_bgs if total_bgs > 0 else 0,
    }
