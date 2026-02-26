"""Key integrity QA checks.

Enforces join cardinality rules to prevent painful bugs.
"""

import pandas as pd

from backend.utils.logging import get_logger
from backend.utils.exceptions import JoinCardinalityError

logger = get_logger("qa.key_integrity")


def check_unique_key(df: pd.DataFrame, key_col: str) -> dict:
    """Verify a column is a unique key."""
    is_unique = df[key_col].is_unique
    n_dupes = df[key_col].duplicated().sum()
    return {"column": key_col, "is_unique": is_unique, "duplicate_count": int(n_dupes)}


def check_foreign_key_coverage(
    child_df: pd.DataFrame,
    child_key: str,
    parent_df: pd.DataFrame,
    parent_key: str,
) -> dict:
    """Check what fraction of child keys exist in parent."""
    child_keys = set(child_df[child_key].dropna().unique())
    parent_keys = set(parent_df[parent_key].dropna().unique())

    matched = child_keys & parent_keys
    coverage = len(matched) / len(child_keys) if child_keys else 0

    return {
        "child_unique_keys": len(child_keys),
        "parent_unique_keys": len(parent_keys),
        "matched_keys": len(matched),
        "coverage_rate": coverage,
    }


def fail_on_bg_to_block_spine_join(df_name: str = ""):
    """Guard against joining BG-grain data directly to block-grain spine.

    This is the #1 cause of accidental row duplication.
    """
    raise JoinCardinalityError(
        f"FORBIDDEN: BG-grain table '{df_name}' must not be joined directly "
        f"to block-grain spine. Use master_spine_bg_index.parquet instead."
    )
