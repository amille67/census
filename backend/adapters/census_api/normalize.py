"""ACS data normalization.

Creates bg_geoid from Census API response components
(state + county + tract + block group).
"""

import pandas as pd

from backend.geo.geoid import compose_bg_geoid, add_bg_hierarchy_columns
from backend.models.census_acs import ACS_VARIABLE_LABELS
from backend.utils.logging import get_logger

logger = get_logger("adapters.census_api.normalize")


def normalize_acs_blockgroup(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize ACS block group data from Census API response.

    Composes bg_geoid from state + county + tract + block group columns,
    renames variables to human-readable names, and adds hierarchy columns.
    """
    df = df.copy()

    # Compose bg_geoid from Census API geography components
    df["bg_geoid"] = df.apply(
        lambda row: compose_bg_geoid(
            str(row.get("state", "")),
            str(row.get("county", "")),
            str(row.get("tract", "")),
            str(row.get("block group", "")),
        ),
        axis=1,
    )

    # Rename variables to human-readable names where available
    rename_map = {k: v for k, v in ACS_VARIABLE_LABELS.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Convert numeric columns
    for col in df.columns:
        if col not in ("bg_geoid", "state", "county", "tract", "block group",
                        "GEO_ID", "NAME"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add hierarchy
    df = add_bg_hierarchy_columns(df, "bg_geoid")

    # Drop raw geography columns from Census API
    drop_cols = ["state", "county", "tract", "block group", "GEO_ID", "NAME"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    logger.info("Normalized ACS data: %d block groups", len(df))
    return df
