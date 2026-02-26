"""GEOID derivation utilities.

Derives bg_geoid, tract_geoid, county_geoid, and state_fips from block_geoid
using substring slicing. Zero spatial cost - purely string operations.

This module is the canonical source of truth for GEOID derivation in the
entire pipeline.
"""

import pandas as pd

from backend.geo.constants import (
    BLOCK_GEOID_LEN,
    BG_GEOID_LEN,
    TRACT_GEOID_LEN,
    COUNTY_GEOID_LEN,
    STATE_FIPS_LEN,
    FIPS_TO_ABBR,
)


def block_to_bg(block_geoid: str) -> str:
    """Derive block group GEOID from block GEOID."""
    return block_geoid[:BG_GEOID_LEN]


def block_to_tract(block_geoid: str) -> str:
    """Derive tract GEOID from block GEOID."""
    return block_geoid[:TRACT_GEOID_LEN]


def block_to_county(block_geoid: str) -> str:
    """Derive county GEOID from block GEOID."""
    return block_geoid[:COUNTY_GEOID_LEN]


def block_to_state_fips(block_geoid: str) -> str:
    """Derive state FIPS from block GEOID."""
    return block_geoid[:STATE_FIPS_LEN]


def state_fips_to_abbr(state_fips: str) -> str:
    """Convert state FIPS code to two-letter abbreviation."""
    return FIPS_TO_ABBR.get(state_fips, "")


def derive_all_from_block(block_geoid: str) -> dict:
    """Derive all hierarchy keys from a single block GEOID."""
    state_fips = block_geoid[:STATE_FIPS_LEN]
    return {
        "block_geoid": block_geoid,
        "bg_geoid": block_geoid[:BG_GEOID_LEN],
        "tract_geoid": block_geoid[:TRACT_GEOID_LEN],
        "county_geoid": block_geoid[:COUNTY_GEOID_LEN],
        "state_fips": state_fips,
        "state_abbr": FIPS_TO_ABBR.get(state_fips, ""),
    }


def compose_bg_geoid(state: str, county: str, tract: str, blockgroup: str) -> str:
    """Compose a bg_geoid from Census API response components.

    Args:
        state: 2-digit state FIPS
        county: 3-digit county FIPS
        tract: 6-digit tract code
        blockgroup: 1-digit block group code

    Returns:
        12-digit bg_geoid string
    """
    return f"{state.zfill(2)}{county.zfill(3)}{tract.zfill(6)}{blockgroup}"


def add_hierarchy_columns(df: pd.DataFrame, block_col: str = "block_geoid") -> pd.DataFrame:
    """Add all hierarchy columns to a DataFrame with a block_geoid column.

    Vectorized string slicing for performance on large DataFrames.
    """
    geoid = df[block_col].astype(str).str.zfill(BLOCK_GEOID_LEN)
    df = df.copy()
    df["bg_geoid"] = geoid.str[:BG_GEOID_LEN]
    df["tract_geoid"] = geoid.str[:TRACT_GEOID_LEN]
    df["county_geoid"] = geoid.str[:COUNTY_GEOID_LEN]
    df["state_fips"] = geoid.str[:STATE_FIPS_LEN]
    df["state_abbr"] = df["state_fips"].map(FIPS_TO_ABBR).fillna("")
    return df


def add_bg_hierarchy_columns(df: pd.DataFrame, bg_col: str = "bg_geoid") -> pd.DataFrame:
    """Add tract/county/state hierarchy from bg_geoid. For BG-native sources."""
    geoid = df[bg_col].astype(str).str.zfill(BG_GEOID_LEN)
    df = df.copy()
    df["tract_geoid"] = geoid.str[:TRACT_GEOID_LEN]
    df["county_geoid"] = geoid.str[:COUNTY_GEOID_LEN]
    df["state_fips"] = geoid.str[:STATE_FIPS_LEN]
    df["state_abbr"] = df["state_fips"].map(FIPS_TO_ABBR).fillna("")
    return df
