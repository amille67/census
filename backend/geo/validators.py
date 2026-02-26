"""GEOID validation utilities.

Validates key length, regex pattern, and type conformity for all
canonical geography keys.
"""

import re

import pandas as pd

from backend.geo.constants import (
    BLOCK_GEOID_LEN,
    BG_GEOID_LEN,
    TRACT_GEOID_LEN,
    COUNTY_GEOID_LEN,
    STATE_FIPS_LEN,
)

_PATTERNS = {
    "block_geoid": re.compile(r"^\d{15}$"),
    "bg_geoid": re.compile(r"^\d{12}$"),
    "tract_geoid": re.compile(r"^\d{11}$"),
    "county_geoid": re.compile(r"^\d{5}$"),
    "state_fips": re.compile(r"^\d{2}$"),
    "msa_geoid": re.compile(r"^\d{5}$"),
}

_LENGTHS = {
    "block_geoid": BLOCK_GEOID_LEN,
    "bg_geoid": BG_GEOID_LEN,
    "tract_geoid": TRACT_GEOID_LEN,
    "county_geoid": COUNTY_GEOID_LEN,
    "state_fips": STATE_FIPS_LEN,
}


def validate_geoid(value: str, key_type: str) -> bool:
    """Validate a single GEOID string against its key type pattern."""
    pattern = _PATTERNS.get(key_type)
    if pattern is None:
        raise ValueError(f"Unknown key type: {key_type}")
    return bool(pattern.match(str(value)))


def validate_block_geoid(value: str) -> bool:
    return validate_geoid(value, "block_geoid")


def validate_bg_geoid(value: str) -> bool:
    return validate_geoid(value, "bg_geoid")


def validate_geoid_series(series: pd.Series, key_type: str) -> pd.Series:
    """Validate a pandas Series of GEOIDs. Returns boolean Series."""
    pattern = _PATTERNS.get(key_type)
    if pattern is None:
        raise ValueError(f"Unknown key type: {key_type}")
    return series.astype(str).str.match(pattern.pattern)


def check_geoid_column(df: pd.DataFrame, column: str, key_type: str) -> dict:
    """Run validation checks on a GEOID column and return a report."""
    series = df[column].astype(str)
    valid_mask = validate_geoid_series(series, key_type)
    total = len(series)
    n_valid = valid_mask.sum()
    n_null = df[column].isna().sum()
    expected_len = _LENGTHS.get(key_type)

    return {
        "column": column,
        "key_type": key_type,
        "total_rows": total,
        "valid_count": int(n_valid),
        "invalid_count": int(total - n_valid),
        "null_count": int(n_null),
        "valid_rate": n_valid / total if total > 0 else 0.0,
        "length_check": bool(
            (series.str.len() == expected_len).all()
        ) if expected_len else None,
        "is_unique": bool(series.is_unique),
    }
