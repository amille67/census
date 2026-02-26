"""Normalize FRS data for hybrid ingest.

The FRS national single file contains:
  - Census Block Code (field 39): 15-digit block GEOID for many records
  - Latitude/Longitude: NAD83 coordinates for most records

This module splits records into two populations:
  1. Records WITH a valid Census Block Code -> Scenario B (native block merge)
  2. Records WITHOUT block code but WITH lat/lon -> Scenario A (point-in-polygon)

And derives standardized columns for both paths.
"""

import re

import pandas as pd

from backend.geo.constants import BLOCK_GEOID_LEN
from backend.utils.logging import get_logger

logger = get_logger("adapters.epa_frs.normalize")

_BLOCK_GEOID_PATTERN = re.compile(r"^\d{15}$")


def _has_valid_block_code(series: pd.Series) -> pd.Series:
    """Check which rows have a valid 15-digit Census Block Code."""
    cleaned = series.fillna("").astype(str).str.strip()
    return cleaned.str.match(r"^\d{15}$")


def _has_valid_coords(df: pd.DataFrame) -> pd.Series:
    """Check which rows have valid lat/lon coordinates."""
    lat_valid = pd.to_numeric(df["latitude"], errors="coerce").notna()
    lon_valid = pd.to_numeric(df["longitude"], errors="coerce").notna()
    # Basic bounds check for CONUS + AK/HI/PR
    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")
    in_bounds = (lat >= 17.0) & (lat <= 72.0) & (lon >= -180.0) & (lon <= -65.0)
    return lat_valid & lon_valid & in_bounds


def _parse_program_acronyms(series: pd.Series) -> pd.DataFrame:
    """Parse the pipe-delimited PGM_SYS_ACRNMS into structured columns.

    Returns DataFrame with:
      - program_count: number of distinct programs
      - has_rcra: bool
      - has_npdes: bool
      - has_air: bool
      - has_tri: bool
      - has_cerclis: bool
    """
    cleaned = series.fillna("").astype(str)
    split = cleaned.str.split(r"\s*\|\s*")
    program_count = split.apply(lambda x: len([p for p in x if p.strip()]))

    # Check for common major program systems
    upper = cleaned.str.upper()
    return pd.DataFrame({
        "program_count": program_count,
        "has_rcra": upper.str.contains("RCRA", na=False),
        "has_npdes": upper.str.contains("NPDES", na=False),
        "has_air": upper.str.contains("AIR|ICIS-AIR|CAA", na=False),
        "has_tri": upper.str.contains("TRI", na=False),
        "has_cerclis": upper.str.contains("CERCLIS|SEMS", na=False),
    })


def normalize_frs(df: pd.DataFrame) -> dict:
    """Normalize FRS data and split into block-coded and point populations.

    Args:
        df: Parsed FRS DataFrame from parse_frs_single_file()

    Returns:
        Dict with keys:
          - 'block_df': DataFrame with valid census_block_code -> Scenario B
          - 'point_df': DataFrame with lat/lon but no block code -> Scenario A
          - 'dropped_df': DataFrame of records that can't be geocoded
          - 'stats': Dict of population statistics
    """
    total = len(df)
    logger.info("Normalizing %d FRS records", total)

    # Derive program flags
    if "pgm_sys_acrnms" in df.columns:
        programs = _parse_program_acronyms(df["pgm_sys_acrnms"])
        df = pd.concat([df, programs], axis=1)
    else:
        df["program_count"] = 0
        df["has_rcra"] = False
        df["has_npdes"] = False
        df["has_air"] = False
        df["has_tri"] = False
        df["has_cerclis"] = False

    # Convert flag columns to boolean
    for flag_col in ["federal_facility_flag", "tribal_land_flag"]:
        if flag_col in df.columns:
            df[flag_col] = df[flag_col].fillna("N").str.upper().str.startswith("Y")

    # Split population: records with valid block code vs. lat/lon only
    has_block = pd.Series(False, index=df.index)
    if "census_block_code" in df.columns:
        has_block = _has_valid_block_code(df["census_block_code"])

    has_coords = _has_valid_coords(df)

    # Population A: has valid block code (Scenario B path)
    block_mask = has_block
    block_df = df[block_mask].copy()
    block_df["block_geoid"] = (
        block_df["census_block_code"].astype(str).str.strip().str.zfill(BLOCK_GEOID_LEN)
    )

    # Population B: no block code but has coords (Scenario A path)
    point_mask = ~has_block & has_coords
    point_df = df[point_mask].copy()
    point_df["latitude"] = pd.to_numeric(point_df["latitude"], errors="coerce")
    point_df["longitude"] = pd.to_numeric(point_df["longitude"], errors="coerce")

    # Population C: neither (dropped)
    dropped_mask = ~has_block & ~has_coords
    dropped_df = df[dropped_mask].copy()

    stats = {
        "total_records": total,
        "block_coded_count": len(block_df),
        "block_coded_pct": len(block_df) / total * 100 if total > 0 else 0,
        "point_only_count": len(point_df),
        "point_only_pct": len(point_df) / total * 100 if total > 0 else 0,
        "dropped_count": len(dropped_df),
        "dropped_pct": len(dropped_df) / total * 100 if total > 0 else 0,
    }

    logger.info(
        "FRS split: %d block-coded (%.1f%%), %d point-only (%.1f%%), %d dropped (%.1f%%)",
        stats["block_coded_count"], stats["block_coded_pct"],
        stats["point_only_count"], stats["point_only_pct"],
        stats["dropped_count"], stats["dropped_pct"],
    )

    return {
        "block_df": block_df,
        "point_df": point_df,
        "dropped_df": dropped_df,
        "stats": stats,
    }
