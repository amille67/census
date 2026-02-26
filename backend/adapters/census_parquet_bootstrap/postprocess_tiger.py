"""Post-process TIGER data for spine construction.

Normalizes block/BG/MSA/areawater parquet files into the standardized
schema needed by the spine builder.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

from backend.geo.constants import CANONICAL_CRS, FIPS_TO_ABBR
from backend.geo.geoid import add_hierarchy_columns
from backend.geo.crs import ensure_crs, compute_area_sq_m
from backend.geo.topology_fixes import fix_invalid_geometries
from backend.utils.logging import get_logger

logger = get_logger("adapters.bootstrap.postprocess_tiger")


def normalize_blocks(blocks_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize block-level TIGER data for spine construction.

    Expects GEOID as index. Outputs standardized columns.
    """
    blocks = blocks_gdf.reset_index()
    blocks = blocks.rename(columns={"GEOID": "block_geoid"})
    blocks["block_geoid"] = blocks["block_geoid"].astype(str).str.zfill(15)

    blocks = add_hierarchy_columns(blocks, "block_geoid")
    blocks = fix_invalid_geometries(blocks)
    blocks = ensure_crs(blocks, CANONICAL_CRS)

    if "ALAND" in blocks.columns:
        blocks["gross_land_area_sq_m"] = blocks["ALAND"].astype(float)
    elif "ALAND20" in blocks.columns:
        blocks["gross_land_area_sq_m"] = blocks["ALAND20"].astype(float)
    else:
        blocks["gross_land_area_sq_m"] = compute_area_sq_m(blocks)

    keep_cols = [
        "block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
        "state_fips", "state_abbr", "gross_land_area_sq_m", "geometry",
    ]
    return blocks[[c for c in keep_cols if c in blocks.columns]]


def normalize_block_groups(bg_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize block group TIGER polygons."""
    bg = bg_gdf.copy()
    if "GEOID" in bg.columns:
        bg = bg.rename(columns={"GEOID": "bg_geoid"})
    elif "GEOID20" in bg.columns:
        bg = bg.rename(columns={"GEOID20": "bg_geoid"})

    bg["bg_geoid"] = bg["bg_geoid"].astype(str).str.zfill(12)
    bg = fix_invalid_geometries(bg)
    bg = ensure_crs(bg, CANONICAL_CRS)

    return bg[["bg_geoid", "geometry"]]


def normalize_msa(msa_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize MSA/CBSA polygons for centroid spatial join."""
    msa = msa_gdf.copy()
    if "CBSAFP" in msa.columns:
        msa = msa.rename(columns={"CBSAFP": "msa_geoid"})
    elif "GEOID" in msa.columns:
        msa = msa.rename(columns={"GEOID": "msa_geoid"})

    msa["msa_geoid"] = msa["msa_geoid"].astype(str).str.zfill(5)
    msa = fix_invalid_geometries(msa)
    msa = ensure_crs(msa, CANONICAL_CRS)

    return msa[["msa_geoid", "geometry"]]
