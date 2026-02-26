"""Master spine schema definitions.

Defines the exact column specifications for:
  A) master_spine_crosswalk.parquet (block-grain)
  B) master_spine_bg_index.parquet (bg-grain projection)
"""

import pyarrow as pa

# Block-grain spine columns
SPINE_COLUMNS = {
    "block_geoid": pa.string(),
    "bg_geoid": pa.string(),
    "tract_geoid": pa.string(),
    "county_geoid": pa.string(),
    "state_fips": pa.string(),
    "state_abbr": pa.string(),
    "msa_geoid": pa.string(),
    "mega_region_id": pa.string(),
    "gross_land_area_sq_m": pa.float64(),
    "water_area_sq_m": pa.float64(),
    "protected_area_sq_m": pa.float64(),
    "net_developable_area_sq_m": pa.float64(),
}

SPINE_SCHEMA = pa.schema([
    pa.field(name, dtype, nullable=(name in ("msa_geoid", "mega_region_id")))
    for name, dtype in SPINE_COLUMNS.items()
])

SPINE_PRIMARY_KEY = "block_geoid"
SPINE_SECONDARY_INDEX = "bg_geoid"

# BG-grain index columns (derived from block-grain spine)
BG_INDEX_COLUMNS = {
    "bg_geoid": pa.string(),
    "tract_geoid": pa.string(),
    "county_geoid": pa.string(),
    "state_fips": pa.string(),
    "state_abbr": pa.string(),
    "msa_geoid": pa.string(),
    "mega_region_id": pa.string(),
    "gross_land_area_sq_m_bg": pa.float64(),
    "water_area_sq_m_bg": pa.float64(),
    "protected_area_sq_m_bg": pa.float64(),
    "net_developable_area_sq_m": pa.float64(),
}

BG_INDEX_SCHEMA = pa.schema([
    pa.field(name, dtype, nullable=(name in ("msa_geoid", "mega_region_id")))
    for name, dtype in BG_INDEX_COLUMNS.items()
])

BG_INDEX_PRIMARY_KEY = "bg_geoid"


def derive_bg_index_from_spine(spine_df):
    """Derive the BG-grain index from the block-grain spine.

    Aggregates area columns by bg_geoid and takes first value for
    hierarchy columns (which are identical within a block group).
    """
    import pandas as pd

    hierarchy_cols = ["tract_geoid", "county_geoid", "state_fips", "state_abbr",
                      "msa_geoid", "mega_region_id"]
    area_cols = ["gross_land_area_sq_m", "water_area_sq_m",
                 "protected_area_sq_m", "net_developable_area_sq_m"]

    hierarchy = spine_df.groupby("bg_geoid")[hierarchy_cols].first()

    areas = spine_df.groupby("bg_geoid")[area_cols].sum()
    areas = areas.rename(columns={
        "gross_land_area_sq_m": "gross_land_area_sq_m_bg",
        "water_area_sq_m": "water_area_sq_m_bg",
        "protected_area_sq_m": "protected_area_sq_m_bg",
    })

    bg_index = pd.concat([hierarchy, areas], axis=1).reset_index()
    return bg_index
