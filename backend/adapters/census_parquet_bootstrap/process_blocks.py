"""Process Census block shapefiles with PL population data.

Adapted from makepath/census-parquet process_blocks.py.
Creates three outputs:
  1. census_blocks_geo.parquet (geometry only)
  2. census_population.parquet (population only)
  3. census_blocks_pops.parquet (combined)
"""

import warnings
from pathlib import Path

import geopandas
import pandas as pd

from backend.geo.constants import FIPS_TO_ABBR, CANONICAL_CRS
from backend.utils.logging import get_logger

logger = get_logger("adapters.bootstrap.process_blocks")

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")


def process_geo(file_path: Path) -> geopandas.GeoDataFrame:
    """Process a single state's block shapefile into a GeoDataFrame.

    Adapted from census-parquet process_geo().
    """
    dtypes = {
        "STATEFP": "int", "COUNTYFP": "int",
        "TRACTCE": "int", "BLOCKCE": "int",
        "HOUSING": "int", "POP": "int",
    }

    gdf = (
        geopandas.read_file(file_path)
        .drop(columns=["MTFCC20", "UR20", "UACE20", "UATYPE20", "FUNCSTAT20", "NAME20"],
              errors="ignore")
        .rename(columns=lambda x: x.rstrip("20") if x.endswith("20") and x != "GEOID20" else x)
    )

    if "GEOID20" in gdf.columns and "GEOID" not in gdf.columns:
        gdf = gdf.rename(columns={"GEOID20": "GEOID"})

    applicable_dtypes = {k: v for k, v in dtypes.items() if k in gdf.columns}
    gdf = gdf.astype(applicable_dtypes)
    gdf = gdf.set_index("GEOID")

    for col in ["INTPTLON", "INTPTLAT"]:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce")

    gdf = gdf.replace([None], 0)
    return gdf


def process_pop(file_path: Path, pl_dir: Path, summary_table: Path) -> pd.DataFrame:
    """Process PL 94-171 population data for a single state.

    Adapted from census-parquet process_pop().
    """
    fips = file_path.stem.split("_")[2]
    if fips not in FIPS_TO_ABBR:
        return None

    abbr = FIPS_TO_ABBR[fips].lower()

    state_1 = pl_dir / f"{abbr}000012020.pl"
    state_geo = pl_dir / f"{abbr}geo2020.pl"

    if not state_1.exists() or not state_geo.exists():
        logger.warning("Missing PL files for state %s (%s)", fips, abbr)
        return None

    seg_1_header_df = pd.read_excel(summary_table, sheet_name="2020 P.L. Segment 1 Fields")
    geo_header_df = pd.read_excel(summary_table, sheet_name="2020 P.L. Geoheader Fields")

    seg_1_df = pd.read_csv(
        state_1, encoding="latin-1", delimiter="|",
        names=seg_1_header_df.columns.to_list(), low_memory=False,
    ).drop(columns=["STUSAB"], errors="ignore")

    geo_df = pd.read_csv(
        state_geo, encoding="latin-1", delimiter="|",
        names=geo_header_df.columns.to_list(), low_memory=False,
    )
    geo_df = geo_df[geo_df["SUMLEV"] == 750]

    block_df = pd.merge(
        left=geo_df[["LOGRECNO", "GEOID", "STUSAB"]],
        right=seg_1_df,
        how="left",
        on="LOGRECNO",
    ).drop(columns=["LOGRECNO", "CHARITER", "STUSAB", "FILEID", "CIFSN"], errors="ignore")

    block_df["GEOID"] = block_df["GEOID"].str.replace("7500000US", "", regex=False)
    block_df = block_df.set_index("GEOID").sort_index()

    return block_df


def process_state_blocks(
    shapefile_path: Path,
    pl_dir: Path,
    summary_table: Path,
    output_dir: Path,
) -> dict:
    """Process a single state's blocks: geometry + population.

    Adapted from census-parquet process_pop_geo().
    """
    fips = shapefile_path.stem.split("_")[2]
    logger.info("Processing state FIPS %s", fips)

    geo = process_geo(shapefile_path)
    pop = process_pop(shapefile_path, pl_dir, summary_table)

    geo_out = output_dir / "geo" / f"{fips}.parquet"
    geo_out.parent.mkdir(parents=True, exist_ok=True)
    geo.to_parquet(geo_out)

    result = {"geo": geo_out, "pop": None, "combined": None}

    if pop is not None:
        pop_out = output_dir / "pop" / f"{fips}.parquet"
        pop_out.parent.mkdir(parents=True, exist_ok=True)
        pop.to_parquet(pop_out)
        result["pop"] = pop_out

        combined = pd.merge(geo, pop, left_index=True, right_index=True)
        comb_out = output_dir / "combined" / f"{fips}.parquet"
        comb_out.parent.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(comb_out)
        result["combined"] = comb_out

    logger.info("Processed state %s: %d blocks", fips, len(geo))
    return result
