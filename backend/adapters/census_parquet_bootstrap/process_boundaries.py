"""Process Census boundary shapefiles into Parquet.

Adapted from makepath/census-parquet process_boundaries.py.
Uses the same dtype normalization pattern but writes to our standard paths.
"""

import warnings
from pathlib import Path

import geopandas
import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("adapters.bootstrap.process_boundaries")

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

# Dtype mapping from census-parquet for TIGER boundary columns
DTYPES = {
    "AFFGEOID": "string", "AFFGEOID20": "string",
    "AIANNHCE": "int", "AIANNHNS": "int",
    "ALAND": "int", "ALAND20": "int",
    "AWATER": "int", "AWATER20": "int",
    "ANRCFP": "int", "ANRCNS": "int",
    "BLKGRPCE": "category",
    "CBSAFP": "int", "CD116FP": "int", "CDSESSN": "int",
    "CNECTAFP": "category",
    "CONCTYFP": "int", "CONCTYNS": "int",
    "COUNTYNS": "string", "COUNTYFP": "category", "COUNTYFP20": "category",
    "COUSUBFP": "category", "COUSUBNS": "string",
    "DIVISIONCE": "int", "ELSDLEA": "int",
    "GEOID": "string", "GEOID20": "string",
    "LSAD": "category", "LSAD20": "category", "LSY": "category",
    "METDIVFP": "int",
    "NAME": "string", "NAME20": "string",
    "NAMELSAD": "string", "NAMELSAD20": "string", "NAMELSADCO": "category",
    "NCTADVFP": "int", "NECTAFP": "int",
    "PARTFLG": "category",
    "PLACEFP": "int", "PLACENS": "int",
    "REGIONCE": "int", "SCSDLEA": "int",
    "SLDLST": "string", "SLDUST": "string",
    "STATE_NAME": "category",
    "STATEFP": "category", "STATEFP20": "category",
    "STATENS": "int", "STUSPS": "category",
    "SUBMCDFP": "int", "SUBMCDNS": "int",
    "TBLKGPCE": "category",
    "TRACTCE": "int", "TTRACTCE": "category",
    "TRSUBCE": "int", "TRSUBNS": "int",
    "UNSDLEA": "int",
    "VTDI20": "category", "VTDST20": "string",
}


def process_boundary_file(path: Path, output_dir: Path) -> Path:
    """Process a single boundary shapefile to Parquet.

    Adapted from census-parquet process_boundary_file().
    """
    logger.info("Processing boundary: %s", path)
    gdf = geopandas.read_file(path)

    applicable_dtypes = {k: DTYPES[k] for k in set(gdf.columns) & set(DTYPES)}
    gdf = gdf.astype(applicable_dtypes)

    if "CSAFP" in gdf.columns:
        gdf["CSAFP"] = gdf["CSAFP"].astype("float64").astype(pd.Int64Dtype())

    output_path = output_dir / Path(path).with_suffix(".parquet").name
    output_dir.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path, index=False)

    logger.info("Wrote boundary parquet: %s (%d rows)", output_path, len(gdf))
    return output_path


def process_all_boundaries(input_dir: Path, output_dir: Path) -> list:
    """Process all boundary shapefiles in a directory."""
    files = list(input_dir.glob("*.shp")) + list(input_dir.glob("*.zip"))
    logger.info("Found %d boundary files to process", len(files))

    results = []
    for f in files:
        output = process_boundary_file(f, output_dir)
        results.append(output)

    return results
