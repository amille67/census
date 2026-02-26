"""Parse the EPA FRS national single facility CSV.

Maps the 39-field schema from the FRS Database Documentation (v1.1.1)
to standardized column names for the ETL pipeline.

EPA Schema Reference (Single Facility File):
  Field  1: FRS Facility Identifier (REGISTRY_ID) - A, 12 - Primary Key
  Field  2: Primary Name (PRIMARY_NAME) - A, 250
  Field  3: Location Address (LOCATION_ADDRESS) - A, 200
  Field  4: Supplemental Location (SUPPLEMENTAL_LOCATION) - A, 100
  Field  5: City Name (CITY_NAME) - A, 100
  Field  6: County Name (COUNTY_NAME) - A, 100
  Field  7: FIPS Code (FIPS_CODE) - A, 5 - 5-digit county FIPS
  Field  8: State Code (STATE_CODE) - A, 2
  Field  9: State Name (STATE_NAME) - A, 30
  Field 10: Country Name (COUNTRY_NAME) - A, 100
  Field 11: Postal Code (POSTAL_CODE) - A, 14
  Field 12: Federal Facility Code (FEDERAL_FACILITY_CODE) - A, 1 (Y/N)
  Field 13: Federal Agency Name (FEDERAL_AGENCY_NAME) - A, 100
  Field 14: Tribal Land Code (TRIBAL_LAND_CODE) - A, 1 (Y/N)
  Field 15: Tribal Land Name (TRIBAL_LAND_NAME) - A, 250
  Field 16: Congressional District Number (CONGRESSIONAL_DIST_NUM) - A, 4
  Field 17: Census Tract Code (CENSUS_TRACT_CODE) - A, 6
  Field 18: HUC Code (HUC_CODE) - A, 16
  Field 19: EPA Region Code (EPA_REGION_CODE) - A, 2
  Field 20: Site Type Name (SITE_TYPE_NAME) - A, 250
  Field 21: Location Description (LOCATION_DESCRIPTION) - A, 200
  Field 22: Create Date (CREATE_DATE) - D, 10
  Field 23: Update Date (UPDATE_DATE) - D, 10
  Field 24: US-Mexico Border Indicator (US_MEXICO_BORDER_IND) - A, 1 (Y/N)
  Field 25: PGM_SYS_ACRNMS (PGM_SYS_ACRNMS) - A, 2000 - pipe-delimited programs
  Field 26: Latitude (LATITUDE83) - N, 9,6
  Field 27: Longitude (LONGITUDE83) - N, 10,6
  Field 28: Collect Method Desc (COLLECT_DESC) - A, 60
  Field 29: Accuracy Value (ACCURACY_VALUE) - N, 8,2
  Field 30: Ref Point Desc (REF_POINT_DESC) - A, 50
  Field 31: HDATUM_DESC (HDATUM_DESC) - A, 60
  Field 32: Source Desc (SOURCE_DESC) - A, 60
  ...
  Field 39: Census Block Code (CENSUS_BLOCK_CODE) - A, 15 - 15-digit block GEOID!

Note: Field numbering in the schema documentation starts at 1 and the actual
CSV column headers use the names shown in parentheses above (e.g., REGISTRY_ID,
not "FRS Facility Identifier").
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("adapters.epa_frs.parse")

# Column specification matching the EPA FRS Single Facility File schema.
# Keys are the actual CSV header names; values are our standardized names.
FRS_COLUMNS = {
    "REGISTRY_ID": "registry_id",
    "PRIMARY_NAME": "primary_name",
    "LOCATION_ADDRESS": "location_address",
    "SUPPLEMENTAL_LOCATION": "supplemental_location",
    "CITY_NAME": "city_name",
    "COUNTY_NAME": "county_name",
    "FIPS_CODE": "fips_code",
    "STATE_CODE": "state_code",
    "STATE_NAME": "state_name",
    "COUNTRY_NAME": "country_name",
    "POSTAL_CODE": "postal_code",
    "FEDERAL_FACILITY_CODE": "federal_facility_flag",
    "FEDERAL_AGENCY_NAME": "federal_agency_name",
    "TRIBAL_LAND_CODE": "tribal_land_flag",
    "TRIBAL_LAND_NAME": "tribal_land_name",
    "CONGRESSIONAL_DIST_NUM": "congressional_dist",
    "CENSUS_TRACT_CODE": "census_tract_code",
    "HUC_CODE": "huc_code",
    "EPA_REGION_CODE": "epa_region_code",
    "SITE_TYPE_NAME": "site_type_name",
    "LOCATION_DESCRIPTION": "location_description",
    "CREATE_DATE": "create_date",
    "UPDATE_DATE": "update_date",
    "US_MEXICO_BORDER_IND": "us_mexico_border_flag",
    "PGM_SYS_ACRNMS": "pgm_sys_acrnms",
    "LATITUDE83": "latitude",
    "LONGITUDE83": "longitude",
    "COLLECT_DESC": "collect_method_desc",
    "ACCURACY_VALUE": "accuracy_value",
    "REF_POINT_DESC": "ref_point_desc",
    "HDATUM_DESC": "hdatum_desc",
    "SOURCE_DESC": "source_desc",
}

# Columns that must be read as strings to preserve leading zeros
STRING_COLUMNS = {
    "REGISTRY_ID": str,
    "FIPS_CODE": str,
    "STATE_CODE": str,
    "POSTAL_CODE": str,
    "CONGRESSIONAL_DIST_NUM": str,
    "CENSUS_TRACT_CODE": str,
    "HUC_CODE": str,
    "EPA_REGION_CODE": str,
    "CENSUS_BLOCK_CODE": str,
}

# Columns we want to keep for the pipeline (drop unnecessary verbose fields)
KEEP_COLUMNS = [
    "REGISTRY_ID",
    "PRIMARY_NAME",
    "CITY_NAME",
    "COUNTY_NAME",
    "FIPS_CODE",
    "STATE_CODE",
    "STATE_NAME",
    "POSTAL_CODE",
    "FEDERAL_FACILITY_CODE",
    "TRIBAL_LAND_CODE",
    "CENSUS_TRACT_CODE",
    "EPA_REGION_CODE",
    "SITE_TYPE_NAME",
    "PGM_SYS_ACRNMS",
    "LATITUDE83",
    "LONGITUDE83",
    "ACCURACY_VALUE",
    "CENSUS_BLOCK_CODE",
]


def parse_frs_single_file(
    csv_path: Path,
    columns: Optional[list] = None,
) -> pd.DataFrame:
    """Parse the FRS national single facility CSV file.

    Args:
        csv_path: Path to the extracted CSV file
        columns: Optional list of columns to keep (defaults to KEEP_COLUMNS)

    Returns:
        DataFrame with parsed FRS facility records
    """
    csv_path = Path(csv_path)
    logger.info("Parsing FRS single file: %s", csv_path)

    # Read with string dtypes for GEOID-like columns
    df = pd.read_csv(
        csv_path,
        dtype=STRING_COLUMNS,
        low_memory=False,
        encoding="latin-1",
    )
    logger.info("Read %d raw FRS records", len(df))

    # Normalize column names to uppercase for consistent matching
    df.columns = df.columns.str.strip().str.upper()

    # Filter to US records only (exclude territories outside US if needed)
    if "COUNTRY_NAME" in df.columns:
        us_mask = df["COUNTRY_NAME"].fillna("").str.upper().str.contains("US|UNITED STATES")
        n_non_us = (~us_mask).sum()
        if n_non_us > 0:
            logger.info("Filtering %d non-US records", n_non_us)
            df = df[us_mask].copy()

    # Select columns
    keep = columns or KEEP_COLUMNS
    available = [c for c in keep if c in df.columns]
    missing = [c for c in keep if c not in df.columns]
    if missing:
        logger.warning("Missing columns in FRS CSV: %s", missing)
    df = df[available].copy()

    # Rename to standardized names
    rename_map = {k: v for k, v in FRS_COLUMNS.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Also rename CENSUS_BLOCK_CODE if present
    if "CENSUS_BLOCK_CODE" in df.columns:
        df = df.rename(columns={"CENSUS_BLOCK_CODE": "census_block_code"})

    logger.info("Parsed %d FRS records with %d columns", len(df), len(df.columns))
    return df
