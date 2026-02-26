"""Geographic constants used throughout the ETL pipeline."""

# Coordinate Reference Systems
CANONICAL_CRS = "EPSG:4269"   # NAD83 - Census Bureau standard
AREA_CRS = "EPSG:5070"        # NAD83 / Conus Albers Equal Area
WEB_CRS = "EPSG:3857"         # Web Mercator

# GEOID lengths by grain
BLOCK_GEOID_LEN = 15
BG_GEOID_LEN = 12
TRACT_GEOID_LEN = 11
COUNTY_GEOID_LEN = 5
STATE_FIPS_LEN = 2

# FIPS code to state abbreviation mapping (from census-parquet)
FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR",
}

ABBR_TO_FIPS = {v: k for k, v in FIPS_TO_ABBR.items()}

# States list for LODES downloads (lowercase abbreviations)
LODES_STATES = [abbr.lower() for abbr in FIPS_TO_ABBR.values()]

# TIGER 2020 download URLs
TIGER_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2020"
TIGER_TABBLOCK20_URL = f"{TIGER_BASE_URL}/TABBLOCK20/"
TIGER_BG_URL = f"{TIGER_BASE_URL}/BG/"
TIGER_AREAWATER_URL = f"{TIGER_BASE_URL}/AREAWATER/"
TIGER_CBSA_URL = f"{TIGER_BASE_URL}/CBSA/"
TIGER_STATE_URL = f"{TIGER_BASE_URL}/STATE/"

CARTOGRAPHIC_BASE_URL = "https://www2.census.gov/geo/tiger/GENZ2020/shp"

# PL 94-171 redistricting data URL
PL_BASE_URL = (
    "https://www2.census.gov/programs-surveys/decennial/2020/data/"
    "01-Redistricting_File--PL_94-171"
)
