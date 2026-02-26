"""Schema inference for point source CSVs."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("adapters.points.schema_inference")

COMMON_LAT_NAMES = ["latitude", "lat", "y", "latitude83", "lat_dd", "centlat"]
COMMON_LON_NAMES = ["longitude", "lon", "long", "x", "longitude83", "lon_dd", "centlon"]


def infer_lat_lon_columns(df: pd.DataFrame) -> dict:
    """Attempt to identify lat/lon columns by name convention."""
    columns_lower = {c.lower(): c for c in df.columns}

    lat_col = None
    lon_col = None

    for name in COMMON_LAT_NAMES:
        if name in columns_lower:
            lat_col = columns_lower[name]
            break

    for name in COMMON_LON_NAMES:
        if name in columns_lower:
            lon_col = columns_lower[name]
            break

    result = {"lat_column": lat_col, "lon_column": lon_col}
    if lat_col and lon_col:
        logger.info("Inferred lat=%s, lon=%s", lat_col, lon_col)
    else:
        logger.warning("Could not infer lat/lon columns from: %s", list(df.columns))

    return result
