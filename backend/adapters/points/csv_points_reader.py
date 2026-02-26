"""CSV point source reader.

Reads CSV files with lat/lon columns and creates GeoDataFrames.
"""

from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from backend.utils.logging import get_logger

logger = get_logger("adapters.points.csv_reader")


def read_csv_points(
    path: Path,
    lat_column: str,
    lon_column: str,
    crs: str = "EPSG:4326",
    id_column: Optional[str] = None,
) -> gpd.GeoDataFrame:
    """Read a CSV file with lat/lon columns into a GeoDataFrame.

    Args:
        path: Path to CSV file
        lat_column: Name of latitude column
        lon_column: Name of longitude column
        crs: Coordinate reference system (default WGS84)
        id_column: Optional row identifier column

    Returns:
        GeoDataFrame with point geometries
    """
    df = pd.read_csv(path)
    logger.info("Read %d rows from %s", len(df), path)

    # Drop rows with missing coordinates
    valid_mask = df[lat_column].notna() & df[lon_column].notna()
    n_dropped = (~valid_mask).sum()
    if n_dropped > 0:
        logger.warning("Dropped %d rows with missing coordinates", n_dropped)
        df = df[valid_mask].copy()

    # Create geometry
    geometry = gpd.points_from_xy(df[lon_column], df[lat_column])
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)

    logger.info("Created GeoDataFrame with %d points", len(gdf))
    return gdf
