"""GeoParquet I/O utilities."""

from pathlib import Path
from typing import Optional

import geopandas as gpd

from backend.utils.logging import get_logger

logger = get_logger("io.geoparquet")


def write_geoparquet(gdf: gpd.GeoDataFrame, path: Path, index: bool = False):
    """Write a GeoDataFrame to GeoParquet."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(path, index=index)
    logger.info("Wrote %d rows to %s", len(gdf), path)


def read_geoparquet(path: Path, columns: Optional[list] = None) -> gpd.GeoDataFrame:
    """Read a GeoParquet file into a GeoDataFrame."""
    path = Path(path)
    gdf = gpd.read_parquet(path, columns=columns)
    logger.info("Read %d rows from %s", len(gdf), path)
    return gdf
