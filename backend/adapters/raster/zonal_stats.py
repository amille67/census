"""Zonal statistics computation for raster data against polygon boundaries."""

from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("adapters.raster.zonal_stats")


def compute_zonal_stats(
    raster_path: Path,
    zones_gdf: gpd.GeoDataFrame,
    zone_id_col: str,
    stats: list = None,
    band: int = 1,
    nodata: Optional[float] = None,
    categorical: bool = False,
) -> pd.DataFrame:
    """Compute zonal statistics of a raster within polygon zones.

    Args:
        raster_path: Path to raster file (GeoTIFF)
        zones_gdf: GeoDataFrame with zone polygons
        zone_id_col: Column identifying each zone
        stats: Statistics to compute (e.g., ['mean', 'sum', 'count'])
        band: Raster band number
        nodata: Nodata value override
        categorical: Whether to compute categorical stats

    Returns:
        DataFrame with zone_id and computed statistics
    """
    from rasterstats import zonal_stats as _zonal_stats

    if stats is None:
        stats = ["mean", "sum", "count", "min", "max"]

    logger.info("Computing zonal stats for %d zones", len(zones_gdf))

    results = _zonal_stats(
        zones_gdf.geometry,
        str(raster_path),
        stats=stats,
        band=band,
        nodata=nodata,
        categorical=categorical,
    )

    df = pd.DataFrame(results)
    df[zone_id_col] = zones_gdf[zone_id_col].values

    logger.info("Computed zonal stats: %d zones, %d columns", len(df), len(df.columns))
    return df
