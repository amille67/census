"""Raster file reading utilities."""

from pathlib import Path

from backend.utils.logging import get_logger

logger = get_logger("adapters.raster.reader")


def read_raster_metadata(path: Path) -> dict:
    """Read raster metadata without loading the full array."""
    import rasterio

    with rasterio.open(path) as src:
        return {
            "crs": str(src.crs),
            "bounds": dict(zip(["left", "bottom", "right", "top"], src.bounds)),
            "shape": src.shape,
            "count": src.count,
            "dtype": str(src.dtypes[0]),
            "nodata": src.nodata,
            "transform": list(src.transform),
        }
