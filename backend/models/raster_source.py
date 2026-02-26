"""Raster/polygon source data model."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class RasterSourceConfig:
    """Configuration for a raster or polygon-overlay data source."""

    band: int = 1
    nodata_value: Optional[float] = None
    stats: list = None
    categorical: bool = False

    def __post_init__(self):
        if self.stats is None:
            self.stats = ["mean", "sum", "count"]
