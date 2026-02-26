"""Point source data model."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PointSourceConfig:
    """Configuration for a point-based data source."""

    lat_column: str
    lon_column: str
    id_column: Optional[str] = None
    crs: str = "EPSG:4326"
    aggregation_rules: dict = None

    def __post_init__(self):
        if self.aggregation_rules is None:
            self.aggregation_rules = {"_count": "count"}
