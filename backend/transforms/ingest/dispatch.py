"""Ingest scenario dispatch.

Routes source data through the correct ingest path based on scenario type:
  A) point -> block -> blockgroup
  B) native_block -> blockgroup
  C) raster_polygon -> blockgroup
  D) native_bg -> blockgroup
"""

from backend.utils.logging import get_logger
from backend.utils.exceptions import SourceConfigError

logger = get_logger("transforms.ingest.dispatch")

VALID_SCENARIOS = {"point", "native_block", "native_bg", "raster_polygon"}


def dispatch_scenario(scenario: str):
    """Return the appropriate ingest transform function for a scenario.

    Args:
        scenario: One of 'point', 'native_block', 'native_bg', 'raster_polygon'

    Returns:
        The ingest function for the given scenario
    """
    if scenario not in VALID_SCENARIOS:
        raise SourceConfigError(
            f"Unknown scenario '{scenario}'. Must be one of: {VALID_SCENARIOS}"
        )

    if scenario == "point":
        from backend.transforms.ingest.point_to_blockgroup import ingest_point_source
        return ingest_point_source
    elif scenario == "native_block":
        from backend.transforms.ingest.native_block_to_blockgroup import ingest_native_block
        return ingest_native_block
    elif scenario == "native_bg":
        from backend.transforms.ingest.native_bg_to_blockgroup import ingest_native_bg
        return ingest_native_bg
    elif scenario == "raster_polygon":
        from backend.transforms.ingest.raster_polygon_to_blockgroup import ingest_raster_polygon
        return ingest_raster_polygon
