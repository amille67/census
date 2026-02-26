"""Vintage enforcement for ingest data.

Ensures all data entering the spine merge path uses 2020 Census geography.
Non-2020 sources must go through crosswalk apportionment first.
"""

from backend.utils.exceptions import VintageError
from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.vintage")

CANONICAL_VINTAGE = 2020


def enforce_vintage(geography_year: int, source_slug: str, requires_crosswalk: bool = False):
    """Validate that the source uses 2020 Census geography.

    Raises VintageError if non-2020 geography is detected without crosswalk.
    """
    if geography_year == CANONICAL_VINTAGE:
        logger.info("Source '%s' uses canonical %d geography", source_slug, CANONICAL_VINTAGE)
        return

    if requires_crosswalk:
        logger.info(
            "Source '%s' uses %d geography with crosswalk enabled",
            source_slug, geography_year,
        )
        return

    raise VintageError(
        f"Source '{source_slug}' uses {geography_year} geography but "
        f"requires_crosswalk is not set. All data must be mapped to "
        f"{CANONICAL_VINTAGE} Census geography before spine merge."
    )
