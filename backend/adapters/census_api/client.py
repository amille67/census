"""Census API client wrapper.

Constructs Census(api_key, year=...) using the npappin-wsu/census package.
"""

from typing import Optional

from backend.utils.env import get_census_api_key
from backend.utils.logging import get_logger

logger = get_logger("adapters.census_api.client")


def get_census_client(api_key: Optional[str] = None, year: int = 2023):
    """Create a Census API client.

    Uses the census package (npappin-wsu/census lineage) which provides:
    - ACS 5yr, 3yr, 1yr access
    - SF1 and PL dataset access
    - Built-in field chunking for >49 fields
    - Retry on transient Census API errors
    """
    from census import Census

    if api_key is None:
        api_key = get_census_api_key()

    if not api_key:
        raise ValueError(
            "Census API key required. Set CENSUS_API_KEY environment variable "
            "or get a key at https://api.census.gov/data/key_signup.html"
        )

    client = Census(api_key, year=year)
    logger.info("Initialized Census API client for year %d", year)
    return client
