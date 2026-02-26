"""ACS 5-Year data fetching at block group grain.

Uses the Census client wrapper to pull ACS data state-by-state
at the block group level.
"""

from typing import Optional

import pandas as pd

from backend.geo.constants import FIPS_TO_ABBR
from backend.models.census_acs import ACS5_BUILD1_VARS
from backend.utils.logging import get_logger

logger = get_logger("adapters.census_api.acs_fetch")


def fetch_acs5_blockgroup(
    client,
    variables: list = None,
    states: list = None,
    year: int = 2023,
) -> pd.DataFrame:
    """Fetch ACS 5-Year data at block group grain for specified states.

    Uses client.acs5.state_county_blockgroup() which leverages the
    census package's built-in chunking for >49 fields.

    Args:
        client: Census client instance
        variables: List of ACS variable codes (default: Build 1 vars)
        states: List of 2-digit FIPS codes (default: all states)
        year: ACS year

    Returns:
        DataFrame with ACS data at block group grain
    """
    if variables is None:
        variables = ACS5_BUILD1_VARS
    if states is None:
        states = sorted(FIPS_TO_ABBR.keys())

    all_rows = []

    for state_fips in states:
        logger.info("Fetching ACS5 BG data for state %s (year=%d)", state_fips, year)
        try:
            rows = client.acs5.get(
                variables,
                geo={
                    "for": "block group:*",
                    "in": f"state:{state_fips} county:*",
                },
                year=year,
            )
            all_rows.extend(rows)
            logger.info("  State %s: %d block groups", state_fips, len(rows))
        except Exception as e:
            logger.error("  Failed for state %s: %s", state_fips, e)

    if not all_rows:
        logger.warning("No ACS data fetched")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    logger.info("Total ACS rows fetched: %d", len(df))
    return df
