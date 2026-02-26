"""PL 94-171 Redistricting data fetching (optional API-based alternative)."""

from typing import Optional

import pandas as pd

from backend.geo.constants import FIPS_TO_ABBR
from backend.utils.logging import get_logger

logger = get_logger("adapters.census_api.pl_fetch")


def fetch_pl_blockgroup(
    client,
    variables: list = None,
    states: list = None,
) -> pd.DataFrame:
    """Fetch PL 94-171 data at block group grain via Census API."""
    if variables is None:
        variables = ["P1_001N", "P1_003N", "P1_004N"]  # Total, White alone, Black alone
    if states is None:
        states = sorted(FIPS_TO_ABBR.keys())

    all_rows = []
    for state_fips in states:
        try:
            rows = client.pl.get(
                variables,
                geo={
                    "for": "block group:*",
                    "in": f"state:{state_fips} county:*",
                },
            )
            all_rows.extend(rows)
        except Exception as e:
            logger.error("Failed PL fetch for state %s: %s", state_fips, e)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
