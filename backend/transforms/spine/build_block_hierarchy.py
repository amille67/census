"""Build block-level hierarchy for the master spine.

Takes normalized TIGER block data and derives all hierarchy keys
using zero-cost string slicing.
"""

import pandas as pd

from backend.geo.geoid import add_hierarchy_columns
from backend.geo.constants import BLOCK_GEOID_LEN
from backend.utils.logging import get_logger

logger = get_logger("transforms.spine.build_block_hierarchy")


def build_block_hierarchy(blocks_df: pd.DataFrame) -> pd.DataFrame:
    """Build the full geographic hierarchy from block_geoid.

    Input: DataFrame with at minimum a 'block_geoid' column
    Output: DataFrame with block_geoid + all derived hierarchy columns
    """
    blocks = blocks_df.copy()

    # Ensure block_geoid is properly formatted
    blocks["block_geoid"] = blocks["block_geoid"].astype(str).str.zfill(BLOCK_GEOID_LEN)

    # Derive all hierarchy columns via string slicing
    blocks = add_hierarchy_columns(blocks, "block_geoid")

    logger.info(
        "Built hierarchy: %d blocks, %d block groups, %d tracts, %d counties, %d states",
        blocks["block_geoid"].nunique(),
        blocks["bg_geoid"].nunique(),
        blocks["tract_geoid"].nunique(),
        blocks["county_geoid"].nunique(),
        blocks["state_fips"].nunique(),
    )

    return blocks
