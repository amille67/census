"""Block to Block Group aggregation."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.aggregations.block_to_bg")


def aggregate_block_to_bg(
    block_df: pd.DataFrame,
    value_columns: list,
    agg_func: str = "sum",
) -> pd.DataFrame:
    """Aggregate block-level data to block group level.

    Args:
        block_df: DataFrame with bg_geoid and value columns
        value_columns: Columns to aggregate
        agg_func: Aggregation function ('sum', 'mean', 'count', etc.)
    """
    available = [c for c in value_columns if c in block_df.columns]
    agg_dict = {col: agg_func for col in available}

    result = block_df.groupby("bg_geoid").agg(agg_dict).reset_index()
    logger.info("Aggregated %d blocks to %d block groups", len(block_df), len(result))
    return result
