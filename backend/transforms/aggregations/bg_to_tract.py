"""Block Group to Tract rollup."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.aggregations.bg_to_tract")


def rollup_bg_to_tract(
    bg_df: pd.DataFrame,
    sum_columns: list = None,
    mean_columns: list = None,
) -> pd.DataFrame:
    """Roll up block group data to tract level."""
    agg_dict = {}
    if sum_columns:
        agg_dict.update({c: "sum" for c in sum_columns if c in bg_df.columns})
    if mean_columns:
        agg_dict.update({c: "mean" for c in mean_columns if c in bg_df.columns})

    if not agg_dict:
        numeric = bg_df.select_dtypes(include=["number"]).columns
        agg_dict = {c: "sum" for c in numeric}

    result = bg_df.groupby("tract_geoid").agg(agg_dict).reset_index()
    logger.info("Rolled up %d BGs to %d tracts", len(bg_df), len(result))
    return result
