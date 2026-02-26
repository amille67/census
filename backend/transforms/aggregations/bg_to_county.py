"""Block Group to County rollup."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.aggregations.bg_to_county")


def rollup_bg_to_county(
    bg_df: pd.DataFrame,
    sum_columns: list = None,
    mean_columns: list = None,
) -> pd.DataFrame:
    """Roll up block group data to county level."""
    agg_dict = {}
    if sum_columns:
        agg_dict.update({c: "sum" for c in sum_columns if c in bg_df.columns})
    if mean_columns:
        agg_dict.update({c: "mean" for c in mean_columns if c in bg_df.columns})

    if not agg_dict:
        numeric = bg_df.select_dtypes(include=["number"]).columns
        agg_dict = {c: "sum" for c in numeric}

    result = bg_df.groupby("county_geoid").agg(agg_dict).reset_index()
    logger.info("Rolled up %d BGs to %d counties", len(bg_df), len(result))
    return result
