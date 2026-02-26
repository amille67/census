"""Block Group to MSA rollup."""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.aggregations.bg_to_msa")


def rollup_bg_to_msa(
    bg_df: pd.DataFrame,
    sum_columns: list = None,
    mean_columns: list = None,
) -> pd.DataFrame:
    """Roll up block group data to MSA level."""
    df = bg_df[bg_df["msa_geoid"].notna()].copy()

    agg_dict = {}
    if sum_columns:
        agg_dict.update({c: "sum" for c in sum_columns if c in df.columns})
    if mean_columns:
        agg_dict.update({c: "mean" for c in mean_columns if c in df.columns})

    if not agg_dict:
        numeric = df.select_dtypes(include=["number"]).columns
        agg_dict = {c: "sum" for c in numeric}

    result = df.groupby("msa_geoid").agg(agg_dict).reset_index()
    logger.info("Rolled up %d BGs to %d MSAs", len(df), len(result))
    return result
