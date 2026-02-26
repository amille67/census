"""Density feature computation."""

import numpy as np
import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.features.density")


def add_density_features(
    df: pd.DataFrame,
    area_col: str = "net_developable_area_sq_m",
    numerator_columns: dict = None,
) -> pd.DataFrame:
    """Add density features for multiple numerators.

    Args:
        df: DataFrame with area column and numerator columns
        area_col: Name of area denominator column
        numerator_columns: Dict of {numerator_col: output_col_name}
    """
    if numerator_columns is None:
        numerator_columns = {}

    df = df.copy()
    area = df[area_col].replace(0, np.nan)

    for num_col, out_col in numerator_columns.items():
        if num_col in df.columns:
            df[out_col] = (df[num_col] / area) * 1_000_000  # per sq km

    logger.info("Added %d density features", len(numerator_columns))
    return df
