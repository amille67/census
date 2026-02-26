"""Weighted density computation using net developable area."""

import numpy as np
import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.aggregations.weighted_density")


def compute_density(
    df: pd.DataFrame,
    numerator_col: str,
    denominator_col: str = "net_developable_area_sq_m",
    output_col: str = None,
    scale_factor: float = 1_000_000,  # per sq km
) -> pd.DataFrame:
    """Compute density metric as numerator / denominator * scale_factor.

    Handles zero-denominator cases by setting density to NaN.
    """
    df = df.copy()
    if output_col is None:
        output_col = f"{numerator_col}_density"

    denominator = df[denominator_col].replace(0, np.nan)
    df[output_col] = (df[numerator_col] / denominator) * scale_factor

    null_pct = df[output_col].isna().mean() * 100
    logger.info("Computed %s: %.1f%% null (zero-area blocks)", output_col, null_pct)
    return df
