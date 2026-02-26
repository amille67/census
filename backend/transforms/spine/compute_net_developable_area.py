"""Compute net developable area for spine blocks."""

import numpy as np
import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("transforms.spine.compute_net_developable")


def compute_net_developable_area(spine_df: pd.DataFrame) -> pd.DataFrame:
    """Compute net developable area = gross_land - water - protected (floored at 0).

    Expects columns: gross_land_area_sq_m, water_area_sq_m, protected_area_sq_m
    """
    spine = spine_df.copy()

    for col in ["water_area_sq_m", "protected_area_sq_m"]:
        if col not in spine.columns:
            spine[col] = 0.0

    spine["net_developable_area_sq_m"] = np.maximum(
        spine["gross_land_area_sq_m"]
        - spine["water_area_sq_m"]
        - spine["protected_area_sq_m"],
        0.0,
    )

    zero_pct = (spine["net_developable_area_sq_m"] == 0).mean() * 100
    logger.info(
        "Net developable area: %.1f%% of blocks have zero area",
        zero_pct,
    )

    return spine
