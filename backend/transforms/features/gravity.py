"""Gravity model utilities."""

import numpy as np
import pandas as pd


def simple_gravity_score(
    mass_col: pd.Series,
    distance_col: pd.Series,
    beta: float = 2.0,
) -> pd.Series:
    """Compute a simple gravity-model score: mass / distance^beta."""
    return mass_col / np.power(distance_col.clip(lower=1e-6), beta)
