"""Diversity index computations."""

import numpy as np
import pandas as pd


def shannon_entropy(shares: pd.DataFrame) -> pd.Series:
    """Compute Shannon entropy across columns (sector shares)."""
    shares = shares.clip(lower=1e-10)
    return -(shares * np.log(shares)).sum(axis=1)


def herfindahl_hirschman_index(shares: pd.DataFrame) -> pd.Series:
    """Compute Herfindahl-Hirschman Index (HHI) across sector shares."""
    return (shares ** 2).sum(axis=1)
