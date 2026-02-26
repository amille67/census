"""LODES-derived economic metrics.

Computes Simpson diversity, Hachman index, manufacturing share,
and Baumol risk from NAICS sector employment data.
"""

import numpy as np
import pandas as pd

from backend.models.lodes import WAC_NAICS_COLUMNS
from backend.utils.logging import get_logger

logger = get_logger("transforms.features.lodes_metrics")


def simpson_diversity_index(df: pd.DataFrame, total_col: str = "C000") -> pd.Series:
    """Compute Simpson Diversity Index across NAICS sectors.

    SDI = 1 - sum(p_i^2) where p_i is sector share of total jobs.
    Range: 0 (monopoly) to ~0.95 (highly diverse).
    """
    available_naics = [c for c in WAC_NAICS_COLUMNS if c in df.columns]
    total = df[total_col].replace(0, np.nan)
    shares = df[available_naics].div(total, axis=0)
    sdi = 1.0 - (shares ** 2).sum(axis=1)
    return sdi


def manufacturing_share(df: pd.DataFrame, total_col: str = "C000") -> pd.Series:
    """Compute manufacturing share of total employment."""
    total = df[total_col].replace(0, np.nan)
    return df.get("CNS05", pd.Series(0, index=df.index)) / total


def baumol_risk(df: pd.DataFrame, total_col: str = "C000") -> pd.Series:
    """Compute Baumol risk: share of employment in low-productivity-growth sectors.

    Sectors: Accommodation/Food (CNS18), Other Services (CNS19),
    Public Administration (CNS20).
    """
    low_prod = sum(df.get(c, 0) for c in ["CNS18", "CNS19", "CNS20"])
    total = df[total_col].replace(0, np.nan)
    return low_prod / total


def add_lodes_metrics(df: pd.DataFrame, total_col: str = "C000") -> pd.DataFrame:
    """Add all LODES-derived metrics to a DataFrame."""
    df = df.copy()
    df["simpson_diversity_index"] = simpson_diversity_index(df, total_col)
    df["manufacturing_share"] = manufacturing_share(df, total_col)
    df["baumol_risk"] = baumol_risk(df, total_col)
    logger.info("Added LODES economic metrics to %d rows", len(df))
    return df
