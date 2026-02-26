"""Data quality expectations framework."""

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("qa.expectations")


@dataclass
class Expectation:
    name: str
    passed: bool
    actual: float
    threshold: float
    message: str = ""


def expect_row_count_between(df: pd.DataFrame, min_rows: int, max_rows: int) -> Expectation:
    count = len(df)
    passed = min_rows <= count <= max_rows
    return Expectation(
        name="row_count_range",
        passed=passed,
        actual=count,
        threshold=min_rows,
        message=f"Row count {count} {'in' if passed else 'outside'} range [{min_rows}, {max_rows}]",
    )


def expect_column_not_null(df: pd.DataFrame, column: str, max_null_rate: float = 0.0) -> Expectation:
    null_rate = df[column].isna().mean()
    passed = null_rate <= max_null_rate
    return Expectation(
        name=f"{column}_null_rate",
        passed=passed,
        actual=null_rate,
        threshold=max_null_rate,
        message=f"{column} null rate: {null_rate:.2%} (max: {max_null_rate:.2%})",
    )


def expect_unique(df: pd.DataFrame, column: str) -> Expectation:
    is_unique = df[column].is_unique
    n_dupes = df[column].duplicated().sum()
    return Expectation(
        name=f"{column}_unique",
        passed=is_unique,
        actual=n_dupes,
        threshold=0,
        message=f"{column} uniqueness: {'unique' if is_unique else f'{n_dupes} duplicates'}",
    )
