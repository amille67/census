"""Ingest result model for standardized ingest outputs."""

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


@dataclass
class IngestResult:
    """Standardized result from any ingest pipeline."""

    source_slug: str
    scenario: str
    data: pd.DataFrame
    input_row_count: int
    match_rate: float
    coverage_rate: float
    null_rates: dict = field(default_factory=dict)
    source_vintage: str = ""
    geography_vintage: int = 2020
    warnings: list = field(default_factory=list)

    @property
    def output_row_count(self) -> int:
        return len(self.data)

    @property
    def qa_passed(self) -> bool:
        return self.match_rate >= 0.80 and self.coverage_rate >= 0.30

    def validate_grain(self, grain_col: str = "bg_geoid") -> bool:
        """Check that the output is at the expected grain."""
        return self.data[grain_col].is_unique
