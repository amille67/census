"""Metadata models for ingest manifest tracking."""

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class IngestMetadata:
    """Metadata manifest written alongside every ingest output."""

    source_slug: str
    source_version: str
    scenario: str
    geography_vintage: int
    input_row_count: int
    output_row_count: int
    match_rate: float
    coverage_rate: float
    null_rates: dict
    output_file: str
    spine_version: str
    qa_passed: bool
    ingest_timestamp: str = ""

    def __post_init__(self):
        if not self.ingest_timestamp:
            self.ingest_timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    def write(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def from_file(cls, path: Path) -> "IngestMetadata":
        with open(path) as f:
            return cls(**json.load(f))
