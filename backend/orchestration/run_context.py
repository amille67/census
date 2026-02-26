"""Run context for pipeline execution."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class RunContext:
    """Tracks state and metadata for a pipeline run."""

    run_id: str = ""
    date_tag: str = ""
    data_root: Path = None
    start_time: datetime = None
    completed_steps: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def __post_init__(self):
        if not self.run_id:
            self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        if not self.date_tag:
            self.date_tag = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.start_time is None:
            self.start_time = datetime.now(timezone.utc)

    def mark_complete(self, step: str):
        self.completed_steps.append(step)

    def mark_error(self, step: str, error: str):
        self.errors.append({"step": step, "error": error})

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0
