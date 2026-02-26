"""LODES filename parsing utilities.

Parses LODES filenames to extract state, data type, segment, job type, and year.
Example filename: al_wac_S000_JT00_2021.csv
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class LODESFileInfo:
    state: str
    data_type: str  # wac, rac, od
    segment: str    # S000, SA01, etc.
    job_type: str   # JT00, JT01, etc.
    year: int
    filename: str

    @property
    def is_wac(self) -> bool:
        return self.data_type == "wac"


_LODES_PATTERN = re.compile(
    r"^(?P<state>[a-z]{2})_"
    r"(?P<type>wac|rac|od)_"
    r"(?P<segment>[A-Z][A-Z0-9]{2,3})_"
    r"(?P<jobtype>JT\d{2})_"
    r"(?P<year>\d{4})"
    r"\.csv$"
)


def parse_lodes_filename(filename: str) -> Optional[LODESFileInfo]:
    """Parse a LODES CSV filename into its components."""
    name = filename.split("/")[-1] if "/" in filename else filename
    match = _LODES_PATTERN.match(name)
    if not match:
        return None

    return LODESFileInfo(
        state=match.group("state"),
        data_type=match.group("type"),
        segment=match.group("segment"),
        job_type=match.group("jobtype"),
        year=int(match.group("year")),
        filename=name,
    )
