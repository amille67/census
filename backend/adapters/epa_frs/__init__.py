"""EPA Facility Registry Service (FRS) adapter.

Downloads, parses, and normalizes the FRS national single facility file
for ingest into the Census block group spine.
"""

from backend.adapters.epa_frs.download import download_frs_national
from backend.adapters.epa_frs.parse import parse_frs_single_file, FRS_COLUMNS
from backend.adapters.epa_frs.normalize import normalize_frs

__all__ = [
    "download_frs_national",
    "parse_frs_single_file",
    "normalize_frs",
    "FRS_COLUMNS",
]
