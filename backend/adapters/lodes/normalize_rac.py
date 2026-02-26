"""LODES RAC (Residence Area Characteristics) normalization.

Similar to WAC but keyed on h_geocode (home block).
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from backend.geo.constants import BLOCK_GEOID_LEN
from backend.adapters.lodes.parse_filenames import parse_lodes_filename
from backend.utils.logging import get_logger

logger = get_logger("adapters.lodes.normalize_rac")


def normalize_rac_file(csv_path: Path) -> Optional[pd.DataFrame]:
    """Read and normalize a single RAC CSV file."""
    file_info = parse_lodes_filename(csv_path.name)
    if file_info is None:
        return None

    df = pd.read_csv(csv_path, dtype={"h_geocode": str})
    df["block_geoid"] = df["h_geocode"].str.zfill(BLOCK_GEOID_LEN)
    df["year"] = file_info.year
    df["segment"] = file_info.segment
    df["job_type"] = file_info.job_type

    logger.info("Normalized RAC %s: %d rows", csv_path.name, len(df))
    return df
