"""LODES WAC normalization.

Reads raw WAC CSVs, normalizes block_geoid from w_geocode,
and produces a standardized DataFrame.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from backend.geo.constants import BLOCK_GEOID_LEN
from backend.geo.geoid import add_hierarchy_columns
from backend.models.lodes import WAC_ALL_NUMERIC_COLUMNS
from backend.adapters.lodes.parse_filenames import parse_lodes_filename
from backend.utils.logging import get_logger

logger = get_logger("adapters.lodes.normalize_wac")


def normalize_wac_file(csv_path: Path) -> Optional[pd.DataFrame]:
    """Read and normalize a single WAC CSV file.

    Normalizes w_geocode to block_geoid (15-digit zero-padded string).
    """
    file_info = parse_lodes_filename(csv_path.name)
    if file_info is None:
        logger.warning("Could not parse filename: %s", csv_path.name)
        return None

    df = pd.read_csv(csv_path, dtype={"w_geocode": str})

    df["block_geoid"] = df["w_geocode"].str.zfill(BLOCK_GEOID_LEN)
    df["year"] = file_info.year
    df["segment"] = file_info.segment
    df["job_type"] = file_info.job_type
    df["state"] = file_info.state

    # Keep only known numeric columns that exist in this file
    available_numeric = [c for c in WAC_ALL_NUMERIC_COLUMNS if c in df.columns]
    keep_cols = ["block_geoid", "year", "segment", "job_type", "state"] + available_numeric

    df = df[keep_cols]
    logger.info("Normalized %s: %d rows", csv_path.name, len(df))
    return df


def normalize_wac_directory(csv_dir: Path, segment: str = "S000", job_type: str = "JT00") -> pd.DataFrame:
    """Normalize all WAC CSVs in a directory, filtering to a specific segment/job_type.

    Args:
        csv_dir: Directory containing WAC CSV files
        segment: LODES segment filter (default S000 = all workers)
        job_type: LODES job type filter (default JT00 = all jobs)

    Returns:
        Combined normalized DataFrame
    """
    csv_files = sorted(csv_dir.glob("*_wac_*.csv"))
    logger.info("Found %d WAC CSV files in %s", len(csv_files), csv_dir)

    frames = []
    for csv_path in csv_files:
        file_info = parse_lodes_filename(csv_path.name)
        if file_info and file_info.segment == segment and file_info.job_type == job_type:
            df = normalize_wac_file(csv_path)
            if df is not None:
                frames.append(df)

    if not frames:
        logger.warning("No WAC files matched segment=%s, job_type=%s", segment, job_type)
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Combined WAC: %d rows from %d files", len(combined), len(frames))
    return combined
