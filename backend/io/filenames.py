"""Filename conventions for the ETL pipeline."""

from datetime import date
from pathlib import Path


def get_date_tag() -> str:
    """Get today's date as a tag string (YYYY-MM-DD)."""
    return date.today().isoformat()


def ingest_output_path(data_root: Path, date_tag: str, source_slug: str) -> Path:
    """Build the standard output path for an ingest file."""
    return data_root / "processed" / "ingests" / date_tag / f"{source_slug}_blockgroup.parquet"


def metadata_output_path(data_root: Path, date_tag: str, source_slug: str) -> Path:
    """Build the standard metadata path for an ingest file."""
    return data_root / "processed" / "ingests" / date_tag / f"{source_slug}_metadata.json"


def assembled_output_path(data_root: Path, date_tag: str) -> Path:
    """Build the standard path for the assembled master blockgroup file."""
    return data_root / "processed" / "assembled" / f"master_blockgroup_{date_tag}.parquet"
