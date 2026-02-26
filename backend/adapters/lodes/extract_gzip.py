"""LODES gzip extraction utilities."""

import gzip
from pathlib import Path

from backend.utils.logging import get_logger

logger = get_logger("adapters.lodes.extract")


def extract_gzip(gz_path: Path, output_dir: Path = None) -> Path:
    """Extract a gzip file to the same directory or a specified output dir."""
    gz_path = Path(gz_path)
    if output_dir is None:
        output_dir = gz_path.parent

    csv_name = gz_path.name.replace(".gz", "")
    csv_path = output_dir / csv_name

    with gzip.open(gz_path, "rb") as f_in:
        with open(csv_path, "wb") as f_out:
            f_out.write(f_in.read())

    logger.info("Extracted %s -> %s", gz_path.name, csv_path)
    return csv_path
