"""Download Census boundary files.

Reimplements census-parquet's download_boundaries.sh in Python for
portability, parameterized paths, and better logging.
"""

import zipfile
from pathlib import Path

from backend.adapters.http.downloader import download_file
from backend.geo.constants import CARTOGRAPHIC_BASE_URL
from backend.utils.logging import get_logger
from backend.utils.env import get_data_root

logger = get_logger("adapters.bootstrap.download_boundaries")

BOUNDARY_FILES = [
    "cb_2020_us_all_500k.zip",
    "cb_2020_us_nation_5m.zip",
]


def download_boundaries(output_dir: Path = None) -> list:
    """Download cartographic boundary shapefiles from Census Bureau.

    Adapted from census-parquet download_boundaries.sh.
    """
    if output_dir is None:
        output_dir = get_data_root() / "staging" / "census_parquet_bootstrap" / "boundary_outputs"

    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []

    for filename in BOUNDARY_FILES:
        url = f"{CARTOGRAPHIC_BASE_URL}/{filename}"
        zip_path = output_dir / filename

        download_file(url, zip_path)

        logger.info("Extracting %s", zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(output_dir)

        downloaded.append(zip_path)

    logger.info("Downloaded %d boundary files to %s", len(downloaded), output_dir)
    return downloaded


if __name__ == "__main__":
    download_boundaries()
