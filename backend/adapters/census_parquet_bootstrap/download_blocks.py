"""Download TIGER 2020 block shapefiles.

Reimplements census-parquet's download_blocks.sh in Python.
"""

from pathlib import Path

from backend.adapters.http.downloader import download_file
from backend.geo.constants import TIGER_TABBLOCK20_URL, FIPS_TO_ABBR
from backend.utils.logging import get_logger
from backend.utils.env import get_data_root

logger = get_logger("adapters.bootstrap.download_blocks")


def build_block_urls() -> list:
    """Build download URLs for all state TABBLOCK20 shapefiles."""
    urls = []
    for fips in sorted(FIPS_TO_ABBR.keys()):
        filename = f"tl_2020_{fips}_tabblock20.zip"
        urls.append(f"{TIGER_TABBLOCK20_URL}{filename}")
    return urls


def download_blocks(output_dir: Path = None) -> list:
    """Download TIGER 2020 block shapefiles for all states.

    Adapted from census-parquet download_blocks.sh.
    """
    if output_dir is None:
        output_dir = get_data_root() / "raw" / "tiger2020" / "tabblock20"

    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []

    for url in build_block_urls():
        filename = url.split("/")[-1]
        output_path = output_dir / filename
        if output_path.exists():
            logger.info("Skipping existing: %s", output_path)
            downloaded.append(output_path)
            continue
        download_file(url, output_path)
        downloaded.append(output_path)

    logger.info("Downloaded %d block files to %s", len(downloaded), output_dir)
    return downloaded


if __name__ == "__main__":
    download_blocks()
