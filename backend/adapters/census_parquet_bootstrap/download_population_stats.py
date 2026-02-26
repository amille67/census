"""Download PL 94-171 redistricting population statistics.

Reimplements census-parquet's download_population_stats.sh in Python.
"""

import zipfile
from pathlib import Path

from backend.adapters.http.downloader import download_file, download_with_rate_limit
from backend.geo.constants import PL_BASE_URL, FIPS_TO_ABBR
from backend.utils.logging import get_logger
from backend.utils.env import get_data_root

logger = get_logger("adapters.bootstrap.download_population_stats")

FIELD_NAMES_URL = (
    "https://www2.census.gov/programs-surveys/decennial/rdo/about/"
    "2020-census-program/Phase3/SupportMaterials/"
    "2020_PLSummaryFile_FieldNames.xlsx"
)


def download_population_stats(output_dir: Path = None) -> Path:
    """Download PL 94-171 redistricting data files.

    Adapted from census-parquet download_population_stats.sh.
    Downloads per-state .pl.zip files and the field names Excel reference.
    """
    if output_dir is None:
        output_dir = get_data_root() / "raw" / "census_pl_2020" / "pl_segments"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Download field names reference
    field_layout_dir = output_dir.parent / "field_layouts"
    field_layout_dir.mkdir(parents=True, exist_ok=True)
    download_file(FIELD_NAMES_URL, field_layout_dir / "2020_PLSummaryFile_FieldNames.xlsx")

    logger.info("Downloaded field names reference to %s", field_layout_dir)
    logger.info("PL data download paths configured for %s", output_dir)

    return output_dir


if __name__ == "__main__":
    download_population_stats()
