"""LODES data download adapter.

Refactored from UrbanInstitute/lodes-data-downloads download_lodes_data.py.
Lifts create_directories, get_links, download_file, unzip_file, process_files
into modular functions with configurable LODES version and years.
"""

import gzip
import re
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

from backend.geo.constants import LODES_STATES
from backend.adapters.http.downloader import download_file as http_download
from backend.utils.logging import get_logger

logger = get_logger("adapters.lodes.download")

# Modernized defaults (LODES8 with 2020 Census blocks)
DEFAULT_LODES_VERSION = "LODES8"
DEFAULT_LODES_BASE_URL = "https://lehd.ces.census.gov/data/lodes"
LEHD_FILES_URL = "https://lehd.ces.census.gov/php/inc_lodesFiles.php"
LEHD_START_URL = "https://lehd.ces.census.gov"


def get_links(state: str, data_type: str, version: str = DEFAULT_LODES_VERSION) -> list:
    """Get download links for a state's LODES data files.

    Adapted from Urban Institute's get_links() function.

    Args:
        state: State abbreviation (lowercase)
        data_type: LODES data type ('wac', 'rac', 'od')
        version: LODES version string (e.g., 'LODES8')

    Returns:
        List of download URLs
    """
    form_data = {
        "version": version,
        "type": data_type,
        "state": state,
    }

    try:
        response = requests.post(LEHD_FILES_URL, data=form_data, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to get links for %s/%s: %s", state, data_type, e)
        return []

    soup = BeautifulSoup(response.text, "lxml")
    file_list_div = soup.find("div", {"id": "lodes_file_list"})
    if not file_list_div:
        logger.warning("No file list found for %s/%s", state, data_type)
        return []

    links = [
        f"{LEHD_START_URL}{a['href']}"
        for a in file_list_div.find_all("a")
        if a.get("href")
    ]
    logger.info("Found %d links for %s/%s", len(links), state, data_type)
    return links


def download_and_extract(url: str, output_dir: Path) -> Optional[Path]:
    """Download a LODES gzip file and extract to CSV.

    Adapted from Urban Institute's download_file() and unzip_file() functions.
    """
    filename = url.split("/")[-1]
    gz_path = output_dir / filename
    csv_filename = filename.replace(".gz", "")
    csv_path = output_dir / csv_filename

    if csv_path.exists():
        logger.info("Skipping existing: %s", csv_path)
        return csv_path

    try:
        http_download(url, gz_path, resume=False)
    except Exception as e:
        logger.error("Failed to download %s: %s", url, e)
        return None

    try:
        with gzip.open(gz_path, "rb") as f_in:
            with open(csv_path, "wb") as f_out:
                f_out.write(f_in.read())
        gz_path.unlink()
        logger.info("Extracted: %s", csv_path)
        return csv_path
    except Exception as e:
        logger.error("Failed to extract %s: %s", gz_path, e)
        return None


def download_wac(
    states: list = None,
    output_dir: Path = None,
    version: str = DEFAULT_LODES_VERSION,
) -> list:
    """Download WAC (Workplace Area Characteristics) data for given states.

    Args:
        states: List of state abbreviations (lowercase). Defaults to all states.
        output_dir: Output directory for extracted CSVs.
        version: LODES version.

    Returns:
        List of paths to extracted CSV files.
    """
    if states is None:
        states = LODES_STATES
    if output_dir is None:
        from backend.utils.env import get_data_root
        output_dir = get_data_root() / "raw" / "lodes" / "LODES8" / "wac" / "extracted_csv"

    output_dir.mkdir(parents=True, exist_ok=True)
    all_csvs = []

    for state in states:
        logger.info("Downloading WAC for %s", state.upper())
        links = get_links(state, "wac", version)
        for url in links:
            csv_path = download_and_extract(url, output_dir)
            if csv_path:
                all_csvs.append(csv_path)

    logger.info("Downloaded %d WAC files", len(all_csvs))
    return all_csvs
