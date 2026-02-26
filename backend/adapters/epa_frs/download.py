"""Download and extract the EPA FRS national single facility file."""

import zipfile
from pathlib import Path

from backend.adapters.http.downloader import download_file
from backend.utils.logging import get_logger

logger = get_logger("adapters.epa_frs.download")

FRS_NATIONAL_URL = (
    "https://ordsext.epa.gov/FLA/www3/state_files/national_single.zip"
)


def download_frs_national(
    output_dir: Path,
    url: str = FRS_NATIONAL_URL,
    force: bool = False,
) -> Path:
    """Download and extract the FRS national single facility ZIP.

    Args:
        output_dir: Directory to save the downloaded and extracted files
        url: Download URL (defaults to national_single.zip)
        force: If True, re-download even if file exists

    Returns:
        Path to the extracted CSV file
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_path = output_dir / "national_single.zip"
    csv_path = output_dir / "NATIONAL_SINGLE.CSV"

    if csv_path.exists() and not force:
        logger.info("FRS CSV already exists: %s", csv_path)
        return csv_path

    # Download ZIP
    logger.info("Downloading FRS national single file from %s", url)
    download_file(url, zip_path, timeout=300)

    # Extract
    logger.info("Extracting %s", zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        if not csv_names:
            raise FileNotFoundError("No CSV file found in FRS ZIP archive")

        # Extract the first (usually only) CSV
        target_name = csv_names[0]
        zf.extract(target_name, output_dir)
        extracted_path = output_dir / target_name

        # Rename to standard name if different
        if extracted_path != csv_path:
            extracted_path.rename(csv_path)

    logger.info("FRS CSV extracted: %s", csv_path)
    return csv_path
