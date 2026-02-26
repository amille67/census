"""HTTP download utilities with retry and resume support."""

import time
from pathlib import Path
from typing import Optional

import requests

from backend.utils.logging import get_logger
from backend.utils.exceptions import DownloadError

logger = get_logger("adapters.http.downloader")

DEFAULT_CHUNK_SIZE = 8192
DEFAULT_TIMEOUT = 60
MAX_RETRIES = 4


def download_file(
    url: str,
    output_path: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = MAX_RETRIES,
    resume: bool = True,
) -> Path:
    """Download a file from a URL with retry and optional resume support.

    Args:
        url: Source URL
        output_path: Where to save the file
        chunk_size: Download chunk size in bytes
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        resume: Whether to attempt resume of partial downloads

    Returns:
        Path to the downloaded file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(retries):
        try:
            headers = {}
            mode = "wb"
            if resume and output_path.exists():
                existing_size = output_path.stat().st_size
                headers["Range"] = f"bytes={existing_size}-"
                mode = "ab"
                logger.info("Resuming download from byte %d: %s", existing_size, url)
            else:
                logger.info("Downloading: %s", url)

            response = requests.get(url, headers=headers, stream=True, timeout=timeout)
            response.raise_for_status()

            with open(output_path, mode) as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            logger.info("Downloaded: %s -> %s", url, output_path)
            return output_path

        except (requests.RequestException, IOError) as e:
            wait_time = 2 ** (attempt + 1)
            logger.warning(
                "Download attempt %d/%d failed for %s: %s. Retrying in %ds",
                attempt + 1, retries, url, e, wait_time,
            )
            if attempt < retries - 1:
                time.sleep(wait_time)

    raise DownloadError(f"Failed to download {url} after {retries} attempts")


def download_with_rate_limit(
    urls: list,
    output_dir: Path,
    rate_limit_seconds: float = 0.5,
) -> list:
    """Download multiple files with rate limiting between requests."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for url in urls:
        filename = url.split("/")[-1]
        output_path = output_dir / filename
        try:
            download_file(url, output_path)
            results.append(output_path)
        except DownloadError:
            logger.error("Skipping failed download: %s", url)
        time.sleep(rate_limit_seconds)

    return results
