"""LODES download and processing manifests."""

from datetime import datetime, timezone
from pathlib import Path

from backend.io.json import write_json
from backend.utils.logging import get_logger

logger = get_logger("adapters.lodes.manifests")


def write_download_manifest(
    output_dir: Path,
    downloaded_files: list,
    version: str,
    states: list,
    data_type: str,
):
    """Write a manifest of downloaded LODES files."""
    manifest = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": version,
        "data_type": data_type,
        "states": states,
        "file_count": len(downloaded_files),
        "files": [str(f) for f in downloaded_files],
    }
    path = output_dir / f"download_manifest_{data_type}.json"
    write_json(manifest, path)
    logger.info("Wrote download manifest: %s", path)
