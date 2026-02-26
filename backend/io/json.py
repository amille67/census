"""JSON I/O utilities."""

import json
from pathlib import Path
from datetime import datetime, timezone

from backend.utils.logging import get_logger

logger = get_logger("io.json")


def write_json(data: dict, path: Path, indent: int = 2):
    """Write a dictionary to a JSON file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=indent, default=str)
    logger.info("Wrote JSON to %s", path)


def read_json(path: Path) -> dict:
    """Read a JSON file into a dictionary."""
    with open(path) as f:
        return json.load(f)


def write_run_manifest(
    output_dir: Path,
    date_tag: str,
    ingest_files: list,
    spine_version: str,
):
    """Write a _run_manifest.json summarizing an ingest run."""
    manifest = {
        "date_tag": date_tag,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "spine_version": spine_version,
        "ingest_files": [str(f) for f in ingest_files],
        "file_count": len(ingest_files),
    }
    write_json(manifest, output_dir / "_run_manifest.json")
