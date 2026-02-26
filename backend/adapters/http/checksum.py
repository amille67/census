"""Checksum verification for downloaded files."""

from pathlib import Path

from backend.utils.hash import file_sha256
from backend.utils.logging import get_logger

logger = get_logger("adapters.http.checksum")


def verify_checksum(file_path: Path, expected_sha256: str) -> bool:
    """Verify a file's SHA-256 checksum."""
    actual = file_sha256(file_path)
    match = actual == expected_sha256
    if not match:
        logger.error(
            "Checksum mismatch for %s: expected %s, got %s",
            file_path, expected_sha256, actual,
        )
    return match
