"""Checkpoint support for pipeline resumability."""

import json
from pathlib import Path

from backend.utils.logging import get_logger

logger = get_logger("orchestration.checkpoints")


def save_checkpoint(checkpoint_dir: Path, step_name: str, metadata: dict = None):
    """Save a checkpoint after a step completes."""
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = checkpoint_dir / f"{step_name}.checkpoint.json"
    data = {"step": step_name, "metadata": metadata or {}}
    with open(checkpoint_file, "w") as f:
        json.dump(data, f)
    logger.info("Checkpoint saved: %s", step_name)


def has_checkpoint(checkpoint_dir: Path, step_name: str) -> bool:
    """Check if a checkpoint exists for a given step."""
    return (checkpoint_dir / f"{step_name}.checkpoint.json").exists()


def clear_checkpoints(checkpoint_dir: Path):
    """Clear all checkpoints."""
    for f in checkpoint_dir.glob("*.checkpoint.json"):
        f.unlink()
    logger.info("Cleared checkpoints in %s", checkpoint_dir)
