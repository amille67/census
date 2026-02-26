"""Run manifests for tracking pipeline executions."""

from datetime import datetime, timezone
from pathlib import Path

from backend.io.json import write_json
from backend.utils.logging import get_logger

logger = get_logger("orchestration.manifests")


def write_pipeline_manifest(
    output_dir: Path,
    run_id: str,
    pipeline_name: str,
    steps_completed: list,
    output_files: list,
    errors: list = None,
):
    """Write a pipeline run manifest."""
    manifest = {
        "run_id": run_id,
        "pipeline": pipeline_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "steps_completed": steps_completed,
        "output_files": [str(f) for f in output_files],
        "errors": errors or [],
        "success": len(errors or []) == 0,
    }
    write_json(manifest, output_dir / f"pipeline_manifest_{run_id}.json")
