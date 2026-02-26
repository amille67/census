"""Validate ingest output conforms to the universal ingest schema."""

import pandas as pd

from backend.geo.validators import check_geoid_column
from backend.utils.logging import get_logger

logger = get_logger("transforms.ingest.validate")

REQUIRED_COLUMNS = ["bg_geoid", "tract_geoid", "county_geoid", "state_fips"]


def validate_ingest_output(df: pd.DataFrame, source_slug: str) -> dict:
    """Validate an ingest output DataFrame."""
    report = {"source_slug": source_slug, "passed": True, "checks": []}

    # Required columns present
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    report["checks"].append({
        "name": "required_columns",
        "passed": len(missing) == 0,
        "missing": missing,
    })
    if missing:
        report["passed"] = False

    # bg_geoid format
    if "bg_geoid" in df.columns:
        bg_check = check_geoid_column(df, "bg_geoid", "bg_geoid")
        report["checks"].append({"name": "bg_geoid_format", **bg_check})
        if bg_check["valid_rate"] < 0.95:
            report["passed"] = False

    # Grain check (bg_geoid should be unique)
    if "bg_geoid" in df.columns:
        is_unique = df["bg_geoid"].is_unique
        report["checks"].append({"name": "bg_grain_unique", "passed": is_unique})
        if not is_unique:
            logger.warning("Ingest output for '%s' has duplicate bg_geoid values", source_slug)

    # Row count
    report["row_count"] = len(df)
    report["checks"].append({
        "name": "non_empty",
        "passed": len(df) > 0,
    })

    status = "PASSED" if report["passed"] else "FAILED"
    logger.info("Ingest validation for '%s': %s", source_slug, status)
    return report
