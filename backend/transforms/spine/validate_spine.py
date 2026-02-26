"""Spine validation checks."""

import pandas as pd

from backend.geo.validators import check_geoid_column
from backend.utils.logging import get_logger
from backend.utils.exceptions import SchemaValidationError

logger = get_logger("transforms.spine.validate")


def validate_spine(spine_df: pd.DataFrame) -> dict:
    """Run validation checks on the master spine crosswalk.

    Checks:
    - block_geoid uniqueness and format
    - bg_geoid format
    - hierarchy derivation consistency
    - non-negative area columns
    - expected state coverage
    """
    report = {"passed": True, "checks": []}

    # 1. block_geoid uniqueness
    block_check = check_geoid_column(spine_df, "block_geoid", "block_geoid")
    report["checks"].append({"name": "block_geoid_format", **block_check})
    if not block_check["is_unique"]:
        report["passed"] = False
        logger.error("block_geoid is not unique!")

    # 2. bg_geoid format
    bg_check = check_geoid_column(spine_df, "bg_geoid", "bg_geoid")
    report["checks"].append({"name": "bg_geoid_format", **bg_check})

    # 3. Hierarchy consistency
    consistent = (
        (spine_df["bg_geoid"] == spine_df["block_geoid"].str[:12]).all()
        and (spine_df["tract_geoid"] == spine_df["block_geoid"].str[:11]).all()
        and (spine_df["county_geoid"] == spine_df["block_geoid"].str[:5]).all()
        and (spine_df["state_fips"] == spine_df["block_geoid"].str[:2]).all()
    )
    report["checks"].append({"name": "hierarchy_consistency", "passed": bool(consistent)})
    if not consistent:
        report["passed"] = False
        logger.error("Hierarchy derivation inconsistency detected!")

    # 4. Non-negative areas
    area_cols = ["gross_land_area_sq_m", "water_area_sq_m",
                 "protected_area_sq_m", "net_developable_area_sq_m"]
    for col in area_cols:
        if col in spine_df.columns:
            has_negative = (spine_df[col] < 0).any()
            report["checks"].append({
                "name": f"{col}_non_negative",
                "passed": not bool(has_negative),
            })

    # 5. State coverage
    n_states = spine_df["state_fips"].nunique()
    report["checks"].append({
        "name": "state_coverage",
        "n_states": n_states,
        "passed": n_states >= 50,
    })

    logger.info("Spine validation: %s (%d checks)", "PASSED" if report["passed"] else "FAILED",
                len(report["checks"]))
    return report
