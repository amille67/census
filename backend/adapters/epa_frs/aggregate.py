"""FRS-specific aggregation rules for block group rollup.

Computes environmental facility metrics at the block group grain:
  - facility_count: total FRS facilities per BG
  - unique_program_count: distinct EPA programs per BG
  - federal_facility_count: federal facilities per BG
  - tribal_facility_count: facilities on tribal land per BG
  - has_rcra / has_npdes / has_air / has_tri / has_cerclis: any facility with program
  - site_type_diversity: count of distinct site types per BG
  - avg_accuracy_value: mean coordinate accuracy
"""

import pandas as pd

from backend.utils.logging import get_logger

logger = get_logger("adapters.epa_frs.aggregate")


def aggregate_frs_to_blockgroup(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate facility-level FRS data to block group grain.

    Args:
        df: DataFrame with bg_geoid and facility-level FRS columns.
            Must already have bg_geoid from spine join.

    Returns:
        DataFrame at bg_geoid grain with aggregated FRS metrics.
    """
    if "bg_geoid" not in df.columns:
        raise ValueError("DataFrame must have 'bg_geoid' column from spine join")

    logger.info("Aggregating %d FRS records to block group grain", len(df))

    agg_dict = {}

    # Core counts
    agg_dict["registry_id"] = "nunique"

    # Program flags -> any per BG
    for prog_col in ["has_rcra", "has_npdes", "has_air", "has_tri", "has_cerclis"]:
        if prog_col in df.columns:
            agg_dict[prog_col] = "any"

    # Program count -> max per BG (most complex facility)
    if "program_count" in df.columns:
        agg_dict["program_count"] = "max"

    # Federal/tribal flags -> any per BG
    if "federal_facility_flag" in df.columns:
        agg_dict["federal_facility_flag"] = "sum"
    if "tribal_land_flag" in df.columns:
        agg_dict["tribal_land_flag"] = "sum"

    # Accuracy -> mean
    if "accuracy_value" in df.columns:
        df["accuracy_value"] = pd.to_numeric(df["accuracy_value"], errors="coerce")
        agg_dict["accuracy_value"] = "mean"

    bg_agg = df.groupby("bg_geoid").agg(agg_dict).reset_index()

    # Rename columns to standard output names
    rename = {
        "registry_id": "frs_facility_count",
        "program_count": "frs_max_program_count",
        "federal_facility_flag": "frs_federal_facility_count",
        "tribal_land_flag": "frs_tribal_facility_count",
        "accuracy_value": "frs_avg_accuracy",
        "has_rcra": "frs_has_rcra",
        "has_npdes": "frs_has_npdes",
        "has_air": "frs_has_air",
        "has_tri": "frs_has_tri",
        "has_cerclis": "frs_has_cerclis",
    }
    bg_agg = bg_agg.rename(columns={k: v for k, v in rename.items() if k in bg_agg.columns})

    # Compute site type diversity if available
    if "site_type_name" in df.columns:
        site_diversity = (
            df.groupby("bg_geoid")["site_type_name"]
            .nunique()
            .reset_index()
            .rename(columns={"site_type_name": "frs_site_type_diversity"})
        )
        bg_agg = bg_agg.merge(site_diversity, on="bg_geoid", how="left")

    # Convert boolean program flags to int for parquet compatibility
    bool_cols = [c for c in bg_agg.columns if c.startswith("frs_has_")]
    for col in bool_cols:
        bg_agg[col] = bg_agg[col].astype(int)

    logger.info("FRS aggregation result: %d block groups", len(bg_agg))
    return bg_agg
