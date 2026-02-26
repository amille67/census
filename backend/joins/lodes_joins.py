"""LODES-specific join contracts."""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


LODES_BLOCK_TO_SPINE = JoinContract(
    name="LODES_BLOCK_TO_SPINE",
    left_dataset="lodes_wac_normalized",
    right_dataset="master_spine_crosswalk",
    join_type=JoinType.INNER,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="block",
    grain_after="block",
    null_policy=NullPolicy.WARN,
    description=(
        "Join LODES WAC data (w_geocode normalized to block_geoid) to spine. "
        "Inner join drops LODES blocks not in 2020 spine (e.g., invalid geocodes)."
    ),
    validations=("check_match_rate_above_95pct",),
)

LODES_BLOCK_TO_BG_AGGREGATION = JoinContract(
    name="LODES_BLOCK_TO_BG_AGGREGATION",
    left_dataset="lodes_wac_with_spine",
    right_dataset="(self-aggregation)",
    join_type=JoinType.INNER,
    keys=["bg_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="block",
    grain_after="block_group",
    null_policy=NullPolicy.FAIL,
    description=(
        "Aggregate LODES block data to block group. "
        "Sum all job count columns. Recompute diversity metrics at BG grain."
    ),
)
