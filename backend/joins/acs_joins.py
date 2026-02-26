"""ACS-specific join contracts."""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


ACS_BG_TO_SPINE_BG_INDEX = JoinContract(
    name="ACS_BG_TO_SPINE_BG_INDEX",
    left_dataset="acs_blockgroup_normalized",
    right_dataset="master_spine_bg_index",
    join_type=JoinType.LEFT,
    keys=["bg_geoid"],
    cardinality=Cardinality.ONE_TO_ONE,
    grain_before="block_group",
    grain_after="block_group",
    null_policy=NullPolicy.WARN,
    description=(
        "Join ACS BG data to spine BG index. Uses BG index (not block-grain spine) "
        "to prevent row duplication. ACS already arrives at BG grain from Census API."
    ),
    validations=("check_no_row_multiplication", "check_match_rate_above_98pct"),
)
