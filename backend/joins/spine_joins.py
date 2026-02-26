"""Spine join contracts."""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


BLOCK_TO_SPINE = JoinContract(
    name="BLOCK_TO_SPINE",
    left_dataset="source_block_data",
    right_dataset="master_spine_crosswalk",
    join_type=JoinType.INNER,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="block",
    grain_after="block",
    null_policy=NullPolicy.WARN,
    description=(
        "Join block-level source data to master spine crosswalk. "
        "Adds hierarchy columns (bg_geoid, tract, county, msa) and area columns."
    ),
)

BG_TO_SPINE_BG_INDEX = JoinContract(
    name="BG_TO_SPINE_BG_INDEX",
    left_dataset="source_bg_data",
    right_dataset="master_spine_bg_index",
    join_type=JoinType.LEFT,
    keys=["bg_geoid"],
    cardinality=Cardinality.ONE_TO_ONE,
    grain_before="block_group",
    grain_after="block_group",
    null_policy=NullPolicy.WARN,
    description=(
        "Join BG-level source data to BG spine index. "
        "CRITICAL: Use bg_index (not block-grain spine) to prevent row duplication."
    ),
)
