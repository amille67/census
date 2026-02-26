"""Assembly join contracts for the master blockgroup table.

Defines the join sequence for assembling all ingest outputs into
the final master_blockgroup table.
"""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN = JoinContract(
    name="ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN",
    left_dataset="master_spine_bg_index",
    right_dataset="ingest_blockgroup_output",
    join_type=JoinType.LEFT,
    keys=["bg_geoid"],
    cardinality=Cardinality.ONE_TO_ONE,
    grain_before="block_group",
    grain_after="block_group",
    null_policy=NullPolicy.ALLOW,
    description=(
        "Left join an ingest output onto the BG spine index. "
        "Uses left join to preserve all BGs in spine even if source has no data. "
        "Each ingest is joined independently to prevent cross-source row issues."
    ),
    validations=("check_no_row_multiplication",),
)


# Recommended join order for assembly (start with complete geographic coverage)
ASSEMBLY_JOIN_ORDER = [
    "master_spine_bg_index",        # Base: complete BG coverage
    "acs_2023_5yr_blockgroup",      # Demographics/economics (high coverage)
    "lodes_wac_blockgroup",         # Employment metrics
    "nlcd_2021_blockgroup",         # Land cover
    "epa_frs_blockgroup",           # Point source aggregates
    # ... additional sources appended here
]
