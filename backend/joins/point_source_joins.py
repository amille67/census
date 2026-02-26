"""Point source join contracts."""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


POINT_TO_BLOCK_SPATIAL = JoinContract(
    name="POINT_TO_BLOCK_SPATIAL",
    left_dataset="geocoded_points",
    right_dataset="tiger_block_polygons",
    join_type=JoinType.SPATIAL,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="point",
    grain_after="point",
    null_policy=NullPolicy.WARN,
    description=(
        "Spatial point-in-polygon join of geocoded points to 2020 TIGER block polygons. "
        "Many points can fall within one block."
    ),
    validations=("check_spatial_match_rate",),
)

POINT_BLOCK_TO_SPINE = JoinContract(
    name="POINT_BLOCK_TO_SPINE",
    left_dataset="points_with_block_geoid",
    right_dataset="master_spine_crosswalk",
    join_type=JoinType.LEFT,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="point",
    grain_after="point",
    null_policy=NullPolicy.WARN,
    description="Attach spine hierarchy to points after spatial block assignment.",
)

POINT_TO_BG_AGGREGATION = JoinContract(
    name="POINT_TO_BG_AGGREGATION",
    left_dataset="points_with_spine_hierarchy",
    right_dataset="(self-aggregation)",
    join_type=JoinType.INNER,
    keys=["bg_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="point",
    grain_after="block_group",
    null_policy=NullPolicy.FAIL,
    description="Aggregate points to block group grain via groupby(bg_geoid).",
)
