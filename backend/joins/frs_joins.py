"""EPA FRS join contracts for hybrid ingest.

FRS uses a hybrid strategy:
  Path 1 (Scenario B): Records with valid Census Block Code (field 39)
    -> direct merge on block_geoid to spine
    -> aggregate to block group

  Path 2 (Scenario A): Records with lat/lon but no block code
    -> spatial point-in-polygon to TIGER blocks
    -> merge on block_geoid to spine
    -> aggregate to block group

  Final: concat both paths, re-aggregate to block group
"""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


# --- Path 1: Block-coded records (Scenario B) ---

FRS_BLOCK_TO_SPINE = JoinContract(
    name="FRS_BLOCK_TO_SPINE",
    left_dataset="frs_block_coded",
    right_dataset="master_spine_crosswalk",
    join_type=JoinType.LEFT,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="facility",
    grain_after="facility",
    null_policy=NullPolicy.WARN,
    description=(
        "Merge FRS records that have a Census Block Code directly to the "
        "master spine crosswalk on block_geoid. Many facilities can map to "
        "one block. Unmatched records (stale block codes) get null hierarchy."
    ),
)

FRS_BLOCK_PATH_TO_BG = JoinContract(
    name="FRS_BLOCK_PATH_TO_BG",
    left_dataset="frs_block_coded_with_spine",
    right_dataset="(self-aggregation)",
    join_type=JoinType.INNER,
    keys=["bg_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="facility",
    grain_after="block_group",
    null_policy=NullPolicy.DROP,
    description=(
        "Aggregate block-coded FRS facilities to block group grain via "
        "groupby(bg_geoid) with FRS-specific aggregation rules."
    ),
)


# --- Path 2: Point-only records (Scenario A) ---

FRS_POINT_TO_BLOCK_SPATIAL = JoinContract(
    name="FRS_POINT_TO_BLOCK_SPATIAL",
    left_dataset="frs_point_only",
    right_dataset="tiger_block_polygons",
    join_type=JoinType.SPATIAL,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="facility",
    grain_after="facility",
    null_policy=NullPolicy.WARN,
    description=(
        "Spatial point-in-polygon join of FRS facilities (without block codes) "
        "to 2020 TIGER block polygons to assign block_geoid."
    ),
    validations=("check_spatial_match_rate",),
)

FRS_POINT_BLOCK_TO_SPINE = JoinContract(
    name="FRS_POINT_BLOCK_TO_SPINE",
    left_dataset="frs_point_with_block",
    right_dataset="master_spine_crosswalk",
    join_type=JoinType.LEFT,
    keys=["block_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="facility",
    grain_after="facility",
    null_policy=NullPolicy.WARN,
    description="Attach spine hierarchy to FRS points after spatial block assignment.",
)

FRS_POINT_PATH_TO_BG = JoinContract(
    name="FRS_POINT_PATH_TO_BG",
    left_dataset="frs_point_with_spine",
    right_dataset="(self-aggregation)",
    join_type=JoinType.INNER,
    keys=["bg_geoid"],
    cardinality=Cardinality.MANY_TO_ONE,
    grain_before="facility",
    grain_after="block_group",
    null_policy=NullPolicy.DROP,
    description=(
        "Aggregate point-path FRS facilities to block group grain via "
        "groupby(bg_geoid) with FRS-specific aggregation rules."
    ),
)


# --- Final merge ---

FRS_COMBINE_PATHS = JoinContract(
    name="FRS_COMBINE_PATHS",
    left_dataset="frs_block_path_bg",
    right_dataset="frs_point_path_bg",
    join_type=JoinType.OUTER,
    keys=["bg_geoid"],
    cardinality=Cardinality.ONE_TO_ONE,
    grain_before="block_group",
    grain_after="block_group",
    null_policy=NullPolicy.ALLOW,
    description=(
        "Outer merge of block-path and point-path BG aggregations. "
        "Each bg_geoid appears in at most one path, so 1:1 expected. "
        "Numeric columns are summed; boolean columns are OR'd."
    ),
)
