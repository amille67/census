"""Raster/polygon source join contracts."""

from backend.joins.contracts import JoinContract, JoinType, Cardinality, NullPolicy


RASTER_ZONAL_TO_BG = JoinContract(
    name="RASTER_ZONAL_TO_BG",
    left_dataset="zonal_stats_output",
    right_dataset="master_spine_bg_index",
    join_type=JoinType.LEFT,
    keys=["bg_geoid"],
    cardinality=Cardinality.ONE_TO_ONE,
    grain_before="block_group",
    grain_after="block_group",
    null_policy=NullPolicy.WARN,
    description=(
        "Join raster zonal statistics (computed against BG polygons) to spine BG index. "
        "Zonal stats already output at BG grain so this is a 1:1 join."
    ),
)
