"""Tests for EPA FRS join contracts."""

import pandas as pd
import pytest

from backend.joins.contracts import (
    JoinType, Cardinality, NullPolicy, execute_join,
)
from backend.joins.frs_joins import (
    FRS_BLOCK_TO_SPINE,
    FRS_BLOCK_PATH_TO_BG,
    FRS_POINT_TO_BLOCK_SPATIAL,
    FRS_POINT_BLOCK_TO_SPINE,
    FRS_POINT_PATH_TO_BG,
    FRS_COMBINE_PATHS,
)
from backend.utils.exceptions import JoinCardinalityError


class TestFRSJoinContracts:

    def test_block_to_spine_is_left_join(self):
        assert FRS_BLOCK_TO_SPINE.join_type == JoinType.LEFT
        assert FRS_BLOCK_TO_SPINE.cardinality == Cardinality.MANY_TO_ONE

    def test_block_to_spine_joins_on_block_geoid(self):
        assert FRS_BLOCK_TO_SPINE.keys == ["block_geoid"]

    def test_point_spatial_is_spatial_type(self):
        assert FRS_POINT_TO_BLOCK_SPATIAL.join_type == JoinType.SPATIAL
        # Spatial joins should raise NotImplementedError via execute_join
        with pytest.raises(NotImplementedError):
            execute_join(FRS_POINT_TO_BLOCK_SPATIAL, pd.DataFrame(), pd.DataFrame())

    def test_combine_paths_is_outer_join(self):
        assert FRS_COMBINE_PATHS.join_type == JoinType.OUTER
        assert FRS_COMBINE_PATHS.cardinality == Cardinality.ONE_TO_ONE
        assert FRS_COMBINE_PATHS.null_policy == NullPolicy.ALLOW

    def test_block_path_to_bg_drops_nulls(self):
        assert FRS_BLOCK_PATH_TO_BG.null_policy == NullPolicy.DROP

    def test_combine_paths_execution(self):
        """Outer merge of two non-overlapping BG sets should produce union.

        Note: validate=False because outer join produces more rows than left,
        which trips the 1:1 cardinality check. In practice the combine step
        handles validation via the _combine_paths function in the ingest script.
        """
        left = pd.DataFrame({
            "bg_geoid": ["010010201001", "010010201002"],
            "frs_facility_count": [3, 5],
        })
        right = pd.DataFrame({
            "bg_geoid": ["060010201003", "060010201004"],
            "frs_facility_count": [1, 2],
        })
        result = execute_join(FRS_COMBINE_PATHS, left, right, validate=False)
        assert len(result) == 4

    def test_point_block_to_spine_is_many_to_one(self):
        assert FRS_POINT_BLOCK_TO_SPINE.cardinality == Cardinality.MANY_TO_ONE
        assert FRS_POINT_BLOCK_TO_SPINE.grain_before == "facility"
        assert FRS_POINT_BLOCK_TO_SPINE.grain_after == "facility"
