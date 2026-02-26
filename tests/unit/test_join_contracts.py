"""Tests for join contract validation."""

import pandas as pd
import pytest

from backend.joins.contracts import (
    JoinContract, JoinType, Cardinality, NullPolicy,
    execute_join,
)
from backend.utils.exceptions import JoinCardinalityError


class TestJoinContractValidation:

    def test_one_to_one_contract_passes(self):
        contract = JoinContract(
            name="test_1to1",
            left_dataset="left",
            right_dataset="right",
            join_type=JoinType.LEFT,
            keys=["bg_geoid"],
            cardinality=Cardinality.ONE_TO_ONE,
            grain_before="bg",
            grain_after="bg",
        )

        left = pd.DataFrame({"bg_geoid": ["001", "002"], "val": [1, 2]})
        right = pd.DataFrame({"bg_geoid": ["001", "002"], "extra": [10, 20]})

        result = execute_join(contract, left, right)
        assert len(result) == 2

    def test_many_to_one_prevents_row_multiplication(self):
        contract = JoinContract(
            name="test_m_to_1",
            left_dataset="left",
            right_dataset="right",
            join_type=JoinType.LEFT,
            keys=["bg_geoid"],
            cardinality=Cardinality.MANY_TO_ONE,
            grain_before="block",
            grain_after="block",
        )

        left = pd.DataFrame({"bg_geoid": ["001", "002"], "val": [1, 2]})
        # Duplicate key in right causes multiplication
        right = pd.DataFrame({"bg_geoid": ["001", "001", "002"], "extra": [10, 11, 20]})

        with pytest.raises(JoinCardinalityError, match="row count increased"):
            execute_join(contract, left, right)

    def test_null_policy_fail_raises(self):
        contract = JoinContract(
            name="test_null_fail",
            left_dataset="left",
            right_dataset="right",
            join_type=JoinType.LEFT,
            keys=["bg_geoid"],
            cardinality=Cardinality.ONE_TO_ONE,
            grain_before="bg",
            grain_after="bg",
            null_policy=NullPolicy.FAIL,
        )

        left = pd.DataFrame({"bg_geoid": ["001", "002", "003"], "val": [1, 2, 3]})
        right = pd.DataFrame({"bg_geoid": ["001", "002"], "extra": [10, 20]})

        with pytest.raises(JoinCardinalityError, match="null_policy=FAIL"):
            execute_join(contract, left, right)

    def test_null_policy_allow_passes(self):
        contract = JoinContract(
            name="test_null_allow",
            left_dataset="left",
            right_dataset="right",
            join_type=JoinType.LEFT,
            keys=["bg_geoid"],
            cardinality=Cardinality.ONE_TO_ONE,
            grain_before="bg",
            grain_after="bg",
            null_policy=NullPolicy.ALLOW,
        )

        left = pd.DataFrame({"bg_geoid": ["001", "002", "003"], "val": [1, 2, 3]})
        right = pd.DataFrame({"bg_geoid": ["001", "002"], "extra": [10, 20]})

        result = execute_join(contract, left, right)
        assert len(result) == 3  # Left join preserves all rows
        assert result["extra"].isna().sum() == 1  # One unmatched


class TestSourceSpecificContracts:

    def test_lodes_contract_exists(self):
        from backend.joins.lodes_joins import LODES_BLOCK_TO_SPINE
        assert LODES_BLOCK_TO_SPINE.keys == ["block_geoid"]
        assert LODES_BLOCK_TO_SPINE.cardinality == Cardinality.MANY_TO_ONE

    def test_acs_contract_uses_bg_index(self):
        from backend.joins.acs_joins import ACS_BG_TO_SPINE_BG_INDEX
        assert ACS_BG_TO_SPINE_BG_INDEX.keys == ["bg_geoid"]
        assert ACS_BG_TO_SPINE_BG_INDEX.cardinality == Cardinality.ONE_TO_ONE

    def test_assembly_contract_is_left_join(self):
        from backend.joins.assembly_joins import ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN
        assert ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN.join_type == JoinType.LEFT
        assert ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN.null_policy == NullPolicy.ALLOW

    def test_point_spatial_join_raises_on_execute(self):
        from backend.joins.point_source_joins import POINT_TO_BLOCK_SPATIAL
        assert POINT_TO_BLOCK_SPATIAL.join_type == JoinType.SPATIAL

        with pytest.raises(NotImplementedError):
            execute_join(
                POINT_TO_BLOCK_SPATIAL,
                pd.DataFrame(),
                pd.DataFrame(),
            )
