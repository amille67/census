"""Join contract definitions.

Defines canonical JoinContract objects that specify exactly how two datasets
should be joined. This is the single most important module for enabling
LLM-generated pipelines to produce stable, correct joins.

Each contract specifies:
  - Left and right datasets
  - Join type and keys
  - Expected cardinality before and after
  - Grain before and after
  - Null policy and validation rules
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import pandas as pd

from backend.utils.exceptions import JoinCardinalityError
from backend.utils.logging import get_logger

logger = get_logger("joins.contracts")


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    SPATIAL = "spatial"


class Cardinality(str, Enum):
    ONE_TO_ONE = "1:1"
    MANY_TO_ONE = "m:1"
    ONE_TO_MANY = "1:m"
    MANY_TO_MANY = "m:m"


class NullPolicy(str, Enum):
    ALLOW = "allow"         # Nulls OK in join key (left join unmatched)
    FAIL = "fail"           # Raise on null join keys
    DROP = "drop"           # Drop rows with null join keys
    WARN = "warn"           # Log warning but continue


@dataclass(frozen=True)
class JoinContract:
    """Canonical join specification.

    Provides a deterministic target that an LLM can reference
    when generating join code.
    """
    name: str
    left_dataset: str
    right_dataset: str
    join_type: JoinType
    keys: list                      # List of join key column names
    cardinality: Cardinality
    grain_before: str               # Grain of primary dataset before join
    grain_after: str                # Expected grain after join
    null_policy: NullPolicy = NullPolicy.WARN
    description: str = ""
    validations: tuple = ()         # Tuple of validation function names

    def validate_result(self, left_df: pd.DataFrame, result_df: pd.DataFrame):
        """Validate the join result against contract expectations."""
        left_count = len(left_df)
        result_count = len(result_df)

        if self.cardinality == Cardinality.MANY_TO_ONE:
            if result_count > left_count:
                raise JoinCardinalityError(
                    f"Contract '{self.name}' expects m:1 but row count increased: "
                    f"{left_count} -> {result_count}. Check for duplicate keys in "
                    f"right dataset '{self.right_dataset}'."
                )

        if self.cardinality == Cardinality.ONE_TO_ONE:
            if result_count != left_count:
                raise JoinCardinalityError(
                    f"Contract '{self.name}' expects 1:1 but row count changed: "
                    f"{left_count} -> {result_count}."
                )

        # Check null policy
        for key in self.keys:
            if key in result_df.columns:
                null_count = result_df[key].isna().sum()
                if null_count > 0:
                    if self.null_policy == NullPolicy.FAIL:
                        raise JoinCardinalityError(
                            f"Contract '{self.name}' has null_policy=FAIL but found "
                            f"{null_count} nulls in '{key}'."
                        )
                    elif self.null_policy == NullPolicy.WARN:
                        logger.warning(
                            "Contract '%s': %d null values in '%s' after join",
                            self.name, null_count, key,
                        )

        logger.info(
            "Contract '%s' validated: %d -> %d rows (%s)",
            self.name, left_count, result_count, self.cardinality.value,
        )


def execute_join(
    contract: JoinContract,
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    validate: bool = True,
) -> pd.DataFrame:
    """Execute a join according to a contract specification."""
    if contract.join_type == JoinType.SPATIAL:
        raise NotImplementedError("Spatial joins must use geo.spatial_join module directly")

    result = left_df.merge(
        right_df,
        on=contract.keys,
        how=contract.join_type.value,
        suffixes=("", "_right"),
    )

    if validate:
        contract.validate_result(left_df, result)

    return result
