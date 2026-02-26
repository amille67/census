"""Schema validation checks."""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backend.utils.logging import get_logger

logger = get_logger("qa.schema")


def check_schema_conformity(df: pd.DataFrame, expected_columns: dict) -> dict:
    """Check that a DataFrame conforms to expected column types.

    Args:
        df: DataFrame to check
        expected_columns: Dict of {column_name: expected_dtype_string}
    """
    results = []
    for col, expected_type in expected_columns.items():
        if col not in df.columns:
            results.append({"column": col, "status": "missing", "passed": False})
            continue

        actual_type = str(df[col].dtype)
        passed = expected_type in actual_type or actual_type == expected_type
        results.append({
            "column": col,
            "expected": expected_type,
            "actual": actual_type,
            "passed": passed,
        })

    all_passed = all(r["passed"] for r in results)
    return {"passed": all_passed, "columns": results}
