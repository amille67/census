"""Row count QA checks."""

from backend.utils.logging import get_logger

logger = get_logger("qa.row_count")


def check_row_count_preserved(before: int, after: int, tolerance: float = 0.01) -> dict:
    """Check that row count is preserved within tolerance."""
    if before == 0:
        return {"passed": after == 0, "before": before, "after": after}

    change_rate = abs(after - before) / before
    passed = change_rate <= tolerance

    if not passed:
        logger.warning("Row count changed: %d -> %d (%.1f%%)", before, after, change_rate * 100)

    return {"passed": passed, "before": before, "after": after, "change_rate": change_rate}


def check_sum_preserved(before_sum: float, after_sum: float, tolerance: float = 0.001) -> dict:
    """Check that sum of a column is preserved before/after aggregation."""
    if before_sum == 0:
        return {"passed": after_sum == 0, "before": before_sum, "after": after_sum}

    diff_rate = abs(after_sum - before_sum) / before_sum
    passed = diff_rate <= tolerance

    if not passed:
        logger.warning("Sum changed: %.2f -> %.2f (%.4f%%)", before_sum, after_sum, diff_rate * 100)

    return {"passed": passed, "before": before_sum, "after": after_sum, "diff_rate": diff_rate}
