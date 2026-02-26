"""Timing utilities for pipeline profiling."""

import time
from contextlib import contextmanager
from backend.utils.logging import get_logger

logger = get_logger("timing")


@contextmanager
def timed(label: str):
    """Context manager that logs elapsed time for a block of code."""
    start = time.perf_counter()
    logger.info("Started: %s", label)
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info("Finished: %s (%.2fs)", label, elapsed)
