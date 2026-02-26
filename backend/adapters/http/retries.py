"""Retry utilities for HTTP operations."""

import time
from functools import wraps

from backend.utils.logging import get_logger

logger = get_logger("adapters.http.retries")


def retry_with_backoff(max_retries: int = 4, base_delay: float = 2.0):
    """Decorator that retries a function with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    logger.warning(
                        "%s attempt %d/%d failed: %s. Retrying in %.1fs",
                        func.__name__, attempt + 1, max_retries, e, delay,
                    )
                    time.sleep(delay)
        return wrapper
    return decorator
