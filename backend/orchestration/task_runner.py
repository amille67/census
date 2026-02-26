"""Task runner for sequential pipeline execution."""

from typing import Callable, List

from backend.orchestration.run_context import RunContext
from backend.utils.logging import get_logger
from backend.utils.timing import timed

logger = get_logger("orchestration.task_runner")


def run_tasks(tasks: List[tuple], context: RunContext):
    """Execute a list of (name, callable) tasks sequentially.

    Args:
        tasks: List of (task_name, task_function) tuples
        context: RunContext for tracking
    """
    for name, func in tasks:
        logger.info("Running task: %s", name)
        try:
            with timed(name):
                func(context)
            context.mark_complete(name)
        except Exception as e:
            logger.error("Task '%s' failed: %s", name, e)
            context.mark_error(name, str(e))
            raise

    logger.info("All %d tasks complete", len(tasks))
