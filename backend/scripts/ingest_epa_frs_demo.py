"""EPA FRS Demo Ingest — DEPRECATED.

This script has been superseded by ingest_epa_frs.py which implements the
full hybrid block/point ingest strategy.

Usage:
  python -m backend.scripts.ingest_epa_frs --data-root ./data
  python -m backend.scripts.ingest_epa_frs --csv path/to/NATIONAL_SINGLE.CSV
"""

from backend.utils.logging import get_logger

logger = get_logger("scripts.ingest_epa_frs_demo")


def main():
    logger.warning(
        "ingest_epa_frs_demo.py is deprecated. "
        "Use ingest_epa_frs.py instead for hybrid block/point ingest."
    )
    from backend.scripts.ingest_epa_frs import main as frs_main
    frs_main()


if __name__ == "__main__":
    main()
