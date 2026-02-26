"""Bootstrap runner for census-parquet adapted pipeline.

Equivalent of makepath's run_census_parquet but integrated into our
pipeline architecture.
"""

from pathlib import Path

from backend.utils.logging import get_logger
from backend.utils.env import get_data_root
from backend.utils.timing import timed

logger = get_logger("adapters.bootstrap.runner")


def run_bootstrap(
    data_root: Path = None,
    skip_download: bool = False,
    skip_boundaries: bool = False,
    skip_blocks: bool = False,
):
    """Run the full census-parquet bootstrap pipeline.

    Steps:
      1. Download boundaries (TIGER cartographic)
      2. Download population stats (PL 94-171)
      3. Download blocks (TIGER TABBLOCK20)
      4. Process boundaries -> parquet
      5. Process blocks + population -> parquet

    Set skip_* flags to resume from a specific step.
    """
    if data_root is None:
        data_root = get_data_root()

    from backend.adapters.census_parquet_bootstrap.download_boundaries import download_boundaries
    from backend.adapters.census_parquet_bootstrap.download_population_stats import download_population_stats
    from backend.adapters.census_parquet_bootstrap.download_blocks import download_blocks
    from backend.adapters.census_parquet_bootstrap.process_boundaries import process_all_boundaries

    staging = data_root / "staging" / "census_parquet_bootstrap"

    if not skip_download:
        if not skip_boundaries:
            with timed("Download boundaries"):
                download_boundaries(staging / "boundary_outputs")

        with timed("Download population stats"):
            download_population_stats(data_root / "raw" / "census_pl_2020" / "pl_segments")

        if not skip_blocks:
            with timed("Download blocks"):
                download_blocks(data_root / "raw" / "tiger2020" / "tabblock20")

    if not skip_boundaries:
        with timed("Process boundaries"):
            process_all_boundaries(
                input_dir=staging / "boundary_outputs",
                output_dir=staging / "boundary_outputs",
            )

    logger.info("Bootstrap pipeline complete. Outputs in %s", staging)
    return staging


if __name__ == "__main__":
    run_bootstrap()
