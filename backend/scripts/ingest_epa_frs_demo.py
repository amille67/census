"""EPA FRS Demo Ingest (Scenario A: Point Source).

Demonstrates the full point-to-blockgroup ingest pipeline using
EPA Facility Registry Service data.

Usage:
  python -m backend.scripts.ingest_epa_frs_demo --csv path/to/frs.csv
"""

import sys
from pathlib import Path

import click
import geopandas as gpd
import pandas as pd

from backend.utils.logging import setup_logging, get_logger
from backend.utils.env import load_dotenv, get_data_root
from backend.utils.timing import timed
from backend.adapters.points.csv_points_reader import read_csv_points
from backend.transforms.ingest.point_to_blockgroup import ingest_point_source
from backend.transforms.common.provenance import add_provenance_columns
from backend.transforms.ingest.validate_ingest_output import validate_ingest_output
from backend.io.parquet import read_parquet, write_parquet
from backend.io.filenames import get_date_tag, ingest_output_path

logger = get_logger("scripts.ingest_epa_frs_demo")


@click.command()
@click.option("--csv", type=click.Path(exists=True), required=True, help="Path to FRS CSV")
@click.option("--data-root", type=click.Path(), default=None)
@click.option("--date-tag", type=str, default=None)
def main(csv, data_root, date_tag):
    """Ingest EPA FRS point data to block group grain."""
    load_dotenv()
    setup_logging()

    data_root = Path(data_root) if data_root else get_data_root()
    date_tag = date_tag or get_date_tag()

    # Read points
    with timed("Read FRS points"):
        points_gdf = read_csv_points(
            Path(csv),
            lat_column="LATITUDE83",
            lon_column="LONGITUDE83",
            id_column="REGISTRY_ID",
        )

    # Load spine and block polygons
    spine_path = data_root / "processed" / "spine" / "master_spine_crosswalk.parquet"
    blocks_path = data_root / "staging" / "spatial" / "tiger_blocks_2020.parquet"

    if not spine_path.exists():
        logger.error("Spine not found. Run build_master_spine first.")
        sys.exit(1)

    spine_df = read_parquet(spine_path)
    block_polygons = gpd.read_parquet(blocks_path) if blocks_path.exists() else None

    if block_polygons is None:
        logger.error("Block polygons not found at %s", blocks_path)
        sys.exit(1)

    # Run point ingest
    with timed("EPA FRS point ingest"):
        result = ingest_point_source(
            points_gdf=points_gdf,
            block_polygons=block_polygons,
            spine_df=spine_df,
            aggregation_rules={"_count": "count"},
        )

    # Add provenance
    result = add_provenance_columns(result, "epa_frs", "2024",
                                     row_count_pre_agg=len(points_gdf))

    # Validate and write
    validate_ingest_output(result, "epa_frs")
    output_path = ingest_output_path(data_root, date_tag, "epa_frs")
    write_parquet(result, output_path)

    logger.info("EPA FRS demo ingest complete: %d BGs -> %s", len(result), output_path)


if __name__ == "__main__":
    main()
