"""EPA FRS Ingest — Hybrid Block/Point Pipeline.

Ingests the EPA Facility Registry Service national single facility file
into the Census 2020 block group spine using a hybrid strategy:

  Path 1 (Scenario B): ~60-70% of records have a Census Block Code (field 39)
    -> direct merge on block_geoid to master spine
    -> aggregate to block group with FRS metrics

  Path 2 (Scenario A): Remaining records have lat/lon but no block code
    -> spatial point-in-polygon to TIGER 2020 block polygons
    -> merge block_geoid to master spine
    -> aggregate to block group with FRS metrics

  Final: outer-merge both paths, re-aggregate overlapping BGs, validate.

Usage:
  python -m backend.scripts.ingest_epa_frs --data-root ./data
  python -m backend.scripts.ingest_epa_frs --csv path/to/NATIONAL_SINGLE.CSV
"""

import json
import sys
from pathlib import Path

import click
import geopandas as gpd
import pandas as pd

from backend.utils.logging import setup_logging, get_logger
from backend.utils.env import load_dotenv, get_data_root
from backend.utils.timing import timed
from backend.adapters.epa_frs.download import download_frs_national
from backend.adapters.epa_frs.parse import parse_frs_single_file
from backend.adapters.epa_frs.normalize import normalize_frs
from backend.adapters.epa_frs.aggregate import aggregate_frs_to_blockgroup
from backend.adapters.points.csv_points_reader import read_csv_points
from backend.geo.spatial_join import point_in_polygon
from backend.geo.constants import BLOCK_GEOID_LEN
from backend.transforms.ingest.native_block_to_blockgroup import ingest_native_block
from backend.transforms.common.provenance import add_provenance_columns
from backend.transforms.ingest.validate_ingest_output import validate_ingest_output
from backend.io.parquet import read_parquet, write_parquet
from backend.io.filenames import get_date_tag, ingest_output_path, metadata_output_path

logger = get_logger("scripts.ingest_epa_frs")


def _load_spine(data_root: Path) -> pd.DataFrame:
    """Load the master spine crosswalk."""
    spine_path = data_root / "processed" / "spine" / "master_spine_crosswalk.parquet"
    if not spine_path.exists():
        logger.error("Spine not found at %s. Run build_master_spine first.", spine_path)
        sys.exit(1)
    return read_parquet(spine_path)


def _load_block_polygons(data_root: Path) -> gpd.GeoDataFrame:
    """Load TIGER 2020 block polygons for spatial join."""
    blocks_path = data_root / "staging" / "spatial" / "tiger_blocks_2020.parquet"
    if not blocks_path.exists():
        logger.error("Block polygons not found at %s", blocks_path)
        sys.exit(1)
    return gpd.read_parquet(blocks_path)


def _ingest_block_path(
    block_df: pd.DataFrame,
    spine_df: pd.DataFrame,
) -> pd.DataFrame:
    """Path 1 (Scenario B): Merge block-coded FRS records to spine, aggregate to BG.

    Records already have block_geoid from the Census Block Code field.
    """
    logger.info("=== Path 1: Block-coded records (Scenario B) ===")
    logger.info("Input: %d records with valid Census Block Code", len(block_df))

    # Normalize block_geoid
    block_df = block_df.copy()
    block_df["block_geoid"] = (
        block_df["block_geoid"].astype(str).str.zfill(BLOCK_GEOID_LEN)
    )

    # Merge with spine to get full hierarchy
    spine_cols = ["block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
                  "state_fips", "msa_geoid"]
    merged = block_df.merge(
        spine_df[spine_cols],
        on="block_geoid",
        how="left",
    )

    match_rate = merged["bg_geoid"].notna().mean()
    logger.info("Block-to-spine match rate: %.1f%% (%d/%d)",
                match_rate * 100, merged["bg_geoid"].notna().sum(), len(merged))

    # Drop unmatched (stale block codes not in 2020 spine)
    merged = merged[merged["bg_geoid"].notna()].copy()

    # Aggregate to BG using FRS-specific rules
    bg_result = aggregate_frs_to_blockgroup(merged)
    logger.info("Path 1 result: %d block groups", len(bg_result))
    return bg_result


def _ingest_point_path(
    point_df: pd.DataFrame,
    spine_df: pd.DataFrame,
    block_polygons: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Path 2 (Scenario A): Spatial join point FRS records, aggregate to BG."""
    logger.info("=== Path 2: Point-only records (Scenario A) ===")
    logger.info("Input: %d records with lat/lon only", len(point_df))

    # Create GeoDataFrame from lat/lon
    point_gdf = gpd.GeoDataFrame(
        point_df,
        geometry=gpd.points_from_xy(point_df["longitude"], point_df["latitude"]),
        crs="EPSG:4326",
    )

    # Spatial join to blocks
    with timed("FRS point-in-polygon spatial join"):
        points_with_block = point_in_polygon(
            point_gdf, block_polygons, "block_geoid"
        )

    match_rate = points_with_block["block_geoid"].notna().mean()
    logger.info("Point-to-block match rate: %.1f%%", match_rate * 100)

    # Merge with spine
    spine_cols = ["block_geoid", "bg_geoid", "tract_geoid", "county_geoid",
                  "state_fips", "msa_geoid"]
    merged = points_with_block.merge(
        spine_df[spine_cols],
        on="block_geoid",
        how="left",
    )
    merged = merged[merged["bg_geoid"].notna()].copy()

    # Aggregate to BG
    bg_result = aggregate_frs_to_blockgroup(pd.DataFrame(merged))
    logger.info("Path 2 result: %d block groups", len(bg_result))
    return bg_result


def _combine_paths(
    block_bg: pd.DataFrame,
    point_bg: pd.DataFrame,
) -> pd.DataFrame:
    """Combine block-path and point-path BG aggregations.

    Some BGs may have facilities from both paths. Re-aggregate to ensure
    each bg_geoid appears exactly once.
    """
    logger.info("=== Combining Path 1 (%d BGs) + Path 2 (%d BGs) ===",
                len(block_bg), len(point_bg))

    combined = pd.concat([block_bg, point_bg], ignore_index=True)

    if combined["bg_geoid"].is_unique:
        logger.info("No overlapping BGs — concat is sufficient")
        return combined

    # Re-aggregate overlapping BGs
    logger.info("Re-aggregating %d overlapping BGs",
                combined["bg_geoid"].duplicated().sum())

    # Numeric columns: sum for counts, max for flags
    count_cols = [c for c in combined.columns if c.endswith("_count")]
    flag_cols = [c for c in combined.columns if c.startswith("frs_has_")]
    other_numeric = ["frs_avg_accuracy", "frs_max_program_count", "frs_site_type_diversity"]

    agg_dict = {}
    for col in count_cols:
        if col in combined.columns:
            agg_dict[col] = "sum"
    for col in flag_cols:
        if col in combined.columns:
            agg_dict[col] = "max"
    if "frs_avg_accuracy" in combined.columns:
        agg_dict["frs_avg_accuracy"] = "mean"
    if "frs_max_program_count" in combined.columns:
        agg_dict["frs_max_program_count"] = "max"
    if "frs_site_type_diversity" in combined.columns:
        agg_dict["frs_site_type_diversity"] = "max"

    result = combined.groupby("bg_geoid").agg(agg_dict).reset_index()

    # Add hierarchy back from first occurrence
    hierarchy_cols = ["bg_geoid", "tract_geoid", "county_geoid", "state_fips", "msa_geoid"]
    available_hierarchy = [c for c in hierarchy_cols if c in combined.columns]
    if len(available_hierarchy) > 1:
        hierarchy = combined[available_hierarchy].drop_duplicates(subset=["bg_geoid"])
        result = result.merge(hierarchy, on="bg_geoid", how="left")

    logger.info("Combined result: %d unique block groups", len(result))
    return result


@click.command()
@click.option("--csv", type=click.Path(exists=True), default=None,
              help="Path to pre-downloaded FRS CSV. If not provided, downloads from EPA.")
@click.option("--data-root", type=click.Path(), default=None,
              help="Data root directory (default: $DATA_ROOT or ./data)")
@click.option("--date-tag", type=str, default=None,
              help="Date tag for output files (default: today)")
@click.option("--skip-download", is_flag=True, default=False,
              help="Skip download step (requires --csv or pre-downloaded file)")
@click.option("--skip-point-path", is_flag=True, default=False,
              help="Skip Scenario A point path (only use block-coded records)")
def main(csv, data_root, date_tag, skip_download, skip_point_path):
    """Ingest EPA FRS facility data to block group grain via hybrid strategy."""
    load_dotenv()
    setup_logging()

    data_root = Path(data_root) if data_root else get_data_root()
    date_tag = date_tag or get_date_tag()

    logger.info("=" * 60)
    logger.info("EPA FRS Hybrid Ingest Pipeline")
    logger.info("=" * 60)

    # Step 1: Acquire CSV
    with timed("Step 1: Acquire FRS data"):
        if csv:
            csv_path = Path(csv)
        elif skip_download:
            csv_path = data_root / "raw" / "epa_frs" / "NATIONAL_SINGLE.CSV"
            if not csv_path.exists():
                logger.error("CSV not found at %s. Provide --csv or remove --skip-download", csv_path)
                sys.exit(1)
        else:
            csv_path = download_frs_national(data_root / "raw" / "epa_frs")

    # Step 2: Parse
    with timed("Step 2: Parse FRS CSV"):
        raw_df = parse_frs_single_file(csv_path)

    # Step 3: Normalize and split
    with timed("Step 3: Normalize and split records"):
        split = normalize_frs(raw_df)
        block_df = split["block_df"]
        point_df = split["point_df"]
        stats = split["stats"]

    # Step 4: Load spine
    with timed("Step 4: Load master spine"):
        spine_df = _load_spine(data_root)

    # Step 5: Path 1 — Block-coded records (Scenario B)
    block_bg = pd.DataFrame()
    if len(block_df) > 0:
        with timed("Step 5: Path 1 — Block-coded ingest"):
            block_bg = _ingest_block_path(block_df, spine_df)
    else:
        logger.warning("No block-coded records found. Skipping Path 1.")

    # Step 6: Path 2 — Point-only records (Scenario A)
    point_bg = pd.DataFrame()
    if len(point_df) > 0 and not skip_point_path:
        with timed("Step 6: Path 2 — Point spatial ingest"):
            block_polygons = _load_block_polygons(data_root)
            point_bg = _ingest_point_path(point_df, spine_df, block_polygons)
    elif skip_point_path:
        logger.info("Skipping Path 2 (--skip-point-path flag)")
    else:
        logger.warning("No point-only records found. Skipping Path 2.")

    # Step 7: Combine paths
    with timed("Step 7: Combine paths"):
        if len(block_bg) > 0 and len(point_bg) > 0:
            result = _combine_paths(block_bg, point_bg)
        elif len(block_bg) > 0:
            result = block_bg
        elif len(point_bg) > 0:
            result = point_bg
        else:
            logger.error("No records processed. Both paths empty.")
            sys.exit(1)

    # Step 8: Add provenance
    result = add_provenance_columns(
        result, "epa_frs", "2024",
        row_count_pre_agg=stats["total_records"],
    )

    # Step 9: Validate
    with timed("Step 9: Validate output"):
        validation = validate_ingest_output(result, "epa_frs")

    # Step 10: Write
    with timed("Step 10: Write output"):
        output_path = ingest_output_path(data_root, date_tag, "epa_frs")
        write_parquet(result, output_path)

        # Write metadata
        metadata = {
            "source": "epa_frs",
            "date_tag": date_tag,
            "pipeline": "hybrid_block_point",
            "stats": stats,
            "validation": validation,
            "output_path": str(output_path),
            "output_rows": len(result),
        }
        meta_path = metadata_output_path(data_root, date_tag, "epa_frs")
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

    logger.info("=" * 60)
    logger.info("EPA FRS ingest complete")
    logger.info("  Total input records: %d", stats["total_records"])
    logger.info("  Block-coded path: %d records -> %d BGs",
                stats["block_coded_count"], len(block_bg) if len(block_bg) > 0 else 0)
    logger.info("  Point path: %d records -> %d BGs",
                stats["point_only_count"], len(point_bg) if len(point_bg) > 0 else 0)
    logger.info("  Output: %d unique block groups -> %s", len(result), output_path)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
