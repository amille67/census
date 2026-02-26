"""Assemble Master Blockgroup Table.

Loads BG spine index as base, then left-joins all ingest outputs
by bg_geoid to produce the final master_blockgroup table.

Join order (recommended):
  1. spine BG index (base - complete geographic coverage)
  2. ACS demographic/economic tables
  3. LODES WAC employment metrics
  4. Land cover / raster stats
  5. Point-source aggregates
  6. Derived density features

Usage:
  python -m backend.scripts.assemble_master_blockgroup
  python -m backend.scripts.assemble_master_blockgroup --date-tag 2026-02-25
"""

import sys
from pathlib import Path

import click
import pandas as pd

from backend.utils.logging import setup_logging, get_logger
from backend.utils.env import load_dotenv, get_data_root
from backend.utils.timing import timed
from backend.io.parquet import read_parquet, write_parquet
from backend.io.json import write_json
from backend.io.filenames import get_date_tag, assembled_output_path
from backend.joins.contracts import execute_join
from backend.joins.assembly_joins import ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN
from backend.transforms.aggregations.weighted_density import compute_density

logger = get_logger("scripts.assemble_master_blockgroup")


@click.command()
@click.option("--data-root", type=click.Path(), default=None, help="Data root directory")
@click.option("--date-tag", type=str, default=None, help="Ingest date tag to assemble")
@click.option("--output", type=click.Path(), default=None, help="Custom output path")
def main(data_root, date_tag, output):
    """Assemble master blockgroup table from all ingest outputs."""
    load_dotenv()
    setup_logging()

    data_root = Path(data_root) if data_root else get_data_root()
    date_tag = date_tag or get_date_tag()

    # Load BG spine index as base
    bg_index_path = data_root / "processed" / "spine" / "master_spine_bg_index.parquet"
    if not bg_index_path.exists():
        logger.error("BG index not found at %s. Run build_master_spine first.", bg_index_path)
        sys.exit(1)

    with timed("Load BG spine index"):
        master = read_parquet(bg_index_path)
        logger.info("Base BG index: %d block groups", len(master))

    # Find all ingest outputs for this date tag
    ingest_dir = data_root / "processed" / "ingests" / date_tag
    if not ingest_dir.exists():
        logger.error("No ingest directory found for date %s at %s", date_tag, ingest_dir)
        sys.exit(1)

    ingest_files = sorted(ingest_dir.glob("*_blockgroup.parquet"))
    logger.info("Found %d ingest files to assemble", len(ingest_files))

    # Left-join each ingest output
    join_log = []
    for ingest_file in ingest_files:
        source_name = ingest_file.stem.replace("_blockgroup", "")

        with timed(f"Join {source_name}"):
            ingest_df = read_parquet(ingest_file)

            # Drop provenance and hierarchy columns (spine provides these)
            drop_cols = [c for c in ingest_df.columns
                         if c.startswith("_") or c in ("tract_geoid", "county_geoid",
                                                        "state_fips", "msa_geoid",
                                                        "mega_region_id")]
            ingest_clean = ingest_df.drop(columns=drop_cols, errors="ignore")

            # Prefix columns to avoid collisions (except bg_geoid)
            rename_map = {
                c: f"{source_name}__{c}"
                for c in ingest_clean.columns
                if c != "bg_geoid"
            }
            ingest_clean = ingest_clean.rename(columns=rename_map)

            before_count = len(master)
            master = execute_join(
                ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN,
                master,
                ingest_clean,
                validate=True,
            )

            coverage = ingest_clean["bg_geoid"].isin(master["bg_geoid"]).mean()
            join_log.append({
                "source": source_name,
                "ingest_rows": len(ingest_df),
                "columns_added": len(rename_map),
                "coverage_rate": round(coverage, 4),
            })

    # Compute density features if area denominator available
    if "net_developable_area_sq_m" in master.columns:
        with timed("Compute density features"):
            for col in master.columns:
                if "total_jobs" in col or "total_population" in col:
                    density_col = col.replace("total_", "") + "_density_per_sq_km"
                    master = compute_density(
                        master, col, "net_developable_area_sq_m",
                        output_col=density_col,
                    )

    # Write assembled output
    if output:
        output_path = Path(output)
    else:
        output_path = assembled_output_path(data_root, date_tag)

    with timed("Write master blockgroup"):
        write_parquet(master, output_path)

    # Write validation report
    validation_report = {
        "date_tag": date_tag,
        "total_block_groups": len(master),
        "total_columns": len(master.columns),
        "join_log": join_log,
        "column_null_rates": {
            col: round(master[col].isna().mean(), 4)
            for col in master.columns
            if master[col].isna().any()
        },
    }

    report_path = output_path.parent / f"validation_report_{date_tag}.json"
    write_json(validation_report, report_path)

    # Write metadata
    metadata_path = output_path.with_suffix(".metadata.json")
    write_json({
        "date_tag": date_tag,
        "row_count": len(master),
        "column_count": len(master.columns),
        "sources_assembled": [j["source"] for j in join_log],
    }, metadata_path)

    # Also write to marts/
    marts_dir = data_root / "processed" / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(master, marts_dir / "bg_feature_matrix_latest.parquet")

    logger.info("Assembly complete: %d BGs x %d columns -> %s",
                len(master), len(master.columns), output_path)


if __name__ == "__main__":
    main()
