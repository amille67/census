"""Universal Ingest Script.

Ingests any source via scenario dispatch:
  A) point -> block -> blockgroup
  B) native_block -> blockgroup (LODES WAC)
  C) raster_polygon -> blockgroup (NLCD, PAD-US)
  D) native_bg -> blockgroup (ACS via Census API)

Usage:
  python -m backend.scripts.universal_ingest --source lodes_wac --scenario native_block
  python -m backend.scripts.universal_ingest --source acs_5yr_2023 --scenario native_bg
  python -m backend.scripts.universal_ingest --config configs/source_registry/epa_frs.yaml
"""

import sys
from pathlib import Path

import click
import pandas as pd
import yaml

from backend.utils.logging import setup_logging, get_logger
from backend.utils.env import load_dotenv, get_data_root
from backend.utils.timing import timed
from backend.transforms.ingest.dispatch import dispatch_scenario
from backend.transforms.ingest.vintage_enforcement import enforce_vintage
from backend.transforms.ingest.validate_ingest_output import validate_ingest_output
from backend.transforms.common.provenance import add_provenance_columns
from backend.transforms.common.nulls import log_null_rates
from backend.io.parquet import read_parquet, write_parquet
from backend.io.filenames import get_date_tag, ingest_output_path, metadata_output_path
from backend.models.metadata import IngestMetadata

logger = get_logger("scripts.universal_ingest")


def load_source_config(config_path: str) -> dict:
    """Load a source registry YAML config."""
    with open(config_path) as f:
        return yaml.safe_load(f)


@click.command()
@click.option("--source", type=str, default=None, help="Source slug (e.g., lodes_wac)")
@click.option("--scenario", type=str, default=None, help="Ingest scenario")
@click.option("--config", type=click.Path(exists=True), default=None, help="Source config YAML")
@click.option("--data-root", type=click.Path(), default=None, help="Data root directory")
@click.option("--date-tag", type=str, default=None, help="Date tag for output (default: today)")
def main(source, scenario, config, data_root, date_tag):
    """Run universal ingest for a data source."""
    load_dotenv()
    setup_logging()

    data_root = Path(data_root) if data_root else get_data_root()
    date_tag = date_tag or get_date_tag()

    # Load source config
    if config:
        source_config = load_source_config(config)
        source = source_config["source"]["slug"]
        scenario = source_config["scenario"]
    elif not source or not scenario:
        logger.error("Must provide either --config or both --source and --scenario")
        sys.exit(1)

    logger.info("Universal ingest: source=%s, scenario=%s, date=%s", source, scenario, date_tag)

    # Load spine
    spine_path = data_root / "processed" / "spine" / "master_spine_crosswalk.parquet"
    bg_index_path = data_root / "processed" / "spine" / "master_spine_bg_index.parquet"

    if not spine_path.exists():
        logger.error("Spine not found at %s. Run build_master_spine first.", spine_path)
        sys.exit(1)

    # Dispatch by scenario
    ingest_fn = dispatch_scenario(scenario)

    if scenario == "native_block" and source == "lodes_wac":
        result_df = _ingest_lodes_wac(data_root, spine_path)
    elif scenario == "native_bg" and source == "acs_5yr_2023":
        result_df = _ingest_acs_bg(data_root, bg_index_path)
    else:
        logger.info("Generic ingest for source=%s, scenario=%s", source, scenario)
        result_df = _ingest_generic(data_root, source, scenario, spine_path, bg_index_path)

    if result_df is None or result_df.empty:
        logger.error("Ingest produced no results for %s", source)
        sys.exit(1)

    # Add provenance
    result_df = add_provenance_columns(
        result_df, source_slug=source, source_vintage=date_tag,
        row_count_pre_agg=len(result_df),
    )

    # Validate
    validation = validate_ingest_output(result_df, source)

    # Write output
    output_path = ingest_output_path(data_root, date_tag, source)
    write_parquet(result_df, output_path)

    # Write metadata
    null_rates = log_null_rates(result_df)
    total_bgs = len(read_parquet(bg_index_path, columns=["bg_geoid"]))
    coverage = len(result_df) / total_bgs if total_bgs > 0 else 0

    metadata = IngestMetadata(
        source_slug=source,
        source_version=date_tag,
        scenario=scenario,
        geography_vintage=2020,
        input_row_count=len(result_df),
        output_row_count=len(result_df),
        match_rate=1.0,
        coverage_rate=coverage,
        null_rates=null_rates,
        output_file=str(output_path),
        spine_version="",
        qa_passed=validation["passed"],
    )
    meta_path = metadata_output_path(data_root, date_tag, source)
    metadata.write(meta_path)

    logger.info("Ingest complete: %s -> %s (%d rows)", source, output_path, len(result_df))


def _ingest_lodes_wac(data_root: Path, spine_path: Path) -> pd.DataFrame:
    """LODES WAC specific ingest (Scenario B)."""
    from backend.adapters.lodes.normalize_wac import normalize_wac_directory
    from backend.transforms.ingest.native_block_to_blockgroup import ingest_native_block
    from backend.transforms.features.lodes_metrics import add_lodes_metrics

    csv_dir = data_root / "raw" / "lodes" / "LODES8" / "wac" / "extracted_csv"
    if not csv_dir.exists() or not list(csv_dir.glob("*.csv")):
        logger.error("No LODES WAC CSVs found in %s", csv_dir)
        return pd.DataFrame()

    with timed("Normalize LODES WAC"):
        wac_df = normalize_wac_directory(csv_dir, segment="S000", job_type="JT00")

    spine_df = read_parquet(spine_path)

    with timed("Block to BG aggregation"):
        from backend.models.lodes import WAC_ALL_NUMERIC_COLUMNS
        agg_rules = {c: "sum" for c in WAC_ALL_NUMERIC_COLUMNS if c in wac_df.columns}
        bg_df = ingest_native_block(wac_df, spine_df, "block_geoid", agg_rules)

    with timed("Compute LODES metrics"):
        bg_df = add_lodes_metrics(bg_df, total_col="C000")

    return bg_df


def _ingest_acs_bg(data_root: Path, bg_index_path: Path) -> pd.DataFrame:
    """ACS 5yr specific ingest (Scenario D)."""
    from backend.adapters.census_api.normalize import normalize_acs_blockgroup
    from backend.transforms.ingest.native_bg_to_blockgroup import ingest_native_bg

    raw_dir = data_root / "raw" / "census_api" / "acs5_2023" / "normalized_csv"
    staging_path = data_root / "staging" / "census_api" / "acs_bg_normalized.parquet"

    if staging_path.exists():
        acs_df = read_parquet(staging_path)
    elif raw_dir.exists() and list(raw_dir.glob("*.csv")):
        frames = [pd.read_csv(f) for f in raw_dir.glob("*.csv")]
        acs_df = pd.concat(frames, ignore_index=True)
        acs_df = normalize_acs_blockgroup(acs_df)
    else:
        logger.warning("No ACS data found. Fetch via Census API first.")
        return pd.DataFrame()

    bg_index = read_parquet(bg_index_path)
    return ingest_native_bg(acs_df, bg_index)


def _ingest_generic(data_root, source, scenario, spine_path, bg_index_path):
    """Generic ingest placeholder for custom sources."""
    logger.info("Generic ingest not fully implemented for %s/%s", source, scenario)
    return pd.DataFrame()


if __name__ == "__main__":
    main()
