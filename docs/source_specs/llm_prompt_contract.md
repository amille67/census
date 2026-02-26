# LLM Prompt Contract

## Purpose
This document defines the contract for LLM-generated ingest pipelines.
When an LLM is given a source URL and this contract, it should be able to
generate a correct, standardized ingest pipeline.

## LLM Input
1. Source URL or data description
2. Source manifest YAML (from `configs/source_registry/`)
3. This contract document

## LLM Output
A Python script or function that:
1. Downloads/reads the source data
2. Identifies the scenario (point/native_block/native_bg/raster_polygon)
3. Maps source columns to canonical GEOID keys
4. Calls the appropriate ingest function from `backend/transforms/ingest/`
5. Uses the correct join contract from `backend/joins/`
6. Outputs a standardized `*_blockgroup.parquet`

## Critical Rules for LLM

### 1. Never join BG data to block-grain spine
Use `master_spine_bg_index.parquet` for BG-native sources.

### 2. Always enforce 2020 vintage
Call `enforce_vintage()` before any spine merge.

### 3. Use string GEOIDs, not integers
All GEOIDs must be zero-padded strings: `"01001"` not `1001`.

### 4. Use the correct join contract
Reference `backend/joins/*.py` for the exact join specification.

### 5. Add provenance columns
Call `add_provenance_columns()` on every output.

### 6. Validate output
Call `validate_ingest_output()` before writing.

## Example Generated Code Pattern

```python
from backend.transforms.ingest.dispatch import dispatch_scenario
from backend.transforms.ingest.vintage_enforcement import enforce_vintage
from backend.transforms.common.provenance import add_provenance_columns
from backend.io.parquet import read_parquet, write_parquet

# 1. Enforce vintage
enforce_vintage(geography_year=2020, source_slug="my_source")

# 2. Load and normalize source data
df = load_my_source(...)

# 3. Dispatch to correct ingest path
ingest_fn = dispatch_scenario("native_block")
result = ingest_fn(df, spine_df, ...)

# 4. Add provenance
result = add_provenance_columns(result, "my_source", "2024")

# 5. Write output
write_parquet(result, output_path)
```
