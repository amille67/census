# Source Manifest Contract

## Purpose
Each source manifest (YAML in `configs/source_registry/`) defines everything
needed for the LLM to generate a complete ingest pipeline from a source URL.

## Required Fields

### Source Identity
- `source.name` — Human-readable name
- `source.slug` — Machine name (snake_case)
- `source.url` — Download URL or API endpoint

### Acquisition
- `acquisition.method` — http_download | api_fetch | local_file
- `acquisition.format` — csv | shapefile | geojson | raster_tif | api_json
- `acquisition.compression` — gzip | zip | null

### Scenario
- `scenario` — point | native_block | native_bg | raster_polygon

### Schema Mapping
For point sources: `lat_column`, `lon_column`
For native_block: `block_geoid_column`
For native_bg: `state_column`, `county_column`, `tract_column`, `blockgroup_column`
For raster: `band`, `nodata_value`

### Vintage
- `vintage.geography_year` — Must be 2020 or `requires_crosswalk: true`
- `vintage.data_year` — Year of actual data

### Aggregation Rules
- Column name → aggregation method (sum, mean, count, count_distinct, etc.)

### QA Thresholds
- `qa.min_match_rate` — Minimum fraction of rows matched to spine
- `qa.min_coverage_rate` — Minimum fraction of BGs with data
- `qa.max_null_rate` — Maximum null rate in output columns
