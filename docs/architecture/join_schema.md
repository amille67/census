# Join Schema

## Critical Design: Two Spine Artifacts

### A) `master_spine_crosswalk.parquet` (block-grain)
- **PK:** `block_geoid`
- **Use for:** Block-level and point-based sources
- **Cardinality:** Many blocks per BG

### B) `master_spine_bg_index.parquet` (BG-grain)
- **PK:** `bg_geoid`
- **Use for:** Native BG sources (ACS, NLCD zonal stats)
- **Derived from:** A, via aggregation

**CRITICAL:** Never join a BG-grain table directly to the block-grain spine. This causes row duplication.

## Join Contracts

All join contracts are defined in `backend/joins/contracts.py` with these properties:
- `name` — unique contract identifier
- `left_dataset` / `right_dataset` — dataset names
- `join_type` — inner, left, right, spatial
- `keys` — join key columns
- `cardinality` — 1:1, m:1, 1:m, m:m
- `grain_before` / `grain_after` — expected grains
- `null_policy` — allow, fail, drop, warn

## Source-Specific Contracts

| Contract | Left | Right | Keys | Cardinality |
|----------|------|-------|------|-------------|
| `POINT_TO_BLOCK_SPATIAL` | points | block polygons | (spatial) | m:1 |
| `BLOCK_TO_SPINE` | block data | spine crosswalk | block_geoid | m:1 |
| `LODES_BLOCK_TO_SPINE` | LODES WAC | spine crosswalk | block_geoid | m:1 |
| `ACS_BG_TO_SPINE_BG_INDEX` | ACS data | BG index | bg_geoid | 1:1 |
| `RASTER_ZONAL_TO_BG` | zonal stats | BG index | bg_geoid | 1:1 |
| `ASSEMBLE_BG_FEATURE_TABLE_LEFT_JOIN` | BG index | any ingest | bg_geoid | 1:1 |

## Forbidden Joins

- BG table → block-grain spine (causes row multiplication)
- Non-2020 GEOIDs → spine (requires crosswalk first)
- Malformed GEOIDs (wrong length, non-digit)
