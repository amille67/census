# Build 1 Scope

## Goal
Build the core ETL pipeline that maps arbitrary data sources to a rigid 2020 Census master spine at the block group grain.

## Core Deliverables

### 1. Master Spine Crosswalk
- **File:** `data/processed/spine/master_spine_crosswalk.parquet`
- **Grain:** One row per 2020 Census block (~8.3M rows)
- **Primary Key:** `block_geoid` (15-digit string)

### 2. BG-Grain Index
- **File:** `data/processed/spine/master_spine_bg_index.parquet`
- **Grain:** One row per 2020 Census block group (~240K rows)
- **Primary Key:** `bg_geoid` (12-digit string)

### 3. Universal Ingest Pipeline
- Routes any source through 4 scenario paths (A/B/C/D)
- Outputs standardized `*_blockgroup.parquet` files

### 4. Master Blockgroup Assembly
- Left-joins all ingest outputs onto BG spine index
- Produces final feature matrix

## Scenarios
| Scenario | Input Grain | Example Source | Join Path |
|----------|------------|---------------|-----------|
| A (Point) | lat/lon points | EPA FRS | Point → Block polygon → Spine → BG |
| B (Native Block) | block_geoid | LODES WAC | Block → Spine → BG aggregation |
| C (Raster/Polygon) | raster pixels | NLCD | Zonal stats against BG polygons |
| D (Native BG) | bg_geoid | ACS 5yr | BG → BG spine index |

## Source Repos Adapted
1. **makepath/census-parquet** → `backend/adapters/census_parquet_bootstrap/`
2. **UrbanInstitute/lodes-data-downloads** → `backend/adapters/lodes/`
3. **npappin-wsu/census** → Used as dependency via `backend/adapters/census_api/`
