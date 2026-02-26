# Vintage Policy

## Rule
All spatial data entering the spine merge path must use **2020 Census geography**.

## 2020-Native Sources (No Crosswalk Needed)
- TIGER/Line 2020 shapefiles
- LODES 8 (uses 2020 Census blocks)
- ACS 5yr 2020+ (uses 2020 Census geography)
- PL 94-171 2020 redistricting data

## Sources Requiring Crosswalk
- Any data using 2010 Census geography (LODES 7, older ACS)
- NHGIS crosswalk files: `data/raw/crosswalks/nhgis_2010_to_2020_bg/`
- Method: Areal interpolation via `backend/geo/areal_interpolation.py`

## Enforcement
- `backend/transforms/ingest/vintage_enforcement.py` validates geography year
- Raises `VintageError` for non-2020 data without crosswalk flag
- Configuration: `configs/vintages.yaml`
