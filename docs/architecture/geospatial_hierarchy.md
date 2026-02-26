# Geospatial Hierarchy

## Canonical Grains (finest to coarsest)

1. **Point** — raw input row (geocoded or lat/lon)
2. **Block** — `block_geoid`, 15-digit, 2020 Census
3. **Block Group** — `bg_geoid`, 12-digit, 2020 ← **core feature grain**
4. **Tract** — `tract_geoid`, 11-digit
5. **County** — `county_geoid`, 5-digit
6. **MSA** — `msa_geoid`, CBSA-based via BG centroid spatial join
7. **Mega Region** — `mega_region_id`, custom polygons via BG centroid

## Key Derivation (Zero Spatial Cost)

All derived from `block_geoid` via string slicing:

```
block_geoid = "010010201001000"  (15 digits)
bg_geoid    = block_geoid[:12]   → "010010201001"
tract_geoid = block_geoid[:11]   → "01001020100"
county_geoid= block_geoid[:5]    → "01001"
state_fips  = block_geoid[:2]    → "01"
```

Implementation: `backend/geo/geoid.py`

## Why Block-Grain Spine

The spine is at block grain (not BG) because:
- LODES WAC lands at block (`w_geocode`)
- Point sources spatially join to blocks first
- Maximum fidelity preserved; single join path
- BG index is a derived artifact for safe BG-level joins
