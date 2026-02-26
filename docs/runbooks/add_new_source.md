# Adding a New Data Source

## Step 1: Create Source Registry YAML

Copy the template and fill in your source details:

```bash
cp configs/source_registry/template_source.yaml configs/source_registry/my_source.yaml
```

Key fields to set:
- `source.slug` — machine name (used in filenames)
- `scenario` — one of: point, native_block, native_bg, raster_polygon
- `schema_mapping` — how to find geographic keys in your data
- `aggregation_rules` — how to aggregate to block group

## Step 2: Determine Scenario

| Your data has... | Scenario | Example |
|-----------------|----------|---------|
| Lat/lon coordinates | `point` | EPA FRS facilities |
| 15-digit block GEOID | `native_block` | LODES WAC |
| State+county+tract+BG | `native_bg` | ACS Census API |
| Raster pixels or polygons | `raster_polygon` | NLCD land cover |

## Step 3: Run Ingest

```bash
python -m backend.scripts.universal_ingest --config configs/source_registry/my_source.yaml
```

## Step 4: Re-assemble

```bash
python -m backend.scripts.assemble_master_blockgroup
```

## Join Contract Reference

Your new source will use one of these join contracts:
- **Point sources** → `POINT_TO_BLOCK_SPATIAL` then `BLOCK_TO_SPINE`
- **Block sources** → `BLOCK_TO_SPINE`
- **BG sources** → `BG_TO_SPINE_BG_INDEX` (uses BG index, not block spine!)
- **Raster sources** → `RASTER_ZONAL_TO_BG`
