# Data Lineage

## Pipeline Flow

```
Raw Sources → Adapters → Staging → Transforms → Processed
     │              │         │          │            │
     │              │         │          │            ├── spine/
     │              │         │          │            ├── ingests/{date}/
     │              │         │          │            ├── assembled/
     │              │         │          │            └── marts/
     │              │         │          │
     │              │         │          ├── spine/ (build hierarchy, overlays)
     │              │         │          └── ingest/ (dispatch A/B/C/D)
     │              │         │
     │              │         ├── spatial/ (normalized parquet)
     │              │         ├── lodes/ (WAC union, normalized)
     │              │         └── census_api/ (ACS normalized)
     │              │
     │              ├── census_parquet_bootstrap/
     │              ├── lodes/
     │              └── census_api/
     │
     ├── tiger2020/
     ├── lodes/LODES8/
     └── census_api/
```

## Provenance Tracking
Every ingest output includes:
- `_source_slug` — source identifier
- `_ingest_timestamp` — ISO 8601 timestamp
- `_source_vintage` — data year
- `_geography_vintage` — always 2020
- `_row_count_pre_agg` — rows before aggregation

## Metadata Manifests
Written to `data/processed/ingests/{date}/{source}_metadata.json`
