# First Run Guide

## Prerequisites
1. Python 3.11+
2. Census API key (https://api.census.gov/data/key_signup.html)

## Setup

```bash
# Install dependencies
pip install -r requirements/base.txt

# Copy environment template
cp .env.example .env
# Edit .env and add your CENSUS_API_KEY
```

## Step 1: Build Master Spine

```bash
python -m backend.scripts.build_master_spine
```

This downloads TIGER 2020 shapefiles, processes blocks, builds hierarchy, and writes:
- `data/processed/spine/master_spine_crosswalk.parquet`
- `data/processed/spine/master_spine_bg_index.parquet`

Use `--skip-download` to reuse previously downloaded files.

## Step 2: Ingest Sources

```bash
# LODES WAC (Scenario B - native block)
python -m backend.scripts.universal_ingest --source lodes_wac --scenario native_block

# ACS 5yr (Scenario D - native BG)
python -m backend.scripts.universal_ingest --source acs_5yr_2023 --scenario native_bg
```

## Step 3: Assemble Master Table

```bash
python -m backend.scripts.assemble_master_blockgroup
```

Outputs: `data/processed/assembled/master_blockgroup_{date}.parquet`
