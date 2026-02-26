"""Tests for EPA FRS CSV parsing and normalization."""

import pandas as pd
import pytest

from backend.adapters.epa_frs.parse import parse_frs_single_file, FRS_COLUMNS, KEEP_COLUMNS


def make_frs_csv(tmp_path, n_rows=20, include_block_code=True):
    """Create a synthetic FRS CSV for testing."""
    import numpy as np
    np.random.seed(42)

    rows = []
    for i in range(n_rows):
        state_fips = "01" if i < n_rows // 2 else "06"
        county_fips = f"{state_fips}001"
        has_block = include_block_code and (i % 3 != 0)  # 2/3 have block code
        block_code = f"{county_fips}020100{i % 4 + 1}{str(i).zfill(3)}" if has_block else ""

        row = {
            "REGISTRY_ID": f"110{str(i).zfill(9)}",
            "PRIMARY_NAME": f"Test Facility {i}",
            "LOCATION_ADDRESS": f"{100 + i} Main St",
            "SUPPLEMENTAL_LOCATION": "",
            "CITY_NAME": "Testville",
            "COUNTY_NAME": "Test County",
            "FIPS_CODE": county_fips,
            "STATE_CODE": "AL" if state_fips == "01" else "CA",
            "STATE_NAME": "ALABAMA" if state_fips == "01" else "CALIFORNIA",
            "COUNTRY_NAME": "US",
            "POSTAL_CODE": "12345",
            "FEDERAL_FACILITY_CODE": "Y" if i % 5 == 0 else "N",
            "TRIBAL_LAND_CODE": "Y" if i % 10 == 0 else "N",
            "CONGRESSIONAL_DIST_NUM": "01",
            "CENSUS_TRACT_CODE": "020100",
            "HUC_CODE": "0301010101",
            "EPA_REGION_CODE": "04",
            "SITE_TYPE_NAME": ["Stationary", "Mobile", "Federal"][i % 3],
            "LOCATION_DESCRIPTION": "",
            "CREATE_DATE": "01/15/2020",
            "UPDATE_DATE": "06/01/2024",
            "US_MEXICO_BORDER_IND": "N",
            "PGM_SYS_ACRNMS": ["RCRA|AIR", "NPDES|TRI", "RCRA", "SEMS|AIR"][i % 4],
            "LATITUDE83": round(32.0 + np.random.uniform(-1, 1), 6),
            "LONGITUDE83": round(-86.0 + np.random.uniform(-1, 1), 6),
            "COLLECT_DESC": "Address Matching",
            "ACCURACY_VALUE": round(np.random.uniform(0.1, 100.0), 2),
            "REF_POINT_DESC": "Center of facility",
            "HDATUM_DESC": "NAD83",
            "SOURCE_DESC": "EPA",
            "CENSUS_BLOCK_CODE": block_code,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    csv_path = tmp_path / "NATIONAL_SINGLE.CSV"
    df.to_csv(csv_path, index=False)
    return csv_path


class TestFRSParse:

    def test_parse_reads_all_rows(self, tmp_path):
        csv_path = make_frs_csv(tmp_path, n_rows=15)
        df = parse_frs_single_file(csv_path)
        assert len(df) == 15

    def test_parse_renames_columns(self, tmp_path):
        csv_path = make_frs_csv(tmp_path)
        df = parse_frs_single_file(csv_path)
        assert "registry_id" in df.columns
        assert "latitude" in df.columns
        assert "longitude" in df.columns
        assert "state_code" in df.columns

    def test_parse_preserves_block_code(self, tmp_path):
        csv_path = make_frs_csv(tmp_path)
        df = parse_frs_single_file(csv_path)
        assert "census_block_code" in df.columns
        # Some should have valid 15-digit codes
        valid = df["census_block_code"].fillna("").str.match(r"^\d{15}$")
        assert valid.sum() > 0

    def test_parse_preserves_fips_leading_zeros(self, tmp_path):
        csv_path = make_frs_csv(tmp_path)
        df = parse_frs_single_file(csv_path)
        assert "fips_code" in df.columns
        # Alabama FIPS starts with "01"
        al_fips = df[df["state_code"] == "AL"]["fips_code"]
        assert all(f.startswith("01") for f in al_fips if pd.notna(f))

    def test_parse_includes_programs(self, tmp_path):
        csv_path = make_frs_csv(tmp_path)
        df = parse_frs_single_file(csv_path)
        assert "pgm_sys_acrnms" in df.columns
        assert df["pgm_sys_acrnms"].notna().sum() > 0
