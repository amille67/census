"""Tests for LODES filename parsing."""

import pytest

from backend.adapters.lodes.parse_filenames import parse_lodes_filename, LODESFileInfo


class TestLODESFilenameParsing:

    def test_parse_standard_wac_filename(self):
        info = parse_lodes_filename("al_wac_S000_JT00_2021.csv")
        assert info is not None
        assert info.state == "al"
        assert info.data_type == "wac"
        assert info.segment == "S000"
        assert info.job_type == "JT00"
        assert info.year == 2021
        assert info.is_wac is True

    def test_parse_rac_filename(self):
        info = parse_lodes_filename("ca_rac_SA01_JT02_2020.csv")
        assert info is not None
        assert info.state == "ca"
        assert info.data_type == "rac"
        assert info.segment == "SA01"
        assert info.job_type == "JT02"
        assert info.year == 2020
        assert info.is_wac is False

    def test_parse_full_path_extracts_filename(self):
        info = parse_lodes_filename("/data/raw/lodes/ny_wac_S000_JT00_2021.csv")
        assert info is not None
        assert info.state == "ny"

    def test_invalid_filename_returns_none(self):
        assert parse_lodes_filename("not_a_lodes_file.csv") is None
        assert parse_lodes_filename("") is None
        assert parse_lodes_filename("al_wac.csv") is None

    def test_all_segments_parse(self):
        segments = ["S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03",
                     "SI01", "SI02", "SI03"]
        for seg in segments:
            info = parse_lodes_filename(f"al_wac_{seg}_JT00_2021.csv")
            assert info is not None, f"Failed to parse segment {seg}"
            assert info.segment == seg

    def test_all_job_types_parse(self):
        for jt in ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"]:
            info = parse_lodes_filename(f"al_wac_S000_{jt}_2021.csv")
            assert info is not None
            assert info.job_type == jt
