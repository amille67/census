"""Tests for vintage enforcement."""

import pytest

from backend.transforms.ingest.vintage_enforcement import enforce_vintage
from backend.utils.exceptions import VintageError


class TestVintageEnforcement:

    def test_2020_vintage_passes(self):
        # Should not raise
        enforce_vintage(2020, "lodes_wac")

    def test_non_2020_without_crosswalk_fails(self):
        with pytest.raises(VintageError, match="2010 geography"):
            enforce_vintage(2010, "old_source")

    def test_non_2020_with_crosswalk_passes(self):
        # Should not raise when crosswalk is enabled
        enforce_vintage(2010, "old_source", requires_crosswalk=True)
