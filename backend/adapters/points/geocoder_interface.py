"""Geocoder interface for point sources missing lat/lon.

Provides a pluggable interface for geocoding addresses to coordinates.
"""

from abc import ABC, abstractmethod
from typing import Optional, Tuple

from backend.utils.logging import get_logger

logger = get_logger("adapters.points.geocoder")


class GeocoderInterface(ABC):
    """Abstract base class for geocoding providers."""

    @abstractmethod
    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """Geocode an address to (latitude, longitude)."""
        pass


class CensusBatchGeocoder(GeocoderInterface):
    """Census Bureau batch geocoding API."""

    ENDPOINT = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        import requests

        params = {
            "address": address,
            "benchmark": "Public_AR_Current",
            "format": "json",
        }
        try:
            resp = requests.get(self.ENDPOINT, params=params, timeout=10)
            resp.raise_for_status()
            matches = resp.json().get("result", {}).get("addressMatches", [])
            if matches:
                coords = matches[0]["coordinates"]
                return (coords["y"], coords["x"])
        except Exception as e:
            logger.warning("Geocoding failed for '%s': %s", address, e)
        return None
