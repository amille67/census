"""Address parsing utilities for point sources without coordinates."""

from backend.utils.logging import get_logger

logger = get_logger("adapters.points.address_parser")


def parse_address_components(address: str) -> dict:
    """Basic address component extraction.

    For production use, consider integrating a proper address parser
    like usaddress or libpostal.
    """
    parts = [p.strip() for p in address.split(",")]
    result = {"raw_address": address}

    if len(parts) >= 3:
        result["street"] = parts[0]
        result["city"] = parts[1]
        state_zip = parts[2].strip().split()
        if state_zip:
            result["state"] = state_zip[0]
        if len(state_zip) > 1:
            result["zip"] = state_zip[1]

    return result
