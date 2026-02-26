"""Canonical key schemas for the universal spatial ingestor.

Each key type has a defined type, length, regex pattern, and vintage.
These schemas are the single source of truth for GEOID validation.
"""

from dataclasses import dataclass
from typing import Optional
import re


@dataclass(frozen=True)
class KeySchema:
    name: str
    dtype: str
    length: Optional[int]
    regex: str
    vintage: int = 2020
    nullable: bool = False

    def validate(self, value: str) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return self.nullable
        return bool(re.match(self.regex, str(value)))


BLOCK_GEOID_SCHEMA = KeySchema(
    name="block_geoid",
    dtype="string",
    length=15,
    regex=r"^\d{15}$",
)

BG_GEOID_SCHEMA = KeySchema(
    name="bg_geoid",
    dtype="string",
    length=12,
    regex=r"^\d{12}$",
)

TRACT_GEOID_SCHEMA = KeySchema(
    name="tract_geoid",
    dtype="string",
    length=11,
    regex=r"^\d{11}$",
)

COUNTY_GEOID_SCHEMA = KeySchema(
    name="county_geoid",
    dtype="string",
    length=5,
    regex=r"^\d{5}$",
)

STATE_FIPS_SCHEMA = KeySchema(
    name="state_fips",
    dtype="string",
    length=2,
    regex=r"^\d{2}$",
)

MSA_GEOID_SCHEMA = KeySchema(
    name="msa_geoid",
    dtype="string",
    length=5,
    regex=r"^\d{5}$",
    nullable=True,
)

MEGA_REGION_SCHEMA = KeySchema(
    name="mega_region_id",
    dtype="string",
    length=None,
    regex=r"^.+$",
    nullable=True,
)

ALL_KEY_SCHEMAS = {
    s.name: s for s in [
        BLOCK_GEOID_SCHEMA,
        BG_GEOID_SCHEMA,
        TRACT_GEOID_SCHEMA,
        COUNTY_GEOID_SCHEMA,
        STATE_FIPS_SCHEMA,
        MSA_GEOID_SCHEMA,
        MEGA_REGION_SCHEMA,
    ]
}
