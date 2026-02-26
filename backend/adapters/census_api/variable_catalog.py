"""ACS variable catalog for the project.

Defines which ACS variables to pull and their human-readable names.
"""

from backend.models.census_acs import ACS5_BUILD1_VARS, ACS_VARIABLE_LABELS


def get_build1_variables() -> list:
    """Return the list of ACS variables needed for Build 1."""
    return ACS5_BUILD1_VARS


def get_variable_labels() -> dict:
    """Return a mapping of ACS variable codes to human-readable names."""
    return ACS_VARIABLE_LABELS.copy()
