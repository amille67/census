"""LODES data models and column definitions."""

# LODES WAC column groups
WAC_JOB_COLUMNS = ["C000"]

WAC_AGE_COLUMNS = ["CA01", "CA02", "CA03"]

WAC_EARNINGS_COLUMNS = ["CE01", "CE02", "CE03"]

WAC_NAICS_COLUMNS = [
    "CNS01", "CNS02", "CNS03", "CNS04", "CNS05",
    "CNS06", "CNS07", "CNS08", "CNS09", "CNS10",
    "CNS11", "CNS12", "CNS13", "CNS14", "CNS15",
    "CNS16", "CNS17", "CNS18", "CNS19", "CNS20",
]

WAC_RACE_COLUMNS = ["CR01", "CR02", "CR03", "CR04", "CR05", "CR06", "CR07"]

WAC_ETHNICITY_COLUMNS = ["CT01", "CT02"]

WAC_EDUCATION_COLUMNS = ["CD01", "CD02", "CD03", "CD04"]

WAC_SEX_COLUMNS = ["CS01", "CS02"]

WAC_ALL_NUMERIC_COLUMNS = (
    WAC_JOB_COLUMNS + WAC_AGE_COLUMNS + WAC_EARNINGS_COLUMNS +
    WAC_NAICS_COLUMNS + WAC_RACE_COLUMNS + WAC_ETHNICITY_COLUMNS +
    WAC_EDUCATION_COLUMNS + WAC_SEX_COLUMNS
)

# LODES filename parsing patterns
LODES_SEGMENTS = ["S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03",
                  "SI01", "SI02", "SI03"]

LODES_JOB_TYPES = ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"]

NAICS_SECTOR_NAMES = {
    "CNS01": "Agriculture",
    "CNS02": "Mining",
    "CNS03": "Utilities",
    "CNS04": "Construction",
    "CNS05": "Manufacturing",
    "CNS06": "Wholesale Trade",
    "CNS07": "Retail Trade",
    "CNS08": "Transportation",
    "CNS09": "Information",
    "CNS10": "Finance",
    "CNS11": "Real Estate",
    "CNS12": "Professional Services",
    "CNS13": "Management",
    "CNS14": "Admin/Waste",
    "CNS15": "Education",
    "CNS16": "Healthcare",
    "CNS17": "Arts/Entertainment",
    "CNS18": "Accommodation/Food",
    "CNS19": "Other Services",
    "CNS20": "Public Administration",
}
