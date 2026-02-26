"""ACS Census data models and variable catalogs."""

# Common ACS 5-Year variable groups for Build 1
ACS5_POPULATION_VARS = [
    "B01001_001E",  # Total population
    "B01001_002E",  # Male
    "B01001_026E",  # Female
]

ACS5_INCOME_VARS = [
    "B19013_001E",  # Median household income
    "B19001_001E",  # Total households by income bracket
]

ACS5_HOUSING_VARS = [
    "B25001_001E",  # Total housing units
    "B25002_002E",  # Occupied
    "B25002_003E",  # Vacant
    "B25077_001E",  # Median home value
]

ACS5_EDUCATION_VARS = [
    "B15003_001E",  # Total population 25+
    "B15003_022E",  # Bachelor's degree
    "B15003_023E",  # Master's degree
    "B15003_025E",  # Doctorate degree
]

ACS5_BUILD1_VARS = (
    ACS5_POPULATION_VARS + ACS5_INCOME_VARS +
    ACS5_HOUSING_VARS + ACS5_EDUCATION_VARS
)

# Human-readable names for ACS variables
ACS_VARIABLE_LABELS = {
    "B01001_001E": "total_population",
    "B01001_002E": "male_population",
    "B01001_026E": "female_population",
    "B19013_001E": "median_household_income",
    "B19001_001E": "total_households_by_income",
    "B25001_001E": "total_housing_units",
    "B25002_002E": "occupied_housing_units",
    "B25002_003E": "vacant_housing_units",
    "B25077_001E": "median_home_value",
    "B15003_001E": "pop_25_plus",
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_025E": "doctorate_degree",
}
