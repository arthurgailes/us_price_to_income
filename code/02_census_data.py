"""
Census data downloaded from data.census.gov

Place median home value table: B25077
  - https://data.census.gov/table/ACSDT5Y2022.B25077?q=B25077%20place
  - Note: this is unused in favor of AEI AVM data

CBSA median income table: B19013
  - https://data.census.gov/table?q=B19013%20&g=010XX00US$3100000

"""

import pandas as pd
# Read the JSON data, then convert to read names
b19013_raw = pd.read_json("https://api.census.gov/data/2022/acs/acs5?get=group(B19013)&ucgid=pseudo(0100000US$3100000)")
b19013 = pd.DataFrame(b19013_raw.values[1:], columns=b19013_raw.iloc[0].str.lower())

b25077_raw = pd.read_json("https://api.census.gov/data/2022/acs/acs5?get=group(B25077)&ucgid=pseudo(0100000US$1600000)")
b25077 = pd.DataFrame(b25077_raw.values[1:], columns=b25077_raw.iloc[0].str.lower())

## CBSA Median Income
b19013["cbsa_2020_id"] = b19013["geo_id"].str[-5:]

cbsa_income = b19013[["cbsa_2020_id", "b19013_001e"]].copy()

cbsa_income = cbsa_income.rename(columns={"b19013_001e": "cbsa_income_2022"})

cbsa_income.to_csv("data/tidy/cbsa_income_2022.csv", index=False)

## Place Median Home Value
b25077["place_2020_id"] = b25077["geo_id"].str[-7:]

place_home_value = b25077[["place_2020_id", "b25077_001e"]].copy()

place_home_value = place_home_value.rename(columns={"b25077_001e": "place_home_value_census_2022"})

place_home_value.to_csv("data/tidy/place_home_value_2022.csv", index=False)
