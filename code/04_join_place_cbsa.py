# Spatial join CBSA income to place home value

import pandas as pd
import geopandas as gpd
import numpy as np
import shutil

place_avm = pd.read_csv("data/tidy/us_place_avm.csv", dtype={"place_2020_id": str})
place_value = pd.read_csv(
    "data/tidy/place_home_value_2022.csv", dtype={"place_2020_id": str}
)
cbsa_income = pd.read_csv("data/tidy/cbsa_income_2022.csv", dtype={"cbsa_2020_id": str})

place_geo = gpd.read_parquet("data/tidy/us_place_2020.parquet")

cbsa_geo = gpd.read_parquet("data/tidy/us_cbsa.parquet")

# join each place to cbsa and keep the largest intersection
place_cbsa = gpd.overlay(place_geo, cbsa_geo, how="intersection")
place_cbsa["intersection_area"] = place_cbsa.geometry.area
place_cbsa = place_cbsa.sort_values(
    "intersection_area", ascending=False
).drop_duplicates(subset="place_2020_id")


# join cbsa code to each place
place_geo_data = place_geo.merge(
    place_cbsa[["place_2020_id", "cbsa_2020_id", "cbsa_name"]], on="place_2020_id"
)

# add cbsa median income to each place
place_geo_data = place_geo_data.merge(cbsa_income, on="cbsa_2020_id")
place_geo_data = place_geo_data.merge(place_value, on="place_2020_id")
# left join to keep PR
place_geo_data = place_geo_data.merge(place_avm, on="place_2020_id", how="left")

place_geo_data["avm_income_ratio"] = (
    place_geo_data["avm_2023"] / place_geo_data["cbsa_income_2022"]
)
place_geo_data["census_income_ratio"] = (
    place_geo_data["place_home_value_census_2022"] / place_geo_data["cbsa_income_2022"]
)

place_geo_data = place_geo_data.rename(
    columns={
        "cbsa_median_income_2022": "cbsa_income_2022",
        "place_median_avm_2023": "avm_2023",
        "avg_annual_hpa_2012_2024": "hpa_2012_2024",
    }
)
place_geo_data.columns

# drop the place geometry column; no longer needed for analysis
place_data = pd.DataFrame(place_geo_data.drop(columns="geometry"))

# store the raw data in case it's needed later
place_data.to_csv("data/tidy/map_data_income_avm_raw.csv", index=False)

labels = [
    "Highly Inclusive: 0-2.9",
    "Inclusive: 3-4.9",
    "At-Risk: 5-9.9",
    "Exclusionary: 10-14.9",
    "Extremely Exclusionary: 15+",
]


# Format data for hover
map_data_lab = place_geo_data.copy()

map_data_lab.drop(columns=["stab", "PlaceName"], inplace=True)

map_data_lab = map_data_lab[
    map_data_lab["avm_income_ratio"].notna()
    | map_data_lab["census_income_ratio"].notna()
]


map_data_lab["avm_income_ratio"] = round(map_data_lab["avm_income_ratio"], 1)
map_data_lab["census_income_ratio"] = round(map_data_lab["census_income_ratio"], 1)

map_data_lab["avm_income_ratio_category"] = (
    pd.cut(
        map_data_lab["avm_income_ratio"],
        bins=[0, 2.9, 4.9, 9.9, 14.9, np.inf],
        labels=labels,
    )
    .astype(str)
    .replace("nan", "No Data")
)

map_data_lab["census_income_ratio_category"] = (
    pd.cut(
        map_data_lab["census_income_ratio"],
        bins=[0, 2.9, 4.9, 9.9, 14.9, np.inf],
        labels=labels,
    )
    .astype(str)
    .replace("nan", "No Data")
)


# Create a dictionary of column names and their descriptions
data_dictionary = {
    "place_2020_id": "Place identifier",
    "avm_income_ratio": "Home Value to Income Ratio",
    "census_income_ratio": "Home Value to Income Ratio (ACS Home Values)",
    "avm_2023": "Median Home Value",
    "place_home_value_census_2022": "Median Home Value (ACS)",
    "cbsa_income_2022": "Median Household Income (CBSA)",
    "avm_income_ratio_category": "Home Value to Income Ratio Category",
    "census_income_ratio_category": "Home Value to Income Ratio Category (ACS Home Values)",
    "hpa_2012_2024": "Average Annual Home Price Appreciation (2012-2024)",
    "place_name": "Place Name",
    "cbsa_name": "CBSA Name",
}

# Create a DataFrame from the data dictionary
data_dict_df = pd.DataFrame.from_dict(
    data_dictionary, orient="index", columns=["Description"]
)

# Reset the index and rename it to 'Column Name'
data_dict_df = data_dict_df.reset_index().rename(columns={"index": "Column Name"})

# Save the data dictionary DataFrame as a CSV file
data_dict_df.to_csv("data/tidy/data_dictionary.csv", index=False)

map_data_lab.to_file("data/tidy/map_data_income_avm.geojson")
map_data_lab.to_file(
    "data/tidy/map_data_income_avm/map_data_income_avm.shp",
    driver="ESRI Shapefile",
)
shutil.make_archive(
    "data/tidy/map_data_income_avm.shp", "zip", "data/tidy/map_data_income_avm"
)

# tests
assert len(map_data_lab) > 2e4, "expect gt 20k places"

assert (
    "hpa_2012_2024" in map_data_lab.columns
), "Column 'hpa_2012_2024' is missing from map_data_lab"

for col in data_dict_df["Column Name"]:
    assert (
        col in map_data_lab.columns
    ), f"Required column '{col}' is missing from map_data_lab"

# Test Puerto Rico inclusion
pr_places = map_data_lab[map_data_lab["place_2020_id"].str.startswith("72")]
assert (
    len(pr_places) > 0
), "No Puerto Rico places (place_2020_id starting with '72') found in the dataset"

# Test state coverage (50 states + DC + PR = 52)
state_codes = map_data_lab["place_2020_id"].str[:2].unique()
assert (
    len(state_codes) == 52
), f"Expected 52 states (including DC and PR), but found {len(state_codes)} in the dataset"

# Test categorical variables
expected_categories = set([*labels, "No Data"])
actual_categories = set(map_data_lab["avm_income_ratio_category"].unique())
assert actual_categories.issubset(
    expected_categories
), "Unexpected categories in avm_income_ratio_category"

# Test for duplicates
assert not map_data_lab["place_2020_id"].duplicated().any(), "Duplicate place IDs found"

# Test ratio calculations
test_rows = map_data_lab[
    map_data_lab["avm_2023"].notna() & map_data_lab["cbsa_income_2022"].notna()
]
calculated_ratio = test_rows["avm_2023"] / test_rows["cbsa_income_2022"]
ratio_diff = abs(calculated_ratio - test_rows["avm_income_ratio"])
assert (ratio_diff < 0.1).all(), "avm_income_ratio calculation mismatch"

# Test for missing values in key fields
key_fields = ["place_2020_id", "place_name", "cbsa_name"]
for field in key_fields:
    assert not map_data_lab[field].isna().any(), f"Missing values found in {field}"

# Test geometry
assert "geometry" in map_data_lab.columns, "Geometry column is missing"
assert not map_data_lab.geometry.isna().all(), "All geometry values are null"
assert map_data_lab.geometry.is_valid.all(), "Invalid geometries found"

assert (
    map_data_lab.cbsa_name[
        map_data_lab.place_name == "Lexington-Fayette urban county"
    ].iloc[0]
    == "Lexington-Fayette, KY"
)
