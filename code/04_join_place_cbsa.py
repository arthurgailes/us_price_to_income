# Spatial join CBSA income to place home value

import pandas as pd
import geopandas as gpd
import numpy as np

place_avm = pd.read_csv("data/tidy/us_place_avm.csv", dtype={"place_2020_id": str})
place_value = pd.read_csv("data/tidy/place_home_value_2022.csv", dtype={"place_2020_id": str})
cbsa_income = pd.read_csv("data/tidy/cbsa_income_2022.csv", dtype={"cbsa_2020_id": str})

place_geo = gpd.read_parquet("data/tidy/us_place_2020.parquet")
cbsa_geo = gpd.read_parquet("data/tidy/us_cbsa.parquet")

# join each place centroid to cbsa
place_cbsa = gpd.sjoin(place_geo, cbsa_geo, how="left", predicate="intersects")
place_cbsa = place_cbsa.drop_duplicates(subset="place_2020_id")

# join cbsa code to each place
place_geo_data = place_geo.merge(
  place_cbsa[["place_2020_id", "cbsa_2020_id", "cbsa_name"]], 
  on="place_2020_id"
)

# add cbsa median income to each place
place_geo_data = place_geo_data.merge(cbsa_income, on="cbsa_2020_id")
place_geo_data = place_geo_data.merge(place_avm, on="place_2020_id")
place_geo_data = place_geo_data.merge(place_value, on="place_2020_id")

place_geo_data["avm_income_ratio"] = place_geo_data["avm_2023"] / place_geo_data["cbsa_income_2022"]
place_geo_data["census_income_ratio"] = place_geo_data["place_home_value_census_2022"] / place_geo_data["cbsa_income_2022"]

place_geo_data.rename(columns={"cbsa_income_2022": "cbsa_median_income_2022", "avm_2023": "place_median_avm_2023"}, inplace=True)

# drop the geometry column; no longer needed for analysis
place_data = pd.DataFrame(place_geo_data.drop(columns='geometry'))

# store the raw data in case it's needed later
place_data.to_csv("data/tidy/map_data_income_avm_raw.csv", index=False)


labels = [
      "Highly Inclusive Jurisdictions: 0-2.9",
      "Inclusive Jurisdictions: 3-4.9",
      "At-Risk Jurisdictions: 5-9.9",
      "Exclusionary Jurisdictions: 10-14.9",
      "Extremely Exclusionary Jurisdictions: 15+"]


# Format data for hover
map_data_lab = place_geo_data.copy()

map_data_lab = map_data_lab[map_data_lab["avm_income_ratio"].notna()]

map_data_lab["avm_income_ratio"] = round(map_data_lab["avm_income_ratio"], 1)

map_data_lab["avm_income_ratio_category"] = pd.cut(
    map_data_lab["avm_income_ratio"],
    bins=[0, 2.9, 4.9, 9.9, 14.9, np.inf],
    labels = labels).astype(str)

map_data_lab["census_income_ratio_category"] = pd.cut(
    map_data_lab["census_income_ratio"],
    bins=[0, 2.9, 4.9, 9.9, 14.9, np.inf],
    labels = labels).astype(str)

# convert home value and income columns to dollars
# map_data_lab["place_median_avm_2023"] = map_data_lab["place_median_avm_2023"].apply(lambda x: "${:,.0f}".format(x))
# map_data_lab["place_home_value_census_2022"] = map_data_lab["place_home_value_census_2022"].apply(lambda x: "${:,.0f}".format(x))
# map_data_lab["cbsa_median_income_2022"] = map_data_lab["cbsa_median_income_2022"].apply(lambda x: "${:,.0f}".format(x))


# Create a dictionary of column names and their descriptions
data_dictionary = {
    "place": "Place identifier",
    "avm_income_ratio": "Home Value to Income Ratio",
    "census_income_ratio": "Home Value to Income Ratio (ACS Home Values)",
    "place_median_avm_2023": "Median Home Value",
    "place_home_value_census_2022": "Median Home Value (ACS)",
    "cbsa_median_income_2022": "Median Household Income (CBSA)",
    "avm_income_ratio_category": "Home Value to Income Ratio Category",
    "census_income_ratio_category": "Home Value to Income Ratio Category (ACS Home Values)",
    "place_name": "Place Name",
    "cbsa_name": "CBSA Name"
}

# Create a DataFrame from the data dictionary
data_dict_df = pd.DataFrame.from_dict(data_dictionary, orient='index', columns=['Description'])

# Reset the index and rename it to 'Column Name'
data_dict_df = data_dict_df.reset_index().rename(columns={'index': 'Column Name'})

# Save the data dictionary DataFrame as a CSV file
data_dict_df.to_csv('data/tidy/data_dictionary.csv', index=False)


# drop unnecessary columns
map_data_lab = map_data_lab.drop(columns=["cbsa_2020_id", "place_2020_id"])

map_data_lab.to_file("data/tidy/map_data_income_avm.geojson")
map_data_lab.to_file("data/tidy/map_data_income_avm/map_data_income_avm.shp", driver = "ESRI Shapefile")

