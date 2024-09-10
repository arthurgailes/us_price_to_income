# Spatial join CBSA income to place home value

import pandas as pd
import geopandas as gpd
import numpy as np

place_avm = pd.read_csv("data/tidy/us_place_avm.csv", dtype={"place_2020_id": str})
place_value = pd.read_csv("data/tidy/place_home_value_2022.csv", dtype={"place_2020_id": str})
cbsa_income = pd.read_csv("data/tidy/cbsa_income_2022.csv", dtype={"cbsa_2020_id": str})

place_geo = gpd.read_parquet("data/tidy/us_place_2020.parquet")
cbsa_geo = gpd.read_parquet("data/tidy/us_cbsa.parquet")

# create centroid version of place geometry
place_centroid = place_geo.copy()
place_centroid["geometry"] = place_geo["geometry"].to_crs(3857).centroid.to_crs(4326)

# join each place centroid to cbsa
place_cbsa = gpd.sjoin(place_geo, cbsa_geo, how="left", predicate="intersects")
place_cbsa = place_cbsa.drop_duplicates(subset="place_2020_id")

# join cbsa code to each place
place_geo_data = place_geo.merge(
  place_cbsa[["place_2020_id", "cbsa_2020_id", "cbsa_name"]], 
  on="place_2020_id"
)


# losing SF on centroid join
# SF = "0667000"
# place_geo[place_geo["place_name"].str.contains("San Francisco")]
# place_geo[place_geo["place_2020_id"] == "0667000"]
# place_cbsa[place_cbsa["place_2020_id"] == "0667000"]
# place_geo_data[place_geo_data["place_2020_id"] == "0667000"]
# cbsa_geo[cbsa_geo["cbsa_name"].str.contains("San Francisco")]

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

# color labels from green to red
colors = [
  "#00ff00", "#a6d96a", "#ffffbf", "#fdae61", "#ff0000"
]

# create a dictionary from labels to colors
color_dict = dict(zip(labels, colors))
color_dict

# Format data for hover
map_data_lab = place_geo_data.copy()

map_data_lab = map_data_lab[map_data_lab["avm_income_ratio"].notna()]

map_data_lab["avm_income_ratio"] = round(map_data_lab["avm_income_ratio"], 1)

map_data_lab["Home Value to Income Ratio Category"] = pd.cut(
    map_data_lab["avm_income_ratio"],
    bins=[0, 2.9, 4.9, 9.9, 14.9, np.inf],
    labels = labels).astype(str)

# map_data_lab["home_inc_color"] = map_data_lab["Home Value to Income Ratio Category"].map(color_dict)

# convert home value and income columns to dollars
map_data_lab["place_median_avm_2023"] = map_data_lab["place_median_avm_2023"].apply(lambda x: "${:,.0f}".format(x))
map_data_lab["cbsa_median_income_2022"] = map_data_lab["cbsa_median_income_2022"].apply(lambda x: "${:,.0f}".format(x))

# remove unnecessary ", CA" suffix from cbsa
map_data_lab["cbsa_name"] = map_data_lab["cbsa_name"].str.replace(", CA", "")

map_data_lab = map_data_lab.rename(columns={
  "place": "Place",
  "avm_income_ratio": "Home Value to Income Ratio",
  "place_median_avm_2023": "Median Home Value (Single Family)",
  "cbsa_median_income_2022": "Median Household Income (CBSA)",
  "place_name": "Place Name",
  "cbsa_name": "CBSA Name"})

# drop unnecessary columns
map_data_lab = map_data_lab.drop(columns=["cbsa_2020_id", "place_2020_id"])

map_data_lab.to_file("data/tidy/map_data_income_avm.geojson")
map_data_lab.to_file("data/tidy/map_data_income_avm/map_data_income_avm.shp", driver = "ESRI Shapefile")

