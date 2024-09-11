"""
  Download places and cbsas for California
"""

# Rewrite the commented code in python
# Path: code/03_census_geo.py

import geopandas as gpd
import pygris

us_place_2020 = pygris.places(cb=True)

us_place_2020 = us_place_2020.to_crs(4326)[["GEOID", "NAMELSAD", "STUSPS", "geometry"]]\
  .rename(columns={"GEOID": "place_2020_id", "NAMELSAD": "place_name", "STUSPS": "state"})\
  .sort_values(by="place_2020_id")

cbsa = pygris.core_based_statistical_areas(cb=True)

cbsa = cbsa.to_crs(4326)[["GEOID", "NAME", "geometry"]]\
  .rename(columns={"GEOID": "cbsa_2020_id", "NAME": "cbsa_name"})\
  .sort_values(by="cbsa_2020_id")

us_place_2020.to_parquet("data/tidy/us_place_2020.parquet")
cbsa.to_parquet("data/tidy/us_cbsa.parquet")
