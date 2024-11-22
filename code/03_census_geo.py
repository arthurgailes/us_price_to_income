"""
Download places and cbsas
"""

# Rewrite the commented code in python
# Path: code/03_census_geo.py

import geopandas as gpd
import pygris
from shapely.validation import make_valid

us_place_2020 = pygris.places(cb=True)

us_place_2020 = (
    us_place_2020.to_crs(4326)[["GEOID", "NAMELSAD", "STUSPS", "geometry"]]
    .rename(
        columns={"GEOID": "place_2020_id", "NAMELSAD": "place_name", "STUSPS": "state"}
    )
    .sort_values(by="place_2020_id")
)

cbsa = pygris.core_based_statistical_areas(cb=True)

cbsa = (
    cbsa.to_crs(4326)[["GEOID", "NAME", "geometry"]]
    .rename(columns={"GEOID": "cbsa_2020_id", "NAME": "cbsa_name"})
    .sort_values(by="cbsa_2020_id")
)

states = us_place_2020["state"].unique().tolist()

for state in states:
    state_places = us_place_2020[us_place_2020["state"] == state]

    counties = pygris.counties(state=state, cb=True).COUNTYFP.tolist()

    # Get water areas for the current state
    water = pygris.area_water(state=state, county=counties).to_crs(state_places.crs)
    unified_water = gpd.GeoDataFrame(
        geometry=[water.geometry.union_all()], crs=water.crs
    )

    state_places_no_water = gpd.overlay(state_places, unified_water, how="difference")

    # Update the main DataFrame
    assert len(state_places_no_water) == len(state_places)
    us_place_2020.loc[us_place_2020["state"] == state, "geometry"] = (
        state_places_no_water["geometry"].values
    )


# simplify
us_place_2020["geometry"] = us_place_2020["geometry"].simplify(
    tolerance=0.001, preserve_topology=True
)


def clean_geometry(geom):
    if geom.is_valid:
        return geom
    return make_valid(geom)


us_place_2020["geometry"] = us_place_2020["geometry"].apply(clean_geometry)

assert us_place_2020.geometry.is_valid.all(), "Invalid geometries found"


us_place_2020.to_parquet("data/tidy/us_place_2020.parquet")
cbsa.to_parquet("data/tidy/us_cbsa.parquet")
