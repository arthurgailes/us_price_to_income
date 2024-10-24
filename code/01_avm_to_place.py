"""
Convert AEI AVM data to place from block (2020 geo)

Manual step required:

* Download Block-Place crosswalk from https://mcdc.missouri.edu/applications/geocorr2022.html
  * Note: this only allows for downloading 13 states at a time, necessitating the
  concatenation below.


To repeat, just download, unzip in data/raw and run this script.
"""

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import csv

# download AEI data
avm_url = "https://aeihousingcenter.org/public/data/wod/block_20240508.zip"
response = requests.get(avm_url)
zip_file = zipfile.ZipFile(io.BytesIO(response.content))
zip_file.extractall("data/raw")

# load data
us_block_data = pd.read_csv(
  "data/raw/datablock_20240508.csv", 
  usecols=["block_2020", "p50_avm_202312", "avg_annual_hpa_2012_2024"],
  dtype={"block_2020": str}
)

geocorr_files = [f for f in os.listdir("data/raw") if f.startswith("geocorr2022")]
xwalk_block_place_names = pd.read_csv("data/raw/" + geocorr_files[0], nrows = 1).columns

xwalk_block_place = pd.DataFrame()

for file in geocorr_files:
    # Read each file in and append
    temp_df = pd.read_csv(
        f"data/raw/{file}",
        header=None,
        skiprows=2,
        names=xwalk_block_place_names,
        encoding='latin-1',
        dtype={"county": str, "tract": str, "block": str, "place": str, "state": str}
    )
    
    # Append the data to the main DataFrame
    xwalk_block_place = pd.concat([xwalk_block_place, temp_df], ignore_index=True)

## Clean and join data

xwalk_block_place["tract"] = xwalk_block_place["tract"].str.replace(".", "")

xwalk_block_place["block_2020"] = xwalk_block_place["county"] + xwalk_block_place["tract"] + xwalk_block_place["block"]
xwalk_block_place = xwalk_block_place.drop_duplicates(subset=["block_2020"])

xwalk_block_place["place_2020_id"] = xwalk_block_place["state"] + xwalk_block_place["place"]

# xwalk_block_place = xwalk_block_place[xwalk_block_place['state'] != 72]

xwalk_clean = xwalk_block_place[["block_2020", "place_2020_id", "PlaceName","stab"]].copy()

us_block_data.rename(columns={"p50_avm_202312": "avm_2023"}, inplace=True)

us_block_data_place = us_block_data.merge(xwalk_clean, on="block_2020", how="inner")
 
#  write intermediates
us_block_data.to_parquet("data/intermed/us_block_data.parquet", index=False)
xwalk_clean.to_parquet("data/intermed/xwalk_block_place.parquet", index=False)

## Collapse to place

# get median avms
us_place_avm = (
  us_block_data_place
    .groupby(["place_2020_id", "PlaceName", "stab"])
    .agg({
        'avm_2023': lambda x: np.nanmedian(x).round(),
        'avg_annual_hpa_2012_2024': 'median'
    })
    .reset_index()
)

# Assert that place_2020_id isn't duplicated
assert us_place_avm['place_2020_id'].duplicated().sum() == 0, "Duplicate place_2020_id found in the result"

# drop NAs
us_place_avm.dropna(inplace=True)

us_place_avm.to_csv("data/tidy/us_place_avm.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

# save tract AVMs for reference
us_tract_avm = (
  us_block_data
    .assign(tract_2020_id=us_block_data['block_2020'].str[:11])
    .groupby('tract_2020_id').agg({
        'avm_2023': lambda x: np.nanmedian(x).round(),
        'avg_annual_hpa_2012_2024': 'median'
    })
    .reset_index()
)

us_tract_avm.to_csv("data/tidy/us_tract_avm.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

# testing
result = us_tract_avm.tract_2020_id.str[:2].value_counts().sort_index()
assert len(result) == 51, f"Expected length 51, but got {len(result)}"

result = us_place_avm.place_2020_id.str[:2].value_counts().sort_index()
assert len(result) == 51, f"Expected length 51, but got {len(result)}"

med_avm = us_place_avm.avm_2023.median()
assert med_avm > 200000
