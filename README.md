# Home Price to Income Ratio Map

This map and dataset shows the ratio of Census Place (city) home values to CBSA (metro) median incomes for each Census Place in the United States.

## Data Guide

The cleaned data used in the map is at:
* Recommended: data\tidy\map_data_income_avm.geojson
* ESRI Shapefile: data\tidy\map_data_income_avm
    * The shapefile works, but column names are truncated due to the format limitations.

Data dictionary: `data\tidy\data_dictionary.csv`

## Home price data

* Home price AVM: Automated Valuation Model (AVM) of single-family properties in 2023 
    * Source: the American Enterprise Institute's Walkable-Oriented Development Map. 
* Household income: Estimated at the Core-Based Statistical Area (CBSA), i.e. Metroplolitan and Micropolitan Statistical Areas.
    * Source: American Community Survey 2022 5-year estimates
    * Note: ACS home value estimates are also provided in the dataset for reference. The AVM is preferred because it is more recent, represents a complete census of properties, and because the ACS estimates are capped at $2 million, which is inappropriate for many jurisdictions. 


### Python environment

The python environment was created with conda. It can be installed locally with the following command:

```
conda env create -p PATH/TO/DIRETORY/venv --file environment.yml`
```

And (re) exported with:

```
conda env export --from-history>environment.yml
```
