# Dask-Landsat 

## Description

This script fetches Landsat resources via the [USGS Landsat Collection-2 STAC API](https://landsatlook.usgs.gov/stac-server/), processes items from each of the appropriate collections (products), and returns the area and geometry for each item based on its projection.

This script uses Dask to parallelize fetching and processing.

## Install 

### Linux
1. [Install make](https://www.gnu.org/software/make/)
2. `make install`

## Run
- `make run`