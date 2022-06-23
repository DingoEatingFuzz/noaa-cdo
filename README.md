## NOAA CDO -> CSV -> Parquet -> DuckDB

This repo is two things:

1. A rust script for converting the special-formatted NOAA CDO data into CSVs
2. A few node scripts for creating DuckDBs out of these data parquet files*

*This repo does not include CSV -> Parquet processing, but that's because you [can grab this utility instead](https://github.com/domoritz/csv2parquet).

## TODO

1. Create a Makefile for downloading the NOAA data, for now you can [grab it from the source](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/) (you want `ghcnd-stations.txt` and `ghcnd_gsn.tar.gz`).
2. Publish the built csv/parquet/duckdb artifacts to GitHub releases, maybe.
