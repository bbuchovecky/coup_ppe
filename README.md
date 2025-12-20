# xppe

A Python package for working with output from perturbed parameter ensembles (PPEs). Built on top of `intake-esm` and `ecg-tools` for cataloging data. Developed for CESM2 output from the [Ecoclimate Lab](https://www.atmos.washington.edu/~aswann/LabWebsitePublic/).

| Ensemble      	| Short Name 	| Status   	                 | Reference
|---------------	|-------------	|--------------------------  | --------------
| CESM2 PI SOM PPE 	| pisom      	| Available                  | [Zarakas et al. (2024)](https://doi.org/10.1029/2024GL108372)
| CESM2 FHIST PPE   | fhist      	| Incomplete (still running) |
| HadCM3 PPE        | hadcm3        | Available                  | [Booth et al. (2012)](https://doi.org/10.1088/1748-9326/7/2/024002)



# Getting started
Clone the repo and install the conda environment.
```Shell
$ git clone https://github.com/bbuchovecky/xppe.git
$ cd xppe
$ conda env create -f environment.yml
$ conda activate xppe
```
Copy and populate `config.yml` according to your filesystem structure.
```Shell
$ cp config_TEMPLATE.yml config.yml
```
Run command line utilities to set up the workspace and create a data catalog of the PPE output.
```Shell
$ cd scripts
$ xppe-build-member-id-map
$ xppe-build-catalog coup pisom timeseries --dask-cluster <ACCOUNT> 15 1GB 01:00:00
```
You can add the flag `--help` to get details about the command line utility.

# Loading PPE output
After building the data catalogs, you can access the output with the `load_output` function.
```Python
import xppe

ds = xppe.load_output(
    varname=["TREFHT", "LHFLX"],
    component="atm",
    frequency="month_1",
    member=["fff", "d_max,min"]
)
```

# TODO
- add tests
- check that the derived variable registry works
- check that cataloging offline simulations works
- `metadata.units`: add functionality to infer units, convert to base units, convert to specified units
- `metadata.variables`: do I still need this?
    - store intensive vs. extensive properties for spatial aggregation
    - write a function to scrape NetCDF metadata and generate NamedTuple or custom data structure for each variable (e.g., base units, dimensions, aggregation weight, intensive/extensive, long name, component, kind: rate/state, formula, derived from, cell methods)
- `metadata.grid`: decide on scope: access files via paths from `config.yml` or do everything here?
    - store grid information (e.g., gridcell area, ocean/land masks)
    - keep paths to these files in `config.yml`
- `stats.aggregate/reduce`: perform spatial aggregation (either area sum or area-weighted average based on intensive/extensive variable property)
- `stats.spatial_subset`: select a specific region by lat and lon (e.g., zonal, box)