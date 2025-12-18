# coup_ppe

A Python package for working with output from CESM2 perturbed parameter ensembles (PPEs) created by the [Ecoclimate Lab](https://www.atmos.washington.edu/~aswann/LabWebsitePublic/). Built on top of `intake-esm` and `ecg-tools` for cataloging the PPE output. 

| Ensemble    	| Default Coupled Case Name                           	| Short Name 	| Status   	                |
|-------------	|-----------------------------------------------------	|------------	|-------------------------	|
| PI SOM PPE  	| COUP0000_PI_SOM_v02                                 	| pisom      	| Complete 	                |
| FHIST PPE   	| f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.000 	| fhist      	| Partial (still running)  	|
| FSSP370 PPE 	| f.e21.FSSP370_BGC.f19_f19_mg17.ssp370.coupPPE.000   	| fssp370    	| TBD      	                |

# Getting started
Clone the repo and install the conda environment.
```Shell
$ git clone https://github.com/bbuchovecky/coup_ppe.git
$ cd coup_ppe
$ conda env create -f environment.yml
$ conda activate coup-ppe
```
Copy and populate `config.yml` according to your filesystem structure.
```Shell
$ cp config_TEMPLATE.yml config.yml
```
Run scripts to set up workspace and create a data catalog of the PPE output.
```Shell
$ cd scripts
$ python create_member_id_map.py
$ python build_catalog.py coup pisom timeseries --dask-cluster <ACCOUNT> 15 1GB 01:00:00
```
You can run `python build_catalog.py --help` for details about its arguments.

# Loading PPE output
After building the data catalogs, you can access the output with the `access.load` module in your Python scripts.
```Python
import coup_ppe.access.load

ds = coup_ppe.access.load.load_ppe(
    varname=["TREFHT", "LHFLX"],
    component="atm",
    frequency="month_1",
    member=["fff", "d_max,min"]
)
```

# TODO
- check that the derived variable registry works
- `metadata.units`: add functionality to infer units, convert to base units, convert to specified units
- `metadata.variables`: do I still need this?
    - store intensive vs. extensive properties for spatial aggregation
    - write a function to scrape NetCDF metadata and generate NamedTuple or custom data structure for each variable (e.g., base units, dimensions, aggregation weight, intensive/extensive, long name, component, kind: rate/state, formula, derived from, cell methods)
- `metadata.grid`: decide on scope: access files via paths from `config.yml` or do everything here?
    - store grid information (e.g., gridcell area, ocean/land masks)
    - keep paths to these files in `config.yml`
- `stats.aggregate/reduce`: perform spatial aggregation (either area sum or area-weighted average based on intensive/extensive variable property)
- `stats.spatial_subset`: select a specific region by lat and lon (e.g., zonal, box)


<!--
# Tasks

## Documentation
- create annotated directory structure

## Catalog model output
- catalog the pisom timeseries

## Derived variable registry
- `registry.derived_vars`: write functions for common derived variables (e.g., PRECT)
- pass `DerivedVariableRegistry` to `access.load.load_ppe`

## Metadata
- write script to create member ID map in new location: `src/coup_ppe/catalogs/`
- `metadata.units`: write functions to infer units, convert to base units, then convert to specified units
- `metadata.variables`: do I still need this?
    - contain intensive vs. extensive properties for spatial aggregation
    - write a function to scrape NetCDF metadata and generate NamedTuple or custom data structure for each variable (base units, dimensions, aggregation weight, intensive/extensive, long name, component, kind: rate/state, formula, derived from, cell_methods)
- `metadata.grid`: store grid information (gridcell area, ocean/land masks)

## Analysis
- `stats.aggregate/reduce`:
    - write function to perform spatial aggregation (either area sum or area-weighted average based on intensive/extensive variable property)
    - write a function to select a specific region by lat and lon (e.g., zonal, box)
-->
