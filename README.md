# coup_ppe

A Python package for working with output from CESM2 perturbed parameter ensembles (PPEs) created by the [Ecoclimate Lab](https://www.atmos.washington.edu/~aswann/LabWebsitePublic/). Built on top of `intake-esm` and `ecg-tools` for cataloging the PPE output. 

| Ensemble    	| Default Coupled Case Name                           	| Short Name 	| Status   	                |
|-------------	|-----------------------------------------------------	|------------	|-------------------------	|
| PI SOM PPE  	| COUP0000_PI_SOM_v02                                 	| pisom      	| Complete 	                |
| FHIST PPE   	| f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.000 	| fhist      	| Partial (still running)  	|
| FSSP370 PPE 	| f.e21.FSSP370_BGC.f19_f19_mg17.ssp370.coupPPE.000   	| fssp370    	| TBD      	                |

# Getting started
```Shell
$ git clone https://github.com/bbuchovecky/coup_ppe.git
$ cd coup_ppe
$ cp config_TEMPLATE.yml config.yml
```
Populate `config.yml` according to your filesystem structure.
```Shell
$ cd scripts
$ python create_member_id_map.py
$ python build_catalog.py coup pisom timeseries
```
You can run `python build_catalog.py --help` for details about its arguments.

<!--
# Tasks

## Documentation
- create annotated directory structure

## Catalog model output
- `access.catalog`: finalize catalog builder function
- catalog the pisom timeseries

## Load model output
- determine where/how to store dictionaries for variables, ensemble names, etc.
- `access.load`: write a wrapper of intake-esm to query, search, and load a dataset

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
