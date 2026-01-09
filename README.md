# cpl_ppe_co2

Code and documentation for coupled CESM PPEs under transient forcings from the historical and future.

## File organization
Adapted from Julius Busecke's [`cookiecuttter-science-project`](https://github.com/jbusecke/cookiecutter-science-project) and Claire Zarakas's [`coupled_PPE`](https://github.com/czarakas/coupled_PPE).


<!-- ```
├── README.md
├── LICENSE
├── environment.yml                <- Conda environment file. Create environment with
│                                     `conda env create -f environment.yml`
│
├── ref                            <- Notes, data dictionaries, and all other explanatory materials.
|
├── utils                          <- Python utilities.
│
├── hist                           <- All files for the PPE under historical forcing.
│   ├── code
|   |   ├── 01_select-parameters   <- Workflow for parameter selection using the land-only PPEs.
|   |   ├── 02_set-up-ensemble     <- Review parameter files and store run script templates.
|   |   ├── 03_run-scripts         <- The run scripts for each simulation in the ensemble.
|   |   ├── 04_check-simulations   <- Quick checks of the simulations.
|   |   ├── 05_postprocess         <- Scripts that postprocess the raw simulation output.
|   |   └── 05_analyze_simulations <- Notebooks for analyzing the ensemble.
|   |
|   └── data
|       ├── setup                  <- Input files for running the PPE simulations.
|       |   ├── namelist_mods      <- Namelist modifications.
|       |   ├── paramfiles         <- Parameter files.
|       |   └── sst                <- Observed SST fields.
|       |
|       ├── interim                <- Small subset datasets needed to reproduce results in the
|       |                             analysis notebooks. Includes area weights for the CAM and
|       |                             CLM grids.
|       |
|       ├── processed              <- Symbolic links to postprocessed simulation output, populated
|       |                             via the postprocessing scripts.
|       |
|       └── raw                    <- Symbolic links to immutable raw simulation output, populated
|                                     directly from the model and never modified manually.
|
└── future                         <- All files for the PPE under future scenario forcings.
    |                                 Same structure as `present`.
    ├── code
    |   ├── 01_select-parameters
    |   ├── 02_set-up-ensemble
    |   ├── 04_check-simulations
    |   ├── 05_postprocess
    |   └── 05_analyze_simulations
    |
    └── data
        ├── setup
        |   ├── namelist_mods
        |   ├── paramfiles
        |   └── sst 
        ├── interim
        ├── processed
        └── raw
``` -->