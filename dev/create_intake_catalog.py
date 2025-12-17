import sys
import os
import pathlib
import traceback

import numpy as np
import pandas as pd
import xarray as xr
import cftime

import intake
import joblib
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK

from parallel.daskhelper import create_dask_cluster
from access.inspect import infer_frequency


def parse_pisom_timeseries(file):
    """ Parses timeseries files from PI SOM PPE """
    file = pathlib.Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")
        name = split[0].split("_")

        info["member"] = name[0]
        if "v" in name[-1]:
            info["version"] = name[-1][1:]
        else:
            info["version"] = np.nan
        info["component"] = split[1]
        info["stream"] = split[2]
        info["variable"] = split[4]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = infer_frequency(ds)
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


cluster, client = create_dask_cluster("UWAS0155", 20, nmem="2GB")

# Create the catalog builder
cat_builder = Builder(
    paths=["/glade/campaign/cgd/tss/czarakas/CoupledPPE/coupled_simulations"],
    depth=5,
    exclude_patterns=["*/hist/*", "*/rest/*"],
    joblib_parallel_kwargs={"n_jobs": -1},
)

# Build the catalog with my custom parser using the Dask client
with joblib.parallel_backend('dask', scatter=[parse_pisom_timeseries]):
    cat_builder = cat_builder.build(parsing_func=parse_pisom_timeseries)

# Save the catalog with proper time concatenation settings
cat_builder.save(
    name="coup_ppe_catalog",
    path_column_name="path", 
    variable_column_name="variable",
    data_format="netcdf",
    groupby_attrs=["member", "component", "stream", "frequency"],
    aggregations=[],
)

os.remove("./dask-worker.*")
