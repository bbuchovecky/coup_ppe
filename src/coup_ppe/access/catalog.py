"""
Tools for building intake-esm catalogs for CESM PPEs.
"""
import pathlib
import traceback

import numpy as np
import xarray as xr

import joblib
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK

import parallel.daskhelper
import access.inspect
import metadata.conventions


def parse_pisom_timeseries(file):
    """Parses timeseries files from PI SOM PPE."""
    file = pathlib.Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")
        name = split[0].split("_")

        info["member"] = str(int(name[0][4:]))
        if "v" in name[-1]:
            info["version"] = str(int(name[-1][1:]))
        else:
            info["version"] = ""
        info["component"] = metadata.conventions.canonical_component(split[1])
        info["stream"] = split[2]
        info["variable"] = split[4]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["variable_long_name"] = ds[info["variable"]].attrs["long_name"]
            info["frequency"] = access.inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_pisom_history(file: str) -> dict:
    """Parses history files from PI SOM PPE."""
    file = pathlib.Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")
        name = split[0].split("_")

        info["member"] = str(int(name[0][4:]))
        if "v" in name[-1]:
            info["version"] = str(int(name[-1][1:]))
        else:
            info["version"] = np.nan
        info["component"] = metadata.conventions.canonical_component(split[1])
        info["stream"] = split[2]
        info["date"] = split[3]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = access.inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["variable"] = [var for var in ds if 'long_name' in ds[var].attrs]
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_fhist_history(file: str) -> dict:
    """Parses history files from FHIST PPE."""
    file = pathlib.Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")

        info["member"] = str(int(split[6]))
        info["component"] = metadata.conventions.canonical_component(split[7])
        info["stream"] = split[8]
        info["date"] = split[9]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = access.inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["variable"] = [var for var in ds if 'long_name' in ds[var].attrs]
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def build_catalog(
        ensemble: str,
        kind: str,
        rootpath: list[str],
        outpath: str,
        tag: str = "",
        depth: int | None = None,
    ) -> str:
    """
    Build an intake-esm catalog for a CESM PPE.
    
    Parameters
    ----------
    ensemble : str
        Name of the ensemble
    kind : str
        Type of catalog: "history" or "timeseries"
    rootpath : list[str]
        Root paths to search for data files
    outpath : str, optional
        Output directory path
    tag : str, optional
        Tag to add to the end of the catalog JSON file
    depth : int, optional
        Directory depth to search
        
    Returns
    -------
    str
        Path to the saved catalog JSON file
    """
    assert kind in ["history", "timeseries"], "kind must be \"history\" or \"timeseries\""

    # Get the correct parsing function
    if ensemble == "pisom":
        if kind == "history":
            parser = parse_pisom_history
        else:
            parser = parse_pisom_timeseries
    elif ensemble == "fhist":
        if kind == "history":
            parser = parse_fhist_history
        else:
            raise NotImplementedError
    elif ensemble == "fssp370":
        raise NotImplementedError
    else:
        raise ValueError(f"{ensemble} not either 'pisom', 'fhist', or 'fssp370')")

    # Configuration for different catalog types
    config = {
        "history": {
            "depth": depth or 5,
            "exclude_patterns": ["*/proc/*", "*/rest/*"],
            "groupby_attrs": ["component", "stream", "frequency"],
            "aggregation": 
            [
                {
                    "type": "join_existing",
                    "attribute_name": "date",
                    "options": {
                        "dim": "time",
                        "coords": "minimal",
                        "compat": "override",
                    },
                },
                {
                    "type": "join_new",
                    "attribute_name": "member",
                    "options": {
                        "dim": "member",
                        "coords": "minimal",
                    },
                }
            ],
        },
        "timeseries": {
            "depth": depth or 2,
            "exclude_patterns": ["*/hist/*", "*/rest/*"],
            "groupby_attrs": ["variable", "component", "stream", "frequency"],
            "aggregation":
            [
                {
                    "type": "union",
                    "attribute_name": "variable",
                    "options": {}
                },
                {
                    "type": "join_new",
                    "attribute_name": "member",
                    "options": {
                        "dim": "member",
                        "coords": "minimal",
                    },
                }
            ],
        }
    }

    cfg = config[kind]

     # Convert to pathlib.Path and ensure directory exists
    outpath = pathlib.Path(outpath).resolve()

    # Create the catalog builder
    cat_builder = Builder(
        paths=rootpath,
        depth=cfg["depth"],
        exclude_patterns=cfg["exclude_patterns"],
        joblib_parallel_kwargs={"n_jobs": -1},
    )

    # Build the catalog
    if parallel.daskhelper.is_dask_available():
        with joblib.parallel_backend('dask', scatter=[parser]):
            cat_builder = cat_builder.build(parsing_func=parser)
    else:
        cat_builder = cat_builder.build(parsing_func=parser)

    # Clean the dataframe
    cat_builder.clean_dataframe()

    # For history files, expand variable lists into separate rows
    if kind == "history":
        df = cat_builder.df
        df = df.explode("variable").reset_index(drop=True)
        cat_builder.df = df

    # Save the catalog with proper time concatenation settings
    # TODO: add system name to catalog path so that each system (e.g., olympus, ncar) had its own catalog file
    catalog_path = f"{ensemble}_{kind}_catalog{tag}"
    cat_builder.save(
        name=catalog_path,
        directory=str(outpath),
        path_column_name="path", 
        variable_column_name="variable",
        data_format="netcdf",
        groupby_attrs=cfg["groupby_attrs"],
        aggregations=cfg["aggregation"],
    )

    return str((outpath / catalog_path).with_suffix('.json'))
