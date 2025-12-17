"""
Parsing functions for building intake-esm catalogs with ecg-tools.
"""
from __future__ import annotations

import pathlib
import traceback
import numpy as np
import xarray as xr
from ecgtools.builder import INVALID_ASSET, TRACEBACK

import coup_ppe.access.inspect
import coup_ppe.metadata.conventions


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
        info["component"] = coup_ppe.metadata.conventions.canonical_component(split[1])
        info["stream"] = split[2]
        info["variable"] = split[4]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True, engine="netcdf4") as ds:
            info["variable_long_name"] = ds[info["variable"]].attrs["long_name"]
            info["frequency"] = coup_ppe.access.inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_pisom_history(file: str | pathlib.Path) -> dict:
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
        info["component"] = coup_ppe.metadata.conventions.canonical_component(split[1])
        info["stream"] = split[2]
        info["date"] = split[3]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = coup_ppe.access.inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["variable"] = [var for var in ds if 'long_name' in ds[var].attrs]
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_fhist_history(file: str | pathlib.Path) -> dict:
    """Parses history files from FHIST PPE."""
    file = pathlib.Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")

        info["member"] = str(int(split[6]))
        info["component"] = coup_ppe.metadata.conventions.canonical_component(split[7])
        info["stream"] = split[8]
        info["date"] = split[9]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = coup_ppe.access.inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["variable"] = [var for var in ds if 'long_name' in ds[var].attrs]
        
        return info
    
    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
