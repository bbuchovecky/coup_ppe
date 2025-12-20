"""
Parsing functions for building intake-esm catalogs with ecg-tools.
"""

from __future__ import annotations
from pathlib import Path
import traceback
import numpy as np
import xarray as xr
from ecgtools.builder import INVALID_ASSET, TRACEBACK

from xppe.access import inspect
from xppe.metadata import conventions


def parse_hadcm3(file: str | Path) -> dict:
    """Parses the timeseries files from HadCM3 PPE."""
    file = Path(file)
    info = {}

    try:
        # Naming conventions for HadCM3 PPE:
        #   control
        #       ensemble = agkv[a-p]
        #       standard = afwpf
        #   a1b
        #       ensemble = agmv[a-p]
        #       standard = afwpi

        stem = file.stem
        split = stem.split("_")
        name = split[-1]
        if name[:4] == "afwp":
            info["member"] = 0
        else:
            info["member"] = ord(name[-1]) - ord("a") + 1

        varname = "_".join(split[:-1])
        if varname == "var_0":
            info["variable"] = "leaf_area_index"
            info["preprocess"] = {
                "rename_vars": {"m01s19i014": "leaf_area_index"},
                "rename_dims": {
                    "pseudo_level": "pft",
                    "latitude": "lat",
                    "longitude": "lon",
                },
            }
        elif varname == "var_1":
            info["variable"] = "pft_fraction"
            info["preprocess"] = {
                "rename_vars": {"m01s19i013": "pft_fraction"},
                "rename_dims": {
                    "pseudo_level": "pft",
                    "latitude": "lat",
                    "longitude": "lon",
                },
            }
        else:
            info["variable"] = varname
            info["preprocess"] = {
                "rename_dims": {
                    "latitude": "lat",
                    "longitude": "lon",
                }
            }

        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()

        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_pisom_timeseries(file: str | Path) -> dict:
    """Parses timeseries files from PI SOM PPE."""
    file = Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")
        name = split[0].split("_")

        info["member"] = int(name[0][4:])
        if "v" in name[-1]:
            info["version"] = str(int(name[-1][1:]))
        else:
            info["version"] = ""
        info["component"] = conventions.canonical_component(split[1])
        info["stream"] = split[2]
        info["variable"] = split[4]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["variable_long_name"] = ds[info["variable"]].attrs["long_name"]
            info["frequency"] = inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()

        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_pisom_history(file: str | Path) -> dict:
    """Parses history files from PI SOM PPE."""
    file = Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")
        name = split[0].split("_")

        info["member"] = int(name[0][4:])
        if "v" in name[-1]:
            info["version"] = str(int(name[-1][1:]))
        else:
            info["version"] = np.nan
        info["component"] = conventions.canonical_component(split[1])
        info["stream"] = split[2]
        info["date"] = split[3]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["variable"] = [var for var in ds if "long_name" in ds[var].attrs]

        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


def parse_fhist_history(file: str | Path) -> dict:
    """Parses history files from FHIST PPE."""
    file = Path(file)
    info = {}

    try:
        stem = file.stem
        split = stem.split(".")

        info["member"] = int(split[6])
        info["component"] = conventions.canonical_component(split[7])
        info["stream"] = split[8]
        info["date"] = split[9]
        info["path"] = str(file)

        with xr.open_dataset(file, decode_timedelta=True, decode_times=True) as ds:
            info["frequency"] = inspect.infer_frequency(ds)
            info["time_steps"] = len(ds.time)
            info["start"] = ds.time.isel(time=0).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["end"] = ds.time.isel(time=-1).dt.strftime("%Y-%m-%d %H:%M:%S").item()
            info["variable"] = [var for var in ds if "long_name" in ds[var].attrs]

        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
