"""
Docstring for coup_ppe.access.inspect
"""

from __future__ import annotations
import numpy as np
import pandas as pd
import xarray as xr


def infer_frequency(
    ds: xr.DataArray | xr.Dataset, time_name: str = "time"
) -> str | None:
    """
    Infer the time frequency of a dataset from its time coordinate.

    Parameters
    ----------
    ds : xr.DataArray or xr.Dataset
        Dataset with time dimension
    time_name : str
        Name of the time dimension (default: "time")

    Returns
    -------
    str or float or None
        Frequency as "hour_3", "hour_6", "day", "month", "year", numeric value, or None
    """
    assert isinstance(
        ds, (xr.DataArray, xr.Dataset)
    ), "only takes xarray.DataArray or xarray.Dataset"
    assert time_name in ds.dims, f"{time_name} needs to be a dimension"

    # Inspect time deltas
    if len(ds[time_name]) > 1:

        # Define reference periods (in days) and tolerances
        periods = {
            "hour_3": (0.125, 0.01),
            "hour_6": (0.25, 0.01),
            "day_1": (1, 0.01),
            "month_1": (30.42, 0.1),
            "year_1": (365, 0.01),
        }

        # Compute the mode of the finite difference of the time coordinate
        dtime = ds.time.diff(time_name) / pd.Timedelta(days=1)
        vals, counts = np.unique(dtime, return_counts=True)
        dtime = vals[np.argmax(counts)]

        # Match to known periods
        for period_name, (period_days, tolerance) in periods.items():
            if abs(dtime - period_days) < (tolerance * period_days):
                return period_name

        return dtime

    # Fallback to metadata attribute
    freq = ds.attrs.get("time_period_freq")
    if freq is None:
        return None
    return freq
