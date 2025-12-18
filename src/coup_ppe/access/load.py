"""
Data loading utilities for PPEs.
"""
from __future__ import annotations

from typing import Callable
import pathlib
import numpy as np
import xarray as xr
import intake
import intake_esm.core

from coup_ppe.metadata.conventions import PISOM_MEMBERS, EnsembleType, validate_ensemble
import coup_ppe.access.config
import coup_ppe.metadata.members
import coup_ppe.registry.derived_variables


def _to_str_list(xs) -> list[str] | None:
    """Convert input to a list of strings."""
    if xs is None:
        return None
    if isinstance(xs, list):
        return [str(x) for x in xs]
    return [str(xs)] if isinstance(xs, (str, int)) else xs


def _shift_time(ds: xr.DataArray | xr.Dataset) -> xr.DataArray | xr.Dataset:
    """Shifts time coordinate from [startyear-02, endyear-01] to [startyear-02, (endyear-1)-12]"""
    if (ds.time[0].dt.month.item() == 2) and (ds.time[-1].dt.month.item() == 1):
        new_time = xr.date_range(
            start=str(ds.time[0].dt.year.item()) + "-01",
            end=str(ds.time[-1].dt.year.item() - 1) + "-12",
            freq="MS",
            calendar="noleap",
            use_cftime=True,
        )
        return ds.assign_coords(time=new_time)
    return ds


def query_catalog(
        ensemble: EnsembleType,
        varname: str | list[str],
        component: str,
        frequency: str,
        member: str | int | list[str | int] | None = None,
        stream: str | None = None,
        catalog_path: str | pathlib.Path | None = None,
    ) -> intake_esm.core.esm_datastores:
    """
    Query an intake-ESM catalog for CESM PPE data.

    Parameters
    ----------
    ensemble : EnsembleType
        The ensemble to query (e.g., 'pisom').
    varname : str | list[str]
        Variable name(s) to query from the catalog.
    component : str
        Model component (e.g., 'atm', 'ocn', 'ice', 'lnd').
    frequency : str
        Temporal frequency of the data (e.g., 'monthly', 'daily').
    member : str | int | list[str | int] | None, optional
        Ensemble member(s) to query. If None, defaults to all PISOM members.
    stream : str | None, optional
        CESM history stream to filter by (e.g., 'h0', 'h1'). If None, no stream filtering is applied.
    catalog_path : str | pathlib.Path | None, optional
        Path to the intake-ESM catalog JSON file. If None, uses the default catalog path
        constructed from ensemble name and filesystem configuration.
    Returns
    -------
    intake_esm.core.esm_datastore
        Subset of the catalog matching the query criteria.
    Notes
    -----
    - The function validates the ensemble type before querying.
    - For 'pisom' ensemble, member '2' is automatically excluded due to simulation issues.
    - Variable names and member IDs are converted to canonical string lists.
    - The catalog supports derived variables through the DVR (Derived Variable Registry).
    """
    validate_ensemble(ensemble)

    # Ensure varname and member are lists
    vars_to_load = _to_str_list(varname)
    if member is None:
        mems_to_load = PISOM_MEMBERS
    else:
        mems_to_load = _to_str_list(member)

    # Get the catalog path
    filesystem = coup_ppe.access.config.get_filesystem()
    catalog_root_path = coup_ppe.access.config.get_catalog_root_path()
    if not catalog_path:
        catalog_path = catalog_root_path / f"{ensemble}_timeseries_catalog_{filesystem}.json"
    else:
        catalog_path = pathlib.Path(catalog_path)
        assert catalog_path.suffix == '.json', f"File must be a JSON file, got {catalog_path.suffix}"

    # Get the derived variable registry
    dvr = coup_ppe.registry.derived_variables.DVR

    # Open catalog
    cat = intake.open_esm_datastore(catalog_path, registry=dvr)

    query = {
        "variable": vars_to_load,
        "component": component,
        "frequency": frequency,
    }

    # Parse the members
    if mems_to_load is not None:
        # Remove ensemble 0002 if pisom, there was an issue with this simulation so ignore
        if (ensemble == "pisom") and ("2" in mems_to_load):
            mems_to_load = [m for m in mems_to_load if m != "2"]
        query["member"] = coup_ppe.metadata.members.get_canonical_member_ids(mems_to_load, ensemble)
    
    # Select the streams if applicable
    if stream is not None:
        query["stream"] = stream

    # Search for the requested data
    cat_subset = cat.search(**query)
    
    return cat_subset


def load_output(
        ensemble: EnsembleType,
        varname: str | list[str],
        component: str,
        frequency: str,
        member: str | int | list[str | int] | None = None,
        catalog_path: str | pathlib.Path | None = None,
        xarray_open_kwargs: dict | None = None,
        chunks: dict | None = None,
        preprocess: Callable[[xr.Dataset], xr.Dataset] | None = None,
    ) -> xr.Dataset:
    """
    Load perturbed parameter ensemble (PPE) output.
    
    Parameters
    ----------
    ensemble : EnsembleType
        Name of the ensemble to load data from.
    varname : str | list[str]
        Variable name(s) to load from the ensemble.
    component : str
        Model component (e.g., 'atm', 'lnd').
    frequency : str
        Temporal frequency of the data (e.g., 'month_1', 'day_1').
    member : str | int | list[str | int] | None, optional
        Single string or list of ensemble members to load. Can be specified as
        parameter names ('kmax'), parameter name and min/max ('kmax,min'), or member
        ID numbers (15 or '15'). If None, all members are loaded.
    catalog_path : str | None, optional
        Path to the intake catalog. If None, uses the default catalog location.
    xarray_open_kwargs : dict | None, optional
        Additional keyword arguments to pass to xarray's open function.
    chunks : dict | None, optional
        Chunk sizes for dask arrays (e.g., {'time': 12, 'lat': 180}).
    preprocess : Callable[[xr.Dataset], xr.Dataset] | None, optional
        Preprocessing function to apply to each dataset before concatenation.
    
    Returns
    -------
    xr.Dataset
        Dataset containing the requested variable(s) across ensemble members.
    
    Examples
    --------
    >>> data = load_output(
    ...     varname=["TREFHT", "LHFLX"],
    ...     ensemble="pisom",
    ...     component="atm",
    ...     frequency="month_1",
    ...     member=["d_max", "fff,min"]
    ... )
    """
    validate_ensemble(ensemble)

    # Ensure varname is a list
    vars_to_load = _to_str_list(varname)
    assert vars_to_load is not None

    # Query the catalog and get the corresponding esm_datastore object
    cat_subset = query_catalog(
        ensemble=ensemble,
        varname=varname,
        component=component,
        frequency=frequency,
        member=member,
        catalog_path=catalog_path,
    )

    # Set up xarray kwargs
    open_kwargs = xarray_open_kwargs or {}
    if "use_cftime" not in open_kwargs:
        open_kwargs["use_cftime"] = True
    if chunks:
        open_kwargs["chunks"] = chunks
    if ensemble == "pisom":
        open_kwargs["drop_variables"] = ["var"]
    
    # Get rid of spurious warnings
    xr.set_options(use_new_combine_kwarg_defaults=True)

    # Set default preprocessing with a wrapper function
    def _preprocess(ds):
        if ensemble == "pisom":
            ds = ds.sel(time=slice("0050-01", "0184-12"))
        if preprocess is not None:
            ds = preprocess(ds)
        return ds

    # Load each variable separately to avoid coordinate conflicts
    datasets = []
    for var in vars_to_load:
        var_cat = cat_subset.search(variable=var)

        ds_dict = var_cat.to_dataset_dict(
            xarray_open_kwargs=open_kwargs,
            xarray_combine_by_coords_kwargs={"join": "outer"},
            preprocess=_preprocess,
            threaded=True,
            progressbar=False,
        )

        # Combine all keys for this variable
        var_ds = list(ds_dict.values())
        if len(var_ds) == 1:
            datasets.append(var_ds[0])
        else:
            # >> This logic could be brittle. <<
            # If len(var_ds) > 1, this is due to multiple streams (h0 and h1) from
            # the same component containing the same variable and frequency. Since
            # it ~should~ be identical output from CESM, just select the first dataset.
            datasets.append(var_ds[0])

            # var_ds_reset = [ds.reset_coords(drop=False) for ds in var_ds]
            # datasets.append(xr.merge(var_ds_reset, join="outer", compat="no_conflicts"))
    
    # Merge all variables together
    result = xr.merge(datasets, join="outer", compat="no_conflicts")

    # Sort the member dimension
    ms = np.sort(result.member.values.astype(int))
    result = result.sel(member=ms.astype(str))
    result["member"] = ms

    # Shift time (only relevant for certain pisom simulations)
    result = _shift_time(result)

    return result
