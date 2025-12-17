"""
Data loading utilities for CESM perturbed parameter ensemble (PPE) experiments.
This module provides functions to load and access output from various PPE experiments,
including PI SOM PPE, FHIST PPE, and FSSP370 PPE simulations. It uses intake-esm
catalogs to efficiently query and load model output data.
Ensemble Short Names
--------------------
- PI SOM PPE:    pi_som
- FHIST PPE:     hist_sst
- FSSP370 PPE:   ssp_sst
Functions
---------
load_ppe
    Load perturbed parameter ensemble output for specified variables and members.
Notes
-----
This module is designed to work with CESM model output stored in standardized
formats and indexed by intake-esm catalogs. Support for derived variables and
custom preprocessing functions is included.
>>> from coup_ppe.access.load import load_ppe
"""
from typing import Callable
import pathlib
import xarray as xr
import intake
import intake_esm.derived
from intake_esm import DerivedVariableRegistry

import access.paths
import metadata.members


def load_ppe(
        ensemble: str,
        varname: str | list[str],
        component: str,
        frequency: str,
        member: str | int | list[str | int] | None = None,
        catalog_path: str | None = None,
        derived_registry: str | None = None,
        xarray_open_kwargs: dict | None = None,
        chunks: dict | None = None,
        preprocess: Callable[[xr.Dataset], xr.Dataset] | None = None,
    ) -> xr.Dataset:
    """
    Load perturbed parameter ensemble (PPE) output.
    
    Parameters
    ----------
    ensemble : str
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
    derived_registry : intake_esm.derived.DerivedVariableRegistry | None, optional
        DerivedVariableRegistry object for the derived variable functionality of
        intake-esm. If None, tries to use the default DerivedVariableRegistry.
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
    >>> data = load_ppe(
    ...     varname=["TREFHT", "LHFLX"],
    ...     ensemble="pisom",
    ...     component="atm",
    ...     frequency="month_1",
    ...     member=["dmax", "fff,min"]
    ... )
    """

    # Ensure varname and member are lists
    vars_to_load = [varname] if isinstance(varname, str) else varname
    mems_to_load = [member] if isinstance(member, (int, str)) else member

    # TODO:
    # implement logic to find default catalog_path if not provided, put default paths in metadata
    # should it look for timeseries or history catalog?
    if not catalog_path:
        # cat_path = access.paths.DEFAULT_CATALOG_PATH[ensemble]
        raise NotImplementedError
    else:
        cat_path = pathlib.Path(catalog_path)
        assert cat_path.suffix == '.json', f"File must be a JSON file, got {cat_path.suffix}"

    # TODO: implement DerivedVariableRegistry
    if not derived_registry:
        # derived_registry = derived.derived_variable_registry.DVR
        pass
    else:
        assert isinstance(derived_registry, intake_esm.derived.DerivedVariableRegistry)

    # Open catalog
    cat = intake.open_esm_datastore(cat_path, registry=derived_registry)

    # Parse the members
    if mems_to_load is not None:
        query = dict(
            variable=vars_to_load,
            component=component,
            frequency=frequency,
            member=metadata.members.get_canonical_member_ids(mems_to_load, ensemble)
        )
    else:
        query = dict(
            variable=vars_to_load,
            component=component,
            frequency=frequency,
        )

    # Search for the requested data
    cat_subset = cat.search(**query)

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

    # Load each variable separately to avoid coordinate conflicts
    datasets = []
    for var in vars_to_load:
        var_cat = cat_subset.search(variable=var)

        ds_dict = var_cat.to_dataset_dict(
            xarray_open_kwargs=open_kwargs,
            xarray_combine_by_coords_kwargs={"join": "outer"},
            preprocess=preprocess,
            threaded=True,
            progressbar=False,
        )

        # Combine all keys for this variable
        var_ds = list(ds_dict.values())
        if len(var_ds) == 1:
            datasets.append(var_ds[0])
        else:
            datasets.append(xr.merge(var_ds))
    
    # Merge all variables together
    result = xr.merge(datasets)

    return result
