"""
Module for managing derived variables.
"""
from __future__ import annotations

import numpy as np
import xarray as xr
from intake_esm import DerivedVariableRegistry

import coup_ppe.metadata.constants as constants


DVR = DerivedVariableRegistry()

@DVR.register(variable='WIND_SPEED', query={'variable': ['U', 'V']})
def calc_wind_speed(ds):
    ds['WIND_SPEED'] = np.sqrt(ds.U ** 2 + ds.V ** 2)
    ds['WIND_SPEED'].attrs = {'units': 'm/s',
                             'long_name': 'wind speed',
                             'derived_by': 'intake-esm'}
    return ds

@DVR.register(variable='PRECT', query={'variable': ['PRECC', 'PRECL']})
def calc_prect(ds):
    ds['PRECT'] = ds.PRECC + ds.PRECL
    ds['PRECT'].attrs = {'units': 'm/s',
                         'long_name': 'total precipitation rate',
                         'derived_by': 'intake-esm'}
    return ds

@DVR.register(variable='MSE', query={'variable': ['T', 'Q', 'Z3']})
def calc_mse(ds):
    ds['MSE'] = ds.T * constants.CP + ds.Q * constants.LV + ds.Z3 * constants.G
    ds['MSE'].attrs = {'units': 'J/kg',
                       'long_name': 'vetically-resolved moist static energy',
                       'derived_by': 'intake-esm'}
    return ds

@DVR.register(variable='MSE850', query={'variable': ['T850', 'Q850', 'Z850']})
def calc_mse850(ds):
    ds['MSE850'] = ds.T850 * constants.CP + ds.Q850 * constants.LV + ds.Z850 * constants.G
    ds['MSE850'].attrs = {'units': 'J/kg',
                          'long_name': '850hPa moist static energy',
                          'derived_by': 'intake-esm'}
    return ds
