#!/glade/work/bbuchovecky/miniforge3/envs/data-sci-py312/bin/python3.12
"""
Generate the sea surface temperature and sea ice timeseries from HadOI
and LENS2 to prescribe for a SSP3-7.0 F-case.
"""

import warnings
from glob import glob
from datetime import datetime
import numpy as np
import xarray as xr
import xclimate as xclim


LENS2_MEMBER = "1231-005"
RELAX_NYEAR = 1
OUT_DIR = '/glade/campaign/univ/uwas0155/ppe/docn_sst'

BMB_VERSION  = 'cmip6'
LENS2_DIR = '/glade/campaign/collections/gdex/data/d651056/CESM2-LE/atm/proc/tseries/month_1'
FULL_TSLICE = slice("2015-01", "2100-12")
OVERLAP_TSLICE = slice("2015-01", "2020-12")


def split_years_months(da):
    assert 'time' in da.dims, ValueError("da must have a time coordinate")
    assert da.time.size % 12 == 0, ValueError("time coordinate must be a multiple of 12")

    name = da.name
    years  = np.unique(da.time.dt.year.values)
    months = np.unique(da.time.dt.month.values)
    
    # Rechunk time dim contiguously before reshape
    da = da.chunk({'time': -1})
    
    new_shape = (len(years), len(months)) + da.shape[1:]
    new_dims  = ['year', 'month'] + list(da.dims[1:])

    extra_coords = {k: v for k, v in da.coords.items()
                    if 'time' not in v.dims}
    
    return xr.DataArray(
        da.data.reshape(new_shape),   # dask-safe
        dims=new_dims,
        coords={'year': years, 'month': months, **extra_coords}
    ).rename(name)


def combine_years_months(da, freq='MS'):
    name = da.name
    years  = da.year.values   # shape (Y,)
    months = da.month.values  # shape (M,)

    # Build full Y×M time index, then flatten
    time_index = xr.date_range(               # or pd.date_range if using numpy datetimes
        start=f'{years[0]}-{months[0]:02d}',
        periods=len(years) * len(months),
        freq=freq,
        use_cftime=True
    )

    # Flatten year×month into time, preserve trailing spatial dims
    trailing_dims   = list(da.dims[2:])          # e.g. ['lat', 'lon']
    trailing_shape  = da.shape[2:]
    flat_data = da.data.reshape(len(time_index), *trailing_shape)  # dask-safe

    # Preserve non-(year,month) coords
    extra_coords = {k: v for k, v in da.coords.items()
                    if not any(d in ('year', 'month') for d in v.dims)}

    return xr.DataArray(
        flat_data,
        dims=['time'] + trailing_dims,
        coords={'time': time_index, **extra_coords}
    ).rename(name)


def load_member(v: str, yr: str, mem_str: str) -> xr.DataArray:
    """Open, time-concat, reindex, and mask one ensemble member."""
    files = sorted(
        glob(f'{LENS2_DIR}/{v}/b.e21.BHIST{BMB_VERSION}.f09_g17.LE2-{yr}.{mem_str}.cam.h0.{v}.*.nc') +
        glob(f'{LENS2_DIR}/{v}/b.e21.BSSP370{BMB_VERSION}.f09_g17.LE2-{yr}.{mem_str}.cam.h0.{v}.*.nc')
    )
    ds = xr.open_mfdataset(
        files,
        parallel=True,
        combine='by_coords',
        data_vars='minimal',
        coords='minimal',
        compat='override',
    )
    return (
        xclim.time_coord.shift_time(ds)
        .sel(time=FULL_TSLICE)[v]
    )


def cos_smooth_weights(n):
    t = np.linspace(0, 1, n + 1)[1:]  # t \in (0, 1], excludes t=0
    return 0.5 * (1 + np.cos(np.pi * t))


def build_relax_param_clim(weights, overlap):
    nyear = len(overlap.year)
    relax_nyear = len(weights)
    return xr.DataArray(
        data=np.concatenate(
            [
                np.stack(12 * [weights], axis=1),
                np.full((nyear - relax_nyear, 12), 0),
            ]
        ),
        coords={"year": overlap.year, "month": overlap.month},
    )


def build_timeseries(lens, obs, weights):
    return lens + weights * (obs - lens)


def fill_nans_with_zonal_mean(da, lon_dim='lon', lat_dim='lat'):
    """Fill NaNs at each time/latitude with that latitude's zonal mean.

    If a latitude's zonal mean is NaN (e.g. an all-land latitude), interpolate
    the zonal mean along the latitude dimension from surrounding valid
    latitudes (linear then nearest) and use that to fill the original data.
    """
    if lon_dim not in da.dims:
        raise ValueError(f"{lon_dim} must be in data dimensions")

    # Compute zonal mean (drops the lon dimension)
    zonal = da.mean(dim=lon_dim, skipna=True)

    # Detect latitude dim if not provided or named differently
    if lat_dim not in zonal.dims:
        lat_candidates = [d for d in zonal.dims if d.lower().startswith('lat')]
        if lat_candidates:
            lat_dim = lat_candidates[0]
        else:
            raise ValueError("latitude dimension not found in data")

    # Interpolate along latitude to fill latitudes that have NaN zonal means.
    # Try linear interpolation across latitudes, then forward/back-fill to
    # cover endpoints (poles) that have no valid neighbors, and finally
    # fall back to nearest if any gaps remain.
    zonal_interp = zonal.interpolate_na(dim=lat_dim, method='linear')

    # Fill extended gaps at the ends (polar rows) by propagating nearest
    # valid latitude values outward.
    zonal_interp = zonal_interp.ffill(dim=lat_dim).bfill(dim=lat_dim)

    # As a last resort, ensure any remaining NaNs are filled with nearest
    zonal_interp = zonal_interp.interpolate_na(dim=lat_dim, method='nearest')

    # Broadcast back to original dims when filling
    return da.fillna(zonal_interp)


def main():

    warnings.filterwarnings(
        "ignore",
        message="Sending large graph of size",
        category=UserWarning,
        module="distributed.client",
    )

    client_cluster = xclim.create_dask_cluster(
        account='UWAS0155',
        nworkers=4,
        ncores=1,
        nmem='2GB',
        walltime='00:30:00'
    )

    # Load ocean mask
    ds = xr.open_dataset('/glade/campaign/collections/gdex/data/d651056/CESM2-LE/atm/proc/tseries/month_1/AREA/b.e21.BHISTcmip6.f09_g17.LE2-1001.001.cam.h0.AREA.195001-195912.nc')
    area = ds.AREA.isel(time=0).drop_vars('time')

    ds = xr.open_dataset('/glade/campaign/collections/gdex/data/d651056/CESM2-LE/lnd/proc/tseries/month_1/GPP/b.e21.BHISTcmip6.f09_g17.LE2-1301.006.clm2.h2.GPP.195001-195912.nc')
    ds = ds.reindex_like(area, tolerance=1e-3, method='nearest').drop_vars('time')
    om = np.isnan(ds.landfrac)
    
    # Load HadOI
    hadbc_path = '/glade/campaign/cesm/cesmdata/inputdata/atm/cam/sst/sst_HadOIBl_bc_0.9x1.25_1850_2022_c241003.nc'
    hadbc = xr.open_dataset(hadbc_path)
    obs_sst = hadbc.SST_cpl_prediddle.sel(time=OVERLAP_TSLICE).reindex_like(area, tolerance=1e-3, method='nearest')
    obs_cice = hadbc.ice_cov_prediddle.sel(time=OVERLAP_TSLICE).reindex_like(area, tolerance=1e-3, method='nearest') * 100

    # Load LENS2 member
    branch_year, member_str = LENS2_MEMBER.split("-")
    lens_sst = load_member("SST", branch_year, member_str).where(om) - 273.15
    lens_cice = load_member("ICEFRAC", branch_year, member_str).where(om) * 100

    # Define overlap timeseries
    lens_sst_overlap = lens_sst.sel(time=OVERLAP_TSLICE)
    lens_cice_overlap = lens_cice.sel(time=OVERLAP_TSLICE)

    # Split (time) into (month,year)
    obs_sst_my = split_years_months(obs_sst)
    obs_cice_my = split_years_months(obs_cice)
    lens_sst_overlap_my = split_years_months(lens_sst_overlap)
    lens_cice_overlap_my = split_years_months(lens_cice_overlap)

    # Create cosine weights
    rw_clim = cos_smooth_weights(RELAX_NYEAR + 1)

    ## Build SST timeseries for overlap period
    rp_clim_my =  build_relax_param_clim(rw_clim, lens_sst_overlap_my)
    sst_overlap_my = build_timeseries(lens_sst_overlap_my, obs_sst_my, rp_clim_my)
    sst_overlap = combine_years_months(sst_overlap_my)

    # Build CICE timeseries for overlap period
    rp_clim_my =  build_relax_param_clim(rw_clim, lens_cice_overlap_my)
    cice_overlap_my = build_timeseries(lens_cice_overlap_my, obs_cice_my, rp_clim_my).clip(min=0, max=1)
    cice_overlap = combine_years_months(cice_overlap_my)

    # Common attributes
    attrs = {
        'method': f'{RELAX_NYEAR}-year cosine-smoothed relaxation over 2015-{2015+RELAX_NYEAR}, transition from HadOI to LENS2 {LENS2_MEMBER}, use prediddled HadOI fields',
        'HadOI': hadbc_path,
        'LENS2_path': LENS2_DIR,
        'LENS2_SST_file': f'b.e21.BHIST{BMB_VERSION}.f09_g17.LE2-{branch_year}.{member_str}.cam.h0.SST.*.nc',
        'LENS2_ICEFRAC_file': f'b.e21.BHIST{BMB_VERSION}.f09_g17.LE2-{branch_year}.{member_str}.cam.h0.ICEFRAC.*.nc',
        'history': f'created on {datetime.now().strftime("%m/%d/%y %H:%M:%S")}',
    }
    
    # Concatenate into single timeseries from 2015-2100
    (
        xr.concat(
            [
                sst_overlap,
                lens_sst.sel(time=slice(OVERLAP_TSLICE.stop, None)).isel(time=slice(1, None)),
            ],
            dim='time',
        )
        .assign_coords(time=lens_sst.time)
        .pipe(fill_nans_with_zonal_mean)
        .rename('SST')
        .assign_attrs(
            {
                'long_name': 'sea surface temperature',
                'units': 'deg_C',
            }
        )
        .to_dataset()
        .assign_attrs(attrs)
        .to_netcdf(f'{OUT_DIR}/sst_prediddle_{RELAX_NYEAR}yr_clim_relax.f09_g17.HadOIBl_c241003.LE2_{branch_year}_{member_str}.nc')
    )

    (
        xr.concat(
            [
                cice_overlap,
                lens_cice.sel(time=slice(OVERLAP_TSLICE.stop, None)).isel(time=slice(1, None)),
            ],
            dim='time',
        )
        .assign_coords(time=lens_cice.time)
        .pipe(fill_nans_with_zonal_mean)
        .clip(min=0, max=1)
        .rename('SEAICE')
        .assign_attrs(
            {
                'long_name': 'sea-ice concentration',
                'units': 'fraction',
            }
        )
        .to_dataset()
        .assign_attrs(attrs)
        .to_netcdf(f'{OUT_DIR}/ice_prediddle_{RELAX_NYEAR}yr_clim_relax.f09_g17.HadOIBl_c241003.LE2_{branch_year}_{member_str}.nc')
    )

    xclim.close_dask_cluster(client_cluster)


if __name__ == '__main__':
    main()
