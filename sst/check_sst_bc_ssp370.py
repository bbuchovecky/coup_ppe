#!/glade/work/bbuchovecky/miniforge3/envs/data-sci-py312/bin/python3.12
"""
Create diagnostic plots of sea surface temperature and sea ice timeseries.
"""

from glob import glob
import numpy as np
import xarray as xr
import cftime
import matplotlib.pyplot as plt
import xclimate as xclim


BC_PATH = "/glade/campaign/univ/uwas0155/ppe/docn_sst/sst_bc_prediddle_2yr_transition_clim.f09_g17.HadOIBl.LE2-1231.005.nc"
FULL_TSLICE = slice("2015-01", "2017-02")

LENS2_MEMBER = "1231-005"
OUT_DIR = '/glade/campaign/univ/uwas0155/ppe/docn_sst'

BMB_VERSION  = 'cmip6'
LENS2_DIR = '/glade/campaign/collections/gdex/data/d651056/CESM2-LE/atm/proc/tseries/month_1'

SST_REGION_BNDS = {
    'glb': [[-90, 90], [0, 360]],
    'nat': [[47, 57], [315, 335]],
    'npa': [[30, 50], [150, 230]],
    'nep': [[35, 50], [190, 220]],
    'sep': [[-62, -47], [220, 290]],
    'ind': [[-15, 5], [60, 90]],
    'sop': [[-62, -47], [100, 140]],
    'so': [[-90, -50], [0, 360]],
}
SST_REGIONS_NAMES = {
    'glb': 'Global',
    'nat': 'N. Atlantic box',
    'npa': 'N. Pacific box',
    'nep': 'N. East Pacific box',
    'sep': 'S. East Pacific box',
    'ind': 'Indian Ocean box',
    'sop': 'Southern Ocean box',
    'so': 'Southern Ocean',
}

# https://www.gfdl.noaa.gov/arctic-sea-ice-predictions/
CICE_NH_LAT_BNDS = [67, 80]
CICE_NH_REGIONS = {
    'gin': [335, 18],
    'barents': [18, 57],
    'kara': [57, 103],
    'laptev': [103, 143],
    'eastsib': [143, 180],
    'chukchi': [180, 200],
    'beaufort': [200, 235],
    'okhotsk': [235, 287],
    'baffin': [287, 320],
    'central': [-1, 360],
}
CICE_NH_REGIONS_NAMES = {k: k.upper() for k in CICE_NH_REGIONS.keys()}

# https://doi.org/10.1029/2023GL105139
CICE_SH_LAT_BNDS = [-90, -55]
CICE_SH_REGIONS = {
    'indian': [20, 90],
    'weddel': [90, 160],
    'amundbell': [160, 230],
    'ross': [230, 300],
    'westpac': [300, 360],
    'pan': [-1, 360],
}
CICE_SH_REGIONS_NAMES = {k: k.upper() for k in CICE_SH_REGIONS.keys()}


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


def build_cice_region_masks(lat, lon, region_lon_bnds, lat_bnds, ocean_mask):
    """Build boolean masks for a dictionary of longitude-bounded regions."""
    region_masks = {}

    for name, lon_bnds in region_lon_bnds.items():
        if lon_bnds[0] > lon_bnds[1]:
            in_region = (
                (lat > lat_bnds[0])
                & (lat <= lat_bnds[1])
                & ((lon > lon_bnds[0]) | (lon <= lon_bnds[1]))
            )
        elif lon_bnds[0] == -1:
            in_region = (
                (lat > lat_bnds[1])
                & (lon > lon_bnds[0])
                & (lon <= lon_bnds[1])
            )
        elif lon_bnds[0] == -2:
            in_region = (
                (lat < lat_bnds[1])
                & (lon > lon_bnds[0])
                & (lon <= lon_bnds[1])
            )
        else:
            in_region = (
                (lat > lat_bnds[0])
                & (lat <= lat_bnds[1])
                & (lon > lon_bnds[0])
                & (lon <= lon_bnds[1])
            )

        region_masks[name] = in_region.rename(name).where(ocean_mask, other=0)

    return region_masks


def plot_regions(obs, lens, ts, regions, region_names, weights):
    """Plot area-weighted mean of regions."""
    if len(regions.keys()) > 3:
        ncol = 2
        nrow = int(np.ceil(len(regions.keys()) / ncol))
    else:
        ncol = 1
        nrow = len(regions.keys())

    fig, axs = plt.subplots(nrow, ncol, figsize=(4 * ncol, 2 * nrow), layout='constrained', sharex=True, dpi=150)
    ax = axs.ravel()

    for i, (name, reg) in enumerate(regions.items()):
        obs_reg = obs.where(reg).weighted(weights).mean(dim=['lat', 'lon']).compute()
        lens_reg = lens.where(reg).weighted(weights).mean(dim=['lat', 'lon']).compute()
        ts_reg = ts.where(reg).weighted(weights).mean(dim=['lat', 'lon']).compute()

        obs_reg.plot(ax=ax[i], c='tab:blue', ls='-', label='O(t)', marker='o', markersize=3)
        lens_reg.plot(ax=ax[i], c='tab:orange', ls='-', label='M(t)', marker='o', markersize=3)
        ts_reg.plot(ax=ax[i], c='tab:red', ls='-', label='SST(t)', marker='o', markersize=3)

        ax[i].axvline(cftime.DatetimeNoLeap(2015, 1, 1), color='k', lw=0.8, ls='-', zorder=0)
        ax[i].axvline(cftime.DatetimeNoLeap(2016, 1, 1), color='k', lw=0.8, ls='-', zorder=0)
        ax[i].axvline(cftime.DatetimeNoLeap(2017, 1, 1), color='k', lw=0.8, ls='-', zorder=0)

        ax[i].set_ylabel('')
        ax[i].set_xlabel('')
        ax[i].set_title(region_names[name])
        
    axs[0, -1].legend(loc='center left', bbox_to_anchor=(1.05, 0.5))
    return fig, axs


def main():
    ds = xr.open_dataset('/glade/campaign/collections/gdex/data/d651056/CESM2-LE/atm/proc/tseries/month_1/AREA/b.e21.BHISTcmip6.f09_g17.LE2-1001.001.cam.h0.AREA.195001-195912.nc')
    area = ds.AREA.isel(time=0).drop_vars('time')

    ds = xr.open_dataset('/glade/campaign/collections/gdex/data/d651056/CESM2-LE/lnd/proc/tseries/month_1/GPP/b.e21.BHISTcmip6.f09_g17.LE2-1301.006.clm2.h2.GPP.195001-195912.nc')
    ds = ds.reindex_like(area, tolerance=1e-3, method='nearest').drop_vars('time')
    om = np.isnan(ds.landfrac)
    oa = area.where(om, other=0)

    # Create SST regions
    sst_regions = {}
    for name, bnds in SST_REGION_BNDS.items():
        lat_bnds = bnds[0]
        lon_bnds = bnds[1]
        reg = ((area.lat >= lat_bnds[0]) & (area.lat < lat_bnds[1]) & (area.lon >= lon_bnds[0]) & (area.lon < lon_bnds[1]))
        sst_regions[name] = reg.where(om).rename(name)
    
    # Create CICE regions
    lat = area.lat
    lon = area.lon
    cice_nh_iregmask = xr.zeros_like(area, dtype=np.int16)
    cice_sh_iregmask = xr.zeros_like(area, dtype=np.int16)
    cice_nh_region_ids = {}
    cice_sh_region_ids = {}

    cice_nh_regions = build_cice_region_masks(lat, lon, CICE_NH_REGIONS, CICE_NH_LAT_BNDS, om)
    for ireg, (name, in_region) in enumerate(cice_nh_regions.items(), start=1):
        cice_nh_iregmask = xr.where(in_region, ireg, cice_nh_iregmask)
        cice_nh_region_ids[name] = ireg

    cice_sh_regions = build_cice_region_masks(lat, lon, CICE_SH_REGIONS, CICE_SH_LAT_BNDS, om)
    for ireg, (name, in_region) in enumerate(cice_sh_regions.items(), start=1):
        cice_sh_iregmask = xr.where(in_region, ireg, cice_sh_iregmask)
        cice_sh_region_ids[name] = ireg

    # Load LENS2 member
    branch_year, member_str = LENS2_MEMBER.split("-")
    lens_sst = load_member("SST", branch_year, member_str) - 273.15
    lens_cice = load_member("ICEFRAC", branch_year, member_str)

    # Load HadOI
    hadbc = xr.open_dataset('/glade/campaign/cesm/cesmdata/inputdata/atm/cam/sst/sst_HadOIBl_bc_0.9x1.25_1850_2022_c241003.nc')
    hadbc = hadbc.sel(time=FULL_TSLICE).assign_coords(time=lens_sst.time)
    obs_sst = hadbc.SST_cpl_prediddle.sel(time=FULL_TSLICE).reindex_like(area, tolerance=1e-3, method='nearest')
    obs_cice = hadbc.ice_cov_prediddle.sel(time=FULL_TSLICE).reindex_like(area, tolerance=1e-3, method='nearest')

    # Load custom BCs
    bc = xr.open_dataset(BC_PATH).sel(time=FULL_TSLICE)

    # Plot SST
    fig, axs = plot_regions(obs_sst, lens_sst, bc.SST_cpl_prediddle, sst_regions, SST_REGIONS_NAMES, oa)
    for i in range(2):
        axs[i, 0].set_ylabel('SST [deg_C]')
    fig.savefig(f'{".".join(BC_PATH.rsplit("/", maxsplit=1)[-1].split(".")[:-1])}.SST.png', dpi=300, bbox_inches='tight')

    # Plot CICE
    fig, axs = plot_regions(obs_cice, lens_cice, bc.ice_cov_prediddle, cice_nh_regions, CICE_NH_REGIONS_NAMES, oa)
    for i in range(2):
        axs[i, 0].set_ylabel('ICEFRAC [1]')
    fig.savefig(f'{".".join(BC_PATH.rsplit("/", maxsplit=1)[-1].split(".")[:-1])}.ICE_arctic.png', dpi=300, bbox_inches='tight')

    fig, axs = plot_regions(obs_cice, lens_cice, bc.ice_cov_prediddle, cice_sh_regions, CICE_SH_REGIONS_NAMES, oa)
    for i in range(axs.shape[1]):
        axs[i, 0].set_ylabel('ICEFRAC [1]')
    fig.savefig(f'{".".join(BC_PATH.rsplit("/", maxsplit=1)[-1].split(".")[:-1])}.ICE_antarctic.png', dpi=300, bbox_inches='tight')

if __name__ == '__main__':
    main()
