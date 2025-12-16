import os
import sys
from glob import glob
import time
from collections import namedtuple
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from dask_jobqueue import PBSCluster
from dask.distributed import Client


def get_ClusterClient(
        ncores=1,
        nmem='25GB',
        walltime='00:30:00',
        account='UWAS0155'):
    """
    Code from Daniel Kennedy
    More info about Dask on HPC - https://ncar.github.io/dask-tutorial/notebooks/05-dask-hpc.html
    """
    cluster = PBSCluster(
        cores=ncores,              # The number of cores you want
        memory=nmem,               # Amount of memory
        processes=ncores,          # How many processes
        queue='develop',            # Queue name
        resource_spec='select=1:ncpus=' +\
        str(ncores)+':mem='+nmem,  # Specify resources
        account=account,           # Input your project ID here
        walltime=walltime,         # Amount of wall time
        interface='hsn0',           # Interface to use
    )

    client = Client(cluster)
    return cluster, client


def _shift_time(da):
    if (da.time[0].dt.month.item() == 2) and (da.time[-1].dt.month.item() == 1):
        new_time = xr.date_range(
            start=str(da.time[0].dt.year.item()) + "-01",
            end=str(da.time[-1].dt.year.item() - 1) + "-12",
            freq="MS",
            calendar="noleap",
            use_cftime=True,
        )
        return da.assign_coords(time=new_time)
    return da


def _get_chunk_size_from_res(case):
    if "f09_f09" in case:
        chunk_size = {"time": 1024}
    else:
        chunk_size = {"time": 2048}
    return chunk_size


def check_frequency(ds):
    time_steps_per_year = len(ds.time) / (ds.time[-1].dt.year - ds.time[0].dt.year + 1)
    if time_steps_per_year == 12:
        freq = "monthly"
    elif time_steps_per_year == 1:
        freq = "yearly"
    else:
        freq = "unknown"
    return freq


def load_variables(varnames, case, basedir, domain="lnd", htape="h0"):
    def _keep_var(ds):
        return ds[varnames]

    component = {
        "lnd": "clm2",
        "atm": "cam",
    }

    data = xr.open_mfdataset(
        f"{basedir}/{case}/{domain}/hist/{case}.{component[domain]}.{htape}.*.nc",
        combine="by_coords",
        decode_timedelta=False,
        parallel=True,
        preprocess=_keep_var,
        chunks=_get_chunk_size_from_res(case),
        engine="netcdf4",
    )

    return _shift_time(data)



cluster, client = get_ClusterClient(nmem="1GB", walltime="04:00:00")
cluster.scale(20)
time.sleep(5)
print(client)
print(cluster.workers)


SIM_DIR = "/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/sims"
ARCH_DIR = "/glade/derecho/scratch/bbuchovecky/archive"
VARIABLES = [
    "TLAI", "TOTECOSYSC", "TOTVEGC", "TOTSOMC",                    # vegetation and carbon
    "EFLX_LH_TOT", "FSH", "FIRE", "FLDS", "FSR", "FSDS", "FGR",    # surface energy budget
    "TSA",                                                         # climate state
]

cplhist = load_variables(VARIABLES, "f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.cplhist", ARCH_DIR).sel(time=slice("1850-01", "1949-12"))
fh0 = glob(f"{ARCH_DIR}/f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.cplhist/lnd/hist/*.h0.*")
grid = xr.open_dataset(fh0[0], decode_timedelta=True, engine="netcdf4")[["area", "landfrac"]]
la = (grid.area * 1e6 * grid.landfrac).fillna(0).compute()  #m2
lw = (la / la.sum()).compute()

ConversionFactor = namedtuple("ConversionFactor", ["cf", "unit", "kind"])
cfs = {
    "TLAI": ConversionFactor(lw, "m2/m2", "intensive"),
    "TOTECOSYSC": ConversionFactor(la/1e15, "PgC", "extensive"),
    "TOTVEGC": ConversionFactor(la/1e15, "PgC", "extensive"),
    "TOTSOMC": ConversionFactor(la/1e15, "PgC", "extensive"),
    "EFLX_LH_TOT": ConversionFactor(lw, "W/m2", "intensive"),
    "FSH": ConversionFactor(lw, "W/m2", "intensive"),
    "FIRE": ConversionFactor(lw, "W/m2", "intensive"),
    "FLDS": ConversionFactor(lw, "W/m2", "intensive"),
    "FSR": ConversionFactor(lw, "W/m2", "intensive"),
    "FSDS": ConversionFactor(lw, "W/m2", "intensive"),
    "FGR": ConversionFactor(lw, "W/m2", "intensive"),
    "TSA": ConversionFactor(lw, "K", "intensive"),
}
labels = {
    "intensive": "mean",
    "extensive": "total"
}

for im in [1, 2]:
    MEM = f"coupPPE.{str(im).zfill(3)}"
    WDIR = f"/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/sims/{MEM}"

    ihist = load_variables(VARIABLES, f"IHistClm50Bgc.CPLHIST.historical.{MEM}.IHIST", ARCH_DIR)

    print(MEM)
    print("loaded variables")
    print(ihist.variables)
    print(cplhist.variables)


    # Figure 1: carbon (land ICs)
    fig, axes = plt.subplots(nrows=3, ncols=4, figsize=(20, 12), layout="tight")
    ax = axes.flatten()
    for i,v in enumerate(["TLAI", "TOTVEGC", "TOTSOMC"]):

        ihist_ann = (ihist[v]*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().compute()
        cplhist_ann = (cplhist[v]*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().compute()

        ihist_ann.plot(ax=ax[4*i], color="tab:blue", alpha=0.5, lw=0.75)
        ihist_ann.rolling(year=5, center=True).mean().plot(ax=ax[4*i], color="tab:blue", alpha=1, lw=1, label=MEM)
        cplhist_ann.plot(ax=ax[4*i], color="tab:orange", alpha=0.5, lw=0.75)
        cplhist_ann.rolling(year=5, center=True).mean().plot(ax=ax[4*i], color="tab:orange", alpha=1, lw=1, label="cplhist")

        ax[4*i].set_ylabel(f"{v} [{cfs[v].unit}]")
        ax[4*i].set_title(f"global annual {labels[cfs[v].kind]} {v}")
        ax[4*i].legend()

        ihist[v].sel(time=slice("1949-01", "1949-12")).mean(dim="time").plot(ax=ax[4*i+1], vmin=0, cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"})
        ax[4*i+1].set_title(f"{MEM} {v} 1949")

        cplhist[v].sel(time=slice("1949-01", "1949-12")).mean(dim="time").plot(ax=ax[4*i+2], vmin=0, cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"})
        ax[4*i+2].set_title(f"cplhist {v} 1949")

        (ihist[v].sel(time=slice("1949-01", "1949-12")) - cplhist[v].sel(time=slice("1949-01", "1949-12"))).mean(dim="time").plot(ax=ax[4*i+3], cmap="PRGn", robust=True, cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"})
        ax[4*i+3].set_title(f"{MEM}$-$cplhist {v} 1949")

        for j in range(1,4):
            ax[4*i+j].set_ylabel("")
            ax[4*i+j].set_xlabel("")

    fig.savefig(f"{SIM_DIR}/{MEM}/IHistClm50Bgc.CPLHIST.historical.{MEM}.IHIST.vegcarbon.png", dpi=300)


    # Figure 2: surface energy terms
    fig, axes = plt.subplots(nrows=2, ncols=4, figsize=(20, 8), layout="tight")
    ax = axes.flatten()
    for i,v in enumerate(["FLDS", "FIRE", "FSDS", "FSR", "EFLX_LH_TOT", "FSH", "FGR", "TSA"]):
        ihist_ann = (ihist[v]*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().compute()    
        cplhist_ann = (cplhist[v]*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().compute()

        ihist_ann.plot(ax=ax[i], color="tab:blue", alpha=0.5, lw=0.75)
        ihist_ann.rolling(year=5, center=True).mean().plot(ax=ax[i], color="tab:blue", alpha=1, lw=1, label=MEM)
        cplhist_ann.plot(ax=ax[i], color="tab:orange", alpha=0.5, lw=0.75)
        cplhist_ann.rolling(year=5, center=True).mean().plot(ax=ax[i], color="tab:orange", alpha=1, lw=1, label="cplhist")

        ax[i].set_ylabel(f"{v} [{cfs[v].unit}]")
        ax[i].set_title(f"global annual {labels[cfs[v].kind]} {v}")
        ax[i].legend()

    fig.savefig(f"{SIM_DIR}/{MEM}/IHistClm50Bgc.CPLHIST.historical.{MEM}.IHIST.sfcenergy.png", dpi=300)


client.shutdown()
del cluster
del client
dask_files = glob("dask-worker.*")
for f in dask_files:
    os.remove(f)
