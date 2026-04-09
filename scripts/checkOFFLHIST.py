import os
import sys
from glob import glob
from collections import namedtuple
from dask.distributed import Client, LocalCluster

import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as clr
import cmocean.cm as cmo


###########################
#### DEFINE CONSTANTS ####
###########################


CASE = sys.argv[1]
CASE_PREFIX = "i.e21.CPLHIST_BGC.f19_f19_mg17.historical"
SIM_DIR = "/glade/u/home/bbuchovecky/projects/coup_ppe/sims"
ARCH_DIR = "/glade/derecho/scratch/bbuchovecky/archive"
LND_VARIABLES = ["TLAI", "TOTVEGC", "EFLX_LH_TOT", "FCTR", "FCEV", "FGEV", "TSA"]
ATM_VARIABLES = ["TREFHT", "PS", "PRECT", "TMQ", "FSNT", "FLNT", "CLDTOT"]

CLIM_YEAR_RANGE = ["1995-01", "1999-12"]
MAP_YEAR_RANGE = ["1999-01", "1999-12"]


##########################
#### DEFINE FUNCTIONS ####
##########################


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


def load_variables(varnames, case, basedir, domain="lnd", htape="h0", suffix=""):
    def _keep_var(ds):
        if "PRECT" in varnames:
            x = ds["PRECC"] + ds["PRECL"]
            x = x.rename("PRECT").assign_attrs(units="m/s", long_name="calculated total precipitation rate (liq + ice)")
            other_varnames = [v for v in varnames if v != "PRECT"]
            if other_varnames:
                return xr.merge([ds[other_varnames], x])
            return x.to_dataset()
        return ds[varnames]

    component = {
        "lnd": "clm2",
        "atm": "cam",
    }

    if len(suffix) > 0:
        suffix = "."+suffix

    if varnames:
        data = xr.open_mfdataset(
            f"{basedir}/{case}{suffix}/{domain}/hist/{case}.{component[domain]}.{htape}.*.nc",
            combine="by_coords",
            decode_timedelta=False,
           parallel=True,
            preprocess=_keep_var,
           chunks=_get_chunk_size_from_res(case),
            engine="netcdf4",
        )
    else:
        data = xr.open_mfdataset(
            f"{basedir}/{case}{suffix}/{domain}/hist/{case}.{component[domain]}.{htape}.*.nc",
            combine="by_coords",
            decode_timedelta=False,
           parallel=True,
           chunks=_get_chunk_size_from_res(case),
            engine="netcdf4",
        )

    return _shift_time(data)


def plot_simple_diags(ihx, ih0, fhx, variables, tag, to_save=True):

    print("variables:", variables)

    fig, axes = plt.subplots(nrows=len(variables), ncols=4, figsize=(20, 4*len(variables)), layout="tight")
    ax = axes.flatten()

    for i,v in enumerate(variables):

        if v in cmaps["veg"]["vars"]:
            cm = cmaps["veg"]
        elif v in cmaps["water"]["vars"]:
            cm = cmaps["water"]
        elif v in cmaps["temp"]["vars"]:
            cm = cmaps["temp"]

        vmin = 0
        if v in ["TSA", "TREFHT"]:
            vmin = None

        ihx_ann = ((ihx[v]+cfs[v].cs)*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().persist()
        ih0_ann = ((ih0[v]+cfs[v].cs)*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().persist()
        fhx_ann = ((fhx[v]+cfs[v].cs)*cfs[v].cf).sum(dim=["lat","lon"]).groupby("time.year").mean().persist()

        ihx_ann.plot(ax=ax[4*i], color="tab:blue", alpha=0.75, lw=0.75)
        ihx_ann.rolling(year=5, center=True).mean().plot(ax=ax[4*i], color="tab:blue", alpha=1, lw=1, label=f"I {CASE}")
        ih0_ann.plot(ax=ax[4*i], color="tab:orange", alpha=0.75, lw=0.75)
        ih0_ann.rolling(year=5, center=True).mean().plot(ax=ax[4*i], color="tab:orange", alpha=1, lw=1, label="I coupPPE.000")
        fhx_ann.plot(ax=ax[4*i], color="tab:red", alpha=0.75, lw=0.75)
        fhx_ann.rolling(year=5, center=True).mean().plot(ax=ax[4*i], color="tab:green", alpha=1, lw=1, label=f"F {CASE}")

        ax[4*i].set_ylabel(f"{v} [{cfs[v].unit}]")
        ax[4*i].set_title(f"global annual {labels[cfs[v].kind]} {v}")
        ax[4*i].legend()

        ihx_clim = ((ihx[v]+cfs[v].cs)*cfs[v].cf).sel(time=slice(CLIM_YEAR_RANGE[0],CLIM_YEAR_RANGE[1])).sum(dim=["lat","lon"]).groupby("time.month").mean()
        ih0_clim = ((ih0[v]+cfs[v].cs)*cfs[v].cf).sel(time=slice(CLIM_YEAR_RANGE[0],CLIM_YEAR_RANGE[1])).sum(dim=["lat","lon"]).groupby("time.month").mean()
        fhx_clim = ((fhx[v]+cfs[v].cs)*cfs[v].cf).sel(time=slice(CLIM_YEAR_RANGE[0],CLIM_YEAR_RANGE[1])).sum(dim=["lat","lon"]).groupby("time.month").mean()

        ihx_clim.plot(ax=ax[4*i+1], color="tab:blue", label=f"I {CASE}")
        ih0_clim.plot(ax=ax[4*i+1], color="tab:orange", label="I coupPPE.000")
        fhx_clim.plot(ax=ax[4*i+1], color="tab:red", label=f"F {CASE}")

        ax[4*i+1].set_ylabel(f"{v} [{cfs[v].unit}]")
        ax[4*i+1].set_xlabel("month")
        ax[4*i+1].set_title(f"global clim {labels[cfs[v].kind]} {v} {CLIM_YEAR_RANGE[0][:4]}-{CLIM_YEAR_RANGE[1][:4]}")
        ax[4*i+1].legend()

        (ih0[v]+cfs[v].cs).sel(time=slice(MAP_YEAR_RANGE[0],MAP_YEAR_RANGE[1])).mean(dim="time").plot(ax=ax[4*i+2], vmin=vmin, cmap=cm["cont"], cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"})
        ax[4*i+2].set_title(f"{CASE} {v} {MAP_YEAR_RANGE[0][:4]}")
        
        ((ihx[v]+cfs[v].cs).sel(time=slice(MAP_YEAR_RANGE[0],MAP_YEAR_RANGE[1])) - (ih0[v]+cfs[v].cs).sel(time=slice(MAP_YEAR_RANGE[0],MAP_YEAR_RANGE[1]))).mean(dim="time").plot(ax=ax[4*i+3], cmap=cm["diff"], norm=clr.CenteredNorm(), robust=True, cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"})
        ax[4*i+3].set_title(f"{CASE[-3:]}$-$000 {v} {MAP_YEAR_RANGE[0][:4]}")

        for j in range(2,4):
            ax[4*i+j].set_ylabel("")
            ax[4*i+j].set_xlabel("")

    if to_save:
        fig.savefig(f"{SIM_DIR}/{CASE}/{CASE_PREFIX}.{CASE}.{tag}.png", dpi=300, bbox_inches="tight")
        plt.close()



if __name__ == "__main__":

    ####################
    #### SETUP DASK ####
    ####################

    NCPUS = 2  # must match allocated ncpus
    NMEM = 4   # must match allocated mem / ncpus (memory PER cpu)

    MEM_LIMIT = f"{NMEM*NCPUS}GB"
    tmpdir = os.environ.get("TMPDIR", "/tmp")

    print("Python version:", sys.version)
    print(f"ncpus: {NCPUS}")
    print(f"memory_per_worker: {MEM_LIMIT}")

    # Avoid oversubscription if libraries spawn processes internally
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")


    # Start local cluster
    cluster = LocalCluster(
        n_workers=NCPUS,
        # threads_per_worker=NCPUS,
        processes=True,             # use processes, not threads (this is nuanced...)
        memory_limit=MEM_LIMIT,     # per-worker memory limit
        local_directory=tmpdir,     # spill + temp files
        dashboard_address=None,     # no dashboard in batch
    )
    client = Client(cluster)
    print("Dask dashboard:", client.dashboard_link)
    print(f"Workers: {NCPUS}, Memory per worker: {MEM_LIMIT}")

    ################################
    #### CREATE VARIABLE LOOKUP ####
    ################################

    ih0 = glob(f"{ARCH_DIR}/{CASE_PREFIX}.coupPPE.000/lnd/hist/*.h0.*")
    grid = xr.open_dataset(ih0[0], decode_timedelta=True, engine="netcdf4")[["area", "landfrac"]]
    la = (grid.area * 1e6 * grid.landfrac).fillna(0)  #m2
    lw = la / la.sum()

    ConversionFactor = namedtuple("ConversionFactor", ["cf", "cs", "unit", "kind"])
    cfs = {
        "TLAI": ConversionFactor(lw, 0, "m2/m2", "intensive"),
        "TOTECOSYSC": ConversionFactor(la/1e15, 0, "PgC", "extensive"),
        "TOTVEGC": ConversionFactor(la/1e15, 0, "PgC", "extensive"),
        "TOTSOMC": ConversionFactor(la/1e15, 0, "PgC", "extensive"),
        "RAIN": ConversionFactor(lw, 0, "mm/s", "intensive"),
        "QRUNOFF": ConversionFactor(lw, 0, "mm/s", "intensive"),
        "QSOIL": ConversionFactor(lw, 0, "mm/s", "intensive"),
        "QVEGE": ConversionFactor(lw, 0, "mm/s", "intensive"),
        "QVEGT": ConversionFactor(lw, 0, "mm/s", "intensive"),
        "TWS": ConversionFactor(lw, 0, "mm", "intensive"),
        "EFLX_LH_TOT": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FCTR": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FCEV": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FGEV": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FSH": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FIRE": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FLDS": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FSR": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FSDS": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FGR": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "TSA": ConversionFactor(lw, -273.15, "degreeC", "intensive"),
        "TREFHT": ConversionFactor(lw, -273.15, "degreeC", "intensive"),
        "PS": ConversionFactor(lw, 0, "Pa", "intensive"),
        "FSNT": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "FLNT": ConversionFactor(lw, 0, "W/m2", "intensive"),
        "CLDTOT": ConversionFactor(lw, 0, "fraction", "intensive"),
        "PRECT": ConversionFactor(lw*1000*60*60*24, 0, "mm/day", "intensive"),
        "TMQ": ConversionFactor(lw, 0, "kg/m2", "intensive"),
    }
    labels = {
        "intensive": "mean",
        "extensive": "total"
    }
    cmaps = {
        "veg": {
            "diff": "PRGn",
            "cont": "viridis",
            "vars": ["TLAI", "TOTVEGC"]},
        "water": {
            "diff": cmo.curl_r,
            "cont": cmo.rain,
            "vars": ["EFLX_LH_TOT", "FCTR", "FCEV", "FGEV", "PRECT", "TMQ"]},
        "temp": {
            "diff": "RdBu_r",
            "cont": "inferno",
            "vars": ["TSA", "TREFHT", "PS", "FSNT", "FLNT", "CLDTOT"]},
    }

    ###########################
    #### START COMPUTATION ####
    ###########################

    ih0_lnd = load_variables(LND_VARIABLES, f"{CASE_PREFIX}.coupPPE.000", ARCH_DIR, domain="lnd")
    ihx_lnd = load_variables(LND_VARIABLES, f"{CASE_PREFIX}.{CASE}", ARCH_DIR, domain="lnd")
    fhx_lnd = load_variables(LND_VARIABLES, f"f.e21.FHIST_BGC.f19_f19_mg17.historical.{CASE}", ARCH_DIR, domain="lnd")

    # ih0_atm = load_variables(ATM_VARIABLES, f"{CASE_PREFIX}.coupPPE.000", ARCH_DIR, domain="atm")
    # ihx_atm = load_variables(ATM_VARIABLES, f"{CASE_PREFIX}.{CASE}", ARCH_DIR, domain="atm")

    plot_simple_diags(ihx_lnd, ih0_lnd, fhx_lnd, LND_VARIABLES, "lnd")
    # plot_simple_diags(ihx_atm, ih0_atm, ATM_VARIABLES, "atm")

    #########################
    #### END COMPUTATION ####
    #########################

    client.close()
    cluster.close()
