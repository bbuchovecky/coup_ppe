#!/glade/work/bbuchovecky/miniforge3/envs/data-sci-py312/bin/python3.12

"""
Generates a summary diagnostic figure for the OFFLHIST simulations.
"""

import argparse
import sys
from glob import glob
from collections import namedtuple
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as clr
import cmocean.cm as cmo


# --- I/O utilities -----------------------------------------------------------

def _shift_time(da):
    """
    Correct time coordinate when CLM5 outputs month-end timestamps.
    Detects the case where the first timestamp is February and the last is
    January (i.e., shifted by one month), and reassigns the coordinate to
    calendar-aligned month-start values.
    """
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


def check_frequency(ds):
    """
    Infer temporal frequency of a dataset from the number of time steps per year.
    Returns 'monthly', 'yearly', or 'unknown'.
    """
    time_steps_per_year = len(ds.time) / (ds.time[-1].dt.year - ds.time[0].dt.year + 1)
    if time_steps_per_year == 12:
        freq = "monthly"
    elif time_steps_per_year == 1:
        freq = "yearly"
    else:
        freq = "unknown"
    return freq


def load_variables(
    varnames,
    case,
    basedir,
    domain="lnd",
    htape="h0",
    suffix="",
    chunks=None,
):
    """
    Load one or more variables from CESM2 history files using xarray.open_mfdataset.

    Parameters
    ----------
    varnames : list of str
        Variable names to load. If 'PRECT' is included, it is computed as
        PRECC + PRECL (convective + large-scale precipitation rates).
    case : str
        CESM2 case name used to construct the file path pattern.
    basedir : str
        Base archive directory (e.g., GLADE scratch or campaign storage).
    domain : {'lnd', 'atm'}
        Model component domain; determines component string in filename.
    htape : str
        History tape identifier (e.g., 'h0', 'h1').
    suffix : str
        Optional suffix appended to the case directory name.

    Returns
    -------
    xr.Dataset
        Dataset with time coordinate shifted if necessary (see _shift_time).
    """
    def _keep_var(ds):
        if "PRECT" in varnames:
            x = ds["PRECC"] + ds["PRECL"]
            x = x.rename("PRECT").assign_attrs(
                units="m/s",
                long_name="calculated total precipitation rate (liq + ice)"
            )
            other_varnames = [v for v in varnames if v != "PRECT"]
            if other_varnames:
                return xr.merge([ds[other_varnames], x])
            else:
                return x.to_dataset()
        return ds[varnames]

    component = {
        "lnd": "clm2",
        "atm": "cam",
    }

    if len(suffix):
        suffix = "." + suffix

    preprocess = _keep_var if varnames else None
    if chunks is None:
        chunks = {"time": -1}

    # Deal with 005 and 006
    dir_case = case
    file_case = case[:-9] if case[-8:] == "no_nlmod" else case

    print(f"{basedir}/{dir_case}{suffix}/{domain}/hist/{file_case}.{component[domain]}.{htape}.*.nc")

    data = xr.open_mfdataset(
        f"{basedir}/{dir_case}{suffix}/{domain}/hist/{file_case}.{component[domain]}.{htape}.*.nc",
        combine="by_coords",
        chunks=chunks,
        decode_timedelta=False,
        preprocess=preprocess,
        engine="netcdf4",
    )

    return _shift_time(data)


# --- Diagnostics plotting ----------------------------------------------------

def plot_simple_diags(ihx, ih0, fhx, variables, tag, to_save=True):
    """
    Produce a four-panel diagnostic figure for each variable:
      col 0: global annual time series (land-area weighted sum or mean)
      col 1: climatological seasonal cycle over CLIM_YEAR_RANGE
      col 2: spatial map of ih0 climatology over MAP_YEAR_RANGE
      col 3: spatial map of (ihx - ih0) difference over MAP_YEAR_RANGE

    Comparisons are between:
      ihx  : interactive run, perturbed member (CASE)
      ih0  : interactive run, control member (coupPPE.000)
      fhx  : offline (FHIST) run, perturbed member (CASE)

    Parameters
    ----------
    ihx, ih0, fhx : xr.Dataset
        Datasets for the three simulations, each containing the variables
        listed in `variables`.
    variables : list of str
        Variable names to plot; must be keys in the module-level `cfs` dict.
    tag : str
        String appended to the output filename (e.g., 'lnd' or 'atm').
    to_save : bool
        If True, save figure to disk and close; otherwise display inline.
    """
    print("variables:", variables)

    fig, axes = plt.subplots(
        nrows=len(variables),
        ncols=4,
        figsize=(20, 4 * len(variables)),
        layout="tight",
    )
    ax = axes.flatten()

    for i, v in enumerate(variables):
        print(v)

        cm = cmaps["temp"]
        if v in cmaps["veg"]["vars"]:
            cm = cmaps["veg"]
        elif v in cmaps["water"]["vars"]:
            cm = cmaps["water"]
        elif v in cmaps["temp"]["vars"]:
            cm = cmaps["temp"]

        vmin = 0
        if v in ["TSA", "TREFHT"]:
            vmin = None

        # --- Time series (col 0) ---
        # (v + cs) * cf applies a shift (cs) and scaling (cf) to convert units;
        # .sum over (lat, lon) gives global integral or weighted mean depending on cf.
        ihx_ann = ((ihx[v] + cfs[v].cs) * cfs[v].cf).sum(dim=["lat", "lon"]).groupby("time.year").mean().chunk({"year": -1})
        ih0_ann = ((ih0[v] + cfs[v].cs) * cfs[v].cf).sum(dim=["lat", "lon"]).groupby("time.year").mean().chunk({"year": -1})
        fhx_ann = ((fhx[v] + cfs[v].cs) * cfs[v].cf).sum(dim=["lat", "lon"]).groupby("time.year").mean().chunk({"year": -1})

        smooth_window = max(1, min(5, int(ihx_ann.sizes.get("year", 1))))

        ih0_ann.plot(ax=ax[4 * i], color="tab:orange", alpha=0.75, lw=0.75)
        ih0_ann.rolling(year=smooth_window, center=True, min_periods=1).mean().plot(
            ax=ax[4 * i], color="tab:orange", alpha=1, lw=1.5, label="I coupPPE.000"
        )
        ihx_ann.plot(ax=ax[4 * i], color="tab:blue", alpha=0.75, lw=0.75)
        ihx_ann.rolling(year=smooth_window, center=True, min_periods=1).mean().plot(
            ax=ax[4 * i], color="tab:blue", alpha=1, lw=1.5, label=f"I {CASE}"
        )
        fhx_ann.plot(ax=ax[4 * i], color="tab:red", alpha=0.75, lw=0.75)
        fhx_ann.rolling(year=smooth_window, center=True, min_periods=1).mean().plot(
            ax=ax[4 * i], color="tab:green", alpha=1, lw=1.5, label=f"F {CASE}"
        )

        ax[4 * i].set_ylabel(f"{v} [{cfs[v].unit}]")
        ax[4 * i].set_title(f"global annual {labels[cfs[v].kind]} {v}")
        ax[4 * i].legend()

        # --- Seasonal cycle (col 1) ---
        ihx_clim = (
            (ihx[v] + cfs[v].cs) * cfs[v].cf
        ).sel(time=slice(CLIM_YEAR_RANGE[0], CLIM_YEAR_RANGE[1])).sum(dim=["lat", "lon"]).groupby("time.month").mean()
        ih0_clim = (
            (ih0[v] + cfs[v].cs) * cfs[v].cf
        ).sel(time=slice(CLIM_YEAR_RANGE[0], CLIM_YEAR_RANGE[1])).sum(dim=["lat", "lon"]).groupby("time.month").mean()
        fhx_clim = (
            (fhx[v] + cfs[v].cs) * cfs[v].cf
        ).sel(time=slice(CLIM_YEAR_RANGE[0], CLIM_YEAR_RANGE[1])).sum(dim=["lat", "lon"]).groupby("time.month").mean()

        ihx_clim.plot(ax=ax[4 * i + 1], color="tab:blue", label=f"I {CASE}")
        ih0_clim.plot(ax=ax[4 * i + 1], color="tab:orange", label="I coupPPE.000")
        fhx_clim.plot(ax=ax[4 * i + 1], color="tab:red", label=f"F {CASE}")

        ax[4 * i + 1].set_ylabel(f"{v} [{cfs[v].unit}]")
        ax[4 * i + 1].set_xlabel("month")
        ax[4 * i + 1].set_title(
            f"global clim {labels[cfs[v].kind]} {v} "
            f"{CLIM_YEAR_RANGE[0][:4]}-{CLIM_YEAR_RANGE[1][:4]}"
        )
        ax[4 * i + 1].legend()

        # --- Spatial map: ih0 climatology (col 2) ---
        (ih0[v] + cfs[v].cs).sel(
            time=slice(MAP_YEAR_RANGE[0], MAP_YEAR_RANGE[1])
        ).mean(dim="time").plot(
            ax=ax[4 * i + 2],
            vmin=vmin,
            cmap=cm["cont"],
            cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"}
        )
        ax[4 * i + 2].set_title(f"{CASE} {v} {MAP_YEAR_RANGE[0][:4]}")

        # --- Spatial map: ihx - ih0 difference (col 3) ---
        (
            (ihx[v] + cfs[v].cs).sel(time=slice(MAP_YEAR_RANGE[0], MAP_YEAR_RANGE[1]))
            - (ih0[v] + cfs[v].cs).sel(time=slice(MAP_YEAR_RANGE[0], MAP_YEAR_RANGE[1]))
        ).mean(dim="time").plot(
            ax=ax[4 * i + 3],
            cmap=cm["diff"],
            norm=clr.CenteredNorm(),
            robust=True,
            cbar_kwargs={"label": f"{v} [{cfs[v].unit}]"}
        )
        ax[4 * i + 3].set_title(f"{CASE[-3:]}$-$000 {v} {MAP_YEAR_RANGE[0][:4]}")

        for j in range(2, 4):
            ax[4 * i + j].set_ylabel("")
            ax[4 * i + j].set_xlabel("")

    if to_save:
        fig.savefig(
            f"{SIM_DIR}/{CASE}/{CASE_PREFIX}.{CASE}.{tag}.png",
            dpi=300, bbox_inches="tight"
        )
        plt.close()


# --- Configuration -----------------------------------------------------------

DEFAULT_CASE_PREFIX = "i.e21.CPLHIST_BGC.f19_f19_mg17.historical"
DEFAULT_SIM_DIR = "/glade/u/home/bbuchovecky/projects/coup_ppe/sims"
DEFAULT_ARCH_DIR = "/glade/derecho/scratch/bbuchovecky/archive"

LND_VARIABLES = ["TLAI", "TOTVEGC", "EFLX_LH_TOT", "FCTR", "FCEV", "FGEV", "TSA"]
ATM_VARIABLES = ["TREFHT", "PS", "PRECT", "TMQ", "FSNT", "FLNT", "CLDTOT"]

DEFAULT_CLIM_YEAR_RANGE = ["1995-01", "1999-12"]
DEFAULT_MAP_YEAR_RANGE = ["1999-01", "1999-12"]

def parse_args(argv=None):
    """Parse command-line arguments for OFFLHIST diagnostics."""
    parser = argparse.ArgumentParser(
        description="Generate OFFLHIST diagnostic plots for a case.",
    )
    parser.add_argument(
        "case",
        help="Case suffix, e.g. coupPPE.001",
    )
    parser.add_argument(
        "--domain",
        choices=["lnd", "atm", "both"],
        default="lnd",
        help="Domain(s) to process.",
    )
    parser.add_argument(
        "--case-prefix",
        default=DEFAULT_CASE_PREFIX,
        help="Interactive run case prefix.",
    )
    parser.add_argument(
        "--sim-dir",
        default=DEFAULT_SIM_DIR,
        help="Directory where output plots are saved.",
    )
    parser.add_argument(
        "--arch-dir",
        default=DEFAULT_ARCH_DIR,
        help="Archive root directory containing history files.",
    )
    parser.add_argument(
        "--clim-start",
        default=DEFAULT_CLIM_YEAR_RANGE[0],
        help="Start month for climatology range (YYYY-MM).",
    )
    parser.add_argument(
        "--clim-end",
        default=DEFAULT_CLIM_YEAR_RANGE[1],
        help="End month for climatology range (YYYY-MM).",
    )
    parser.add_argument(
        "--map-start",
        default=DEFAULT_MAP_YEAR_RANGE[0],
        help="Start month for map range (YYYY-MM).",
    )
    parser.add_argument(
        "--map-end",
        default=DEFAULT_MAP_YEAR_RANGE[1],
        help="End month for map range (YYYY-MM).",
    )
    parser.add_argument(
        "--time-chunk",
        type=int,
        default=-1,
        help="Chunk size along time for xarray/dask (-1 = full time per chunk).",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Show plots instead of saving PNG files.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Run OFFLHIST diagnostics from command line."""
    global CASE, CASE_PREFIX, SIM_DIR, ARCH_DIR, CLIM_YEAR_RANGE, MAP_YEAR_RANGE
    global cfs, labels, cmaps

    args = parse_args(argv)

    CASE = args.case
    CASE_PREFIX = args.case_prefix
    SIM_DIR = args.sim_dir
    ARCH_DIR = args.arch_dir
    CLIM_YEAR_RANGE = [args.clim_start, args.clim_end]
    MAP_YEAR_RANGE = [args.map_start, args.map_end]

    chunks = {"time": args.time_chunk}

    # Deal with 005 and 006 (for now)
    if CASE in ("coupPPE.005", "coupPPE.006"):
        CASE += ".no_nlmod"

    print(f"CASE = {CASE}")
    print(f"CASE_PREFIX = {CASE_PREFIX}")
    print(f"SIM_DIR = {SIM_DIR}")
    print(f"ARCH_DIR = {ARCH_DIR}")
    print(f"CLIM_YEAR_RANGE = {CLIM_YEAR_RANGE}")
    print(f"MAP_YEAR_RANGE = {MAP_YEAR_RANGE}")
    print(f"chunks = {chunks}")

    # --- Load data -----------------------------------------------------------
    if args.domain in ["lnd", "both"]:
        ih0_lnd = load_variables(LND_VARIABLES, f"{CASE_PREFIX}.coupPPE.000", ARCH_DIR, domain="lnd", chunks=chunks)
        ihx_lnd = load_variables(LND_VARIABLES, f"{CASE_PREFIX}.{CASE}", ARCH_DIR, domain="lnd", chunks=chunks)
        fhx_lnd = load_variables(
            LND_VARIABLES,
            f"f.e21.FHIST_BGC.f19_f19_mg17.historical.{CASE}",
            ARCH_DIR,
            domain="lnd",
            chunks=chunks,
        )

    if args.domain in ["atm", "both"]:
        ih0_atm = load_variables(ATM_VARIABLES, f"{CASE_PREFIX}.coupPPE.000", ARCH_DIR, domain="atm", chunks=chunks)
        ihx_atm = load_variables(ATM_VARIABLES, f"{CASE_PREFIX}.{CASE}", ARCH_DIR, domain="atm", chunks=chunks)
        fhx_atm = load_variables(
            ATM_VARIABLES,
            f"f.e21.FHIST_BGC.f19_f19_mg17.historical.{CASE}",
            ARCH_DIR,
            domain="atm",
            chunks=chunks,
        )

    # --- Grid and conversion factors -----------------------------------------
    fh0 = glob(f"{ARCH_DIR}/{CASE_PREFIX}.coupPPE.000/lnd/hist/*.h0.*")
    grid = xr.open_dataset(fh0[0], decode_timedelta=True, engine="netcdf4")[["area", "landfrac"]]

    # la  : land area per grid cell [m²]; area is in km², landfrac is dimensionless
    # lw  : fractional land area weight (sums to 1 globally)
    la = (grid.area * 1e6 * grid.landfrac).fillna(0)
    lw = la / la.sum()

    ConversionFactor = namedtuple("ConversionFactor", ["cf", "cs", "unit", "kind"])
    # cf : multiplicative conversion factor (lw for intensive, la/1e15 for PgC extensive)
    # cs : additive constant (e.g., -273.15 K→°C for temperature variables)
    cfs = {
        "TLAI":       ConversionFactor(lw,          0,       "m2/m2",    "intensive"),
        "TOTECOSYSC": ConversionFactor(la / 1e15,   0,       "PgC",      "extensive"),
        "TOTVEGC":    ConversionFactor(la / 1e15,   0,       "PgC",      "extensive"),
        "TOTSOMC":    ConversionFactor(la / 1e15,   0,       "PgC",      "extensive"),
        "RAIN":       ConversionFactor(lw,          0,       "mm/s",     "intensive"),
        "QRUNOFF":    ConversionFactor(lw,          0,       "mm/s",     "intensive"),
        "QSOIL":      ConversionFactor(lw,          0,       "mm/s",     "intensive"),
        "QVEGE":      ConversionFactor(lw,          0,       "mm/s",     "intensive"),
        "QVEGT":      ConversionFactor(lw,          0,       "mm/s",     "intensive"),
        "TWS":        ConversionFactor(lw,          0,       "mm",       "intensive"),
        "EFLX_LH_TOT":ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FCTR":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FCEV":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FGEV":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FSH":        ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FIRE":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FLDS":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FSR":        ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FSDS":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FGR":        ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "TSA":        ConversionFactor(lw,          -273.15, "degreeC",  "intensive"),
        "TREFHT":     ConversionFactor(lw,          -273.15, "degreeC",  "intensive"),
        "PS":         ConversionFactor(lw,          0,       "Pa",       "intensive"),
        "FSNT":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "FLNT":       ConversionFactor(lw,          0,       "W/m2",     "intensive"),
        "CLDTOT":     ConversionFactor(lw,          0,       "fraction", "intensive"),
        "PRECT":      ConversionFactor(lw*1000*86400, 0,     "mm/day",   "intensive"),
        "TMQ":        ConversionFactor(lw,          0,       "kg/m2",    "intensive"),
    }

    labels = {
        "intensive": "mean",
        "extensive": "total",
    }

    cmaps = {
        "veg": {
            "diff": "PRGn",
            "cont": "viridis",
            "vars": ["TLAI", "TOTVEGC"],
        },
        "water": {
            "diff": cmo.curl_r,
            "cont": cmo.rain,
            "vars": ["EFLX_LH_TOT", "FCTR", "FCEV", "FGEV", "PRECT", "TMQ"],
        },
        "temp": {
            "diff": "RdBu_r",
            "cont": "inferno",
            "vars": ["TSA", "TREFHT", "PS", "FSNT", "FLNT", "CLDTOT"],
        },
    }

    # --- Run diagnostics -----------------------------------------------------
    if args.domain in ["lnd", "both"]:
        plot_simple_diags(ihx_lnd, ih0_lnd, fhx_lnd, LND_VARIABLES, "lnd", to_save=not args.show)

    if args.domain in ["atm", "both"]:
        plot_simple_diags(ihx_atm, ih0_atm, fhx_atm, ATM_VARIABLES, "atm", to_save=not args.show)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
