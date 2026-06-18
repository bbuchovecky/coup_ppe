"""
ppe_ts_to_zarr.py

Convert CESM2 processed timeseries NetCDF output into
per-variable, member-concatenated Zarr stores for fast
ensemble analysis.

Assumes standard CESM2 timeseries structure:

    <TS_ROOT>/
        <case0>/
            <component>/proc/tseries/
                <frequency>/
                    <case0>.<scomp>.<stream>.<VAR>.<YYYYMM>-<YYYYMM>.nc
                    ...
        <case1>/
            ...

One Zarr store is written per variable:

    <ZARR_ROOT>/
        <gcomp>/<freq>/
            <case>.<scomp>.<stream>.<VAR>.zarr   ->   shape (member, time, lat, lon, [lev])

"""

import argparse
import re
import sys
import time
from pathlib import Path

import xarray as xr


#### Configuration ####

# Root directory: one subdirectory per ensemble member
TS_ROOT = Path("/glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations")

# Output directory: Zarr stores on fast scratch filesystem
ZARR_ROOT = Path("/glade/derecho/scratch/bbuchovecky/zarr/ppe/historical/coupled_simulations")

# Glob template to match timeseries files for a given variable within a member directory.
# {case}, {gcomp}, {scomp}, {freq}, and {var} are substituted at runtime.
# Standard CESM2 tseries filename: <case>.<member>.<scomp>.<stream>.<VAR>.<YYYYMM>-<YYYYMM>.nc
TS_GLOB_TEMPLATE = (
    "{gcomp}/proc/tseries/{freq}/{case}.[0-9][0-9][0-9].{scomp}.{stream}.{var}.*.nc"
)

# Map gcomp to scomp
CESM2_COMPONENT_MAP = {
    "atm":    "cam",
    "lnd":    "clm2",
    "cice":   "cice",
    "mosart": "rof",
    "glc":    "cism",
    "cpl":    "cpl",
}

# Chunk dimensions
# time = 12  -> one year per chunk for monthly data
# lat, lon   -> full 2deg grid
# member     -> all members in one chunk; optimal for ensemble-dimension reductions
# lev        -> all levels in one chunk
CHUNKS = {"member": -1, "time": 12, "lat": -1, "lon": -1}
CHUNK_TIME = {"month_1": 12, "day_1": 365}
CHUNK_LEV = -1


def discover_members(ts_root: Path, case: str) -> list[Path]:
    """
    Return sorted list of member directories under ts_root whose names
    begin with the base case name.

    CESM2 PPE member directories are typically named <case>.<member_suffix>
    or <case>_<member_suffix>. Filtering on the case prefix avoids picking
    up unrelated directories (e.g. other experiments, log dirs) that may
    coexist under TS_ROOT.

    Parameters
    ----------
    ts_root : Path
        Root directory containing all ensemble member subdirectories.
    case : str
        Base case name (e.g. 'FHIST_BGC'). Used as a prefix filter.

    Returns
    -------
    list[Path]
        Sorted list of matching member directories.
    """
    members = sorted([
        d for d in ts_root.iterdir()
        if d.is_dir() and d.name.startswith(case) and d.name[-3:].isdigit()
        # ! d.name[-3:].isdigit() omits cplhist and no_nlmod
    ])
    if not members:
        raise FileNotFoundError(
            f"No member directories matching '{case}.*' found under {ts_root}"
        )
    return members


def extract_member_id(member_dir: Path, case: str) -> int:
    """
    Derive a integer member label from the directory name
    by stripping the base case prefix and parsing the remainder.

    Examples (case='FHIST_BGC'):
      FHIST_BGC.001 -> '1'

    Falls back to the raw suffix string if no integer is found.

    Parameters
    ----------
    member_dir : Path
        Member root directory.
    case : str
        Base case name to strip from the directory name.
    """
    name = member_dir.name
    # Strip case prefix and any leading separator (. or _)
    suffix = re.sub(rf"^{re.escape(case)}[._-]?", "", name)
    # Parse integer out of the suffix
    match = re.search(r"\d+", suffix)
    if match:
        return int(match.group())
    raise ValueError


def discover_variables(member_dir: Path, gcomp: str, freq: str, stream: str) -> list[str]:
    """
    Infer available variables from timeseries filenames in the first member
    directory. Parses the variable name field from the standard CESM2 tseries
    filename: <case>.<member>.<component_model>.<stream>.<VAR>.<YYYYMM>-<YYYYMM>.nc

    Parameters
    ----------
    member_dir : Path
        Root directory of the first ensemble member.

    Returns
    -------
    list[str]
        Sorted list of variable name strings.
    """
    ts_dir = member_dir / gcomp / "proc" / "tseries" / freq

    nc_files = sorted(ts_dir.rglob("*.nc"))
    if not nc_files:
        raise FileNotFoundError(
            f"No timeseries NetCDF files found under {ts_dir}\n"
            f"Check TS_GLOB_TEMPLATE settings."
        )

    vars_found = set()
    pattern = re.compile(rf"\.({re.escape(stream)})\.([A-Za-z_][A-Za-z0-9_]*)\.(\d{{6,8}}-\d{{6,8}})\.nc$")
    for f in nc_files:
        m = pattern.search(f.name)
        if m:
            vars_found.add(m.group(2))

    if not vars_found:
        raise ValueError(
            f"Could not parse variable names from filenames in {ts_dir}.\n"
            f"Expected pattern: <case>.<stream>.<VAR>.<YYYYMMDD>-<YYYYMMDD>.nc"
        )
    return sorted(vars_found)


def load_member_ts(member_dir: Path, case: str, var: str, gcomp: str, freq: str, stream: str) -> xr.DataArray:
    """
    Open all timeseries files for one variable in one member directory,
    concatenate along time, and attach a member coordinate.

    Parameters
    ----------
    member_dir : Path
        Root directory for this ensemble member.
    case : str
        Base case name; used to construct the filename glob and member label.
    var : str
        Variable name (e.g., 'ET', 'RAIN').
    gcomp : str
        General model component (e.g., 'atm', 'lnd')
    freq : str
        Variable frequency (e.g., 'month_1', 'day_1')
    stream : str
        Variable stream (e.g., 'h0', 'h1', 'h2')

    Returns
    -------
    xr.DataArray
        Shape (1, time, lat, lon) with member coordinate assigned.
        Chunked along time only at this stage; rechunked after concat.
    """
    # "{gcomp}/proc/tseries/{freq}/{case}.*.{scomp}.{stream}.{var}.*.nc"
    glob_pattern = TS_GLOB_TEMPLATE.format(
        gcomp=gcomp,
        scomp=CESM2_COMPONENT_MAP[gcomp],
        case=case,
        freq=freq,
        stream=stream,
        var=var,
    )
    files = sorted(member_dir.glob(glob_pattern))

    if not files:
        raise FileNotFoundError(
            f"No timeseries files for variable '{var}' in {member_dir}\n"
            f"Glob pattern used: {glob_pattern}"
        )

    member_id = extract_member_id(member_dir, case)

    ds = xr.open_mfdataset(
        files,
        combine="by_coords",
        parallel=True,
        chunks={"time": CHUNKS["time"]},
        data_vars="minimal",   # avoids broadcasting non-data coord vars across files
        coords="minimal",      # skips redundant coordinate consistency checks
        compat="override",     # suppresses spurious attribute mismatch errors
    )

    return (
        ds[var]
        .assign_coords(member=member_id)
        .expand_dims("member")
    )


def build_ensemble_ts(member_dirs: list[Path], case: str, var: str, gcomp: str, freq: str, stream: str) -> xr.DataArray:
    """
    Load all members for one variable and concatenate along the member dimension.

    Parameters
    ----------
    member_dirs : list[Path]
        Sorted list of member root directories.
    case : str
        Base case name; passed through to load_member_ts.
    var : str
        Variable name to load.

    Returns
    -------
    xr.DataArray
        Shape (member, time, lat, lon), rechunked per CHUNKS.
    """
    members = []
    skipped = []
    for d in member_dirs:
        try:
            da = load_member_ts(d, case, var, gcomp, freq, stream)
            members.append(da)
        except FileNotFoundError as e:
            print(f"    SKIP: {e}")
            skipped.append(d.name)

    if not members:
        raise RuntimeError(f"No members successfully loaded for variable '{var}'.")
    if skipped:
        print(f"  WARNING: {len(skipped)} member(s) skipped for '{var}': {skipped}")

    ens = xr.concat(members, dim="member")

    if len(ens.coords) > 3:
        extra = set(ens.coords) - set(['time', 'lat', 'lon'])
        if len(extra) > 1:
            raise ValueError(f"More than 4 coordinates: {list(ens.coords)}")
        ens = ens.chunk(CHUNKS | {"time": CHUNK_TIME[freq]} | {next(iter(extra)): CHUNK_LEV})
    else:
        ens = ens.chunk(CHUNKS | {"time": CHUNK_TIME[freq]})

    return ens


def write_zarr(ens: xr.DataArray, case:str, var: str, scomp: str, stream: str, zarr_dir: Path, dry_run: bool=False) -> Path:
    """
    Write a member-concatenated DataArray to a Zarr store with lz4 compression.

    Parameters
    ----------
    ens : xr.DataArray
        Ensemble array of shape (member, time, lat, lon).
    case : str
        Base case name
    var : str
        Variable name; determines the store name: <zarr_dir>/<case>.<stream>.<var>.zarr
    stream : str
        Variable stream 
    zarr_dir : Path
        Parent directory for all Zarr stores.
    dry_run : bool
        If True, skip the write and return the would-be path.

    Returns
    -------
    Path
        Path to the written (or would-be) Zarr store.
    """
    zarr_path = zarr_dir / f"{case}.{scomp}.{stream}.{var}.zarr"

    if dry_run:
        print(f"  [dry-run] Would write -> {zarr_path}")
        return zarr_path

    zarr_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    print(f"  Writing -> {zarr_path}")

    # compute=False defers evaluation to .compute(), allowing Dask to
    # schedule the write across workers rather than blocking immediately.
    write_job = ens.to_dataset(name=var).to_zarr(
        zarr_path,
        mode="w",
        compute=False,
    )
    write_job.compute()

    elapsed = time.perf_counter() - t0
    size_gb = sum(f.stat().st_size for f in zarr_path.rglob("*") if f.is_file()) / 1e9
    print(f"  Done in {elapsed:.1f}s — store size: {size_gb:.2f} GB")

    return zarr_path


def print_array_info(ens: xr.DataArray) -> None:
    """Print shape, chunk layout, dtype, and Dask graph size."""
    shape_str = " x ".join(f"{d}={s}" for d, s in zip(ens.dims, ens.shape))
    if getattr(ens, "chunks", None) is None:
        chunk_str = "n/a"
    else:
        chunk_str = " x ".join(f"{d}={c[0]}" for d, c in zip(ens.dims, ens.chunks))

    dask_graph = getattr(ens, "__dask_graph__", None)
    if callable(dask_graph):
        graph = dask_graph()
        graph_size = graph.size() if graph is not None and hasattr(graph, "size") else 0
    else:
        graph_size = 0

    nbytes_gb  = ens.nbytes / 1e9
    print(f"    Shape:      {shape_str}")
    print(f"    Chunks:     {chunk_str}")
    print(f"    dtype:      {ens.dtype}")
    print(f"    Uncompressed size: {nbytes_gb:.2f} GB")
    print(f"    Dask graph tasks:  {graph_size}")



def main():
    parser = argparse.ArgumentParser(
        description="Convert CESM2 PPE processed timeseries NetCDF to Zarr stores.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--case", required=True, type=str,
        help=(
            "Base case name (e.g. 'f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE'). "
            "Used to filter member directories under --ts-root and to construct timeseries filename globs."
        ),
    )

    parser.add_argument(
        "--ts-root", required=True, type=Path,
        help="Timeseries root directory containing one subdirectory per ensemble member.",
    )

    parser.add_argument(
        "--zarr-root", required=True, type=Path,
        help="Output directory for Zarr stores.",
    )

    parser.add_argument(
        "--gcomp", required=True, type=str,
        help="Variable frequency (e.g., --gcomp atm)"
    )

    parser.add_argument(
        "--freq", required=True, type=str,
        help="Variable frequency (e.g., --freq month_1)"
    )

    parser.add_argument(
        "--stream", required=True, type=str,
        help="Variable stream (e.g., --stream h0)"
    )

    var_group = parser.add_mutually_exclusive_group(required=True)
    var_group.add_argument(
        "--var", nargs="+", type=str, metavar="VAR",
        help="One or more CLM/CAM variable names (e.g. --var ET RAIN QRUNOFF).",
    )
    var_group.add_argument(
        "--all-vars", action="store_true",
        help="Discover and process all variables found in the third member directory.",
    )


    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip variables whose Zarr store already exists in --zarr-root.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be written without performing any I/O.",
    )
    
    args = parser.parse_args()

    # Namespace Zarr output under <ZARR_ROOT>/<case>/ by default
    zarr_root = args.zarr_root if args.zarr_root is not None else ZARR_ROOT / args.case
    
    #### Member discovery ####
    print(f"Discovering members under: {args.ts_root}")
    print(f"  Case filter: '{args.case}.*'")
    member_dirs = discover_members(args.ts_root, args.case)
    print(f"  Found {len(member_dirs)} member directories:\n"
          + "\n".join(f"    {d.name}" for d in member_dirs) + "\n")

    #### Variable list ####
    if args.all_vars:
        print(f"Discovering variables from third member: {member_dirs[3].name}")
        variables = discover_variables(member_dirs[3], args.gcomp, args.freq, args.stream)
        print(f"  Found {len(variables)} variables: {variables}\n")
    else:
        variables = args.var

    # Filter already-written stores if requested
    if args.skip_existing:
        before = len(variables)
        variables = [
            v for v in variables
            if not (zarr_root / f"{args.case}.{v}.zarr").exists()
        ]
        n_skipped = before - len(variables)
        if n_skipped:
            print(f"--skip-existing: skipping {n_skipped} already-written store(s).\n")

    if not variables:
        print("Nothing to do — all requested variables already written.")
        sys.exit(0)

    #### Per-variable processing ####
    print(f"Processing {len(variables)} variable(s): {variables}")
    print(f"Output directory: {zarr_root}\n")
    written = []
    failed  = []

    for i, var in enumerate(variables, start=1):
        print(f"[{i}/{len(variables)}] {var}")
        try:
            ens = build_ensemble_ts(member_dirs, args.case, var, args.gcomp, args.freq, args.stream)
            print_array_info(ens)
            zarr_dir = zarr_root / args.freq
            scomp = CESM2_COMPONENT_MAP[args.gcomp]
            zarr_path = write_zarr(ens, args.case, var, scomp, args.stream, zarr_dir, dry_run=args.dry_run)
            written.append(zarr_path)
        except Exception as e:
            print(f"  ERROR processing '{var}': {e}")
            failed.append(var)
        print()


if __name__ == "__main__":
    main()
