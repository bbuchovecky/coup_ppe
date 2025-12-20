"""
Tools for building intake-esm catalogs for CESM PPEs.
"""

from __future__ import annotations
from typing import Optional
from typing import Callable
from pathlib import Path
import joblib
from ecgtools import Builder

from xppe import daskhelper
from xppe.catalog import parsers
from xppe.access import config
from xppe.metadata.conventions import (
    validate_ensemble_kind,
    validate_file_format,
    EnsembleType, Kind, FileFormat,
)

MODELS = config.get_models()
PARSERS_CONFIG = config.get_parsers_config()
PARSERS = {
    "pisom": {
        "history": parsers.parse_pisom_history,
        "timeseries": parsers.parse_pisom_timeseries,
    },
    "fhist": {
        "history": parsers.parse_fhist_history,
    },
    "hadcm3": {
        "timeseries": parsers.parse_hadcm3,
    },
}


def _get_parser(
    ensemble: str,
    file_format: str,
) -> Callable:
    """Get the appropriate parser function for ensemble and file format."""
    try:
        return PARSERS[ensemble][file_format]
    except KeyError as exc:
        raise NotImplementedError(
            f"Parser not implemented for ensemble='{ensemble}', file_format='{file_format}'"
        ) from exc


def _normalize_paths(
    rootpath: str | Path | list[str | Path],
) -> list[Path]:
    """Convert rootpath to list of Path objects."""
    if not isinstance(rootpath, list):
        rootpath = [rootpath]
    return [Path(p) for p in rootpath]


def _create_catalog_builder(
    paths: list[Path],
    cfg: dict,
    parser: Callable,
) -> Builder:
    """Create the catalog builder."""
    builder = Builder(
        paths=[str(p) for p in paths],
        depth=cfg["depth"],
        exclude_patterns=cfg["exclude_patterns"],
        joblib_parallel_kwargs={"n_jobs": -1},
    )

    if daskhelper.is_dask_available():
        with joblib.parallel_backend("dask", scatter=[parser]):
            builder = builder.build(parsing_func=parser)
    else:
        builder = builder.build(parsing_func=parser)

    return builder


def _postprocess_history_catalog(builder: Builder) -> Builder:
    """Expand variable lists into separate rows for history catalogs."""
    df = builder.df.explode("variable").reset_index(drop=True)
    builder.df = df
    return builder


def build_catalog(
    kind: Kind,
    ensemble: EnsembleType,
    file_format: FileFormat,
    rootpath: str | Path | list[str | Path],
    outpath: str | Path,
    tag: str = "",
    depth: Optional[int] = None,
) -> Path:
    """
    Build an intake-esm catalog for PPE output.

    Parameters
    ----------
    kind : str
        Kind of simulation ('coup', 'offl') for CESM PPEs or
        ('control', 'a1b') for HadCM3 PPE.
    ensemble : str
        Ensemble name ('pisom', 'fhist', 'hadcm3').
    file_format : FileFormat
        Format of data files to catalog ('history', 'timeseries').
    rootpath : str | pathlib.Path | list[str | pathlib.Path]
        Root path(s) to search for data files.
    outpath : pathlib.Path | str
        Output directory path.
    tag : str, optional
        Tag to append to catalog filename (default: "").
    depth : int, optional
        Directory depth to search (overrides config default).

    Returns
    -------
    pathlib.Path
        Path to the saved catalog JSON file

    Raises
    ------
    ValueError
        If ensemble or file_format are not supported
    NotImplementedError
        If parser not available for ensemble/file_format combination
    """
    validate_ensemble_kind(ensemble, kind)
    validate_file_format(file_format)

    # Get configuration and parser
    cfg = PARSERS_CONFIG[MODELS[ensemble]][file_format].copy()
    if depth is not None:
        cfg["depth"] = depth

    parser = _get_parser(ensemble, file_format)
    paths = _normalize_paths(rootpath)

    # Validate output path
    outpath = Path(outpath).resolve()
    if not outpath.exists():
        raise FileNotFoundError(f"Output directory does not exist: {outpath}")

    # Create the catalog builder
    builder = _create_catalog_builder(paths, cfg, parser)
    builder.clean_dataframe()

    # Post-process for history files
    if file_format == "history":
        builder = _postprocess_history_catalog(builder)

    # Save catalog
    catalog_name = f"{kind}_{ensemble}_{file_format}_catalog{tag}"
    builder.save(
        name=catalog_name,
        directory=str(outpath),
        path_column_name="path",
        variable_column_name="variable",
        data_format="netcdf",
        groupby_attrs=cfg["groupby_attrs"],
        aggregations=cfg["aggregation"],
    )

    return (outpath / catalog_name).with_suffix(".json")


def build_catalog_cli(
    kind: Kind,
    ensemble: EnsembleType,
    file_format: FileFormat,
    dask_cluster: Optional[tuple] = None,
):
    """
    Build an intake-esm catalog for a PPE ensemble.

    Parameters
    ----------
    kind : str
        Kind of simulation ('coup', 'offl') for CESM PPEs or
        ('control', 'a1b') for HadCM3 PPE.
    ensemble : str
        Ensemble name ('pisom', 'fhist', 'hadcm3').
    file_format : str
        Format of model output files ('history' or 'timeseries')
    dask_cluster : tuple, optional
        (account, nworkers, nmem, walltime) for dask cluster
    """
    # Define paths
    filesystem = config.get_filesystem()
    data_root_path = config.get_data_root_path(kind, ensemble)
    catalog_root_path = config.get_catalog_root_path()

    # Start Dask cluster if requested
    client, cluster = None, None
    if dask_cluster:
        assert (
            len(dask_cluster) == 4
        ), "dask_cluster must contain (account, nworkers, nmem, walltime)"
        try:
            account, nworkers, nmem, walltime = dask_cluster
            client, cluster = daskhelper.create_dask_cluster(
                account=account,
                nworkers=int(nworkers),
                nmem=nmem,
                walltime=walltime,
            )
        except KeyError:
            print("Unable to start a dask cluster, continuing without")

    try:
        build_catalog(
            kind=kind,
            ensemble=ensemble,
            file_format=file_format,
            rootpath=data_root_path,
            outpath=catalog_root_path,
            tag=f"_{str(filesystem)}",
        )
    finally:
        if client is not None and cluster is not None:
            daskhelper.close_dask_cluster(client, cluster)
