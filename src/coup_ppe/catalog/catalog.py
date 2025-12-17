"""
Tools for building intake-esm catalogs for CESM PPEs.
"""
from __future__ import annotations
from typing import Optional
from typing import Callable
import pathlib
import joblib
from ecgtools import Builder

from coup_ppe.metadata.conventions import EnsembleType, validate_ensemble
from coup_ppe.metadata.conventions import FileFormat, validate_file_format
import coup_ppe.parallel.daskhelper
import coup_ppe.catalog.parsers


# Catalog configuration constants
CATALOG_CONFIGS = {
    "history": {
        "depth": 3,
        "exclude_patterns": ["*/proc/*", "*/rest/*"],
        "groupby_attrs": ["component", "stream", "frequency"],
        "aggregation": [
            {
                "type": "join_existing",
                "attribute_name": "date",
                "options": {
                    "dim": "time",
                    "coords": "minimal",
                    "compat": "override",
                },
            },
            {
                "type": "join_new",
                "attribute_name": "member",
                "options": {
                    "dim": "member",
                    "coords": "minimal",
                },
            }
        ],
    },
    "timeseries": {
        "depth": 4,
        "exclude_patterns": ["*/hist/*", "*/rest/*"],
        "groupby_attrs": ["variable", "component", "stream", "frequency"],
        "aggregation": [
            {
                "type": "union",
                "attribute_name": "variable",
                "options": {}
            },
            {
                "type": "join_new",
                "attribute_name": "member",
                "options": {
                    "dim": "member",
                    "coords": "minimal",
                },
            }
        ],
    }
}

# Parser function mapping
PARSERS = {
    "pisom": {
        "history": coup_ppe.catalog.parsers.parse_pisom_history,
        "timeseries": coup_ppe.catalog.parsers.parse_pisom_timeseries,
    },
    "fhist": {
        "history": coup_ppe.catalog.parsers.parse_fhist_history,
    }
}


def _get_parser(ensemble: str, file_format: str):
    """Get the appropriate parser function for ensemble and file format."""
    try:
        return PARSERS[ensemble][file_format]
    except KeyError as exc:
        raise NotImplementedError(
            f"Parser not implemented for ensemble='{ensemble}', file_format='{file_format}'"
        ) from exc

def _normalize_paths(rootpath: str | pathlib.Path | list[str | pathlib.Path]) -> list[pathlib.Path]:
    """Convert rootpath to list of Path objects."""
    if not isinstance(rootpath, list):
        rootpath = [rootpath]
    return [pathlib.Path(p) for p in rootpath]


def _build_catalog_builder(paths: list[pathlib.Path], config: dict, parser: Callable) -> Builder:
    """Create and build the catalog builder."""
    builder = Builder(
        paths=paths,
        depth=config["depth"],
        exclude_patterns=config["exclude_patterns"],
        joblib_parallel_kwargs={"n_jobs": -1},
    )
    
    if coup_ppe.parallel.daskhelper.is_dask_available():
        with joblib.parallel_backend('dask', scatter=[parser]):
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
        ensemble: EnsembleType,
        file_format: FileFormat,
        rootpath: str | pathlib.Path | list[str | pathlib.Path],
        outpath: str | pathlib.Path,
        tag: str = "",
        depth: Optional[int] = None,
    ) -> pathlib.Path:
    """
    Build an intake-esm catalog for a CESM PPE.
    
    Parameters
    ----------
    ensemble : EnsembleType
        Ensemble name ('pisom', 'fhist', 'fssp370')
    file_format : FileFormat
        Format of data files to catalog ('history', 'timeseries')
    rootpath : str | pathlib.Path | list[str | pathlib.Path]
        Root path(s) to search for data files
    outpath : pathlib.Path | str
        Output directory path
    tag : str, optional
        Tag to append to catalog filename (default: "")
    depth : int, optional
        Directory depth to search (overrides config default)
        
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
    validate_ensemble(ensemble)
    validate_file_format(file_format)
    
    # Get configuration and parser
    config = CATALOG_CONFIGS[file_format].copy()
    if depth is not None:
        config["depth"] = depth
    
    parser = _get_parser(ensemble, file_format)
    paths = _normalize_paths(rootpath)
    
    # Validate output path
    outpath = pathlib.Path(outpath).resolve()
    if not outpath.exists():
        raise FileNotFoundError(f"Output directory does not exist: {outpath}")
    
    # Build catalog
    builder = _build_catalog_builder(paths, config, parser)
    builder.clean_dataframe()
    
    # Post-process for history files
    if file_format == "history":
        builder = _postprocess_history_catalog(builder)
    
    # Save catalog
    catalog_name = f"{ensemble}_{file_format}_catalog{tag}"
    builder.save(
        name=catalog_name,
        directory=str(outpath),
        path_column_name="path",
        variable_column_name="variable",
        data_format="netcdf",
        groupby_attrs=config["groupby_attrs"],
        aggregations=config["aggregation"],
    )
    
    return (outpath / catalog_name).with_suffix('.json')
