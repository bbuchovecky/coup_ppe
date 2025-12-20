"""
Utilities to read and access configuration settings.
"""

from __future__ import annotations
from typing import Optional
from pathlib import Path
import yaml
from xppe.metadata.conventions import (
    Kind, EnsembleType,
    validate_ensemble, validate_file_format
)


# Use package location for root path instead of relative parents
PACKAGE_ROOT = Path(__file__).parent.parent.parent.parent
CONFIG_PATH = PACKAGE_ROOT / "config.yml"


def _load_config() -> dict:
    """Load config once, use caching if needed."""
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def get_filesystem() -> str:
    """Get the filesystem from config.yml."""
    return _load_config()["FILESYSTEM"]


def get_ensembles() -> list[str]:
    """Get configured ensembles."""
    config = _load_config()
    ensembles = config["ENSEMBLES"]
    # Validate that each ensemble is supported
    # -> code defines capabilities, config defines deployment choices
    for ens in ensembles:
        validate_ensemble(ens)
    return ensembles


def get_file_formats() -> list[str]:
    """Get configured data file formats."""
    config = _load_config()
    file_formats = config["FILE_FORMATS"]
    # Validate that each file format is supported
    # -> code defines capabilities, config defines deployment choices
    for ff in file_formats:
        validate_file_format(ff)
    return file_formats


def get_models() -> dict:
    """Get the model corresponding to each PPE."""
    config = _load_config()
    return config["MODELS"]


def get_data_root_path(
        kind: Kind,
        ensemble: EnsembleType,
        filesystem: Optional[str] = None
    ) -> Path:
    """
    Get data root path for a specific ensemble and filesystem.

    Parameters
    ----------
    kind : str
        Kind of simulation ('coup', 'offl') for CESM PPEs or
        ('control', 'a1b') for HadCM3 PPE.
    ensemble : str
        Ensemble name ('pisom', 'fhist', 'hadcm3').
    filesystem str, optional
        Filesystem name (overrides config default)
    """
    config = _load_config()
    fs = filesystem or config["FILESYSTEM"]
    data_path = config["DATA_ROOT_PATHS"][fs][ensemble][kind]
    return PACKAGE_ROOT / Path(data_path)


def get_catalog_root_path() -> Path:
    """Get the catalog root path from config.yml."""
    config = _load_config()
    return PACKAGE_ROOT / Path(config["CATALOG_ROOT_PATH"])


def get_crosswalk_root_path() -> Path:
    """Get the crosswalk root path from config.yml."""
    config = _load_config()
    return PACKAGE_ROOT / Path(config["CROSSWALK_ROOT_PATH"])


def get_member_id_map_path() -> Path:
    """Get the member ID map path from config.yml."""
    config = _load_config()
    return PACKAGE_ROOT / Path(config["MEMBER_ID_MAP_PATH"])


def get_parsers_config() -> dict:
    """Get the parsers config dictionary."""
    config = _load_config()
    return config["PARSERS_CONFIG"]
