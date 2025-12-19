"""
Utilities to read and access configuration settings.
"""

from __future__ import annotations
from typing import Optional
import pathlib
import yaml
from xppe.metadata.conventions import validate_ensemble, validate_file_format


# Use package location for root path instead of relative parents
PACKAGE_ROOT = pathlib.Path(__file__).parent.parent.parent.parent
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


def get_data_root_path(ensemble: str, filesystem: Optional[str] = None) -> pathlib.Path:
    """
    Get data root path for a specific ensemble and filesystem.

    Parameters
    ----------
    ensemble : str
        Ensemble name ('pisom', 'fhist', 'fssp370')
    filesystem str, optional
        Filesystem name (overrides config default)
    """
    config = _load_config()
    fs = filesystem or config["FILESYSTEM"]
    data_path = config["DATA_ROOT_PATHS"][fs][ensemble]["coup"]
    return PACKAGE_ROOT / pathlib.Path(data_path)


def get_catalog_root_path() -> pathlib.Path:
    """Get the catalog root path from config.yml."""
    config = _load_config()
    return PACKAGE_ROOT / pathlib.Path(config["CATALOG_ROOT_PATH"])


def get_crosswalk_root_path() -> pathlib.Path:
    """Get the crosswalk root path from config.yml."""
    config = _load_config()
    return PACKAGE_ROOT / pathlib.Path(config["CROSSWALK_ROOT_PATH"])


def get_member_id_map_path() -> pathlib.Path:
    """Get the member ID map path from config.yml."""
    config = _load_config()
    return PACKAGE_ROOT / pathlib.Path(config["MEMBER_ID_MAP_PATH"])
