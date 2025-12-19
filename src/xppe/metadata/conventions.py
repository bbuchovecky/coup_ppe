"""
Docstring for coup_ppe.metadata.conventions
"""

from __future__ import annotations
from typing import Literal

# Valid ensemble identifiers
VALID_ENSEMBLES = ["pisom", "fhist", "ssp370"]
EnsembleType = Literal["pisom", "fhist", "ssp370"]

# Valid data file formats
VALID_FILE_FORMATS = ["history", "timeseries"]
FileFormat = Literal["history", "timeseries"]

# Valid 'kind'
VALID_KINDS = ["coup", "offl"]
Kind = Literal["coup", "offl"]

# For black formatter
# fmt: off

# Valid pisom member IDs
PISOM_MEMBERS = [
    '0',  '1',        '3',  '4',  '5',  '6',  '7',  '8',  '9',
   '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
   '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
   '30', '31', '32', '33', '34', '35', '36', '37',
]

# Valid fhist member IDs
FHIST_MEMBERS = [
    '0',  '1',  '2',  '3',  '4',  '5',  '6',  '7',  '8',  '9',
   '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
   '20', '21', '22', '23', '24', '25', '26', '27', '28',
]

# For black formatter
# fmt: on

PPE_NAME_MAP = {
    "pisom": "PI SOM PPE",
    "fhist": "FHIST PPE",
    "fssp370": "FSSP370 PPE",
}

CESM2_COMPONENT_MAP = {
    "cam": "atm",
    "clm2": "lnd",
    "cice": "ice",
    "mosart": "rof",
    "cism": "glc",
    "cpl": "cpl",
}


def canonical_component(component: str) -> str:
    """Map CESM2 component identifiers to canonical component names."""
    key = component.lower().strip()
    return CESM2_COMPONENT_MAP.get(key, key)


def ppe_long_name(ensemble: str) -> str:
    """Map PPE short names to PPE long names."""
    key = ensemble.lower().strip()
    return PPE_NAME_MAP.get(key, key)


def validate_ensemble(ensemble: str) -> None:
    """
    Validate that ensemble is a recognized PPE.

    Raises
    ------
    ValueError
        If ensemble is not in VALID_ENSEMBLES
    """
    if ensemble not in VALID_ENSEMBLES:
        raise ValueError(f"ensemble must be one of {VALID_ENSEMBLES}, got '{ensemble}'")


def validate_file_format(file_format: str) -> None:
    """
    Validate that the data file format is supported.

    Raises
    ------
    ValueError
        If file_format is not in VALID_FILE_FORMATS
    """
    if file_format not in VALID_FILE_FORMATS:
        raise ValueError(
            f"file_format must be one of {VALID_FILE_FORMATS}, got '{file_format}'"
        )

def validate_kind(kind: str) -> None:
    """
    Validate that the kind ('coup', 'offl') is supported.

    Raises
    ------
    ValueError
        If kind is not in VALID_KINDS
    """
    if kind not in VALID_KINDS:
        raise ValueError(
            f"kind must be one of {VALID_KINDS}, got '{kind}'"
        )
