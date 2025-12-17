"""
Docstring for coup_ppe.metadata.conventions
"""

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


def canonical_component(name: str) -> str:
    """Map CESM2 component identifiers to canonical component names."""
    key = name.lower().strip()
    return CESM2_COMPONENT_MAP.get(key, key)


def ppe_long_name(name: str) -> str:
    """Map PPE short names to PPE long names."""
    key = name.lower().strip()
    return PPE_NAME_MAP.get(key, key)
