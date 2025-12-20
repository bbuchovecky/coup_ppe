"""
Metadata and utilities for managing ensemble member identifiers.
"""

from __future__ import annotations
from typing import Optional
from pathlib import Path
import pandas as pd
import yaml

from xppe.metadata.conventions import EnsembleType, validate_ensemble
from xppe.access import config


MODULE_DIR = Path(__file__).parent.resolve()
PACKAGE_ROOT_DIR = Path(__file__).parent.parent.parent.resolve()


def _get_member_id(
    member: str,
    ensemble: EnsembleType,
) -> str:
    """Helper function to get the member ID."""
    if ensemble == "pisom":
        return str(int(member[4:8]))
    if ensemble == "hadcm3":
        return str(int(member))
    return str(int(member[-3:]))


def parse_member_id(
    member: str,
    ensemble: Optional[EnsembleType] = None,
) -> str:
    """Parse the member ID from a string."""
    if ensemble:
        return _get_member_id(member, ensemble)
    if "coupPPE" in member:
        return _get_member_id(member, "fhist")
    if "COUP" in member:
        return _get_member_id(member, "pisom")
    return str(int(member))


def _crosswalk_df_to_member_id_map(cw: pd.DataFrame) -> dict:
    """Convert the crosswalk DataFrame to a member map dictionary."""
    member_param_map = {}
    for _, row in cw.iterrows():
        param = row["param"]
        minmax = row["minmax"]
        member_id = parse_member_id(str(row["member"]))

        # Add the param -> minmax -> member_id mapping
        if pd.notna(minmax):
            # Initialize param dict if it doesn't exist
            if param not in member_param_map:
                member_param_map[param] = {}
            member_param_map[param][minmax] = member_id
        # Else add the param -> member_id mapping, for HadCM3
        else:
            member_param_map[param] = member_id

    return member_param_map


def create_member_id_map(ensemble: EnsembleType) -> dict:
    """Convert a crosswalk csv file to a member ID map dictionary."""
    validate_ensemble(ensemble)
    crosswalk_root_path = config.get_crosswalk_root_path()
    crosswalk_path = crosswalk_root_path / f"{ensemble}_crosswalk.csv"
    crosswalk_df = pd.read_csv(crosswalk_path, names=["member", "param", "minmax"])
    return _crosswalk_df_to_member_id_map(crosswalk_df)


def build_member_id_map_yaml() -> Path:
    """Build a member ID map dictionary and save as a YAML file."""
    ensembles = config.get_ensembles()
    member_id_map_path = config.get_member_id_map_path()

    member_id_map = {}
    for ens in ensembles:
        member_id_map[ens] = create_member_id_map(ens)

    # Save the member map dictionary as a YAML file
    with open(member_id_map_path, "w") as f:
        yaml.dump(member_id_map, f, default_flow_style=False)

    return member_id_map_path


def load_member_id_map(ensemble: EnsembleType) -> dict:
    """Load a member map dictionary from the YAML file."""
    validate_ensemble(ensemble)
    member_id_map_path = config.get_member_id_map_path()
    with open(member_id_map_path, "r") as f:
        member_id_map = yaml.safe_load(f)
    try:
        return member_id_map[ensemble]
    except Exception as exc:
        raise KeyError(f"'{ensemble}' not a valid key in {member_id_map_path}") from exc


def get_canonical_member_ids(
    members: int | str | list[int | str],
    ensemble: EnsembleType,
) -> str | list[str]:
    """
    Map member identifiers to canonical member IDs.

    Parameters
    ----------
    members : int | str | list[int | str]
        Any member identifier.
    ensemble: str
        Ensemble name ('pisom', 'fhist', 'hadcm3').

    Returns
    -------
    member_ids : list[str]
        List of canonical member IDs corresponding to the member identifiers.
    """
    validate_ensemble(ensemble)

    # Ensure members is a list
    members = [members] if isinstance(members, (int, str)) else members

    # Convert member IDs to strings
    if all(isinstance(m, int) for m in members):
        return sorted([str(m) for m in members])
    
    # Load member ID -> parameter maps
    member_id_map = load_member_id_map(ensemble)

    # Logic to deal with member names
    if all(isinstance(m, str) for m in members):
        # Deal with HadCM3 members
        if ensemble == "hadcm3":
            return sorted([parse_member_id(m, ensemble) for m in members])

        member_ids = set()
        for m in members:
            try:
                # If format is a string number (e.g., '1', '001', '0001')
                member_ids.add(str(int(m)))

            except ValueError:
                # If format is a full member name (e.g., 'COUP0001_PI_SOM_v02', 'coupPPE.002')
                if any(pattern in m for pattern in ["COUP", "OFFL", "coupPPE", "offlPPE"]):
                    member_ids.add(parse_member_id(m))

                # If format is parameter name and min/max (e.g., 'fff,min')
                elif "," in m:
                    param, minmax = m.split(",")
                    assert (
                        param in member_id_map.keys()
                    ), f"{param} not a valid parameter in '{ensemble}'"
                    assert minmax in [
                        "min",
                        "max",
                    ], f"{minmax} not either 'min' or 'max'"
                    member_ids.add(member_id_map[param][minmax])

                # If format is parameter name (e.g, 'fff')
                else:
                    assert (
                        m in member_id_map.keys()
                    ), f"{m} not a valid parameter in '{ensemble}'"
                    member_ids.add(member_id_map[m]["min"])
                    member_ids.add(member_id_map[m]["max"])

        member_ids = list(member_ids)

    else:
        raise ValueError("All members must be of the same type (all int or all str)")

    return sorted(member_ids)
