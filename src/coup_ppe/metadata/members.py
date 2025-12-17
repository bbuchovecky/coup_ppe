"""
Metadata and utilities for managing ensemble member identifiers.
"""
import pathlib
import pandas as pd
import yaml

import coup_ppe.access.paths
import coup_ppe.metadata.conventions


MODULE_DIR = pathlib.Path(__file__).parent.resolve()


def get_member_id(
        member: str,
        ensemble: str,
    ) -> str:
    """Helper function to get the member ID."""
    if ensemble == "pisom":
        return str(int(member[4:8]))
    elif ensemble in ["fhist", "fssp370"]:
        return str(int(member[-3:]))


def parse_member_id(
        member: str,
        ensemble: str = None,
    ) -> str:
    """Parse the member ID from a string."""
    if ensemble:
        return get_member_id(member, ensemble)
    else:
        if "coupPPE" in member:
            return get_member_id(member, "fhist")
        elif "COUP" in member:
            return get_member_id(member, "pisom")
        else:
            return str(int(member))


def crosswalk_csv_to_member_param_map(
        ensemble: str,
        cw_path: str | None = None,
    ):
    """
    Convert the crosswalk csv file to a member map dictionary where
    the keys are 'param' and the values are dicts mapping 'minmax' to 'member_id'.
    
    Returns
    -------
    dict
        Nested dictionary: {'param': {'minmax': 'member_id'}}
    """
    if cw_path:
        cw_path = pathlib.Path(cw_path).resolve()
    else:
        cw_path = coup_ppe.access.paths.CROSSWALK_PATH[ensemble]
    
    df = pd.read_csv(cw_path, names=["member", "param", "minmax"])

    member_param_map = {}
    for index, row in df.iterrows():
        param = row['param']
        minmax = row['minmax']
        member_id = parse_member_id(row['member'])
        
        # Initialize param dict if it doesn't exist
        if param not in member_param_map:
            member_param_map[param] = {}
        
        # Add the minmax -> member_id mapping
        member_param_map[param][minmax] = member_id
    
    return member_param_map


def create_maps_yaml_from_crosswalk_csv(
    csv_rootpath: str,
    yaml_outpath: str,
    ) -> str:
    """
    """
    csv_rootpath = pathlib.Path(csv_rootpath).resolve()
    cws_paths = list(csv_rootpath.glob("*_crosswalk.csv"))

    yaml_path = pathlib.Path(yaml_outpath).resolve()
    assert yaml_path.is_dir(), f"yaml_outpath must be a directory, got {yaml_outpath}"
    yaml_path = yaml_path / "member_id_map.yaml"

    member_param_map = {}
    for cw_path in cws_paths:
        cw_split = cw_path.stem.split("_")
        ensemble = cw_split[0]
        member_param_map[ensemble] = crosswalk_csv_to_member_param_map(ensemble, cw_path)
    
    # Save the member map dictionary as a YAML file
    with open(yaml_path, 'w') as f:
        yaml.dump(member_param_map, f, default_flow_style=False)

    return str(yaml_path)


def load_yaml(
    file: str,
    ) -> dict:
    """Load a member map dictionary from a YAML file."""
    path = pathlib.Path(file).resolve()
    assert path.suffix in ['.yml', '.yaml'], f"File must be a YAML file (.yml or .yaml), got {path.suffix}"

    # Load the YAML file as a dictionary
    with open(path, 'r') as f:
        member_map = yaml.safe_load(f)

    return member_map


def get_canonical_member_ids(
        members: int | str | list[int | str],
        ensemble: str,
    ) -> str | list[str]:
    """Map member identifiers to canonical member IDs."""

    # Ensure members is a list
    members = [members] if isinstance(members, (int, str)) else members

    # Load member ID -> parameter maps
    member_param_map = load_yaml(f"{str(MODULE_DIR)}/members_map.yaml")

    # Convert member IDs to strings
    if all(isinstance(m, int) for m in members):
        member_ids = [str(m) for m in members]
    
    # Logic to deal with member names
    elif all(isinstance(m, str) for m in members):
        member_ids = set()
        for m in members:
            try:
                # If format is a string number (e.g., '1', '001', '0001')
                member_ids.add(str(int(m)))
            
            except ValueError:
                # If format is a full member name (e.g., 'COUP0001_PI_SOM_v02', 'coupPPE.002')
                if any(pattern in m for pattern in ['COUP', 'coupPPE']):
                    member_ids.add(parse_member_id(m))
                
                # If format is parameter name and min/max (e.g., 'fff,min')
                elif "," in m:
                    param, minmax = m.split(",")
                    assert param in member_param_map[ensemble].keys(), f"{param} not a valid parameter in {coup_ppe.metadata.conventions.PPE_NAME_MAP[ensemble]}"
                    assert minmax in ["min", "max"], f"{minmax} not either 'min' or 'max'"
                    member_ids.add(member_param_map[ensemble][param][minmax])

                # If format is parameter name (e.g, 'fff')
                else:
                    assert m in member_param_map[ensemble].keys(), f"{m} not a valid parameter in {coup_ppe.metadata.conventions.PPE_NAME_MAP[ensemble]}"
                    member_ids.add(member_param_map[ensemble][m]['min'])
                    member_ids.add(member_param_map[ensemble][m]['max'])
        
        member_ids = list(member_ids)
    
    else:
        raise ValueError("All members must be of the same type (all int or all str)")
    
    return sorted(member_ids)
