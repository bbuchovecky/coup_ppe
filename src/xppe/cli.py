"""
Command line interface for coup_ppe.
"""

import argparse
from xppe.catalog import build_catalog_cli
from xppe.metadata import build_member_id_map_yaml


def build_member_id_map():
    parser = argparse.ArgumentParser(
        description="build a member ID map for all PPE ensembles listed in config.yml"
    )
    parser.parse_args()
    build_member_id_map_yaml()


def build_catalog():
    parser = argparse.ArgumentParser(
        description="build an intake-esm catalog for a PPE ensemble"
    )
    parser.add_argument(
        "kind",
        nargs="?",
        default="coup",
        choices=["coup", "offl"],
        help="kind of simulation (coupled or offline)"
    )
    parser.add_argument(
        "ensemble",
        choices=["pisom", "fhist"],
        help="PPE short name"
    )
    parser.add_argument(
        "file_format",
        choices=["history", "timeseries"],
        help="format of model output files"
    )
    parser.add_argument(
        "-d", "--dask-cluster",
        nargs=4,
        metavar=("ACCOUNT", "NWORKERS", "NMEM", "WALLTIME"),
        help="launch a Dask cluster charged to ACCOUNT with NCORES cores, NWORKERS workers, NMEM memory per worker, and WALLTIME walltime"
    )
    
    args = parser.parse_args()
    build_catalog_cli(
        kind=args.kind,
        ensemble=args.ensemble,
        file_format=args.file_format,
        dask_cluster=args.dask_cluster,
    )
