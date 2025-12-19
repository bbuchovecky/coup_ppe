#!/usr/bin/env python
"""
Build an intake-esm catalog for a PPE ensemble.
"""
import argparse
from xppe.access import config
from xppe.catalog import catalog
from xppe import daskhelper


def main():
    parser = argparse.ArgumentParser(
        description="build an intake-esm catalog for a PPE ensemble."
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

    # Define paths
    filesystem = config.get_filesystem()
    data_root_path = config.get_data_root_path(args.ensemble)
    catalog_root_path = config.get_catalog_root_path()

    # Start Dask cluster if requested
    client, cluster = None, None
    if args.dask_cluster:
        try:
            account, nworkers, nmem, walltime = args.dask_cluster
            client, cluster = daskhelper.create_dask_cluster(
                account=account,
                nworkers=int(nworkers),
                nmem=nmem,
                walltime=walltime,
            )
        except KeyError:
            print("Unable to start a dask cluster, continuing without")

    catalog.build_catalog(
        kind=args.kind,
        ensemble=args.ensemble,
        file_format=args.file_format,
        rootpath=data_root_path,
        outpath=catalog_root_path,
        tag=f"_{str(filesystem)}",
    )

    if client is not None and cluster is not None:
        daskhelper.close_dask_cluster(client, cluster)


if __name__ == "__main__":
    main()
