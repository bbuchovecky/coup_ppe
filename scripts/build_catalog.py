#!/usr/bin/env python
"""
Build an intake-esm catalog for a PPE ensemble.
"""
import argparse
import coup_ppe.access.config
import coup_ppe.catalog.catalog
import coup_ppe.parallel.daskhelper


def main():
    parser = argparse.ArgumentParser(
        description="Build an intake-esm catalog for a PPE ensemble."
    )
    parser.add_argument(
        "kind",
        nargs="?",
        default="coup",
        choices=["coup", "offl"],
        help="Kind of simulation (coupled or offline)"
    )
    parser.add_argument(
        "ensemble",
        choices=["pisom", "fhist"],
        help="PPE short name"
    )
    parser.add_argument(
        "file_format",
        choices=["history", "timeseries"],
        help="Format of model output files"
    )
    parser.add_argument(
        "-d", "--dask-cluster",
        nargs=4,
        metavar=("ACCOUNT", "NWORKERS", "NMEM", "WALLTIME"),
        help="Launch a Dask cluster charged to ACCOUNT with NCORES cores, NWORKERS workers, NMEM memory per worker, and WALLTIME walltime"
    )
    
    args = parser.parse_args()

    # Define paths
    filesystem = coup_ppe.access.config.get_filesystem()
    data_root_path = coup_ppe.access.config.get_data_root_path(args.ensemble)
    catalog_root_path = coup_ppe.access.config.get_catalog_root_path()

    # Start Dask cluster if requested
    client, cluster = None, None
    if args.dask_cluster:
        account, nworkers, nmem, walltime = args.dask_cluster
        client, cluster = coup_ppe.parallel.daskhelper.create_dask_cluster(
            account=account,
            nworkers=int(nworkers),
            nmem=nmem,
            walltime=walltime,
        )

    coup_ppe.catalog.catalog.build_catalog(
        ensemble=args.ensemble,
        file_format=args.file_format,
        rootpath=data_root_path,
        outpath=catalog_root_path,
        tag=f"_{str(filesystem)}",
    )

    if client is not None and cluster is not None:
        coup_ppe.parallel.daskhelper.close_dask_cluster(client, cluster)


if __name__ == "__main__":
    main()
