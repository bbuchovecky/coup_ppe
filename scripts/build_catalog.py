#!/usr/bin/env python
"""
Build an intake-esm catalog for a PPE ensemble.
"""
import argparse
import coup_ppe.access.config
import coup_ppe.catalog.catalog


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
    
    args = parser.parse_args()

    # Define paths
    filesystem = coup_ppe.access.config.get_filesystem()
    data_root_path = coup_ppe.access.config.get_data_root_path(args.ensemble)
    catalog_root_path = coup_ppe.access.config.get_catalog_root_path()

    coup_ppe.catalog.catalog.build_catalog(
        ensemble=args.ensemble,
        file_format=args.file_format,
        rootpath=data_root_path,
        outpath=catalog_root_path,
        tag=f"_{str(filesystem)}",
    )


if __name__ == "__main__":
    main()
