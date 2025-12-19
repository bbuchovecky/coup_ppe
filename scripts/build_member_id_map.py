#!/usr/bin/env python
"""
Create YAML dictionary for the member ID map.
"""

from xppe.metadata import members


def main():
    members.build_member_id_map_yaml()

if __name__ == "__main__":
    main()
