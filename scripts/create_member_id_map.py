#!/usr/bin/env python
"""
Create YAML dictionary for the member ID map.
"""
import coup_ppe.metadata.members


def main():
    coup_ppe.metadata.members.create_member_id_map_yaml()

if __name__ == "__main__":
    main()
