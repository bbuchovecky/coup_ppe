"""
coup_ppe: Tools for PPE analysis.
"""

from xppe import access
from xppe import catalog
from xppe import stats
from xppe import metadata
from xppe.access.load import load_output, query_catalog

__version__ = "0.1.0"

__all__ = [
    # Submodules
    "access",
    "catalog",
    "stats",
    "metadata",
    # Common functions
    "load_output",
    "query_catalog",
]
