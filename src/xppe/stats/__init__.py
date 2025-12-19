"""
Statistical functions for analyzing PPE output.
"""

from xppe.stats.regression import (
    ols_single,
    ols_field,
    odr_single,
    odr_field,
)

__all__ = [
    "ols_single",
    "ols_field",
    "odr_single",
    "odr_field",
]
