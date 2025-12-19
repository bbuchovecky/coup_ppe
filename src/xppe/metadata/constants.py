"""
Constants for scientific calculations.
"""

CONSTANTS = {
    "G": {
        "value": 9.81,
        "units": "m/s/s",
        "long_name": "gravitational acceleration",
    },
    "LV": {
        "value": 2.260e6,
        "units": "J/kg",
        "long_name": "latent heat of vaporization",
    },
    "CP": {
        "value": 1004,
        "units": "J/kg/K",
        "long_name": "specific heat capacity at constant pressure",
    },
}

# For convenience, also expose as module-level variables
G = CONSTANTS["G"]["value"]
LV = CONSTANTS["LV"]["value"]
CP = CONSTANTS["CP"]["value"]
