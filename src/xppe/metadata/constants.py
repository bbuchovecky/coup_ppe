"""
Constants for scientific calculations.
"""

from dataclasses import dataclass
from typing import Dict, Callable


@dataclass(frozen=True)
class Constant:
    """Represents a physical constant with metadata."""
    value: float
    units: str
    long_name: str


class PhysicalConstants:
    """Physical constants for xppe."""

    G = Constant(
        value=9.81,
        units="m/s/s",
        long_name="gravitational accelerations",
    )

    CP = Constant(
        value=1004,
        units="J/kg",
        long_name="specific heat capacity at constant pressure",
    )

    LV = Constant(
        value=2.260e6,
        units="J/kg",
        long_name="latent heat of vaporization of water",
    )

    @classmethod
    def get_all(cls) -> Dict[str, Callable]:
        """Get all constants as a dictionary."""
        return {
            name: getattr(cls, name)
            for name in dir(cls)
            if isinstance(getattr(cls, name), Constant)
        }

    @classmethod
    def get_value(cls, name: str) -> float:
        """Get just the numerical value of a constant."""
        const = getattr(cls, name)
        if isinstance(const, Constant):
            return const.value
        raise AttributeError(f"Unknown constant: {name}")


# Expose as module-level variables
G = PhysicalConstants.G.value
LV = PhysicalConstants.LV.value
CP = PhysicalConstants.CP.value
