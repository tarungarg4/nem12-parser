"""MeterReading dataclass representing a single meter reading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class MeterReading:
    """
    Represents a single meter reading with NMI, timestamp, and consumption value.
    
    This is an immutable data structure that validates its contents on creation.
    
    Attributes:
        nmi: National Meter Identifier (1-10 characters)
        timestamp: The end time of the interval period
        consumption: The energy consumption value (must be non-negative)
    """
    
    nmi: str
    timestamp: datetime
    consumption: Decimal
    
    def __post_init__(self) -> None:
        """Validate meter reading data on creation."""
        if not self.nmi or len(self.nmi) > 10:
            raise ValueError(f"Invalid NMI: '{self.nmi}' (must be 1-10 characters)")
        if self.consumption < 0:
            raise ValueError(f"Invalid consumption: {self.consumption} (must be non-negative)")
