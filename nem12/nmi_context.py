"""NMIContext dataclass for storing 200 record context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class NMIContext:
    """
    Context from a 200 record that applies to subsequent 300 records.
    
    The NEM12 format uses 200 records to define the meter configuration,
    which then applies to all following 300 (interval data) records until
    the next 200 record is encountered.
    
    Attributes:
        nmi: National Meter Identifier from the 200 record
        interval_minutes: The interval length in minutes (e.g., 5, 15, or 30)
    """
    
    nmi: str
    interval_minutes: int
    
    @classmethod
    def from_record(cls, fields: list[str]) -> NMIContext:
        """
        Parse a 200 record to extract NMI context.
        
        200 record format:
        - Field 1: Record indicator (200)
        - Field 2: NMI (National Meter Identifier)
        - Field 3: NMIConfiguration 
        - Field 4: RegisterID 
        - Field 5: NMISuffix 
        - Field 6: MDMDataStreamIdentifier 
        - Field 7: MeterSerialNumber 
        - Field 8: UOM (Unit of Measure)
        - Field 9: IntervalLength 
        - Field 10: NextScheduledReadDate
        
        Args:
            fields: List of field values from the CSV row
            
        Returns:
            NMIContext with extracted NMI and interval length
            
        Raises:
            ValueError: If the record is malformed or contains invalid data
        """
        if len(fields) < 9:
            raise ValueError(
                f"Invalid 200 record: insufficient fields (got {len(fields)}, need at least 9)"
            )
        
        nmi = fields[1].strip()
        if not nmi:
            raise ValueError("Invalid 200 record: empty NMI")
        
        try:
            interval_minutes = int(fields[8])
        except ValueError as e:
            raise ValueError(f"Invalid interval length in 200 record: '{fields[8]}'") from e
        
        if interval_minutes <= 0:
            raise ValueError(f"Invalid interval length: {interval_minutes} (must be positive)")
        
        return cls(nmi=nmi, interval_minutes=interval_minutes)
