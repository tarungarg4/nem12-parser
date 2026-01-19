"""NEM12Parser class for parsing NEM12 format meter data files."""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Generator, TextIO

from .meter_reading import MeterReading
from .nmi_context import NMIContext


class NEM12Parser:
    """
    Parser for NEM12 format meter data files.
    
    Uses streaming/generator-based processing to handle files of any size
    with constant memory usage. Maintains hierarchical context between
    200 (meter info) and 300 (interval data) records.
    
    Example:
        parser = NEM12Parser()
        for reading in parser.parse(Path("meter_data.csv")):
            print(f"{reading.nmi}: {reading.consumption} at {reading.timestamp}")
    """
    
    # Record type indicators
    RECORD_100 = "100"  # Header
    RECORD_200 = "200"  # NMI data details
    RECORD_300 = "300"  # Interval data
    RECORD_400 = "400"  # Interval event (not used)
    RECORD_500 = "500"  # B2B details (not used)
    RECORD_900 = "900"  # End of data
    
    def __init__(self) -> None:
        """Initialize the parser with empty state."""
        self._current_context: NMIContext | None = None
        self._line_number: int = 0
    
    def parse(self, file_path: Path) -> Generator[MeterReading, None, None]:
        """
        Parse a NEM12 file and yield MeterReading objects.
        
        This is a generator that reads the file line by line, maintaining
        minimal memory footprint regardless of file size.
        
        Args:
            file_path: Path to the NEM12 CSV file
            
        Yields:
            MeterReading objects for each interval reading
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file format is invalid
        """
        self._current_context = None
        self._line_number = 0
        
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            yield from self._parse_stream(f)
    
    def _parse_stream(self, stream: TextIO) -> Generator[MeterReading, None, None]:
        """Parse a text stream and yield MeterReading objects."""
        reader = csv.reader(stream)
        
        for row in reader:
            self._line_number += 1
            
            if not row:
                continue
            
            record_type = row[0].strip()
            
            if record_type == self.RECORD_200:
                self._current_context = NMIContext.from_record(row)
            
            elif record_type == self.RECORD_300:
                if self._current_context is None:
                    raise ValueError(
                        f"Line {self._line_number}: 300 record found without preceding 200 record"
                    )
                yield from self._parse_interval_record(row, self._current_context)
            
            elif record_type == self.RECORD_900:
                # End of file marker
                break
    
    def _parse_interval_record(
        self, 
        fields: list[str], 
        context: NMIContext
    ) -> Generator[MeterReading, None, None]:
        """
        Parse a 300 interval data record and yield MeterReading objects.
        
        300 record format:
        - Field 1: Record indicator (300)
        - Field 2: Interval date (YYYYMMDD)
        - Fields 3...: Interval consumption values (up to 288 for 5-min intervals)
        - Remaining fields: Quality flag, reason codes, update dates (not used here)
        
        Args:
            fields: List of field values from the CSV row
            context: The current NMI context from the preceding 200 record
            
        Yields:
            MeterReading objects for each valid interval value
        """
        if len(fields) < 3:
            raise ValueError(
                f"Line {self._line_number}: Invalid 300 record - insufficient fields"
            )
        
        # Parse the interval date
        date_str = fields[1].strip()
        try:
            interval_date = datetime.strptime(date_str, "%Y%m%d")
        except ValueError as e:
            raise ValueError(
                f"Line {self._line_number}: Invalid date format '{date_str}'"
            ) from e
        
        # Calculate how many intervals we expect based on interval length
        intervals_per_day = (24 * 60) // context.interval_minutes
        
        # Extract consumption values
        # Note: field indices are 0-based, so fields[2] is the first consumption value
        consumption_start_index = 2
        consumption_end_index = min(
            consumption_start_index + intervals_per_day,
            len(fields)
        )
        
        for i, consumption_str in enumerate(
            fields[consumption_start_index:consumption_end_index], 
            start=1
        ):
            # Skip empty values
            consumption_str = consumption_str.strip()
            if not consumption_str:
                continue
            
            try:
                consumption = Decimal(consumption_str)
            except InvalidOperation:
                # Log warning and skip invalid values rather than failing entirely
                print(
                    f"Warning: Line {self._line_number}: "
                    f"Skipping invalid consumption value '{consumption_str}' at interval {i}",
                    file=sys.stderr
                )
                continue
            
            # Calculate timestamp for this interval
            # Interval 1 ends at 00:30 (for 30-min intervals), interval 2 at 01:00, etc.
            timestamp = interval_date + timedelta(minutes=i * context.interval_minutes)
            
            yield MeterReading(
                nmi=context.nmi,
                timestamp=timestamp,
                consumption=consumption
            )
