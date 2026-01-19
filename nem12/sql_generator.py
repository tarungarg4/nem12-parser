"""SQLGenerator class for generating SQL INSERT statements."""

from __future__ import annotations

from typing import Generator, Iterable

from .meter_reading import MeterReading


class SQLGenerator:
    """
    Generates SQL INSERT statements for meter readings.
    
    Uses batch processing to create efficient multi-row INSERT statements
    that can be executed in a single database operation.
    
    Example:
        generator = SQLGenerator(batch_size=1000)
        for statement in generator.generate(readings):
            cursor.execute(statement)
    """
    
    TABLE_NAME = "meter_readings"
    COLUMNS = ("nmi", "timestamp", "consumption")
    
    def __init__(self, batch_size: int = 1000) -> None:
        """
        Initialize the SQL generator.
        
        Args:
            batch_size: Number of rows per INSERT statement.
                       Larger batches are more efficient but use more memory.
                       
        Raises:
            ValueError: If batch_size is less than 1
        """
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self.batch_size = batch_size
    
    def generate(
        self, 
        readings: Iterable[MeterReading]
    ) -> Generator[str, None, None]:
        """
        Generate SQL INSERT statements from meter readings.
        
        Args:
            readings: Iterable of MeterReading objects
            
        Yields:
            SQL INSERT statements as strings, each containing up to batch_size rows
        """
        batch: list[str] = []
        
        for reading in readings:
            value_tuple = self._format_value(reading)
            batch.append(value_tuple)
            
            if len(batch) >= self.batch_size:
                yield self._build_insert_statement(batch)
                batch = []
        
        # Yield any remaining rows
        if batch:
            yield self._build_insert_statement(batch)
    
    def _format_value(self, reading: MeterReading) -> str:
        """Format a single reading as a SQL value tuple."""
        # Escape single quotes in NMI (defensive)
        nmi_escaped = reading.nmi.replace("'", "''")
        timestamp_str = reading.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        return f"('{nmi_escaped}', '{timestamp_str}', {reading.consumption})"
    
    def _build_insert_statement(self, values: list[str]) -> str:
        """Build a multi-row INSERT statement."""
        columns_str = ", ".join(f'"{col}"' for col in self.COLUMNS)
        values_str = ",\n".join(values)
        
        return f"INSERT INTO {self.TABLE_NAME} ({columns_str}) VALUES\n{values_str};"
