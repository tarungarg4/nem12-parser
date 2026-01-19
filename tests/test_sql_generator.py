"""Tests for the SQLGenerator class."""

from datetime import datetime
from decimal import Decimal

import pytest

from nem12 import MeterReading, SQLGenerator


class TestSQLGenerator:
    """Tests for the SQLGenerator class."""
    
    def test_generate_single_reading(self):
        """Test generating SQL for a single reading."""
        readings = [
            MeterReading(
                nmi="NEM1201009",
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("0.461")
            )
        ]
        
        generator = SQLGenerator(batch_size=100)
        statements = list(generator.generate(readings))
        
        assert len(statements) == 1
        assert "INSERT INTO meter_readings" in statements[0]
        assert "NEM1201009" in statements[0]
        assert "2005-03-01 00:30:00" in statements[0]
        assert "0.461" in statements[0]
    
    def test_generate_batch_size(self):
        """Test that batching works correctly."""
        readings = [
            MeterReading(
                nmi="NEM1201009",
                timestamp=datetime(2005, 3, 1, i, 0),
                consumption=Decimal(str(i))
            )
            for i in range(5)
        ]
        
        generator = SQLGenerator(batch_size=2)
        statements = list(generator.generate(readings))
        
        # 5 readings with batch size 2 = 3 statements (2 + 2 + 1)
        assert len(statements) == 3
    
    def test_column_quoting(self):
        """Test that column names are properly quoted."""
        readings = [
            MeterReading(
                nmi="TEST",
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("1.0")
            )
        ]
        
        generator = SQLGenerator()
        statements = list(generator.generate(readings))
        
        # Columns should be double-quoted (PostgreSQL style)
        assert '"nmi"' in statements[0]
        assert '"timestamp"' in statements[0]
        assert '"consumption"' in statements[0]
    
    def test_sql_injection_prevention(self):
        """Test that single quotes in NMI are properly escaped."""
        readings = [
            MeterReading(
                nmi="TEST'123",
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("1.0")
            )
        ]
        
        generator = SQLGenerator()
        statements = list(generator.generate(readings))
        
        # Single quote should be escaped
        assert "TEST''123" in statements[0]
    
    def test_invalid_batch_size_raises_error(self):
        """Test that batch size less than 1 raises ValueError."""
        with pytest.raises(ValueError, match="batch_size must be at least 1"):
            SQLGenerator(batch_size=0)
    
    def test_empty_readings_produces_no_statements(self):
        """Test that empty readings iterable produces no statements."""
        generator = SQLGenerator()
        statements = list(generator.generate([]))
        
        assert len(statements) == 0
    
    def test_default_batch_size(self):
        """Test that default batch size is 1000."""
        generator = SQLGenerator()
        assert generator.batch_size == 1000
