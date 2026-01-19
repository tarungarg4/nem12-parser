"""Tests for the MeterReading dataclass."""

from datetime import datetime
from decimal import Decimal

import pytest

from nem12 import MeterReading


class TestMeterReading:
    """Tests for the MeterReading dataclass."""
    
    def test_valid_reading(self):
        """Test creating a valid meter reading."""
        reading = MeterReading(
            nmi="NEM1201009",
            timestamp=datetime(2005, 3, 1, 0, 30),
            consumption=Decimal("0.461")
        )
        assert reading.nmi == "NEM1201009"
        assert reading.timestamp == datetime(2005, 3, 1, 0, 30)
        assert reading.consumption == Decimal("0.461")
    
    def test_empty_nmi_raises_error(self):
        """Test that empty NMI raises ValueError."""
        with pytest.raises(ValueError, match="Invalid NMI"):
            MeterReading(
                nmi="",
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("0.461")
            )
    
    def test_nmi_too_long_raises_error(self):
        """Test that NMI longer than 10 characters raises ValueError."""
        with pytest.raises(ValueError, match="Invalid NMI"):
            MeterReading(
                nmi="A" * 11,
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("0.461")
            )
    
    def test_negative_consumption_raises_error(self):
        """Test that negative consumption raises ValueError."""
        with pytest.raises(ValueError, match="Invalid consumption"):
            MeterReading(
                nmi="NEM1201009",
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("-1.5")
            )
    
    def test_zero_consumption_is_valid(self):
        """Test that zero consumption is valid."""
        reading = MeterReading(
            nmi="NEM1201009",
            timestamp=datetime(2005, 3, 1, 0, 30),
            consumption=Decimal("0")
        )
        assert reading.consumption == Decimal("0")
    
    def test_immutability(self):
        """Test that MeterReading instances are immutable."""
        reading = MeterReading(
            nmi="NEM1201009",
            timestamp=datetime(2005, 3, 1, 0, 30),
            consumption=Decimal("0.461")
        )
        with pytest.raises(AttributeError):
            reading.nmi = "CHANGED"
