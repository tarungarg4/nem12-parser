"""Tests for the NMIContext dataclass."""

import pytest

from nem12 import NMIContext


class TestNMIContext:
    """Tests for the NMIContext parsing."""
    
    def test_parse_valid_200_record(self):
        """Test parsing a valid 200 record."""
        fields = ["200", "NEM1201009", "E1E2", "1", "E1", "N1", "01009", "kWh", "30", "20050610"]
        context = NMIContext.from_record(fields)
        
        assert context.nmi == "NEM1201009"
        assert context.interval_minutes == 30
    
    def test_parse_15_minute_interval(self):
        """Test parsing a 200 record with 15-minute intervals."""
        fields = ["200", "TEST123456", "E1E2", "1", "E1", "N1", "01009", "kWh", "15", "20050610"]
        context = NMIContext.from_record(fields)
        
        assert context.interval_minutes == 15
    
    def test_parse_5_minute_interval(self):
        """Test parsing a 200 record with 5-minute intervals."""
        fields = ["200", "TEST123456", "E1E2", "1", "E1", "N1", "01009", "kWh", "5", "20050610"]
        context = NMIContext.from_record(fields)
        
        assert context.interval_minutes == 5
    
    def test_insufficient_fields_raises_error(self):
        """Test that 200 record with too few fields raises ValueError."""
        fields = ["200", "NEM1201009", "E1E2"]
        with pytest.raises(ValueError, match="insufficient fields"):
            NMIContext.from_record(fields)
    
    def test_empty_nmi_raises_error(self):
        """Test that empty NMI in 200 record raises ValueError."""
        fields = ["200", "", "E1E2", "1", "E1", "N1", "01009", "kWh", "30", "20050610"]
        with pytest.raises(ValueError, match="empty NMI"):
            NMIContext.from_record(fields)
    
    def test_whitespace_nmi_is_stripped(self):
        """Test that whitespace around NMI is stripped."""
        fields = ["200", "  NEM1201009  ", "E1E2", "1", "E1", "N1", "01009", "kWh", "30", "20050610"]
        context = NMIContext.from_record(fields)
        assert context.nmi == "NEM1201009"
    
    def test_invalid_interval_raises_error(self):
        """Test that non-numeric interval raises ValueError."""
        fields = ["200", "NEM1201009", "E1E2", "1", "E1", "N1", "01009", "kWh", "abc", "20050610"]
        with pytest.raises(ValueError, match="Invalid interval length"):
            NMIContext.from_record(fields)
    
    def test_zero_interval_raises_error(self):
        """Test that zero interval raises ValueError."""
        fields = ["200", "NEM1201009", "E1E2", "1", "E1", "N1", "01009", "kWh", "0", "20050610"]
        with pytest.raises(ValueError, match="must be positive"):
            NMIContext.from_record(fields)
    
    def test_negative_interval_raises_error(self):
        """Test that negative interval raises ValueError."""
        fields = ["200", "NEM1201009", "E1E2", "1", "E1", "N1", "01009", "kWh", "-30", "20050610"]
        with pytest.raises(ValueError, match="must be positive"):
            NMIContext.from_record(fields)
