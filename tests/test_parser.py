"""Tests for the NEM12Parser class."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from nem12 import NEM12Parser
from tests.conftest import create_300_record, create_nem12_file


class TestNEM12Parser:
    """Tests for the NEM12Parser class."""
    
    def test_parse_simple_file(self, temp_csv_file: Path):
        """Test parsing a simple NEM12 file with one 200 and one 300 record."""
        values = [0.5, 0.6, 0.7]
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", values)}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        assert len(readings) == 3
        
        # Check first reading
        assert readings[0].nmi == "NEM1201009"
        assert readings[0].timestamp == datetime(2005, 3, 1, 0, 30)
        assert readings[0].consumption == Decimal("0.5")
        
        # Check second reading
        assert readings[1].timestamp == datetime(2005, 3, 1, 1, 0)
        assert readings[1].consumption == Decimal("0.6")
        
        # Check third reading
        assert readings[2].timestamp == datetime(2005, 3, 1, 1, 30)
        assert readings[2].consumption == Decimal("0.7")
    
    def test_parse_multiple_nmi(self, temp_csv_file: Path):
        """Test parsing file with multiple 200 records (different NMIs)."""
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", [1.0])}
200,NEM1201010,E1E2,2,E2,,01009,kWh,30,20050610
{create_300_record("20050301", [2.0])}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        assert len(readings) == 2
        assert readings[0].nmi == "NEM1201009"
        assert readings[1].nmi == "NEM1201010"
    
    def test_parse_48_intervals(self, temp_csv_file: Path):
        """Test parsing a 300 record with all 48 intervals (full day at 30 min)."""
        values = [i * 0.1 for i in range(48)]
        
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", values)}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        assert len(readings) == 48
        
        # First interval ends at 00:30
        assert readings[0].timestamp == datetime(2005, 3, 1, 0, 30)
        
        # Last interval ends at 24:00 (midnight next day)
        assert readings[47].timestamp == datetime(2005, 3, 2, 0, 0)
    
    def test_parse_15_minute_intervals(self, temp_csv_file: Path):
        """Test parsing with 15-minute interval length (96 readings per day)."""
        values = [0.1, 0.2, 0.3, 0.4] + [""] * 92
        intervals_str = ",".join(str(v) for v in values)
        
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,15,20050610
300,20050301,{intervals_str},A,,,20050310121004,
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        assert len(readings) == 4
        assert readings[0].timestamp == datetime(2005, 3, 1, 0, 15)
        assert readings[1].timestamp == datetime(2005, 3, 1, 0, 30)
        assert readings[2].timestamp == datetime(2005, 3, 1, 0, 45)
        assert readings[3].timestamp == datetime(2005, 3, 1, 1, 0)
    
    def test_skip_empty_consumption_values(self, temp_csv_file: Path):
        """Test that empty consumption values are skipped."""
        values = [0.5, "", 0.7] + [""] * 45
        intervals_str = ",".join(str(v) for v in values)
        
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
300,20050301,{intervals_str},A,,,20050310121004,
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        # Should have 2 readings (skipping the empty one)
        assert len(readings) == 2
        assert readings[0].consumption == Decimal("0.5")
        assert readings[1].consumption == Decimal("0.7")
        
        # Timestamps should be for intervals 1 and 3
        assert readings[0].timestamp == datetime(2005, 3, 1, 0, 30)
        assert readings[1].timestamp == datetime(2005, 3, 1, 1, 30)
    
    def test_300_without_200_raises_error(self, temp_csv_file: Path):
        """Test that 300 record without preceding 200 record raises error."""
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
{create_300_record("20050301", [0.5])}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        with pytest.raises(ValueError, match="without preceding 200 record"):
            list(parser.parse(temp_csv_file))
    
    def test_parse_stops_at_900_record(self, temp_csv_file: Path):
        """Test that parsing stops at 900 (end) record."""
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", [1.0])}
900
200,SHOULDNOTPARSE,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", [2.0])}
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        # Should only have reading from before the 900 record
        assert len(readings) == 1
        assert readings[0].nmi == "NEM1201009"


class TestTimestampCalculation:
    """Tests for timestamp calculation logic in NEM12Parser."""
    
    def test_midnight_boundary(self, temp_csv_file: Path):
        """Test that interval 48 (for 30-min) correctly crosses to next day."""
        values = [1.0] * 48
        
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", values)}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        # Last reading should be at midnight on March 2nd
        last_reading = readings[-1]
        assert last_reading.timestamp.day == 2
        assert last_reading.timestamp.hour == 0
        assert last_reading.timestamp.minute == 0
    
    def test_specific_interval_times(self, temp_csv_file: Path):
        """Test specific interval timestamps for 30-minute intervals."""
        values = list(range(1, 14))  # 13 values
        
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", values)}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        parser = NEM12Parser()
        readings = list(parser.parse(temp_csv_file))
        
        # Verify key timestamps
        expected_times = [
            (0, 30),   # Interval 1: 00:30
            (1, 0),    # Interval 2: 01:00
            (1, 30),   # Interval 3: 01:30
            (2, 0),    # Interval 4: 02:00
            (2, 30),   # Interval 5: 02:30
            (3, 0),    # Interval 6: 03:00
            (3, 30),   # Interval 7: 03:30
            (4, 0),    # Interval 8: 04:00
            (4, 30),   # Interval 9: 04:30
            (5, 0),    # Interval 10: 05:00
            (5, 30),   # Interval 11: 05:30
            (6, 0),    # Interval 12: 06:00
            (6, 30),   # Interval 13: 06:30
        ]
        
        for i, (hour, minute) in enumerate(expected_times):
            assert readings[i].timestamp.hour == hour
            assert readings[i].timestamp.minute == minute
