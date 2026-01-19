"""Integration tests for the process_file function."""

from pathlib import Path

from main import process_file
from tests.conftest import create_300_record, create_nem12_file


class TestProcessFile:
    """Integration tests for the process_file function."""
    
    def test_process_sample_data(self, temp_csv_file: Path, temp_sql_file: Path):
        """Test processing the sample data file."""
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", [0.5, 0.6, 0.7])}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        total = process_file(temp_csv_file, temp_sql_file, batch_size=100)
        
        assert total == 3
        
        # Read output file and verify content
        sql_content = temp_sql_file.read_text()
        assert "INSERT INTO meter_readings" in sql_content
        assert "NEM1201009" in sql_content
        assert "Total readings: 3" in sql_content
    
    def test_process_multiple_days(self, temp_csv_file: Path, temp_sql_file: Path):
        """Test processing multiple days of data."""
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", [1.0, 2.0])}
{create_300_record("20050302", [3.0, 4.0])}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        total = process_file(temp_csv_file, temp_sql_file, batch_size=100)
        
        assert total == 4
    
    def test_process_with_custom_batch_size(self, temp_csv_file: Path, temp_sql_file: Path):
        """Test processing with custom batch size."""
        # Create 5 readings
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        content = f"""100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
{create_300_record("20050301", values)}
900
"""
        create_nem12_file(content, temp_csv_file)
        
        total = process_file(temp_csv_file, temp_sql_file, batch_size=2)
        
        assert total == 5
        
        # Should have 3 INSERT statements (2 + 2 + 1)
        sql_content = temp_sql_file.read_text()
        assert sql_content.count("INSERT INTO") == 3
