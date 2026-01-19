"""Shared test utilities and fixtures."""

import tempfile
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture
def temp_csv_file() -> Generator[Path, None, None]:
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        temp_path = Path(f.name)
    yield temp_path
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def temp_sql_file() -> Generator[Path, None, None]:
    """Create a temporary SQL file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        temp_path = Path(f.name)
    yield temp_path
    if temp_path.exists():
        temp_path.unlink()


def create_nem12_file(content: str, path: Path) -> Path:
    """Write NEM12 content to a file and return the path."""
    path.write_text(content)
    return path


def create_300_record(date: str, values: list, quality_flag: str = "A") -> str:
    """
    Helper to create a properly formatted 300 record with 48 intervals.
    
    Args:
        date: Interval date in YYYYMMDD format
        values: List of consumption values (will be padded to 48)
        quality_flag: Quality flag (default: 'A' for Actual)
        
    Returns:
        Formatted 300 record string
    """
    # Pad with empty values to make exactly 48 intervals
    padded_values = values + [""] * (48 - len(values))
    intervals_str = ",".join(str(v) for v in padded_values)
    return f"300,{date},{intervals_str},{quality_flag},,,20050310121004,20050310182204"
