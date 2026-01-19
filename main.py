#!/usr/bin/env python3
"""
NEM12 Meter Reading Parser - Command Line Interface

A production-grade parser for NEM12 format meter data files that generates
SQL INSERT statements for the meter_readings table.

Usage:
    python main.py <input_file> [--output <output_file>] [--batch-size <size>]

Example:
    python main.py sample_data.csv --output meter_readings.sql --batch-size 1000
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from nem12 import NEM12Parser, SQLGenerator


def process_file(
    input_path: Path,
    output_path: Path | None = None,
    batch_size: int = 1000
) -> int:
    """
    Process a NEM12 file and generate SQL INSERT statements.
    
    Args:
        input_path: Path to the input NEM12 CSV file
        output_path: Optional path to output SQL file. If None, writes to stdout.
        batch_size: Number of rows per INSERT statement
        
    Returns:
        Total number of readings processed
    """
    parser = NEM12Parser()
    generator = SQLGenerator(batch_size=batch_size)
    
    readings = parser.parse(input_path)
    statements = generator.generate(readings)
    
    total_readings = 0
    
    # Determine output destination
    if output_path:
        output_file = open(output_path, "w", encoding="utf-8")
    else:
        output_file = sys.stdout
    
    try:
        # Write header comment
        header = f"-- Generated from: {input_path.name}\n"
        header += f"-- Generated at: {datetime.now().isoformat()}\n"
        header += f"-- Batch size: {batch_size}\n\n"
        output_file.write(header)
        
        for statement in statements:
            output_file.write(statement)
            output_file.write("\n\n")
            # Count rows in this statement (count value tuples)
            total_readings += statement.count("('")
        
        # Write footer
        footer = f"-- Total readings: {total_readings}\n"
        output_file.write(footer)
        
    finally:
        if output_path:
            output_file.close()
    
    return total_readings


def main() -> int:
    """Main entry point for CLI usage."""
    parser = argparse.ArgumentParser(
        description="Parse NEM12 meter data files and generate SQL INSERT statements.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Output to stdout
    python main.py sample_data.csv
    
    # Output to file
    python main.py sample_data.csv --output meter_readings.sql
    
    # Custom batch size for very large files
    python main.py large_file.csv --output output.sql --batch-size 5000
        """
    )
    
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the NEM12 CSV input file"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=Path,
        dest="output_file",
        help="Path to the output SQL file (default: stdout)"
    )
    
    parser.add_argument(
        "-b", "--batch-size",
        type=int,
        default=1000,
        help="Number of rows per INSERT statement (default: 1000)"
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not args.input_file.exists():
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        return 1
    
    if not args.input_file.is_file():
        print(f"Error: Not a file: {args.input_file}", file=sys.stderr)
        return 1
    
    try:
        total = process_file(
            input_path=args.input_file,
            output_path=args.output_file,
            batch_size=args.batch_size
        )
        
        # Print summary to stderr (so it doesn't mix with stdout output)
        print(f"Successfully processed {total} readings.", file=sys.stderr)
        return 0
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
