# NEM12 Meter Reading Parser

A production-grade parser for NEM12 format meter data files that generates SQL INSERT statements for the `meter_readings` table.

## Features

- **Memory Efficient**: Uses streaming/generator-based processing to handle files of any size with constant memory usage
- **Batch Processing**: Generates multi-row INSERT statements for efficient database imports
- **Robust Validation**: Validates NMI, timestamps, and consumption values
- **Configurable**: Adjustable batch size for different use cases
- **Well Tested**: Comprehensive unit test suite

## Installation

No external dependencies required for core functionality. Uses Python 3.11+ standard library only.

For running tests:
```bash
pip install pytest
```

## Project Structure

```
flo-energy/
├── nem12/                        # Core package
│   ├── __init__.py               # Package exports
│   ├── meter_reading.py          # MeterReading dataclass
│   ├── nmi_context.py            # NMIContext dataclass
│   ├── parser.py                 # NEM12Parser class
│   └── sql_generator.py          # SQLGenerator class
├── tests/                        # Test package
│   ├── conftest.py               # Shared fixtures and helpers
│   ├── test_meter_reading.py     # MeterReading tests
│   ├── test_nmi_context.py       # NMIContext tests
│   ├── test_parser.py            # NEM12Parser tests
│   ├── test_sql_generator.py     # SQLGenerator tests
│   └── test_integration.py       # Integration tests
├── main.py                       # CLI entry point
├── sample_data.csv               # Sample NEM12 data
└── README.md
```

## Usage

### Command Line

```bash
# Output to stdout
python main.py sample_data.csv

# Output to file
python main.py sample_data.csv --output meter_readings.sql

# Custom batch size for very large files
python main.py large_file.csv --output output.sql --batch-size 5000
```

### As a Library

```python
from pathlib import Path
from nem12 import NEM12Parser, SQLGenerator

# Parse file and generate readings
parser = NEM12Parser()
readings = parser.parse(Path("sample_data.csv"))

# Generate SQL statements
generator = SQLGenerator(batch_size=1000)
for statement in generator.generate(readings):
    print(statement)
```

## NEM12 Format Overview

The parser handles the NEM12 format with the following record types:

| Record | Description | Key Fields |
|--------|-------------|------------|
| 100 | Header | File identifier |
| 200 | NMI Details | NMI (field 2), Interval length (field 9) |
| 300 | Interval Data | Date (field 2), Consumption values (fields 3-50) |
| 400 | Interval Event | Ignored |
| 500 | B2B Details | Ignored |
| 900 | End of Data | Stops parsing |

### Timestamp Calculation

For 30-minute intervals (48 readings per day):
- Interval 1 → 00:30 (end of first 30-min period)
- Interval 2 → 01:00
- ...
- Interval 48 → 00:00 next day

## Running Tests

```bash
python -m pytest tests/ -v
```

## Output Example

```sql
-- Generated from: sample_data.csv
-- Generated at: 2024-01-15T10:30:00
-- Batch size: 1000

INSERT INTO meter_readings ("nmi", "timestamp", "consumption") VALUES
('NEM1201009', '2005-03-01 00:30:00', 0),
('NEM1201009', '2005-03-01 01:00:00', 0),
('NEM1201009', '2005-03-01 06:30:00', 0.461);

-- Total readings: 384
```

---

## Assessment Questions

### Q1: What is the rationale for the technologies you have decided to use?

#### Language Choice: Python 3.11+

I chose **Python** as the implementation language for the following reasons:

| Rationale | Benefit |
|-----------|---------|
| **Built-in CSV module** | The standard library's `csv` module is lightweight and fully sufficient for parsing NEM12 format, eliminating external dependencies |
| **First-class generator support** | Python generators enable memory-efficient streaming, allowing the parser to process files of any size with constant O(1) memory usage |
| **Type hints with dataclasses** | Modern Python type annotations provide self-documenting code, better IDE support, and enable static analysis tools |
| **Standard library datetime** | Robust date/time handling for timestamp calculations without external dependencies |
| **Ubiquity and readability** | Python's widespread adoption makes the code accessible for reviewers and future maintainers |

#### Why No ORM?

I deliberately chose **direct SQL generation** over an ORM (like SQLAlchemy) because:

1. **Write-only operation**: This is a data ingestion task with no read queries; ORM's query-building capabilities provide no benefit
2. **Transparency**: Direct SQL output is auditable and can be reviewed before execution
3. **Batch control**: Direct SQL allows precise control over INSERT batch sizes for performance tuning
4. **Flexibility**: Output can be piped directly to `psql`, saved for review, or integrated into deployment pipelines

#### Why No External CSV Parser?

While libraries like `pandas` could parse CSVs, I opted for the standard library because:
- The NEM12 format is well-defined and simple enough for `csv.reader`
- Pandas would load entire files into memory, defeating our large-file requirements
- Zero external dependencies simplifies deployment and reduces security surface

---

### Q2: What would you have done differently if you had more time?

With additional time, I would prioritise the following enhancements:

#### High Priority

| Enhancement | Description | Rationale |
|-------------|-------------|-----------|
| **PostgreSQL COPY import** | Direct database connection with `COPY FROM` for bulk loading | 10-100x faster than individual INSERT statements for large datasets |
| **Validation mode** | Dry-run that scans the entire file for errors before generating SQL | Prevents partial imports from malformed files |
| **Duplicate handling** | ON CONFLICT clause for the unique constraint `(nmi, timestamp)` | Enables idempotent re-runs and incremental updates |

#### Medium Priority

| Enhancement | Description | Rationale |
|-------------|-------------|-----------|
| **Quality flag extraction** | Parse and store the quality flag (A=Actual, E=Estimated, S=Substituted) | Enables downstream data quality analysis |
| **400 record processing** | Parse interval-specific quality overrides from 400 records | Complete NEM12 specification compliance |
| **Structured logging** | Replace stderr warnings with proper logging framework | Production-ready observability |
| **Progress reporting** | Add `tqdm` progress bar for large file processing | Better user experience for long-running jobs |

#### Lower Priority

| Enhancement | Description | Rationale |
|-------------|-------------|-----------|
| **Parallel processing** | Multiprocessing with NMI-based partitioning | Horizontal scaling for very large files |
| **Multiple output formats** | Support JSON, CSV, Parquet output | Integration flexibility |
| **Docker packaging** | Containerised deployment with volume mounts | Consistent execution environment |
| **Configuration file** | YAML/TOML config for batch size, output format, etc. | Operator-friendly customisation |

---

### Q3: What is the rationale for the design choices that you have made?

#### 1. Streaming Architecture with Generators

**The Problem**: NEM12 files can be arbitrarily large (millions of readings across months of data).

**The Solution**: The parser uses Python generators to yield `MeterReading` objects one at a time:

```python
def parse(self, file_path: Path) -> Generator[MeterReading, None, None]:
    with open(file_path, "r") as f:
        for row in csv.reader(f):
            yield from self._parse_interval_record(row)
```

**Benefits**:
- **Constant memory**: O(1) memory regardless of file size
- **Lazy evaluation**: Processing starts immediately; no waiting for full file load
- **Composability**: Generators can be chained (`parse() → generate_sql()`) without intermediate storage

#### 2. Hierarchical State Machine

**The Problem**: NEM12 has inherent hierarchy—300 records depend on the preceding 200 record for NMI and interval length.

**The Solution**: The parser maintains minimal mutable state:

```python
self._current_context: NMIContext | None = None  # Updated on each 200 record
```

When a 300 record is encountered, it uses the current context to generate readings with correct NMI and timestamps. This approach:
- Correctly handles multiple 200 records (same NMI, different registers)
- Supports meter changes (same NMI, different meters over time)
- Minimises memory footprint (only stores current context, not all previous records)

#### 3. Batch Processing for INSERT Statements

**The Problem**: Executing thousands of individual INSERT statements is slow due to per-statement overhead.

**The Solution**: Generate multi-row INSERT statements with configurable batch size:

```sql
INSERT INTO meter_readings ("nmi", "timestamp", "consumption") VALUES
('NEM1201009', '2005-03-01 00:30:00', 0.461),
('NEM1201009', '2005-03-01 01:00:00', 0.810),
... (up to 1000 rows per statement)
```

**Trade-offs**:
- Larger batches = fewer round-trips = faster imports
- Smaller batches = lower memory usage = more granular error recovery
- Default of 1000 balances these concerns for typical use cases

#### 4. Immutable Dataclasses with Validation

**The Solution**: Use `@dataclass(frozen=True, slots=True)` with `__post_init__` validation:

```python
@dataclass(frozen=True, slots=True)
class MeterReading:
    nmi: str
    timestamp: datetime
    consumption: Decimal
    
    def __post_init__(self) -> None:
        if not self.nmi or len(self.nmi) > 10:
            raise ValueError(f"Invalid NMI: '{self.nmi}'")
```

**Benefits**:
- **Immutability**: Prevents accidental modification after creation
- **Memory efficiency**: `slots=True` reduces per-instance memory overhead
- **Fail-fast validation**: Invalid data is rejected immediately at construction
- **Self-documenting**: Type hints and dataclass syntax clearly define the data model

#### 5. Decimal for Consumption Values

**The Problem**: Floating-point arithmetic can introduce precision errors in energy calculations.

**The Solution**: Use Python's `Decimal` type:

```python
consumption = Decimal(consumption_str)  # Exact decimal representation
```

**Benefits**:
- Exact decimal representation matches PostgreSQL `numeric` type
- No floating-point precision surprises (e.g., `0.1 + 0.2 != 0.3` in float)
- Critical for financial/billing calculations in energy domain

#### 6. Graceful Error Handling

**The Problem**: Real-world data contains anomalies; failing on the first error would prevent processing of valid data.

**The Solution**: Log warnings for recoverable errors, fail only on unrecoverable ones:

```python
try:
    consumption = Decimal(consumption_str)
except InvalidOperation:
    print(f"Warning: Line {line}: Skipping invalid value '{consumption_str}'", file=sys.stderr)
    continue  # Skip this interval, continue processing
```

**Benefits**:
- Maximises data recovery from imperfect files
- Clear error messages with line numbers for debugging
- Unrecoverable errors (e.g., missing 200 record) still raise exceptions
