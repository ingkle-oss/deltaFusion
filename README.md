# deltaFusion

High-performance Delta Lake query engine for Python, powered by Rust.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

deltaFusion provides SQL query capabilities over Delta Lake tables using [DataFusion](https://github.com/apache/datafusion) as the query engine. Data is transferred via zero-copy [Apache Arrow](https://arrow.apache.org/) for maximum performance.

### Key Features

- **SQL Queries**: Full SQL support via DataFusion
- **Zero-Copy Transfer**: Arrow-based data exchange with Python (no serialization overhead)
- **Time Travel**: Query specific table versions
- **Storage Support**: Local filesystem and S3-compatible storage
- **GIL Release**: Python threads remain active during Rust operations

## Installation

```bash
# From source (requires Rust toolchain and maturin)
pip install maturin
maturin develop --release
```

## Quick Start

```python
from delta_fusion import DeltaEngine

# Create engine
engine = DeltaEngine()

# Register a Delta table
engine.register_table("sales", "/path/to/delta/table")

# Query with SQL (returns PyArrow RecordBatches)
batches = engine.query("SELECT * FROM sales WHERE year = 2024")

# Convert to pandas
import pyarrow as pa
table = pa.Table.from_batches(batches)
df = table.to_pandas()

# Or get results as list of dicts (for small datasets)
rows = engine.query_to_dicts("SELECT * FROM sales LIMIT 10")
```

### S3 Storage

```python
# Option 1: Environment variables
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# Option 2: Explicit configuration
engine = DeltaEngine({
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "aws_region": "us-east-1",
    "aws_endpoint": "http://localhost:9000",  # For MinIO
    "aws_allow_http": True
})

engine.register_table("data", "s3://bucket/delta-table")
```

### Time Travel

```python
# Query a specific version
engine.register_table("sales", "/path/to/table", version=5)

# Get table metadata
info = engine.table_info("/path/to/table")
print(f"Version: {info['version']}, Files: {info['num_files']}")
```

## Development

```bash
# Build for development
maturin develop

# Run Rust tests
cargo test --no-default-features

# Run Python tests
pytest

# Check and format
cargo check
cargo clippy
cargo fmt
```

## Architecture

```
src/
├── lib.rs       # Module entry point
├── config.rs    # Storage configuration
├── error.rs     # Error types
├── engine.rs    # DeltaEngine (DataFusion integration)
└── python.rs    # PyO3 bindings

python/delta_fusion/
├── __init__.py  # Python API
└── __init__.pyi # Type stubs
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
