# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

deltaFusion is a high-performance Delta Lake query engine for Python, powered by Rust. It uses DataFusion as the SQL engine and provides zero-copy Arrow data transfer to Python via PyO3.

## Development Commands

```bash
# Build and install for development
maturin develop

# Build release wheel
maturin build --release

# Run Rust tests (without Python features)
cargo test --no-default-features

# Run Python tests
pytest

# Check and lint Rust code
cargo check
cargo clippy

# Format code
cargo fmt
```

## Architecture

```
src/
├── lib.rs       # Module entry point, re-exports
├── config.rs    # Storage configuration (S3, local)
├── error.rs     # Error types with Python conversion
├── engine.rs    # DeltaEngine with DataFusion SessionContext
└── python.rs    # PyO3 bindings with GIL release

python/delta_fusion/
├── __init__.py  # Python API wrapper
└── __init__.pyi # Type stubs
```

### Core Components

- **DeltaEngine**: Wraps DataFusion `SessionContext` with Delta Lake table support
- **PyDeltaEngine**: PyO3 class exposing engine to Python with async/GIL handling
- **StorageConfig**: S3/local storage configuration from environment or explicit options

### Key Design Decisions

1. **Zero-copy Arrow transfer**: Uses `pyo3-arrow` for efficient data transfer without serialization
2. **GIL release**: `Python::allow_threads` releases GIL during Rust async operations
3. **Feature-gated Python**: `--no-default-features` enables pure Rust testing
4. **Async bridging**: Tokio runtime wraps async delta-rs/DataFusion operations

### Performance Considerations

From expected-bottleneck.md:
- Log replay minimization via delta-rs Snapshot caching
- Zero-copy Arrow transfer (avoid Python dict conversion for large data)
- Predicate pushdown for I/O reduction
- GIL release during query execution

## Environment Variables

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: S3 credentials
- `AWS_REGION` / `AWS_DEFAULT_REGION`: S3 region
- `AWS_ENDPOINT_URL`: Custom S3 endpoint (MinIO, LocalStack)
- `AWS_ALLOW_HTTP`: Allow non-SSL connections
- `DELTA_FUSION_LOG`: Log level (default: warn)
