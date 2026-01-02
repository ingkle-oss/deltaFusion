# delta_fusion Benchmark Results

> Benchmark Date: 2026-01-02
> Environment: macOS (Apple Silicon), Python 3.12
> Versions: delta_fusion 1.0.2, polars 1.36.1, pyarrow 22.0.0

## Executive Summary

| Scenario | delta_fusion | polars | Speedup |
|----------|--------------|--------|---------|
| Small files (720 files) | 65ms | 215ms | **3.3x faster** |
| Large dataset (1M rows) | 50ms | 56ms | **1.1x faster** |
| Time series query (10 min) | 38ms | N/A | Partition pruning |
| S3 COUNT(*) (1000 files) | 9ms | N/A | Metadata only |
| S3 partition query | 2159ms | N/A | Network bound |

## Test 1: Streaming Ingestion Scenario

**Scenario**: Simulates real-time data ingestion with 600 rows per file, 1 file per minute.

This is a common pattern in IoT, sensor data, and log ingestion where many small parquet files accumulate over time.

### Results

| Files | delta_fusion | polars | delta_fusion ms/file |
|-------|--------------|--------|----------------------|
| 10 | 1.81 ms | 3.92 ms | 0.181 |
| 30 | 3.75 ms | 10.84 ms | 0.125 |
| 60 | 6.23 ms | 19.13 ms | 0.104 |
| 120 | 11.52 ms | 37.08 ms | 0.096 |
| 360 | 33.03 ms | 109.78 ms | 0.092 |
| 720 | 65.42 ms | 215.40 ms | 0.091 |

### Key Observations

1. **Linear Scaling**: delta_fusion scales linearly at ~0.09 ms/file
2. **3.3x Faster**: Consistently faster than polars across all file counts
3. **No Fixed Overhead**: Unlike previous SQL-based approach (which had ~150ms fixed cost)

### Why delta_fusion is Faster

delta_fusion uses **direct parquet reading** via Arrow APIs instead of DataFusion SQL:

```
Previous approach (SQL-based):
┌─────────────────────────────────────────────────────────┐
│ register_parquet() → SQL parsing → Query planning → Execute │
│         ~50ms            ~50ms         ~50ms                 │
│                    Total: ~150ms fixed overhead              │
└─────────────────────────────────────────────────────────────┘

Current approach (Direct reading):
┌─────────────────────────────────────────────────────────┐
│ File.open() → ParquetReader → Arrow filter → Results       │
│    ~0.05ms        ~0.03ms        ~0.01ms                    │
│              Total: ~0.09ms per file                        │
└─────────────────────────────────────────────────────────────┘
```

## Test 2: Query Range Performance

**Scenario**: 720 files (12 hours of data), query different time ranges.

Tests how query time scales with the amount of data being read.

### Results

| Query Range | Mean (ms) | Files Read | Rows |
|-------------|-----------|------------|------|
| 1 minute | 53.38 | 1 | 600 |
| 5 minutes | 55.14 | 5 | 3,000 |
| 30 minutes | 55.10 | 30 | 18,000 |
| 1 hour | 55.46 | 60 | 36,000 |
| 6 hours | 59.50 | 360 | 216,000 |
| 12 hours (all) | 64.08 | 720 | 432,000 |

### Key Observations

1. **Partition Pruning**: Only reads files in the requested date range
2. **Consistent Performance**: ~53-64ms regardless of query range
3. **Efficient Filtering**: Arrow-native timestamp filtering on read

## Test 3: Large Dataset Comparison

**Scenario**: 1 million rows (100 files × 10,000 rows each)

Compares full scan and filtered query performance.

### Results

| Library | Operation | Mean (ms) | Rows |
|---------|-----------|-----------|------|
| delta_fusion | 10 min query | 37.95 | 33,000 |
| delta_fusion | Full scan | 50.19 | 1,000,000 |
| polars | Full scan | 55.87 | 1,000,000 |
| pyarrow | Full scan | 64.67 | 1,000,000 |

### Key Observations

1. **Competitive Performance**: delta_fusion matches or beats polars/pyarrow
2. **Partition Advantage**: Time series queries benefit from partition pruning
3. **Zero-Copy Transfer**: Arrow data passes to Python without serialization

## Test 4: S3 Cloud Storage Performance

**Scenario**: Query Delta Lake table stored on S3-compatible storage (MinIO).

Tests real-world cloud storage performance with network latency.

### Environment

- Storage: MinIO (S3-compatible)
- Table: ~1000 parquet files, date-partitioned
- Network: Local network (192.168.x.x)
- Delta Version: 306433

### Results

| Query Type | Mean (ms) | Notes |
|------------|-----------|-------|
| SELECT * LIMIT 10 | 168 | Initial metadata fetch |
| SELECT * LIMIT 100 | 143 | Warm cache |
| SELECT * LIMIT 1000 | 125 | Batch efficiency |
| SELECT * LIMIT 10000 | 262 | More data transfer |
| COUNT(*) | 9 | Metadata-only query |
| WHERE date='2026-01-02' | 2159 | 14,249 rows, partition filter |
| 2 columns LIMIT 10000 | 146 | Column projection |

### Key Observations

1. **COUNT(*) is extremely fast** (~9ms): Uses Delta Lake metadata without scanning files
2. **LIMIT queries are efficient**: Only fetches required rows from first matching files
3. **Partition filtering works**: Only scans files in the requested date partition
4. **Column projection reduces I/O**: Selecting fewer columns is faster than SELECT *
5. **Network latency is the dominant factor**: S3 is ~2-4x slower than local storage

### S3 vs Local Comparison

| Operation | Local (ms) | S3 (ms) | S3 Overhead |
|-----------|------------|---------|-------------|
| Small query (LIMIT 10) | ~1-2 | ~168 | Network RTT |
| Large query (10K rows) | ~65 | ~262 | ~4x slower |
| COUNT(*) | N/A | ~9 | Metadata only |

### S3 Configuration

```python
from delta_fusion import DeltaEngine

engine = DeltaEngine(
    endpoint_url='http://your-minio:9000',
    access_key='your-access-key',
    secret_key='your-secret-key',
    region='us-east-1',
    allow_http=True  # For non-SSL endpoints
)

# Query S3-based Delta table
result = engine.query(
    "s3://bucket/path/to/delta",
    "SELECT * FROM delta WHERE date = '2026-01-02'"
)
```

## Architecture: Why It's Fast

### 1. Direct Parquet Reading (for local files)

```rust
// Instead of: ctx.register_parquet() + ctx.sql()
// We use: ParquetRecordBatchReader + Arrow filter

for file_path in files {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    for batch in reader {
        // Arrow-native filtering
        let mask = and(
            gt_eq(ts_array, &start_scalar)?,
            lt(ts_array, &end_scalar)?
        )?;
        let filtered = filter_record_batch(&batch, &mask)?;
    }
}
```

### 2. Partition Pruning

```
Query: 2024-01-15T10:00:00 to 2024-01-15T14:00:00

Data Layout:
├── date=2024-01-14/  ← SKIPPED
├── date=2024-01-15/  ← READ (only matching partition)
└── date=2024-01-16/  ← SKIPPED
```

### 3. Zero-Copy Arrow Transfer

```
Rust RecordBatch → PyArrow RecordBatch (shared memory, no copy)
```

## When to Use delta_fusion

### Best For

- **Streaming/IoT data**: Many small files per partition
- **Time series queries**: Date-partitioned data with time range filters
- **Real-time dashboards**: Low-latency queries on recent data
- **Python applications**: Zero-copy Arrow integration

### Consider Alternatives For

- **Complex SQL joins**: Use DataFusion or DuckDB directly
- **Delta-specific features**: Use delta-rs for ACID transactions, CDC
- **Non-partitioned data**: Polars or DuckDB may be simpler

## Reproducing Benchmarks

```bash
# Install dependencies
pip install delta_fusion polars pyarrow

# Run benchmarks
python benchmarks/run_benchmark.py
```

## Appendix: System Information

```
Platform: macOS (Darwin 24.6.0)
Architecture: ARM64 (Apple Silicon)
Python: 3.12
Rust: 1.83 (compiled with --release)
```
