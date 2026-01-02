#!/usr/bin/env python3
"""
Comprehensive benchmark comparing delta_fusion with other Delta Lake libraries.

Compares:
- delta_fusion (this library)
- deltalake-python (delta-rs Python bindings)
- polars (native Delta reading)

Run: python benchmarks/run_benchmark.py
"""

import gc
import os
import shutil
import statistics
import time
import tracemalloc
import threading
from datetime import datetime, timedelta
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.parquet as pq

# Check available libraries
HAVE_DELTALAKE = False
HAVE_POLARS = False
HAVE_DELTA_FUSION = False

try:
    import deltalake
    HAVE_DELTALAKE = True
except ImportError:
    print("Warning: deltalake not installed, skipping deltalake benchmarks")

try:
    import polars as pl
    HAVE_POLARS = True
except ImportError:
    print("Warning: polars not installed, skipping polars benchmarks")

try:
    import delta_fusion
    HAVE_DELTA_FUSION = True
except ImportError:
    print("Warning: delta_fusion not installed, run 'maturin develop' first")


# =============================================================================
# Configuration
# =============================================================================

BENCHMARK_DIR = Path(__file__).parent / "data"
NUM_ROWS_OPTIONS = [10_000, 100_000, 1_000_000]  # Different data sizes
NUM_WARMUP = 1
NUM_RUNS = 5


# =============================================================================
# Data Generation
# =============================================================================

def generate_test_data(num_rows: int) -> pa.Table:
    """Generate test data with time series structure."""
    base_date = datetime(2024, 1, 1)

    # Generate timestamps across 30 days
    timestamps = []
    dates = []
    for i in range(num_rows):
        dt = base_date + timedelta(seconds=i * 10)
        timestamps.append(dt)
        dates.append(dt.strftime("%Y-%m-%d"))

    # Create Arrow table
    table = pa.table({
        "timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "date": pa.array(dates),
        "id": pa.array(range(num_rows)),
        "value": pa.array([float(i % 1000) for i in range(num_rows)]),
        "category": pa.array([f"cat_{i % 10}" for i in range(num_rows)]),
        "description": pa.array([f"Description for row {i}" for i in range(num_rows)]),
    })

    return table


def create_delta_table(path: Path, num_rows: int):
    """Create a Delta table with test data."""
    if path.exists():
        shutil.rmtree(path)

    table = generate_test_data(num_rows)

    if HAVE_DELTALAKE:
        # Use deltalake to create table (partitioned by date)
        deltalake.write_deltalake(
            str(path),
            table,
            partition_by=["date"],
            mode="overwrite",
        )
        print(f"  Created Delta table at {path} with {num_rows:,} rows")
    else:
        raise RuntimeError("deltalake required to create test tables")


def create_parquet_partitioned(path: Path, num_rows: int):
    """Create partitioned parquet files (for time series benchmark)."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)

    table = generate_test_data(num_rows)
    df = table.to_pandas()

    # Write partitioned by date
    for date, group in df.groupby("date"):
        partition_dir = path / f"date={date}"
        partition_dir.mkdir(exist_ok=True)

        partition_table = pa.Table.from_pandas(group.drop(columns=["date"]))
        pq.write_table(partition_table, partition_dir / "data.parquet")

    print(f"  Created partitioned parquet at {path} with {num_rows:,} rows")


# =============================================================================
# Benchmark Utilities
# =============================================================================

class ResourceMonitor:
    """Monitor CPU and memory usage during benchmark execution."""

    def __init__(self, interval: float = 0.01):
        self.interval = interval
        self.cpu_samples = []
        self.memory_samples = []
        self._stop = False
        self._thread = None
        self.process = psutil.Process()

    def start(self):
        self._stop = False
        self.cpu_samples = []
        self.memory_samples = []
        self._thread = threading.Thread(target=self._monitor)
        self._thread.start()

    def stop(self):
        self._stop = True
        if self._thread:
            self._thread.join()

    def _monitor(self):
        while not self._stop:
            try:
                self.cpu_samples.append(self.process.cpu_percent())
                self.memory_samples.append(self.process.memory_info().rss / 1024 / 1024)  # MB
            except:
                pass
            time.sleep(self.interval)

    def get_stats(self) -> dict:
        return {
            "cpu_mean": statistics.mean(self.cpu_samples) if self.cpu_samples else 0,
            "cpu_max": max(self.cpu_samples) if self.cpu_samples else 0,
            "mem_mean": statistics.mean(self.memory_samples) if self.memory_samples else 0,
            "mem_max": max(self.memory_samples) if self.memory_samples else 0,
        }


def run_benchmark(name: str, func, warmup: int = NUM_WARMUP, runs: int = NUM_RUNS) -> dict:
    """Run a benchmark function multiple times and collect statistics."""
    gc.collect()

    # Warmup
    for _ in range(warmup):
        try:
            func()
        except Exception as e:
            return {"name": name, "error": str(e)}

    # Timed runs with resource monitoring
    times = []
    memory_peaks = []
    cpu_maxes = []
    result_count = 0

    monitor = ResourceMonitor()

    for _ in range(runs):
        gc.collect()

        # Start monitoring
        tracemalloc.start()
        monitor.start()

        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start

        # Stop monitoring
        monitor.stop()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        memory_peaks.append(peak / 1024 / 1024)  # MB

        stats = monitor.get_stats()
        cpu_maxes.append(stats["cpu_max"])

        # Get result count
        if hasattr(result, "__len__"):
            result_count = len(result)
        elif hasattr(result, "num_rows"):
            result_count = result.num_rows

    return {
        "name": name,
        "mean": statistics.mean(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times),
        "mem_peak": statistics.mean(memory_peaks),
        "cpu_max": statistics.mean(cpu_maxes),
        "runs": runs,
        "rows": result_count,
    }


def print_results(results: list[dict], title: str):
    """Print benchmark results in a formatted table."""
    print(f"\n{'=' * 90}")
    print(f" {title}")
    print("=" * 90)
    print(f"{'Library':<25} {'Mean (s)':<10} {'Std':<8} {'Min':<8} {'Mem (MB)':<10} {'CPU %':<8}")
    print("-" * 90)

    for r in sorted(results, key=lambda x: x.get("mean", float("inf"))):
        if "error" in r:
            print(f"{r['name']:<25} ERROR: {r['error'][:50]}")
        else:
            print(
                f"{r['name']:<25} "
                f"{r['mean']:<10.4f} "
                f"{r['std']:<8.4f} "
                f"{r['min']:<8.4f} "
                f"{r.get('mem_peak', 0):<10.2f} "
                f"{r.get('cpu_max', 0):<8.1f}"
            )

    print()


# =============================================================================
# Benchmark Tests
# =============================================================================

def benchmark_full_scan(delta_path: Path, num_rows: int) -> list[dict]:
    """Benchmark full table scan."""
    results = []
    table_uri = str(delta_path)

    # delta_fusion
    if HAVE_DELTA_FUSION:
        engine = delta_fusion.DeltaEngine()
        engine.register_table("bench", table_uri)

        def df_query():
            return engine.query("SELECT * FROM bench")

        results.append(run_benchmark("delta_fusion", df_query))

    # deltalake
    if HAVE_DELTALAKE:
        def dl_query():
            dt = deltalake.DeltaTable(table_uri)
            return dt.to_pyarrow_table()

        results.append(run_benchmark("deltalake-python", dl_query))

    # polars
    if HAVE_POLARS:
        def pl_query():
            return pl.read_delta(table_uri)

        results.append(run_benchmark("polars", pl_query))

    return results


def benchmark_filtered_query(delta_path: Path, num_rows: int) -> list[dict]:
    """Benchmark filtered query (WHERE clause on partition column)."""
    results = []
    table_uri = str(delta_path)

    # Use partition column filter which works reliably
    filter_date = "2024-01-05"

    # delta_fusion with SQL (partition filter)
    if HAVE_DELTA_FUSION:
        engine = delta_fusion.DeltaEngine()
        engine.register_table("bench", table_uri)

        def df_query():
            return engine.query(f"SELECT * FROM bench WHERE date >= '{filter_date}'")

        results.append(run_benchmark("delta_fusion (SQL)", df_query))

    # deltalake with partition filter
    if HAVE_DELTALAKE:
        def dl_query():
            dt = deltalake.DeltaTable(table_uri)
            return dt.to_pyarrow_table(
                partitions=[("date", ">=", filter_date)]
            )

        results.append(run_benchmark("deltalake-python", dl_query))

    # polars with filter
    if HAVE_POLARS:
        def pl_query():
            df = pl.read_delta(table_uri)
            return df.filter(pl.col("date") >= filter_date)

        results.append(run_benchmark("polars", pl_query))

    return results


def benchmark_aggregation(delta_path: Path, num_rows: int) -> list[dict]:
    """Benchmark aggregation query."""
    results = []
    table_uri = str(delta_path)

    # delta_fusion with SQL aggregation
    if HAVE_DELTA_FUSION:
        engine = delta_fusion.DeltaEngine()
        engine.register_table("bench", table_uri)

        def df_query():
            return engine.query("""
                SELECT category, COUNT(*) as cnt, AVG(value) as avg_val, SUM(value) as sum_val
                FROM bench
                GROUP BY category
                ORDER BY category
            """)

        results.append(run_benchmark("delta_fusion (SQL)", df_query))

    # deltalake + pyarrow compute
    if HAVE_DELTALAKE:
        def dl_query():
            dt = deltalake.DeltaTable(table_uri)
            table = dt.to_pyarrow_table()
            # PyArrow doesn't have easy GROUP BY, convert to pandas
            df = table.to_pandas()
            return df.groupby("category").agg({"value": ["count", "mean", "sum"]})

        results.append(run_benchmark("deltalake + pandas", dl_query))

    # polars aggregation
    if HAVE_POLARS:
        def pl_query():
            df = pl.read_delta(table_uri)
            return df.group_by("category").agg([
                pl.len().alias("cnt"),
                pl.col("value").mean().alias("avg_val"),
                pl.col("value").sum().alias("sum_val"),
            ]).sort("category")

        results.append(run_benchmark("polars", pl_query))

    return results


def benchmark_time_series(parquet_path: Path, delta_path: Path, num_rows: int) -> list[dict]:
    """Benchmark time series queries (partition-based access)."""
    results = []

    # Date range for query (first 3 days)
    start_date = "2024-01-01"
    end_date = "2024-01-04"

    # delta_fusion time series API (bypasses Delta log)
    if HAVE_DELTA_FUSION:
        engine = delta_fusion.DeltaEngine()
        engine.register_time_series(
            "ts_bench",
            str(parquet_path),
            timestamp_col="timestamp",
            partition_col="date",
        )

        def df_ts_query():
            return engine.read_time_range("ts_bench", start_date, end_date)

        results.append(run_benchmark("delta_fusion (time_series)", df_ts_query))

        # Also compare with regular Delta query
        engine.register_table("delta_bench", str(delta_path))

        def df_delta_query():
            return engine.query(f"""
                SELECT * FROM delta_bench
                WHERE date >= '{start_date}' AND date < '{end_date}'
            """)

        results.append(run_benchmark("delta_fusion (Delta SQL)", df_delta_query))

    # deltalake with partition filter
    if HAVE_DELTALAKE:
        def dl_query():
            dt = deltalake.DeltaTable(str(delta_path))
            # Use partition filter
            return dt.to_pyarrow_table(
                partitions=[("date", ">=", start_date), ("date", "<", end_date)]
            )

        results.append(run_benchmark("deltalake-python", dl_query))

    # polars
    if HAVE_POLARS:
        def pl_query():
            df = pl.read_delta(str(delta_path))
            return df.filter(
                (pl.col("date") >= start_date) & (pl.col("date") < end_date)
            )

        results.append(run_benchmark("polars", pl_query))

    return results


# =============================================================================
# Main
# =============================================================================

def main():
    print("=" * 70)
    print(" Delta Lake Query Engine Benchmark")
    print("=" * 70)
    print(f"\nLibraries available:")
    print(f"  - delta_fusion: {HAVE_DELTA_FUSION}")
    print(f"  - deltalake-python: {HAVE_DELTALAKE}")
    print(f"  - polars: {HAVE_POLARS}")
    print(f"\nConfiguration:")
    print(f"  - Warmup runs: {NUM_WARMUP}")
    print(f"  - Timed runs: {NUM_RUNS}")
    print(f"  - Data sizes: {NUM_ROWS_OPTIONS}")

    # Ensure data directory exists
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)

    for num_rows in NUM_ROWS_OPTIONS:
        print(f"\n{'#' * 70}")
        print(f" Dataset: {num_rows:,} rows")
        print("#" * 70)

        # Create test data
        delta_path = BENCHMARK_DIR / f"delta_{num_rows}"
        parquet_path = BENCHMARK_DIR / f"parquet_{num_rows}"

        print("\nGenerating test data...")
        create_delta_table(delta_path, num_rows)
        create_parquet_partitioned(parquet_path, num_rows)

        # Run benchmarks
        print("\nRunning benchmarks...")

        # 1. Full scan
        results = benchmark_full_scan(delta_path, num_rows)
        print_results(results, f"Full Table Scan ({num_rows:,} rows)")

        # 2. Filtered query
        results = benchmark_filtered_query(delta_path, num_rows)
        print_results(results, f"Filtered Query ({num_rows:,} rows)")

        # 3. Aggregation
        results = benchmark_aggregation(delta_path, num_rows)
        print_results(results, f"Aggregation Query ({num_rows:,} rows)")

        # 4. Time series
        results = benchmark_time_series(parquet_path, delta_path, num_rows)
        print_results(results, f"Time Series Query ({num_rows:,} rows, 3 days)")

    # Cleanup option
    print("\nBenchmark complete!")
    print(f"Test data stored in: {BENCHMARK_DIR}")
    print("Run with --clean to remove test data")


if __name__ == "__main__":
    import sys
    if "--clean" in sys.argv:
        if BENCHMARK_DIR.exists():
            shutil.rmtree(BENCHMARK_DIR)
            print(f"Cleaned up {BENCHMARK_DIR}")
    else:
        main()
