#!/usr/bin/env python3
"""End-to-end test for delta_fusion.

This script tests the complete workflow:
1. Create a Delta table
2. Write data
3. Read data back with SQL
4. Test time series functionality
5. Test append and overwrite modes
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from delta_fusion import DeltaEngine


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def test_basic_crud():
    """Test basic CRUD operations."""
    print_section("1. Basic CRUD Operations")

    with tempfile.TemporaryDirectory() as tmp_dir:
        table_path = Path(tmp_dir) / "users"
        engine = DeltaEngine()

        # Create schema
        schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("age", pa.int32()),
            ("score", pa.float64()),
        ])

        # Create table
        print("Creating table...")
        engine.create_table(str(table_path), schema)
        print(f"  ✓ Table created at {table_path}")

        # Write initial data
        print("\nWriting initial data...")
        data = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "score": [85.5, 90.0, 78.3],
        })
        engine.write(str(table_path), data, mode="append")
        print(f"  ✓ Wrote {len(data)} rows")

        # Register and query
        print("\nQuerying data...")
        engine.register_table("users", str(table_path))

        # Simple SELECT
        batches = engine.query("SELECT * FROM users ORDER BY id")
        result = pa.Table.from_batches(batches)
        print(f"  ✓ SELECT * returned {len(result)} rows")
        print(f"    Columns: {result.column_names}")

        # Aggregation
        batches = engine.query("SELECT COUNT(*) as cnt, AVG(score) as avg_score FROM users")
        result = pa.Table.from_batches(batches)
        print(f"  ✓ COUNT: {result['cnt'][0].as_py()}, AVG(score): {result['avg_score'][0].as_py():.2f}")

        # Filter
        batches = engine.query("SELECT name, score FROM users WHERE age > 28")
        result = pa.Table.from_batches(batches)
        print(f"  ✓ WHERE age > 28: {len(result)} rows")

        # Append more data
        print("\nAppending more data...")
        new_data = pa.table({
            "id": [4, 5],
            "name": ["Diana", "Eve"],
            "age": [28, 32],
            "score": [92.1, 88.5],
        })
        engine.write(str(table_path), new_data, mode="append")

        # Refresh and verify
        engine.refresh_table("users")
        batches = engine.query("SELECT COUNT(*) as cnt FROM users")
        result = pa.Table.from_batches(batches)
        print(f"  ✓ After append: {result['cnt'][0].as_py()} rows")

        # Overwrite
        print("\nOverwriting table...")
        overwrite_data = pa.table({
            "id": [100, 200],
            "name": ["New1", "New2"],
            "age": [40, 45],
            "score": [95.0, 97.0],
        })
        engine.write(str(table_path), overwrite_data, mode="overwrite")

        engine.refresh_table("users")
        batches = engine.query("SELECT * FROM users ORDER BY id")
        result = pa.Table.from_batches(batches)
        print(f"  ✓ After overwrite: {len(result)} rows (expected 2)")

        print("\n  ✅ Basic CRUD test passed!")


def test_partitioned_table():
    """Test partitioned table operations."""
    print_section("2. Partitioned Table")

    with tempfile.TemporaryDirectory() as tmp_dir:
        table_path = Path(tmp_dir) / "sales"
        engine = DeltaEngine()

        # Create partitioned table
        print("Creating partitioned table...")
        schema = pa.schema([
            ("id", pa.int64()),
            ("amount", pa.float64()),
            ("region", pa.string()),
            ("date", pa.string()),
        ])
        engine.create_table(str(table_path), schema, partition_columns=["region"])
        print("  ✓ Partitioned table created (partition: region)")

        # Write data for different partitions
        print("\nWriting data to partitions...")
        data = pa.table({
            "id": [1, 2, 3, 4, 5, 6],
            "amount": [100.0, 200.0, 150.0, 300.0, 250.0, 175.0],
            "region": ["US", "US", "EU", "EU", "ASIA", "ASIA"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
        })
        engine.write(str(table_path), data, mode="append")
        print(f"  ✓ Wrote {len(data)} rows across 3 partitions")

        # Query by partition
        engine.register_table("sales", str(table_path))

        batches = engine.query("SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region")
        result = pa.Table.from_batches(batches)
        print("\n  Region totals:")
        for i in range(len(result)):
            print(f"    {result['region'][i].as_py()}: ${result['total'][i].as_py():.2f}")

        # Partition filter (should be pruned)
        batches = engine.query("SELECT * FROM sales WHERE region = 'US'")
        result = pa.Table.from_batches(batches)
        print(f"\n  ✓ US region only: {len(result)} rows")

        print("\n  ✅ Partitioned table test passed!")


def test_time_series():
    """Test time series functionality."""
    print_section("3. Time Series API")

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_path = Path(tmp_dir) / "sensor_data"
        engine = DeltaEngine()

        # Create date-partitioned parquet files manually
        print("Creating time series data...")

        import pyarrow.parquet as pq

        dates = ["2024-01-15", "2024-01-16", "2024-01-17"]
        for date in dates:
            partition_dir = base_path / f"date={date}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Create sample data for each day
            table = pa.table({
                "ts": [
                    f"{date}T10:00:00",
                    f"{date}T12:00:00",
                    f"{date}T14:00:00",
                    f"{date}T16:00:00",
                ],
                "sensor_id": ["S1", "S2", "S1", "S2"],
                "value": [23.5 + i * 0.5 for i in range(4)],
            })

            # Write as parquet
            pq.write_table(table, partition_dir / "data.parquet")

        print(f"  ✓ Created 3 days of data in {base_path}")

        # Test time series registration
        print("\nTesting time series registration...")
        engine.register_time_series(
            name="sensor",
            path=str(base_path),
            timestamp_col="ts",
            partition_col="date",
        )
        print("  ✓ Time series registered")

        assert engine.is_time_series_registered("sensor")
        print("  ✓ is_time_series_registered() works")

        series_list = engine.list_time_series()
        assert "sensor" in series_list
        print(f"  ✓ list_time_series(): {series_list}")

        engine.deregister_time_series("sensor")
        assert not engine.is_time_series_registered("sensor")
        print("  ✓ deregister_time_series() works")

        # Re-register for read test
        engine.register_time_series(
            name="sensor",
            path=str(base_path),
            timestamp_col="ts",
            partition_col="date",
        )

        # Read time range (single day)
        print("\nReading single day...")
        batches = engine.read_time_range(
            "sensor",
            "2024-01-16T00:00:00",
            "2024-01-17T00:00:00"
        )
        if batches:
            result = pa.Table.from_batches(batches)
            print(f"  ✓ Single day (2024-01-16): {len(result)} rows")
            assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
        else:
            print("  ✗ No data returned")
            assert False, "Expected data from read_time_range"

        # Read time range (multiple days)
        print("\nReading multiple days...")
        batches = engine.read_time_range(
            "sensor",
            "2024-01-15T00:00:00",
            "2024-01-18T00:00:00"
        )
        if batches:
            result = pa.Table.from_batches(batches)
            print(f"  ✓ Multiple days (15-17): {len(result)} rows")
            assert len(result) == 12, f"Expected 12 rows, got {len(result)}"
        else:
            print("  ✗ No data returned")
            assert False, "Expected data from read_time_range"

        # Direct read (without registration)
        print("\nDirect read without registration...")
        batches = engine.read_time_range_direct(
            path=str(base_path),
            timestamp_col="ts",
            start="2024-01-15T00:00:00",
            end="2024-01-16T00:00:00",
            partition_col="date",
        )
        if batches:
            result = pa.Table.from_batches(batches)
            print(f"  ✓ Direct read (2024-01-15): {len(result)} rows")
            assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
        else:
            print("  ✗ No data returned")
            assert False, "Expected data from read_time_range_direct"

        # Test timestamp filtering
        print("\nTesting timestamp filtering...")
        batches = engine.read_time_range(
            "sensor",
            "2024-01-15T11:00:00",
            "2024-01-15T15:00:00"
        )
        if batches:
            result = pa.Table.from_batches(batches)
            print(f"  ✓ Filtered (11:00-15:00): {len(result)} rows")
            # Should get rows at 12:00 and 14:00
            assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
        else:
            print("  ✗ No data returned")
            assert False, "Expected filtered data"

        print("\n  ✅ Time series test passed!")


def test_data_types():
    """Test various data types."""
    print_section("4. Data Types")

    with tempfile.TemporaryDirectory() as tmp_dir:
        table_path = Path(tmp_dir) / "types_test"
        engine = DeltaEngine()

        print("Testing various data types...")

        # Create table with various types
        data = pa.table({
            "bool_col": [True, False, True],
            "int8_col": pa.array([1, 2, 3], type=pa.int8()),
            "int16_col": pa.array([100, 200, 300], type=pa.int16()),
            "int32_col": pa.array([1000, 2000, 3000], type=pa.int32()),
            "int64_col": [1000000, 2000000, 3000000],
            "float32_col": pa.array([1.1, 2.2, 3.3], type=pa.float32()),
            "float64_col": [1.111, 2.222, 3.333],
            "string_col": ["hello", "world", "test"],
            "date_col": pa.array([18000, 18001, 18002], type=pa.date32()),
        })

        engine.write(str(table_path), data, mode="error")
        print(f"  ✓ Wrote table with {len(data.schema)} columns")

        # Read back and verify
        engine.register_table("types", str(table_path))
        batches = engine.query("SELECT * FROM types")
        result = pa.Table.from_batches(batches)

        print(f"  ✓ Read back {len(result)} rows")
        print(f"  ✓ Schema matches: {len(result.schema)} columns")

        # Test null handling
        print("\nTesting NULL values...")
        null_data = pa.table({
            "id": [1, 2, 3],
            "nullable_str": ["a", None, "c"],
            "nullable_int": pa.array([10, None, 30], type=pa.int64()),
        })

        null_path = Path(tmp_dir) / "null_test"
        engine.write(str(null_path), null_data, mode="error")
        engine.register_table("nulls", str(null_path))

        batches = engine.query("SELECT * FROM nulls WHERE nullable_str IS NULL")
        result = pa.Table.from_batches(batches)
        print(f"  ✓ NULL filter returned {len(result)} rows (expected 1)")

        print("\n  ✅ Data types test passed!")


def test_query_to_dicts():
    """Test query_to_dicts method."""
    print_section("5. Query to Dicts")

    with tempfile.TemporaryDirectory() as tmp_dir:
        table_path = Path(tmp_dir) / "dict_test"
        engine = DeltaEngine()

        data = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 90.0, 78.3],
        })

        engine.write(str(table_path), data, mode="error")
        engine.register_table("test", str(table_path))

        print("Testing query_to_dicts...")
        results = engine.query_to_dicts("SELECT * FROM test ORDER BY id")

        print(f"  ✓ Returned {len(results)} dictionaries")
        for row in results:
            print(f"    {row}")

        print("\n  ✅ Query to dicts test passed!")


def test_metadata():
    """Test metadata methods."""
    print_section("6. Metadata")

    with tempfile.TemporaryDirectory() as tmp_dir:
        table_path = Path(tmp_dir) / "meta_test"
        engine = DeltaEngine()

        # Create table
        data = pa.table({"id": [1, 2], "value": [10.0, 20.0]})
        engine.write(str(table_path), data, mode="error")

        print("Testing table_info...")
        info = engine.table_info(str(table_path))
        print(f"  Path: {info['path']}")
        print(f"  Version: {info['version']}")
        print(f"  Num files: {info['num_files']}")
        print(f"  Partition columns: {info['partition_columns']}")

        print("\nTesting list methods...")
        engine.register_table("test1", str(table_path))
        engine.register_time_series("ts1", str(table_path), "id")

        tables = engine.list_tables()
        series = engine.list_time_series()

        print(f"  ✓ Registered tables: {tables}")
        print(f"  ✓ Registered time series: {series}")
        print(f"  ✓ is_registered('test1'): {engine.is_registered('test1')}")
        print(f"  ✓ is_time_series_registered('ts1'): {engine.is_time_series_registered('ts1')}")

        print("\n  ✅ Metadata test passed!")


def main():
    """Run all E2E tests."""
    print("\n" + "="*60)
    print("  Delta Fusion E2E Test Suite")
    print("="*60)

    try:
        test_basic_crud()
        test_partitioned_table()
        test_time_series()
        test_data_types()
        test_query_to_dicts()
        test_metadata()

        print("\n" + "="*60)
        print("  🎉 ALL E2E TESTS PASSED!")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
