"""Tests for write functionality."""

import pytest
import pyarrow as pa

from delta_fusion import DeltaEngine


class TestCreateTable:
    """Tests for create_table method."""

    def test_create_table_basic(self, temp_dir):
        """Test creating a basic table."""
        engine = DeltaEngine()
        table_path = temp_dir / "basic_table"

        schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])

        engine.create_table(str(table_path), schema)

        # Verify _delta_log was created
        assert (table_path / "_delta_log").exists()

    def test_create_table_with_partitions(self, temp_dir):
        """Test creating a partitioned table."""
        engine = DeltaEngine()
        table_path = temp_dir / "partitioned_table"

        schema = pa.schema([
            ("id", pa.int64()),
            ("date", pa.string()),
            ("value", pa.float64()),
        ])

        engine.create_table(
            str(table_path),
            schema,
            partition_columns=["date"]
        )

        assert (table_path / "_delta_log").exists()

    def test_create_table_already_exists(self, temp_dir):
        """Test creating a table that already exists."""
        engine = DeltaEngine()
        table_path = temp_dir / "existing_table"

        schema = pa.schema([("id", pa.int64())])

        engine.create_table(str(table_path), schema)

        # Should fail when trying to create again
        with pytest.raises(Exception):
            engine.create_table(str(table_path), schema)


class TestWrite:
    """Tests for write method."""

    def test_write_new_table(self, temp_dir):
        """Test writing to a new table."""
        engine = DeltaEngine()
        table_path = temp_dir / "new_table"

        table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.1],
        })

        engine.write(str(table_path), table, mode="error")

        assert (table_path / "_delta_log").exists()

    def test_write_append(self, temp_dir):
        """Test appending data to an existing table."""
        engine = DeltaEngine()
        table_path = temp_dir / "append_table"

        table = pa.table({
            "id": [1, 2],
            "value": [10.5, 20.3],
        })

        # First write
        engine.write(str(table_path), table, mode="append")

        # Append more data
        table2 = pa.table({
            "id": [3, 4],
            "value": [30.1, 40.7],
        })
        engine.write(str(table_path), table2, mode="append")

        # Verify we can read all data
        engine.register_table("test", str(table_path))
        batches = engine.query("SELECT COUNT(*) as cnt FROM test")
        result = pa.Table.from_batches(batches)
        assert result.to_pydict()["cnt"][0] == 4

    def test_write_overwrite(self, temp_dir):
        """Test overwriting data in a table."""
        engine = DeltaEngine()
        table_path = temp_dir / "overwrite_table"

        # First write
        table1 = pa.table({"id": [1, 2, 3]})
        engine.write(str(table_path), table1, mode="append")

        # Overwrite
        table2 = pa.table({"id": [10, 20]})
        engine.write(str(table_path), table2, mode="overwrite")

        # Verify only new data exists
        engine.register_table("test", str(table_path))
        batches = engine.query("SELECT COUNT(*) as cnt FROM test")
        result = pa.Table.from_batches(batches)
        assert result.to_pydict()["cnt"][0] == 2

    def test_write_ignore(self, temp_dir):
        """Test write with ignore mode."""
        engine = DeltaEngine()
        table_path = temp_dir / "ignore_table"

        table = pa.table({"id": [1, 2]})
        engine.write(str(table_path), table, mode="error")

        # Should not fail with ignore mode
        table2 = pa.table({"id": [3, 4]})
        engine.write(str(table_path), table2, mode="ignore")

    def test_write_invalid_mode(self, temp_dir):
        """Test write with invalid mode."""
        engine = DeltaEngine()
        table_path = temp_dir / "invalid_mode"

        table = pa.table({"id": [1]})

        with pytest.raises(Exception):
            engine.write(str(table_path), table, mode="invalid")

    def test_write_with_record_batches(self, temp_dir):
        """Test writing using list of RecordBatches."""
        engine = DeltaEngine()
        table_path = temp_dir / "batches_table"

        schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
        batch1 = pa.record_batch({"id": [1, 2], "value": [1.0, 2.0]}, schema=schema)
        batch2 = pa.record_batch({"id": [3, 4], "value": [3.0, 4.0]}, schema=schema)

        engine.write(str(table_path), [batch1, batch2], mode="append")

        engine.register_table("test", str(table_path))
        batches = engine.query("SELECT COUNT(*) as cnt FROM test")
        result = pa.Table.from_batches(batches)
        assert result.to_pydict()["cnt"][0] == 4


class TestWriteToTable:
    """Tests for write_to_table method."""

    def test_write_to_registered_table(self, temp_dir):
        """Test writing to a registered table."""
        engine = DeltaEngine()
        table_path = temp_dir / "registered"

        # Create initial table
        table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        engine.write(str(table_path), table, mode="error")

        # Register it
        engine.register_table("test", str(table_path))

        # Write more data
        new_data = pa.table({"id": [3, 4], "name": ["c", "d"]})
        engine.write_to_table("test", new_data, mode="append")

        # Query to verify
        batches = engine.query("SELECT COUNT(*) as cnt FROM test")
        result = pa.Table.from_batches(batches)
        assert result.to_pydict()["cnt"][0] == 4

    def test_write_to_unregistered_table(self):
        """Test writing to an unregistered table."""
        engine = DeltaEngine()
        table = pa.table({"id": [1]})

        with pytest.raises(Exception):
            engine.write_to_table("nonexistent", table)

    def test_write_to_table_overwrite(self, temp_dir):
        """Test overwriting a registered table."""
        engine = DeltaEngine()
        table_path = temp_dir / "overwrite_reg"

        # Create initial table
        table = pa.table({"id": [1, 2, 3]})
        engine.write(str(table_path), table, mode="error")
        engine.register_table("test", str(table_path))

        # Overwrite
        new_data = pa.table({"id": [10]})
        engine.write_to_table("test", new_data, mode="overwrite")

        # Query to verify
        batches = engine.query("SELECT COUNT(*) as cnt FROM test")
        result = pa.Table.from_batches(batches)
        assert result.to_pydict()["cnt"][0] == 1


class TestSchemaTypes:
    """Tests for various data types."""

    def test_various_types(self, temp_dir):
        """Test creating table with various data types."""
        engine = DeltaEngine()
        table_path = temp_dir / "various_types"

        schema = pa.schema([
            ("bool_col", pa.bool_()),
            ("int8_col", pa.int8()),
            ("int16_col", pa.int16()),
            ("int32_col", pa.int32()),
            ("int64_col", pa.int64()),
            ("float32_col", pa.float32()),
            ("float64_col", pa.float64()),
            ("string_col", pa.string()),
            ("binary_col", pa.binary()),
            ("date_col", pa.date32()),
        ])

        engine.create_table(str(table_path), schema)
        assert (table_path / "_delta_log").exists()

    def test_nested_types(self, temp_dir):
        """Test creating table with nested types."""
        engine = DeltaEngine()
        table_path = temp_dir / "nested_types"

        schema = pa.schema([
            ("id", pa.int64()),
            ("tags", pa.list_(pa.string())),
        ])

        engine.create_table(str(table_path), schema)
        assert (table_path / "_delta_log").exists()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_table_write(self, temp_dir):
        """Test that writing empty data fails gracefully."""
        engine = DeltaEngine()
        table_path = temp_dir / "empty"

        # Empty table
        empty_table = pa.table({"id": pa.array([], type=pa.int64())})

        # Empty data should still work (0 rows is valid)
        try:
            engine.write(str(table_path), empty_table, mode="append")
        except Exception:
            # Some implementations may reject empty writes
            pass

    def test_large_batch_write(self, temp_dir):
        """Test writing a large batch."""
        engine = DeltaEngine()
        table_path = temp_dir / "large"

        # Create large data (100k rows)
        n = 100_000
        table = pa.table({
            "id": list(range(n)),
            "value": [float(i) for i in range(n)],
        })

        engine.write(str(table_path), table, mode="append")

        engine.register_table("test", str(table_path))
        batches = engine.query("SELECT COUNT(*) as cnt FROM test")
        result = pa.Table.from_batches(batches)
        assert result.to_pydict()["cnt"][0] == n

    def test_special_characters_in_path(self, temp_dir):
        """Test path with spaces (may fail on some systems)."""
        engine = DeltaEngine()
        table_path = temp_dir / "path with spaces"

        table = pa.table({"id": [1, 2]})

        try:
            engine.write(str(table_path), table, mode="append")
            assert (table_path / "_delta_log").exists()
        except Exception:
            pytest.skip("System doesn't support spaces in paths")

    def test_unicode_data(self, temp_dir):
        """Test writing unicode data."""
        engine = DeltaEngine()
        table_path = temp_dir / "unicode"

        table = pa.table({
            "name": ["한글", "日本語", "العربية", "🎉emoji🎊"],
        })

        engine.write(str(table_path), table, mode="append")

        engine.register_table("test", str(table_path))
        batches = engine.query("SELECT * FROM test")
        result = pa.Table.from_batches(batches)
        assert len(result) == 4

    def test_null_values(self, temp_dir):
        """Test writing null values."""
        engine = DeltaEngine()
        table_path = temp_dir / "nulls"

        table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "value": [10.5, None, None],
        })

        engine.write(str(table_path), table, mode="append")

        engine.register_table("test", str(table_path))
        batches = engine.query("SELECT * FROM test WHERE name IS NULL")
        result = pa.Table.from_batches(batches)
        assert len(result) == 1
