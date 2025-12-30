"""Pytest fixtures for delta_fusion tests."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample parquet file."""
    table = pa.table({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.5, 20.3, 30.1, 40.7, 50.2],
    })

    path = temp_dir / "sample.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture
def time_series_data(temp_dir):
    """Create time series parquet files with date partitions."""
    import datetime

    base_path = temp_dir / "time_series"
    base_path.mkdir(exist_ok=True)

    dates = ["2024-01-15", "2024-01-16", "2024-01-17"]

    for date_str in dates:
        partition_dir = base_path / f"date={date_str}"
        partition_dir.mkdir(exist_ok=True)

        # Create timestamps for this date
        base_dt = datetime.datetime.fromisoformat(f"{date_str}T00:00:00")
        timestamps = [
            base_dt + datetime.timedelta(hours=h)
            for h in range(0, 24, 2)
        ]

        table = pa.table({
            "timestamp": pa.array(timestamps, type=pa.timestamp("us")),
            "value": [float(i) for i in range(12)],
            "sensor_id": ["sensor_1"] * 12,
        })

        pq.write_table(table, partition_dir / "data.parquet")

    return base_path


@pytest.fixture
def delta_table_path(temp_dir):
    """Create a minimal delta table structure."""
    import json

    delta_path = temp_dir / "delta_table"
    delta_path.mkdir(exist_ok=True)

    # Create _delta_log directory
    delta_log = delta_path / "_delta_log"
    delta_log.mkdir(exist_ok=True)

    # Create sample data
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    })
    pq.write_table(table, delta_path / "part-00000.parquet")

    # Create delta log
    log_entries = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": "test-table-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                        {"name": "name", "type": "string", "nullable": False, "metadata": {}},
                    ]
                }),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1700000000000,
            }
        },
        {
            "add": {
                "path": "part-00000.parquet",
                "size": 1000,
                "partitionValues": {},
                "modificationTime": 1700000000000,
                "dataChange": True,
            }
        },
    ]

    log_content = "\n".join(json.dumps(entry) for entry in log_entries) + "\n"
    (delta_log / "00000000000000000000.json").write_text(log_content)

    return delta_path
