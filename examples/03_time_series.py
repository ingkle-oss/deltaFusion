"""Time Series Example

Demonstrates fast partition-based time series queries that bypass Delta log.
Requires: pip install delta_fusion[polars] or delta_fusion[pyarrow]
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from delta_fusion import DeltaEngine


def create_sample_time_series(base_path: Path, days: int = 3):
    """Create sample time series data with date partitions."""
    base_path.mkdir(parents=True, exist_ok=True)

    for day_offset in range(days):
        date = datetime(2024, 1, 1) + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # Create partition directory
        partition_dir = base_path / f"date={date_str}"
        partition_dir.mkdir(exist_ok=True)

        # Generate hourly data for this day
        timestamps = []
        values = []
        for hour in range(24):
            for minute in [0, 30]:  # Every 30 minutes
                ts = date.replace(hour=hour, minute=minute)
                timestamps.append(ts.isoformat())
                values.append(100 + hour * 2 + minute / 30)

        # Write parquet file
        table = pa.table({"timestamp": timestamps, "value": values})
        pq.write_table(table, partition_dir / "data.parquet")

    print(f"Created {days} days of data at {base_path}")


def main():
    engine = DeltaEngine()

    with tempfile.TemporaryDirectory() as tmp:
        data_path = Path(tmp) / "sensor_data"

        # Create sample data
        create_sample_time_series(data_path, days=3)

        # Register time series (instant - no data reading)
        engine.register_time_series(
            name="sensor",
            path=str(data_path),
            timestamp_col="timestamp",
            partition_col="date",
        )

        # Query specific time range (only reads relevant partitions)
        print("\n--- Query 6 hours on Jan 1st ---")
        batches = engine.read_time_range(
            "sensor",
            start="2024-01-01T08:00:00",
            end="2024-01-01T14:00:00",
        )
        table = pa.Table.from_batches(batches)
        print(f"Rows: {len(table)}")
        print(table.to_pandas())

        # Query with Polars
        print("\n--- Same query with Polars ---")
        df = engine.read_time_range_polars(
            "sensor",
            start="2024-01-01T08:00:00",
            end="2024-01-01T14:00:00",
        )
        print(df)

        # Query across multiple days
        print("\n--- Query across 2 days ---")
        df = engine.read_time_range_polars(
            "sensor",
            start="2024-01-01T20:00:00",
            end="2024-01-02T04:00:00",
        )
        print(f"Rows: {len(df)}")
        print(df.head(5))


if __name__ == "__main__":
    main()
