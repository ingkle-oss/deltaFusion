"""S3/Cloud Storage Example

Demonstrates connecting to Delta tables on S3 or S3-compatible storage.
Requires: pip install delta_fusion[pyarrow]

Note: This example shows the configuration pattern.
Replace with your actual S3 credentials and paths.
"""

from delta_fusion import DeltaEngine


def example_aws_s3():
    """Connect to AWS S3."""
    engine = DeltaEngine(
        storage_options={
            "aws_access_key_id": "YOUR_ACCESS_KEY",
            "aws_secret_access_key": "YOUR_SECRET_KEY",
            "aws_region": "us-east-1",
        }
    )

    # Register table from S3
    engine.register_table("sales", "s3://your-bucket/delta-tables/sales")

    # Query normally
    batches = engine.query("SELECT * FROM sales LIMIT 10")
    print(f"Got {len(batches)} batches")


def example_minio():
    """Connect to MinIO (S3-compatible)."""
    engine = DeltaEngine(
        storage_options={
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "aws_endpoint": "http://localhost:9000",
            "aws_allow_http": "true",
        }
    )

    # Register table from MinIO
    engine.register_table("logs", "s3://my-bucket/logs")

    # Query
    batches = engine.query("SELECT * FROM logs WHERE level = 'ERROR'")
    print(f"Got {len(batches)} batches")


def example_time_series_s3():
    """Fast time series queries on S3."""
    engine = DeltaEngine(
        storage_options={
            "aws_access_key_id": "YOUR_ACCESS_KEY",
            "aws_secret_access_key": "YOUR_SECRET_KEY",
            "aws_region": "ap-northeast-2",
        }
    )

    # Register time series (instant - no data reading)
    engine.register_time_series(
        name="sensor",
        path="s3://your-bucket/sensor-data",
        timestamp_col="timestamp",
        partition_col="date",
    )

    # Query specific time range (only fetches relevant partitions)
    df = engine.read_time_range_polars(
        "sensor",
        start="2024-01-01T00:00:00",
        end="2024-01-01T12:00:00",
    )
    print(df)


def example_hierarchical_partitions():
    """Time series with year/month/day partitions."""
    engine = DeltaEngine(
        storage_options={
            "aws_access_key_id": "YOUR_ACCESS_KEY",
            "aws_secret_access_key": "YOUR_SECRET_KEY",
            "aws_region": "us-west-2",
        }
    )

    # For data partitioned as: year=2024/month=01/day=15/
    engine.register_time_series_hierarchical(
        name="events",
        path="s3://your-bucket/events",
        timestamp_col="event_time",
        partition_cols=["year", "month", "day"],
        partition_formats=["%Y", "%m", "%d"],
    )

    # Query across partitions
    df = engine.read_time_range_polars(
        "events",
        start="2024-01-15T00:00:00",
        end="2024-01-20T00:00:00",
    )
    print(f"Got {len(df)} rows")


if __name__ == "__main__":
    print("S3 Storage Examples")
    print("=" * 50)
    print(
        """
This file shows configuration patterns for S3/cloud storage.
Replace the placeholder values with your actual credentials.

Available examples:
- example_aws_s3(): Connect to AWS S3
- example_minio(): Connect to MinIO
- example_time_series_s3(): Fast time series on S3
- example_hierarchical_partitions(): year/month/day partitions
"""
    )
