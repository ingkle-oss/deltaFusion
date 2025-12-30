"""delta_fusion - High-performance Delta Lake query engine for Python.

This library provides SQL query capabilities over Delta Lake tables using
DataFusion as the query engine. Data is transferred via zero-copy Arrow
for maximum performance.

For time series data, use the time series API which bypasses Delta log
entirely for much faster partition-based access.

Example:
    # Standard Delta table access
    >>> from delta_fusion import DeltaEngine
    >>> engine = DeltaEngine()
    >>> engine.register_table("my_table", "/path/to/delta/table")
    >>> batches = engine.query("SELECT * FROM my_table WHERE id > 100")

    # Fast time series access (bypasses Delta log)
    >>> engine.register_time_series("sensor", "s3://bucket/data", "dt", "timestamp")
    >>> batches = engine.read_time_range("sensor", "2024-01-01T00:00:00", "2024-01-01T12:00:00")
"""

from delta_fusion._core import PyDeltaEngine

__all__ = ["DeltaEngine"]


class DeltaEngine:
    """High-performance Delta Lake query engine.

    Uses DataFusion for SQL query execution and zero-copy Arrow transfer
    for optimal performance. The GIL is released during query execution.

    For time series data with date partitions, use `register_time_series()`
    and `read_time_range()` which bypass Delta log for faster access.

    Args:
        storage_options: Optional dict with storage configuration:
            - aws_access_key_id: AWS access key
            - aws_secret_access_key: AWS secret key
            - aws_region: AWS region
            - aws_endpoint: Custom S3 endpoint (for MinIO, etc.)
            - aws_allow_http: Allow non-SSL connections

    Example:
        >>> engine = DeltaEngine()
        >>> engine.register_table("sales", "s3://bucket/sales_delta")
        >>> results = engine.query("SELECT * FROM sales WHERE year = 2024")
    """

    def __init__(self, storage_options: dict | None = None):
        self._engine = PyDeltaEngine(storage_options)

    # ========================================================================
    # Delta Table Methods
    # ========================================================================

    def register_table(
        self, name: str, path: str, version: int | None = None
    ) -> None:
        """Register a Delta table for querying.

        The table is cached after loading to avoid repeated log replay.
        Use `refresh_table()` to reload with latest changes.

        Args:
            name: Table name to use in SQL queries
            path: Path to Delta table (local path or s3://)
            version: Optional specific version for time travel
        """
        self._engine.register_table(name, path, version)

    def refresh_table(self, name: str) -> None:
        """Refresh a registered table to load latest changes."""
        self._engine.refresh_table(name)

    def refresh_all(self) -> None:
        """Refresh all registered tables."""
        self._engine.refresh_all()

    def deregister_table(self, name: str) -> None:
        """Deregister a table."""
        self._engine.deregister_table(name)

    # ========================================================================
    # Time Series Methods (Delta log bypass - FAST)
    # ========================================================================

    def register_time_series(
        self,
        name: str,
        path: str,
        timestamp_col: str,
        partition_col: str = "date",
        partition_format: str | None = None,
    ) -> None:
        """Register a time series table configuration.

        This stores the configuration for fast partition-based access.
        Use `read_time_range()` to query data.

        NOTE: This does NOT read any data or Delta log. It's instant.

        Args:
            name: Name for the time series
            path: Base path to the data (e.g., "s3://bucket/sensor_data")
            timestamp_col: Timestamp column name in parquet files
            partition_col: Partition column name (default: "date")
            partition_format: Partition date format (default: "%Y-%m-%d")

        Example:
            >>> engine.register_time_series(
            ...     "sensor",
            ...     "s3://bucket/sensor_data",
            ...     timestamp_col="timestamp"
            ... )
        """
        self._engine.register_time_series(
            name, path, partition_col, timestamp_col, partition_format
        )

    def read_time_range(self, name: str, start: str, end: str) -> list:
        """Read time range data directly from parquet files.

        This BYPASSES Delta log entirely for maximum performance.
        Only reads the partitions that match the date range.

        Args:
            name: Registered time series name
            start: Start timestamp (ISO 8601, e.g., "2024-01-01T10:30:00")
            end: End timestamp (ISO 8601, e.g., "2024-01-01T14:00:00")

        Returns:
            List of pyarrow.RecordBatch objects

        Example:
            >>> batches = engine.read_time_range(
            ...     "sensor",
            ...     "2024-01-01T10:30:00",
            ...     "2024-01-01T14:00:00"
            ... )
            >>> import pyarrow as pa
            >>> table = pa.Table.from_batches(batches)
            >>> df = table.to_pandas()
        """
        return self._engine.read_time_range(name, start, end)

    def read_time_range_direct(
        self,
        path: str,
        timestamp_col: str,
        start: str,
        end: str,
        partition_col: str = "date",
    ) -> list:
        """Read time range directly from path (without pre-registration).

        One-shot read without needing to register first.

        Args:
            path: Base path to the data
            timestamp_col: Timestamp column name in parquet files
            start: Start timestamp (ISO 8601)
            end: End timestamp (ISO 8601)
            partition_col: Partition column name (default: "date")

        Returns:
            List of pyarrow.RecordBatch objects
        """
        return self._engine.read_time_range_direct(
            path, partition_col, timestamp_col, start, end
        )

    def deregister_time_series(self, name: str) -> None:
        """Deregister a time series."""
        self._engine.deregister_time_series(name)

    # ========================================================================
    # Query Methods
    # ========================================================================

    def query(self, sql: str) -> list:
        """Execute SQL query and return PyArrow RecordBatches.

        This is the primary query method, returning zero-copy Arrow data.

        Args:
            sql: SQL query string

        Returns:
            List of pyarrow.RecordBatch objects
        """
        return self._engine.query(sql)

    def query_to_dicts(self, sql: str) -> list[dict]:
        """Execute SQL query and return as list of dictionaries.

        WARNING: Slow for large datasets. Prefer query() with PyArrow.
        """
        return self._engine.query_to_dicts(sql)

    # ========================================================================
    # Metadata Methods
    # ========================================================================

    def table_info(self, name_or_path: str) -> dict:
        """Get metadata about a Delta table."""
        return self._engine.table_info(name_or_path)

    def list_tables(self) -> list[str]:
        """List all registered Delta table names."""
        return self._engine.list_tables()

    def list_time_series(self) -> list[str]:
        """List all registered time series names."""
        return self._engine.list_time_series()

    def is_registered(self, name: str) -> bool:
        """Check if a Delta table is registered."""
        return self._engine.is_registered(name)

    def is_time_series_registered(self, name: str) -> bool:
        """Check if a time series is registered."""
        return self._engine.is_time_series_registered(name)
