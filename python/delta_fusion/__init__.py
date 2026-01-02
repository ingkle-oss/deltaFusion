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

from delta_fusion._core import DeltaEngine as _CoreEngine

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
        target_partitions: Number of partitions for parallel execution.
            - None (default): Use all CPU cores
            - 1: Single-threaded, lowest CPU usage
            - 2-4: Balanced CPU usage
            - Higher: More parallelism, higher CPU usage

    Example:
        >>> engine = DeltaEngine()
        >>> engine.register_table("sales", "s3://bucket/sales_delta")
        >>> results = engine.query("SELECT * FROM sales WHERE year = 2024")

        # Low CPU usage mode
        >>> engine = DeltaEngine(target_partitions=2)
    """

    def __init__(
        self, storage_options: dict | None = None, target_partitions: int | None = None
    ):
        self._engine = _CoreEngine(storage_options, target_partitions)

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
            name, path, timestamp_col, partition_col, partition_format
        )

    def register_time_series_hierarchical(
        self,
        name: str,
        path: str,
        timestamp_col: str,
        partition_cols: list[str],
        partition_formats: list[str],
    ) -> None:
        """Register a time series with hierarchical partitions.

        Use this for data partitioned like: `year=2024/month=01/day=15`

        NOTE: This does NOT read any data or Delta log. It's instant.

        Args:
            name: Name for the time series
            path: Base path to the data (e.g., "s3://bucket/sensor_data")
            timestamp_col: Timestamp column name in parquet files
            partition_cols: List of partition column names (e.g., ["year", "month", "day"])
            partition_formats: List of format strings (e.g., ["%Y", "%m", "%d"])

        Example:
            >>> engine.register_time_series_hierarchical(
            ...     "sensor",
            ...     "s3://bucket/sensor_data",
            ...     timestamp_col="timestamp",
            ...     partition_cols=["year", "month", "day"],
            ...     partition_formats=["%Y", "%m", "%d"],
            ... )
        """
        self._engine.register_time_series_hierarchical(
            name, path, timestamp_col, partition_cols, partition_formats
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

    def read_time_range_polars(self, name: str, start: str, end: str) -> "pl.DataFrame":
        """Read time range data and return Polars DataFrame.

        This BYPASSES Delta log entirely for maximum performance.
        Requires: pip install delta_fusion[polars]

        Args:
            name: Registered time series name
            start: Start timestamp (ISO 8601, e.g., "2024-01-01T10:30:00")
            end: End timestamp (ISO 8601, e.g., "2024-01-01T14:00:00")

        Returns:
            polars.DataFrame
        """
        import io
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "polars is required for read_time_range_polars(). "
                "Install with: pip install delta_fusion[polars]"
            )

        ipc_bytes = self._engine.read_time_range_ipc(name, start, end)
        if not ipc_bytes:
            return pl.DataFrame()
        return pl.read_ipc(io.BytesIO(ipc_bytes))

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
            path, timestamp_col, start, end, partition_col
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
        Requires: pip install delta_fusion[pyarrow]

        Args:
            sql: SQL query string

        Returns:
            List of pyarrow.RecordBatch objects
        """
        return self._engine.query(sql)

    def query_polars(self, sql: str) -> "pl.DataFrame":
        """Execute SQL query and return Polars DataFrame.

        Requires: pip install delta_fusion[polars]

        Args:
            sql: SQL query string

        Returns:
            polars.DataFrame
        """
        import io
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "polars is required for query_polars(). "
                "Install with: pip install delta_fusion[polars]"
            )

        ipc_bytes = self._engine.query_ipc(sql)
        if not ipc_bytes:
            # Return empty DataFrame
            return pl.DataFrame()
        return pl.read_ipc(io.BytesIO(ipc_bytes))

    def query_to_dicts(self, sql: str) -> list[dict]:
        """Execute SQL query and return as list of dictionaries.

        No external dependencies required.
        WARNING: Slow for large datasets. Prefer query_polars() or query().
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

    # ========================================================================
    # Write Methods
    # ========================================================================

    def create_table(
        self,
        path: str,
        schema: "pa.Schema",
        partition_columns: list[str] | None = None,
    ) -> None:
        """Create a new Delta table at the specified path.

        Args:
            path: Path where the table will be created (local or s3://)
            schema: PyArrow schema defining the table structure
            partition_columns: Optional list of column names to partition by

        Example:
            >>> import pyarrow as pa
            >>> schema = pa.schema([
            ...     ("id", pa.int64()),
            ...     ("name", pa.string()),
            ...     ("value", pa.float64()),
            ... ])
            >>> engine.create_table("/path/to/table", schema)
        """
        self._engine.create_table(path, schema, partition_columns)

    def write(
        self,
        path: str,
        data: "pa.Table | list[pa.RecordBatch]",
        mode: str = "append",
        partition_columns: list[str] | None = None,
        schema_mode: str | None = None,
    ) -> None:
        """Write data to a Delta table.

        Creates the table if it doesn't exist. For existing tables,
        new columns are automatically added (merge mode by default).

        Args:
            path: Path to the Delta table
            data: PyArrow Table or list of RecordBatches to write
            mode: Write mode:
                - "append": Add data to existing table (default)
                - "overwrite": Replace all existing data
                - "error": Fail if table exists
                - "ignore": Do nothing if table exists
            partition_columns: Partition columns (only for new tables)
            schema_mode: Schema evolution mode:
                - "merge": Automatically add new columns (default for append)
                - "overwrite": Replace the table schema with the new schema
                - None: Uses "merge" for append mode

        Example:
            >>> import pyarrow as pa
            >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
            >>> engine.write("/path/to/table", table)

            # New columns are automatically merged
            >>> new_data = pa.table({"id": [3], "name": ["c"], "age": [25]})
            >>> engine.write("/path/to/table", new_data)  # age column auto-added
        """
        # Default to merge mode for append to support schema evolution
        effective_schema_mode = schema_mode
        if effective_schema_mode is None and mode == "append":
            effective_schema_mode = "merge"

        self._engine.write(path, data, mode, partition_columns, effective_schema_mode)

    def write_to_table(
        self,
        name: str,
        data: "pa.Table | list[pa.RecordBatch]",
        mode: str = "append",
    ) -> None:
        """Write data to a registered table.

        The table must be registered first using register_table().
        After writing, the table cache is automatically refreshed.

        Args:
            name: Registered table name
            data: PyArrow Table or list of RecordBatches to write
            mode: Write mode ("append" or "overwrite")

        Example:
            >>> engine.register_table("users", "/path/to/users")
            >>> table = pa.table({"id": [3], "name": ["c"]})
            >>> engine.write_to_table("users", table)
        """
        self._engine.write_to_table(name, data, mode)
