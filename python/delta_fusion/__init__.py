"""delta_fusion - High-performance Delta Lake query engine for Python.

This library provides SQL query capabilities over Delta Lake tables using
DataFusion as the query engine. Data is transferred via zero-copy Arrow
for maximum performance. Tables are cached after registration to avoid
repeated log replay overhead.

Example:
    >>> from delta_fusion import DeltaEngine
    >>> engine = DeltaEngine()
    >>> engine.register_table("my_table", "/path/to/delta/table")
    >>> batches = engine.query("SELECT * FROM my_table WHERE id > 100")
    >>> # Convert to pandas
    >>> import pyarrow as pa
    >>> table = pa.Table.from_batches(batches)
    >>> df = table.to_pandas()
"""

from delta_fusion._core import PyDeltaEngine

__all__ = ["DeltaEngine"]


class DeltaEngine:
    """High-performance Delta Lake query engine.

    Uses DataFusion for SQL query execution and zero-copy Arrow transfer
    for optimal performance. The GIL is released during query execution
    to allow Python threads to continue.

    Tables are cached after registration to avoid repeated log replay.
    Use `refresh_table()` to reload tables with latest changes.

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
        """Refresh a registered table to load latest changes.

        This reloads the table from storage, picking up any new commits.

        Args:
            name: The registered table name to refresh
        """
        self._engine.refresh_table(name)

    def refresh_all(self) -> None:
        """Refresh all registered tables.

        Reloads all tables from storage to pick up latest changes.
        """
        self._engine.refresh_all()

    def deregister_table(self, name: str) -> None:
        """Deregister a table.

        Args:
            name: The table name to deregister
        """
        self._engine.deregister_table(name)

    def query(self, sql: str) -> list:
        """Execute SQL query and return PyArrow RecordBatches.

        This is the primary query method, returning zero-copy Arrow data
        for maximum performance. Use with pyarrow for further processing.

        Args:
            sql: SQL query string

        Returns:
            List of pyarrow.RecordBatch objects

        Example:
            >>> batches = engine.query("SELECT * FROM table")
            >>> import pyarrow as pa
            >>> table = pa.Table.from_batches(batches)
            >>> df = table.to_pandas()
        """
        return self._engine.query(sql)

    def query_to_dicts(self, sql: str) -> list[dict]:
        """Execute SQL query and return as list of dictionaries.

        WARNING: This method copies data row-by-row and is slow for large
        datasets. For large data, prefer query() with PyArrow.

        Args:
            sql: SQL query string

        Returns:
            List of row dictionaries
        """
        return self._engine.query_to_dicts(sql)

    def table_info(self, name_or_path: str) -> dict:
        """Get metadata about a Delta table.

        If the name matches a registered table, uses cached metadata (no I/O).
        Otherwise, opens the table fresh from the path.

        Args:
            name_or_path: Registered table name or path to Delta table

        Returns:
            Dict with path, version, schema, num_files, partition_columns
        """
        return self._engine.table_info(name_or_path)

    def list_tables(self) -> list[str]:
        """List all registered table names."""
        return self._engine.list_tables()

    def is_registered(self, name: str) -> bool:
        """Check if a table is registered.

        Args:
            name: Table name to check

        Returns:
            True if the table is registered
        """
        return self._engine.is_registered(name)
