"""delta_fusion - High-performance Delta Lake query engine for Python.

This library provides SQL query capabilities over Delta Lake tables using
DataFusion as the query engine. Data is transferred via zero-copy Arrow
for maximum performance.

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

        Args:
            name: Table name to use in SQL queries
            path: Path to Delta table (local path or s3://)
            version: Optional specific version for time travel
        """
        self._engine.register_table(name, path, version)

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

        Convenience method for smaller result sets. For large data,
        prefer query() with PyArrow for better performance.

        Args:
            sql: SQL query string

        Returns:
            List of row dictionaries
        """
        return self._engine.query_to_dicts(sql)

    def table_info(self, path: str) -> dict:
        """Get metadata about a Delta table.

        Args:
            path: Path to Delta table

        Returns:
            Dict with version, schema, num_files, partition_columns
        """
        return self._engine.table_info(path)

    def list_tables(self) -> list[str]:
        """List all registered table names."""
        return self._engine.list_tables()
