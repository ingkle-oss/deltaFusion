"""Type stubs for delta_fusion."""

from typing import Any

class DeltaEngine:
    """High-performance Delta Lake query engine."""

    def __init__(self, storage_options: dict[str, Any] | None = None) -> None: ...

    # Delta Table Methods
    def register_table(
        self, name: str, path: str, version: int | None = None
    ) -> None: ...
    def refresh_table(self, name: str) -> None: ...
    def refresh_all(self) -> None: ...
    def deregister_table(self, name: str) -> None: ...

    # Time Series Methods
    def register_time_series(
        self,
        name: str,
        path: str,
        timestamp_col: str,
        partition_col: str = "date",
        partition_format: str | None = None,
    ) -> None: ...
    def read_time_range(self, name: str, start: str, end: str) -> list[Any]: ...
    def read_time_range_direct(
        self,
        path: str,
        timestamp_col: str,
        start: str,
        end: str,
        partition_col: str = "date",
    ) -> list[Any]: ...
    def deregister_time_series(self, name: str) -> None: ...

    # Query Methods
    def query(self, sql: str) -> list[Any]: ...
    def query_to_dicts(self, sql: str) -> list[dict[str, Any]]: ...

    # Metadata Methods
    def table_info(self, name_or_path: str) -> dict[str, Any]: ...
    def list_tables(self) -> list[str]: ...
    def list_time_series(self) -> list[str]: ...
    def is_registered(self, name: str) -> bool: ...
    def is_time_series_registered(self, name: str) -> bool: ...

    # Write Methods
    def create_table(
        self,
        path: str,
        schema: Any,
        partition_columns: list[str] | None = None,
    ) -> None: ...
    def write(
        self,
        path: str,
        data: Any,
        mode: str = "append",
        partition_columns: list[str] | None = None,
    ) -> None: ...
    def write_to_table(
        self,
        name: str,
        data: Any,
        mode: str = "append",
    ) -> None: ...
