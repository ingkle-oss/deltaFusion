"""Polars Integration Example

Demonstrates using delta_fusion with Polars instead of PyArrow.
Requires: pip install delta_fusion[all]  # or [polars] + [pyarrow] for write
"""

import tempfile
from pathlib import Path

import polars as pl
import pyarrow as pa  # Only needed for write

from delta_fusion import DeltaEngine


def main():
    engine = DeltaEngine()

    with tempfile.TemporaryDirectory() as tmp:
        table_path = Path(tmp) / "sales"

        # Create sample data (using PyArrow for write)
        sales = pa.table(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
                "product": ["A", "B", "A", "B"],
                "quantity": [10, 20, 15, 25],
                "price": [100.0, 200.0, 100.0, 200.0],
            }
        )
        engine.write(str(table_path), sales, mode="error")
        engine.register_table("sales", str(table_path))

        # Query directly to Polars DataFrame
        print("--- Query with Polars ---")
        df = engine.query_polars("SELECT * FROM sales")
        print(df)
        print(f"Type: {type(df)}")

        # Use Polars operations
        print("\n--- Polars aggregation ---")
        result = (
            df.group_by("product")
            .agg(
                [
                    pl.col("quantity").sum().alias("total_qty"),
                    (pl.col("quantity") * pl.col("price")).sum().alias("revenue"),
                ]
            )
            .sort("revenue", descending=True)
        )
        print(result)

        # Lazy evaluation with SQL + Polars
        print("\n--- SQL filter + Polars lazy ---")
        df = engine.query_polars("SELECT * FROM sales WHERE product = 'A'")
        lazy_result = df.lazy().with_columns(
            (pl.col("quantity") * pl.col("price")).alias("total")
        )
        print(lazy_result.collect())


if __name__ == "__main__":
    main()
