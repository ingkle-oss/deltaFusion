"""Schema Evolution Example

Demonstrates automatic schema evolution when appending data with new columns.
Requires: pip install delta_fusion[pyarrow]
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from delta_fusion import DeltaEngine


def main():
    engine = DeltaEngine()

    with tempfile.TemporaryDirectory() as tmp:
        table_path = Path(tmp) / "products"

        # Initial schema: id, name, price
        print("--- Step 1: Create initial table ---")
        initial_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Apple", "Banana", "Cherry"],
                "price": [1.0, 0.5, 2.0],
            }
        )
        engine.write(str(table_path), initial_data, mode="error")
        engine.register_table("products", str(table_path))

        result = pa.Table.from_batches(engine.query("SELECT * FROM products"))
        print(f"Columns: {result.column_names}")
        print(result.to_pandas())

        # Append with new column: category (auto-merged)
        print("\n--- Step 2: Append with new 'category' column ---")
        new_data = pa.table(
            {
                "id": [4, 5],
                "name": ["Date", "Elderberry"],
                "price": [3.0, 4.0],
                "category": ["dried", "berry"],  # New column!
            }
        )
        # Default mode is append with schema_mode="merge"
        engine.write(str(table_path), new_data)
        engine.refresh_table("products")

        result = pa.Table.from_batches(engine.query("SELECT * FROM products"))
        print(f"Columns: {result.column_names}")
        print(result.to_pandas())

        # Append with another new column: stock
        print("\n--- Step 3: Append with 'stock' column ---")
        more_data = pa.table(
            {
                "id": [6],
                "name": ["Fig"],
                "price": [2.5],
                "category": ["dried"],
                "stock": [100],  # Another new column!
            }
        )
        engine.write(str(table_path), more_data)
        engine.refresh_table("products")

        result = pa.Table.from_batches(engine.query("SELECT * FROM products"))
        print(f"Columns: {result.column_names}")
        print(result.to_pandas())

        # Query with the new columns
        print("\n--- Query: Products with stock info ---")
        result = pa.Table.from_batches(
            engine.query(
                """
                SELECT name, price, category, stock
                FROM products
                WHERE stock IS NOT NULL
                """
            )
        )
        print(result.to_pandas())


if __name__ == "__main__":
    main()
