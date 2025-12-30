"""Basic Delta Lake Query Example

Demonstrates basic table registration and SQL queries.
Requires: pip install delta_fusion[pyarrow]
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from delta_fusion import DeltaEngine


def main():
    engine = DeltaEngine()

    with tempfile.TemporaryDirectory() as tmp:
        table_path = Path(tmp) / "users"

        # Create sample data
        users = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 35, 28, 32],
                "city": ["Seoul", "Busan", "Seoul", "Incheon", "Busan"],
            }
        )

        # Write to Delta table
        engine.write(str(table_path), users, mode="error")
        print(f"Created table at: {table_path}")

        # Register for querying
        engine.register_table("users", str(table_path))

        # Simple SELECT
        print("\n--- All users ---")
        batches = engine.query("SELECT * FROM users")
        table = pa.Table.from_batches(batches)
        print(table.to_pandas())

        # Filtered query
        print("\n--- Users in Seoul ---")
        batches = engine.query("SELECT name, age FROM users WHERE city = 'Seoul'")
        table = pa.Table.from_batches(batches)
        print(table.to_pandas())

        # Aggregation
        print("\n--- Average age by city ---")
        batches = engine.query(
            """
            SELECT city, COUNT(*) as count, AVG(age) as avg_age
            FROM users
            GROUP BY city
            ORDER BY count DESC
            """
        )
        table = pa.Table.from_batches(batches)
        print(table.to_pandas())


if __name__ == "__main__":
    main()
