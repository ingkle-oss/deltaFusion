"""Multi-Table Join and Time Travel Example

Demonstrates joining multiple Delta tables and time travel queries.
Requires: pip install delta_fusion[pyarrow]
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from delta_fusion import DeltaEngine


def main():
    engine = DeltaEngine()

    with tempfile.TemporaryDirectory() as tmp:
        base_path = Path(tmp)

        # Create customers table
        customers_path = base_path / "customers"
        customers = pa.table(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "tier": ["gold", "silver", "gold"],
            }
        )
        engine.write(str(customers_path), customers, mode="error")

        # Create orders table
        orders_path = base_path / "orders"
        orders = pa.table(
            {
                "order_id": [101, 102, 103, 104],
                "customer_id": [1, 2, 1, 3],
                "amount": [100.0, 200.0, 150.0, 300.0],
                "status": ["completed", "pending", "completed", "completed"],
            }
        )
        engine.write(str(orders_path), orders, mode="error")

        # Register tables
        engine.register_table("customers", str(customers_path))
        engine.register_table("orders", str(orders_path))

        # Join query
        print("--- Customer Orders (JOIN) ---")
        batches = engine.query(
            """
            SELECT
                c.name,
                c.tier,
                o.order_id,
                o.amount,
                o.status
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            ORDER BY c.name, o.order_id
            """
        )
        print(pa.Table.from_batches(batches).to_pandas())

        # Aggregation with join
        print("\n--- Revenue by Customer Tier ---")
        batches = engine.query(
            """
            SELECT
                c.tier,
                COUNT(*) as order_count,
                SUM(o.amount) as total_revenue
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE o.status = 'completed'
            GROUP BY c.tier
            ORDER BY total_revenue DESC
            """
        )
        print(pa.Table.from_batches(batches).to_pandas())

        # Demonstrate time travel (versioning)
        print("\n--- Time Travel Demo ---")

        # Version 0: Initial state
        info = engine.table_info(str(orders_path))
        print(f"Orders table version: {info['version']}")

        # Add more orders (creates version 1)
        new_orders = pa.table(
            {
                "order_id": [105, 106],
                "customer_id": [2, 3],
                "amount": [250.0, 175.0],
                "status": ["completed", "pending"],
            }
        )
        engine.write(str(orders_path), new_orders)

        # Refresh to see new version
        engine.refresh_table("orders")
        info = engine.table_info(str(orders_path))
        print(f"Orders table version after append: {info['version']}")

        # Query current version
        batches = engine.query("SELECT COUNT(*) as count FROM orders")
        current_count = pa.Table.from_batches(batches).to_pandas()["count"][0]
        print(f"Current order count: {current_count}")

        # Query specific version (time travel)
        engine.deregister_table("orders")
        engine.register_table("orders_v0", str(orders_path), version=0)

        batches = engine.query("SELECT COUNT(*) as count FROM orders_v0")
        v0_count = pa.Table.from_batches(batches).to_pandas()["count"][0]
        print(f"Version 0 order count: {v0_count}")


if __name__ == "__main__":
    main()
