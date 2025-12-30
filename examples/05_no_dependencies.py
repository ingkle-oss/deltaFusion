"""No External Dependencies Example

Demonstrates using delta_fusion without PyArrow or Polars.
Only uses query_to_dicts() which returns plain Python dicts.

Requires: pip install delta_fusion (no extras needed for reading)
Note: Writing still requires PyArrow.
"""

import json
import tempfile
from pathlib import Path

# Only import for creating test data - not needed for reading
import pyarrow as pa

from delta_fusion import DeltaEngine


def main():
    engine = DeltaEngine()

    with tempfile.TemporaryDirectory() as tmp:
        table_path = Path(tmp) / "logs"

        # Create sample data (requires PyArrow)
        logs = pa.table(
            {
                "timestamp": [
                    "2024-01-01T10:00:00",
                    "2024-01-01T10:01:00",
                    "2024-01-01T10:02:00",
                ],
                "level": ["INFO", "WARNING", "ERROR"],
                "message": ["Started", "High memory", "Connection failed"],
                "code": [200, 300, 500],
            }
        )
        engine.write(str(table_path), logs, mode="error")
        engine.register_table("logs", str(table_path))

        # Query to Python dicts (no PyArrow/Polars needed)
        print("--- Query to dicts ---")
        rows = engine.query_to_dicts("SELECT * FROM logs")
        print(f"Type: {type(rows)}")
        print(f"Count: {len(rows)}")

        for row in rows:
            print(f"  {row}")

        # Works with standard library
        print("\n--- JSON serialization ---")
        errors = engine.query_to_dicts("SELECT * FROM logs WHERE level = 'ERROR'")
        print(json.dumps(errors, indent=2))

        # Aggregation also works
        print("\n--- Aggregation ---")
        stats = engine.query_to_dicts(
            """
            SELECT level, COUNT(*) as count
            FROM logs
            GROUP BY level
            """
        )
        for stat in stats:
            print(f"  {stat['level']}: {stat['count']} events")

        # Use with list comprehension
        print("\n--- Filter with Python ---")
        all_logs = engine.query_to_dicts("SELECT * FROM logs")
        warnings_and_errors = [
            log for log in all_logs if log["code"] >= 300
        ]
        print(f"Found {len(warnings_and_errors)} warnings/errors")


if __name__ == "__main__":
    main()
