# Non-partition column filter queries fail with DataFusion errors

## Description

SQL queries with WHERE clauses on non-partition columns fail with DataFusion errors.

## Errors Observed

### 1. Numeric column filter
```sql
SELECT * FROM table WHERE value > 500
```
**Error:**
```
DataFusion error: Internal error: Only intervals with the same data type are comparable, lhs:Int64, rhs:Float64
```

### 2. String column filter
```sql
SELECT * FROM table WHERE category = 'cat_5'
```
**Error:**
```
DataFusion error: External error: Parquet error: External: bad data
```

## Working Cases

- Partition column filters work correctly: `WHERE date = '2024-01-01'`
- Full table scans work correctly: `SELECT * FROM table`
- Aggregation queries work correctly: `SELECT category, COUNT(*) FROM table GROUP BY category`

## Environment

- deltalake: 1.0+
- DataFusion: 43.0
- delta_fusion: 0.0.1

## Possible Causes

1. **Numeric filter issue**: Type coercion between SQL literal (Int64) and column type (Float64)
2. **String filter issue**: Possible delta-rs/DataFusion integration issue with predicate pushdown to Parquet

## Reproduction

```python
import delta_fusion

engine = delta_fusion.DeltaEngine()
engine.register_table("test", "/path/to/delta/table")

# This fails
engine.query("SELECT * FROM test WHERE category = 'cat_5'")
```

## Related

This may be related to how delta-rs registers tables with DataFusion's SessionContext.
