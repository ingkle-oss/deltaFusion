//! Common test utilities for integration tests.

#![allow(dead_code)]

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

/// Create a test parquet file with sample time series data.
pub fn create_test_parquet(
    path: &Path,
    partition_col: &str,
    partition_value: &str,
    timestamp_col: &str,
    timestamps: Vec<i64>,
    values: Vec<f64>,
) -> std::io::Result<()> {
    // Create partition directory
    let partition_dir = path.join(format!("{partition_col}={partition_value}"));
    fs::create_dir_all(&partition_dir)?;

    let file_path = partition_dir.join("data.parquet");
    let file = File::create(&file_path)?;

    // Create schema
    let schema = Schema::new(vec![
        Field::new(
            timestamp_col,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("id", DataType::Int64, false),
    ]);
    let schema = Arc::new(schema);

    // Create arrays
    let ts_array = TimestampMicrosecondArray::from(timestamps.clone());
    let value_array = Float64Array::from(values);
    let id_array = Int64Array::from(vec![1i64; timestamps.len()]);

    // Create record batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts_array),
            Arc::new(value_array),
            Arc::new(id_array),
        ],
    )
    .expect("Failed to create record batch");

    // Write to parquet
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("Failed to create writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    Ok(())
}

/// Create a simple Delta table structure (minimal, for testing).
/// Note: This creates a basic delta log structure without actual transactions.
pub fn create_test_delta_table(path: &Path, data: Vec<(&str, Vec<i64>)>) -> std::io::Result<()> {
    fs::create_dir_all(path)?;

    // Create _delta_log directory
    let delta_log = path.join("_delta_log");
    fs::create_dir_all(&delta_log)?;

    // Create parquet data file
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let schema = Arc::new(schema);

    let ids: Vec<i64> = data.iter().flat_map(|(_, ids)| ids.clone()).collect();
    let names: Vec<&str> = data
        .iter()
        .flat_map(|(name, ids)| vec![*name; ids.len()])
        .collect();

    let id_array = Int64Array::from(ids);
    let name_array = StringArray::from(names);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .expect("Failed to create record batch");

    let parquet_path = path.join("part-00000.parquet");
    let file = File::create(&parquet_path)?;
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), None).expect("Failed to create writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    // Create minimal delta log (version 0)
    let log_entry = serde_json::json!({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    });
    let protocol_line = serde_json::to_string(&log_entry).unwrap();

    let meta_entry = serde_json::json!({
        "metaData": {
            "id": "test-table-id",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":false,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1700000000000i64
        }
    });
    let meta_line = serde_json::to_string(&meta_entry).unwrap();

    let add_entry = serde_json::json!({
        "add": {
            "path": "part-00000.parquet",
            "size": 1000,
            "partitionValues": {},
            "modificationTime": 1700000000000i64,
            "dataChange": true
        }
    });
    let add_line = serde_json::to_string(&add_entry).unwrap();

    let log_content = format!("{protocol_line}\n{meta_line}\n{add_line}\n");
    fs::write(delta_log.join("00000000000000000000.json"), log_content)?;

    Ok(())
}

/// Generate microsecond timestamps for a given date.
pub fn generate_timestamps(date: &str, count: usize, interval_secs: i64) -> Vec<i64> {
    use chrono::NaiveDateTime;

    let base = NaiveDateTime::parse_from_str(&format!("{date}T00:00:00"), "%Y-%m-%dT%H:%M:%S")
        .expect("Invalid date");
    let base_micros = base.and_utc().timestamp_micros();

    (0..count)
        .map(|i| base_micros + (i as i64 * interval_secs * 1_000_000))
        .collect()
}
