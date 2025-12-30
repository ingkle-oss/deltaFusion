//! Integration tests for write functionality.

mod common;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use delta_fusion::{DeltaEngine, WriteMode};
use std::sync::Arc;
use tempfile::TempDir;

// =============================================================================
// Create Table Tests
// =============================================================================

#[tokio::test]
async fn test_create_table_basic() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("test_table");

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]);

    let engine = DeltaEngine::new();
    let result = engine
        .create_table(table_path.to_str().unwrap(), &schema, None)
        .await;

    assert!(result.is_ok());

    // Verify table was created by checking for _delta_log
    let delta_log = table_path.join("_delta_log");
    assert!(delta_log.exists());
}

#[tokio::test]
async fn test_create_table_with_partitions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("partitioned_table");

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]);

    let engine = DeltaEngine::new();
    let result = engine
        .create_table(
            table_path.to_str().unwrap(),
            &schema,
            Some(vec!["date".to_string()]),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_table_already_exists() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("existing_table");

    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

    let engine = DeltaEngine::new();

    // Create first time
    let _ = engine
        .create_table(table_path.to_str().unwrap(), &schema, None)
        .await
        .unwrap();

    // Try to create again - should fail
    let result = engine
        .create_table(table_path.to_str().unwrap(), &schema, None)
        .await;

    assert!(result.is_err());
}

// =============================================================================
// Write Tests
// =============================================================================

fn create_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]);

    let id_array = Int64Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), Some("Charlie")]);
    let value_array = Float64Array::from(vec![10.5, 20.3, 30.1]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_write_new_table() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("new_table");

    let batch = create_test_batch();
    let engine = DeltaEngine::new();

    let result = engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch],
            WriteMode::ErrorIfExists,
            None,
        )
        .await;

    assert!(result.is_ok());

    // Verify table was created
    let delta_log = table_path.join("_delta_log");
    assert!(delta_log.exists());
}

#[tokio::test]
async fn test_write_append() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("append_table");

    let batch = create_test_batch();
    let engine = DeltaEngine::new();

    // First write
    engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch.clone()],
            WriteMode::Append,
            None,
        )
        .await
        .unwrap();

    // Second write (append)
    let result = engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch],
            WriteMode::Append,
            None,
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_write_overwrite() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("overwrite_table");

    let batch = create_test_batch();
    let engine = DeltaEngine::new();

    // First write
    engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch.clone()],
            WriteMode::Append,
            None,
        )
        .await
        .unwrap();

    // Overwrite
    let result = engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch],
            WriteMode::Overwrite,
            None,
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_write_empty_batches() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("empty_table");

    let engine = DeltaEngine::new();

    let result = engine
        .write(table_path.to_str().unwrap(), vec![], WriteMode::Append, None)
        .await;

    // Should fail - no data to write
    assert!(result.is_err());
}

#[tokio::test]
async fn test_write_ignore_existing() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("ignore_table");

    let batch = create_test_batch();
    let engine = DeltaEngine::new();

    // First write
    engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch.clone()],
            WriteMode::ErrorIfExists,
            None,
        )
        .await
        .unwrap();

    // Try with Ignore mode - should not fail
    let result = engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch],
            WriteMode::Ignore,
            None,
        )
        .await;

    assert!(result.is_ok());
}

// =============================================================================
// Write to Registered Table Tests
// =============================================================================

#[tokio::test]
async fn test_write_to_registered_table() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("registered_table");

    let batch = create_test_batch();
    let mut engine = DeltaEngine::new();

    // Create table first
    engine
        .write(
            table_path.to_str().unwrap(),
            vec![batch.clone()],
            WriteMode::ErrorIfExists,
            None,
        )
        .await
        .unwrap();

    // Register the table
    engine
        .register_table("test", table_path.to_str().unwrap())
        .await
        .unwrap();

    // Write to registered table
    let result = engine
        .write_to_table("test", vec![batch], WriteMode::Append)
        .await;

    assert!(result.is_ok());

    // Verify table is still registered
    assert!(engine.is_registered("test"));
}

#[tokio::test]
async fn test_write_to_unregistered_table() {
    let batch = create_test_batch();
    let mut engine = DeltaEngine::new();

    let result = engine
        .write_to_table("nonexistent", vec![batch], WriteMode::Append)
        .await;

    assert!(result.is_err());
}

// =============================================================================
// Schema Conversion Tests
// =============================================================================

#[tokio::test]
async fn test_create_table_various_types() {
    use arrow::datatypes::TimeUnit;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let table_path = temp_dir.path().join("various_types");

    let schema = Schema::new(vec![
        Field::new("bool_col", DataType::Boolean, true),
        Field::new("int8_col", DataType::Int8, true),
        Field::new("int16_col", DataType::Int16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("int64_col", DataType::Int64, true),
        Field::new("float32_col", DataType::Float32, true),
        Field::new("float64_col", DataType::Float64, true),
        Field::new("string_col", DataType::Utf8, true),
        Field::new("binary_col", DataType::Binary, true),
        Field::new("date_col", DataType::Date32, true),
        Field::new(
            "timestamp_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);

    let engine = DeltaEngine::new();
    let result = engine
        .create_table(table_path.to_str().unwrap(), &schema, None)
        .await;

    assert!(result.is_ok());
}
