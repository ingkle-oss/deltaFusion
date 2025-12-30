//! Python bindings for delta_fusion using PyO3.
//!
//! Key design decisions:
//! - GIL is released during async Rust operations for maximum concurrency
//! - Arrow data is transferred via zero-copy when possible
//! - Errors are converted to Python exceptions
//! - Tables are cached to avoid repeated log replay
//! - Time series API bypasses Delta log for fast partition-based access

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::ToPyObject;

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::config::{init_logging, StorageConfig};
use crate::engine::DeltaEngine;
use crate::error::DeltaFusionError;

/// Python wrapper for DeltaEngine.
#[pyclass]
pub struct PyDeltaEngine {
    engine: Arc<tokio::sync::Mutex<DeltaEngine>>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyDeltaEngine {
    /// Create a new DeltaEngine.
    #[new]
    #[pyo3(signature = (storage_options=None))]
    fn new(storage_options: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        init_logging();

        let storage_config = if let Some(opts) = storage_options {
            let mut config = StorageConfig::default();
            if let Some(val) = opts.get_item("aws_access_key_id")? {
                config.aws_access_key_id = Some(val.extract()?);
            }
            if let Some(val) = opts.get_item("aws_secret_access_key")? {
                config.aws_secret_access_key = Some(val.extract()?);
            }
            if let Some(val) = opts.get_item("aws_region")? {
                config.aws_region = Some(val.extract()?);
            }
            if let Some(val) = opts.get_item("aws_endpoint")? {
                config.aws_endpoint = Some(val.extract()?);
            }
            if let Some(val) = opts.get_item("aws_allow_http")? {
                config.aws_allow_http = val.extract()?;
            }
            config
        } else {
            StorageConfig::from_env()
        };

        let runtime = Runtime::new().map_err(|e| {
            DeltaFusionError::Runtime(format!("Failed to create Tokio runtime: {}", e))
        })?;

        let engine = DeltaEngine::with_config(storage_config);

        Ok(Self {
            engine: Arc::new(tokio::sync::Mutex::new(engine)),
            runtime: Arc::new(runtime),
        })
    }

    // ========================================================================
    // Delta Table Methods
    // ========================================================================

    /// Register a Delta table for querying.
    #[pyo3(signature = (name, path, version=None))]
    fn register_table(
        &self,
        py: Python<'_>,
        name: String,
        path: String,
        version: Option<i64>,
    ) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                if let Some(v) = version {
                    engine.register_table_with_version(&name, &path, v).await
                } else {
                    engine.register_table(&name, &path).await
                }
            })
        })?;

        Ok(())
    }

    /// Refresh a registered table to load latest changes.
    fn refresh_table(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                engine.refresh_table(&name).await
            })
        })?;

        Ok(())
    }

    /// Refresh all registered tables.
    fn refresh_all(&self, py: Python<'_>) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                engine.refresh_all().await
            })
        })?;

        Ok(())
    }

    /// Deregister a table.
    fn deregister_table(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                engine.deregister_table(&name)
            })
        })?;

        Ok(())
    }

    // ========================================================================
    // Time Series Methods (Delta log bypass)
    // ========================================================================

    /// Register a time series table configuration.
    ///
    /// This does NOT read any data - it just stores the configuration.
    /// Use `read_time_range` to actually query data.
    ///
    /// Args:
    ///     name: Name for the time series
    ///     path: Base path to the data (e.g., "s3://bucket/sensor_data")
    ///     partition_col: Partition column name (e.g., "dt")
    ///     timestamp_col: Timestamp column name in parquet files
    ///     partition_format: Optional partition date format (default: "%Y-%m-%d")
    #[pyo3(signature = (name, path, partition_col, timestamp_col, partition_format=None))]
    fn register_time_series(
        &self,
        py: Python<'_>,
        name: String,
        path: String,
        partition_col: String,
        timestamp_col: String,
        partition_format: Option<String>,
    ) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                if let Some(fmt) = partition_format {
                    engine.register_time_series_with_format(
                        &name,
                        &path,
                        &partition_col,
                        &timestamp_col,
                        &fmt,
                    );
                } else {
                    engine.register_time_series(&name, &path, &partition_col, &timestamp_col);
                }
                Ok::<(), DeltaFusionError>(())
            })
        })?;

        Ok(())
    }

    /// Read time range data directly from parquet files.
    ///
    /// This bypasses Delta log entirely for maximum performance.
    ///
    /// Args:
    ///     name: Registered time series name
    ///     start: Start timestamp (ISO 8601 format)
    ///     end: End timestamp (ISO 8601 format)
    ///
    /// Returns:
    ///     List of PyArrow RecordBatches
    fn read_time_range(
        &self,
        py: Python<'_>,
        name: String,
        start: String,
        end: String,
    ) -> PyResult<Vec<PyObject>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        let batches: Vec<RecordBatch> = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.read_time_range(&name, &start, &end).await
            })
        })?;

        batches.into_iter().map(|batch| batch.to_pyarrow(py)).collect()
    }

    /// Read time range directly from a path (without pre-registration).
    ///
    /// Args:
    ///     path: Base path to the data
    ///     partition_col: Partition column name (e.g., "dt")
    ///     timestamp_col: Timestamp column name in parquet files
    ///     start: Start timestamp (ISO 8601 format)
    ///     end: End timestamp (ISO 8601 format)
    ///
    /// Returns:
    ///     List of PyArrow RecordBatches
    fn read_time_range_direct(
        &self,
        py: Python<'_>,
        path: String,
        partition_col: String,
        timestamp_col: String,
        start: String,
        end: String,
    ) -> PyResult<Vec<PyObject>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        let batches: Vec<RecordBatch> = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine
                    .read_time_range_direct(&path, &partition_col, &timestamp_col, &start, &end)
                    .await
            })
        })?;

        batches.into_iter().map(|batch| batch.to_pyarrow(py)).collect()
    }

    /// Deregister a time series.
    fn deregister_time_series(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                engine.deregister_time_series(&name);
                Ok::<(), DeltaFusionError>(())
            })
        })?;

        Ok(())
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a SQL query and return results as PyArrow RecordBatches.
    fn query(&self, py: Python<'_>, sql: String) -> PyResult<Vec<PyObject>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        let batches: Vec<RecordBatch> = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.query(&sql).await
            })
        })?;

        batches.into_iter().map(|batch| batch.to_pyarrow(py)).collect()
    }

    /// Execute a SQL query and return results as a list of dictionaries.
    ///
    /// WARNING: This method copies data row-by-row and is slow for large datasets.
    fn query_to_dicts(&self, py: Python<'_>, sql: String) -> PyResult<Py<PyList>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        let batches: Vec<RecordBatch> = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.query(&sql).await
            })
        })?;

        let result = PyList::empty_bound(py);
        for batch in batches {
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let row_dict = PyDict::new_bound(py);
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    let value = arrow_value_to_py(py, column, row_idx)?;
                    row_dict.set_item(field.name(), value)?;
                }
                result.append(row_dict)?;
            }
        }

        Ok(result.into())
    }

    // ========================================================================
    // Metadata Methods
    // ========================================================================

    /// Get information about a Delta table.
    fn table_info(&self, py: Python<'_>, name_or_path: String) -> PyResult<Py<PyDict>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        let info = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.table_info(&name_or_path).await
            })
        })?;

        let result = PyDict::new_bound(py);
        result.set_item("path", info.path)?;
        result.set_item("version", info.version)?;
        result.set_item("schema", info.schema)?;
        result.set_item("num_files", info.num_files)?;
        result.set_item("partition_columns", info.partition_columns)?;

        Ok(result.into())
    }

    /// List all registered table names.
    fn list_tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                Ok(engine.list_tables())
            })
        })
    }

    /// List all registered time series names.
    fn list_time_series(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                Ok(engine.list_time_series())
            })
        })
    }

    /// Check if a table is registered.
    fn is_registered(&self, py: Python<'_>, name: String) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                Ok(engine.is_registered(&name))
            })
        })
    }

    /// Check if a time series is registered.
    fn is_time_series_registered(&self, py: Python<'_>, name: String) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                Ok(engine.is_time_series_registered(&name))
            })
        })
    }

    // ========================================================================
    // Write Methods
    // ========================================================================

    /// Create a new Delta table at the specified path.
    ///
    /// Args:
    ///     path: Path where the table will be created
    ///     schema: PyArrow schema for the table
    ///     partition_columns: Optional list of partition column names
    #[pyo3(signature = (path, schema, partition_columns=None))]
    fn create_table(
        &self,
        py: Python<'_>,
        path: String,
        schema: PyObject,
        partition_columns: Option<Vec<String>>,
    ) -> PyResult<()> {
        use arrow::pyarrow::FromPyArrow;

        let arrow_schema = arrow::datatypes::Schema::from_pyarrow_bound(schema.bind(py))?;
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.create_table(&path, &arrow_schema, partition_columns).await
            })
        })?;

        Ok(())
    }

    /// Write RecordBatches to a Delta table.
    ///
    /// Args:
    ///     path: Path to the Delta table
    ///     data: PyArrow Table or list of RecordBatches
    ///     mode: Write mode - "append", "overwrite", "error", or "ignore"
    ///     partition_columns: Optional partition columns (for new tables)
    #[pyo3(signature = (path, data, mode="append", partition_columns=None))]
    fn write(
        &self,
        py: Python<'_>,
        path: String,
        data: PyObject,
        mode: &str,
        partition_columns: Option<Vec<String>>,
    ) -> PyResult<()> {
        use arrow::pyarrow::FromPyArrow;
        use crate::engine::WriteMode;

        let write_mode = match mode {
            "append" => WriteMode::Append,
            "overwrite" => WriteMode::Overwrite,
            "error" => WriteMode::ErrorIfExists,
            "ignore" => WriteMode::Ignore,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid mode: {}. Use 'append', 'overwrite', 'error', or 'ignore'",
                    mode
                )));
            }
        };

        // Convert data to RecordBatches
        let batches: Vec<RecordBatch> = {
            // Check if it's a PyArrow Table by checking for to_batches method
            let data_bound = data.bind(py);
            if data_bound.hasattr("to_batches")? {
                // It's a Table - convert to batches
                let batch_list = data_bound.call_method0("to_batches")?;
                let batch_vec = batch_list.extract::<Vec<PyObject>>()?;
                batch_vec
                    .into_iter()
                    .map(|obj| RecordBatch::from_pyarrow_bound(obj.bind(py)))
                    .collect::<Result<Vec<_>, _>>()?
            } else if let Ok(list) = data.extract::<Vec<PyObject>>(py) {
                // It's a list of RecordBatches
                list.into_iter()
                    .map(|obj| RecordBatch::from_pyarrow_bound(obj.bind(py)))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                // Try as a single RecordBatch
                vec![RecordBatch::from_pyarrow_bound(data_bound)?]
            }
        };

        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.write(&path, batches, write_mode, partition_columns).await
            })
        })?;

        Ok(())
    }

    /// Write data to a registered table by name.
    ///
    /// Args:
    ///     name: Registered table name
    ///     data: PyArrow Table or list of RecordBatches
    ///     mode: Write mode - "append" or "overwrite"
    #[pyo3(signature = (name, data, mode="append"))]
    fn write_to_table(
        &self,
        py: Python<'_>,
        name: String,
        data: PyObject,
        mode: &str,
    ) -> PyResult<()> {
        use arrow::pyarrow::FromPyArrow;
        use crate::engine::WriteMode;

        let write_mode = match mode {
            "append" => WriteMode::Append,
            "overwrite" => WriteMode::Overwrite,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid mode: {}. Use 'append' or 'overwrite'",
                    mode
                )));
            }
        };

        // Convert data to RecordBatches
        let batches: Vec<RecordBatch> = {
            let data_bound = data.bind(py);
            if data_bound.hasattr("to_batches")? {
                let batch_list = data_bound.call_method0("to_batches")?;
                let batch_vec = batch_list.extract::<Vec<PyObject>>()?;
                batch_vec
                    .into_iter()
                    .map(|obj| RecordBatch::from_pyarrow_bound(obj.bind(py)))
                    .collect::<Result<Vec<_>, _>>()?
            } else if let Ok(list) = data.extract::<Vec<PyObject>>(py) {
                list.into_iter()
                    .map(|obj| RecordBatch::from_pyarrow_bound(obj.bind(py)))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                vec![RecordBatch::from_pyarrow_bound(data_bound)?]
            }
        };

        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                engine.write_to_table(&name, batches, write_mode).await
            })
        })?;

        Ok(())
    }
}

/// Convert an Arrow array value at a specific index to a Python object.
fn arrow_value_to_py(
    py: Python<'_>,
    array: &arrow::array::ArrayRef,
    idx: usize,
) -> PyResult<PyObject> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(idx) {
        return Ok(py.None());
    }

    let value: PyObject = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            arr.value(idx).to_object(py)
        }
        _ => {
            // Fallback: convert to string representation
            let arr = arrow::util::display::array_value_to_string(array, idx)
                .unwrap_or_else(|_| "".to_string());
            arr.to_object(py)
        }
    };

    Ok(value)
}

/// Python module definition.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDeltaEngine>()?;
    Ok(())
}
