//! Python bindings for delta_fusion using PyO3.
//!
//! Key design decisions:
//! - GIL is released during async Rust operations for maximum concurrency
//! - Arrow data is transferred via zero-copy when possible
//! - Errors are converted to Python exceptions

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
///
/// Provides SQL query capabilities over Delta Lake tables with zero-copy
/// Arrow data transfer to Python.
#[pyclass]
pub struct PyDeltaEngine {
    engine: Arc<tokio::sync::Mutex<DeltaEngine>>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyDeltaEngine {
    /// Create a new DeltaEngine.
    ///
    /// Configuration is loaded from environment variables:
    /// - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY: S3 credentials
    /// - AWS_REGION: S3 region
    /// - AWS_ENDPOINT_URL: Custom S3 endpoint (for MinIO, etc.)
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

    /// Register a Delta table for querying.
    ///
    /// Args:
    ///     name: The name to use in SQL queries
    ///     path: Path to the Delta table (local or s3://)
    ///     version: Optional specific version to load
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

        // Release GIL during async operation
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

    /// Execute a SQL query and return results as PyArrow RecordBatches.
    ///
    /// This uses zero-copy transfer when possible for maximum performance.
    ///
    /// Args:
    ///     sql: SQL query string
    ///
    /// Returns:
    ///     List of PyArrow RecordBatches
    fn query(&self, py: Python<'_>, sql: String) -> PyResult<Vec<PyObject>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        // Release GIL during query execution
        let batches: Vec<RecordBatch> = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.query(&sql).await
            })
        })?;

        // Convert to PyArrow RecordBatches via zero-copy
        let py_batches: PyResult<Vec<PyObject>> = batches
            .into_iter()
            .map(|batch| batch.to_pyarrow(py))
            .collect();

        py_batches
    }

    /// Execute a SQL query and return results as a list of dictionaries.
    ///
    /// This is a convenience method for smaller result sets.
    /// For large data, prefer `query()` with PyArrow.
    ///
    /// Args:
    ///     sql: SQL query string
    ///
    /// Returns:
    ///     List of row dictionaries
    fn query_to_dicts(&self, py: Python<'_>, sql: String) -> PyResult<Py<PyList>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        // Release GIL during query execution
        let batches: Vec<RecordBatch> = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.query(&sql).await
            })
        })?;

        // Convert to Python list of dicts
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

    /// Get information about a Delta table.
    ///
    /// Args:
    ///     path: Path to the Delta table
    ///
    /// Returns:
    ///     Dictionary with version, schema, num_files, partition_columns
    fn table_info(&self, py: Python<'_>, path: String) -> PyResult<Py<PyDict>> {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        let info = py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                engine.table_info(&path).await
            })
        })?;

        let result = PyDict::new_bound(py);
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
