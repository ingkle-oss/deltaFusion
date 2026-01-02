//! Python bindings for delta_fusion using PyO3.
//!
//! Key design decisions:
//! - GIL is released during async Rust operations for maximum concurrency
//! - Arrow data is transferred via zero-copy when possible
//! - Errors are converted to Python exceptions
//! - Tables are cached to avoid repeated log replay
//! - Time series API bypasses Delta log for fast partition-based access

// Allow useless_conversion warning for this module.
// PyO3's #[pymethods] macro makes clippy incorrectly flag DeltaFusionError -> PyErr conversions
// as "useless" even though they're required for the error type conversion.
#![allow(clippy::useless_conversion)]

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::ToPyObject;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::config::{init_logging, StorageConfig};
use crate::engine::{DeltaEngine, EngineConfig, WriteMode};
use crate::error::DeltaFusionError;

// =============================================================================
// Helper Types and Functions
// =============================================================================

/// Async executor that handles GIL release and runtime management.
struct AsyncExecutor {
    engine: Arc<tokio::sync::Mutex<DeltaEngine>>,
    runtime: Arc<Runtime>,
}

impl AsyncExecutor {
    fn new(engine: Arc<tokio::sync::Mutex<DeltaEngine>>, runtime: Arc<Runtime>) -> Self {
        Self { engine, runtime }
    }

    /// Execute an async operation with GIL release.
    fn run<F, T>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        F: FnOnce(
                &mut DeltaEngine,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = crate::error::Result<T>> + Send + '_>,
            > + Send,
        T: Send,
    {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                f(&mut engine).await
            })
        })
        .map_err(Into::into)
    }

    /// Execute an async operation that doesn't need mutable access.
    fn run_readonly<F, T>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        F: FnOnce(
                &DeltaEngine,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = crate::error::Result<T>> + Send + '_>,
            > + Send,
        T: Send,
    {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let engine = engine.lock().await;
                f(&engine).await
            })
        })
        .map_err(Into::into)
    }

    /// Execute a sync operation with GIL release.
    fn run_sync<F, T>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        F: FnOnce(&mut DeltaEngine) -> crate::error::Result<T> + Send,
        T: Send,
    {
        let engine = Arc::clone(&self.engine);
        let runtime = Arc::clone(&self.runtime);

        py.allow_threads(|| {
            runtime.block_on(async {
                let mut engine = engine.lock().await;
                f(&mut engine)
            })
        })
        .map_err(Into::into)
    }
}

/// Convert PyArrow data (Table or RecordBatches) to Vec<RecordBatch>.
fn pyarrow_to_batches(py: Python<'_>, data: &PyObject) -> PyResult<Vec<RecordBatch>> {
    let data_bound = data.bind(py);

    if data_bound.hasattr("to_batches")? {
        // It's a PyArrow Table - convert to batches
        let batch_list = data_bound.call_method0("to_batches")?;
        let batch_vec = batch_list.extract::<Vec<PyObject>>()?;
        batch_vec
            .into_iter()
            .map(|obj| RecordBatch::from_pyarrow_bound(obj.bind(py)))
            .collect()
    } else if let Ok(list) = data.extract::<Vec<PyObject>>(py) {
        // It's a list of RecordBatches
        list.into_iter()
            .map(|obj| RecordBatch::from_pyarrow_bound(obj.bind(py)))
            .collect()
    } else {
        // Try as a single RecordBatch
        Ok(vec![RecordBatch::from_pyarrow_bound(data_bound)?])
    }
}

/// Convert RecordBatches to PyArrow objects.
fn batches_to_pyarrow(py: Python<'_>, batches: Vec<RecordBatch>) -> PyResult<Vec<PyObject>> {
    batches
        .into_iter()
        .map(|batch| batch.to_pyarrow(py))
        .collect()
}

/// Convert RecordBatches to Arrow IPC bytes (as Python bytes object).
/// Uses IPC File format (with footer) for compatibility with Polars.
fn batches_to_ipc_bytes(py: Python<'_>, batches: Vec<RecordBatch>) -> PyResult<Py<PyBytes>> {
    use arrow_ipc::writer::FileWriter;

    if batches.is_empty() {
        return Ok(PyBytes::new_bound(py, &[]).unbind());
    }

    let schema = batches[0].schema();
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = FileWriter::try_new(&mut buffer, &schema)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("IPC writer error: {e}")))?;

    for batch in batches {
        writer.write(&batch).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("IPC write error: {e}"))
        })?;
    }

    writer
        .finish()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("IPC finish error: {e}")))?;

    Ok(PyBytes::new_bound(py, &buffer).unbind())
}

/// Parse write mode string to WriteMode enum.
fn parse_write_mode(mode: &str, allow_error_ignore: bool) -> PyResult<WriteMode> {
    match mode {
        "append" => Ok(WriteMode::Append),
        "overwrite" => Ok(WriteMode::Overwrite),
        "error" if allow_error_ignore => Ok(WriteMode::ErrorIfExists),
        "ignore" if allow_error_ignore => Ok(WriteMode::Ignore),
        _ => {
            let valid_modes = if allow_error_ignore {
                "'append', 'overwrite', 'error', or 'ignore'"
            } else {
                "'append' or 'overwrite'"
            };
            Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Invalid mode: {mode}. Use {valid_modes}"
            )))
        }
    }
}

// =============================================================================
// Python Engine Wrapper
// =============================================================================

/// Python wrapper for DeltaEngine.
#[pyclass(name = "DeltaEngine")]
pub struct PyDeltaEngine {
    executor: AsyncExecutor,
}

#[pymethods]
impl PyDeltaEngine {
    /// Create a new DeltaEngine.
    ///
    /// Args:
    ///     storage_options: Optional dict with S3/storage configuration
    ///     target_partitions: Number of partitions for parallel execution.
    ///         - None (default): Use all CPU cores
    ///         - 1: Single-threaded, lowest CPU usage
    ///         - 2-4: Balanced CPU usage
    ///         - Higher: More parallelism, higher CPU usage
    #[new]
    #[pyo3(signature = (storage_options=None, target_partitions=None))]
    fn new(
        storage_options: Option<&Bound<'_, PyDict>>,
        target_partitions: Option<usize>,
    ) -> PyResult<Self> {
        init_logging();

        let storage_config = parse_storage_options(storage_options)?;

        let engine_config = match target_partitions {
            Some(1) => EngineConfig::single_threaded(),
            Some(n) => EngineConfig::with_partitions(n),
            None => EngineConfig::default(),
        };

        let runtime = Runtime::new().map_err(|e| {
            DeltaFusionError::Runtime(format!("Failed to create Tokio runtime: {e}"))
        })?;

        let engine = DeltaEngine::with_configs(storage_config, engine_config);

        Ok(Self {
            executor: AsyncExecutor::new(
                Arc::new(tokio::sync::Mutex::new(engine)),
                Arc::new(runtime),
            ),
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
        self.executor.run(py, |engine| {
            Box::pin(async move {
                if let Some(v) = version {
                    engine.register_table_with_version(&name, &path, v).await
                } else {
                    engine.register_table(&name, &path).await
                }
            })
        })
    }

    /// Refresh a registered table to load latest changes.
    fn refresh_table(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.executor.run(py, |engine| {
            Box::pin(async move { engine.refresh_table(&name).await })
        })
    }

    /// Refresh all registered tables.
    fn refresh_all(&self, py: Python<'_>) -> PyResult<()> {
        self.executor.run(py, |engine| {
            Box::pin(async move { engine.refresh_all().await })
        })
    }

    /// Deregister a table.
    fn deregister_table(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.executor
            .run_sync(py, |engine| engine.deregister_table(&name))
    }

    // ========================================================================
    // Time Series Methods (Delta log bypass)
    // ========================================================================

    /// Register a time series table configuration.
    #[pyo3(signature = (name, path, timestamp_col, partition_col="date", partition_format=None))]
    fn register_time_series(
        &self,
        py: Python<'_>,
        name: String,
        path: String,
        timestamp_col: String,
        partition_col: &str,
        partition_format: Option<String>,
    ) -> PyResult<()> {
        self.executor.run_sync(py, |engine| {
            if let Some(fmt) = partition_format {
                engine.register_time_series_with_format(
                    &name,
                    &path,
                    partition_col,
                    &timestamp_col,
                    &fmt,
                );
            } else {
                engine.register_time_series(&name, &path, partition_col, &timestamp_col);
            }
            Ok(())
        })
    }

    /// Register a time series with hierarchical partitions (e.g., year/month/day).
    ///
    /// This method supports multiple partition columns for hierarchical date partitioning.
    ///
    /// Args:
    ///     name: Name for the time series
    ///     path: Base path to the data
    ///     timestamp_col: Timestamp column name in parquet files
    ///     partition_cols: List of partition column names (e.g., ["year", "month", "day"])
    ///     partition_formats: List of format strings (e.g., ["%Y", "%m", "%d"])
    #[pyo3(signature = (name, path, timestamp_col, partition_cols, partition_formats))]
    fn register_time_series_hierarchical(
        &self,
        py: Python<'_>,
        name: String,
        path: String,
        timestamp_col: String,
        partition_cols: Vec<String>,
        partition_formats: Vec<String>,
    ) -> PyResult<()> {
        if partition_cols.len() != partition_formats.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "partition_cols and partition_formats must have the same length",
            ));
        }

        self.executor.run_sync(py, |engine| {
            engine.register_time_series_hierarchical(
                &name,
                &path,
                &timestamp_col,
                &partition_cols
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
                &partition_formats
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            );
            Ok(())
        })
    }

    /// Read time range data directly from parquet files.
    fn read_time_range(
        &self,
        py: Python<'_>,
        name: String,
        start: String,
        end: String,
    ) -> PyResult<Vec<PyObject>> {
        let batches = self.executor.run_readonly(py, |engine| {
            Box::pin(async move { engine.read_time_range(&name, &start, &end).await })
        })?;
        batches_to_pyarrow(py, batches)
    }

    /// Read time range directly from a path (without pre-registration).
    #[pyo3(signature = (path, timestamp_col, start, end, partition_col="date"))]
    fn read_time_range_direct(
        &self,
        py: Python<'_>,
        path: String,
        timestamp_col: String,
        start: String,
        end: String,
        partition_col: &str,
    ) -> PyResult<Vec<PyObject>> {
        let batches = self.executor.run_readonly(py, |engine| {
            let partition_col = partition_col.to_string();
            Box::pin(async move {
                engine
                    .read_time_range_direct(&path, &partition_col, &timestamp_col, &start, &end)
                    .await
            })
        })?;
        batches_to_pyarrow(py, batches)
    }

    /// Read time range data and return as Arrow IPC bytes.
    fn read_time_range_ipc(
        &self,
        py: Python<'_>,
        name: String,
        start: String,
        end: String,
    ) -> PyResult<Py<PyBytes>> {
        let batches = self.executor.run_readonly(py, |engine| {
            Box::pin(async move { engine.read_time_range(&name, &start, &end).await })
        })?;
        batches_to_ipc_bytes(py, batches)
    }

    /// Deregister a time series.
    fn deregister_time_series(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.executor.run_sync(py, |engine| {
            engine.deregister_time_series(&name);
            Ok(())
        })
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a SQL query and return results as PyArrow RecordBatches.
    fn query(&self, py: Python<'_>, sql: String) -> PyResult<Vec<PyObject>> {
        let batches = self.executor.run_readonly(py, |engine| {
            Box::pin(async move { engine.query(&sql).await })
        })?;
        batches_to_pyarrow(py, batches)
    }

    /// Execute a SQL query and return results as a list of dictionaries.
    fn query_to_dicts(&self, py: Python<'_>, sql: String) -> PyResult<Py<PyList>> {
        let batches: Vec<RecordBatch> = self.executor.run_readonly(py, |engine| {
            Box::pin(async move { engine.query(&sql).await })
        })?;

        batches_to_dict_list(py, &batches)
    }

    /// Execute a SQL query and return results as Arrow IPC bytes.
    ///
    /// This method is useful for Polars integration without PyArrow dependency.
    /// Use `polars.read_ipc(bytes)` to convert to a Polars DataFrame.
    fn query_ipc(&self, py: Python<'_>, sql: String) -> PyResult<Py<PyBytes>> {
        let batches: Vec<RecordBatch> = self.executor.run_readonly(py, |engine| {
            Box::pin(async move { engine.query(&sql).await })
        })?;

        batches_to_ipc_bytes(py, batches)
    }

    // ========================================================================
    // Metadata Methods
    // ========================================================================

    /// Get information about a Delta table.
    fn table_info(&self, py: Python<'_>, name_or_path: String) -> PyResult<Py<PyDict>> {
        let info = self.executor.run_readonly(py, |engine| {
            Box::pin(async move { engine.table_info(&name_or_path).await })
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
        self.executor
            .run_sync(py, |engine| Ok(engine.list_tables()))
    }

    /// List all registered time series names.
    fn list_time_series(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.executor
            .run_sync(py, |engine| Ok(engine.list_time_series()))
    }

    /// Check if a table is registered.
    fn is_registered(&self, py: Python<'_>, name: String) -> PyResult<bool> {
        self.executor
            .run_sync(py, |engine| Ok(engine.is_registered(&name)))
    }

    /// Check if a time series is registered.
    fn is_time_series_registered(&self, py: Python<'_>, name: String) -> PyResult<bool> {
        self.executor
            .run_sync(py, |engine| Ok(engine.is_time_series_registered(&name)))
    }

    // ========================================================================
    // Write Methods
    // ========================================================================

    /// Create a new Delta table at the specified path.
    #[pyo3(signature = (path, schema, partition_columns=None))]
    fn create_table(
        &self,
        py: Python<'_>,
        path: String,
        schema: PyObject,
        partition_columns: Option<Vec<String>>,
    ) -> PyResult<()> {
        let arrow_schema = arrow::datatypes::Schema::from_pyarrow_bound(schema.bind(py))?;

        self.executor.run_readonly(py, |engine| {
            Box::pin(async move {
                engine
                    .create_table(&path, &arrow_schema, partition_columns)
                    .await
                    .map(|_| ())
            })
        })
    }

    /// Write RecordBatches to a Delta table.
    ///
    /// Args:
    ///     path: Path to the Delta table
    ///     data: PyArrow Table or list of RecordBatches
    ///     mode: Write mode ("append", "overwrite", "error", "ignore")
    ///     partition_columns: Optional partition columns (for new tables)
    ///     schema_mode: Schema evolution mode ("merge" or "overwrite")
    ///         - "merge": Add new columns to existing schema
    ///         - "overwrite": Replace schema entirely
    #[pyo3(signature = (path, data, mode="append", partition_columns=None, schema_mode=None))]
    fn write(
        &self,
        py: Python<'_>,
        path: String,
        data: PyObject,
        mode: &str,
        partition_columns: Option<Vec<String>>,
        schema_mode: Option<String>,
    ) -> PyResult<()> {
        let write_mode = parse_write_mode(mode, true)?;
        let batches = pyarrow_to_batches(py, &data)?;

        self.executor.run_readonly(py, |engine| {
            Box::pin(async move {
                engine
                    .write_with_options(&path, batches, write_mode, partition_columns, schema_mode)
                    .await
                    .map(|_| ())
            })
        })
    }

    /// Write data to a registered table by name.
    #[pyo3(signature = (name, data, mode="append"))]
    fn write_to_table(
        &self,
        py: Python<'_>,
        name: String,
        data: PyObject,
        mode: &str,
    ) -> PyResult<()> {
        let write_mode = parse_write_mode(mode, false)?;
        let batches = pyarrow_to_batches(py, &data)?;

        self.executor.run(py, |engine| {
            Box::pin(async move { engine.write_to_table(&name, batches, write_mode).await })
        })
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Parse storage options from Python dict.
fn parse_storage_options(opts: Option<&Bound<'_, PyDict>>) -> PyResult<StorageConfig> {
    if let Some(opts) = opts {
        let mut builder = StorageConfig::builder();
        if let Some(val) = opts.get_item("aws_access_key_id")? {
            builder = builder.aws_access_key_id(val.extract::<String>()?);
        }
        if let Some(val) = opts.get_item("aws_secret_access_key")? {
            builder = builder.aws_secret_access_key(val.extract::<String>()?);
        }
        if let Some(val) = opts.get_item("aws_region")? {
            builder = builder.aws_region(val.extract::<String>()?);
        }
        if let Some(val) = opts.get_item("aws_endpoint")? {
            builder = builder.aws_endpoint(val.extract::<String>()?);
        }
        if let Some(val) = opts.get_item("aws_allow_http")? {
            builder = builder.aws_allow_http(val.extract()?);
        }
        Ok(builder.build())
    } else {
        Ok(StorageConfig::from_env())
    }
}

/// Convert RecordBatches to a list of Python dictionaries.
fn batches_to_dict_list(py: Python<'_>, batches: &[RecordBatch]) -> PyResult<Py<PyList>> {
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

    // Macro to reduce repetition for primitive types
    macro_rules! extract_value {
        ($array_type:ty, $array:expr, $idx:expr) => {{
            let arr = $array.as_any().downcast_ref::<$array_type>().unwrap();
            arr.value($idx).to_object(py)
        }};
    }

    let value: PyObject = match array.data_type() {
        DataType::Boolean => extract_value!(BooleanArray, array, idx),
        DataType::Int8 => extract_value!(Int8Array, array, idx),
        DataType::Int16 => extract_value!(Int16Array, array, idx),
        DataType::Int32 => extract_value!(Int32Array, array, idx),
        DataType::Int64 => extract_value!(Int64Array, array, idx),
        DataType::UInt8 => extract_value!(UInt8Array, array, idx),
        DataType::UInt16 => extract_value!(UInt16Array, array, idx),
        DataType::UInt32 => extract_value!(UInt32Array, array, idx),
        DataType::UInt64 => extract_value!(UInt64Array, array, idx),
        DataType::Float32 => extract_value!(Float32Array, array, idx),
        DataType::Float64 => extract_value!(Float64Array, array, idx),
        DataType::Utf8 => extract_value!(StringArray, array, idx),
        DataType::LargeUtf8 => extract_value!(LargeStringArray, array, idx),
        DataType::Date32 => extract_value!(Date32Array, array, idx),
        DataType::Date64 => extract_value!(Date64Array, array, idx),
        _ => {
            // Fallback: convert to string representation
            arrow::util::display::array_value_to_string(array, idx)
                .unwrap_or_default()
                .to_object(py)
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
