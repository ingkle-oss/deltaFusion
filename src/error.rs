//! Error types for delta_fusion.

use thiserror::Error;

/// Custom error type for delta_fusion operations.
#[derive(Error, Debug)]
pub enum DeltaFusionError {
    #[error("Delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Runtime error: {0}")]
    Runtime(String),
}

pub type Result<T> = std::result::Result<T, DeltaFusionError>;

#[cfg(feature = "python")]
impl From<DeltaFusionError> for pyo3::PyErr {
    fn from(err: DeltaFusionError) -> pyo3::PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
    }
}
