//! Error types for delta_fusion.

use std::fmt;
use thiserror::Error;

/// Error category for quick classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Storage or I/O related errors
    Storage,
    /// Query execution errors
    Query,
    /// Schema or type errors
    Schema,
    /// Configuration errors
    Config,
    /// Resource not found
    NotFound,
    /// Resource already exists
    AlreadyExists,
    /// Runtime or internal errors
    Runtime,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Storage => write!(f, "Storage"),
            ErrorKind::Query => write!(f, "Query"),
            ErrorKind::Schema => write!(f, "Schema"),
            ErrorKind::Config => write!(f, "Config"),
            ErrorKind::NotFound => write!(f, "NotFound"),
            ErrorKind::AlreadyExists => write!(f, "AlreadyExists"),
            ErrorKind::Runtime => write!(f, "Runtime"),
        }
    }
}

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

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Write error: {0}")]
    Write(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("{context}: {source}")]
    WithContext {
        context: String,
        #[source]
        source: Box<DeltaFusionError>,
    },
}

impl DeltaFusionError {
    /// Get the error kind for categorization.
    pub fn kind(&self) -> ErrorKind {
        match self {
            DeltaFusionError::DeltaTable(_) => ErrorKind::Storage,
            DeltaFusionError::Arrow(_) => ErrorKind::Schema,
            DeltaFusionError::DataFusion(_) => ErrorKind::Query,
            DeltaFusionError::Io(_) => ErrorKind::Storage,
            DeltaFusionError::Json(_) => ErrorKind::Schema,
            DeltaFusionError::Parquet(_) => ErrorKind::Storage,
            DeltaFusionError::TableNotFound(_) => ErrorKind::NotFound,
            DeltaFusionError::InvalidConfig(_) => ErrorKind::Config,
            DeltaFusionError::Query(_) => ErrorKind::Query,
            DeltaFusionError::Runtime(_) => ErrorKind::Runtime,
            DeltaFusionError::Write(_) => ErrorKind::Storage,
            DeltaFusionError::Schema(_) => ErrorKind::Schema,
            DeltaFusionError::TableExists(_) => ErrorKind::AlreadyExists,
            DeltaFusionError::WithContext { source, .. } => source.kind(),
        }
    }

    /// Check if this is a "not found" error.
    pub fn is_not_found(&self) -> bool {
        self.kind() == ErrorKind::NotFound
    }

    /// Check if this is a storage/IO error.
    pub fn is_storage_error(&self) -> bool {
        self.kind() == ErrorKind::Storage
    }

    /// Check if this is a schema/type error.
    pub fn is_schema_error(&self) -> bool {
        self.kind() == ErrorKind::Schema
    }

    /// Add context to this error.
    pub fn with_context(self, context: impl Into<String>) -> Self {
        DeltaFusionError::WithContext {
            context: context.into(),
            source: Box::new(self),
        }
    }

    /// Create a table not found error.
    pub fn table_not_found(name: impl Into<String>) -> Self {
        DeltaFusionError::TableNotFound(name.into())
    }

    /// Create an invalid config error.
    pub fn invalid_config(msg: impl Into<String>) -> Self {
        DeltaFusionError::InvalidConfig(msg.into())
    }

    /// Create a query error.
    pub fn query(msg: impl Into<String>) -> Self {
        DeltaFusionError::Query(msg.into())
    }

    /// Create a write error.
    pub fn write(msg: impl Into<String>) -> Self {
        DeltaFusionError::Write(msg.into())
    }

    /// Create a schema error.
    pub fn schema(msg: impl Into<String>) -> Self {
        DeltaFusionError::Schema(msg.into())
    }

    /// Create a runtime error.
    pub fn runtime(msg: impl Into<String>) -> Self {
        DeltaFusionError::Runtime(msg.into())
    }
}

/// Result type alias for delta_fusion operations.
pub type Result<T> = std::result::Result<T, DeltaFusionError>;

/// Extension trait to add context to Result types.
pub trait ResultExt<T> {
    /// Add context to an error.
    fn context(self, ctx: impl Into<String>) -> Result<T>;

    /// Add context lazily (only evaluated on error).
    fn with_context<F, S>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: Into<String>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, ctx: impl Into<String>) -> Result<T> {
        self.map_err(|e| e.with_context(ctx))
    }

    fn with_context<F, S>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        self.map_err(|e| e.with_context(f()))
    }
}

#[cfg(feature = "python")]
impl From<DeltaFusionError> for pyo3::PyErr {
    fn from(err: DeltaFusionError) -> pyo3::PyErr {
        use pyo3::exceptions::*;

        // Map error kinds to appropriate Python exception types
        match err.kind() {
            ErrorKind::NotFound => PyFileNotFoundError::new_err(err.to_string()),
            ErrorKind::AlreadyExists => PyFileExistsError::new_err(err.to_string()),
            ErrorKind::Config => PyValueError::new_err(err.to_string()),
            ErrorKind::Schema => PyTypeError::new_err(err.to_string()),
            ErrorKind::Storage => PyIOError::new_err(err.to_string()),
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}
