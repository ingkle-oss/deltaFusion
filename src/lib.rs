//! delta_fusion - High-performance Delta Lake query engine for Python.
//!
//! This library provides a Python interface to query Delta Lake tables using
//! DataFusion as the query engine. Data is transferred via zero-copy Arrow
//! for maximum performance.
//!
//! # Architecture
//!
//! - `config`: Storage configuration (S3, local filesystem)
//! - `error`: Error types with automatic Python exception conversion
//! - `engine`: Core DeltaEngine with DataFusion SessionContext
//! - `python`: PyO3 bindings with GIL release for async operations
//!
//! # Key Features
//!
//! - SQL queries over Delta Lake tables
//! - Zero-copy Arrow data transfer to Python
//! - S3 and local filesystem support
//! - Time travel queries via version specification
//! - GIL release during async operations

pub mod config;
pub mod engine;
pub mod error;
pub mod time_series;

#[cfg(feature = "python")]
mod python;

pub use config::{StorageConfig, StorageConfigBuilder};
pub use engine::{DeltaEngine, EngineConfig, TableInfo, WriteMode};
pub use error::{DeltaFusionError, ErrorKind, Result, ResultExt};

// Re-export PyO3 module for maturin
#[cfg(feature = "python")]
pub use python::*;
