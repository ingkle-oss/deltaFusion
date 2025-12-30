//! Time series utilities for direct parquet reading.
//!
//! Provides fast access to time-partitioned data by bypassing Delta log
//! and reading parquet files directly based on partition paths.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::error::{DeltaFusionError, Result};

/// ISO 8601 timestamp formats supported for parsing.
const ISO8601_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.fZ",      // 2024-01-15T10:30:00.123Z
    "%Y-%m-%dT%H:%M:%SZ",         // 2024-01-15T10:30:00Z
    "%Y-%m-%dT%H:%M:%S%.f%:z",    // 2024-01-15T10:30:00.123+09:00
    "%Y-%m-%dT%H:%M:%S%:z",       // 2024-01-15T10:30:00+09:00
    "%Y-%m-%dT%H:%M:%S%.f",       // 2024-01-15T10:30:00.123
    "%Y-%m-%dT%H:%M:%S",          // 2024-01-15T10:30:00
    "%Y-%m-%d %H:%M:%S%.f",       // 2024-01-15 10:30:00.123 (space separator)
    "%Y-%m-%d %H:%M:%S",          // 2024-01-15 10:30:00 (space separator)
    "%Y-%m-%d",                   // 2024-01-15
];

/// Configuration for a time series table.
#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    /// Base path to the data (e.g., "s3://bucket/sensor_data")
    path: String,
    /// Partition column name (e.g., "date")
    partition_col: String,
    /// Partition date format (e.g., "%Y-%m-%d")
    partition_format: String,
    /// Timestamp column name in parquet files (e.g., "timestamp")
    timestamp_col: String,
}

impl TimeSeriesConfig {
    /// Create a new TimeSeriesConfig with default partition format.
    pub fn new(path: &str, partition_col: &str, timestamp_col: &str) -> Self {
        Self {
            path: normalize_path(path),
            partition_col: partition_col.to_string(),
            partition_format: "%Y-%m-%d".to_string(),
            timestamp_col: timestamp_col.to_string(),
        }
    }

    /// Set custom partition format.
    pub fn with_partition_format(mut self, format: &str) -> Self {
        self.partition_format = format.to_string();
        self
    }

    /// Get the base path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the partition column name.
    pub fn partition_col(&self) -> &str {
        &self.partition_col
    }

    /// Get the partition format.
    pub fn partition_format(&self) -> &str {
        &self.partition_format
    }

    /// Get the timestamp column name.
    pub fn timestamp_col(&self) -> &str {
        &self.timestamp_col
    }
}

/// Normalize a path by removing trailing slashes.
fn normalize_path(path: &str) -> String {
    path.trim_end_matches('/').to_string()
}

/// Parsed ISO 8601 timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    inner: NaiveDateTime,
}

impl Timestamp {
    /// Parse an ISO 8601 timestamp string.
    ///
    /// Supported formats:
    /// - `2024-01-15T10:30:00Z` (UTC)
    /// - `2024-01-15T10:30:00+09:00` (with timezone)
    /// - `2024-01-15T10:30:00.123` (with milliseconds)
    /// - `2024-01-15T10:30:00` (local time)
    /// - `2024-01-15` (date only, midnight)
    pub fn parse(s: &str) -> Result<Self> {
        // Try parsing with timezone (returns DateTime<Utc>)
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(Self {
                inner: dt.with_timezone(&Utc).naive_utc(),
            });
        }

        // Try other ISO 8601 formats
        for fmt in ISO8601_FORMATS {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Ok(Self { inner: dt });
            }
            if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
                return Ok(Self {
                    inner: d.and_hms_opt(0, 0, 0).unwrap(),
                });
            }
        }

        Err(DeltaFusionError::InvalidConfig(format!(
            "Invalid ISO 8601 timestamp: '{}'. Expected format like '2024-01-15T10:30:00Z'",
            s
        )))
    }

    /// Get the date part.
    pub fn date(&self) -> NaiveDate {
        self.inner.date()
    }

    /// Get the inner NaiveDateTime.
    pub fn as_naive(&self) -> NaiveDateTime {
        self.inner
    }

    /// Format as ISO 8601 string.
    pub fn to_iso8601(&self) -> String {
        self.inner.format("%Y-%m-%dT%H:%M:%S").to_string()
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_iso8601())
    }
}

/// Parse an ISO 8601 timestamp string.
///
/// This is a convenience function that wraps `Timestamp::parse`.
pub fn parse_timestamp(ts: &str) -> Result<NaiveDateTime> {
    Ok(Timestamp::parse(ts)?.as_naive())
}

/// Generate partition paths for a date range.
///
/// Returns paths in the format: `{base_path}/{partition_col}={date}`
pub fn generate_partition_paths(
    config: &TimeSeriesConfig,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<String> {
    let mut paths = Vec::new();
    let mut current_date = start.date();
    let end_date = end.date();

    while current_date <= end_date {
        let partition_value = current_date.format(&config.partition_format).to_string();
        paths.push(format!(
            "{}/{}={}",
            config.path, config.partition_col, partition_value
        ));

        // Move to next day
        current_date = match current_date.succ_opt() {
            Some(d) => d,
            None => break, // Handle edge case of max date
        };
    }

    paths
}

/// Generate glob patterns for partition paths.
///
/// Returns patterns like: `{base_path}/{partition_col}={date}/*.parquet`
pub fn generate_partition_glob(
    config: &TimeSeriesConfig,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<String> {
    generate_partition_paths(config, start, end)
        .into_iter()
        .map(|p| format!("{}/*.parquet", p))
        .collect()
}
