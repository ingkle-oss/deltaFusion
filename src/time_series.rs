//! Time series utilities for direct parquet reading.
//!
//! Provides fast access to time-partitioned data by bypassing Delta log
//! and reading parquet files directly based on partition paths.

use chrono::{NaiveDate, NaiveDateTime};

use crate::error::{DeltaFusionError, Result};

/// Configuration for a time series table.
#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    /// Base path to the data (e.g., "s3://bucket/sensor_data")
    pub path: String,
    /// Partition column name (e.g., "dt")
    pub partition_col: String,
    /// Partition date format (e.g., "%Y-%m-%d")
    pub partition_format: String,
    /// Timestamp column name in parquet files (e.g., "timestamp")
    pub timestamp_col: String,
}

impl TimeSeriesConfig {
    pub fn new(path: &str, partition_col: &str, timestamp_col: &str) -> Self {
        Self {
            path: path.trim_end_matches('/').to_string(),
            partition_col: partition_col.to_string(),
            partition_format: "%Y-%m-%d".to_string(),
            timestamp_col: timestamp_col.to_string(),
        }
    }

    pub fn with_partition_format(mut self, format: &str) -> Self {
        self.partition_format = format.to_string();
        self
    }
}

/// Parse a timestamp string into NaiveDateTime.
/// Supports multiple formats.
pub fn parse_timestamp(ts: &str) -> Result<NaiveDateTime> {
    // Try ISO 8601 formats
    let formats = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];

    for fmt in formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(ts, fmt) {
            return Ok(dt);
        }
        // Try parsing as date only
        if let Ok(d) = NaiveDate::parse_from_str(ts, fmt) {
            return Ok(d.and_hms_opt(0, 0, 0).unwrap());
        }
    }

    Err(DeltaFusionError::InvalidConfig(format!(
        "Cannot parse timestamp: {}",
        ts
    )))
}

/// Generate partition paths for a date range.
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
        let path = format!(
            "{}/{}={}",
            config.path, config.partition_col, partition_value
        );
        paths.push(path);
        current_date = current_date.succ_opt().unwrap_or(current_date);
    }

    paths
}

/// Generate a glob pattern for partition paths.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let ts = parse_timestamp("2024-01-15T10:30:00").unwrap();
        assert_eq!(ts.date().to_string(), "2024-01-15");
        assert_eq!(ts.time().hour(), 10);
        assert_eq!(ts.time().minute(), 30);
    }

    #[test]
    fn test_parse_timestamp_date_only() {
        let ts = parse_timestamp("2024-01-15").unwrap();
        assert_eq!(ts.date().to_string(), "2024-01-15");
        assert_eq!(ts.time().hour(), 0);
    }

    #[test]
    fn test_generate_partition_paths() {
        let config = TimeSeriesConfig::new("s3://bucket/data", "dt", "timestamp");
        let start = parse_timestamp("2024-01-15T10:00:00").unwrap();
        let end = parse_timestamp("2024-01-17T14:00:00").unwrap();

        let paths = generate_partition_paths(&config, start, end);
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0], "s3://bucket/data/dt=2024-01-15");
        assert_eq!(paths[1], "s3://bucket/data/dt=2024-01-16");
        assert_eq!(paths[2], "s3://bucket/data/dt=2024-01-17");
    }
}
