//! Time series utilities for direct parquet reading.
//!
//! Provides fast access to time-partitioned data by bypassing Delta log
//! and reading parquet files directly based on partition paths.
//!
//! # Partition Schemes
//!
//! Supports both single and hierarchical partition schemes:
//!
//! - **Single partition**: `date=2024-01-15`
//! - **Hierarchical**: `year=2024/month=01/day=15`
//!
//! # Examples
//!
//! ```rust,ignore
//! // Single partition (default)
//! let config = TimeSeriesConfig::new("/data", "date", "timestamp");
//!
//! // Hierarchical partitions (year/month/day)
//! let config = TimeSeriesConfig::with_partitions(
//!     "/data",
//!     vec!["year", "month", "day"],
//!     vec!["%Y", "%m", "%d"],
//!     "timestamp",
//! );
//! ```

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::error::{DeltaFusionError, Result};

/// ISO 8601 timestamp formats supported for parsing.
const ISO8601_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.fZ",   // 2024-01-15T10:30:00.123Z
    "%Y-%m-%dT%H:%M:%SZ",      // 2024-01-15T10:30:00Z
    "%Y-%m-%dT%H:%M:%S%.f%:z", // 2024-01-15T10:30:00.123+09:00
    "%Y-%m-%dT%H:%M:%S%:z",    // 2024-01-15T10:30:00+09:00
    "%Y-%m-%dT%H:%M:%S%.f",    // 2024-01-15T10:30:00.123
    "%Y-%m-%dT%H:%M:%S",       // 2024-01-15T10:30:00
    "%Y-%m-%d %H:%M:%S%.f",    // 2024-01-15 10:30:00.123 (space separator)
    "%Y-%m-%d %H:%M:%S",       // 2024-01-15 10:30:00 (space separator)
    "%Y-%m-%d",                // 2024-01-15
];

/// Partition granularity for hierarchical partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionGranularity {
    /// Year only (year=2024)
    Year,
    /// Year and month (year=2024/month=01)
    Month,
    /// Year, month, and day (year=2024/month=01/day=15)
    Day,
}

/// Configuration for a time series table.
///
/// Supports both single-column partitions (`date=2024-01-15`) and
/// hierarchical partitions (`year=2024/month=01/day=15`).
#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    /// Base path to the data (e.g., "s3://bucket/sensor_data")
    path: String,
    /// Partition column names (e.g., ["year", "month", "day"] or ["date"])
    partition_cols: Vec<String>,
    /// Partition date formats for each column (e.g., ["%Y", "%m", "%d"] or ["%Y-%m-%d"])
    partition_formats: Vec<String>,
    /// Timestamp column name in parquet files (e.g., "timestamp")
    timestamp_col: String,
}

impl TimeSeriesConfig {
    /// Create a new TimeSeriesConfig with a single partition column.
    ///
    /// Uses default partition format `%Y-%m-%d` for daily partitions.
    ///
    /// # Arguments
    /// * `path` - Base path to the data
    /// * `partition_col` - Single partition column name (e.g., "date")
    /// * `timestamp_col` - Timestamp column name in parquet files
    pub fn new(path: &str, partition_col: &str, timestamp_col: &str) -> Self {
        Self {
            path: normalize_path(path),
            partition_cols: vec![partition_col.to_string()],
            partition_formats: vec!["%Y-%m-%d".to_string()],
            timestamp_col: timestamp_col.to_string(),
        }
    }

    /// Create a TimeSeriesConfig with multiple hierarchical partitions.
    ///
    /// # Arguments
    /// * `path` - Base path to the data
    /// * `partition_cols` - Partition column names (e.g., ["year", "month", "day"])
    /// * `partition_formats` - Corresponding format strings (e.g., ["%Y", "%m", "%d"])
    /// * `timestamp_col` - Timestamp column name in parquet files
    ///
    /// # Panics
    /// Panics if `partition_cols` and `partition_formats` have different lengths.
    pub fn with_partitions(
        path: &str,
        partition_cols: Vec<&str>,
        partition_formats: Vec<&str>,
        timestamp_col: &str,
    ) -> Self {
        assert_eq!(
            partition_cols.len(),
            partition_formats.len(),
            "partition_cols and partition_formats must have the same length"
        );
        Self {
            path: normalize_path(path),
            partition_cols: partition_cols.into_iter().map(String::from).collect(),
            partition_formats: partition_formats.into_iter().map(String::from).collect(),
            timestamp_col: timestamp_col.to_string(),
        }
    }

    /// Create a TimeSeriesConfig for common year/month/day hierarchical partitioning.
    ///
    /// This is a convenience constructor for the common pattern:
    /// `year=2024/month=01/day=15`
    pub fn year_month_day(path: &str, timestamp_col: &str) -> Self {
        Self::with_partitions(
            path,
            vec!["year", "month", "day"],
            vec!["%Y", "%m", "%d"],
            timestamp_col,
        )
    }

    /// Create a TimeSeriesConfig for year/month partitioning.
    ///
    /// Pattern: `year=2024/month=01`
    pub fn year_month(path: &str, timestamp_col: &str) -> Self {
        Self::with_partitions(path, vec!["year", "month"], vec!["%Y", "%m"], timestamp_col)
    }

    /// Set custom partition format (for single partition column).
    ///
    /// Only affects the first partition column.
    pub fn with_partition_format(mut self, format: &str) -> Self {
        if !self.partition_formats.is_empty() {
            self.partition_formats[0] = format.to_string();
        }
        self
    }

    /// Get the base path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the partition column name (first column for backwards compatibility).
    pub fn partition_col(&self) -> &str {
        self.partition_cols
            .first()
            .map(|s| s.as_str())
            .unwrap_or("")
    }

    /// Get all partition column names.
    pub fn partition_cols(&self) -> &[String] {
        &self.partition_cols
    }

    /// Get the partition format (first format for backwards compatibility).
    pub fn partition_format(&self) -> &str {
        self.partition_formats
            .first()
            .map(|s| s.as_str())
            .unwrap_or("")
    }

    /// Get all partition formats.
    pub fn partition_formats(&self) -> &[String] {
        &self.partition_formats
    }

    /// Get the timestamp column name.
    pub fn timestamp_col(&self) -> &str {
        &self.timestamp_col
    }

    /// Check if this config uses hierarchical partitioning.
    pub fn is_hierarchical(&self) -> bool {
        self.partition_cols.len() > 1
    }

    /// Infer partition granularity from partition columns.
    pub fn granularity(&self) -> PartitionGranularity {
        match self.partition_cols.len() {
            1 => PartitionGranularity::Day, // Single col like "date" assumed to be daily
            2 => PartitionGranularity::Month,
            _ => PartitionGranularity::Day,
        }
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
/// For single partition column:
/// - Returns paths like: `{base_path}/date=2024-01-15`
///
/// For hierarchical partitions:
/// - Returns paths like: `{base_path}/year=2024/month=01/day=15`
pub fn generate_partition_paths(
    config: &TimeSeriesConfig,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<String> {
    if config.is_hierarchical() {
        generate_hierarchical_partition_paths(config, start, end)
    } else {
        generate_single_partition_paths(config, start, end)
    }
}

/// Generate paths for single partition column (e.g., date=2024-01-15).
fn generate_single_partition_paths(
    config: &TimeSeriesConfig,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<String> {
    let mut paths = Vec::new();
    let mut current_date = start.date();
    let end_date = end.date();

    while current_date <= end_date {
        let partition_value = current_date.format(config.partition_format()).to_string();
        paths.push(format!(
            "{}/{}={}",
            config.path(),
            config.partition_col(),
            partition_value
        ));

        current_date = match current_date.succ_opt() {
            Some(d) => d,
            None => break,
        };
    }

    paths
}

/// Generate paths for hierarchical partition columns (e.g., year=2024/month=01/day=15).
fn generate_hierarchical_partition_paths(
    config: &TimeSeriesConfig,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<String> {
    let partition_cols = config.partition_cols();
    let partition_formats = config.partition_formats();

    let mut paths = Vec::new();
    let mut current_date = start.date();
    let end_date = end.date();

    // Track unique paths to avoid duplicates (for month-level granularity)
    let mut seen_paths = std::collections::HashSet::new();

    while current_date <= end_date {
        // Build hierarchical path: base_path/col1=val1/col2=val2/...
        let mut path_parts = vec![config.path().to_string()];

        for (col, fmt) in partition_cols.iter().zip(partition_formats.iter()) {
            let value = current_date.format(fmt).to_string();
            path_parts.push(format!("{}={}", col, value));
        }

        let full_path = path_parts.join("/");

        // Only add if we haven't seen this path yet
        if seen_paths.insert(full_path.clone()) {
            paths.push(full_path);
        }

        // Move to next day
        current_date = match current_date.succ_opt() {
            Some(d) => d,
            None => break,
        };
    }

    paths
}

/// Generate unique partition values for a date range.
///
/// Returns a vector of tuples where each tuple contains the partition values
/// for each partition column at the given date.
pub fn generate_partition_values(
    config: &TimeSeriesConfig,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<Vec<String>> {
    let partition_formats = config.partition_formats();
    let mut values = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let mut current_date = start.date();
    let end_date = end.date();

    while current_date <= end_date {
        let row_values: Vec<String> = partition_formats
            .iter()
            .map(|fmt| current_date.format(fmt).to_string())
            .collect();

        let key = row_values.join("/");
        if seen.insert(key) {
            values.push(row_values);
        }

        current_date = match current_date.succ_opt() {
            Some(d) => d,
            None => break,
        };
    }

    values
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_partition_config() {
        let config = TimeSeriesConfig::new("/data", "date", "ts");
        assert_eq!(config.path(), "/data");
        assert_eq!(config.partition_col(), "date");
        assert_eq!(config.partition_format(), "%Y-%m-%d");
        assert_eq!(config.timestamp_col(), "ts");
        assert!(!config.is_hierarchical());
    }

    #[test]
    fn test_hierarchical_partition_config() {
        let config = TimeSeriesConfig::with_partitions(
            "/data",
            vec!["year", "month", "day"],
            vec!["%Y", "%m", "%d"],
            "timestamp",
        );
        assert_eq!(config.partition_cols(), &["year", "month", "day"]);
        assert_eq!(config.partition_formats(), &["%Y", "%m", "%d"]);
        assert!(config.is_hierarchical());
    }

    #[test]
    fn test_year_month_day_convenience() {
        let config = TimeSeriesConfig::year_month_day("/data", "ts");
        assert_eq!(config.partition_cols(), &["year", "month", "day"]);
        assert_eq!(config.partition_formats(), &["%Y", "%m", "%d"]);
    }

    #[test]
    fn test_year_month_convenience() {
        let config = TimeSeriesConfig::year_month("/data", "ts");
        assert_eq!(config.partition_cols(), &["year", "month"]);
        assert_eq!(config.partition_formats(), &["%Y", "%m"]);
    }

    #[test]
    fn test_single_partition_path_generation() {
        let config = TimeSeriesConfig::new("/data", "date", "ts");
        let start = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 17)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let paths = generate_partition_paths(&config, start, end);
        assert_eq!(
            paths,
            vec![
                "/data/date=2024-01-15",
                "/data/date=2024-01-16",
                "/data/date=2024-01-17",
            ]
        );
    }

    #[test]
    fn test_hierarchical_partition_path_generation() {
        let config = TimeSeriesConfig::year_month_day("/data", "ts");
        let start = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 17)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let paths = generate_partition_paths(&config, start, end);
        assert_eq!(
            paths,
            vec![
                "/data/year=2024/month=01/day=15",
                "/data/year=2024/month=01/day=16",
                "/data/year=2024/month=01/day=17",
            ]
        );
    }

    #[test]
    fn test_hierarchical_cross_month_paths() {
        let config = TimeSeriesConfig::year_month_day("/data", "ts");
        let start = NaiveDate::from_ymd_opt(2024, 1, 30)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let paths = generate_partition_paths(&config, start, end);
        assert_eq!(
            paths,
            vec![
                "/data/year=2024/month=01/day=30",
                "/data/year=2024/month=01/day=31",
                "/data/year=2024/month=02/day=01",
                "/data/year=2024/month=02/day=02",
            ]
        );
    }

    #[test]
    fn test_year_month_partition_paths() {
        let config = TimeSeriesConfig::year_month("/data", "ts");
        let start = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let paths = generate_partition_paths(&config, start, end);
        // Should only have 2 unique month paths even though it spans many days
        assert_eq!(
            paths,
            vec!["/data/year=2024/month=01", "/data/year=2024/month=02",]
        );
    }

    #[test]
    fn test_partition_values_generation() {
        let config = TimeSeriesConfig::year_month_day("/data", "ts");
        let start = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 16)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let values = generate_partition_values(&config, start, end);
        assert_eq!(
            values,
            vec![vec!["2024", "01", "15"], vec!["2024", "01", "16"],]
        );
    }

    #[test]
    fn test_timestamp_parsing() {
        assert!(Timestamp::parse("2024-01-15T10:30:00").is_ok());
        assert!(Timestamp::parse("2024-01-15T10:30:00Z").is_ok());
        assert!(Timestamp::parse("2024-01-15").is_ok());
        assert!(Timestamp::parse("invalid").is_err());
    }

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/data/"), "/data");
        assert_eq!(normalize_path("/data///"), "/data");
        assert_eq!(normalize_path("/data"), "/data");
    }
}
