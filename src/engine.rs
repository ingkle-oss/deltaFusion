//! Core DeltaEngine implementation with DataFusion integration.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use deltalake::kernel::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;

use crate::config::StorageConfig;
use crate::error::{DeltaFusionError, Result};
use crate::time_series::{generate_partition_glob, parse_timestamp, TimeSeriesConfig};

/// Write mode for delta operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Append data to the table.
    Append,
    /// Overwrite existing data.
    Overwrite,
    /// Error if table exists (for create operations).
    ErrorIfExists,
    /// Ignore if table exists (for create operations).
    Ignore,
}

/// Cached table entry with metadata.
struct CachedTable {
    /// The Delta table instance.
    table: Arc<DeltaTable>,
    /// Original path for refresh.
    path: String,
    /// Version when loaded (None if loaded at latest).
    loaded_version: Option<i64>,
}

/// DeltaEngine provides SQL query capabilities over Delta Lake tables.
///
/// Uses DataFusion as the query engine with zero-copy Arrow data transfer.
/// Tables are cached after registration to avoid repeated log replay.
pub struct DeltaEngine {
    ctx: SessionContext,
    storage_config: StorageConfig,
    /// Cached Delta tables: name -> CachedTable
    tables: HashMap<String, CachedTable>,
    /// Time series configurations: name -> TimeSeriesConfig
    time_series: HashMap<String, TimeSeriesConfig>,
}

impl DeltaEngine {
    /// Create a new DeltaEngine instance.
    pub fn new() -> Self {
        Self::with_config(StorageConfig::from_env())
    }

    /// Create a new DeltaEngine with custom storage configuration.
    pub fn with_config(storage_config: StorageConfig) -> Self {
        let config = SessionConfig::new()
            .with_target_partitions(num_cpus::get())
            .with_batch_size(8192);
        let ctx = SessionContext::new_with_config(config);

        Self {
            ctx,
            storage_config,
            tables: HashMap::new(),
            time_series: HashMap::new(),
        }
    }

    // ========================================================================
    // Delta Table Methods (existing)
    // ========================================================================

    /// Register a Delta table with the given name.
    pub async fn register_table(&mut self, name: &str, path: &str) -> Result<()> {
        let table = self.open_delta_table(path).await?;
        let arc_table = Arc::new(table);

        self.ctx.register_table(name, arc_table.clone())?;

        self.tables.insert(
            name.to_string(),
            CachedTable {
                table: arc_table,
                path: path.to_string(),
                loaded_version: None,
            },
        );

        Ok(())
    }

    /// Register a Delta table at a specific version.
    pub async fn register_table_with_version(
        &mut self,
        name: &str,
        path: &str,
        version: i64,
    ) -> Result<()> {
        let mut table = self.open_delta_table(path).await?;
        table.load_version(version).await?;
        let arc_table = Arc::new(table);

        self.ctx.register_table(name, arc_table.clone())?;

        self.tables.insert(
            name.to_string(),
            CachedTable {
                table: arc_table,
                path: path.to_string(),
                loaded_version: Some(version),
            },
        );

        Ok(())
    }

    /// Refresh a registered table to load latest changes.
    pub async fn refresh_table(&mut self, name: &str) -> Result<()> {
        let cached = self
            .tables
            .get(name)
            .ok_or_else(|| DeltaFusionError::TableNotFound(name.to_string()))?;

        let path = cached.path.clone();
        let loaded_version = cached.loaded_version;

        self.ctx.deregister_table(name)?;

        let mut table = self.open_delta_table(&path).await?;
        if let Some(version) = loaded_version {
            table.load_version(version).await?;
        }
        let arc_table = Arc::new(table);

        self.ctx.register_table(name, arc_table.clone())?;

        self.tables.insert(
            name.to_string(),
            CachedTable {
                table: arc_table,
                path,
                loaded_version,
            },
        );

        Ok(())
    }

    /// Refresh all registered tables.
    pub async fn refresh_all(&mut self) -> Result<()> {
        let names: Vec<String> = self.tables.keys().cloned().collect();
        for name in names {
            self.refresh_table(&name).await?;
        }
        Ok(())
    }

    // ========================================================================
    // Time Series Methods (new - Delta log 스킵)
    // ========================================================================

    /// Register a time series table configuration.
    ///
    /// This does NOT read any data - it just stores the configuration.
    /// Use `read_time_range` to actually query data.
    pub fn register_time_series(
        &mut self,
        name: &str,
        path: &str,
        partition_col: &str,
        timestamp_col: &str,
    ) {
        let config = TimeSeriesConfig::new(path, partition_col, timestamp_col);
        self.time_series.insert(name.to_string(), config);
    }

    /// Register a time series with custom partition format.
    pub fn register_time_series_with_format(
        &mut self,
        name: &str,
        path: &str,
        partition_col: &str,
        timestamp_col: &str,
        partition_format: &str,
    ) {
        let config = TimeSeriesConfig::new(path, partition_col, timestamp_col)
            .with_partition_format(partition_format);
        self.time_series.insert(name.to_string(), config);
    }

    /// Read time range data directly from parquet files.
    ///
    /// This bypasses Delta log entirely for maximum performance.
    /// Partition pruning is done based on date, then DataFusion
    /// applies row group pruning and predicate pushdown.
    pub async fn read_time_range(
        &self,
        name: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<RecordBatch>> {
        let config = self
            .time_series
            .get(name)
            .ok_or_else(|| DeltaFusionError::TableNotFound(name.to_string()))?;

        let start_ts = parse_timestamp(start)?;
        let end_ts = parse_timestamp(end)?;

        // Generate partition paths
        let globs = generate_partition_glob(config, start_ts, end_ts);

        if globs.is_empty() {
            return Ok(vec![]);
        }

        // Create a temporary session for this query
        let ctx = self.create_session_with_storage().await?;

        // Register parquet files
        let options = ListingOptions::new(Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
        ))
        .with_file_extension(".parquet");

        // Try to register all paths at once first
        let table_paths: Vec<_> = globs.iter().map(|g| g.as_str()).collect();
        let combined_path = table_paths.join(",");

        let registration_result = ctx
            .register_listing_table("_ts_temp", &combined_path, options.clone(), None, None)
            .await;

        if registration_result.is_err() {
            // If combined registration fails, try registering paths individually
            for (i, glob) in globs.iter().enumerate() {
                let name = format!("_ts_part_{}", i);
                let _ = ctx
                    .register_listing_table(&name, glob, options.clone(), None, None)
                    .await;
            }
        }

        // Build query with timestamp filter
        let batches = if registration_result.is_ok() {
            // Combined table registered successfully
            let sql = format!(
                "SELECT * FROM _ts_temp WHERE {} >= '{}' AND {} < '{}'",
                config.timestamp_col, start, config.timestamp_col, end
            );
            let df = ctx.sql(&sql).await?;
            df.collect().await?
        } else {
            // Use union of individual partitions
            let partition_count = globs.len();
            let mut union_sql = String::new();
            for i in 0..partition_count {
                if i > 0 {
                    union_sql.push_str(" UNION ALL ");
                }
                union_sql.push_str(&format!(
                    "SELECT * FROM _ts_part_{} WHERE {} >= '{}' AND {} < '{}'",
                    i, config.timestamp_col, start, config.timestamp_col, end
                ));
            }
            match ctx.sql(&union_sql).await {
                Ok(df) => df.collect().await?,
                Err(_) => vec![], // No partitions found
            }
        };

        Ok(batches)
    }

    /// Read time range directly from a path (without pre-registration).
    pub async fn read_time_range_direct(
        &self,
        path: &str,
        partition_col: &str,
        timestamp_col: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<RecordBatch>> {
        let config = TimeSeriesConfig::new(path, partition_col, timestamp_col);

        let start_ts = parse_timestamp(start)?;
        let end_ts = parse_timestamp(end)?;

        let globs = generate_partition_glob(&config, start_ts, end_ts);

        if globs.is_empty() {
            return Ok(vec![]);
        }

        let ctx = self.create_session_with_storage().await?;

        // Register all partition paths
        for (i, glob) in globs.iter().enumerate() {
            let options = ListingOptions::new(Arc::new(
                datafusion::datasource::file_format::parquet::ParquetFormat::default(),
            ))
            .with_file_extension(".parquet");

            let table_name = format!("_part_{}", i);
            if ctx
                .register_listing_table(&table_name, glob, options, None, None)
                .await
                .is_err()
            {
                // Skip partitions that don't exist
                continue;
            }
        }

        // Union all partitions
        let partition_count = globs.len();
        if partition_count == 0 {
            return Ok(vec![]);
        }

        let mut union_sql = String::new();
        for i in 0..partition_count {
            if i > 0 {
                union_sql.push_str(" UNION ALL ");
            }
            union_sql.push_str(&format!(
                "SELECT * FROM _part_{} WHERE {} >= '{}' AND {} < '{}'",
                i, timestamp_col, start, timestamp_col, end
            ));
        }

        // Try the union query, fall back to simpler approach if needed
        let df = match ctx.sql(&union_sql).await {
            Ok(df) => df,
            Err(_) => {
                // Fallback: try first available partition
                for i in 0..partition_count {
                    let sql = format!(
                        "SELECT * FROM _part_{} WHERE {} >= '{}' AND {} < '{}'",
                        i, timestamp_col, start, timestamp_col, end
                    );
                    if let Ok(df) = ctx.sql(&sql).await {
                        return Ok(df.collect().await?);
                    }
                }
                return Ok(vec![]);
            }
        };

        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Create a new session context with storage options.
    async fn create_session_with_storage(&self) -> Result<SessionContext> {
        let config = SessionConfig::new()
            .with_target_partitions(num_cpus::get())
            .with_batch_size(8192);

        let ctx = SessionContext::new_with_config(config);

        // Register object store if needed
        let storage_opts = self.storage_config.to_storage_options();
        if !storage_opts.is_empty() {
            // For S3, we need to configure the runtime
            // This is handled by DataFusion's built-in S3 support
            // when we use s3:// URLs
        }

        Ok(ctx)
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a SQL query and return results as Arrow RecordBatches.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Execute a SQL query and return a DataFrame.
    pub async fn query_df(&self, sql: &str) -> Result<DataFrame> {
        let df = self.ctx.sql(sql).await?;
        Ok(df)
    }

    // ========================================================================
    // Metadata Methods
    // ========================================================================

    /// Get table metadata from cache.
    pub async fn table_info(&self, name_or_path: &str) -> Result<TableInfo> {
        if let Some(cached) = self.tables.get(name_or_path) {
            return self.extract_table_info(&cached.table, &cached.path);
        }

        let table = self.open_delta_table(name_or_path).await?;
        self.extract_table_info(&table, name_or_path)
    }

    /// Get info for a registered table by name.
    pub fn registered_table_info(&self, name: &str) -> Result<TableInfo> {
        let cached = self
            .tables
            .get(name)
            .ok_or_else(|| DeltaFusionError::TableNotFound(name.to_string()))?;

        self.extract_table_info(&cached.table, &cached.path)
    }

    fn extract_table_info(&self, table: &DeltaTable, path: &str) -> Result<TableInfo> {
        let version = table.version();
        let schema = table.get_schema()?.clone();
        let num_files = table.get_files_count();

        let partition_columns = table
            .metadata()
            .map(|m| m.partition_columns.clone())
            .unwrap_or_default();

        Ok(TableInfo {
            path: path.to_string(),
            version,
            schema: format!("{:?}", schema),
            num_files,
            partition_columns,
        })
    }

    /// List all registered table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// List all registered time series names.
    pub fn list_time_series(&self) -> Vec<String> {
        self.time_series.keys().cloned().collect()
    }

    /// Check if a table is registered.
    pub fn is_registered(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Check if a time series is registered.
    pub fn is_time_series_registered(&self, name: &str) -> bool {
        self.time_series.contains_key(name)
    }

    /// Deregister a table.
    pub fn deregister_table(&mut self, name: &str) -> Result<()> {
        self.ctx.deregister_table(name)?;
        self.tables.remove(name);
        Ok(())
    }

    /// Deregister a time series.
    pub fn deregister_time_series(&mut self, name: &str) {
        self.time_series.remove(name);
    }

    // ========================================================================
    // Write Methods
    // ========================================================================

    /// Create a new Delta table at the specified path.
    ///
    /// # Arguments
    /// * `path` - Path where the table will be created
    /// * `schema` - Arrow schema for the table
    /// * `partition_columns` - Optional list of partition column names
    ///
    /// # Returns
    /// The created DeltaTable
    pub async fn create_table(
        &self,
        path: &str,
        schema: &ArrowSchema,
        partition_columns: Option<Vec<String>>,
    ) -> Result<DeltaTable> {
        let storage_options = self.storage_config.to_storage_options();

        // Convert Arrow schema to Delta schema
        let delta_schema = arrow_schema_to_delta(schema)?;

        let mut builder = DeltaOps::try_from_uri_with_storage_options(path, storage_options)
            .await?
            .create()
            .with_columns(delta_schema.fields().cloned());

        if let Some(cols) = partition_columns {
            builder = builder.with_partition_columns(cols);
        }

        let table = builder.with_save_mode(SaveMode::ErrorIfExists).await?;

        Ok(table)
    }

    /// Write RecordBatches to a Delta table at the specified path.
    ///
    /// # Arguments
    /// * `path` - Path to the Delta table
    /// * `batches` - Data to write
    /// * `mode` - Write mode (Append or Overwrite)
    /// * `partition_columns` - Optional partition columns (only used when creating new table)
    ///
    /// # Returns
    /// The updated DeltaTable
    pub async fn write(
        &self,
        path: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
        partition_columns: Option<Vec<String>>,
    ) -> Result<DeltaTable> {
        if batches.is_empty() {
            return Err(DeltaFusionError::Write("No data to write".to_string()));
        }

        let storage_options = self.storage_config.to_storage_options();
        let save_mode = match mode {
            WriteMode::Append => SaveMode::Append,
            WriteMode::Overwrite => SaveMode::Overwrite,
            WriteMode::ErrorIfExists => SaveMode::ErrorIfExists,
            WriteMode::Ignore => SaveMode::Ignore,
        };

        let mut builder = DeltaOps::try_from_uri_with_storage_options(path, storage_options)
            .await?
            .write(batches)
            .with_save_mode(save_mode);

        if let Some(cols) = partition_columns {
            builder = builder.with_partition_columns(cols);
        }

        let table = builder.await?;
        Ok(table)
    }

    /// Write to a registered table by name.
    ///
    /// # Arguments
    /// * `name` - Registered table name
    /// * `batches` - Data to write
    /// * `mode` - Write mode (Append or Overwrite)
    pub async fn write_to_table(
        &mut self,
        name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        let cached = self
            .tables
            .get(name)
            .ok_or_else(|| DeltaFusionError::TableNotFound(name.to_string()))?;

        let path = cached.path.clone();
        let _ = self.write(&path, batches, mode, None).await?;

        // Refresh the table cache after write
        self.refresh_table(name).await?;

        Ok(())
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /// Open a Delta table with storage options.
    async fn open_delta_table(&self, path: &str) -> Result<DeltaTable> {
        let storage_options = self.storage_config.to_storage_options();

        let table = if storage_options.is_empty() {
            deltalake::open_table(path).await?
        } else {
            deltalake::DeltaTableBuilder::from_uri(path)
                .with_storage_options(storage_options)
                .load()
                .await?
        };

        Ok(table)
    }
}

impl Default for DeltaEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert Arrow schema to Delta schema.
fn arrow_schema_to_delta(schema: &ArrowSchema) -> Result<StructType> {
    let fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            let delta_type = arrow_type_to_delta(field.data_type())?;
            Ok(StructField::new(
                field.name().clone(),
                delta_type,
                field.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StructType::new(fields))
}

/// Convert Arrow data type to Delta data type.
fn arrow_type_to_delta(arrow_type: &arrow::datatypes::DataType) -> Result<DataType> {
    use arrow::datatypes::DataType as ArrowDataType;
    use arrow::datatypes::TimeUnit;

    let delta_type = match arrow_type {
        // Primitive types
        ArrowDataType::Boolean => DataType::Primitive(PrimitiveType::Boolean),
        ArrowDataType::Int8 => DataType::Primitive(PrimitiveType::Byte),
        ArrowDataType::Int16 => DataType::Primitive(PrimitiveType::Short),
        ArrowDataType::Int32 => DataType::Primitive(PrimitiveType::Integer),
        ArrowDataType::Int64 => DataType::Primitive(PrimitiveType::Long),
        ArrowDataType::UInt8 => DataType::Primitive(PrimitiveType::Byte),
        ArrowDataType::UInt16 => DataType::Primitive(PrimitiveType::Short),
        ArrowDataType::UInt32 => DataType::Primitive(PrimitiveType::Integer),
        ArrowDataType::UInt64 => DataType::Primitive(PrimitiveType::Long),
        ArrowDataType::Float32 => DataType::Primitive(PrimitiveType::Float),
        ArrowDataType::Float64 => DataType::Primitive(PrimitiveType::Double),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            DataType::Primitive(PrimitiveType::String)
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            DataType::Primitive(PrimitiveType::Binary)
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => {
            DataType::Primitive(PrimitiveType::Date)
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, tz) => {
            if tz.is_some() {
                DataType::Primitive(PrimitiveType::TimestampNtz)
            } else {
                DataType::Primitive(PrimitiveType::TimestampNtz)
            }
        }
        ArrowDataType::Timestamp(_, _) => DataType::Primitive(PrimitiveType::TimestampNtz),
        ArrowDataType::Decimal128(precision, scale) => {
            DataType::decimal(*precision, *scale as u8).map_err(|e| {
                DeltaFusionError::Schema(format!("Invalid decimal: {}", e))
            })?
        }
        ArrowDataType::Decimal256(precision, scale) => {
            DataType::decimal(*precision, *scale as u8).map_err(|e| {
                DeltaFusionError::Schema(format!("Invalid decimal: {}", e))
            })?
        }
        // Complex types
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let element_type = arrow_type_to_delta(field.data_type())?;
            DataType::Array(Box::new(ArrayType::new(element_type, field.is_nullable())))
        }
        ArrowDataType::Struct(fields) => {
            let delta_fields: Vec<StructField> = fields
                .iter()
                .map(|f| {
                    let dt = arrow_type_to_delta(f.data_type())?;
                    Ok(StructField::new(f.name().clone(), dt, f.is_nullable()))
                })
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(Box::new(StructType::new(delta_fields)))
        }
        ArrowDataType::Map(field, _) => {
            if let ArrowDataType::Struct(fields) = field.data_type() {
                if fields.len() == 2 {
                    let key_type = arrow_type_to_delta(fields[0].data_type())?;
                    let value_type = arrow_type_to_delta(fields[1].data_type())?;
                    return Ok(DataType::Map(Box::new(MapType::new(
                        key_type,
                        value_type,
                        fields[1].is_nullable(),
                    ))));
                }
            }
            return Err(DeltaFusionError::Schema(format!(
                "Unsupported map type: {:?}",
                arrow_type
            )));
        }
        _ => {
            return Err(DeltaFusionError::Schema(format!(
                "Unsupported Arrow type: {:?}",
                arrow_type
            )));
        }
    };

    Ok(delta_type)
}

/// Information about a Delta table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub path: String,
    pub version: i64,
    pub schema: String,
    pub num_files: usize,
    pub partition_columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = DeltaEngine::new();
        assert!(engine.list_tables().is_empty());
    }

    #[tokio::test]
    async fn test_is_registered() {
        let engine = DeltaEngine::new();
        assert!(!engine.is_registered("nonexistent"));
    }

    #[test]
    fn test_register_time_series() {
        let mut engine = DeltaEngine::new();
        engine.register_time_series("sensor", "s3://bucket/data", "dt", "timestamp");
        assert!(engine.is_time_series_registered("sensor"));
        assert!(!engine.is_time_series_registered("other"));
    }
}
