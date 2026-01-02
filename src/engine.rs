//! Core DeltaEngine implementation with DataFusion integration.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use deltalake::kernel::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
use deltalake::operations::write::SchemaMode;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;

use crate::config::StorageConfig;
use crate::error::{DeltaFusionError, Result};
use crate::time_series::{generate_partition_paths, parse_timestamp, TimeSeriesConfig};

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

/// Engine configuration options.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Number of partitions for parallel query execution.
    /// Lower values reduce CPU usage but may slow down large queries.
    /// Default: number of CPU cores
    pub target_partitions: usize,
    /// Batch size for query execution.
    /// Default: 8192
    pub batch_size: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            target_partitions: num_cpus::get(),
            batch_size: 8192,
        }
    }
}

impl EngineConfig {
    /// Create config optimized for low CPU usage.
    /// Uses fewer partitions (max 4) to reduce parallelism overhead.
    pub fn low_cpu() -> Self {
        Self {
            target_partitions: num_cpus::get().min(4),
            batch_size: 8192,
        }
    }

    /// Create config optimized for small datasets.
    /// Uses single partition to minimize overhead.
    pub fn single_threaded() -> Self {
        Self {
            target_partitions: 1,
            batch_size: 8192,
        }
    }

    /// Create config with custom partition count.
    pub fn with_partitions(partitions: usize) -> Self {
        Self {
            target_partitions: partitions.max(1),
            batch_size: 8192,
        }
    }
}

/// DeltaEngine provides SQL query capabilities over Delta Lake tables.
///
/// Uses DataFusion as the query engine with zero-copy Arrow data transfer.
/// Tables are cached after registration to avoid repeated log replay.
pub struct DeltaEngine {
    ctx: SessionContext,
    storage_config: StorageConfig,
    engine_config: EngineConfig,
    /// Cached Delta tables: name -> CachedTable
    tables: HashMap<String, CachedTable>,
    /// Time series configurations: name -> TimeSeriesConfig
    time_series: HashMap<String, TimeSeriesConfig>,
}

impl DeltaEngine {
    /// Create a new DeltaEngine instance.
    pub fn new() -> Self {
        Self::with_configs(StorageConfig::from_env(), EngineConfig::default())
    }

    /// Create a new DeltaEngine with custom storage configuration.
    pub fn with_config(storage_config: StorageConfig) -> Self {
        Self::with_configs(storage_config, EngineConfig::default())
    }

    /// Create a new DeltaEngine with custom storage and engine configuration.
    pub fn with_configs(storage_config: StorageConfig, engine_config: EngineConfig) -> Self {
        let config = SessionConfig::new()
            .with_target_partitions(engine_config.target_partitions)
            .with_batch_size(engine_config.batch_size);
        let ctx = SessionContext::new_with_config(config);

        Self {
            ctx,
            storage_config,
            engine_config,
            tables: HashMap::new(),
            time_series: HashMap::new(),
        }
    }

    /// Get current engine configuration.
    pub fn engine_config(&self) -> &EngineConfig {
        &self.engine_config
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

    /// Register a time series with hierarchical partitions (e.g., year/month/day).
    ///
    /// This enables reading data partitioned like: `year=2024/month=01/day=15`
    ///
    /// # Arguments
    /// * `name` - Name for the time series
    /// * `path` - Base path to the data
    /// * `timestamp_col` - Timestamp column name in parquet files
    /// * `partition_cols` - Partition column names (e.g., ["year", "month", "day"])
    /// * `partition_formats` - Format strings for each partition column (e.g., ["%Y", "%m", "%d"])
    pub fn register_time_series_hierarchical(
        &mut self,
        name: &str,
        path: &str,
        timestamp_col: &str,
        partition_cols: &[&str],
        partition_formats: &[&str],
    ) {
        let config = TimeSeriesConfig::with_partitions(
            path,
            partition_cols.to_vec(),
            partition_formats.to_vec(),
            timestamp_col,
        );
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

        // Generate partition paths (directories like /data/date=2024-01-15)
        let partition_paths = generate_partition_paths(config, start_ts, end_ts);

        if partition_paths.is_empty() {
            return Ok(vec![]);
        }

        // Filter to only existing directories that contain parquet files
        let valid_partitions = filter_valid_partition_dirs(&partition_paths)?;

        if valid_partitions.is_empty() {
            return Ok(vec![]);
        }

        // Create a temporary session for this query
        let ctx = self.create_session_with_storage().await?;

        // Register partition DIRECTORIES (not individual files)
        // DataFusion handles multiple files within each directory efficiently
        let timestamp_col = config.timestamp_col();
        self.read_parquet_files_with_filter(&ctx, &valid_partitions, timestamp_col, start, end)
            .await
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

        let partition_paths = generate_partition_paths(&config, start_ts, end_ts);

        if partition_paths.is_empty() {
            return Ok(vec![]);
        }

        // Filter to only existing directories that contain parquet files
        let valid_partitions = filter_valid_partition_dirs(&partition_paths)?;

        if valid_partitions.is_empty() {
            return Ok(vec![]);
        }

        let ctx = self.create_session_with_storage().await?;

        self.read_parquet_files_with_filter(&ctx, &valid_partitions, timestamp_col, start, end)
            .await
    }

    /// Read parquet files with timestamp filter.
    ///
    /// Registers partition DIRECTORIES (not individual files) as tables.
    /// DataFusion handles multiple files within each directory efficiently.
    async fn read_parquet_files_with_filter(
        &self,
        ctx: &SessionContext,
        partition_dirs: &[String],
        timestamp_col: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<RecordBatch>> {
        if partition_dirs.is_empty() {
            return Ok(vec![]);
        }

        // Register each partition DIRECTORY as a table
        // DataFusion will discover and read all parquet files in each directory
        for (i, dir_path) in partition_dirs.iter().enumerate() {
            let table_name = format!("_ts_part_{i}");
            ctx.register_parquet(&table_name, dir_path, Default::default())
                .await?;
        }

        // Build UNION ALL query - now only N partitions (days), not N files
        let mut union_parts = Vec::new();
        for i in 0..partition_dirs.len() {
            union_parts.push(format!(
                "SELECT * FROM _ts_part_{i} WHERE {timestamp_col} >= '{start}' AND {timestamp_col} < '{end}'"
            ));
        }

        let sql = union_parts.join(" UNION ALL ");
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        Ok(batches)
    }

    /// Create a new session context with storage options.
    async fn create_session_with_storage(&self) -> Result<SessionContext> {
        let config = SessionConfig::new()
            .with_target_partitions(self.engine_config.target_partitions)
            .with_batch_size(self.engine_config.batch_size);

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
            schema: format!("{schema:?}"),
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
        self.write_with_options(path, batches, mode, partition_columns, None)
            .await
    }

    /// Write RecordBatches with schema evolution options.
    ///
    /// # Arguments
    /// * `path` - Path to the Delta table
    /// * `batches` - Data to write
    /// * `mode` - Write mode (Append or Overwrite)
    /// * `partition_columns` - Optional partition columns (only used when creating new table)
    /// * `schema_mode` - Schema mode: "merge" to add new columns, "overwrite" to replace schema
    ///
    /// # Returns
    /// The updated DeltaTable
    pub async fn write_with_options(
        &self,
        path: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
        partition_columns: Option<Vec<String>>,
        schema_mode: Option<String>,
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

        // Apply schema mode if specified
        if let Some(ref mode_str) = schema_mode {
            let schema_mode = match mode_str.to_lowercase().as_str() {
                "merge" => SchemaMode::Merge,
                "overwrite" => SchemaMode::Overwrite,
                _ => {
                    return Err(DeltaFusionError::InvalidConfig(format!(
                        "Invalid schema_mode: '{mode_str}'. Use 'merge' or 'overwrite'"
                    )))
                }
            };
            builder = builder.with_schema_mode(schema_mode);
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
        ArrowDataType::Date32 | ArrowDataType::Date64 => DataType::Primitive(PrimitiveType::Date),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            DataType::Primitive(PrimitiveType::TimestampNtz)
        }
        ArrowDataType::Timestamp(_, _) => DataType::Primitive(PrimitiveType::TimestampNtz),
        ArrowDataType::Decimal128(precision, scale) => DataType::decimal(*precision, *scale as u8)
            .map_err(|e| DeltaFusionError::Schema(format!("Invalid decimal: {e}")))?,
        ArrowDataType::Decimal256(precision, scale) => DataType::decimal(*precision, *scale as u8)
            .map_err(|e| DeltaFusionError::Schema(format!("Invalid decimal: {e}")))?,
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
                "Unsupported map type: {arrow_type:?}"
            )));
        }
        _ => {
            return Err(DeltaFusionError::Schema(format!(
                "Unsupported Arrow type: {arrow_type:?}"
            )));
        }
    };

    Ok(delta_type)
}

/// Filter partition directories to only include those that exist and contain parquet files.
///
/// Returns the directory paths (not individual files) for DataFusion to process.
/// DataFusion's register_parquet can accept a directory and will discover all parquet files.
fn filter_valid_partition_dirs(partition_paths: &[String]) -> Result<Vec<String>> {
    let mut valid_dirs = Vec::new();

    for partition_path in partition_paths {
        // Check if it's a cloud storage path
        if partition_path.starts_with("s3://")
            || partition_path.starts_with("gs://")
            || partition_path.starts_with("az://")
        {
            // For cloud storage, assume the path is valid
            // DataFusion will handle discovery
            valid_dirs.push(partition_path.clone());
        } else {
            // Local filesystem - check if directory exists and has parquet files
            let path = Path::new(partition_path);
            if path.exists() && path.is_dir() {
                // Check if there's at least one parquet file
                let has_parquet = std::fs::read_dir(path)
                    .map(|entries| {
                        entries
                            .flatten()
                            .any(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
                    })
                    .unwrap_or(false);

                if has_parquet {
                    valid_dirs.push(partition_path.clone());
                }
            }
        }
    }

    Ok(valid_dirs)
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
