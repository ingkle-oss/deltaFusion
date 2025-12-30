//! Core DeltaEngine implementation with DataFusion integration.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use deltalake::DeltaTable;

use crate::config::StorageConfig;
use crate::error::{DeltaFusionError, Result};

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
    /// Cached tables: name -> CachedTable
    tables: HashMap<String, CachedTable>,
}

impl DeltaEngine {
    /// Create a new DeltaEngine instance.
    pub fn new() -> Self {
        Self::with_config(StorageConfig::from_env())
    }

    /// Create a new DeltaEngine with custom storage configuration.
    pub fn with_config(storage_config: StorageConfig) -> Self {
        let ctx = SessionContext::new();
        Self {
            ctx,
            storage_config,
            tables: HashMap::new(),
        }
    }

    /// Register a Delta table with the given name.
    ///
    /// The table is cached after loading to avoid repeated log replay.
    /// Use `refresh_table` to reload the table with latest changes.
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
    ///
    /// The table is cached at the specified version.
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
    ///
    /// This reloads the table from storage, picking up any new commits.
    /// If the table was registered at a specific version, it stays at that version.
    pub async fn refresh_table(&mut self, name: &str) -> Result<()> {
        let cached = self
            .tables
            .get(name)
            .ok_or_else(|| DeltaFusionError::TableNotFound(name.to_string()))?;

        let path = cached.path.clone();
        let loaded_version = cached.loaded_version;

        // Deregister old table
        self.ctx.deregister_table(name)?;

        // Reload table
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

    /// Execute a SQL query and return results as Arrow RecordBatches.
    ///
    /// This is the primary query interface, returning zero-copy Arrow data.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Execute a SQL query and return a DataFrame for further processing.
    pub async fn query_df(&self, sql: &str) -> Result<DataFrame> {
        let df = self.ctx.sql(sql).await?;
        Ok(df)
    }

    /// Get table metadata from cache (no I/O if already registered).
    ///
    /// If the table is registered, uses cached metadata.
    /// Otherwise, opens the table fresh (use `register_table` to cache it).
    pub async fn table_info(&self, name_or_path: &str) -> Result<TableInfo> {
        // First check if it's a registered table name
        if let Some(cached) = self.tables.get(name_or_path) {
            return self.extract_table_info(&cached.table, &cached.path);
        }

        // Otherwise treat as path and open fresh (not cached)
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

    /// Extract TableInfo from a DeltaTable.
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

    /// Check if a table is registered.
    pub fn is_registered(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Deregister a table.
    pub fn deregister_table(&mut self, name: &str) -> Result<()> {
        self.ctx.deregister_table(name)?;
        self.tables.remove(name);
        Ok(())
    }

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
}
