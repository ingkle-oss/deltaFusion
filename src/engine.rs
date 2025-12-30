//! Core DeltaEngine implementation with DataFusion integration.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use deltalake::DeltaTable;

use crate::config::StorageConfig;
use crate::error::Result;

/// DeltaEngine provides SQL query capabilities over Delta Lake tables.
///
/// Uses DataFusion as the query engine with zero-copy Arrow data transfer.
pub struct DeltaEngine {
    ctx: SessionContext,
    storage_config: StorageConfig,
    registered_tables: HashMap<String, String>,
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
            registered_tables: HashMap::new(),
        }
    }

    /// Register a Delta table with the given name.
    ///
    /// The table can then be queried using SQL with this name.
    pub async fn register_table(&mut self, name: &str, path: &str) -> Result<()> {
        let table = self.open_delta_table(path).await?;
        self.ctx.register_table(name, Arc::new(table))?;
        self.registered_tables
            .insert(name.to_string(), path.to_string());
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
        self.ctx.register_table(name, Arc::new(table))?;
        self.registered_tables
            .insert(name.to_string(), path.to_string());
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

    /// Get table metadata including schema and version.
    pub async fn table_info(&self, path: &str) -> Result<TableInfo> {
        let table = self.open_delta_table(path).await?;

        let version = table.version();
        let schema = table.get_schema()?.clone();
        let num_files = table.get_files_count();

        // Get partition columns from metadata
        let partition_columns = table
            .metadata()
            .map(|m| m.partition_columns.clone())
            .unwrap_or_default();

        Ok(TableInfo {
            version,
            schema: format!("{:?}", schema),
            num_files,
            partition_columns,
        })
    }

    /// List all registered table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.registered_tables.keys().cloned().collect()
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
}
