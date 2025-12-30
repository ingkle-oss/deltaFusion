//! Configuration for delta_fusion.

use std::collections::HashMap;
use std::env;

/// Configuration for Delta Lake storage backends.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    /// AWS access key ID
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key
    pub aws_secret_access_key: Option<String>,
    /// AWS region
    pub aws_region: Option<String>,
    /// Custom S3 endpoint (for MinIO, LocalStack, etc.)
    pub aws_endpoint: Option<String>,
    /// Allow HTTP (non-SSL) connections
    pub aws_allow_http: bool,
}

impl StorageConfig {
    /// Create a new configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            aws_access_key_id: env::var("AWS_ACCESS_KEY_ID").ok(),
            aws_secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").ok(),
            aws_region: env::var("AWS_REGION")
                .or_else(|_| env::var("AWS_DEFAULT_REGION"))
                .ok(),
            aws_endpoint: env::var("AWS_ENDPOINT_URL").ok(),
            aws_allow_http: env::var("AWS_ALLOW_HTTP")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
        }
    }

    /// Convert to HashMap for delta-rs storage options.
    pub fn to_storage_options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::new();

        if let Some(ref key) = self.aws_access_key_id {
            opts.insert("AWS_ACCESS_KEY_ID".to_string(), key.clone());
        }
        if let Some(ref secret) = self.aws_secret_access_key {
            opts.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret.clone());
        }
        if let Some(ref region) = self.aws_region {
            opts.insert("AWS_REGION".to_string(), region.clone());
        }
        if let Some(ref endpoint) = self.aws_endpoint {
            opts.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone());
        }
        if self.aws_allow_http {
            opts.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
        }

        opts
    }
}

/// Initialize logging based on environment variable.
pub fn init_logging() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().filter_or("DELTA_FUSION_LOG", "warn"),
    )
    .try_init();
}
