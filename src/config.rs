//! Configuration for delta_fusion.

use std::collections::HashMap;
use std::env;

/// Environment variable names for storage configuration.
mod env_vars {
    pub const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
    pub const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
    pub const AWS_REGION: &str = "AWS_REGION";
    pub const AWS_DEFAULT_REGION: &str = "AWS_DEFAULT_REGION";
    pub const AWS_ENDPOINT_URL: &str = "AWS_ENDPOINT_URL";
    pub const AWS_ALLOW_HTTP: &str = "AWS_ALLOW_HTTP";
}

/// Configuration for Delta Lake storage backends.
///
/// Supports AWS S3 and compatible storage (MinIO, LocalStack).
/// Use the builder pattern for fluent configuration.
///
/// # Example
/// ```
/// use delta_fusion::StorageConfig;
///
/// let config = StorageConfig::builder()
///     .aws_access_key_id("AKIAIOSFODNN7EXAMPLE")
///     .aws_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
///     .aws_region("us-east-1")
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_region: Option<String>,
    aws_endpoint: Option<String>,
    aws_allow_http: bool,
}

impl StorageConfig {
    /// Create a new builder for StorageConfig.
    pub fn builder() -> StorageConfigBuilder {
        StorageConfigBuilder::default()
    }

    /// Create a new configuration from environment variables.
    ///
    /// Reads the following environment variables:
    /// - `AWS_ACCESS_KEY_ID`
    /// - `AWS_SECRET_ACCESS_KEY`
    /// - `AWS_REGION` or `AWS_DEFAULT_REGION`
    /// - `AWS_ENDPOINT_URL`
    /// - `AWS_ALLOW_HTTP` ("true" or "1")
    pub fn from_env() -> Self {
        Self {
            aws_access_key_id: env::var(env_vars::AWS_ACCESS_KEY_ID).ok(),
            aws_secret_access_key: env::var(env_vars::AWS_SECRET_ACCESS_KEY).ok(),
            aws_region: env::var(env_vars::AWS_REGION)
                .or_else(|_| env::var(env_vars::AWS_DEFAULT_REGION))
                .ok(),
            aws_endpoint: env::var(env_vars::AWS_ENDPOINT_URL).ok(),
            aws_allow_http: env::var(env_vars::AWS_ALLOW_HTTP)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
        }
    }

    /// Get AWS access key ID.
    pub fn aws_access_key_id(&self) -> Option<&str> {
        self.aws_access_key_id.as_deref()
    }

    /// Get AWS secret access key.
    pub fn aws_secret_access_key(&self) -> Option<&str> {
        self.aws_secret_access_key.as_deref()
    }

    /// Get AWS region.
    pub fn aws_region(&self) -> Option<&str> {
        self.aws_region.as_deref()
    }

    /// Get custom AWS endpoint URL.
    pub fn aws_endpoint(&self) -> Option<&str> {
        self.aws_endpoint.as_deref()
    }

    /// Check if HTTP connections are allowed.
    pub fn aws_allow_http(&self) -> bool {
        self.aws_allow_http
    }

    /// Check if S3 credentials are configured.
    pub fn has_s3_credentials(&self) -> bool {
        self.aws_access_key_id.is_some() && self.aws_secret_access_key.is_some()
    }

    /// Convert to HashMap for delta-rs storage options.
    pub fn to_storage_options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::new();

        // Helper to insert option if present
        let mut insert_if_some = |key: &str, value: &Option<String>| {
            if let Some(ref v) = value {
                opts.insert(key.to_string(), v.clone());
            }
        };

        insert_if_some(env_vars::AWS_ACCESS_KEY_ID, &self.aws_access_key_id);
        insert_if_some(env_vars::AWS_SECRET_ACCESS_KEY, &self.aws_secret_access_key);
        insert_if_some(env_vars::AWS_REGION, &self.aws_region);
        insert_if_some(env_vars::AWS_ENDPOINT_URL, &self.aws_endpoint);

        if self.aws_allow_http {
            opts.insert(env_vars::AWS_ALLOW_HTTP.to_string(), "true".to_string());
        }

        opts
    }
}

/// Builder for StorageConfig.
#[derive(Debug, Clone, Default)]
pub struct StorageConfigBuilder {
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_region: Option<String>,
    aws_endpoint: Option<String>,
    aws_allow_http: bool,
}

impl StorageConfigBuilder {
    /// Set AWS access key ID.
    pub fn aws_access_key_id(mut self, key: impl Into<String>) -> Self {
        self.aws_access_key_id = Some(key.into());
        self
    }

    /// Set AWS secret access key.
    pub fn aws_secret_access_key(mut self, secret: impl Into<String>) -> Self {
        self.aws_secret_access_key = Some(secret.into());
        self
    }

    /// Set AWS region.
    pub fn aws_region(mut self, region: impl Into<String>) -> Self {
        self.aws_region = Some(region.into());
        self
    }

    /// Set custom AWS endpoint URL (for MinIO, LocalStack, etc.).
    pub fn aws_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.aws_endpoint = Some(endpoint.into());
        self
    }

    /// Allow HTTP connections (default: false).
    pub fn aws_allow_http(mut self, allow: bool) -> Self {
        self.aws_allow_http = allow;
        self
    }

    /// Build the StorageConfig.
    pub fn build(self) -> StorageConfig {
        StorageConfig {
            aws_access_key_id: self.aws_access_key_id,
            aws_secret_access_key: self.aws_secret_access_key,
            aws_region: self.aws_region,
            aws_endpoint: self.aws_endpoint,
            aws_allow_http: self.aws_allow_http,
        }
    }
}

/// Initialize logging based on environment variable.
///
/// Uses the `DELTA_FUSION_LOG` environment variable to control log level.
/// Defaults to "warn" if not set.
pub fn init_logging() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().filter_or("DELTA_FUSION_LOG", "warn"),
    )
    .try_init();
}
