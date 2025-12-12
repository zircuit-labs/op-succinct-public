mod local;
mod s3;
#[cfg(test)]
mod tests;

use crate::storage::local::LocalStorage;
use crate::storage::s3::S3Storage;
use anyhow::Result;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use strum;
use strum::EnumString;

/// Storage strategy enum to select between different storage implementations
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StorageStrategy {
    /// Store files locally in the filesystem
    Local,
    /// Store files in an S3 bucket
    S3,
}

/// Configuration for storage providers
#[derive(Debug, Clone)]
pub enum StorageConfig {
    Local {
        base_path: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        endpoint: String,
        no_credentials: bool,
        force_path_style: bool,
    },
}

/// Trait defining storage operations
#[async_trait]
pub trait Storage: Send + Sync {
    /// Store data and return a URL that can be used to retrieve it
    async fn store(&self, data: Vec<u8>, key: &str) -> Result<String>;

    /// Retrieve data from storage using a URL
    async fn retrieve(&self, url: &str) -> Result<Vec<u8>>;
}

/// Factory for creating storage implementations based on configuration
pub struct StorageFactory;

impl StorageFactory {
    /// Create a new storage implementation based on the provided configuration
    pub async fn create(config: StorageConfig) -> Result<Arc<dyn Storage>> {
        match config {
            StorageConfig::Local { base_path } => Ok(Arc::new(LocalStorage::new(base_path)?)),
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                no_credentials,
                force_path_style,
            } => Ok(Arc::new(
                S3Storage::new(bucket, region, endpoint, no_credentials, force_path_style).await?,
            )),
        }
    }
}
