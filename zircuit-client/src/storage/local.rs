use crate::storage::Storage;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tracing::info;

pub(crate) const LOCAL_FILE_PREFIX: &str = "file://";

#[derive(Clone, Debug)]
pub struct LocalStorage {
    pub(crate) base_path: PathBuf,
}

impl LocalStorage {
    /// Create a new local storage with the given base path
    pub fn new(base_path: PathBuf) -> anyhow::Result<Self> {
        // Create the directory if it doesn't exist
        std::fs::create_dir_all(&base_path)?;

        Ok(LocalStorage { base_path })
    }

    /// Get the full path for a key
    fn get_path(&self, key: &str) -> PathBuf {
        self.base_path.join(key)
    }

    /// Convert a path to a URL
    fn path_to_url(&self, path: &Path) -> String {
        format!("{LOCAL_FILE_PREFIX}{}", path.display())
    }

    /// Extract the key from a URL
    fn url_to_key(&self, url: &str) -> anyhow::Result<String> {
        if !url.starts_with(LOCAL_FILE_PREFIX) {
            return Err(anyhow::anyhow!("Invalid URL format: {}", url));
        }

        let path = url.trim_start_matches(LOCAL_FILE_PREFIX);
        let path = Path::new(path);

        if let Some(file_name) = path.file_name() {
            Ok(file_name.to_string_lossy().to_string())
        } else {
            Err(anyhow::anyhow!("Invalid URL: {}", url))
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, data: Vec<u8>, key: &str) -> anyhow::Result<String> {
        info!("Local store: {} bytes. Key: {}", data.len(), key);

        let path = self.get_path(key);
        tokio::fs::write(&path, data).await?;
        Ok(self.path_to_url(&path))
    }

    async fn retrieve(&self, url: &str) -> anyhow::Result<Vec<u8>> {
        let key = self.url_to_key(url)?;
        info!("Local retrieve: {key}");

        let path = self.get_path(&key);
        let data = tokio::fs::read(path).await?;
        Ok(data)
    }
}
