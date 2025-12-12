use crate::storage::Storage;
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use tracing::info;

pub(crate) const S3_FILE_PREFIX: &str = "s3://";

pub struct S3Storage {
    bucket: String,
    client: aws_sdk_s3::Client,
}

impl S3Storage {
    /// Create a new S3 storage with the given bucket and region
    pub async fn new(
        bucket: String,
        region: String,
        endpoint: String,
        no_credentials: bool,
        force_path_style: bool,
    ) -> anyhow::Result<Self> {
        let mut config = aws_config::defaults(BehaviorVersion::v2025_01_17())
            .endpoint_url(endpoint)
            .region(Region::new(region));

        // for the tests we expect a public bucket which does not require credentials
        if no_credentials {
            config = config.no_credentials();
        }

        let config = config.load().await;

        let mut builder = aws_sdk_s3::config::Builder::from(&config);
        builder.set_force_path_style(Some(force_path_style));

        let config = builder.build();
        let client = aws_sdk_s3::Client::from_conf(config);

        Ok(S3Storage { bucket, client })
    }

    /// Convert a key to an S3 URL
    fn key_to_url(&self, key: &str) -> String {
        format!("{S3_FILE_PREFIX}{}/{}", self.bucket, key)
    }

    /// Extract the key from an S3 URL
    fn url_to_key(&self, url: &str) -> anyhow::Result<String> {
        if !url.starts_with(S3_FILE_PREFIX) {
            return Err(anyhow::anyhow!("Invalid S3 URL format: {}", url));
        }

        let parts: Vec<&str> = url.trim_start_matches(S3_FILE_PREFIX).split('/').collect();
        if parts.len() < 2 {
            return Err(anyhow::anyhow!("Invalid S3 URL: {}", url));
        }

        // Skip the bucket name (parts[0]) and join the rest as the key
        Ok(parts[1..].join("/"))
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn store(&self, data: Vec<u8>, key: &str) -> anyhow::Result<String> {
        info!("S3 store: {} bytes. Key: {}", data.len(), key);
        let body = aws_sdk_s3::primitives::ByteStream::from(data);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await?;

        Ok(self.key_to_url(key))
    }

    async fn retrieve(&self, url: &str) -> anyhow::Result<Vec<u8>> {
        let key = self.url_to_key(url)?;

        info!("S3 retrieve: {key}");
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let data = response.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }
}
