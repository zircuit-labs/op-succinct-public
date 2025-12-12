#[cfg(test)]
mod storage_tests {
    use crate::storage::local::LOCAL_FILE_PREFIX;
    use crate::storage::*;
    use tempfile::tempdir;
    use tokio::test;

    #[test]
    async fn test_storage_strategy_from_str() {
        assert_eq!(
            StorageStrategy::try_from("local").unwrap(),
            StorageStrategy::Local
        );
        assert_eq!(
            StorageStrategy::try_from("LOCAL").unwrap(),
            StorageStrategy::Local
        );
        assert_eq!(
            StorageStrategy::try_from("s3").unwrap(),
            StorageStrategy::S3
        );
        assert_eq!(
            StorageStrategy::try_from("S3").unwrap(),
            StorageStrategy::S3
        );

        // Test invalid strategy
        assert!(StorageStrategy::try_from("invalid").is_err());
    }

    #[test]
    async fn test_storage_factory_local() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();

        let config = StorageConfig::Local {
            base_path: temp_dir.path().to_path_buf(),
        };

        // Create a storage instance
        let storage = StorageFactory::create(config).await.unwrap();

        // Test data
        let data = b"test data for factory".to_vec();
        let key = "factory-test.txt";

        // Store the data
        let url = storage.store(data.clone(), key).await.unwrap();

        // Verify the URL format
        assert!(url.starts_with(LOCAL_FILE_PREFIX));
        assert!(url.contains(key));

        // Retrieve the data
        let retrieved = storage.retrieve(&url).await.unwrap();

        // Verify the data is the same
        assert_eq!(data, retrieved);
    }

    // needs a local s3 server running
    #[test]
    async fn test_storage_factory_s3() {
        let config = StorageConfig::S3 {
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            endpoint: "http://localhost:9100".to_string(),
            no_credentials: true,
            force_path_style: true,
        };

        // Create a storage instance
        let storage = StorageFactory::create(config).await.unwrap();

        // Test data
        let data = b"test data for factory".to_vec();
        let key = "s3-bucket-test.txt";

        // Store the data
        let url = storage.store(data.clone(), key).await.unwrap();

        // Verify the URL format
        assert!(url.starts_with("s3://"));
        assert!(url.contains(key));

        // Retrieve the data
        let retrieved = storage.retrieve(&url).await.unwrap();

        // Verify the data is the same
        assert_eq!(data, retrieved);
    }
}
