#[cfg(test)]
mod zircuit_client_tests {
    use crate::client::{MessagePollingConfig, ZircuitClient, ZircuitClientConfig};
    use crate::db::client::DriverDBClient;
    use crate::logger::init_logger;
    use crate::message_bus::nats::NatsBusConfig;
    use crate::message_bus::{MessageBusVariant, ProofResponseMessage};
    use crate::storage::StorageConfig;
    use crate::{JobStatus, ProverType, RequestType};
    use ctor::ctor;
    use sqlx::PgPool;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::sleep;

    const MESSAGE_PROCESS_SLEEP: Duration = Duration::from_millis(200);

    #[ctor]
    fn init() {
        init_logger();
    }

    /// test the instantiation of the client with real services
    /// this test requires actual services to be running (NATS, minio, postgres). useful for testing
    #[tokio::test]
    async fn test_client_creation_with_real_services() {
        let nats_config = NatsBusConfig {
            nats_url: "nats://0.0.0.0:4223".to_string(),
            stream: "proof_request_client".to_string(),
            input_subject: "input_subject".to_string(),
            output_subject: "output_subject".to_string(),
            credentials_path: None,
        };

        let config = ZircuitClientConfig {
            db_url: "postgresql://op-succinct@0.0.0.0:5432/op-succinct".to_string(),
            storage_config: StorageConfig::S3 {
                region: "us-east-1".to_string(),
                bucket: "test-bucket".to_string(),
                endpoint: "http://0.0.0.0:9100".to_string(),
                no_credentials: true,
                force_path_style: true,
            },
            proof_request_timeout: Duration::from_secs(1),
            message_bus_variant: MessageBusVariant::Nats(nats_config.clone()),
            prover_type: ProverType::Sp1Cpu,
        };

        let mut client = ZircuitClient::new(config).await.unwrap();
        client
            .run_message_handler_thread(MessagePollingConfig::default())
            .expect("Failed to run message handler thread");
    }

    // test used to create a proof request and retrieve it
    // this test requires actual services NATS, Postgres
    #[sqlx::test]
    async fn request_proof_round_trip(pool: PgPool) {
        // Create a temporary directory for storage
        let temp_dir = tempdir().unwrap();

        // Create a storage config for local storage
        let storage_config = StorageConfig::Local {
            base_path: temp_dir.path().to_path_buf(),
        };

        let nats_config = NatsBusConfig {
            nats_url: "nats://0.0.0.0:4223".to_string(),
            stream: "proof_request_client".to_string(),
            input_subject: "proof_request_client.input_subject".to_string(),
            output_subject: "proof_request_client.output_subject".to_string(),
            credentials_path: None,
        };
        // Create client config
        let client_config = ZircuitClientConfig {
            db_url: "mock://".to_string(), // not needed as we use sqlx::test to provide the pool
            proof_request_timeout: Duration::from_secs(30),
            storage_config,
            message_bus_variant: MessageBusVariant::Nats(nats_config.clone()),
            prover_type: ProverType::Sp1Cpu,
        };

        // Create a client
        let db_driver = DriverDBClient::new_with_pool(pool);
        let client = ZircuitClient::with_db_driver(client_config, db_driver)
            .await
            .unwrap();

        // Test data
        let proof_input = b"test proof input data".to_vec();
        let meta = Some(HashMap::from([("key1".to_string(), "value1".to_string())]));

        // Request a proof
        let proof_request = client
            .request_proof(proof_input.clone(), meta.clone(), RequestType::Range)
            .await
            .unwrap();

        println!("Proof request: {proof_request:#?}");
        // Verify the proof request
        assert!(!proof_request.proof_id.is_empty());
        assert!(proof_request.proof_input_url.starts_with("file://"));
        assert_eq!(proof_request.status, JobStatus::Submitted);
        assert_eq!(proof_request.request_type, RequestType::Range);

        // Verify we can retrieve the proof input
        let retrieved_input = client
            .get_storage()
            .retrieve(&proof_request.proof_input_url)
            .await
            .unwrap();

        assert_eq!(proof_input, retrieved_input);
    }

    /// test the creating of proof requests as well as the handling of proof responses
    /// this test does not require real services running
    #[sqlx::test]
    async fn test_message_handler_with_basic_message_bus(pool: PgPool) {
        // Create a temporary directory for storage - needs to stay in scope for the duration of the test
        let temp_dir = tempdir().unwrap();
        let client = create_test_zircuit_client(pool, temp_dir.path().to_path_buf()).await;

        // Test data
        let proof_input = b"test proof input data".to_vec();
        let meta = HashMap::from([("key1".to_string(), "value1".to_string())]);

        // Request a proof
        let proof_request = client
            .request_proof(
                proof_input.clone(),
                Some(meta.clone()),
                RequestType::Aggregation,
            )
            .await
            .unwrap();

        assert_eq!(proof_request.status, JobStatus::Submitted);
        assert!(proof_request.request_completed.is_none());
        assert_eq!(proof_request.request_type, RequestType::Aggregation);

        // Create a proof response message
        let response = ProofResponseMessage {
            proof_id: proof_request.proof_id.clone(),
            proof: Some("file:///test/proof.bin".to_string()),
        };

        // Serialize and publish the response
        let response_bytes = serde_json::to_vec(&response).unwrap();
        client
            .get_message_bus()
            .publish(&response_bytes)
            .await
            .unwrap();

        // Wait for the message handler loop to run and process the message
        sleep(MESSAGE_PROCESS_SLEEP).await;

        // Verify that the proof request was updated
        let updated_request = client
            .get_proof_request(&proof_request.proof_id, false)
            .await
            .unwrap()
            .0;

        assert_eq!(updated_request.status, JobStatus::Completed);
        assert_eq!(
            updated_request.proof_url,
            Some("file:///test/proof.bin".to_string())
        );
        assert!(updated_request.request_completed.is_some());
        assert_eq!(updated_request.meta.unwrap().0, meta);
        assert_eq!(updated_request.request_type, RequestType::Aggregation);
    }

    #[sqlx::test]
    async fn test_failing_proof_responses(pool: PgPool) {
        // Create a temporary directory for storage - needs to stay in scope for the duration of the test
        let temp_dir = tempdir().unwrap();
        let client = create_test_zircuit_client(pool, temp_dir.path().to_path_buf()).await;
        // Test data
        let proof_input = b"test proof input data".to_vec();

        // Request a proof
        let proof_request = client
            .request_proof(proof_input.clone(), None, RequestType::Range)
            .await
            .unwrap();

        assert_eq!(proof_request.status, JobStatus::Submitted);
        assert!(proof_request.request_completed.is_none());

        // Create a proof response message
        let response = ProofResponseMessage {
            proof_id: proof_request.proof_id.clone(),
            proof: None,
        };

        // Serialize and publish the response
        let response_bytes = serde_json::to_vec(&response).unwrap();
        client
            .get_message_bus()
            .publish(&response_bytes)
            .await
            .unwrap();

        // Wait for the message handler loop to run and process the message
        sleep(MESSAGE_PROCESS_SLEEP).await;

        // Verify that the proof request was updated
        let updated_request = client
            .get_proof_request(&proof_request.proof_id, false)
            .await
            .unwrap()
            .0;

        assert_eq!(updated_request.status, JobStatus::Failed);
        assert_eq!(updated_request.proof_url, None);
        assert!(updated_request.request_completed.is_some());
    }

    #[sqlx::test]
    async fn try_updating_finalized_proof_responses(pool: PgPool) {
        // Create a temporary directory for storage - needs to stay in scope for the duration of the test
        let temp_dir = tempdir().unwrap();
        let client = create_test_zircuit_client(pool, temp_dir.path().to_path_buf()).await;

        // Request a proof
        let proof_request = client
            .request_proof(b"test proof input data".to_vec(), None, RequestType::Range)
            .await
            .unwrap();

        // Create a proof response message
        let response = ProofResponseMessage {
            proof_id: proof_request.proof_id.clone(),
            proof: None,
        };

        let response_bytes = serde_json::to_vec(&response).unwrap();
        client
            .get_message_bus()
            .publish(&response_bytes)
            .await
            .unwrap();

        // Wait for the message handler loop to run and process the message
        sleep(MESSAGE_PROCESS_SLEEP).await;

        // Verify that the proof request was updated
        let updated_request = client
            .get_proof_request(&proof_request.proof_id, false)
            .await
            .unwrap()
            .0;

        // push another message with a proof this time. we should not see an updated proof request after finalized
        let response = ProofResponseMessage {
            proof_id: proof_request.proof_id.clone(),
            proof: Some("file:///dummy.bin".to_string()),
        };
        let response_bytes = serde_json::to_vec(&response).unwrap();
        client
            .get_message_bus()
            .publish(&response_bytes)
            .await
            .unwrap();

        // Wait for the message handler loop to run and process the message
        sleep(MESSAGE_PROCESS_SLEEP).await;

        // Verify that the proof request was updated
        let updated_request_2 = client
            .get_proof_request(&proof_request.proof_id, false)
            .await
            .unwrap()
            .0;

        assert_eq!(updated_request_2.status, JobStatus::Failed);
        assert_eq!(updated_request_2.proof_url, None);
        assert_eq!(
            updated_request_2.request_completed,
            updated_request.request_completed
        );
    }

    /// creates a ZircuitClient and a BasicMessageBus
    /// expects the db to be running and uses the local storage strategy and message bus
    async fn create_test_zircuit_client(pool: PgPool, storage_path: PathBuf) -> ZircuitClient {
        // Create a storage config for local storage
        let storage_config = StorageConfig::Local {
            base_path: storage_path,
        };

        // Create client config with BasicMessageBus
        let client_config = ZircuitClientConfig {
            db_url: "mock://".to_string(), // not needed as we use sqlx::test to provide the pool
            proof_request_timeout: Duration::from_secs(30),
            storage_config,
            message_bus_variant: MessageBusVariant::Basic,
            prover_type: ProverType::Sp1Cpu,
        };

        // Create a client
        let db_driver = DriverDBClient::new_with_pool(pool);
        let mut client = ZircuitClient::with_db_driver(client_config, db_driver)
            .await
            .unwrap();

        client
            .run_message_handler_thread(MessagePollingConfig::new(Duration::from_millis(20)))
            .expect("Failed to run message handler thread");

        client
    }
}
