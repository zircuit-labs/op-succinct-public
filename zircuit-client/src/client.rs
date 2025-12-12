use crate::db::client::DriverDBClient;
use crate::db::types::ProofRequest;
use crate::message_bus::basic::{BasicMessage, BasicMessageBus};
use crate::message_bus::nats::NatsMessageBus;
use crate::message_bus::{
    AnyMessageBus, GenericMessageBus, MessageBus, MessageBusVariant, ProofRequestMessage,
    ProofResponseMessage,
};
use crate::storage::{Storage, StorageConfig, StorageFactory};
use crate::{JobStatus, ProverType, RequestType};
use anyhow::{bail, Result};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::interval;
use uuid::Uuid;

pub struct ZircuitClientConfig {
    pub db_url: String,
    pub proof_request_timeout: Duration,
    pub storage_config: StorageConfig,
    pub message_bus_variant: MessageBusVariant,
    pub prover_type: ProverType,
}
pub struct ZircuitClient {
    config: ZircuitClientConfig,
    db_client: Arc<DriverDBClient>,
    storage: Arc<dyn Storage>,
    handler_handle: Option<JoinHandle<Result<()>>>,
    message_bus: Arc<dyn GenericMessageBus>,
}

impl ZircuitClient {
    pub async fn new(config: ZircuitClientConfig) -> Result<Self> {
        tracing::info!("Creating zircuit client");

        tracing::info!("Initializing storage provider");
        let storage = StorageFactory::create(config.storage_config.clone()).await?;

        tracing::info!("Establish db connection");
        let db_driver = DriverDBClient::new(&config.db_url).await?;

        let message_bus = create_message_bus(&config.message_bus_variant).await?;

        Ok(ZircuitClient {
            config,
            db_client: Arc::new(db_driver),
            storage,
            message_bus,
            handler_handle: None,
        })
    }

    pub fn run_message_handler_thread(
        &mut self,
        polling_config: MessagePollingConfig,
    ) -> Result<()> {
        tracing::info!("Starting message handler thread");
        if self.handler_handle.is_some() {
            return Err(anyhow::anyhow!("Message handler thread already running"));
        }

        let db_client = self.db_client.clone();
        let message_bus = self.message_bus.clone();

        let handle = tokio::spawn(async move {
            let mut interval = interval(polling_config.poll_interval);

            tracing::info!(
                "Starting message handler thread with poll interval: {:?}",
                polling_config.poll_interval
            );

            loop {
                interval.tick().await;

                match message_bus.next_message().await {
                    Ok(Some(message)) => {
                        tracing::info!("Received message with id: {}", message.id());

                        let payload = message.payload();
                        let deserialize = serde_json::from_slice::<ProofResponseMessage>(&payload);
                        let response = match deserialize {
                            Ok(response) => response,
                            Err(e) => {
                                tracing::error!("Failed to deserialize message: {e:?}");
                                continue;
                            }
                        };
                        tracing::info!("Received proof response: {:?}", response);

                        let existing_proof_request =
                            db_client.fetch_request(&response.proof_id).await?;
                        if !existing_proof_request.is_finalized() {
                            let status = if response.proof.is_some() {
                                JobStatus::Completed
                            } else {
                                JobStatus::Failed
                            };
                            let proof_request = db_client
                                .update_request(&response.proof_id, status, response.proof)
                                .await;

                            if let Err(e) = proof_request {
                                tracing::error!("Failed to update proof request: {e:?}");
                                continue;
                            }
                        } else {
                            tracing::warn!(
                                "Proof request already finalized: {:?} ",
                                &existing_proof_request
                            );
                        }

                        if let Err(e) = message.ack().await {
                            tracing::error!("Failed to ack message: {e:?}");
                        } else {
                            tracing::debug!("Message acknowledged successfully");
                        }
                    }
                    Ok(None) => {
                        tracing::debug!("No messages available");
                    }
                    Err(e) => {
                        tracing::error!("Error polling messages: {e:?}");
                        tokio::time::sleep(polling_config.error_backoff).await;
                    }
                }

                // todo: fail request after a timeout?
            }
        });

        self.handler_handle = Some(handle);

        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn with_db_driver(
        config: ZircuitClientConfig,
        driver: DriverDBClient,
    ) -> Result<Self> {
        tracing::info!("Creating zircuit client");

        tracing::info!("Initializing storage provider");
        let storage = StorageFactory::create(config.storage_config.clone()).await?;

        let message_bus = create_message_bus(&config.message_bus_variant).await?;

        Ok(ZircuitClient {
            config,
            db_client: Arc::new(driver),
            storage,
            handler_handle: None,
            message_bus,
        })
    }

    /// Request the proof request from the database.
    /// In `request_proof` is set, the proof will also be retrieved but only if it exists and `proof_url`
    /// already set on the proof request
    pub async fn get_proof_request(
        &self,
        proof_uuid: &str,
        include_proof: bool,
    ) -> Result<(ProofRequest, Option<Vec<u8>>)> {
        let proof_request = self.db_client.fetch_request(proof_uuid).await?;
        let mut proof: Option<Vec<u8>> = None;
        if include_proof && proof_request.status == JobStatus::Completed {
            if let Some(proof_url) = &proof_request.proof_url {
                proof = Some(self.get_proof_data(proof_url).await?);
            } else {
                tracing::error!("Proof request has no proof URL");
            }
        }
        Ok((proof_request, proof))
    }

    pub async fn get_proof_data(&self, proof_url: &String) -> Result<Vec<u8>> {
        tracing::info!("Retrieving proof from URL: {}", proof_url);

        // Use the storage provider to retrieve the proof
        let proof_data = self.storage.retrieve(proof_url).await?;
        tracing::info!("Retrieved proof with size: {} bytes", proof_data.len());

        Ok(proof_data)
    }

    pub fn get_storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    pub fn get_message_bus(&self) -> Arc<dyn GenericMessageBus + 'static> {
        self.message_bus.clone()
    }

    pub async fn request_proof(
        &self,
        proof_input: Vec<u8>,
        meta: Option<HashMap<String, String>>,
        request_type: RequestType,
    ) -> Result<ProofRequest> {
        tracing::info!(
            "Requesting proof with input size: {} bytes",
            proof_input.len()
        );

        if !self.message_bus.is_ready() {
            tracing::warn!("Message bus not ready, skipping message publishing");
            bail!("Message bus not ready");
        }

        // Generate a unique key for the proof input
        let input_key = format!("proof-input-{}.bin", Uuid::new_v4());

        // Store the proof input using the configured storage provider
        tracing::info!("Storing proof input with key: {}", input_key);
        let input_url = self
            .storage
            .store(proof_input, &input_key)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to store proof input: {e:?}"))?;
        tracing::info!("Proof input stored at: {}", input_url);

        // Create a proof request in the database
        tracing::info!("Creating proof request in database");
        let prover_type = &self.config.prover_type;
        let proof_request = self
            .db_client
            .insert_request(&input_url, meta.clone(), *prover_type, request_type)
            .await?;
        tracing::info!("Created proof request with ID: {}", proof_request.proof_id);

        tracing::info!("Publishing proof request message to message bus");
        // Create a proof request message
        let proof_request_message = ProofRequestMessage::from_proof_request(&proof_request);

        let message_json = serde_json::to_vec(&proof_request_message)?;
        let subject_suffix = proof_request_message.get_subject_suffix();

        let sequence = self
            .message_bus
            .publish_with_subject(&message_json, subject_suffix)
            .await?;

        let proof_request = self
            .db_client
            .update_request(&proof_request.proof_id, JobStatus::Submitted, None)
            .await?;

        tracing::info!(
            "Published proof request message with sequence number: {}",
            sequence
        );

        Ok(proof_request)
    }

    pub fn set_prover(&mut self, prover_type: ProverType) {
        tracing::info!("Setting prover type to: {:?}", prover_type);
        self.config.prover_type = prover_type;
    }
}

async fn create_message_bus(config: &MessageBusVariant) -> Result<Arc<dyn GenericMessageBus>> {
    let message_bus = match config {
        MessageBusVariant::Nats(nats_config) => {
            tracing::info!("Initializing NATS message bus");
            let nats_bus = NatsMessageBus::new(nats_config).await?;
            let nats_bus_arc =
                Arc::new(nats_bus) as Arc<dyn MessageBus<async_nats::jetstream::Message>>;
            let any_bus = AnyMessageBus::new(nats_bus_arc);
            Arc::new(any_bus) as Arc<dyn GenericMessageBus>
        }
        MessageBusVariant::Basic => {
            tracing::info!("Initializing basic message bus");
            let basic_bus = BasicMessageBus::new();
            let basic_bus_arc = Arc::new(basic_bus) as Arc<dyn MessageBus<BasicMessage>>;
            let any_bus = AnyMessageBus::new(basic_bus_arc);
            Arc::new(any_bus) as Arc<dyn GenericMessageBus>
        }
    };

    Ok(message_bus)
}

pub struct MessagePollingConfig {
    pub poll_interval: Duration,
    pub error_backoff: Duration,
}

impl Default for MessagePollingConfig {
    fn default() -> Self {
        const DEFAULT_POLL_INTERVAL: u64 = 1;
        const DEFAULT_ERROR_BACKOFF: u64 = 1;

        MessagePollingConfig {
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL),
            error_backoff: Duration::from_secs(DEFAULT_ERROR_BACKOFF),
        }
    }
}
impl MessagePollingConfig {
    pub fn new(poll_interval: Duration) -> Self {
        MessagePollingConfig {
            poll_interval,
            ..Default::default()
        }
    }
}
