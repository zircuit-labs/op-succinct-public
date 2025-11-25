use anyhow::Result;
use op_succinct_elfs::RANGE_ELF_EMBEDDED;
use sp1_sdk::{CpuProver, Prover, ProverClient, SP1Stdin};
use std::mem;
use std::sync::Arc;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use zircuit_client::client::{ZircuitClient, ZircuitClientConfig};
use zircuit_client::env::client_config_from_env;
use zircuit_client::logger::init_logger;
use zircuit_client::message_bus::{
    MessageBusVariant, ProofRequestMessage, ProofResponseMessage, ZircuitClientMessage,
};
use zircuit_client::storage::Storage;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();

    tracing::info!("Starting test proving client...");

    // Create client configuration from environment variables
    let mut client_config = client_config_from_env()?;

    match &mut client_config.message_bus_variant {
        MessageBusVariant::Basic => {
            tracing::error!("Basic message bus is not supported for test proving client");
        }
        MessageBusVariant::Nats(config) => {
            // swap the input and output streams as we are the proof producer here as opposed to the zircuit client
            mem::swap(&mut config.input_subject, &mut config.output_subject);
        }
    }

    // Create a channel for shutdown signal
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn a task to handle Ctrl+C
    tokio::spawn(async move {
        signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl+C event");
        tracing::info!("Received Ctrl+C, shutting down test prover...");
        let _ = shutdown_tx.send(());
    });

    // Spawn a task to handle proof requests
    let request_handler = create_message_handler_thread(client_config);

    // Wait for a shutdown signal
    tokio::select! {
        _ = shutdown_rx => {
            tracing::info!("Received shutdown signal, shutting down test prover...");
        }
        result = request_handler => {
            match result {
                Ok(Ok(())) => tracing::info!("Request handler completed successfully"),
                Ok(Err(e)) => tracing::error!("Request handler error: {e:?}"),
                Err(e) => tracing::error!("Request handler task error: {e:?}"),
            }
        }
    }

    tracing::info!("Cleaning up remaining tasks and exiting.");
    Ok(())
}

fn create_message_handler_thread(client_config: ZircuitClientConfig) -> JoinHandle<Result<()>> {
    let request_handler: JoinHandle<Result<()>> = tokio::spawn(async move {
        tracing::info!("Starting request handler task...");

        let client = Arc::new(ZircuitClient::new(client_config).await?);
        let message_bus = client.get_message_bus();
        let storage = client.get_storage();

        tracing::info!("Spawning mock prover client");
        let prover_client =
            Arc::new(tokio::task::spawn_blocking(|| ProverClient::builder().mock().build()).await?);

        tracing::info!("Listening for proof request messages...");
        loop {
            match message_bus.next_message().await {
                Ok(Some(message)) => {
                    tracing::info!("Received proof message with id: {}", message.id());

                    let response =
                        handle_proof_request(&message, prover_client.clone(), storage.clone())
                            .await?;
                    let response = serde_json::to_vec(&response).unwrap();

                    let message_id = message_bus.publish(&response).await?;
                    tracing::info!("Published proof response with id: {}", message_id);

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
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });
    request_handler
}

#[allow(clippy::borrowed_box)]
async fn handle_proof_request(
    message: &Box<dyn ZircuitClientMessage>,
    prover_client: Arc<CpuProver>,
    storage: Arc<dyn Storage>,
) -> Result<ProofResponseMessage> {
    let payload = message.payload();
    let proof_request = serde_json::from_slice::<ProofRequestMessage>(&payload)?;

    tracing::info!("Start proving");
    let proof_input = storage.retrieve(&proof_request.proof_input).await?;

    let stdin: SP1Stdin = bincode::deserialize_from(proof_input.as_slice())?;
    let (circuit_pk, _vk) = prover_client.setup(RANGE_ELF_EMBEDDED);
    let proof = match tokio::task::spawn_blocking(move || {
        prover_client.prove(&circuit_pk, &stdin).compressed().run()
    })
    .await?
    {
        Ok(proof) => proof,
        Err(e) => {
            tracing::error!("Error executing proof: {e:?}");
            return Err(e);
        }
    };
    tracing::info!("Proving done");

    let proof_blob = bincode::serialize(&proof)?;
    let proof_key = format!("proof_output-{}.bin", proof_request.proof_id);
    let proof_url = storage.store(proof_blob, &proof_key).await?;
    tracing::info!("Proof URL: {proof_url}");

    let response = ProofResponseMessage {
        proof_id: proof_request.proof_id,
        proof: Some(proof_url),
    };

    Ok(response)
}
