use anyhow::Result;
use sp1_sdk::SP1Stdin;
use std::io::BufReader;
use tokio::signal;
use zircuit_client::client::ZircuitClient;
use zircuit_client::env::client_config_from_env;
use zircuit_client::logger::init_logger;
use zircuit_client::RequestType;

/// A simple binary which runs the zircuit client with the configuration from env.
/// Useful for testing or running the client as a standalone process as opposed to instantiating
/// it in the Proposer.
#[tokio::main]
async fn main() -> Result<()> {
    init_logger();

    // Get the file path from command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        tracing::warn!("Usage: {} <sp1_stdin_file_path>", args[0]);
        std::process::exit(1);
    }
    let file_path = &args[1];

    // Create client configuration from environment variables
    let client_config = client_config_from_env()?;

    // Create and run the client
    let mut client = ZircuitClient::new(client_config).await?;

    // Run the message handler thread
    client.run_message_handler_thread(Default::default())?;

    tracing::info!("Requesting a proof for the input witness...");
    let file = std::fs::File::open(file_path)?;
    let reader = BufReader::new(file);
    let stdin: SP1Stdin = bincode::deserialize_from(reader)?;
    let proof_input = bincode::serialize(&stdin)?;

    // Request a proof
    let proof_request = client
        .request_proof(proof_input.clone(), None, RequestType::Range)
        .await?;

    tracing::info!("Proof request: {proof_request:#?}");

    // Keep the application running until a termination signal is received
    tracing::info!("Zircuit client is running. Press Ctrl+C to stop.");
    match signal::ctrl_c().await {
        Ok(()) => {
            tracing::info!("Shutting down zircuit client...");
        }
        Err(err) => {
            tracing::error!("Error waiting for Ctrl+C: {err:?}");
        }
    }

    Ok(())
}
