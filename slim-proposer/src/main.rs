mod env;
mod logger;
mod nats;
mod slim_proposer;

use anyhow::Result;
use env::read_slim_proposer_env;
use op_succinct_host_utils::fetcher::OPSuccinctDataFetcher;
use std::sync::Arc;
use tracing::info;

use crate::logger::setup_proposer_logger;
use crate::nats::message_bus::NatsBusConfig;
use crate::slim_proposer::{SlimProposer, TxSubmissionConfig};
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the environment file
    #[arg(long, default_value = ".env")]
    env_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    dotenv::from_filename(args.env_file).ok();

    setup_proposer_logger();

    if cfg!(feature = "echo") {
        info!("Echo proofs enabled");
    } else {
        info!("Echo proofs disabled");
    }

    let provider = rustls::crypto::ring::default_provider();
    provider
        .install_default()
        .map_err(|e| anyhow::anyhow!("Failed to install default provider: {:?}", e))?;

    // Load slim proposer config with both NATS and proposer variables
    let slim_config = read_slim_proposer_env().await?;

    let nats_config = NatsBusConfig {
        nats_url: slim_config.nats_url,
        stream: slim_config.nats_stream,
        input_subject: slim_config.nats_input_subject,
        output_subject: slim_config.nats_output_subject,
        credentials_path: slim_config.nats_credentials_path,
    };

    let tx_config = TxSubmissionConfig {
        gas_max_fee: slim_config.gas_max_fee,
        gas_priority_fee: slim_config.gas_priority_fee,
    };

    let fetcher = Arc::new(OPSuccinctDataFetcher::new_with_rollup_config().await?);

    // Construct and run the slim proposer loop
    let mut proposer = SlimProposer::new(
        nats_config,
        tx_config,
        slim_config.signer,
        slim_config.l2oo_address,
        fetcher,
    )
    .await?;

    info!("Starting slim proposer loop (NATS-driven)");
    proposer.run_loop().await
}
