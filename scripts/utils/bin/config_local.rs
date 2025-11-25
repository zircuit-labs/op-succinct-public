use alloy_primitives::B256;
use anyhow::Result;
use clap::Parser;
use op_succinct_client_utils::{boot::hash_rollup_config, types::u32_to_u8};
use op_succinct_elfs::AGGREGATION_ELF;
use op_succinct_proof_utils::get_range_elf_embedded;
use serde::Serialize;
use sp1_sdk::{HashableKey, Prover, ProverClient};
use std::{fs::File, path::PathBuf};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Rollup Config Path
    #[arg(long)]
    rollup_config_path: Option<PathBuf>,
}

#[derive(Serialize)]
struct ConfigOutput {
    aggregation_vk: String,
    range_vk: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    rollup_config_hash: Option<String>,
}

// Get the verification keys for the ELFs and check them against the contract.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let prover = ProverClient::builder().cpu().build();

    let (_, range_vk) = prover.setup(get_range_elf_embedded());
    let range_vk_hash = B256::from(u32_to_u8(range_vk.vk.hash_u32()));

    let (_, agg_vk) = prover.setup(AGGREGATION_ELF);

    let rollup_config_hash = args
        .rollup_config_path
        .map(|path| {
            let file = File::open(path)?;
            let rollup_config = serde_json::from_reader(&file)?;
            Ok::<String, anyhow::Error>(hash_rollup_config(&rollup_config).to_string())
        })
        .transpose()?;

    let output_config = ConfigOutput {
        aggregation_vk: agg_vk.bytes32(),
        range_vk: range_vk_hash.to_string(),
        rollup_config_hash,
    };

    println!("{}", serde_json::to_string_pretty(&output_config)?);

    Ok(())
}
