use std::env;

use crate::client_proxy::ClientVariant;
use alloy_primitives::Address;
use anyhow::Result;
use op_succinct_signer_utils::Signer;
use reqwest::Url;
use sp1_sdk::{network::FulfillmentStrategy, SP1ProofMode};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct SlimProposerConfig {
    pub nats_url: String,
    pub nats_stream: String,
    pub nats_input_subject: String,
    pub nats_output_subject: String,
    pub nats_credentials_path: Option<String>,

    // Minimal proposer configuration
    pub l1_rpc: Url,
    pub signer: Signer,
    pub l2oo_address: Address,

    // tx fee overrides
    pub gas_priority_fee: Option<u128>,
    pub gas_max_fee: Option<u128>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    pub db_url: String,
    pub metrics_port: u16,
    pub l1_rpc: Url,
    pub signer: Signer,
    pub prover_address: Address,
    pub loop_interval: u64,
    pub range_proof_strategy: FulfillmentStrategy,
    pub agg_proof_strategy: FulfillmentStrategy,
    pub agg_proof_mode: SP1ProofMode,
    pub l2oo_address: Address,
    pub dgf_address: Address,
    pub range_proof_interval: u64,
    pub max_concurrent_witness_gen: u64,
    pub max_concurrent_proof_requests: u64,
    pub max_new_ranges_per_iteration: u64,
    pub cost_estimator_timeout_secs: u64,
    pub witness_gen_timeout_secs: u64,
    pub max_exec_timeout_retries: u64,
    pub fork_start_block: Option<u64>,
    pub submission_interval: u64,
    pub max_aggregated_ranges: u64,
    pub mock: bool,
    pub safe_db_fallback: bool,
    pub proof_request_client: ClientVariant,
    pub high_memory_threshold: Option<u64>,
    pub instruction_count_limit: u64,
    pub split_on_timeout: bool,
    pub set_for_upgrade: bool,
    pub force_start_block: Option<u64>,
}

/// Helper function to get environment variables with a default value and parse them.
fn get_env_var<T>(key: &str, default: Option<T>) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("Failed to parse {}: {:?}", key, e)),
        Err(_) => match default {
            Some(default_val) => Ok(default_val),
            None => anyhow::bail!("{} is not set", key),
        },
    }
}

fn get_optional_env_var<T>(key: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(value) => {
            Ok(Some(value.parse::<T>().map_err(|e| {
                anyhow::anyhow!("Failed to parse {}: {:?}", key, e)
            })?))
        }
        Err(_) => Ok(None),
    }
}

// 1 minute default loop interval.
const DEFAULT_LOOP_INTERVAL: u64 = 60;

/// Read proposer environment variables and return a config.
///
/// Signer address and signer URL take precedence over private key.
pub async fn read_proposer_env() -> Result<EnvironmentConfig> {
    let signer = Signer::from_env().await?;

    // The prover address takes precedence over the signer address. Note: Setting the prover address
    // in the context of the OP Succinct proposer typically does not make sense, as the contract
    // will verify `tx.origin` matches the `proverAddress`.
    let prover_address = get_env_var("PROVER_ADDRESS", Some(signer.address()))?;

    // Parse strategy values
    let range_proof_strategy = if get_env_var("RANGE_PROOF_STRATEGY", Some("reserved".to_string()))?
        .to_lowercase()
        == "hosted"
    {
        FulfillmentStrategy::Hosted
    } else {
        FulfillmentStrategy::Reserved
    };

    let agg_proof_strategy = if get_env_var("AGG_PROOF_STRATEGY", Some("reserved".to_string()))?
        .to_lowercase()
        == "hosted"
    {
        FulfillmentStrategy::Hosted
    } else {
        FulfillmentStrategy::Reserved
    };

    // Parse proof mode
    let agg_proof_mode =
        if get_env_var("AGG_PROOF_MODE", Some("groth16".to_string()))?.to_lowercase() == "plonk" {
            SP1ProofMode::Plonk
        } else {
            SP1ProofMode::Groth16
        };

    let fork_start_block = get_optional_env_var("FORK_START_BLOCK")?;
    if let Some(fork_start_block) = fork_start_block {
        warn!("FORK_START_BLOCK has been set to {fork_start_block}. Proposer will start proposing from block {fork_start_block}");
    }

    let force_start_block = get_optional_env_var("FORCE_START_BLOCK")?;
    if let Some(start_block) = force_start_block {
        warn!("FORCE_START_BLOCK has been set to {start_block}. Proposer will start proposing from block {start_block}");
    }

    // Optional loop interval
    let loop_interval = get_env_var("LOOP_INTERVAL", Some(DEFAULT_LOOP_INTERVAL))?;

    let max_aggregated_ranges = get_env_var("MAX_AGGREGATED_RANGES", Some(100))?;
    let range_proof_interval = get_env_var("RANGE_PROOF_INTERVAL", Some(1800))?;
    let submission_interval = get_env_var("SUBMISSION_INTERVAL", Some(1800))?;
    let cost_estimator_timeout_secs = get_env_var("COST_ESTIMATOR_TIMEOUT_SECS", Some(120))?;
    let witness_gen_timeout_secs = get_env_var("WITNESS_GEN_TIMEOUT_SECS", Some(120))?;
    let max_exec_timeout_retries = get_env_var("MAX_EXEC_TIMEOUT_RETRIES", Some(5))?;

    let config = EnvironmentConfig {
        metrics_port: get_env_var("METRICS_PORT", Some(8080))?,
        l1_rpc: get_env_var("L1_RPC", None)?,
        signer,
        prover_address,
        db_url: get_env_var("DATABASE_URL", None)?,
        range_proof_strategy,
        agg_proof_strategy,
        agg_proof_mode,
        fork_start_block,
        force_start_block,
        l2oo_address: get_env_var("L2OO_ADDRESS", Some(Address::ZERO))?,
        dgf_address: get_env_var("DGF_ADDRESS", Some(Address::ZERO))?,
        range_proof_interval,
        max_concurrent_witness_gen: get_env_var("MAX_CONCURRENT_WITNESS_GEN", Some(1))?,
        max_concurrent_proof_requests: get_env_var("MAX_CONCURRENT_PROOF_REQUESTS", Some(1))?,
        max_new_ranges_per_iteration: get_env_var("MAX_NEW_RANGES_PER_ITERATION", Some(300))?,
        cost_estimator_timeout_secs,
        witness_gen_timeout_secs,
        max_exec_timeout_retries,
        submission_interval,
        max_aggregated_ranges,
        mock: get_env_var("OP_SUCCINCT_MOCK", Some(false))?,
        loop_interval,
        safe_db_fallback: get_env_var("SAFE_DB_FALLBACK", Some(false))?,
        proof_request_client: get_env_var("PROOF_REQUEST_CLIENT", Some(ClientVariant::Sindri))?,
        high_memory_threshold: get_optional_env_var("HIGH_MEMORY_THRESHOLD")?,
        instruction_count_limit: get_env_var("INSTRUCTION_COUNT_LIMIT", Some(5_000_000_000))?,
        split_on_timeout: get_env_var("SPLIT_ON_TIMEOUT", Some(true))?,
        set_for_upgrade: get_env_var("SET_FOR_UPGRADE", Some(false))?,
    };

    if config.range_proof_interval == 0 || config.submission_interval == 0 {
        return Err(anyhow::anyhow!(
            "Invalid configuration, range_proof_interval: {} and submission_interval: {} must be positive",
            config.range_proof_interval,
            config.submission_interval
        ));
    }

    let agg_batch_size = max_aggregated_ranges * range_proof_interval;
    if agg_batch_size < submission_interval {
        return Err(anyhow::anyhow!(
            "Wrong configuration, agg_batch_size: {agg_batch_size} = {max_aggregated_ranges} * {range_proof_interval} is smaller than submission_interval: {submission_interval}"
        ));
    }

    Ok(config)
}

/// Read slim proposer environment variables and return a minimal config.
pub async fn read_slim_proposer_env() -> Result<SlimProposerConfig> {
    let signer = Signer::from_env().await?;

    let nats_url = get_env_var("NATS_URL", Some("nats://localhost:4223".to_string()))?;
    let nats_stream = get_env_var("NATS_STREAM", None)?;
    let nats_input_subject = get_env_var("NATS_REQUEST_SUBJECT", None)?;
    let nats_output_subject = get_env_var("NATS_RESPONSE_SUBJECT", None)?;
    let nats_credentials_path = get_optional_env_var("NATS_CREDENTIALS_PATH")?;

    let gas_priority_fee = get_optional_env_var::<u128>("GAS_PRIORITY_FEE")?;
    let gas_max_fee = get_optional_env_var::<u128>("GAS_MAX_FEE")?;
    if let (Some(max_fee), Some(priority_fee)) = (gas_max_fee, gas_priority_fee) {
        if max_fee < priority_fee {
            anyhow::bail!("GAS_MAX_FEE ({max_fee}) must be >= GAS_PRIORITY_FEE ({priority_fee})");
        }
    }

    let config = SlimProposerConfig {
        // NATS configuration
        nats_url,
        nats_stream,
        nats_input_subject,
        nats_output_subject,
        nats_credentials_path,

        l1_rpc: get_env_var("L1_RPC", None)?,
        signer,
        l2oo_address: get_env_var("L2OO_ADDRESS", Some(Address::ZERO))?,

        gas_priority_fee,
        gas_max_fee,
    };

    Ok(config)
}
