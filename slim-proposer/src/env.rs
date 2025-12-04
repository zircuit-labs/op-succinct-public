use alloy_primitives::Address;
use anyhow::Result;
use op_succinct_signer_utils::Signer;
use std::env;

#[derive(Debug, Clone)]
pub struct SlimProposerConfig {
    pub nats_url: String,
    pub nats_stream: String,
    pub nats_input_subject: String,
    pub nats_output_subject: String,
    pub nats_credentials_path: Option<String>,

    pub signer: Signer,
    pub l2oo_address: Address,

    // tx fee overrides
    pub gas_priority_fee: Option<u128>,
    pub gas_max_fee: Option<u128>,
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
        nats_url,
        nats_stream,
        nats_input_subject,
        nats_output_subject,
        nats_credentials_path,

        signer,
        l2oo_address: get_env_var("L2OO_ADDRESS", Some(Address::ZERO))?,

        gas_priority_fee,
        gas_max_fee,
    };

    Ok(config)
}
