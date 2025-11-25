use crate::client::ZircuitClientConfig;
use crate::message_bus::nats::NatsBusConfig;
use crate::message_bus::MessageBusVariant;
use crate::storage::{StorageConfig, StorageStrategy};
use crate::ProverType::Sp1Cpu;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    // postgres env config
    pub db_url: String,
    // nats env config
    pub nats_url: String,
    pub nats_stream: String,
    pub nats_output_subject: String,
    pub nats_input_subject: String,
    pub nats_credentials_path: Option<String>,

    // client env config
    pub proof_request_timeout: Duration,

    // storage config
    pub storage_strategy: StorageStrategy,
    pub storage_local_path: PathBuf,
    pub storage_s3_bucket: Option<String>,
    pub storage_s3_region: Option<String>,
    pub storage_s3_endpoint: Option<String>,
    pub storage_s3_no_credentials: bool,
    pub storage_s3_force_path_style: bool,
}

fn get_env_var_or<T>(key: &str, fallback: Option<T>) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("Failed to parse {}: {:?}", key, e)),
        Err(_) => match fallback {
            Some(default_val) => Ok(default_val),
            None => anyhow::bail!("{} is not set", key),
        },
    }
}

impl EnvironmentConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let db_url = get_env_var_or("DATABASE_URL", None)?;

        // NATS configuration with defaults
        let nats_url = get_env_var_or("ZC_NATS_URL", Some("nats://localhost:4223".to_string()))?;
        let nats_stream = get_env_var_or("ZC_NATS_STREAM", None)?;
        let nats_request_subject = get_env_var_or("ZC_NATS_REQUEST_SUBJECT", None)?;
        let nats_response_subject = get_env_var_or("ZC_NATS_RESPONSE_SUBJECT", None)?;
        let nats_credentials_path = get_env_var_or("NATS_CREDENTIALS_PATH", None).ok();

        // Optional timeout configuration
        let proof_request_timeout_secs =
            get_env_var_or::<u64>("ZC_PROOF_REQUEST_TIMEOUT_SECONDS", Some(4000))?;
        let proof_request_timeout = Duration::from_secs(proof_request_timeout_secs);

        // Storage configuration
        let storage_strategy: StorageStrategy =
            match get_env_var_or::<String>("ZC_STORAGE_STRATEGY", None) {
                Ok(p) => p.parse().unwrap_or_else(|e| {
                    tracing::warn!("Invalid STORAGE_STRATEGY value: {}. Defaulting to Local", e);
                    StorageStrategy::Local
                }),
                Err(_) => {
                    tracing::warn!("STORAGE_STRATEGY is not set, defaulting to Local");
                    StorageStrategy::Local
                }
            };

        let storage_local_path =
            get_env_var_or::<String>("ZC_STORAGE_LOCAL_PATH", Some("./storage".to_string()))?;
        let storage_local_path = PathBuf::from(storage_local_path);

        let storage_s3_bucket = get_env_var_or::<String>("ZC_STORAGE_S3_BUCKET", None).ok();
        let storage_s3_region =
            get_env_var_or::<String>("ZC_STORAGE_S3_REGION", Some("us-east-1".to_string())).ok();
        let storage_s3_endpoint = get_env_var_or::<String>("ZC_STORAGE_S3_ENDPOINT", None).ok();
        let storage_s3_no_credentials =
            get_env_var_or::<bool>("ZC_STORAGE_S3_NO_CREDENTIALS", Some(false))?;
        let storage_s3_force_path_style =
            get_env_var_or::<bool>("ZC_STORAGE_S3_FORCE_PATH_STYLE", Some(true))?;

        Ok(EnvironmentConfig {
            db_url,
            nats_url,
            nats_stream,
            nats_output_subject: nats_request_subject,
            nats_input_subject: nats_response_subject,
            nats_credentials_path,
            proof_request_timeout,
            storage_strategy,
            storage_local_path,
            storage_s3_bucket,
            storage_s3_region,
            storage_s3_endpoint,
            storage_s3_no_credentials,
            storage_s3_force_path_style,
        })
    }
}

pub fn client_config_from_env() -> anyhow::Result<ZircuitClientConfig> {
    let env_config = EnvironmentConfig::from_env()?;

    let storage_config = match env_config.storage_strategy {
        StorageStrategy::Local => StorageConfig::Local {
            base_path: env_config.storage_local_path,
        },
        StorageStrategy::S3 => {
            let bucket = env_config
                .storage_s3_bucket
                .ok_or_else(|| anyhow::anyhow!("s3 bucket is required when using S3 storage"))?;
            let region = env_config
                .storage_s3_region
                .ok_or_else(|| anyhow::anyhow!("s3 region is required when using S3 storage"))?;
            let endpoint = env_config
                .storage_s3_endpoint
                .ok_or_else(|| anyhow::anyhow!("s3 endpoint is required when using S3 storage"))?;

            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                no_credentials: env_config.storage_s3_no_credentials,
                force_path_style: env_config.storage_s3_force_path_style,
            }
        }
    };

    // we are assuming that nats is used as the message bus
    // the basic variant should only be used for testing
    let nats_config = NatsBusConfig {
        nats_url: env_config.nats_url,
        stream: env_config.nats_stream,
        input_subject: env_config.nats_input_subject,
        output_subject: env_config.nats_output_subject,
        credentials_path: env_config.nats_credentials_path,
    };

    Ok(ZircuitClientConfig {
        db_url: env_config.db_url,
        proof_request_timeout: env_config.proof_request_timeout,
        storage_config,
        message_bus_variant: MessageBusVariant::Nats(nats_config),
        prover_type: Sp1Cpu,
    })
}
