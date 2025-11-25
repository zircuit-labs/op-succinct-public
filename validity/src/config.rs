use crate::client_proxy::ClientVariant;
use alloy_primitives::{Address, B256};
use alloy_provider::Provider;
use op_succinct_host_utils::{
    DisputeGameFactory::DisputeGameFactoryInstance as DisputeGameFactoryContract,
    OPSuccinctL2OutputOracle::OPSuccinctL2OutputOracleInstance as OPSuccinctL2OOContract,
};
use sp1_sdk::{network::FulfillmentStrategy, SP1ProofMode, SP1ProvingKey, SP1VerifyingKey};
use std::sync::Arc;

const RANGE_CIRCUIT_NAME: &str = "range-elf-embedded";
const AGGREGATION_CIRCUIT_NAME: &str = "aggregation-elf";

pub struct ContractConfig<P>
where
    P: Provider + 'static,
{
    pub l2oo_address: Address,
    pub dgf_address: Address,
    pub l2oo_contract: OPSuccinctL2OOContract<P>,
    pub dgf_contract: DisputeGameFactoryContract<P>,
}

#[derive(Debug, Clone)]
pub struct CommitmentConfig {
    pub range_vkey_commitment: B256,
    pub agg_vkey_hash: B256,
    pub rollup_config_hash: B256,
}

#[derive(Clone)]
pub struct ProgramConfig {
    pub range_vk: Arc<SP1VerifyingKey>,
    pub range_pk: Arc<SP1ProvingKey>,
    pub agg_vk: Arc<SP1VerifyingKey>,
    pub agg_pk: Arc<SP1ProvingKey>,
    pub commitments: CommitmentConfig,
}

impl ProgramConfig {
    pub fn range_circuit_id(&self) -> String {
        format!(
            "{}:{}",
            RANGE_CIRCUIT_NAME, self.commitments.range_vkey_commitment
        )
    }

    pub fn aggregation_circuit_id(&self) -> String {
        format!(
            "{}:{}",
            AGGREGATION_CIRCUIT_NAME, self.commitments.agg_vkey_hash
        )
    }
}

pub struct RequesterConfig {
    pub l1_chain_id: i64,
    pub l2_chain_id: i64,
    // The address being committed to when generating the aggregation proof to prevent
    // front-running attacks. This should be the same address that is being used to send
    // `proposeL2Output` transactions.
    pub prover_address: Address,
    pub l2oo_address: Address,
    pub dgf_address: Address,
    pub range_proof_interval: u64,
    pub submission_interval: u64,
    pub max_aggregated_ranges: u64,
    pub max_concurrent_witness_gen: u64,
    pub max_concurrent_proof_requests: u64,
    pub max_new_ranges_per_iteration: u64,
    pub cost_estimator_timeout_secs: u64,
    pub witness_gen_timeout_secs: u64,
    pub max_exec_timeout_retries: u64,
    pub range_proof_strategy: FulfillmentStrategy,
    pub agg_proof_strategy: FulfillmentStrategy,
    pub agg_proof_mode: SP1ProofMode,
    /// Block from which the proposer should start proposing
    pub fork_start_block: Option<u64>,
    /// The start block to use instead of the latest proposed block from the contract. Intended for simulations and testing only.
    pub force_start_block: Option<u64>,
    /// If true, do not create or submit aggregation proofs and L2OO submissions. Enabled by default if `force_start_block` is set. Used for simulations and testing.
    pub skip_aggregation_proofs: bool,
    pub mock: bool,
    /// Whether to fallback to timestamp-based L1 head estimation even though SafeDB is not
    /// activated for op-node.
    pub safe_db_fallback: bool,
    pub high_memory_threshold: Option<u64>,
    pub instruction_count_limit: u64,
    pub split_on_timeout: bool,
    /// If this value is set range proof generation will halt and aggregation will proceed
    /// until there's no more range proofs
    pub set_for_upgrade: bool,
    pub proof_request_client: ClientVariant,
}
