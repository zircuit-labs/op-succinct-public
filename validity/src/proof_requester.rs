use alloy_primitives::{hex, Address, B256};
use alloy_provider::Provider;
use anyhow::{anyhow, Context, Result};
use num_traits::cast::ToPrimitive;
use op_succinct_client_utils::boot::BootInfoStruct;
use op_succinct_elfs::AGGREGATION_ELF;
use op_succinct_host_utils::{
    fetcher::OPSuccinctDataFetcher,
    get_agg_proof_stdin,
    host::OPSuccinctHost,
    metrics::MetricsGauge,
    stats::{ExecutionParams, ExecutionStats},
    witness_generation::WitnessGenerator,
};
use op_succinct_proof_utils::get_range_elf_embedded;
use sp1_sdk::{
    network::{proto::network::ExecutionStatus, FulfillmentStrategy},
    CpuProver, SP1Proof, SP1ProofMode, SP1ProofWithPublicValues, SP1Stdin, SP1_CIRCUIT_VERSION,
};
use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{task, time};
use tracing::{debug, error, info, warn};

use crate::client_proxy::ClientProxy;
use crate::{
    db::DriverDBClient, OPSuccinctRequest, ProgramConfig, ProofRequesterError,
    RequestExecutionStatistics, RequestMode, RequestStatus, RequestType, ValidityGauge,
};

pub struct OPSuccinctProofRequester<H: OPSuccinctHost> {
    pub host: Arc<H>,
    pub network_prover: Arc<CpuProver>,
    pub client: Arc<ClientProxy>,
    pub fetcher: Arc<OPSuccinctDataFetcher>,
    pub db_client: Arc<DriverDBClient>,
    pub program_config: ProgramConfig,
    pub mock: bool,
    pub range_strategy: FulfillmentStrategy,
    pub agg_strategy: FulfillmentStrategy,
    pub agg_mode: SP1ProofMode,
    pub safe_db_fallback: bool,
    pub cost_estimator_timeout_secs: u64,
    pub witness_gen_timeout_secs: u64,
    pub instruction_count_limit: u64,
    pub max_exec_timeout_retries: u64,
}

impl<H: OPSuccinctHost> OPSuccinctProofRequester<H> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: Arc<H>,
        network_prover: Arc<CpuProver>,
        client: Arc<ClientProxy>,
        fetcher: Arc<OPSuccinctDataFetcher>,
        db_client: Arc<DriverDBClient>,
        program_config: ProgramConfig,
        mock: bool,
        range_strategy: FulfillmentStrategy,
        agg_strategy: FulfillmentStrategy,
        agg_mode: SP1ProofMode,
        safe_db_fallback: bool,
        cost_estimator_timeout_secs: u64,
        witness_gen_timeout_secs: u64,
        instruction_count_limit: u64,
        max_exec_timeout_retries: u64,
    ) -> Self {
        Self {
            host,
            network_prover,
            client,
            fetcher,
            db_client,
            program_config,
            mock,
            range_strategy,
            agg_strategy,
            agg_mode,
            safe_db_fallback,
            cost_estimator_timeout_secs,
            witness_gen_timeout_secs,
            instruction_count_limit,
            max_exec_timeout_retries,
        }
    }

    /// Generates the witness for a range proof.
    pub async fn range_proof_witnessgen(&self, request: &OPSuccinctRequest) -> Result<SP1Stdin> {
        let host_args = self
            .host
            .fetch(
                request.start_block as u64,
                request.end_block as u64,
                None,
                self.safe_db_fallback,
            )
            .await?;

        if let Some(l1_head) = self.host.get_l1_head_hash(&host_args) {
            let l1_head_block_number = self.fetcher.get_l1_header(l1_head.into()).await?.number;
            self.db_client
                .update_l1_head_block_number(request.id, l1_head_block_number as i64)
                .await?;
        }

        let witness = self.host.run(&host_args).await?;
        let sp1_stdin = self
            .host
            .witness_generator()
            .get_sp1_stdin(witness)
            .unwrap();

        Ok(sp1_stdin)
    }

    /// Generates the witness for an aggregation proof.
    #[allow(clippy::too_many_arguments)]
    pub async fn agg_proof_witnessgen(
        &self,
        start_block: i64,
        end_block: i64,
        checkpointed_l1_block_hash: B256,
        l1_chain_id: i64,
        l2_chain_id: i64,
        prover_address: Address,
        preroot: Option<Vec<u8>>,
        postroot: Option<Vec<u8>>,
    ) -> Result<(SP1Stdin, RequestMode)> {
        // Fetch consecutive range proofs from the database.
        let range_proofs = self
            .db_client
            .get_consecutive_complete_range_proofs(
                start_block,
                end_block,
                &self.program_config.commitments,
                l1_chain_id,
                l2_chain_id,
            )
            .await?;

        if range_proofs.len() > 1 {
            // Check if they are consecutive
            let mut expected_root = range_proofs.first().unwrap().postroot.as_ref();
            range_proofs.iter().skip(1).for_each(|range| {
                    assert_eq!(
                        range.preroot.as_ref(),
                        expected_root,
                        "Pre proof {:?} should equal expected root {:?}. Inner range id: {:?}, start block: {}, end block: {}",
                        range.preroot.as_ref(),
                        expected_root,
                        range.id,
                        range.start_block,
                        range.end_block
                    );
                    expected_root = range.postroot.as_ref();
                });
        } else {
            tracing::warn!(
                "range_proofs has less than 2 items, len: {}",
                range_proofs.len()
            );
        }

        if !range_proofs.is_empty() {
            if let Some(first) = range_proofs.first() {
                assert_eq!(
                    first.preroot, preroot,
                    "preroot mismatch in range_proofs: {range_proofs:?}"
                );
            }
            if let Some(last) = range_proofs.last() {
                assert_eq!(
                    last.postroot, postroot,
                    "postroot mismatch in range_proofs: {range_proofs:?}"
                );
            }
        }

        let request_mode = if range_proofs
            .iter()
            .all(|request| request.mode == RequestMode::Real)
        {
            RequestMode::Real
        } else {
            warn!("Found at least one mock range proofs in agg proof request");
            RequestMode::Mock
        };
        // Deserialize the proofs and extract the boot infos and proofs.
        let (boot_infos, proofs): (Vec<BootInfoStruct>, Vec<SP1Proof>) = range_proofs
            .iter()
            .map(|proof| {
                // TODO: This should be reverted once Sindri supports sp1-sdk >=4.2.0
                let mut proof_with_pv: SP1ProofWithPublicValues =
                    bincode::deserialize(proof.proof.as_ref().unwrap())
                        .expect("Deserialization failure for range proof");
                let boot_info: BootInfoStruct = proof_with_pv.public_values.read();
                assert_eq!(Some(boot_info.l2PreRoot.to_vec()), proof.preroot);
                assert_eq!(Some(boot_info.l2PostRoot.to_vec()), proof.postroot);
                (boot_info, proof_with_pv.proof.clone())
            })
            .unzip();

        // This can fail for a few reasons:
        // 1. The L1 RPC is down (e.g. error code 32001). Double-check the L1 RPC is running
        //    correctly.
        // 2. The L1 head was re-orged and the block is no longer available. This is unlikely given
        //    we wait for 3 confirmations on a transaction.
        let headers = self
            .fetcher
            .get_header_preimages(&boot_infos, checkpointed_l1_block_hash)
            .await
            .context("Failed to get header preimages")?;

        let stdin = get_agg_proof_stdin(
            proofs,
            boot_infos,
            headers,
            &self.program_config.range_vk,
            checkpointed_l1_block_hash,
            prover_address,
        )
        .context("Failed to get agg proof stdin")?;

        Ok((stdin, request_mode))
    }

    /// Requests a range proof via the network prover.
    pub async fn request_range_proof(
        &self,
        stdin: SP1Stdin,
        metadata: HashMap<String, String>,
    ) -> Result<B256> {
        let proof_info = self
            .client
            .request_proof(RequestType::Range, stdin, Some(metadata))
            .await
            .map_err(|e| {
                ValidityGauge::RangeProofRequestErrorCount.increment(1.0);
                anyhow::anyhow!("Failed to request range proof: {e:?}")
            })?;

        let proof_id = proof_info.request_id;

        // Convert UUID string to bytes
        let uuid_bytes = uuid::Uuid::parse_str(&proof_id)
            .map_err(|e| anyhow::anyhow!("Failed to parse UUID: {}", e))?
            .as_bytes()
            .to_vec();

        // Pad to 32 bytes with zeros
        let mut padded_bytes = vec![0u8; 32];
        padded_bytes[..16].copy_from_slice(&uuid_bytes);

        // Convert to B256
        let proof_id_bytes = B256::from_slice(&padded_bytes);

        info!(
            proof_id = hex::encode(proof_id_bytes),
            "Requested range proof"
        );

        Ok(proof_id_bytes)
    }

    /// Requests an aggregation proof via the network prover.
    pub async fn request_agg_proof(
        &self,
        stdin: SP1Stdin,
        metadata: HashMap<String, String>,
    ) -> Result<B256> {
        let proof_info = self
            .client
            .request_proof(RequestType::Aggregation, stdin, Some(metadata))
            .await
            .map_err(|e| {
                ValidityGauge::AggProofRequestErrorCount.increment(1.0);
                anyhow::anyhow!("Failed to request aggregation proof: {e:?}")
            })?;

        let proof_id = proof_info.request_id;

        // Convert UUID string to bytes
        let uuid_bytes = uuid::Uuid::parse_str(&proof_id)
            .map_err(|e| anyhow::anyhow!("Failed to parse UUID: {}", e))?
            .as_bytes()
            .to_vec();

        // Pad to 32 bytes with zeros
        let mut padded_bytes = vec![0u8; 32];
        padded_bytes[..16].copy_from_slice(&uuid_bytes);

        // Convert to B256
        let proof_id_bytes = B256::from_slice(&padded_bytes);

        Ok(proof_id_bytes)
    }

    /// Generates a mock range proof and writes the execution statistics to the database.
    pub async fn generate_mock_range_proof(
        &self,
        request: &OPSuccinctRequest,
        stdin: SP1Stdin,
    ) -> Result<SP1ProofWithPublicValues> {
        info!(
            request_id = request.id,
            request_type = ?request.req_type,
            start_block = request.start_block,
            end_block = request.end_block,
            "Executing mock range proof"
        );

        let start_time = Instant::now();
        let network_prover = self.network_prover.clone();
        // Move the CPU-intensive operation to a dedicated thread.
        let (pv, report) = match tokio::task::spawn_blocking(move || {
            network_prover
                .execute(get_range_elf_embedded(), &stdin)
                .run()
        })
        .await?
        {
            Ok((pv, report)) => (pv, report),
            Err(e) => {
                ValidityGauge::ExecutionErrorCount.increment(1.0);
                return Err(e);
            }
        };

        let execution_duration = start_time.elapsed().as_secs();

        info!(
            request_id = request.id,
            request_type = ?request.req_type,
            start_block = request.start_block,
            end_block = request.end_block,
            duration_s = execution_duration,
            "Executed mock range proof.",
        );

        let execution_statistics = RequestExecutionStatistics::new(report);

        // Write the execution data to the database.
        self.db_client
            .insert_execution_statistics(
                request.id,
                serde_json::to_value(execution_statistics)?,
                execution_duration as i64,
            )
            .await?;

        Ok(SP1ProofWithPublicValues::create_mock_proof(
            &self.program_config.range_pk,
            pv.clone(),
            SP1ProofMode::Compressed,
            SP1_CIRCUIT_VERSION,
        ))
    }

    /// Generates a mock aggregation proof.
    pub async fn generate_mock_agg_proof(
        &self,
        request: &OPSuccinctRequest,
        stdin: SP1Stdin,
    ) -> Result<SP1ProofWithPublicValues> {
        let start_time = Instant::now();
        let network_prover = self.network_prover.clone();
        // Move the CPU-intensive operation to a dedicated thread.
        let (pv, report) = match tokio::task::spawn_blocking(move || {
            network_prover
                .execute(AGGREGATION_ELF, &stdin)
                .deferred_proof_verification(false)
                .run()
        })
        .await?
        {
            Ok((pv, report)) => (pv, report),
            Err(e) => {
                ValidityGauge::ExecutionErrorCount.increment(1.0);
                return Err(e);
            }
        };

        let execution_duration = start_time.elapsed().as_secs();

        info!(
            request_id = request.id,
            request_type = ?request.req_type,
            start_block = request.start_block,
            end_block = request.end_block,
            duration_s = execution_duration,
            "Executed mock aggregation proof.",
        );

        let execution_statistics = RequestExecutionStatistics::new(report);

        // Write the execution data to the database.
        self.db_client
            .insert_execution_statistics(
                request.id,
                serde_json::to_value(execution_statistics)?,
                execution_duration as i64,
            )
            .await?;

        Ok(SP1ProofWithPublicValues::create_mock_proof(
            &self.program_config.agg_pk,
            pv.clone(),
            self.agg_mode,
            SP1_CIRCUIT_VERSION,
        ))
    }

    pub async fn split_range(&self, request: OPSuccinctRequest, mut parts: usize) -> Result<()> {
        // Validate that the range can be split
        if request.end_block - request.start_block <= 1 {
            return Err(anyhow!("Cannot split range with only one block"));
        }
        let num_blocks = (request.end_block - request.start_block) as usize;
        if parts > num_blocks {
            parts = num_blocks;
        }
        if parts < 2 {
            return Err(anyhow!("Cannot split range into {parts} parts"));
        }

        info!(
            request_id = request.id,
            start_block = request.start_block,
            end_block = request.end_block,
            "Splitting range request"
        );
        let l1_chain_id = self.fetcher.l1_provider.get_chain_id().await?;
        let l2_chain_id = self.fetcher.l2_provider.get_chain_id().await?;

        let base = num_blocks / parts; // minimal chunk size
        let extra = num_blocks % parts;

        let mut start = request.start_block;
        let mut preroot = request.preroot.clone();
        let mut new_requests = vec![];
        for i in 0..parts {
            let size = base as i64 + if i < extra { 1 } else { 0 };
            let end = start + size;
            let root_resp = self.fetcher.get_l2_output_at_block(end as u64).await?;
            let root = root_resp.output_root.to_vec();

            new_requests.push(
                OPSuccinctRequest::create_range_request(
                    request.mode,
                    start,
                    end,
                    self.program_config.commitments.range_vkey_commitment,
                    self.program_config.commitments.rollup_config_hash,
                    l1_chain_id as i64,
                    l2_chain_id as i64,
                    preroot,
                    Some(root.clone()),
                    self.fetcher.clone(),
                    self.max_exec_timeout_retries as i64,
                )
                .await?,
            );
            start = end;
            preroot = Some(root);
        }

        self.db_client.insert_requests(&new_requests).await?;

        Ok(())
    }

    /// Handles a failed proof request.
    ///
    /// If the request is a range proof and the number of failed requests is greater than 2 or the
    /// execution status is unexecutable, the request is split into two new requests. Otherwise,
    /// add_new_ranges will insert the new request. This ensures better failure-resilience. If the
    /// request to add two range requests fails, add_new_ranges will handle it gracefully by
    /// submitting the same range.
    #[tracing::instrument(
        name = "proof_requester.handle_failed_request",
        skip(self, request, execution_status)
    )]
    pub async fn handle_failed_request(
        &self,
        request: OPSuccinctRequest,
        execution_status: ExecutionStatus,
    ) -> Result<()> {
        warn!(
            id = request.id,
            start_block = request.start_block,
            end_block = request.end_block,
            req_type = ?request.req_type,
            "Setting request to failed"
        );

        // Mark the existing request as failed.
        self.db_client
            .update_request_status(request.id, RequestStatus::Failed)
            .await?;

        if request.end_block - request.start_block > 1 && request.req_type == RequestType::Range {
            let num_failed_requests = self
                .db_client
                .fetch_failed_request_count_by_block_range(
                    request.start_block,
                    request.end_block,
                    request.l1_chain_id,
                    request.l2_chain_id,
                    &self.program_config.commitments,
                )
                .await?;

            // NOTE: The failed_requests check here can be removed in V5.
            if num_failed_requests > 2 || execution_status == ExecutionStatus::Unexecutable {
                info!("Splitting failed request into two: {:?}", request.id);
                return self.split_range(request, 2).await;
            }
        }

        Ok(())
    }

    /// Generates the stdin needed for a proof.
    async fn generate_proof_stdin(
        &self,
        request: &OPSuccinctRequest,
    ) -> Result<(SP1Stdin, RequestMode)> {
        let stdin = match request.req_type {
            RequestType::Range => {
                let stdin = self.range_proof_witnessgen(request).await?;
                (stdin, request.mode)
            }
            RequestType::Aggregation => {
                self.agg_proof_witnessgen(
                    request.start_block,
                    request.end_block,
                    B256::from_slice(request.checkpointed_l1_block_hash.as_ref().ok_or_else(
                        || anyhow::anyhow!("Aggregation proof has no checkpointed block."),
                    )?),
                    request.l1_chain_id,
                    request.l2_chain_id,
                    Address::from_slice(request.prover_address.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("Prover address must be set for aggregation proofs.")
                    })?),
                    request.preroot.clone(),
                    request.postroot.clone(),
                )
                .await?
            }
        };

        Ok(stdin)
    }

    /// Makes a proof request by updating statuses, generating witnesses, and then either requesting
    /// or mocking the proof depending on configuration.
    ///
    /// Note: Any error from this function will cause the proof to be retried.
    #[tracing::instrument(
        name = "proof_requester.make_proof_request",
        skip(self, request, high_memory_threshold)
    )]
    pub async fn make_proof_request(
        &self,
        request: OPSuccinctRequest,
        high_memory_threshold: Option<u64>,
    ) -> Result<(), ProofRequesterError> {
        // Update status to WitnessGeneration.
        self.db_client
            .update_request_status(request.id, RequestStatus::WitnessGeneration)
            .await?;

        info!(
            request_id = request.id,
            request_type = ?request.req_type,
            start_block = request.start_block,
            end_block = request.end_block,
            "Starting witness generation"
        );

        let (stdin, witness_gen_request_mode, duration) =
            match time::timeout(Duration::from_secs(self.witness_gen_timeout_secs), async {
                let witnessgen_duration = Instant::now();
                // Generate the stdin needed for the proof. If this fails, retry the request.
                let (stdin, witness_gen_request_mode) =
                    match self.generate_proof_stdin(&request).await {
                        Ok(witness) => witness,
                        Err(e) => {
                            ValidityGauge::WitnessgenErrorCount.increment(1.0);
                            return Err(e);
                        }
                    };
                let duration = witnessgen_duration.elapsed();

                Ok((stdin, witness_gen_request_mode, duration))
            })
            .await
            {
                Ok(Ok(witness)) => witness,
                Ok(Err(e)) => {
                    return Err(e.into());
                }
                Err(_) => {
                    error!(
                        request_id = request.id,
                        request_type = ?request.req_type,
                        start_block = request.start_block,
                        end_block = request.end_block,
                        "Witness generation timed out after {}s",
                        self.witness_gen_timeout_secs
                    );
                    return Err(ProofRequesterError::WitnessGenTimeout);
                }
            };

        self.db_client
            .update_witnessgen_duration(request.id, duration.as_secs() as i64)
            .await?;

        info!(
            request_id = request.id,
            start_block = request.start_block,
            end_block = request.end_block,
            request_type = ?request.req_type,
            duration_s = duration.as_secs(),
            "Completed witness generation"
        );

        // For mock mode, update status to Execution before proceeding.
        if request.mode == RequestMode::Mock {
            self.db_client
                .update_request_status(request.id, RequestStatus::Execution)
                .await?;
        }

        match request.req_type {
            RequestType::Range => {
                let metadata = match self
                    .build_range_metadata(&request, &stdin, high_memory_threshold)
                    .await
                {
                    Ok(metadata) => {
                        let instruction_count = metadata
                            .get("total_instruction_count")
                            .ok_or(anyhow!("Instruction count is not a valid number"))?
                            .parse::<u64>()
                            .map_err(|e| anyhow!(e))?;

                        let derivation_count = metadata
                            .get("derivation_instruction_count")
                            .ok_or(anyhow!(
                                "Derivation instruction count is not a valid number"
                            ))?
                            .parse::<u64>()
                            .map_err(|e| anyhow!(e))?;

                        if instruction_count < derivation_count {
                            return Err(
                                ProofRequesterError::InstructionCountSmallerThanDerivation(
                                    instruction_count,
                                    derivation_count,
                                ),
                            );
                        }
                        let count = instruction_count - derivation_count;
                        if request.end_block - request.start_block > 1
                            && count > self.instruction_count_limit
                        {
                            return Err(ProofRequesterError::InstructionCountTooLarge(count));
                        }
                        metadata
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Metadata can not be computed for request with id {}",
                            request.id
                        );

                        return Err(e);
                    }
                };
                match request.mode {
                    RequestMode::Real => {
                        let proof_id = self.request_range_proof(stdin, metadata).await?;
                        self.db_client
                            .update_request_to_prove(request.id, proof_id)
                            .await?;
                    }
                    RequestMode::Mock => {
                        let proof = self.generate_mock_range_proof(&request, stdin).await?;
                        let proof_bytes = bincode::serialize(&proof).map_err(|e| anyhow!(e))?;
                        self.db_client
                            .update_proof_to_complete(request.id, &proof_bytes, B256::ZERO)
                            .await?;
                    }
                }
            }
            RequestType::Aggregation => {
                let metadata = self
                    .build_agg_metadata(&request, &stdin, high_memory_threshold)
                    .await?;

                match (request.mode, witness_gen_request_mode) {
                    // compute real proof only if the request mode is real and all range proofs are real
                    (RequestMode::Real, RequestMode::Real) => {
                        let proof_id = self.request_agg_proof(stdin, metadata).await?;
                        self.db_client
                            .update_request_to_prove(request.id, proof_id)
                            .await?;
                    }
                    _ => {
                        // if any mock range proofs were found during the witness gen, update it as a mock proof
                        if request.mode == RequestMode::Real
                            && witness_gen_request_mode == RequestMode::Mock
                        {
                            warn!("Found mock range proofs during witness gen. Updating request mode and using mock prover.");
                            self.db_client
                                .update_request_mode(request.id, RequestMode::Mock)
                                .await?;
                        }

                        let proof = self.generate_mock_agg_proof(&request, stdin).await?;
                        self.db_client
                            .update_proof_to_complete(request.id, &proof.bytes(), B256::ZERO)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn build_range_metadata(
        &self,
        request: &OPSuccinctRequest,
        stdin: &SP1Stdin,
        high_memory_threshold: Option<u64>,
    ) -> Result<HashMap<String, String>, ProofRequesterError> {
        let (execution_duration, execution_stats) = self
            .compute_and_store_execution_stats(request, stdin, high_memory_threshold)
            .await?;

        let mut metadata = HashMap::new();
        metadata.insert(
            "report_execution_duration_ms".to_string(),
            execution_duration.to_string(),
        );
        metadata.insert(
            "total_instruction_count".to_string(),
            execution_stats.total_instruction_count.to_string(),
        );
        metadata.insert(
            "oracle_verify_instruction_count".to_string(),
            execution_stats.oracle_verify_instruction_count.to_string(),
        );
        metadata.insert(
            "derivation_instruction_count".to_string(),
            execution_stats.derivation_instruction_count.to_string(),
        );
        metadata.insert(
            "block_execution_instruction_count".to_string(),
            execution_stats
                .block_execution_instruction_count
                .to_string(),
        );
        metadata.insert(
            "blob_verification_instruction_count".to_string(),
            execution_stats
                .blob_verification_instruction_count
                .to_string(),
        );
        metadata.insert(
            "cycles_per_block".to_string(),
            execution_stats.cycles_per_block.to_string(),
        );
        metadata.insert(
            "cycles_per_transaction".to_string(),
            execution_stats.cycles_per_transaction.to_string(),
        );
        metadata.insert(
            "eth_gas_used".to_string(),
            execution_stats.eth_gas_used.to_string(),
        );
        let eth_gas_used_per_block = if execution_stats.nb_blocks > 0 {
            execution_stats.eth_gas_used / execution_stats.nb_blocks
        } else {
            0
        };
        metadata.insert(
            "eth_gas_used_per_block".to_string(),
            eth_gas_used_per_block.to_string(),
        );
        metadata.insert(
            "total_sp1_gas".to_string(),
            execution_stats.total_sp1_gas.to_string(),
        );
        metadata.insert(
            "gas_used_per_block".to_string(),
            execution_stats.gas_used_per_block.to_string(),
        );
        metadata.insert(
            "total_nb_transactions".to_string(),
            request.total_nb_transactions.to_string(),
        );

        metadata.insert("request_id".to_string(), request.id.to_string());
        metadata.insert("start_block".to_string(), request.start_block.to_string());
        metadata.insert("end_block".to_string(), request.end_block.to_string());
        metadata.insert(
            "preroot".to_string(),
            request
                .preroot
                .as_ref()
                .map(hex::encode)
                .unwrap_or_default(),
        );
        metadata.insert(
            "postroot".to_string(),
            request
                .postroot
                .as_ref()
                .map(hex::encode)
                .unwrap_or_default(),
        );
        metadata.insert(
            "high_memory".to_string(),
            execution_stats.use_high_memory().to_string(),
        );

        Ok(metadata)
    }

    async fn build_agg_metadata(
        &self,
        request: &OPSuccinctRequest,
        stdin: &SP1Stdin,
        high_memory_threshold: Option<u64>,
    ) -> Result<HashMap<String, String>> {
        let (execution_duration, execution_stats) = self
            .compute_and_store_execution_stats(request, stdin, high_memory_threshold)
            .await?;

        let mut metadata = HashMap::new();
        metadata.insert(
            "report_execution_duration_ms".to_string(),
            execution_duration.to_string(),
        );
        metadata.insert(
            "total_instruction_count".to_string(),
            execution_stats.total_instruction_count.to_string(),
        );
        metadata.insert(
            "cycles_per_block".to_string(),
            execution_stats.cycles_per_block.to_string(),
        );

        metadata.insert(
            "total_sp1_gas".to_string(),
            execution_stats.total_sp1_gas.to_string(),
        );

        metadata.insert("request_id".to_string(), request.id.to_string());
        metadata.insert("start_block".to_string(), request.start_block.to_string());
        metadata.insert("end_block".to_string(), request.end_block.to_string());
        metadata.insert(
            "preroot".to_string(),
            request
                .preroot
                .as_ref()
                .map(hex::encode)
                .unwrap_or_default(),
        );
        metadata.insert(
            "postroot".to_string(),
            request
                .postroot
                .as_ref()
                .map(hex::encode)
                .unwrap_or_default(),
        );
        metadata.insert(
            "contract_root".to_string(),
            request
                .contract_root
                .as_ref()
                .map(hex::encode)
                .unwrap_or_default(),
        );
        metadata.insert(
            "high_memory".to_string(),
            execution_stats.use_high_memory().to_string(),
        );

        Ok(metadata)
    }

    async fn compute_and_store_execution_stats(
        &self,
        request: &OPSuccinctRequest,
        stdin: &SP1Stdin,
        high_memory_threshold: Option<u64>,
    ) -> Result<(u128, ExecutionStats), ProofRequesterError> {
        let start_time = Instant::now();
        let network_prover = self.network_prover.clone();
        let cloned_stdin = stdin.clone();
        let cloned_requests = request.clone();
        let l1_head_hash = self
            .host
            .calculate_safe_l1_head(
                &self.fetcher,
                request.end_block as u64,
                self.safe_db_fallback,
            )
            .await?;

        let (l1_head_number, l1_origin, l1_txs_origin_to_head, l1_da_txs_origin_to_head) = self
            .fetcher
            .get_l1_traversal_stats(request.start_block as u64, l1_head_hash)
            .await?;
        let handle = task::spawn_blocking(move || {
            catch_unwind(AssertUnwindSafe(
                || -> Result<ExecutionStats, anyhow::Error> {
                    info!(
                        "EYECATCHER: About to generate report for {:?} [{:?}–{:?}]",
                        &cloned_requests.req_type,
                        cloned_requests.start_block,
                        cloned_requests.end_block
                    );
                    let run_res = match cloned_requests.req_type {
                        RequestType::Range => network_prover
                            .execute(get_range_elf_embedded(), &cloned_stdin)
                            .run(),
                        RequestType::Aggregation => network_prover
                            .execute(AGGREGATION_ELF, &cloned_stdin)
                            .deferred_proof_verification(false)
                            .run(),
                    };

                    match run_res {
                        Ok((_pv, report)) => {
                            let execution_params = ExecutionParams {
                                is_agg: cloned_requests.req_type == RequestType::Aggregation,
                                l1_head: l1_head_number,
                                l1_origin,
                                l1_txs_origin_to_head,
                                l1_da_txs_origin_to_head,
                                batch_start: cloned_requests.start_block as u64,
                                batch_end: cloned_requests.end_block as u64,
                                nb_transactions: cloned_requests.total_nb_transactions as u64,
                                l1_fees: cloned_requests.total_l1_fees.to_i128().unwrap() as u128,
                                eth_gas_used: cloned_requests.total_eth_gas_used as u64,
                                total_tx_fees: cloned_requests.total_tx_fees.to_i128().unwrap()
                                    as u128,
                                witness_generation_time_sec: 0,
                                total_execution_time_sec: 0,
                            };
                            let execution_stats = ExecutionStats::new_raw(
                                &execution_params,
                                &report,
                                high_memory_threshold,
                            );
                            Ok(execution_stats)
                        }
                        Err(e) => {
                            error!("ERROR generating report: {:?}", e);
                            Err(e)
                        }
                    }
                },
            ))
        });

        let execution_stats = match time::timeout(
            Duration::from_secs(self.cost_estimator_timeout_secs),
            handle,
        )
        .await
        {
            Ok(join_res) => {
                // did the blocking thread panic?
                let panicked = join_res
                    .map_err(|join_err| anyhow!("Proof thread panicked: {:?}", join_err))?;
                // did our inner catch_unwind catch a panic?
                match panicked {
                    Ok(Ok(report)) => report,
                    Ok(Err(prover_err)) => {
                        error!("Prover error: {:?}", prover_err);

                        return Err(prover_err.into());
                    }
                    Err(panic_payload) => {
                        // an unwound panic
                        error!("Proof generation panicked: {:?}", panic_payload);

                        return Err(anyhow!("Proof generation panicked").into());
                    }
                }
            }
            // timed out
            Err(_) => {
                // best effort: cancel the handle so it doesn’t hold resources
                // (spawn_blocking cannot truly kill the thread, but this drops the JoinHandle)
                // Note: you can also call `handle.abort()` here if you want
                error!(
                    "Proof execution (cost estimation) timed out after {}s",
                    self.cost_estimator_timeout_secs
                );

                return Err(ProofRequesterError::MetadataComputeTimeout);
            }
        };

        let execution_duration = start_time.elapsed().as_millis();
        debug!(
            "It took {execution_duration} ms to create range proof report for range [{}-{}]",
            request.start_block, request.end_block
        );

        self.db_client
            .insert_execution_stats(request.id, execution_duration as i64, &execution_stats)
            .await?;

        Ok((execution_duration, execution_stats))
    }
}
