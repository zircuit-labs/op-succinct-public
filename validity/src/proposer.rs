use std::{collections::HashMap, env, str::FromStr, sync::Arc, time::Duration};

use crate::client_proxy::ClientProxy;
use crate::ProofRequesterError;
use crate::{
    db::{DriverDBClient, OPSuccinctRequest, RequestMode, RequestStatus},
    find_gaps, get_latest_proposed_block_number, get_latest_proposed_root, get_ranges_to_prove,
    CommitmentConfig, ContractConfig, OPSuccinctProofRequester, ProgramConfig, RequesterConfig,
    ValidityGauge,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_provider::{network::ReceiptResponse, Provider};
use alloy_sol_types::SolValue;
use anyhow::{anyhow, bail, Context, Result};
use futures_util::{stream, StreamExt, TryStreamExt};
use op_succinct_client_utils::boot::BootInfoStruct;
use op_succinct_client_utils::{boot::hash_rollup_config, types::u32_to_u8};
use op_succinct_elfs::AGGREGATION_ELF;
use op_succinct_host_utils::{
    fetcher::OPSuccinctDataFetcher, host::OPSuccinctHost, metrics::MetricsGauge,
    DisputeGameFactory::DisputeGameFactoryInstance as DisputeGameFactoryContract,
    OPSuccinctL2OutputOracle::OPSuccinctL2OutputOracleInstance as OPSuccinctL2OOContract,
};
use op_succinct_proof_utils::get_range_elf_embedded;
use op_succinct_signer_utils::Signer;
use sp1_sdk::SP1ProofWithPublicValues;
use sp1_sdk::{
    network::proto::network::ExecutionStatus, CpuProver, HashableKey, Prover, ProverClient,
    SP1Proof,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use zircuit_client::JobStatus;

struct RangesRoots {
    start_block: i64,
    end_block: i64,
    preroot: B256,
    postroot: B256,
}

#[derive(Clone)]
pub struct CompletedRangeProofInfo {
    pub start_block: i64,
    pub end_block: i64,
    pub l1_head_block: Option<i64>,
}

/// Configuration for the driver.
pub struct DriverConfig {
    pub network_prover: Arc<CpuProver>,
    pub fetcher: Arc<OPSuccinctDataFetcher>,
    pub driver_db_client: Arc<DriverDBClient>,
    pub signer: Signer,
    pub loop_interval: u64,
}
/// Type alias for a map of task IDs to their join handles and associated requests
pub type TaskMap = HashMap<
    i64,
    (
        tokio::task::JoinHandle<Result<(), ProofRequesterError>>,
        OPSuccinctRequest,
    ),
>;

pub struct Proposer<P, H: OPSuccinctHost>
where
    P: Provider + 'static,
{
    driver_config: DriverConfig,
    contract_config: ContractConfig<P>,
    program_config: ProgramConfig,
    requester_config: RequesterConfig,
    proof_requester: Arc<OPSuccinctProofRequester<H>>,
    tasks: Arc<Mutex<TaskMap>>,
}

impl<P, H: OPSuccinctHost> Proposer<P, H>
where
    P: Provider + 'static + Clone,
{
    pub async fn new(
        provider: P,
        db_client: Arc<DriverDBClient>,
        fetcher: Arc<OPSuccinctDataFetcher>,
        requester_config: RequesterConfig,
        signer: Signer,
        loop_interval: u64,
        host: Arc<H>,
    ) -> Result<Self> {
        // This check prevents users from running multiple proposers for the same chain at the same
        // time.
        let is_locked = db_client
            .is_chain_locked(
                requester_config.l1_chain_id,
                requester_config.l2_chain_id,
                Duration::from_secs(loop_interval),
            )
            .await?;
        if is_locked {
            return Err(anyhow!(
                "There is another proposer for the same chain connected to the database. Only one proposer can be connected to the database for a chain at a time."
            ));
        }

        // Add the chain lock to the database.
        db_client
            .add_chain_lock(requester_config.l1_chain_id, requester_config.l2_chain_id)
            .await?;

        // Set a default network private key to avoid an error in mock mode.
        let _private_key = env::var("NETWORK_PRIVATE_KEY").unwrap_or_else(|_| {
            warn!("Using default NETWORK_PRIVATE_KEY of 0x01. This is only valid in mock mode.");
            "0x0000000000000000000000000000000000000000000000000000000000000001".to_string()
        });

        let network_prover = Arc::new(ProverClient::builder().cpu().build());
        let (range_pk, range_vk) = network_prover.setup(get_range_elf_embedded());

        let (agg_pk, agg_vk) = network_prover.setup(AGGREGATION_ELF);
        let multi_block_vkey_u8 = u32_to_u8(range_vk.vk.hash_u32());
        let range_vkey_commitment = B256::from(multi_block_vkey_u8);
        let agg_vkey_hash = B256::from_str(&agg_vk.bytes32()).unwrap();

        // Initialize fetcher
        let rollup_config_hash = hash_rollup_config(fetcher.rollup_config.as_ref().unwrap());

        let program_config = ProgramConfig {
            range_vk: Arc::new(range_vk),
            range_pk: Arc::new(range_pk),
            agg_vk: Arc::new(agg_vk),
            agg_pk: Arc::new(agg_pk),
            commitments: CommitmentConfig {
                range_vkey_commitment,
                agg_vkey_hash,
                rollup_config_hash,
            },
        };

        let proxy_client = ClientProxy::init_client(
            requester_config.proof_request_client,
            program_config.clone(),
        )
        .await?;
        let client = Arc::new(proxy_client);

        // Initialize the proof requester.
        let proof_requester = Arc::new(OPSuccinctProofRequester::new(
            host,
            network_prover.clone(),
            client,
            fetcher.clone(),
            db_client.clone(),
            program_config.clone(),
            requester_config.mock,
            requester_config.range_proof_strategy,
            requester_config.agg_proof_strategy,
            requester_config.agg_proof_mode,
            requester_config.safe_db_fallback,
            requester_config.cost_estimator_timeout_secs,
            requester_config.witness_gen_timeout_secs,
            requester_config.instruction_count_limit,
            requester_config.max_exec_timeout_retries,
        ));

        let l2oo_contract =
            OPSuccinctL2OOContract::new(requester_config.l2oo_address, provider.clone());

        let dgf_contract =
            DisputeGameFactoryContract::new(requester_config.dgf_address, provider.clone());

        let proposer = Proposer {
            driver_config: DriverConfig {
                network_prover,
                fetcher,
                driver_db_client: db_client,
                signer,
                loop_interval,
            },
            contract_config: ContractConfig {
                l2oo_address: requester_config.l2oo_address,
                dgf_address: requester_config.dgf_address,
                l2oo_contract,
                dgf_contract,
            },
            program_config,
            requester_config,
            proof_requester,
            tasks: Arc::new(Mutex::new(HashMap::new())),
        };
        Ok(proposer)
    }

    /// Use the in-memory index of the highest block number to add new ranges to the database.
    #[tracing::instrument(name = "proposer.add_new_ranges", skip(self))]
    pub async fn add_new_ranges(&self) -> Result<()> {
        if self.requester_config.set_for_upgrade {
            return Ok(());
        }

        // Get the latest proposed block number on the contract.
        let mut latest_proposed_block_number = get_latest_proposed_block_number(
            self.contract_config.l2oo_address,
            self.driver_config.fetcher.as_ref(),
        )
        .await
        .context("Failed to get latest proposed block number")?;

        if let Some(force_start_block) = self.requester_config.force_start_block {
            warn!("Forced start block set. Overriding the latest proposed block from contract to: {force_start_block}");
            latest_proposed_block_number = force_start_block;
        }

        if let Some(fork_start_block) = self.requester_config.fork_start_block {
            if latest_proposed_block_number + 1 < fork_start_block {
                // l2oo is still waiting on the forkblock proof to come
                return Ok(());
            }
        }

        let finalized_block_number = match self
            .proof_requester
            .host
            .get_finalized_l2_block_number(
                self.driver_config.fetcher.as_ref(),
                latest_proposed_block_number,
            )
            .await
            .context("Failed to fetch latest finalized block number")?
        {
            Some(block_number) => {
                info!("Found finalized block number: {}", block_number);
                block_number
            }
            None => {
                info!("No new finalized block number found since last proposed block. No new range proof requests will be added.");
                return Ok(());
            }
        };

        // Get all active (non-failed) requests with the same commitment config and start block >=
        // latest_proposed_block_number. These requests are non-overlapping.
        let mut requests = self
            .driver_config
            .driver_db_client
            .fetch_ranges_after_block(
                &[
                    RequestStatus::Unrequested,
                    RequestStatus::WitnessGeneration,
                    RequestStatus::Execution,
                    RequestStatus::Prove,
                    RequestStatus::Complete,
                ],
                latest_proposed_block_number as i64,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        // Sort the requests by start block.
        requests.sort_by_key(|r| r.0);

        // Find gaps between ranges and gap between start_block_number and
        // finalized_block_number. Here we assume finalized_block_number comes after
        // start_block_number
        let disjoint_ranges = find_gaps(
            latest_proposed_block_number as i64,
            finalized_block_number as i64,
            &requests,
        );

        // For any large range, divide them into smaller ranges of range_proof_interval size
        let ranges_to_prove = get_ranges_to_prove(
            &disjoint_ranges,
            self.requester_config.range_proof_interval as i64,
        );

        info!(
            "Ranges to prove: {}. Fetching roots.",
            ranges_to_prove.len()
        );

        const MAX_CONCURRENT_ROOT_REQUESTS: usize = 10;
        let ranges_roots = stream::iter(ranges_to_prove)
            // here we limit the number of ranges to prove since too many ranges can cause
            // timeouts or rate limits on our rpc nodes
            .take(self.requester_config.max_new_ranges_per_iteration as usize)
            .map(async |(start_block, end_block)| {
                let timeouts = self
                    .driver_config
                    .driver_db_client
                    .get_range_timeouts(
                        &self.program_config.commitments,
                        start_block,
                        end_block,
                        self.requester_config.l1_chain_id,
                        self.requester_config.l2_chain_id,
                    )
                    .await?;

                if timeouts.len() >= self.requester_config.max_exec_timeout_retries as usize {
                    warn!(
                        "Skipping request for range: [{start_block},{end_block}] since it timed out {} times before while the max is {}",
                        timeouts.len(),
                        self.requester_config.max_exec_timeout_retries,
                    );
                    Ok::<Option<RangesRoots>, anyhow::Error>(None)
                } else {
                    let (pre_root, post_root) =
                        self.fetch_pre_post_roots(start_block, end_block).await?;

                    Ok(Some(RangesRoots {
                        start_block,
                        end_block,
                        preroot: pre_root,
                        postroot: post_root,
                    }))
                }
            })
            .buffered(MAX_CONCURRENT_ROOT_REQUESTS) // Do 10 at a time, otherwise it's too slow when fetching the block range data.
            .try_collect::<Vec<Option<RangesRoots>>>()
            .await
            .context("Failed to fetch ranges roots")?
            .into_iter()
            .flatten()
            .collect::<Vec<RangesRoots>>();

        info!("Ranges roots: {}", ranges_roots.len());
        if !ranges_roots.is_empty() {
            info!(
                "Inserting {} range proof requests into the database.",
                ranges_roots.len()
            );

            // Create range proof requests for the ranges to prove in parallel
            const MAX_CONCURRENT_RANGE_REQUESTS: usize = 5;
            let new_range_requests = stream::iter(ranges_roots)
                .map(|range_roots| {
                    let mode = if self.requester_config.mock {
                        RequestMode::Mock
                    } else {
                        RequestMode::Real
                    };
                    OPSuccinctRequest::create_range_request(
                        mode,
                        range_roots.start_block,
                        range_roots.end_block,
                        self.program_config.commitments.range_vkey_commitment,
                        self.program_config.commitments.rollup_config_hash,
                        self.requester_config.l1_chain_id,
                        self.requester_config.l2_chain_id,
                        Some(range_roots.preroot.to_vec()),
                        Some(range_roots.postroot.to_vec()),
                        self.driver_config.fetcher.clone(),
                        self.requester_config.max_exec_timeout_retries as i64,
                    )
                })
                // Do 5 at a time, otherwise it's too slow when fetching the block range data.
                // More may cause too many rate limit issues as the blocks are also fetched in parallel
                .buffered(MAX_CONCURRENT_RANGE_REQUESTS)
                .collect::<Vec<Result<OPSuccinctRequest>>>()
                .await
                .into_iter()
                .filter_map(|result| match result {
                    Ok(request) => Some(request),
                    Err(e) => {
                        error!("Failed to create range request: {e}. Will be retried.");
                        None // Skip failed requests - they should be retried in the next iteration
                    }
                })
                .collect::<Vec<OPSuccinctRequest>>();

            info!(
                "Inserting {} created ranged into db",
                new_range_requests.len()
            );
            // Insert the new range proof requests into the database.
            self.driver_config
                .driver_db_client
                .insert_requests(&new_range_requests)
                .await?;
        }

        Ok(())
    }

    /// Handle all proof requests in the Prove state.
    #[tracing::instrument(name = "proposer.handle_proving_requests", skip(self))]
    pub async fn handle_proving_requests(&self) -> Result<()> {
        // Get all requests from the database.
        let prove_requests = self
            .driver_config
            .driver_db_client
            .fetch_requests_by_status(
                RequestStatus::Prove,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        // Get the proof status of all of the requests.
        for request in prove_requests {
            self.process_proof_request_status(request).await?;
        }

        Ok(())
    }

    /// Process a single OP Succinct request's proof status.
    #[tracing::instrument(name = "proposer.process_proof_request_status", skip(self, request))]
    pub async fn process_proof_request_status(&self, request: OPSuccinctRequest) -> Result<()> {
        if let Some(proof_request_id) = request.proof_request_id.as_ref() {
            let proof_uuid = B256::from_slice(proof_request_id);
            let bytes = proof_uuid.to_vec();
            let uuid_bytes = &bytes[..16];
            let id_str = uuid::Uuid::from_slice(uuid_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to create UUID from bytes: {}", e))?
                .to_string();

            let proof_details = self
                .proof_requester
                .client
                .get_proof_request(&id_str, true)
                .await?;

            // If the proof request has been fulfilled, update the request to status Complete and
            // add the proof bytes to the database.
            if proof_details.status == JobStatus::Completed {
                let proof = proof_details
                    .proof
                    .context("Job completed but no proof found.")?;

                let proof_bytes = match proof.proof {
                    // If it's a compressed proof, serialize with bincode.
                    SP1Proof::Compressed(_) => bincode::serialize(&proof).unwrap(),
                    // If it's Groth16 or PLONK, get the on-chain proof bytes.
                    SP1Proof::Groth16(_) | SP1Proof::Plonk(_) => proof.bytes(),
                    SP1Proof::Core(_) => return Err(anyhow!("Core proofs are not supported.")),
                };

                // Add the completed proof to the database.
                self.driver_config
                    .driver_db_client
                    .update_proof_to_complete(request.id, &proof_bytes, proof_uuid)
                    .await?;
                // Update the prove_duration based on the current time and the proof_request_time.
                self.driver_config
                    .driver_db_client
                    .update_prove_duration(request.id)
                    .await?;
            } else if proof_details.status == JobStatus::Failed {
                self.proof_requester
                    .handle_failed_request(request, ExecutionStatus::Executed) // fake unused status
                    .await?;
                ValidityGauge::ProofRequestRetryCount.increment(1.0);
            }
        } else {
            // There should never be a proof request in Prove status without a proof request id.
            warn!(id = request.id, start_block = request.start_block, end_block = request.end_block, req_type = ?request.req_type, "Request has no proof request id");
        }

        Ok(())
    }

    pub async fn should_aggregate_earlier(
        &self,
        latest_proposed_block_number: i64,
    ) -> Result<bool> {
        if !self.requester_config.set_for_upgrade {
            return Ok(false);
        }

        let pending_completion_range_proofs = self
            .driver_config
            .driver_db_client
            .fetch_ranges_after_block(
                &[
                    RequestStatus::Unrequested,
                    RequestStatus::WitnessGeneration,
                    RequestStatus::Execution,
                    RequestStatus::Prove,
                ],
                latest_proposed_block_number,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        Ok(pending_completion_range_proofs.is_empty())
    }

    /// Create aggregation proofs based on the completed range proofs. The range proofs must be
    /// contiguous and have the same range vkey commitment. Assumes that the range proof retry
    /// logic guarantees that there is not two potential contiguous chains of range proofs.
    ///
    /// Only creates an Aggregation proof if there's not an Aggregation proof in progress with the
    /// same start block.
    #[tracing::instrument(name = "proposer.create_aggregation_proofs", skip(self))]
    pub async fn create_aggregation_proofs(&self) -> Result<()> {
        if self.requester_config.skip_aggregation_proofs {
            warn!("Proposer configured to skip aggregation proofs.");
            return Ok(());
        }
        // Check if there's an Aggregation proof with the same start block AND range verification
        // key commitment AND aggregation vkey. If so, return.
        let latest_proposed_block_number = get_latest_proposed_block_number(
            self.contract_config.l2oo_address,
            self.driver_config.fetcher.as_ref(),
        )
        .await? as i64;
        if let Some(fork_start_block) = self.requester_config.fork_start_block {
            if latest_proposed_block_number + 1 < fork_start_block as i64 {
                // l2oo is still waiting on the forkblock proof to come
                return Ok(());
            }
        }
        // Get all active Aggregation proofs with the same start block, range vkey commitment, and
        // aggregation vkey.
        let active_agg_proofs_count = self
            .driver_config
            .driver_db_client
            .fetch_active_agg_proofs_count(
                latest_proposed_block_number as i64,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        if active_agg_proofs_count > 0 {
            debug!("There is already an Aggregation proof queued with the same start block, range vkey commitment, and aggregation vkey.");
            return Ok(());
        }

        // Get the completed range proofs with a start block greater than the latest proposed block
        // number. These blocks are sorted.
        let mut completed_range_proofs = self
            .driver_config
            .driver_db_client
            .fetch_completed_ranges(
                &self.program_config.commitments,
                latest_proposed_block_number as i64,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        // Sort the completed range proofs by start block.
        completed_range_proofs
            .sort_by_key(|CompletedRangeProofInfo { start_block, .. }| *start_block);

        // This vector affects how many ranges will be aggregated as part of this agg request,
        // therefore we truncate it to be within the maximum allowed. If there are fewer range
        // requests, the vector is not affected.
        completed_range_proofs.truncate(self.requester_config.max_aggregated_ranges as usize);

        // Fixed bug: if the very first range proof after a completed agg proof was larger than
        // SUBMISSION_INTERVAL then proposer would erroneously try to propose an aggregation proof
        // even though it didn't contain the correct sequential range proofs. Which is why we
        // "continue" and wait for the range proofs in the gap to come in before continuing.
        if let Some(first_range) = completed_range_proofs.first() {
            if first_range.start_block != latest_proposed_block_number {
                debug!(
                    "Waiting for the next contiguous range proof to come in. The start block of completed_range_proofs {} doesn't match latest: {}",
                    first_range.start_block,
                    latest_proposed_block_number
                );
                return Ok(());
            }
        }

        // Get the highest block number of the completed range proofs.
        let highest_proven_contiguous_block_number = match self
            .get_highest_proven_contiguous_block(completed_range_proofs.clone())?
        {
            Some(block) => block,
            None => return Ok(()), /* No completed range proofs contiguous to the latest proposed
                                    * block number, so no need to create an aggregation proof. */
        };

        debug!(
            "completed_range_proofs: {}, start block: {}, end block: {}, highest_proven_contiguous_block_number: {}",
            completed_range_proofs.len(),
            completed_range_proofs.first().unwrap().start_block,
            completed_range_proofs.last().unwrap().end_block,
            highest_proven_contiguous_block_number
        );

        let submission_interval = self.requester_config.submission_interval as i64;

        debug!(
            "Submission interval for aggregation proof: {}.",
            submission_interval
        );

        // If the highest proven contiguous block number is greater than the latest proposed block
        // number plus the submission interval, create an aggregation proof.
        if (highest_proven_contiguous_block_number - latest_proposed_block_number)
            >= submission_interval
            || self
                .should_aggregate_earlier(latest_proposed_block_number)
                .await?
        {
            info!("Submission interval for aggregation proof: {submission_interval}");

            // Find an L1 block that will be used to create the aggregation proof.
            let latest_finalized_header = self
                .driver_config
                .fetcher
                .get_l1_header(BlockId::finalized())
                .await?;

            let l1_range_head = completed_range_proofs
                .last()
                .ok_or_else(|| anyhow!("No completed range proofs"))?
                .l1_head_block
                .ok_or_else(|| anyhow!("Range proof does not have known L1 head block"))?;

            if l1_range_head as u64 >= latest_finalized_header.number {
                warn!("Finalized L1 block is not ahead L1 range head");
                return Ok(());
            }

            info!(
                "Checkpointed L1 block number: {:?}.",
                latest_finalized_header.number
            );

            let checkpointed_l1_block_hash = latest_finalized_header.hash_slow();
            let checkpointed_l1_block_number = latest_finalized_header.number as i64;

            let start_block = latest_proposed_block_number;
            let end_block = highest_proven_contiguous_block_number;
            // Get the output at the end block of the last completed aggregation proof.
            let (pre_root, post_root) = self.fetch_pre_post_roots(start_block, end_block).await?;
            // can we get this from op?
            let latest_proposed_root = get_latest_proposed_root(
                self.contract_config.l2oo_address,
                self.driver_config.fetcher.as_ref(),
            )
            .await? as B256;

            debug!(
                "create_aggregation_proofs: aggregate proofrequestID TODO startBlock {}, pre_root {:?}, endBlock {}, rootAtEndBlock (postroot) {:?}, latest_proposed_root: {:?}",
                start_block, pre_root, end_block,
                post_root, latest_proposed_root
            );

            // If pre_root is different from the latest proposed root, verification is going to
            // fail. This is likely due to trying to create an aggregation proof in the
            // future
            if latest_proposed_root != pre_root {
                warn!(
                    "create_aggregation_proofs: latest_proposed_root != pre_root: {:?} != {:?}",
                    latest_proposed_root, pre_root
                );
                return Ok(());
            }

            // Create an aggregation proof request to cover the range with the
            // checkpointed L1 block hash.
            self.driver_config
                .driver_db_client
                .insert_request(&OPSuccinctRequest::new_agg_request(
                    if self.requester_config.mock {
                        RequestMode::Mock
                    } else {
                        RequestMode::Real
                    },
                    latest_proposed_block_number,
                    highest_proven_contiguous_block_number,
                    self.program_config.commitments.range_vkey_commitment,
                    self.program_config.commitments.agg_vkey_hash,
                    self.program_config.commitments.rollup_config_hash,
                    self.requester_config.l1_chain_id,
                    self.requester_config.l2_chain_id,
                    checkpointed_l1_block_number,
                    checkpointed_l1_block_hash,
                    self.requester_config.prover_address,
                    pre_root,
                    post_root,
                    latest_proposed_root,
                ))
                .await?;
        } else if completed_range_proofs.len()
            == self.requester_config.max_aggregated_ranges as usize
        {
            error!(
                "There are {} range proofs ready for aggregation, but total number of blocks in them is less than {}. Aggregate proof will not be created with this setup. Adjust ENV!",
                completed_range_proofs.len(),
                submission_interval
            );
        }

        Ok(())
    }

    /// Request all unrequested proofs up to MAX_CONCURRENT_PROOF_REQUESTS. If there are already
    /// MAX_CONCURRENT_PROOF_REQUESTS proofs in WitnessGeneration, Execute, and Prove status,
    /// return. If there are already MAX_CONCURRENT_WITNESS_GEN proofs in WitnessGeneration status, return.
    ///
    /// Note: In the future, submit up to MAX_CONCURRENT_PROOF_REQUESTS at a time. Don't do one per
    /// loop.
    #[tracing::instrument(name = "proposer.request_queued_proofs", skip(self))]
    async fn request_queued_proofs(&self) -> Result<()> {
        let commitments = self.program_config.commitments.clone();
        let l1_chain_id = self.requester_config.l1_chain_id;
        let l2_chain_id = self.requester_config.l2_chain_id;

        let witness_gen_count = self
            .driver_config
            .driver_db_client
            .fetch_request_count(
                RequestStatus::WitnessGeneration,
                &commitments,
                l1_chain_id,
                l2_chain_id,
            )
            .await?;

        let execution_count = self
            .driver_config
            .driver_db_client
            .fetch_request_count(
                RequestStatus::Execution,
                &commitments,
                l1_chain_id,
                l2_chain_id,
            )
            .await?;

        let prove_count = self
            .driver_config
            .driver_db_client
            .fetch_request_count(RequestStatus::Prove, &commitments, l1_chain_id, l2_chain_id)
            .await?;

        // If there are already MAX_CONCURRENT_PROOF_REQUESTS proofs in WitnessGeneration, Execute,
        // and Prove status, return.
        if witness_gen_count + execution_count + prove_count
            >= self.requester_config.max_concurrent_proof_requests as i64
        {
            debug!("There are already MAX_CONCURRENT_PROOF_REQUESTS proofs in WitnessGeneration, Execute, and Prove status.");
            return Ok(());
        }

        // If there are already MAX_CONCURRENT_WITNESS_GEN proofs in WitnessGeneration status,
        // return.
        if witness_gen_count >= self.requester_config.max_concurrent_witness_gen as i64 {
            debug!(
                "There are already MAX_CONCURRENT_WITNESS_GEN proofs in WitnessGeneration status."
            );
            return Ok(());
        }

        if let Some(request) = self.get_next_unrequested_proof().await? {
            info!(
                request_id = request.id,
                request_type = ?request.req_type,
                start_block = request.start_block,
                end_block = request.end_block,
                "Making proof request"
            );
            let high_memory_threshold = self.requester_config.high_memory_threshold;
            let request_clone = request.clone();
            let proof_requester = self.proof_requester.clone();
            let handle = tokio::spawn(async move {
                proof_requester
                    .make_proof_request(request_clone, high_memory_threshold)
                    .await
            });
            self.tasks
                .lock()
                .await
                .insert(request.id, (handle, request));
        }

        Ok(())
    }

    /// Get the next unrequested proof from the database.
    ///
    /// If there is an Aggregation proof with the same start block, range vkey commitment, and
    /// aggregation vkey, return that. Otherwise, return a range proof with the lowest start
    /// block.
    async fn get_next_unrequested_proof(&self) -> Result<Option<OPSuccinctRequest>> {
        let mut latest_proposed_block_number = get_latest_proposed_block_number(
            self.contract_config.l2oo_address,
            self.driver_config.fetcher.as_ref(),
        )
        .await?;

        if let Some(force_start_block) = self.requester_config.force_start_block {
            warn!("Forced start block set. Overriding the latest proposed block from contract to: {force_start_block}");
            latest_proposed_block_number = force_start_block;
        }

        let unreq_agg_request = self
            .driver_config
            .driver_db_client
            .fetch_unrequested_agg_proof(
                latest_proposed_block_number as i64,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        if let Some(unreq_agg_request) = unreq_agg_request {
            return Ok(Some(unreq_agg_request));
        }

        let unreq_range_request = self
            .driver_config
            .driver_db_client
            .fetch_first_unrequested_range_proof(
                latest_proposed_block_number as i64,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        if let Some(unreq_range_request) = unreq_range_request {
            return Ok(Some(unreq_range_request));
        }

        Ok(None)
    }

    /// Relay all completed aggregation proofs to the contract.
    #[tracing::instrument(name = "proposer.submit_agg_proofs", skip(self))]
    async fn submit_agg_proofs(&self) -> Result<()> {
        let latest_proposed_block_number = get_latest_proposed_block_number(
            self.contract_config.l2oo_address,
            self.driver_config.fetcher.as_ref(),
        )
        .await?;

        // See if there is an aggregation proof that is complete for this start block. NOTE: There
        // should only be one "pending" aggregation proof at a time for a specific start block.
        let completed_agg_proof = self
            .driver_config
            .driver_db_client
            .fetch_completed_agg_proof_after_block(
                latest_proposed_block_number as i64,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        // If there are no completed aggregation proofs, do nothing.
        let completed_agg_proof = match completed_agg_proof {
            Some(proof) => proof,
            None => return Ok(()),
        };

        // Get the output at the end block of the last completed aggregation proof.
        let (pre_root, post_root) = self
            .fetch_pre_post_roots(
                completed_agg_proof.start_block,
                completed_agg_proof.end_block,
            )
            .await?;

        let proof_req_id = completed_agg_proof
            .proof_request_id
            .as_ref()
            .map(|id| B256::from_slice(id))
            .unwrap_or_default();
        debug!(
            "submit_agg_proofs: aggregate proofrequestID proof request id: {:?} completed_agg_proof.id: {} startBlock {}, pre_root: {:?}, endBlock {}, post_root {:?}",
            proof_req_id, completed_agg_proof.id,
            completed_agg_proof.start_block, pre_root,
            completed_agg_proof.end_block, post_root
        );

        // Relay the aggregation proof.
        let transaction_hash = match self.relay_aggregation_proof(&completed_agg_proof).await {
            Ok(transaction_hash) => transaction_hash,
            Err(e) => {
                ValidityGauge::RelayAggProofErrorCount.increment(1.0);
                return Err(e);
            }
        };

        info!(
            "Relayed aggregation proof. Transaction hash: {:?}",
            transaction_hash
        );

        // Update the request to status RELAYED.
        self.driver_config
            .driver_db_client
            .update_request_to_relayed(
                completed_agg_proof.id,
                transaction_hash,
                self.contract_config.l2oo_address,
            )
            .await?;

        Ok(())
    }

    /// Submit the transaction to create a validity dispute game.
    ///
    /// If the DGF address is set, use it to create a new validity dispute game that will resolve
    /// with the proof. Otherwise, propose the L2 output.
    async fn relay_aggregation_proof(
        &self,
        completed_agg_proof: &OPSuccinctRequest,
    ) -> Result<B256> {
        // Get the output at the end block of the last completed aggregation proof.
        let output = self
            .driver_config
            .fetcher
            .get_l2_output_at_block(completed_agg_proof.end_block as u64)
            .await?;

        let (l1_head_hash, _) = self
            .driver_config
            .fetcher
            .get_l1_head(completed_agg_proof.end_block as u64, false)
            .await?;

        let (claim_sender_address, claim_nonce) = self
            .driver_config
            .fetcher
            .get_batcher_sender_info_at(BlockId::hash(l1_head_hash))
            .await?;

        let proof_req_id = completed_agg_proof
            .proof_request_id
            .as_ref()
            .map(|id| B256::from_slice(id))
            .unwrap_or_default();

        // Validate the proof's public values match the L1 block data.
        if completed_agg_proof.mode == RequestMode::Real {
            let mut proof_with_pv: SP1ProofWithPublicValues =
                bincode::deserialize(completed_agg_proof.proof.as_ref().unwrap())
                    .expect("Deserialization failure for range proof");
            let boot_info: BootInfoStruct = proof_with_pv.public_values.read();

            assert_eq!(
                boot_info.postNonce, claim_nonce,
                "Nonce mismatch between proof and L1 block data"
            );
            assert_eq!(
                boot_info.postSenderAddress, claim_sender_address,
                "Batcher address mismatch between proof and L1 block data"
            );
            assert_eq!(
                boot_info.l2PostRoot, output.output_root,
                "L2 post-root mismatch"
            );
        };

        info!(
            proof_req_id = ?proof_req_id,
            block_number = completed_agg_proof.end_block,
            output_root = output.output_root.to_string(),
            completed_agg_proof_id = completed_agg_proof.id,
            "Relay aggregation proof",
        );

        // If the DisputeGameFactory address is set, use it to create a new validity dispute game
        // that will resolve with the proof. Note: In the DGF setting, the proof immediately
        // resolves the game. Otherwise, propose the L2 output.
        let receipt = if self.contract_config.dgf_address != Address::ZERO {
            // Validity game type: https://github.com/ethereum-optimism/optimism/blob/develop/packages/contracts-bedrock/src/dispute/lib/Types.sol#L64.
            const OP_SUCCINCT_VALIDITY_DISPUTE_GAME_TYPE: u32 = 6;

            // Get the initialization bond for the validity dispute game.
            let init_bond = self
                .contract_config
                .dgf_contract
                .initBonds(OP_SUCCINCT_VALIDITY_DISPUTE_GAME_TYPE)
                .call()
                .await?;

            let extra_data = <(U256, U256, Address, Bytes)>::abi_encode_packed(&(
                U256::from(completed_agg_proof.end_block as u64),
                U256::from(completed_agg_proof.checkpointed_l1_block_number.unwrap() as u64),
                self.requester_config.prover_address,
                completed_agg_proof.proof.as_ref().unwrap().clone().into(),
            ));

            let transaction_request = self
                .contract_config
                .dgf_contract
                .create(
                    OP_SUCCINCT_VALIDITY_DISPUTE_GAME_TYPE,
                    output.output_root,
                    extra_data.into(),
                )
                .value(init_bond)
                .into_transaction_request();

            self.driver_config
                .signer
                .send_transaction_request(
                    self.driver_config
                        .fetcher
                        .as_ref()
                        .rpc_config
                        .l1_rpc
                        .clone(),
                    transaction_request,
                )
                .await?
        } else {
            let mut proof = completed_agg_proof
                .proof
                .as_ref()
                .ok_or_else(|| anyhow!("Proof is missing in completed aggregation proof."))?
                .clone();

            // Propose the L2 output.
            if proof.is_empty() {
                match completed_agg_proof.mode {
                    RequestMode::Mock => {
                        warn!("Replacing empty proof with a routable dummy proof of [0xff; 32]");
                    }
                    RequestMode::Real => {
                        bail!("Got empty proof without mock mode.");
                    }
                }
                proof = [0xff; 32].to_vec();
            }
            let transaction_request = self
                .contract_config
                .l2oo_contract
                .proposeL2OutputV3(
                    output.output_root,
                    claim_nonce,
                    claim_sender_address,
                    U256::from(completed_agg_proof.end_block),
                    U256::from(completed_agg_proof.checkpointed_l1_block_number.unwrap()),
                    proof.into(),
                    self.requester_config.prover_address,
                )
                .into_transaction_request();

            self.driver_config
                .signer
                .send_transaction_request(
                    self.driver_config
                        .fetcher
                        .as_ref()
                        .rpc_config
                        .l1_rpc
                        .clone(),
                    transaction_request,
                )
                .await?
        };

        // If the transaction reverted, log the error.
        if !receipt.status() {
            return Err(anyhow!("Transaction reverted: {:?}", receipt));
        }

        Ok(receipt.transaction_hash())
    }

    /// Validate the requester config matches the contract.
    async fn validate_contract_config(&self) -> Result<()> {
        // Validate the requester config matches the contract.
        let contract_rollup_config_hash = self
            .contract_config
            .l2oo_contract
            .rollupConfigHash()
            .call()
            .await?;
        let contract_agg_vkey_hash = self
            .contract_config
            .l2oo_contract
            .aggregationVkey()
            .call()
            .await?;
        let contract_range_vkey_commitment = self
            .contract_config
            .l2oo_contract
            .rangeVkeyCommitment()
            .call()
            .await?;

        let rollup_config_hash_match =
            contract_rollup_config_hash == self.program_config.commitments.rollup_config_hash;
        let agg_vkey_hash_match =
            contract_agg_vkey_hash == self.program_config.commitments.agg_vkey_hash;
        let range_vkey_commitment_match =
            contract_range_vkey_commitment == self.program_config.commitments.range_vkey_commitment;

        if !rollup_config_hash_match || !agg_vkey_hash_match || !range_vkey_commitment_match {
            error!(
                rollup_config_hash_match = rollup_config_hash_match,
                agg_vkey_hash_match = agg_vkey_hash_match,
                range_vkey_commitment_match = range_vkey_commitment_match,
                "Config mismatches detected."
            );

            if !rollup_config_hash_match {
                error!(
                    received = ?contract_rollup_config_hash,
                    expected = ?self.program_config.commitments.rollup_config_hash,
                    "Rollup config hash mismatch"
                );
            }

            if !agg_vkey_hash_match {
                error!(
                    received = ?contract_agg_vkey_hash,
                    expected = ?self.program_config.commitments.agg_vkey_hash,
                    "Aggregation vkey hash mismatch"
                );
            }

            if !range_vkey_commitment_match {
                error!(
                    received = ?contract_range_vkey_commitment,
                    expected = ?self.program_config.commitments.range_vkey_commitment,
                    "Range vkey commitment mismatch"
                );
            }

            return Err(anyhow::anyhow!("Config mismatches detected. Please run {{cargo run --bin config --release -- --env-file ENV_FILE}} to get the expected config for your contract."));
        }

        Ok(())
    }

    /// Set orphaned tasks to status FAILED. If a task is in the database in status Execution or
    /// WitnessGeneration but not in the tasks map, set it to status FAILED.
    async fn set_orphaned_tasks_to_failed(&self) -> Result<()> {
        let witnessgen_requests = self
            .driver_config
            .driver_db_client
            .fetch_requests_by_status(
                RequestStatus::WitnessGeneration,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        let execution_requests = self
            .driver_config
            .driver_db_client
            .fetch_requests_by_status(
                RequestStatus::Execution,
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        let requests = [witnessgen_requests, execution_requests].concat();

        // If a task is in the database in status Execution or WitnessGeneration but not in the
        // tasks map, set it to status FAILED.
        let tasks = self.tasks.lock().await;
        let orphaned_ids: Vec<i64> = requests
            .into_iter()
            .filter(|req| !tasks.contains_key(&req.id))
            .map(|req| {
                warn!(
                request_id = req.id,
                request_type = ?req.req_type,
                "Task is in the database in status Execution or WitnessGeneration but not in the tasks map, setting to status FAILED."
            );
                req.id
            })
            .collect();

        drop(tasks); // Release the lock before DB call

        if !orphaned_ids.is_empty() {
            warn!(orphaned_ids = ?orphaned_ids, "Setting orphaned tasks to status FAILED.");
            self.driver_config
                .driver_db_client
                .update_request_status_batch(orphaned_ids, RequestStatus::Failed)
                .await?;
        }

        Ok(())
    }

    /// Handle the ongoing witness generation and execution tasks.
    async fn handle_ongoing_tasks(&self) -> Result<()> {
        let mut tasks = self.tasks.lock().await;
        let mut completed = Vec::new();

        // Check and process completed tasks
        for (id, (handle, _)) in tasks.iter() {
            if handle.is_finished() {
                completed.push(*id);
            }
        }

        // Process completed tasks - this will properly await and drop them
        for id in completed {
            if let Some((handle, request)) = tasks.remove(&id) {
                // First await the handle to properly clean up the task.
                match handle.await {
                    Ok(result) => {
                        if let Err(e) = result {
                            warn!(
                                request_id = request.id,
                                request_type = ?request.req_type,
                                error = ?e,
                                "Task failed with error"
                            );
                            match e {
                                ProofRequesterError::MetadataComputeTimeout => {
                                    warn!(
                                        "Setting request id {} to Timedout. error: ({e})",
                                        request.id
                                    );

                                    self.proof_requester
                                        .db_client
                                        .set_request_timeout(
                                            request.id,
                                            self.proof_requester.cost_estimator_timeout_secs as i64,
                                        )
                                        .await?;
                                    if self.requester_config.split_on_timeout {
                                        info!("Splitting timedout request");
                                        // timeout is expected when there are a lot of cycles, in such scenario we want
                                        // to split into more parts to avoid getting timeout again
                                        self.proof_requester.split_range(request, 4).await?;
                                    }
                                }
                                ProofRequesterError::InstructionCountTooLarge(_)
                                | ProofRequesterError::InstructionCountSmallerThanDerivation(
                                    _,
                                    _,
                                )
                                | ProofRequesterError::WitnessGenTimeout => {
                                    // if instruction count is too high, or we are unable to compute metadata or generate witness in reasonable time
                                    // immidiatelly split range, as there is no point in sending request to proving since we expect it to fail
                                    info!(
                                        "Splitting request with id {} before sending it out ({e})",
                                        request.id
                                    );
                                    self.proof_requester
                                        .db_client
                                        .update_request_status(request.id, RequestStatus::Cancelled)
                                        .await?;
                                    self.proof_requester.split_range(request, 2).await?;
                                }
                                ProofRequesterError::Generic(_)
                                | ProofRequesterError::DBError(_) => {
                                    // Now safe to retry as original task is cleaned up
                                    match self
                                        .proof_requester
                                        .handle_failed_request(
                                            request,
                                            ExecutionStatus::UnspecifiedExecutionStatus,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            ValidityGauge::ProofRequestRetryCount.increment(1.0);
                                        }
                                        Err(retry_err) => {
                                            warn!(error = ?retry_err, "Failed to retry request");
                                            ValidityGauge::RetryErrorCount.increment(1.0);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            request_id = request.id,
                            request_type = ?request.req_type,
                            error = ?e,
                            "Task panicked"
                        );
                        // Now safe to retry as original task is cleaned up
                        match self
                            .proof_requester
                            .handle_failed_request(
                                request,
                                ExecutionStatus::UnspecifiedExecutionStatus,
                            )
                            .await
                        {
                            Ok(_) => {
                                ValidityGauge::ProofRequestRetryCount.increment(1.0);
                            }
                            Err(retry_err) => {
                                warn!(error = ?retry_err, "Failed to retry request after panic");
                                ValidityGauge::RetryErrorCount.increment(1.0);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Initialize the proposer by cleaning up stale requests and creating new range proof requests
    /// for the proposer with the given chain ID.
    ///
    /// This function performs several key tasks:
    /// 1. Validates that the proposer's config matches the contract
    /// 2. Deletes unrecoverable requests (UNREQUESTED, EXECUTION, WITNESS_GENERATION)
    /// 3. Cancels PROVE requests with mismatched commitment configs
    ///
    /// The goal is to ensure the database is in a clean state and all block ranges
    /// between the latest proposed block and finalized block have corresponding requests.
    #[tracing::instrument(name = "proposer.initialize_proposer", skip(self))]
    async fn initialize_proposer(&self) -> Result<()> {
        // Validate the requester config matches the contract.
        self.validate_contract_config()
            .await
            .context("Failed to validate the requester config matches the contract.")?;

        // Delete all requests for the same chain ID that are of status UNREQUESTED, EXECUTION or
        // WITNESS_GENERATION as they're unrecoverable.
        self.driver_config
            .driver_db_client
            .delete_all_requests_with_statuses(
                &[
                    RequestStatus::Unrequested,
                    RequestStatus::Execution,
                    RequestStatus::WitnessGeneration,
                ],
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        // Cancel all requests in PROVE state for the same chain id's that have a different
        // commitment config.
        self.driver_config
            .driver_db_client
            .cancel_prove_requests_with_different_commitment_config(
                &self.program_config.commitments,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        info!("Deleted all unrequested, execution, and witness generation requests and canceled all prove requests with different commitment configs.");

        Ok(())
    }

    /// Fetch and log the proposer metrics.
    async fn log_proposer_metrics(&self) -> Result<()> {
        // Get the latest proposed block number on the contract.
        let latest_proposed_block_number = get_latest_proposed_block_number(
            self.contract_config.l2oo_address,
            self.driver_config.fetcher.as_ref(),
        )
        .await?;

        // Get all completed range proofs from the database.
        let completed_range_proofs = self
            .driver_config
            .driver_db_client
            .fetch_completed_ranges(
                &self.program_config.commitments,
                latest_proposed_block_number as i64,
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        // Get the highest proven contiguous block.
        let highest_block_number = self
            .get_highest_proven_contiguous_block(completed_range_proofs)?
            .map_or(latest_proposed_block_number, |block| block as u64);

        // Fetch request counts for different statuses
        let commitments = &self.program_config.commitments;
        let l1_chain_id = self.requester_config.l1_chain_id;
        let l2_chain_id = self.requester_config.l2_chain_id;
        let db_client = &self.driver_config.driver_db_client;

        // Define statuses and their corresponding variable names
        let (
            num_unrequested_requests,
            num_prove_requests,
            num_execution_requests,
            num_witness_generation_requests,
        ) = (
            db_client
                .fetch_request_count(
                    RequestStatus::Unrequested,
                    commitments,
                    l1_chain_id,
                    l2_chain_id,
                )
                .await?,
            db_client
                .fetch_request_count(RequestStatus::Prove, commitments, l1_chain_id, l2_chain_id)
                .await?,
            db_client
                .fetch_request_count(
                    RequestStatus::Execution,
                    commitments,
                    l1_chain_id,
                    l2_chain_id,
                )
                .await?,
            db_client
                .fetch_request_count(
                    RequestStatus::WitnessGeneration,
                    commitments,
                    l1_chain_id,
                    l2_chain_id,
                )
                .await?,
        );

        // Log metrics
        info!(
            target: "proposer_metrics",
            "unrequested={num_unrequested_requests} prove={num_prove_requests} execution={num_execution_requests} witness_generation={num_witness_generation_requests} highest_contiguous_proven_block={highest_block_number} latest_proposed_block={latest_proposed_block_number}"
        );

        // Update gauges for proof counts
        ValidityGauge::CurrentUnrequestedProofs.set(num_unrequested_requests as f64);
        ValidityGauge::CurrentProvingProofs.set(num_prove_requests as f64);
        ValidityGauge::CurrentWitnessgenProofs.set(num_witness_generation_requests as f64);
        ValidityGauge::CurrentExecuteProofs.set(num_execution_requests as f64);
        ValidityGauge::HighestProvenContiguousBlock.set(highest_block_number as f64);
        ValidityGauge::LatestContractL2Block.set(latest_proposed_block_number as f64);

        // Get and set L2 block metrics
        let fetcher = &self.proof_requester.fetcher;
        let latest_l2_block = fetcher
            .get_l2_header(BlockId::latest())
            .await
            .inspect_err(|e| error!(error = ?e, "Failed to get latest L2 block for metrics."))
            .map(|header| header.number as f64)
            .unwrap_or_default();
        let finalized_l2_block = fetcher
            .get_l2_header(BlockId::finalized())
            .await
            .inspect_err(|e| error!(error = ?e, "Failed to get finalized L2 block for metrics."))
            .map(|header| header.number as f64)
            .unwrap_or_default();

        ValidityGauge::L2UnsafeHeadBlock.set(latest_l2_block);
        ValidityGauge::L2FinalizedBlock.set(finalized_l2_block);

        // Get the submission interval from the contract and set the gauge
        let contract_submission_interval: u64 = 1;
        // self
        // .contract_config
        // .l2oo_contract
        // .submissionInterval()
        // .call()
        // .await?
        // .try_into()
        // .unwrap();

        let submission_interval =
            contract_submission_interval.max(self.requester_config.submission_interval);
        ValidityGauge::MinBlockToProveToAgg
            .set((latest_proposed_block_number + submission_interval) as f64);

        Ok(())
    }

    #[tracing::instrument(name = "proposer.run", skip(self))]
    pub async fn run(&self) -> Result<()> {
        // Handle the case where the proposer is being re-started and the proposer state needs to be
        // updated.
        self.initialize_proposer().await?;

        // Initialize the metrics gauges.
        ValidityGauge::init_all();

        // Loop interval in seconds.
        loop {
            // Wrap the entire loop body in a match to handle errors
            match self.run_loop_iteration().await {
                Ok(_) => {
                    // Normal sleep between iterations
                    tokio::time::sleep(Duration::from_secs(self.driver_config.loop_interval)).await;
                }
                Err(e) => {
                    // Log the error
                    error!("Error in proposer loop: {:?}", e);
                    // Update the error gauge
                    ValidityGauge::TotalErrorCount.increment(1.0);
                    // Pause for 10 seconds before restarting
                    debug!("Pausing for 10 seconds before restarting the process");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }
    }

    // Run a single loop of the validity proposer.
    async fn run_loop_iteration(&self) -> Result<()> {
        // Validate the requester config matches the contract.
        self.validate_contract_config().await?;

        // Log the proposer metrics.
        self.log_proposer_metrics().await?;

        // Handle the ongoing tasks.
        self.handle_ongoing_tasks().await?;

        // Set orphaned WitnessGeneration and Execution tasks to status Failed.
        self.set_orphaned_tasks_to_failed().await?;

        // Get all proof statuses of all requests in the proving state.
        self.handle_proving_requests().await?;

        // Add new range requests to the database.
        self.add_new_ranges().await?;

        // Create aggregation proofs based on the completed range proofs. Checkpoints the block hash
        // associated with the aggregation proof in advance.
        self.create_aggregation_proofs().await?;

        // Request all unrequested proofs from the prover network.
        self.request_queued_proofs().await?;

        // Submit any aggregation proofs that are complete.
        self.submit_agg_proofs().await?;

        // Update the chain lock.
        self.proof_requester
            .db_client
            .update_chain_lock(
                self.requester_config.l1_chain_id,
                self.requester_config.l2_chain_id,
            )
            .await?;

        Ok(())
    }

    /// Get the highest block number at the end of the largest contiguous range of completed range
    /// proofs. Returns None if there are no completed range proofs.
    fn get_highest_proven_contiguous_block(
        &self,
        completed_range_proofs: Vec<CompletedRangeProofInfo>,
    ) -> Result<Option<i64>> {
        if completed_range_proofs.is_empty() {
            return Ok(None);
        }

        let mut current_end = completed_range_proofs[0].end_block;

        for proof in completed_range_proofs.iter().skip(1) {
            if proof.start_block != current_end {
                break;
            }
            current_end = proof.end_block;
        }

        Ok(Some(current_end))
    }

    #[tracing::instrument(name = "proposer.fetch_pre_post_roots", skip(self))]
    pub async fn fetch_pre_post_roots(
        &self,
        start_block: i64,
        end_block: i64,
    ) -> Result<(B256, B256)> {
        let pre_root = self
            .driver_config
            .fetcher
            .get_l2_output_at_block(start_block as u64)
            .await?;
        let post_root = self
            .driver_config
            .fetcher
            .get_l2_output_at_block(end_block as u64)
            .await?;

        Ok((pre_root.output_root, post_root.output_root))
    }
}
