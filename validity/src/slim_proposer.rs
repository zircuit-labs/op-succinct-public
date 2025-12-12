use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, FixedBytes, U256};
use alloy_provider::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
};
use alloy_provider::network::TransactionBuilder;
use alloy_provider::{network::ReceiptResponse, Identity, ProviderBuilder, RootProvider};
use alloy_rpc_types_eth::TransactionRequest;
use anyhow::{Context, Result};
use op_succinct_host_utils::fetcher::OPSuccinctDataFetcher;
use op_succinct_host_utils::OPSuccinctL2OutputOracle::OPSuccinctL2OutputOracleInstance as OPSuccinctL2OOContract;
use op_succinct_signer_utils::Signer;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use zircuit_client::message_bus::nats::{NatsBusConfig, NatsMessageBus};
use zircuit_client::message_bus::{MessageBus, ZircuitClientMessage};
use zircuit_client::Message;

const MAX_MSG_REPROCESSING_RETRIES: u8 = 50;
const SLEEP_DURATION_MS: u64 = 200;

type HttpProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

pub struct TxSubmissionConfig {
    pub gas_priority_fee: Option<u128>,
    pub gas_max_fee: Option<u128>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum SlimProposerRequest {
    // Checkpoint a specific L1 block number on L2OO
    Checkpoint {
        l1_block_number: u64,
    },
    // Propose a new L2 state root with the provided proof
    Propose {
        start_block: u64,
        end_block: u64,
        checkpointed_l1_block_number: u64,
        proof_hex: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        agg_vk: Option<String>,
    },
    // Propose a new L2 state root without a proof. will default to a dummy proof
    // (needs to be supported though the L2OO Verifier Gateway)
    ProposeDummyProof {
        start_block: u64,
        end_block: u64,
        checkpointed_l1_block_number: u64,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum SlimProposerResponse {
    Checkpointed {
        l1_block_number: u64,
        l1_block_hash: String,
    },
    Proposed {
        end_block: u64,
        l2_output_root: String,
    },
}

pub struct SlimProposer {
    nats: NatsMessageBus,
    tx_config: TxSubmissionConfig,
    signer: Signer,
    l2oo_contract: OPSuccinctL2OOContract<HttpProvider>,
    fetcher: Arc<OPSuccinctDataFetcher>,
    failure_map: HashMap<u64, u8>,
}

impl SlimProposer {
    pub async fn new(
        nats_config: NatsBusConfig,
        tx_config: TxSubmissionConfig,
        signer: Signer,
        l2oo_address: Address,
        fetcher: Arc<OPSuccinctDataFetcher>,
    ) -> Result<Self> {
        let nats = NatsMessageBus::new(&nats_config).await?;

        let provider =
            ProviderBuilder::new().connect_http(fetcher.as_ref().rpc_config.l1_rpc.clone());
        let l2oo_contract_instance = OPSuccinctL2OOContract::new(l2oo_address, provider);

        Ok(Self {
            nats,
            tx_config,
            signer,
            fetcher,
            l2oo_contract: l2oo_contract_instance,
            failure_map: HashMap::new(),
        })
    }

    async fn handle_checkpoint(&self, l1_block_number: u64) -> Result<()> {
        info!(l1_block_number = l1_block_number, "Checkpointing L1 block");

        let block_number = U256::from(l1_block_number);
        let block_hash = self
            .l2oo_contract
            .historicBlockHashes(block_number)
            .call()
            .await?;
        debug!("Existing checkpoint: {}", block_hash.to_string());

        let checkpointed_block_hash = if block_hash.is_zero() {
            info!("Checkpoint does not exits, creating new checkpoint.");
            let mut tx = self
                .l2oo_contract
                .checkpointBlockHash(U256::from(l1_block_number))
                .into_transaction_request();

            self.apply_fee_overrides(&mut tx);

            info!("Submitting transaction");

            let receipt = self
                .signer
                .send_transaction_request(self.fetcher.as_ref().rpc_config.l1_rpc.clone(), tx)
                .await?;
            if !receipt.status() {
                anyhow::bail!("Checkpoint transaction reverted: {:?}", receipt);
            }
            info!(
                l1_block_number = l1_block_number,
                tx_hash = ?receipt.transaction_hash(),
                gas_used = receipt.gas_used(),
                "Checkpoint completed successfully"
            );

            let block_hash = self
                .l2oo_contract
                .historicBlockHashes(block_number)
                .call()
                .await?;

            block_hash
        } else {
            warn!("Checkpoint already exist. Skipping checkpointing.");
            block_hash
        };

        info!("Sending response message to NATS");
        let response = SlimProposerResponse::Checkpointed {
            l1_block_hash: checkpointed_block_hash.to_string(),
            l1_block_number,
        };
        let response_payload = serde_json::to_vec(&response)?;
        let id = self
            .nats
            .publish(&response_payload)
            .await
            .context("Failed to send response message to NATS")?;

        info!("Response message sent to NATS with id {}", id);

        Ok(())
    }

    async fn handle_propose(
        &self,
        start_block: u64,
        end_block: u64,
        checkpointed_l1_block_number: u64,
        proof_hex: Option<String>,
        agg_vk: Option<String>,
    ) -> Result<()> {
        info!(end_block = end_block, "Proposing new output root");
        let output = self.fetcher.get_l2_output_at_block(end_block).await?;
        let (l1_head_hash, _) = self.fetcher.get_l1_head(end_block, false).await?;

        let (sender_address, sender_nonce) = self
            .fetcher
            .get_batcher_sender_info_at(BlockId::hash(l1_head_hash))
            .await?;

        let latest_block = self
            .l2oo_contract
            .latestBlockNumber()
            .call()
            .await
            .context("Failed to get latest block number from L2OO")?;

        info!(
            "Latest proposed block from contract: {}",
            latest_block.to_string()
        );

        match U256::from(start_block).cmp(&latest_block) {
            Ordering::Equal => {
                if let Some(agg_vk_hash) = agg_vk {
                    let agg_vk_hash = agg_vk_hash.strip_prefix("0x").unwrap_or(&agg_vk_hash);
                    let agg_vk_bytes = FixedBytes::from_slice(&hex::decode(agg_vk_hash)?);
                    let contract_agg_vkey_bytes =
                        self.l2oo_contract.aggregationVkey().call().await?;
                    if contract_agg_vkey_bytes != agg_vk_bytes {
                        let contract_agg_vkey_hash = hex::encode(contract_agg_vkey_bytes);
                        tracing::error!("Agg vk from proof ({agg_vk_hash}) is not the same as vk on the contract ({contract_agg_vkey_hash})");

                        anyhow::bail!("VK mismatch");
                    }
                }
                info!(
                    end_block = end_block,
                    output_root = ?output.output_root,
                    sender_address = ?sender_address,
                    sender_nonce = sender_nonce,
                    "Proposing L2 output",
                );
                let proof = if let Some(proof_hex) = proof_hex {
                    hex::decode(proof_hex.strip_prefix("0x").unwrap_or(&proof_hex))?
                } else if cfg!(feature = "echo") {
                    warn!("Echo mode enabled, using proof [0xff; 32] as dummy proof");
                    vec![0xff; 32]
                } else {
                    anyhow::bail!("Received empty proof, but echo mode is disabled")
                };

                let mut tx = self
                    .l2oo_contract
                    .proposeL2OutputV3(
                        output.output_root,
                        sender_nonce,
                        sender_address,
                        U256::from(end_block),
                        U256::from(checkpointed_l1_block_number),
                        Bytes::from(proof),
                        self.signer.address(),
                    )
                    .into_transaction_request();

                self.apply_fee_overrides(&mut tx);

                info!("Submitting transaction");

                let receipt = self
                    .signer
                    .send_transaction_request(self.fetcher.as_ref().rpc_config.l1_rpc.clone(), tx)
                    .await?;
                if !receipt.status() {
                    anyhow::bail!("Propose transaction reverted: {:?}", receipt);
                }

                info!(
                    end_block = end_block,
                    output = ?output.output_root,
                    tx_hash = ?receipt.transaction_hash(),
                    gas_used = receipt.gas_used(),
                    "L2 output successfully proposed"
                );
            }
            Ordering::Greater => {
                warn!(
                    start_block = start_block,
                    end_block = end_block,
                    latest_block = ?latest_block,
                    "Start block is not chained to proposed block. Waiting with proof."
                );
                anyhow::bail!("Waiting for missing proofs");
            }
            Ordering::Less => {
                warn!(
                    start_block = start_block,
                    end_block = end_block,
                    latest_block = ?latest_block,
                    "Block was already proposed. Skipping."
                );
                return Ok(());
            }
        }

        info!("Sending response message to NATS");
        let response = SlimProposerResponse::Proposed {
            l2_output_root: output.output_root.to_string(),
            end_block,
        };
        let response_payload = serde_json::to_vec(&response)?;
        let id = self
            .nats
            .publish(&response_payload)
            .await
            .context("Failed to send response message to NATS")?;

        info!("Response message sent to NATS with id {}", id);

        Ok(())
    }

    pub async fn run_loop(&mut self) -> Result<()> {
        loop {
            let msg = match self.nats.next_message().await? {
                Some(msg) => msg,
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(SLEEP_DURATION_MS)).await;
                    continue;
                }
            };

            self.process_message(msg).await;
        }
    }

    async fn process_message(&mut self, msg: Message) {
        let payload = msg.payload();

        let request = match self.parse_request(&payload) {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to parse message {}: {:?}", msg.id(), e);
                self.ack_message_with_reason(&msg, "malformed").await;
                return;
            }
        };

        let result = self.handle_request(request).await;

        match result {
            Ok(_) => {
                if let Err(e) = msg.ack().await {
                    warn!("Failed to ack message {}: {:?}", msg.id(), e);
                } else {
                    info!("Acked message {}", msg.id());
                }
            }
            Err(e) => {
                let id = msg.id();
                let failure_count = self.failure_map.entry(id).or_insert(1);

                error!(
                    "Failed to handle request {} (fail count: {}): {:?}",
                    msg.id(),
                    failure_count,
                    e
                );

                if *failure_count > MAX_MSG_REPROCESSING_RETRIES {
                    // use Term?
                    self.ack_message_with_reason(&msg, "failed").await;
                    self.failure_map.remove(&id);
                } else {
                    *failure_count += 1;
                }
            }
        }
    }

    fn parse_request(&self, payload: &[u8]) -> Result<SlimProposerRequest> {
        serde_json::from_slice(payload).map_err(|e| anyhow::anyhow!("JSON parsing error: {}", e))
    }

    async fn handle_request(&self, request: SlimProposerRequest) -> Result<()> {
        match request {
            SlimProposerRequest::Checkpoint { l1_block_number } => {
                self.handle_checkpoint(l1_block_number).await
            }
            SlimProposerRequest::Propose {
                start_block,
                end_block,
                checkpointed_l1_block_number,
                proof_hex,
                agg_vk,
            } => {
                self.handle_propose(
                    start_block,
                    end_block,
                    checkpointed_l1_block_number,
                    Some(proof_hex),
                    agg_vk,
                )
                .await
            }
            SlimProposerRequest::ProposeDummyProof {
                start_block,
                end_block,
                checkpointed_l1_block_number,
            } => {
                self.handle_propose(
                    start_block,
                    end_block,
                    checkpointed_l1_block_number,
                    None,
                    None,
                )
                .await
            }
        }
    }

    async fn ack_message_with_reason(&self, msg: &Message, reason: &str) {
        if let Err(e) = msg.ack().await {
            warn!("Failed to ack {} message {}: {:?}", reason, msg.id(), e);
        } else {
            warn!("Acked {} message {}", reason, msg.id());
        }
    }

    fn apply_fee_overrides(&self, tx: &mut TransactionRequest) {
        if let Some(gas_priority_fee) = self.tx_config.gas_priority_fee {
            info!("Setting priority fee to {}", gas_priority_fee);
            tx.set_max_priority_fee_per_gas(gas_priority_fee);
        }
        if let Some(gas_max_fee) = self.tx_config.gas_max_fee {
            info!("Setting max fee to {}", gas_max_fee);
            tx.set_max_fee_per_gas(gas_max_fee);
        }
    }
}
