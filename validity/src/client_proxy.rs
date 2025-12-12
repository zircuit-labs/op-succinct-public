use crate::{ProgramConfig, RequestType};
use anyhow::Result;
use sindri::client::SindriClient;
use sindri::integrations::sp1_v5::SP1ProofInfo;
use sindri::ProofInput;
use sp1_sdk::{SP1ProofWithPublicValues, SP1Stdin, SP1VerifyingKey};
use std::collections::HashMap;
use strum_macros::EnumString;
use zircuit_client::client::ZircuitClient;
use zircuit_client::env::client_config_from_env;
use zircuit_client::{JobStatus, ProverType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
pub enum ClientVariant {
    Sindri,
    ZircuitCpu,
    ZircuitCuda,
    ZircuitMock,
    ZircuitSindri,
}
pub enum ProofRequestClient {
    Sindri(SindriClient),
    Zircuit(ZircuitClient),
}

pub struct RequestedProofInfo {
    pub request_id: String,
    pub status: JobStatus,
    pub proof: Option<SP1ProofWithPublicValues>,
}

pub struct ClientProxy {
    client: ProofRequestClient,
    program_config: ProgramConfig,
}
impl ClientProxy {
    pub async fn init_client(
        variant: ClientVariant,
        program_config: ProgramConfig,
    ) -> Result<Self> {
        let client = match variant {
            ClientVariant::Sindri => {
                tracing::info!("Initializing Sindri Client");
                ProofRequestClient::Sindri(SindriClient::default())
            }
            ClientVariant::ZircuitCpu
            | ClientVariant::ZircuitMock
            | ClientVariant::ZircuitCuda
            | ClientVariant::ZircuitSindri => {
                tracing::info!("Initializing Zircuit Client");
                let config = client_config_from_env()?;
                let mut client = ZircuitClient::new(config).await?;
                client.set_prover(variant.into());
                client.run_message_handler_thread(Default::default())?;

                ProofRequestClient::Zircuit(client)
            }
        };

        tracing::info!("Proxy initialized with client: {variant:?}");
        Ok(Self {
            client,
            program_config,
        })
    }

    pub async fn request_proof(
        &self,
        request_type: RequestType,
        proof_input: SP1Stdin,
        meta: Option<HashMap<String, String>>,
    ) -> Result<RequestedProofInfo> {
        match &self.client {
            ProofRequestClient::Sindri(client) => {
                let circuit_id = match request_type {
                    RequestType::Range => self.program_config.range_circuit_id(),
                    RequestType::Aggregation => self.program_config.aggregation_circuit_id(),
                };
                let input = ProofInput::try_from(proof_input)?;
                let response = client
                    .request_proof(&circuit_id, input, meta, None, None)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to request proof from Sindri: {}", e))?;
                Ok(RequestedProofInfo {
                    request_id: response.proof_id,
                    status: convert_sindri_job_status(response.status),
                    proof: None,
                })
            }
            ProofRequestClient::Zircuit(client) => {
                let proof_input = bincode::serialize(&proof_input)?;
                let response = client
                    .request_proof(proof_input, meta, request_type.into())
                    .await?;
                Ok(RequestedProofInfo {
                    request_id: response.proof_id,
                    status: response.status,
                    proof: None,
                })
            }
        }
    }

    pub async fn get_proof_request(
        &self,
        proof_request_id: &str,
        include_proof: bool,
    ) -> Result<RequestedProofInfo> {
        let proof_info = match &self.client {
            ProofRequestClient::Sindri(client) => {
                let response = client
                    .get_proof(proof_request_id, Some(include_proof), None, None)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to get proof request from Sindri: {}", e)
                    })?;

                let mut proof_info = RequestedProofInfo {
                    request_id: proof_request_id.to_string(),
                    status: convert_sindri_job_status(response.status),
                    proof: None,
                };
                if include_proof && proof_info.status == JobStatus::Completed {
                    let proof = response.to_sp1_proof_with_public().map_err(|e| {
                        anyhow::anyhow!("Failed to extract proof from Sindri response: {}", e)
                    })?;
                    proof_info.proof = Some(proof);
                }
                proof_info
            }
            ProofRequestClient::Zircuit(client) => {
                let (proof_details, proof_blob) = client
                    .get_proof_request(proof_request_id, include_proof)
                    .await
                    .inspect_err(|err| tracing::error!("Failed to get proof request: {err}"))?;

                let mut proof_info = RequestedProofInfo {
                    request_id: proof_details.proof_id.clone(),
                    status: proof_details.status,
                    proof: None,
                };
                if include_proof && proof_info.status == JobStatus::Completed {
                    let proof_blob = proof_blob.expect("Proof should exist for a completed proof");
                    let (_vk, proof): (SP1VerifyingKey, SP1ProofWithPublicValues) =
                        bincode::deserialize(&proof_blob)
                            .map_err(|e| anyhow::anyhow!("Failed to deserialize proof: {}", e))?;
                    proof_info.proof = Some(proof);
                }
                proof_info
            }
        };
        Ok(proof_info)
    }
}

fn convert_sindri_job_status(status: sindri::JobStatus) -> JobStatus {
    match status {
        sindri::JobStatus::Queued => JobStatus::Created,
        sindri::JobStatus::InProgress => JobStatus::Submitted,
        sindri::JobStatus::Ready => JobStatus::Completed,
        sindri::JobStatus::Failed => JobStatus::Failed,
    }
}

impl From<RequestType> for zircuit_client::RequestType {
    fn from(request_type: RequestType) -> Self {
        match request_type {
            RequestType::Range => zircuit_client::RequestType::Range,
            RequestType::Aggregation => zircuit_client::RequestType::Aggregation,
        }
    }
}

impl From<ClientVariant> for ProverType {
    fn from(variant: ClientVariant) -> Self {
        match variant {
            ClientVariant::ZircuitMock => ProverType::Sp1Mock,
            ClientVariant::ZircuitCpu => ProverType::Sp1Cpu,
            ClientVariant::ZircuitCuda => ProverType::Sp1Cuda,
            ClientVariant::ZircuitSindri => ProverType::Sindri,
            ClientVariant::Sindri => ProverType::Sindri,
        }
    }
}
