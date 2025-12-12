pub(super) mod basic;
pub mod nats;

use crate::message_bus::nats::NatsBusConfig;
use crate::{ProofRequest, ProverType, RequestType};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub enum MessageBusVariant {
    Basic,
    Nats(NatsBusConfig),
}

#[async_trait]
pub trait ZircuitClientMessage: Send + Sync {
    fn id(&self) -> u64;

    fn payload(&self) -> Vec<u8>;

    async fn ack(&self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait MessageBus<M: ZircuitClientMessage + 'static>: Send + Sync {
    async fn publish(&self, msg: &[u8]) -> anyhow::Result<u64>;

    async fn publish_with_subject(&self, msg: &[u8], subject_suffix: String)
        -> anyhow::Result<u64>;

    fn is_ready(&self) -> bool;

    async fn next_message(&self) -> anyhow::Result<Option<M>>;
}

/// A trait for message buses that can work with any message type.
/// This is a type-erased wrapper for the MessageBus trait.
#[async_trait]
pub trait GenericMessageBus: Send + Sync {
    async fn publish(&self, msg: &[u8]) -> anyhow::Result<u64>;

    async fn publish_with_subject(&self, msg: &[u8], subject_suffix: String)
        -> anyhow::Result<u64>;

    fn is_ready(&self) -> bool;
    async fn next_message(&self) -> anyhow::Result<Option<Box<dyn ZircuitClientMessage>>>;
}

pub struct AnyMessageBus<M: ZircuitClientMessage + 'static> {
    inner: Arc<dyn MessageBus<M>>,
}

impl<M: ZircuitClientMessage + 'static> AnyMessageBus<M> {
    pub fn new(inner: Arc<dyn MessageBus<M>>) -> Self {
        Self { inner }
    }
}

/// Message submitted to the message bus with proof requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProofRequestMessage {
    pub proof_id: String,
    pub meta: Option<HashMap<String, String>>,
    pub proof_input: String,
    pub prover: ProverType,
    pub request_type: RequestType,
}

impl ProofRequestMessage {
    pub fn get_subject_suffix(&self) -> String {
        match self.request_type {
            RequestType::Range => RequestType::Range.to_string().to_lowercase(),
            RequestType::Aggregation => RequestType::Aggregation.to_string().to_lowercase(),
        }
    }
}

/// Message received from the message bus with proof results
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProofResponseMessage {
    pub proof_id: String,
    pub proof: Option<String>,
}

impl ProofRequestMessage {
    /// Create a new proof request message from a proof request
    pub fn from_proof_request(proof_request: &ProofRequest) -> Self {
        Self {
            proof_id: proof_request.proof_id.clone(),
            meta: proof_request.meta.clone().map(|m| m.0),
            proof_input: proof_request.proof_input_url.clone(),
            prover: proof_request.prover,
            request_type: proof_request.request_type,
        }
    }
}

#[async_trait]
impl<M: ZircuitClientMessage + 'static> GenericMessageBus for AnyMessageBus<M> {
    async fn publish(&self, msg: &[u8]) -> anyhow::Result<u64> {
        self.inner.publish(msg).await
    }
    async fn publish_with_subject(
        &self,
        msg: &[u8],
        subject_suffix: String,
    ) -> anyhow::Result<u64> {
        self.inner.publish_with_subject(msg, subject_suffix).await
    }
    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }
    async fn next_message(&self) -> anyhow::Result<Option<Box<dyn ZircuitClientMessage>>> {
        match self.inner.next_message().await? {
            Some(msg) => Ok(Some(
                Box::new(AnyMessage::new(msg)) as Box<dyn ZircuitClientMessage>
            )),
            None => Ok(None),
        }
    }
}

pub struct AnyMessage<M: ZircuitClientMessage + 'static> {
    inner: M,
}

impl<M: ZircuitClientMessage + 'static> AnyMessage<M> {
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<M: ZircuitClientMessage + 'static> ZircuitClientMessage for AnyMessage<M> {
    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn payload(&self) -> Vec<u8> {
        self.inner.payload()
    }

    async fn ack(&self) -> anyhow::Result<()> {
        self.inner.ack().await
    }
}
