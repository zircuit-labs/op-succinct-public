pub mod message_bus;

use async_trait::async_trait;

#[async_trait]
pub trait ProposerMessage: Send + Sync {
    fn id(&self) -> u64;
    fn payload(&self) -> Vec<u8>;
    #[allow(dead_code)]
    async fn ack(&self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait MessageBus<M: ProposerMessage + 'static>: Send + Sync {
    async fn publish(&self, msg: &[u8]) -> anyhow::Result<u64>;
    fn is_ready(&self) -> bool;
    async fn next_message(&self) -> anyhow::Result<Option<M>>;
}
