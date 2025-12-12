use crate::message_bus::{MessageBus, ZircuitClientMessage};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

type BasicMessageBusMessages = Mutex<VecDeque<(u64, Vec<u8>)>>;
/// A simple message bus implementation for testing purposes.
/// It stores messages in an in-memory queue.
pub struct BasicMessageBus {
    messages: Arc<BasicMessageBusMessages>,
    next_id: Arc<Mutex<u64>>,
    is_ready: Arc<Mutex<bool>>,
}

/// A simple message implementation for testing purposes.
pub struct BasicMessage {
    id: u64,
    payload: Vec<u8>,
    acknowledged: Arc<Mutex<bool>>,
}

impl BasicMessageBus {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(VecDeque::new())),
            next_id: Arc::new(Mutex::new(1)),
            is_ready: Arc::new(Mutex::new(true)),
        }
    }

    /// Set the ready state of the message bus
    #[allow(dead_code)]
    pub fn set_ready(&self, ready: bool) {
        let mut is_ready = self.is_ready.lock().unwrap();
        *is_ready = ready;
    }

    /// Get the number of messages in the queue
    pub fn message_count(&self) -> usize {
        let messages = self.messages.lock().unwrap();
        messages.len()
    }

    /// Clear all messages from the queue
    #[allow(dead_code)]
    pub fn clear_messages(&self) {
        let mut messages = self.messages.lock().unwrap();
        messages.clear();
    }
}

#[async_trait]
impl MessageBus<BasicMessage> for BasicMessageBus {
    async fn publish(&self, msg: &[u8]) -> anyhow::Result<u64> {
        tracing::info!(
            "Publish message. Messages in queue: {}",
            self.message_count()
        );
        // Check if the bus is ready
        if !self.is_ready() {
            return Err(anyhow::anyhow!("Message bus is not ready"));
        }

        // Get the next message ID
        let id = {
            let mut next_id = self.next_id.lock().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Add the message to the queue
        {
            let mut messages = self.messages.lock().unwrap();
            messages.push_back((id, msg.to_vec()));
        }

        Ok(id)
    }

    async fn publish_with_subject(&self, msg: &[u8], _: String) -> anyhow::Result<u64> {
        self.publish(msg).await
    }

    fn is_ready(&self) -> bool {
        let is_ready = self.is_ready.lock().unwrap();
        *is_ready
    }

    async fn next_message(&self) -> anyhow::Result<Option<BasicMessage>> {
        tracing::info!(
            "Get next message. Messages in queue: {}",
            self.message_count()
        );
        // Check if the bus is ready
        if !self.is_ready() {
            return Err(anyhow::anyhow!("Message bus is not ready"));
        }

        // Get the next message from the queue
        let message = {
            let mut messages = self.messages.lock().unwrap();
            messages.pop_front()
        };

        // If there's a message, create a BasicMessage from it
        if let Some((id, payload)) = message {
            Ok(Some(BasicMessage {
                id,
                payload,
                acknowledged: Arc::new(Mutex::new(false)),
            }))
        } else {
            Ok(None)
        }
    }
}

impl Clone for BasicMessageBus {
    fn clone(&self) -> Self {
        Self {
            messages: Arc::clone(&self.messages),
            next_id: Arc::clone(&self.next_id),
            is_ready: Arc::clone(&self.is_ready),
        }
    }
}

#[async_trait]
impl ZircuitClientMessage for BasicMessage {
    fn id(&self) -> u64 {
        self.id
    }

    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    async fn ack(&self) -> anyhow::Result<()> {
        let mut acknowledged = self.acknowledged.lock().unwrap();
        if *acknowledged {
            return Err(anyhow::anyhow!("Message already acknowledged"));
        }
        *acknowledged = true;
        Ok(())
    }
}

impl Clone for BasicMessage {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            payload: self.payload.clone(),
            acknowledged: Arc::clone(&self.acknowledged),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_basic_message_bus() {
        let bus = BasicMessageBus::new();

        assert!(bus.is_ready());

        assert_eq!(bus.message_count(), 0);

        // Create a test message
        let test_message = json!({
            "test": "message",
            "value": 123
        });
        let message_bytes = serde_json::to_vec(&test_message).unwrap();

        // Publish the message
        let id = bus.publish(&message_bytes).await.unwrap();

        // Check that the ID is 1 (first message)
        assert_eq!(id, 1);

        // Check that there is now 1 message
        assert_eq!(bus.message_count(), 1);

        let message = bus.next_message().await.unwrap().unwrap();
        assert_eq!(message.id(), 1);

        // Check that the message has the correct payload
        let payload = message.payload();
        let payload_json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(payload_json, test_message);

        message.ack().await.unwrap();

        assert!(message.ack().await.is_err());

        assert_eq!(bus.message_count(), 0);
        assert!(bus.next_message().await.unwrap().is_none());

        // Set the bus to not ready
        bus.set_ready(false);
        assert!(!bus.is_ready());

        // Try to publish a message (should fail)
        assert!(bus.publish(&message_bytes).await.is_err());

        // Try to get a message (should fail)
        assert!(bus.next_message().await.is_err());

        // Set the bus back to ready
        bus.set_ready(true);

        // Check that the bus is ready again
        assert!(bus.is_ready());

        // Publish multiple messages
        let id1 = bus.publish(&message_bytes).await.unwrap();
        let id2 = bus.publish(&message_bytes).await.unwrap();
        let id3 = bus.publish(&message_bytes).await.unwrap();

        // Check that the IDs are sequential
        assert_eq!(id1, 2);
        assert_eq!(id2, 3);
        assert_eq!(id3, 4);

        // Check that there are 3 messages
        assert_eq!(bus.message_count(), 3);

        // Clear the messages
        bus.clear_messages();

        // Check that there are no messages
        assert_eq!(bus.message_count(), 0);
    }
}
