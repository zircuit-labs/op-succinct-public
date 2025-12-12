use crate::message_bus::{MessageBus, ZircuitClientMessage};
use async_nats::client::ReconnectError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::time::Duration;

const NATS_CONSUMER_NAME: &str = "zircuit_proposer";
const MAX_STREAM_MESSAGES: i64 = 10_000;
const ACK_TIME_SECS: u64 = 120;
const MAX_RECONNECT_ATTEMPTS: usize = 5;
const BATCH_MESSAGES: usize = 1;

#[derive(Debug, Clone)]
pub struct NatsMessageBus {
    config: NatsBusConfig,
    nats_client: async_nats::client::Client,
    jetstream_client: async_nats::jetstream::Context,
    consumer: async_nats::jetstream::consumer::PullConsumer,
}

impl NatsMessageBus {
    pub async fn new(config: &NatsBusConfig) -> anyhow::Result<NatsMessageBus> {
        tracing::info!("Establishing NATS connection to: {}", config.nats_url);

        let mut connection_options =
            async_nats::ConnectOptions::new().max_reconnects(MAX_RECONNECT_ATTEMPTS);

        if let Some(credentials_path) = &config.credentials_path {
            tracing::info!("Using NATS credentials file");
            let creds = std::fs::read_to_string(credentials_path)?;
            connection_options = connection_options.credentials(&creds)?;
        }
        let client = connection_options.connect(&config.nats_url).await?;
        let jetstream = async_nats::jetstream::new(client.clone());

        let (_, consumer) =
            NatsMessageBus::get_or_create_stream_and_consumer(config, &jetstream).await?;

        Ok(NatsMessageBus {
            config: config.clone(),
            nats_client: client,
            jetstream_client: jetstream,
            consumer,
        })
    }

    pub async fn try_reconnecting(&self) -> Result<(), ReconnectError> {
        if self.is_ready() {
            return Ok(());
        }
        self.nats_client.force_reconnect().await
    }

    pub async fn get_or_create_stream_and_consumer(
        config: &NatsBusConfig,
        js: &async_nats::jetstream::Context,
    ) -> anyhow::Result<(
        async_nats::jetstream::stream::Stream,
        async_nats::jetstream::consumer::PullConsumer,
    )> {
        tracing::info!("Initializing NATS stream");
        let stream_subjects = format!("{}.>", config.stream);
        tracing::info!("Creating NATS stream with subjects: {stream_subjects}");
        let stream_config = async_nats::jetstream::stream::Config {
            name: config.stream.clone(),
            max_messages: MAX_STREAM_MESSAGES,
            subjects: vec![stream_subjects],
            ..Default::default()
        };
        let stream = js.get_or_create_stream(stream_config).await?;

        let consumer_name = format!(
            "{}__{}",
            NATS_CONSUMER_NAME,
            config.input_subject.replace('.', "_")
        );
        tracing::info!("Creating NATS consumer with name: {}", consumer_name);
        let consumer_config = async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(consumer_name.clone()),
            filter_subject: config.input_subject.clone(),
            ack_wait: Duration::from_secs(ACK_TIME_SECS),
            // todo: add configurable deliver policy?
            ..Default::default()
        };
        let consumer = stream
            .get_or_create_consumer(&consumer_name, consumer_config)
            .await?;

        tracing::info!("NATS consumer created");
        Ok((stream, consumer))
    }
}

#[async_trait]
impl MessageBus<async_nats::jetstream::Message> for NatsMessageBus {
    async fn publish(&self, msg: &[u8]) -> anyhow::Result<u64> {
        let output_subject = self.config.output_subject.clone();

        tracing::info!("Publishing message to NATS: {output_subject}");
        let publish_ack = self
            .jetstream_client
            .publish(output_subject, Bytes::copy_from_slice(msg))
            .await?
            .await?;

        Ok(publish_ack.sequence)
    }

    async fn publish_with_subject(
        &self,
        msg: &[u8],
        subject_suffix: String,
    ) -> anyhow::Result<u64> {
        let mut output_subject = self.config.output_subject.clone();
        output_subject.push_str(&format!(".{}", &subject_suffix));

        tracing::info!("Publishing message to NATS: {output_subject}");
        let publish_ack = self
            .jetstream_client
            .publish(output_subject, Bytes::copy_from_slice(msg))
            .await?
            .await?;

        Ok(publish_ack.sequence)
    }

    fn is_ready(&self) -> bool {
        self.nats_client.connection_state() == async_nats::connection::State::Connected
    }

    async fn next_message(&self) -> anyhow::Result<Option<async_nats::jetstream::Message>> {
        match self.is_ready() {
            true => {
                let mut batch = self
                    .consumer
                    .fetch()
                    .max_messages(BATCH_MESSAGES)
                    .messages()
                    .await?;

                match batch.next().await {
                    Some(Ok(msg)) => {
                        let id = match msg.info() {
                            Ok(info) => info.stream_sequence,
                            Err(_) => {
                                tracing::warn!(
                                    "Error getting message info. Defaulting to 0 for message id"
                                );
                                0
                            }
                        };
                        tracing::info!("Received message from NATS with sequence number: {id}");
                        Ok(Some(msg))
                    }
                    Some(Err(e)) => {
                        tracing::warn!("Error pulling message from NATS batch: {e:?}");
                        Err(anyhow::anyhow!(
                            "Error pulling message from NATS batch: {e}"
                        ))
                    }
                    None => Ok(None),
                }
            }
            false => {
                tracing::warn!("NATS connection not ready. Try reconnecting");
                let _ = self.try_reconnecting().await;
                Err(anyhow::anyhow!("Message bus is not ready"))
            }
        }
    }
}

#[async_trait]
impl ZircuitClientMessage for async_nats::jetstream::Message {
    fn id(&self) -> u64 {
        match self.info() {
            Ok(info) => info.stream_sequence,
            Err(_) => {
                tracing::warn!("Error getting message info. Defaulting to 0 for message id");
                0
            }
        }
    }

    fn payload(&self) -> Vec<u8> {
        self.message.payload.clone().into()
    }

    async fn ack(&self) -> anyhow::Result<()> {
        async_nats::jetstream::Message::ack(self)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NatsBusConfig {
    pub nats_url: String,
    pub stream: String, // split into input and output stream if necessary
    pub input_subject: String,
    pub output_subject: String,
    pub credentials_path: Option<String>,
}
