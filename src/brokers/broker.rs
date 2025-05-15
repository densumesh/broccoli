use std::collections::HashMap;

use crate::{
    error::BroccoliError,
    queue::{ConsumeOptions, PublishOptions},
};

/// Trait for message broker implementations.
#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    /// Connects to the broker using the provided URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError>;

    /// Publishes a message to the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn publish(
        &self,
        queue_name: &str,
        disambiguator: Option<String>,
        message: &[InternalBrokerMessage],
        options: Option<PublishOptions>,
    ) -> Result<Vec<InternalBrokerMessage>, BroccoliError>;

    /// Attempts to consume a message from the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing an `Some(String)` with the message if available or `None`
    /// if no message is avaiable, and a `BroccoliError` on failure.
    async fn try_consume(
        &self,
        queue_name: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<InternalBrokerMessage>, BroccoliError>;

    /// Consumes a message from the specified queue, blocking until a message is available.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing the message as a `String`, or a `BroccoliError` on failure.
    async fn consume(
        &self,
        queue_name: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<InternalBrokerMessage, BroccoliError>;

    /// Acknowledges the processing of a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be acknowledged.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn acknowledge(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError>;

    /// Rejects a message, re-queuing it or moving it to a failed queue if the retry limit is reached.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be rejected.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn reject(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError>;

    /// Cancels a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message_id` - The ID of the message to be canceled.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn cancel(&self, queue_name: &str, message_id: String) -> Result<(), BroccoliError>;
}

/// Configuration options for broker behavior.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Maximum number of retry attempts for failed messages
    pub retry_attempts: Option<u8>,
    /// Whether to retry failed messages
    pub retry_failed: Option<bool>,
    /// Number of connections to maintain in the connection pool
    pub pool_connections: Option<u8>,
    /// Whether to enable scheduling for messages
    ///
    /// NOTE: If you enable this w/ rabbitmq, you will need to install the delayed-exchange plugin
    /// <https://www.rabbitmq.com/blog/2015/04/16/scheduling-messages-with-rabbitmq>
    pub enable_scheduling: Option<bool>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            retry_attempts: Some(3),
            retry_failed: Some(true),
            pool_connections: Some(10),
            enable_scheduling: Some(false),
        }
    }
}

/// A wrapper for messages that includes metadata for processing.
///
/// # Type Parameters
/// * `T` - The type of the payload, must implement Clone and Serialize
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrokerMessage<T: Clone> {
    /// Unique identifier for the message
    pub task_id: uuid::Uuid,
    /// The actual message content
    pub payload: T,
    /// Number of processing attempts made
    pub attempts: u8,
    /// Disambiguator for message fairness
    pub disambiguator: Option<String>,
    /// Additional metadata for the message
    #[serde(skip)]
    pub(crate) metadata: Option<HashMap<String, MetadataTypes>>,
}

impl<T: Clone + serde::Serialize> BrokerMessage<T> {
    /// Creates a new `BrokerMessage` with the provided payload.
    pub fn new(payload: T, disambiguator: Option<String>) -> Self {
        Self {
            task_id: uuid::Uuid::new_v4(),
            payload,
            attempts: 0,
            disambiguator,
            metadata: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum MetadataTypes {
    String(String),
    U64(u64),
}

/// A message with metadata for internal broker operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InternalBrokerMessage {
    /// Unique identifier for the message
    pub task_id: String,
    /// The actual message content stringified
    pub payload: String,
    /// Number of processing attempts made
    pub attempts: u8,
    /// Disambiguator for message fairness
    pub disambiguator: Option<String>,
    /// Additional metadata for the message
    #[serde(skip)]
    pub(crate) metadata: Option<HashMap<String, MetadataTypes>>,
}

impl InternalBrokerMessage {
    /// Creates a new `InternalBrokerMessage` with the provided metadata.
    #[must_use]
    pub const fn new(
        task_id: String,
        payload: String,
        attempts: u8,
        disambiguator: Option<String>,
    ) -> Self {
        Self {
            task_id,
            payload,
            attempts,
            disambiguator,
            metadata: None,
        }
    }
}

impl<T: Clone + serde::Serialize> From<BrokerMessage<T>> for InternalBrokerMessage {
    fn from(msg: BrokerMessage<T>) -> Self {
        Self {
            task_id: msg.task_id.to_string(),
            payload: serde_json::to_string(&msg.payload).unwrap_or_default(),
            attempts: msg.attempts,
            disambiguator: msg.disambiguator,
            metadata: msg.metadata,
        }
    }
}

impl<T: Clone + serde::Serialize> From<&BrokerMessage<T>> for InternalBrokerMessage {
    fn from(msg: &BrokerMessage<T>) -> Self {
        Self {
            task_id: msg.task_id.to_string(),
            payload: serde_json::to_string(&msg.payload).unwrap_or_default(),
            attempts: msg.attempts,
            disambiguator: msg.disambiguator.clone(),
            metadata: msg.metadata.clone(),
        }
    }
}

impl InternalBrokerMessage {
    /// Converts the internal message to a `BrokerMessage`.
    ///
    /// # Returns
    /// A `Result` containing the `BrokerMessage` or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// If the payload cannot be deserialized.
    pub fn into_message<T: Clone + serde::de::DeserializeOwned + serde::Serialize>(
        &self,
    ) -> Result<BrokerMessage<T>, BroccoliError> {
        Ok(BrokerMessage {
            task_id: self.task_id.parse().unwrap_or_default(),
            payload: serde_json::from_str(&self.payload).map_err(|e| {
                BroccoliError::Broker(format!("Failed to parse message payload: {e}"))
            })?,
            attempts: self.attempts,
            disambiguator: self.disambiguator.clone(),
            metadata: self.metadata.clone(),
        })
    }
}

/// Supported message broker implementations.
pub enum BrokerType {
    /// Redis-based message broker
    #[cfg(feature = "redis")]
    Redis,
    /// RabbitMQ-based message broker
    #[cfg(feature = "rabbitmq")]
    RabbitMQ,
    /// SurrealDB-based message broker
    #[cfg(feature = "surrealdb")]
    SurrealDB,
}
