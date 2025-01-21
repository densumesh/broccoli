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

    /// Gets the position of a message in the queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message_id` - The ID of the message.
    ///
    /// # Returns
    /// A `Result` containing the position of the message in the queue, or `None` if the message is not found.
    async fn get_message_position(
        &self,
        queue_name: &str,
        message_id: String,
    ) -> Result<Option<usize>, BroccoliError>;
}

/// Configuration options for broker behavior.
pub struct BrokerConfig {
    /// Maximum number of retry attempts for failed messages
    pub retry_attempts: Option<u8>,
    /// Whether to retry failed messages
    pub retry_failed: Option<bool>,
    /// Number of connections to maintain in the connection pool
    pub pool_connections: Option<u8>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            retry_attempts: Some(3),
            retry_failed: Some(true),
            pool_connections: Some(10),
        }
    }
}

/// A wrapper for messages that includes metadata for processing.
///
/// # Type Parameters
/// * `T` - The type of the payload, must implement Clone and Serialize
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrokerMessage<T: Clone + serde::Serialize> {
    /// Unique identifier for the message
    pub task_id: uuid::Uuid,
    /// The actual message content
    pub payload: T,
    /// Number of processing attempts made
    pub attempts: u8,
    /// Additional metadata for the message
    #[serde(skip)]
    pub metadata: Option<HashMap<String, String>>,
}

impl<T: Clone + serde::Serialize> BrokerMessage<T> {
    /// Creates a new `BrokerMessage` with the provided payload.
    pub fn new(payload: T) -> Self {
        BrokerMessage {
            task_id: uuid::Uuid::new_v4(),
            payload,
            attempts: 0,
            metadata: None,
        }
    }

    /// Creates a new `BrokerMessage` with the provided payload and number of attempts.
    pub fn new_with_attempts(payload: T, attempts: u8) -> Self {
        BrokerMessage {
            task_id: uuid::Uuid::new_v4(),
            payload,
            attempts,
            metadata: None,
        }
    }
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
    /// Additional metadata for the message
    #[serde(skip)]
    pub metadata: Option<HashMap<String, String>>,
}

impl InternalBrokerMessage {
    /// Creates a new `InternalBrokerMessage` with the provided metadata.
    pub fn new(
        task_id: String,
        payload: String,
        attempts: u8,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        InternalBrokerMessage {
            task_id,
            payload,
            attempts,
            metadata,
        }
    }
}

impl<T: Clone + serde::Serialize> From<BrokerMessage<T>> for InternalBrokerMessage {
    fn from(msg: BrokerMessage<T>) -> Self {
        InternalBrokerMessage {
            task_id: msg.task_id.to_string(),
            payload: serde_json::to_string(&msg.payload).unwrap_or_default(),
            attempts: msg.attempts,
            metadata: msg.metadata,
        }
    }
}

impl<T: Clone + serde::Serialize> From<&BrokerMessage<T>> for InternalBrokerMessage {
    fn from(msg: &BrokerMessage<T>) -> Self {
        InternalBrokerMessage {
            task_id: msg.task_id.to_string(),
            payload: serde_json::to_string(&msg.payload).unwrap_or_default(),
            attempts: msg.attempts,
            metadata: msg.metadata.clone(),
        }
    }
}

impl InternalBrokerMessage {
    /// Converts the internal message to a `BrokerMessage`.
    pub fn into_message<T: Clone + serde::de::DeserializeOwned + serde::Serialize>(
        &self,
    ) -> Result<BrokerMessage<T>, BroccoliError> {
        Ok(BrokerMessage {
            task_id: self.task_id.parse().unwrap_or_default(),
            payload: serde_json::from_str(&self.payload).map_err(|e| {
                BroccoliError::Broker(format!("Failed to parse message payload: {}", e))
            })?,
            attempts: self.attempts,
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
}
