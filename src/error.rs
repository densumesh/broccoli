/// Error types for the Broccoli message queue system.
///
/// This enum represents all possible errors that can occur within the Broccoli system,
/// including broker operations, message handling, and serialization errors.
#[derive(Debug, thiserror::Error)]
pub enum BroccoliError {
    /// Represents errors that occur during broker operations.
    ///
    /// # Examples
    /// - Connection failures
    /// - Pool initialization errors
    #[error("Broker error: {0}")]
    Broker(String),

    /// Represents errors that occur during message publishing.
    ///
    /// # Examples
    /// - Failed to send message to broker
    /// - Message serialization errors
    #[error("Failed to publish message: {0}")]
    Publish(String),

    /// Represents errors that occur during message consumption.
    ///
    /// # Examples
    /// - Failed to retrieve message from broker
    /// - Message deserialization errors
    #[error("Failed to consume message: {0}")]
    Consume(String),

    /// Represents errors that occur during message acknowledgment.
    ///
    /// # Examples
    /// - Failed to acknowledge message processing
    /// - Failed to remove message from processing queue
    #[error("Failed to acknowledge message: {0}")]
    Acknowledge(String),

    /// Represents errors that occur during message rejection.
    ///
    /// # Examples
    /// - Failed to reject message processing
    /// - Failed to remove message from processing queue
    #[error("Failed to reject message: {0}")]
    Reject(String),

    /// Represents errors that occur during message cacnelling.
    ///
    /// # Examples
    /// - Failed to cancel message processing
    /// - Failed to remove message from processing queue
    #[error("Failed to cancel message: {0}")]
    Cancel(String),

    /// Represents errors that occur during getting a messages position.
    ///
    /// # Examples
    /// - Failed to get message position
    #[error("Failed to get message position: {0}")]
    GetMessagePosition(String),

    /// Represents errors that occur during message serialization/deserialization.
    ///
    /// This variant wraps the underlying `serde_json` error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Represents Redis-specific errors.
    ///
    /// This variant wraps the underlying Redis error.
    #[cfg(feature = "redis")]
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// Represents errors that occur during job processing.
    #[error("Job error: {0}")]
    Job(String),

    /// Represents errors that occur during checking queue status.
    #[error("Queue status error: {0}")]
    QueueStatus(String),

    /// Represents connection timeout errors.
    ///
    /// # Arguments
    /// * `0` - The number of retry attempts that were made before timing out
    #[error("Connection timeout after {0} retries")]
    ConnectionTimeout(u32),

    /// Represents errors that occur when a feature is not implemented.
    ///
    /// # Examples
    /// - Getting the position of a message for `RabbitMQ`
    #[error("Feature not implemented")]
    NotImplemented,
}
