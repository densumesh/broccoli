use derive_more::Display;

use crate::error::BroccoliError;

use super::broker::Broker;

#[async_trait::async_trait]
/// Trait for managing queues.
pub trait QueueManagement {
    /// Retries messages in the queue.
    async fn retry_queue(
        &self,
        queue_name: String,
        source_type: QueueType,
    ) -> Result<usize, BroccoliError>;
    /// Gets the status of specific or all queues. If `queue_name` is `None`, returns the status of all queues.
    async fn get_queue_status(
        &self,
        queue_name: Option<String>,
    ) -> Result<Vec<QueueStatus>, BroccoliError>;
}

pub(crate) trait BrokerWithManagement: Broker + QueueManagement {}

#[derive(Debug, Clone, PartialEq, Eq, Display)]
/// Enum representing the type of queue.
pub enum QueueType {
    /// Failed queue.
    #[display("failed")]
    Failed,
    /// Processing queue.
    #[display("processing")]
    Processing,
    /// Main queue.
    #[display("main")]
    Main,
    /// Fairness queue.
    #[display("fairness")]
    Fairness,
}

#[derive(Debug, Clone)]
/// Struct representing the status of a queue.
pub struct QueueStatus {
    /// Name of the queue.
    pub name: String,
    /// Type of the queue.
    pub queue_type: QueueType,
    /// Size of the queue.
    pub size: usize,
    /// Number of messages that are being processed.
    pub processing: usize,
    /// Number of messages that failed to be processed.
    pub failed: usize,
    /// If the queue is a fairness queue, the number of disambiguators.
    pub disambiguator_count: Option<usize>,
}
