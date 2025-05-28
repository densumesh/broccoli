use super::broker::Broker;
use crate::error::BroccoliError;
use derive_more::Display;

#[async_trait::async_trait]
/// Trait for managing the state of broccoli queues.
pub trait QueueManagement {
    /// Gets the status of the queue with the specified queue name.
    async fn get_queue_status(
        &self,
        queue_name: String,
        disambiguator: Option<String>,
    ) -> Result<QueueStatus, BroccoliError>;
}

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

pub(crate) trait BrokerWithManagement: Broker + QueueManagement {}

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
