
use crate::{
    brokers::management::{BrokerWithManagement, QueueManagement, QueueStatus, QueueType},
    error::BroccoliError,
};

use super::RabbitMQBroker;

#[async_trait::async_trait]
impl QueueManagement for RabbitMQBroker {
    async fn get_queue_status(
        &self,
        queue_name: String,
        disambiguator: Option<String>,
    ) -> Result<QueueStatus, BroccoliError> {
        let pool = self.ensure_pool()?;
        let conn = pool
            .get()
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
        let _channel = conn.create_channel().await.map_err(|e| {
            BroccoliError::Consume(format!("Failed to create channel: {e:?}"))
        })?;
        
        // List queues through management API or channel operations
        // This is a simplified version - in practice you'd want to use the RabbitMQ Management API
        let statuses = QueueStatus {
            name: queue_name,
            queue_type: QueueType::Main,
            size: 0,
            processing: 0,
            failed: 0,
            disambiguator_count: None,
        };

        // Implementation note: RabbitMQ doesn't provide memory usage through regular AMQP
        // You would need to use the HTTP Management API to get this information
        Ok(statuses)
    }
}

impl BrokerWithManagement for RabbitMQBroker {}
