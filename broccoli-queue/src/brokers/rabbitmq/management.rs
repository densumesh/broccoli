use lapin::{
    options::{BasicAckOptions, BasicGetOptions, BasicPublishOptions},
    BasicProperties,
};

use crate::{
    brokers::management::{BrokerWithManagement, QueueManagement, QueueStatus, QueueType},
    error::BroccoliError,
};

use super::RabbitMQBroker;

#[async_trait::async_trait]
impl QueueManagement for RabbitMQBroker {
    async fn retry_queue(
        &self,
        queue_name: String,
        source_type: QueueType,
    ) -> Result<usize, BroccoliError> {
        let pool = self.ensure_pool()?;
        let conn = pool
            .get()
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
        let channel = conn.create_channel().await?;

        let source_queue = match source_type {
            QueueType::Failed => format!("{queue_name}_failed"),
            QueueType::Processing => format!("{queue_name}_processing"),
            QueueType::Main => {
                return Err(BroccoliError::InvalidOperation(
                    "Cannot retry from ingestion queue".into(),
                ))
            }
            QueueType::Fairness => {
                return Err(BroccoliError::InvalidOperation(
                    "Cannot retry from fairness queue".into(),
                ))
            }
        };

        let mut count = 0;
        while let Some(delivery) = channel
            .basic_get(&source_queue, BasicGetOptions::default())
            .await?
        {
            channel
                .basic_publish(
                    "broccoli",
                    &queue_name,
                    BasicPublishOptions::default(),
                    &delivery.data,
                    BasicProperties::default(),
                )
                .await?;

            channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await?;
            count += 1;
        }

        Ok(count)
    }

    async fn get_queue_status(
        &self,
        queue_name: Option<String>,
    ) -> Result<Vec<QueueStatus>, BroccoliError> {
        let pool = self.ensure_pool()?;
        let conn = pool
            .get()
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
        let channel = conn.create_channel().await?;

        // List queues through management API or channel operations
        // This is a simplified version - in practice you'd want to use the RabbitMQ Management API
        let statuses = Vec::new();

        // Implementation note: RabbitMQ doesn't provide memory usage through regular AMQP
        // You would need to use the HTTP Management API to get this information

        Ok(statuses)
    }
}

impl BrokerWithManagement for RabbitMQBroker {}
