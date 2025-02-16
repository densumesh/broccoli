use lapin::{
    options::{BasicAckOptions, BasicGetOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
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
        queue_name: &str,
        _disambiguator: Option<String>,
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
        };

        let mut count = 0;
        while let Some(delivery) = channel
            .basic_get(&source_queue, BasicGetOptions::default())
            .await?
        {
            channel
                .basic_publish(
                    "broccoli",
                    queue_name,
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

    async fn get_queue_size(
        &self,
        queue_name: &str,
        queue_type: QueueType,
    ) -> Result<usize, BroccoliError> {
        let pool = self.ensure_pool()?;
        let conn = pool
            .get()
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
        let channel = conn.create_channel().await?;

        let queue = match queue_type {
            QueueType::Failed => format!("{queue_name}_failed"),
            QueueType::Processing => format!("{queue_name}_processing"),
            QueueType::Main => queue_name.to_string(),
        };

        let queue_info = channel
            .queue_declare(
                &queue,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(queue_info.message_count() as usize)
    }

    async fn get_queue_status(&self) -> Result<Vec<QueueStatus>, BroccoliError> {
        let pool = self.ensure_pool()?;
        let conn = pool
            .get()
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
        let channel = conn.create_channel().await?;

        // List queues through management API or channel operations
        // This is a simplified version - in practice you'd want to use the RabbitMQ Management API
        let mut statuses = Vec::new();

        // Implementation note: RabbitMQ doesn't provide memory usage through regular AMQP
        // You would need to use the HTTP Management API to get this information

        Ok(statuses)
    }
}

impl BrokerWithManagement for RabbitMQBroker {}
