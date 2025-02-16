#[async_trait::async_trait]
impl QueueManagement for RabbitMQBroker {
    async fn retry_queue(&self, queue_name: &str, source_type: QueueType) -> Result<usize, BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await?;
        let channel = conn.create_channel().await?;
        
        let source_queue = match source_type {
            QueueType::Failed => format!("{}_failed", queue_name),
            QueueType::Processing => format!("{}_processing", queue_name),
            QueueType::Ingestion => return Err(BroccoliError::InvalidOperation("Cannot retry from ingestion queue".into())),
        };

        let mut count = 0;
        while let Some(delivery) = channel.basic_get(&source_queue, BasicGetOptions::default()).await? {
            channel.basic_publish(
                "broccoli",
                queue_name,
                BasicPublishOptions::default(),
                delivery.data,
                BasicProperties::default()
            ).await?;
            
            channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).await?;
            count += 1;
        }

        Ok(count)
    }

    async fn get_queue_size(&self, queue_name: &str, queue_type: QueueType) -> Result<usize, BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await?;
        let channel = conn.create_channel().await?;

        let queue = match queue_type {
            QueueType::Failed => format!("{}_failed", queue_name),
            QueueType::Processing => format!("{}_processing", queue_name),
            QueueType::Ingestion => queue_name.to_string(),
        };

        let queue_info = channel.queue_declare(&queue, QueueDeclareOptions::default(), FieldTable::default()).await?;
        Ok(queue_info.message_count() as usize)
    }

    async fn get_queue_status(&self) -> Result<Vec<QueueStatus>, BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await?;
        let channel = conn.create_channel().await?;

        // List queues through management API or channel operations
        // This is a simplified version - in practice you'd want to use the RabbitMQ Management API
        let mut statuses = Vec::new();
        
        // Implementation note: RabbitMQ doesn't provide memory usage through regular AMQP
        // You would need to use the HTTP Management API to get this information
        
        Ok(statuses)
    }
}
