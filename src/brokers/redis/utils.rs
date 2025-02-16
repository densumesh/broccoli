// ...existing code...

#[async_trait::async_trait]
impl QueueManagement for RedisBroker {
    async fn retry_queue(&self, queue_name: &str, source_type: QueueType) -> Result<usize, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let source_queue = match source_type {
            QueueType::Failed => format!("{}_failed", queue_name),
            QueueType::Processing => format!("{}_processing", queue_name),
            QueueType::Ingestion => return Err(BroccoliError::InvalidOperation("Cannot retry from ingestion queue".into())),
        };

        let count: usize = redis.llen(&source_queue).await.map_err(|e| BroccoliError::Broker(e.to_string()))?;
        if count == 0 {
            return Ok(0);
        }

        for _ in 0..count {
            let message: Option<String> = redis.lpop(&source_queue, None).await?;
            if let Some(msg) = message {
                redis.zadd(
                    queue_name,
                    msg,
                    time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64,
                ).await?;
            }
        }

        Ok(count)
    }

    async fn get_queue_size(&self, queue_name: &str, queue_type: QueueType) -> Result<usize, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let queue = match queue_type {
            QueueType::Failed => format!("{}_failed", queue_name),
            QueueType::Processing => format!("{}_processing", queue_name),
            QueueType::Ingestion => queue_name.to_string(),
        };

        match queue_type {
            QueueType::Ingestion => redis.zcard(&queue).await.map_err(|e| e.into()),
            _ => redis.llen(&queue).await.map_err(|e| e.into()),
        }
    }

    async fn get_queue_status(&self) -> Result<Vec<QueueStatus>, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let mut keys: Vec<String> = redis.keys("*").await?;
        keys.sort();

        let mut statuses = Vec::new();
        for key in keys {
            let (size, queue_type) = if key.ends_with("_failed") || key.ends_with("_processing") {
                let size: usize = redis.llen(&key).await?;
                let queue_type = if key.ends_with("_failed") {
                    QueueType::Failed
                } else {
                    QueueType::Processing
                };
                (size, queue_type)
            } else {
                let size: usize = redis.zcard(&key).await?;
                (size, QueueType::Ingestion)
            };

            let memory: i64 = redis.memory_usage(&key, None).await?;
            
            statuses.push(QueueStatus {
                name: key,
                queue_type,
                size,
                memory_usage: memory as u64,
            });
        }

        Ok(statuses)
    }
}
