use std::collections::HashMap;

use redis::AsyncCommands;

use crate::{
    brokers::{
        broker::Broker,
        management::{BrokerWithManagement, QueueManagement, QueueStatus, QueueType},
    },
    error::BroccoliError,
};

use super::{utils::OptionalInternalBrokerMessage, RedisBroker};

#[async_trait::async_trait]
impl QueueManagement for RedisBroker {
    async fn retry_queue(
        &self,
        queue_name: &str,
        disambiguator: Option<String>,
        source_type: QueueType,
    ) -> Result<usize, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let source_queue = if let Some(ref disambiguator) = disambiguator {
            match source_type {
                QueueType::Failed => format!("{queue_name}_{disambiguator}_failed"),
                QueueType::Processing => format!("{queue_name}_{disambiguator}_processing"),
                QueueType::Main => {
                    return Err(BroccoliError::InvalidOperation(
                        "Cannot retry from ingestion queue".into(),
                    ))
                }
            }
        } else {
            match source_type {
                QueueType::Failed => format!("{queue_name}_failed"),
                QueueType::Processing => format!("{queue_name}_processing"),
                QueueType::Main => {
                    return Err(BroccoliError::InvalidOperation(
                        "Cannot retry from ingestion queue".into(),
                    ))
                }
            }
        };

        let count = redis.llen(&source_queue).await?;

        if count == 0 {
            return Ok(0);
        }

        let mut messages = vec![];
        for _ in 0..count {
            let task_id: Option<String> = redis.lpop(&source_queue, None).await?;
            let message: OptionalInternalBrokerMessage = redis
                .hgetall(&task_id)
                .await
                .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
            if let Some(mut message) = message.0 {
                message.attempts = 0;
                messages.push(message);
            }
        }

        self.publish(queue_name, disambiguator, &messages, None)
            .await?;

        Ok(count)
    }

    async fn get_queue_size(
        &self,
        queue_name: &str,
        queue_type: QueueType,
    ) -> Result<usize, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let queue = match queue_type {
            QueueType::Failed => format!("{queue_name}_failed"),
            QueueType::Processing => format!("{queue_name}_processing"),
            QueueType::Main => queue_name.to_string(),
        };

        match queue_type {
            QueueType::Main => redis.zcard(&queue).await.map_err(std::convert::Into::into),
            _ => redis.llen(&queue).await.map_err(std::convert::Into::into),
        }
    }

    async fn get_queue_status(
        &self,
        queue_name: Option<&str>,
    ) -> Result<Vec<QueueStatus>, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let mut keys: Vec<String> = if let Some(queue_name) = queue_name {
            redis.keys(&format!("{}*", queue_name)).await?
        } else {
            redis.keys("*").await?
        };

        keys.sort();

        let mut queues: HashMap<String, QueueStatus> = HashMap::new();
        let grouped_keys = keys.iter().fold(queues, |mut acc, key| {
            let (queue_name, queue_type) =
                if key.ends_with("_failed") || key.ends_with("_processing") {
                    let queue_name = key
                        .trim_end_matches("_failed")
                        .trim_end_matches("_processing");
                    let queue_type = if key.ends_with("_failed") {
                        QueueType::Failed
                    } else {
                        QueueType::Processing
                    };
                    (queue_name, queue_type)
                } else if redis.key_type::<&str, String>(&key).await? == *"zset" {
                    let queue_name = key;
                    let queue_type = QueueType::Main;
                    (queue_name, queue_type)
                } else {
                    return acc;
                };

            let status = acc.entry(queue_name.to_string()).or_insert(QueueStatus {
                name: queue_name.to_string(),
                queue_type,
                size: 0,
            });

            match queue_type {
                QueueType::Main => {
                    let size: usize = redis.zcard(&queue_name).await.unwrap_or(0);
                    status.size = size;
                }
                _ => {
                    let size: usize = redis.llen(&key).await.unwrap_or(0);
                    match queue_type {
                        QueueType::Failed => status.failed = size,
                        QueueType::Processing => status.processing = size,
                        _ => {}
                    }
                }
            }

            acc
        });

        for key in keys {
            let (size, queue_type) = if key.ends_with("_failed") || key.ends_with("_processing") {
                let size: usize = redis.llen(&key).await?;
                let queue_type = if key.ends_with("_failed") {
                    QueueType::Failed
                } else {
                    QueueType::Processing
                };
                (size, queue_type)
            } else if redis.key_type::<&str, String>(&key).await? == *"zset" {
                let size: usize = redis.zcard(&key).await?;
                (size, QueueType::Main)
            } else {
                continue;
            };

            QueueStatus {
                name: key,
                queue_type,
                size,
            }
        }

        Ok(statuses)
    }
}

impl BrokerWithManagement for RedisBroker {}
