use std::collections::{HashMap, HashSet};

use redis::AsyncCommands;

use crate::{
    brokers::management::{BrokerWithManagement, QueueManagement, QueueStatus, QueueType},
    error::BroccoliError,
};

use super::RedisBroker;

#[async_trait::async_trait]
impl QueueManagement for RedisBroker {
    async fn retry_queue(
        &self,
        queue_name: String,
        source_type: QueueType,
    ) -> Result<usize, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;

        if source_type == QueueType::Main {
            return Err(BroccoliError::InvalidOperation(
                "Cannot retry from main queue".into(),
            ));
        }

        let fairness_pattern = format!("{queue_name}*_{}", source_type.to_string().to_lowercase());
        let is_fairness_queue = !redis
            .keys::<&String, Vec<String>>(&fairness_pattern)
            .await?
            .is_empty();

        let mut total_retried = 0;

        if is_fairness_queue {
            let failed_pattern =
                format!("{}*_{}", queue_name, source_type.to_string().to_lowercase());
            let failed_queues: Vec<String> = redis.keys(&failed_pattern).await?;

            for failed_queue in failed_queues {
                println!("Retrying from queue: {failed_queue}");
                let parts: Vec<&str> = failed_queue.split('_').collect();
                if parts.len() < 3 {
                    continue;
                }

                let disambiguator = parts[1];

                let target_queue = format!("{queue_name}_{disambiguator}_queue");

                let count = redis.llen(&failed_queue).await?;
                if count == 0 {
                    continue;
                }

                for _ in 0..count {
                    let task_id: Option<String> = redis.lpop(&failed_queue, None).await?;
                    if let Some(task_id) = task_id {
                        redis
                            .zadd::<&str, i64, &str, ()>(
                                &target_queue,
                                &task_id,
                                time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
                            )
                            .await?;
                    }
                }

                let fairness_set = format!("{queue_name}_fairness_set");
                redis
                    .sadd::<&str, &str, ()>(&fairness_set, disambiguator)
                    .await?;

                let fairness_round_robin = format!("{queue_name}_fairness_round_robin");
                redis
                    .rpush::<&str, &str, ()>(&fairness_round_robin, disambiguator)
                    .await?;

                total_retried += count as usize;
            }

            Ok(total_retried)
        } else {
            let source_queue = format!("{}_{}", queue_name, source_type.to_string().to_lowercase());

            let count = redis.llen(&source_queue).await?;

            if count == 0 {
                return Ok(0);
            }

            for _ in 0..count {
                let task_id: Option<String> = redis.lpop(&source_queue, None).await?;
                if let Some(task_id) = task_id {
                    redis
                        .zadd::<&str, f64, &str, ()>(
                            &queue_name,
                            &task_id,
                            time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64,
                        )
                        .await?;
                    println!("Retrying task: {task_id}");
                }
            }

            Ok(count as usize)
        }
    }

    async fn get_queue_status(
        &self,
        queue_name: Option<String>,
    ) -> Result<Vec<QueueStatus>, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        let mut keys: Vec<String> = if let Some(queue_name) = queue_name {
            redis.keys(format!("{queue_name}*")).await?
        } else {
            redis.keys("*").await?
        };

        keys.sort();

        let mut queues: HashMap<String, QueueStatus> = HashMap::new();
        let mut fairness_queues: HashMap<String, HashSet<String>> = HashMap::new();

        // First pass: identify fairness queues and collect disambiguators
        for key in &keys {
            // Check if it's a fairness queue by looking for pattern {base_name}_{disambiguator}_queue
            if key.contains("_queue") {
                let parts: Vec<&str> = key.split('_').collect();
                if parts.len() >= 3 && parts.last() == Some(&"queue") {
                    // This is potentially a fairness queue
                    // Extract the base name (everything before the disambiguator)
                    let pos = key.find('_').unwrap_or(0);
                    if pos > 0 {
                        let base_name = &key[0..pos];

                        // Extract the disambiguator (between base_name and "_queue")
                        let disambiguator = key[pos + 1..key.len() - 6].to_string(); // -6 to remove "_queue"

                        // Add this disambiguator to the set for this base queue
                        fairness_queues
                            .entry(base_name.to_string())
                            .or_default()
                            .insert(disambiguator);
                    }
                }
            }
        }

        // Second pass: Process all queues
        for key in keys {
            let is_fairness_subqueue = |k: &str| -> Option<(String, String)> {
                for base_name in fairness_queues.keys() {
                    if k.starts_with(base_name)
                        && (k.ends_with("_failed")
                            || k.ends_with("_processing")
                            || k.ends_with("_queue"))
                    {
                        let remaining = &k[base_name.len() + 1..]; // +1 for the underscore
                        let parts: Vec<&str> = remaining.split('_').collect();
                        if parts.len() >= 2 {
                            let disambiguator = parts[0..parts.len() - 1].join("_");
                            return Some((base_name.clone(), disambiguator));
                        }
                    }
                }
                None
            };

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

            // Check if this is a subqueue of a fairness queue
            if let Some((base_name, _)) = is_fairness_subqueue(&key) {
                // This is a fairness subqueue, add its stats to the base fairness queue
                let status = queues.entry(base_name.clone()).or_insert(QueueStatus {
                    name: base_name.clone(),
                    queue_type: QueueType::Fairness,
                    size: 0,
                    failed: 0,
                    processing: 0,
                    disambiguator_count: fairness_queues
                        .get(&base_name)
                        .map_or(Some(0), |set| Some(set.len())),
                });

                match queue_type {
                    QueueType::Main => status.size += size,
                    QueueType::Failed => status.failed += size,
                    QueueType::Processing => status.processing += size,
                    QueueType::Fairness => {}
                }
            } else {
                // Regular queue processing
                let queue_name = if queue_type == QueueType::Main {
                    key.to_string()
                } else {
                    key.trim_end_matches("_failed")
                        .trim_end_matches("_processing")
                        .to_string()
                };

                let status = queues.entry(queue_name.clone()).or_insert(QueueStatus {
                    name: queue_name,
                    queue_type: QueueType::Main,
                    size: 0,
                    failed: 0,
                    processing: 0,
                    disambiguator_count: None,
                });

                match queue_type {
                    QueueType::Main => status.size = size,
                    QueueType::Failed => status.failed = size,
                    QueueType::Processing => status.processing = size,
                    QueueType::Fairness => {}
                }
            }
        }

        // Add fairness queue type for identified fairness queues
        for (base_name, disambiguators) in fairness_queues {
            if let Some(status) = queues.get_mut(&base_name) {
                status.queue_type = QueueType::Fairness; // You'll need to add this variant to QueueType
                status.disambiguator_count = Some(disambiguators.len());
            }
        }

        Ok(queues.into_values().collect())
    }
}

impl BrokerWithManagement for RedisBroker {}
