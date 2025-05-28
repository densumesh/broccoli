use std::collections::{HashMap, HashSet};

use redis::AsyncCommands;

use crate::{
    brokers::management::{BrokerWithManagement, QueueManagement, QueueStatus, QueueType},
    error::BroccoliError,
};

use super::RedisBroker;

#[async_trait::async_trait]
impl QueueManagement for RedisBroker {
    async fn get_queue_status(
        &self,
        queue_name: String,
        disambiguator: Option<String>,
    ) -> Result<Vec<QueueStatus>, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        if queue_name.is_empty() {
            return Err(BroccoliError::QueueStatus(
                "Queue name cannot be empty. Please specify a queue name to avoid scanning all Redis keys.".to_string()
            ));
        }
        
        let keys: Vec<String> = redis.keys(format!("{queue_name}*")).await?;

        let mut queues: HashMap<String, QueueStatus> = HashMap::new();
        let mut fairness_queues: HashMap<String, HashSet<String>> = HashMap::new();

        // First pass: identify fairness queues and collect disambiguators
        for key in &keys {
            // Check if it's a fairness queue by looking for pattern {base_name}_{disambiguator}_queue
            if key.contains("_queue") {
                let parts: Vec<&str> = key.split('_').collect();
                if parts.len() >= 3 && parts.last() == Some(&"queue") {
                    // This is potentially a fairness queue
                    // Extract the base name (everything before the first underscore)
                    let pos = key.find('_').unwrap_or(0);
                    if pos > 0 {
                        let base_name = &key[0..pos];

                        // Skip if this doesn't match the requested queue name
                        if !queue_name.is_empty() && !base_name.starts_with(&queue_name) {
                            continue;
                        }

                        // Extract the disambiguator (between base_name and "_queue")
                        let queue_disambiguator = key[pos + 1..key.len() - 6].to_string(); // -6 to remove "_queue"

                        // If a specific disambiguator is requested, only include matching ones
                        if let Some(ref requested_disambiguator) = disambiguator {
                            if queue_disambiguator != *requested_disambiguator {
                                continue;
                            }
                        }

                        // Add this disambiguator to the set for this base queue
                        fairness_queues
                            .entry(base_name.to_string())
                            .or_default()
                            .insert(queue_disambiguator);
                    }
                }
            }
        }

        // Second pass: Process all queues
        for key in keys {
            // Skip keys that don't match the queue name filter
            if !queue_name.is_empty() && !key.starts_with(&queue_name) {
                continue;
            }

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
                            let queue_disambiguator = parts[0..parts.len() - 1].join("_");
                            
                            // If a specific disambiguator is requested, only include matching ones
                            if let Some(ref requested_disambiguator) = disambiguator {
                                if queue_disambiguator != *requested_disambiguator {
                                    return None;
                                }
                            }
                            
                            return Some((base_name.clone(), queue_disambiguator));
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
            if let Some((base_name, queue_disambiguator)) = is_fairness_subqueue(&key) {
                let queue_identifier = if disambiguator.is_some() {
                    // When filtering by disambiguator, use base_name + disambiguator as identifier
                    format!("{base_name}_{queue_disambiguator}") // base_name + disambiguator
                } else {
                    // When not filtering, group all under base_name
                    base_name.clone()
                };

                let status = queues.entry(queue_identifier.clone()).or_insert(QueueStatus {
                    name: queue_identifier.clone(),
                    queue_type: QueueType::Fairness,
                    size: 0,
                    failed: 0,
                    processing: 0,
                    disambiguator_count: if disambiguator.is_some() {
                        Some(1) // When filtering by disambiguator, count is 1
                    } else {
                        fairness_queues.get(&base_name).map(std::collections::HashSet::len)
                    },
                });

                match queue_type {
                    QueueType::Main => status.size += size,
                    QueueType::Failed => status.failed += size,
                    QueueType::Processing => status.processing += size,
                    QueueType::Fairness => {}
                }
            } else {
                // Regular queue processing
                let queue_name_extracted = if queue_type == QueueType::Main {
                    key.to_string()
                } else {
                    key.trim_end_matches("_failed")
                        .trim_end_matches("_processing")
                        .to_string()
                };

                let status = queues.entry(queue_name_extracted.clone()).or_insert(QueueStatus {
                    name: queue_name_extracted,
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

        // Update fairness queue types for queues that weren't processed as subqueues
        for (base_name, disambiguators) in fairness_queues {
            if let Some(status) = queues.get_mut(&base_name) {
                status.queue_type = QueueType::Fairness;
                status.disambiguator_count = Some(disambiguators.len());
            }
        }

        Ok(queues.into_values().collect())
    }
}

impl BrokerWithManagement for RedisBroker {}