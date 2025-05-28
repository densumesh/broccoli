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
    ) -> Result<QueueStatus, BroccoliError> {
        let mut redis = self.get_redis_connection().await?;
        if queue_name.is_empty() {
            return Err(BroccoliError::QueueStatus(
                "Queue name cannot be empty. Please specify a queue name to avoid scanning all Redis keys.".to_string()
            ));
        }
        
        if let Some(disambiguator) = disambiguator {
            // Handle specific subqueue of fairness queue
            let subqueue_name = format!("{queue_name}_{disambiguator}_queue");
            let failed_queue_name = format!("{queue_name}_{disambiguator}_failed");
            let processing_queue_name = format!("{queue_name}_{disambiguator}_processing");
            
            // Check if the subqueue exists and get its size
            let main_size: usize = if redis.key_type::<&str, String>(&subqueue_name).await? == *"zset" {
                redis.zcard(&subqueue_name).await?
            } else {
                0
            };
            
            let failed_size: usize = redis.llen(&failed_queue_name).await.unwrap_or(0);
            let processing_size: usize = redis.llen(&processing_queue_name).await.unwrap_or(0);
            
            return Ok(QueueStatus {
                name: format!("{queue_name}_{disambiguator}"),
                queue_type: QueueType::Fairness,
                size: main_size,
                failed: failed_size,
                processing: processing_size,
                disambiguator_count: Some(1),
            });
        }
        
        // Check if this is a fairness queue by looking for subqueues with pattern {queue_name}_*_queue
        let fairness_keys: Vec<String> = redis.keys(format!("{queue_name}_*_queue")).await?;
        
        if !fairness_keys.is_empty() {
            // This is a fairness queue, sum up all subqueues
            let mut total_size = 0;
            let mut total_failed = 0;
            let mut total_processing = 0;
            let mut disambiguators = HashSet::new();
            
            for key in fairness_keys {
                // Extract disambiguator from key like "queue_name_disambiguator_queue"
                if let Some(disambiguator_part) = key.strip_prefix(&format!("{queue_name}_")).and_then(|s| s.strip_suffix("_queue")) {
                    disambiguators.insert(disambiguator_part.to_string());
                    
                    // Get sizes for this subqueue
                    let main_size: usize = if redis.key_type::<&str, String>(&key).await? == *"zset" {
                        redis.zcard(&key).await?
                    } else {
                        0
                    };
                    
                    let failed_queue = format!("{queue_name}_{disambiguator_part}_failed");
                    let processing_queue = format!("{queue_name}_{disambiguator_part}_processing");
                    
                    let failed_size: usize = redis.llen(&failed_queue).await.unwrap_or(0);
                    let processing_size: usize = redis.llen(&processing_queue).await.unwrap_or(0);
                    
                    total_size += main_size;
                    total_failed += failed_size;
                    total_processing += processing_size;
                }
            }
            
            return Ok(QueueStatus {
                name: queue_name,
                queue_type: QueueType::Fairness,
                size: total_size,
                failed: total_failed,
                processing: total_processing,
                disambiguator_count: Some(disambiguators.len()),
            });
        }
        
        // This is a regular queue - check if it exists as zset or list
        let main_size: usize = match redis.key_type::<&str, String>(&queue_name).await? {
            key_type if key_type == *"zset" => redis.zcard(&queue_name).await?,
            key_type if key_type == *"list" => redis.llen(&queue_name).await?,
            _ => 0, // Key doesn't exist or is of unknown type
        };
        
        let failed_queue_name = format!("{queue_name}_failed");
        let processing_queue_name = format!("{queue_name}_processing");
        
        let failed_size: usize = redis.llen(&failed_queue_name).await.unwrap_or(0);
        let processing_size: usize = redis.llen(&processing_queue_name).await.unwrap_or(0);
        
        Ok(QueueStatus {
            name: queue_name,
            queue_type: QueueType::Main,
            size: main_size,
            failed: failed_size,
            processing: processing_size,
            disambiguator_count: None,
        })
    }
}

impl BrokerWithManagement for RedisBroker {}