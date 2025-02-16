//! Utility functions for Redis broker implementation.
//!
//! This module provides helper methods for managing Redis message operations,
//! including message parsing and manipulation of message metadata.

use std::collections::HashMap;

use super::broker::{RedisBroker, RedisPool};
use crate::{
    brokers::broker::{BrokerConfig, InternalBrokerMessage, MetadataTypes},
    error::BroccoliError,
    queue::ConsumeOptions,
};
use redis::{aio::MultiplexedConnection, FromRedisValue};

impl RedisBroker {
    /// Creates a new `RedisBroker` instance with default configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            redis_pool: None,
            broker_url: String::new(),
            config: None,
        }
    }

    /// Creates a new `RedisBroker` instance with the specified configuration.
    ///
    /// # Arguments
    /// * `config` - The broker configuration to use
    #[must_use]
    pub const fn new_with_config(config: BrokerConfig) -> Self {
        Self {
            redis_pool: None,
            broker_url: String::new(),
            config: Some(config),
        }
    }

    pub(crate) fn ensure_pool(&self) -> Result<RedisPool, BroccoliError> {
        match &self.redis_pool {
            Some(pool) => Ok(pool
                .try_read()
                .map_err(|_| {
                    BroccoliError::Broker("Failed to acquire read lock on redis pool".to_string())
                })?
                .clone()),
            None => Err(BroccoliError::Broker(
                "Redis pool not initialized".to_string(),
            )),
        }
    }

    pub(crate) async fn get_task_id(
        &self,
        queue_name: &str,
        redis_connection: &mut MultiplexedConnection,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<String>, BroccoliError> {
        let popped_message: Option<String> = if options
            .as_ref()
            .is_some_and(|x| x.fairness.unwrap_or(false))
        {
            let script = redis::Script::new(
                r#"
                local current_time = tonumber(ARGV[1])
                local queue_to_process = redis.call('LPOP', KEYS[1])
                if not queue_to_process then
                return nil
                end

                local popped_message = redis.call('ZPOPMIN', string.format("%s_%s_queue", KEYS[2], queue_to_process), 1)
                if #popped_message == 0 then
                return nil
                end

                local message = popped_message[1]
                local score = tonumber(popped_message[2])

                if score > (5.0 * current_time) then
                redis.call('ZADD', string.format("%s_%s_queue", KEYS[2], queue_to_process), score, message)
                redis.call('RPUSH', KEYS[1], queue_to_process)
                return nil
                end

                local does_subqueue_exist = redis.call('EXISTS', string.format("%s_%s_queue", KEYS[2], queue_to_process)) == 1
                if does_subqueue_exist then
                redis.call('RPUSH', KEYS[1], queue_to_process)
                else
                redis.call('SREM', KEYS[3], queue_to_process)
                end

                if ARGV[2] == "false" then
                    local processing_queue_name = string.format("%s_%s_processing", KEYS[2], queue_to_process)
                    redis.call('LPUSH', processing_queue_name, message)
                end

                return message
            "#,
            );

            script
                .arg(time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64)
                .arg(
                    options
                        .is_some_and(|x| x.auto_ack.unwrap_or(false))
                        .to_string(),
                )
                .key(format!("{queue_name}_fairness_round_robin"))
                .key(queue_name)
                .key(format!("{queue_name}_fairness_set"))
                .invoke_async(redis_connection)
                .await?
        } else {
            let script = redis::Script::new(
                r#"
                local current_time = tonumber(ARGV[1])
                local popped_message = redis.call('ZPOPMIN', KEYS[1], 1)
                if #popped_message == 0 then
                return nil
                end

                local message = popped_message[1]
                local score = tonumber(popped_message[2])

                if score > (5.0 * current_time) then
                redis.call('ZADD', KEYS[1], score, message)
                return nil
                end
                
                if ARGV[2] == "false" then
                    redis.call('LPUSH', KEYS[2], message)
                end
                return message
            "#,
            );

            script
                .arg(time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64)
                .arg(
                    options
                        .is_some_and(|x| x.auto_ack.unwrap_or(false))
                        .to_string(),
                )
                .key(queue_name)
                .key(format!("{queue_name}_processing"))
                .invoke_async(redis_connection)
                .await?
        };

        Ok(popped_message)
    }

    /// Retrieves a Redis connection from the pool, retrying with exponential backoff if necessary.
    ///
    /// # Arguments
    /// * `redis_pool` - A reference to the Redis connection pool.
    ///
    /// # Returns
    /// A `Result` containing a `RedisConnection` on success, or a `BroccoliError` on failure.
    pub(crate) async fn get_redis_connection(
        &self,
    ) -> Result<MultiplexedConnection, BroccoliError> {
        let mut redis_conn_sleep = std::time::Duration::from_secs(1);

        let redis_pool = self.ensure_pool()?;

        #[allow(unused_assignments)]
        let mut opt_redis_connection = None;

        loop {
            let borrowed_redis_connection = match redis_pool.get().await {
                Ok(redis_connection) => Some(redis_connection),
                Err(err) => {
                    let redis_manager = bb8_redis::RedisConnectionManager::new(
                        self.broker_url.clone(),
                    )
                    .map_err(|e| {
                        BroccoliError::Broker(format!("Failed to create redis manager: {e:?}"))
                    })?;

                    let redis_pool = bb8_redis::bb8::Pool::builder()
                        .max_size(
                            self.config
                                .as_ref()
                                .map_or(10, |config| config.pool_connections.unwrap_or(10))
                                .into(),
                        )
                        .connection_timeout(std::time::Duration::from_secs(2))
                        .build(redis_manager)
                        .await
                        .map_err(|e| {
                            BroccoliError::Broker(format!("Failed to create redis pool: {e:?}"))
                        })?;

                    {
                        let mut pool_write = self
                            .redis_pool
                            .as_ref()
                            .ok_or_else(|| {
                                BroccoliError::Broker("Redis pool not initialized".to_string())
                            })?
                            .write()
                            .map_err(|_| {
                                BroccoliError::Broker(
                                    "Failed to acquire write lock on redis pool".to_string(),
                                )
                            })?;
                        *pool_write = redis_pool;
                    }
                    BroccoliError::Broker(format!("Failed to get redis connection: {err:?}"));
                    None
                }
            };

            if borrowed_redis_connection.is_some() {
                opt_redis_connection = borrowed_redis_connection;
                break;
            }

            tokio::time::sleep(redis_conn_sleep).await;
            redis_conn_sleep =
                std::cmp::min(redis_conn_sleep * 2, std::time::Duration::from_secs(300));
        }

        let redis_connection = opt_redis_connection
            .ok_or_else(|| BroccoliError::Broker("Failed to get redis connection".to_string()))?;

        Ok(redis_connection.clone())
    }
}

pub struct OptionalInternalBrokerMessage(pub Option<InternalBrokerMessage>);

impl FromRedisValue for OptionalInternalBrokerMessage {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let map: std::collections::HashMap<String, String> = redis::from_redis_value(v)?;
        if map.is_empty() {
            return Ok(Self(None));
        }

        let task_id = map.get("task_id").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: task_id"))
        })?;

        let payload = map.get("payload").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: payload"))
        })?;

        let attempts = map.get("attempts").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: attempts"))
        })?;

        let priority = map.get("priority").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: priority"))
        })?;

        let disambiguator = map.get("disambiguator");

        let mut metadata = HashMap::new();
        metadata.insert(
            "priority".to_string(),
            MetadataTypes::String(priority.to_string()),
        );

        Ok(Self(Some(InternalBrokerMessage {
            task_id: task_id.to_string(),
            payload: payload.to_string(),
            attempts: attempts.parse().unwrap_or_default(),
            disambiguator: disambiguator.cloned(),
            metadata: Some(metadata),
        })))
    }
}
