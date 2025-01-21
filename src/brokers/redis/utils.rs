//! Utility functions for Redis broker implementation.
//!
//! This module provides helper methods for managing Redis message operations,
//! including message parsing and manipulation of message metadata.

use std::num::NonZero;

use super::broker::{RedisBroker, RedisConnection, RedisPool};
use crate::{
    brokers::broker::{BrokerConfig, InternalBrokerMessage},
    error::BroccoliError,
    queue::ConsumeOptions,
};
use redis::{AsyncCommands, FromRedisValue};

impl RedisBroker {
    /// Creates a new `RedisBroker` instance with default configuration.
    pub fn new() -> Self {
        RedisBroker {
            redis_pool: None,
            connected: false,
            config: None,
        }
    }

    /// Creates a new `RedisBroker` instance with the specified configuration.
    ///
    /// # Arguments
    /// * `config` - The broker configuration to use
    pub fn new_with_config(config: BrokerConfig) -> Self {
        RedisBroker {
            redis_pool: None,
            connected: false,
            config: Some(config),
        }
    }

    pub(crate) fn ensure_pool(&self) -> Result<RedisPool, BroccoliError> {
        if !self.connected {
            return Err(BroccoliError::Broker(
                "Redis broker not connected".to_string(),
            ));
        }
        match &self.redis_pool {
            Some(pool) => Ok(pool.clone()),
            None => Err(BroccoliError::Broker(
                "Redis pool not initialized".to_string(),
            )),
        }
    }

    pub(crate) async fn get_task_id(
        &self,
        queue_name: &str,
        redis_connection: &mut RedisConnection<'_>,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<String>, BroccoliError> {
        let expired_messages: Vec<String> = redis_connection
            .zrangebyscore(
                format!("{}_expired_messages", queue_name),
                0,
                time::OffsetDateTime::now_utc().unix_timestamp(),
            )
            .await?;

        if !expired_messages.is_empty() {
            for expired_message in expired_messages.iter() {
                let task_id = expired_message.clone();
                redis_connection
                    .lrem::<&str, &str, String>(queue_name, 1, &task_id) // Remove from queue
                    .await?;
            }

            redis_connection
                .del::<&Vec<String>, String>(&expired_messages)
                .await?; // Remove message

            redis_connection
                .zrem::<String, &Vec<String>, String>(
                    format!("{}_scheduled_messages", queue_name),
                    &expired_messages,
                ) // Remove from scheduled
                .await?;

            redis_connection
                .zrem::<String, Vec<String>, String>(
                    format!("{}_expired_messages", queue_name),
                    expired_messages,
                ) // Remove from expired
                .await?;
        }

        let scheduled_messages: Vec<String> = redis_connection
            .zrangebyscore(
                format!("{}_scheduled_messages", queue_name),
                0,
                time::OffsetDateTime::now_utc().unix_timestamp(),
            )
            .await?;

        if !scheduled_messages.is_empty() {
            let task_id = scheduled_messages[0].clone();
            redis_connection
                .zrem::<String, &str, String>(
                    format!("{}_scheduled_messages", queue_name),
                    &task_id,
                )
                .await?;
            Ok(Some(task_id))
        } else if options.is_some_and(|x| x.auto_ack.unwrap_or(false)) {
            let popped_messages: Vec<String> =
                redis_connection.rpop(queue_name, NonZero::new(1)).await?;
            Ok(popped_messages.first().cloned())
        } else {
            Ok(redis_connection
                .lmove(
                    queue_name,
                    format!("{}_processing", queue_name),
                    redis::Direction::Right,
                    redis::Direction::Left,
                )
                .await?)
        }
    }
}

impl FromRedisValue for InternalBrokerMessage {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let map: std::collections::HashMap<String, String> = redis::from_redis_value(v)?;

        let task_id = map.get("task_id").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: task_id"))
        })?;

        let payload = map.get("payload").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: payload"))
        })?;

        let attempts = map.get("attempts").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: attempts"))
        })?;

        Ok(InternalBrokerMessage {
            task_id: task_id.to_string(),
            payload: payload.to_string(),
            attempts: attempts.parse().unwrap_or_default(),
            metadata: None,
        })
    }
}
