use std::sync::RwLock;

use redis::AsyncCommands;

use crate::{
    brokers::broker::{Broker, BrokerConfig, InternalBrokerMessage, MetadataTypes},
    error::BroccoliError,
    queue::{ConsumeOptions, PublishOptions},
};

use super::utils::OptionalInternalBrokerMessage;

pub(crate) type RedisPool = bb8_redis::bb8::Pool<bb8_redis::RedisConnectionManager>;

#[derive(Default)]
/// A message broker implementation for Redis.
pub struct RedisBroker {
    pub(crate) redis_pool: Option<RwLock<RedisPool>>,
    pub(crate) broker_url: String,
    pub(crate) config: Option<BrokerConfig>,
}

/// Implementation of the `Broker` trait for `RedisBroker`.
#[async_trait::async_trait]
impl Broker for RedisBroker {
    /// Connects to the Redis broker using the provided URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the Redis broker.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError> {
        let redis_manager = bb8_redis::RedisConnectionManager::new(broker_url)
            .map_err(|e| BroccoliError::Broker(format!("Failed to create redis manager: {e:?}")))?;

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
            .map_err(|e| BroccoliError::Broker(format!("Failed to create redis pool: {e:?}")))?;

        self.redis_pool = Some(RwLock::new(redis_pool));
        self.broker_url = broker_url.to_string();
        Ok(())
    }

    /// Publishes a message to the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn publish(
        &self,
        queue_name: &str,
        disambiguator: Option<String>,
        messages: &[InternalBrokerMessage],
        publish_options: Option<PublishOptions>,
    ) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
        let mut redis_connection = self.get_redis_connection().await?;

        for msg in messages {
            let attempts = msg.attempts.to_string();

            let priority = i64::from(
                publish_options
                    .clone()
                    .unwrap_or_default()
                    .priority
                    .unwrap_or(5),
            );

            if !(1..=5).contains(&priority) {
                return Err(BroccoliError::Broker(
                    "Priority must be between 1 and 5".to_string(),
                ));
            }

            let priority_str = priority.to_string();
            let mut items: Vec<(&str, &str)> = vec![
                ("task_id", &msg.task_id),
                ("payload", &msg.payload),
                ("attempts", &attempts),
                ("priority", &priority_str),
            ];

            if let Some(ref disambiguator) = disambiguator {
                items.push(("disambiguator", disambiguator));
            }

            let mut score = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;

            redis_connection
                .hset_multiple::<&str, &str, &str, String>(&msg.task_id.to_string(), &items)
                .await?;

            if let Some(ref publish_options) = publish_options {
                if let Some(delay) = publish_options.delay {
                    if self
                        .config
                        .as_ref()
                        .is_some_and(|c| c.enable_scheduling.unwrap_or(false))
                    {
                        score += (delay.as_seconds_f32() * 1_000_000_000.0) as i64;
                    }
                }

                if let Some(timestamp) = publish_options.scheduled_at {
                    if self
                        .config
                        .as_ref()
                        .is_some_and(|c| c.enable_scheduling.unwrap_or(false))
                    {
                        score = timestamp.unix_timestamp_nanos() as i64;
                    }
                }

                if let Some(ttl) = publish_options.ttl {
                    redis_connection
                        .pexpire::<&str, String>(
                            &msg.task_id.to_string(),
                            (ttl.as_seconds_f64() * 1000.0) as i64,
                        )
                        .await?;
                }
            }

            let script = redis::Script::new(
                r#"
                local base_queue_name = KEYS[1]
                local task_id = ARGV[1]
                local score = tonumber(ARGV[2])
                local priority = tonumber(ARGV[3])
                local disambiguator = ARGV[4]

                local queue_name = base_queue_name
                if disambiguator ~= '' then
                    queue_name = string.format("%s_%s_queue", base_queue_name, disambiguator)
                end

                redis.call('ZADD', queue_name, priority * score, task_id)

                if disambiguator ~= '' then
                    local fairness_set = string.format("%s_fairness_set", base_queue_name)
                    local fairness_round_robin = string.format("%s_fairness_round_robin", base_queue_name)
                    local exists_in_tracking_set = redis.call('SISMEMBER', fairness_set, disambiguator)

                    if exists_in_tracking_set == 0 then
                        redis.call('SADD', fairness_set, disambiguator)
                        redis.call('RPUSH', fairness_round_robin, disambiguator)
                    end
                end
            "#,
            );

            let disambiguator_str = disambiguator.clone().unwrap_or_default();

            script
                .key(queue_name)
                .arg(&msg.task_id)
                .arg(score)
                .arg(priority)
                .arg(&disambiguator_str)
                .invoke_async::<()>(&mut redis_connection)
                .await?;
        }

        Ok(messages.to_vec())
    }

    /// Attempts to consume a message from the specified queue. Will not block if no message is available.
    /// This will check for scheduled messages first and then attempt to consume a message from the queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing an `Some(String)` with the message if available or `None`
    /// if no message is avaiable, and a `BroccoliError` on failure.
    async fn try_consume(
        &self,
        queue_name: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<InternalBrokerMessage>, BroccoliError> {
        let mut redis_connection = self.get_redis_connection().await?;
        let mut payload: OptionalInternalBrokerMessage = OptionalInternalBrokerMessage(None);

        while payload.0.is_none() {
            let task_id: Option<String> = self
                .get_task_id(queue_name, &mut redis_connection, options.clone())
                .await?;

            if task_id.is_none() {
                break;
            }

            payload = redis_connection
                .hgetall(&task_id)
                .await
                .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;
        }

        Ok(payload.0)
    }

    /// Consumes a message from the specified queue, blocking until a message is available.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing the message as a `String`, or a `BroccoliError` on failure.
    async fn consume(
        &self,
        queue_name: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<InternalBrokerMessage, BroccoliError> {
        self.ensure_pool()?;
        let mut message: Option<InternalBrokerMessage> = None;

        while message.is_none() {
            message = self.try_consume(queue_name, options.clone()).await?;
            if message.is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }

        let message = message.ok_or_else(|| {
            BroccoliError::Consume("Failed to consume message: No message available".to_string())
        })?;

        Ok(message)
    }

    /// Acknowledges the processing of a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be acknowledged.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn acknowledge(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let mut redis_connection = self.get_redis_connection().await?;

        let processing_queue_name = message.disambiguator.as_ref().map_or_else(
            || format!("{queue_name}_processing"),
            |disambiguator| format!("{queue_name}_{disambiguator}_processing"),
        );

        redis_connection
            .lrem::<String, &str, String>(processing_queue_name, 1, &message.task_id)
            .await?;

        redis_connection
            .del::<&str, String>(&message.task_id)
            .await?;

        Ok(())
    }

    /// Rejects a message, re-queuing it or moving it to a failed queue if the retry limit is reached.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be rejected.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn reject(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let mut redis_connection = self.get_redis_connection().await?;

        let attempts = message.attempts + 1;

        let processing_queue_name = message.disambiguator.as_ref().map_or_else(
            || format!("{queue_name}_processing"),
            |disambiguator| format!("{queue_name}_{disambiguator}_processing"),
        );

        redis_connection
            .lrem::<String, &str, String>(processing_queue_name, 1, &message.task_id)
            .await?;

        if (attempts
            >= self
                .config
                .as_ref()
                .map_or(3, |config| config.retry_attempts.unwrap_or(3)))
            || !self
                .config
                .as_ref()
                .map_or(true, |config| config.retry_failed.unwrap_or(true))
        {
            let failed_queue_name = message.disambiguator.as_ref().map_or_else(
                || format!("{queue_name}_failed"),
                |disambiguator| format!("{queue_name}_{disambiguator}_failed"),
            );

            redis_connection
                .lpush::<String, &str, String>(failed_queue_name, &message.task_id)
                .await?;

            log::error!(
                "Message {} has reached max attempts and has been pushed to failed queue",
                message.task_id
            );

            return Ok(());
        }

        if self
            .config
            .as_ref()
            .map_or(true, |config| config.retry_failed.unwrap_or(true))
        {
            let priority = message
                .metadata
                .as_ref()
                .and_then(|m| m.get("priority"))
                .and_then(|v| match v {
                    MetadataTypes::String(priority) => Some(priority),
                    MetadataTypes::U64(_) => None,
                })
                .ok_or_else(|| BroccoliError::Acknowledge("Missing priority".to_string()))?
                .parse::<u8>()
                .map_err(|e| {
                    BroccoliError::Acknowledge(format!("Failed to parse priority: {e}"))
                })?;

            self.publish(
                queue_name,
                message.disambiguator.clone(),
                &[message.clone()],
                Some(PublishOptions {
                    priority: Some(priority),
                    ..Default::default()
                }),
            )
            .await?;

            redis_connection
                .hincr::<&str, &str, u8, String>(&message.task_id, "attempts", 1)
                .await?;
        }

        Ok(())
    }

    /// Cancels a message, removing it from the queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message_id` - The ID of the message to be canceled.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn cancel(&self, queue_name: &str, message_id: String) -> Result<(), BroccoliError> {
        let mut redis_connection = self.get_redis_connection().await?;

        redis_connection
            .zrem::<&str, &str, String>(queue_name, &message_id)
            .await?;

        redis_connection.del::<&str, String>(&message_id).await?;

        Ok(())
    }
}
