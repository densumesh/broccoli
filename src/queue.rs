use std::{future::Future, sync::Arc, time::Instant};

use futures::stream::FuturesUnordered;
use time::Duration;

use time::OffsetDateTime;

use crate::{
    brokers::{
        broker::{Broker, BrokerConfig, BrokerMessage, InternalBrokerMessage},
        connect::connect_to_broker,
    },
    error::BroccoliError,
};

/// Configuration for message retry behavior.
///
/// This struct defines how failed messages should be handled,
/// including whether they should be retried and how many attempts should be made.
pub struct RetryStrategy {
    /// Whether failed messages should be retried
    pub retry_failed: bool,
    /// Maximum number of retry attempts, if None defaults to 3
    pub attempts: Option<u8>,
}

impl Default for RetryStrategy {
    /// Creates a default retry strategy with 3 attempts and retries enabled.
    fn default() -> Self {
        Self {
            retry_failed: true,
            attempts: Some(3),
        }
    }
}

impl RetryStrategy {
    /// Creates a new retry strategy with default settings (3 attempts, retry enabled).
    ///
    /// # Returns
    /// A new `RetryStrategy` instance with default configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            retry_failed: true,
            attempts: Some(3),
        }
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// # Arguments
    /// * `attempts` - The maximum number of times a failed message should be retried.
    ///
    /// # Returns
    /// The updated `RetryStrategy` instance.
    #[must_use]
    pub const fn with_attempts(mut self, attempts: u8) -> Self {
        self.attempts = Some(attempts);
        self
    }

    /// Enables or disables message retrying.
    ///
    /// # Arguments
    /// * `retry_failed` - If true, failed messages will be retried according to the attempts configuration.
    ///                    If false, failed messages will be immediately moved to the failed queue.
    ///
    /// # Returns
    /// The updated `RetryStrategy` instance.
    #[must_use]
    pub const fn retry_failed(mut self, retry_failed: bool) -> Self {
        self.retry_failed = retry_failed;
        self
    }
}

/// Builder for configuring and creating a `BroccoliQueue` instance.
///
/// This struct provides a fluent interface for setting up queue parameters
/// such as retry strategy and connection pool size.
pub struct BroccoliQueueBuilder {
    /// The URL of the message broker
    broker_url: String,
    /// Maximum number of retry attempts for failed messages
    retry_attempts: Option<u8>,
    /// Whether failed messages should be retried
    retry_failed: Option<bool>,
    /// Number of connections to maintain in the connection pool
    pool_connections: Option<u8>,
    /// Whether to enable message scheduling
    ///
    /// NOTE: If you enable this w/ rabbitmq, you will need to install the delayed-exchange plugin
    /// <https://www.rabbitmq.com/blog/2015/04/16/scheduling-messages-with-rabbitmq>
    enable_scheduling: Option<bool>,
}

impl BroccoliQueueBuilder {
    /// Creates a new `BroccoliQueueBuilder` with the specified broker URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A new `BroccoliQueueBuilder` instance.
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            retry_attempts: None,
            retry_failed: None,
            pool_connections: None,
            enable_scheduling: None,
        }
    }

    /// Sets the retry strategy for failed messages.
    ///
    /// # Arguments
    /// * `strategy` - The retry strategy to use.
    ///
    /// # Returns
    /// The updated `BroccoliQueueBuilder` instance.
    #[must_use]
    pub const fn failed_message_retry_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.retry_attempts = strategy.attempts;
        self.retry_failed = Some(strategy.retry_failed);
        self
    }

    /// Sets the number of connections in the connection pool.
    ///
    /// # Arguments
    /// * `connections` - The number of connections.
    ///
    /// # Returns
    /// The updated `BroccoliQueueBuilder` instance.
    #[must_use]
    pub const fn pool_connections(mut self, connections: u8) -> Self {
        self.pool_connections = Some(connections);
        self
    }

    /// Enables or disables message scheduling.
    ///
    /// NOTE: If you enable this w/ rabbitmq, you will need to install the delayed-exchange plugin
    /// <https://www.rabbitmq.com/blog/2015/04/16/scheduling-messages-with-rabbitmq>
    ///
    /// # Arguments
    /// * `enable_scheduling` - If true, message scheduling will be enabled.
    ///
    /// # Returns
    /// The updated `BroccoliQueueBuilder` instance.
    #[must_use]
    pub const fn enable_scheduling(mut self, enable_scheduling: bool) -> Self {
        self.enable_scheduling = Some(enable_scheduling);
        self
    }

    /// Builds the `BroccoliQueue` with the specified configuration.
    ///
    /// # Returns
    /// A `Result` containing the `BroccoliQueue` on success, or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// If the broker connection fails, a `BroccoliError` will be returned.
    pub async fn build(self) -> Result<BroccoliQueue, BroccoliError> {
        let config = BrokerConfig {
            retry_attempts: self.retry_attempts,
            retry_failed: self.retry_failed,
            pool_connections: self.pool_connections,
            enable_scheduling: self.enable_scheduling,
        };

        let broker = connect_to_broker(&self.broker_url, Some(config))
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to connect to broker: {e}")))?;

        Ok(BroccoliQueue {
            broker: Arc::new(broker),
        })
    }
}

/// Options for consuming messages from the broker.
#[derive(Debug, Clone)]
pub struct ConsumeOptions {
    /// Whether to auto-acknowledge messages. Default is false. If you try to acknowledge or reject a message that has been auto-acknowledged, it will result in an error.
    pub auto_ack: Option<bool>,
    /// Whether to consume from a fairness queue or not. This is only supported by the Redis Broker.
    pub fairness: Option<bool>,
    /// how long to wait in tight consumer loops, defaults to zero for `process_messages` and `process_messages_with_handlers`,
    /// and 500ms for `consume`, which allows those functions to be stopped in a `tokkio::spawn` thread
    pub consume_wait: Option<std::time::Duration>,
    // unfortunately, since the options builder can be used in a constant setting, we cannot
    // add a CancellationToken as an option which would be great way to stop gracefully
}

impl Default for ConsumeOptions {
    fn default() -> Self {
        Self {
            auto_ack: Some(false),
            fairness: None,
            consume_wait: None,
        }
    }
}

impl ConsumeOptions {
    /// Creates a new `ConsumeOptionsBuilder`.
    #[must_use]
    pub const fn builder() -> ConsumeOptionsBuilder {
        ConsumeOptionsBuilder::new()
    }
}

/// Builder for constructing `ConsumeOptions` with a fluent interface.
#[derive(Default, Debug)]
pub struct ConsumeOptionsBuilder {
    auto_ack: Option<bool>,
    fairness: Option<bool>,
    consume_wait: Option<std::time::Duration>,
}

impl ConsumeOptionsBuilder {
    /// Creates a new `ConsumeOptionsBuilder` with default values.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            auto_ack: None,
            fairness: None,
            consume_wait: None,
        }
    }

    /// Sets whether messages should be auto-acknowledged.
    #[must_use]
    pub const fn auto_ack(mut self, auto_ack: bool) -> Self {
        self.auto_ack = Some(auto_ack);
        self
    }

    /// Sets whether to consume from a fairness queue.
    #[must_use]
    pub const fn fairness(mut self, fairness: bool) -> Self {
        self.fairness = Some(fairness);
        self
    }

    /// Sets whether to consume from a fairness queue.
    #[must_use]
    pub const fn consume_wait(mut self, consume_wait: std::time::Duration) -> Self {
        self.consume_wait = Some(consume_wait);
        self
    }

    /// Builds the `ConsumeOptions` with the configured values.
    #[must_use]
    pub const fn build(self) -> ConsumeOptions {
        ConsumeOptions {
            auto_ack: self.auto_ack,
            fairness: self.fairness,
            consume_wait: self.consume_wait,
        }
    }
}

/// Options for publishing messages to the broker.
#[derive(Debug, Default, Clone)]
pub struct PublishOptions {
    /// Time-to-live for the message
    pub ttl: Option<Duration>,
    /// Message priority level. This is a number between 1 and 5, where 5 is the lowest priority and 1 is the highest.
    pub priority: Option<u8>,
    /// Delay before the message is published
    pub delay: Option<Duration>,
    /// Scheduled time for the message to be published
    pub scheduled_at: Option<OffsetDateTime>,
}

impl PublishOptions {
    /// Creates a new `PublishOptionsBuilder`.
    #[must_use]
    pub const fn builder() -> PublishOptionsBuilder {
        PublishOptionsBuilder::new()
    }
}

/// Builder for constructing `PublishOptions` with a fluent interface.
#[derive(Default, Debug)]
pub struct PublishOptionsBuilder {
    ttl: Option<Duration>,
    priority: Option<u8>,
    delay: Option<Duration>,
    scheduled_at: Option<OffsetDateTime>,
}

impl PublishOptionsBuilder {
    /// Creates a new `PublishOptionsBuilder` with default values.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ttl: None,
            priority: None,
            delay: None,
            scheduled_at: None,
        }
    }

    /// Sets the time-to-live duration for the message.
    #[must_use]
    pub const fn ttl(mut self, duration: Duration) -> Self {
        self.ttl = Some(duration);
        self
    }

    /// Sets a delay before the message is published.
    #[must_use]
    pub const fn delay(mut self, duration: Duration) -> Self {
        self.delay = Some(duration);
        self
    }

    /// Sets a specific time for the message to be published.
    #[must_use]
    pub const fn schedule_at(mut self, time: OffsetDateTime) -> Self {
        self.scheduled_at = Some(time);
        self
    }

    /// Sets the priority level for the message. This is a number between 1 and 5, where 5 is the lowest priority and 1 is the highest.
    ///
    /// # Panics
    /// Panics if the priority is not between 1 and 5.
    #[must_use]
    pub fn priority(mut self, priority: u8) -> Self {
        assert!(
            (1..=5).contains(&priority),
            "Priority must be between 1 and 5"
        );

        self.priority = Some(priority);
        self
    }

    /// Builds the `PublishOptions` with the configured values.
    #[must_use]
    pub const fn build(self) -> PublishOptions {
        PublishOptions {
            ttl: self.ttl,
            priority: self.priority,
            delay: self.delay,
            scheduled_at: self.scheduled_at,
        }
    }
}

/// Main queue interface for interacting with the message broker.
///
/// `BroccoliQueue` provides methods for publishing and consuming messages,
/// as well as processing messages with custom handlers.
pub struct BroccoliQueue {
    /// The underlying message broker implementation
    broker: Arc<Box<dyn Broker>>,
}

impl Clone for BroccoliQueue {
    fn clone(&self) -> Self {
        Self {
            broker: Arc::clone(&self.broker),
        }
    }
}

impl BroccoliQueue {
    /// Creates a new `BroccoliQueueBuilder` with the specified broker URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A new `BroccoliQueueBuilder` instance.
    pub fn builder(broker_url: impl Into<String>) -> BroccoliQueueBuilder {
        BroccoliQueueBuilder::new(broker_url)
    }

    /// Publishes a message to the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `message` - The message to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the message fails to publish, a `BroccoliError` will be returned.
    pub async fn publish<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        disambiguator: Option<String>,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<BrokerMessage<T>, BroccoliError> {
        let message = BrokerMessage::new(message.clone(), disambiguator.clone());

        let message = self
            .broker
            .publish(topic, disambiguator, &[message.into()], options)
            .await
            .map_err(|e| BroccoliError::Publish(format!("Failed to publish message: {e:?}")))?
            .pop()
            .ok_or_else(|| BroccoliError::Publish("Failed to publish message".to_string()))?;

        message.into_message()
    }

    /// Publishes a batch of messages to the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `messages` - An iterator over the messages to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the messages fail to publish, a `BroccoliError` will be returned.
    pub async fn publish_batch<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        disambiguator: Option<String>,
        messages: impl IntoIterator<Item = T>,
        options: Option<PublishOptions>,
    ) -> Result<Vec<BrokerMessage<T>>, BroccoliError> {
        let messages: Vec<BrokerMessage<T>> = messages
            .into_iter()
            .map(|msg| BrokerMessage::new(msg, disambiguator.clone()))
            .collect();

        let internal_messages = messages
            .iter()
            .map(Into::into)
            .collect::<Vec<InternalBrokerMessage>>();

        let messages = self
            .broker
            .publish(topic, disambiguator, &internal_messages, options)
            .await
            .map_err(|e| BroccoliError::Publish(format!("Failed to publish messages: {e:?}")))?;

        messages
            .into_iter()
            .map(|msg| msg.into_message())
            .collect::<Result<Vec<BrokerMessage<T>>, BroccoliError>>()
    }

    /// Consumes a message from the specified topic. This method will block until a message is available.
    /// This will not acknowledge the message, use `acknowledge` to remove the message from the processing queue,
    /// or `reject` to move the message to the failed queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    /// A `Result` containing the consumed message, or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// If the message fails to consume, a `BroccoliError` will be returned.
    pub async fn consume<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<BrokerMessage<T>, BroccoliError> {
        let message = self
            .broker
            .consume(topic, options)
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;

        message.into_message()
    }

    /// Consumes a batch of messages from the specified topic. This method will block until the specified number of messages are consumed.
    /// This will not acknowledge the message, use `acknowledge` to remove the message from the processing queue,
    /// or `reject` to move the message to the failed queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `batch_size` - The number of messages to consume.
    /// * `timeout` - The timeout duration for consuming messages.
    ///
    /// # Returns
    /// A `Result` containing a vector of consumed messages, or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// If the messages fail to consume, a `BroccoliError` will be returned.
    pub async fn consume_batch<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &'static str,
        batch_size: usize,
        timeout: Duration,
        options: Option<ConsumeOptions>,
    ) -> Result<Vec<BrokerMessage<T>>, BroccoliError> {
        let mut messages = Vec::with_capacity(batch_size);
        let deadline = Instant::now() + timeout;

        while messages.len() < batch_size && Instant::now() < deadline {
            if let Ok(Some(msg)) = self.try_consume::<T>(topic, options.clone()).await {
                messages.push(msg);
            }
        }

        Ok(messages)
    }

    /// Attempts to consume a message from the specified topic. This method will not block, returning immediately if no message is available.
    /// This will not acknowledge the message, use `acknowledge` to remove the message from the processing queue,
    /// or `reject` to move the message to the failed queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    /// A `Result` containing an `Option` with the consumed message if available, or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// If the message fails to consume, a `BroccoliError` will be returned.
    pub async fn try_consume<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &'static str,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<BrokerMessage<T>>, BroccoliError> {
        let serialized_message = self
            .broker
            .try_consume(topic, options)
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {e:?}")))?;

        if let Some(message) = serialized_message {
            Ok(Some(message.into_message()?))
        } else {
            Ok(None)
        }
    }

    /// Acknowledges the processing of a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `message` - The message to be acknowledged.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the message fails to acknowledge, a `BroccoliError` will be returned.
    pub async fn acknowledge<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &'static str,
        message: BrokerMessage<T>,
    ) -> Result<(), BroccoliError> {
        self.broker
            .acknowledge(topic, message.into())
            .await
            .map_err(|e| {
                BroccoliError::Acknowledge(format!("Failed to acknowledge message: {e:?}"))
            })?;

        Ok(())
    }

    /// Rejects the processing of a message, moving it to the failed queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `message` - The message to be rejected.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the message fails to reject, a `BroccoliError` will be returned.
    pub async fn reject<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &'static str,
        message: BrokerMessage<T>,
    ) -> Result<(), BroccoliError> {
        self.broker
            .reject(topic, message.into())
            .await
            .map_err(|e| BroccoliError::Reject(format!("Failed to reject message: {e:?}")))?;

        Ok(())
    }

    /// Cancels the processing of a message, deleting it from the queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `message_id` - The ID of the message to cancel.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the message fails to cancel, a `BroccoliError` will be returned.
    pub async fn cancel(&self, topic: &str, message_id: String) -> Result<(), BroccoliError> {
        self.broker
            .cancel(topic, message_id)
            .await
            .map_err(|e| BroccoliError::Cancel(format!("Failed to cancel message: {e:?}")))?;

        Ok(())
    }

    /// Processes messages from the specified topic with the provided handler function.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use broccoli_queue::queue::BroccoliQueue;
    /// use broccoli_queue::brokers::broker::BrokerMessage;
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// struct JobPayload {
    ///    id: String,
    ///    task_name: String,
    ///    created_at: chrono::DateTime<chrono::Utc>,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///    let queue = BroccoliQueue::builder("redis://localhost:6379")
    ///       .failed_message_retry_strategy(Default::default())
    ///       .pool_connections(5)
    ///       .build()
    ///       .await
    ///       .unwrap();
    ///
    ///   queue.process_messages("jobs", None, None, |message: BrokerMessage<JobPayload>| async move {
    ///         println!("Received message: {:?}", message);
    ///         Ok(())
    ///     }).await.unwrap();
    ///
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `concurrency` - The number of concurrent message handlers.
    /// * `handler` - The handler function to process messages. This function should return a `BroccoliError` on failure.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the message fails to process, a `BroccoliError` will be returned.
    pub async fn process_messages<T, F, Fut>(
        &self,
        topic: &'static str,
        concurrency: Option<usize>,
        consume_options: Option<ConsumeOptions>,
        handler: F,
    ) -> Result<(), BroccoliError>
    where
        T: serde::de::DeserializeOwned + Send + Clone + serde::Serialize + 'static,
        F: Fn(BrokerMessage<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
    {
        let future_handles = FuturesUnordered::new();
        let consume_options = consume_options.clone();
        // tokio can't abort CPU bound loops, by calling the sleep await, we allow tokio to abort
        // the running thread, even if the sleep is set to zero
        let sleep = consume_options
            .clone()
            .unwrap_or_default()
            .consume_wait
            .unwrap_or(std::time::Duration::ZERO);
        loop {
            tokio::time::sleep(sleep).await;
            if let Some(concurrency) = concurrency {
                while future_handles.len() < concurrency {
                    let broker = Arc::clone(&self.broker);
                    let topic = topic.to_string();
                    let handler = handler.clone();
                    let consume_options = consume_options.clone();

                    let handle = tokio::spawn(async move {
                        loop {
                            let message = broker
                                .consume(&topic, consume_options.clone())
                                .await
                                .map_err(|e| {
                                    log::error!("Failed to consume message: {:?}", e);
                                });

                            if let Ok(message) = message {
                                let Ok(broker_message) = message.into_message() else {
                                    log::error!("Failed to deserialize message");
                                    continue;
                                };
                                match handler(broker_message).await {
                                    Ok(()) => {
                                        // Message processed successfully
                                        let _ = broker.acknowledge(&topic, message).await.map_err(
                                            |e| {
                                                log::error!(
                                                    "Failed to acknowledge message: {:?}",
                                                    e
                                                );
                                            },
                                        );
                                    }
                                    Err(_) => {
                                        // Message processing failed
                                        let _ = broker.reject(&topic, message).await.map_err(|e| {
                                            log::error!("Failed to reject message: {:?}", e);
                                        });
                                    }
                                }
                            } else {
                                log::error!("Failed to consume message");
                                continue;
                            }
                        }
                    });

                    future_handles.push(handle);
                }
            } else {
                let message = self
                    .broker
                    .consume(topic, consume_options.clone())
                    .await
                    .map_err(|e| {
                        log::error!("Failed to consume message: {:?}", e);
                        BroccoliError::Consume(format!("Failed to consume message: {e:?}"))
                    })?;

                match handler(message.into_message()?).await {
                    Ok(()) => {
                        // Message processed successfully
                        let _ = self.broker.acknowledge(topic, message).await.map_err(|e| {
                            log::error!("Failed to acknowledge message: {:?}", e);
                        });
                    }
                    Err(_) => {
                        // Message processing failed
                        let _ = self.broker.reject(topic, message).await.map_err(|e| {
                            log::error!("Failed to reject message: {:?}", e);
                        });
                    }
                }
            }
        }
    }

    /// Processes messages from the specified topic with the provided handler functions for message processing, success, and error handling.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use broccoli_queue::queue::BroccoliQueue;
    /// use broccoli_queue::brokers::broker::BrokerMessage;
    /// use broccoli_queue::error::BroccoliError;
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// struct JobPayload {
    ///     id: String,
    ///     task_name: String,
    ///     created_at: chrono::DateTime<chrono::Utc>,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let queue = BroccoliQueue::builder("redis://localhost:6379")
    ///         .failed_message_retry_strategy(Default::default())
    ///         .pool_connections(5)
    ///         .build()
    ///         .await
    ///         .unwrap();
    ///
    ///     // Define handlers
    ///     async fn process_message(message: BrokerMessage<JobPayload>) -> Result<(), BroccoliError> {
    ///         println!("Processing message: {:?}", message);
    ///         Ok(())
    ///     }
    ///
    ///     async fn on_success(message: BrokerMessage<JobPayload>) -> Result<(), BroccoliError> {
    ///         println!("Successfully processed message: {}", message.task_id);
    ///         Ok(())
    ///     }
    ///
    ///     async fn on_error(message: BrokerMessage<JobPayload>, error: BroccoliError) -> Result<(), BroccoliError> {
    ///         println!("Failed to process message {}: {:?}", message.task_id, error);
    ///         Ok(())
    ///     }
    ///
    ///     // Process messages with 3 concurrent workers
    ///     queue.process_messages_with_handlers(
    ///         "jobs",
    ///         Some(3),
    ///         None,    
    ///         process_message,
    ///         on_success,
    ///         on_error
    ///     ).await.unwrap();
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `concurrency` - The number of concurrent message handlers.
    /// * `message_handler` - The handler function to process messages. This function should return a `BroccoliError` on failure.
    /// * `on_success` - The handler function to call on successful message processing. This function should return a `BroccoliError` on failure.
    /// * `on_error` - The handler function to call on message processing failure. This function should return a `BroccoliError` on failure.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    /// If the message fails to process, a `BroccoliError` will be returned.
    pub async fn process_messages_with_handlers<T, F, MessageFut, SuccessFut, ErrorFut, S, E>(
        &self,
        topic: &'static str,
        concurrency: Option<usize>,
        consume_options: Option<ConsumeOptions>,
        message_handler: F,
        on_success: S,
        on_error: E,
    ) -> Result<(), BroccoliError>
    where
        T: serde::de::DeserializeOwned + Send + Clone + serde::Serialize + 'static,
        F: Fn(BrokerMessage<T>) -> MessageFut + Send + Sync + Clone + 'static,
        MessageFut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
        S: Fn(BrokerMessage<T>) -> SuccessFut + Send + Sync + Clone + 'static,
        SuccessFut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
        E: Fn(BrokerMessage<T>, BroccoliError) -> ErrorFut + Send + Sync + Clone + 'static,
        ErrorFut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
    {
        let handles = FuturesUnordered::new();
        let consume_options = consume_options.clone();
        // tokio can't abort CPU bound loops, by calling the sleep await, we allow tokio to abort
        // the running thread, even if the sleep is set to zero
        let sleep = consume_options
            .clone()
            .unwrap_or_default()
            .consume_wait
            .unwrap_or(std::time::Duration::ZERO);
        loop {
            tokio::time::sleep(sleep).await;
            if let Some(concurrency) = concurrency {
                while handles.len() < concurrency {
                    let broker = Arc::clone(&self.broker);
                    let topic = topic.to_string();
                    let message_handler = message_handler.clone();
                    let on_success = on_success.clone();
                    let on_error = on_error.clone();
                    let consume_options = consume_options.clone();

                    let handle = tokio::spawn(async move {
                        loop {
                            let message = broker
                                .consume(&topic, consume_options.clone())
                                .await
                                .map_err(|e| {
                                    log::error!("Failed to consume message: {:?}", e);
                                });

                            if let Ok(message) = message {
                                let Ok(broker_message) = message.into_message() else {
                                    log::error!("Failed to deserialize message");
                                    continue;
                                };
                                match message_handler(broker_message.clone()).await {
                                    Ok(()) => {
                                        let _ = on_success(broker_message).await.map_err(|e| {
                                            log::error!(
                                                "Success Handler to process message: {:?}",
                                                e
                                            );
                                        });
                                        let _ = broker.acknowledge(&topic, message).await.map_err(
                                            |e| {
                                                log::error!(
                                                    "Failed to acknowledge message: {:?}",
                                                    e
                                                );
                                            },
                                        );
                                    }
                                    Err(e) => {
                                        let _ = on_error(broker_message, e).await.map_err(|e| {
                                            log::error!(
                                                "Error Handler to process message: {:?}",
                                                e
                                            );
                                        });
                                        let _ = broker.reject(&topic, message).await.map_err(|e| {
                                            log::error!("Failed to reject message: {:?}", e);
                                        });
                                    }
                                }
                            } else {
                                log::error!("Failed to consume message");
                                continue;
                            }
                        }
                    });

                    handles.push(handle);
                }
            } else {
                let message = self
                    .broker
                    .consume(topic, consume_options.clone())
                    .await
                    .map_err(|e| {
                        log::error!("Failed to consume message: {:?}", e);
                        BroccoliError::Consume(format!("Failed to consume message: {e:?}"))
                    })?;

                match message_handler(message.into_message()?).await {
                    Ok(()) => {
                        let _ = on_success(message.into_message()?).await.map_err(|e| {
                            log::error!("Success Handler to process message: {:?}", e);
                        });
                        let _ = self
                            .broker
                            .acknowledge(topic, message)
                            .await
                            .map_err(|e| log::error!("Failed to acknowledge message: {:?}", e));
                    }
                    Err(e) => {
                        let _ = on_error(message.into_message()?, e)
                            .await
                            .map_err(|e| log::error!("Error Handler to process message: {:?}", e));
                        let _ = self
                            .broker
                            .reject(topic, message)
                            .await
                            .map_err(|e| log::error!("Failed to reject message: {:?}", e));
                    }
                }
            }
        }
    }
}
