use lazy_static::lazy_static;
use std::str::FromStr;

use crate::{
    brokers::broker::{Broker, BrokerConfig, InternalBrokerMessage},
    error::BroccoliError,
    queue::{ConsumeOptions, PublishOptions},
};

use surrealdb::{engine::any::Any, RecordId, Value};
use surrealdb::{Notification, Surreal};
use time::Duration;

use super::utils;

/// `SurrealDB` state struct
pub struct SurrealDBBroker {
    pub(crate) db: Option<Surreal<Any>>,
    pub(crate) connected: bool,
    pub(crate) config: Option<BrokerConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerMessage {
    /// Actual record id in surrealDB (`topicname,task_id`)
    pub id: RecordId,
    /// Unique identifier for the message (external version, without table name)
    pub task_id: String,
    /// The actual message content stringified
    pub payload: String,
    /// Number of processing attempts made
    pub attempts: u8,
    /// Additional metadata for the message
    #[serde(skip)]
    pub(crate) metadata:
        Option<std::collections::HashMap<String, crate::brokers::broker::MetadataTypes>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerQueuedMessageRecord {
    pub(crate) id: RecordId, // this is the queue record id [timestamp, message id]
    pub(crate) message_id: RecordId, // this is the message id: queue_name:task_id
    pub(crate) priority: i64, // message priority copy, to use for sorting in consumption
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerQueuedMessage {
    pub(crate) message_id: RecordId, // this is the message id: queue_name:task_id
    pub(crate) priority: i64,        // message priority copy, to use for sorting in consumption
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerProcessingMessage {
    pub(crate) message_id: RecordId, // this is the message id: queue_name:task_id
    pub(crate) priority: i64,        // if we reenqueue, we set the same priority
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerFailedMessage {
    pub(crate) original_msg: InternalSurrealDBBrokerMessage, // full original message
}

// consumer lock
lazy_static! {
    // TODO: to improve performance we could have a hash table of mutexes, or sharding
    static ref consume_mutex: std::sync::Arc<tokio::sync::Mutex<u8>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(0));
}

/// Implementation of the `Broker` trait for `SurrealDBBroker`.
#[async_trait::async_trait]
impl Broker for SurrealDBBroker {
    /// Connects to the broker using the provided URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker, in URL format with params, namely:
    /// `<protocol>://<host[:port]/?ns=<namespace>&db=<database>...` with the following params
    /// *Mandatory*
    /// - username
    /// - password
    /// - ns
    /// - db
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    ///
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError> {
        let db = Self::client_from_url(broker_url).await?;
        self.db = db;
        self.connected = true;
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
        _disambiguator: Option<String>,
        messages: &[InternalBrokerMessage],
        publish_options: Option<PublishOptions>,
    ) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
        let db = self.check_connected()?;

        let publish_options = publish_options.unwrap_or_default();
        if publish_options.ttl.is_some() {
            return Err(BroccoliError::NotImplemented);
        }

        let config = self.config.clone().unwrap_or_default();
        let priority = i64::from(publish_options.priority.unwrap_or(5));
        if !(1..=5).contains(&priority) {
            return Err(BroccoliError::Broker(
                "Priority must be between 1 and 5".to_string(),
            ));
        }
        // if we have a delay and scheduling is enabled
        let delay: Option<Duration> = if config.enable_scheduling.is_some() {
            publish_options.delay
        } else {
            None
        };
        let scheduled_at: Option<time::OffsetDateTime> = if config.enable_scheduling.is_some() {
            publish_options.scheduled_at
        } else {
            None
        };
        let mut published: Vec<InternalBrokerMessage> = Vec::new();
        for msg in messages {
            // 1: insert actual message //
            let inserted =
                utils::add_message(&db, queue_name, msg, "Could not publish (add msg)").await?;
            published.push(inserted);

            // 2: add to queue //
            if delay.is_none() && scheduled_at.is_none() {
                utils::add_to_queue(
                    &db,
                    queue_name,
                    &msg.task_id,
                    priority,
                    "Could not publish (enqueue)",
                )
                .await?;
            } else {
                // we either have a delay or a schedule, schedule takes priority
                if let Some(when) = scheduled_at {
                    utils::add_to_queue_scheduled(
                        &db,
                        queue_name,
                        &msg.task_id,
                        priority,
                        when,
                        "Could not publish scheduled (enqueue)",
                    )
                    .await?;
                } else if let Some(when) = delay {
                    utils::add_to_queue_delayed(
                        &db,
                        queue_name,
                        &msg.task_id,
                        priority,
                        when,
                        "Could not publish delayed (enqueue)",
                    )
                    .await?;
                }
            }
        }
        Ok(published)
    }

    /// Attempts to consume a message from the specified queue.
    /// Cost is O(NlogN) with N being the number of pending messages to be consumed
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
        let db = self.check_connected()?;

        //////// CRITICAL AREA START //////
        let _lock = consume_mutex.lock().await;

        //// 1: get message from queue ////
        let queued_message =
            utils::get_queued(&db, queue_name, "Could not try consume (get queued)").await?;
        match queued_message {
            Some(queued_message) => {
                //// 2: delete it from queue ////
                utils::remove_from_queue(
                    &db,
                    queue_name,
                    queued_message.id.clone(),
                    "Could not try consume (removing from queue)",
                )
                .await?;
                let _lock: Option<u8> = None;
                //////// CRITICAL AREA ENDS (Some branch) //////

                //// 3: if not autoack then add it to processing ////
                let auto_ack = options.is_some_and(|x| x.auto_ack.unwrap_or(false));
                if !auto_ack {
                    //// 4: add to processing queue ////
                    utils::add_to_processing(
                        &db,
                        queue_name,
                        queued_message.clone(),
                        "Could not try consume (add to processing)",
                    )
                    .await?;
                }

                //// 4: return the actual payload ////
                let msg = utils::get_message_from(
                    &db,
                    queue_name,
                    queued_message,
                    "Could not try consume (get actual message)",
                )
                .await;
                match msg {
                    Ok(msg) => Ok(Some(msg)),
                    Err(e) => Err(e),
                }
            }
            None => Ok(None), //////// CRITICAL AREA ENDS (None branch) //////
        }
    }

    /// Consumes a message from the specified queue, blocking until a message is available.
    /// Uses live querying, so if there are no messages yet, it will block efficiently without polling
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
        // first of all, we try to consume without blocking, and return if we have messages
        let resp = Self::try_consume(self, queue_name, options).await?;
        if let Some(message) = resp {
            return Ok(message);
        }

        // if there were no messages, we block using a live query and wait
        let db = self.check_connected()?;
        let queue_table = utils::queue_table(queue_name);
        let mut stream = db
            .select(queue_table)
            .range(
                vec![Value::default(), Value::default()] // note default is 'None'
                ..vec![Value::from_str("time::now()").unwrap_or_default(),Value::default()],
            ) // should notify when future becomes present
            .live()
            .await
            .map_err(|err| BroccoliError::Broker(format!("Could not consume: {err:?}")))?;
        let mut queued_message: Result<InternalSurrealDBBrokerQueuedMessageRecord, BroccoliError> =
            Err(BroccoliError::NotImplemented);
        while let Some(notification) = futures::StreamExt::next(&mut stream).await {
            // we have a notification and exit the loop if it's a create
            let payload: Option<Result<InternalSurrealDBBrokerQueuedMessageRecord, BroccoliError>> =
                match notification {
                    Ok(notification) => {
                        let notification: Notification<InternalSurrealDBBrokerQueuedMessageRecord> =
                            notification;
                        let payload = notification.data;
                        match notification.action {
                            surrealdb::Action::Create => Some(Ok(payload)),
                            _ => None,
                        }
                    }
                    Err(error) => Some(Err(BroccoliError::Broker(format!(
                        "Could not consume::'{queue_name}' {error}"
                    )))),
                };
            if let Some(message) = payload {
                queued_message = message;
                break;
            }
        }
        match queued_message {
            Ok(message) => Ok(utils::get_message_from(
                &db,
                queue_name,
                message,
                "Could not consume (retrieving message) ",
            )
            .await?),
            Err(e) => Err(e),
        }
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
        let db = self.check_connected()?;

        // 1: remove from processing queue
        let processing = utils::remove_from_processing(
            &db,
            queue_name,
            &message.task_id,
            "Could not acknowledge (remove from processing)",
        )
        .await?;

        // 2: remove actual message
        utils::remove_message(
            &db,
            queue_name,
            processing.message_id,
            &message.task_id,
            "Could not acknowledge (remove message)",
        )
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
        let db = self.check_connected()?;

        let attempts = message.attempts + 1;

        //// 1: remove from processing ////
        let rejected = utils::remove_from_processing(
            &db,
            queue_name,
            &message.task_id,
            "Could not reject (remove from processed)",
        )
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
            let msg = utils::get_message(
                &db,
                queue_name,
                rejected.message_id.clone(),
                "Could not reject (get original)",
            )
            .await?;
            //// 2: add to failed if excceded all attempts ////
            utils::add_to_failed(&db, queue_name, msg, "Could not reject (adding to failed)")
                .await?;
            //// 4: we nuke the old message ////
            utils::remove_message(
                &db,
                queue_name,
                rejected.message_id,
                &message.task_id,
                "Could not reject (removing message)",
            )
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
            //// 4: if retry is configured, we increase attempts ////
            let mut message = message;
            message.attempts = attempts;
            let task_id = message.task_id.clone();
            let priority = rejected.priority;
            utils::update_message(&db, queue_name, message, "Could not reject (attempts+1)")
                .await?;
            //// 4: and reenqueue ////
            utils::add_to_queue(
                &db,
                queue_name,
                &task_id,
                priority,
                "Could not reject (reenqueue)",
            )
            .await?;
        }

        Ok(())
    }

    /// Cancels a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message_id` - The ID of the message to be canceled.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn cancel(&self, queue_name: &str, task_id: String) -> Result<(), BroccoliError> {
        let db = self.check_connected()?;

        //// 1: remove from queue using the index ////
        let queued = utils::remove_queued_from_index(
            &db,
            queue_name,
            &task_id,
            "Could not cancel (remove from queue using index)",
        )
        .await?;
        match queued {
            Some(queued) => {
                //// 2: remove the actual message  ////
                utils::remove_message(
                    &db,
                    queue_name,
                    queued.message_id,
                    &task_id,
                    "Could not cancel (remove actual message)",
                )
                .await
            }
            None => Err(BroccoliError::Broker(format!(
                "Could not cancel (task_id not found):{queue_name}:{task_id}"
            ))),
        }
    }
}
