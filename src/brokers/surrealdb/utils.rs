use std::str::FromStr;
use surrealdb::engine::any::connect;
use surrealdb::engine::any::Any;
use surrealdb::RecordId;
use surrealdb::Response;
use surrealdb::Surreal;
use time::format_description::well_known::Rfc3339;
use time::Duration;
use time::OffsetDateTime;
use url::Url;

use crate::brokers::broker::BrokerConfig;
use crate::brokers::broker::InternalBrokerMessage;
use crate::error::BroccoliError;

use super::broker::InternalSurrealDBBrokerFailedMessage;
use super::broker::InternalSurrealDBBrokerMessage;
use super::broker::InternalSurrealDBBrokerProcessingMessage;
use super::broker::InternalSurrealDBBrokerQueuedMessage;
use super::broker::InternalSurrealDBBrokerQueuedMessageRecord;
use super::SurrealDBBroker;

#[derive(Default)]
struct SurrealDBConnectionConfig {
    username: String,
    password: String,
    ns: String,
    database: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct InternalSurrealDBBrokerQueueIndex {
    pub queue_id: RecordId, // points to queue:[timestamp,messageid]
}

impl Default for SurrealDBBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl SurrealDBBroker {
    /// Creates a new `SurrealDBBroker` instance with default configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            db: None,
            connected: false,
            config: None,
        }
    }

    /// new with passed configuration
    #[must_use]
    pub(crate) const fn new_with_config(config: BrokerConfig) -> Self {
        Self {
            db: None,
            connected: false,
            config: Some(config),
        }
    }

    /// check and return current active connection
    pub(crate) fn check_connected(&self) -> Result<Surreal<Any>, BroccoliError> {
        self.db.as_ref().map_or_else(
            || Err(BroccoliError::Broker("Not connected".to_string())),
            |db| Ok(db.to_owned()),
        )
    }

    /// we create a surreadlb connection from the url configuration
    /// URL parameters after ? are username, password, ns, database
    /// if unspecified, will default back to root,root,test,test (only for testing!)
    /// note that only 'ws' is supported RTN to allow for live querying
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A `Result` containing a boxed broker implementation, or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// * `BroccoliError::Broker` - If the connection URL is invalid.
    pub async fn client_from_url(
        broker_url: &str,
    ) -> Result<std::option::Option<Surreal<Any>>, BroccoliError> {
        let url = Url::parse(broker_url)
            .map_err(|e| BroccoliError::Broker(format!("Failed to parse connection URL: {e:?}")))?;
        let config = SurrealDBConnectionConfig {
            username: Self::get_param_value(&url, "username")
                .unwrap_or_else(|_| "root".to_string()),
            password: Self::get_param_value(&url, "password")
                .unwrap_or_else(|_| "root".to_string()),
            ns: Self::get_param_value(&url, "ns").unwrap_or_else(|_| "test".to_string()),
            database: Self::get_param_value(&url, "database")
                .unwrap_or_else(|_| "test".to_string()),
        };
        if !url.has_host() || !url.has_host() {
            return Err(BroccoliError::Broker(
                "Failed to coonect to SurrealDB: Missing scheme:://host".to_string(),
            ));
        }
        let scheme = url.scheme();
        if scheme != "ws" {
            return Err(BroccoliError::Broker(
                "Failed to connect to SurrealDB: only ws:// is supported".to_string(),
            ));
        }
        let port = url.port();
        if port.is_none() {
            return Err(BroccoliError::Broker(
                "Failed to connect to SurrealDB: missing port number".to_string(),
            ));
        }

        let connection_url = format!(
            "ws://{}:{}/rpc",
            url.host_str().unwrap_or("localhost"),
            port.unwrap_or(8000)
        );
        let db = connect(connection_url)
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to connect to SurrealDB: {e:?}")))?;

        db.signin(surrealdb::opt::auth::Root {
            username: &config.username,
            password: &config.password,
        })
        .await
        .map_err(|e| {
            BroccoliError::Broker(format!("Incorrect credentials for SurrealDB: {e:?}"))
        })?;

        // Select a specific namespace / database
        db.use_ns(config.ns)
            .use_db(config.database)
            .await
            .map_err(|e| BroccoliError::Broker(format!("NS/DB not found for SurrealDB: {e:?}")))?;

        Ok(Some(db))
    }

    /// helper: given a url get a named parameter
    pub(crate) fn get_param_value(url: &Url, name: &str) -> Result<String, BroccoliError> {
        url.query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
            .map_or_else(
                || {
                    Err(BroccoliError::Broker(format!(
                        "Missing connection param: {name}"
                    )))
                },
                Ok,
            )
    }
}

// convenience into and from conversion between the broccoli and the surrealdb layer

impl From<InternalSurrealDBBrokerMessage> for InternalBrokerMessage {
    fn from(val: InternalSurrealDBBrokerMessage) -> Self {
        Self {
            task_id: val.task_id,
            payload: val.payload,
            attempts: val.attempts,
            disambiguator: None,
            metadata: val.metadata,
        }
    }
}

impl InternalSurrealDBBrokerMessage {
    fn from(queue_name: &str, msg: InternalBrokerMessage) -> Self {
        let id: RecordId = (queue_name, msg.task_id.clone()).into();
        Self {
            id,
            task_id: msg.task_id,
            payload: msg.payload,
            attempts: msg.attempts,
            metadata: msg.metadata,
        }
    }
}

/// this table holds a timesorted timeseries, <`queue_name>`:[<timestamp>,<taskid>]
/// and acts as the queue
pub fn queue_table(queue_name: &str) -> String {
    format!("{queue_name}___queue")
}

/// this table holds messages in process
fn processing_table(queue_name: &str) -> String {
    format!("{queue_name}___processing")
}

/// this is the failed messages table
fn failed_table(queue_name: &str) -> String {
    format!("{queue_name}___failed")
}

/// this is an index to go from <queue_name>___index:[<taskid>,<queuename>] to the queue table
/// in O(k) time
fn index_table(queue_name: &str) -> String {
    format!("{queue_name}___index")
}

/// time+id range record id, namely `queue_table`:[when,<uuid>`task_id`]
fn queue_record_id(queue_name: &str, when: &str, task_id: &str) -> Result<RecordId, BroccoliError> {
    // TODO: look at building the record programmatically, move when to typed
    // compromise here is that we do explicit casting of the uuid, if it's not correct it will fail
    let queue_table = self::queue_table(queue_name);
    let queue_record_str = format!("{queue_table}:[{when},<uuid>'{task_id}']");
    let queue_record_id = RecordId::from_str(&queue_record_str);
    match queue_record_id {
        Ok(record_id) => Ok(record_id),
        Err(e) => Err(BroccoliError::Broker(format!(
            "Incorrect task id for queue ({e})"
        ))),
    }
}

/// `index_table`:[<uuid>`task_id`, `queue_name`]
fn index_record_id(task_id: &str, queue_name: &str) -> Result<RecordId, BroccoliError> {
    let index_table = self::index_table(queue_name);
    let index_record_str = format!("{index_table}:[<uuid>'{task_id}','{queue_name}']");
    let index_record_id = RecordId::from_str(&index_record_str);
    match index_record_id {
        Ok(record_id) => Ok(record_id),
        Err(e) => Err(BroccoliError::Broker(format!(
            "Incorrect task id for index ({e})"
        ))),
    }
}

/// add to the timeseries
pub async fn add_to_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let now = OffsetDateTime::now_utc();
    let () = self::add_to_queue_scheduled(db, queue_name, task_id, priority, now, err_msg).await?;
    Ok(())
}

/// add to the timeseries with a delay duration
pub async fn add_to_queue_delayed(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    delay: Duration,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let when = OffsetDateTime::now_utc() + delay;
    let () = self::add_to_queue_scheduled(db, queue_name, task_id, priority, when, err_msg).await?;
    Ok(())
}

/// add to the timeseries at a scheduled time, can be in the past and it will be triggered immediately
pub async fn add_to_queue_scheduled(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    when: OffsetDateTime,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let time = to_rfc3339(when);
    match time {
        Ok(time) => {
            let when = format!("<datetime>'{time}'");
            let () =
                self::add_record_to_queue(db, queue_name, task_id, priority, when, err_msg).await?;
            Ok(())
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}' could not convert delay: {e})"
        ))),
    }
}

// implementation
async fn add_record_to_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    when: String,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let queue_record_id = queue_record_id(queue_name, &when, task_id)?;
    let message_record_id: RecordId = (queue_name, task_id).into();
    let qm: Option<InternalSurrealDBBrokerQueuedMessage> = db
        .create(queue_record_id.clone())
        .content(InternalSurrealDBBrokerQueuedMessage {
            message_id: message_record_id,
            priority,
        })
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    match qm {
        Some(_) => {
            // now we insert into the index and we are done, note we insert the queue id
            let () =
                self::add_to_queue_index(db, queue_name, task_id, queue_record_id, err_msg).await?;
            Ok(())
        }
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': adding to queue (silently did not add)",
        ))),
    }
}

// we add an entry into a queue index, index:[messageid,queue_name] {queue_id} where
// queue_id is basically: queue:[timestamp,messageid].
// We can use this index we can do cancellations in O(k) time and workaround parser issues
async fn add_to_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    queue_id: RecordId, // queue:[timestamp, task_id]
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    // we create the index record and add to the index
    // we upsert because if re reenqueue we will be re-setting and not creating from scratch
    let index_record_id = index_record_id(task_id, queue_name)?;
    let qm: Option<InternalSurrealDBBrokerQueueIndex> = db
        .upsert(index_record_id)
        .content(InternalSurrealDBBrokerQueueIndex { queue_id })
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    match qm {
        Some(_) => Ok(()), // happy path
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': adding to index (silently did not add)",
        ))),
    }
}

/// get the index message given a queue name and task id, in O(k) time
/// None is a valid return value if the message was never in the system in the first place
async fn get_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueueIndex>, BroccoliError> {
    let index_record_id = index_record_id(task_id, queue_name)?;
    let queue_index: Option<InternalSurrealDBBrokerQueueIndex> = db
        .select(index_record_id)
        .await
        .map_err(
        |e: surrealdb::Error| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")),
    )?;
    Ok(queue_index)
}

// clear the index table
async fn remove_from_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let index_table = self::index_table(queue_name);
    let index_record_str = format!("{index_table}:[<uuid>'{task_id}','{queue_name}']");
    let index_record_id = RecordId::from_str(&index_record_str);
    match index_record_id {
        Ok(index_record) => {
            let deleted: Option<InternalSurrealDBBrokerQueueIndex> = db
                .delete(index_record)
                .await
                .map_err(|e: surrealdb::Error| {
                    BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
                })?;
            match deleted {
                Some(_) => Ok(()), // happy path
                None => Err(BroccoliError::Broker(format!(
                    "{err_msg}:'{queue_name}': removing from queue index (silently did not add)",
                ))),
            }
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': removing from queue index (wrong index record): {e}",
        ))),
    }
}

/// get first queued message if any, non-blocking
pub async fn get_queued(
    db: &Surreal<Any>,
    queue_name: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueuedMessageRecord>, BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let q = "SELECT * FROM (SELECT * FROM type::thing($queue_table,type::range([[None,None],[time::now(),None]]))) ORDER BY priority,id[0] ASC LIMIT 1";
    let mut queued: Response = db
        .query(q)
        .bind(("queue_table", queue_table))
        //.bind(("range", range))
        .await
        .map_err(|e| {
            BroccoliError::Broker(format!(
                "{err_msg}:'{queue_name}' Could not get queued: {e}"
            ))
        })?;
    let queued = queued.take(0);
    match queued {
        Ok(queued) => {
            let queued: Option<InternalSurrealDBBrokerQueuedMessageRecord> = queued;
            queued.map_or_else(|| Ok(None), |queued| Ok(Some(queued)))
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}' Could not get queued (taking value): {e}"
        ))),
    }
}

/// remove from ordered queue
/// `queued_message_id` must be: queue:[timestamp, `task_id`]
pub async fn remove_from_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message_id: RecordId, // queue:[timestamp, task_id]
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerQueuedMessage, BroccoliError> {
    let deleted: Option<InternalSurrealDBBrokerQueuedMessage> = db
        .delete(queued_message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    deleted.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
                "{err_msg}:'{queue_name}': Removing from queue (silently nothing was removed)",
            )))
        },
        Ok,
    )
}

/// given the user facing task id, remove from the queue
pub async fn remove_queued_from_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueuedMessage>, BroccoliError> {
    let queue_index = self::get_queue_index(db, queue_name, task_id, err_msg).await?;
    match queue_index {
        Some(queue_index) => {
            let removed =
                self::remove_from_queue(db, queue_name, queue_index.queue_id, err_msg).await?;
            Ok(Some(removed))
        }
        None => Ok(None), // message was not in the system
    }
}

/// add the message itself with it's payload
pub async fn add_message(
    db: &Surreal<Any>,
    queue_name: &str,
    msg: &InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let added: Option<InternalSurrealDBBrokerMessage> = db
        .create((queue_name, &msg.task_id))
        .content(InternalSurrealDBBrokerMessage::from(
            queue_name,
            msg.to_owned(),
        ))
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    added.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
                "{err_msg}: adding message (silently did not add anything)",
            )))
        },
        |added| Ok(added.into()),
    )
}

/// update the message, done to update the number of attempts, leaving rest unchanged
pub async fn update_message(
    db: &Surreal<Any>,
    queue_name: &str,
    msg: InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let updated: Option<InternalSurrealDBBrokerMessage> = db
        .update((queue_name, &msg.task_id))
        .content(InternalSurrealDBBrokerMessage::from(queue_name, msg))
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    match updated {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': Updating message (silently no update)"
        ))),
    }
}

/// get the message payload given the queue record
pub async fn get_message_from(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message: InternalSurrealDBBrokerQueuedMessageRecord,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message_id = queued_message.message_id;
    self::get_message(db, queue_name, message_id, err_msg).await
}

/// get the actual message
/// `message_id` <`queue_table>`:[<`task_id`>]
pub async fn get_message(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: RecordId,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message: Option<InternalSurrealDBBrokerMessage> = db
        .select(message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    message.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
                "{err_msg}:'{queue_name}': getting message (silently did not get anything)"
            )))
        },
        |message| Ok(message.into()),
    )
}

/// remove actual message (the one with the payload)
/// we also remove it from the internal index
/// `message_id`: <`queue_table>`:[<`task_id`>]
pub async fn remove_message(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: RecordId, //<queue_table>:[<task_id>]
    task_id: &str,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let resp: Option<InternalSurrealDBBrokerMessage> = db
        .delete(message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    match resp {
        Some(_) => {
            // remove from index
            let () = self::remove_from_queue_index(db, queue_name, task_id, err_msg).await?;
            Ok(())
        }
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}: removing message (silently did not remove anything)"
        ))),
    }
}

/// add to processing queue
pub async fn add_to_processing(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message: InternalSurrealDBBrokerQueuedMessageRecord,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let processing_table = self::processing_table(queue_name);
    let message_id = queued_message.message_id;
    let uuid = message_id.key().clone();
    let priority = queued_message.priority;
    let processing: Option<InternalSurrealDBBrokerProcessingMessage> = db
        .create((processing_table, uuid))
        .content(InternalSurrealDBBrokerProcessingMessage {
            message_id,
            priority,
        })
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    match processing {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': adding to processing (silently did not add anything)"
        ))),
    }
}

/// remove from the processing queue
pub async fn remove_from_processing(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerProcessingMessage, BroccoliError> {
    let processing_table = self::processing_table(queue_name);
    let processed: Option<InternalSurrealDBBrokerProcessingMessage> = db
        .delete((processing_table, message_id))
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}' {e}")))?;
    processed.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}' removing from processing (silently did not remove anything)"
        )))
        },
        Ok,
    )
}

/// add to the failed queue, will also remove from index
pub async fn add_to_failed(
    db: &Surreal<Any>,
    queue_name: &str,
    msg: InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let failed_table = self::failed_table(queue_name);
    let failed: Option<InternalSurrealDBBrokerFailedMessage> = db
        .create((failed_table, msg.task_id.clone()))
        .content(InternalSurrealDBBrokerFailedMessage {
            original_msg: InternalSurrealDBBrokerMessage::from(queue_name, msg),
        })
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}' {e}")))?;
    match failed {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': adding to failed (silently did not add anything)"
        ))),
    }
}

// used to parse dates in surrealdb format
fn to_rfc3339<T>(dt: T) -> Result<std::string::String, time::error::Format>
where
    T: Into<OffsetDateTime>,
{
    dt.into().format(&Rfc3339)
}
