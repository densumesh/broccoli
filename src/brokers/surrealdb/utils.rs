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

impl SurrealDBBroker {
    /// Creates a new `SurrealDBBroker` instance with default configuration.
    pub fn new() -> Self {
        SurrealDBBroker {
            db: None,
            connected: false,
            config: None,
        }
    }

    /// new with passed configuration
    pub(crate) fn new_with_config(config: BrokerConfig) -> Self {
        Self {
            db: None,
            connected: false,
            config: Some(config),
        }
    }

    /// check and return current active connection
    pub(crate) fn check_connected(&self) -> Result<Surreal<Any>, BroccoliError> {
        match &self.db {
            Some(db) => Ok(db.to_owned()),
            None => Err(BroccoliError::Broker("Not connected".to_string())),
        }
    }

    /// we create a surreadlb connection from the url configuration
    /// URL parameters after ? are username, password, ns, database
    /// if unspecified, will default back to root,root,test,test (only for testing!)
    /// note that only 'ws' is supported RTN to allow for live querying
    pub async fn client_from_url(
        broker_url: &str,
    ) -> Result<std::option::Option<Surreal<Any>>, BroccoliError> {
        let url = Url::parse(broker_url).map_err(|e| {
            BroccoliError::Broker(format!("Failed to parse connection URL: {:?}", e))
        })?;
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
        let connection_url = format!("ws://{}:{}/rpc", url.host_str().unwrap(), port.unwrap());
        let db = connect(connection_url).await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to connect to SurrealDB: {:?}", e))
        })?;

        db.signin(surrealdb::opt::auth::Root {
            username: &config.username,
            password: &config.password,
        })
        .await
        .map_err(|e| {
            BroccoliError::Broker(format!("Incorrect credentials for SurrealDB: {:?}", e))
        })?;

        // Select a specific namespace / database
        db.use_ns(config.ns)
            .use_db(config.database)
            .await
            .map_err(|e| {
                BroccoliError::Broker(format!("NS/DB not found for SurrealDB: {:?}", e))
            })?;

        Ok(Some(db))
    }

    /// helper: given a url get a named parameter
    pub(crate) fn get_param_value(url: &Url, name: &str) -> Result<String, BroccoliError> {
        url.query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned().to_string())
            .map_or_else(
                || {
                    Err(BroccoliError::Broker(format!(
                        "Missing connection param: {}",
                        name
                    )))
                },
                |v| Ok(v),
            )
    }
}

// convenience into and from conversion between the broccoli and the surrealdb layer

impl Into<InternalBrokerMessage> for InternalSurrealDBBrokerMessage {
    fn into(self) -> InternalBrokerMessage {
        InternalBrokerMessage {
            task_id: self.task_id,
            payload: self.payload,
            attempts: self.attempts,
            disambiguator: None,
            metadata: self.metadata,
        }
    }
}

impl InternalSurrealDBBrokerMessage {
    fn from(queue_name: &str, msg: InternalBrokerMessage) -> Self {
        let id: RecordId = (queue_name, msg.task_id.clone()).into();
        InternalSurrealDBBrokerMessage {
            id,
            task_id: msg.task_id,
            payload: msg.payload,
            attempts: msg.attempts,
            metadata: msg.metadata,
        }
    }
}

/// this table holds a timesorted timeseries, <queue_name>:[<timestamp>,<taskid>]
/// and acts as the queue
pub(crate) fn queue_table(queue_name: &str) -> String {
    format!("{}___queue", queue_name)
}

/// this table holds messages in process
fn processing_table(queue_name: &str) -> String {
    format!("{}___processing", queue_name)
}

/// this is the failed messages table
fn failed_table(queue_name: &str) -> String {
    format!("{}___failed", queue_name)
}

/// this is an index to go from <queue_name>___index:[<taskid>,<queuename>] to the queue table
/// in O(k) time
fn index_table(queue_name: &str) -> String {
    format!("{}___index", queue_name)
}

/// add to the timeseries
pub(crate) async fn add_to_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let now = OffsetDateTime::now_utc();
    let _ = self::add_to_queue_scheduled(&db, queue_name, task_id, priority, now, err_msg).await?;
    Ok(())
}

/// add to the timeseries with a delay duration
pub(crate) async fn add_to_queue_delayed(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    delay: Duration,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let when = OffsetDateTime::now_utc() + delay;
    let _ = self::add_to_queue_scheduled(&db, queue_name, task_id, priority, when, err_msg).await?;
    Ok(())
}

/// add to the timeseries at a scheduled time, can be in the past and it will be triggered immediately
pub(crate) async fn add_to_queue_scheduled(
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
            let when = format!("<datetime>'{}'", time);
            let _ =
                self::add_record_to_queue(db, queue_name, task_id, priority, when, err_msg).await?;
            Ok(())
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{}:'{}' could not convert delay: {})",
            err_msg, queue_name, e
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
    let queue_table = self::queue_table(queue_name);
    // TODO: look at building the record programmatically, move when to typed
    // compromise here is that we do explicit casting of the uuid, if it's not correct it will fail
    let queue_record_str = format!("{}:[{},<uuid>'{}']", queue_table, when, task_id);
    let queue_record_id = RecordId::from_str(&queue_record_str);
    match queue_record_id {
        Ok(queue_id) => {
            let message_record_id: RecordId = (queue_name, task_id).into();
            let qm: Option<InternalSurrealDBBrokerQueuedMessage> = db
                .create(queue_id.clone())
                .content(InternalSurrealDBBrokerQueuedMessage {
                    message_id: message_record_id,
                    priority,
                })
                .await
                .map_err(|e: surrealdb::Error| {
                    BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e))
                })?;
            match qm {
                Some(_) => {
                    // now we insert into the index and we are done, note we insert the queue id
                    let _ = self::add_to_queue_index(&db, queue_name, task_id, queue_id, err_msg)
                        .await?;
                    Ok(())
                }
                None => Err(BroccoliError::Broker(format!(
                    "{}:'{}': adding to queue (silently did not add)",
                    err_msg, queue_name,
                ))),
            }
        }
        Err(err) => Err(BroccoliError::Broker(format!(
            "{}:'{}': adding to queue  (wrong record): {}",
            err_msg, queue_name, err,
        ))),
    }
}

// we add an entry into a queue index, index:[messageid,queue_name] {queue_id} where
// queue_id is basically: queue:[timestamp,messageid].
// We can use this index we can do cancellations in O(k) time and workaround parser issues
async fn add_to_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    queue_id: RecordId, // queue:[timestamp, task_id]
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    // we create the index record and add to the index
    // we upsert because if re reenqueue we will be re-setting and not creating from scratch
    // error: index record is wrong, there is a serde problem or we did not insert anything
    let index_table = self::index_table(queue_name);
    let index_record_str = format!("{}:[<uuid>'{}','{}']", index_table, task_id, queue_name);
    let index_record_id = RecordId::from_str(&index_record_str);
    match index_record_id {
        Ok(record_id) => {
            let qm: Option<InternalSurrealDBBrokerQueueIndex> = db
                .upsert(record_id)
                .content(InternalSurrealDBBrokerQueueIndex { queue_id })
                .await
                .map_err(|e: surrealdb::Error| {
                    BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e))
                })?;
            match qm {
                Some(_) => Ok(()), // happy path
                None => Err(BroccoliError::Broker(format!(
                    "{}:'{}': adding to index (silently did not add)",
                    err_msg, queue_name,
                ))),
            }
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{}:'{}': adding to index (wrong index record): {}",
            err_msg, queue_name, e,
        ))),
    }
}

// get the index message given a queue name and task id, in O(k) time
async fn get_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueueIndex>, BroccoliError> {
    let index_table = self::index_table(queue_name);
    let index_record_str = format!("{}:[<uuid>'{}','{}']", index_table, task_id, queue_name);
    let index_record_id = RecordId::from_str(&index_record_str);
    match index_record_id {
        Ok(record_id) => {
            let message: Option<InternalSurrealDBBrokerQueueIndex> =
                db.select(record_id).await.map_err(|e: surrealdb::Error| {
                    BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e))
                })?;
            Ok(message)
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{}:'{}': getting message id from index (wrong index record): {}",
            err_msg, queue_name, e,
        ))),
    }
}

// clear the index table
async fn remove_from_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let index_table = self::index_table(queue_name);
    let index_record_str = format!("{}:[<uuid>'{}','{}']", index_table, task_id, queue_name);
    let index_record_id = RecordId::from_str(&index_record_str);
    match index_record_id {
        Ok(index_record) => {
            let deleted: Option<InternalSurrealDBBrokerQueueIndex> = db
                .delete(index_record)
                .await
                .map_err(|e: surrealdb::Error| {
                    BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e))
                })?;
            match deleted {
                Some(_) => Ok(()), // happy path
                None => Err(BroccoliError::Broker(format!(
                    "{}:'{}': removing from queue index (silently did not add)",
                    err_msg, queue_name,
                ))),
            }
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{}:'{}': removing from queue index (wrong index record): {}",
            err_msg, queue_name, e,
        ))),
    }
}

/// get first queued message if any, non-blocking
pub(crate) async fn get_queued(
    db: &Surreal<Any>,
    queue_name: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueuedMessageRecord>, BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let q = "SELECT * FROM type::thing($queue_table,type::range([[None,None],[time::now(),None]])) ORDER BY priority ASC LIMIT 1";
    let mut queued: Response = db
        .query(q)
        .bind(("queue_table", queue_table))
        //.bind(("range", range))
        .await
        .map_err(|e| {
            BroccoliError::Broker(format!(
                "{}:'{}' Could not get queued: {}",
                err_msg, queue_name, e
            ))
        })?;
    let queued = queued.take(0);
    match queued {
        Ok(queued) => {
            let queued: Option<InternalSurrealDBBrokerQueuedMessageRecord> = queued;
            match queued {
                Some(queued) => Ok(Some(queued.to_owned())),
                None => Ok(None), // nothing was queued
            }
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{}:'{}' Could not get queued (taking value): {}",
            err_msg, queue_name, e
        ))),
    }
}

/// remove from ordered queue
/// queued_message_id must be: queue:[timestamp, task_id]
pub(crate) async fn remove_from_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message_id: RecordId, // queue:[timestamp, task_id]
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerQueuedMessage, BroccoliError> {
    let deleted: Option<InternalSurrealDBBrokerQueuedMessage> = db
        .delete(queued_message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e)))?;
    match deleted {
        Some(message) => Ok(message),
        None => Err(BroccoliError::Broker(format!(
            "{}:'{}': Removing from queue (silently nothing was removed)",
            err_msg, queue_name,
        ))),
    }
}

/// given the user facing task id, remove from the queue
pub(crate) async fn remove_queued_from_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
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
pub(crate) async fn add_message(
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
            BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e))
        })?;
    match added {
        Some(added) => Ok(added.into()),
        None => Err(BroccoliError::Broker(format!(
            "{}: adding message (silently did not add anything)",
            err_msg,
        ))),
    }
}

/// update the message, done to update the number of attempts, leaving rest unchanged
pub(crate) async fn update_message(
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
            BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e))
        })?;
    match updated {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{}:'{}': Updating message (silently no update)",
            err_msg, queue_name
        ))),
    }
}

/// get the message payload given the queue record
pub(crate) async fn get_message_from(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message: InternalSurrealDBBrokerQueuedMessageRecord,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message_id = queued_message.message_id;
    self::get_message(db, queue_name, message_id, err_msg).await
}

/// get the actual message
/// message_id <queue_table>:[<task_id>]
pub(crate) async fn get_message(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: RecordId,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message: Option<InternalSurrealDBBrokerMessage> = db
        .select(message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e)))?;
    match message {
        Some(message) => Ok(message.into()),
        None => Err(BroccoliError::Broker(format!(
            "{}:'{}': getting message (silently did not get anything)",
            err_msg, queue_name
        ))),
    }
}

/// remove actual message (the one with the payload)
/// we also remove it from the internal index
/// message_id: <queue_table>:[<task_id>]
pub(crate) async fn remove_message(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: RecordId, //<queue_table>:[<task_id>]
    task_id: &str,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let resp: Option<InternalSurrealDBBrokerMessage> = db
        .delete(message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e)))?;
    match resp {
        Some(_) => {
            // remove from index
            let _ = self::remove_from_queue_index(db, queue_name, task_id, err_msg).await?;
            Ok(())
        }
        None => Err(BroccoliError::Broker(format!(
            "{}: removing message (silently did not remove anything)",
            err_msg
        ))),
    }
}

/// add to processing queue
pub(crate) async fn add_to_processing(
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
        .map_err(|e| BroccoliError::Broker(format!("{}:'{}': {}", err_msg, queue_name, e)))?;
    match processing {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{}:'{}': adding to processing (silently did not add anything)",
            err_msg, queue_name
        ))),
    }
}

/// remove from the processing queue
pub(crate) async fn remove_from_processing(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerProcessingMessage, BroccoliError> {
    let processing_table = self::processing_table(queue_name);
    let processed: Option<InternalSurrealDBBrokerProcessingMessage> = db
        .delete((processing_table, message_id))
        .await
        .map_err(|e| BroccoliError::Broker(format!("{}:'{}' {}", err_msg, queue_name, e)))?;
    match processed {
        Some(processed) => Ok(processed),
        None => Err(BroccoliError::Broker(format!(
            "{}:'{}' removing from processing (silently did not remove anything)",
            err_msg, queue_name
        ))),
    }
}

/// add to the failed queue, will also remove from index
pub(crate) async fn add_to_failed(
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
        .map_err(|e| BroccoliError::Broker(format!("{}:'{}' {}", err_msg, queue_name, e)))?;
    match failed {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{}:'{}': adding to failed (silently did not add anything)",
            err_msg, queue_name
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

#[cfg(test)]
#[tokio::test]
async fn new_with_url_test() {
    let url = Url::parse("ws://127.0.0.1/?username=user&password=passwd&ns=namespace&dtabase=db")
        .unwrap();
    let username = SurrealDBBroker::get_param_value(&url, "username").unwrap();
    assert_eq!("user", username);
    let password = SurrealDBBroker::get_param_value(&url, "password").unwrap();
    assert_eq!("passwd", password);
    let err = SurrealDBBroker::get_param_value(&url, "foo");
    assert!(err.is_err());
}
