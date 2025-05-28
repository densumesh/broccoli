use std::sync::Arc;

use broccoli_queue::error::BroccoliError;
use broccoli_queue::queue::ConsumeOptions;
use broccoli_queue::queue::PublishOptions;
#[cfg(feature = "redis")]
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use time::Duration;
use tokio::sync::Mutex;

mod common;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    id: String,
    content: String,
}

#[tokio::test]
async fn test_invalid_broker_url() {
    let result = common::setup_queue_with_url("invalid://localhost:6379").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_empty_payload() {
    let queue = common::setup_queue().await;

    let test_topic = "test_empty_topic";

    let empty_message = TestMessage {
        id: "".to_string(),
        content: "".to_string(),
    };

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

    #[cfg(not(feature = "test-fairness"))]
    let result = queue.publish(test_topic, None, &empty_message, None).await;
    #[cfg(feature = "test-fairness")]
    let result = queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &empty_message,
            None,
        )
        .await;
    assert!(result.is_ok());

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    let consumed = queue
        .consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .unwrap();
    assert_eq!(consumed.payload, empty_message);

    #[cfg(feature = "redis")]
    {
        // Verify queue state after consuming empty message
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        let queue_len: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(queue_len, 0, "Queue should be empty after consuming");
    }
}

#[tokio::test]
async fn test_very_large_payload() {
    let queue = common::setup_queue().await;

    let test_topic = "test_large_topic";

    let large_content = "x".repeat(1024 * 1024); // 1MB of data
    let large_message = TestMessage {
        id: "large".to_string(),
        content: large_content.clone(),
    };

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

    #[cfg(not(feature = "test-fairness"))]
    let result = queue.publish(test_topic, None, &large_message, None).await;
    #[cfg(feature = "test-fairness")]
    let result = queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &large_message,
            None,
        )
        .await;
    assert!(result.is_ok());

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    let consumed = queue
        .consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .unwrap();
    assert_eq!(consumed.payload.content.len(), large_content.len());

    #[cfg(feature = "redis")]
    {
        let stored_payload: String = redis
            .hget(consumed.task_id.to_string(), "payload")
            .await
            .unwrap();
        assert_eq!(
            stored_payload.len(),
            serde_json::to_string(&large_message).unwrap().len()
        );
    }
}

#[tokio::test]
async fn test_concurrent_consume() {
    let queue = common::setup_queue().await;

    let test_topic = "test_concurrent_topic";

    // Redis client for verification
    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

    // Publish multiple messages
    let messages: Vec<_> = (0..10)
        .map(|i| TestMessage {
            id: i.to_string(),
            content: format!("content {}", i),
        })
        .collect();

    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish_batch(test_topic, None, messages, None)
        .await
        .unwrap();

    #[cfg(feature = "test-fairness")]
    queue
        .publish_batch(test_topic, Some(String::from("job-1")), messages, None)
        .await
        .unwrap();

    // Consume concurrently
    let mut handles = vec![];
    for _ in 0..5 {
        let queue_clone = queue.clone();
        let topic = test_topic.to_string();
        handles.push(tokio::spawn(async move {
            #[cfg(not(feature = "test-fairness"))]
            let consume_options = ConsumeOptions::default();
            #[cfg(feature = "test-fairness")]
            let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

            let msg = queue_clone
                .consume::<TestMessage>(&topic, Some(consume_options))
                .await
                .unwrap();
            queue_clone
                .acknowledge(test_topic, msg.clone())
                .await
                .unwrap();
            msg
        }));
    }

    let results = futures::future::join_all(handles).await;
    let consumed_messages: Vec<_> = results.into_iter().map(|r| r.unwrap().payload).collect();

    assert_eq!(consumed_messages.len(), 5);
    // Ensure no duplicate messages were consumed
    let unique_ids: std::collections::HashSet<_> =
        consumed_messages.iter().map(|m| m.id.clone()).collect();
    assert_eq!(unique_ids.len(), 5);
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn test_ttl_not_implemented() {
    let queue = common::setup_queue().await;
    let test_topic = "test_zero_ttl_topic";
    let message = TestMessage {
        id: "zero_ttl".to_string(),
        content: "expires immediately".to_string(),
    };
    let options = PublishOptions::builder().ttl(Duration::seconds(0)).build();
    let result = queue
        .publish(test_topic, None, &message, Some(options))
        .await;

    if result.is_ok() {
        panic!("Should be unimplemented")
    }
}

#[cfg(not(feature = "surrealdb"))]
#[tokio::test]
async fn test_zero_ttl() {
    let queue = common::setup_queue().await;

    let test_topic = "test_zero_ttl_topic";

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

    let message = TestMessage {
        id: "zero_ttl".to_string(),
        content: "expires immediately".to_string(),
    };

    let options = PublishOptions::builder().ttl(Duration::seconds(0)).build();

    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &message, Some(options))
        .await
        .unwrap();
    #[cfg(feature = "test-fairness")]
    queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &message,
            Some(options),
        )
        .await
        .unwrap();

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Message should not be available
    let result = queue
        .try_consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .unwrap();
    assert!(result.is_none());

    #[cfg(feature = "redis")]
    {
        // Verify message was deleted due to TTL
        let exists: bool = redis.exists(&message.id).await.unwrap();
        assert!(!exists, "Message should be deleted due to TTL");

        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify queue is empty
        let queue_len: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(queue_len, 0, "Queue should be empty");

        // Verify immediate TTL expiration
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let exists: bool = redis.exists(&message.id).await.unwrap();
        assert!(!exists, "Message should be deleted due to zero TTL");

        // Verify message not in queue
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        let queue_len: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(queue_len, 0, "Queue should be empty with zero TTL message");
    }
}

#[tokio::test]
async fn test_message_ordering() {
    let queue = common::setup_queue().await;

    let test_topic = "test_ordering_topic";

    // Redis client for verification
    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

    // Publish messages with different delays
    let messages = vec![
        (
            TestMessage {
                id: "1".to_string(),
                content: "first".to_string(),
            },
            Some(
                PublishOptions::builder()
                    .delay(Duration::seconds(2))
                    .build(),
            ),
        ),
        (
            TestMessage {
                id: "2".to_string(),
                content: "second".to_string(),
            },
            Some(
                PublishOptions::builder()
                    .delay(Duration::seconds(1))
                    .build(),
            ),
        ),
        (
            TestMessage {
                id: "3".to_string(),
                content: "third".to_string(),
            },
            None,
        ),
    ];

    for (msg, opt) in messages {
        #[cfg(not(feature = "test-fairness"))]
        queue.publish(test_topic, None, &msg, opt).await.unwrap();
        #[cfg(feature = "test-fairness")]
        queue
            .publish(test_topic, Some(String::from("job-1")), &msg, opt)
            .await
            .unwrap();
    }

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Consume messages
    let third = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .unwrap();
    queue.acknowledge(test_topic, third.clone()).await.unwrap();
    assert_eq!(third.payload.id, "3");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let second = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .unwrap();
    queue.acknowledge(test_topic, second.clone()).await.unwrap();
    let first = queue
        .consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .unwrap();
    queue.acknowledge(test_topic, first.clone()).await.unwrap();

    assert_eq!(second.payload.id, "2");
    assert_eq!(first.payload.id, "1");

    // Verify Redis state after test
    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Check queue is empty
        let len: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(len, 0, "Queue should be empty after consuming all messages");

        #[cfg(not(feature = "test-fairness"))]
        let processing_queue_name = format!("{}_processing", test_topic);
        #[cfg(feature = "test-fairness")]
        let processing_queue_name = format!("{}_job-1_processing", test_topic);

        // Check processing queue is empty
        let proc_len: usize = redis.llen(processing_queue_name).await.unwrap();
        assert_eq!(proc_len, 0, "Processing queue should be empty");

        // Verify message order in Redis sorted set
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        let scores: Vec<(String, f64)> = redis
            .zrangebyscore_withscores(queue_name, "-inf", "+inf")
            .await
            .unwrap();

        // Verify scores are ordered correctly (delayed messages have higher scores)
        for i in 0..scores.len().saturating_sub(1) {
            assert!(
                scores[i].1 <= scores[i + 1].1,
                "Messages should be ordered by score"
            );
        }

        // Verify processing state after consumption
        #[cfg(not(feature = "test-fairness"))]
        let processing_queue = format!("{}_processing", test_topic);
        #[cfg(feature = "test-fairness")]
        let processing_queue = format!("{}_job-1_processing", test_topic);

        let proc_len: usize = redis.llen(&processing_queue).await.unwrap();
        assert_eq!(
            proc_len, 0,
            "Processing queue should be empty after acknowledgments"
        );
    }
}

#[tokio::test]
#[cfg(feature = "surrealdb")]
async fn test_multiple_batch_publish_and_consume() {
    // in surrealdb mem:// sometimes we get out of order publishes
    // code relies time now()-like calls that may or may not be granular enough for strict
    // ordering, this manifests rarely, but it does happen
    // publish(1, now()),publish(2, now()) --> depending on the now impl, order is non-deterministic
    // note that this also happens when transitioning from publish to consume
    // publish(2, now()),consume(now()) with now() returning the same

    let test_topic = "test_multiple_batch_publish_and_consume_topic";
    let queue = common::setup_queue().await;

    // Test messages
    let messages = vec![
        TestMessage {
            id: "1".to_string(),
            content: "content 1".to_string(),
        },
        TestMessage {
            id: "2".to_string(),
            content: "content 2 [last]".to_string(),
        },
    ];

    for i in 0..10 {
        let published: Vec<TestMessage> = queue
            .publish_batch(test_topic, None, messages.clone(), None)
            .await
            .expect("Failed to publish batch")
            .into_iter()
            .map(|m| m.payload)
            .collect();
        assert_eq!(2, published.len());
        tokio::time::sleep(tokio::time::Duration::ZERO).await;
        let mut read: usize = 0;
        let mut consumed: Vec<TestMessage> = Vec::with_capacity(2);
        for _ in 0..2 {
            let msg: TestMessage = queue
                .consume::<TestMessage>(test_topic, None)
                .await
                .expect("should not fail to consume")
                .payload;
            read += msg.id.parse::<usize>().unwrap();
            consumed.push(msg);
            // be mindful of ids that start with zero when doing this checks
            if read == 3 {
                break;
            }
        }
        assert_eq!(2, consumed.len(), "missed read in iteration {}", i);
        assert_eq!(published.len(), consumed.len());
        assert!(
            (published[0] == consumed[0] && published[1] == consumed[1])
                || (published[0] == consumed[1] && published[1] == consumed[0])
        );
    }
}

lazy_static::lazy_static! {
    // warning: do not share these variables across tests in the same run
    static ref processed: Arc<tokio::sync::Mutex<usize>> = Arc::new(Mutex::new(0));
}

async fn process_job(m: TestMessage) -> Result<(), BroccoliError> {
    // helper function to test process_messages
    let mut lock = processed.lock().await;
    let value = m.id.parse::<usize>().expect("should have id as an int");
    *lock += value;
    Ok(())
}

#[tokio::test]
#[cfg(feature = "surrealdb")]
async fn test_multiple_batch_publish_and_handler() {
    // similar situation with handlers

    let test_topic = "test_multiple_batch_publish_and_handler_topic";
    let queue = common::setup_queue().await;

    // Test messages
    let messages = vec![
        TestMessage {
            id: "1".to_string(),
            content: "content 1".to_string(),
        },
        TestMessage {
            id: "2".to_string(),
            content: "content 2 [last]".to_string(),
        },
    ];

    for _ in 0..10 {
        // launch consumer first

        // CRITICAL AREA START //
        {
            let mut _lock = processed.lock().await;
            *_lock = 0;
            let _lock: Option<()> = None;
        }
        // CRITICAL AREA END //

        let queue_clone = queue.clone();
        let consumer = tokio::spawn(async move {
            let _ = queue_clone
                .process_messages(
                    test_topic,
                    None,
                    Some(
                        ConsumeOptions::builder()
                            .consume_wait(std::time::Duration::from_millis(1))
                            .build(),
                    ),
                    |msg| async { process_job(msg.payload).await },
                )
                .await;
            panic!("Spawn should have been killed while processing");
        });

        let published: Vec<TestMessage> = queue
            .publish_batch(test_topic, None, messages.clone(), None)
            .await
            .expect("Failed to publish batch")
            .into_iter()
            .map(|m| m.payload)
            .collect();
        assert_eq!(2, published.len());

        let expected_count = 3; // 1+2
        let wait = tokio::spawn(async move {
            let mut counter = 0;
            while counter < expected_count {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                // CRITICAL AREA START //
                let _lock = processed.lock().await;
                counter = *_lock;
                let _lock: Option<()> = None;
                // CRITICAL AREA END //
            }
            consumer.abort();
        });
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(20), wait).await;
        // CRITICAL AREA START //
        {
            let _lock = processed.lock().await;
            assert_eq!(3, *_lock);
            let _lock: Option<()> = None;
        }
        // CRITICAL AREA END //
    }
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn test_redis_specific_queue_structure() {
    use std::collections::HashMap;

    let queue = common::setup_queue().await;
    let test_topic = "test_redis_structure";
    let mut redis = common::get_redis_client().await;

    // Test message
    let message = TestMessage {
        id: "struct_test".to_string(),
        content: "test content".to_string(),
    };

    // Publish message
    let broker_message = queue
        .publish(test_topic, None, &message, None)
        .await
        .unwrap();

    // Verify queue structure
    let queue_type: String = redis.key_type(test_topic).await.unwrap();
    assert_eq!(queue_type, "zset", "Main queue should be a sorted set");

    // Verify message metadata
    let metadata: HashMap<String, String> = redis
        .hgetall(broker_message.task_id.to_string())
        .await
        .unwrap();
    assert!(
        metadata.contains_key("task_id"),
        "Message should have task_id"
    );
    assert!(
        metadata.contains_key("payload"),
        "Message should have payload"
    );
    assert!(
        metadata.contains_key("attempts"),
        "Message should have attempts counter"
    );

    // Consume message
    let consumed = queue
        .consume::<TestMessage>(test_topic, None)
        .await
        .unwrap();

    // Verify processing queue
    let proc_type: String = redis
        .key_type(format!("{}_processing", test_topic))
        .await
        .unwrap();
    assert_eq!(proc_type, "list", "Processing queue should be a list");

    // Acknowledge and verify cleanup
    queue.acknowledge(test_topic, consumed).await.unwrap();
    let exists: bool = redis.exists(&message.id).await.unwrap();
    assert!(!exists, "Message should be cleaned up after acknowledgment");
}
