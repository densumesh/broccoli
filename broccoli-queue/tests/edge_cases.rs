use broccoli_queue::queue::ConsumeOptions;
use broccoli_queue::queue::ConsumeOptionsBuilder;
use broccoli_queue::queue::PublishOptions;
#[cfg(feature = "redis")]
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use time::Duration;

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

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify remaining message count
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(remaining, 5, "Should have 5 messages remaining");

        #[cfg(not(feature = "test-fairness"))]
        let processing_queue_name = format!("{}_processing", test_topic);
        #[cfg(feature = "test-fairness")]
        let processing_queue_name = format!("{}_job-1_processing", test_topic);

        // Verify processing queue
        let processing: usize = redis.llen(processing_queue_name).await.unwrap();
        assert_eq!(
            processing, 0,
            "Processing queue should be empty after acknowledgments"
        );

        // Add verification for each consumed message
        for consumed_msg in &consumed_messages {
            let task_exists: bool = redis
                .hexists(consumed_msg.id.clone(), "payload")
                .await
                .unwrap();
            assert!(
                !task_exists,
                "Message should be cleaned up after acknowledgment"
            );
        }

        // Verify fairness is maintained during concurrent consumption
        if unique_ids.len() != 5 {
            let mut id_counts = std::collections::HashMap::new();
            for msg in consumed_messages {
                *id_counts.entry(msg.id).or_insert(0) += 1;
            }
            for (id, count) in id_counts {
                assert!(count <= 1, "Message {} was consumed {} times", id, count);
            }
        }
    }
}

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
