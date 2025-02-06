use broccoli_queue::queue::{ConsumeOptionsBuilder, PublishOptions};
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
async fn test_publish_and_consume() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_publish_topic";

    // Test message
    let message = TestMessage {
        id: "1".to_string(),
        content: "test content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "test-fairness"))]
    let published = queue
        .publish(test_topic, None, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "test-fairness")]
    let published = queue
        .publish(test_topic, Some(String::from("job-1")), &message, None)
        .await
        .expect("Failed to publish message");

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Consume message
    let consumed = queue
        .consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .expect("Failed to consume message");

    assert_eq!(published.payload, consumed.payload);
    assert_eq!(published.task_id, consumed.task_id);

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let processing_queue_name = format!("{}_processing", test_topic);
        #[cfg(feature = "test-fairness")]
        let processing_queue_name = format!("{}_job-1_processing", test_topic);

        // Verify message is in the processing queue
        let processing: usize = redis.llen(processing_queue_name).await.unwrap();
        assert_eq!(processing, 1, "Message should be in processing queue");

        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        queue
            .acknowledge(&queue_name, consumed)
            .await
            .expect("Failed to acknowledge message");

        // After acknowledge, verify cleanup
        let exists: bool = redis.exists(published.task_id.to_string()).await.unwrap();
        assert!(!exists, "Message should be cleaned up after acknowledge");
    }
}

#[tokio::test]
async fn test_batch_publish_and_consume() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_batch_topic";

    // Test messages
    let messages = vec![
        TestMessage {
            id: "1".to_string(),
            content: "content 1".to_string(),
        },
        TestMessage {
            id: "2".to_string(),
            content: "content 2".to_string(),
        },
    ];

    // Publish batch
    #[cfg(not(feature = "test-fairness"))]
    let published = queue
        .publish_batch(test_topic, None, messages.clone(), None)
        .await
        .expect("Failed to publish batch");
    #[cfg(feature = "test-fairness")]
    let published = queue
        .publish_batch(
            test_topic,
            Some(String::from("job-1")),
            messages.clone(),
            None,
        )
        .await
        .expect("Failed to publish batch");

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Consume messages
    let consumed = queue
        .consume_batch::<TestMessage>(test_topic, 2, Duration::seconds(5), Some(consume_options))
        .await
        .expect("Failed to consume batch");

    assert_eq!(published.len(), consumed.len());
    assert_eq!(published[0].payload, consumed[0].payload);
    assert_eq!(published[1].payload, consumed[1].payload);

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify queue size after consuming
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(
            remaining, 0,
            "Queue should be empty after consuming all messages"
        );
    }
}

#[tokio::test]
async fn test_delayed_message() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_delayed_topic";

    let message = TestMessage {
        id: "delayed".to_string(),
        content: "delayed content".to_string(),
    };

    // Publish with delay
    let options = PublishOptions::builder()
        .delay(time::Duration::seconds(2))
        .build();

    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &message, Some(options))
        .await
        .expect("Failed to publish delayed message");
    #[cfg(feature = "test-fairness")]
    queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &message,
            Some(options),
        )
        .await
        .expect("Failed to publish delayed message");

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();
    // Try immediate consume (should be None)
    let immediate_result = queue
        .try_consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to try_consume");
    assert!(immediate_result.is_none());

    // Wait for delay
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify delayed message score
        let scores: Vec<(String, f64)> = redis
            .zrangebyscore_withscores(queue_name, "-inf", "+inf")
            .await
            .unwrap();
        assert!(!scores.is_empty(), "Delayed message should be in queue");
        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64;
        assert!(scores[0].1 > now, "Message score should be in future");
    }

    // Now consume (should get message)
    let delayed_result = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume delayed message");

    assert_eq!(message.content, delayed_result.payload.content);
}

#[tokio::test]
async fn test_scheduled_message() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_scheduled_topic";

    let message = TestMessage {
        id: "scheduled".to_string(),
        content: "scheduled content".to_string(),
    };

    // Schedule for 2 seconds in the future
    let schedule_time = time::OffsetDateTime::now_utc() + time::Duration::seconds(2);
    let options = PublishOptions::builder().schedule_at(schedule_time).build();

    #[cfg(not(feature = "test-fairness"))]
    let published = queue
        .publish(test_topic, None, &message, Some(options))
        .await
        .expect("Failed to publish scheduled message");
    #[cfg(feature = "test-fairness")]
    let published = queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &message,
            Some(options),
        )
        .await
        .expect("Failed to publish scheduled message");

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Try immediate consume (should be None)
    let immediate_result = queue
        .try_consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to try_consume");

    assert!(immediate_result.is_none());

    // Wait for schedule time
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Now consume (should get message)
    let scheduled_result = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume scheduled message");

    assert_eq!(published.payload.content, scheduled_result.payload.content);

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify queue is empty
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(remaining, 0, "Queue should be empty after consuming");
    }
}

#[tokio::test]
async fn test_message_retry() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_retry_topic";

    let message = TestMessage {
        id: "retry".to_string(),
        content: "retry content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "test-fairness"))]
    let published = queue
        .publish(test_topic, None, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "test-fairness")]
    let published = queue
        .publish(test_topic, Some(String::from("job-1")), &message, None)
        .await
        .expect("Failed to publish message");

    // Simulate failed processing 3 times
    for _ in 0..3 {
        #[cfg(not(feature = "test-fairness"))]
        let consume_options = ConsumeOptions::default();
        #[cfg(feature = "test-fairness")]
        let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

        let consumed = queue
            .consume::<TestMessage>(test_topic, Some(consume_options))
            .await
            .expect("Failed to consume message");

        // Reject the message
        queue
            .reject(test_topic, consumed)
            .await
            .expect("Failed to reject message");
    }

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // // Try to consume again - should be in failed queue
    let result = queue
        .try_consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .unwrap();
    assert!(result.is_none());

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let failed_queue_name = format!("{}_failed", test_topic);
        #[cfg(feature = "test-fairness")]
        let failed_queue_name = format!("{}_job-1_failed", test_topic);

        // Verify message in failed queue
        let failed_len: usize = redis.llen(failed_queue_name).await.unwrap();
        assert_eq!(failed_len, 1, "Message should be in failed queue");

        // Verify attempts counter
        let attempts: String = redis
            .hget(published.task_id.to_string(), "attempts")
            .await
            .unwrap();
        assert_eq!(attempts, "2", "Attempts counter should be 2");
    }
}

#[tokio::test]
async fn test_message_acknowledgment() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_ack_topic";

    let message = TestMessage {
        id: "ack".to_string(),
        content: "ack content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "test-fairness")]
    queue
        .publish(test_topic, Some(String::from("job-1")), &message, None)
        .await
        .expect("Failed to publish message");

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();
    // Consume and acknowledge
    let consumed = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume message");

    queue
        .acknowledge(test_topic, consumed)
        .await
        .expect("Failed to acknowledge message");

    // Try to consume again - should be none
    let result = queue
        .try_consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .unwrap();
    assert!(result.is_none());

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify queue is empty
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(remaining, 0, "Queue should be empty after acknowledgment");

        // Verify processing queue is empty
        #[cfg(not(feature = "test-fairness"))]
        let processing_queue = format!("{}_processing", test_topic);
        #[cfg(feature = "test-fairness")]
        let processing_queue = format!("{}_job-1_processing", test_topic);

        let processing: usize = redis.llen(processing_queue).await.unwrap();
        assert_eq!(processing, 0, "Processing queue should be empty");
    }
}

#[tokio::test]
async fn test_message_auto_ack() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_auto_ack_topic";

    let message = TestMessage {
        id: "ack".to_string(),
        content: "ack content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "test-fairness")]
    queue
        .publish(test_topic, Some(String::from("job-1")), &message, None)
        .await
        .expect("Failed to publish message");

    #[cfg(not(feature = "test-fairness"))]
    let options = ConsumeOptionsBuilder::new().auto_ack(true).build();
    #[cfg(feature = "test-fairness")]
    let options = ConsumeOptionsBuilder::new()
        .fairness(true)
        .auto_ack(true)
        .build();
    // Consume and auto-ack
    queue
        .consume::<TestMessage>(test_topic, Some(options.clone()))
        .await
        .expect("Failed to consume message");
    // Try to consume again - should be none
    let result = queue
        .try_consume::<TestMessage>(test_topic, Some(options))
        .await
        .unwrap();
    assert!(result.is_none());

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify queue is empty
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(remaining, 0, "Queue should be empty after auto-ack");

        // Verify processing queue doesn't exist (auto-ack skips processing queue)
        #[cfg(not(feature = "test-fairness"))]
        let processing_queue = format!("{}_processing", test_topic);
        #[cfg(feature = "test-fairness")]
        let processing_queue = format!("{}_job-1_processing", test_topic);

        let processing: usize = redis.llen(processing_queue).await.unwrap();
        assert_eq!(processing, 0, "Processing queue should be empty");
    }
}

#[tokio::test]
async fn test_message_cancellation() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_cancel_topic";

    let message = TestMessage {
        id: "cancel".to_string(),
        content: "cancel content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "test-fairness"))]
    let published = queue
        .publish(test_topic, None, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "test-fairness")]
    let published = queue
        .publish(test_topic, Some(String::from("job-1")), &message, None)
        .await
        .expect("Failed to publish message");
    // Cancel the message
    let result = queue
        .cancel(test_topic, published.task_id.to_string())
        .await;

    match result {
        Ok(()) => (),
        Err(e) if e.to_string().contains("NotImplemented") => {
            println!("This feature is not implemented for this broker");
            return;
        }
        Err(e) => {
            panic!("Failed to get message position: {:?}", e);
        }
    };

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Try to consume - should be none
    let result = queue
        .try_consume::<TestMessage>(test_topic, Some(consume_options))
        .await
        .unwrap();
    assert!(result.is_none());

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify message removed from queue
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(remaining, 0, "Queue should be empty after cancellation");

        // Verify message metadata cleaned up
        let exists: bool = redis.exists(published.task_id.to_string()).await.unwrap();
        assert!(!exists, "Message should be cleaned up after cancellation");
    }
}

#[tokio::test]
async fn test_message_priority() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic = "test_priority_topic";

    // Create messages with different priorities
    let messages = [
        TestMessage {
            id: "1".to_string(),
            content: "low priority".to_string(),
        },
        TestMessage {
            id: "2".to_string(),
            content: "high priority".to_string(),
        },
        TestMessage {
            id: "3".to_string(),
            content: "medium priority".to_string(),
        },
    ];

    // Publish messages with different priorities
    let options_low = PublishOptions::builder().priority(5).build();
    let options_high = PublishOptions::builder().priority(1).build();
    let options_medium = PublishOptions::builder().priority(3).build();

    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &messages[0], Some(options_low))
        .await
        .expect("Failed to publish low priority message");
    #[cfg(feature = "test-fairness")]
    queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &messages[0],
            Some(options_low),
        )
        .await
        .expect("Failed to publish low priority message");

    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &messages[1], Some(options_high))
        .await
        .expect("Failed to publish high priority message");
    #[cfg(feature = "test-fairness")]
    queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &messages[1],
            Some(options_high),
        )
        .await
        .expect("Failed to publish high priority message");

    #[cfg(not(feature = "test-fairness"))]
    queue
        .publish(test_topic, None, &messages[2], Some(options_medium))
        .await
        .expect("Failed to publish medium priority message");
    #[cfg(feature = "test-fairness")]
    queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &messages[2],
            Some(options_medium),
        )
        .await
        .expect("Failed to publish medium priority message");

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Consume messages - they should come in priority order (high to low)
    let first = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume first message");
    queue
        .acknowledge(test_topic, first.clone())
        .await
        .expect("Failed to acknowledge first message");
    let second = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume second message");
    queue
        .acknowledge(test_topic, second.clone())
        .await
        .expect("Failed to acknowledge second message");
    let third = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume third message");
    queue
        .acknowledge(test_topic, third.clone())
        .await
        .expect("Failed to acknowledge third message");

    // Verify priority ordering
    assert_eq!(first.payload.content, "high priority");
    assert_eq!(second.payload.content, "medium priority");
    assert_eq!(third.payload.content, "low priority");

    #[cfg(feature = "redis")]
    {
        #[cfg(not(feature = "test-fairness"))]
        let queue_name = test_topic;
        #[cfg(feature = "test-fairness")]
        let queue_name = format!("{}_job-1_queue", test_topic);

        // Verify queue is empty
        let remaining: usize = redis.zcard(queue_name).await.unwrap();
        assert_eq!(
            remaining, 0,
            "Queue should be empty after consuming all messages"
        );

        // Verify all messages cleaned up
        let exists_first: bool = redis.exists(first.task_id.to_string()).await.unwrap();
        let exists_second: bool = redis.exists(second.task_id.to_string()).await.unwrap();
        let exists_third: bool = redis.exists(third.task_id.to_string()).await.unwrap();
        assert!(
            !exists_first && !exists_second && !exists_third,
            "All messages should be cleaned up"
        );
    }
}
