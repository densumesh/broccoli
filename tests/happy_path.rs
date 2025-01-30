use broccoli_queue::queue::ConsumeOptionsBuilder;
use broccoli_queue::queue::PublishOptions;
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
    let test_topic = "test_publish_topic";

    // Test message
    let message = TestMessage {
        id: "1".to_string(),
        content: "test content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "fairness"))]
    let published = queue
        .publish(test_topic, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "fairness")]
    let published = queue
        .publish(test_topic, String::from("job-1"), &message, None)
        .await
        .expect("Failed to publish message");

    // Consume message
    let consumed = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .expect("Failed to consume message");

    assert_eq!(published.payload, consumed.payload);
    assert_eq!(published.task_id, consumed.task_id);
}

#[tokio::test]
async fn test_batch_publish_and_consume() {
    let queue = common::setup_queue().await;
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
    #[cfg(not(feature = "fairness"))]
    let published = queue
        .publish_batch(test_topic, messages.clone(), None)
        .await
        .expect("Failed to publish batch");
    #[cfg(feature = "fairness")]
    let published = queue
        .publish_batch(test_topic, String::from("job-1"), messages.clone(), None)
        .await
        .expect("Failed to publish batch");

    // Consume messages
    let consumed = queue
        .consume_batch::<TestMessage>(test_topic, 2, Duration::seconds(5), Default::default())
        .await
        .expect("Failed to consume batch");

    assert_eq!(published.len(), consumed.len());
    assert_eq!(published[0].payload, consumed[0].payload);
    assert_eq!(published[1].payload, consumed[1].payload);
}

#[tokio::test]
async fn test_delayed_message() {
    let queue = common::setup_queue().await;
    let test_topic = "test_delayed_topic";

    let message = TestMessage {
        id: "delayed".to_string(),
        content: "delayed content".to_string(),
    };

    // Publish with delay
    let options = PublishOptions::builder()
        .delay(time::Duration::seconds(2))
        .build();

    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &message, Some(options))
        .await
        .expect("Failed to publish delayed message");
    #[cfg(feature = "fairness")]
    queue
        .publish(test_topic, String::from("job-1"), &message, Some(options))
        .await
        .expect("Failed to publish delayed message");

    // Try immediate consume (should be None)
    let immediate_result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .expect("Failed to try_consume");
    assert!(immediate_result.is_none());

    // Wait for delay
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Now consume (should get message)
    let delayed_result = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .expect("Failed to consume delayed message");

    assert_eq!(message.content, delayed_result.payload.content);
}

#[tokio::test]
async fn test_scheduled_message() {
    let queue = common::setup_queue().await;
    let test_topic = "test_scheduled_topic";

    let message = TestMessage {
        id: "scheduled".to_string(),
        content: "scheduled content".to_string(),
    };

    // Schedule for 2 seconds in the future
    let schedule_time = time::OffsetDateTime::now_utc() + time::Duration::seconds(2);
    let options = PublishOptions::builder().schedule_at(schedule_time).build();

    #[cfg(not(feature = "fairness"))]
    let published = queue
        .publish(test_topic, &message, Some(options))
        .await
        .expect("Failed to publish scheduled message");
    #[cfg(feature = "fairness")]
    let published = queue
        .publish(test_topic, String::from("job-1"), &message, Some(options))
        .await
        .expect("Failed to publish scheduled message");

    // Try immediate consume (should be None)
    let immediate_result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .expect("Failed to try_consume");

    assert!(immediate_result.is_none());

    // Wait for schedule time
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Now consume (should get message)
    let scheduled_result = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .expect("Failed to consume scheduled message");

    assert_eq!(published.payload.content, scheduled_result.payload.content);
}

#[tokio::test]
async fn test_message_retry() {
    let queue = common::setup_queue().await;
    let test_topic = "test_retry_topic";

    let message = TestMessage {
        id: "retry".to_string(),
        content: "retry content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "fairness")]
    queue
        .publish(test_topic, String::from("job-1"), &message, None)
        .await
        .expect("Failed to publish message");

    // Simulate failed processing 3 times
    for _ in 0..3 {
        let consumed = queue
            .consume::<TestMessage>(test_topic, Default::default())
            .await
            .expect("Failed to consume message");

        // Reject the message
        #[cfg(not(feature = "fairness"))]
        queue
            .reject(test_topic, consumed)
            .await
            .expect("Failed to reject message");
        #[cfg(feature = "fairness")]
        queue
            .reject(test_topic, String::from("job-1"), consumed)
            .await
            .expect("Failed to reject message");
    }

    // // Try to consume again - should be in failed queue
    let result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_message_acknowledgment() {
    let queue = common::setup_queue().await;
    let test_topic = "test_ack_topic";

    let message = TestMessage {
        id: "ack".to_string(),
        content: "ack content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "fairness")]
    queue
        .publish(test_topic, String::from("job-1"), &message, None)
        .await
        .expect("Failed to publish message");

    // Consume and acknowledge
    let consumed = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .expect("Failed to consume message");

    queue
        .acknowledge(test_topic, consumed)
        .await
        .expect("Failed to acknowledge message");

    // Try to consume again - should be none
    let result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_message_auto_ack() {
    let queue = common::setup_queue().await;
    let test_topic = "test_auto_ack_topic";

    let message = TestMessage {
        id: "ack".to_string(),
        content: "ack content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "fairness")]
    queue
        .publish(test_topic, String::from("job-1"), &message, None)
        .await
        .expect("Failed to publish message");

    let options = ConsumeOptionsBuilder::new().auto_ack(true).build();
    // Consume and auto-ack
    queue
        .consume::<TestMessage>(test_topic, Some(options))
        .await
        .expect("Failed to consume message");
    // Try to consume again - should be none
    let result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_message_cancellation() {
    let queue = common::setup_queue().await;
    let test_topic = "test_cancel_topic";

    let message = TestMessage {
        id: "cancel".to_string(),
        content: "cancel content".to_string(),
    };

    // Publish message
    #[cfg(not(feature = "fairness"))]
    let published = queue
        .publish(test_topic, &message, None)
        .await
        .expect("Failed to publish message");
    #[cfg(feature = "fairness")]
    let published = queue
        .publish(test_topic, String::from("job-1"), &message, None)
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

    // Try to consume - should be none
    let result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_message_priority() {
    let queue = common::setup_queue().await;
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

    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &messages[0], Some(options_low))
        .await
        .expect("Failed to publish low priority message");
    #[cfg(feature = "fairness")]
    queue
        .publish(
            test_topic,
            String::from("job-1"),
            &messages[0],
            Some(options_low),
        )
        .await
        .expect("Failed to publish low priority message");

    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &messages[1], Some(options_high))
        .await
        .expect("Failed to publish high priority message");
    #[cfg(feature = "fairness")]
    queue
        .publish(
            test_topic,
            String::from("job-1"),
            &messages[1],
            Some(options_high),
        )
        .await
        .expect("Failed to publish high priority message");

    #[cfg(not(feature = "fairness"))]
    queue
        .publish(test_topic, &messages[2], Some(options_medium))
        .await
        .expect("Failed to publish medium priority message");
    #[cfg(feature = "fairness")]
    queue
        .publish(
            test_topic,
            String::from("job-1"),
            &messages[2],
            Some(options_medium),
        )
        .await
        .expect("Failed to publish medium priority message");

    // Consume messages - they should come in priority order (high to low)
    let first = queue
        .consume::<TestMessage>(test_topic, None)
        .await
        .expect("Failed to consume first message");
    queue
        .acknowledge(test_topic, first.clone())
        .await
        .expect("Failed to acknowledge first message");
    let second = queue
        .consume::<TestMessage>(test_topic, None)
        .await
        .expect("Failed to consume second message");
    queue
        .acknowledge(test_topic, second.clone())
        .await
        .expect("Failed to acknowledge second message");
    let third = queue
        .consume::<TestMessage>(test_topic, None)
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
}
