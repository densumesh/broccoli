use broccoli_queue::queue::ConsumeOptionsBuilder;

#[cfg(any(
    feature = "redis",
    all(feature = "rabbitmq", feature = "rabbitmq-delay")
))]
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
    let published = queue
        .publish(test_topic, &message, None)
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
    let published = queue
        .publish_batch(test_topic, messages.clone(), None)
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
#[cfg(any(feature = "redis", feature = "rabbitmq-delay"))]
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

    queue
        .publish(test_topic, &message, Some(options))
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
#[cfg(any(feature = "redis", feature = "rabbitmq-delay"))]
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

    let published = queue
        .publish(test_topic, &message, Some(options))
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
    queue
        .publish(test_topic, &message, None)
        .await
        .expect("Failed to publish message");

    // Simulate failed processing 3 times
    for _ in 0..3 {
        let consumed = queue
            .consume::<TestMessage>(test_topic, Default::default())
            .await
            .expect("Failed to consume message");

        // Reject the message
        queue
            .reject(test_topic, consumed)
            .await
            .expect("Failed to reject message");
    }

    // // Try to consume again - should be in failed queue
    let result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert!(result.is_none());

    // Check failed queue
    let failed_result = queue
        .try_consume::<TestMessage>(&format!("{}_failed", test_topic), Default::default())
        .await
        .unwrap();
    assert!(failed_result.is_some());
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
    queue
        .publish(test_topic, &message, None)
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
    queue
        .publish(test_topic, &message, None)
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
    let published = queue
        .publish(test_topic, &message, None)
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
async fn test_queue_position() {
    let queue = common::setup_queue().await;
    let test_topic = "test_position_topic";

    // Publish multiple messages
    let messages = vec![
        TestMessage {
            id: "1".to_string(),
            content: "first".to_string(),
        },
        TestMessage {
            id: "2".to_string(),
            content: "second".to_string(),
        },
        TestMessage {
            id: "3".to_string(),
            content: "third".to_string(),
        },
    ];

    let published = queue
        .publish_batch(test_topic, messages, None)
        .await
        .expect("Failed to publish batch");

    // Check position of second message
    let position_result = queue
        .get_message_position(test_topic, published[1].task_id.to_string())
        .await;

    let position = match position_result {
        Ok(Some(pos)) => pos,
        Ok(None) => {
            panic!("Message not found");
        }
        Err(e) if e.to_string().contains("NotImplemented") => {
            println!("This feature is not implemented for this broker");
            return;
        }
        Err(e) => {
            panic!("Failed to get message position: {:?}", e);
        }
    };

    assert_eq!(position, 1);
}
