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

    let result = queue.publish(test_topic, &empty_message, None).await;
    assert!(result.is_ok());

    let consumed = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert_eq!(consumed.payload, empty_message);
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

    let result = queue.publish(test_topic, &large_message, None).await;
    assert!(result.is_ok());

    let consumed = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert_eq!(consumed.payload.content.len(), large_content.len());
}

#[tokio::test]
async fn test_concurrent_consume() {
    let queue = common::setup_queue().await;
    let test_topic = "test_concurrent_topic";

    // Publish multiple messages
    let messages: Vec<_> = (0..10)
        .map(|i| TestMessage {
            id: i.to_string(),
            content: format!("content {}", i),
        })
        .collect();

    queue
        .publish_batch(test_topic, messages, None)
        .await
        .unwrap();

    // Consume concurrently
    let mut handles = vec![];
    for _ in 0..5 {
        let queue_clone = queue.clone();
        let topic = test_topic.to_string();
        handles.push(tokio::spawn(async move {
            let msg = queue_clone
                .consume::<TestMessage>(&topic, Default::default())
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

#[tokio::test]
async fn test_zero_ttl() {
    let queue = common::setup_queue().await;
    let test_topic = "test_zero_ttl_topic";

    let message = TestMessage {
        id: "zero_ttl".to_string(),
        content: "expires immediately".to_string(),
    };

    let options = PublishOptions::builder().ttl(Duration::seconds(0)).build();

    queue
        .publish(test_topic, &message, Some(options))
        .await
        .unwrap();

    // Message should not be available
    let result = queue
        .try_consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
#[cfg(any(feature = "redis", feature = "rabbitmq-delay"))]
async fn test_message_ordering() {
    let queue = common::setup_queue().await;
    let test_topic = "test_ordering_topic";

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
        queue.publish(test_topic, &msg, opt).await.unwrap();
    }

    // Consume messages
    let third = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    queue.acknowledge(test_topic, third.clone()).await.unwrap();
    assert_eq!(third.payload.id, "3");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let second = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    queue.acknowledge(test_topic, second.clone()).await.unwrap();
    let first = queue
        .consume::<TestMessage>(test_topic, Default::default())
        .await
        .unwrap();
    queue.acknowledge(test_topic, first.clone()).await.unwrap();

    assert_eq!(second.payload.id, "2");
    assert_eq!(first.payload.id, "1");
}
