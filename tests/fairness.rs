#[cfg(all(feature = "redis", feature = "test-fairness"))]
use broccoli_queue::queue::PublishOptions;
#[cfg(all(feature = "redis", feature = "test-fairness"))]
use serde::{Deserialize, Serialize};
#[cfg(all(feature = "redis", feature = "test-fairness"))]
use time::Duration;

mod common;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg(all(feature = "redis", feature = "test-fairness"))]
struct TestMessage {
    id: String,
    content: String,
}

#[tokio::test]
#[cfg(all(feature = "redis", feature = "test-fairness"))]
async fn test_fairness_round_robin() {
    let queue = common::setup_fair_queue().await;
    let test_topic = "test_fairness_topic";

    // Publish messages from different jobs
    let messages = vec![
        ("job-1", "message 1 from job 1"),
        ("job-1", "message 2 from job 1"),
        ("job-2", "message 1 from job 2"),
        ("job-2", "message 2 from job 2"),
        ("job-3", "message 1 from job 3"),
        ("job-3", "message 2 from job 3"),
    ];

    for (job_id, content) in messages {
        let message = TestMessage {
            id: job_id.to_string(),
            content: content.to_string(),
        };
        queue
            .publish(test_topic, Some(String::from(job_id)), &message, None)
            .await
            .expect("Failed to publish message");
    }

    // Consume messages - they should come in round-robin order
    let mut consumed_messages = Vec::new();
    for _ in 0..6 {
        let msg = queue
            .consume::<TestMessage>(test_topic, None)
            .await
            .expect("Failed to consume message");
        consumed_messages.push((msg.payload.id.clone(), msg.payload.content.clone()));
        queue
            .acknowledge(test_topic, msg)
            .await
            .expect("Failed to acknowledge message");
    }

    // Verify round-robin order (one from each job before repeating)
    assert_eq!(consumed_messages[0].0, "job-1");
    assert_eq!(consumed_messages[1].0, "job-2");
    assert_eq!(consumed_messages[2].0, "job-3");
    assert_eq!(consumed_messages[3].0, "job-1");
    assert_eq!(consumed_messages[4].0, "job-2");
    assert_eq!(consumed_messages[5].0, "job-3");
}

#[tokio::test]
#[cfg(all(feature = "redis", feature = "test-fairness"))]
async fn test_fairness_with_priorities() {
    let queue = common::setup_fair_queue().await;
    let test_topic = "test_fairness_priority_topic";

    // Publish messages with different priorities from different jobs
    let messages = vec![
        ("job-1", 5, "low priority from job 1"),
        ("job-2", 1, "high priority from job 2"),
        ("job-1", 3, "medium priority from job 1"),
        ("job-2", 3, "medium priority from job 2"),
    ];

    for (job_id, priority, content) in messages {
        let message = TestMessage {
            id: job_id.to_string(),
            content: content.to_string(),
        };
        let options = PublishOptions::builder().priority(priority).build();
        queue
            .publish(
                test_topic,
                Some(String::from(job_id)),
                &message,
                Some(options),
            )
            .await
            .expect("Failed to publish message");
    }

    // Consume messages - they should respect both fairness and priority
    let mut consumed_messages = Vec::new();
    for _ in 0..4 {
        let msg = queue
            .consume::<TestMessage>(test_topic, None)
            .await
            .expect("Failed to consume message");
        consumed_messages.push((msg.payload.id.clone(), msg.payload.content.clone()));
        queue
            .acknowledge(test_topic, msg)
            .await
            .expect("Failed to acknowledge message");
    }

    // Verify that high priority messages come first but still alternate between jobs
    assert!(
        consumed_messages[0].1.contains("high priority from job 2")
            || consumed_messages[1].1.contains("high priority from job 2")
    );
    assert!(
        consumed_messages[2].1.contains("medium priority")
            && consumed_messages[3].1.contains("low priority")
            || consumed_messages[2].1.contains("low priority")
                && consumed_messages[3].1.contains("medium priority")
    );
}

#[tokio::test]
#[cfg(all(feature = "redis", feature = "test-fairness"))]
async fn test_fairness_with_delayed_messages() {
    let queue = common::setup_fair_queue().await;
    let test_topic = "test_fairness_delay_topic";

    // Publish immediate and delayed messages from different jobs
    let immediate_msg = TestMessage {
        id: "job-1".to_string(),
        content: "immediate from job 1".to_string(),
    };
    queue
        .publish(
            test_topic,
            Some(String::from("job-1")),
            &immediate_msg,
            None,
        )
        .await
        .expect("Failed to publish immediate message");

    let delayed_msg = TestMessage {
        id: "job-2".to_string(),
        content: "delayed from job 2".to_string(),
    };
    let options = PublishOptions::builder()
        .delay(Duration::seconds(2))
        .build();
    queue
        .publish(
            test_topic,
            Some(String::from("job-2")),
            &delayed_msg,
            Some(options),
        )
        .await
        .expect("Failed to publish delayed message");

    // Consume immediate message
    let first = queue
        .consume::<TestMessage>(test_topic, None)
        .await
        .expect("Failed to consume first message");
    assert_eq!(first.payload.content, "immediate from job 1");
    queue
        .acknowledge(test_topic, first)
        .await
        .expect("Failed to acknowledge first message");

    // Wait for delayed message
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Consume delayed message
    let second = queue
        .consume::<TestMessage>(test_topic, None)
        .await
        .expect("Failed to consume second message");
    assert_eq!(second.payload.content, "delayed from job 2");
    queue
        .acknowledge(test_topic, second)
        .await
        .expect("Failed to acknowledge second message");
}

#[tokio::test]
#[cfg(all(feature = "redis", feature = "test-fairness"))]
async fn test_fairness_with_retries() {
    let queue = common::setup_fair_queue().await;
    let test_topic = "test_fairness_retry_topic";

    // Publish messages from different jobs
    let messages = vec![
        ("job-1", "message from job 1"),
        ("job-2", "message from job 2"),
    ];

    for (job_id, content) in messages {
        let message = TestMessage {
            id: job_id.to_string(),
            content: content.to_string(),
        };
        queue
            .publish(test_topic, Some(String::from(job_id)), &message, None)
            .await
            .expect("Failed to publish message");
    }

    // Consume and reject messages
    for _ in 0..2 {
        for _ in 0..3 {
            let msg = queue
                .consume::<TestMessage>(test_topic, None)
                .await
                .expect("Failed to consume message");
            queue
                .reject(test_topic, msg)
                .await
                .expect("Failed to reject message");
        }
    }

    // Verify messages are in failed queue
    let result = queue
        .try_consume::<TestMessage>(test_topic, None)
        .await
        .expect("Failed to try consume");
    assert!(result.is_none());
}
