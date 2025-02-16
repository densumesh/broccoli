use broccoli_queue::queue::ConsumeOptionsBuilder;
#[cfg(all(feature = "redis", feature = "test-fairness"))]
use broccoli_queue::queue::PublishOptions;
#[cfg(all(feature = "redis", feature = "test-fairness"))]
use redis::AsyncCommands;
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
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
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

    #[cfg(feature = "redis")]
    {
        // Verify fairness data structures
        let set_exists: bool = redis
            .exists(format!("{}_fairness_set", test_topic))
            .await
            .unwrap();
        assert!(set_exists, "Fairness set should exist");

        let round_robin_exists: bool = redis
            .exists(format!("{}_fairness_round_robin", test_topic))
            .await
            .unwrap();
        assert!(round_robin_exists, "Round robin list should exist");

        // Verify job queues
        for job_id in ["job-1", "job-2", "job-3"] {
            let queue_exists: bool = redis
                .exists(format!("{}_{}_queue", test_topic, job_id))
                .await
                .unwrap();
            assert!(queue_exists, "Job queue for {} should exist", job_id);
        }
    }

    // Consume messages - they should come in round-robin order
    let mut consumed_messages = Vec::new();
    for _ in 0..6 {
        #[cfg(not(feature = "test-fairness"))]
        let consume_options = ConsumeOptions::default();
        #[cfg(feature = "test-fairness")]
        let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

        let msg = queue
            .consume::<TestMessage>(test_topic, Some(consume_options))
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
    let queue = common::setup_queue().await;
    let test_topic = "test_fairness_priority_topic";
    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

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
        let published = queue
            .publish(
                test_topic,
                Some(String::from(job_id)),
                &message,
                Some(options),
            )
            .await
            .expect("Failed to publish message");

        #[cfg(feature = "redis")]
        {
            // Verify message metadata includes priority
            let priority_stored: String = redis
                .hget(published.task_id.to_string(), "priority")
                .await
                .unwrap();
            assert_eq!(
                priority_stored,
                priority.to_string(),
                "Priority should be stored correctly"
            );
        }
    }

    // Consume messages - they should respect both fairness and priority
    let mut consumed_messages = Vec::new();
    for _ in 0..4 {
        #[cfg(not(feature = "test-fairness"))]
        let consume_options = ConsumeOptions::default();
        #[cfg(feature = "test-fairness")]
        let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

        let msg = queue
            .consume::<TestMessage>(test_topic, Some(consume_options))
            .await
            .expect("Failed to consume message");
        consumed_messages.push((msg.payload.id.clone(), msg.payload.content.clone()));
        queue
            .acknowledge(test_topic, msg)
            .await
            .expect("Failed to acknowledge message");
    }

    #[cfg(feature = "redis")]
    {
        // Verify queues are empty after consumption
        for job_id in ["job-1", "job-2"] {
            let queue_name = format!("{}_{}_queue", test_topic, job_id);
            let queue_len: usize = redis.zcard(&queue_name).await.unwrap();
            assert_eq!(queue_len, 0, "Queue should be empty after consumption");

            let processing_queue = format!("{}_{}_processing", test_topic, job_id);
            let proc_len: usize = redis.llen(&processing_queue).await.unwrap();
            assert_eq!(proc_len, 0, "Processing queue should be empty");
        }
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
    let queue = common::setup_queue().await;
    let test_topic = "test_fairness_delay_topic";
    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

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

    #[cfg(feature = "redis")]
    {
        // Verify initial state
        let queue_name = format!("{}_job-1_queue", test_topic);
        let queue_len: usize = redis.zcard(&queue_name).await.unwrap();
        assert_eq!(queue_len, 1, "Queue should have one message");

        let queue_name_2 = format!("{}_job-2_queue", test_topic);
        let queue_len_2: usize = redis.zcard(&queue_name_2).await.unwrap();
        assert_eq!(queue_len_2, 1, "Queue should have one message");

        // Verify message scores (delayed message should have higher score)
        let scores_1: Vec<(String, f64)> = redis
            .zrangebyscore_withscores(&queue_name, "-inf", "+inf")
            .await
            .unwrap();
        let scores_2: Vec<(String, f64)> = redis
            .zrangebyscore_withscores(&queue_name_2, "-inf", "+inf")
            .await
            .unwrap();

        assert!(
            scores_2[0].1 > scores_1[0].1,
            "Delayed message should have higher score"
        );
    }

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Consume immediate message
    let first = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
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
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume second message");
    assert_eq!(second.payload.content, "delayed from job 2");
    queue
        .acknowledge(test_topic, second)
        .await
        .expect("Failed to acknowledge second message");

    #[cfg(feature = "redis")]
    {
        // Verify final state
        for job_id in ["job-1", "job-2"] {
            let queue_name = format!("{}_{}_queue", test_topic, job_id);
            let queue_len: usize = redis.zcard(&queue_name).await.unwrap();
            assert_eq!(queue_len, 0, "Queue should be empty after consumption");

            let processing_queue = format!("{}_{}_processing", test_topic, job_id);
            let proc_len: usize = redis.llen(&processing_queue).await.unwrap();
            assert_eq!(proc_len, 0, "Processing queue should be empty");
        }
    }
}

#[tokio::test]
#[cfg(all(feature = "redis", feature = "test-fairness"))]
async fn test_fairness_with_retries() {
    let queue = common::setup_queue().await;
    let test_topic = "test_fairness_retry_topic";
    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;

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

    #[cfg(not(feature = "test-fairness"))]
    let consume_options = ConsumeOptions::default();
    #[cfg(feature = "test-fairness")]
    let consume_options = ConsumeOptionsBuilder::new().fairness(true).build();

    // Consume and reject messages
    for _ in 0..2 {
        for _ in 0..3 {
            let msg = queue
                .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
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
        .try_consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to try consume");
    assert!(result.is_none());

    #[cfg(feature = "redis")]
    {
        for job_id in ["job-1", "job-2"] {
            // Verify messages moved to failed queues
            let failed_queue = format!("{}_{}_failed", test_topic, job_id);
            let failed_len: usize = redis.llen(&failed_queue).await.unwrap();
            assert_eq!(
                failed_len, 1,
                "Failed queue for {} should have one message",
                job_id
            );

            // Verify original queues are empty
            let queue_name = format!("{}_{}_queue", test_topic, job_id);
            let queue_len: usize = redis.zcard(&queue_name).await.unwrap();
            assert_eq!(queue_len, 0, "Queue should be empty after retries");

            // Verify processing queues are empty
            let processing_queue = format!("{}_{}_processing", test_topic, job_id);
            let proc_len: usize = redis.llen(&processing_queue).await.unwrap();
            assert_eq!(proc_len, 0, "Processing queue should be empty");
        }
    }
}
