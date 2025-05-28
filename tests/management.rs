mod common;

use broccoli_queue::brokers::management::QueueType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    id: String,
    content: String,
}

#[tokio::test]
#[cfg(feature = "management")]
async fn test_queue_status_main_queue() {
    let queue = common::setup_queue().await;

    #[cfg(feature = "redis")]
    let mut redis = common::get_redis_client().await;
    let test_topic_main = "test_status_main";

    // Test messages
    let message1 = TestMessage {
        id: "status1".to_string(),
        content: "status content 1".to_string(),
    };
    let message2 = TestMessage {
        id: "status2".to_string(),
        content: "status content 2".to_string(),
    };

    // Test 1: Empty queue status
    let status = queue
        .queue_status(test_topic_main.to_string(), None)
        .await
        .expect("Failed to get queue status");
    assert!(
        status.is_empty(),
        "Status should be empty for non-existent queue"
    );

    // Test 2: Publish messages to main queue and verify status
    #[cfg(not(feature = "test-fairness"))]
    {
        let _published1 = queue
            .publish(test_topic_main, None, &message1, None)
            .await
            .expect("Failed to publish message 1");
        let _published2 = queue
            .publish(test_topic_main, None, &message2, None)
            .await
            .expect("Failed to publish message 2");

        // Get status for specific queue
        let status = queue
            .queue_status(test_topic_main.to_string(), None)
            .await
            .expect("Failed to get queue status");

        assert_eq!(status.len(), 1, "Should have one queue status entry");
        let queue_status = &status[0];
        assert_eq!(queue_status.name, test_topic_main);
        assert_eq!(queue_status.queue_type, QueueType::Main);
        assert_eq!(queue_status.size, 2, "Queue should have 2 messages");
        assert_eq!(
            queue_status.processing, 0,
            "No messages should be processing"
        );
        assert_eq!(queue_status.failed, 0, "No messages should be failed");
        assert_eq!(
            queue_status.disambiguator_count, None,
            "Main queue should not have disambiguator count"
        );
    }

    // Test 3: Get all queue statuses (empty queue name)
    let all_status = queue
        .queue_status("".to_string(), None)
        .await
        .expect("Failed to get all queue statuses");

    #[cfg(not(feature = "test-fairness"))]
    {
        // Find our test queue in the status list
        let our_queue = all_status.iter().find(|s| s.name == test_topic_main);
        assert!(
            our_queue.is_some(),
            "Our test queue should be in the status list"
        );
    }

    #[cfg(feature = "redis")]
    {
        // Cleanup
        #[cfg(not(feature = "test-fairness"))]
        {
            let consume_options = broccoli_queue::queue::ConsumeOptions::default();
            while let Ok(Some(msg)) = queue
                .try_consume::<TestMessage>(test_topic_main, Some(consume_options.clone()))
                .await
            {
                let _ = queue.acknowledge(test_topic_main, msg).await;
            }
        }
    }
}

#[tokio::test]
#[cfg(all(feature = "management", feature = "test-fairness"))]
async fn test_queue_status_fairness_queue() {
    let queue = common::setup_queue().await;
    let test_topic_fairness = "test_status_fairness";

    // Test messages
    let message1 = TestMessage {
        id: "fairness1".to_string(),
        content: "fairness content 1".to_string(),
    };
    let message2 = TestMessage {
        id: "fairness2".to_string(),
        content: "fairness content 2".to_string(),
    };
    let message3 = TestMessage {
        id: "fairness3".to_string(),
        content: "fairness content 3".to_string(),
    };

    // Test 1: Publish messages to fairness queue with different disambiguators
    let _published1 = queue
        .publish(
            test_topic_fairness,
            Some("job-1".to_string()),
            &message1,
            None,
        )
        .await
        .expect("Failed to publish fairness message 1");
    let _published2 = queue
        .publish(
            test_topic_fairness,
            Some("job-2".to_string()),
            &message2,
            None,
        )
        .await
        .expect("Failed to publish fairness message 2");
    let _published3 = queue
        .publish(
            test_topic_fairness,
            Some("job-1".to_string()),
            &message3,
            None,
        )
        .await
        .expect("Failed to publish fairness message 3");

    // Test 2: Get status for fairness queue (all disambiguators)
    let status = queue
        .queue_status(test_topic_fairness.to_string(), None)
        .await
        .expect("Failed to get fairness queue status");

    assert_eq!(
        status.len(),
        1,
        "Should have one fairness queue status entry"
    );
    let queue_status = &status[0];
    assert_eq!(queue_status.name, test_topic_fairness);
    assert_eq!(queue_status.queue_type, QueueType::Fairness);
    assert_eq!(
        queue_status.size, 3,
        "Fairness queue should have 3 messages total"
    );
    assert_eq!(
        queue_status.processing, 0,
        "No messages should be processing"
    );
    assert_eq!(queue_status.failed, 0, "No messages should be failed");
    assert_eq!(
        queue_status.disambiguator_count,
        Some(2),
        "Fairness queue should have 2 disambiguators"
    );

    // Test 3: Get status for specific disambiguator
    let status_job1 = queue
        .queue_status(test_topic_fairness.to_string(), Some("job-1".to_string()))
        .await
        .expect("Failed to get fairness queue status for job-1");

    assert_eq!(
        status_job1.len(),
        1,
        "Should have one queue status entry for job-1"
    );
    let queue_status_job1 = &status_job1[0];
    assert_eq!(queue_status_job1.name, format!("{}_job-1", test_topic_fairness));
    assert_eq!(queue_status_job1.queue_type, QueueType::Fairness);
    assert_eq!(
        queue_status_job1.size, 2,
        "job-1 disambiguator should have 2 messages"
    );
    assert_eq!(
        queue_status_job1.processing, 0,
        "No messages should be processing"
    );
    assert_eq!(queue_status_job1.failed, 0, "No messages should be failed");
    assert_eq!(
        queue_status_job1.disambiguator_count,
        Some(1),
        "Should show count of 1 when filtering by disambiguator"
    );

    // Test 4: Get status for another specific disambiguator
    let status_job2 = queue
        .queue_status(test_topic_fairness.to_string(), Some("job-2".to_string()))
        .await
        .expect("Failed to get fairness queue status for job-2");

    assert_eq!(
        status_job2.len(),
        1,
        "Should have one queue status entry for job-2"
    );
    let queue_status_job2 = &status_job2[0];
    assert_eq!(queue_status_job2.name, format!("{}_job-2", test_topic_fairness));
    assert_eq!(queue_status_job2.queue_type, QueueType::Fairness);
    assert_eq!(
        queue_status_job2.size, 1,
        "job-2 disambiguator should have 1 message"
    );

    // Test 5: Get status for non-existent disambiguator
    let status_nonexistent = queue
        .queue_status(test_topic_fairness.to_string(), Some("job-999".to_string()))
        .await
        .expect("Failed to get fairness queue status for non-existent disambiguator");

    assert!(
        status_nonexistent.is_empty(),
        "Should have no status entries for non-existent disambiguator"
    );

    #[cfg(feature = "redis")]
    {
        // Cleanup
        let consume_options = broccoli_queue::queue::ConsumeOptionsBuilder::new()
            .fairness(true)
            .build();
        while let Ok(Some(msg)) = queue
            .try_consume::<TestMessage>(test_topic_fairness, Some(consume_options.clone()))
            .await
        {
            let _ = queue.acknowledge(test_topic_fairness, msg).await;
        }
    }
}

#[tokio::test]
#[cfg(all(feature = "management", feature = "test-fairness"))]
async fn test_queue_status_processing_and_failed() {
    let queue = common::setup_queue().await;
    let test_topic = "test_status_processing";

    let message = TestMessage {
        id: "processing1".to_string(),
        content: "processing content".to_string(),
    };

    // Publish message
    let _published = queue
        .publish(test_topic, Some("job-1".to_string()), &message, None)
        .await
        .expect("Failed to publish message");

    // Consume message to put it in processing state
    let consume_options = broccoli_queue::queue::ConsumeOptionsBuilder::new()
        .fairness(true)
        .build();
    let consumed = queue
        .consume::<TestMessage>(test_topic, Some(consume_options.clone()))
        .await
        .expect("Failed to consume message");

    // Check processing status
    let status = queue
        .queue_status(test_topic.to_string(), Some("job-1".to_string()))
        .await
        .expect("Failed to get queue status");

    let queue_status = &status[0];
    assert_eq!(
        queue_status.processing, 1,
        "One message should be processing"
    );

    // Reject message to move it to failed queue
    queue
        .reject(test_topic, consumed)
        .await
        .expect("Failed to reject message");

    // Give it time for reject processing
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let status_after_reject = queue
        .queue_status(test_topic.to_string(), Some("job-1".to_string()))
        .await
        .expect("Failed to get queue status after reject");

    let queue_status_after = &status_after_reject[0];
    assert_eq!(
        queue_status_after.processing, 0,
        "No messages should be processing after reject"
    );
    // The message should either be back in queue or in failed queue depending on retry settings

    #[cfg(feature = "redis")]
    {
        // Cleanup
        while let Ok(Some(msg)) = queue
            .try_consume::<TestMessage>(test_topic, Some(consume_options.clone()))
            .await
        {
            let _ = queue.acknowledge(test_topic, msg).await;
        }
    }
}

#[tokio::test]
#[cfg(feature = "management")]
async fn test_queue_status_pattern_matching() {
    let queue = common::setup_queue().await;
    let base_name = "test_pattern";

    let message = TestMessage {
        id: "pattern1".to_string(),
        content: "pattern content".to_string(),
    };

    // Create multiple queues with similar names
    #[cfg(not(feature = "test-fairness"))]
    {
        let _pub1 = queue
            .publish(&format!("{}_queue1", base_name), None, &message, None)
            .await
            .expect("Failed to publish to queue1");
        let _pub2 = queue
            .publish(&format!("{}_queue2", base_name), None, &message, None)
            .await
            .expect("Failed to publish to queue2");
    }

    #[cfg(feature = "test-fairness")]
    {
        let _pub1 = queue
            .publish(&format!("{}_queue1", base_name), Some("job-1".to_string()), &message, None)
            .await
            .expect("Failed to publish to queue1");
        let _pub2 = queue
            .publish(&format!("{}_queue2", base_name), Some("job-1".to_string()), &message, None)
            .await
            .expect("Failed to publish to queue2");
    }

    // Test pattern matching with base name
    let status = queue
        .queue_status(base_name.to_string(), None)
        .await
        .expect("Failed to get pattern matched queue status");

    assert!(
        status.len() >= 1,
        "Should find at least one queue matching the pattern"
    );

    // Test exact queue name match
    let exact_status = queue
        .queue_status(format!("{}_queue1", base_name), None)
        .await
        .expect("Failed to get exact queue status");

    assert!(
        exact_status.len() >= 1,
        "Should find the exact queue"
    );

    #[cfg(feature = "redis")]
    {
        // Cleanup
        #[cfg(not(feature = "test-fairness"))]
        {
            let consume_options = broccoli_queue::queue::ConsumeOptions::default();
            for queue_name in [&format!("{}_queue1", base_name), &format!("{}_queue2", base_name)] {
                while let Ok(Some(msg)) = queue
                    .try_consume::<TestMessage>(queue_name, Some(consume_options.clone()))
                    .await
                {
                    let _ = queue.acknowledge(queue_name, msg).await;
                }
            }
        }
        
        #[cfg(feature = "test-fairness")]
        {
            let consume_options = broccoli_queue::queue::ConsumeOptionsBuilder::new()
                .fairness(true)
                .build();
            for queue_name in [&format!("{}_queue1", base_name), &format!("{}_queue2", base_name)] {
                while let Ok(Some(msg)) = queue
                    .try_consume::<TestMessage>(queue_name, Some(consume_options.clone()))
                    .await
                {
                    let _ = queue.acknowledge(queue_name, msg).await;
                }
            }
        }
    }
}
