mod common;

#[cfg(feature = "management")]
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
    let redis = common::get_redis_client().await;
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

    // Test 1: Non-existent queue should return empty status (size 0)
    let status = queue
        .queue_status(test_topic_main.to_string(), None)
        .await
        .expect("Failed to get queue status");
    
    assert_eq!(status.name, test_topic_main);
    assert_eq!(status.queue_type, QueueType::Main);
    assert_eq!(status.size, 0, "Non-existent queue should have size 0");
    assert_eq!(status.processing, 0, "Non-existent queue should have 0 processing");
    assert_eq!(status.failed, 0, "Non-existent queue should have 0 failed");

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

        assert_eq!(status.name, test_topic_main);
        assert_eq!(status.queue_type, QueueType::Main);
        assert_eq!(status.size, 2, "Queue should have 2 messages");
        assert_eq!(
            status.processing, 0,
            "No messages should be processing"
        );
        assert_eq!(status.failed, 0, "No messages should be failed");
        assert_eq!(
            status.disambiguator_count, None,
            "Main queue should not have disambiguator count"
        );
    }

    // Test 3: Verify empty queue name returns error
    let empty_result = queue
        .queue_status("".to_string(), None)
        .await;
    assert!(
        empty_result.is_err(),
        "Empty queue name should return an error to prevent scanning all Redis keys"
    );

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

    assert_eq!(status.name, test_topic_fairness);
    assert_eq!(status.queue_type, QueueType::Fairness);
    assert_eq!(
        status.size, 3,
        "Fairness queue should have 3 messages total"
    );
    assert_eq!(
        status.processing, 0,
        "No messages should be processing"
    );
    assert_eq!(status.failed, 0, "No messages should be failed");
    assert_eq!(
        status.disambiguator_count,
        Some(2),
        "Fairness queue should have 2 disambiguators"
    );

    // Test 3: Get status for specific disambiguator
    let status_job1 = queue
        .queue_status(test_topic_fairness.to_string(), Some("job-1".to_string()))
        .await
        .expect("Failed to get fairness queue status for job-1");

    assert_eq!(status_job1.name, format!("{}_job-1", test_topic_fairness));
    assert_eq!(status_job1.queue_type, QueueType::Fairness);
    assert_eq!(
        status_job1.size, 2,
        "job-1 disambiguator should have 2 messages"
    );
    assert_eq!(
        status_job1.processing, 0,
        "No messages should be processing"
    );
    assert_eq!(status_job1.failed, 0, "No messages should be failed");
    assert_eq!(
        status_job1.disambiguator_count,
        Some(1),
        "Should show count of 1 when filtering by disambiguator"
    );

    // Test 4: Get status for another specific disambiguator
    let status_job2 = queue
        .queue_status(test_topic_fairness.to_string(), Some("job-2".to_string()))
        .await
        .expect("Failed to get fairness queue status for job-2");

    assert_eq!(status_job2.name, format!("{}_job-2", test_topic_fairness));
    assert_eq!(status_job2.queue_type, QueueType::Fairness);
    assert_eq!(
        status_job2.size, 1,
        "job-2 disambiguator should have 1 message"
    );

    // Test 5: Get status for non-existent disambiguator
    let status_nonexistent = queue
        .queue_status(test_topic_fairness.to_string(), Some("job-999".to_string()))
        .await
        .expect("Failed to get fairness queue status for non-existent disambiguator");

    assert_eq!(status_nonexistent.name, format!("{}_job-999", test_topic_fairness));
    assert_eq!(status_nonexistent.queue_type, QueueType::Fairness);
    assert_eq!(status_nonexistent.size, 0, "Non-existent disambiguator should have size 0");
    assert_eq!(status_nonexistent.processing, 0, "Non-existent disambiguator should have 0 processing");
    assert_eq!(status_nonexistent.failed, 0, "Non-existent disambiguator should have 0 failed");

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

    assert_eq!(
        status.processing, 1,
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

    assert_eq!(
        status_after_reject.processing, 0,
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
async fn test_queue_status_specific_queue_lookup() {
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

    // Test exact queue name match for queue1
    let exact_status1 = queue
        .queue_status(format!("{}_queue1", base_name), None)
        .await
        .expect("Failed to get exact queue status");

    assert_eq!(exact_status1.name, format!("{}_queue1", base_name));
    assert!(exact_status1.size > 0, "Queue1 should have messages");

    // Test exact queue name match for queue2
    let exact_status2 = queue
        .queue_status(format!("{}_queue2", base_name), None)
        .await
        .expect("Failed to get exact queue status");

    assert_eq!(exact_status2.name, format!("{}_queue2", base_name));
    assert!(exact_status2.size > 0, "Queue2 should have messages");

    // Test non-existent queue returns empty status
    let nonexistent_status = queue
        .queue_status(format!("{}_nonexistent", base_name), None)
        .await
        .expect("Failed to get non-existent queue status");

    assert_eq!(nonexistent_status.name, format!("{}_nonexistent", base_name));
    assert_eq!(nonexistent_status.size, 0, "Non-existent queue should have size 0");

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

#[tokio::test]
#[cfg(feature = "management")]
async fn test_queue_status_empty_name_error() {
    let queue = common::setup_queue().await;

    // Test that empty queue name returns an error to prevent dangerous KEYS * operations
    let result = queue.queue_status("".to_string(), None).await;
    
    assert!(result.is_err(), "Empty queue name should return an error");
    
    if let Err(e) = result {
        let error_message = e.to_string();
        assert!(
            error_message.contains("Queue name cannot be empty") || 
            error_message.contains("avoid scanning all Redis keys"),
            "Error should mention avoiding Redis key scanning, got: {}", 
            error_message
        );
    }
}
