use broccoli_queue::{
    error::BroccoliError,
    queue::{BroccoliQueue, PublishOptions},
};
use serde::{Deserialize, Serialize};
use time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobPayload {
    id: String,
    task_name: String,
    parameters: Parameters,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Parameters {
    input_data: String,
    priority: u8,
    timeout_seconds: u32,
}

#[tokio::main]
async fn main() -> Result<(), BroccoliError> {
    #[cfg(feature = "redis")]
    let queue_url = "redis://localhost:6380";
    #[cfg(feature = "rabbitmq")]
    let queue_url = "amqp://localhost:5672";

    let queue = BroccoliQueue::builder(queue_url)
        .failed_message_retry_strategy(Default::default())
        .pool_connections(5)
        .enable_scheduling(true)
        .build()
        .await?;

    // Create some example jobs
    let scheduled_jobs = vec![
        JobPayload {
            id: "job-1".to_string(),
            task_name: "process_data".to_string(),
            parameters: Parameters {
                input_data: "data-1".to_string(),
                priority: 1,
                timeout_seconds: 300,
            },
            created_at: chrono::Utc::now(),
        },
        JobPayload {
            id: "job-2".to_string(),
            task_name: "generate_report".to_string(),
            parameters: Parameters {
                input_data: "data-2".to_string(),
                priority: 2,
                timeout_seconds: 600,
            },
            created_at: chrono::Utc::now(),
        },
    ];

    // Publish jobs in batch
    println!("Publishing delayed jobs...");
    #[cfg(not(feature = "fairness"))]
    let scheduled_jobs = queue
        .publish_batch(
            "jobs",
            scheduled_jobs,
            Some(PublishOptions {
                delay: Some(Duration::seconds(10)),
                scheduled_at: None,
                ttl: None,
                priority: None,
            }),
        )
        .await?;
    #[cfg(feature = "fairness")]
    let scheduled_jobs = queue
        .publish_batch(
            "jobs",
            String::from("job-1"),
            scheduled_jobs,
            Some(PublishOptions {
                delay: Some(Duration::seconds(10)),
                scheduled_at: None,
                ttl: None,
                priority: None,
            }),
        )
        .await?;

    println!(
        "Published scheduled jobs: {:?}",
        scheduled_jobs.iter().map(|j| j.task_id).collect::<Vec<_>>()
    );

    let immediate_jobs = vec![
        JobPayload {
            id: "job-3".to_string(),
            task_name: "process_data".to_string(),
            parameters: Parameters {
                input_data: "data-3".to_string(),
                priority: 1,
                timeout_seconds: 300,
            },
            created_at: chrono::Utc::now(),
        },
        JobPayload {
            id: "job-4".to_string(),
            task_name: "generate_report".to_string(),
            parameters: Parameters {
                input_data: "data-4".to_string(),
                priority: 2,
                timeout_seconds: 600,
            },
            created_at: chrono::Utc::now(),
        },
    ];

    // Publish jobs in batch
    println!("Publishing immediate jobs...");
    #[cfg(not(feature = "fairness"))]
    let immediate_jobs = queue.publish_batch("jobs", immediate_jobs, None).await?;
    #[cfg(feature = "fairness")]
    let immediate_jobs = queue
        .publish_batch("jobs", String::from("job-2"), immediate_jobs, None)
        .await?;

    println!(
        "Published immediate jobs: {:?}",
        immediate_jobs.iter().map(|j| j.task_id).collect::<Vec<_>>()
    );

    Ok(())
}
