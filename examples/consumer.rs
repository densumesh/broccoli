use broccoli_queue::{error::BroccoliError, queue::BroccoliQueue};
use serde::{Deserialize, Serialize};
use std::{error::Error, time::Duration};

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

async fn process_job(job: JobPayload) -> Result<(), BroccoliError> {
    println!("Processing job: {} ({})", job.id, job.task_name);

    // Simulate some work
    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}

async fn success_handler(msg: JobPayload) -> Result<(), BroccoliError> {
    println!("Successfully processed message: {:?}", msg);
    Ok(())
}

async fn error_handler(msg: JobPayload, err: BroccoliError) -> Result<(), BroccoliError> {
    eprintln!("Failed to process message: {}. Error: {}", msg.id, err);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    #[cfg(feature = "redis")]
    let queue_url = "redis://localhost:6380";
    #[cfg(feature = "rabbitmq")]
    let queue_url = "amqp://localhost:5672";

    // Initialize the queue
    let queue = BroccoliQueue::builder(queue_url)
        .pool_connections(5)
        .failed_message_retry_strategy(Default::default())
        .enable_scheduling(true)
        .build()
        .await?;

    // Process regular jobs
    queue
        .process_messages("jobs", Some(4), |msg| async move {
            process_job(msg.payload).await
        })
        .await
        .unwrap();

    queue
        .process_messages_with_handlers(
            "jobs",
            Some(5),
            |msg| async move { process_job(msg.payload).await },
            |msg| async { success_handler(msg.payload).await },
            |msg, err| async { error_handler(msg.payload, err).await },
        )
        .await?;

    println!("Consumer shutdown complete");
    Ok(())
}
