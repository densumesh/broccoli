use broccoli_queue::queue::BroccoliQueue;

pub async fn setup_queue() -> BroccoliQueue {
    let queue_url = std::env::var("BROCCOLI_QUEUE_URL").unwrap();
    BroccoliQueue::builder(queue_url)
        .pool_connections(5)
        .enable_scheduling(true)
        .build()
        .await
        .expect("Queue setup failed. Are you sure Redis/RabbitMQ is running?")
}

pub async fn setup_fair_queue() -> BroccoliQueue {
    let queue_url = std::env::var("BROCCOLI_QUEUE_URL").unwrap();
    BroccoliQueue::builder(queue_url)
        .pool_connections(5)
        .enable_scheduling(true)
        .enable_fairness(true)
        .build()
        .await
        .expect("Queue setup failed. Are you sure Redis/RabbitMQ is running?")
}

pub async fn setup_queue_with_url(
    url: &str,
) -> Result<BroccoliQueue, broccoli_queue::error::BroccoliError> {
    BroccoliQueue::builder(url)
        .pool_connections(5)
        .build()
        .await
}
