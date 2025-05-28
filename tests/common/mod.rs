use broccoli_queue::queue::BroccoliQueue;

pub async fn setup_queue() -> BroccoliQueue {
    let queue_url = std::env::var("BROCCOLI_QUEUE_URL").unwrap();

    BroccoliQueue::builder(queue_url)
        .pool_connections(5)
        .enable_scheduling(true)
        .build()
        .await
        .expect("Queue setup failed. Are you sure Redis/RabbitMQ/SurrealDB is running?")
}

#[cfg(feature = "redis")]
pub async fn get_redis_client() -> redis::aio::MultiplexedConnection {
    let queue_url = std::env::var("BROCCOLI_QUEUE_URL").unwrap();
    let client = redis::Client::open(queue_url).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

pub async fn setup_queue_with_url(
    url: &str,
) -> Result<BroccoliQueue, broccoli_queue::error::BroccoliError> {
    BroccoliQueue::builder(url)
        .pool_connections(5)
        .build()
        .await
}
