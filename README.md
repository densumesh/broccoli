# Broccoli
[![Crates.io](https://img.shields.io/crates/v/broccoli_queue)](https://crates.io/crates/broccoli_queue)
[![Docs.rs](https://docs.rs/broccoli_queue/badge.svg)](https://docs.rs/broccoli_queue)
[![License](https://img.shields.io/crates/l/broccoli_queue)](https://github.com/densumesh/broccoli/blob/main/LICENSE)

A robust message queue system for Rust applications, designed as a Rust alternative to [Celery](https://docs.celeryq.dev/en/stable/getting-started/introduction.html). Currently Redis-backed, with planned support for RabbitMQ, Kafka, and other message brokers. This is by no means a finished product, but we're actively working on it and would love your feedback and contributions!

## Features
- Asynchronous message processing
- Redis-backed persistence (with RabbitMQ and Kafka support planned)
- Configurable retry strategies
- Connection pooling
- Type-safe message handling
- Error handling with custom handlers
- Flexible message processing patterns

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
broccoli_queue = "0.1.0"
```

## Quick Start

### Producer
```rust
use broccoli_queue::{queue::BroccoliQueue, brokers::broker::BrokerMessage};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobPayload {
    id: String,
    task_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the queue
    let queue = BroccoliQueue::builder("redis://localhost:6379")
        .pool_connections(5) // Optional: Number of connections to pool
        .failed_message_retry_strategy(Default::default()) // Optional: Retry strategy (max retries, etc)
        .build()
        .await?;

    // Create some example jobs
    let jobs = vec![
        JobPayload {
            id: "job-1".to_string(),
            task_name: "process_data".to_string(),
        },
        JobPayload {
            id: "job-2".to_string(),
            task_name: "generate_report".to_string(),
        },
    ];

    // Publish jobs in batch
    queue.publish_batch(
        "jobs", // Queue name
         jobs // Jobs to publish
    ).await?;

    Ok(())
}
```

### Consumer
```rust
use broccoli_queue::{queue::BroccoliQueue, brokers::broker::BrokerMessage};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobPayload {
    id: String,
    task_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the queue
    let queue = BroccoliQueue::builder("redis://localhost:6379")
        .pool_connections(5)
        .failed_message_retry_strategy(Default::default()) // Optional: Retry strategy (max retries, etc)
        .build()
        .await?;

    // Process messages
    queue.process_messages(
        "jobs", // Queue name
        Some(2), // Optional: Number of worker threads to spin up
        |message: BrokerMessage<JobPayload>| async move { // Message handler
            println!("Processing job: {:?}", message);
            Ok(())
        },
    ).await?;

    Ok(())
}
```

## Why Broccoli?

If you're familiar with Celery but want to leverage Rust's performance and type safety, Broccoli is your answer. While currently focused on Redis as the message broker, we're actively working on supporting multiple brokers to provide the same flexibility as Celery:

- Redis (Current)
- RabbitMQ (Planned)
- Kafka (Planned)
- And more to come!

This multi-broker approach will allow you to choose the message broker that best fits your needs while maintaining a consistent API.

## Documentation

- [API Documentation](https://docs.rs/broccoli_queue)

## Roadmap

- [x] Redis broker implementation
- [ ] RabbitMQ broker support
- [ ] Kafka broker support
- [ ] Proc macros for building workers
- [ ] Additional message patterns
- [ ] Enhanced monitoring and metrics
- [ ] Web interface for job monitoring

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. We're particularly interested in contributions for:

- New broker implementations
- Additional message patterns
- Performance improvements
- Documentation enhancements
- Bug fixes
