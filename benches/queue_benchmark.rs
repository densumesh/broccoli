use broccoli_queue::queue::BroccoliQueue;
use criterion::{criterion_group, criterion_main, Criterion};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMessage {
    id: String,
    data: String,
    timestamp: i64,
}

async fn setup_redis() -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open("redis://localhost:6380").unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

async fn setup_broccoli() -> BroccoliQueue {
    BroccoliQueue::builder("redis://localhost:6380")
        .pool_connections(10)
        .build()
        .await
        .unwrap()
}

async fn benchmark_raw_redis_throughput(
    conn: &mut redis::aio::MultiplexedConnection,
    message_count: usize,
) -> (f64, f64) {
    let queue_name = "bench_raw_redis";
    let now = Instant::now();

    // Generate test messages
    let messages: Vec<BenchmarkMessage> = (0..message_count)
        .map(|i| BenchmarkMessage {
            id: i.to_string(),
            data: format!("test data {}", i),
            timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
        })
        .collect();

    // Publish messages
    for msg in &messages {
        let _: () = conn
            .lpush(queue_name, serde_json::to_string(msg).unwrap())
            .await
            .unwrap();
    }

    // Consume messages
    for _ in 0..message_count {
        // Set up the queue with a processing queue
        let string_msg: String = conn
            .brpoplpush(queue_name, format!("{}_processing", queue_name), 1.0)
            .await
            .unwrap();
        // Deserialize the message
        let _: BenchmarkMessage = serde_json::from_str(&string_msg).unwrap();
        // Acknowledge the message
        let _: () = conn
            .lrem(format!("{}_processing", queue_name), 1, &string_msg)
            .await
            .unwrap();
    }

    let total_time = now.elapsed().as_secs_f64();
    let throughput = message_count as f64 / total_time;
    let avg_latency = total_time / message_count as f64;

    (throughput, avg_latency)
}

async fn benchmark_broccoli_throughput(queue: &BroccoliQueue, message_count: usize) -> (f64, f64) {
    let queue_name = "bench_broccoli";
    let now = Instant::now();

    // Generate test messages
    let messages: Vec<BenchmarkMessage> = (0..message_count)
        .map(|i| BenchmarkMessage {
            id: i.to_string(),
            data: format!("test data {}", i),
            timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
        })
        .collect();

    // Publish messages
    for msg in &messages {
        #[cfg(not(feature = "fairness"))]
        queue.publish(queue_name, msg, None).await.unwrap();
        #[cfg(feature = "fairness")]
        queue
            .publish(queue_name, String::from("job-1"), msg, None)
            .await
            .unwrap();
    }

    // Consume messages
    for _ in 0..message_count {
        let msg = queue
            .consume::<BenchmarkMessage>(queue_name, None)
            .await
            .unwrap();
        queue.acknowledge(queue_name, msg).await.unwrap();
    }

    let total_time = now.elapsed().as_secs_f64();
    let throughput = message_count as f64 / total_time;
    let avg_latency = total_time / message_count as f64;

    (throughput, avg_latency)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("Queue Performance");
    let message_counts = [100, 1000, 10000];

    let mut redis_conn = rt.block_on(setup_redis());
    let broccoli_queue = rt.block_on(setup_broccoli());

    for &count in &message_counts {
        group.bench_function(format!("Raw Redis {}", count), |b| {
            b.iter(|| {
                rt.block_on(async { benchmark_raw_redis_throughput(&mut redis_conn, count).await })
            })
        });

        group.bench_function(format!("Broccoli {}", count), |b| {
            b.iter(|| {
                rt.block_on(async { benchmark_broccoli_throughput(&broccoli_queue, count).await })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
