use broccoli_queue::queue::BroccoliQueue;
use criterion::{criterion_group, criterion_main, Criterion};
use lapin::{options::*, BasicProperties, Connection, ConnectionProperties, Result};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMessage {
    id: String,
    data: String,
    timestamp: i64,
}
const ADDR: &str = "amqp://localhost:5672/";

async fn setup_amqp() -> Result<Connection> {
    Connection::connect(ADDR, ConnectionProperties::default()).await
}

async fn setup_broccoli() -> BroccoliQueue {
    BroccoliQueue::builder(ADDR)
        .pool_connections(10)
        .build()
        .await
        .unwrap()
}

async fn benchmark_raw_amqp_throughput(conn: &mut Connection, message_count: usize) -> (f64, f64) {
    let queue_name = "bench_raw_amqp";
    let now = Instant::now();

    // Generate test messages
    let messages: Vec<BenchmarkMessage> = (0..message_count)
        .map(|i| BenchmarkMessage {
            id: i.to_string(),
            data: format!("test data {}", i),
            timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
        })
        .collect();
    let ch = conn.create_channel().await.unwrap();
    // Publish messages
    for msg in &messages {
        let payload = serde_json::to_vec(msg).unwrap();
        ch.basic_publish(
            "",
            queue_name,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await
        .expect("publish failed");
    }

    let ch = conn.create_channel().await.unwrap();
    for _ in 0..message_count {
        let _ = ch.basic_get(queue_name, BasicGetOptions::default());
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
        queue.publish(queue_name, None, msg, None).await.unwrap();
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

    let mut conn = rt.block_on(setup_amqp()).unwrap();
    let broccoli_queue = rt.block_on(setup_broccoli());

    for &count in &message_counts {
        group.bench_function(format!("Raw AMQP {}", count), |b| {
            b.iter(|| rt.block_on(async { benchmark_raw_amqp_throughput(&mut conn, count).await }))
        });

        group.bench_function(format!("Broccoli AMQP {}", count), |b| {
            b.iter(|| {
                rt.block_on(async { benchmark_broccoli_throughput(&broccoli_queue, count).await })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
