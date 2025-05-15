use std::env;
use std::str::FromStr;
use std::time::Instant;

use broccoli_queue::queue::BroccoliQueue;
use criterion::{criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::connect;
use surrealdb::{engine::any::Any, RecordId};
use surrealdb::{Response, Surreal};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMessage {
    id: RecordId, // queue_name:id
    data: String,
    timestamp: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMessageEntry {
    message_id: RecordId, // queue_name:id
    priority: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMessageIndex {
    queue_id: RecordId, // queue:[timestamp,id]
}

fn read_param(s: &str) -> &str {
    &s[s.find("=").unwrap()+1..]
}

async fn setup_surrealdb() -> Surreal<Any> {
    match env::var("SURREALDB_URL") {
        Ok(url) => {
            // it's annoying to make the utils package public just because of this so adding some shortcut code here
            let i = url.find("?").unwrap();
            let c = &url[0..i];
            let url = &url[i..];
            let url: Vec<&str> = url.split("&").collect();
            let u = read_param(url[0]);
            let p = read_param(url[1]);
            let ns = read_param(url[2]);
            let database = read_param(url[3]);
            let db = connect(c).await.unwrap();
            db.signin(surrealdb::opt::auth::Root {
                username: u,
                password: p,
            })
            .await
            .unwrap();
            db.use_ns(ns).use_db(database).await.unwrap();
            db
        }
        Err(_) => panic!("Missing SURREALDB_URL env var (in order: ws://localhost:8001?username=<USERNAME>&password=<PASSWD>&ns=test&database=broker)"),
    }
}

async fn setup_broccoli() -> BroccoliQueue {
    match env::var("SURREALDB_URL") {
        Ok(url) => 
        BroccoliQueue::builder(url)
            .pool_connections(10)
            .build()
            .await
            .unwrap()
        ,
        Err(_) => panic!("Missing SURREALDB_URL env var (in order: ws://localhost:8001?username=<USERNAME>&password=<PASSWD>&ns=test&database=broker)"),
    }
}

async fn generate_test_messages(queue_name: &str, n: usize) -> Vec<BenchmarkMessage> {
    let messages: Vec<BenchmarkMessage> = (0..n)
        .map(|i| BenchmarkMessage {
            id: RecordId::from_str(&format!("{}:{}", queue_name, i.to_string())).unwrap(),
            data: format!("test data {}{}", i, if i == n - 1 { " [last]" } else { "" }),
            timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
        })
        .collect();
    messages
}

// used to parse dates in surrealdb format
fn to_rfc3339<T>(dt: T) -> Result<std::string::String, time::error::Format>
where
    T: Into<time::OffsetDateTime>,
{
    dt.into()
        .format(&time::format_description::well_known::Rfc3339)
}

async fn benchmark_raw_surrealdb_throughput(db: &Surreal<Any>, message_count: usize) -> (f64, f64) {
    let queue_name = "bench_raw_surrealdb";

    let index_table = format!("{}___index", queue_name);
    let queue_table = format!("{}___queue", queue_name);
    let processing_table = format!("{}___processing", queue_name);

    // clear tables if they exist
    // let tables = vec![
    //     queue_name.to_string(),
    //     index_table.clone(),
    //     queue_table.clone(),
    //     processing_table.clone(),
    // ];
    // for t in tables {
    //     let q = "REMOVE TABLE IF EXISTS $table;";
    //     db.query(q).bind(("table", t)).await.unwrap();
    // }

    // Generate test messages
    let messages = generate_test_messages(queue_name, message_count).await;

    let now = Instant::now();
    // Publish messages
    for msg in messages {
        // insert payload
        let _: Option<BenchmarkMessage> = db.create(&msg.id).content(msg.clone()).await.unwrap();

        // insert queue entry
        let id = msg.id.key().clone();
        let now = to_rfc3339(time::OffsetDateTime::now_utc()).unwrap();
        let queue_record_str = format!("{queue_name}___queue:[<datetime>'{now}',{id}]");
        let queue_record_id = RecordId::from_str(&queue_record_str).unwrap();
        let message_record_id: RecordId = msg.id.clone();

        let _: Option<BenchmarkMessageEntry> = db
            .create(queue_record_id.clone())
            .content(BenchmarkMessageEntry {
                message_id: message_record_id, // queue_name:id
                priority: 5,
            })
            .await
            .unwrap();

        // add to index
        let _: Option<BenchmarkMessageIndex> = db
            .create((index_table.clone(), id))
            .content(BenchmarkMessageIndex {
                queue_id: queue_record_id, // queue:[timestamp,id]
            })
            .await
            .unwrap();
    }

    // Consume messages
    for _ in 0..message_count {
        // read from queue, add processing and return
        let q = "
            BEGIN TRANSACTION;
            LET $m = SELECT * FROM ONLY (SELECT * FROM type::thing($queue_table,type::range([[None,None],[time::now(),None]]))) ORDER BY priority,id[0] ASC LIMIT 1;
            CREATE type::table($processing_table) CONTENT {
                id: type::thing($processing_table, $m.id[1]),
                message_id: $m.message_id,
                priority: $m.priority
            };
            -- remove from queue and return payload
            -- remember we don't delete from index, instead acknowledge/reject/cancel will do it
            DELETE $m.id;
            LET $payload = SELECT * FROM  $m.message_id;
            COMMIT TRANSACTION;
            $payload;
    ";
        let mut queued: Response = db
            .query(q)
            .bind(("queue_table", queue_table.clone()))
            .bind(("processing_table", processing_table.clone()))
            .await
            .unwrap();

        // deserialize payload
        let queued: Option<BenchmarkMessage> = queued.take(queued.num_statements() - 1).unwrap();
        let queued = queued.unwrap();

        // remove processing table
        // [date,id] --> id
        let message_id = queued.id.key().to_string();
        let message_id: Vec<&str> = message_id.split(",").collect();
        let message_id = message_id[0];
        let message_id = message_id.replace("]", "");
        let message_id = message_id.trim();
        let message_record_id =
            RecordId::from_str(&format!("{}:{}", processing_table, message_id)).unwrap();

        let processing: Option<BenchmarkMessageEntry> = db.delete(message_record_id).await.unwrap();
        processing.unwrap();

        // ack: remove payload
        let payload_record_id =
            RecordId::from_str(&format!("{}:{}", queue_name, message_id)).unwrap();
        let removed: Option<BenchmarkMessage> = db.delete(payload_record_id).await.unwrap();
        removed.unwrap();

        // ack: remove index
        let index_record_id =
            RecordId::from_str(&format!("{}:{}", index_table.clone(), message_id)).unwrap();
        let removed: Option<BenchmarkMessageIndex> = db.delete(index_record_id).await.unwrap();
        removed.unwrap();
    }

    let total_time = now.elapsed().as_secs_f64();
    let throughput = message_count as f64 / total_time;
    let avg_latency = total_time / message_count as f64;

    (throughput, avg_latency)
}

async fn benchmark_broccoli_batch_publish_consume_throughput(
    queue: &BroccoliQueue,
    message_count: usize,
) -> (f64, f64) {
    let queue_name = "bench_broccoli";

    // Generate test messages
    let messages = generate_test_messages(queue_name, message_count).await;

    let now = Instant::now();
    // Publish messages
    queue
        .publish_batch(queue_name, None, messages, None)
        .await
        .expect("Could not publish");

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

async fn benchmark_broccoli_batch_consume_throughput(
    queue: &BroccoliQueue,
    message_count: usize,
) -> (f64, f64) {
    let queue_name = "bench_broccoli";

    // Generate test messages
    let messages = generate_test_messages(queue_name, message_count).await;

    // Publish messages
    queue
        .publish_batch(queue_name, None, messages, None)
        .await
        .expect("Could not publish");

    let now = Instant::now();
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

async fn _process_job(m: BenchmarkMessage) -> Result<(), broccoli_queue::error::BroccoliError> {
    if m.data.contains("[last]") {
        // CRITICAL AREA START //
        // we acquire and release the lock and only after we cancel the token
        let mut _mutex = handler_mutex.lock().await;
        let handler_token = _mutex.clone();
        let _mutex = 0;
        handler_token.cancel();
    }
    Ok(())
}

lazy_static::lazy_static! {
    static ref handler_mutex: std::sync::Arc<tokio::sync::Mutex<CancellationToken>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(CancellationToken::new()));
}

async fn benchmark_broccoli_batch_handler_throughput(
    queue: &BroccoliQueue,
    message_count: usize,
) -> (f64, f64) {
    let queue_name = "bench_handler_broccoli";

    let shared_token = CancellationToken::new();
    let handler_token = shared_token.clone();
    // CRITICAL AREA START //
    {
        let mut _mutex = handler_mutex.lock().await;
        *_mutex = handler_token;
        let _mutex = 0;
    }
    // CRITICAL AREA END //

    // launch consumer first
    let queue_clone = queue.clone();
    let consumer = tokio::spawn(async move {
        tokio::select! {
            // Step 3: Using cloned token to listen to cancellation requests
            _ = shared_token.cancelled() => {
                // The token was cancelled, task can shut down
            }
            _ = queue_clone
                .process_messages(queue_name, Some(10), None, |msg| async {
                    _process_job(msg.payload).await
                })
                 => {
                // unreachable
            }
        };
    });

    let messages = generate_test_messages(queue_name, message_count).await;
    let now = Instant::now();
    queue
        .publish_batch(queue_name, None, messages, None)
        .await
        .expect("Could not publish");

    // we wait for the consumer to finish
    let _ = consumer.await;

    let total_time = now.elapsed().as_secs_f64();
    let throughput = message_count as f64 / total_time;
    let avg_latency = total_time / message_count as f64;

    (throughput, avg_latency)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("Queue Performance");
    let message_counts = [1, 10, 100];

    let db = rt.block_on(setup_surrealdb());
    let broccoli_queue = rt.block_on(setup_broccoli());

    for &count in &message_counts {
        group.bench_function(
            format!("Raw surrealdb publish loop + consume loop {}", count),
            |b| {
                b.iter(|| {
                    rt.block_on(async { benchmark_raw_surrealdb_throughput(&db, count).await })
                })
            },
        );

        group.bench_function(
            format!("Broccoli surrealdb batch_publish + consume loop {}", count),
            |b| {
                b.iter(|| {
                    rt.block_on(async {
                        benchmark_broccoli_batch_publish_consume_throughput(&broccoli_queue, count)
                            .await
                    })
                })
            },
        );
        group.bench_function(format!("Broccoli surrealdb consume loop {}", count), |b| {
            b.iter(|| {
                rt.block_on(async {
                    benchmark_broccoli_batch_consume_throughput(&broccoli_queue, count).await
                })
            })
        });
        group.bench_function(format!("Broccoli surrealdb handler {}", count), |b| {
            b.iter(|| {
                rt.block_on(async {
                    benchmark_broccoli_batch_handler_throughput(&broccoli_queue, count).await
                })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
