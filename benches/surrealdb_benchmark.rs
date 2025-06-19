use std::env;
use std::io::{self, Write};
use std::str::FromStr;
use std::time::Instant;

use broccoli_queue::queue::{BroccoliQueue, ConsumeOptions};
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
    &s[s.find("=").unwrap() + 1..]
}

/// setup the connection to the database if `url` is informed or create an in-memory instance otherwise
async fn setup_surrealdb(url: Option<String>) -> Surreal<Any> {
    match url {
        Some(url) => {
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
        None => {
            let url = "mem://".to_string();
            let db = connect(url).await.unwrap();
            db.use_ns("app").await.unwrap();
            db.use_db("app").await.unwrap();
            db
        }
    }
}

async fn setup_broccoli(url: String) -> BroccoliQueue {
    BroccoliQueue::builder(url)
        .pool_connections(10)
        .build()
        .await
        .unwrap()
}

fn target_counter(message_count: usize) -> usize {
    // n = 2 2*3/2 = 3
    // 3-1-0 --> 1!
    //  n = 2 2*3/2 -1 = 2
    // 3-1-0 --> 1!
    // n = 1, 1*2/2 = 1
    // also sustract 1
    let target = ((message_count * (message_count + 1)) as f64) / 2_f64;
    (target.floor() as usize) - 2
}

async fn generate_test_messages(queue_name: &str, n: usize) -> Vec<BenchmarkMessage> {
    let messages: Vec<BenchmarkMessage> = (0..n)
        .map(|i| BenchmarkMessage {
            id: RecordId::from_str(&format!("{}:{}", queue_name, i)).unwrap(),
            data: format!("test data {}{}", i, if i == n - 1 { " [last]" } else { "" }),
            timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
        })
        .collect();
    messages
}

async fn consume_loop(
    queue: &BroccoliQueue,
    queue_name: &str,
    message_count: usize,
) -> Vec<BenchmarkMessage> {
    let mut consumed: Vec<BenchmarkMessage> = Vec::with_capacity(message_count);
    let mut counter = target_counter(message_count);
    for _ in 0..message_count {
        let msg = queue
            .consume::<BenchmarkMessage>(queue_name, None)
            .await
            .unwrap();
        consumed.push(msg.payload.clone());
        let i = msg
            .payload
            .id
            .key()
            .to_string()
            .parse::<usize>()
            .expect("message id was not parseable");
        counter -= i;
        let is_last = counter == 0;
        io::stderr().flush().unwrap();
        queue.acknowledge(queue_name, msg).await.unwrap();
        if is_last {
            break;
        }
    }
    consumed
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
        let queue_record_str = format!("{queue_table}:[5,<datetime>'{now}',{id}]");
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
            {
                LET $m = {
                    FOR $p IN 1..=5 {
                        LET $output = SELECT * FROM ONLY type::thing($queue_table,type::range([[$p,None],[$p,time::now()]])) LIMIT 1;
                        IF $output {
                            RETURN $output;
                        };
                    };
                };
                IF !$m {
                    RETURN NONE // no data available
                };
                CREATE type::table($processing_table) CONTENT {
                    id: type::thing($processing_table, $m.id[2]),
                    message_id: $m.message_id,
                    priority: $m.priority
                };
                -- remove from queue and return payload
                --  remember we don't delete from index, instead acknowledge/reject/cancel will do it
                LET $deleted = DELETE $m.id RETURN BEFORE;
                IF !$deleted {
                    THROW 'Transaction failed as $m.id already deleted (CONCURRENT_READ)';
                };
                LET $payload = SELECT * FROM  $m.message_id;
                $payload
            };
            COMMIT TRANSACTION;
    ";
        let queued: Response = db
            .query(q)
            .bind(("queue_table", queue_table.clone()))
            .bind(("processing_table", processing_table.clone()))
            .await
            .unwrap();

        // deserialize payload
        let queued: Option<BenchmarkMessage> = match queued.check() {
            Ok(mut queued) => queued.take(queued.num_statements() - 1).unwrap(),
            Err(e) => panic!("Could not do the raw transaction {:?}", e),
        };
        let queued = match queued {
            Some(queued) => queued,
            None => continue,
        };

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
    let published = queue
        .publish_batch(queue_name, None, messages, None)
        .await
        .expect("Could not publish");
    let n = published.len();
    if n != message_count {
        panic!("Only published {}/{} messages", message_count, n);
    }

    // when using mem://, we have a race condition between end of publish and consume, we yield to the
    // runtime executor
    tokio::time::sleep(tokio::time::Duration::ZERO).await;
    // Consume messages
    let consumed = consume_loop(queue, queue_name, message_count).await;
    let n = consumed.len();
    if n != message_count {
        panic!("Only consumed {}/{} messages", message_count, n);
    }

    let total_time = now.elapsed().as_secs_f64();
    let throughput = message_count as f64 / total_time;
    let avg_latency = total_time / message_count as f64;

    (throughput, avg_latency)
}

async fn benchmark_broccoli_consume_loop_throughput(
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

    // when using mem://, we have a race condition between end of publish and consume
    tokio::time::sleep(tokio::time::Duration::ZERO).await;

    let now = Instant::now();
    // Consume messages in a tight loop
    let consumed = consume_loop(queue, queue_name, message_count).await;
    //let consumed = consume_batch(queue, queue_name, 10, time::Duration::seconds(10)).await;
    let n = consumed.len();
    if n != message_count {
        panic!("Only consumed {}/{} messages", message_count, n);
    }

    let total_time = now.elapsed().as_secs_f64();
    let throughput = message_count as f64 / total_time;
    let avg_latency = total_time / message_count as f64;

    (throughput, avg_latency)
}

async fn process_job(m: BenchmarkMessage) -> Result<(), broccoli_queue::error::BroccoliError> {
    // CRITICAL AREA START //
    let i =
        m.id.key()
            .to_string()
            .parse::<usize>()
            .expect("message id was not parseable");
    let mut _mutex = handler_counter.lock().await;
    let mut v = *_mutex;
    v = v.checked_sub(i).expect("repeated message");
    *_mutex = v;
    let finished = *_mutex == 0;
    let _mutex = 0;
    // CRITICAL AREA END //
    if finished {
        // CRITICAL AREA START //
        // we acquire and release the lock and only after we cancel the token
        let handler_token = {
            let mut _mutex = handler_mutex.lock().await;
            _mutex.clone()
        };
        // CRITICAL AREA END //
        handler_token.cancel();
    }
    Ok(())
}

lazy_static::lazy_static! {
static ref handler_mutex: std::sync::Arc<tokio::sync::Mutex<CancellationToken>> =
    std::sync::Arc::new(tokio::sync::Mutex::new(CancellationToken::new()));
static ref handler_counter: std::sync::Arc<tokio::sync::Mutex<usize>> =
    std::sync::Arc::new(tokio::sync::Mutex::new(0));

}

async fn benchmark_broccoli_batch_handler_throughput(
    queue: &BroccoliQueue,
    options: Option<ConsumeOptions>,
    message_count: usize,
) -> (f64, f64) {
    let queue_name = "bench_handler_broccoli";

    let shared_token = CancellationToken::new();
    let handler_token = shared_token.clone();
    let target_counter = target_counter(message_count);
    // CRITICAL AREA START //
    {
        let mut _mutex = handler_mutex.lock().await;
        *_mutex = handler_token;
        let _mutex = 0;
        let mut _mutex = handler_counter.lock().await;
        *_mutex = target_counter;
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
                .process_messages(queue_name, Some(5), options, |msg| async {
                    process_job(msg.payload).await
                })
                 => {
                // unreachable
            }
        };
    });
    // when using mem://, we have a race condition between end of publish and consume
    tokio::time::sleep(tokio::time::Duration::ZERO).await;

    let messages = generate_test_messages(queue_name, message_count).await;

    let now = Instant::now();
    let published = queue
        .publish_batch(queue_name, None, messages, None)
        .await
        .expect("Could not publish");
    let n = published.len();
    if n != message_count {
        panic!("Only published {}/{} messages", message_count, n);
    }

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

    let url = env::var("SURREALDB_URL");
    if url.is_err() {
        panic!("Missing SURREALDB_URL env var (in order: ws://localhost:8001?username=<USERNAME>&password=<PASSWD>&ns=test&database=broker)")
    };
    let url = url.ok();

    // setup in memory and persistent instances, note each implementation will use what is most appropriate
    let db_mem = rt.block_on(setup_surrealdb(None));
    let db = rt.block_on(setup_surrealdb(url.clone()));
    let broccoli_mem_queue = rt.block_on(setup_broccoli("mem://".to_string()));
    let broccoli_queue = rt.block_on(setup_broccoli(url.clone().unwrap()));
    let instances = vec![
        (db_mem, broccoli_mem_queue, "mem"),
        (db, broccoli_queue, "disk"),
    ];
    let consume_options = [None, Some(ConsumeOptions::builder().auto_ack(true).build())];
    let message_counts = [1, 10, 100];
    for (db, broccoli_queue, instance) in instances {
        for &count in &message_counts {
            //TODO: mem raw test runs into transaction issues so skipping it
            // if instance == "mem" {
            // group.bench_function(
            //     format!(
            //         "Raw surrealdb publish loop + consume loop {} - {}",
            //         instance, count
            //     ),
            //     |b| {
            //         b.iter(|| {
            //             rt.block_on(async { benchmark_raw_surrealdb_throughput(&db, count).await })
            //         })
            //     },
            // );
            // }

            for options in &consume_options {
                let opt_str = if options.is_some() {
                    " [auto-ack] "
                } else {
                    ""
                };
                group.bench_function(
                    format!(
                        "Broccoli surrealdb batch_publish + consume loop {}{} - {}",
                        instance, opt_str, count
                    ),
                    |b| {
                        b.iter(|| {
                            rt.block_on(async {
                                benchmark_broccoli_batch_publish_consume_throughput(
                                    &broccoli_queue,
                                    count,
                                )
                                .await
                            })
                        })
                    },
                );
                group.bench_function(
                    format!(
                        "Broccoli surrealdb consume loop {}{} - {}",
                        instance, opt_str, count
                    ),
                    |b| {
                        b.iter(|| {
                            rt.block_on(async {
                                benchmark_broccoli_consume_loop_throughput(&broccoli_queue, count)
                                    .await
                            })
                        })
                    },
                );

                // TODO: handlers use live select that occasionally blocks
                // group.bench_function(
                //     format!(
                //         "Broccoli surrealdb handler {} {} - {}",
                //         instance, opt_str, count
                //     ),
                //     |b| {
                //         b.iter(|| {
                //             rt.block_on(async {
                //                 benchmark_broccoli_batch_handler_throughput(
                //                     &broccoli_queue,
                //                     options.clone(),
                //                     count,
                //                 )
                //                 .await
                //             })
                //         })
                //     },
                // );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
