//! Fixed-work PostgreSQL baseline, never a substitute for business E2E.
use runtime::{RuntimeEventInput, RuntimeEventScope, RuntimeProjectionWorkClass};
use runtime_postgres::PostgresRuntimeEventStore;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::time::{Duration, Instant};
use storage::{
    PostgresConnectionConfig, PostgresPoolLaneConfig, PostgresPoolSet, PostgresPoolSetConfig,
    StaticSecretRefResolver,
};

#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

struct BackgroundWork {
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl BackgroundWork {
    fn stop_and_join(&mut self) -> std::thread::Result<()> {
        self.stop.store(true, Ordering::Release);
        self.worker.take().expect("one background owner").join()
    }
}

impl Drop for BackgroundWork {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let (_, fields) = stat.rsplit_once(')')?;
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[test]
fn background_work_is_stopped_and_joined_during_unwind() {
    let finished = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&finished);
    assert!(std::panic::catch_unwind(move || {
        let stop = Arc::new(AtomicBool::new(false));
        let child_stop = Arc::clone(&stop);
        let _owner = BackgroundWork {
            stop,
            worker: Some(std::thread::spawn(move || {
                while !child_stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                observed.store(true, Ordering::Release);
            })),
        };
        panic!("intentional failed measurement");
    })
    .is_err());
    assert!(finished.load(Ordering::Acquire));
}

fn percentile(samples: &[u128], percent: usize) -> u128 {
    samples[(samples.len() - 1) * percent / 100]
}
fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[test]
#[ignore = "requires isolated COWD_TEST_POSTGRES_URL and COWD_PG_MATRIX_REPORT"]
fn production_postgres_fixed_workload_matrix_and_failure_recovery() {
    const OPERATIONS: usize = 128;
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated PG URL required");
    let report_path =
        std::env::var("COWD_PG_MATRIX_REPORT").expect("explicit report path required");
    let resolver = StaticSecretRefResolver::new([("matrix.pg".into(), url)]);
    let pools = PostgresPoolSet::connect(
        PostgresPoolSetConfig {
            connection: PostgresConnectionConfig::new(
                "runtime-pg-matrix",
                "matrix.pg",
                "cowd-pg-matrix",
            ),
            server_reserve: 1,
            critical: PostgresPoolLaneConfig::new(8, Some(1), 5_000),
            online_read: PostgresPoolLaneConfig::new(8, Some(1), 5_000),
            background: PostgresPoolLaneConfig::new(2, Some(1), 5_000),
        },
        &resolver,
    )
    .unwrap();
    let base = pools.executor();
    let namespace = PostgresTestScope::new();
    let scoped = namespace.bind(&base);
    let schema: String = scoped
        .checkout_critical()
        .unwrap()
        .query_one("SELECT current_schema()", &[])
        .unwrap()
        .get(0);
    let store = Arc::new(
        PostgresRuntimeEventStore::new(scoped.clone())
            .unwrap()
            .into_runtime_event_store(),
    );
    let mut samples = Vec::new();
    for concurrency in [1usize, 2, 4, 8, 16, 64] {
        for repeat in 0..5 {
            let stop = Arc::new(AtomicBool::new(false));
            let scans = Arc::new(AtomicUsize::new(0));
            let bg_store = Arc::clone(&store);
            let bg_stop = Arc::clone(&stop);
            let bg_scans = Arc::clone(&scans);
            let background = std::thread::spawn(move || {
                while !bg_stop.load(Ordering::Acquire) {
                    bg_store.run_projection_work(RuntimeProjectionWorkClass::Background, || {
                        bg_store.events_after_cursor(0, 32).unwrap();
                    });
                    bg_scans.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            let mut background = BackgroundWork {
                stop,
                worker: Some(background),
            };
            let barrier = Arc::new(Barrier::new(concurrency + 1));
            let active = Arc::new(AtomicUsize::new(0));
            let maximum = Arc::new(AtomicUsize::new(0));
            let before = scoped.health();
            let cpu_before = cpu_ticks();
            let workers=(0..concurrency).map(|worker| {
                let store=Arc::clone(&store);let barrier=Arc::clone(&barrier);let active=Arc::clone(&active);let maximum=Arc::clone(&maximum);
                std::thread::spawn(move || {
                    let mut durations=Vec::new();barrier.wait();
                    for operation in (worker..OPERATIONS).step_by(concurrency) {
                        let started=Instant::now();
                        let count=active.fetch_add(1,Ordering::SeqCst)+1;maximum.fetch_max(count,Ordering::SeqCst);
                        let stream=format!("matrix:{concurrency}:{repeat}:{operation}");
                        let body=serde_json::json!({"operation":operation,"original":"source-quality-中文","repeat":repeat,"concurrency":concurrency});
                        let committed=store.append(RuntimeEventInput {
                            stream_id:stream.clone(),scope:RuntimeEventScope::Recovery,kind:"matrix.source_committed".into(),
                            status:Some("committed".into()),actor:Some("pg-matrix".into()),refs:vec![],payload:body.clone(),
                        }).unwrap();
                        let read=store.list_stream(&stream).unwrap();
                        assert_eq!(read.len(),1);assert_eq!(read[0].payload,body);assert_eq!(read[0].event_id,committed.event_id);
                        active.fetch_sub(1,Ordering::SeqCst);durations.push(started.elapsed().as_micros());
                    }
                    durations
                })
            }).collect::<Vec<_>>();
            let wall = Instant::now();
            barrier.wait();
            let joined = workers
                .into_iter()
                .map(|worker| worker.join())
                .collect::<Vec<_>>();
            let wall_us = wall.elapsed().as_micros();
            let cleanup = Instant::now();
            let background_result = background.stop_and_join();
            let cleanup_us = cleanup.elapsed().as_micros();
            let mut latencies = joined
                .into_iter()
                .flat_map(Result::unwrap)
                .collect::<Vec<_>>();
            background_result.unwrap();
            latencies.sort_unstable();
            assert_eq!(latencies.len(), OPERATIONS);
            assert!(
                scans.load(Ordering::Relaxed) > 0,
                "background must make progress"
            );
            if concurrency > 1 {
                assert!(
                    maximum.load(Ordering::SeqCst) > 1,
                    "independent operations must overlap"
                );
            }
            let after = scoped.health();
            assert_eq!(
                after.metrics.checkout_timeout_count,
                before.metrics.checkout_timeout_count
            );
            assert_eq!(
                after.metrics.query_error_count,
                before.metrics.query_error_count
            );
            assert!(
                after.lanes.iter().all(|lane| lane.active_connections == 0),
                "connections released after joined work"
            );
            let sample = serde_json::json!({"concurrency":concurrency,"repeat":repeat,"operations":OPERATIONS,
                "wall_us":wall_us,"throughput_ops_s":OPERATIONS as f64*1_000_000.0/wall_us.max(1) as f64,
                "p50_us":percentile(&latencies,50),"p95_us":percentile(&latencies,95),"p99_us":percentile(&latencies,99),
                "max_inflight_operations":maximum.load(Ordering::SeqCst),"background_scans":scans.load(Ordering::Relaxed),
                "cleanup_us":cleanup_us,"rss_kib":rss_kib(),"pg_before":before,"pg_after":after,
                "process_cpu_delta_kernel_ticks":cpu_before.zip(cpu_ticks()).map(|(before,after)|after.saturating_sub(before)),
                "latencies_us":latencies});
            println!(
                "PG matrix concurrency={concurrency} repeat={repeat} p95_us={} ops_s={}",
                sample["p95_us"], sample["throughput_ops_s"]
            );
            samples.push(sample);
            // Preserve partial measurements if a later gate fails.
            std::fs::write(&report_path,serde_json::to_vec_pretty(&serde_json::json!({"status":"running","baseline_kind":"current_postgres_path","schema":schema,"samples":samples})).unwrap()).unwrap();
        }
    }
    // Query failure must not poison the pool or leave a checked-out connection.
    assert!(scoped
        .checkout_background()
        .unwrap()
        .query("SELECT 1/0", &[])
        .is_err());
    assert_eq!(
        scoped
            .checkout_background()
            .unwrap()
            .query_one("SELECT 1::integer", &[])
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    {
        let mut connection = scoped.checkout_critical().unwrap();
        connection
            .batch_execute("CREATE TABLE matrix_rollback_probe (value integer NOT NULL)")
            .unwrap();
        let mut transaction = connection.transaction().unwrap();
        transaction
            .execute("INSERT INTO matrix_rollback_probe VALUES (1)", &[])
            .unwrap();
        // Drop without commit models abandoned work; the original owner rolls back.
    }
    assert_eq!(
        scoped
            .checkout_online_read()
            .unwrap()
            .query_one("SELECT count(*) FROM matrix_rollback_probe", &[])
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(store);
    let reopened = PostgresRuntimeEventStore::new(scoped.clone())
        .unwrap()
        .into_runtime_event_store();
    assert_eq!(reopened.list_stream("matrix:64:4:127").unwrap().len(), 1);
    let final_health = scoped.health();
    assert!(final_health
        .lanes
        .iter()
        .all(|lane| lane.active_connections == 0));
    drop(reopened);
    drop(namespace);
    std::fs::write(&report_path,serde_json::to_vec_pretty(&serde_json::json!({
        "status":"baseline_measured","baseline_kind":"current_postgres_path","historical_comparison":false,
        "business_cancellation_gate":false,"fixed_operations_per_sample":OPERATIONS,"samples":samples,
        "query_failure_recovery":true,"uncommitted_transaction_rollback":true,"adapter_reconstruction":true,
        "final_pg_health":final_health,"owned_schema_removed":schema
    })).unwrap()).unwrap();
}
