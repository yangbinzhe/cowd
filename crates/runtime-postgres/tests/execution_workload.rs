//! Deterministic work through the production PG graph supervisor and resource
//! manager. These measurements are not provider or ToolHost business E2E.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
    ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use runtime::execution_core::graph::{
    ExecutionGraphStateStore, ExecutionResourceKind, NodeExecutionContext, NodeExecutionOutcome,
    NodeExecutionTicket, NodeExecutor, NodeExecutorError, ResourceQuota,
};
use runtime::{RuntimeEventStore, RuntimeServices};
use runtime_postgres::{PostgresArtifactRepository, PostgresRuntimeEventStore, PostgresTaskStore};
use serde_json::{json, Value};

#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

#[derive(Default)]
struct Work {
    active: AtomicUsize,
    peak: AtomicUsize,
    starts: Mutex<Vec<(String, Instant)>>,
    runs: Mutex<Vec<(String, Instant, Instant)>>,
}

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl NodeExecutor for Work {
    fn kind(&self) -> &str {
        "pg_execution_fixed_work"
    }
    fn validate(&self, _: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    async fn start(
        &self,
        ctx: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.starts
            .lock()
            .unwrap()
            .push((ctx.node.id.clone(), Instant::now()));
        Ok(NodeExecutionTicket {
            graph_id: ctx.graph.id.clone(),
            node_id: ctx.node.id.clone(),
            executor_kind: self.kind().into(),
            service_class: ctx.graph.service_class,
            attempt: ctx.attempt,
            idempotency_key: ctx.node.idempotency_key,
            payload_ref: ctx.node.payload_ref,
        })
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let start = Instant::now();
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = Active(&self.active);
        self.peak.fetch_max(active, Ordering::SeqCst);
        if ticket.payload_ref == "cancel" {
            std::future::pending::<()>().await;
        } else {
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        self.runs
            .lock()
            .unwrap()
            .push((ticket.node_id.clone(), start, Instant::now()));
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: None,
            summary: None,
            evidence_refs: vec![],
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        }))
    }
}

fn graph(mode: &str) -> ExecutionGraph {
    let mut graph = ExecutionGraph::new("fixed PG execution workload, not business acceptance");
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: format!("fixture-session:{}", graph.id),
        turn_id: format!("fixture-turn:{}", graph.id),
        root_task_id: format!("fixture-task:{}", graph.id),
        task_id: format!("fixture-task:{}", graph.id),
        generation: 1,
    });
    graph.nodes = (0..16)
        .map(|_| {
            ExecutionNodeSpec::new(
                ExecutionNodeKind::ToolBatch,
                "pg_execution_fixed_work",
                mode,
            )
        })
        .collect();
    graph
}

fn services(
    scope: &PostgresTestScope,
    root: &std::path::Path,
    capacity: usize,
) -> Arc<RuntimeServices> {
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let pool = scope.reconnect();
    RuntimeServices::builder(root.join("home"), &workspace)
        .runtime_event_store(Arc::new(
            PostgresRuntimeEventStore::new(pool.clone())
                .unwrap()
                .into_runtime_event_store(),
        ))
        .artifact_store(Arc::new(
            runtime::ArtifactStore::new(
                root.join("artifacts"),
                Arc::new(PostgresArtifactRepository::new(pool.clone()).unwrap()),
                runtime::ArtifactStoreConfig::default(),
            )
            .unwrap(),
        ))
        .task_aggregate_service(Arc::new(
            PostgresTaskStore::new(pool).unwrap().into_task_service(),
        ))
        .collaboration_capacity(Default::default(), capacity)
        .resource_quotas(
            [
                ExecutionResourceKind::Provider,
                ExecutionResourceKind::Agent,
                ExecutionResourceKind::Tool,
            ]
            .map(|kind| {
                (
                    kind,
                    ResourceQuota::new(capacity, capacity, capacity).unwrap(),
                )
            }),
        )
        .build()
        .unwrap()
}

fn replay(scope: &PostgresTestScope, id: &str) -> ExecutionGraph {
    let store: RuntimeEventStore = PostgresRuntimeEventStore::new(scope.reconnect())
        .unwrap()
        .into_runtime_event_store();
    ExecutionGraphStateStore::new(Arc::new(store))
        .load(id)
        .unwrap()
}

fn process_usage() -> (u64, u64) {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("Linux process CPU counters");
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    let ticks = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let rss_kib = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|s| s.split_whitespace().next())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .unwrap();
    (ticks, rss_kib)
}

async fn drained(services: &RuntimeServices, work: &Work) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = services
                .resource_manager()
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap();
            if work.active.load(Ordering::SeqCst) == 0
                && snapshot.active_leases == 0
                && snapshot.queued_waiters == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("all execution futures and resource admissions must drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn production_pg_execution_fixed_workload_and_cancel_release() {
    let scope = PostgresTestScope::new();
    let temp = tempfile::tempdir().unwrap();
    let mut samples: Vec<Value> = vec![];
    for capacity in [1, 2, 4, 8] {
        for repeat in 0..5 {
            eprintln!("execution_workload capacity={capacity} repeat={repeat}");
            let svc = services(
                &scope,
                &temp.path().join(format!("{capacity}-{repeat}")),
                capacity,
            );
            let work = Arc::new(Work::default());
            svc.executor_registry().register(work.clone()).unwrap();
            let graph = graph("complete");
            let id = graph.id.clone();
            let admitted = Instant::now();
            let usage_before = process_usage();
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                svc.execution_supervisor().submit_and_wait(
                    graph,
                    ExecutionGraphCommand::Start {
                        expected_revision: 0,
                    },
                ),
            )
            .await;
            let terminal = Instant::now();
            // Always shut down this owned supervisor before propagating assertions.
            svc.execution_supervisor().shutdown().await;
            svc.shutdown_maintenance().await;
            let (_, report) = result
                .expect("bounded fixture completion")
                .expect("canonical commit");
            assert_eq!(report.completed, 16);
            drained(&svc, &work).await;
            let cleanup = terminal.elapsed().as_micros();
            let usage_after = process_usage();
            let peak = work.peak.load(Ordering::SeqCst);
            assert!(
                peak > 0 && peak <= capacity,
                "resource quota must bound execution"
            );
            if capacity > 1 {
                assert!(peak > 1, "independent work must overlap");
            }
            let restored = replay(&scope, &id);
            assert_eq!(restored.node_statuses.len(), 16);
            assert!(restored
                .node_statuses
                .values()
                .all(|s| *s == ExecutionNodeStatus::Completed));
            let starts = work.starts.lock().unwrap();
            let runs = work.runs.lock().unwrap();
            assert_eq!(starts.len(), 16);
            assert_eq!(runs.len(), 16);
            let node_samples: Vec<_> = runs.iter().map(|(node, start, end)| {
                let executor_start = starts.iter().find(|(id, _)| id == node).unwrap().1;
                json!({"node_id":node,"admission_to_start_us":executor_start.duration_since(admitted).as_micros(),
                    "admission_to_poll_us":start.duration_since(admitted).as_micros(),
                    "run_us":end.duration_since(*start).as_micros()})
            }).collect();
            samples.push(json!({"capacity":capacity,"repeat":repeat,"nodes":node_samples,
                "terminal_us":terminal.duration_since(admitted).as_micros(), "cleanup_us":cleanup,"peak":peak,
                "cpu_kernel_ticks":usage_after.0 - usage_before.0,"rss_kib":usage_after.1,
                "resource":svc.resource_manager().snapshot(&ExecutionResourceKind::Tool).unwrap()}));
            let weak = Arc::downgrade(&svc);
            let supervisor = Arc::downgrade(svc.execution_supervisor());
            drop(svc);
            assert!(
                weak.upgrade().is_none(),
                "shut down RuntimeServices must not retain itself"
            );
            // A final observer task can legitimately hold a temporary strong
            // reference until its PG read completes. A permanent callback cycle
            // must still fail, without racing that bounded tail.
            tokio::time::timeout(Duration::from_secs(10), async {
                while supervisor.upgrade().is_some() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("settled observer must release the supervisor and PG pool");
        }
        let svc = services(
            &scope,
            &temp.path().join(format!("cancel-{capacity}")),
            capacity,
        );
        let work = Arc::new(Work::default());
        svc.executor_registry().register(work.clone()).unwrap();
        let graph = graph("cancel");
        let id = graph.id.clone();
        let running = {
            let svc = svc.clone();
            tokio::spawn(async move {
                svc.execution_supervisor()
                    .submit_and_wait(
                        graph,
                        ExecutionGraphCommand::Start {
                            expected_revision: 0,
                        },
                    )
                    .await
            })
        };
        let active = tokio::time::timeout(Duration::from_secs(10), async {
            while work.active.load(Ordering::SeqCst) < capacity {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let started = Instant::now();
        let cancelled = svc
            .cancel_execution_tree(&id, "owned fixed-work cancellation gate")
            .await;
        svc.execution_supervisor().shutdown().await;
        svc.shutdown_maintenance().await;
        let joined = tokio::time::timeout(Duration::from_secs(10), running).await;
        active.expect("fixture reached configured active capacity");
        cancelled.expect("cancel original graph through production owner");
        let outcome = joined
            .expect("graph wait is reclaimed")
            .expect("join owned submit task");
        if let Err(error) = outcome {
            assert!(
                error
                    .to_string()
                    .contains("execution cancelled while graph pump was running"),
                "unexpected submit failure: {error}"
            );
        }
        drained(&svc, &work).await;
        let restored = replay(&scope, &id);
        assert!(restored
            .node_statuses
            .values()
            .all(|s| *s == ExecutionNodeStatus::Cancelled));
        assert!(
            work.runs.lock().unwrap().is_empty(),
            "cancelled work must not publish completion"
        );
        samples.push(
            json!({"capacity":capacity,"cancel_release_us":started.elapsed().as_micros(),
            "resource":svc.resource_manager().snapshot(&ExecutionResourceKind::Tool).unwrap()}),
        );
    }
    assert_eq!(samples.len(), 24);
    let report = json!({"schema_version":1,"scope":"production_pg_execution_kernel_deterministic_work",
        "business_provider_or_toolhost_e2e":false,"profile":"recorded by invoking runner",
        "nodes_per_sample":16,"fixed_work_ms":60,"repeats":5,"samples":samples});
    println!("PG_EXECUTION_WORKLOAD_REPORT={report}");
    if let Some(path) = std::env::var_os("COWD_PG_EXECUTION_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}
