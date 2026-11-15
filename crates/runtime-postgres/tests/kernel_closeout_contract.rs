use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use runtime::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventReactor,
    RuntimeEventScope, RuntimeEventStore, RuntimeProjectionDescriptor,
    RuntimeProjectionEventInterest, RuntimeProjectionInterest, RuntimeProjectionLane,
    RuntimeProjectionPass, SessionTerminalInput,
};
use runtime_postgres::PostgresRuntimeEventStore;

#[path = "../test-support/current_terminal_fixture.rs"]
mod current_terminal_fixture;
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

fn conclusion() -> AppendTransactionRequest {
    AppendTransactionRequest {
        transaction_id: "program-conclusion:kernel:1".into(),
        expected_streams: vec![
            ExpectedStreamRevision {
                stream_id: "goal:kernel".into(),
                expected_revision: 0,
            },
            ExpectedStreamRevision {
                stream_id: "agentic-program:kernel".into(),
                expected_revision: 0,
            },
        ],
        events: vec![
            RuntimeEventInput {
                stream_id: "goal:kernel".into(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.completed".into(),
                status: Some("satisfied".into()),
                actor: Some("kernel-persistence-fixture".into()),
                refs: vec![],
                payload: serde_json::json!({"fixture":"persistence-boundary-only"}),
            }
            .into(),
            RuntimeEventInput {
                stream_id: "agentic-program:kernel".into(),
                scope: RuntimeEventScope::Program,
                kind: "agentic.objective_verdict_bound".into(),
                status: Some("verified".into()),
                actor: Some("kernel-persistence-fixture".into()),
                refs: vec![],
                payload: serde_json::json!({"fixture":"persistence-boundary-only"}),
            }
            .into(),
        ],
    }
}

fn graph_terminal(
    verdict_event_id: &str,
    store: &RuntimeEventStore,
    artifacts: &Arc<runtime::ArtifactStore>,
    source: &std::path::Path,
) -> (AppendTransactionRequest, SessionTerminalInput) {
    let mut graph =
        harness_contract::execution_graph::ExecutionGraph::new("kernel persistence fixture")
            .with_lineage(harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session:kernel".into(),
                turn_id: "turn:kernel".into(),
                root_task_id: "task:kernel".into(),
                task_id: "task:kernel".into(),
                generation: 1,
            });
    graph.id = "execution-graph:kernel".into();
    graph.revision = 1;
    let binding = serde_json::from_value(serde_json::json!({
        "root_execution_id": graph.id, "session_id": "session:kernel", "turn_id": "turn:kernel",
        "root_task_id": "task:kernel", "task_id": "task:kernel", "activity_id": format!("activity:execution:{}", graph.id),
        "revision": 1, "fence": 1, "generation": 1
    })).unwrap();
    let mut terminal = SessionTerminalInput {
        terminal_id: "terminal:kernel".into(),
        message_id: "message:kernel".into(),
        session_id: "session:kernel".into(),
        execution_id: Some(graph.id.clone()),
        turn_id: Some("turn:kernel".into()),
        request_id: Some("request:kernel".into()),
        session_generation: Some(1),
        input_sequence: Some(0),
        input_claim_owner: Some("owner:kernel".into()),
        input_claim_token: Some("token:kernel".into()),
        input_claim_revision: Some(1),
        controlled_recovery_claim_fingerprints: vec![],
        payload_ref: String::new(),
    };
    let mut payload = current_terminal_fixture::current_payload(
        "terminal:kernel",
        "ingress:kernel",
        0,
        "persistence fixture",
    );
    payload["goal_completion"] = serde_json::json!("satisfied");
    current_terminal_fixture::publish_file(store, artifacts, source, &mut terminal, &payload);
    let transaction = AppendTransactionRequest {
        transaction_id: "graph-terminal:kernel".into(),
        expected_streams: vec![
            ExpectedStreamRevision {
                stream_id: graph.id.clone(),
                expected_revision: 0,
            },
            ExpectedStreamRevision {
                stream_id: "session-terminal:kernel".into(),
                expected_revision: 0,
            },
        ],
        events: vec![
            current_terminal_fixture::inline_graph_event(
                runtime::execution_core::graph::ExecutionGraphEvent::Planned {
                    graph: graph.clone(),
                },
                &graph.id,
                "kernel:planned",
                binding,
                vec![runtime::RuntimeEventRef {
                    kind: "objective_verdict".into(),
                    id: verdict_event_id.into(),
                }],
            ),
            runtime::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: "session-terminal:kernel".into(),
                    scope: RuntimeEventScope::SessionInput,
                    kind: "runtime.session.terminal_requested".into(),
                    status: Some("pending".into()),
                    actor: Some("kernel-persistence-fixture".into()),
                    refs: vec![],
                    payload: serde_json::to_value(&terminal).unwrap(),
                },
                idempotency_key: Some("kernel:terminal-requested".into()),
                schema_version: 1,
            },
        ],
    };
    RuntimeEventStore::validate_current_content_inputs(&transaction).unwrap();
    assert_eq!(
        transaction.events[1].event.payload,
        serde_json::to_value(&terminal).unwrap()
    );
    (transaction, terminal)
}

fn connect(
    fixture: &PostgresTestScope,
    artifacts: Option<&Arc<runtime::ArtifactStore>>,
) -> RuntimeEventStore {
    let events = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    if let Some(artifacts) = artifacts {
        events.bind_artifact_store(Arc::clone(artifacts)).unwrap();
    }
    events
}

fn assert_fault_witness(executor: &storage::PostgresExecutor, sequence: &str, called: bool) {
    let row = executor
        .checkout_critical()
        .unwrap()
        .query_one(
            &format!("SELECT last_value, is_called FROM {sequence}"),
            &[],
        )
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, bool>(1)), (1, called));
}

fn install_fault_witness(
    executor: &storage::PostgresExecutor,
    constraint: &str,
    kind: &str,
) -> String {
    let namespace: String = executor
        .checkout_critical()
        .unwrap()
        .query_one("SELECT current_schema()", &[])
        .unwrap()
        .get(0);
    assert!(namespace
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    assert!(constraint
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    assert!(matches!(
        kind,
        "agentic.objective_verdict_bound" | "runtime.session.terminal_requested"
    ));
    let sequence = format!("\"{namespace}\".\"{constraint}_hit\"");
    executor
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!(
            "CREATE SEQUENCE {sequence} START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE CACHE 1"
        ))
        .unwrap();
    assert_fault_witness(executor, &sequence, false);
    executor.checkout_critical().unwrap().batch_execute(&format!(
        "ALTER TABLE \"{namespace}\".runtime_events ADD CONSTRAINT {constraint} CHECK (CASE WHEN kind = '{kind}' THEN nextval('{sequence}'::regclass) = 0 ELSE TRUE END)"
    )).unwrap();
    assert_fault_witness(executor, &sequence, false);
    sequence
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_conclusion_rolls_back_all_streams_when_the_second_event_insert_fails() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresRuntimeEventStore::new(executor.clone())
        .unwrap()
        .into_runtime_event_store();
    let sequence = install_fault_witness(
        &executor,
        "kernel_reject_verdict",
        "agentic.objective_verdict_bound",
    );
    let error = store.append_transaction(conclusion()).unwrap_err();
    assert!(
        matches!(&error, runtime::RuntimeEventStoreError::Storage(_)),
        "{error:?}"
    );
    assert_fault_witness(&executor, &sequence, true);
    for stream in ["goal:kernel", "agentic-program:kernel"] {
        assert_eq!(store.stream_revision(stream).unwrap(), 0);
        assert!(store.list_stream(stream).unwrap().is_empty());
    }
    assert!(store.events_after_cursor(0, 10).unwrap().is_empty());
    executor
        .checkout_critical()
        .unwrap()
        .batch_execute("ALTER TABLE runtime_events DROP CONSTRAINT kernel_reject_verdict")
        .unwrap();
    let receipt = store.append_transaction(conclusion()).unwrap();
    assert_eq!(receipt.event_ids.iter().collect::<std::collections::BTreeSet<_>>().len(), 2);
    assert_eq!(receipt.event_ids.len(), 2);
    assert!(!receipt.duplicate);
    drop(store);
    let reopened = connect(&fixture, None);
    let duplicate = reopened.append_transaction(conclusion()).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.commit_cursor, receipt.commit_cursor);
    assert_eq!(duplicate.event_ids, receipt.event_ids);
    let conclusion_batches = reopened.events_after_cursor(0, 10).unwrap();
    assert_eq!(conclusion_batches.iter().map(|batch| batch.transaction_id.as_str()).collect::<Vec<_>>(), vec!["program-conclusion:kernel:1"]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_graph_failure_keeps_the_committed_verdict_and_rolls_back_its_outbox() {
    let fixture = PostgresTestScope::new();
    let root = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(
        runtime::ArtifactStore::new(
            root.path().join("artifacts"),
            Arc::new(
                runtime_postgres::PostgresArtifactRepository::new(fixture.reconnect()).unwrap(),
            ),
            runtime::ArtifactStoreConfig::default(),
        )
        .unwrap(),
    );
    let executor = fixture.reconnect();
    let store = PostgresRuntimeEventStore::new(executor.clone())
        .unwrap()
        .into_runtime_event_store();
    let verdict = store.append_transaction(conclusion()).unwrap();
    let (transaction, terminal) = graph_terminal(
        &verdict.event_ids[1],
        &store,
        &artifacts,
        &root.path().join("terminal.json"),
    );
    let sequence = install_fault_witness(
        &executor,
        "kernel_reject_terminal",
        "runtime.session.terminal_requested",
    );
    let error = store
        .append_transaction_with_terminal(transaction.clone(), terminal.clone())
        .unwrap_err();
    assert!(
        matches!(&error, runtime::RuntimeEventStoreError::Storage(_)),
        "{error:?}"
    );
    assert_fault_witness(&executor, &sequence, true);
    assert_eq!(store.stream_revision("goal:kernel").unwrap(), 1);
    assert_eq!(store.stream_revision("agentic-program:kernel").unwrap(), 1);
    assert_eq!(store.stream_revision("execution-graph:kernel").unwrap(), 0);
    assert_eq!(store.stream_revision("session-terminal:kernel").unwrap(), 0);
    assert!(store
        .list_stream("execution-graph:kernel")
        .unwrap()
        .is_empty());
    assert!(store
        .list_stream("session-terminal:kernel")
        .unwrap()
        .is_empty());
    assert!(store
        .session_terminal(&terminal.terminal_id)
        .unwrap()
        .is_none());
    executor
        .checkout_critical()
        .unwrap()
        .batch_execute("ALTER TABLE runtime_events DROP CONSTRAINT kernel_reject_terminal")
        .unwrap();
    let graph = store
        .append_transaction_with_terminal(transaction.clone(), terminal.clone())
        .unwrap();
    assert_eq!(graph.event_ids.iter().collect::<std::collections::BTreeSet<_>>().len(), 2);
    assert_eq!(graph.event_ids.len(), 2);
    for stream in ["execution-graph:kernel", "session-terminal:kernel"] {
        assert_eq!(store.stream_revision(stream).unwrap(), 1);
        let events = store.list_stream(stream).unwrap();
        assert_eq!(events.iter().map(|event| event.commit_cursor).collect::<Vec<_>>(), vec![graph.commit_cursor]);
        assert_eq!(events[0].commit_cursor, graph.commit_cursor);
    }
    drop(store);
    let reopened = connect(&fixture, Some(&artifacts));
    let duplicate = reopened
        .append_transaction_with_terminal(transaction, terminal.clone())
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.event_ids, graph.event_ids);
    assert_eq!(duplicate.commit_cursor, graph.commit_cursor);
    assert_eq!(reopened.stream_revision("goal:kernel").unwrap(), 1);
    assert_eq!(
        reopened.stream_revision("execution-graph:kernel").unwrap(),
        1
    );
    assert_eq!(
        reopened.stream_revision("session-terminal:kernel").unwrap(),
        1
    );
    let graph_reader = runtime::execution_core::graph::ExecutionGraphStateStore::new(Arc::new(
        connect(&fixture, Some(&artifacts)),
    ));
    let recovered_graph = graph_reader.load("execution-graph:kernel").unwrap();
    assert_eq!(recovered_graph.id, "execution-graph:kernel");
    assert_eq!(recovered_graph.revision, 1);
    let outbox = reopened
        .session_terminal(&terminal.terminal_id)
        .unwrap()
        .unwrap();
    assert_eq!(outbox.commit_cursor, graph.commit_cursor);
    assert_eq!(outbox.message_id, terminal.message_id);
    assert_eq!(outbox.payload_ref, terminal.payload_ref);
    assert_eq!(outbox.status, "pending");
    let closeout_batches = reopened.events_after_cursor(0, 10).unwrap();
    assert_eq!(closeout_batches.iter().map(|batch| batch.transaction_id.clone()).collect::<std::collections::BTreeSet<_>>().len(), 2);
    assert_eq!(closeout_batches.len(), 2);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_outbox_restart_reclaims_the_same_message_and_rejects_the_old_worker_ack() {
    let fixture = PostgresTestScope::new();
    let root = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(
        runtime::ArtifactStore::new(
            root.path().join("artifacts"),
            Arc::new(
                runtime_postgres::PostgresArtifactRepository::new(fixture.reconnect()).unwrap(),
            ),
            runtime::ArtifactStoreConfig::default(),
        )
        .unwrap(),
    );
    let store = connect(&fixture, Some(&artifacts));
    let verdict = store.append_transaction(conclusion()).unwrap();
    let (transaction, terminal) = graph_terminal(
        &verdict.event_ids[1],
        &store,
        &artifacts,
        &root.path().join("terminal.json"),
    );
    let receipt = store
        .append_transaction_with_terminal(transaction.clone(), terminal.clone())
        .unwrap();
    let first = store
        .claim_session_terminals("crashed-worker", 100, 10, 1)
        .unwrap()
        .remove(0);
    assert_eq!(first.message_id, terminal.message_id);
    drop(store);
    let reopened = connect(&fixture, Some(&artifacts));
    let duplicate = reopened
        .append_transaction_with_terminal(transaction, terminal.clone())
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.event_ids, receipt.event_ids);
    assert_eq!(duplicate.commit_cursor, receipt.commit_cursor);
    assert_eq!(
        reopened
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .unwrap()
            .payload_ref,
        terminal.payload_ref
    );
    assert!(reopened
        .claim_session_terminals("new-worker", 109, 10, 1)
        .unwrap()
        .is_empty());
    let second = reopened
        .claim_session_terminals("new-worker", 111, 10, 1)
        .unwrap()
        .remove(0);
    assert_eq!(second.message_id, first.message_id);
    assert_eq!(second.commit_cursor, first.commit_cursor);
    assert_eq!(second.attempts, 2);
    assert!(reopened
        .ack_session_terminal(&first.terminal_id, "crashed-worker", first.revision, 112)
        .is_err());
    let ack = reopened
        .ack_session_terminal(&second.terminal_id, "new-worker", second.revision, 112)
        .unwrap();
    assert_eq!(ack.status, "materialized");
    assert_eq!(ack.message_id, terminal.message_id);
    drop(reopened);
    let final_store = connect(&fixture, Some(&artifacts));
    assert!(final_store
        .claim_session_terminals("third-worker", 200, 10, 1)
        .unwrap()
        .is_empty());
    assert_eq!(
        final_store
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .unwrap()
            .status,
        "materialized"
    );
    assert_eq!(final_store.stream_revision("goal:kernel").unwrap(), 1);
    assert_eq!(
        final_store
            .stream_revision("execution-graph:kernel")
            .unwrap(),
        1
    );
    assert_eq!(
        final_store
            .stream_revision("session-terminal:kernel")
            .unwrap(),
        1
    );
}

struct PublicationDelay {
    connection: storage::PostgresConnection,
    key: i64,
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_concurrent_publication_keeps_every_committed_event_recoverable() {
    let fixture = PostgresTestScope::new();
    let reader = connect(&fixture, None);
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut workers = Vec::new();
    for worker in 0..4 {
        let store = connect(&fixture, None);
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let mut samples = Vec::new();
            barrier.wait();
            for sequence in 0..25 {
                let started = std::time::Instant::now();
                let event = store
                    .append(RuntimeEventInput {
                        stream_id: format!("kernel:publication-load:{worker}"),
                        scope: RuntimeEventScope::Recovery,
                        kind: "kernel.publication_sample".into(),
                        status: None,
                        actor: Some(format!("load-worker-{worker}")),
                        refs: vec![],
                        payload: serde_json::json!({"worker":worker,"sequence":sequence}),
                    })
                    .unwrap();
                samples.push((event.commit_cursor, started.elapsed().as_micros() as u64));
            }
            samples
        }));
    }
    let started = std::time::Instant::now();
    barrier.wait();
    let mut samples = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    let elapsed = started.elapsed();
    assert_eq!(samples.iter().map(|sample| sample.0).collect::<std::collections::BTreeSet<_>>().len(), 100);
    assert_eq!(samples.len(), 100);
    samples.sort_unstable();
    assert!(samples.windows(2).all(|pair| pair[0].0 < pair[1].0));
    let recovered = reader.events_after_cursor(0, 128).unwrap();
    assert_eq!(recovered.len(), samples.len());
    assert_eq!(
        recovered
            .iter()
            .map(|batch| batch.commit_cursor)
            .collect::<Vec<_>>(),
        samples.iter().map(|sample| sample.0).collect::<Vec<_>>()
    );
    assert_eq!(
        reader.durable_commit_cursor().unwrap(),
        samples.last().unwrap().0
    );
    for worker in 0..4 {
        assert_eq!(
            reader
                .list_stream(&format!("kernel:publication-load:{worker}"))
                .unwrap()
                .len(),
            25
        );
    }
    let mut latency = samples
        .into_iter()
        .map(|sample| sample.1)
        .collect::<Vec<_>>();
    latency.sort_unstable();
    println!(
        "kernel_publication_benchmark={}",
        serde_json::json!({
            "workers":4,"commits":100,"elapsed_ms":elapsed.as_millis(),
            "commits_per_second":100.0/elapsed.as_secs_f64(),
            "p50_us":latency[49],"p95_us":latency[94],"max_us":latency[99],
            "scope":"isolated local fixture only; no baseline improvement claim",
        })
    );
}

impl Drop for PublicationDelay {
    fn drop(&mut self) {
        let _ = self
            .connection
            .query_one("SELECT pg_advisory_unlock($1)", &[&self.key]);
    }
}

fn publication_request(label: &str) -> AppendTransactionRequest {
    let mut request = conclusion();
    request.transaction_id = format!("kernel-publication:{label}");
    for expected in &mut request.expected_streams {
        expected.stream_id = format!("{}:{label}", expected.stream_id);
    }
    for input in &mut request.events {
        input.event.stream_id = format!("{}:{label}", input.event.stream_id);
        input.event.actor = Some(format!("kernel-publish-{label}"));
    }
    request
}

fn publication_delay_trigger_sql(key: i64) -> String {
    format!(
        "CREATE FUNCTION kernel_delay_publication() RETURNS trigger LANGUAGE plpgsql AS $kernel_publication$
         BEGIN PERFORM pg_advisory_xact_lock(({key})::bigint); RETURN NEW; END;
         $kernel_publication$;
         CREATE TRIGGER kernel_delay_publication BEFORE INSERT ON runtime_events
         FOR EACH ROW WHEN (NEW.actor = 'kernel-publish-slow') EXECUTE FUNCTION kernel_delay_publication()"
    )
}

#[test]
fn publication_delay_trigger_sql_keeps_a_complete_dollar_quoted_body() {
    for lock_key in [i64::MIN, -1, 0, 1, i64::MAX] {
        let sql = publication_delay_trigger_sql(lock_key);
        assert_eq!(sql.matches("$kernel_publication$").count(), 2);
        let body = sql.split("$kernel_publication$").nth(1).unwrap();
        assert_eq!(
            body.trim(),
            format!(
                "BEGIN PERFORM pg_advisory_xact_lock(({lock_key})::bigint); RETURN NEW; END;"
            )
        );
        assert!(sql.starts_with(
            "CREATE FUNCTION kernel_delay_publication() RETURNS trigger LANGUAGE plpgsql AS $kernel_publication$"
        ));
        assert!(sql.contains("$kernel_publication$;\n         CREATE TRIGGER"));
        assert!(sql.ends_with(
            "FOR EACH ROW WHEN (NEW.actor = 'kernel-publish-slow') EXECUTE FUNCTION kernel_delay_publication()"
        ));
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_visible_commit_order_cannot_skip_an_earlier_inflight_cursor() {
    let fixture = PostgresTestScope::new();
    let reader = connect(&fixture, None);
    let slow = connect(&fixture, None);
    let fast = connect(&fixture, None);
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    let key: i64 = connection
        .query_one(
            "SELECT hashtextextended(current_schema() || ':kernel-publication-delay', 0)",
            &[],
        )
        .unwrap()
        .get(0);
    connection
        .query_one("SELECT pg_advisory_lock($1)", &[&key])
        .unwrap();
    let mut delay = PublicationDelay { connection, key };
    delay
        .connection
        .batch_execute(&publication_delay_trigger_sql(key))
        .unwrap();
    let slow_thread =
        std::thread::spawn(move || slow.append_transaction(publication_request("slow")));
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let upper_key = ((key as u64) >> 32) as i64;
    let lower_key = ((key as u64) & u64::from(u32::MAX)) as i64;
    let mut first_is_inflight = false;
    while std::time::Instant::now() < deadline {
        first_is_inflight = delay
            .connection
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted
             AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1)",
                &[&upper_key, &lower_key],
            )
            .unwrap()
            .get(0);
        if first_is_inflight {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let (finished_sender, finished_receiver) = std::sync::mpsc::sync_channel(1);
    let fast_thread = std::thread::spawn(move || {
        let result = fast.append_transaction(publication_request("fast"));
        finished_sender.send(()).unwrap();
        result
    });
    let overtook = finished_receiver
        .recv_timeout(Duration::from_millis(250))
        .is_ok();
    drop(delay);
    let first = slow_thread.join().unwrap().unwrap();
    let second = fast_thread.join().unwrap().unwrap();
    assert!(
        first_is_inflight,
        "the trigger must really pause a cursor-owning transaction"
    );
    assert!(
        !overtook,
        "a later visible cursor can make a durable reader skip a late earlier commit"
    );
    assert!(first.commit_cursor < second.commit_cursor);
    let batches = reader.events_after_cursor(0, 10).unwrap();
    assert_eq!(batches.iter().map(|batch| batch.transaction_id.as_str()).collect::<Vec<_>>(), vec!["kernel-publication:slow", "kernel-publication:fast"]);
    assert_eq!(batches[0].transaction_id, "kernel-publication:slow");
    assert_eq!(batches[1].transaction_id, "kernel-publication:fast");
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_production_snapshot_and_delta_recover_a_separate_writers_goal_without_notify() {
    let fixture = PostgresTestScope::new();
    let reader = Arc::new(connect(&fixture, None));
    let writer = Arc::new(connect(&fixture, None));
    let temporary = tempfile::tempdir().unwrap();
    let workspace = temporary.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = runtime::RuntimeServices::builder(temporary.path().join("home"), &workspace)
        .non_durable_backends_for_testing(temporary.path())
        .runtime_event_store(reader.clone())
        .build()
        .unwrap();
    let graph =
        harness_contract::execution_graph::ExecutionGraph::new("PG external Goal projection")
            .with_lineage(harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "pg-projection-session".into(),
                turn_id: "pg-projection-turn".into(),
                root_task_id: "pg-projection-task".into(),
                task_id: "pg-projection-task".into(),
                generation: 1,
            });
    let graph_id = graph.id.clone();
    services
        .execution_supervisor()
        .submit_and_wait(
            graph,
            harness_contract::execution_graph::ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .unwrap();
    let query = harness_contract::projection::ProjectionQueryContext {
        principal: "kernel-pg-test".into(),
        workspace_id: services.workspace_key().into(),
        session_scopes: vec![],
        mission_scopes: vec![],
        visibility_grants: vec![
            format!("workspace:{}", services.workspace_key()),
            "resource:*".into(),
        ],
        detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
        authorization_revision: 1,
    };
    let initial = runtime::execution_projection::snapshot(&services, &graph_id, &query)
        .await
        .unwrap();
    assert!(initial.goals.is_empty());
    assert!(initial.graph.commit_cursor <= initial.cursor);
    assert!(initial.terminal_axes.source_cursor <= initial.cursor);
    assert!(!initial.terminal_axes.source_race);
    let mut snapshot_query = query.clone();
    snapshot_query.principal = "kernel-pg-snapshot-test".into();
    let snapshot_initial =
        runtime::execution_projection::snapshot(&services, &graph_id, &snapshot_query)
            .await
            .unwrap();
    assert!(snapshot_initial.goals.is_empty());
    assert_ne!(initial.redaction_revision, snapshot_initial.redaction_revision);
    services.shutdown_maintenance().await;
    let local_signal = reader.subscribe_commits();
    let hint = reader.current_commit_cursor();
    let goal_id = format!("goal:{graph_id}");
    let goal = serde_json::from_value(serde_json::json!({
        "id":goal_id,"session_id":"pg-projection-session","objective":"Recover external Goal facts",
        "criteria":[{"id":"pg-external-criterion","statement":"visible in snapshot and delta","status":"open"}],
        "phase":"execution","scope":"internal","completion":"open","revision":1,"user_sequence":1,
        "spec_revision":1,"spec_digest":"kernel-pg-external-goal-fixture-v1",
    })).unwrap();
    runtime::execution_core::GoalStore::new(writer)
        .create(goal)
        .unwrap();
    assert_eq!(reader.current_commit_cursor(), hint);
    assert!(!local_signal.has_changed().unwrap());
    let durable = reader.durable_commit_cursor().unwrap();
    assert!(durable > initial.cursor);
    assert!(durable > snapshot_initial.cursor);
    let snapshot_recovered =
        runtime::execution_projection::snapshot(&services, &graph_id, &snapshot_query)
            .await
            .unwrap();
    assert_eq!(snapshot_recovered.cursor, durable);
    assert!(snapshot_recovered.goals.iter().any(|goal| goal.id == goal_id));
    let update = runtime::execution_projection::delta(
        &services,
        &graph_id,
        initial.revision,
        initial.cursor,
        &query,
    )
    .unwrap();
    assert_eq!(update.base_cursor, initial.cursor);
    assert_eq!(update.from_revision, initial.revision);
    assert_eq!(update.target_cursor, durable);
    assert!(update.resync_reason.is_none());
    assert!(update.operations.iter().any(|operation| matches!(operation,
        harness_contract::projection::ProjectionOperation::UpsertEntity {
            collection: harness_contract::projection::ProjectionEntityCollection::Goals, entity,
        } if entity.id == goal_id)));
    let reduced = harness_contract::projection::reduce_projection_delta(&initial, &update).unwrap();
    assert!(reduced.goals.iter().any(|goal| goal.id == goal_id));
    let recovered = runtime::execution_projection::snapshot(&services, &graph_id, &query)
        .await
        .unwrap();
    assert_eq!(recovered.cursor, durable);
    assert!(recovered.goals.iter().any(|goal| goal.id == goal_id));
    assert_eq!(reduced.cursor, recovered.cursor);
    assert_eq!(reduced.goals, recovered.goals);
    assert_eq!(snapshot_recovered.goals, recovered.goals);
    assert_eq!(reader.current_commit_cursor(), hint);
    assert!(!local_signal.has_changed().unwrap());
    services.shutdown_execution().await;
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_external_commit_without_a_local_doorbell_is_recovered_by_durable_scan() {
    let fixture = PostgresTestScope::new();
    let reader = Arc::new(connect(&fixture, None));
    let writer = connect(&fixture, None);
    let local_signal = reader.subscribe_commits();
    let cursor = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let recovered = Arc::new(tokio::sync::Notify::new());
    let interest = RuntimeProjectionInterest::new([RuntimeProjectionEventInterest::new(
        RuntimeEventScope::Program,
        "agentic.objective_verdict_bound",
    )]);
    let descriptor = RuntimeProjectionDescriptor::new(
        "kernel:external-doorbell-loss",
        interest.clone(),
        8,
        Duration::from_millis(50),
    )
    .unwrap();
    let lane_reader = reader.clone();
    let lane_cursor = cursor.clone();
    let lane_entered = entered.clone();
    let lane_recovered = recovered.clone();
    let lane = RuntimeProjectionLane::blocking(descriptor, move |batch_size| {
        let page = lane_reader
            .projection_scan_page(
                lane_cursor.load(Ordering::SeqCst),
                &interest,
                batch_size,
                100,
                1024 * 1024,
            )
            .map_err(|error| error.to_string())?;
        lane_cursor.store(page.scanned_through_cursor, Ordering::SeqCst);
        lane_entered.notify_one();
        if page.matched_events > 0 {
            lane_recovered.notify_one();
        }
        Ok(
            RuntimeProjectionPass::scanned(page.scanned_commits, batch_size)
                .with_matches(page.matched_events),
        )
    });
    let reactor = Arc::new(RuntimeEventReactor::sealed(reader.clone(), [lane]).unwrap());
    reactor.start().unwrap();
    let initialized = tokio::time::timeout(Duration::from_secs(3), entered.notified()).await;
    let receipt = writer.append_transaction(conclusion()).unwrap();
    assert!(!local_signal.has_changed().unwrap());
    assert_eq!(
        reader.durable_commit_cursor().unwrap(),
        receipt.commit_cursor
    );
    let result = tokio::time::timeout(Duration::from_secs(3), recovered.notified()).await;
    let shutdown = reactor.shutdown().await;
    assert!(initialized.is_ok());
    assert!(result.is_ok());
    assert!(shutdown.timed_out_lanes.is_empty());
    assert_eq!(cursor.load(Ordering::SeqCst), receipt.commit_cursor);
}
