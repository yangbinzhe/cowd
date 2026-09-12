use std::sync::{Arc, Barrier};
use std::thread;

use runtime::RuntimeServices;
use storage::StaticSecretRefResolver;

use super::*;
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

#[test]
fn runtime_event_initial_migration_remains_immutable() {
    let initial = RUNTIME_EVENT_MIGRATIONS
        .iter()
        .find(|migration| migration.id == "runtime_event.0001.initial")
        .expect("initial Runtime event migration exists");

    assert_eq!(
        initial.checksum(),
        "c29d153132dcd497b6665b9f7a1cbe376d5ce1f39f2f37db308963bb1bc3bd3d"
    );
    assert!(RUNTIME_EVENT_MIGRATIONS
        .iter()
        .any(|migration| migration.id == "runtime_event.0010.activity-identity-index"));
}

fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
    RuntimeEventInput {
        stream_id: stream_id.to_string(),
        scope,
        kind: kind.to_string(),
        status: Some("running".to_string()),
        actor: Some("runtime-postgres-test".to_string()),
        refs: Vec::new(),
        payload: serde_json::json!({"kind": kind}),
    }
}

fn open_real_store() -> (RuntimeEventStore, PostgresTestScope) {
    let scope = PostgresTestScope::new();
    let store = PostgresRuntimeEventStore::new(scope.reconnect())
        .expect("owned Runtime event namespace")
        .into_runtime_event_store();
    (store, scope)
}

fn policy_bound_task_spec(session_id: &str, objective: &str) -> TaskSpec {
    let policy = SessionExecutionPolicy::from_profile(
        AutonomyProfileId::Supervised,
        1,
        SessionExecutionPolicyOrigin::SessionExplicit,
    );
    let mut spec = TaskSpec::new(objective);
    spec.execution_policy = spec.execution_policy.bind(ExecutionPolicyBinding::bind(
        session_id,
        &policy,
        PermissionMode::ReadOnly,
    ));
    spec
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn projection_work_class_maps_background_without_downgrading_recovery() {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("projection-lanes.pg".to_string(), url)]);
    let pool_set = storage::PostgresPoolSet::connect(
        storage::PostgresPoolSetConfig {
            connection: PostgresConnectionConfig::new(
                "runtime-projection-lanes",
                "projection-lanes.pg",
                "cowd-runtime-projection-lanes",
            ),
            server_reserve: 1,
            critical: storage::PostgresPoolLaneConfig::new(2, Some(1), 1_000),
            online_read: storage::PostgresPoolLaneConfig::new(2, Some(1), 1_000),
            background: storage::PostgresPoolLaneConfig::new(1, Some(1), 250),
        },
        &resolver,
    )
    .expect("isolated pool set");
    let fixture = PostgresTestScope::new();
    let executor = fixture.bind(&pool_set.executor());
    let store = PostgresRuntimeEventStore::new(executor.clone())
        .expect("runtime store")
        .into_runtime_event_store();
    let before = executor.health();
    store.run_projection_work(runtime::RuntimeProjectionWorkClass::Background, || {
        store
            .append(input(
                "projection:background",
                RuntimeEventScope::Evolution,
                "projection.background",
            ))
            .unwrap();
        store.events_after_cursor(0, 1).unwrap();
        store
            .put_projection_checkpoint(
                "projector:lane-proof",
                1,
                &serde_json::json!({"ok": true}),
                1,
            )
            .unwrap();
    });
    let after_background = executor.health();
    let delta = |health: &storage::PostgresExecutorHealth,
                 workload: storage::PostgresWorkloadClass| {
        let current = health
            .lanes
            .iter()
            .find(|lane| lane.workload == workload)
            .unwrap()
            .metrics
            .checkout_count;
        let prior = before
            .lanes
            .iter()
            .find(|lane| lane.workload == workload)
            .unwrap()
            .metrics
            .checkout_count;
        current.saturating_sub(prior)
    };
    assert!(
        delta(
            &after_background,
            storage::PostgresWorkloadClass::Background
        ) >= 3
    );
    assert_eq!(
        delta(&after_background, storage::PostgresWorkloadClass::Critical),
        0
    );
    assert_eq!(
        delta(
            &after_background,
            storage::PostgresWorkloadClass::OnlineRead
        ),
        0
    );

    store.run_projection_work(runtime::RuntimeProjectionWorkClass::Recovery, || {
        store.events_after_cursor(0, 1).unwrap();
        store
            .append(input(
                "projection:recovery",
                RuntimeEventScope::Recovery,
                "projection.recovery",
            ))
            .unwrap();
    });
    let after_recovery = executor.health();
    assert!(
        after_recovery
            .lanes
            .iter()
            .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
            .unwrap()
            .metrics
            .checkout_count
            > after_background
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
                .unwrap()
                .metrics
                .checkout_count
    );
    assert!(
        after_recovery
            .lanes
            .iter()
            .find(|lane| lane.workload == storage::PostgresWorkloadClass::Critical)
            .unwrap()
            .metrics
            .checkout_count
            > after_background
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::Critical)
                .unwrap()
                .metrics
                .checkout_count
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_sequence_pages_freeze_the_head_during_independent_concurrent_appends() {
    let (store, fixture) = open_real_store();
    let stream = "program:sequence-page";
    for index in 0..64 {
        let mut event = input(stream, RuntimeEventScope::Program, "page.seed");
        event.payload = serde_json::json!({"seed":index});
        store.append(event).unwrap();
    }
    let frozen_head = store.stream_revision(stream).unwrap();
    assert_eq!(frozen_head, 64);
    let barrier = Arc::new(Barrier::new(3));
    let writers = (0..2)
        .map(|writer| {
            let adapter = PostgresRuntimeEventStore::new(fixture.reconnect())
                .unwrap()
                .into_runtime_event_store();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                (0..32)
                    .map(|index| {
                        let mut event =
                            input(stream, RuntimeEventScope::Program, "page.concurrent");
                        event.payload = serde_json::json!({"writer":writer,"index":index});
                        adapter.append(event).unwrap()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let read_range = |store: &RuntimeEventStore, from: u64, through: u64| {
        let mut cursor = from;
        let mut events = vec![];
        while cursor < through {
            let page = store
                .list_stream_after(stream, cursor, through, 7, 8 * 1024)
                .unwrap();
            assert!(!page.is_empty(), "a stable committed interval has no gaps");
            assert!(page.len() <= 7);
            for event in page {
                assert_eq!(event.sequence, cursor + 1);
                assert!(event.sequence <= through);
                cursor = event.sequence;
                events.push(event);
            }
        }
        events
    };
    barrier.wait();
    let first = read_range(&store, 0, frozen_head);
    let written = writers
        .into_iter()
        .flat_map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    let new_head = store.stream_revision(stream).unwrap();
    assert_eq!(new_head, 128);
    assert!(first.iter().all(|event| event.kind == "page.seed"));
    let tail = read_range(&store, frozen_head, new_head);
    let written_ids = written
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let read_ids = tail
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(read_ids, written_ids);
    assert_eq!(read_ids.len(), 64);
    let reopened = PostgresRuntimeEventStore::new(fixture.reconnect())
        .unwrap()
        .into_runtime_event_store();
    let replay = read_range(&reopened, 0, new_head);
    let expected = first
        .iter()
        .chain(tail.iter())
        .map(|event| (&event.event_id, &event.payload))
        .collect::<Vec<_>>();
    assert_eq!(
        replay
            .iter()
            .map(|event| (&event.event_id, &event.payload))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(reopened
        .list_stream_after(stream, new_head, new_head, 7, 8 * 1024)
        .unwrap()
        .is_empty());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_runtime_event_store_preserves_fences_outbox_restart_and_runtime_composition() {
    let (store, fixture) = open_real_store();
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "postgres-source-transaction".to_string(),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: "copy:stream".to_string(),
                    expected_revision: 0,
                },
                ExpectedStreamRevision {
                    stream_id: "copy:empty-stream".to_string(),
                    expected_revision: 0,
                },
            ],
            events: vec![input(
                "copy:stream",
                RuntimeEventScope::Recovery,
                "postgres.source_seeded",
            )
            .into()],
        })
        .expect("PostgreSQL source event");
    let store = Arc::new(store);
    store
        .append(input(
            "graph:concurrent",
            RuntimeEventScope::ExecutionGraph,
            "graph.seeded",
        ))
        .expect("seed append");
    store
        .append(input(
            "evolution:signal:prefix-contract",
            RuntimeEventScope::Evolution,
            "evolution.signal.recorded.v1",
        ))
        .expect("prefix target append");
    store
        .append(input(
            "evolution:signal-other",
            RuntimeEventScope::Evolution,
            "evolution.signal.recorded.v1",
        ))
        .expect("prefix neighbour append");
    store
        .append(input(
            "evolution:mission:prefix-contract",
            RuntimeEventScope::Evolution,
            "evolution.mission.created.v1",
        ))
        .expect("different prefix append");
    let prefix_events = store
        .replay_scope_stream_prefix(RuntimeEventScope::Evolution, "evolution:signal:")
        .expect("prefix replay must be independent of database collation");
    assert_eq!(prefix_events.len(), 1);
    assert_eq!(
        prefix_events[0].stream_id,
        "evolution:signal:prefix-contract"
    );

    let barrier = Arc::new(Barrier::new(2));
    let writers = (0..2)
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            let store = Arc::clone(&store);
            thread::spawn(move || {
                barrier.wait();
                store.append_transaction(AppendTransactionRequest {
                    transaction_id: format!("concurrent-writer-{writer}"),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: "graph:concurrent".to_string(),
                        expected_revision: 1,
                    }],
                    events: vec![RuntimeTransactionEventInput {
                        event: input(
                            "graph:concurrent",
                            RuntimeEventScope::ExecutionGraph,
                            "graph.concurrent",
                        ),
                        idempotency_key: Some(format!("writer-{writer}")),
                        schema_version: 1,
                    }],
                })
            })
        })
        .collect::<Vec<_>>();
    let outcomes = writers
        .into_iter()
        .map(|writer| writer.join().expect("writer thread"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(RuntimeEventStoreError::StaleRevision { .. })))
            .count(),
        1
    );
    assert_eq!(store.stream_revision("graph:concurrent").unwrap(), 2);
    let commit_cursor_before_checkpoint = *store.subscribe_commits().borrow();
    let checkpoint = store
        .put_projection_checkpoint(
            "projector:postgres-contract",
            commit_cursor_before_checkpoint,
            &serde_json::json!({"cursor": commit_cursor_before_checkpoint}),
            100,
        )
        .expect("mutable projection checkpoint");
    assert_eq!(checkpoint.revision, 1);
    assert_eq!(
        *store.subscribe_commits().borrow(),
        commit_cursor_before_checkpoint,
        "mutable projection checkpoints must not emit journal commits"
    );
    assert_eq!(
        store
            .projection_checkpoint("projector:postgres-contract")
            .expect("read checkpoint")
            .expect("checkpoint exists"),
        checkpoint
    );
    assert!(matches!(
        store.put_projection_checkpoint(
            "projector:postgres-contract",
            commit_cursor_before_checkpoint.saturating_sub(1),
            &serde_json::json!({"cursor": "stale"}),
            101,
        ),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));

    let terminal_request = AppendTransactionRequest {
        transaction_id: "terminal-transaction".to_string(),
        expected_streams: vec![ExpectedStreamRevision {
            stream_id: "session-input:real".to_string(),
            expected_revision: 0,
        }],
        events: vec![RuntimeTransactionEventInput {
            event: input(
                "session-input:real",
                RuntimeEventScope::SessionInput,
                "runtime.session.terminal_requested",
            ),
            idempotency_key: Some("terminal-request".to_string()),
            schema_version: 1,
        }],
    };
    let terminal_input = SessionTerminalInput {
        terminal_id: "terminal-real".to_string(),
        message_id: "message-real".to_string(),
        session_id: "session-real".to_string(),
        execution_id: Some("execution-real".to_string()),
        turn_id: Some("turn-real".to_string()),
        request_id: Some("request-real".to_string()),
        session_generation: Some(1),
        input_sequence: Some(0),
        input_claim_owner: Some("worker-real".to_string()),
        input_claim_token: Some("claim-real".to_string()),
        input_claim_revision: Some(3),
        controlled_recovery_claim_fingerprints: Vec::new(),
        payload_ref: "payload-real".to_string(),
    };
    let terminal_receipt = store
        .append_transaction_with_terminal(terminal_request.clone(), terminal_input.clone())
        .expect("terminal transaction");
    assert!(terminal_receipt.commit_cursor > 0);

    let claim_barrier = Arc::new(Barrier::new(2));
    let workers = (0..2)
        .map(|worker| {
            let barrier = Arc::clone(&claim_barrier);
            let store = Arc::clone(&store);
            thread::spawn(move || {
                barrier.wait();
                store.claim_session_terminals(&format!("worker-{worker}"), 100, 1_000, 1)
            })
        })
        .collect::<Vec<_>>();
    let claims = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker thread").expect("claim"))
        .collect::<Vec<_>>();
    let claimed = claims.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(claimed.len(), 1);
    let claim = &claimed[0];
    assert_eq!(claim.request_id.as_deref(), Some("request-real"));
    assert_eq!(claim.session_generation, Some(1));
    assert_eq!(claim.input_sequence, Some(0));
    assert_eq!(claim.input_claim_owner.as_deref(), Some("worker-real"));
    assert_eq!(claim.input_claim_token.as_deref(), Some("claim-real"));
    assert_eq!(claim.input_claim_revision, Some(3));
    assert_eq!(claim.status, "claimed");
    assert_eq!(claim.attempts, 1);
    assert_eq!(claim.claim_expires_at_ms, Some(1_100));
    drop(store);
    let store = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .expect("postgres event store reopens after delivery crash")
            .into_runtime_event_store(),
    );
    assert!(matches!(
        store.adopt_session_terminal_fence(&RuntimeSessionTerminalFenceAdoption {
            terminal_id: claim.terminal_id.clone(),
            expected_terminal_revision: claim.revision,
            request_id: "request-real".to_string(),
            session_id: "session-real".to_string(),
            turn_id: "turn-real".to_string(),
            session_generation: 1,
            input_sequence: 0,
            claim_owner: "session-worker-reclaimed".to_string(),
            claim_token: "session-claim-reclaimed".to_string(),
            claim_revision: 5,
            claim_expires_at_ms: 2_000,
            adopted_at_ms: 1_099,
        }),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));
    let adoption = RuntimeSessionTerminalFenceAdoption {
        terminal_id: claim.terminal_id.clone(),
        expected_terminal_revision: claim.revision,
        request_id: "request-real".to_string(),
        session_id: "session-real".to_string(),
        turn_id: "turn-real".to_string(),
        session_generation: 1,
        input_sequence: 0,
        claim_owner: "session-worker-reclaimed".to_string(),
        claim_token: "session-claim-reclaimed".to_string(),
        claim_revision: 5,
        claim_expires_at_ms: 2_000,
        adopted_at_ms: 1_100,
    };
    let adopted = store
        .adopt_session_terminal_fence(&adoption)
        .expect("expired delivery claim adopts reclaimed Session fence");
    assert_eq!(adopted.status, "pending");
    assert_eq!(adopted.input_claim_revision, Some(5));
    let replay = store
        .append_transaction_with_terminal(terminal_request.clone(), terminal_input.clone())
        .expect("initial terminal transaction replays after fence adoption");
    assert!(replay.duplicate);
    let mut conflicting_initial_fence = terminal_input;
    conflicting_initial_fence.input_claim_token = Some("different-initial-fence".to_string());
    assert!(matches!(
        store.append_transaction_with_terminal(terminal_request, conflicting_initial_fence),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));
    assert_eq!(
        store
            .adopt_session_terminal_fence(&adoption)
            .expect("adoption replay")
            .revision,
        adopted.revision
    );
    let claim = store
        .claim_session_terminals("worker-after-adoption", 1_101, 1_000, 1)
        .expect("claim adopted terminal")
        .remove(0);
    let materialized = store
        .ack_session_terminal(
            &claim.terminal_id,
            claim.claim_owner.as_deref().expect("claim owner"),
            claim.revision,
            1_102,
        )
        .expect("ack claimed terminal");
    assert_eq!(materialized.status, "materialized");
    assert_eq!(
        store
            .materialized_session_terminals_after("session-real", 0, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        store.enqueue_session_terminal(
            "terminal-unfenced",
            "message-unfenced",
            "session-real",
            terminal_receipt.commit_cursor,
            "payload-unfenced",
        ),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));

    let duplicate_request = AppendTransactionRequest {
        transaction_id: "duplicate-transaction".to_string(),
        expected_streams: vec![ExpectedStreamRevision {
            stream_id: "graph:duplicate".to_string(),
            expected_revision: 0,
        }],
        events: vec![RuntimeTransactionEventInput {
            event: input(
                "graph:duplicate",
                RuntimeEventScope::ExecutionGraph,
                "graph.duplicate",
            ),
            idempotency_key: Some("duplicate".to_string()),
            schema_version: 1,
        }],
    };
    assert!(
        !store
            .append_transaction(duplicate_request.clone())
            .expect("first idempotent transaction")
            .duplicate
    );
    assert!(
        store
            .append_transaction(duplicate_request)
            .expect("duplicate transaction")
            .duplicate
    );

    drop(store);
    let reopened = Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .expect("postgres event store reopens")
            .into_runtime_event_store(),
    );
    assert_eq!(reopened.stream_revision("graph:concurrent").unwrap(), 2);
    let terminal = reopened
        .session_terminal("terminal-real")
        .unwrap()
        .expect("terminal persists");
    assert_eq!(terminal.status, "materialized");
    assert_eq!(terminal.execution_id.as_deref(), Some("execution-real"));
    assert_eq!(terminal.turn_id.as_deref(), Some("turn-real"));

    let temp = tempfile::tempdir().expect("temporary Runtime host");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace exists");
    let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
        .runtime_event_store(reopened)
        .artifact_store(Arc::new(
            runtime::ArtifactStore::new(
                temp.path().join("artifact-blobs"),
                Arc::new(
                    PostgresArtifactRepository::new(fixture.reconnect())
                        .expect("PG artifact backend"),
                ),
                runtime::ArtifactStoreConfig::default(),
            )
            .expect("PG artifact store"),
        ))
        .task_aggregate_service(Arc::new(
            PostgresTaskStore::new(fixture.reconnect())
                .expect("PG task backend")
                .into_task_service(),
        ))
        .build()
        .expect("RuntimeServices composes PostgreSQL event backend");
    services.publish_session_execution_policy(
        "session:postgres-composed",
        runtime::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Supervised,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    let mission_id = services.mission_runtime().default_mission_id().to_string();
    services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "postgres-composed".to_string(),
            mission_id,
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id: "session:postgres-composed".to_string(),
            origin_turn_id: "turn:postgres-composed".to_string(),
            root_task_id: "postgres-composed".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: services
                .task_runtime_port()
                .bind_task_spec(
                    "session:postgres-composed",
                    None,
                    harness_contract::task::TaskSpec::new(
                        "prove canonical Task outbox reaches PostgreSQL event store",
                    ),
                )
                .expect("bind canonical Task policy for PostgreSQL composition"),
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test_fixture",
                "test://runtime-postgres/composed-task",
            )],
        })
        .expect("canonical Task outbox reaches PostgreSQL event backend");
    assert!(services
        .event_reader()
        .list_stream("task:postgres-composed")
        .expect("read composed event")
        .iter()
        .any(|event| event.kind == "task.created"));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_task_store_preserves_restart_and_per_task_concurrency() {
    let fixture = PostgresTestScope::new();
    let pg_store = PostgresTaskStore::new(fixture.reconnect()).expect("postgres task store opens");
    let executor = pg_store.executor().clone();
    let target = Arc::new(pg_store.into_task_service());
    let source_task = target
        .create(TaskCreateCommand {
            task_id: "task-pg-durable".to_string(),
            mission_id: "mission-pg-durable".to_string(),
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id: "session-pg-durable".to_string(),
            origin_turn_id: "turn-pg-durable".to_string(),
            root_task_id: "task-pg-durable".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: policy_bound_task_spec("session-pg-durable", "Verify the task control plane"),
            evidence_refs: vec![EvidenceRef::observed(
                "test_fixture",
                "test://runtime-postgres/task-durability",
            )],
        })
        .expect("PostgreSQL task starts")
        .aggregate;
    let phase = target
        .start_phase(
            &source_task.task_id,
            source_task.revision,
            TaskPhaseSpec {
                name: "postgres-verification".to_string(),
                objective: "prove PostgreSQL preserves the task record".to_string(),
                dependency_refs: Vec::new(),
                plan: vec!["persist task state".to_string()],
                acceptance: vec!["restart equality".to_string()],
                test_commands: vec!["real PostgreSQL task test".to_string()],
            },
            Vec::new(),
        )
        .expect("PostgreSQL phase starts")
        .aggregate;
    let phase_id = phase.phases.last().expect("phase exists").phase_id.clone();
    target
        .record_phase_artifact(
            &source_task.task_id,
            phase.revision,
            &phase_id,
            "evidence",
            "durability",
            "PostgreSQL state is canonical",
            Vec::new(),
        )
        .expect("PostgreSQL artifact persists");

    let barrier = Arc::new(Barrier::new(2));
    let workers = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let target = Arc::clone(&target);
            thread::spawn(move || {
                barrier.wait();
                target.create(TaskCreateCommand {
                    task_id: "task-pg-concurrent".to_string(),
                    mission_id: "mission-pg-concurrent".to_string(),
                    kind: TaskKind::Root,
                    origin: TaskOrigin::User,
                    origin_session_id: "session-pg-concurrent".to_string(),
                    origin_turn_id: "turn-pg-concurrent".to_string(),
                    root_task_id: "task-pg-concurrent".to_string(),
                    parent_task_id: None,
                    predecessor_task_id: None,
                    mission_assignment: TaskMissionAssignment::Default,
                    mission_assigned_by: "test".to_string(),
                    spec: policy_bound_task_spec(
                        "session-pg-concurrent",
                        "one governed concurrent task",
                    ),
                    evidence_refs: Vec::new(),
                })
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().expect("task worker joins"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    let receipts = results
        .into_iter()
        .map(|result| result.expect("concurrent create replays canonical receipt"))
        .collect::<Vec<_>>();
    assert_eq!(receipts[0].receipt, receipts[1].receipt);
    assert_eq!(receipts[0].outbox, receipts[1].outbox);
    let concurrent = target
        .list()
        .expect("target task list")
        .into_iter()
        .find(|task| task.task_id == "task-pg-concurrent")
        .expect("one concurrent task persists");
    assert_eq!(concurrent.objective, "one governed concurrent task");
    assert!(target
        .create(TaskCreateCommand {
            task_id: "task-pg-concurrent".to_string(),
            mission_id: "mission-pg-concurrent".to_string(),
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id: "session-pg-concurrent".to_string(),
            origin_turn_id: "turn-pg-concurrent".to_string(),
            root_task_id: "task-pg-concurrent".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: policy_bound_task_spec("session-pg-concurrent", "a conflicting objective",),
            evidence_refs: Vec::new(),
        })
        .is_err());

    let organization = MissionOrganizationDecision {
        decision_id: "mission-organization:task-pg-concurrent".to_string(),
        workspace_id: "workspace-pg-contract".to_string(),
        root_task_id: "task-pg-concurrent".to_string(),
        affected_task_ids: vec!["task-pg-concurrent".to_string()],
        action: MissionOrganizationAction::KeepDefault,
        target_mission_id: "mission-pg-concurrent".to_string(),
        proposed_objective: None,
        status: MissionOrganizationStatus::Pending,
        reason: "verify immutable organization root".to_string(),
        candidate_count: 0,
        provider_invoked: false,
        provider_model: None,
        provider_input_tokens: 0,
        provider_output_tokens: 0,
        elapsed_ms: 0,
        rejected_reason: None,
        evidence_refs: vec![EvidenceRef::observed(
            "test_fixture",
            "test://runtime-postgres/organization-root",
        )],
        attempt: 0,
        next_attempt_at_ms: 1,
        claim_token: None,
        revision: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    target
        .save_organization_decision(&organization, None)
        .expect("initial PostgreSQL organization decision persists");
    let mut clustered_replay = organization.clone();
    clustered_replay
        .affected_task_ids
        .push("task-pg-durable".to_string());
    let retained = target
        .save_organization_decision(&clustered_replay, None)
        .expect("mutable cluster membership does not break Root idempotency");
    assert_eq!(retained, organization);
    let mut foreign_root = clustered_replay;
    foreign_root.root_task_id = "task-pg-foreign".to_string();
    assert!(target
        .save_organization_decision(&foreign_root, None)
        .is_err());

    let reopened = PostgresTaskStore::new(fixture.reconnect())
        .expect("postgres task store reopens")
        .into_task_service();
    let restored = reopened
        .list()
        .expect("reopened task list")
        .into_iter()
        .find(|task| task.task_id == source_task.task_id)
        .expect("durable task survives reopen");
    assert!(restored
        .phases
        .iter()
        .any(|candidate| candidate.phase_id == phase_id && !candidate.artifacts.is_empty()));
    assert!(executor.health().metrics.checkout_count > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_artifact_repository_preserves_selector_and_scope_contract() {
    let fixture = PostgresTestScope::new();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let executor = fixture.reconnect();
    let repository = Arc::new(
        PostgresArtifactRepository::new(executor).expect("PostgreSQL artifact migrations apply"),
    );
    let root = tempfile::tempdir().expect("artifact blob root");
    let store = runtime::ArtifactStore::new(
        root.path(),
        repository.clone(),
        runtime::ArtifactStoreConfig {
            compact_threshold_bytes: 8,
            max_object_bytes: 2 * 1024 * 1024,
            total_quota_bytes: 4 * 1024 * 1024,
            gc_high_water_bytes: 3 * 1024 * 1024,
            gc_low_water_bytes: 2 * 1024 * 1024,
            orphan_grace_ms: 0,
        },
    )
    .expect("PostgreSQL artifact store composes");
    let scope = format!("session:artifact-{suffix}");
    let artifact = store
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/octet-stream".to_string(),
                visibility_scope: scope.clone(),
                expected_bytes: Some(32),
                original_name: Some("postgres.bin".to_string()),
            },
            &[0x44; 32],
        )
        .await
        .expect("PostgreSQL artifact write");
    assert!(artifact.selector.starts_with("artifact://"));
    assert_eq!(
        store
            .read(&artifact, &scope, Some(4..12))
            .await
            .expect("PostgreSQL artifact range read"),
        vec![0x44; 8]
    );
    assert!(matches!(
        store.read(&artifact, "session:other", None).await,
        Err(runtime::ArtifactError::Unauthorized)
    ));
    let second_root = tempfile::tempdir().expect("second artifact blob root");
    let second_repository: Arc<dyn runtime::ArtifactMetadataRepository> = repository.clone();
    let second_store = runtime::ArtifactStore::new(
        second_root.path(),
        second_repository,
        runtime::ArtifactStoreConfig {
            compact_threshold_bytes: 8,
            max_object_bytes: 2 * 1024 * 1024,
            total_quota_bytes: 4 * 1024 * 1024,
            gc_high_water_bytes: 3 * 1024 * 1024,
            gc_low_water_bytes: 2 * 1024 * 1024,
            orphan_grace_ms: 0,
        },
    )
    .expect("second PostgreSQL artifact store composes");
    let repeated = second_store
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/octet-stream".to_string(),
                visibility_scope: scope.clone(),
                expected_bytes: Some(32),
                original_name: Some("postgres-repeat.bin".to_string()),
            },
            &[0x44; 32],
        )
        .await
        .expect("repeated hash repairs the selected local blob root");
    assert_eq!(
        second_store
            .read(&repeated, &scope, None)
            .await
            .expect("repaired PostgreSQL artifact read"),
        vec![0x44; 32]
    );
    store
        .pin(
            &artifact,
            "postgres-parity",
            runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        )
        .expect("PostgreSQL artifact pin");
    assert!(store.delete(&artifact, &scope).is_err());
    store
        .unpin(&artifact, "postgres-parity")
        .expect("PostgreSQL artifact unpin");
    store
        .delete(&artifact, &scope)
        .expect("PostgreSQL artifact record delete");
    second_store
        .delete(&repeated, &scope)
        .expect("second PostgreSQL artifact record delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_artifact_catalog_excludes_inflight_and_later_receipts() {
    use sha2::Digest;
    let fixture = PostgresTestScope::new();
    let scoped = fixture.reconnect();
    let repository = Arc::new(PostgresArtifactRepository::new(scoped.clone()).unwrap());
    let root = tempfile::tempdir().unwrap();
    let store = runtime::ArtifactStore::new(
        root.path(),
        repository.clone(),
        runtime::ArtifactStoreConfig::default(),
    )
    .unwrap();
    let descriptor = harness_contract::context::ArtifactWriteDescriptor {
        media_type: "text/plain".into(),
        visibility_scope: "session:catalog-pg".into(),
        expected_bytes: None,
        original_name: None,
    };
    let scopes = vec![descriptor.visibility_scope.clone()];
    let mut expected = std::collections::BTreeSet::new();
    let mut sample = None;
    for index in 0..140 {
        let reference = store
            .write_bytes(
                descriptor.clone(),
                format!("catalog source {index}").as_bytes(),
            )
            .await
            .unwrap();
        expected.insert(reference.selector.clone());
        sample = Some(reference);
    }
    let sample = sample.unwrap();
    // This transaction has inserted its row before the first page, but has
    // not committed. Committing later must not inject it into that snapshot.
    let mut pending_connection = scoped.checkout_critical().unwrap();
    let mut pending = pending_connection.transaction().unwrap();
    pending.execute("INSERT INTO artifact_records(artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms)
        SELECT 'art_inflight',sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms FROM artifact_records WHERE artifact_id=$1", &[&sample.selector.strip_prefix("artifact://").unwrap()]).unwrap();
    let mut page = store
        .discover_page(&scopes, "text/plain", None, 7)
        .await
        .unwrap();
    let first_cursor = page.next_cursor.clone().unwrap();
    pending.commit().unwrap();
    drop(pending_connection);
    let mut actual = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 30);
        for record in page.records {
            let reference = record.content_reference();
            assert!(actual.insert(reference.selector.clone()));
            let bytes = store.read(&reference, &scopes[0], None).await.unwrap();
            assert_eq!(
                format!("sha256:{:x}", sha2::Sha256::digest(&bytes)),
                reference.sha256
            );
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        store
            .write_bytes(descriptor.clone(), b"own catalog tool receipt")
            .await
            .unwrap();
        page = store
            .discover_page(&scopes, "text/plain", Some(&cursor), 7)
            .await
            .unwrap();
    }
    assert_eq!(actual, expected);
    let fresh = store
        .discover_page(&scopes, "art_inflight", None, 7)
        .await
        .unwrap();
    assert_eq!(fresh.records.len(), 1);
    assert_eq!(
        fresh.records[0].content_reference().selector,
        "artifact://art_inflight"
    );
    store.delete(&sample, &scopes[0]).unwrap();
    assert!(store
        .discover_page(&scopes, "text/plain", Some(&first_cursor), 7)
        .await
        .unwrap_err()
        .to_string()
        .contains("source changed"));
    drop(store);
    drop(repository);
    drop(scoped);
}
