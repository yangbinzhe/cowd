use super::*;
use std::sync::Arc;

#[test]
fn projection_work_class_is_nested_and_never_leaks_to_foreground_callers() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    assert_eq!(RuntimeEventStore::current_projection_work_class(), None);
    store.run_projection_work(RuntimeProjectionWorkClass::Background, || {
        assert_eq!(
            RuntimeEventStore::current_projection_work_class(),
            Some(RuntimeProjectionWorkClass::Background)
        );
        store.run_projection_work(RuntimeProjectionWorkClass::Recovery, || {
            assert_eq!(
                RuntimeEventStore::current_projection_work_class(),
                Some(RuntimeProjectionWorkClass::Recovery)
            );
        });
        assert_eq!(
            RuntimeEventStore::current_projection_work_class(),
            Some(RuntimeProjectionWorkClass::Background)
        );
    });
    assert_eq!(RuntimeEventStore::current_projection_work_class(), None);
}

#[test]
fn background_projection_connections_wait_briefly_for_sqlite_writer_handoff() {
    let store = SqliteRuntimeEventStore::try_open_in_memory().unwrap();
    let work_class = RuntimeEventStore::try_open_in_memory().unwrap();
    let timeout_ms = work_class.run_projection_work(RuntimeProjectionWorkClass::Background, || {
        let connection = store.checkout_event_connection().unwrap();
        connection
            .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, u64>(0))
            .unwrap()
    });
    assert_eq!(timeout_ms, BACKGROUND_PROJECTION_BUSY_TIMEOUT_MS);

    let foreground = store.checkout_event_connection().unwrap();
    let foreground_timeout = foreground
        .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, u64>(0))
        .unwrap();
    assert_eq!(foreground_timeout, 5_000);
}

#[test]
fn per_stream_lock_serializes_read_append_windows_without_stale_revision() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let threads = 8;
    let iterations = 20;
    let mut handles = Vec::new();
    for thread in 0..threads {
        let store = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for iteration in 0..iterations {
                let transaction_id = format!("race-{thread}-{iteration}");
                store.with_stream_lock("session:race", || {
                    let expected_revision = store.stream_revision("session:race").unwrap();
                    store
                        .append_transaction_locked(AppendTransactionRequest {
                            transaction_id,
                            expected_streams: vec![ExpectedStreamRevision {
                                stream_id: "session:race".to_string(),
                                expected_revision,
                            }],
                            events: vec![RuntimeTransactionEventInput {
                                event: input(
                                    "session:race",
                                    RuntimeEventScope::Session,
                                    "session.race_event",
                                ),
                                idempotency_key: Some(format!("race-{thread}-{iteration}")),
                                schema_version: 1,
                            }],
                        })
                        .expect("locked read+append must never observe a stale revision");
                });
            }
        }));
    }
    for handle in handles {
        handle.join().expect("race thread");
    }
    assert_eq!(
        store.stream_revision("session:race").unwrap(),
        (threads * iterations) as u64
    );
}

fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
    RuntimeEventInput {
        stream_id: stream_id.to_string(),
        scope,
        kind: kind.to_string(),
        status: Some("running".to_string()),
        actor: Some("test".to_string()),
        refs: Vec::new(),
        payload: serde_json::json!({"kind": kind}),
    }
}

fn transaction(id: &str) -> AppendTransactionRequest {
    AppendTransactionRequest {
        transaction_id: id.to_string(),
        expected_streams: vec![
            ExpectedStreamRevision {
                stream_id: "graph:g1".to_string(),
                expected_revision: 0,
            },
            ExpectedStreamRevision {
                stream_id: "node:n1".to_string(),
                expected_revision: 0,
            },
        ],
        events: vec![
            RuntimeTransactionEventInput {
                event: input(
                    "graph:g1",
                    RuntimeEventScope::ExecutionGraph,
                    "graph.started",
                ),
                idempotency_key: Some("graph-start".to_string()),
                schema_version: 1,
            },
            RuntimeTransactionEventInput {
                event: input("node:n1", RuntimeEventScope::ExecutionNode, "node.running"),
                idempotency_key: Some("node-run".to_string()),
                schema_version: 1,
            },
        ],
    }
}

fn fenced_terminal(id: &str, claim_revision: u64) -> SessionTerminalInput {
    SessionTerminalInput {
        terminal_id: format!("terminal-{id}"),
        message_id: format!("assistant-{id}"),
        session_id: format!("session-{id}"),
        execution_id: Some(format!("execution-{id}")),
        turn_id: Some(format!("turn-{id}")),
        request_id: Some(format!("request-{id}")),
        session_generation: Some(1),
        input_sequence: Some(1),
        input_claim_owner: Some("session-worker-old".to_string()),
        input_claim_token: Some(format!("claim-old-{id}")),
        input_claim_revision: Some(claim_revision),
        controlled_recovery_claim_fingerprints: Vec::new(),
        payload_ref: format!("assistant_json:\"{id}\""),
    }
}

#[test]
fn legacy_session_terminal_defaults_controlled_recovery_carrier_to_empty() {
    let terminal = serde_json::from_value::<SessionTerminalInput>(serde_json::json!({
        "terminal_id": "legacy-terminal",
        "message_id": "legacy-message",
        "session_id": "legacy-session",
        "execution_id": "legacy-execution",
        "turn_id": "legacy-turn",
        "request_id": "legacy-request",
        "session_generation": 1,
        "input_sequence": 1,
        "input_claim_owner": "legacy-worker",
        "input_claim_token": "legacy-claim",
        "input_claim_revision": 1,
        "payload_ref": "assistant_json:\"legacy\""
    }))
    .expect("legacy terminal remains readable");
    assert!(terminal.controlled_recovery_claim_fingerprints.is_empty());
}

#[test]
fn migration_snapshot_round_trip_preserves_canonical_digest_and_rejects_nonempty_target() {
    let source = RuntimeEventStore::try_open_in_memory().expect("source store");
    source
        .append_transaction_with_terminal(
            AppendTransactionRequest {
                transaction_id: "migration-round-trip".to_string(),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: "migration:stream".to_string(),
                        expected_revision: 0,
                    },
                    ExpectedStreamRevision {
                        stream_id: "migration:empty-stream".to_string(),
                        expected_revision: 0,
                    },
                ],
                events: vec![input(
                    "migration:stream",
                    RuntimeEventScope::Recovery,
                    "migration.seeded",
                )
                .into()],
            },
            SessionTerminalInput {
                terminal_id: "migration-terminal".to_string(),
                message_id: "migration-message".to_string(),
                session_id: "migration-session".to_string(),
                execution_id: Some("migration-execution".to_string()),
                turn_id: Some("migration-turn".to_string()),
                request_id: Some("migration-request".to_string()),
                session_generation: Some(1),
                input_sequence: Some(1),
                input_claim_owner: Some("migration-worker".to_string()),
                input_claim_token: Some("migration-claim".to_string()),
                input_claim_revision: Some(3),
                controlled_recovery_claim_fingerprints: Vec::new(),
                payload_ref: "assistant_json:\"done\"".to_string(),
            },
        )
        .expect("source event");
    let snapshot = source
        .export_migration_snapshot()
        .expect("export source snapshot");
    assert_eq!(snapshot.session_outbox.len(), 1);
    assert_eq!(
        snapshot.session_outbox[0].execution_id.as_deref(),
        Some("migration-execution")
    );
    assert_eq!(
        snapshot.session_outbox[0].turn_id.as_deref(),
        Some("migration-turn")
    );
    let digest = snapshot.canonical_digest().expect("source digest");
    let target = RuntimeEventStore::try_open_in_memory().expect("target store");
    target
        .import_migration_snapshot(&snapshot)
        .expect("import snapshot");
    assert_eq!(
        target
            .export_migration_snapshot()
            .expect("export target snapshot")
            .canonical_digest()
            .expect("target digest"),
        digest
    );
    assert!(target.import_migration_snapshot(&snapshot).is_err());
}

#[test]
fn multi_stream_transaction_is_atomic_and_idempotent() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let request = transaction("tx-1");
    let first = store
        .append_transaction(request.clone())
        .expect("first commit");
    let duplicate = store.append_transaction(request).expect("idempotent retry");
    assert_eq!(first.commit_cursor, duplicate.commit_cursor);
    assert!(!first.duplicate);
    assert!(duplicate.duplicate);
    assert_eq!(store.stream_revision("graph:g1").unwrap(), 1);
    assert_eq!(store.stream_revision("node:n1").unwrap(), 1);
    assert_eq!(store.all_events(100).unwrap().len(), 2);
}

#[test]
fn business_lifecycle_event_without_activity_binding_is_rejected_atomically() {
    let store = RuntimeEventStore::open_in_memory().expect("event store");
    let event = RuntimeEventInput {
        stream_id: "session:binding-gate".to_string(),
        scope: RuntimeEventScope::Tool,
        kind: "tool.invocation.started".to_string(),
        status: Some("running".to_string()),
        actor: Some("test".to_string()),
        refs: Vec::new(),
        payload: serde_json::json!({}),
    };

    let error = store
        .append(event)
        .expect_err("missing binding is rejected");
    assert!(error
        .to_string()
        .contains("requires RuntimeActivityBinding"));
    assert_eq!(store.stream_revision("session:binding-gate").unwrap(), 0);
    assert_eq!(*store.subscribe_commits().borrow(), 0);
}

#[test]
fn business_lifecycle_event_with_incomplete_identity_is_rejected_atomically() {
    let store = RuntimeEventStore::open_in_memory().expect("event store");
    let event = RuntimeEventInput {
        stream_id: "session:binding-fields-gate".to_string(),
        scope: RuntimeEventScope::Tool,
        kind: "tool.invocation.started".to_string(),
        status: Some("running".to_string()),
        actor: Some("test".to_string()),
        refs: Vec::new(),
        payload: serde_json::json!({}),
    }
    .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
        root_execution_id: "execution-binding-fields".to_string(),
        session_id: "session-binding-fields".to_string(),
        turn_id: "turn-binding-fields".to_string(),
        root_task_id: "task-binding-fields".to_string(),
        task_id: "task-binding-fields".to_string(),
        activity_id: "activity:execution:execution-binding-fields:tool:call-1".to_string(),
        node_id: None,
        parent_activity_id: None,
        initiator_activity_id: None,
        team_run_id: None,
        agent_instance_id: None,
        agent_run_id: None,
        skill_id: None,
        skill_revision: None,
        skill_activation_id: None,
        tool_contract_id: None,
        tool_call_id: None,
        approval_id: None,
        parallel_group_id: None,
        revision: 1,
        fence: 1,
        generation: 1,
    })
    .expect("base binding is structurally valid");

    let error = store
        .append(event)
        .expect_err("incomplete tool identity is rejected");

    assert!(error
        .to_string()
        .contains("missing parent_activity_id, tool_contract_id, tool_call_id"));
    assert_eq!(
        store
            .stream_revision("session:binding-fields-gate")
            .unwrap(),
        0
    );
    assert_eq!(*store.subscribe_commits().borrow(), 0);
}

#[test]
fn execution_scope_query_excludes_unrelated_session_history() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    for execution_id in ["execution-a", "execution-b"] {
        let event = input(
            &format!("{execution_id}:node:verify"),
            RuntimeEventScope::ExecutionNode,
            "node.completed",
        )
        .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
            root_execution_id: execution_id.to_string(),
            session_id: format!("session-{execution_id}"),
            turn_id: format!("turn-{execution_id}"),
            root_task_id: format!("task-{execution_id}"),
            task_id: format!("task-{execution_id}"),
            activity_id: format!("activity:execution:{execution_id}:node:verify"),
            node_id: Some("verify".to_string()),
            parent_activity_id: Some(format!("activity:execution:{execution_id}")),
            initiator_activity_id: Some(format!("activity:execution:{execution_id}")),
            team_run_id: None,
            agent_instance_id: None,
            agent_run_id: None,
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            parallel_group_id: None,
            revision: 1,
            fence: 1,
            generation: 1,
        })
        .expect("bind activity identity");
        store.append(event).expect("append scoped event");
    }

    let events = store
        .events_for_root_execution("execution-a", None, 100)
        .expect("query execution scope");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].stream_id, "execution-a:node:verify");
    assert_eq!(
        store
            .events_for_activity("activity:execution:execution-a:node:verify", None, 100,)
            .expect("query activity"),
        events
    );
    assert_eq!(
        store
            .events_for_root_execution_kind("execution-a", "node.completed", None, 100)
            .expect("query exact execution event kind"),
        events
    );
    assert!(store
        .events_for_root_execution_kind("execution-a", "node.running", None, 100)
        .expect("query absent execution event kind")
        .is_empty());
}

#[test]
fn latest_stream_kind_uses_the_exact_kind_cursor_without_reading_the_stream() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let stream_id = "projector:cursor";
    store
        .append(input(
            stream_id,
            RuntimeEventScope::Recovery,
            "projector.checkpoint",
        ))
        .expect("first checkpoint");
    store
        .append(input(
            stream_id,
            RuntimeEventScope::Recovery,
            "projector.diagnostic",
        ))
        .expect("diagnostic");
    store
        .append(input(
            stream_id,
            RuntimeEventScope::Recovery,
            "projector.checkpoint",
        ))
        .expect("second checkpoint");

    let checkpoint = store
        .latest_for_stream_kind(stream_id, "projector.checkpoint")
        .expect("checkpoint query")
        .expect("checkpoint");
    let diagnostic = store
        .latest_for_stream_kind(stream_id, "projector.diagnostic")
        .expect("diagnostic query")
        .expect("diagnostic");

    assert_eq!(checkpoint.sequence, 3);
    assert_eq!(checkpoint.kind, "projector.checkpoint");
    assert_eq!(diagnostic.sequence, 2);
    assert_eq!(diagnostic.kind, "projector.diagnostic");
    assert!(store
        .latest_for_stream_kind(stream_id, "projector.missing")
        .expect("missing query")
        .is_none());
}

#[test]
fn session_execution_events_follow_durable_terminal_graph_reference() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let graph_id = "graph:session-a";
    let child_graph_id = "graph:session-a:team";
    store
        .append(input(
            graph_id,
            RuntimeEventScope::ExecutionGraph,
            "execution_graph.planned",
        ))
        .unwrap();
    let mut child = input(
        child_graph_id,
        RuntimeEventScope::ExecutionGraph,
        "execution_graph.planned",
    );
    child.payload = serde_json::json!({
        "event": "planned",
        "graph": {"id": child_graph_id, "nodes": [{"kind": "agent_task"}]},
    });
    store.append(child).unwrap();
    let mut lineage = input(
        &format!("execution-lineage:{graph_id}"),
        RuntimeEventScope::Relation,
        "execution.lineage.child_registered.v1",
    );
    lineage.payload = serde_json::json!({
        "parent_execution_id": graph_id,
        "parent_node_id": "model",
        "child_execution_id": child_graph_id,
        "child_objective": "parallel review",
    });
    store.append(lineage).unwrap();
    let mut terminal = input(
        "session-terminal:request-a",
        RuntimeEventScope::SessionInput,
        "runtime.session.terminal_requested",
    );
    terminal.payload = serde_json::json!({"session_id": "session-a"});
    terminal.refs = vec![
        RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: graph_id.to_string(),
        },
        RuntimeEventRef {
            kind: "session".to_string(),
            id: "session-a".to_string(),
        },
    ];
    store.append(terminal).unwrap();
    let mut task = input("task:task-a", RuntimeEventScope::Task, "task.created");
    task.refs = vec![RuntimeEventRef {
        kind: "session".to_string(),
        id: "session-a".to_string(),
    }];
    store.append(task).unwrap();

    let related = store
        .execution_events_for_session("session-a", None, 20)
        .unwrap();
    assert!(related
        .iter()
        .any(|event| event.stream_id == graph_id && event.kind == "execution_graph.planned"));
    assert!(related.iter().any(|event| {
        event.stream_id == child_graph_id && event.kind == "execution_graph.planned"
    }));
    assert!(related.iter().any(|event| {
        event.stream_id == format!("execution-lineage:{graph_id}")
            && event.kind == "execution.lineage.child_registered.v1"
    }));
    assert!(related
        .iter()
        .any(|event| event.kind == "runtime.session.terminal_requested"));
    assert!(related
        .iter()
        .any(|event| event.stream_id == "task:task-a" && event.kind == "task.created"));
    assert!(store
        .execution_events_for_session("session-b", None, 20)
        .unwrap()
        .is_empty());
}

#[test]
fn schema_v6_backfills_terminal_session_reference() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime-events.sqlite");
    {
        let store = RuntimeEventStore::try_open(&path).expect("event store");
        let mut terminal = input(
            "session-terminal:legacy-request",
            RuntimeEventScope::SessionInput,
            "runtime.session.terminal_requested",
        );
        terminal.payload = serde_json::json!({"session_id": "legacy-session"});
        terminal.refs = vec![RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: "legacy-graph".to_string(),
        }];
        store.append(terminal).expect("legacy terminal");
    }
    {
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 5).unwrap();
    }

    let migrated = RuntimeEventStore::try_open(&path).expect("migrated store");
    let events = migrated
        .execution_events_for_session("legacy-session", None, 10)
        .expect("session events");
    assert!(events
        .iter()
        .any(|event| event.kind == "runtime.session.terminal_requested"));
    let refs = migrated
        .all_events(10)
        .unwrap()
        .into_iter()
        .find(|event| event.kind == "runtime.session.terminal_requested")
        .unwrap()
        .refs;
    assert!(refs
        .iter()
        .any(|reference| { reference.kind == "session" && reference.id == "legacy-session" }));
}

#[test]
fn scope_replay_crosses_backend_page_boundaries_without_truncation() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let event_count = SCOPE_REPLAY_PAGE_SIZE + 3;
    for index in 0..event_count {
        store
            .append(input(
                &format!("approval:{index}"),
                RuntimeEventScope::Approval,
                "approval.seeded",
            ))
            .expect("scope event");
    }

    let replayed = store
        .replay_scope(RuntimeEventScope::Approval)
        .expect("scope replay");
    assert_eq!(replayed.len(), event_count);
    assert!(replayed.windows(2).all(|events| {
        (events[0].commit_cursor, events[0].transaction_index)
            < (events[1].commit_cursor, events[1].transaction_index)
    }));
}

#[test]
fn scope_kind_replay_uses_kind_boundary_without_losing_commit_order() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    for index in 0..(SCOPE_REPLAY_PAGE_SIZE + 3) {
        let kind = if index % 2 == 0 {
            "evolution.release.assignment_authorized"
        } else {
            "evolution.signal.projector.checkpoint.v1"
        };
        store
            .append(input(
                &format!("evolution:{index}"),
                RuntimeEventScope::Evolution,
                kind,
            ))
            .expect("scope event");
    }

    let replayed = store
        .replay_scope_kind(
            RuntimeEventScope::Evolution,
            "evolution.release.assignment_authorized",
        )
        .expect("scope-kind replay");
    assert_eq!(replayed.len(), (SCOPE_REPLAY_PAGE_SIZE + 4) / 2);
    assert!(replayed
        .iter()
        .all(|event| event.kind == "evolution.release.assignment_authorized"));
    assert!(replayed.windows(2).all(|events| {
        (events[0].commit_cursor, events[0].transaction_index)
            < (events[1].commit_cursor, events[1].transaction_index)
    }));
}

#[test]
fn scope_stream_prefix_replay_excludes_unrelated_aggregate_families() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    for index in 0..(SCOPE_REPLAY_PAGE_SIZE + 3) {
        let stream = if index % 2 == 0 {
            format!("evolution:candidate:{index}")
        } else {
            format!("evolution:signal:{index}")
        };
        store
            .append(input(
                &stream,
                RuntimeEventScope::Evolution,
                "evolution.test",
            ))
            .expect("scope event");
    }

    let replayed = store
        .replay_scope_stream_prefix(RuntimeEventScope::Evolution, "evolution:candidate:")
        .expect("scope-prefix replay");
    assert_eq!(replayed.len(), (SCOPE_REPLAY_PAGE_SIZE + 4) / 2);
    assert!(replayed
        .iter()
        .all(|event| event.stream_id.starts_with("evolution:candidate:")));
    assert!(replayed.windows(2).all(|events| {
        (events[0].commit_cursor, events[0].transaction_index)
            < (events[1].commit_cursor, events[1].transaction_index)
    }));
}

#[test]
fn transaction_id_reuse_with_different_request_is_rejected() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let request = transaction("tx-conflict");
    store.append_transaction(request.clone()).expect("commit");
    let mut changed = request;
    changed.events[0].event.kind = "graph.changed".to_string();
    assert!(matches!(
        store.append_transaction(changed),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));
}

#[test]
fn stale_revision_rolls_back_entire_transaction_without_visible_cursor() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    store
        .append(input(
            "node:n1",
            RuntimeEventScope::ExecutionNode,
            "node.created",
        ))
        .expect("seed");
    let before = store.events_after_cursor(0, 100).unwrap();
    let request = transaction("tx-stale");
    assert!(matches!(
        store.append_transaction(request),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));
    let after = store.events_after_cursor(0, 100).unwrap();
    assert_eq!(before, after);
    assert!(store.list_stream("graph:g1").unwrap().is_empty());
}

#[test]
fn cursor_pagination_never_splits_a_transaction() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let first = store.append_transaction(transaction("tx-page-1")).unwrap();
    let second = store
        .append_batch_if_revision(
            "graph:g1",
            1,
            "tx-page-2",
            vec![input(
                "graph:g1",
                RuntimeEventScope::ExecutionGraph,
                "graph.completed",
            )
            .into()],
        )
        .unwrap();
    let page = store.events_after_cursor(0, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].commit_cursor, first.commit_cursor);
    assert_eq!(page[0].events.len(), 2);
    let next = store.events_after_cursor(first.commit_cursor, 1).unwrap();
    assert_eq!(next[0].commit_cursor, second.commit_cursor);
    assert_eq!(next[0].events.len(), 1);
}

#[test]
fn legacy_version_zero_database_is_migrated_without_losing_events() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
            "CREATE TABLE runtime_events (
                event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                scope TEXT NOT NULL, kind TEXT NOT NULL, status TEXT, actor TEXT,
                payload TEXT NOT NULL, refs TEXT NOT NULL, created_at_ms INTEGER NOT NULL
             );
             INSERT INTO runtime_events VALUES
                ('old-1', 'mission:m1', 1, 'mission', 'mission.started', 'running', NULL, '{}', '[]', 1);",
        )
        .unwrap();
    drop(conn);

    let store = RuntimeEventStore::try_open(&path).expect("legacy migration");
    let events = store.list_stream("mission:m1").unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, "old-1");
    assert!(events[0].commit_cursor > 0);
    let conn = Connection::open(path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, STORE_SCHEMA_VERSION);
}

#[test]
fn historical_session_command_scope_remains_replayable_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite");
    let store = RuntimeEventStore::try_open(&path).expect("event store opens");
    store
        .append(input(
            "session-command:legacy-1",
            RuntimeEventScope::SessionCommand,
            "session_execution.dispatched",
        ))
        .expect("historical command event persists");
    drop(store);

    let reopened = RuntimeEventStore::try_open(&path)
        .expect("historical session command scope remains readable");
    let events = reopened
        .list_scope(RuntimeEventScope::SessionCommand, 10)
        .expect("historical command scope lists");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].scope, RuntimeEventScope::SessionCommand);
    assert_eq!(events[0].kind, "session_execution.dispatched");
}

#[test]
fn unknown_legacy_scope_aborts_migration_and_preserves_version_zero() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE runtime_events (
                event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                scope TEXT NOT NULL, kind TEXT NOT NULL, status TEXT, actor TEXT,
                payload TEXT NOT NULL, refs TEXT NOT NULL, created_at_ms INTEGER NOT NULL
             );
             INSERT INTO runtime_events VALUES
                ('bad-1', 'bad:x', 1, 'unknown_scope', 'bad', NULL, NULL, '{}', '[]', 1);",
    )
    .unwrap();
    drop(conn);

    assert!(matches!(
        RuntimeEventStore::try_open(&path),
        Err(RuntimeEventStoreError::UnknownScope(_))
    ));
    let conn = Connection::open(path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 0);
    assert!(!table_has_column(&conn, "runtime_events", "commit_cursor").unwrap());
}

#[test]
fn legacy_terminal_outbox_schema_gains_nullable_execution_relation() {
    let mut conn = Connection::open_in_memory().expect("sqlite opens");
    conn.execute_batch(
        "CREATE TABLE runtime_session_outbox (
                terminal_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL UNIQUE,
                session_id TEXT NOT NULL,
                commit_cursor INTEGER NOT NULL,
                payload_ref TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER,
                claim_owner TEXT,
                claim_expires_at INTEGER,
                failure_class TEXT,
                last_error TEXT,
                materialized_at INTEGER,
                revision INTEGER NOT NULL DEFAULT 0
            );",
    )
    .expect("legacy outbox schema");
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .expect("migration transaction");
    create_current_tables(&tx).expect("additive schema migration");
    tx.commit().expect("migration commits");
    assert!(table_has_column(&conn, "runtime_session_outbox", "execution_id").unwrap());
    assert!(table_has_column(&conn, "runtime_session_outbox", "turn_id").unwrap());
}

#[test]
fn idempotency_key_can_resolve_the_committed_side_effect() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    store.append_transaction(transaction("tx-idem")).unwrap();
    let event = store
        .event_by_idempotency_key("node:n1", "node-run")
        .unwrap()
        .expect("idempotent event");
    assert_eq!(event.kind, "node.running");
}

#[test]
fn decision_lease_consumption_is_durable_and_replay_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime-events.db");
    let store = RuntimeEventStore::try_open(&path).expect("event store");
    store
        .consume_verified_decision_lease(
            "lease-1",
            "human-1",
            "candidate:c-1",
            "promote",
            "evolution.candidate:c-1",
            "sha256:evidence",
            2,
            10,
        )
        .expect("first consumption");
    assert!(matches!(
        store.consume_verified_decision_lease(
            "lease-1",
            "human-1",
            "candidate:c-1",
            "promote",
            "evolution.candidate:c-1",
            "sha256:evidence",
            2,
            11,
        ),
        Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed { .. })
    ));
    drop(store);
    let reopened = RuntimeEventStore::try_open(&path).expect("reopen");
    assert!(matches!(
        reopened.consume_verified_decision_lease(
            "lease-1",
            "human-1",
            "candidate:c-1",
            "promote",
            "evolution.candidate:c-1",
            "sha256:evidence",
            2,
            12,
        ),
        Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed { .. })
    ));
}

#[test]
fn terminal_outbox_reclaims_expired_lease_and_materializes_once() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    store
        .enqueue_session_terminal("t1", "m1", "s1", 7, "e:1")
        .unwrap();
    let first = store.claim_session_terminals("a", 100, 50, 8).unwrap();
    assert_eq!(first.len(), 1);
    assert!(store
        .claim_session_terminals("b", 149, 50, 8)
        .unwrap()
        .is_empty());
    let reclaimed = store.claim_session_terminals("b", 150, 50, 8).unwrap();
    let done = store
        .ack_session_terminal("t1", "b", reclaimed[0].revision, 151)
        .unwrap();
    assert_eq!(done.status, "materialized");
    assert!(store
        .claim_session_terminals("c", 1_000, 50, 8)
        .unwrap()
        .is_empty());
}

#[test]
fn terminal_drain_probe_tracks_unmaterialized_session_work() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    assert!(!store
        .has_unsettled_session_terminals("drain-session")
        .unwrap());
    store
        .enqueue_session_terminal(
            "drain-terminal",
            "drain-message",
            "drain-session",
            7,
            "assistant_json:\"done\"",
        )
        .unwrap();
    assert!(store
        .has_unsettled_session_terminals("drain-session")
        .unwrap());
    assert!(!store
        .has_unsettled_session_terminals("other-session")
        .unwrap());

    let claim = store
        .claim_session_terminals("drain-worker", 100, 50, 1)
        .unwrap()
        .remove(0);
    assert!(store
        .has_unsettled_session_terminals("drain-session")
        .unwrap());
    store
        .ack_session_terminal(&claim.terminal_id, "drain-worker", claim.revision, 101)
        .unwrap();
    assert!(!store
        .has_unsettled_session_terminals("drain-session")
        .unwrap());
}

#[test]
fn materialized_terminal_replay_is_scoped_cursor_ordered_and_excludes_pending() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    for (terminal, message, session, cursor) in [
        ("t-old", "m-old", "s-a", 4_u64),
        ("t-new", "m-new", "s-a", 9_u64),
        ("t-other", "m-other", "s-b", 12_u64),
        ("t-pending", "m-pending", "s-a", 15_u64),
    ] {
        store
            .enqueue_session_terminal(terminal, message, session, cursor, "assistant_json:\"ok\"")
            .unwrap();
    }
    let claims = store
        .claim_session_terminals("worker", 100, 50, 10)
        .unwrap();
    for terminal in ["t-old", "t-new", "t-other"] {
        let claimed = claims
            .iter()
            .find(|record| record.terminal_id == terminal)
            .unwrap();
        store
            .ack_session_terminal(terminal, "worker", claimed.revision, 101)
            .unwrap();
    }

    let replay = store
        .materialized_session_terminals_after("s-a", 4, 10)
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].terminal_id, "t-new");
    assert_eq!(replay[0].commit_cursor, 9);
}

#[test]
fn terminal_outbox_retries_then_blocks_and_rejects_conflict() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    store
        .enqueue_session_terminal("t2", "m2", "s2", 8, "e:2")
        .unwrap();
    assert!(matches!(
        store.enqueue_session_terminal("t2", "different", "s2", 8, "e:2"),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));
    let first = store
        .claim_session_terminals("w", 200, 50, 1)
        .unwrap()
        .pop()
        .unwrap();
    let retry = store
        .fail_session_terminal(
            "t2",
            "w",
            first.revision,
            RuntimeSessionOutboxFailureClass::Retryable,
            "temporary",
            300,
            2,
            201,
        )
        .unwrap();
    assert_eq!(retry.status, "retry_scheduled");
    let second = store
        .claim_session_terminals("w", 300, 50, 1)
        .unwrap()
        .pop()
        .unwrap();
    let blocked = store
        .fail_session_terminal(
            "t2",
            "w",
            second.revision,
            RuntimeSessionOutboxFailureClass::Retryable,
            "still unavailable",
            400,
            2,
            301,
        )
        .unwrap();
    assert_eq!(blocked.status, "blocked");
}

#[test]
fn terminal_outbox_permanent_failure_never_retries() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    store
        .enqueue_session_terminal("t3", "m3", "s3", 9, "e:3")
        .unwrap();
    let claim = store
        .claim_session_terminals("w", 400, 50, 1)
        .unwrap()
        .pop()
        .unwrap();
    let blocked = store
        .fail_session_terminal(
            "t3",
            "w",
            claim.revision,
            RuntimeSessionOutboxFailureClass::CorruptPayload,
            "invalid payload",
            500,
            10,
            401,
        )
        .unwrap();
    assert_eq!(blocked.status, "blocked");
    assert!(store
        .claim_session_terminals("w", 10_000, 50, 1)
        .unwrap()
        .is_empty());
    assert_eq!(store.blocked_session_terminals(10).unwrap().len(), 1);
    let retried = store
        .retry_session_terminal("t3", "operator", "payload repaired", 10_001)
        .unwrap();
    assert_eq!(retried.status, "retry_scheduled");
}

#[test]
fn terminal_outbox_survives_restart_and_two_workers_claim_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime-events.db");
    {
        let store = RuntimeEventStore::open(&path).unwrap();
        store
            .enqueue_session_terminal("restart", "m4", "s4", 10, "assistant_json:\"ok\"")
            .unwrap();
    }
    let store = Arc::new(RuntimeEventStore::open(&path).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut handles = Vec::new();
    for worker in ["worker-a", "worker-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            store.claim_session_terminals(worker, 100, 50, 1).unwrap()
        }));
    }
    barrier.wait();
    let claimed = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        claimed.len(),
        1,
        "concurrent workers must claim exactly once"
    );
}

#[test]
fn terminal_transaction_requires_a_complete_fence_and_binds_replay_identity() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    let request = transaction("terminal-fence-identity");
    let terminal = fenced_terminal("identity", 2);
    let first = store
        .append_transaction_with_terminal(request.clone(), terminal.clone())
        .expect("fenced terminal commits");
    assert!(!first.duplicate);
    let replay = store
        .append_transaction_with_terminal(request.clone(), terminal.clone())
        .expect("exact terminal replay is idempotent");
    assert!(replay.duplicate);

    let mut conflicting = terminal.clone();
    conflicting.terminal_id = "terminal-conflict".to_string();
    conflicting.message_id = "assistant-conflict".to_string();
    assert!(matches!(
        store.append_transaction_with_terminal(request, conflicting),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));
    assert!(store
        .session_terminal("terminal-conflict")
        .unwrap()
        .is_none());

    let mut unfenced = terminal;
    unfenced.input_claim_token = None;
    assert!(matches!(
        RuntimeEventStore::try_open_in_memory()
            .unwrap()
            .append_transaction_with_terminal(transaction("terminal-unfenced"), unfenced),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));
}

#[test]
fn retried_terminal_ack_after_materialization_is_idempotent() {
    let store = RuntimeEventStore::try_open_in_memory().unwrap();
    store
        .append_transaction_with_terminal(
            transaction("terminal-retry-ack"),
            fenced_terminal("retry-ack", 1),
        )
        .expect("terminal commits");
    let claimed = store
        .claim_session_terminals("worker-a", 100, 50, 1)
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    let revision = claimed[0].revision;
    let acked = store
        .ack_session_terminal("terminal-retry-ack", "worker-a", revision, 101)
        .expect("first ack");
    assert_eq!(acked.status, "materialized");
    // A retried acknowledgement carrying the pre-ack revision must be
    // treated as idempotent success instead of a stale-revision failure.
    let retried = store
        .ack_session_terminal("terminal-retry-ack", "worker-a", revision, 102)
        .expect("retried ack is idempotent");
    assert_eq!(retried.status, "materialized");
    assert!(retried.revision >= acked.revision);
}

#[test]
fn expired_terminal_delivery_adopts_the_reclaimed_session_fence_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime-terminal-adoption.db");
    let terminal = fenced_terminal("adoption", 2);
    {
        let store = RuntimeEventStore::open(&path).unwrap();
        store
            .append_transaction_with_terminal(transaction("terminal-adoption"), terminal.clone())
            .unwrap();
        let delivery = store
            .claim_session_terminals("delivery-before-crash", 100, 50, 1)
            .unwrap()
            .remove(0);
        assert_eq!(delivery.revision, 1);
    }

    let store = RuntimeEventStore::open(&path).unwrap();
    let current = store
        .session_terminal(&terminal.terminal_id)
        .unwrap()
        .unwrap();
    let adoption = RuntimeSessionTerminalFenceAdoption {
        terminal_id: terminal.terminal_id.clone(),
        expected_terminal_revision: current.revision,
        request_id: terminal.request_id.clone().unwrap(),
        session_id: terminal.session_id.clone(),
        turn_id: terminal.turn_id.clone().unwrap(),
        session_generation: 1,
        input_sequence: terminal.input_sequence.unwrap(),
        claim_owner: "session-worker-reclaimed".to_string(),
        claim_token: "claim-reclaimed".to_string(),
        claim_revision: 5,
        claim_expires_at_ms: 1_000,
        adopted_at_ms: 150,
    };
    let adopted = store
        .adopt_session_terminal_fence(&adoption)
        .expect("expired delivery adopts live Session fence");
    assert_eq!(adopted.status, "pending");
    assert_eq!(
        adopted.input_claim_owner.as_deref(),
        Some("session-worker-reclaimed")
    );
    assert_eq!(
        adopted.input_claim_token.as_deref(),
        Some("claim-reclaimed")
    );
    assert_eq!(adopted.input_claim_revision, Some(5));
    assert_eq!(adopted.claim_owner, None);
    assert_eq!(adopted.claim_expires_at_ms, None);
    let replay = store
        .append_transaction_with_terminal(transaction("terminal-adoption"), terminal.clone())
        .expect("initial transaction remains idempotent after fence adoption");
    assert!(replay.duplicate);

    let duplicate = store
        .adopt_session_terminal_fence(&adoption)
        .expect("same desired fence is idempotent despite old CAS revision");
    assert_eq!(duplicate.revision, adopted.revision);

    let claimed = store
        .claim_session_terminals("delivery-after-adoption", 151, 50, 1)
        .unwrap()
        .remove(0);
    assert_eq!(claimed.input_claim_revision, Some(5));
    let mut stale = adoption.clone();
    stale.expected_terminal_revision = claimed.revision;
    stale.claim_token = "claim-stale".to_string();
    stale.claim_revision = 4;
    assert!(matches!(
        store.adopt_session_terminal_fence(&stale),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));

    let materialized = store
        .ack_session_terminal(
            &claimed.terminal_id,
            claimed.claim_owner.as_deref().unwrap(),
            claimed.revision,
            152,
        )
        .unwrap();
    let mut after_materialized = adoption;
    after_materialized.expected_terminal_revision = materialized.revision;
    after_materialized.claim_token = "claim-newer".to_string();
    after_materialized.claim_revision = 6;
    assert!(matches!(
        store.adopt_session_terminal_fence(&after_materialized),
        Err(RuntimeEventStoreError::InvalidTransaction(_))
    ));
}

#[test]
fn projection_checkpoints_are_monotonic_mutable_state_not_committed_events() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let commits = store.subscribe_commits();
    let initial_commit_cursor = *commits.borrow();
    let first = store
        .put_projection_checkpoint(
            "projector:test-a",
            7,
            &serde_json::json!({"cursor": 7}),
            100,
        )
        .expect("first checkpoint");
    assert_eq!(first.revision, 1);
    assert_eq!(*commits.borrow(), initial_commit_cursor);
    assert!(store.all_events(10).unwrap().is_empty());

    let duplicate = store
        .put_projection_checkpoint(
            "projector:test-a",
            7,
            &serde_json::json!({"cursor": 7}),
            101,
        )
        .expect("exact replay is idempotent");
    assert_eq!(duplicate.revision, first.revision);
    assert_eq!(duplicate.updated_at_ms, first.updated_at_ms);
    assert!(matches!(
        store.put_projection_checkpoint(
            "projector:test-a",
            6,
            &serde_json::json!({"cursor": 6}),
            102,
        ),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));
    assert!(matches!(
        store.put_projection_checkpoint(
            "projector:test-a",
            7,
            &serde_json::json!({"cursor": "conflict"}),
            103,
        ),
        Err(RuntimeEventStoreError::TransactionConflict { .. })
    ));

    let advanced = store
        .put_projection_checkpoint(
            "projector:test-a",
            8,
            &serde_json::json!({"cursor": 8}),
            104,
        )
        .expect("advance checkpoint");
    assert_eq!(advanced.revision, 2);
    assert_eq!(
        store
            .projection_checkpoints_with_prefix("projector:test")
            .unwrap(),
        vec![advanced.clone()]
    );
    assert_eq!(*commits.borrow(), initial_commit_cursor);
    assert!(store.all_events(10).unwrap().is_empty());

    let same_source = store
        .compare_and_put_projection_checkpoint(
            "projector:test-a",
            8,
            advanced.revision,
            &serde_json::json!({"cursor": 8, "live_revision": 2}),
            105,
        )
        .expect("mutable live state may advance at the same source cursor under CAS");
    assert_eq!(same_source.source_cursor, 8);
    assert_eq!(same_source.revision, advanced.revision + 1);
    assert!(matches!(
        store.compare_and_put_projection_checkpoint(
            "projector:test-a",
            8,
            advanced.revision,
            &serde_json::json!({"cursor": 8, "live_revision": 3}),
            106,
        ),
        Err(RuntimeEventStoreError::StaleRevision { .. })
    ));
    assert!(store
        .delete_projection_checkpoint("projector:test-a")
        .expect("delete checkpoint"));
    assert!(store
        .projection_checkpoint("projector:test-a")
        .unwrap()
        .is_none());
}

#[test]
fn legacy_live_snapshot_history_migrates_only_active_state_and_removes_orphans() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite");
    let store = RuntimeEventStore::try_open(&path).expect("event store");
    for (execution_id, status) in [
        ("execution-active", "calling_model"),
        ("execution-terminal", "complete"),
    ] {
        let mut snapshot = input(
            &format!("execution-live:{execution_id}"),
            RuntimeEventScope::ExecutionLive,
            "execution.live.snapshot.v1",
        );
        snapshot.status = Some(status.to_string());
        snapshot.payload = serde_json::json!({
            "execution_id": execution_id,
            "session_id": "session-legacy-live",
            "live": {"status": status}
        });
        store.append(snapshot).expect("legacy live snapshot");
    }
    drop(store);
    let legacy = Connection::open(&path).expect("legacy database");
    legacy
        .pragma_update(None, "user_version", STORE_SCHEMA_VERSION - 1)
        .expect("mark previous schema");
    drop(legacy);

    let migrated = RuntimeEventStore::try_open(&path).expect("migrated store");
    assert!(migrated
        .projection_checkpoint("execution-live:execution-active")
        .unwrap()
        .is_some());
    assert!(migrated
        .projection_checkpoint("execution-live:execution-terminal")
        .unwrap()
        .is_none());
    assert!(migrated
        .all_events(10)
        .unwrap()
        .iter()
        .all(|event| event.kind != "execution.live.snapshot.v1"));
    drop(migrated);

    let connection = Connection::open(path).unwrap();
    let orphan_heads: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_stream_heads AS head
                  WHERE NOT EXISTS (
                      SELECT 1 FROM runtime_events AS event
                       WHERE event.stream_id=head.stream_id
                  )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(orphan_heads, 0);
}

#[test]
fn legacy_mission_checkpoint_migrates_the_inner_projection_and_removes_orphans() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite");
    let store = RuntimeEventStore::try_open(&path).expect("event store");
    let mut checkpoint = input(
        "mission-evidence-projector",
        RuntimeEventScope::Recovery,
        "mission_evidence.projector.checkpoint.v1",
    );
    checkpoint.payload = serde_json::json!({
        "type": "MissionEvidenceCheckpoint",
        "projection": {
            "source_cursor": 17,
            "revision": 4,
            "projected_at_ms": 100,
            "records": {},
            "dlq_count": 0
        }
    });
    store.append(checkpoint).expect("legacy checkpoint");
    drop(store);
    let legacy = Connection::open(&path).expect("legacy database");
    legacy
        .pragma_update(None, "user_version", STORE_SCHEMA_VERSION - 1)
        .expect("mark database as the previous schema");
    drop(legacy);

    let migrated = RuntimeEventStore::try_open(&path).expect("migrated store");
    let checkpoint = migrated
        .projection_checkpoint("projector:mission-evidence")
        .expect("checkpoint query")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.source_cursor, 17);
    assert_eq!(checkpoint.payload["source_cursor"], 17);
    assert_eq!(checkpoint.payload["revision"], 4);
    assert!(checkpoint.payload.get("projection").is_none());
    assert!(migrated.all_events(10).unwrap().is_empty());
    drop(migrated);

    let connection = Connection::open(path).unwrap();
    let orphan_commits: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_commits AS committed
                  WHERE NOT EXISTS (
                      SELECT 1 FROM runtime_events AS event
                       WHERE event.transaction_id=committed.transaction_id
                  )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let orphan_streams: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_transaction_streams AS stream
                  WHERE NOT EXISTS (
                      SELECT 1 FROM runtime_events AS event
                       WHERE event.transaction_id=stream.transaction_id
                  )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(orphan_commits, 0);
    assert_eq!(orphan_streams, 0);
}

#[tokio::test]
async fn commit_subscription_wakes_after_a_new_durable_commit() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let mut commits = store.subscribe_commits();
    let event = store
        .append(RuntimeEventInput {
            stream_id: "commit-watch".to_string(),
            scope: RuntimeEventScope::Mission,
            kind: "mission.commit_watch.v1".to_string(),
            status: Some("committed".to_string()),
            actor: Some("test".to_string()),
            refs: Vec::new(),
            payload: serde_json::Value::Null,
        })
        .expect("append");
    tokio::time::timeout(std::time::Duration::from_secs(1), commits.changed())
        .await
        .expect("commit notification")
        .expect("watch remains open");
    assert_eq!(*commits.borrow(), event.commit_cursor);
}

#[test]
fn projection_scan_filters_payloads_but_advances_across_unrelated_commits() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    let unrelated = store
        .append(input(
            "task:unrelated",
            RuntimeEventScope::Task,
            "task.noise",
        ))
        .expect("unrelated event");
    let matched = store
        .append(input(
            "mission:matched",
            RuntimeEventScope::Mission,
            "mission.target",
        ))
        .expect("matched event");
    let trailing = store
        .append(input(
            "skill:unrelated",
            RuntimeEventScope::Skill,
            "skill.noise",
        ))
        .expect("trailing event");
    let interest = RuntimeProjectionInterest::new([RuntimeProjectionEventInterest::new(
        RuntimeEventScope::Mission,
        "mission.target",
    )]);

    let page = store
        .projection_scan_page(0, &interest, 3, 16, 1024 * 1024)
        .expect("projection page");

    assert_eq!(page.scanned_commits, 3);
    assert_eq!(page.scanned_through_cursor, trailing.commit_cursor);
    assert_eq!(page.matched_events, 1);
    assert_eq!(page.batches.len(), 1);
    assert_eq!(page.batches[0].commit_cursor, matched.commit_cursor);
    assert_eq!(page.batches[0].events[0].kind, "mission.target");
    assert!(page.batches[0].commit_cursor > unrelated.commit_cursor);
}

#[test]
fn projection_scan_crosses_large_unrelated_windows_without_materializing_noise() {
    let store = RuntimeEventStore::try_open_in_memory().expect("event store");
    for index in 0..1_000 {
        store
            .append(input(
                &format!("noise:{index}"),
                RuntimeEventScope::Task,
                "task.noise",
            ))
            .expect("noise event");
    }
    let target = store
        .append(input(
            "mission:target",
            RuntimeEventScope::Mission,
            "mission.target",
        ))
        .expect("target event");
    let interest = RuntimeProjectionInterest::new([RuntimeProjectionEventInterest::new(
        RuntimeEventScope::Mission,
        "mission.target",
    )]);
    let first = store
        .projection_scan_page(0, &interest, 1_000, 8, 1024 * 1024)
        .expect("noise page");
    assert_eq!(first.scanned_commits, 1_000);
    assert_eq!(first.matched_events, 0);
    assert!(first.batches.is_empty());
    let second = store
        .projection_scan_page(first.scanned_through_cursor, &interest, 8, 8, 1024 * 1024)
        .expect("target page");
    assert_eq!(second.scanned_through_cursor, target.commit_cursor);
    assert_eq!(second.matched_events, 1);
    assert_eq!(second.batches[0].events[0].event_id, target.event_id);
}
