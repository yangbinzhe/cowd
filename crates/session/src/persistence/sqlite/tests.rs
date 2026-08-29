use super::*;
use tempfile::tempdir;

fn make_store() -> (SqliteSessionStore, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sessions.db");
    let store = SqliteSessionStore::open(&path).expect("open session store");
    (store, dir)
}

fn make_record(id: &str) -> SessionRecord {
    SessionRecord {
        session_id: id.to_string(),
        platform: "test".to_string(),
        chat_id: "chat-1".to_string(),
        user_id: Some("user-1".to_string()),
        model: None,
        created_at: "2024-01-01T00:00:00Z".to_string(),
        last_activity: "2024-01-01T00:01:00Z".to_string(),
        message_count: 1,
        reset_policy: "None".to_string(),
        metadata_json: None,
        input_tokens: 0,
        output_tokens: 0,
        status: "active".to_string(),
    }
}

#[test]
fn test_create_and_get() {
    let (store, _dir) = make_store();
    let rec = make_record("session-001");
    store.create_session(&rec).unwrap();
    let loaded = store.get_session("session-001").unwrap().unwrap();
    assert_eq!(loaded.session_id, "session-001");
    assert_eq!(loaded.platform, "test");
    assert_eq!(loaded.message_count, 1);
}

#[test]
fn create_session_populates_millisecond_timestamps() {
    let (store, _dir) = make_store();
    let rec = make_record("session-ms");
    store.create_session(&rec).unwrap();
    let conn = store.conn().unwrap();
    let (created_at_ms, updated_at_ms): (i64, i64) = conn
        .query_row(
            "SELECT created_at_ms, updated_at_ms FROM sessions WHERE session_id = ?1",
            params!["session-ms"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(created_at_ms, 1_704_067_200_000);
    assert_eq!(updated_at_ms, 1_704_067_260_000);
}

#[test]
fn test_update_session() {
    let (store, _dir) = make_store();
    let mut rec = make_record("session-002");
    store.create_session(&rec).unwrap();
    rec.message_count = 42;
    rec.last_activity = "2024-01-02T00:00:00Z".to_string();
    store.update_session(&rec).unwrap();
    let loaded = store.get_session("session-002").unwrap().unwrap();
    assert_eq!(loaded.message_count, 42);
}

#[test]
fn test_upsert_session() {
    let (store, _dir) = make_store();
    let mut rec = make_record("session-003");
    store.upsert_session(&rec).unwrap();
    rec.message_count = 99;
    store.upsert_session(&rec).unwrap();
    let loaded = store.get_session("session-003").unwrap().unwrap();
    assert_eq!(loaded.message_count, 99);
}

#[test]
fn test_delete_session() {
    let (store, _dir) = make_store();
    let rec = make_record("session-004");
    store.create_session(&rec).unwrap();
    store.delete_session("session-004").unwrap();
    assert!(store.get_session("session-004").unwrap().is_none());
}

#[test]
fn test_list_sessions() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s1")).unwrap();
    store.create_session(&make_record("s2")).unwrap();
    let list = store.list_sessions().unwrap();
    assert_eq!(list.len(), 2);
}

#[test]
fn scoped_message_search_preserves_authorized_results_when_other_sessions_rank_first() {
    let (store, _dir) = make_store();
    for session_id in ["foreign", "authorized"] {
        store.create_session(&make_record(session_id)).unwrap();
    }
    for (session_id, sequence) in [("foreign", 0), ("authorized", 0)] {
        store
            .insert_message(&SessionMessage {
                stable_message_id: format!("{session_id}:{sequence}"),
                session_id: session_id.to_string(),
                sequence,
                role: "user".to_string(),
                content_json: r#"[{"type":"text","text":"tenant ranked search phrase"}]"#
                    .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 1,
            })
            .unwrap();
    }

    let results = store
        .search_messages_in_sessions("tenant", &["authorized".to_string()], 1)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].session_id, "authorized");
}

#[test]
fn list_sessions_by_workspace_root_filters_and_orders_by_activity() {
    let (store, _dir) = make_store();
    let workspace_a = "/tmp/cowd-workspace-a";
    let workspace_b = "/tmp/cowd-workspace-b";

    let mut older = make_record("workspace-a-older");
    older.last_activity = "2024-01-01T00:00:00Z".to_string();
    older.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
    store.create_session(&older).unwrap();

    let mut newer = make_record("workspace-a-newer");
    newer.last_activity = "2024-01-02T00:00:00Z".to_string();
    newer.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
    store.create_session(&newer).unwrap();

    let mut other_workspace = make_record("workspace-b");
    other_workspace.last_activity = "2024-01-03T00:00:00Z".to_string();
    other_workspace.metadata_json =
        Some(serde_json::json!({"workspace_root": workspace_b}).to_string());
    store.create_session(&other_workspace).unwrap();

    let records = store
        .list_sessions_by_workspace_root(workspace_a)
        .expect("workspace sessions should list");

    assert_eq!(
        records
            .iter()
            .map(|record| record.session_id.as_str())
            .collect::<Vec<_>>(),
        vec!["workspace-a-newer", "workspace-a-older"]
    );
}

#[test]
fn list_sessions_page_filters_sorts_and_counts_at_scale() {
    let (store, _dir) = make_store();
    {
        let mut conn = store.conn().unwrap();
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                    .prepare(
                        r"INSERT INTO sessions
                           (session_id, platform, chat_id, user_id, model,
                            created_at, last_activity, message_count, reset_policy, metadata_json,
                            input_tokens, output_tokens, status)
                           VALUES (?1, 'api_server', ?1, NULL, ?2, ?3, ?3, ?4, 'none', ?5, 0, 0, ?6)",
                    )
                    .unwrap();
            for i in 0..10_000 {
                let model = if i % 2 == 0 {
                    "claude-sonnet-4-6"
                } else {
                    "claude-haiku-4-5"
                };
                let status = if i % 3 == 0 { "active" } else { "closed" };
                let ts = format!(
                    "2026-06-04T{:02}:{:02}:{:02}Z",
                    (i / 3600) % 24,
                    (i / 60) % 60,
                    i % 60
                );
                let title =
                    serde_json::json!({"title": format!("Perf Session {i:05}")}).to_string();
                stmt.execute(params![
                    format!("perf-{i:05}"),
                    model,
                    ts,
                    i as i64,
                    title,
                    status
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    let page = store
        .list_sessions_page(&SessionListOptions {
            model: Some("claude-sonnet-4-6"),
            status: Some("active"),
            unrestricted: true,
            sort: "last_activity",
            order: "desc",
            limit: 7,
            offset: 0,
            ..SessionListOptions::default()
        })
        .unwrap();

    assert_eq!(page.total, 1667);
    assert_eq!(page.records.len(), 7);
    assert!(page
        .records
        .windows(2)
        .all(|pair| pair[0].last_activity >= pair[1].last_activity));
    assert!(page
        .records
        .iter()
        .all(|r| r.model.as_deref() == Some("claude-sonnet-4-6") && r.status == "active"));
}

#[test]
fn list_sessions_page_applies_owner_grants_and_tombstone_visibility_in_sql() {
    let (store, _dir) = make_store();
    for (id, owner, status) in [
        ("owned", "principal-a", "active"),
        ("granted", "principal-b", "closed"),
        ("hidden", "principal-b", "active"),
        ("deleted", "principal-a", "deleted"),
    ] {
        let mut record = make_record(id);
        record.status = status.to_string();
        record.metadata_json = Some(serde_json::json!({"owner_principal_id": owner}).to_string());
        store.create_session(&record).unwrap();
    }

    let grants = vec!["granted".to_string()];
    let page = store
        .list_sessions_page(&SessionListOptions {
            owner_principal_id: Some("principal-a"),
            visible_session_ids: &grants,
            sort: "last_activity",
            order: "desc",
            limit: 20,
            ..SessionListOptions::default()
        })
        .unwrap();
    let ids = page
        .records
        .iter()
        .map(|record| record.session_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(page.total, 2);
    assert_eq!(ids, std::collections::BTreeSet::from(["granted", "owned"]));
}

#[test]
fn list_sessions_page_escapes_like_wildcards() {
    let (store, _dir) = make_store();
    let mut literal = make_record("literal-percent");
    literal.metadata_json = Some(serde_json::json!({"title":"Auth% Literal"}).to_string());
    store.create_session(&literal).unwrap();

    let mut wildcard = make_record("wildcard-match");
    wildcard.metadata_json = Some(serde_json::json!({"title":"Auth Wildcard"}).to_string());
    store.create_session(&wildcard).unwrap();

    let page = store
        .list_sessions_page(&SessionListOptions {
            query: Some("Auth%"),
            unrestricted: true,
            limit: 20,
            ..SessionListOptions::default()
        })
        .unwrap();

    assert_eq!(page.total, 1);
    assert_eq!(page.records[0].session_id, "literal-percent");
}

#[test]
fn status_model_recent_session_query_uses_composite_index() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-index")).unwrap();
    let conn = store.conn().unwrap();
    let mut stmt = conn
        .prepare(
            r"EXPLAIN QUERY PLAN
                  SELECT session_id FROM sessions
                  WHERE status = ?1 COLLATE NOCASE AND model = ?2 COLLATE NOCASE
                  ORDER BY last_activity DESC
                  LIMIT 20 OFFSET 0",
        )
        .unwrap();
    let plan: Vec<String> = stmt
        .query_map(params!["active", "claude-sonnet-4-6"], |row| row.get(3))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let plan_text = plan.join(" | ");
    assert!(
        plan_text.contains("idx_sessions_status_model_last_activity"),
        "expected composite index in query plan, got: {plan_text}"
    );
}

#[test]
fn get_events_limited_pages_from_sequence_and_counts_total() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-events")).unwrap();
    for i in 0..1000 {
        store
            .append_event(&SessionEvent {
                session_id: "s-events".to_string(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({"sequence": i}).to_string(),
                sequence: i,
                created_at_ms: i as u64,
            })
            .unwrap();
    }

    let events = store.get_events_limited("s-events", 990, 5).unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(events[0].sequence, 990);
    assert_eq!(events[4].sequence, 994);
    assert_eq!(store.count_events_from("s-events", 990).unwrap(), 10);
}

#[test]
fn get_events_by_type_pages_context_envelopes_only() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-context-events"))
        .unwrap();
    for (sequence, event_type) in [
        (0, "TextDelta"),
        (1, "ContextEnvelope"),
        (2, "ToolStart"),
        (3, "ContextEnvelope"),
    ] {
        store
            .append_event(&SessionEvent {
                session_id: "s-context-events".to_string(),
                event_type: event_type.to_string(),
                event_json: serde_json::json!({
                    "envelope_id": format!("env-{sequence}"),
                    "envelope": {"id": format!("env-{sequence}")}
                })
                .to_string(),
                sequence,
                created_at_ms: sequence as u64,
            })
            .unwrap();
    }

    let events = store
        .get_events_by_type_limited("s-context-events", "ContextEnvelope", 0, 10)
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[1].sequence, 3);
    assert_eq!(
        store
            .count_events_by_type_from("s-context-events", "ContextEnvelope", 0)
            .unwrap(),
        2
    );
}

#[test]
fn get_context_event_by_envelope_id_reads_json_payload() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-context-id")).unwrap();
    store
        .append_event(&SessionEvent {
            session_id: "s-context-id".to_string(),
            event_type: "ContextEnvelope".to_string(),
            event_json: serde_json::json!({
                "envelope_id": "env-target",
                "envelope": {"id": "env-target", "intent": "ship"}
            })
            .to_string(),
            sequence: 7,
            created_at_ms: 7,
        })
        .unwrap();

    let event = store
        .get_context_event_by_envelope_id("env-target")
        .unwrap()
        .expect("context event");
    assert_eq!(event.session_id, "s-context-id");
    assert_eq!(event.sequence, 7);
    assert!(event.event_json.contains("ship"));
}

#[test]
fn append_context_envelope_event_if_absent_skips_duplicate_envelope_id() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-context-once"))
        .unwrap();
    let first = SessionEvent {
        session_id: "s-context-once".to_string(),
        event_type: "ContextEnvelope".to_string(),
        event_json: serde_json::json!({
            "envelope_id": "env-once",
            "envelope": {"id": "env-once", "intent": "first"}
        })
        .to_string(),
        sequence: 1,
        created_at_ms: 1,
    };
    let duplicate = SessionEvent {
        sequence: 2,
        created_at_ms: 2,
        event_json: serde_json::json!({
            "envelope_id": "env-once",
            "envelope": {"id": "env-once", "intent": "duplicate"}
        })
        .to_string(),
        ..first.clone()
    };

    assert!(store
        .append_context_envelope_event_if_absent(&first)
        .unwrap());
    assert!(!store
        .append_context_envelope_event_if_absent(&duplicate)
        .unwrap());

    let events = store
        .get_events_by_type_limited("s-context-once", "ContextEnvelope", 0, 10)
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 0);
    assert!(events[0].event_json.contains("first"));
}

#[test]
fn delete_events_from_removes_tail_only() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-events-delete"))
        .unwrap();
    for i in 0..5 {
        store
            .append_event(&SessionEvent {
                session_id: "s-events-delete".to_string(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({"sequence": i}).to_string(),
                sequence: i,
                created_at_ms: i as u64,
            })
            .unwrap();
    }

    assert_eq!(store.delete_events_from("s-events-delete", 3).unwrap(), 2);
    let events = store.get_events("s-events-delete", 0).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].sequence, 0);
    assert_eq!(events[2].sequence, 2);
}

#[test]
fn delete_events_by_type_from_preserves_other_event_types() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-events-delete-type"))
        .unwrap();
    for (sequence, event_type) in [
        (0, "message_appended"),
        (1, "TextDelta"),
        (2, "message_appended"),
        (3, "ToolStart"),
    ] {
        store
            .append_event(&SessionEvent {
                session_id: "s-events-delete-type".to_string(),
                event_type: event_type.to_string(),
                event_json: serde_json::json!({"sequence": sequence}).to_string(),
                sequence,
                created_at_ms: sequence as u64,
            })
            .unwrap();
    }

    assert_eq!(
        store
            .delete_events_by_type_from("s-events-delete-type", "message_appended", 0)
            .unwrap(),
        2
    );
    let events = store.get_events("s-events-delete-type", 0).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "TextDelta");
    assert_eq!(events[1].event_type, "ToolStart");
}

#[test]
fn next_event_sequence_uses_max_sequence_plus_one() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-next-event")).unwrap();
    assert_eq!(store.next_event_sequence("s-next-event").unwrap(), 0);

    for sequence in [0, 5, 2] {
        store
            .append_event(&SessionEvent {
                session_id: "s-next-event".to_string(),
                event_type: "TextDelta".to_string(),
                event_json: serde_json::json!({"sequence": sequence}).to_string(),
                sequence,
                created_at_ms: sequence as u64,
            })
            .unwrap();
    }

    assert_eq!(store.next_event_sequence("s-next-event").unwrap(), 6);
}

#[test]
fn allocating_sequence_appends_contiguous_batch_atomically() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-atomic-batch"))
        .unwrap();
    let events = ["first", "second", "third"].map(|event_type| SessionEvent {
        session_id: "s-atomic-batch".to_string(),
        event_type: event_type.to_string(),
        event_json: "{}".to_string(),
        sequence: usize::MAX,
        created_at_ms: 1,
    });

    let appended = store
        .append_events_allocating_sequence(&events)
        .expect("atomic batch should append");
    assert_eq!(
        appended
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(store.get_events("s-atomic-batch", 0).unwrap().len(), 3);
}

#[test]
fn allocating_sequence_is_atomic_across_parallel_sqlite_connections() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-parallel-sqlite"))
        .unwrap();
    let store = std::sync::Arc::new(store);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(100));
    let mut workers = Vec::new();
    for index in 0..100usize {
        let store = std::sync::Arc::clone(&store);
        let barrier = std::sync::Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            store
                .append_event_allocating_sequence(&SessionEvent {
                    session_id: "s-parallel-sqlite".to_string(),
                    event_type: "parallel".to_string(),
                    event_json: format!(r#"{{"index":{index}}}"#),
                    sequence: usize::MAX,
                    created_at_ms: index as u64,
                })
                .unwrap()
                .sequence
        }));
    }
    let mut sequences = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    assert_eq!(sequences, (0..100).collect::<Vec<_>>());
}

#[test]
fn session_event_sequence_constraint_rejects_duplicate() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-unique-event"))
        .unwrap();
    let event = SessionEvent {
        session_id: "s-unique-event".to_string(),
        event_type: "first".to_string(),
        event_json: "{}".to_string(),
        sequence: 0,
        created_at_ms: 1,
    };
    store.append_event(&event).unwrap();
    let mut duplicate = event;
    duplicate.event_type = "duplicate".to_string();
    assert!(store.append_event(&duplicate).is_err());
    assert_eq!(store.get_events("s-unique-event", 0).unwrap().len(), 1);
}

#[test]
fn allocating_batch_rolls_back_when_runtime_envelope_is_invalid() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-batch-rollback"))
        .unwrap();
    let events = vec![
        SessionEvent {
            session_id: "s-batch-rollback".to_string(),
            event_type: "normal".to_string(),
            event_json: "{}".to_string(),
            sequence: usize::MAX,
            created_at_ms: 1,
        },
        SessionEvent {
            session_id: "s-batch-rollback".to_string(),
            event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: "not-json".to_string(),
            sequence: usize::MAX,
            created_at_ms: 2,
        },
    ];
    assert!(store.append_events_allocating_sequence(&events).is_err());
    assert!(store.get_events("s-batch-rollback", 0).unwrap().is_empty());
}

#[test]
fn checkpoint_batch_timestamp_overflow_rolls_back_without_partial_event() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-checkpoint-timestamp-overflow"))
        .unwrap();
    let checkpoint_id = "checkpoint-timestamp-overflow";
    let event = SessionEvent {
        session_id: "s-checkpoint-timestamp-overflow".to_string(),
        event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
        event_json: serde_json::json!({
            "kind": "memory.semantic_checkpoint.created",
            "payload": {"checkpoint": {"checkpoint_id": checkpoint_id}},
        })
        .to_string(),
        sequence: usize::MAX,
        created_at_ms: u64::MAX,
    };

    assert!(store
        .append_events_allocating_sequence_if_checkpoint_absent(&[event], checkpoint_id)
        .is_err());
    assert!(store
        .get_events("s-checkpoint-timestamp-overflow", 0)
        .unwrap()
        .is_empty());
}

#[test]
fn event_page_query_uses_session_sequence_index() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-event-index")).unwrap();
    let conn = store.conn().unwrap();
    let mut stmt = conn
        .prepare(
            r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
        )
        .unwrap();
    let plan: Vec<String> = stmt
        .query_map(params!["s-event-index", 100_i64, 20_i64], |row| row.get(3))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let plan_text = plan.join(" | ");
    assert!(
        plan_text.contains("idx_session_events_session_seq")
            || plan_text.contains("uq_session_events_session_sequence"),
        "expected event sequence index in query plan, got: {plan_text}"
    );
}

#[test]
fn event_type_page_query_uses_session_type_sequence_index() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-context-event-index"))
        .unwrap();
    let conn = store.conn().unwrap();
    let mut stmt = conn
        .prepare(
            r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3
                  ORDER BY sequence ASC
                  LIMIT ?4",
        )
        .unwrap();
    let plan: Vec<String> = stmt
        .query_map(
            params!["s-context-event-index", "ContextEnvelope", 100_i64, 20_i64],
            |row| row.get(3),
        )
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let plan_text = plan.join(" | ");
    assert!(
        plan_text.contains("idx_session_events_session_type_seq"),
        "expected context event type index in query plan, got: {plan_text}"
    );
}

#[test]
fn context_envelope_lookup_uses_envelope_id_index() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-context-envelope-index"))
        .unwrap();
    let conn = store.conn().unwrap();
    let mut stmt = conn
        .prepare(
            r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE event_type = 'ContextEnvelope'
                    AND json_extract(event_json, '$.envelope.id') = ?1
                  ORDER BY created_at_ms DESC
                  LIMIT 1",
        )
        .unwrap();
    let plan: Vec<String> = stmt
        .query_map(params!["env-indexed"], |row| row.get(3))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let plan_text = plan.join(" | ");
    assert!(
        plan_text.contains("idx_session_events_context_envelope_id"),
        "expected context envelope id index in query plan, got: {plan_text}"
    );
}

#[test]
fn get_messages_from_sequence_pages_100k_history() {
    let (store, _dir) = make_store();
    let mut record = make_record("s-100k");
    record.message_count = 100_000;
    store.create_session(&record).unwrap();
    {
        let mut conn = store.conn().unwrap();
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                    .prepare(
                        r"INSERT INTO messages
                           (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                            tool_use_id, tool_name, token_usage_json, created_at_ms)
                           VALUES (printf('bulk:%d', ?1), 's-100k', ?1, ?2, ?3, 1, NULL, NULL, NULL, ?4)",
                    )
                    .unwrap();
            for i in 0..100_000 {
                let role = if i % 2 == 0 { "user" } else { "assistant" };
                let content =
                    serde_json::json!([{"type":"text","text":format!("message {i}")}]).to_string();
                stmt.execute(params![i as i64, role, content, i as i64])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    let page = store
        .get_messages_from_sequence("s-100k", 99_950, 50)
        .unwrap();
    assert_eq!(page.len(), 50);
    assert_eq!(page[0].sequence, 99_950);
    assert_eq!(page[49].sequence, 99_999);
    let outbox_rows: i64 = store
        .conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_context_index_outbox WHERE session_id='s-100k'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        outbox_rows, 1,
        "append indexing must remain O(1) per Session"
    );
}

#[test]
fn exact_message_reads_and_metadata_page_preserve_stable_identity() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-exact-message"))
        .unwrap();
    for sequence in 0..3 {
        store
            .insert_message(&SessionMessage {
                stable_message_id: format!("exact-{sequence}"),
                session_id: "s-exact-message".to_string(),
                sequence,
                role: if sequence % 2 == 0 {
                    "user"
                } else {
                    "assistant"
                }
                .to_string(),
                content_json: serde_json::json!([
                    {"type":"text","text":format!("payload-{sequence}")}
                ])
                .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: sequence as u64,
            })
            .unwrap();
    }

    assert_eq!(
        store
            .get_message_by_stable_id("s-exact-message", "exact-1")
            .unwrap()
            .unwrap()
            .sequence,
        1
    );
    assert_eq!(
        store
            .get_message_by_sequence("s-exact-message", 2)
            .unwrap()
            .unwrap()
            .stable_message_id,
        "exact-2"
    );
    let metadata = store
        .get_message_metadata_page("s-exact-message", 1, 2)
        .unwrap();
    assert_eq!(
        metadata
            .iter()
            .map(|message| message.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(metadata.iter().all(|message| message.content_bytes > 0));
}

#[test]
fn latest_checkpoint_lookup_uses_full_index_beyond_legacy_page_boundary() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-late-checkpoint"))
        .unwrap();
    {
        let mut conn = store.conn().unwrap();
        let tx = conn.transaction().unwrap();
        {
            let mut statement = tx
                .prepare(
                    r"INSERT INTO session_events(
                               session_id, event_type, event_json, sequence, created_at_ms
                           ) VALUES (
                               's-late-checkpoint', 'SessionDomainEvent', ?1, ?2, ?2
                           )",
                )
                .unwrap();
            for sequence in 0..5_000 {
                let kind = if sequence == 4_999 {
                    "memory.semantic_checkpoint.created"
                } else {
                    "runtime.progress"
                };
                let event_json = serde_json::json!({
                    "event_id": format!("event-{sequence}"),
                    "session_id": "s-late-checkpoint",
                    "sequence": sequence,
                    "scope": "runtime",
                    "kind": kind,
                    "payload": {},
                    "created_at_ms": sequence,
                })
                .to_string();
                statement
                    .execute(params![event_json, sequence as i64])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    let latest = store
        .get_latest_session_domain_event_by_kind(
            "s-late-checkpoint",
            "memory.semantic_checkpoint.created",
        )
        .unwrap()
        .unwrap();
    assert_eq!(latest.sequence, 4_999);
    let manifest = store
        .get_session_recovery_manifest("s-late-checkpoint")
        .unwrap()
        .unwrap();
    assert_eq!(manifest.event_cursor, 5_000);
    assert_eq!(manifest.latest_checkpoint_sequence, Some(4_999));
    assert_eq!(
        manifest.latest_checkpoint_event_id.as_deref(),
        Some("event-4999")
    );
}

#[test]
fn context_index_reconciliation_is_complete_idempotent_and_repairable() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-context-index"))
        .unwrap();
    for sequence in 0..513 {
        store
            .insert_message(&SessionMessage {
                stable_message_id: format!("index-{sequence}"),
                session_id: "s-context-index".to_string(),
                sequence,
                role: "user".to_string(),
                content_json: serde_json::json!([
                    {"type":"text","text":format!("indexed payload {sequence}")}
                ])
                .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: sequence as u64,
            })
            .unwrap();
    }
    let first = store
        .reconcile_session_context_index("s-context-index", 128, 4, 1_000)
        .unwrap();
    assert!(first.complete);
    assert_eq!(first.source_messages, 513);
    assert_eq!(first.covered_messages, 513);
    assert_eq!(first.indexed_through_sequence, Some(512));
    assert!(!first.source_digest.is_empty());
    {
        let conn = store.conn().unwrap();
        conn.execute(
            "DELETE FROM session_context_index_cards
                  WHERE card_id=(
                      SELECT card_id FROM session_context_index_cards
                       WHERE session_id='s-context-index' LIMIT 1
                  )",
            [],
        )
        .unwrap();
    }
    let repaired = store
        .reconcile_session_context_index("s-context-index", 128, 4, 2_000)
        .unwrap();
    assert!(repaired.complete);
    assert_eq!(repaired.source_digest, first.source_digest);
    assert_eq!(repaired.generation, first.generation + 1);
    assert_eq!(
        store
            .get_context_index_cards("s-context-index", 64)
            .unwrap()
            .len(),
        repaired.card_count
    );
}

#[test]
fn missing_manifest_rebuilds_from_authoritative_history_and_checkpoint() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-manifest-rebuild"))
        .unwrap();
    store
        .insert_message(&SessionMessage {
            stable_message_id: "manifest-message".to_string(),
            session_id: "s-manifest-rebuild".to_string(),
            sequence: 0,
            role: "user".to_string(),
            content_json: r#"[{"type":"text","text":"authoritative"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 10,
        })
        .unwrap();
    store
        .append_event(&SessionEvent {
            session_id: "s-manifest-rebuild".to_string(),
            event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: serde_json::json!({
                "event_id": "checkpoint-rebuild",
                "session_id": "s-manifest-rebuild",
                "sequence": 0,
                "scope": "runtime",
                "kind": "memory.semantic_checkpoint.created",
                "payload": {},
                "created_at_ms": 11
            })
            .to_string(),
            sequence: 0,
            created_at_ms: 11,
        })
        .unwrap();
    let conn = store.conn().unwrap();
    conn.execute(
        "DELETE FROM session_recovery_manifest WHERE session_id='s-manifest-rebuild'",
        [],
    )
    .unwrap();
    drop(conn);

    let rebuilt = store
        .rebuild_session_recovery_manifest("s-manifest-rebuild", 12)
        .unwrap()
        .unwrap();
    assert_eq!(rebuilt.durable_cursor, 1);
    assert_eq!(rebuilt.event_cursor, 1);
    assert_eq!(rebuilt.transcript_messages, 1);
    assert_eq!(rebuilt.latest_checkpoint_sequence, Some(0));
    assert_eq!(
        rebuilt.latest_checkpoint_event_id.as_deref(),
        Some("checkpoint-rebuild")
    );
    assert!(rebuilt.index_pending);
    let pending: i64 = store
        .conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_context_index_outbox
                  WHERE session_id='s-manifest-rebuild' AND status='pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 1);
}

#[test]
fn semantic_checkpoint_alone_enqueues_context_index_reconciliation() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-checkpoint-index-outbox"))
        .unwrap();
    store
        .append_event(&SessionEvent {
            session_id: "s-checkpoint-index-outbox".to_string(),
            event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: serde_json::json!({
                "event_id": "checkpoint-only",
                "session_id": "s-checkpoint-index-outbox",
                "sequence": 0,
                "scope": "runtime",
                "kind": "memory.semantic_checkpoint.created",
                "payload": {},
                "created_at_ms": 20
            })
            .to_string(),
            sequence: 0,
            created_at_ms: 20,
        })
        .unwrap();
    let manifest = store
        .get_session_recovery_manifest("s-checkpoint-index-outbox")
        .unwrap()
        .unwrap();
    assert!(manifest.index_pending);
    let pending: i64 = store
        .conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_context_index_outbox
                  WHERE session_id='s-checkpoint-index-outbox' AND status='pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 1);
}

#[test]
fn message_sequence_page_query_uses_session_sequence_index() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-message-index"))
        .unwrap();
    let conn = store.conn().unwrap();
    let mut stmt = conn
        .prepare(
            r"EXPLAIN QUERY PLAN
                  SELECT session_id, sequence, role, content_json,
                         blocks_count, tool_use_id, tool_name,
                         token_usage_json, created_at_ms
                  FROM messages
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
        )
        .unwrap();
    let plan: Vec<String> = stmt
        .query_map(params!["s-message-index", 99_950_i64, 50_i64], |row| {
            row.get(3)
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let plan_text = plan.join(" | ");
    assert!(
        plan_text.contains("idx_messages_session_seq"),
        "expected message sequence index in query plan, got: {plan_text}"
    );
}

#[test]
fn branch_copy_uses_stable_cutoff_and_rejects_nonempty_target() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("branch-source")).unwrap();
    store.create_session(&make_record("branch-target")).unwrap();
    for sequence in 0..3 {
        store
            .insert_message(&SessionMessage {
                stable_message_id: format!("source-{sequence}"),
                session_id: "branch-source".to_string(),
                sequence,
                role: "user".to_string(),
                content_json: format!(r#"[{{"type":"text","text":"{sequence}"}}]"#),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: sequence as u64,
            })
            .unwrap();
    }

    let copied = store
        .copy_session_messages_at_cutoff("branch-source", "branch-target", 2)
        .unwrap();
    assert_eq!(copied, 2);
    let target = store.get_all_messages("branch-target").unwrap();
    assert_eq!(target.len(), 2);
    assert_eq!(target[0].stable_message_id, "branch:branch-target:source-0");
    assert_eq!(target[1].sequence, 1);
    assert!(store
        .copy_session_messages_at_cutoff("branch-source", "branch-target", 3)
        .is_err());
    assert_eq!(store.get_message_count("branch-source").unwrap(), 3);
}

#[test]
fn test_list_by_platform() {
    let (store, _dir) = make_store();
    let mut rec = make_record("s-tg");
    rec.platform = "telegram".to_string();
    store.create_session(&rec).unwrap();
    store.create_session(&make_record("s-test")).unwrap();
    let tg = store.list_sessions_by_platform("telegram").unwrap();
    assert_eq!(tg.len(), 1);
    assert_eq!(tg[0].session_id, "s-tg");
}

#[test]
fn test_memory_associations() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-mem")).unwrap();
    store.associate_memory("s-mem", "mem-1").unwrap();
    store.associate_memory("s-mem", "mem-2").unwrap();
    // Idempotent
    store.associate_memory("s-mem", "mem-1").unwrap();
    let mems = store.get_session_memories("s-mem").unwrap();
    assert_eq!(mems.len(), 2);
    store.disassociate_memory("s-mem", "mem-1").unwrap();
    let mems = store.get_session_memories("s-mem").unwrap();
    assert_eq!(mems.len(), 1);
    assert_eq!(mems[0], "mem-2");
}

#[test]
fn conn_sets_busy_timeout() {
    let store = SqliteSessionStore::open_in_memory().unwrap();
    let conn = store.conn().unwrap();
    let timeout: i32 = conn
        .pragma_query_value(None, "busy_timeout", |row| row.get(0))
        .unwrap();
    assert!(timeout > 0, "busy_timeout should be > 0, got {}", timeout);
}

#[test]
fn open_migrates_legacy_message_block_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("legacy-sessions.db");
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
                r#"
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY,
                    platform TEXT DEFAULT '',
                    chat_id TEXT DEFAULT '',
                    user_id TEXT DEFAULT '',
                    model TEXT,
                    created_at TEXT NOT NULL DEFAULT '',
                    last_activity TEXT NOT NULL DEFAULT '',
                    message_count INTEGER DEFAULT 0,
                    reset_policy TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT DEFAULT '{}',
                    created_at_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    usage_input INTEGER DEFAULT 0,
                    usage_output INTEGER DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(session_id, sequence)
                );
                CREATE TABLE message_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL,
                    block_order INTEGER NOT NULL,
                    block_type TEXT NOT NULL,
                    text TEXT,
                    signature TEXT,
                    tool_id TEXT,
                    tool_name TEXT,
                    tool_input TEXT,
                    tool_output TEXT,
                    is_error INTEGER DEFAULT 0,
                    created_at_ms INTEGER NOT NULL
                );
                INSERT INTO sessions(session_id, message_count) VALUES ('legacy', 1);
                INSERT INTO messages(session_id, sequence, role, created_at_ms)
                    VALUES ('legacy', 0, 'user', 1);
                INSERT INTO message_blocks(message_id, session_id, block_order, block_type, text, created_at_ms)
                    VALUES (1, 'legacy', 0, 'text', 'resume survives migration', 1);
                "#,
            )
            .unwrap();
    }

    let store = SqliteSessionStore::open(&db).unwrap();
    let messages = store.get_all_messages("legacy").unwrap();
    assert_eq!(messages.len(), 1);
    assert!(messages[0]
        .content_json
        .contains("resume survives migration"));
    store
        .insert_message(&SessionMessage {
            stable_message_id: "legacy:new-write".to_string(),
            session_id: "legacy".to_string(),
            sequence: 1,
            role: "assistant".to_string(),
            content_json: r#"[{"type":"text","text":"new write works"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 2,
        })
        .unwrap();
    assert_eq!(store.get_message_count("legacy").unwrap(), 2);
}

#[test]
fn open_repairs_legacy_duplicate_event_sequences_without_dropping_events() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("duplicate-events.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
            r#"
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                user_id TEXT,
                model TEXT,
                created_at TEXT NOT NULL,
                last_activity TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                reset_policy TEXT NOT NULL,
                metadata_json TEXT,
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at_ms INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE session_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_json TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            INSERT INTO sessions(session_id, platform, chat_id, created_at, last_activity, reset_policy)
            VALUES ('duplicate-session', 'test', 'chat', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', 'None');
            INSERT INTO session_events(session_id, event_type, event_json, sequence, created_at_ms)
            VALUES ('duplicate-session', 'one', '{}', 0, 1),
                   ('duplicate-session', 'two', '{}', 0, 2);
            "#,
        )
        .unwrap();
    drop(conn);

    let store = SqliteSessionStore::open(&path).expect("legacy events are resequenced");
    let events = store.get_events("duplicate-session", 0).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| {
                serde_json::from_str::<serde_json::Value>(&event.event_json)
                    .ok()
                    .and_then(|value| value["sequence"].as_u64())
            })
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1)]
    );
}

#[test]
fn test_prune_before() {
    let (store, _dir) = make_store();
    let mut old = make_record("old-session");
    old.last_activity = "2020-01-01T00:00:00Z".to_string();
    store.create_session(&old).unwrap();
    store.create_session(&make_record("new-session")).unwrap();
    let removed = store.prune_before("2021-01-01T00:00:00Z").unwrap();
    assert_eq!(removed, 1);
    assert!(store.get_session("old-session").unwrap().is_none());
    assert!(store.get_session("new-session").unwrap().is_some());
}

fn outbox_message(session_id: &str) -> SessionMessage {
    SessionMessage {
        stable_message_id: "message-1".to_string(),
        session_id: session_id.to_string(),
        sequence: 0,
        role: "user".to_string(),
        content_json: r#"[{"type":"text","text":"run this"}]"#.to_string(),
        blocks_count: 1,
        tool_use_id: None,
        tool_name: None,
        token_usage_json: None,
        created_at_ms: 100,
    }
}

fn outbox_request() -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: "input-1".to_string(),
        request_id: "request-1".to_string(),
        turn_id: "turn-1".to_string(),
        message_id: "message-1".to_string(),
        session_generation: 1,
        decision: InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: Some(r#"{"code":"new_turn"}"#.to_string()),
        task_route_hint: None,
        created_at_ms: 100,
        runtime_options_json: None,
    }
}

fn ingress_request(
    id: &str,
    generation: u64,
    decision: InputRoutingDecision,
    target_turn_id: Option<&str>,
    created_at_ms: u64,
) -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: format!("input-{id}"),
        request_id: format!("request-{id}"),
        turn_id: format!("turn-{id}"),
        message_id: format!("message-{id}"),
        session_generation: generation,
        decision,
        target_turn_id: target_turn_id.map(str::to_string),
        classification_json: Some(format!(r#"{{"classification":"{id}"}}"#)),
        task_route_hint: None,
        created_at_ms,
        runtime_options_json: None,
    }
}

#[test]
fn source_message_and_outbox_are_atomic_and_idempotent() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-outbox")).unwrap();
    let message = outbox_message("s-outbox");
    let request = outbox_request();

    let first = store
        .append_message_with_runtime_outbox(&message, &request)
        .unwrap();
    let duplicate = store
        .append_message_with_runtime_outbox(&message, &request)
        .unwrap();
    assert_eq!(first, duplicate);
    assert_eq!(first.status, SessionRuntimeInputStatus::Queued);
    assert_eq!(first.input_id, "input-1");
    assert_eq!(first.revision, 2);
    assert_eq!(
        store
            .get_session_runtime_outbox_by_input_id("input-1")
            .unwrap(),
        Some(first.clone())
    );
    let timeline = store
        .get_session_domain_timeline_limited("s-outbox", 0, 10)
        .unwrap();
    assert_eq!(timeline.len(), 3);
    assert_eq!(
        timeline
            .iter()
            .map(|event| { SessionDomainEvent::from_session_event(event).unwrap().kind })
            .collect::<Vec<_>>(),
        vec![
            SessionRuntimeInputStatus::Accepted
                .timeline_event_kind()
                .to_string(),
            SessionRuntimeInputStatus::Classified
                .timeline_event_kind()
                .to_string(),
            SessionRuntimeInputStatus::Queued
                .timeline_event_kind()
                .to_string(),
        ]
    );
    assert_eq!(store.get_message_count("s-outbox").unwrap(), 1);
    assert_eq!(
        store
            .get_session("s-outbox")
            .unwrap()
            .unwrap()
            .message_count,
        1
    );

    let mut conflicting = request;
    conflicting.turn_id = "turn-other".to_string();
    assert!(store
        .append_message_with_runtime_outbox(&message, &conflicting)
        .is_err());
    assert_eq!(store.get_message_count("s-outbox").unwrap(), 1);
}

#[test]
fn classifier_rejections_are_auditable_terminal_inputs_and_never_runnable() {
    for (suffix, decision, expected_status) in [
        (
            "duplicate",
            InputRoutingDecision::RejectDuplicate,
            SessionRuntimeInputStatus::RejectedDuplicate,
        ),
        (
            "policy",
            InputRoutingDecision::RejectPolicy,
            SessionRuntimeInputStatus::RejectedPolicy,
        ),
    ] {
        let (store, _dir) = make_store();
        let session_id = format!("s-reject-{suffix}");
        store.create_session(&make_record(&session_id)).unwrap();
        let request = ingress_request(suffix, 1, decision, None, 100);

        let stored = store
            .append_ingress_with_runtime_outbox(
                &session_id,
                "user",
                Some(r#"[{"type":"text","text":"classified rejection"}]"#),
                100,
                &request,
            )
            .expect("rejection is durable, not a validation error");
        assert_eq!(stored.status, expected_status);
        assert!(stored.status.is_terminal());
        assert_eq!(stored.terminal_at_ms, Some(100));
        assert_eq!(store.get_message_count(&session_id).unwrap(), 1);
        assert!(store
            .claim_session_runtime_outbox("worker", 100, 1_000, 10)
            .unwrap()
            .is_empty());
        assert!(store.active_session_runtime_outbox(10).unwrap().is_empty());

        let timeline = store
            .get_session_domain_timeline_limited(&session_id, 0, 10)
            .unwrap()
            .into_iter()
            .map(|event| SessionDomainEvent::from_session_event(&event).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(timeline.len(), 3);
        assert_eq!(
            timeline
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            vec![
                SessionRuntimeInputStatus::Accepted.timeline_event_kind(),
                SessionRuntimeInputStatus::Classified.timeline_event_kind(),
                expected_status.timeline_event_kind(),
            ]
        );
        assert_eq!(
            timeline.last().and_then(|event| event.status.as_deref()),
            Some(expected_status.as_str())
        );
        assert_eq!(
            timeline
                .last()
                .and_then(|event| event.payload["decision"].as_str()),
            Some(input_decision_as_str(decision))
        );

        let health = store.session_runtime_outbox_health().unwrap();
        assert_eq!(
            health.rejected_duplicate,
            usize::from(decision == InputRoutingDecision::RejectDuplicate)
        );
        assert_eq!(
            health.rejected_policy,
            usize::from(decision == InputRoutingDecision::RejectPolicy)
        );
    }
}

#[test]
fn runtime_options_remain_opaque_and_durable_with_session_ingress() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-runtime-options"))
        .unwrap();
    let message = outbox_message("s-runtime-options");
    let mut request = outbox_request();
    request.request_id = "request-runtime-options".to_string();
    request.runtime_options_json = Some(
        r#"{"profile":"surface_quick_reply","pre_messages":[{"role":"user","blocks":[]}]}"#
            .to_string(),
    );

    let first = store
        .append_message_with_runtime_outbox(&message, &request)
        .unwrap();
    assert_eq!(first.runtime_options_json, request.runtime_options_json);
    let reloaded = store
        .get_session_runtime_outbox(&request.request_id)
        .unwrap()
        .expect("outbox record must persist");
    assert_eq!(reloaded.runtime_options_json, request.runtime_options_json);
}

#[test]
fn claim_returns_only_each_session_runnable_head() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("session-a")).unwrap();
    store.create_session(&make_record("session-b")).unwrap();
    for (session_id, id, timestamp) in [
        ("session-a", "a-1", 100),
        ("session-a", "a-2", 101),
        ("session-b", "b-1", 102),
        ("session-b", "b-2", 103),
    ] {
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"queued"}]"#),
                timestamp,
                &ingress_request(id, 1, InputRoutingDecision::StartNewTurn, None, timestamp),
            )
            .unwrap();
    }

    let first = store
        .claim_session_runtime_outbox("worker", 200, 1_000, 10)
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(
        first
            .iter()
            .map(|record| record.input_id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["input-a-1", "input-b-1"])
    );
    assert!(store
        .claim_session_runtime_outbox("other", 201, 1_000, 10)
        .unwrap()
        .is_empty());

    let a = first
        .into_iter()
        .find(|record| record.session_id == "session-a")
        .unwrap();
    let token = a.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            &a.request_id,
            "worker",
            a.session_generation,
            &token,
            a.revision,
            202,
        )
        .unwrap();
    store
        .ack_session_runtime_outbox(
            &running.request_id,
            "worker",
            running.session_generation,
            &token,
            running.revision,
            SessionRuntimeInputStatus::Completed,
            1,
            203,
        )
        .unwrap();
    let next = store
        .claim_session_runtime_outbox("worker", 204, 1_000, 10)
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].input_id, "input-a-2");
}

#[test]
fn input_id_drives_reclassify_cancel_and_terminal_outcomes() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("session-input-id"))
        .unwrap();
    let queued = store
        .append_ingress_with_runtime_outbox(
            "session-input-id",
            "user",
            Some(r#"[{"type":"text","text":"supplement"}]"#),
            100,
            &ingress_request(
                "reclassify",
                1,
                InputRoutingDecision::StartNewTurn,
                None,
                100,
            ),
        )
        .unwrap();
    let reclassified = store
        .reclassify_session_runtime_outbox(
            "input-reclassify",
            1,
            queued.revision,
            InputRoutingDecision::SupplementCurrentTurn,
            Some("turn-active"),
            Some(r#"{"classification":"supplement"}"#),
            "user",
            "continuation of active turn",
            101,
        )
        .unwrap();
    assert_eq!(reclassified.status, SessionRuntimeInputStatus::Reclassified);
    assert_eq!(reclassified.target_turn_id.as_deref(), Some("turn-active"));
    let claimed = store
        .claim_session_runtime_outbox("worker", 102, 1_000, 1)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            &token,
            claimed.revision,
            103,
        )
        .unwrap();
    let supplemented = store
        .ack_session_runtime_outbox(
            &running.request_id,
            "worker",
            running.session_generation,
            &token,
            running.revision,
            SessionRuntimeInputStatus::Supplemented,
            7,
            104,
        )
        .unwrap();
    assert_eq!(supplemented.status, SessionRuntimeInputStatus::Supplemented);

    let queued = store
        .append_ingress_with_runtime_outbox(
            "session-input-id",
            "user",
            Some(r#"[{"type":"text","text":"cancel"}]"#),
            105,
            &ingress_request("cancel", 1, InputRoutingDecision::StartNewTurn, None, 105),
        )
        .unwrap();
    let cancelled = store
        .cancel_session_runtime_outbox(
            "input-cancel",
            1,
            queued.revision,
            "user",
            "no longer needed",
            106,
        )
        .unwrap();
    assert_eq!(cancelled.status, SessionRuntimeInputStatus::Cancelled);
    assert_eq!(cancelled.terminal_at_ms, Some(106));
    assert_eq!(
        store
            .get_session_runtime_outbox_by_input_id("input-cancel")
            .unwrap(),
        Some(cancelled)
    );

    store
        .append_ingress_with_runtime_outbox(
            "session-input-id",
            "user",
            None,
            107,
            &ingress_request(
                "worker-cancel",
                1,
                InputRoutingDecision::StartNewTurn,
                None,
                107,
            ),
        )
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("worker", 108, 1_000, 1)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            &token,
            claimed.revision,
            109,
        )
        .unwrap();
    let cancelled_by_owner = store
        .ack_session_runtime_outbox(
            &running.request_id,
            "worker",
            running.session_generation,
            &token,
            running.revision,
            SessionRuntimeInputStatus::Cancelled,
            0,
            110,
        )
        .unwrap();
    assert_eq!(
        cancelled_by_owner.status,
        SessionRuntimeInputStatus::Cancelled
    );
}

#[test]
fn attached_supplement_can_roll_forward_as_a_new_turn() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("session-attached-roll-forward"))
        .unwrap();
    let mut request = ingress_request(
        "attached-roll-forward",
        1,
        InputRoutingDecision::SupplementCurrentTurn,
        Some("turn-failed"),
        100,
    );
    request.turn_id = "turn-supplement".to_string();
    let queued = store
        .append_ingress_with_runtime_outbox(
            "session-attached-roll-forward",
            "user",
            Some(r#"[{"type":"text","text":"continue independently"}]"#),
            100,
            &request,
        )
        .unwrap();
    let attached = store
        .attach_session_runtime_outbox(
            &queued.input_id,
            queued.session_generation,
            queued.revision,
            "turn-failed",
            "test",
            "delivered to active turn",
            101,
        )
        .unwrap();
    assert_eq!(attached.status, SessionRuntimeInputStatus::Attached);

    let rolled = store
        .reclassify_session_runtime_outbox(
            &attached.input_id,
            attached.session_generation,
            attached.revision,
            InputRoutingDecision::StartNewTurn,
            None,
            attached.classification_json.as_deref(),
            "test",
            "target turn failed",
            102,
        )
        .unwrap();
    assert_eq!(rolled.status, SessionRuntimeInputStatus::Reclassified);
    assert_eq!(rolled.decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(rolled.target_turn_id, None);
    assert_eq!(
        store
            .claim_session_runtime_outbox("worker", 103, 1_000, 1)
            .unwrap()
            .remove(0)
            .input_id,
        attached.input_id
    );
}

#[test]
fn generation_advance_closes_admission_and_fences_stale_claims() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("session-generation"))
        .unwrap();
    assert_eq!(
        store
            .get_session_input_admission("session-generation")
            .unwrap()
            .unwrap(),
        SessionInputAdmission {
            session_id: "session-generation".to_string(),
            generation: 1,
            open: true,
        }
    );
    store
        .append_ingress_with_runtime_outbox(
            "session-generation",
            "user",
            None,
            100,
            &ingress_request(
                "generation-1",
                1,
                InputRoutingDecision::StartNewTurn,
                None,
                100,
            ),
        )
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("worker", 101, 1_000, 1)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let closed = store
        .close_session_input_admission("session-generation", 1, "lifecycle", "archive", 102)
        .unwrap();
    assert_eq!(closed.generation, 2);
    assert!(!closed.open);
    assert!(store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            &token,
            claimed.revision,
            103,
        )
        .is_err());
    let expired = store
        .get_session_runtime_outbox_by_input_id("input-generation-1")
        .unwrap()
        .unwrap();
    assert_eq!(expired.status, SessionRuntimeInputStatus::Expired);
    assert_eq!(expired.terminal_at_ms, Some(102));
    assert!(store
        .append_ingress_with_runtime_outbox(
            "session-generation",
            "user",
            None,
            104,
            &ingress_request(
                "generation-2-closed",
                2,
                InputRoutingDecision::StartNewTurn,
                None,
                104,
            ),
        )
        .is_err());
    let reopened = store
        .advance_session_input_generation(
            "session-generation",
            2,
            true,
            "branch",
            "new branch authority",
            105,
        )
        .unwrap();
    assert_eq!(reopened.generation, 3);
    assert!(reopened.open);
    assert!(store
        .append_ingress_with_runtime_outbox(
            "session-generation",
            "user",
            None,
            106,
            &ingress_request(
                "generation-3",
                3,
                InputRoutingDecision::StartNewTurn,
                None,
                106,
            ),
        )
        .is_ok());
}

#[test]
fn claimed_target_loss_reclassifies_and_requeues_under_owner_fence() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("session-target-loss"))
        .unwrap();
    store
        .append_ingress_with_runtime_outbox(
            "session-target-loss",
            "user",
            None,
            100,
            &ingress_request(
                "target-loss",
                1,
                InputRoutingDecision::SupplementCurrentTurn,
                Some("turn-ended"),
                100,
            ),
        )
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("worker", 101, 1_000, 1)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    assert!(store
        .requeue_claimed_session_runtime_outbox(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            "wrong-token",
            claimed.revision,
            InputRoutingDecision::StartNewTurn,
            None,
            Some(r#"{"classification":"target_ended"}"#),
            "target turn no longer exists",
            102,
        )
        .is_err());
    let requeued = store
        .requeue_claimed_session_runtime_outbox(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            &token,
            claimed.revision,
            InputRoutingDecision::StartNewTurn,
            None,
            Some(r#"{"classification":"target_ended"}"#),
            "target turn no longer exists",
            102,
        )
        .unwrap();
    assert_eq!(requeued.status, SessionRuntimeInputStatus::Reclassified);
    assert_eq!(requeued.decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(requeued.target_turn_id, None);
    assert_eq!(requeued.claim_owner, None);
    assert_eq!(requeued.claim_token, None);
    assert!(store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            &token,
            claimed.revision,
            103,
        )
        .is_err());
    let reclaimed = store
        .claim_session_runtime_outbox("worker-next", 103, 1_000, 1)
        .unwrap()
        .remove(0);
    assert_eq!(reclaimed.input_id, claimed.input_id);
    assert_ne!(reclaimed.claim_token, Some(token));
    let timeline = store
        .get_session_domain_timeline_limited("session-target-loss", 0, 10)
        .unwrap();
    assert_eq!(timeline.len(), 4);
    assert!(timeline[3]
        .event_json
        .contains("target turn no longer exists"));
}

#[test]
fn source_transaction_rolls_back_when_outbox_identity_conflicts() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-rollback")).unwrap();
    let first = outbox_message("s-rollback");
    let request = outbox_request();
    store
        .append_message_with_runtime_outbox(&first, &request)
        .unwrap();

    let mut second = first;
    second.sequence = 1;
    let conflicting = SessionRuntimeOutboxRequest {
        input_id: "input-2".to_string(),
        request_id: "request-2".to_string(),
        turn_id: "turn-2".to_string(),
        message_id: "message-1".to_string(),
        session_generation: 1,
        decision: InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: None,
        task_route_hint: None,
        created_at_ms: 101,
        runtime_options_json: None,
    };
    assert!(store
        .append_message_with_runtime_outbox(&second, &conflicting)
        .is_err());
    assert_eq!(store.get_message_count("s-rollback").unwrap(), 1);
    assert!(store
        .get_session_runtime_outbox("request-2")
        .unwrap()
        .is_none());
}

#[test]
fn duplicate_input_id_rolls_back_message_and_outbox_atomically() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("s-input-identity"))
        .unwrap();
    store
        .append_ingress_with_runtime_outbox(
            "s-input-identity",
            "user",
            None,
            100,
            &ingress_request("identity", 1, InputRoutingDecision::StartNewTurn, None, 100),
        )
        .unwrap();
    let mut duplicate = ingress_request(
        "other-request",
        1,
        InputRoutingDecision::StartNewTurn,
        None,
        101,
    );
    duplicate.input_id = "input-identity".to_string();
    assert!(store
        .append_ingress_with_runtime_outbox("s-input-identity", "user", None, 101, &duplicate,)
        .is_err());
    assert_eq!(store.get_message_count("s-input-identity").unwrap(), 1);
    assert!(store
        .get_session_runtime_outbox(&duplicate.request_id)
        .unwrap()
        .is_none());
}

#[test]
fn legacy_runtime_outbox_schema_migrates_in_place_and_remains_readable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy-runtime-outbox.db");
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            r"
                PRAGMA foreign_keys=ON;
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    user_id TEXT,
                    model TEXT,
                    created_at TEXT NOT NULL,
                    last_activity TEXT NOT NULL,
                    message_count INTEGER NOT NULL DEFAULT 0,
                    reset_policy TEXT NOT NULL,
                    metadata_json TEXT,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stable_message_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    blocks_count INTEGER NOT NULL DEFAULT 1,
                    tool_use_id TEXT,
                    tool_name TEXT,
                    token_usage_json TEXT,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(session_id, sequence)
                );
                CREATE TABLE session_runtime_outbox (
                    request_id TEXT PRIMARY KEY,
                    turn_id TEXT NOT NULL UNIQUE,
                    message_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    runtime_commit_cursor INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at_ms INTEGER NOT NULL,
                    claim_owner TEXT,
                    claim_expires_at_ms INTEGER,
                    failure_class TEXT,
                    last_error TEXT,
                    revision INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    runtime_options_json TEXT
                );
                INSERT INTO sessions (
                    session_id, platform, chat_id, created_at, last_activity,
                    reset_policy, created_at_ms, updated_at_ms
                ) VALUES (
                    'legacy-session', 'test', 'chat', '2024-01-01T00:00:00Z',
                    '2024-01-01T00:00:00Z', 'None', 100, 100
                );
                INSERT INTO messages (
                    stable_message_id, session_id, sequence, role,
                    content_json, created_at_ms
                ) VALUES (
                    'legacy-message', 'legacy-session', 0, 'user', '[]', 100
                );
                INSERT INTO messages (
                    stable_message_id, session_id, sequence, role,
                    content_json, created_at_ms
                ) VALUES (
                    'legacy-running-message', 'legacy-session', 1, 'user', '[]', 121
                );
                INSERT INTO session_runtime_outbox (
                    request_id, turn_id, message_id, session_id, sequence,
                    status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                    revision, created_at_ms, updated_at_ms
                ) VALUES (
                    'legacy-request', 'legacy-turn', 'legacy-message',
                    'legacy-session', 0, 'materialized', 9, 1, 100, 4, 100, 120
                );
                INSERT INTO session_runtime_outbox (
                    request_id, turn_id, message_id, session_id, sequence,
                    status, attempts, next_attempt_at_ms, claim_owner,
                    claim_expires_at_ms, revision, created_at_ms, updated_at_ms
                ) VALUES (
                    'legacy-running-request', 'legacy-running-turn',
                    'legacy-running-message', 'legacy-session', 1, 'running',
                    1, 100, 'legacy-worker', 999, 4, 121, 121
                );
                ",
        )
        .unwrap();
    }

    let store = SqliteSessionStore::open(&path).unwrap();
    let migrated = store
        .get_session_runtime_outbox("legacy-request")
        .unwrap()
        .unwrap();
    assert_eq!(migrated.input_id, "legacy-request");
    assert_eq!(migrated.session_generation, 1);
    assert_eq!(migrated.decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(migrated.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(migrated.terminal_at_ms, Some(120));
    let migrated_running = store
        .get_session_runtime_outbox("legacy-running-request")
        .unwrap()
        .unwrap();
    assert_eq!(migrated_running.status, SessionRuntimeInputStatus::Running);
    assert!(migrated_running
        .claim_token
        .as_deref()
        .is_some_and(|token| token.starts_with("legacy:legacy-running-request:4")));
    assert_eq!(migrated_running.claim_fence_epoch, Some(4));
    assert_eq!(
        store
            .get_session_input_admission("legacy-session")
            .unwrap()
            .unwrap()
            .generation,
        1
    );
}

#[test]
fn multiple_supplements_keep_distinct_turn_identities_for_one_target() {
    let (store, _dir) = make_store();
    store
        .create_session(&make_record("session-supplements"))
        .unwrap();

    let first = store
        .append_ingress_with_runtime_outbox(
            "session-supplements",
            "user",
            Some(r#"[{"type":"text","text":"first supplement"}]"#),
            100,
            &ingress_request(
                "supplement-1",
                1,
                InputRoutingDecision::SupplementCurrentTurn,
                Some("turn-active"),
                100,
            ),
        )
        .unwrap();
    let second = store
        .append_ingress_with_runtime_outbox(
            "session-supplements",
            "user",
            Some(r#"[{"type":"text","text":"second supplement"}]"#),
            101,
            &ingress_request(
                "supplement-2",
                1,
                InputRoutingDecision::SupplementCurrentTurn,
                Some("turn-active"),
                101,
            ),
        )
        .unwrap();

    assert_ne!(first.turn_id, second.turn_id);
    assert_eq!(first.target_turn_id.as_deref(), Some("turn-active"));
    assert_eq!(second.target_turn_id.as_deref(), Some("turn-active"));
}

#[test]
fn batched_execution_history_limits_turn_roots_after_filtering_related_inputs() {
    let (store, _dir) = make_store();
    let session_id = "session-root-recovery";
    store.create_session(&make_record(session_id)).unwrap();
    store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            None,
            100,
            &ingress_request(
                "root-recovery",
                1,
                InputRoutingDecision::StartNewTurn,
                None,
                100,
            ),
        )
        .unwrap();
    for index in 0..3 {
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                None,
                101 + index,
                &ingress_request(
                    &format!("root-supplement-{index}"),
                    1,
                    InputRoutingDecision::SupplementCurrentTurn,
                    Some("turn-root-recovery"),
                    101 + index,
                ),
            )
            .unwrap();
    }
    store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            None,
            110,
            &ingress_request(
                "root-rejected",
                1,
                InputRoutingDecision::RejectDuplicate,
                None,
                110,
            ),
        )
        .unwrap();

    let roots = store
        .session_runtime_outbox_for_sessions(&[session_id.to_string()], 1)
        .unwrap();

    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(roots[0].request_id, "request-root-recovery");
}

#[test]
fn outbox_claim_lease_retry_block_manual_retry_and_ack_are_guarded() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-lifecycle")).unwrap();
    store
        .append_message_with_runtime_outbox(&outbox_message("s-lifecycle"), &outbox_request())
        .unwrap();

    let first = store
        .claim_session_runtime_outbox("worker-a", 100, 50, 10)
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attempts, 1);
    assert!(store
        .claim_session_runtime_outbox("worker-b", 149, 50, 10)
        .unwrap()
        .is_empty());
    let reclaimed = store
        .claim_session_runtime_outbox("worker-b", 150, 50, 10)
        .unwrap();
    assert_eq!(reclaimed[0].attempts, 2);
    let reclaimed_token = reclaimed[0].claim_token.clone().unwrap();

    let retry = store
        .fail_session_runtime_outbox(
            "request-1",
            "worker-b",
            reclaimed[0].session_generation,
            &reclaimed_token,
            reclaimed[0].revision,
            OutboxFailureClass::Retryable,
            "runtime unavailable",
            250,
            3,
            151,
        )
        .unwrap();
    assert_eq!(retry.status, SessionRuntimeInputStatus::Queued);
    assert!(store
        .claim_session_runtime_outbox("worker-c", 249, 50, 10)
        .unwrap()
        .is_empty());
    let final_claim = store
        .claim_session_runtime_outbox("worker-c", 250, 50, 10)
        .unwrap();
    assert_eq!(final_claim[0].attempts, 3);
    let final_token = final_claim[0].claim_token.clone().unwrap();
    let blocked = store
        .fail_session_runtime_outbox(
            "request-1",
            "worker-c",
            final_claim[0].session_generation,
            &final_token,
            final_claim[0].revision,
            OutboxFailureClass::AuthorizationBlocked,
            "retry exhausted",
            500,
            3,
            251,
        )
        .unwrap();
    assert_eq!(blocked.status, SessionRuntimeInputStatus::Blocked);

    let pending = store
        .retry_blocked_session_runtime_outbox(
            "request-1",
            blocked.session_generation,
            blocked.revision,
            "operator-1",
            "runtime recovered",
            300,
        )
        .unwrap();
    assert_eq!(pending.status, SessionRuntimeInputStatus::Queued);
    assert_eq!(pending.attempts, 3);
    let claimed = store
        .claim_session_runtime_outbox("worker-d", 300, 50, 10)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            "request-1",
            "worker-d",
            claimed.session_generation,
            &token,
            claimed.revision,
            301,
        )
        .unwrap();
    let done = store
        .ack_session_runtime_outbox(
            "request-1",
            "worker-d",
            running.session_generation,
            &token,
            running.revision,
            SessionRuntimeInputStatus::Completed,
            42,
            302,
        )
        .unwrap();
    assert_eq!(done.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(done.terminal_at_ms, Some(302));
    assert_eq!(done.runtime_commit_cursor, Some(42));
    assert_eq!(store.session_runtime_outbox_health().unwrap().completed, 1);
}

#[test]
fn outbox_lease_renewal_rejects_stale_ack_and_prevents_reclaim() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-renew")).unwrap();
    store
        .append_message_with_runtime_outbox(&outbox_message("s-renew"), &outbox_request())
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("worker-a", 100, 50, 1)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let renewed = store
        .renew_session_runtime_outbox_lease(
            "request-1",
            "worker-a",
            claimed.session_generation,
            &token,
            claimed.revision,
            140,
            50,
        )
        .unwrap();
    assert!(store
        .claim_session_runtime_outbox("worker-b", 151, 50, 1)
        .unwrap()
        .is_empty());
    assert!(store
        .mark_session_runtime_outbox_running(
            "request-1",
            "worker-a",
            claimed.session_generation,
            &token,
            claimed.revision,
            152,
        )
        .is_err());
    let running = store
        .mark_session_runtime_outbox_running(
            "request-1",
            "worker-a",
            renewed.session_generation,
            &token,
            renewed.revision,
            153,
        )
        .unwrap();
    assert!(store
        .ack_session_runtime_outbox(
            "request-1",
            "worker-a",
            running.session_generation,
            "wrong-token",
            running.revision,
            SessionRuntimeInputStatus::Completed,
            7,
            154,
        )
        .is_err());
    let done = store
        .ack_session_runtime_outbox(
            "request-1",
            "worker-a",
            running.session_generation,
            &token,
            running.revision,
            SessionRuntimeInputStatus::Completed,
            7,
            154,
        )
        .unwrap();
    assert_eq!(done.status, SessionRuntimeInputStatus::Completed);
}

#[test]
fn recovery_manifest_tracks_transcript_outbox_and_external_signals() {
    let (store, _dir) = make_store();
    store.create_session(&make_record("s-recovery")).unwrap();
    let initial = store
        .get_session_recovery_manifest("s-recovery")
        .unwrap()
        .unwrap();
    assert_eq!(initial.transcript_messages, 0);
    assert!(!initial.requires_hydration());

    let mut message = outbox_message("s-recovery");
    message.stable_message_id = "recovery-message".to_string();
    message.content_json = r#"[{"type":"text","text":"恢复中文"}]"#.to_string();
    let mut request = outbox_request();
    request.message_id = message.stable_message_id.clone();
    request.request_id = "recovery-request".to_string();
    request.turn_id = "recovery-turn".to_string();
    store
        .append_message_with_runtime_outbox(&message, &request)
        .unwrap();
    let pending = store
        .get_session_recovery_manifest("s-recovery")
        .unwrap()
        .unwrap();
    assert_eq!(pending.durable_cursor, 1);
    assert_eq!(pending.transcript_messages, 1);
    let expected_bytes = message.stable_message_id.len()
        + message.session_id.len()
        + message.role.len()
        + message.content_json.len()
        + message.token_usage_json.as_ref().map_or(0, String::len)
        + message.tool_use_id.as_ref().map_or(0, String::len)
        + message.tool_name.as_ref().map_or(0, String::len);
    assert_eq!(pending.transcript_bytes, expected_bytes as u64);
    assert!(pending.in_flight_turn);

    let claimed = store
        .claim_session_runtime_outbox("worker", 100, 1_000, 1)
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "worker",
            claimed.session_generation,
            &token,
            claimed.revision,
            101,
        )
        .unwrap();
    store
        .ack_session_runtime_outbox(
            &running.request_id,
            "worker",
            running.session_generation,
            &token,
            running.revision,
            SessionRuntimeInputStatus::Completed,
            1,
            102,
        )
        .unwrap();
    let settled = store
        .set_session_recovery_signal(
            "s-recovery",
            SessionRecoverySignal::PendingApproval,
            true,
            103,
        )
        .unwrap();
    assert!(!settled.in_flight_turn);
    assert!(settled.pending_approval);
    assert!(settled.requires_hydration());
    assert_eq!(
        store
            .list_active_session_recovery_manifests(0, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_required_session_recovery_manifests(0, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .get_session_recovery_manifests_by_ids(&["s-recovery".to_string()])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn recovery_manifest_backfills_existing_transcript_without_body_loss() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("recovery-backfill.db");
    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store.create_session(&make_record("s-backfill")).unwrap();
        store.insert_message(&outbox_message("s-backfill")).unwrap();
        let connection = store.conn().unwrap();
        connection
            .execute_batch(
                "DROP TRIGGER IF EXISTS session_recovery_session_insert;
                     DROP TRIGGER IF EXISTS session_recovery_session_update;
                     DROP TRIGGER IF EXISTS session_recovery_message_insert;
                     DROP TRIGGER IF EXISTS session_recovery_message_delete;
                     DROP TRIGGER IF EXISTS session_recovery_message_update;
                     DROP TRIGGER IF EXISTS session_recovery_lifecycle_event_insert;
                     DROP TRIGGER IF EXISTS session_recovery_runtime_outbox_insert;
                     DROP TRIGGER IF EXISTS session_recovery_runtime_outbox_update;
                     DROP TABLE session_recovery_manifest;",
            )
            .unwrap();
    }
    let reopened = SqliteSessionStore::open(&path).unwrap();
    let manifest = reopened
        .get_session_recovery_manifest("s-backfill")
        .unwrap()
        .unwrap();
    assert_eq!(manifest.transcript_messages, 1);
    assert_eq!(manifest.durable_cursor, 1);
    assert!(manifest.transcript_bytes > 0);
}

#[test]
fn legacy_process_presence_is_removed_instead_of_restored_as_online() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("presence-migration.db");
    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .create_session(&make_record("s-legacy-presence"))
            .unwrap();
        let connection = store.conn().unwrap();
        connection
            .execute(
                "INSERT INTO session_events(
                        session_id, event_type, event_json, sequence, created_at_ms
                     ) VALUES (?1, 'session.lifecycle.v1', ?2, 1, 10)",
                params![
                    "s-legacy-presence",
                    r#"{"snapshot":{"attachments":[{"actor":{"id":"dead-web","role":"webui"}}]}}"#
                ],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE session_recovery_manifest
                        SET active_writer_or_attachment=1
                      WHERE session_id=?1",
                params!["s-legacy-presence"],
            )
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert!(reopened
        .get_events("s-legacy-presence", 0)
        .unwrap()
        .iter()
        .all(|event| event.event_type != "session.lifecycle.v1"));
    assert!(reopened
        .get_session_presence_projection("s-legacy-presence")
        .unwrap()
        .is_none());
    assert!(
        !reopened
            .get_session_recovery_manifest("s-legacy-presence")
            .unwrap()
            .unwrap()
            .active_writer_or_attachment
    );
}
