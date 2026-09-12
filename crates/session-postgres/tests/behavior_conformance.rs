//! Original public Session backend behavior, exercised on isolated PostgreSQL.
//! SQL-layout and retired-format cases are audited separately, not silently skipped.
use harness_contract::turn::InputRoutingDecision;
use session::persistence::domain::ingress::input_decision_as_str;
use session::*;
use session_postgres::PostgresSessionStore;
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

fn make_store() -> (PostgresTestScope, PostgresSessionStore) {
    let fixture = PostgresTestScope::new();
    let store = PostgresSessionStore::new(fixture.reconnect()).expect("owned Session namespace");
    (fixture, store)
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn get_messages_from_sequence_pages_100k_history() {
    let (fixture, store) = make_store();
    store.create_session(&make_record("s-100k")).unwrap();
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute(
        "ALTER TABLE session_messages DISABLE TRIGGER USER;
         INSERT INTO session_messages(stable_message_id,session_id,sequence,role,content_json,blocks_count,created_at_ms)
         SELECT 'bulk:'||n,'s-100k',n,CASE WHEN n%2=0 THEN 'user' ELSE 'assistant' END,
           jsonb_build_array(jsonb_build_object('type','text','text','message '||n))::text,1,n
         FROM generate_series(0,99999) n;
         ALTER TABLE session_messages ENABLE TRIGGER USER;
         INSERT INTO session_context_index_outbox(session_id,source_sequence,operation,status,created_at_ms,updated_at_ms)
         VALUES ('s-100k',0,'reconcile','pending',0,0)
         ON CONFLICT(session_id,source_sequence,operation) DO NOTHING;
         ANALYZE session_messages;"
    ).unwrap();
    let plan: serde_json::Value = connection.query_one(
        "EXPLAIN (FORMAT JSON) SELECT stable_message_id,session_id,sequence,role,content_json,blocks_count,
         tool_use_id,tool_name,token_usage_json,created_at_ms FROM session_messages
         WHERE session_id='s-100k' AND sequence>=99950 ORDER BY sequence ASC LIMIT 50", &[]
    ).unwrap().get(0);
    assert!(plan.to_string().contains("Index Scan"), "{plan}");
    let outbox_rows: i64 = connection
        .query_one(
            "SELECT COUNT(*) FROM session_context_index_outbox WHERE session_id='s-100k'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        outbox_rows, 1,
        "index admission remains one row per Session"
    );
    drop(connection);
    let page = store
        .get_messages_from_sequence("s-100k", 99_950, 50)
        .unwrap();
    assert_eq!(page.len(), 50);
    for (offset, message) in page.iter().enumerate() {
        let sequence = 99_950 + offset;
        assert_eq!(message.sequence, sequence);
        assert_eq!(message.stable_message_id, format!("bulk:{sequence}"));
        let content: serde_json::Value = serde_json::from_str(&message.content_json).unwrap();
        assert_eq!(content[0]["text"], format!("message {sequence}"));
    }
    assert!(store
        .get_messages_from_sequence("s-100k", 100_000, 50)
        .unwrap()
        .is_empty());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn latest_checkpoint_lookup_uses_full_index_beyond_legacy_page_boundary() {
    let (fixture, store) = make_store();
    store
        .create_session(&make_record("s-late-checkpoint"))
        .unwrap();
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute(
        "INSERT INTO session_events(session_id,event_type,event_json,sequence,created_at_ms)
         SELECT 's-late-checkpoint','SessionDomainEvent',jsonb_build_object(
           'event_id','event-'||n,'session_id','s-late-checkpoint','sequence',n,'scope','runtime',
           'kind',CASE WHEN n=4999 THEN 'memory.semantic_checkpoint.created' ELSE 'runtime.progress' END,
           'payload','{}'::jsonb,'created_at_ms',n)::text,n,n FROM generate_series(0,4999) n;
         ANALYZE session_events;"
    ).unwrap();
    drop(connection);
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn context_index_reconciliation_is_complete_idempotent_and_repairable() {
    let (fixture, store) = make_store();
    store
        .create_session(&make_record("s-context-index"))
        .unwrap();
    let messages: Vec<_> = (0..513)
        .map(|sequence| SessionMessage {
            stable_message_id: format!("index-{sequence}"),
            session_id: "s-context-index".into(),
            sequence,
            role: "user".into(),
            content_json:
                serde_json::json!([{"type":"text","text":format!("indexed payload {sequence}")}])
                    .to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: sequence as u64,
        })
        .collect();
    store.insert_messages_batch(&messages).unwrap();
    let first = store
        .reconcile_session_context_index("s-context-index", 128, 4, 1_000)
        .unwrap();
    assert!(first.complete);
    assert_eq!(first.source_messages, 513);
    assert_eq!(first.covered_messages, 513);
    assert_eq!(first.indexed_through_sequence, Some(512));
    assert!(!first.source_digest.is_empty());
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    assert_eq!(
        connection
            .execute(
                "DELETE FROM session_context_index_cards WHERE card_id=(SELECT card_id
         FROM session_context_index_cards WHERE session_id='s-context-index' LIMIT 1)",
                &[]
            )
            .unwrap(),
        1
    );
    drop(connection);
    let repaired = store
        .reconcile_session_context_index("s-context-index", 128, 4, 2_000)
        .unwrap();
    assert!(repaired.complete);
    assert_eq!(repaired.source_digest, first.source_digest);
    assert_eq!(repaired.generation, first.generation + 1);
    assert_eq!(repaired.card_count, first.card_count);
    assert_eq!(
        store
            .get_context_index_cards("s-context-index", 64)
            .unwrap()
            .len(),
        repaired.card_count
    );
    let reopened = PostgresSessionStore::new(fixture.reconnect()).unwrap();
    assert_eq!(reopened.get_message_count("s-context-index").unwrap(), 513);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn semantic_checkpoint_alone_enqueues_context_index_reconciliation() {
    let (fixture, store) = make_store();
    store
        .create_session(&make_record("s-checkpoint-index-outbox"))
        .unwrap();
    store
        .append_event(&SessionEvent {
            session_id: "s-checkpoint-index-outbox".into(),
            event_type: SESSION_DOMAIN_EVENT_TYPE.into(),
            sequence: 0,
            created_at_ms: 20,
            event_json: serde_json::json!({
                "event_id":"checkpoint-only", "session_id":"s-checkpoint-index-outbox",
                "sequence":0,"scope":"runtime","kind":"memory.semantic_checkpoint.created",
                "payload":{},"created_at_ms":20
            })
            .to_string(),
        })
        .unwrap();
    assert!(
        store
            .get_session_recovery_manifest("s-checkpoint-index-outbox")
            .unwrap()
            .unwrap()
            .index_pending
    );
    let executor = fixture.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    let pending: i64 = connection
        .query_one(
            "SELECT COUNT(*) FROM session_context_index_outbox
         WHERE session_id='s-checkpoint-index-outbox' AND status='pending'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(pending, 1);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_create_and_get() {
    let (_fixture, store) = make_store();
    let rec = make_record("session-001");
    store.create_session(&rec).unwrap();
    let loaded = store.get_session("session-001").unwrap().unwrap();
    assert_eq!(loaded.session_id, "session-001");
    assert_eq!(loaded.platform, "test");
    assert_eq!(loaded.message_count, 1);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_update_session() {
    let (_fixture, store) = make_store();
    let mut rec = make_record("session-002");
    store.create_session(&rec).unwrap();
    rec.message_count = 42;
    rec.last_activity = "2024-01-02T00:00:00Z".to_string();
    store.update_session(&rec).unwrap();
    let loaded = store.get_session("session-002").unwrap().unwrap();
    assert_eq!(loaded.message_count, 42);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_upsert_session() {
    let (_fixture, store) = make_store();
    let mut rec = make_record("session-003");
    store.upsert_session(&rec).unwrap();
    rec.message_count = 99;
    store.upsert_session(&rec).unwrap();
    let loaded = store.get_session("session-003").unwrap().unwrap();
    assert_eq!(loaded.message_count, 99);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_delete_session() {
    let (_fixture, store) = make_store();
    let rec = make_record("session-004");
    store.create_session(&rec).unwrap();
    store.delete_session("session-004").unwrap();
    assert!(store.get_session("session-004").unwrap().is_none());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_list_sessions() {
    let (_fixture, store) = make_store();
    store.create_session(&make_record("s1")).unwrap();
    store.create_session(&make_record("s2")).unwrap();
    let list = store.list_sessions().unwrap();
    assert_eq!(list.len(), 2);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn scoped_message_search_preserves_authorized_results_when_other_sessions_rank_first() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn list_sessions_by_workspace_root_filters_and_orders_by_activity() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn list_sessions_page_applies_owner_grants_and_tombstone_visibility_in_sql() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn list_sessions_page_escapes_like_wildcards() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn literal_list_filter_covers_all_original_fields_without_expanding_authority() {
    let (_fixture, store) = make_store();
    let marker = "Needle%_literal";
    for field in 0..7 {
        let mut record = make_record(&format!("field-{field}"));
        record.metadata_json = Some(serde_json::json!({"owner_principal_id":"owner"}).to_string());
        match field {
            0 => record.session_id = marker.into(),
            1 => record.platform = marker.into(),
            2 => record.chat_id = marker.into(),
            3 => record.user_id = Some(marker.into()),
            4 => record.model = Some(marker.into()),
            5 => record.status = marker.into(),
            _ => {
                record.metadata_json = Some(
                    serde_json::json!({"owner_principal_id":"owner", "title":marker}).to_string(),
                )
            }
        }
        store.create_session(&record).unwrap();
    }
    for (id, title, owner) in [
        ("wildcard", "NeedleXliteral", "owner"),
        ("foreign", marker, "other"),
    ] {
        let mut record = make_record(id);
        record.metadata_json =
            Some(serde_json::json!({"owner_principal_id":owner, "title":title}).to_string());
        store.create_session(&record).unwrap();
    }
    let page = store
        .list_sessions_page(&SessionListOptions {
            query: Some(" NEEDLE%_ "),
            owner_principal_id: Some("owner"),
            limit: 20,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.total, 7);
    assert_eq!(page.records.len(), 7);
    assert!(!page
        .records
        .iter()
        .any(|record| ["wildcard", "foreign"].contains(&record.session_id.as_str())));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn get_events_limited_pages_from_sequence_and_counts_total() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn get_events_by_type_pages_context_envelopes_only() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn get_context_event_by_envelope_id_reads_json_payload() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn append_context_envelope_event_if_absent_skips_duplicate_envelope_id() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn delete_events_from_removes_tail_only() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn delete_events_by_type_from_preserves_other_event_types() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn next_event_sequence_uses_max_sequence_plus_one() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn allocating_sequence_appends_contiguous_batch_atomically() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn allocating_sequence_is_atomic_across_parallel_postgres_connections() {
    let (_fixture, store) = make_store();
    store
        .create_session(&make_record("s-parallel-postgres"))
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
                    session_id: "s-parallel-postgres".to_string(),
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn session_event_sequence_constraint_rejects_duplicate() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn allocating_batch_rolls_back_when_runtime_envelope_is_invalid() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn checkpoint_batch_timestamp_overflow_rolls_back_without_partial_event() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn exact_message_reads_and_metadata_page_preserve_stable_identity() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn branch_copy_uses_stable_cutoff_and_rejects_nonempty_target() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_list_by_platform() {
    let (_fixture, store) = make_store();
    let mut rec = make_record("s-tg");
    rec.platform = "telegram".to_string();
    store.create_session(&rec).unwrap();
    store.create_session(&make_record("s-test")).unwrap();
    let tg = store.list_sessions_by_platform("telegram").unwrap();
    assert_eq!(tg.len(), 1);
    assert_eq!(tg[0].session_id, "s-tg");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_memory_associations() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn test_prune_before() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn source_message_and_outbox_are_atomic_and_idempotent() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
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
        let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn runtime_options_remain_opaque_and_durable_with_session_ingress() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn claim_returns_only_each_session_runnable_head() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn input_id_drives_reclassify_cancel_and_terminal_outcomes() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn attached_supplement_can_roll_forward_as_a_new_turn() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn generation_advance_closes_admission_and_fences_stale_claims() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn claimed_target_loss_reclassifies_and_requeues_under_owner_fence() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn source_transaction_rolls_back_when_outbox_identity_conflicts() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn duplicate_input_id_rolls_back_message_and_outbox_atomically() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn multiple_supplements_keep_distinct_turn_identities_for_one_target() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn batched_execution_history_limits_turn_roots_after_filtering_related_inputs() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn outbox_claim_lease_retry_block_manual_retry_and_ack_are_guarded() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn outbox_lease_renewal_rejects_stale_ack_and_prevents_reclaim() {
    let (_fixture, store) = make_store();
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
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn recovery_manifest_tracks_transcript_outbox_and_external_signals() {
    let (_fixture, store) = make_store();
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
