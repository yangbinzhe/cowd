use std::sync::Arc;

use harness_contract::knowledge::{
    KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeUsageSignal,
};
use memory::{
    code_indexer::{CodeSymbol, SymbolKind},
    knowledge::{DocumentContent, KnowledgeFabric, KnowledgeStore},
    project_scope::MemoryScope,
    store::MemoryStore,
    types::AgentVisibility,
    MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
};
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

use super::*;

fn memory_entry(id: uuid::Uuid, marker: &str) -> MemoryEntry {
    let now = chrono::Utc::now();
    MemoryEntry {
        id,
        layer: MemoryLayer::L3,
        category: MemoryCategory::ProjectKnowledge,
        priority: Priority::High,
        source: MemorySource::Import,
        title: format!("durable {marker}"),
        content: format!("portable memory truth {marker}"),
        embedding: Some(vec![0.25, 0.75]),
        tags: vec!["durability".to_string()],
        relations: Vec::new(),
        confidence: 0.9,
        access_count: 0,
        staleness: 0.0,
        created_at: now,
        updated_at: now,
        last_accessed_at: None,
        scope: MemoryScope::Project("snapshot-test".to_string()),
        session_id: Some("session-snapshot".to_string()),
        source_agent: Some("test-agent".to_string()),
        visibility: AgentVisibility::Private,
    }
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_embedding_partial_progress_survives_store_and_client_reopen() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fixture = PostgresTestScope::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let config = memory::config::VectorConfig {
        enabled: true,
        api_url: format!("http://{}/embeddings", listener.local_addr().unwrap()),
        model: "pg-partial-fixture".into(),
        dimension: 2,
        batch_size: 1,
        ..Default::default()
    };
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (offset, length) = loop {
                let mut buf = [0; 4096];
                let count = socket.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let size = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, size);
                }
            };
            while bytes.len() < offset + length {
                let mut buf = [0; 4096];
                let count = socket.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
            }
            let request: serde_json::Value =
                serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
            let call = observed.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                request["input"][0],
                if call == 0 { "first" } else { "second" },
                "successful first batch must not be resent after reopening PG"
            );
            let (status, body) = if call == 1 {
                (
                    "400 Bad Request",
                    r#"{"error":"invalid input fixture failure"}"#,
                )
            } else {
                ("200 OK", r#"{"data":[{"index":0,"embedding":[1.0,2.0]}]}"#)
            };
            socket.write_all(format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let store: Arc<dyn MemoryStore> =
        Arc::new(PostgresMemoryStore::new(fixture.reconnect()).unwrap());
    let first =
        memory::embedding::EmbeddingClient::new(config.clone()).with_progress_store(store.clone());
    assert!(first.embed(&["first", "second"]).await.is_err());
    assert_eq!(store.list_key_values().await.unwrap().len(), 1);
    drop(first);
    drop(store);
    let reopened: Arc<dyn MemoryStore> =
        Arc::new(PostgresMemoryStore::new(fixture.reconnect()).unwrap());
    let resumed =
        memory::embedding::EmbeddingClient::new(config).with_progress_store(reopened.clone());
    assert_eq!(
        resumed.embed(&["first", "second"]).await.unwrap(),
        vec![vec![1.0, 2.0], vec![1.0, 2.0]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(reopened.list_key_values().await.unwrap().is_empty());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_memory_roundtrip() {
    let fixture = PostgresTestScope::new();
    let marker = uuid::Uuid::new_v4().simple().to_string();
    let store = PostgresMemoryStore::new(fixture.reconnect()).expect("owned memory namespace");
    let id = uuid::Uuid::new_v4();
    let entry = memory_entry(id, &marker);
    store.insert(&entry).await.unwrap();
    store
        .kv_put(&format!("durability:{marker}"), "present")
        .await
        .unwrap();
    store
        .insert_symbol(&CodeSymbol {
            id: format!("symbol-{marker}"),
            name: marker.clone(),
            kind: SymbolKind::Function,
            file_path: "src/real_pg_test.rs".to_string(),
            line: 1,
            signature: format!("fn {marker}()"),
            doc: None,
        })
        .await
        .unwrap();
    store
        .link_symbol_to_memory(&format!("symbol-{marker}"), &id, None, "test", 1)
        .await
        .unwrap();

    let reopened = PostgresMemoryStore::new(fixture.reconnect()).expect("reopen PostgreSQL owner");
    let loaded = reopened.get(&id).await.unwrap().expect("persisted entry");
    assert_eq!(loaded.id, id);
    let queue =
        PostgresMaintenanceQueue::new(reopened.executor().clone()).expect("maintenance queue");
    let candidate = MaintenanceCandidate {
        id: format!("maintenance-{marker}"),
        kind: memory::MaintenanceCandidateKind::Stale,
        status: MaintenanceCandidateStatus::Open,
        entry_ids: vec![id],
        summary: "review stale imported memory".to_string(),
        reason: "real PostgreSQL durability test".to_string(),
        confidence: 0.9,
        source: Some("postgres-test".to_string()),
        source_ref: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    assert_eq!(
        queue.upsert_many(std::slice::from_ref(&candidate)).unwrap(),
        1
    );
    queue
        .transition(&candidate.id, MaintenanceCandidateStatus::Acknowledged)
        .unwrap();
    let reopened_queue =
        PostgresMaintenanceQueue::new(reopened.executor().clone()).expect("reopened queue");
    assert!(reopened_queue
        .list(MaintenanceCandidateFilter {
            status: Some(MaintenanceCandidateStatus::Acknowledged),
            source: Some("postgres-test".to_string()),
            ..MaintenanceCandidateFilter::default()
        })
        .unwrap()
        .iter()
        .any(|value| value.id == candidate.id));
    assert!(reopened
        .search_fts(&marker, 10)
        .await
        .unwrap()
        .iter()
        .any(|item| item.id == id));
    assert!(reopened
        .search_fts_scoped(
            &marker,
            &MemoryScope::Project("snapshot-test".to_string()),
            10,
        )
        .await
        .unwrap()
        .iter()
        .any(|item| item.id == id));
    assert_eq!(
        reopened.find_memories_by_symbol(&marker).await.unwrap(),
        vec![id]
    );
    assert_eq!(
        reopened
            .kv_get(&format!("durability:{marker}"))
            .await
            .unwrap()
            .as_deref(),
        Some("present")
    );
    reopened
        .kv_delete(&format!("durability:{marker}"))
        .await
        .unwrap();
    reopened
        .kv_delete(&format!("durability:{marker}"))
        .await
        .unwrap();
    assert!(reopened
        .kv_get(&format!("durability:{marker}"))
        .await
        .unwrap()
        .is_none());

    let knowledge_target = Arc::new(
        PostgresKnowledgeStore::new(reopened.executor().clone())
            .expect("PostgreSQL Knowledge owner"),
    );
    let fabric = KnowledgeFabric::with_store(knowledge_target.clone());
    let receipt = fabric
        .ingest_document(
            KnowledgeNamespace::Project(format!("project-{marker}")),
            KnowledgeActivationPolicy::OnDemand,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new("Real PG", format!("knowledge {marker}")),
        )
        .unwrap();
    knowledge_target
        .record_usage(&KnowledgeUsageSignal {
            signal_id: format!("usage-{marker}"),
            session_id: format!("session-{marker}"),
            pack_id: receipt.pack.pack_id,
            action: "activated".to_string(),
            summary: "real PostgreSQL test".to_string(),
            score_delta_bp: 25,
            occurred_at: chrono::Utc::now(),
        })
        .unwrap();
    let knowledge_reopened =
        PostgresKnowledgeStore::new(reopened.executor().clone()).expect("reopen Knowledge owner");
    let knowledge_snapshot = knowledge_reopened.snapshot().unwrap();
    assert_eq!(knowledge_snapshot.corpus.len(), 1);
    assert_eq!(knowledge_snapshot.usage.len(), 1);

    let mut concurrent_ids = Vec::new();
    let mut tasks = Vec::new();
    for index in 0..8 {
        let concurrent_id = uuid::Uuid::new_v4();
        concurrent_ids.push(concurrent_id);
        let concurrent_store = reopened.clone();
        let concurrent_marker = format!("{marker}-{index}");
        tasks.push(tokio::spawn(async move {
            concurrent_store
                .insert(&memory_entry(concurrent_id, &concurrent_marker))
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    for concurrent_id in concurrent_ids {
        assert!(reopened.get(&concurrent_id).await.unwrap().is_some());
        reopened.delete(&concurrent_id).await.unwrap();
    }
    assert!(reopened.executor().health().metrics.checkout_count > 8);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_authority_filters_private_and_team_domains_before_limit() {
    let fixture = PostgresTestScope::new();
    let marker = uuid::Uuid::new_v4().simple().to_string();
    let store = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    let mut own = memory_entry(uuid::Uuid::new_v4(), &marker);
    own.scope = MemoryScope::Project(format!("authority-{marker}"));
    own.source_agent = Some("owner".into());
    own.updated_at -= chrono::Duration::hours(1);
    store.insert(&own).await.unwrap();
    for i in 0..70 {
        let mut hidden = own.clone();
        hidden.id = uuid::Uuid::new_v4();
        hidden.source_agent = Some(format!("hidden-{i}"));
        hidden.updated_at = chrono::Utc::now();
        store.insert(&hidden).await.unwrap();
    }
    let mut query = memory::store::AuthorityLookup {
        fingerprint: memory::same_memory_key(&own),
        scope: own.scope.clone(),
        visibility: AgentVisibility::Private,
        source_agent: own.source_agent.clone(),
        limit: 1,
    };
    let found = store
        .lookup_authority_candidates(query.clone())
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, own.id);
    query.source_agent = None;
    assert!(store
        .lookup_authority_candidates(query.clone())
        .await
        .unwrap()
        .is_empty());
    for visibility in [
        AgentVisibility::Shared,
        AgentVisibility::TeamScoped("team-a".into()),
        AgentVisibility::TeamScoped("team-b".into()),
    ] {
        let mut scoped = own.clone();
        scoped.id = uuid::Uuid::new_v4();
        scoped.visibility = visibility.clone();
        scoped.source_agent = Some("contributor".into());
        store.insert(&scoped).await.unwrap();
        query.visibility = visibility;
        let rows = store
            .lookup_authority_candidates(query.clone())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, scoped.id);
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_roundtrip_and_usage_persist_across_reopen() {
    let fixture = PostgresTestScope::new();
    let store: Arc<PostgresKnowledgeStore> = Arc::new(
        PostgresKnowledgeStore::new(fixture.reconnect()).expect("owned knowledge namespace"),
    );
    let fabric = KnowledgeFabric::with_store(store.clone());
    let receipt = fabric
        .ingest_document(
            KnowledgeNamespace::Project("pg-knowledge".to_string()),
            KnowledgeActivationPolicy::OnDemand,
            KnowledgeGovernanceLevel::Advisory,
            DocumentContent::new(
                "durable postgres knowledge",
                "knowledge roundtrip body marker",
            ),
        )
        .unwrap();
    assert!(
        receipt
            .warnings
            .iter()
            .all(|warning| !warning.contains("persist failed")),
        "knowledge receipt must persist through PostgreSQL: {:?}",
        receipt.warnings
    );
    let snapshot = store.snapshot().expect("knowledge snapshot");
    assert_eq!(snapshot.corpus.len(), 1);
    assert_eq!(snapshot.packs.len(), 1);
    assert_eq!(snapshot.canon.len(), 1);
    assert_eq!(snapshot.chunks.len(), receipt.chunks.len());
    assert_eq!(snapshot.corpus[0].corpus_id, receipt.corpus.corpus_id);
    assert_eq!(snapshot.packs[0].pack_id, receipt.pack.pack_id);

    let signal = KnowledgeUsageSignal {
        signal_id: format!("signal-{}", receipt.pack.pack_id),
        session_id: "pg-knowledge-session".to_string(),
        pack_id: receipt.pack.pack_id.clone(),
        action: "activated".to_string(),
        summary: "durable postgres usage".to_string(),
        score_delta_bp: 10,
        occurred_at: chrono::Utc::now(),
    };
    store.record_usage(&signal).expect("usage persisted");
    drop(fabric);
    drop(store);

    let reopened =
        PostgresKnowledgeStore::new(fixture.reconnect()).expect("reopened knowledge namespace");
    let snapshot = reopened.snapshot().expect("reopened knowledge snapshot");
    assert_eq!(snapshot.packs.len(), 1);
    assert_eq!(snapshot.packs[0].pack_id, receipt.pack.pack_id);
    assert_eq!(snapshot.usage.len(), 1);
    assert_eq!(snapshot.usage[0].signal_id, signal.signal_id);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_symbol_memory_link_is_idempotent_with_null_turn_index() {
    // Regression for PLAN M-02 / W5 Step1: a plain unique index treats NULL as
    // distinct, so `ON CONFLICT DO NOTHING` silently no-ops and duplicate rows
    // accumulate whenever turn_index is None (the HTTP/default path).
    let fixture = PostgresTestScope::new();
    let marker = uuid::Uuid::new_v4().simple().to_string();
    let store = PostgresMemoryStore::new(fixture.reconnect()).expect("owned memory namespace");
    let id = uuid::Uuid::new_v4();
    store.insert(&memory_entry(id, &marker)).await.unwrap();
    let symbol = format!("symbol-{marker}");
    store
        .link_symbol_to_memory(&symbol, &id, None, "test", 1)
        .await
        .unwrap();
    store
        .link_symbol_to_memory(&symbol, &id, None, "test", 2)
        .await
        .unwrap();
    let linked = store
        .list_symbol_memory_references()
        .await
        .unwrap()
        .into_iter()
        .filter(|reference| reference.symbol_id == symbol && reference.memory_id == id)
        .count();
    assert_eq!(
        linked, 1,
        "a NULL turn_index must still deduplicate symbol->memory links"
    );
}

fn scenario_entity(scope: &str, index: usize) -> memory::entity::Entity {
    let now = chrono::Utc::now();
    memory::entity::Entity {
        id: format!("entity-{scope}-{index}"),
        name: format!("entity {scope} {index}"),
        entity_type: memory::entity::EntityType::Concept,
        confidence: 0.5,
        frequency: 1,
        first_seen: now,
        last_seen: now,
        source_ids: Vec::new(),
        source_type: "kg-concurrency-test".to_string(),
    }
}

fn scenario_triple(scope: &str, index: usize) -> memory::entity::Triple {
    memory::entity::Triple {
        id: format!("triple-{scope}-{index}"),
        subject_id: format!("entity-{scope}-{index}"),
        predicate: "depends_on".to_string(),
        object_id: format!("entity-{scope}-{}", index + 1),
        valid_from: None,
        valid_to: None,
        source: Some("kg-concurrency-test".to_string()),
        confidence: 0.5,
        created_at: chrono::Utc::now(),
        source_agent: None,
    }
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_knowledge_graph_scopes_never_cross_wipe_under_concurrency() {
    // PLAN M-02 / W5: a snapshot replace must only ever touch its own scope,
    // and turn-end maintenance (which snapshots the whole in-memory KG) must
    // not wipe rows another writer owns.
    let fixture = PostgresTestScope::new();
    let store = PostgresMemoryStore::new(fixture.reconnect()).expect("owned memory namespace");
    const SCOPES: usize = 4;

    // Seed every scope with a distinct baseline so a cross-wipe is observable.
    for s in 0..SCOPES {
        let scope = format!("scope-{s}");
        store
            .save_knowledge_graph(
                &scope,
                &(0..50).map(|i| scenario_entity(&scope, i)).collect::<Vec<_>>(),
                &(0..50).map(|i| scenario_triple(&scope, i)).collect::<Vec<_>>(),
            )
            .await
            .expect("seed knowledge graph");
    }

    // Concurrent writers: each scope re-snapshots a *different* count. With a
    // whole-table DELETE (the old behaviour) every writer would erase the
    // other scopes; scoping must make the writes independent.
    let mut handles = Vec::new();
    for s in 0..SCOPES {
        let store = Arc::new(store.clone());
        let scope = format!("scope-{s}");
        handles.push(tokio::spawn(async move {
            let entities = (0..80 + s).map(|i| scenario_entity(&scope, i)).collect::<Vec<_>>();
            let triples = (0..80 + s).map(|i| scenario_triple(&scope, i)).collect::<Vec<_>>();
            store
                .save_knowledge_graph(&scope, &entities, &triples)
                .await
                .expect("concurrent knowledge graph write");
        }));
    }
    for h in handles {
        h.await.expect("writer task joins");
    }

    for s in 0..SCOPES {
        let scope = format!("scope-{s}");
        let entities = store.load_entities(&scope).await.expect("load entities");
        let triples = store.load_triples(&scope).await.expect("load triples");
        assert_eq!(
            entities.len(),
            80 + s,
            "scope {scope} lost or gained entities (cross-scope wipe)"
        );
        assert_eq!(
            triples.len(),
            80 + s,
            "scope {scope} lost or gained triples (cross-scope wipe)"
        );
    }

    // A smaller snapshot must replace only its own scope, not merge into it.
    store
        .save_knowledge_graph("scope-0", &[scenario_entity("scope-0", 0)], &[scenario_triple("scope-0", 0)])
        .await
        .expect("smaller snapshot");
    assert_eq!(
        store.load_entities("scope-0").await.expect("reload scope-0").len(),
        1,
        "a snapshot replace must be a replace, not a merge"
    );
    assert_eq!(store.load_entities("scope-1").await.expect("reload scope-1").len(), 81);
}


#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_empty_knowledge_graph_snapshot_is_rejected() {
// Regression for PLAN M-02 / W5 Step2: a failed/empty KG load must never
// silently clear durable entities/triples.
let fixture = PostgresTestScope::new();
let store = PostgresMemoryStore::new(fixture.reconnect()).expect("owned memory namespace");
let error = store
    .save_knowledge_graph(memory::store::KG_DEFAULT_SCOPE, &[], &[])
    .await
    .expect_err("an empty snapshot must be refused");
assert!(
    error.to_string().contains("empty snapshot"),
    "unexpected error: {error}"
);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL; failed schema retained"]
async fn real_postgres_memory_get_is_readonly_even_when_updates_are_forbidden() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let store = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    let id = uuid::Uuid::new_v4();
    let mut entry = memory_entry(id, "read-only lookup");
    entry.access_count = 11;
    store.insert(&entry).await.unwrap();
    let mut connection = store.executor().checkout_critical().unwrap();
    let before: serde_json::Value = connection
        .query_one(
            "SELECT payload FROM memory_entries WHERE id=$1",
            &[&id.to_string()],
        )
        .unwrap()
        .get(0);
    connection
        .batch_execute(
            "CREATE FUNCTION reject_memory_lookup_write() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'lookup must not mutate memory'; END $$;
         CREATE TRIGGER reject_memory_lookup_write BEFORE UPDATE ON memory_entries
            FOR EACH ROW EXECUTE FUNCTION reject_memory_lookup_write();",
        )
        .unwrap();
    assert!(
        connection
            .execute(
                "UPDATE memory_entries SET payload=payload WHERE id=$1",
                &[&id.to_string()]
            )
            .is_err(),
        "the negative write oracle must actually be armed"
    );
    for _ in 0..3 {
        let loaded = store.get(&id).await.unwrap().unwrap();
        assert_eq!(loaded.id, id);
        assert_eq!(loaded.title, entry.title);
        assert_eq!(loaded.content, entry.content);
        assert_eq!(loaded.access_count, 11);
        assert_eq!(loaded.last_accessed_at, None);
    }
    assert!(store.get(&uuid::Uuid::new_v4()).await.unwrap().is_none());
    let after: serde_json::Value = connection
        .query_one(
            "SELECT payload FROM memory_entries WHERE id=$1",
            &[&id.to_string()],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        after, before,
        "lookup must leave the complete original record unchanged"
    );
    drop(connection);
    drop(store);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_refusal_rolls_back_receipt_and_all_original_rows() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE FUNCTION refuse_canon() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused canon'; END $$; CREATE TRIGGER refuse_canon BEFORE INSERT ON knowledge_canon FOR EACH ROW EXECUTE FUNCTION refuse_canon()").unwrap();
    let document = memory::DocumentContent::new("durable rule", "must retain approval evidence");
    let ingest = || {
        fabric.ingest_document(
            KnowledgeNamespace::Project("cowd".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document.clone(),
        )
    };
    assert!(ingest().is_err());
    let row = connection.query_one("SELECT (SELECT count(*) FROM knowledge_ingestion_receipt), (SELECT count(*) FROM knowledge_corpus), (SELECT count(*) FROM knowledge_pack), (SELECT count(*) FROM knowledge_canon)", &[]).unwrap();
    for column in 0..4 {
        assert_eq!(
            row.get::<_, i64>(column),
            0,
            "a failed transaction must leave no partial receipt or materialization"
        );
    }
    assert!(store.snapshot().unwrap().packs.is_empty());
    connection
        .batch_execute("DROP TRIGGER refuse_canon ON knowledge_canon; DROP FUNCTION refuse_canon()")
        .unwrap();
    let original = ingest().unwrap();
    let repeated = ingest().unwrap();
    assert_eq!(repeated.pack, original.pack);
    assert_eq!(store.snapshot().unwrap().packs.len(), 1);
    drop(connection);
    drop(fabric);
    drop(store);
    drop(executor);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_maintenance_is_atomic_and_rejects_stale_read_sets() {
    use harness_contract::knowledge::KnowledgeObjectState;
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let mut document = memory::DocumentContent::new(
        "conflicted rule",
        "must retain approval evidence\nmust not retain approval evidence",
    );
    document.source = Some("memory:conflicted".into());
    let receipt = fabric
        .ingest_document(
            KnowledgeNamespace::Project("cowd".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document.clone(),
        )
        .unwrap();
    assert!(
        !receipt.conflicts.is_empty(),
        "the late-write refusal must actually be exercised"
    );
    let original = store.snapshot().unwrap();
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE FUNCTION refuse_conflict_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused conflict update'; END $$; CREATE TRIGGER refuse_conflict_update BEFORE UPDATE ON knowledge_conflict FOR EACH ROW EXECUTE FUNCTION refuse_conflict_update()").unwrap();
    assert!(fabric.quarantine_source("memory:conflicted").is_err());
    assert_eq!(
        serde_json::to_value(store.snapshot().unwrap()).unwrap(),
        serde_json::to_value(&original).unwrap(),
        "the earlier pack change must roll back when conflict retirement fails"
    );
    connection.batch_execute("DROP TRIGGER refuse_conflict_update ON knowledge_conflict; DROP FUNCTION refuse_conflict_update()").unwrap();
    let mut changed = receipt.pack.clone();
    changed.governance_level = KnowledgeGovernanceLevel::Blocking;
    store
        .commit_maintenance(
            &store.maintenance_inputs().unwrap(),
            &[changed.clone()],
            &[],
        )
        .unwrap();
    let mut stale = receipt.pack.clone();
    stale.state = KnowledgeObjectState::Quarantined;
    assert!(matches!(
        store.commit_maintenance(&original, &[stale], &[]),
        Err(KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(store.snapshot().unwrap().packs[0], changed);
    fabric.quarantine_source("memory:conflicted").unwrap();
    // Rebuilt owner observes the same original transaction, not an in-process mirror.
    drop(fabric);
    drop(store);
    let reopened_store = Arc::new(PostgresKnowledgeStore::new(fixture.reconnect()).unwrap());
    let reopened = KnowledgeFabric::with_store(reopened_store.clone());
    let replayed = reopened
        .ingest_document(
            KnowledgeNamespace::Project("cowd".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document,
        )
        .unwrap();
    assert_eq!(
        replayed.pack, receipt.pack,
        "receipt replay returns the immutable original fact"
    );
    let current = reopened_store.snapshot().unwrap();
    assert_eq!(current.packs[0].state, KnowledgeObjectState::Quarantined);
    assert_eq!(
        current.conflicts[0].decision.as_deref(),
        Some("source_pack_inactive")
    );
    drop(connection);
    drop(reopened);
    drop(reopened_store);
    drop(executor);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_read_errors_and_oversize_are_not_empty_successes() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let receipt = fabric
        .ingest_document(
            KnowledgeNamespace::Project("cowd".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            memory::DocumentContent::new("policy", "must retain approval evidence"),
        )
        .unwrap();
    let mut connection = executor.checkout_critical().unwrap();
    connection.execute("UPDATE knowledge_canon SET payload=jsonb_set(payload,'{summary}',to_jsonb(repeat('x',8388609))) WHERE canon_id=$1", &[&receipt.canon.canon_id]).unwrap();
    assert!(matches!(
        fabric.activate("s", "approval", "main", Some("cowd")),
        Err(KnowledgeStoreError::ReadLimit)
    ));
    connection
        .execute(
            "UPDATE knowledge_canon SET payload=$2 WHERE canon_id=$1",
            &[
                &receipt.canon.canon_id,
                &serde_json::to_value(&receipt.canon).unwrap(),
            ],
        )
        .unwrap();
    assert!(fabric
        .activate("s", "approval", "main", Some("cowd"))
        .unwrap()
        .0
        .active_pack_ids
        .contains(&receipt.pack.pack_id));
    connection
        .execute(
            "UPDATE knowledge_canon SET payload=payload-'summary' WHERE canon_id=$1",
            &[&receipt.canon.canon_id],
        )
        .unwrap();
    assert!(fabric
        .activate("s", "approval", "main", Some("cowd"))
        .is_err());
    assert!(fabric
        .search_namespace(&KnowledgeNamespace::Project("cowd".into()), "")
        .is_err());
    drop(connection);
    drop(fabric);
    drop(store);
    drop(executor);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_usage_duplicate_is_idempotent_and_changed_identity_rejected() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let store = PostgresKnowledgeStore::new(fixture.reconnect()).unwrap();
    let signal = KnowledgeUsageSignal {
        signal_id: "one-observation".into(),
        session_id: "session".into(),
        pack_id: "pack".into(),
        action: "selected".into(),
        summary: "observed".into(),
        score_delta_bp: 1,
        occurred_at: chrono::Utc::now(),
    };
    store.record_usage(&signal).unwrap();
    store.record_usage(&signal).unwrap();
    let mut wrong = signal.clone();
    wrong.session_id = "foreign".into();
    assert!(store.record_usage(&wrong).is_err());
    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.usage.len(), 1);
    assert_eq!(snapshot.usage[0].session_id, "session");
    drop(store);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_memory_body_and_verbatim_rollback_and_reopen_together() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let mut entry = memory_entry(uuid::Uuid::new_v4(), "atomic-verbatim");
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE FUNCTION refuse_verbatim_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'verbatim transaction refusal'; END $$; CREATE TRIGGER refuse_verbatim_write BEFORE INSERT OR UPDATE OR DELETE ON memory_verbatim FOR EACH ROW EXECUTE FUNCTION refuse_verbatim_write()").unwrap();
    assert!(store.insert(&entry).await.is_err());
    assert!(
        store.get(&entry.id).await.unwrap().is_none(),
        "body insert must roll back on auxiliary refusal"
    );
    assert!(store
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .is_none());
    connection
        .batch_execute("DROP TRIGGER refuse_verbatim_write ON memory_verbatim")
        .unwrap();
    store.insert(&entry).await.unwrap();
    let original = entry.clone();
    let first = store
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.content, "portable memory truth atomic-verbatim");
    assert_eq!(first.source, "Import");
    assert_eq!(first.layer, 3);
    connection.batch_execute("CREATE TRIGGER refuse_verbatim_write BEFORE UPDATE OR DELETE ON memory_verbatim FOR EACH ROW EXECUTE FUNCTION refuse_verbatim_write()").unwrap();
    entry.content = "corrected durable body".into();
    entry.updated_at = chrono::Utc::now();
    assert!(store.update(&entry).await.is_err());
    assert_eq!(
        store.get(&entry.id).await.unwrap().unwrap().content,
        original.content,
        "body update must roll back with refused verbatim update"
    );
    assert_eq!(
        serde_json::to_value(
            store
                .load_verbatim_by_id(&entry.id.to_string())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    assert!(store.delete(&entry.id).await.is_err());
    assert!(
        store.get(&entry.id).await.unwrap().is_some(),
        "delete must roll back when retained text deletion fails"
    );
    assert_eq!(
        serde_json::to_value(
            store
                .load_verbatim_by_id(&entry.id.to_string())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    connection.batch_execute("DROP TRIGGER refuse_verbatim_write ON memory_verbatim; DROP FUNCTION refuse_verbatim_write()").unwrap();
    store.update(&entry).await.unwrap();
    drop(store);
    let reopened = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    assert_eq!(
        reopened.get(&entry.id).await.unwrap().unwrap().content,
        "corrected durable body"
    );
    let corrected = reopened
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(corrected.content, "corrected durable body");
    assert_eq!(corrected.timestamp, entry.updated_at.to_rfc3339());
    reopened.delete(&entry.id).await.unwrap();
    assert!(reopened.get(&entry.id).await.unwrap().is_none());
    assert!(reopened
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        reopened.update(&entry).await,
        Err(MemoryError::NotFound(_))
    ));
    assert!(reopened
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .is_none());
    drop(connection);
    drop(reopened);
    drop(executor);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

fn postgres_query_rule(
    fabric: &KnowledgeFabric,
    namespace: KnowledgeNamespace,
    title: &str,
    level: KnowledgeGovernanceLevel,
) -> memory::KnowledgeIngestionReceipt {
    fabric
        .ingest_document(
            namespace,
            KnowledgeActivationPolicy::DefaultForDomain,
            level,
            DocumentContent::new(title, "must validate source before write"),
        )
        .unwrap()
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_scope_is_applied_before_foreign_payload_budget() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let store = Arc::new(PostgresKnowledgeStore::new(fixture.reconnect()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let own = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A policy",
        KnowledgeGovernanceLevel::Required,
    );
    let foreign = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("B".into()),
        "B policy",
        KnowledgeGovernanceLevel::Advisory,
    );
    let shared = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::SharedLibrary("global".into()),
        "shared safety",
        KnowledgeGovernanceLevel::Blocking,
    );
    let mut oversized = foreign.pack.clone();
    oversized.name = "x".repeat(memory::knowledge::KNOWLEDGE_READ_MAX_BYTES + 1);
    store
        .commit_maintenance(&store.maintenance_inputs().unwrap(), &[oversized], &[])
        .unwrap();
    let (plan, canon, warnings) = fabric
        .activate("s", "validate source", "main", Some("A"))
        .unwrap();
    assert!(plan.active_pack_ids.contains(&own.pack.pack_id));
    assert!(plan.active_pack_ids.contains(&shared.pack.pack_id));
    assert!(!plan.active_pack_ids.contains(&foreign.pack.pack_id));
    assert!(canon.iter().any(|item| item.pack_id == shared.pack.pack_id));
    assert!(warnings
        .iter()
        .any(|item| item.pack_id == shared.pack.pack_id
            && item.level == KnowledgeGovernanceLevel::Blocking));
    assert!(matches!(
        fabric.activate("s", "validate source", "main", Some("B")),
        Err(KnowledgeStoreError::ReadLimit)
    ));
    drop(fabric);
    drop(store);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_continuation_preserves_scope_revision_and_restart_identity() {
    use memory::knowledge::{KnowledgeCandidateScope, KNOWLEDGE_READ_MAX_BYTES};
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let store = Arc::new(PostgresKnowledgeStore::new(fixture.reconnect()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let first_receipt = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A first",
        KnowledgeGovernanceLevel::Required,
    );
    postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A second",
        KnowledgeGovernanceLevel::Required,
    );
    postgres_query_rule(
        &fabric,
        KnowledgeNamespace::SharedLibrary("global".into()),
        "shared first",
        KnowledgeGovernanceLevel::Blocking,
    );
    let scope = KnowledgeCandidateScope::Activation {
        project_id: Some("A".into()),
    };
    let first = store
        .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
        .unwrap();
    let cursor = first
        .next
        .as_ref()
        .expect("the original read must actually continue")
        .clone();
    postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("B".into()),
        "B late",
        KnowledgeGovernanceLevel::Required,
    );
    // Original idempotent receipt replay must not advance an unchanged source.
    let replay = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A first",
        KnowledgeGovernanceLevel::Required,
    );
    assert_eq!(replay.pack, first_receipt.pack);
    drop(fabric);
    drop(store);
    let store = Arc::new(PostgresKnowledgeStore::new(fixture.reconnect()).unwrap());
    let continued = store
        .read_candidate_page(&scope, Some(&cursor), 1, KNOWLEDGE_READ_MAX_BYTES)
        .unwrap();
    assert_eq!(continued.revision, first.revision);
    assert_eq!(continued.packs.len(), 1);
    assert_ne!(continued.packs[0].pack_id, first.packs[0].pack_id);
    let wrong_scope = KnowledgeCandidateScope::Activation {
        project_id: Some("B".into()),
    };
    assert!(matches!(
        store.read_candidate_page(&wrong_scope, Some(&cursor), 1, KNOWLEDGE_READ_MAX_BYTES),
        Err(KnowledgeStoreError::StaleRead)
    ));
    let fabric = KnowledgeFabric::with_store(store.clone());
    postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A late",
        KnowledgeGovernanceLevel::Required,
    );
    assert!(matches!(
        store.read_candidate_page(&scope, Some(&cursor), 1, KNOWLEDGE_READ_MAX_BYTES),
        Err(KnowledgeStoreError::StaleRead)
    ));
    let fresh = store
        .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
        .unwrap();
    postgres_query_rule(
        &fabric,
        KnowledgeNamespace::SharedLibrary("global".into()),
        "shared late",
        KnowledgeGovernanceLevel::Blocking,
    );
    assert!(matches!(
        store.read_candidate_page(&scope, fresh.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES),
        Err(KnowledgeStoreError::StaleRead)
    ));
    drop(fabric);
    drop(store);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_canonical_source_change_invalidates_continuation() {
    use memory::knowledge::{KnowledgeCandidateScope, KNOWLEDGE_READ_MAX_BYTES};
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let first = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A first",
        KnowledgeGovernanceLevel::Required,
    );
    let second = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("A".into()),
        "A second",
        KnowledgeGovernanceLevel::Required,
    );
    let scope = KnowledgeCandidateScope::Activation {
        project_id: Some("A".into()),
    };
    let page = store
        .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
        .unwrap();
    assert!(page.next.is_some());
    let mut connection = executor.checkout_critical().unwrap();
    connection.execute("UPDATE knowledge_canon SET payload=jsonb_set(payload,'{summary}',to_jsonb('changed canonical source'::text)) WHERE canon_id=$1",&[&second.canon.canon_id]).unwrap();
    assert!(matches!(
        store.read_candidate_page(&scope, page.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES),
        Err(KnowledgeStoreError::StaleRead)
    ));
    // A foreign canonical binding is not granted by matching project metadata.
    let foreign = postgres_query_rule(
        &fabric,
        KnowledgeNamespace::Project("B".into()),
        "B policy",
        KnowledgeGovernanceLevel::Blocking,
    );
    let mut wrong = first.pack;
    wrong.canon_pack_ref = Some(foreign.canon.canon_id);
    store
        .commit_maintenance(&store.maintenance_inputs().unwrap(), &[wrong], &[])
        .unwrap();
    assert!(fabric
        .activate("s", "validate source", "main", Some("A"))
        .is_err());
    drop(connection);
    drop(fabric);
    drop(store);
    drop(executor);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_continuation_cannot_borrow_another_store_with_equal_counters() {
    use memory::knowledge::{KnowledgeCandidateScope, KNOWLEDGE_READ_MAX_BYTES};
    let left = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let right = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let first = Arc::new(PostgresKnowledgeStore::new(left.reconnect()).unwrap());
    let second = Arc::new(PostgresKnowledgeStore::new(right.reconnect()).unwrap());
    for store in [&first, &second] {
        let fabric = KnowledgeFabric::with_store(store.clone());
        postgres_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A first",
            KnowledgeGovernanceLevel::Required,
        );
        postgres_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A second",
            KnowledgeGovernanceLevel::Required,
        );
    }
    let scope = KnowledgeCandidateScope::Activation {
        project_id: Some("A".into()),
    };
    let a = first
        .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
        .unwrap();
    let b = second
        .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
        .unwrap();
    assert_eq!(a.revision.counters, b.revision.counters);
    assert_ne!(a.revision.store_id, b.revision.store_id);
    assert!(a.next.is_some());
    assert!(b.next.is_some());
    assert!(matches!(
        second.read_candidate_page(&scope, a.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES),
        Err(KnowledgeStoreError::StaleRead)
    ));
    assert!(second
        .read_candidate_page(&scope, b.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES)
        .is_ok());
    drop(first);
    drop(second);
    drop(std::mem::ManuallyDrop::into_inner(left));
    drop(std::mem::ManuallyDrop::into_inner(right));
}
