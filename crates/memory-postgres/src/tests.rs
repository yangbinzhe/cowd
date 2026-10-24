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

fn erase_command(entry: &MemoryEntry, history: Option<String>) -> MemoryLifecycleMutation {
    let from: Vec<memory::MemoryLifecycleEvent> = history
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .unwrap()
        .unwrap_or_default();
    MemoryLifecycleMutation::erase(
        entry,
        history,
        memory::MemoryLifecycleEvent {
            memory_id: entry.id,
            from: from.last().map(|event| event.to),
            to: memory::MemoryState::Archived,
            reason: "explicit native erasure".into(),
            session_id: "native-test".into(),
            agent_id: "native-test".into(),
            occurred_at: chrono::Utc::now(),
        },
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_memory_body_cas_create_conflict_and_observation_survive_reopen() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let original = memory_entry(uuid::Uuid::new_v4(), "target-cas");
    store.insert(&original).await.unwrap();
    let concurrent = memory_entry(uuid::Uuid::new_v4(), "same-concurrent-create");
    let (left, right) = tokio::join!(store.insert(&concurrent), store.insert(&concurrent));
    assert_eq!(left.unwrap(), concurrent.id);
    assert_eq!(right.unwrap(), concurrent.id);
    let revision = MemoryEntryRevision::of(&original).unwrap();
    {
        let mut c = executor.checkout_critical().unwrap();
        c.batch_execute("CREATE FUNCTION refuse_replay_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'exact create must not rewrite'; END $$; CREATE TRIGGER refuse_replay_write BEFORE INSERT OR UPDATE ON memory_entries FOR EACH ROW EXECUTE FUNCTION refuse_replay_write()").unwrap();
    }
    assert_eq!(store.insert(&original).await.unwrap(), original.id);
    {
        let mut c = executor.checkout_critical().unwrap();
        c.batch_execute("DROP TRIGGER refuse_replay_write ON memory_entries; DROP FUNCTION refuse_replay_write()").unwrap();
    }
    let mut first = original.clone();
    first.content = "first contender".into();
    let mut second = original.clone();
    second.content = "second contender".into();
    assert!(store.insert(&first).await.is_err());
    let (a, b) = tokio::join!(
        store.update(&first, &revision),
        store.update(&second, &revision)
    );
    assert_ne!(
        a.is_ok(),
        b.is_ok(),
        "exactly one original revision may commit"
    );
    let winner = if a.is_ok() { first } else { second };
    let winner_revision = MemoryEntryRevision::of(&winner).unwrap();
    let mut wrong_scope = winner.clone();
    wrong_scope.scope = MemoryScope::Project("foreign".into());
    assert!(store.update(&wrong_scope, &winner_revision).await.is_err());
    let mut wrong_id = winner.clone();
    wrong_id.id = uuid::Uuid::new_v4();
    assert!(store.update(&wrong_id, &winner_revision).await.is_err());
    let mut observed = winner.clone();
    observed.access_count = 7;
    observed.last_accessed_at = Some(chrono::Utc::now());
    store.update(&observed, &winner_revision).await.unwrap();
    let mut correction = winner;
    correction.content = "correction after observation".into();
    store.update(&correction, &winner_revision).await.unwrap();
    drop(store);
    let reopened = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    let got = reopened.get(&original.id).await.unwrap().unwrap();
    assert_eq!(got.content, "correction after observation");
    assert_eq!(got.scope, original.scope);
    assert_eq!(got.access_count, 7);
    assert_eq!(got.last_accessed_at, observed.last_accessed_at);
    assert_eq!(
        reopened
            .load_verbatim_by_id(&original.id.to_string())
            .await
            .unwrap()
            .unwrap()
            .content,
        "correction after observation"
    );
    let archive = MemoryLifecycleMutation::new(
        &got,
        None,
        memory::MemoryLifecycleEvent {
            memory_id: got.id,
            from: None,
            to: memory::MemoryState::Archived,
            reason: "original semantic cut after observation".into(),
            session_id: "cas-session".into(),
            agent_id: "cas-agent".into(),
            occurred_at: chrono::Utc::now(),
        },
    )
    .unwrap();
    let mut later_observation = got.clone();
    later_observation.access_count = 8;
    reopened
        .update(&later_observation, &MemoryEntryRevision::of(&got).unwrap())
        .await
        .unwrap();
    let receipt = reopened.commit_lifecycle(archive.clone()).await.unwrap();
    assert!(
        receipt.source.retired,
        "native lifecycle uses the same semantic body cut as correction"
    );
    assert_eq!(
        reopened
            .lifecycle_receipt(&archive.mutation_id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(reopened
        .lifecycle_receipt("unknown-operation")
        .await
        .unwrap()
        .is_none());
    archive.validate_retirement_confirmation(&receipt).unwrap();
    for field in ["scope", "actor", "memory", "state", "reason"] {
        let mut foreign = archive.clone();
        match field {
            "scope" => foreign.scope_key = "foreign-scope".into(),
            "actor" => foreign.event.agent_id = "foreign-agent".into(),
            "memory" => foreign.event.memory_id = uuid::Uuid::new_v4(),
            "state" => foreign.event.to = memory::MemoryState::Observed,
            "reason" => foreign.event.reason = "foreign reason".into(),
            _ => unreachable!(),
        }
        assert!(foreign.validate_retirement_confirmation(&receipt).is_err());
    }
    let mut forged = archive.clone();
    forged.expected_entry_digest = "different cut".into();
    assert!(reopened.commit_lifecycle(forged).await.is_err());
    assert_eq!(reopened.commit_lifecycle(archive).await.unwrap(), receipt);
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
        let original = reopened.get(&concurrent_id).await.unwrap().unwrap();
        reopened
            .commit_lifecycle(erase_command(&original, None))
            .await
            .unwrap();
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
        memory_source_revisions: Vec::new(),
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
        memory_source_revision: None,
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
    document.source = Some("import-fixture:conflicted".into());
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
    assert!(fabric
        .quarantine_source("import-fixture:conflicted")
        .is_err());
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
    fabric
        .quarantine_source("import-fixture:conflicted")
        .unwrap();
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
    assert!(store
        .update(&entry, &MemoryEntryRevision::of(&original).unwrap())
        .await
        .is_err());
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
    let rejected_erase = erase_command(&original, None);
    assert!(store
        .commit_lifecycle(rejected_erase.clone())
        .await
        .is_err());
    for table in ["memory_lifecycle_receipts", "memory_cleanup_pending"] {
        let count: i64 = connection
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE mutation_id=$1"),
                &[&rejected_erase.mutation_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            count, 0,
            "a refused retained-text delete cannot publish receipt or pending"
        );
    }
    let fences: i64 = connection
        .query_one(
            "SELECT count(*) FROM memory_source_fences WHERE source_ref=$1",
            &[&format!("memory:{}", original.id)],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        fences, 0,
        "the same refusal must roll back source retirement"
    );
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
    store
        .update(&entry, &MemoryEntryRevision::of(&original).unwrap())
        .await
        .unwrap();
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
    reopened
        .commit_lifecycle(erase_command(&entry, None))
        .await
        .unwrap();
    assert!(reopened.get(&entry.id).await.unwrap().is_none());
    assert!(reopened
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .is_none());
    assert!(
        reopened
            .update(&entry, &MemoryEntryRevision::of(&entry).unwrap())
            .await
            .is_err(),
        "erased source must remain fenced against rewriting"
    );
    assert!(
        reopened
            .read_source_fence(&format!("memory:{}", entry.id))
            .unwrap()
            .unwrap()
            .retired
    );
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

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_memory_archive_pending_replays_after_knowledge_failure_and_reopen() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let root = std::env::temp_dir().join(format!("cowd-memory-cleanup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let knowledge = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(knowledge.clone()).with_memory_sources(store.clone());
    // Namespace-only search has no Agent grant; these original project
    // sources are explicitly shared. Private-source denial is a separate gate.
    let mut entry = memory_entry(uuid::Uuid::new_v4(), "archive-cleanup");
    entry.visibility = AgentVisibility::Shared;
    let mut independent = memory_entry(uuid::Uuid::new_v4(), "independent-cleanup");
    independent.visibility = AgentVisibility::Shared;
    store.insert(&entry).await.unwrap();
    store.insert(&independent).await.unwrap();
    let mut foreign_source = memory_entry(uuid::Uuid::new_v4(), "foreign original source");
    foreign_source.scope = MemoryScope::Project("foreign-project".into());
    foreign_source.visibility = AgentVisibility::Shared;
    store.insert(&foreign_source).await.unwrap();
    let foreign_scope = foreign_source.scope.scope_key();
    let mut document = DocumentContent::new("retiring pack", "Keep original evidence.");
    document.source = Some(format!("memory:{}", entry.id));
    document.memory_source_revision = Some(memory::store::MemoryEntryRevision::of(&entry).unwrap());
    let retired = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document,
        )
        .unwrap();
    let mut other = DocumentContent::new("independent pack", "Independent valid source.");
    other.source = Some(format!("memory:{}", independent.id));
    other.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&independent).unwrap());
    let other = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            other,
        )
        .unwrap();
    let entity = |name: &str, sources: Vec<String>| memory::entity::Entity {
        id: name.into(),
        name: name.into(),
        entity_type: memory::entity::EntityType::Concept,
        confidence: 1.0,
        frequency: 1,
        first_seen: chrono::Utc::now(),
        last_seen: chrono::Utc::now(),
        source_ids: sources.clone(),
        source_type: if sources.is_empty() {
            "independent"
        } else {
            "memory"
        }
        .into(),
        memory_source_revisions: sources
            .iter()
            .map(|id| {
                let source = [&entry, &independent, &foreign_source]
                    .into_iter()
                    .find(|source| source.id.to_string() == *id)
                    .unwrap();
                memory::store::MemoryEntryRevision::of(source).unwrap()
            })
            .collect(),
    };
    let only = entity("only-source", vec![entry.id.to_string()]);
    let multi = entity(
        "multi-source",
        vec![entry.id.to_string(), independent.id.to_string()],
    );
    let empty = entity("independent-empty", vec![]);
    let scope = entry.scope.scope_key();
    store
        .save_entities(&scope, &[only.clone(), multi.clone(), empty.clone()])
        .await
        .unwrap();
    assert!(store
        .save_entities(&foreign_scope, &[only.clone()])
        .await
        .is_err());
    let mut foreign_entity = only.clone();
    foreign_entity.source_ids = vec![foreign_source.id.to_string()];
    foreign_entity.memory_source_revisions =
        vec![memory::store::MemoryEntryRevision::of(&foreign_source).unwrap()];
    store
        .save_entities(&foreign_scope, &[foreign_entity])
        .await
        .unwrap();
    let triple = memory::entity::Triple {
        id: "same-scoped-triple".into(),
        subject_id: "only-source".into(),
        predicate: "must retain".into(),
        object_id: "independent-empty".into(),
        valid_from: None,
        valid_to: None,
        source: Some(format!("memory:{}", entry.id)),
        confidence: 1.0,
        created_at: chrono::Utc::now(),
        source_agent: None,
        memory_source_revision: Some(memory::store::MemoryEntryRevision::of(&entry).unwrap()),
    };
    let mut independent_triple = triple.clone();
    independent_triple.id = "independent-triple".into();
    independent_triple.source = Some(format!("memory:{}", independent.id));
    independent_triple.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&independent).unwrap());
    store
        .save_triples(&scope, &[triple.clone(), independent_triple.clone()])
        .await
        .unwrap();
    assert!(store
        .save_triples(&foreign_scope, &[triple.clone()])
        .await
        .is_err());
    let mut foreign_triple = triple.clone();
    foreign_triple.source = Some(format!("memory:{}", foreign_source.id));
    foreign_triple.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&foreign_source).unwrap());
    store
        .save_triples(&foreign_scope, &[foreign_triple])
        .await
        .unwrap();
    // A persisted fence payload cannot borrow another source identity, even
    // when its namespace happens to match this pack's namespace.
    let source = format!("memory:{}", independent.id);
    let valid = MemorySourceFence {
        source_ref: source.clone(),
        scope_key: scope.clone(),
        delete_epoch: 0,
        retired: false,
    };
    let mut malformed = valid.clone();
    malformed.source_ref = format!("memory:{}", entry.id);
    let mut c = executor.checkout_critical().unwrap();
    c.execute("INSERT INTO memory_source_fences(source_ref,scope_key,delete_epoch,payload) VALUES($1,$2,0,$3)",&[&source,&scope,&serde_json::to_value(malformed).unwrap()]).unwrap();
    assert!(matches!(
        fabric.search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), ""),
        Err(memory::KnowledgeStoreError::MemorySourceRead(_))
    ));
    c.execute(
        "UPDATE memory_source_fences SET payload=$2 WHERE source_ref=$1",
        &[&source, &serde_json::to_value(valid).unwrap()],
    )
    .unwrap();
    drop(c);
    let visible = fabric
        .search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), "")
        .unwrap();
    assert!(visible
        .packs
        .iter()
        .any(|p| p.pack_id == other.pack.pack_id));
    assert!(visible
        .packs
        .iter()
        .any(|p| p.pack_id == retired.pack.pack_id));
    let config = || {
        let mut config = memory::MemoryConfig::default();
        config.store.blob_dir = root.join("blobs");
        config.store.enable_vector_index = false;
        config.governance.enabled = false;
        config
    };
    let manager = Arc::new(
        memory::CognitiveContextManager::new_with_selected_store(config(), None, store.clone())
            .await
            .unwrap(),
    );
    let weak = Arc::downgrade(&manager);
    let kernel = memory::MemoryKernel::new(manager.clone());
    let ctx = memory::MemoryTurnContext::new("durable-archive", "operator");
    let mut connection = executor.checkout_critical().unwrap();
    // This really refuses the original Knowledge transaction, after the Memory
    // transaction has already committed the source fence and pending fact.
    connection.batch_execute("CREATE FUNCTION refuse_cleanup_pack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'quarantine unavailable'; END $$; CREATE TRIGGER refuse_cleanup_pack BEFORE UPDATE ON knowledge_pack FOR EACH ROW EXECUTE FUNCTION refuse_cleanup_pack()").unwrap();
    kernel
        .archive(&ctx, entry.id, "archive exactly once")
        .await
        .unwrap();
    let pending = store.pending_memory_cleanup(None, 16).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].source.delete_epoch, 1);
    assert!(!manager
        .recover_memory_cleanup(&fabric, 16)
        .await
        .unwrap()
        .is_empty());
    let visible = fabric
        .search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), "")
        .unwrap();
    assert!(visible
        .packs
        .iter()
        .all(|p| p.pack_id != retired.pack.pack_id));
    assert!(visible
        .packs
        .iter()
        .any(|p| p.pack_id == other.pack.pack_id));
    assert!(store
        .update(&entry, &MemoryEntryRevision::of(&entry).unwrap())
        .await
        .is_err());
    assert!(store.insert(&entry).await.is_err());
    assert!(store.pending_memory_cleanup(None, 16).await.unwrap()[0]
        .knowledge_receipt_digest
        .is_none());
    connection
        .batch_execute("DROP TRIGGER refuse_cleanup_pack ON knowledge_pack")
        .unwrap();
    assert_eq!(
        connection
            .query_one(
                "SELECT count(*) FROM knowledge_memory_retirement_receipts",
                &[]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // Refuse the last write too: pack changes may not escape without their
    // exact source/epoch commit evidence.
    connection.batch_execute("CREATE FUNCTION refuse_cleanup_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'receipt unavailable'; END $$; CREATE TRIGGER refuse_cleanup_receipt BEFORE INSERT ON knowledge_memory_retirement_receipts FOR EACH ROW EXECUTE FUNCTION refuse_cleanup_receipt()").unwrap();
    assert!(!manager
        .recover_memory_cleanup(&fabric, 16)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        connection
            .query_one(
                "SELECT count(*) FROM knowledge_memory_retirement_receipts",
                &[]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let original_row = connection
        .query_one(
            "SELECT payload FROM knowledge_pack WHERE pack_id=$1",
            &[&retired.pack.pack_id],
        )
        .unwrap()
        .get::<_, serde_json::Value>(0);
    assert_eq!(original_row, serde_json::to_value(&retired.pack).unwrap());
    assert!(store.pending_memory_cleanup(None, 16).await.unwrap()[0]
        .knowledge_receipt_digest
        .is_none());
    connection
        .batch_execute(
            "DROP TRIGGER refuse_cleanup_receipt ON knowledge_memory_retirement_receipts",
        )
        .unwrap();

    drop(connection);
    drop(kernel);
    drop(manager);
    assert!(weak.upgrade().is_none());
    drop(fabric);
    drop(knowledge);
    drop(store);
    drop(executor);
    let reopened_executor = fixture.reconnect();
    let reopened_store = Arc::new(PostgresMemoryStore::new(reopened_executor.clone()).unwrap());
    let reopened_fabric = KnowledgeFabric::with_store(Arc::new(
        PostgresKnowledgeStore::new(reopened_executor).unwrap(),
    ))
    .with_memory_sources(reopened_store.clone());
    let reopened = memory::CognitiveContextManager::new_with_selected_store(
        config(),
        None,
        reopened_store.clone(),
    )
    .await
    .unwrap();
    assert!(!config().governance.enabled);
    // No client archive or explicit Graph registration is replayed.
    assert!(reopened
        .recover_memory_cleanup(&reopened_fabric, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(reopened_store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    let actual_receipt = reopened_fabric
        .quarantine_memory_source(&pending[0].source)
        .unwrap();
    assert_eq!(actual_receipt.source, pending[0].source);
    assert_eq!(actual_receipt.pack_ids, vec![retired.pack.pack_id.clone()]);
    assert_eq!(
        reopened_fabric
            .quarantine_memory_source(&pending[0].source)
            .unwrap(),
        actual_receipt
    );
    let mut inspect = fixture.reconnect().checkout_online_read().unwrap();
    let stored_receipt: memory::knowledge::MemorySourceQuarantineReceipt = serde_json::from_value(inspect.query_one("SELECT payload FROM knowledge_memory_retirement_receipts WHERE source_ref=$1 AND delete_epoch=1", &[&pending[0].source.source_ref]).unwrap().get(0)).unwrap();
    assert_eq!(stored_receipt, actual_receipt);
    assert_eq!(inspect.query_one("SELECT receipt_digest FROM knowledge_memory_retirement_receipts WHERE source_ref=$1 AND delete_epoch=1", &[&pending[0].source.source_ref]).unwrap().get::<_, String>(0), actual_receipt.digest().unwrap());
    // A payload-only corruption may not borrow the original receipt key/digest.
    drop(inspect);
    let mut inspect = fixture.reconnect().checkout_critical().unwrap();
    inspect.execute("UPDATE knowledge_memory_retirement_receipts SET payload=jsonb_set(payload,'{pack_ids}','[\"foreign-pack\"]'::jsonb) WHERE source_ref=$1 AND delete_epoch=1", &[&pending[0].source.source_ref]).unwrap();
    assert!(matches!(
        reopened_fabric.quarantine_memory_source(&pending[0].source),
        Err(KnowledgeStoreError::StaleRead)
    ));
    inspect.execute("UPDATE knowledge_memory_retirement_receipts SET payload=$2 WHERE source_ref=$1 AND delete_epoch=1", &[&pending[0].source.source_ref,&serde_json::to_value(&actual_receipt).unwrap()]).unwrap();
    assert_eq!(
        reopened_fabric
            .quarantine_memory_source(&pending[0].source)
            .unwrap(),
        actual_receipt
    );
    drop(inspect);
    // A valid prepared result arriving late cannot bypass the native write
    // fence; immutable original receipt replay is still a read of its old fact.
    let writer = PostgresKnowledgeStore::new(fixture.reconnect()).unwrap();
    let mut late = retired.clone();
    late.corpus.corpus_id = "late-after-retirement-corpus".into();
    late.pack.pack_id = "late-after-retirement-pack".into();
    late.pack.source_corpus_refs = vec![late.corpus.corpus_id.clone()];
    late.canon.canon_id = "late-after-retirement-canon".into();
    late.canon.pack_id = late.pack.pack_id.clone();
    late.pack.canon_pack_ref = Some(late.canon.canon_id.clone());
    late.chunks.clear();
    late.corpus.chunk_count = 0;
    late.conflicts.clear();
    assert!(matches!(
        writer.save_receipt(&late),
        Err(KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(writer.save_receipt(&retired).unwrap().pack, retired.pack);
    let mut revive = writer
        .snapshot()
        .unwrap()
        .packs
        .into_iter()
        .find(|p| p.pack_id == retired.pack.pack_id)
        .unwrap();
    revive.state = harness_contract::knowledge::KnowledgeObjectState::Active;
    revive.source_corpus_refs = vec![other.corpus.corpus_id.clone()];
    assert!(matches!(
        writer.commit_maintenance(&writer.maintenance_inputs().unwrap(), &[revive], &[]),
        Err(KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        writer.snapshot().unwrap().packs.len(),
        2,
        "late failed command leaves no new body/receipt materialization"
    );
    drop(writer);
    let triples = reopened_store.load_triples(&scope).await.unwrap();
    assert_eq!(triples.len(), 1);
    assert_eq!(triples[0].id, "independent-triple");
    assert_eq!(
        reopened_store.load_triples(&foreign_scope).await.unwrap()[0].id,
        "same-scoped-triple"
    );
    let entities = reopened_store.load_entities(&scope).await.unwrap();
    assert!(!entities.iter().any(|e| e.id == "only-source"));
    assert!(entities
        .iter()
        .any(|e| e.id == "independent-empty" && e.source_ids.is_empty()));
    assert_eq!(
        entities
            .iter()
            .find(|e| e.id == "multi-source")
            .unwrap()
            .source_ids,
        vec![independent.id.to_string()]
    );
    assert_eq!(
        reopened_store.load_entities(&foreign_scope).await.unwrap()[0].source_ids,
        vec![foreign_source.id.to_string()]
    );
    assert!(reopened_store.get(&entry.id).await.unwrap().is_some());
    assert!(reopened_store
        .load_verbatim_by_id(&entry.id.to_string())
        .await
        .unwrap()
        .is_some());
    let reopened = Arc::new(reopened);
    let reopened_kernel = memory::MemoryKernel::new(reopened.clone());
    reopened_kernel
        .archive(&ctx, entry.id, "archive exactly once")
        .await
        .unwrap();
    assert_eq!(
        reopened_store
            .read_source_fence(&format!("memory:{}", entry.id))
            .unwrap()
            .unwrap()
            .delete_epoch,
        1
    );
    assert!(reopened_store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    // The native receiver must also accept an authentic older retirement
    // receipt after its source advances to another irreversible retirement.
    reopened_kernel
        .archive(&ctx, entry.id, "second independent retirement")
        .await
        .unwrap();
    assert_eq!(
        reopened_store
            .read_source_fence(&pending[0].source.source_ref)
            .unwrap()
            .unwrap()
            .delete_epoch,
        2
    );
    assert_eq!(
        reopened_fabric
            .quarantine_memory_source(&pending[0].source)
            .unwrap(),
        actual_receipt
    );
    assert!(reopened
        .recover_memory_cleanup(&reopened_fabric, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(reopened_store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    drop(reopened_kernel);
    drop(reopened);
    // Deliberate storage corruption in this owned schema, never a production
    // deletion entry: a live fence must not authorize an absent original body.
    {
        let mut connection = reopened_store.executor().checkout_critical().unwrap();
        connection
            .execute(
                "DELETE FROM memory_entries WHERE id=$1",
                &[&independent.id.to_string()],
            )
            .unwrap();
    }
    assert!(reopened_store
        .read_source_fence(&format!("memory:{}", independent.id))
        .is_err());
    assert!(matches!(
        reopened_fabric.search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), ""),
        Err(memory::KnowledgeStoreError::MemorySourceRead(_))
    ));
    drop(reopened_fabric);
    drop(reopened_store);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_knowledge_source_retirement_ignores_foreign_budget_and_reopens() {
    use harness_contract::knowledge::KnowledgeObjectState;
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let mut document = memory::DocumentContent::new("own", "must retain\nmust not retain");
    document.source = Some("import-fixture:own-retirement".into());
    let own = fabric
        .ingest_document(
            KnowledgeNamespace::Project("A".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document,
        )
        .unwrap();
    let mut foreign_doc = memory::DocumentContent::new("foreign", "foreign policy");
    foreign_doc.source = Some("import-fixture:foreign-retirement".into());
    let foreign = fabric
        .ingest_document(
            KnowledgeNamespace::Project("B".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            foreign_doc,
        )
        .unwrap();
    let mut oversized = foreign.pack.clone();
    oversized.name = "x".repeat(memory::knowledge::KNOWLEDGE_READ_MAX_BYTES + 1);
    store
        .commit_maintenance(
            &store.maintenance_inputs().unwrap(),
            &[oversized.clone()],
            &[],
        )
        .unwrap();
    assert!(matches!(
        store.maintenance_inputs(),
        Err(KnowledgeStoreError::ReadLimit)
    ));
    assert!(!own.conflicts.is_empty());
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE FUNCTION refuse_retire() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused retirement'; END $$; CREATE TRIGGER refuse_retire BEFORE UPDATE ON knowledge_conflict FOR EACH ROW EXECUTE FUNCTION refuse_retire()").unwrap();
    assert!(fabric
        .quarantine_source("import-fixture:own-retirement")
        .is_err());
    let row = connection
        .query_one(
            "SELECT payload FROM knowledge_pack WHERE pack_id=$1",
            &[&own.pack.pack_id],
        )
        .unwrap();
    assert_eq!(
        row.get::<_, serde_json::Value>(0),
        serde_json::to_value(&own.pack).unwrap(),
        "late conflict failure rolls pack write back"
    );
    connection
        .batch_execute(
            "DROP TRIGGER refuse_retire ON knowledge_conflict; DROP FUNCTION refuse_retire()",
        )
        .unwrap();
    assert_eq!(
        fabric
            .quarantine_source("import-fixture:own-retirement")
            .unwrap(),
        vec![own.pack.pack_id.clone()]
    );
    assert!(matches!(
        fabric.quarantine_source("import-fixture:foreign-retirement"),
        Err(KnowledgeStoreError::ReadLimit)
    ));
    let row = connection
        .query_one(
            "SELECT payload FROM knowledge_pack WHERE pack_id=$1",
            &[&foreign.pack.pack_id],
        )
        .unwrap();
    assert_eq!(
        row.get::<_, serde_json::Value>(0),
        serde_json::to_value(&oversized).unwrap()
    );
    drop(connection);
    drop(fabric);
    drop(store);
    drop(executor);
    let reopened = Arc::new(PostgresKnowledgeStore::new(fixture.reconnect()).unwrap());
    assert!(reopened
        .quarantine_source("import-fixture:own-retirement", None)
        .unwrap()
        .pack_ids
        .is_empty());
    let mut connection = reopened.executor.checkout_online_read().unwrap();
    let row = connection
        .query_one(
            "SELECT payload FROM knowledge_pack WHERE pack_id=$1",
            &[&own.pack.pack_id],
        )
        .unwrap();
    let pack: harness_contract::knowledge::KnowledgePack =
        serde_json::from_value(row.get(0)).unwrap();
    assert_eq!(pack.state, KnowledgeObjectState::Quarantined);
    for conflict in own.conflicts {
        let row = connection
            .query_one(
                "SELECT payload->>'decision' FROM knowledge_conflict WHERE conflict_id=$1",
                &[&conflict.conflict_id],
            )
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "source_pack_inactive");
    }
    drop(connection);
    drop(reopened);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_lifecycle_receipt_binds_cut_and_later_observations_after_reopen() {
    let fixture = PostgresTestScope::new();
    let store = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    let original = memory_entry(uuid::Uuid::new_v4(), "receipt-cut");
    store.insert(&original).await.unwrap();
    let first_event = memory::MemoryLifecycleEvent {
        memory_id: original.id,
        from: None,
        to: memory::MemoryState::Observed,
        reason: "accepted source observation".into(),
        session_id: "receipt-session".into(),
        agent_id: "receipt-agent".into(),
        occurred_at: chrono::Utc::now(),
    };
    let first =
        memory::store::MemoryLifecycleMutation::new(&original, None, first_event.clone()).unwrap();
    let receipt = store.commit_lifecycle(first.clone()).await.unwrap();
    let history_key = format!("memory_lifecycle:{}", original.id);
    let history = store.kv_get(&history_key).await.unwrap().unwrap();
    for field in [
        "body", "history", "from", "scope", "actor", "memory", "state", "reason",
    ] {
        let mut forged = first.clone();
        match field {
            "body" => forged.expected_entry_digest = "foreign-body".into(),
            "history" => forged.expected_history = Some("[]".into()),
            "from" => forged.event.from = Some(memory::MemoryState::Active),
            "scope" => forged.scope_key = "foreign-scope".into(),
            "actor" => forged.event.agent_id = "foreign-agent".into(),
            "memory" => forged.event.memory_id = uuid::Uuid::new_v4(),
            "state" => forged.event.to = memory::MemoryState::Active,
            "reason" => forged.event.reason = "foreign reason".into(),
            _ => unreachable!(),
        }
        assert!(
            store.commit_lifecycle(forged).await.is_err(),
            "replay must bind original {field}"
        );
        assert_eq!(
            store.kv_get(&history_key).await.unwrap().as_deref(),
            Some(history.as_str())
        );
    }
    let mut second_event = first_event.clone();
    second_event.from = Some(memory::MemoryState::Observed);
    second_event.occurred_at += chrono::Duration::seconds(1);
    let second =
        memory::store::MemoryLifecycleMutation::new(&original, Some(history), second_event.clone())
            .unwrap();
    let second_receipt = store.commit_lifecycle(second).await.unwrap();
    assert_eq!(second_receipt.event, second_event);
    let mut third_event = second_event.clone();
    third_event.occurred_at += chrono::Duration::seconds(1);
    let third = memory::store::MemoryLifecycleMutation::new(
        &original,
        store.kv_get(&history_key).await.unwrap(),
        third_event.clone(),
    )
    .unwrap();
    assert_eq!(
        store.commit_lifecycle(third).await.unwrap().event,
        third_event
    );
    assert_eq!(
        store.commit_lifecycle(first.clone()).await.unwrap(),
        receipt
    );
    let events: Vec<memory::MemoryLifecycleEvent> =
        serde_json::from_str(&store.kv_get(&history_key).await.unwrap().unwrap()).unwrap();
    assert_eq!(events, vec![first_event, second_event, third_event]);
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    drop(store);
    let reopened = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    assert_eq!(
        reopened.commit_lifecycle(first.clone()).await.unwrap(),
        receipt
    );
    let mut forged = first;
    forged.expected_history = Some("[]".into());
    assert!(reopened.commit_lifecycle(forged).await.is_err());
    let recovered: Vec<memory::MemoryLifecycleEvent> =
        serde_json::from_str(&reopened.kv_get(&history_key).await.unwrap().unwrap()).unwrap();
    assert_eq!(recovered, events);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_knowledge_source_cut_refuses_changed_body_and_reopens_original_receipt() {
    let fixture = std::mem::ManuallyDrop::new(PostgresTestScope::new());
    let memory_store = Arc::new(PostgresMemoryStore::new(fixture.reconnect()).unwrap());
    let knowledge = Arc::new(PostgresKnowledgeStore::new(fixture.reconnect()).unwrap());
    let fabric =
        KnowledgeFabric::with_store(knowledge.clone()).with_memory_sources(memory_store.clone());
    let original = memory_entry(uuid::Uuid::new_v4(), "source-cut");
    memory_store.insert(&original).await.unwrap();
    let source_ref = format!("memory:{}", original.id);
    let original_cut = memory::store::MemoryEntryRevision::of(&original).unwrap();
    let mut document =
        DocumentContent::new("source-backed policy", "must retain the original evidence");
    document.source = Some(source_ref.clone());
    document.memory_source_revision = Some(original_cut.clone());
    let first = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document.clone(),
        )
        .unwrap();
    let mut observed = original.clone();
    observed.access_count = 10;
    memory_store.update(&observed, &original_cut).await.unwrap();
    assert!(fabric
        .search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), "")
        .is_ok());
    let prepared = |suffix: &str| {
        let mut receipt = first.clone();
        receipt.corpus.corpus_id = format!("prepared-corpus-{suffix}");
        receipt.pack.pack_id = format!("prepared-pack-{suffix}");
        receipt.pack.source_corpus_refs = vec![receipt.corpus.corpus_id.clone()];
        receipt.canon.canon_id = format!("prepared-canon-{suffix}");
        receipt.canon.pack_id = receipt.pack.pack_id.clone();
        receipt.pack.canon_pack_ref = Some(receipt.canon.canon_id.clone());
        receipt.chunks.clear();
        receipt.corpus.chunk_count = 0;
        receipt.conflicts.clear();
        receipt
    };
    let after_observation = prepared("observation");
    knowledge.save_receipt(&after_observation).unwrap();
    let mut correction = original.clone();
    correction.content = "actual replacement of the authoritative source".into();
    memory_store
        .update(&correction, &original_cut)
        .await
        .unwrap();
    let before = serde_json::to_value(knowledge.snapshot().unwrap()).unwrap();
    assert!(matches!(
        knowledge.save_receipt(&prepared("stale-body")),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before,
        "refusal must commit no corpus/pack/canon/receipt projection"
    );
    assert!(matches!(
        fabric.ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document
        ),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert!(matches!(
        fabric.search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), ""),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        knowledge.save_receipt(&first).unwrap().corpus,
        first.corpus,
        "an immutable old receipt remains its original fact"
    );
    let mut fresh =
        DocumentContent::new("source-backed policy", "must retain the corrected evidence");
    fresh.source = Some(source_ref.clone());
    fresh.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&correction).unwrap());
    let current = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            fresh,
        )
        .unwrap();
    assert_ne!(
        current.corpus.corpus_id, first.corpus.corpus_id,
        "a different source cut cannot rebind the original corpus identity"
    );
    assert!(matches!(
        memory_store.read_source_revision(&source_ref, 1),
        Err(memory::MemoryError::SourceReadLimit)
    ));
    let mut detached_ingestion = current.clone();
    detached_ingestion.pack.source_corpus_refs.clear();
    let before_detached = serde_json::to_value(knowledge.snapshot().unwrap()).unwrap();
    assert!(matches!(
        knowledge.save_receipt(&detached_ingestion),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before_detached
    );
    let mut rebound = current.clone();
    rebound.pack.pack_id = "rebind-old-corpus-pack".into();
    rebound.canon.pack_id = rebound.pack.pack_id.clone();
    rebound.canon.canon_id = "rebind-old-corpus-canon".into();
    rebound.pack.canon_pack_ref = Some(rebound.canon.canon_id.clone());
    rebound.corpus.corpus_id = first.corpus.corpus_id.clone();
    rebound.pack.source_corpus_refs = vec![first.corpus.corpus_id.clone()];
    rebound.chunks.clear();
    rebound.corpus.chunk_count = 0;
    rebound.conflicts.clear();
    let before_rebind = serde_json::to_value(knowledge.snapshot().unwrap()).unwrap();
    assert!(matches!(
        knowledge.save_receipt(&rebound),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before_rebind
    );
    let mut detached = after_observation.pack.clone();
    detached.source_corpus_refs.clear();
    assert!(matches!(
        knowledge.commit_maintenance(&knowledge.maintenance_inputs().unwrap(), &[detached], &[]),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before_rebind
    );
    let history_key = format!("memory_lifecycle:{}", original.id);
    // Legal JSON with a huge prefix; the source budget must reject before
    // history materialization, rather than merely refusing its later digest.
    memory_store
        .kv_put(
            &history_key,
            &format!(
                "{}[]",
                " ".repeat(memory::knowledge::KNOWLEDGE_READ_MAX_BYTES)
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        fabric.search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), ""),
        Err(memory::KnowledgeStoreError::ReadLimit)
    ));
    memory_store.kv_delete(&history_key).await.unwrap();
    // Long authorized metadata is bounded by the actual total reservation,
    // not rejected by an invented 4KiB scope/field limit.
    let mut long_scope = original.clone();
    long_scope.id = uuid::Uuid::new_v4();
    long_scope.scope = MemoryScope::Project("\"".repeat(3000));
    memory_store.insert(&long_scope).await.unwrap();
    let long_source = format!("memory:{}", long_scope.id);
    let live = memory_store
        .read_source_revision(&long_source, memory::knowledge::KNOWLEDGE_READ_MAX_BYTES)
        .unwrap()
        .unwrap();
    assert_eq!(
        live.revision.unwrap(),
        memory::store::MemoryEntryRevision::of(&long_scope).unwrap()
    );
    let archive = memory::store::MemoryLifecycleMutation::new(
        &long_scope,
        None,
        memory::MemoryLifecycleEvent {
            memory_id: long_scope.id,
            from: None,
            to: memory::MemoryState::Archived,
            reason: "long original source scope".into(),
            session_id: "s".into(),
            agent_id: "a".into(),
            occurred_at: chrono::Utc::now(),
        },
    )
    .unwrap();
    memory_store.commit_lifecycle(archive).await.unwrap();
    let retired = memory_store
        .read_source_revision(&long_source, memory::knowledge::KNOWLEDGE_READ_MAX_BYTES)
        .unwrap()
        .unwrap();
    assert!(retired.fence.retired);
    assert!(retired.revision.is_none());
    assert!(retired.materialized_bytes > 4096);
    assert!(matches!(
        memory_store.read_source_revision(&long_source, 4096),
        Err(memory::MemoryError::SourceReadLimit)
    ));
    let mut foreign = prepared("foreign-scope");
    foreign.corpus.memory_body_revision = current.corpus.memory_body_revision.clone();
    foreign.corpus.namespace = KnowledgeNamespace::Project("foreign".into());
    foreign.pack.namespace = foreign.corpus.namespace.clone();
    assert!(matches!(
        knowledge.save_receipt(&foreign),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    drop(fabric);
    drop(knowledge);
    drop(memory_store);
    let reopened = PostgresKnowledgeStore::new(fixture.reconnect()).unwrap();
    assert_eq!(reopened.save_receipt(&first).unwrap().corpus, first.corpus);
    assert!(matches!(
        reopened.save_receipt(&prepared("stale-after-reopen")),
        Err(KnowledgeStoreError::StaleRead)
    ));
    drop(reopened);
    drop(std::mem::ManuallyDrop::into_inner(fixture));
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_extraction_origin_retirement_fences_new_ids_after_reopen() {
    use memory::store::{MemoryExtractionOrigin, MemoryLifecycleMutation, MemorySourceFenceReader};
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let turn = memory::MemoryTurnContext::new("session-snapshot", "test-agent");
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000001";
    let mut source = memory_entry(uuid::Uuid::new_v4(), "early extraction");
    source.source = MemorySource::AutoExtracted;
    source.tags.push(batch.into());
    let origin = MemoryExtractionOrigin::for_entry(&turn, &source, batch).unwrap();
    store
        .write_extraction(&source, &origin, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .write_extraction(&source, &origin, None)
            .await
            .unwrap(),
        source.id
    );
    let mut sibling = source.clone();
    sibling.id = uuid::Uuid::new_v4();
    sibling.title = "another derived view".into();
    store
        .write_extraction(&sibling, &origin, None)
        .await
        .unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: source.id,
        from: None,
        to: memory::MemoryState::Archived,
        reason: "retirement while original semantic request is outstanding".into(),
        session_id: turn.session_id.clone(),
        agent_id: turn.agent_id.clone(),
        occurred_at: chrono::Utc::now(),
    };
    let command = MemoryLifecycleMutation::new(&source, None, event).unwrap();
    store.commit_lifecycle(command.clone()).await.unwrap();
    drop(store);
    let reopened = PostgresMemoryStore::new(executor).unwrap();
    reopened.commit_lifecycle(command).await.unwrap();
    let mut late = source.clone();
    late.id = uuid::Uuid::new_v4();
    late.content = "late paraphrase, different target ID".into();
    assert!(reopened
        .write_extraction(&late, &origin, None)
        .await
        .is_err());
    assert!(reopened.get(&late.id).await.unwrap().is_none());
    assert!(reopened.insert(&late).await.is_err());
    let read = reopened
        .read_source_revision(&format!("memory:{}", sibling.id), 1024 * 1024)
        .unwrap()
        .unwrap();
    assert!(read.origin_retired);
    assert!(read.revision.is_none());
    let independent_batch = "extraction-batch:00000000-0000-0000-0000-000000000002";
    late.tags = vec![independent_batch.into()];
    let independent = MemoryExtractionOrigin::for_entry(&turn, &late, independent_batch).unwrap();
    reopened
        .write_extraction(&late, &independent, None)
        .await
        .unwrap();
    let read = reopened
        .read_source_revision(&format!("memory:{}", late.id), 1024 * 1024)
        .unwrap()
        .unwrap();
    assert!(!read.origin_retired);
    assert!(read.revision.is_some());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_actual_source_readset_rejects_late_new_origin_without_target_writes() {
    use memory::store::{MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation};
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let mut parent = memory_entry(uuid::Uuid::new_v4(), "actual Provider source");
    store.insert(&parent).await.unwrap();
    let cut = MemoryEntryRevision::of(&parent).unwrap();
    let turn = memory::MemoryTurnContext::new("session-snapshot", "test-agent")
        .with_project_id(Some("snapshot-test".into()))
        .with_turn_ref(Some("original-turn".into()))
        .with_source_readset(Some(vec![cut.clone()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000003";
    let mut output = memory_entry(uuid::Uuid::new_v4(), "late new output");
    output.source = MemorySource::AutoExtracted;
    output.tags = vec![batch.into()];
    let origin = MemoryExtractionOrigin::for_entry(&turn, &output, batch).unwrap();
    parent.access_count += 1;
    store.update(&parent, &cut).await.unwrap();
    store
        .write_extraction(&output, &origin, None)
        .await
        .unwrap();
    parent.content = "source changed while the semantic request was outstanding".into();
    store.update(&parent, &cut).await.unwrap();
    output.id = uuid::Uuid::new_v4();
    assert!(store
        .write_extraction(&output, &origin, None)
        .await
        .is_err());
    assert!(store.get(&output.id).await.unwrap().is_none());
    {
        let mut connection = executor.checkout_online_read().unwrap();
        let count: i64 = connection
            .query_one(
                "SELECT count(*) FROM memory_kv WHERE key=$1",
                &[&MemoryExtractionOrigin::binding_key(output.id)],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            count, 0,
            "failed source-CAS cannot publish even the origin binding"
        );
    }
    let fresh_turn =
        turn.with_source_readset(Some(vec![MemoryEntryRevision::of(&parent).unwrap()]));
    let fresh = MemoryExtractionOrigin::for_entry(&fresh_turn, &output, batch).unwrap();
    store.write_extraction(&output, &fresh, None).await.unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: parent.id,
        from: None,
        to: memory::MemoryState::Archived,
        reason: "retired source after use".into(),
        session_id: "session-snapshot".into(),
        agent_id: "test-agent".into(),
        occurred_at: chrono::Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::new(&parent, None, event).unwrap())
        .await
        .unwrap();
    drop(store);
    let reopened = PostgresMemoryStore::new(executor).unwrap();
    output.id = uuid::Uuid::new_v4();
    assert!(reopened
        .write_extraction(&output, &fresh, None)
        .await
        .is_err());
    assert!(reopened.get(&output.id).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_transitive_original_cut_survives_reopen_and_fences_new_targets() {
    use memory::store::{MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation};
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let mut a = memory_entry(uuid::Uuid::from_u128(1), "ancestor A");
    store.insert(&a).await.unwrap();
    let a_cut = MemoryEntryRevision::of(&a).unwrap();
    let turn = memory::MemoryTurnContext::new("session-snapshot", "test-agent")
        .with_source_readset(Some(vec![a_cut.clone()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000012";
    let mut b = memory_entry(uuid::Uuid::from_u128(2), "derived B");
    b.source = MemorySource::AutoExtracted;
    b.tags = vec![batch.into()];
    store
        .write_extraction(
            &b,
            &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
            None,
        )
        .await
        .unwrap();
    let b_turn = turn.with_source_readset(Some(vec![MemoryEntryRevision::of(&b).unwrap()]));
    let mut c = b.clone();
    c.id = uuid::Uuid::from_u128(3);
    c.title = "third generation C".into();
    let c_origin = MemoryExtractionOrigin::for_entry(&b_turn, &c, batch).unwrap();
    store.write_extraction(&c, &c_origin, None).await.unwrap();
    a.content = "source A corrected after B/C were committed".into();
    store.update(&a, &a_cut).await.unwrap();
    for id in [b.id, c.id] {
        assert!(store
            .read_source_revision(&format!("memory:{id}"), 1024 * 1024)
            .unwrap()
            .unwrap()
            .revision
            .is_none());
    }
    let mut late = c.clone();
    late.id = uuid::Uuid::from_u128(4);
    assert!(store
        .write_extraction(&late, &c_origin, None)
        .await
        .is_err());
    assert!(store.get(&late.id).await.unwrap().is_none());
    let history = memory::MemoryLifecycleEvent {
        memory_id: a.id,
        from: None,
        to: memory::MemoryState::Archived,
        reason: "retire ancestor".into(),
        session_id: "session-snapshot".into(),
        agent_id: "test-agent".into(),
        occurred_at: chrono::Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::new(&a, None, history).unwrap())
        .await
        .unwrap();
    drop(store);
    let reopened = PostgresMemoryStore::new(executor).unwrap();
    assert!(reopened
        .read_source_revision(&format!("memory:{}", c.id), 1024 * 1024)
        .unwrap()
        .unwrap()
        .revision
        .is_none());
    assert!(reopened
        .write_extraction(&late, &c_origin, None)
        .await
        .is_err());
    assert!(reopened.get(&late.id).await.unwrap().is_none());
    let independent = memory_entry(uuid::Uuid::from_u128(5), "independent source");
    reopened.insert(&independent).await.unwrap();
    assert!(reopened
        .read_source_revision(&format!("memory:{}", independent.id), 1024 * 1024)
        .unwrap()
        .unwrap()
        .revision
        .is_some());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_new_origin_between_preparation_and_body_lock_cannot_pass_source_cas() {
    use memory::store::{MemoryEntryRevision, MemoryExtractionOrigin};
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let a = memory_entry(uuid::Uuid::from_u128(1), "real source A");
    store.insert(&a).await.unwrap();
    let mut b = memory_entry(uuid::Uuid::from_u128(2), "new source B");
    b.source = MemorySource::AutoExtracted;
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000013";
    b.tags = vec![batch.into()];
    let mut connection = executor.checkout_online_read().unwrap();
    let mut original = connection.transaction().unwrap();
    assert!(original
        .query_opt(
            "SELECT value FROM memory_kv WHERE key=$1",
            &[&MemoryExtractionOrigin::binding_key(b.id)]
        )
        .unwrap()
        .is_none());
    let prepared = std::collections::BTreeMap::from([(b.id, None)]);
    let ctx = memory::MemoryTurnContext::new("session-snapshot", "test-agent")
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&a).unwrap()]));
    let origin = MemoryExtractionOrigin::for_entry(&ctx, &b, batch).unwrap();
    store.write_extraction(&b, &origin, None).await.unwrap();
    original
        .query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
            &[&format!("cowd-memory:body:{}", b.id)],
        )
        .unwrap();
    assert!(
        matches!(
            validate_original_origin_snapshot(&mut original, &prepared),
            Err(MemoryError::StaleSource)
        ),
        "body lock plus exact origin CAS must refuse a newly arrived ancestor dependency"
    );
    original.rollback().unwrap();
    assert!(
        store
            .read_source_revision(&format!("memory:{}", b.id), 1024 * 1024)
            .unwrap()
            .unwrap()
            .revision
            .is_some(),
        "a fresh full frame including the real origin remains legal"
    );
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_retired_derivative_never_hydrates_its_large_ancestor() {
    use memory::store::{MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation};
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor).unwrap();
    let mut a = memory_entry(uuid::Uuid::new_v4(), "large original");
    a.content = "x".repeat(64 * 1024);
    store.insert(&a).await.unwrap();
    let turn = memory::MemoryTurnContext::new("session-snapshot", "test-agent")
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&a).unwrap()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000016";
    let mut b = memory_entry(uuid::Uuid::new_v4(), "retired derivative");
    b.source = MemorySource::AutoExtracted;
    b.tags = vec![batch.into()];
    store
        .write_extraction(
            &b,
            &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
            None,
        )
        .await
        .unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: b.id,
        from: None,
        to: memory::MemoryState::Archived,
        reason: "B independently retired".into(),
        session_id: "session-snapshot".into(),
        agent_id: "test-agent".into(),
        occurred_at: chrono::Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::new(&b, None, event).unwrap())
        .await
        .unwrap();
    let read = store
        .read_source_revision(&format!("memory:{}", b.id), 4096)
        .unwrap()
        .unwrap();
    assert!(read.fence.retired);
    assert!(read.revision.is_none());
    assert!(read.materialized_bytes < 4096);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_erasure_receipt_binds_original_cut_identity_and_reopens_without_body() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let mut original = memory_entry(uuid::Uuid::new_v4(), "erase original");
    store.insert(&original).await.unwrap();
    let old = erase_command(&original, None);
    let cut = MemoryEntryRevision::of(&original).unwrap();
    original.content = "new content before erase admission".into();
    store.update(&original, &cut).await.unwrap();
    assert!(store.commit_lifecycle(old.clone()).await.is_err());
    assert!(store.get(&original.id).await.unwrap().is_some());
    let command = erase_command(&original, None);
    let receipt = store.commit_lifecycle(command.clone()).await.unwrap();
    assert!(receipt.body_erased);
    assert!(store.get(&original.id).await.unwrap().is_none());
    assert!(store
        .load_verbatim_by_id(&original.id.to_string())
        .await
        .unwrap()
        .is_none());
    assert!(
        store.commit_lifecycle(old).await.is_err(),
        "the operation ID cannot confirm a different original body cut"
    );
    let mut foreign = command.clone();
    foreign.scope_key = "project_foreign".into();
    assert!(store.commit_lifecycle(foreign).await.is_err());
    let mut wrong_actor = command.clone();
    wrong_actor.event.agent_id = "foreign-agent".into();
    assert!(store.commit_lifecycle(wrong_actor).await.is_err());
    drop(store);
    let reopened = PostgresMemoryStore::new(executor).unwrap();
    assert_eq!(reopened.commit_lifecycle(command).await.unwrap(), receipt);
    assert_eq!(
        reopened
            .lifecycle_receipt(&receipt.mutation_id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    let pending = reopened.pending_memory_cleanup(None, 16).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].mutation_id, receipt.mutation_id);
    assert!(reopened.insert(&original).await.is_err());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_erasure_skipped_delete_cannot_publish_a_receipt() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE FUNCTION skip_erasure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$").unwrap();
    for table in ["memory_entries", "memory_verbatim"] {
        let original = memory_entry(uuid::Uuid::new_v4(), table);
        store.insert(&original).await.unwrap();
        let command = erase_command(&original, None);
        connection.batch_execute(&format!("CREATE TRIGGER skip_erasure BEFORE DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION skip_erasure()")).unwrap();
        assert!(
            store.commit_lifecycle(command.clone()).await.is_err(),
            "a skipped {table} deletion is not erasure"
        );
        assert_eq!(
            store.get(&original.id).await.unwrap().unwrap().content,
            original.content
        );
        assert_eq!(
            store
                .load_verbatim_by_id(&original.id.to_string())
                .await
                .unwrap()
                .unwrap()
                .content,
            original.content
        );
        for metadata in ["memory_lifecycle_receipts", "memory_cleanup_pending"] {
            let count: i64 = connection
                .query_one(
                    &format!("SELECT count(*) FROM {metadata} WHERE mutation_id=$1"),
                    &[&command.mutation_id],
                )
                .unwrap()
                .get(0);
            assert_eq!(count, 0, "skipped deletion cannot publish {metadata}");
        }
        let fences: i64 = connection
            .query_one(
                "SELECT count(*) FROM memory_source_fences WHERE source_ref=$1",
                &[&format!("memory:{}", original.id)],
            )
            .unwrap()
            .get(0);
        assert_eq!(fences, 0);
        assert!(store
            .kv_get(&format!("memory_lifecycle:{}", original.id))
            .await
            .unwrap()
            .is_none());
        connection
            .batch_execute(&format!("DROP TRIGGER skip_erasure ON {table}"))
            .unwrap();
        assert!(store.commit_lifecycle(command).await.unwrap().body_erased);
        assert!(store.get(&original.id).await.unwrap().is_none());
        assert!(store
            .load_verbatim_by_id(&original.id.to_string())
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_auxiliary_original_cuts_and_cleanup_refusal_survive_reopen() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = PostgresMemoryStore::new(executor.clone()).unwrap();
    let mut a = memory_entry(uuid::Uuid::new_v4(), "aux upstream original");
    let d = memory_entry(uuid::Uuid::new_v4(), "aux independent support");
    store.insert(&a).await.unwrap();
    store.insert(&d).await.unwrap();
    let a_cut = MemoryEntryRevision::of(&a).unwrap();
    let d_cut = MemoryEntryRevision::of(&d).unwrap();
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000057";
    let turn = memory::MemoryTurnContext::new("aux-session", "aux-agent")
        .with_source_readset(Some(vec![a_cut.clone()]));
    let mut b = memory_entry(uuid::Uuid::new_v4(), "aux derived original");
    b.source = MemorySource::AutoExtracted;
    b.session_id = Some(turn.session_id.clone());
    b.source_agent = Some(turn.agent_id.clone());
    b.tags = vec![batch.into()];
    store
        .write_extraction(
            &b,
            &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
            None,
        )
        .await
        .unwrap();
    let mut backed = scenario_entity("auxiliary-cut", 1);
    backed.source_type = "memory".into();
    backed.source_ids = vec![b.id.to_string(), d.id.to_string()];
    backed.memory_source_revisions = vec![MemoryEntryRevision::of(&b).unwrap(), d_cut.clone()];
    let mut external = scenario_entity("external-uuid", 1);
    external.source_type = "file".into();
    external.source_ids = vec![a.id.to_string()];
    let mut triple = scenario_triple("auxiliary-cut", 1);
    triple.source = Some(format!("memory:{}", b.id));
    triple.memory_source_revision = Some(MemoryEntryRevision::of(&b).unwrap());
    let scope = a.scope.scope_key();
    store
        .save_knowledge_graph(
            &scope,
            &[backed.clone(), external.clone()],
            &[triple.clone()],
        )
        .await
        .unwrap();
    a.content = "corrected after original snapshot capture".into();
    store.update(&a, &a_cut).await.unwrap();
    let visible = store.load_entities(&scope).await.unwrap();
    let multi = visible
        .iter()
        .find(|entity| entity.id == backed.id)
        .unwrap();
    assert_eq!(multi.source_ids, vec![d.id.to_string()]);
    assert_eq!(multi.memory_source_revisions, vec![d_cut]);
    assert!(store.load_triples(&scope).await.unwrap().is_empty());
    assert!(store
        .save_knowledge_graph(&scope, &[backed], &[triple])
        .await
        .is_err());
    let mut fresh = scenario_entity("fresh-original-cut", 1);
    fresh.source_type = "memory".into();
    fresh.source_ids = vec![a.id.to_string()];
    fresh.memory_source_revisions = vec![MemoryEntryRevision::of(&a).unwrap()];
    let mut fresh_triple = scenario_triple("fresh-original-cut", 1);
    fresh_triple.source = Some(format!("memory:{}", a.id));
    fresh_triple.memory_source_revision = Some(MemoryEntryRevision::of(&a).unwrap());
    store
        .save_knowledge_graph(
            &scope,
            &[fresh.clone(), external.clone()],
            &[fresh_triple.clone()],
        )
        .await
        .unwrap();
    let mut washed = fresh.clone();
    washed.source_type = "file".into();
    washed.source_ids.clear();
    washed.memory_source_revisions.clear();
    assert!(store.save_entities(&scope, &[washed]).await.is_err());
    let receipt = store
        .commit_lifecycle(erase_command(&a, None))
        .await
        .unwrap();
    let pending = store.pending_memory_cleanup(None, 16).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].mutation_id, receipt.mutation_id);
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE FUNCTION skip_auxiliary_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER skip_auxiliary_cleanup BEFORE DELETE ON memory_entities FOR EACH ROW EXECUTE FUNCTION skip_auxiliary_cleanup()").unwrap();
    assert!(store
        .confirm_memory_cleanup(&pending[0], None, true)
        .await
        .is_err());
    assert!(!store.pending_memory_cleanup(None, 16).await.unwrap()[0].auxiliary_confirmed);
    for (table, id) in [
        ("memory_entities", &fresh.id),
        ("memory_triples", &fresh_triple.id),
    ] {
        let count: i64 = connection
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE scope_key=$1 AND id=$2"),
                &[&scope, id],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            count, 1,
            "a skipped deletion cannot publish partial cleanup"
        );
    }
    let visible = store.load_entities(&scope).await.unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].id, external.id,
        "read fence precedes physical cleanup"
    );
    connection
        .batch_execute("DROP TRIGGER skip_auxiliary_cleanup ON memory_entities")
        .unwrap();
    drop(store);
    let reopened = Arc::new(PostgresMemoryStore::new(fixture.reconnect()).unwrap());
    let knowledge = KnowledgeFabric::with_store(Arc::new(
        PostgresKnowledgeStore::new(executor.clone()).unwrap(),
    ))
    .with_memory_sources(reopened.clone());
    let quarantine = knowledge
        .quarantine_memory_source(&pending[0].source)
        .unwrap();
    reopened
        .confirm_memory_cleanup(&pending[0], Some(&quarantine), true)
        .await
        .unwrap();
    assert!(reopened
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(reopened.load_triples(&scope).await.unwrap().is_empty());
    let visible = reopened.load_entities(&scope).await.unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].source_ids,
        vec![a.id.to_string()],
        "an external UUID must not be erased as a Memory reference"
    );
    fresh.id = "late-new-auxiliary-id".into();
    assert!(reopened.save_entities(&scope, &[fresh]).await.is_err());
    assert_eq!(
        reopened.load_entities(&scope).await.unwrap()[0].id,
        external.id
    );
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_auxiliary_production_filter_shares_one_bounded_source_frame() {
    let fixture = PostgresTestScope::new();
    let store = PostgresMemoryStore::new(fixture.reconnect()).unwrap();
    let mut a = memory_entry(uuid::Uuid::new_v4(), "shared 5MiB source");
    a.content = "z".repeat(5 * 1024 * 1024);
    store.insert(&a).await.unwrap();
    let turn = memory::MemoryTurnContext::new("frame-session", "frame-agent")
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&a).unwrap()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000058";
    let mut leaves = Vec::new();
    for marker in ["first small leaf", "second small leaf"] {
        let mut b = memory_entry(uuid::Uuid::new_v4(), marker);
        b.source = MemorySource::AutoExtracted;
        b.session_id = Some(turn.session_id.clone());
        b.source_agent = Some(turn.agent_id.clone());
        b.tags = vec![batch.into()];
        store
            .write_extraction(
                &b,
                &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
                None,
            )
            .await
            .unwrap();
        leaves.push(b);
    }
    let mut entity = scenario_entity("shared-frame", 1);
    entity.source_type = "memory".into();
    entity.source_ids = leaves.iter().map(|entry| entry.id.to_string()).collect();
    entity.memory_source_revisions = leaves
        .iter()
        .map(|entry| MemoryEntryRevision::of(entry).unwrap())
        .collect();
    let scope = a.scope.scope_key();
    let (kept, _) =
        memory::store::filter_auxiliary_snapshot(&store, &scope, &[entity], &[]).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].source_ids.len(), 2);
    let (frame, spent) = store
        .read_source_revisions(
            &leaves.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            8 * 1024 * 1024,
        )
        .unwrap();
    assert_eq!(frame.len(), 2);
    assert!(spent > a.content.len() && spent < 6 * 1024 * 1024);
    store
        .save_knowledge_graph(&scope, &kept, &[])
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_legacy_retirement_adopts_original_cut_after_reopen_without_a_second_epoch() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let original = memory_entry(uuid::Uuid::from_u128(2), "legacy-retired");
    let mut huge = memory_entry(uuid::Uuid::from_u128(1), "oversized-legacy");
    huge.content = "x".repeat(memory::knowledge::KNOWLEDGE_READ_MAX_BYTES + 1);
    let knowledge = Arc::new(PostgresKnowledgeStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(knowledge.clone()).with_memory_sources(store.clone());
    store.insert(&original).await.unwrap();
    let mut doc = DocumentContent::new("legacy derived", "Original source material.");
    doc.source = Some(format!("memory:{}", original.id));
    doc.memory_source_revision = Some(MemoryEntryRevision::of(&original).unwrap());
    let pack = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            doc,
        )
        .unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: original.id,
        from: None,
        to: memory::MemoryState::Archived,
        reason: "original pre-fence retirement".into(),
        session_id: "legacy-session".into(),
        agent_id: "legacy-actor".into(),
        occurred_at: chrono::Utc::now(),
    };
    let raw = serde_json::to_string(&vec![event.clone()]).unwrap();
    let key = format!("memory_lifecycle:{}", original.id);
    store.kv_put(&key, &raw).await.unwrap();
    store.insert(&huge).await.unwrap();
    store
        .kv_put(&format!("memory_lifecycle:{}", huge.id), "[]")
        .await
        .unwrap();
    let page = store.legacy_lifecycle_page(None, 1).await.unwrap();
    assert_eq!(page.after_id, Some(huge.id.to_string()));
    assert_eq!(page.unavailable_ids, vec![huge.id.to_string()]);
    assert!(page.entries.is_empty());
    let next = store
        .legacy_lifecycle_page(page.after_id.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(next.entries.len(), 1);
    assert_eq!(next.entries[0].0.id, original.id);
    let captured =
        MemoryLifecycleMutation::adopt_legacy_retirement(&next.entries[0].0, &next.entries[0].1)
            .unwrap()
            .unwrap();
    let mut changed = event;
    changed.reason = "concurrent history edit".into();
    store
        .kv_put(&key, &serde_json::to_string(&vec![changed]).unwrap())
        .await
        .unwrap();
    assert!(store.commit_lifecycle(captured.clone()).await.is_err());
    assert!(store
        .pending_memory_cleanup(None, 2)
        .await
        .unwrap()
        .is_empty());
    store.kv_put(&key, &raw).await.unwrap();
    // Lose the complete old reader graph. Recovery gets no APP command or
    // explicit register/adoption call; only the original persisted witnesses.
    drop(fabric);
    drop(knowledge);
    drop(store);
    drop(executor);
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let fabric = KnowledgeFabric::with_store(Arc::new(
        PostgresKnowledgeStore::new(executor.clone()).unwrap(),
    ))
    .with_memory_sources(store.clone());
    let root =
        std::env::temp_dir().join(format!("cowd-legacy-retirement-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config = memory::MemoryConfig::default();
    config.store.blob_dir = root.join("blobs");
    config.store.enable_vector_index = false;
    config.governance.enabled = false;
    let manager =
        memory::CognitiveContextManager::new_with_selected_store(config, None, store.clone())
            .await
            .unwrap();
    // The oversized row is reported/held but cannot starve the next source.
    let errors = manager.recover_memory_cleanup(&fabric, 1).await.unwrap();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains(&huge.id.to_string()));
    assert!(store
        .lifecycle_receipt(&captured.mutation_id)
        .await
        .unwrap()
        .is_none());
    assert!(manager
        .recover_memory_cleanup(&fabric, 1)
        .await
        .unwrap()
        .is_empty());
    let receipt = store
        .lifecycle_receipt(&captured.mutation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.source.delete_epoch, 1);
    assert!(!receipt.body_erased);
    assert!(store
        .pending_memory_cleanup(None, 2)
        .await
        .unwrap()
        .is_empty());
    let retained = fabric
        .snapshot()
        .unwrap()
        .packs
        .into_iter()
        .find(|p| p.pack_id == pack.pack.pack_id)
        .unwrap();
    assert_eq!(
        retained.state,
        harness_contract::knowledge::KnowledgeObjectState::Quarantined
    );
    assert_eq!(
        store.get(&original.id).await.unwrap().unwrap().content,
        original.content
    );
    for _ in 0..4 {
        let _held = manager.recover_memory_cleanup(&fabric, 1).await.unwrap();
    }
    assert_eq!(
        store
            .lifecycle_receipt(&captured.mutation_id)
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .read_source_fence(&format!("memory:{}", original.id))
            .unwrap()
            .unwrap()
            .delete_epoch,
        1
    );
    assert_eq!(store.commit_lifecycle(captured).await.unwrap(), receipt);
    drop(manager);
    drop(fabric);
    drop(store);
    drop(executor);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_runtime_turn_writes_keep_original_cuts_and_legacy_unknown_after_reopen() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let mut source = memory_entry(uuid::Uuid::new_v4(), "runtime-consumed-original");
    source.visibility = AgentVisibility::Shared;
    store.insert(&source).await.unwrap();
    let cut = MemoryEntryRevision::of(&source).unwrap();
    let root =
        std::env::temp_dir().join(format!("cowd-runtime-memory-cuts-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config = memory::MemoryConfig::default();
    config.store.blob_dir = root.join("blobs");
    config.store.enable_vector_index = false;
    let manager = Arc::new(
        memory::CognitiveContextManager::new_with_selected_store(config, None, store.clone())
            .await
            .unwrap(),
    );
    let kernel = memory::MemoryKernel::new(manager.clone());
    let ctx = memory::MemoryTurnContext::new("provider-session", "provider-agent")
        .with_project_id(Some("snapshot-test".into()))
        .with_turn_ref(Some("original-provider-turn".into()))
        .with_source_readset(Some(vec![cut.clone()]));
    let mut note = memory_entry(uuid::Uuid::new_v4(), "original-note-action");
    note.source = MemorySource::AutoExtracted;
    note.session_id = None;
    note.source_agent = None;
    kernel.remember(&ctx, note.clone()).await.unwrap();
    let mut sibling = note.clone();
    sibling.id = uuid::Uuid::new_v4();
    sibling.title = "independent-sibling-action".into();
    sibling.content = "A separate original action.".into();
    kernel.remember(&ctx, sibling.clone()).await.unwrap();
    kernel
        .erase(&ctx, note.id, "only the original note")
        .await
        .unwrap();
    assert!(kernel
        .retrieve_visible_entry(&ctx, sibling.id)
        .await
        .unwrap()
        .is_some());
    source.content = "actual corrected independent source".into();
    store.update(&source, &cut).await.unwrap();
    let mut stale = note.clone();
    stale.id = uuid::Uuid::new_v4();
    stale.title = "late-stale-cut".into();
    assert!(kernel.remember(&ctx, stale.clone()).await.is_err());
    assert!(store.get(&stale.id).await.unwrap().is_none());
    let mut unknown = ctx.clone();
    unknown.source_readset = None;
    assert!(kernel.remember(&unknown, stale.clone()).await.is_err());
    let current = ctx
        .clone()
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&source).unwrap()]));
    kernel
        .archive(&current, source.id, "source erased from active input")
        .await
        .unwrap();
    let mut late = note;
    late.id = uuid::Uuid::new_v4();
    late.title = "new-id-after-original-retirement".into();
    assert!(kernel.remember(&current, late.clone()).await.is_err());
    assert!(store.get(&late.id).await.unwrap().is_none());
    let mut legacy = memory_entry(uuid::Uuid::new_v4(), "legacy-unbound-extraction");
    legacy.source = MemorySource::AutoExtracted;
    legacy.scope = MemoryScope::Global;
    legacy.visibility = AgentVisibility::Shared;
    store.insert(&legacy).await.unwrap();
    let mut wash = legacy.clone();
    wash.source = MemorySource::Import;
    assert!(store
        .update(&wash, &MemoryEntryRevision::of(&legacy).unwrap())
        .await
        .is_err());
    drop(kernel);
    drop(manager);
    drop(store);
    drop(executor);
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    assert!(store
        .read_source_revision(
            &format!("memory:{}", legacy.id),
            memory::knowledge::KNOWLEDGE_READ_MAX_BYTES
        )
        .is_err());
    let old_note = store
        .read_source_revision(
            &format!("memory:{}", sibling.id),
            memory::knowledge::KNOWLEDGE_READ_MAX_BYTES,
        )
        .unwrap()
        .unwrap();
    assert!(old_note.origin_retired);
    assert!(old_note.revision.is_none());
    // The body's own deletion fence is distinct from its inherited eligibility.
    let note_fence = store
        .read_source_fence(&format!("memory:{}", sibling.id))
        .unwrap()
        .unwrap();
    assert!(!note_fence.retired);
    assert_eq!(note_fence.delete_epoch, 0);
    let mut independent = memory_entry(uuid::Uuid::new_v4(), "explicit-independent-user-root");
    independent.scope = MemoryScope::Global;
    independent.visibility = AgentVisibility::Shared;
    store.insert(&independent).await.unwrap();
    assert!(store
        .read_source_revision(
            &format!("memory:{}", independent.id),
            memory::knowledge::KNOWLEDGE_READ_MAX_BYTES
        )
        .unwrap()
        .unwrap()
        .revision
        .is_some());
    let now = chrono::Utc::now();
    let node = |id: &str, source: Option<&MemoryEntry>| memory::entity::Entity {
        id: id.into(),
        name: id.into(),
        entity_type: memory::entity::EntityType::Concept,
        confidence: 1.0,
        frequency: 1,
        first_seen: now,
        last_seen: now,
        source_type: if source.is_some() { "memory" } else { "file" }.into(),
        source_ids: source.map(|s| vec![s.id.to_string()]).unwrap_or_default(),
        memory_source_revisions: source
            .map(|s| vec![MemoryEntryRevision::of(s).unwrap()])
            .unwrap_or_default(),
    };
    let mut multi = node("multi-node", Some(&legacy));
    multi.source_ids.push(independent.id.to_string());
    multi
        .memory_source_revisions
        .push(MemoryEntryRevision::of(&independent).unwrap());
    let held = node("old-held-node", Some(&legacy));
    let good = node("independent-node", Some(&independent));
    let external = node("external-node", None);
    {
        let mut c = executor.checkout_critical().unwrap();
        for e in [&held, &good, &external, &multi] {
            c.execute(
                "INSERT INTO memory_entities(scope_key,id,payload) VALUES('global',$1,$2)",
                &[&e.id, &serde_json::to_value(e).unwrap()],
            )
            .unwrap();
        }
    }
    let visible = store.load_entities("global").await.unwrap();
    assert_eq!(
        visible
            .iter()
            .map(|e| e.id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["independent-node", "external-node", "multi-node"])
    );
    assert!(store
        .save_entities("global", std::slice::from_ref(&held))
        .await
        .is_err());
    store.save_entities("global", &visible).await.unwrap();
    let mut changed = visible.clone();
    changed
        .iter_mut()
        .find(|e| e.id == "multi-node")
        .unwrap()
        .name = "unproven metadata change".into();
    assert!(store.save_entities("global", &changed).await.is_err());
    store
        .save_knowledge_graph("global", &visible, &[])
        .await
        .unwrap();

    {
        let mut c = executor.checkout_online_read().unwrap();
        let raw: serde_json::Value = c
            .query_one(
                "SELECT payload FROM memory_entities WHERE scope_key='global' AND id=$1",
                &[&held.id],
            )
            .unwrap()
            .get(0);
        assert_eq!(raw, serde_json::to_value(&held).unwrap());
    }
    {
        let mut c = executor.checkout_online_read().unwrap();
        let raw: serde_json::Value = c
            .query_one(
                "SELECT payload FROM memory_entities WHERE scope_key='global' AND id=$1",
                &[&multi.id],
            )
            .unwrap()
            .get(0);
        assert_eq!(raw, serde_json::to_value(&multi).unwrap());
    }
    let mut config = memory::MemoryConfig::default();
    config.store.blob_dir = root.join("reopened-blobs");
    config.store.enable_vector_index = false;
    let manager =
        memory::CognitiveContextManager::new_with_selected_store(config, None, store.clone())
            .await
            .unwrap();
    assert_eq!(manager.list_entities().await.unwrap().len(), 3);
    for id in [legacy.id, independent.id] {
        let fence = store
            .read_source_fence(&format!("memory:{id}"))
            .unwrap()
            .unwrap();
        assert_eq!(fence.delete_epoch, 0);
        assert!(!fence.retired);
    }
    drop(manager);
    drop(store);
    drop(executor);
}


#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_erasure_result_keeps_exact_cleanup_and_retention_after_reopen() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let root = std::env::temp_dir().join(format!("cowd-erasure-result-{}", uuid::Uuid::new_v4()));
    let mut config = memory::MemoryConfig::default();
    config.store.blob_dir = root.join("blobs");
    config.store.enable_vector_index = false;
    let manager = Arc::new(
        memory::CognitiveContextManager::new_with_selected_store(
            config.clone(),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = memory::MemoryKernel::new(manager.clone());
    let ctx = memory::MemoryTurnContext::new("erase-session", "erase-agent");
    let mut source = memory_entry(uuid::Uuid::new_v4(), "retained-evidence");
    source.scope = MemoryScope::Global;
    source.visibility = AgentVisibility::Shared;
    source.session_id = None;
    source.source_agent = None;
    kernel.remember(&ctx, source.clone()).await.unwrap();
    let result = kernel
        .erase(&ctx, source.id, "explicit target erasure")
        .await
        .unwrap();
    assert!(result.receipt.body_erased);
    assert_eq!(result.cleanup_confirmed(), Some(false));
    assert_eq!(
        store
            .memory_cleanup(&result.receipt.mutation_id)
            .await
            .unwrap(),
        result.cleanup
    );
    assert!(store
        .memory_cleanup("absent-original-operation")
        .await
        .unwrap()
        .is_none());
    let fabric = memory::KnowledgeFabric::with_store(Arc::new(
        PostgresKnowledgeStore::new(executor.clone()).unwrap(),
    ))
    .with_memory_sources(store.clone());
    assert!(manager
        .recover_memory_cleanup(&fabric, 16)
        .await
        .unwrap()
        .is_empty());
    drop(kernel);
    drop(manager);
    drop(fabric);
    drop(store);
    drop(executor);
    let executor = fixture.reconnect();
    let store = Arc::new(PostgresMemoryStore::new(executor.clone()).unwrap());
    let manager = Arc::new(
        memory::CognitiveContextManager::new_with_selected_store(config, None, store.clone())
            .await
            .unwrap(),
    );
    let kernel = memory::MemoryKernel::new(manager.clone());
    let confirmed = kernel
        .erase(&ctx, source.id, "explicit target erasure")
        .await
        .unwrap();
    assert_eq!(confirmed.receipt, result.receipt);
    assert_eq!(confirmed.cleanup_confirmed(), Some(true));
    assert_eq!(
        confirmed.may_retain,
        [
            "historical_audit_summaries",
            "immutable_knowledge_evidence",
            "derived_memory_and_historical_projections",
        ]
    );
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    let original = serde_json::to_value(confirmed.cleanup.as_ref().unwrap()).unwrap();
    let mut foreign = original.clone();
    foreign["source"]["scope_key"] = serde_json::json!("project:foreign");
    {
        let mut c = executor.checkout_critical().unwrap();
        c.execute(
            "UPDATE memory_cleanup_pending SET payload=$2 WHERE mutation_id=$1",
            &[&result.receipt.mutation_id, &foreign],
        )
        .unwrap();
    }
    assert!(store
        .memory_cleanup(&result.receipt.mutation_id)
        .await
        .is_err());
    let unknown = kernel
        .erase(&ctx, source.id, "explicit target erasure")
        .await
        .unwrap();
    assert!(unknown.receipt.body_erased);
    assert_eq!(unknown.cleanup_confirmed(), None);
    {
        let mut c = executor.checkout_critical().unwrap();
        c.execute(
            "UPDATE memory_cleanup_pending SET payload=$2,confirmed=FALSE WHERE mutation_id=$1",
            &[&result.receipt.mutation_id, &original],
        )
        .unwrap();
    }
    assert!(store
        .memory_cleanup(&result.receipt.mutation_id)
        .await
        .is_err());
    {
        let mut c = executor.checkout_critical().unwrap();
        c.execute(
            "DELETE FROM memory_cleanup_pending WHERE mutation_id=$1",
            &[&result.receipt.mutation_id],
        )
        .unwrap();
    }
    let unknown = kernel
        .erase(&ctx, source.id, "explicit target erasure")
        .await
        .unwrap();
    assert_eq!(unknown.cleanup_confirmed(), None);
    assert_eq!(unknown.receipt, result.receipt);
    assert!(store.get(&source.id).await.unwrap().is_none());
    assert!(manager.audit_entries(100).unwrap().iter().any(|entry| {
        entry.entry_id == source.id.to_string() && entry.operation == memory::AuditOperation::Create
    }));
    drop(kernel);
    drop(manager);
    drop(store);
    drop(executor);
}
