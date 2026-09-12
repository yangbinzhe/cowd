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
    let receipt = fabric.ingest_document(
        KnowledgeNamespace::Project(format!("project-{marker}")),
        KnowledgeActivationPolicy::OnDemand,
        KnowledgeGovernanceLevel::Required,
        DocumentContent::new("Real PG", format!("knowledge {marker}")),
    );
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
    let receipt = fabric.ingest_document(
        KnowledgeNamespace::Project("pg-knowledge".to_string()),
        KnowledgeActivationPolicy::OnDemand,
        KnowledgeGovernanceLevel::Advisory,
        DocumentContent::new(
            "durable postgres knowledge",
            "knowledge roundtrip body marker",
        ),
    );
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
