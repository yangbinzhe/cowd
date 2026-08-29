use std::sync::Arc;

use harness_contract::knowledge::{
    KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeUsageSignal,
};
use memory::{
    code_indexer::{CodeSymbol, SymbolEdge, SymbolEdgeType, SymbolKind},
    entity::{Entity, EntityType, Triple},
    knowledge::{DocumentContent, InMemoryKnowledgeStore, KnowledgeFabric, KnowledgeStore},
    project_scope::MemoryScope,
    store::{sqlite::SqliteStore, MemoryStore},
    types::AgentVisibility,
    MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
};
use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

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
        tags: vec!["migration".to_string()],
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
async fn sqlite_snapshot_covers_every_durable_memory_class() {
    let store = SqliteStore::open_in_memory().expect("sqlite memory store");
    let id = uuid::Uuid::new_v4();
    store.insert(&memory_entry(id, "snapshot")).await.unwrap();
    store
        .save_entities(&[Entity {
            id: "entity-1".to_string(),
            name: "Cowd".to_string(),
            entity_type: EntityType::Project,
            confidence: 1.0,
            frequency: 2,
            first_seen: chrono::Utc::now(),
            last_seen: chrono::Utc::now(),
            source_ids: vec![id.to_string()],
            source_type: "memory".to_string(),
        }])
        .await
        .unwrap();
    store
        .save_triples(&[Triple {
            id: "triple-1".to_string(),
            subject_id: "entity-1".to_string(),
            predicate: "uses".to_string(),
            object_id: "postgres".to_string(),
            valid_from: None,
            valid_to: None,
            source: Some("test".to_string()),
            confidence: 1.0,
            created_at: chrono::Utc::now(),
            source_agent: None,
        }])
        .await
        .unwrap();
    store
        .save_verbatim("verbatim-1", "raw", "test", 3, "2026-07-23T00:00:00Z")
        .await
        .unwrap();
    store
        .insert_symbol(&CodeSymbol {
            id: "symbol-1".to_string(),
            name: "snapshot".to_string(),
            kind: SymbolKind::Function,
            file_path: "src/lib.rs".to_string(),
            line: 7,
            signature: "fn snapshot()".to_string(),
            doc: Some("test".to_string()),
        })
        .await
        .unwrap();
    store
        .insert_edge(&SymbolEdge {
            source_id: "symbol-1".to_string(),
            target_id: "symbol-2".to_string(),
            edge_type: SymbolEdgeType::Calls,
            file_path: "src/lib.rs".to_string(),
        })
        .await
        .unwrap();
    store
        .link_symbol_to_memory("symbol-1", &id, Some(3), "mentioned", 42)
        .await
        .unwrap();
    store.kv_put("closet:test", "value").await.unwrap();

    let first = export_memory_snapshot(&store).await.unwrap();
    let second = export_memory_snapshot(&store).await.unwrap();
    assert_eq!(
        first.canonical_digest().unwrap(),
        second.canonical_digest().unwrap()
    );
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entities.len(), 1);
    assert_eq!(first.triples.len(), 1);
    assert_eq!(first.verbatim.len(), 1);
    assert_eq!(first.symbols.len(), 1);
    assert_eq!(first.edges.len(), 1);
    assert_eq!(first.symbol_memory_references.len(), 1);
    assert_eq!(first.key_values.len(), 1);
    assert_eq!(
        first.entries[0].embedding.as_deref(),
        Some(&[0.25, 0.75][..])
    );
}

#[tokio::test]
async fn vector_unavailable_is_not_reported_as_no_match() {
    let store = SqliteStore::open_in_memory().expect("sqlite memory store");
    assert!(!store.capabilities().vector_search);
    let error = store.search_vector(&[0.5], 1).await.unwrap_err();
    assert!(matches!(error, MemoryError::CapabilityUnavailable { .. }));
}

#[test]
fn knowledge_snapshot_is_canonical_and_complete() {
    let store = Arc::new(InMemoryKnowledgeStore::new());
    let fabric = KnowledgeFabric::with_store(store.clone());
    let receipt = fabric.ingest_document(
        KnowledgeNamespace::Project("snapshot-test".to_string()),
        KnowledgeActivationPolicy::OnDemand,
        KnowledgeGovernanceLevel::Advisory,
        DocumentContent::new("Snapshot", "The durable knowledge body."),
    );
    store
        .record_usage(&KnowledgeUsageSignal {
            signal_id: "usage-1".to_string(),
            session_id: "session-1".to_string(),
            pack_id: receipt.pack.pack_id,
            action: "activated".to_string(),
            summary: "used in test".to_string(),
            score_delta_bp: 10,
            occurred_at: chrono::Utc::now(),
        })
        .unwrap();
    let first = export_knowledge_snapshot(store.as_ref()).unwrap();
    let second = export_knowledge_snapshot(store.as_ref()).unwrap();
    assert_eq!(
        first.canonical_digest().unwrap(),
        second.canonical_digest().unwrap()
    );
    assert_eq!(first.state.corpus.len(), 1);
    assert_eq!(first.state.packs.len(), 1);
    assert_eq!(first.state.canon.len(), 1);
    assert!(!first.state.chunks.is_empty());
    assert_eq!(first.state.usage.len(), 1);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_postgres_memory_roundtrip() {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let marker = uuid::Uuid::new_v4().simple().to_string();
    let mut config = PostgresConnectionConfig::new(
        format!("memory-test-{marker}"),
        "memory-test-url",
        format!("cowd-memory-test-{marker}"),
    );
    config.max_connections = 4;
    let resolver = StaticSecretRefResolver::new([("memory-test-url".to_string(), url)]);
    let store =
        PostgresMemoryStore::connect(config.clone(), &resolver).expect("connect PostgreSQL");
    let source = SqliteStore::open_in_memory().expect("SQLite migration source");
    let id = uuid::Uuid::new_v4();
    let entry = memory_entry(id, &marker);
    source.insert(&entry).await.unwrap();
    source
        .kv_put(&format!("migration:{marker}"), "present")
        .await
        .unwrap();
    source
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
    source
        .link_symbol_to_memory(&format!("symbol-{marker}"), &id, None, "test", 1)
        .await
        .unwrap();

    let manifest = copy_quiesced_memory_store(&source, &store)
        .await
        .expect("quiesced Memory copy");
    assert_eq!(manifest.source_digest, manifest.target_digest);
    assert_eq!(manifest.entry_count, 1);
    assert_eq!(manifest.symbol_reference_count, 1);

    let reopened =
        PostgresMemoryStore::connect(config, &resolver).expect("reopen PostgreSQL owner");
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
            .kv_get(&format!("migration:{marker}"))
            .await
            .unwrap()
            .as_deref(),
        Some("present")
    );

    let knowledge_source = Arc::new(InMemoryKnowledgeStore::new());
    let fabric = KnowledgeFabric::with_store(knowledge_source.clone());
    let receipt = fabric.ingest_document(
        KnowledgeNamespace::Project(format!("project-{marker}")),
        KnowledgeActivationPolicy::OnDemand,
        KnowledgeGovernanceLevel::Required,
        DocumentContent::new("Real PG", format!("knowledge {marker}")),
    );
    knowledge_source
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
    let knowledge_target = PostgresKnowledgeStore::new(reopened.executor().clone())
        .expect("PostgreSQL Knowledge owner");
    let knowledge_manifest =
        copy_quiesced_knowledge_store(knowledge_source.as_ref(), &knowledge_target)
            .expect("quiesced Knowledge copy");
    assert_eq!(
        knowledge_manifest.source_digest,
        knowledge_manifest.target_digest
    );
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
