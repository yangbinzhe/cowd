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
            .kv_get(&format!("durability:{marker}"))
            .await
            .unwrap()
            .as_deref(),
        Some("present")
    );

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
