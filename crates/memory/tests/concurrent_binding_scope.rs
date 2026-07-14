#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use memory::store::{sqlite::SqliteStore, MemoryStore};
use memory::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemoryOrchestrator, MemoryScope,
    MemorySource, Priority,
};

fn entry(instance: &str, index: usize) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Shared,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
        title: format!("working-note-{instance}-{index}"),
        content: format!("private evidence for {instance} item {index}"),
        embedding: None,
        tags: vec!["binding-scope-test".to_string()],
        relations: Vec::new(),
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::AgentInstance(instance.to_string()),
        visibility: AgentVisibility::Private,
        source_agent: Some(instance.to_string()),
        session_id: Some("session:parallel-binding".to_string()),
    }
}

#[tokio::test]
async fn concurrent_instance_writes_preserve_explicit_agent_instance_scope_without_ambient_identity(
) {
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_in_memory().expect("sqlite"));
    let memory =
        Arc::new(MemoryOrchestrator::from_store(Default::default(), store, None).expect("memory"));
    let mut tasks = Vec::new();
    for slot in 1..=8 {
        let memory = Arc::clone(&memory);
        tasks.push(tokio::spawn(async move {
            let instance = format!("instance:researcher:{slot}");
            let mut ids = Vec::new();
            for index in 0..3 {
                ids.push(
                    memory
                        .remember(entry(&instance, index))
                        .await
                        .expect("write"),
                );
            }
            (instance, ids)
        }));
    }
    let mut counts = BTreeMap::new();
    for task in tasks {
        let (instance, ids) = task.await.expect("task");
        for id in ids {
            let stored = memory.recall(&id).await.expect("recall").expect("entry");
            assert_eq!(stored.scope, MemoryScope::AgentInstance(instance.clone()));
            assert_eq!(stored.source_agent.as_deref(), Some(instance.as_str()));
        }
        counts.insert(instance, 3usize);
    }
    assert_eq!(counts.len(), 8);
    assert_eq!(memory.list_layer(MemoryLayer::L3).await.unwrap().len(), 24);
}
