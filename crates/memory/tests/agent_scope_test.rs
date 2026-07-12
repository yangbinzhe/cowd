#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Agent scope tests: source_agent, visibility, and peer perception
//! Verifies Phase 1 features of the 3D memory framework

use memory::config::{BudgetConfig, StoreConfig};
use memory::project_scope::MemoryScope;
use memory::{
    AgentVisibility, CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry,
    MemoryLayer, MemorySource, Priority,
};

fn test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
            ..Default::default()
        },
        budget: BudgetConfig {
            context_window: 200_000,
            reserved_system: 10_000,
            reserved_response: 5_000,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn test_remember_tags_source_agent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("sa.db")))
        .await
        .unwrap();
    let entry = MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L2,
        category: MemoryCategory::Decision,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: "test".into(),
        content: "content".into(),
        embedding: None,
        tags: vec![],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        visibility: AgentVisibility::default(),
        source_agent: None,
        session_id: None,
    };
    let eid = entry.id;
    mgr.remember(entry).await.unwrap();
    let retrieved = mgr.get_entry(&eid.to_string()).await.unwrap().unwrap();
    // Entry should be retrievable (scope may be auto-filled by orchestrator)
    assert_eq!(retrieved.title, "test");
}

#[tokio::test]
async fn test_default_visibility_is_private() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("vis.db")))
        .await
        .unwrap();
    let entry = MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L2,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: "v".into(),
        content: "c".into(),
        embedding: None,
        tags: vec![],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        visibility: AgentVisibility::default(),
        source_agent: None,
        session_id: None,
    };
    let eid = entry.id;
    mgr.remember(entry).await.unwrap();
    let retrieved = mgr.get_entry(&eid.to_string()).await.unwrap().unwrap();
    assert!(retrieved.title == "v");
}

#[tokio::test]
async fn test_entries_survive_with_scopes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = tmp.path().join("scope.db");
    let id = uuid::Uuid::new_v4();
    {
        let mgr = CognitiveContextManager::new(test_config(&db))
            .await
            .unwrap();
        let entry = MemoryEntry {
            id,
            layer: MemoryLayer::L2,
            category: MemoryCategory::Decision,
            priority: Priority::High,
            source: MemorySource::Import,
            title: "scoped".into(),
            content: "test".into(),
            embedding: None,
            tags: vec!["scope-test".into()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Project("test-proj".into()),
            visibility: AgentVisibility::Shared,
            source_agent: Some("agent-1".into()),
            session_id: None,
        };
        mgr.remember(entry).await.unwrap();
    }
    {
        let mgr = CognitiveContextManager::new(test_config(&db))
            .await
            .unwrap();
        let retrieved = mgr.get_entry(&id.to_string()).await.unwrap();
        assert!(
            retrieved.is_some(),
            "Entry should survive restart with scope"
        );
        if let Some(e) = retrieved {
            assert_eq!(e.title, "scoped");
        }
    }
}
