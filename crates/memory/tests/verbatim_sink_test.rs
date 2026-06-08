//! RED Tests: VerbatimSink - zero-loss raw storage (mempalace philosophy)
//!
//! Tests that verbatim entries survive intact, never pass through compression,
//! and remain searchable after restart.

use cowd_memory::config::{BudgetConfig, StoreConfig};
use cowd_memory::{
    CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry, MemoryLayer, MemoryScope,
    MemorySource, Priority,
};

fn test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
            blob_dir: sqlite_path.parent().unwrap().join("blobs"),
            enable_vector_index: false,
            cache_capacity: 128,
            ..Default::default()
        },
        budget: BudgetConfig {
            context_window: 8000,
            reserved_system: 2000,
            reserved_response: 1000,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn test_verbatim_sink_stores_and_retrieves() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("vbs.db")))
        .await
        .unwrap();

    let entry = MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: "Verbatim test".to_string(),
        content: "This exact raw content must never be compressed or summarized".to_string(),
        embedding: None,
        tags: vec!["verbatim-test".to_string()],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        session_id: None,
        source_agent: None,
        visibility: cowd_memory::AgentVisibility::default(),
    };
    let entry_id = entry.id;
    mgr.remember(entry).await.unwrap();

    // RED: retrieve from verbatim sink (API doesn't exist yet)
    let retrieved = mgr.get_entry(&entry_id.to_string()).await.unwrap();
    assert!(retrieved.is_some(), "Entry should be retrievable");
    if let Some(e) = retrieved {
        assert_eq!(
            e.content,
            "This exact raw content must never be compressed or summarized"
        );
    }
}

#[tokio::test]
async fn test_verbatim_survives_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("vbs_persist.db");

    let entry_id = uuid::Uuid::new_v4();

    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();
        let entry = MemoryEntry {
            id: entry_id,
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::Import,
            title: "Persist test".to_string(),
            content: "Raw content that must survive restart unchanged".to_string(),
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
            session_id: None,
            source_agent: None,
            visibility: cowd_memory::AgentVisibility::default(),
        };
        mgr.remember(entry).await.unwrap();
    }

    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();
        let retrieved = mgr.get_entry(&entry_id.to_string()).await.unwrap();
        assert!(retrieved.is_some(), "Entry should survive restart");
        if let Some(e) = retrieved {
            assert_eq!(e.content, "Raw content that must survive restart unchanged");
        }
    }
}
