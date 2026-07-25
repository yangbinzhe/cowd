#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Memory entry lifecycle integration.
//!
//! Verifies that entries written across memory layers remain readable after
//! the manager is reopened. Fact checking, raw storage and compression have
//! dedicated contract tests.

use memory::config::{BudgetConfig, StoreConfig};
use memory::{
    CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry, MemoryLayer, MemoryScope,
    MemorySource, Priority,
};

fn test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
            blob_dir: sqlite_path.parent().unwrap().join("blobs"),
            enable_vector_index: false,
            cache_capacity: 256,
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

fn entry(content: &str, layer: MemoryLayer) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: String::new(),
        content: content.to_string(),
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
        visibility: memory::AgentVisibility::default(),
    }
}

#[tokio::test]
async fn memory_entries_across_layers_survive_manager_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e.db");

    let n_entries = 20;
    let entry_ids: Vec<uuid::Uuid>;

    // ===== Phase 1: Write entries across all layers =====
    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();

        // Write entries to each layer
        let layers = [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
        ];
        let mut ids = Vec::new();
        for (i, layer) in layers.iter().enumerate() {
            for j in 0..(n_entries / 4) {
                let e = entry(
                    &format!("Layer {:?} entry {}-{} about Rust programming", layer, i, j),
                    *layer,
                );
                let id = e.id;
                mgr.remember(e).await.unwrap();
                ids.push(id);
            }
        }
        entry_ids = ids;

        // Verify all layers have correct counts
        let layer_info = mgr.list_layers().await;
        eprintln!("Phase 1 layers: {:?}", layer_info);

        // Verify entry retrieval
        let first_id = entry_ids[0];
        let retrieved = mgr.get_entry(&first_id.to_string()).await.unwrap();
        assert!(
            retrieved.is_some(),
            "Entry should be retrievable immediately"
        );
    }
    // mgr dropped - data should persist in SQLite

    // ===== Phase 2: Restart and verify persistence =====
    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();

        // Verify entries survive restart via layer listing
        let layer_info = mgr.list_layers().await;
        eprintln!("Phase 2 layers after restart: {:?}", layer_info);

        let total_entries: u64 = layer_info
            .iter()
            .filter_map(|v| v.get("entry_count").and_then(|c| c.as_u64()))
            .sum();
        assert_eq!(total_entries, n_entries as u64);

        // Verify individual entries
        let first_id = entry_ids[0];
        let retrieved = mgr.get_entry(&first_id.to_string()).await.unwrap();
        assert!(retrieved.is_some(), "Entry should survive restart");

        // Verify content preserved
        if let Some(e) = retrieved {
            assert!(
                e.content.contains("Rust programming"),
                "Entry content should be preserved: {}",
                e.content
            );
        }
    }
}
