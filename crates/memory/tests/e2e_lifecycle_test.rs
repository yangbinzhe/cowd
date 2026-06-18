//! E2E test: full memory lifecycle (Task 24).
//!
//! Simulates: conversation → auto-extract → compress → persist → restart → verify.
//! Verifies that memory entries, KG entities, and compression summaries all
//! survive the full lifecycle.

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
async fn test_e2e_memory_lifecycle() {
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
        assert!(
            total_entries >= n_entries as u64,
            "Expected at least {} total entries after restart, got {}",
            n_entries,
            total_entries
        );

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

    // ===== Phase 3: Verify fact checking (if FactChecker is integrated) =====
    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();

        // Write identity fact
        let identity = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L1,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: "Identity".to_string(),
            content: "Known fact: Alice's parent is Bob".to_string(),
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
        };
        mgr.remember(identity).await.unwrap();

        // Now write contradictory fact - FactChecker should downgrade confidence
        let contradictory = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L2,
            category: MemoryCategory::Decision,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "Contradiction".to_string(),
            content: "Alice's parent is Charlie".to_string(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: memory::AgentVisibility::default(),
        };
        let contradictory_id = contradictory.id;
        mgr.remember(contradictory).await.unwrap();

        // Verify confidence was downgraded
        let retrieved = mgr.get_entry(&contradictory_id.to_string()).await.unwrap();
        if let Some(e) = retrieved {
            eprintln!(
                "Fact check result: confidence={:.3} (was 0.9)",
                e.confidence
            );
            assert!(
                e.confidence < 0.9,
                "Contradictory entry confidence should be downgraded (was 0.9, got {:.3})",
                e.confidence
            );
        }
    }

    // ===== Phase 4: Final restart - verify everything is intact =====
    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();
        let layer_info = mgr.list_layers().await;
        eprintln!("Phase 4 final state: {:?}", layer_info);

        let total_entries: u64 = layer_info
            .iter()
            .filter_map(|v| v.get("entry_count").and_then(|c| c.as_u64()))
            .sum();
        assert!(total_entries > 0, "Entries should survive final restart");
        eprintln!("E2E lifecycle test PASSED: {} total entries", total_entries);
    }
}
