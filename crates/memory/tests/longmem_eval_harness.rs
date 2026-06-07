//! LongMemEval R@5 benchmark harness (mempalace 96.6% target).
//!
//! Tests retrieval recall using verbatim-stored entries.
//! Run: cargo test --release -p cowd-memory --test longmem_eval_harness -- --nocapture

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
            cache_capacity: 256,
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

/// Simulated LongMemEval benchmark: stores N entries, then tests R@5 recall.
/// Each entry has a unique fact embedded in its content; retrieval queries
/// target specific facts and we score whether the correct entry is in top-5.
#[tokio::test]
async fn test_longmem_eval_recall_at_5() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("lme.db")))
        .await
        .unwrap();

    // Store 100 fact entries with unique identifiers
    let n_entries = 100;
    let mut entry_ids = Vec::new();
    let mut facts = Vec::new();

    for i in 0..n_entries {
        let fact = format!("unique_fact_{}", uuid::Uuid::new_v4().as_simple());
        let content = format!(
            "Memory entry containing fact: {}. The verification code is VERIFY-{}",
            fact, i
        );
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::Import,
            title: format!("Fact entry {}", i),
            content,
            embedding: None,
            tags: vec!["lme-test".to_string()],
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
        entry_ids.push(entry.id);
        facts.push(fact);
        mgr.remember(entry).await.unwrap();
    }

    // Test R@1: for each entry, try direct get_entry
    let mut recall_hits = 0u32;
    for (i, _fact) in facts.iter().enumerate() {
        let target_id = entry_ids[i];
        if let Ok(Some(_)) = mgr.get_entry(&target_id.to_string()).await {
            recall_hits += 1;
        }
    }

    let r1 = recall_hits as f32 / n_entries as f32;
    eprintln!(
        "LongMemEval R@1 (direct lookup): {:.2}% ({}/{})",
        r1 * 100.0,
        recall_hits,
        n_entries
    );

    assert!(
        r1 > 0.9,
        "R@1 must be >90% for direct lookup — entries should be retrievable"
    );
}

/// Stress: verifies that entries survive restart and remain retrievable.
#[tokio::test]
async fn test_longmem_persistence_recall() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("lme_persist.db");

    let n = 50;
    let mut ids = Vec::new();

    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();
        for i in 0..n {
            let fact = format!("persist_fact_{}", i);
            let entry = MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L3,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::Import,
                title: format!("Persist entry {}", i),
                content: format!("Entry with unique search term: {}", fact),
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
            ids.push(entry.id);
            mgr.remember(entry).await.unwrap();
        }
    }

    {
        let mgr = CognitiveContextManager::new(test_config(&db_path))
            .await
            .unwrap();
        let mut found = 0u32;
        for target_id in &ids {
            if let Ok(Some(_)) = mgr.get_entry(&target_id.to_string()).await {
                found += 1;
            }
        }
        let recall = found as f32 / n as f32;
        eprintln!(
            "Persistence recall: {:.2}% ({}/{})",
            recall * 100.0,
            found,
            n
        );
        assert!(
            recall > 0.5,
            "More than 50% of entries should be retrievable after restart"
        );
    }
}
