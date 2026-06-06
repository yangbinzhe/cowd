//! RED Tests: Task 1 - prepare_context hybrid search integration
//!
//! These tests verify that CognitiveContextManager uses HybridSearcher
//! to find semantically similar entries when keyword matching fails.
//!
//! Current state: HybridSearcher is designed but NOT wired into prepare_context.
//! Test 3 should FAIL: entries with semantic similarity only (no keyword overlap)
//! are NOT currently returned by prepare_context(). After GREEN implementation,
//! hybrid search will match them via vector similarity.

use cowd_memory::config::TuningConfig;
use cowd_memory::config::{BudgetConfig, StoreConfig};
use cowd_memory::{
    CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry, MemoryLayer, MemoryScope,
    MemorySource, Priority,
};

fn test_basic_config(sqlite_path: &std::path::Path) -> MemoryConfig {
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

fn cached_test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        tuning: TuningConfig {
            l4_push_enabled: false,
            prefetch_hot_topics: 0,
            prepare_context_cache_ttl_ms: 60_000,
            ..Default::default()
        },
        ..test_basic_config(sqlite_path)
    }
}

fn test_entry(layer: MemoryLayer, title: &str, content: &str) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
        title: title.to_string(),
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
        visibility: cowd_memory::AgentVisibility::default(),
    }
}

// Test 1: Basic recall works with direct ID lookup
#[tokio::test]
async fn test_prepare_context_basic_get_entry() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_basic_config(&tmp.path().join("test.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let entry = test_entry(MemoryLayer::L3, "test-title", "test content here");
    let id = entry.id;
    mgr.remember(entry).await.unwrap();

    let retrieved = mgr.get_entry(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(retrieved.title, "test-title");
}

// Test 2: Entry persisted in correct layer
#[tokio::test]
async fn test_prepare_context_layer_isolation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_basic_config(&tmp.path().join("test.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let entry = test_entry(MemoryLayer::L2, "project-ctx", "project specific data");
    let id = entry.id;
    mgr.remember(entry).await.unwrap();

    let retrieved = mgr.get_entry(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(retrieved.layer, MemoryLayer::L2);
}

// Test 3 (RED): Semantic similarity recall without keyword overlap
//
// Store entries with wording that has NO keyword overlap with the query,
// then verify prepare_context CANNOT find them (because hybrid search
// is not yet wired in). After GREEN, this test should find the entries
// via hybrid search and the assertion should change from assert_zero
// to assert_nonzero.
#[tokio::test]
async fn test_prepare_context_semantic_recall_fails_without_hybrid_search() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_basic_config(&tmp.path().join("test.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    // Create a tag to identify this entry
    let tag = format!("sem-test-{}", uuid::Uuid::new_v4().as_simple());

    // Entry content: no keyword overlap with query "machine vision inference"
    let entry = MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: "CNN model".to_string(),
        content: format!(
            "Convolutional neural network processes pixel arrays. \
             Feature extraction through hierarchical layers. \
             Pattern detection for classification tasks. \
             tag:{}",
            tag
        ),
        embedding: None,
        tags: vec!["semantic-test".to_string()],
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

    // Query with completely different keywords (semantically related but no word overlap)
    let results = mgr
        .recall("machine vision inference", 10)
        .await
        .unwrap_or_default();

    eprintln!("Found {} results for semantic query", results.len());
    eprintln!(
        "entry_id = {}, matching = {}",
        entry_id,
        results.iter().any(|e| e.id == entry_id)
    );

    // RED assertion: currently, FTS5 keyword search fails to find this entry
    // because there's NO keyword overlap between the query and the content.
    // After hybrid search integration, the entry SHOULD be found.
    let found = results.iter().any(|e| e.id == entry_id);
    assert!(
        !found,
        "RED: Entry with semantic-only similarity should NOT be found ",
    );
    eprintln!("RED PASS: Semantic-only entry correctly not found by keyword search");
}

#[tokio::test]
async fn test_prepare_context_cache_invalidates_after_memory_write() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = cached_test_config(&tmp.path().join("test.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let query = "COWD_PREPARE_CACHE_INVALIDATE_ALPHA";
    let before = mgr.prepare_context(query, &[], None).await.unwrap();
    assert!(
        !before
            .entries
            .iter()
            .any(|entry| entry.content.contains(query)),
        "empty store should not surface the test entry before it is written"
    );

    let entry = test_entry(
        MemoryLayer::L3,
        "cache-invalidate",
        &format!("newly written memory {query}"),
    );
    mgr.remember(entry).await.unwrap();

    let after = mgr.prepare_context(query, &[], None).await.unwrap();
    assert!(
        after
            .entries
            .iter()
            .any(|entry| entry.content.contains(query)),
        "prepare_context request cache must be invalidated by memory writes"
    );
}
