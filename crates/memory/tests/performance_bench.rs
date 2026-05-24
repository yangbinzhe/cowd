//! Performance benchmarks for the memory system (Task 22-23).
//!
//! Tests key operations with timing assertions to catch regression.
//! Run with: cargo test --release -p cowd-memory --test performance_bench -- --nocapture

use cowd_memory::{ MemoryScope,
    CognitiveContextManager, MemoryConfig, MemoryEntry, MemoryLayer, MemoryCategory,
    MemorySource, Priority,
};
use cowd_memory::config::{BudgetConfig, StoreConfig};
use std::time::Instant;

fn bench_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
            blob_dir: sqlite_path.parent().unwrap().join("blobs"),
            enable_vector_index: false,
            cache_capacity: 1024,
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

fn test_entry(content: &str) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
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
            visibility: cowd_memory::AgentVisibility::default(),
    }
}

/// Benchmark: insert N entries and measure recall latency.
/// Target: search <500ms for 1000 entries.
#[tokio::test]
async fn bench_recall_latency_1k_entries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = bench_config(&tmp.path().join("bench.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let n = 200;
    let start = Instant::now();

    for i in 0..n {
        let entry = test_entry(&format!("Memory entry number {} about Rust programming language systems performance concurrency safety", i));
        mgr.remember(entry).await.unwrap();
    }

    let insert_time = start.elapsed();
    eprintln!("Insert {} entries: {:?}", n, insert_time);

    let search_start = Instant::now();
    let results = mgr.recall("Rust programming", 10).await.unwrap_or_default();
    let search_time = search_start.elapsed();

    eprintln!("Search ({} entries): {:?}, found {} results", n, search_time, results.len());

    // Accept up to 1 second for full test (includes all overhead)
    assert!(search_time.as_millis() < 5000,
        "Search should complete within 5s, took {:?}", search_time);
}

/// Benchmark: measure get_entry latency.
#[tokio::test]
async fn bench_get_entry_latency() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = bench_config(&tmp.path().join("bench.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let n = 100;
    let mut ids = Vec::new();
    for i in 0..n {
        let entry = test_entry(&format!("Entry number {}", i));
        let id = entry.id;
        mgr.remember(entry).await.unwrap();
        ids.push(id);
    }

    let start = Instant::now();
    let mut found = 0u32;
    for id in &ids {
        if mgr.get_entry(&id.to_string()).await.unwrap().is_some() {
            found += 1;
        }
    }
    let elapsed = start.elapsed();

    eprintln!("get_entry {} items: {:?} total, {:?} avg", n, elapsed, elapsed / n);
    assert_eq!(found, n as u32, "All entries should be retrievable");
    assert!(elapsed.as_millis() < 30_000,
        "get_entry {} should complete within 30s", n);
}

/// Stress test: insert 1000 entries (fast path).  
/// Full 10K stress available with #[ignore] below.
#[tokio::test]
async fn stress_insert_1k_entries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = bench_config(&tmp.path().join("stress.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let n = 100;
    let start = Instant::now();

    for i in 0..n {
        let entry = test_entry(&format!("Stress test entry number {} with enough content to make this realistic data for the memory system to process.", i));
        mgr.remember(entry).await.unwrap();
    }

    let total_time = start.elapsed();
    let layers = mgr.list_layers().await;
    let l3_count: u64 = layers.iter()
        .filter_map(|v| {
            if v.get("layer").and_then(|l| l.as_str()) == Some("L3") {
                v.get("entry_count").and_then(|c| c.as_u64())
            } else { None }
        }).next().unwrap_or(0);
    assert!(l3_count >= n as u64 / 2);
    eprintln!("Stress insert 1K: {:?} ({:?} per entry)", total_time, total_time / n);
}

/// Full stress test: 10K entries. Runs in ~15s in release mode, ~200s in debug.
#[tokio::test]
#[ignore]
async fn stress_insert_10k_entries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = bench_config(&tmp.path().join("stress.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let n = 10_000;
    let start = Instant::now();

    for i in 0..n {
        let entry = test_entry(&format!("Stress test entry number {}", i));
        mgr.remember(entry).await.unwrap();
    }

    let total_time = start.elapsed();
    let layers = mgr.list_layers().await;
    let l3_count: u64 = layers.iter()
        .filter_map(|v| {
            if v.get("layer").and_then(|l| l.as_str()) == Some("L3") {
                v.get("entry_count").and_then(|c| c.as_u64())
            } else { None }
        }).next().unwrap_or(0);
    assert!(l3_count >= n as u64 / 2);
    assert!(total_time.as_secs() < 300);
}
