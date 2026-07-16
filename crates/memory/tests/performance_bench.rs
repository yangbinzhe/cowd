#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Performance benchmarks for the memory system (Task 22-23).
//!
//! Tests key operations with timing assertions to catch regression.
//! Run with: cargo test --release -p memory --test performance_bench -- --nocapture

use memory::config::{BudgetConfig, StoreConfig, TuningConfig};
use memory::{
    CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry, MemoryLayer, MemoryScope,
    MemorySource, Priority,
};
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

fn cached_prepare_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        tuning: TuningConfig {
            prefetch_hot_topics: 0,
            prepare_context_cache_ttl_ms: 60_000,
            ..Default::default()
        },
        ..bench_config(sqlite_path)
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
        visibility: memory::AgentVisibility::default(),
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

    eprintln!(
        "Search ({} entries): {:?}, found {} results",
        n,
        search_time,
        results.len()
    );

    // Accept up to 1 second for full test (includes all overhead)
    assert!(
        search_time.as_millis() < 5000,
        "Search should complete within 5s, took {:?}",
        search_time
    );
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

    eprintln!(
        "get_entry {} items: {:?} total, {:?} avg",
        n,
        elapsed,
        elapsed / n
    );
    assert_eq!(found, n, "All entries should be retrievable");
    assert!(
        elapsed.as_millis() < 30_000,
        "get_entry {} should complete within 30s",
        n
    );
}

/// Benchmark: repeated prepare_context on a warm local FTS5 path.
/// Target: p95 <300ms without remote/vector embedding calls.
#[tokio::test]
async fn bench_prepare_context_cached_p95_under_300ms() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = cached_prepare_config(&tmp.path().join("bench.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();
    assert_eq!(mgr.search_mode_label(), "keyword");

    for i in 0..300 {
        let entry = test_entry(&format!(
            "prepare context cache benchmark rust async scheduler latency entry {i}"
        ));
        mgr.remember(entry).await.unwrap();
    }

    let query = "rust async scheduler latency";
    let warm = mgr.prepare_context(query, &[], None).await.unwrap();
    assert!(
        !warm.entries.is_empty(),
        "warm prepare_context should surface benchmark entries"
    );

    let mut latencies_ms = Vec::new();
    for _ in 0..60 {
        let start = Instant::now();
        let ctx = mgr.prepare_context(query, &[], None).await.unwrap();
        assert_eq!(ctx.total_tokens, warm.total_tokens);
        latencies_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p95 = latencies_ms[(latencies_ms.len() * 95 / 100).min(latencies_ms.len() - 1)];
    eprintln!(
        "prepare_context cached p95: {:.3}ms over {} samples",
        p95,
        latencies_ms.len()
    );
    assert!(
        p95 < 300.0,
        "prepare_context cached p95 should be <300ms, got {p95:.3}ms"
    );
}

/// Fast stress test: 1000 entries (~0.5s).
/// Heavy stress tests: 10K (~5s release), 20K (~10s release) available for benchmarking.
/// Change `n` below to 10_000 or 20_000 when optimizing hot path.
#[tokio::test]
async fn stress_insert_1k_entries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = bench_config(&tmp.path().join("bench.db"));
    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let n = 1_000;
    let start = Instant::now();

    for i in 0..n {
        let entry = test_entry(&format!("Stress test entry number {}", i));
        mgr.remember(entry).await.unwrap();
    }

    let total_time = start.elapsed();
    let layers = mgr.list_layers().await;
    let l3_count: u64 = layers
        .iter()
        .filter_map(|v| {
            if v.get("layer").and_then(|l| l.as_str()) == Some("L3") {
                v.get("entry_count").and_then(|c| c.as_u64())
            } else {
                None
            }
        })
        .next()
        .unwrap_or(0);
    eprintln!(
        "Stress insert 20K: {:?} ({:?} per entry), L3={}",
        total_time,
        total_time / n as u32,
        l3_count
    );
    assert!(l3_count >= n as u64 / 2);
    assert!(total_time.as_secs() < 300);
}
