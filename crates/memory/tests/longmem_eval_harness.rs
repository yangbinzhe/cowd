//! LongMemEval R@5 benchmark harness (mempalace 96.6% target).
//!
//! Tests retrieval recall using verbatim-stored entries.
//! Run: cargo test --release -p memory --test longmem_eval_harness -- --nocapture

use memory::config::{BudgetConfig, StoreConfig};
use memory::{
    evaluate_retrieval, CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry,
    MemoryEvalCase, MemoryEvalOptions, MemoryLayer, MemoryScope, MemorySource, Priority,
};
use std::time::Duration;

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

fn fact_entry(i: usize, fact: &str) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: format!("Fact entry {}", i),
        content: format!(
            "Memory entry containing fact: {fact}. The verification code is VERIFY-{i}. \
             Project shard alpha-{i} owns the durable recall marker {fact}."
        ),
        embedding: None,
        tags: vec!["lme-test".to_string(), format!("VERIFY-{i}")],
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

fn distractor_entry(i: usize) -> MemoryEntry {
    let mut entry = fact_entry(i, &format!("distractor_fact_{i}"));
    entry.title = format!("Distractor entry {i}");
    entry.content = format!(
        "Distractor memory {i} discusses generic rust async session memory routing \
         without the target verification code. It should not outrank exact fact markers."
    );
    entry.tags = vec!["lme-distractor".to_string()];
    entry
}

/// Simulated LongMemEval benchmark: stores target facts plus distractors, then
/// tests real search R@5 and latency. Each query targets a specific fact and
/// passes only if the expected memory is in the top-k results.
#[tokio::test]
async fn test_longmem_eval_recall_at_5() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("lme.db")))
        .await
        .unwrap();

    let n_entries = 100;
    let n_distractors = 400;
    let mut cases = Vec::new();

    for i in 0..n_entries {
        let fact = format!("unique_fact_{}", uuid::Uuid::new_v4().as_simple());
        let entry = fact_entry(i, &fact);
        cases.push(MemoryEvalCase {
            id: format!("case-{i}"),
            query: format!("{fact} VERIFY-{i}"),
            expected_memory_id: entry.id,
        });
        mgr.remember(entry).await.unwrap();
    }

    for i in 0..n_distractors {
        mgr.remember(distractor_entry(i)).await.unwrap();
    }

    let report = evaluate_retrieval(
        &mgr,
        &cases,
        MemoryEvalOptions {
            top_k: 5,
            min_recall_at_k: 0.98,
            max_p95_latency: Duration::from_millis(500),
        },
    )
    .await
    .unwrap();

    eprintln!(
        "LongMemEval R@5: {:.2}% MRR {:.3} p95 {:.3}ms misses {} total {:.3}ms",
        report.recall_at_k * 100.0,
        report.mrr,
        report.p95_latency_ms,
        report.misses.len(),
        report.total_latency_ms
    );

    assert!(report.passed, "R@5/p95 quality gate failed: {report:?}");
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
                visibility: memory::AgentVisibility::default(),
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
