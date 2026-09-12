#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Fact-check write path integration tests.
//!
//! Explicit FactChecker validation remains available. Memory writes preserve
//! source claims; a process-global regex inference is not evidence authority.

use memory::config::{BudgetConfig, StoreConfig};
use memory::{
    CognitiveContextManager, FactChecker, MemoryCategory, MemoryConfig, MemoryEntry, MemoryLayer,
    MemoryScope, MemorySource, Priority,
};

/// 创建一个基础的测试配置
fn test_config(blob_dir: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            blob_dir: blob_dir.to_path_buf(),
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

// =========================================================================
// Test 1: FactChecker 能正确检测矛盾三元组
//
// 使用 FactChecker 的单元测试能力（不经过 CognitiveContextManager）
// 验证: 矛盾的三元组被检测 → is_consistent = false
// 这个测试直接验证 FactChecker 的矛盾检测合同。
// =========================================================================
#[test]
fn test_fact_checker_detects_contradiction() {
    use memory::temporal_graph::{EntityFacts, Triple};

    let mut checker = FactChecker::new();
    let mut facts = EntityFacts::default();
    facts.entity_type = Some("person".to_string());
    facts.parent = Some("Bob".to_string());
    checker.register_facts("Alice", facts);

    let triple = Triple {
        id: "test-triple-1".to_string(),
        subject: "alice".to_string(),
        predicate: "child_of".to_string(),
        object: "Charlie".to_string(),
        confidence: 1.0,
        valid_from: None,
        valid_until: None,
        source_memory_id: None,
        source_file: None,
        source_agent: None,
    };

    let result = checker.check_triple(&triple);

    eprintln!(
        "Fact check result: is_consistent={}, confidence={:.2}",
        result.is_consistent, result.confidence
    );
    if let Some(ref c) = result.contradiction {
        eprintln!("Contradiction: {}", c);
    }

    assert!(
        !result.is_consistent,
        "Should detect contradiction: Alice parent=Bob but triple says Charlie"
    );
    assert!(result.confidence < 1.0, "Confidence should be downgraded");
    assert!(
        result.contradiction.is_some(),
        "Should describe the contradiction"
    );
}

// =========================================================================
// Test 2: 私有来源主张写入不触发全局正则改写
//
// 写入两个不同来源的主张，验证：
// 1. 两个来源都保留自己的原文
// 2. 来源置信度不被全局推断自动改写
// 3. 私有可见性与来源身份保留
// =========================================================================
#[tokio::test]
async fn test_remember_preserves_private_source_claims_without_global_fact_rewrite() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp.path().join("blobs"));

    let mgr = CognitiveContextManager::new_ephemeral(config)
        .await
        .expect("Should create CognitiveContextManager");

    // A private claim does not register process-global verified identity.
    let identity_entry = MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L1,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: "Known Identity".to_string(),
        content: "Alice's parent is Bob".to_string(),
        embedding: None,
        tags: vec!["identity".to_string()],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        session_id: None,
        source_agent: Some("source-a".to_string()),
        visibility: memory::AgentVisibility::Private,
    };
    mgr.remember(identity_entry)
        .await
        .expect("Should remember identity entry");

    // A different source may disagree; retain provenance for explicit review.
    let contradictory_id = uuid::Uuid::new_v4();
    let contradictory_entry = MemoryEntry {
        id: contradictory_id,
        layer: MemoryLayer::L2,
        category: MemoryCategory::Decision,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
        title: "Contradictory Info".to_string(),
        content: "Alice's parent is Charlie".to_string(),
        embedding: None,
        tags: vec!["contradiction-test".to_string()],
        relations: vec![],
        confidence: 0.9,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        session_id: None,
        source_agent: Some("source-b".to_string()),
        visibility: memory::AgentVisibility::Private,
    };

    let original_confidence = contradictory_entry.confidence;
    mgr.remember(contradictory_entry)
        .await
        .expect("Should retain the independently attributed claim");

    let retrieved = mgr
        .get_entry(&contradictory_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.confidence, original_confidence);
    assert_eq!(retrieved.content, "Alice's parent is Charlie");
    assert_eq!(retrieved.source_agent.as_deref(), Some("source-b"));
    assert_eq!(retrieved.visibility, memory::AgentVisibility::Private);
}

// =========================================================================
// Test 3: 一致的条目不受影响
//
// 写入一个不矛盾的条目，置信度应保持不变
// 事实检查不能误伤没有矛盾的用户偏好。
// =========================================================================
#[tokio::test]
async fn test_remember_accepts_consistent_entry() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp.path().join("blobs"));

    let mgr = CognitiveContextManager::new_ephemeral(config)
        .await
        .expect("Should create CognitiveContextManager");

    let entry_id = uuid::Uuid::new_v4();

    // 写入一个无矛盾的条目
    let entry = MemoryEntry {
        id: entry_id,
        layer: MemoryLayer::L1,
        category: MemoryCategory::UserPreference,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: "Normal Preference".to_string(),
        content: "User prefers dark mode in all applications".to_string(),
        embedding: None,
        tags: vec!["preference".to_string(), "ui".to_string()],
        relations: vec![],
        confidence: 0.95,
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

    mgr.remember(entry).await.expect("Should remember entry");

    let retrieved = mgr
        .get_entry(&entry_id.to_string())
        .await
        .expect("Should get entry")
        .expect("Entry should exist");

    assert_eq!(
        retrieved.confidence, 0.95,
        "Consistent entry confidence should remain unchanged"
    );
}
