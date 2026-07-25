#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Fact-check write path integration tests.
//!
//! These tests verify that `remember()` uses `FactChecker` to detect
//! contradictions while preserving consistent facts.

use memory::config::{BudgetConfig, StoreConfig};
use memory::{
    CognitiveContextManager, FactChecker, MemoryCategory, MemoryConfig, MemoryEntry, MemoryLayer,
    MemoryScope, MemorySource, Priority,
};

/// 创建一个基础的测试配置
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
// Test 2: 通过 CognitiveContextManager 写入时触发事实校验
//
// 写入一个包含矛盾三元组的记忆条目，验证：
// 1. 写入成功（FactChecker 只降级置信度，不阻止写入）
// 2. 置信度被降级
// 3. 持久化后的条目保留降级结果
// =========================================================================
#[tokio::test]
async fn test_remember_contradictory_triple_confidence_downgraded() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp.path().join("test.db"));

    let mgr = CognitiveContextManager::new(config)
        .await
        .expect("Should create CognitiveContextManager");

    // First, register the baseline fact: Alice's parent is Bob
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
        source_agent: None,
        visibility: memory::AgentVisibility::default(),
    };
    mgr.remember(identity_entry)
        .await
        .expect("Should remember identity entry");

    // Now write the contradictory entry: FactChecker should detect the conflict
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
        source_agent: None,
        visibility: memory::AgentVisibility::default(),
    };

    let original_confidence = contradictory_entry.confidence;
    mgr.remember(contradictory_entry)
        .await
        .expect("Should remember contradictory entry (FactChecker only downgrades, never rejects)");

    let id_str = contradictory_id.to_string();
    match mgr.get_entry(&id_str).await {
        Ok(Some(retrieved)) => {
            eprintln!(
                "Retrieved entry: title={}, confidence={:.3}, original={:.3}",
                retrieved.title, retrieved.confidence, original_confidence
            );

            assert!(
                retrieved.confidence < original_confidence,
                "Contradictory entry confidence should be below {:.3}; got {:.3}",
                original_confidence,
                retrieved.confidence
            );
        }
        Ok(None) => {
            panic!("Entry with id {} should exist after remember()", id_str);
        }
        Err(e) => {
            panic!("Failed to retrieve entry: {}", e);
        }
    }
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
    let config = test_config(&tmp.path().join("test.db"));

    let mgr = CognitiveContextManager::new(config)
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
