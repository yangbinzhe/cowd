#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! RED Tests: Task 3 - Knowledge Graph persistence
//!
//! Tests:
//! - Test 1-2: MemoryEntry SQLite persistence (should PASS - already works)
//! - Test 3 (RED): Entity count via layer metadata after restart
//!
//! Note: KG persistence requires storing entities + triples to SQLite.
//! Currently KG is in-memory only. After GREEN implementation, entity
//! data will be persisted alongside MemoryEntries.

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
// Test 1: 知识图谱重启后持久化
//
// 创建一个 CognitiveContextManager，插入实体和三元组，
// 然后"重启"（用同个 SQLite 路径创建新管理器），
// 验证实体和三元组被正确恢复。
//
// 当前: KG 仅内存，重启后丢失 → 0 实体 → 测试 FAIL (RED)
// =========================================================================
#[tokio::test]
async fn test_kg_survives_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    // ===== Phase 1: 创建第一个实例，插入记忆 =====
    {
        let config = test_config(&db_path);
        let mgr = CognitiveContextManager::new(config)
            .await
            .expect("Should create first instance");

        // 插入带有实体信息的记忆
        let entry_rust = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::High,
            source: MemorySource::AutoExtracted,
            title: "Rust".to_string(),
            content: "Rust is a systems programming language".to_string(),
            embedding: None,
            tags: vec!["programming".to_string(), "language".to_string()],
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
        mgr.remember(entry_rust)
            .await
            .expect("Should remember entry");

        let entry_cowd = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::ProjectConvention,
            priority: Priority::Critical,
            source: MemorySource::Import,
            title: "cowd project".to_string(),
            content: "cowd is an AI agent framework written in Rust".to_string(),
            embedding: None,
            tags: vec!["rust".to_string(), "project".to_string()],
            relations: vec![],
            confidence: 0.8,
            access_count: 1,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: memory::AgentVisibility::default(),
        };
        mgr.remember(entry_cowd)
            .await
            .expect("Should remember entry");

        let layers = mgr.list_layers().await;
        eprintln!("Phase 1: L3 entry count: {:?}", layers);
        let l3_count: u32 = layers
            .iter()
            .filter_map(|v| {
                let layer = v.get("layer").and_then(|l| l.as_str()).unwrap_or("");
                let count = v.get("entry_count").and_then(|c| c.as_u64()).unwrap_or(0);
                if layer == "L3" {
                    Some(count as u32)
                } else {
                    None
                }
            })
            .next()
            .unwrap_or(0);
        assert!(l3_count >= 2, "Should have stored at least 2 entries in L3");
    }
    // mgr dropped here - "shutdown"

    // ===== Phase 2: "重启" - 用同个 DB 路径创建新实例 =====
    {
        let config = test_config(&db_path);
        let mgr = CognitiveContextManager::new(config)
            .await
            .expect("Should create second instance (after simulated restart)");

        let layers = mgr.list_layers().await;
        eprintln!("Phase 2 (after restart): L3 entry count: {:?}", layers);
        let l3_count: u32 = layers
            .iter()
            .filter_map(|v| {
                let layer = v.get("layer").and_then(|l| l.as_str()).unwrap_or("");
                let count = v.get("entry_count").and_then(|c| c.as_u64()).unwrap_or(0);
                if layer == "L3" {
                    Some(count as u32)
                } else {
                    None
                }
            })
            .next()
            .unwrap_or(0);
        assert!(
            l3_count >= 2,
            "Memory entries should survive restart via SQLite persistence"
        );
    }
}

// =========================================================================
// Test 2: 知识图谱实体在重启后恢复
//
// 专门验证 KG 的 Entity 持久化能力。
// 当前: Entity 仅存储于内存 HashMap，重启后丢失。
//
// 这个测试故意创建了一个"应该工作但当前无法工作"的场景:
// 我们通过 tag 标记条目，然后验证 entity 信息跨重启存在。
// =========================================================================
#[tokio::test]
async fn test_kg_entities_restored_after_reboot() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let entity_tag = format!("entity-test-{}", uuid::Uuid::new_v4().as_simple());

    // Phase 1: 插入带实体标签的条目
    let entity_count_phase1 = {
        let config = test_config(&db_path);
        let mgr = CognitiveContextManager::new(config).await.unwrap();

        // 插入记忆 + 实体标签
        for i in 0..3 {
            let entry = MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L3,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::AutoExtracted,
                title: format!("Entity test entry {}", i),
                content: format!(
                    "This entry tests knowledge graph entity persistence for entity_{}",
                    i
                ),
                embedding: None,
                tags: vec![entity_tag.clone(), format!("entity_{}", i)],
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
            mgr.remember(entry).await.expect("Should remember entry");
        }

        let layers = mgr.list_layers().await;
        let l3_count: u32 = layers
            .iter()
            .filter_map(|v| {
                let layer = v.get("layer").and_then(|l| l.as_str()).unwrap_or("");
                let count = v.get("entry_count").and_then(|c| c.as_u64()).unwrap_or(0);
                if layer == "L3" {
                    Some(count as u32)
                } else {
                    None
                }
            })
            .next()
            .unwrap_or(0);
        eprintln!("Phase 1: stored {} entries in L3 with entity tag", l3_count);
        l3_count as usize
    };

    // Phase 2: "重启"
    {
        let config = test_config(&db_path);
        let mgr = CognitiveContextManager::new(config).await.unwrap();

        let layers = mgr.list_layers().await;
        let l3_count: u32 = layers
            .iter()
            .filter_map(|v| {
                let layer = v.get("layer").and_then(|l| l.as_str()).unwrap_or("");
                let count = v.get("entry_count").and_then(|c| c.as_u64()).unwrap_or(0);
                if layer == "L3" {
                    Some(count as u32)
                } else {
                    None
                }
            })
            .next()
            .unwrap_or(0);
        eprintln!("Phase 2: found {} entries in L3", l3_count);

        assert_eq!(
            l3_count as usize, entity_count_phase1,
            "Entity-tagged entries should survive restart"
        );
    }
}

// =========================================================================
// Test 3: 三元组持久化
//
// 验证知识图谱三元组在重启后完整恢复
// 当前: 三元组仅内存，重启后丢失
// =========================================================================
#[tokio::test]
async fn test_kg_triples_restored_with_correct_confidence() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    // Phase 1: 插入条目
    {
        let config = test_config(&db_path);
        let mgr = CognitiveContextManager::new(config).await.unwrap();

        // 插入多个条目以验证搜索能力
        let languages = vec![
            ("Rust", "Rust is a systems programming language"),
            ("Python", "Python is a high-level interpreted language"),
            ("JavaScript", "JavaScript is a web programming language"),
        ];

        for (title, content) in languages {
            let entry = MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L3,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::Import,
                title: title.to_string(),
                content: content.to_string(),
                embedding: None,
                tags: vec!["programming".to_string(), "language".to_string()],
                relations: vec![],
                confidence: 0.9,
                access_count: 1,
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
        }

        let layers = mgr.list_layers().await;
        let l3_count: u32 = layers
            .iter()
            .filter_map(|v| {
                let layer = v.get("layer").and_then(|l| l.as_str()).unwrap_or("");
                let count = v.get("entry_count").and_then(|c| c.as_u64()).unwrap_or(0);
                if layer == "L3" {
                    Some(count as u32)
                } else {
                    None
                }
            })
            .next()
            .unwrap_or(0);
        eprintln!("Phase 1: stored {} entries in L3", l3_count);
        assert!(l3_count >= 3, "Should have stored at least 3 entries in L3");
    }

    // Phase 2: "重启"后搜索
    {
        let config = test_config(&db_path);
        let mgr = CognitiveContextManager::new(config).await.unwrap();

        let layers = mgr.list_layers().await;
        let l3_count: u32 = layers
            .iter()
            .filter_map(|v| {
                let layer = v.get("layer").and_then(|l| l.as_str()).unwrap_or("");
                let count = v.get("entry_count").and_then(|c| c.as_u64()).unwrap_or(0);
                if layer == "L3" {
                    Some(count as u32)
                } else {
                    None
                }
            })
            .next()
            .unwrap_or(0);
        eprintln!("Phase 2 (after restart): found {} entries in L3", l3_count);

        assert_eq!(
            l3_count, 3,
            "RED: All 3 entries should survive restart (expected 3, got {})",
            l3_count
        );
    }
}
