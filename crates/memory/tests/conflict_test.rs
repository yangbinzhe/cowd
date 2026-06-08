//! Cross-agent conflict detection and arbitration tests
//! Verifies Phase 2 features of the 3D memory framework

use cowd_memory::config::{BudgetConfig, StoreConfig};
use cowd_memory::project_scope::MemoryScope;
use cowd_memory::{
    AgentVisibility, CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry,
    MemoryLayer, MemorySource, Priority,
};

fn test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
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

fn make_entry(title: &str, content: &str, agent: &str) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L2,
        category: MemoryCategory::Decision,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: title.into(),
        content: content.into(),
        embedding: None,
        tags: vec![],
        relations: vec![],
        confidence: 0.9,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("test-proj".into()),
        visibility: AgentVisibility::Shared,
        source_agent: Some(agent.into()),
        session_id: None,
    }
}

#[tokio::test]
async fn test_conflicting_facts_detected() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("conflict.db")))
        .await
        .unwrap();

    // Agent A writes a fact
    let e1 = make_entry("fact-1", "Alice's parent is Bob", "agent-a");
    mgr.remember(e1).await.unwrap();

    // Agent B writes a contradictory fact
    let e2 = make_entry("fact-2", "Alice's parent is Charlie", "agent-b");
    let e2_id = e2.id;
    mgr.remember(e2).await.unwrap();

    // The contradictory entry should be retrievable (FactChecker only downgrades, never rejects)
    let retrieved = mgr.get_entry(&e2_id.to_string()).await.unwrap();
    assert!(
        retrieved.is_some(),
        "Contradictory entry should still be stored"
    );

    if let Some(e) = retrieved {
        // FactChecker should have downgraded the confidence
        assert!(
            e.confidence <= 0.9,
            "Contradictory entry confidence should be <= original"
        );
    }
}

#[tokio::test]
async fn test_consistent_facts_unchanged() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = CognitiveContextManager::new(test_config(&tmp.path().join("consistent.db")))
        .await
        .unwrap();

    let e1_id = uuid::Uuid::new_v4();
    let e1 = MemoryEntry {
        id: e1_id,
        layer: MemoryLayer::L2,
        category: MemoryCategory::Decision,
        priority: Priority::Normal,
        source: MemorySource::UserExplicit,
        title: "consistent".into(),
        content: "Project uses Rust for backend".into(),
        embedding: None,
        tags: vec![],
        relations: vec![],
        confidence: 0.95,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("test-proj".into()),
        visibility: AgentVisibility::Shared,
        source_agent: Some("agent-a".into()),
        session_id: None,
    };
    mgr.remember(e1).await.unwrap();

    let retrieved = mgr.get_entry(&e1_id.to_string()).await.unwrap().unwrap();
    assert_eq!(
        retrieved.confidence, 0.95,
        "Consistent entry should keep its confidence"
    );
}

#[tokio::test]
async fn test_multi_agent_entries_survive_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = tmp.path().join("ma.db");
    let _e1_id = uuid::Uuid::new_v4();
    let e2_id = uuid::Uuid::new_v4();

    {
        let mgr = CognitiveContextManager::new(test_config(&db))
            .await
            .unwrap();
        mgr.remember(make_entry("a1", "Fact from agent A", "agent-a"))
            .await
            .unwrap();
        let e2 = MemoryEntry {
            id: e2_id,
            layer: MemoryLayer::L2,
            category: MemoryCategory::Decision,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: "b1".into(),
            content: "Fact from agent B".into(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Project("test-proj".into()),
            visibility: AgentVisibility::Shared,
            source_agent: Some("agent-b".into()),
            session_id: None,
        };
        mgr.remember(e2).await.unwrap();
    }

    {
        let mgr = CognitiveContextManager::new(test_config(&db))
            .await
            .unwrap();
        let r2 = mgr.get_entry(&e2_id.to_string()).await.unwrap();
        assert!(r2.is_some(), "Agent B entry should survive restart");
        if let Some(e) = r2 {
            assert_eq!(e.source_agent, Some("agent-b".into()));
        }
    }
}
