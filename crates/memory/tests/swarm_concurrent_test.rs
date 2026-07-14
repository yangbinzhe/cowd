#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Concurrent scoped-memory stress test: 5 agents write L3 evidence simultaneously.
//!
//! Verifies:
//! - Zero data loss under concurrent writes
//! - Correct source_agent tracking per entry
//! - Cross-agent conflict detection (FactChecker integration)
//! - Scope isolation across project scopes

use std::sync::Arc;

use memory::config::MemoryConfig;
use memory::store::{sqlite::SqliteStore, MemoryStore};
use memory::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemoryOrchestrator, MemoryScope,
    MemorySource, Priority,
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_config() -> MemoryConfig {
    MemoryConfig::default()
}

fn make_entry(agent_id: &str, idx: u32, content: &str, scope: MemoryScope) -> MemoryEntry {
    let now = chrono::Utc::now();
    let tag = format!("agent-{agent_id}-entry-{idx}");
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Shared,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
        title: format!("swarm-{agent_id}-{idx}"),
        content: content.to_string(),
        embedding: None,
        tags: vec!["swarm-test".into(), tag],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: now,
        updated_at: now,
        last_accessed_at: None,
        scope,
        session_id: None,
        source_agent: Some(agent_id.to_string()),
        visibility: AgentVisibility::Shared,
    }
}

// ---------------------------------------------------------------------------
// Test 1: 5 agents writing concurrently — zero data loss
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_5_agents_concurrent_writes_no_data_loss() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_nodataloss.db");
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_path(&db_path).unwrap());

    let orch =
        Arc::new(MemoryOrchestrator::from_store(test_config(), Arc::clone(&store), None).unwrap());

    const AGENTS: usize = 5;
    const ENTRIES_PER_AGENT: usize = 10;
    const TOTAL: usize = AGENTS * ENTRIES_PER_AGENT;

    // Spawn concurrent writes
    let mut handles = Vec::new();
    for i in 0..AGENTS {
        let orch = Arc::clone(&orch);
        let agent = format!("agent-{i}");
        let scope = MemoryScope::Project("concurrent-project".into());
        handles.push(tokio::spawn(async move {
            for j in 0..ENTRIES_PER_AGENT {
                let content = format!(
                    "Concurrent write from {agent} entry #{j}: exploring swarm memory infrastructure."
                );
                let entry = make_entry(&agent, j as u32, &content, scope.clone());
                orch.remember(entry).await.expect("remember must succeed");
            }
        }));
    }

    // Await all spawns
    for h in handles {
        h.await.expect("tokio task should not panic");
    }

    // Verify: all entries survived
    let l3_meta = orch.list_layer(MemoryLayer::L3).await.unwrap();
    assert_eq!(
        l3_meta.len(),
        TOTAL,
        "Expected {TOTAL} entries in L3, got {}",
        l3_meta.len()
    );

    // Verify each entry is retrievable
    for meta in &l3_meta {
        let entry = orch.recall(&meta.id).await.unwrap();
        assert!(entry.is_some(), "Entry {} should be retrievable", meta.id);
    }

    // Verify layer distribution (all should be L3)
    let retrieved = orch.list_layer(MemoryLayer::L3).await.unwrap();
    for meta in &retrieved {
        let e = orch.recall(&meta.id).await.unwrap().unwrap();
        assert_eq!(e.layer, MemoryLayer::L3, "All entries should be in L3");
    }
}

// ---------------------------------------------------------------------------
// Test 2: source_agent tracking — each entry tagged with correct agent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_source_agent_tracking() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_agent_tracking.db");
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_path(&db_path).unwrap());

    let orch =
        Arc::new(MemoryOrchestrator::from_store(test_config(), Arc::clone(&store), None).unwrap());

    // Set common scope
    const AGENTS: usize = 5;
    const ENTRIES_PER_AGENT: usize = 4;

    let mut handles = Vec::new();
    for i in 0..AGENTS {
        let orch = Arc::clone(&orch);
        let agent = format!("swarm-agent-{}", i);
        let scope = MemoryScope::Project("tracking-project".into());
        handles.push(tokio::spawn(async move {
            for j in 0..ENTRIES_PER_AGENT {
                let content = format!("Tracking test: {agent} writes entry {j}.");
                let entry = make_entry(&agent, j as u32, &content, scope.clone());
                orch.remember(entry).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Fetch all L3 entries and verify source_agent
    let metas = orch.list_layer(MemoryLayer::L3).await.unwrap();
    assert_eq!(metas.len(), AGENTS * ENTRIES_PER_AGENT);

    let mut agent_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for meta in &metas {
        let entry = orch.recall(&meta.id).await.unwrap().unwrap();
        let sa = entry
            .source_agent
            .expect("Every entry must have a source_agent");
        assert!(
            sa.starts_with("swarm-agent-"),
            "source_agent should match expected pattern, got: {sa}"
        );
        *agent_counts.entry(sa).or_insert(0) += 1;
    }

    // Each agent should have exactly ENTRIES_PER_AGENT entries
    for i in 0..AGENTS {
        let key = format!("swarm-agent-{i}");
        assert_eq!(
            agent_counts.get(&key).copied().unwrap_or(0),
            ENTRIES_PER_AGENT,
            "Agent {key} should have {ENTRIES_PER_AGENT} entries"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 3: Cross-agent conflict detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_cross_agent_conflict_detection() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_conflict.db");
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_path(&db_path).unwrap());

    let orch = MemoryOrchestrator::from_store(test_config(), Arc::clone(&store), None).unwrap();

    // Agent A registers a fact

    let fact_a_id = uuid::Uuid::new_v4();
    let entry_a = MemoryEntry {
        id: fact_a_id,
        layer: MemoryLayer::L3,
        category: MemoryCategory::Decision,
        priority: Priority::High,
        source: MemorySource::Import,
        title: "Agent A fact".into(),
        content: "Bob's parent is Alice".to_string(),
        embedding: None,
        tags: vec!["conflict-test".into()],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("conflict-proj".into()),
        session_id: None,
        source_agent: Some("agent-planner".into()),
        visibility: AgentVisibility::Shared,
    };
    orch.remember(entry_a).await.unwrap();

    // Verify Agent A's fact was written
    let got_a = orch.recall(&fact_a_id).await.unwrap().unwrap();
    assert_eq!(got_a.source_agent.as_deref(), Some("agent-planner"));

    // Agent B writes a contradictory fact
    let fact_b_id = uuid::Uuid::new_v4();
    let entry_b = MemoryEntry {
        id: fact_b_id,
        layer: MemoryLayer::L3,
        category: MemoryCategory::Decision,
        priority: Priority::High,
        source: MemorySource::Import,
        title: "Agent B contradiction".into(),
        // "Bob's parent is Alice" vs "Bob's parent is Charlie" — contradiction
        content: "Bob's parent is Charlie".to_string(),
        embedding: None,
        tags: vec!["conflict-test".into()],
        relations: vec![],
        confidence: 0.9,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("conflict-proj".into()),
        session_id: None,
        source_agent: Some("agent-executor".into()),
        visibility: AgentVisibility::Shared,
    };
    orch.remember(entry_b).await.unwrap();

    // Verify Agent B's entry was downgraded in confidence
    let got_b = orch.recall(&fact_b_id).await.unwrap().unwrap();
    eprintln!(
        "Conflict test: Agent B confidence = {:.3} (was 0.9)",
        got_b.confidence
    );
    assert!(
        got_b.confidence < 0.9,
        "Contradictory entry from Agent B should have confidence < 0.9, got {:.3}",
        got_b.confidence
    );
    assert_eq!(
        got_b.source_agent.as_deref(),
        Some("agent-executor"),
        "Agent B entry should track correct source_agent"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Scope isolation — entries in different project scopes don't leak
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_scope_isolation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_scope.db");
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_path(&db_path).unwrap());

    let orch = MemoryOrchestrator::from_store(test_config(), Arc::clone(&store), None).unwrap();

    // Agent 1 writes to project-alpha
    for j in 0..5 {
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: format!("alpha-entry-{j}"),
            content: format!("Project Alpha shared knowledge item {j}"),
            embedding: None,
            tags: vec!["scope-test".into(), "alpha".into()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Project("project-alpha".into()),
            session_id: None,
            source_agent: Some("agent-alpha".into()),
            visibility: AgentVisibility::Shared,
        };
        orch.remember(entry).await.unwrap();
    }

    // Agent 2 writes to project-beta
    for j in 0..3 {
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: format!("beta-entry-{j}"),
            content: format!("Project Beta shared knowledge item {j}"),
            embedding: None,
            tags: vec!["scope-test".into(), "beta".into()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Project("project-beta".into()),
            session_id: None,
            source_agent: Some("agent-beta".into()),
            visibility: AgentVisibility::Shared,
        };
        orch.remember(entry).await.unwrap();
    }

    // All entries should be persisted (8 total)
    let all = orch.list_layer(MemoryLayer::L3).await.unwrap();
    assert_eq!(all.len(), 8, "Expected 8 total entries across scopes");

    // Verify scope isolation: recall entries and verify their scope values
    let mut alpha_entries = 0usize;
    let mut beta_entries = 0usize;
    for meta in &all {
        if let Some(entry) = orch.recall(&meta.id).await.unwrap() {
            match &entry.scope {
                MemoryScope::Project(p) if p == "project-alpha" => alpha_entries += 1,
                MemoryScope::Project(p) if p == "project-beta" => beta_entries += 1,
                _ => {}
            }
        }
    }
    assert_eq!(alpha_entries, 5, "Expected 5 project-alpha entries");
    assert_eq!(beta_entries, 3, "Expected 3 project-beta entries");

    // Verify source_agent tracking per scope
    let mut alpha_agent_count = 0usize;
    for m in &all {
        if let Ok(Some(entry)) = orch.recall(&m.id).await {
            if entry.source_agent.as_deref() == Some("agent-alpha") {
                alpha_agent_count += 1;
            }
        }
    }
    assert_eq!(
        alpha_agent_count, 5,
        "All alpha entries should have agent-alpha source"
    );
}
