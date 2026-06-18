//! Swarm E2E test: full Planner → Executor → Reviewer lifecycle.
//!
//! Simulates a three-agent collaboration through the L4 shared layer:
//! 1. Agent Planner writes a task to L4 (Shared visibility)
//! 2. Agent Executor reads the task, executes it, writes the result
//! 3. Agent Reviewer reads both, writes a review
//! 4. Agent Planner sees the complete lifecycle
//!
//! Verifies:
//! - source_agent tracking across the full pipeline
//! - scope isolation
//! - peer perception (each agent can see prior agents' L4 entries)

use memory::config::{BudgetConfig, StoreConfig};
use memory::{
    AgentVisibility, CognitiveContextManager, MemoryCategory, MemoryConfig, MemoryEntry,
    MemoryLayer, MemoryScope, MemorySource, Priority,
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

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

fn make_agent_entry(
    agent_id: &str,
    title: &str,
    content: &str,
    category: MemoryCategory,
    tags: Vec<String>,
    scope: MemoryScope,
    layer: MemoryLayer,
) -> MemoryEntry {
    let now = chrono::Utc::now();
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
        title: title.to_string(),
        content: content.to_string(),
        embedding: None,
        tags,
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
// E2E Test: Planner → Executor → Reviewer → Planner (full lifecycle)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_e2e_planner_executor_reviewer_lifecycle() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_e2e.db");
    let config = test_config(&db_path);

    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let project_scope = MemoryScope::Project("e2e-swarm-project".into());
    let lifecycle_tag = format!("e2e-lifecycle-{}", uuid::Uuid::new_v4().as_simple());

    // =====================================================================
    // Phase 1: Agent Planner writes task
    // =====================================================================
    mgr.set_active_agent("agent-planner".into());

    let planner_task_id = uuid::Uuid::new_v4();
    let planner_task = make_agent_entry(
        "agent-planner",
        "E2E Task: Build auth middleware",
        "Implement OAuth2 middleware for the API gateway. Requirements: \
         - Support JWT and API key auth. \
         - Rate limiting per token. \
         - Audit logging for all auth events.",
        MemoryCategory::Decision,
        vec!["e2e".into(), lifecycle_tag.clone(), "task".into()],
        project_scope.clone(),
        MemoryLayer::L4,
    );
    // Override the random ID with a known one
    let planner_task = MemoryEntry {
        id: planner_task_id,
        ..planner_task
    };
    mgr.remember(planner_task).await.unwrap();

    eprintln!("[Planner] Wrote task: {planner_task_id}");

    // Verify Planner can see own task
    let own_task = mgr.get_entry(&planner_task_id.to_string()).await.unwrap();
    assert!(own_task.is_some(), "Planner should see own task");
    assert_eq!(
        own_task.as_ref().unwrap().source_agent.as_deref(),
        Some("agent-planner")
    );
    assert_eq!(own_task.unwrap().visibility, AgentVisibility::Shared);

    // =====================================================================
    // Phase 2: Agent Executor reads task, executes, writes result
    // =====================================================================
    mgr.set_active_agent("agent-executor".into());

    // Executor searches for pending tasks
    let tasks = mgr.recall("auth middleware", 10).await.unwrap();
    eprintln!(
        "[Executor] Found {} relevant entries when searching for tasks",
        tasks.len()
    );

    // Executor should find the Planner's task (via L4 recall since visibility is Shared)
    let found_task = tasks.iter().find(|e| e.id == planner_task_id);
    assert!(
        found_task.is_some(),
        "Executor should see Planner's shared task. Found entries: {}",
        tasks
            .iter()
            .map(|e| format!("{} ({})", e.title, e.id))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let task_content = found_task.unwrap().content.clone();
    eprintln!("[Executor] Found task content: {:.60}...", task_content);

    // Executor writes execution result
    let executor_result_id = uuid::Uuid::new_v4();
    let executor_result = make_agent_entry(
        "agent-executor",
        "Auth middleware implementation result",
        "Completed OAuth2 middleware implementation. \
         - JWT validation with JWKS endpoint. \
         - API key validation via header lookup. \
         - Token bucket rate limiter (100 req/min). \
         - Structured audit logs to `auth_events` table. \
         Status: DONE. Tests: 23/23 pass.",
        MemoryCategory::Decision,
        vec![
            "e2e".into(),
            lifecycle_tag.clone(),
            "result".into(),
            "task-complete".into(),
        ],
        project_scope.clone(),
        MemoryLayer::L4,
    );
    let executor_result = MemoryEntry {
        id: executor_result_id,
        ..executor_result
    };
    mgr.remember(executor_result).await.unwrap();
    eprintln!("[Executor] Wrote result: {executor_result_id}");

    // =====================================================================
    // Phase 3: Agent Reviewer reads both, writes review
    // =====================================================================
    mgr.set_active_agent("agent-reviewer".into());

    // Reviewer searches for all entries in this lifecycle
    let all_entries = mgr.recall(&lifecycle_tag, 10).await.unwrap();
    eprintln!("[Reviewer] Found {} lifecycle entries", all_entries.len());
    assert!(
        all_entries.len() >= 2,
        "Reviewer should see at least 2 entries (task + result), got {}",
        all_entries.len()
    );

    // Verify Reviewer can see both Planner's task and Executor's result
    let planner_seen = all_entries.iter().any(|e| e.id == planner_task_id);
    let executor_seen = all_entries.iter().any(|e| e.id == executor_result_id);
    assert!(planner_seen, "Reviewer should see Planner's task");
    assert!(executor_seen, "Reviewer should see Executor's result");

    // Verify source_agent tracking from Reviewer's perspective
    for entry in &all_entries {
        assert!(
            entry.source_agent.is_some(),
            "Entry {} should have a source_agent",
            entry.id
        );
    }

    // Reviewer writes review
    let review_id = uuid::Uuid::new_v4();
    let review = make_agent_entry(
        "agent-reviewer",
        "Code review: Auth middleware",
        "Review PASSED. Comments: \
         - JWT implementation follows best practices. \
         - Rate limiter configurable via env vars. \
         - Audit logging includes request metadata. \
         - Suggestion: add HMAC signature option for webhook auth.",
        MemoryCategory::Decision,
        vec!["e2e".into(), lifecycle_tag.clone(), "review".into()],
        project_scope.clone(),
        MemoryLayer::L4,
    );
    let review = MemoryEntry {
        id: review_id,
        ..review
    };
    mgr.remember(review).await.unwrap();
    eprintln!("[Reviewer] Wrote review: {review_id}");

    // =====================================================================
    // Phase 4: Agent Planner sees complete lifecycle
    // =====================================================================
    mgr.set_active_agent("agent-planner".into());

    // Planner searches for the complete lifecycle
    let complete = mgr.recall(&lifecycle_tag, 10).await.unwrap();
    eprintln!(
        "[Planner] Lifecycle complete — found {} entries",
        complete.len()
    );

    // Verify Planner sees all 3 entries (task, result, review)
    assert!(
        complete.len() >= 3,
        "Planner should see all 3 lifecycle entries, got {}",
        complete.len()
    );

    let has_task = complete.iter().any(|e| e.id == planner_task_id);
    let has_result = complete.iter().any(|e| e.id == executor_result_id);
    let has_review = complete.iter().any(|e| e.id == review_id);

    assert!(has_task, "Planner should see own task");
    assert!(has_result, "Planner should see Executor's result");
    assert!(has_review, "Planner should see Reviewer's review");

    eprintln!("=== E2E Lifecycle PASSED ===");
    eprintln!(
        "Planner sees: task={}, result={}, review={}",
        has_task, has_result, has_review
    );

    // Verify all entries share the same scope
    if !complete.is_empty() {
        let first_scope = &complete[0].scope;
        for entry in &complete {
            assert_eq!(
                &entry.scope, first_scope,
                "All lifecycle entries should share the same scope"
            );
        }
    }

    // =====================================================================
    // Phase 6: Verify source_agent diversity (3 distinct agents)
    // =====================================================================
    let agents: std::collections::HashSet<String> = complete
        .iter()
        .filter_map(|e| e.source_agent.clone())
        .collect();
    eprintln!("[Verification] Distinct source agents: {:?}", agents);
    assert!(
        agents.len() >= 3,
        "Expected at least 3 distinct agents, got {}: {:?}",
        agents.len(),
        agents
    );
    assert!(agents.contains("agent-planner"));
    assert!(agents.contains("agent-executor"));
    assert!(agents.contains("agent-reviewer"));

    // =====================================================================
    // Phase 7: Verify entries are in L4
    // =====================================================================
    let l4_meta = mgr.list_layer_entries(MemoryLayer::L4).await.unwrap();
    eprintln!("[Verification] Total L4 entries: {}", l4_meta.len());
    assert!(
        l4_meta.len() >= 3,
        "Expected at least 3 entries in L4, got {}",
        l4_meta.len()
    );
}

// ---------------------------------------------------------------------------
// E2E Test: Peer perception — each agent sees prior agents' shared work
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_e2e_peer_perception() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_peer_perception.db");
    let config = test_config(&db_path);

    let mgr = CognitiveContextManager::new(config).await.unwrap();

    let scope = MemoryScope::Project("peer-perception-proj".into());
    let session_tag = format!("peer-test-{}", uuid::Uuid::new_v4().as_simple());

    // Agent 1 (Planner): writes task
    mgr.set_active_agent("agent-alpha".into());

    let task_id = uuid::Uuid::new_v4();
    let task = MemoryEntry {
        id: task_id,
        ..make_agent_entry(
            "agent-alpha",
            "Peer test task: Optimize DB queries",
            "Need to optimize slow Postgres queries in the user service. \
             Current p99 latency is 850ms, target is <50ms.",
            MemoryCategory::Decision,
            vec!["peer-test".into(), session_tag.clone(), "task".into()],
            scope.clone(),
            MemoryLayer::L4,
        )
    };
    mgr.remember(task).await.unwrap();

    // Agent 2 (Executor): searches for tasks by other agents
    mgr.set_active_agent("agent-bravo".into());

    // Executor recalls entries from "other" agents (i.e., not themselves)
    let peer_tasks = mgr.recall("optimize DB", 10).await.unwrap();
    eprintln!(
        "[Agent Bravo] Found {} entries in peer search",
        peer_tasks.len()
    );

    let found_alpha_task = peer_tasks
        .iter()
        .any(|e| e.source_agent.as_deref() == Some("agent-alpha") && e.id == task_id);
    assert!(
        found_alpha_task,
        "Agent Bravo should perceive Agent Alpha's shared task"
    );

    // Verify Agent Bravo does NOT see its own entries (it hasn't written any)
    let own_entries = peer_tasks
        .iter()
        .filter(|e| e.source_agent.as_deref() == Some("agent-bravo"));
    assert_eq!(
        own_entries.count(),
        0,
        "Agent Bravo should not see any entries from itself"
    );

    // Agent Bravo writes a result
    let result_id = uuid::Uuid::new_v4();
    let result = MemoryEntry {
        id: result_id,
        ..make_agent_entry(
            "agent-bravo",
            "DB optimization result",
            "Added composite indexes on (user_id, created_at) and (status, org_id). \
             Query plans now use index-only scans. Latency down to 32ms p99.",
            MemoryCategory::Decision,
            vec!["peer-test".into(), session_tag.clone(), "result".into()],
            scope.clone(),
            MemoryLayer::L4,
        )
    };
    mgr.remember(result).await.unwrap();

    // Agent 3 (Reviewer): sees both agents
    mgr.set_active_agent("agent-charlie".into());

    let all_peer = mgr.recall(&session_tag, 10).await.unwrap();
    let seen_agents: std::collections::HashSet<String> = all_peer
        .iter()
        .filter_map(|e| e.source_agent.clone())
        .collect();

    eprintln!(
        "[Agent Charlie] Sees agents: {:?} across {} entries",
        seen_agents,
        all_peer.len()
    );
    assert!(seen_agents.contains("agent-alpha"), "Charlie sees Alpha");
    assert!(seen_agents.contains("agent-bravo"), "Charlie sees Bravo");
    // Charlie should NOT see their own entries yet (haven't written)
    assert!(
        !seen_agents.contains("agent-charlie"),
        "Charlie should not see own entries before writing"
    );

    // Charlie writes review
    let review_id = uuid::Uuid::new_v4();
    let review = MemoryEntry {
        id: review_id,
        ..make_agent_entry(
            "agent-charlie",
            "Review: DB optimization",
            "LGTM. Index choices are well-justified. Consider partial indexes \
             for active users only to reduce index size.",
            MemoryCategory::Decision,
            vec!["peer-test".into(), session_tag.clone(), "review".into()],
            scope.clone(),
            MemoryLayer::L4,
        )
    };
    mgr.remember(review).await.unwrap();

    // Agent Alpha (Planner): now sees complete peer picture
    mgr.set_active_agent("agent-alpha".into());

    let final_view = mgr.recall(&session_tag, 10).await.unwrap();
    eprintln!("[Agent Alpha] Final view: {} entries", final_view.len());

    // Verify complete 3-agent lifecycle visible to planner
    let has_bravo = final_view
        .iter()
        .any(|e| e.source_agent.as_deref() == Some("agent-bravo"));
    let has_charlie = final_view
        .iter()
        .any(|e| e.source_agent.as_deref() == Some("agent-charlie"));
    let has_alpha = final_view
        .iter()
        .any(|e| e.source_agent.as_deref() == Some("agent-alpha"));

    assert!(has_alpha, "Alpha should see own task");
    assert!(
        has_bravo,
        "Alpha should see Bravo's result (peer perception)"
    );
    assert!(
        has_charlie,
        "Alpha should see Charlie's review (peer perception)"
    );

    // Verify all entries share the same scope
    if !final_view.is_empty() {
        let first_scope = &final_view[0].scope;
        for entry in &final_view {
            assert_eq!(
                &entry.scope, first_scope,
                "All peer perception entries should share the same scope"
            );
        }
    }

    // Verify L4 layer consistency
    let l4_entries = mgr.list_layer_entries(MemoryLayer::L4).await.unwrap();
    assert!(l4_entries.len() >= 3);
    for meta in &l4_entries {
        assert_eq!(meta.layer, MemoryLayer::L4);
    }

    eprintln!("=== Peer perception test PASSED ===");
}

// ---------------------------------------------------------------------------
// E2E Test: Scope isolation — cross-scope boundaries are not breached
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_swarm_e2e_cross_scope_isolation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("swarm_scope_isolation.db");
    let config = test_config(&db_path);

    let mgr = CognitiveContextManager::new(config).await.unwrap();

    // Agent in Project A writes
    mgr.set_active_agent("agent-proj-a".into());
    let scope_a = MemoryScope::Project("project-a".into());

    for i in 0..3 {
        let entry = make_agent_entry(
            "agent-proj-a",
            &format!("proj-a-entry-{i}"),
            &format!("Project A internal knowledge item {i}"),
            MemoryCategory::ProjectConvention,
            vec!["scope-isolation".into(), "proj-a".into()],
            scope_a.clone(),
            MemoryLayer::L4,
        );
        mgr.remember(entry).await.unwrap();
    }

    // Agent in Project B writes
    mgr.set_active_agent("agent-proj-b".into());
    let scope_b = MemoryScope::Project("project-b".into());

    for i in 0..2 {
        let entry = make_agent_entry(
            "agent-proj-b",
            &format!("proj-b-entry-{i}"),
            &format!("Project B internal knowledge item {i}"),
            MemoryCategory::ProjectConvention,
            vec!["scope-isolation".into(), "proj-b".into()],
            scope_b.clone(),
            MemoryLayer::L4,
        );
        mgr.remember(entry).await.unwrap();
    }

    // Verify total: 5 entries across both scopes
    let all_l4 = mgr.list_layer_entries(MemoryLayer::L4).await.unwrap();
    assert_eq!(all_l4.len(), 5, "Expected 5 total entries in L4");

    // Query by project scope identifier in content
    let proj_a_results = mgr.recall("proj-a-entry", 10).await.unwrap();
    // proj_a_results should only contain entries from project A
    for entry in &proj_a_results {
        assert_eq!(
            entry.source_agent.as_deref(),
            Some("agent-proj-a"),
            "proj-a query should only return agent-proj-a entries, got {:?}",
            entry.source_agent
        );
    }

    let proj_b_results = mgr.recall("proj-b-entry", 10).await.unwrap();
    for entry in &proj_b_results {
        assert_eq!(
            entry.source_agent.as_deref(),
            Some("agent-proj-b"),
            "proj-b query should only return agent-proj-b entries"
        );
    }

    eprintln!(
        "Scope isolation: proj-a={} entries, proj-b={} entries",
        proj_a_results.len(),
        proj_b_results.len()
    );
    eprintln!("=== Cross-scope isolation test PASSED ===");
}
