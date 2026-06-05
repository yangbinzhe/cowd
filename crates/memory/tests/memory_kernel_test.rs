use std::sync::Arc;

use chrono::Utc;
use cowd_memory::config::{BudgetConfig, StoreConfig};
use cowd_memory::{
    AgentVisibility, CognitiveContextManager, MemoryAtomView, MemoryCategory, MemoryConfig,
    MemoryHealth, MemoryInformationState, MemoryKernel, MemoryLayer, MemoryLayerView,
    MemoryPrimitive, MemoryScope, MemorySource, MemoryState, MemoryTurnContext, Priority,
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

fn entry(layer: MemoryLayer, source: MemorySource, title: &str) -> cowd_memory::MemoryEntry {
    cowd_memory::MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source,
        title: title.to_string(),
        content: format!("{title} content"),
        embedding: None,
        tags: vec!["kernel-test".to_string()],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::default(),
    }
}

#[test]
fn memory_primitives_cover_all_memory_views() {
    let primitives = MemoryPrimitive::all();
    assert_eq!(primitives.len(), 5);
    assert!(primitives.contains(&MemoryPrimitive::Atom));
    assert!(primitives.contains(&MemoryPrimitive::Evidence));
    assert!(primitives.contains(&MemoryPrimitive::Link));
    assert!(primitives.contains(&MemoryPrimitive::State));
    assert!(primitives.contains(&MemoryPrimitive::Recall));
}

#[test]
fn orientation_requires_evidence_or_explicit_authority() {
    let l0 = entry(MemoryLayer::L0, MemorySource::UserExplicit, "identity");
    let l0_view = MemoryAtomView::from_entry(&l0, MemoryInformationState::Orientation);
    assert!(l0_view.is_explainable_orientation());

    let l3 = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "deep evidence",
    );
    let l3_view = MemoryAtomView::from_entry(&l3, MemoryInformationState::Orientation);
    assert!(l3_view.is_explainable_orientation());

    let mut ungrounded = l3_view.clone();
    ungrounded.evidence_pointer = None;
    ungrounded.explicit_authority = false;
    assert!(!ungrounded.is_explainable_orientation());
}

#[test]
fn living_memory_pulse_preserves_evidence_immutability() {
    let mut view = MemoryAtomView::from_entry(
        &entry(MemoryLayer::L3, MemorySource::Import, "historical fact"),
        MemoryInformationState::Pattern,
    );
    let evidence = view.evidence_pointer.clone();

    view.state = MemoryState::Stale;
    view.confidence = 0.42;

    assert_eq!(view.evidence_pointer, evidence);
}

#[test]
fn layer_visibility_projection_is_read_only() {
    let atom = MemoryAtomView::from_entry(
        &entry(MemoryLayer::L2, MemorySource::AutoExtracted, "project rule"),
        MemoryInformationState::Orientation,
    );
    let view = MemoryLayerView::new(MemoryLayer::L2, vec![atom]);
    assert!(view.read_only);
    assert_eq!(view.layer, MemoryLayer::L2);
    assert_eq!(view.atoms.len(), 1);
}

#[test]
fn memory_health_reports_degradation_state() {
    let mut health = MemoryHealth::default();
    assert!(!health.is_degraded());
    health
        .degraded
        .push(cowd_memory::MemoryDegradation::VectorUnavailable);
    assert!(health.is_degraded());
}

#[tokio::test]
async fn memory_kernel_binds_session_agent_scope() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("kernel.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-kernel", "agent-planner");

    let prepared = kernel.prepare(&ctx, "anything", &[]).await.unwrap();

    assert_eq!(
        manager.active_session_id(),
        Some("session-kernel".to_string())
    );
    assert_eq!(
        prepared.total_tokens,
        prepared
            .entries
            .iter()
            .map(|e| e.content.len() as u64 / 4)
            .sum::<u64>()
    );
}

#[tokio::test]
async fn memory_kernel_health_is_visible_and_non_degraded_on_empty_store() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("health.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(manager);
    let ctx = MemoryTurnContext::new("session-health", "agent-primary");

    let health = kernel.health(&ctx).await.unwrap();

    assert!(!health.is_degraded());
    assert_eq!(health.orientation_pressure, 0.0);
    assert_eq!(health.evidence_coverage, 1.0);
}

#[tokio::test]
async fn memory_kernel_layer_views_project_atoms_read_only() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("views.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    manager
        .remember(entry(
            MemoryLayer::L2,
            MemorySource::UserExplicit,
            "project invariant",
        ))
        .await
        .unwrap();

    let l2 = kernel
        .layer_view(MemoryLayer::L2, MemoryInformationState::Orientation)
        .await
        .unwrap();
    let all = kernel
        .layer_views(MemoryInformationState::Orientation)
        .await
        .unwrap();

    assert!(l2.read_only);
    assert_eq!(l2.layer, MemoryLayer::L2);
    assert_eq!(l2.atoms.len(), 1);
    assert!(l2.atoms[0].is_explainable_orientation());
    assert_eq!(all.len(), 5);
    assert_eq!(
        all.iter()
            .find(|view| view.layer == MemoryLayer::L2)
            .unwrap()
            .atoms
            .len(),
        1
    );
}

#[tokio::test]
async fn memory_kernel_remember_binds_session_agent_and_scope() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("remember.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let mut ctx = MemoryTurnContext::new("session-write", "agent-writer");
    ctx.project_id = Some("project-alpha".to_string());

    let mut private_entry = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "private observation",
    );
    private_entry.scope = MemoryScope::Global;
    private_entry.visibility = AgentVisibility::Private;
    let private_id = private_entry.id;

    kernel.remember(&ctx, private_entry).await.unwrap();

    let stored_private = manager
        .get_entry(&private_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_private.session_id.as_deref(), Some("session-write"));
    assert_eq!(stored_private.source_agent.as_deref(), Some("agent-writer"));
    assert_eq!(
        stored_private.scope,
        MemoryScope::Agent("agent-writer".to_string())
    );
}

#[tokio::test]
async fn memory_kernel_remember_replaces_default_project_scope() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("default-scope.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let mut ctx = MemoryTurnContext::new("session-project", "agent-shared");
    ctx.project_id = Some("project-beta".to_string());

    let mut shared_entry = entry(MemoryLayer::L4, MemorySource::Import, "shared decision");
    shared_entry.scope = MemoryScope::Project("default".to_string());
    shared_entry.visibility = AgentVisibility::Shared;
    let shared_id = shared_entry.id;

    kernel.remember(&ctx, shared_entry).await.unwrap();

    let stored_shared = manager
        .get_entry(&shared_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored_shared.scope,
        MemoryScope::Project("project-beta".to_string())
    );
    assert_eq!(stored_shared.source_agent.as_deref(), Some("agent-shared"));
}

#[tokio::test]
async fn memory_kernel_post_turn_preserves_turn_success() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("post-turn.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-post-turn", "agent-primary");
    let mut messages = vec![
        cowd_memory::types::Message::user("remember that Cowd memory must be explainable"),
        cowd_memory::types::Message::assistant("acknowledged"),
    ];

    let result = kernel.post_turn(&ctx, &mut messages).await;

    assert!(result.is_ok());
    assert_eq!(
        manager.active_session_id(),
        Some("session-post-turn".to_string())
    );
}

#[tokio::test]
async fn concurrent_agents_do_not_share_memory_turn_context() {
    let ctx_a = MemoryTurnContext::new("session-a", "agent-a");
    let ctx_b = MemoryTurnContext::new("session-b", "agent-b");

    let cloned_a = ctx_a.clone();
    let cloned_b = ctx_b.clone();

    let (a, b) = tokio::join!(async move { cloned_a }, async move { cloned_b });

    assert_eq!(a.session_id, "session-a");
    assert_eq!(a.agent_id, "agent-a");
    assert_eq!(b.session_id, "session-b");
    assert_eq!(b.agent_id, "agent-b");
}
