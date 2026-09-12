#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::sync::Arc;

use chrono::Utc;
use harness_contract::reality::EvidenceRef;
use memory::compression::session::{
    CheckpointFactKind, CheckpointTokenStats, CompactionSourceRange, SessionCheckpointFact,
    SessionResumeCursor, SessionSemanticCheckpoint, SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION,
};
use memory::config::{BudgetConfig, StoreConfig};
use memory::{
    AgentVisibility, CognitiveContextManager, MemoryAtomView, MemoryCategory, MemoryConfig,
    MemoryHealth, MemoryInformationState, MemoryKernel, MemoryLayer, MemoryLayerView,
    MemoryLinkKind, MemoryPacketRole, MemoryPrimitive, MemoryScope, MemorySource, MemoryState,
    MemoryTurnContext, Priority,
};

fn test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
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

fn entry(layer: MemoryLayer, source: MemorySource, title: &str) -> memory::MemoryEntry {
    memory::MemoryEntry {
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
        .push(memory::MemoryDegradation::VectorUnavailable);
    assert!(health.is_degraded());
}

#[tokio::test]
async fn memory_kernel_binds_session_agent_scope() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("kernel.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-kernel", "agent-planner");

    let remembered = entry(MemoryLayer::L2, MemorySource::AutoExtracted, "turn-scoped");
    let remembered_id = remembered.id;
    kernel.remember(&ctx, remembered).await.unwrap();

    let prepared = kernel.prepare(&ctx, "anything", &[]).await.unwrap();

    let stored = manager
        .get_entry(&remembered_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.session_id.as_deref(), Some("session-kernel"));
    assert_eq!(stored.source_agent.as_deref(), Some("agent-planner"));
    assert_eq!(
        stored.scope,
        MemoryScope::Session("session-kernel".to_string())
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
async fn checkpoint_compaction_promotes_only_reviewed_candidates() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("checkpoint.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let mut ctx = MemoryTurnContext::new("session-checkpoint", "agent-primary");
    ctx.project_id = Some("project-checkpoint".to_string());
    ctx.task_id = Some("task-checkpoint".to_string());
    let mut existing = entry(
        MemoryLayer::L2,
        MemorySource::Compression,
        "Existing checkpoint decision",
    );
    existing.category = MemoryCategory::Decision;
    existing.content = "Existing checkpoint duplicate should be held".to_string();
    existing.scope = MemoryScope::Task("task-checkpoint".to_string());
    manager.remember(existing).await.unwrap();

    let evidence_ref = EvidenceRef::observed("session-message", "session-checkpoint:0")
        .with_source("source message");
    let checkpoint = SessionSemanticCheckpoint {
        schema_version: SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION,
        checkpoint_id: "checkpoint-review".to_string(),
        execution_identity: harness_contract::execution::ExecutionIdentity::for_session_turn(
            "agent-primary",
            "workspace-checkpoint",
            "session-checkpoint",
            "turn-checkpoint",
        )
        .unwrap(),
        session_id: "session-checkpoint".to_string(),
        agent_id: "agent-primary".to_string(),
        project_id: ctx.project_id.clone(),
        task_id: ctx.task_id.clone(),
        team_id: None,
        summary: "checkpoint summary".to_string(),
        user_rules: Vec::new(),
        goal: None,
        constraints: Vec::new(),
        decisions: Vec::new(),
        evidence_refs: Vec::new(),
        unresolved: Vec::new(),
        file_changes: Vec::new(),
        resume_cursor: SessionResumeCursor {
            message_index: 1,
            event_sequence: Some(1),
            checkpoint_id: "checkpoint-review".to_string(),
        },
        token_stats: CheckpointTokenStats {
            before: 100,
            after: 25,
            message_count: 3,
        },
        source_range: CompactionSourceRange {
            session_id: "session-checkpoint".to_string(),
            message_start: 0,
            message_end_exclusive: 1,
            event_start: Some(0),
            event_end_exclusive: Some(1),
            raw_refs: vec![evidence_ref.clone()],
        },
        facts: vec![
            SessionCheckpointFact {
                kind: CheckpointFactKind::Decision,
                title: "Duplicate decision".to_string(),
                content: "Existing checkpoint duplicate should be held".to_string(),
                category: MemoryCategory::Decision,
                layer: MemoryLayer::L2,
                tags: vec!["semantic-checkpoint".to_string()],
                confidence: 0.9,
                evidence_refs: vec![evidence_ref.clone()],
            },
            SessionCheckpointFact {
                kind: CheckpointFactKind::Decision,
                title: "Decision".to_string(),
                content: "Use fact review before checkpoint memory promotion".to_string(),
                category: MemoryCategory::Decision,
                layer: MemoryLayer::L2,
                tags: vec!["semantic-checkpoint".to_string()],
                confidence: 0.9,
                evidence_refs: vec![evidence_ref],
            },
            SessionCheckpointFact {
                kind: CheckpointFactKind::Preference,
                title: "Ungrounded preference".to_string(),
                content: "This candidate lacks evidence and must stay out of memory".to_string(),
                category: MemoryCategory::UserPreference,
                layer: MemoryLayer::L2,
                tags: vec!["semantic-checkpoint".to_string()],
                confidence: 0.9,
                evidence_refs: Vec::new(),
            },
        ],
    };

    let receipt = kernel
        .checkpoint_compaction(&ctx, checkpoint)
        .await
        .unwrap();

    assert_eq!(receipt.fact_review.promoted.len(), 1);
    assert_eq!(receipt.fact_review.held.len(), 2);
    assert_eq!(receipt.memory_ids.len(), 1);
    let entries = manager.list_all_entries().await.unwrap();
    assert!(entries
        .iter()
        .any(|entry| entry.content.contains("Use fact review")));
    assert!(!entries
        .iter()
        .any(|entry| entry.content.contains("lacks evidence")));
}

#[tokio::test]
async fn memory_kernel_health_is_visible_and_non_degraded_on_empty_store() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("health.db")))
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
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("views.db")))
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
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("remember.db")))
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
        MemoryScope::AgentInstance("agent-writer".to_string())
    );
}

#[tokio::test]
async fn memory_kernel_remember_replaces_default_project_scope() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("default-scope.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let mut ctx = MemoryTurnContext::new("session-project", "agent-shared");
    ctx.project_id = Some("project-beta".to_string());

    // Ordinary Runtime facts are scoped L3. L4 is reserved for the separate
    // governed Team promotion path and is intentionally not writable through
    // MemoryKernel.
    let mut shared_entry = entry(MemoryLayer::L3, MemorySource::Import, "shared decision");
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
async fn memory_kernel_records_lifecycle_events() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("lifecycle.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-life", "agent-life");
    let lifecycle_entry = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "lifecycle evidence",
    );
    let id = lifecycle_entry.id;

    kernel.remember(&ctx, lifecycle_entry).await.unwrap();
    kernel
        .transition_state(&ctx, id, MemoryState::Superseded, "newer evidence won")
        .await
        .unwrap();

    let events = kernel.lifecycle_events(id).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].to, MemoryState::Active);
    assert_eq!(events[1].from, Some(MemoryState::Active));
    assert_eq!(events[1].to, MemoryState::Superseded);
    assert_eq!(events[1].agent_id, "agent-life");
}

#[tokio::test]
async fn memory_kernel_layer_view_reflects_lifecycle_state() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("state-view.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-state", "agent-state");
    let state_entry = entry(MemoryLayer::L2, MemorySource::UserExplicit, "stateful rule");
    let id = state_entry.id;

    kernel.remember(&ctx, state_entry).await.unwrap();
    kernel
        .transition_state(&ctx, id, MemoryState::Archived, "retired decision")
        .await
        .unwrap();

    let view = kernel
        .layer_view(MemoryLayer::L2, MemoryInformationState::Orientation)
        .await
        .unwrap();

    assert_eq!(view.atoms[0].state, MemoryState::Archived);
}

#[tokio::test]
async fn superseded_atom_is_hidden_from_active_kernel_entries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("prepare-state.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-prepare-state", "agent-prepare");
    let mut active_entry = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "current recall marker",
    );
    active_entry.scope = MemoryScope::Session(ctx.session_id.clone());
    active_entry.session_id = Some(ctx.session_id.clone());
    let mut superseded_entry = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "old recall marker",
    );
    superseded_entry.scope = MemoryScope::Session(ctx.session_id.clone());
    superseded_entry.session_id = Some(ctx.session_id.clone());
    let superseded_id = superseded_entry.id;

    let active = active_entry.clone();
    let superseded = superseded_entry.clone();
    manager.remember(active_entry).await.unwrap();
    manager.remember(superseded_entry).await.unwrap();
    kernel
        .transition_state(
            &ctx,
            superseded_id,
            MemoryState::Superseded,
            "covered by current marker",
        )
        .await
        .unwrap();

    let active_entries = kernel.filter_active_entries(vec![active, superseded]).await;

    assert!(active_entries
        .iter()
        .any(|entry| entry.title == "current recall marker"));
    assert!(!active_entries
        .iter()
        .any(|entry| entry.title == "old recall marker"));
}

#[tokio::test]
async fn memory_links_unify_relation_session_agent_and_tag_edges() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("links.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-links", "agent-links");
    let target = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "target decision",
    );
    let target_id = target.id;
    let mut source = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "source summary",
    );
    source.relations.push(memory::types::Relation {
        target_id,
        kind: memory::types::RelationKind::Summarizes,
        strength: 0.9,
        temporal: None,
        entity: None,
    });
    source.tags.push("linked-topic".to_string());
    let mut peer = entry(MemoryLayer::L3, MemorySource::UserExplicit, "tag peer");
    peer.tags.push("linked-topic".to_string());

    kernel.remember(&ctx, target).await.unwrap();
    kernel.remember(&ctx, source).await.unwrap();
    kernel.remember(&ctx, peer).await.unwrap();

    let links = kernel.links().await.unwrap();

    assert!(links
        .iter()
        .any(|link| link.kind == MemoryLinkKind::Summarizes));
    assert!(links
        .iter()
        .any(|link| link.kind == MemoryLinkKind::BelongsTo));
    assert!(links
        .iter()
        .any(|link| link.kind == MemoryLinkKind::ProducedBy));
    assert!(links
        .iter()
        .any(|link| link.kind == MemoryLinkKind::Mentions));
}

#[tokio::test]
async fn path_recall_finds_related_decision() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("path.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-path", "agent-path");
    let decision = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "linked decision",
    );
    let decision_id = decision.id;
    let mut evidence = entry(MemoryLayer::L3, MemorySource::Import, "linked evidence");
    evidence.relations.push(memory::types::Relation {
        target_id: decision_id,
        kind: memory::types::RelationKind::DependsOn,
        strength: 0.8,
        temporal: None,
        entity: None,
    });
    let evidence_id = evidence.id;

    kernel.remember(&ctx, decision).await.unwrap();
    kernel.remember(&ctx, evidence).await.unwrap();

    let path = kernel.path_recall(evidence_id, 2, 8).await.unwrap();

    assert!(path
        .entries
        .iter()
        .any(|atom| atom.title == "linked decision"));
    assert!(path
        .links
        .iter()
        .any(|link| link.kind == MemoryLinkKind::DependsOn));
    assert!(!path.truncated);
}

#[tokio::test]
async fn path_recall_caps_expansion_on_dense_graph() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("dense-path.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-dense", "agent-dense");
    let mut first_id = None;
    for idx in 0..20 {
        let mut dense = entry(
            MemoryLayer::L3,
            MemorySource::UserExplicit,
            &format!("dense {idx}"),
        );
        dense.tags.push("dense-topic".to_string());
        first_id.get_or_insert(dense.id);
        kernel.remember(&ctx, dense).await.unwrap();
    }

    let path = kernel.path_recall(first_id.unwrap(), 4, 5).await.unwrap();

    assert!(path.entries.len() <= 5);
    assert!(path.truncated);
}

#[tokio::test]
async fn memory_context_packet_prefers_explainable_orientation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("packet.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-packet", "agent-packet");
    let mut orientation = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "PACKET_ORIENTATION_ALPHA",
    );
    orientation.content = "PACKET_ORIENTATION_ALPHA is the active project direction.".to_string();
    orientation.scope = MemoryScope::Session(ctx.session_id.clone());
    orientation.session_id = Some(ctx.session_id.clone());
    manager.remember_for_turn(&ctx, orientation).await.unwrap();

    let packet = kernel
        .context_packet(&ctx, "PACKET_ORIENTATION_ALPHA", &[], 8, 1_000)
        .await
        .unwrap();

    assert!(packet
        .selected
        .iter()
        .any(|item| item.role == MemoryPacketRole::Orientation));
    assert!(!packet.truncated);
}

#[tokio::test]
async fn memory_context_packet_includes_scoped_semantic_checkpoint() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(
            &tmp.path().join("checkpoint-packet.db"),
        ))
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-checkpoint", "agent-checkpoint");
    let mut checkpoint = entry(
        MemoryLayer::L3,
        MemorySource::Compression,
        "SEMANTIC_CHECKPOINT_VISIBLE",
    );
    checkpoint.category = MemoryCategory::CompressedSummary;
    checkpoint.content =
        "SEMANTIC_CHECKPOINT_VISIBLE carries the previous session decisions.".to_string();
    checkpoint.tags = vec!["semantic-checkpoint".to_string()];
    checkpoint.scope = MemoryScope::Session(ctx.session_id.clone());
    checkpoint.session_id = Some(ctx.session_id.clone());
    manager.remember_for_turn(&ctx, checkpoint).await.unwrap();

    let packet = kernel
        .context_packet(&ctx, "previous session decisions", &[], 8, 1_000)
        .await
        .unwrap();

    assert!(packet
        .selected
        .iter()
        .any(|item| item.atom.title == "SEMANTIC_CHECKPOINT_VISIBLE"));
}

#[tokio::test]
async fn memory_context_packet_omits_unrelated_semantic_checkpoint() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(
            &tmp.path().join("checkpoint-unrelated.db"),
        ))
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-checkpoint-unrelated", "agent-checkpoint");
    let mut checkpoint = entry(
        MemoryLayer::L3,
        MemorySource::Compression,
        "SUPPLY_CHAIN_CHECKPOINT",
    );
    checkpoint.category = MemoryCategory::CompressedSummary;
    checkpoint.content =
        "SUPPLY_CHAIN_CHECKPOINT contains supplier lead time decisions.".to_string();
    checkpoint.tags = vec!["semantic-checkpoint".to_string()];
    checkpoint.scope = MemoryScope::Session(ctx.session_id.clone());
    checkpoint.session_id = Some(ctx.session_id.clone());
    manager.remember_for_turn(&ctx, checkpoint).await.unwrap();

    let packet = kernel
        .context_packet(&ctx, "frontend color palette", &[], 8, 1_000)
        .await
        .unwrap();

    assert!(!packet
        .selected
        .iter()
        .any(|item| item.atom.title == "SUPPLY_CHAIN_CHECKPOINT"));
}

#[tokio::test]
async fn task_scoped_checkpoint_isolated_between_tasks() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("checkpoint-task.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx_a = MemoryTurnContext::new("session-task-scope", "agent-checkpoint")
        .with_task_id(Some("task-a".to_string()));
    let ctx_b = MemoryTurnContext::new("session-task-scope", "agent-checkpoint")
        .with_task_id(Some("task-b".to_string()));
    let mut checkpoint = entry(
        MemoryLayer::L3,
        MemorySource::Compression,
        "TASK_A_CHECKPOINT",
    );
    checkpoint.category = MemoryCategory::CompressedSummary;
    checkpoint.content = "TASK_A_CHECKPOINT records backend migration decisions.".to_string();
    checkpoint.tags = vec!["semantic-checkpoint".to_string()];
    checkpoint.scope = MemoryScope::Task("task-a".to_string());
    checkpoint.session_id = Some(ctx_a.session_id.clone());
    manager.remember_for_turn(&ctx_a, checkpoint).await.unwrap();

    let packet_a = kernel
        .context_packet(&ctx_a, "backend migration decisions", &[], 8, 1_000)
        .await
        .unwrap();
    let packet_b = kernel
        .context_packet(&ctx_b, "backend migration decisions", &[], 8, 1_000)
        .await
        .unwrap();

    assert!(packet_a
        .selected
        .iter()
        .any(|item| item.atom.title == "TASK_A_CHECKPOINT"));
    assert!(!packet_b
        .selected
        .iter()
        .any(|item| item.atom.title == "TASK_A_CHECKPOINT"));
}

#[tokio::test]
async fn memory_context_packet_stays_bounded_on_large_candidate_set() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("packet-bounded.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-packet-bounded", "agent-packet");
    let mut candidates = Vec::new();
    for idx in 0..12 {
        let mut candidate = entry(
            MemoryLayer::L3,
            MemorySource::UserExplicit,
            &format!("PACKET_BOUNDED_ALPHA {idx}"),
        );
        candidate.content = format!("PACKET_BOUNDED_ALPHA candidate {idx}");
        candidate.scope = MemoryScope::Session(ctx.session_id.clone());
        candidate.session_id = Some(ctx.session_id.clone());
        candidates.push(candidate.clone());
        manager.remember(candidate).await.unwrap();
    }

    let packet = kernel
        .context_packet_from_entries(candidates, 3, 10_000)
        .await
        .unwrap();

    assert!(packet.selected.len() <= 3);
    assert!(packet.truncated);
    assert!(!packet.omitted.is_empty());
}

#[tokio::test]
async fn runtime_managed_memory_packet_enforces_layer_budget() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = test_config(&tmp.path().join("layer-budget.db"));
    cfg.budget.runtime_managed = true;
    cfg.budget.l2_project = 12;
    cfg.budget.l3_deep = 1_000;
    cfg.budget.l3_checkpoint = 1_000;
    let manager = Arc::new(CognitiveContextManager::new_ephemeral(cfg).await.unwrap());
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let candidates = (0..3)
        .map(|idx| {
            let mut candidate = entry(
                MemoryLayer::L2,
                MemorySource::UserExplicit,
                &format!("L2_NOISE_{idx}"),
            );
            candidate.content = "x".repeat(24);
            candidate
        })
        .collect::<Vec<_>>();

    let packet = kernel
        .context_packet_from_entries(candidates, 8, 1_000)
        .await
        .unwrap();

    assert_eq!(packet.selected.len(), 2);
    assert_eq!(packet.omitted.len(), 1);
    assert!(packet
        .omitted
        .iter()
        .any(|item| item.reason.contains("layer L2 budget exhausted")));
}

#[tokio::test]
async fn long_memory_discovery_charges_preview_not_durable_source() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&temporary.path().join("memory")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(manager);
    let mut long = entry(MemoryLayer::L2, MemorySource::UserExplicit, "long source");
    long.content = "甲🙂乙".repeat(100_000);
    let expected_id = long.id;
    let packet = kernel
        .context_packet_from_entries(vec![long], 1, 1_000)
        .await
        .unwrap();
    assert_eq!(packet.selected.len(), 1);
    assert_eq!(packet.selected[0].atom.id, expected_id);
    assert!(packet.selected[0].content_preview.chars().count() <= 483);
    assert!(packet.token_estimate <= 1_000);
    assert!(packet.omitted.is_empty());
}

#[tokio::test]
async fn l0_requires_user_or_system_write() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("l0-guard.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-l0", "agent-inference");
    let forbidden = entry(
        MemoryLayer::L0,
        MemorySource::AutoExtracted,
        "forbidden identity",
    );
    let id = forbidden.id;

    kernel.remember(&ctx, forbidden).await.unwrap();

    assert!(manager.get_entry(&id.to_string()).await.unwrap().is_none());
}

#[tokio::test]
async fn archive_hides_memory_without_deleting_evidence() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("archive.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-archive", "agent-archive");
    let archived = entry(MemoryLayer::L3, MemorySource::UserExplicit, "archive me");
    let id = archived.id;
    let candidate = archived.clone();

    kernel.remember(&ctx, archived).await.unwrap();
    kernel
        .archive(&ctx, id, "user removed from active context")
        .await
        .unwrap();

    assert!(manager.get_entry(&id.to_string()).await.unwrap().is_some());
    assert!(kernel
        .filter_active_entries(vec![candidate])
        .await
        .is_empty());
    assert_eq!(
        kernel
            .lifecycle_events(id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Archived
    );
}

#[tokio::test]
async fn authoritative_memory_supersedes_old_fact() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("authority.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-authority", "agent-authority");
    let mut old = entry(MemoryLayer::L3, MemorySource::AutoExtracted, "runtime rule");
    old.content = "runtime rule is old".to_string();
    old.source_agent = Some("agent-authority".to_string());
    let old_id = old.id;
    let mut new = entry(MemoryLayer::L3, MemorySource::UserExplicit, "runtime rule");
    new.content = "runtime rule is new".to_string();
    let new_id = new.id;

    kernel.remember(&ctx, old).await.unwrap();
    kernel.remember(&ctx, new).await.unwrap();

    assert_eq!(
        kernel
            .lifecycle_events(old_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Superseded
    );
    assert_eq!(
        kernel
            .lifecycle_events(new_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Active
    );
}

#[tokio::test]
async fn equal_authority_conflict_is_visible_for_review() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("conflict.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-conflict", "agent-conflict");
    let mut first = entry(MemoryLayer::L3, MemorySource::UserExplicit, "conflict rule");
    first.content = "value A".to_string();
    let first_id = first.id;
    let mut second = entry(MemoryLayer::L3, MemorySource::UserExplicit, "conflict rule");
    second.content = "value B".to_string();
    let second_id = second.id;

    kernel.remember(&ctx, first).await.unwrap();
    kernel.remember(&ctx, second).await.unwrap();

    assert_eq!(
        kernel
            .lifecycle_events(first_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Conflicted
    );
    assert_eq!(
        kernel
            .lifecycle_events(second_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Conflicted
    );
}

#[tokio::test]
async fn duplicate_memory_write_is_not_persisted_again() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("duplicate.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-duplicate", "agent-duplicate");
    let first = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "duplicate rule",
    );
    let first_id = first.id;
    let mut second = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "duplicate rule",
    );
    second.content = first.content.clone();
    let second_id = second.id;

    kernel.remember(&ctx, first).await.unwrap();
    kernel.remember(&ctx, second).await.unwrap();

    let entries = manager.list_all_entries().await.unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries.iter().any(|entry| entry.id == first_id));
    assert!(!entries.iter().any(|entry| entry.id == second_id));
    assert_eq!(
        kernel
            .lifecycle_events(first_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Observed
    );
}

#[tokio::test]
async fn memory_runtime_clusters_large_documents_without_loading_full_body() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("clusters.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-cluster", "agent-cluster");
    for idx in 0..5 {
        let mut doc = entry(
            MemoryLayer::L3,
            MemorySource::Import,
            &format!("large doc {idx}"),
        );
        doc.content = "large document body ".repeat(600);
        doc.tags = vec!["large-docs".to_string()];
        kernel.remember(&ctx, doc).await.unwrap();
    }

    let clusters = kernel.clusters(4).await.unwrap();

    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0].entry_ids.len(), 5);
    assert!(clusters[0].summary.len() <= 960);
    assert!(clusters[0].truncated);
}

#[tokio::test]
async fn context_usage_feedback_updates_hot_summary_without_validating_memory() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("usage.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-usage", "agent-usage");
    let mut hot = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "HOT_MEMORY_ALPHA",
    );
    hot.content = "HOT_MEMORY_ALPHA should be selected repeatedly".to_string();
    hot.scope = MemoryScope::Session(ctx.session_id.clone());
    hot.session_id = Some(ctx.session_id.clone());
    let hot_id = hot.id;
    manager.remember_for_turn(&ctx, hot).await.unwrap();

    for _ in 0..3 {
        kernel
            .context_packet(&ctx, "HOT_MEMORY_ALPHA", &[], 4, 1_000)
            .await
            .unwrap();
    }

    let runtime = kernel.runtime_snapshot().await.unwrap();

    assert!(runtime.usage.hot_memory_ids.contains(&hot_id));
    assert!(
        kernel.lifecycle_events(hot_id).await.unwrap().is_empty(),
        "selection frequency is ranking evidence, not validation evidence"
    );
}

#[tokio::test]
async fn context_packet_preview_does_not_record_usage_or_validate_atoms() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("usage-preview.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-usage-preview", "agent-usage");
    let mut entry = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "PREVIEW_MEMORY_ALPHA",
    );
    entry.content =
        "PREVIEW_MEMORY_ALPHA should be visible but not recorded by preview".to_string();
    entry.scope = MemoryScope::Session(ctx.session_id.clone());
    entry.session_id = Some(ctx.session_id.clone());
    let memory_id = entry.id;
    manager.remember_for_turn(&ctx, entry).await.unwrap();

    let preview = kernel
        .context_packet_preview(&ctx, "PREVIEW_MEMORY_ALPHA", &[], 4, 1_000)
        .await
        .unwrap();
    assert!(preview
        .selected
        .iter()
        .any(|item| item.atom.id == memory_id));
    assert!(preview.selected.iter().any(|item| {
        item.atom.id == memory_id
            && item
                .content_preview
                .contains("PREVIEW_MEMORY_ALPHA should be visible")
    }));
    assert!(preview.recall_report.selected.iter().any(|candidate| {
        candidate.id == memory_id
            && candidate
                .content_preview
                .contains("PREVIEW_MEMORY_ALPHA should be visible")
    }));
    assert_eq!(kernel.usage_summary().await.unwrap().total_selected, 0);

    let recorded = kernel
        .context_packet(&ctx, "PREVIEW_MEMORY_ALPHA", &[], 4, 1_000)
        .await
        .unwrap();
    assert!(recorded
        .selected
        .iter()
        .any(|item| item.atom.id == memory_id));
    assert!(kernel.usage_summary().await.unwrap().total_selected > 0);
    let feedback = kernel
        .context_packet_preview(&ctx, "PREVIEW_MEMORY_ALPHA", &[], 4, 1_000)
        .await
        .unwrap();
    assert!(feedback.selected.iter().any(|item| {
        item.atom.id == memory_id && item.reason.contains("usage_feedback:selected_count=")
    }));
}

#[tokio::test]
async fn large_memory_context_packet_stays_bounded() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("large-packet.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(manager);
    let mut candidates = Vec::new();
    for idx in 0..500 {
        let mut candidate = entry(
            MemoryLayer::L3,
            MemorySource::UserExplicit,
            &format!("large candidate {idx}"),
        );
        candidate.content = "large packet content ".repeat(20);
        candidates.push(candidate);
    }

    let packet = kernel
        .context_packet_from_entries(candidates, 24, 512)
        .await
        .unwrap();

    assert!(packet.selected.len() <= 24);
    assert!(packet.token_estimate <= 512);
    assert!(packet.truncated);
    assert!(!packet.omitted.is_empty());
}

#[tokio::test]
async fn memory_kernel_post_turn_preserves_turn_success() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("post-turn.db")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx = MemoryTurnContext::new("session-post-turn", "agent-primary");
    let mut messages = vec![
        memory::types::Message::user("remember that Cowd memory must be explainable"),
        memory::types::Message::assistant("acknowledged"),
    ];

    let result = kernel.post_turn(&ctx, &mut messages).await;

    assert!(result.is_ok());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_kernel_turns_do_not_cross_write_or_recall_identity() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(
            &tmp.path().join("concurrent-turns.db"),
        ))
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let ctx_a = MemoryTurnContext::new("session-a", "agent-a");
    let ctx_b = MemoryTurnContext::new("session-b", "agent-b");

    let mut a_entry = entry(
        MemoryLayer::L2,
        MemorySource::AutoExtracted,
        "agent-a evidence",
    );
    a_entry.content = "agent-a private turn evidence".to_string();
    let a_id = a_entry.id;
    let mut b_entry = entry(
        MemoryLayer::L2,
        MemorySource::AutoExtracted,
        "agent-b evidence",
    );
    b_entry.content = "agent-b private turn evidence".to_string();
    let b_id = b_entry.id;

    let writer_a = {
        let kernel = kernel.clone();
        let ctx_a = ctx_a.clone();
        async move {
            tokio::task::yield_now().await;
            kernel.remember(&ctx_a, a_entry).await.unwrap();
        }
    };
    let writer_b = {
        let kernel = kernel.clone();
        async move {
            tokio::task::yield_now().await;
            kernel.remember(&ctx_b, b_entry).await.unwrap();
        }
    };
    tokio::join!(writer_a, writer_b);

    let a = manager.get_entry(&a_id.to_string()).await.unwrap().unwrap();
    let b = manager.get_entry(&b_id.to_string()).await.unwrap().unwrap();
    assert_eq!(a.session_id.as_deref(), Some("session-a"));
    assert_eq!(a.source_agent.as_deref(), Some("agent-a"));
    assert_eq!(a.scope, MemoryScope::Session("session-a".to_string()));
    assert_eq!(b.session_id.as_deref(), Some("session-b"));
    assert_eq!(b.source_agent.as_deref(), Some("agent-b"));
    assert_eq!(b.scope, MemoryScope::Session("session-b".to_string()));

    let visible_to_a = kernel.prepare(&ctx_a, "turn evidence", &[]).await.unwrap();
    assert!(visible_to_a.entries.iter().any(|item| item.id == a_id));
    assert!(!visible_to_a.entries.iter().any(|item| item.id == b_id));
}

#[tokio::test]
async fn memory_discovery_pages_all_matches_without_leaking_hidden_cursor_ids() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("discovery")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(manager.clone());
    let orchestrator = manager.orchestrator();
    let store = orchestrator.store();
    let ctx = MemoryTurnContext::new("catalog-session", "catalog-agent")
        .with_project_id(Some("catalog-project".into()))
        .with_cognitive_read_scopes(vec![
            harness_contract::agent::CognitiveReadScope::Project,
            harness_contract::agent::CognitiveReadScope::WorkspaceKnowledge,
        ]);
    let mut expected = std::collections::BTreeSet::new();
    let mut hidden_ids = Vec::new();
    for index in 0..140u128 {
        let mut visible = entry(MemoryLayer::L3, MemorySource::Import, "catalog needle");
        visible.id = uuid::Uuid::from_u128(index * 2 + 2);
        visible.scope = MemoryScope::Project("catalog-project".into());
        visible.source_agent = Some(ctx.agent_id.clone());
        visible.content = format!("needle {}", "中文".repeat(500));
        expected.insert(visible.id);
        store.insert(&visible).await.unwrap();
        let mut hidden = visible.clone();
        hidden.id = uuid::Uuid::from_u128(index * 2 + 1);
        hidden.scope = MemoryScope::Global;
        hidden.source = MemorySource::AutoExtracted;
        hidden.visibility = AgentVisibility::Private;
        hidden_ids.push(hidden.id.to_string());
        store.insert(&hidden).await.unwrap();
    }
    let first = kernel.discover_page(&ctx, "needle", None, 7).await.unwrap();
    let first_cursor = first.next_cursor.clone().unwrap();
    assert!(kernel
        .discover_page(&ctx, "other", Some(&first_cursor), 7)
        .await
        .is_err());
    let other = MemoryTurnContext::new("other-session", "other-agent");
    assert!(kernel
        .discover_page(&other, "needle", Some(&first_cursor), 7)
        .await
        .is_err());
    let mut cursor = None;
    let mut actual = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = kernel
            .discover_page(&ctx, "needle", cursor.as_deref(), 7)
            .await
            .unwrap();
        pages += 1;
        assert!(pages < 100);
        for item in page.selected {
            assert!(actual.insert(item.atom.id));
            assert_eq!(item.preview.chars().count(), 480);
        }
        cursor = page.next_cursor;
        let Some(value) = &cursor else {
            break;
        };
        assert!(
            hidden_ids.iter().all(|id| !value.contains(id)),
            "cursor must not expose private global IDs"
        );
    }
    assert_eq!(actual, expected);
    let id = *expected.first().unwrap();
    let mut saved = store.get(&id).await.unwrap().unwrap();
    saved.access_count += 1;
    saved.last_accessed_at = Some(Utc::now());
    store.update(&saved).await.unwrap();
    kernel
        .discover_page(&ctx, "needle", Some(&first_cursor), 7)
        .await
        .unwrap();
    let mut unrelated = saved.clone();
    unrelated.id = uuid::Uuid::new_v4();
    unrelated.scope = MemoryScope::Project("unrelated".into());
    store.insert(&unrelated).await.unwrap();
    kernel
        .discover_page(&ctx, "needle", Some(&first_cursor), 7)
        .await
        .unwrap();
    saved
        .content
        .push_str(" changed without changing timestamp");
    store.update(&saved).await.unwrap();
    assert!(kernel
        .discover_page(&ctx, "needle", Some(&first_cursor), 7)
        .await
        .unwrap_err()
        .to_string()
        .contains("source changed"));
    let fresh = kernel.discover_page(&ctx, "needle", None, 7).await.unwrap();
    kernel
        .transition_state(&ctx, id, MemoryState::Archived, "source retired")
        .await
        .unwrap();
    assert!(kernel
        .discover_page(&ctx, "needle", fresh.next_cursor.as_deref(), 7)
        .await
        .is_err());
    let active = kernel.discover_page(&ctx, "needle", None, 7).await.unwrap();
    assert!(active.selected.iter().all(|item| item.atom.id != id));
    let cursor = active.next_cursor.unwrap();
    store.delete(&id).await.unwrap();
    assert!(kernel
        .discover_page(&ctx, "needle", Some(&cursor), 7)
        .await
        .is_err());
}

#[tokio::test]
async fn private_writes_never_deduplicate_or_conflict_across_agents_in_one_project() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(test_config(&tmp.path().join("private-domain")))
            .await
            .unwrap(),
    );
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let a = MemoryTurnContext::new("private-domain-session", "private-agent-a")
        .with_project_id(Some("private-domain-project".into()));
    let b = MemoryTurnContext::new("private-domain-session", "private-agent-b")
        .with_project_id(Some("private-domain-project".into()));
    let mut first = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "private same key",
    );
    first.scope = MemoryScope::Project("private-domain-project".into());
    first.content = "ScopeSubject's parent is SecretA".into();
    first.confidence = 0.73;
    let first_id = first.id;
    kernel.remember(&a, first.clone()).await.unwrap();
    let mut second = first.clone();
    second.id = uuid::Uuid::new_v4();
    let second_id = second.id;
    kernel.remember(&b, second).await.unwrap();
    assert!(
        kernel
            .retrieve_visible_entry(&b, second_id)
            .await
            .unwrap()
            .is_some(),
        "another Agent's private duplicate cannot suppress this write"
    );
    let mut third = first;
    third.id = uuid::Uuid::new_v4();
    third.content = "ScopeSubject's parent is SecretB".into();
    let third_id = third.id;
    kernel.remember(&b, third).await.unwrap();
    assert_eq!(
        kernel
            .lifecycle_events(first_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Active
    );
    assert_eq!(
        kernel
            .lifecycle_events(second_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Conflicted
    );
    assert_eq!(
        kernel
            .lifecycle_events(third_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Conflicted
    );
    let own = kernel
        .retrieve_visible_entry(&a, first_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(own.confidence, 0.73);
    assert!(kernel
        .retrieve_visible_entry(&b, first_id)
        .await
        .unwrap()
        .is_none());
    // A separate title still cannot populate an unscoped regex fact registry
    // or change the confidence on the basis of another private statement.
    let mut unrelated = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "private unrelated key",
    );
    unrelated.content = "ScopeSubject's parent is OtherSecret".into();
    unrelated.confidence = 0.73;
    let unrelated_id = unrelated.id;
    kernel.remember(&b, unrelated).await.unwrap();
    assert_eq!(
        kernel
            .retrieve_visible_entry(&b, unrelated_id)
            .await
            .unwrap()
            .unwrap()
            .confidence,
        0.73
    );
}
