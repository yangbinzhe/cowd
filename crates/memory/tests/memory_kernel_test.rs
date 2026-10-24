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

    let active_entries = kernel
        .filter_active_entries(vec![active, superseded])
        .await
        .unwrap();

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
        .unwrap()
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
    let revision = memory::store::MemoryEntryRevision::of(&saved).unwrap();
    saved.access_count += 1;
    saved.last_accessed_at = Some(Utc::now());
    store.update(&saved, &revision).await.unwrap();
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
    store.update(&saved, &revision).await.unwrap();
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
    kernel
        .erase(&ctx, id, "erase retired discovery target")
        .await
        .unwrap();
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

#[tokio::test]
async fn malformed_lifecycle_cannot_become_active_or_an_observed_projection() {
    use memory::store::{EphemeralMemoryStore, MemoryStore};
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&tmp.path().join("lifecycle-read.db")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager.clone());
    let ctx = MemoryTurnContext::new("lifecycle-session", "lifecycle-agent");
    let candidate = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "retained evidence",
    );
    store.insert(&candidate).await.unwrap();
    // Actual absence is valid; a failed decode must not be treated as absence.
    assert_eq!(
        kernel
            .filter_active_entries(vec![candidate.clone()])
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(kernel
        .lifecycle_events(candidate.id)
        .await
        .unwrap()
        .is_empty());
    let key = format!("memory_lifecycle:{}", candidate.id);
    store.kv_put(&key, "{broken lifecycle").await.unwrap();
    assert!(kernel
        .filter_active_entries(vec![candidate.clone()])
        .await
        .is_err());
    assert!(kernel.lifecycle_events(candidate.id).await.is_err());
    assert!(kernel
        .layer_view(MemoryLayer::L3, MemoryInformationState::Trace)
        .await
        .is_err());
    assert!(kernel
        .layer_views(MemoryInformationState::Trace)
        .await
        .is_err());
    assert!(kernel.runtime_snapshot().await.is_err());
    assert!(kernel.clusters(10).await.is_err());
    assert!(kernel.path_recall(candidate.id, 1, 10).await.is_err());
    assert!(kernel
        .context_packet_from_entries(vec![candidate.clone()], 10, 1000)
        .await
        .is_err());
    assert!(kernel
        .archive(&ctx, candidate.id, "must not overwrite unread history")
        .await
        .is_err());
    assert_eq!(
        store.kv_get(&key).await.unwrap().unwrap(),
        "{broken lifecycle"
    );
    assert!(manager
        .get_entry(&candidate.id.to_string())
        .await
        .unwrap()
        .is_some());
    // Restore an actual empty history, then exercise the real archive writer.
    store.kv_put(&key, "[]").await.unwrap();
    kernel
        .archive(&ctx, candidate.id, "user archive")
        .await
        .unwrap();
    assert!(kernel
        .filter_active_entries(vec![candidate.clone()])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        kernel
            .lifecycle_events(candidate.id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Archived
    );
}

#[tokio::test]
async fn archive_atomically_fences_knowledge_and_cleanup_survives_manager_reopen() {
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::store::{EphemeralMemoryStore, MemorySourceFenceReader, MemoryStore};
    use memory::KnowledgeStore;
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let knowledge_store =
        Arc::new(memory::InMemoryKnowledgeStore::new().with_memory_owner(store.clone()));
    let fabric = memory::KnowledgeFabric::with_store(knowledge_store.clone())
        .with_memory_sources(store.clone());
    let mut retired = entry(MemoryLayer::L3, MemorySource::Import, "retire rule");
    retired.scope = MemoryScope::Project("A".into());
    retired.visibility = AgentVisibility::Shared;
    let mut other = entry(MemoryLayer::L3, MemorySource::Import, "independent rule");
    other.scope = MemoryScope::Project("A".into());
    other.visibility = AgentVisibility::Shared;
    store.insert(&retired).await.unwrap();
    store.insert(&other).await.unwrap();
    let mut foreign_source = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        "foreign original source",
    );
    foreign_source.scope = MemoryScope::Project("foreign-project".into());
    foreign_source.visibility = AgentVisibility::Shared;
    store.insert(&foreign_source).await.unwrap();
    let foreign_scope = foreign_source.scope.scope_key();
    let ingest = |source: &memory::MemoryEntry, title: &str| {
        let mut document = memory::DocumentContent::new(title, "Retain the original evidence.");
        document.source = Some(format!("memory:{}", source.id));
        document.memory_source_revision =
            Some(memory::store::MemoryEntryRevision::of(&source).unwrap());
        fabric.ingest_document(
            KnowledgeNamespace::Project("A".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document,
        )
    };
    let retired_receipt = ingest(&retired, "retired rule").unwrap();
    let other_receipt = ingest(&other, "independent rule").unwrap();
    assert_eq!(
        fabric
            .search_namespace(&KnowledgeNamespace::Project("A".into()), "")
            .unwrap()
            .packs
            .len(),
        2
    );
    let entity = |name: &str, sources: Vec<String>| memory::entity::Entity {
        id: name.into(),
        name: name.into(),
        entity_type: memory::entity::EntityType::Concept,
        confidence: 1.0,
        frequency: 1,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        source_ids: sources.clone(),
        source_type: if sources.is_empty() {
            "independent"
        } else {
            "memory"
        }
        .into(),
        memory_source_revisions: sources
            .iter()
            .map(|id| {
                let source = [&retired, &other, &foreign_source]
                    .into_iter()
                    .find(|source| source.id.to_string() == *id)
                    .unwrap();
                memory::store::MemoryEntryRevision::of(source).unwrap()
            })
            .collect(),
    };
    let only = entity("same-scoped-entity", vec![retired.id.to_string()]);
    let multi = entity(
        "multi-source-entity",
        vec![retired.id.to_string(), other.id.to_string()],
    );
    let empty = entity("independent-empty-entity", vec![]);
    let scope = retired.scope.scope_key();
    store
        .save_entities(&scope, &[only.clone(), multi, empty])
        .await
        .unwrap();
    assert!(store
        .save_entities(&foreign_scope, &[only.clone()])
        .await
        .is_err());
    let mut foreign_entity = only.clone();
    foreign_entity.source_ids = vec![foreign_source.id.to_string()];
    foreign_entity.memory_source_revisions =
        vec![memory::store::MemoryEntryRevision::of(&foreign_source).unwrap()];
    store
        .save_entities(&foreign_scope, &[foreign_entity])
        .await
        .unwrap();
    let triple = memory::entity::Triple {
        id: "same-scoped-triple".into(),
        subject_id: "same-scoped-entity".into(),
        predicate: "retain".into(),
        object_id: "independent-empty-entity".into(),
        valid_from: None,
        valid_to: None,
        source: Some(format!("memory:{}", retired.id)),
        confidence: 1.0,
        created_at: Utc::now(),
        source_agent: None,
        memory_source_revision: Some(memory::store::MemoryEntryRevision::of(&retired).unwrap()),
    };
    store.save_triples(&scope, &[triple.clone()]).await.unwrap();
    assert!(store
        .save_triples(&foreign_scope, &[triple.clone()])
        .await
        .is_err());
    let mut foreign_triple = triple.clone();
    foreign_triple.source = Some(format!("memory:{}", foreign_source.id));
    foreign_triple.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&foreign_source).unwrap());
    store
        .save_triples(&foreign_scope, &[foreign_triple])
        .await
        .unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("first")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let weak = Arc::downgrade(&manager);
    let kernel = MemoryKernel::new(manager.clone());
    let ctx = MemoryTurnContext::new("archive-command", "operator");
    kernel
        .archive(&ctx, retired.id, "user archive")
        .await
        .unwrap();
    let pending = store.pending_memory_cleanup(None, 16).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].source.delete_epoch, 1);
    assert!(pending[0].knowledge_receipt_digest.is_none());
    assert!(!pending[0].auxiliary_confirmed);
    assert!(
        store
            .read_source_fence(&format!("memory:{}", retired.id))
            .unwrap()
            .unwrap()
            .retired
    );
    // Before quarantine has run, original Knowledge rows still contain both
    // packs, but its real read path must omit only the retired source.
    assert_eq!(fabric.snapshot().unwrap().packs.len(), 2);
    let visible = fabric
        .search_namespace(&KnowledgeNamespace::Project("A".into()), "")
        .unwrap();
    assert!(visible
        .packs
        .iter()
        .all(|p| p.pack_id != retired_receipt.pack.pack_id));
    assert!(visible
        .packs
        .iter()
        .any(|p| p.pack_id == other_receipt.pack.pack_id));
    assert!(ingest(&retired, "different pack same retired source").is_err());
    let mut wrong_scope = memory::DocumentContent::new("foreign", "must not import");
    wrong_scope.source = Some(format!("memory:{}", other.id));
    wrong_scope.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&other).unwrap());
    assert!(fabric
        .ingest_document(
            KnowledgeNamespace::Project("B".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            wrong_scope
        )
        .is_err());
    assert!(store
        .update(
            &retired,
            &memory::store::MemoryEntryRevision::of(&retired).unwrap()
        )
        .await
        .is_err());
    assert!(store.insert(&retired).await.is_err());
    let mut forged = pending[0].clone();
    forged.source.delete_epoch += 1;
    assert!(store
        .confirm_memory_cleanup(&forged, None, true)
        .await
        .is_err());
    // Knowledge committed, but Memory has not acknowledged it yet. A wrong
    // epoch/scope receipt cannot stand in for this exact cross-domain fact.
    let quarantine = fabric.quarantine_memory_source(&pending[0].source).unwrap();
    assert_eq!(quarantine.source, pending[0].source);
    assert_eq!(
        quarantine.pack_ids,
        vec![retired_receipt.pack.pack_id.clone()]
    );
    let mut wrong_receipt = quarantine.clone();
    wrong_receipt.source.delete_epoch += 1;
    assert!(store
        .confirm_memory_cleanup(&pending[0], Some(&wrong_receipt), false)
        .await
        .is_err());
    assert_eq!(
        store.pending_memory_cleanup(None, 16).await.unwrap(),
        pending
    );
    wrong_receipt.source = quarantine.source.clone();
    wrong_receipt.source.scope_key = MemoryScope::Project("B".into()).scope_key();
    assert!(store
        .confirm_memory_cleanup(&pending[0], Some(&wrong_receipt), false)
        .await
        .is_err());
    assert!(
        fabric
            .quarantine_source(&pending[0].source.source_ref)
            .is_err(),
        "a bare Memory source string is not an epoch proof"
    );
    // Model a valid precomputed artifact delayed until after retirement. The
    // native store must reject it even if a caller bypasses Fabric preflight.
    let mut late = retired_receipt.clone();
    late.corpus.corpus_id = "late-corpus-after-retirement".into();
    late.pack.pack_id = "late-pack-after-retirement".into();
    late.pack.source_corpus_refs = vec![late.corpus.corpus_id.clone()];
    late.canon.canon_id = "late-canon-after-retirement".into();
    late.canon.pack_id = late.pack.pack_id.clone();
    late.pack.canon_pack_ref = Some(late.canon.canon_id.clone());
    late.chunks.clear();
    late.corpus.chunk_count = 0;
    late.conflicts.clear();
    assert!(matches!(
        knowledge_store.save_receipt(&late),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        knowledge_store.save_receipt(&retired_receipt).unwrap().pack,
        retired_receipt.pack,
        "immutable receipt replay returns its original fact, never reactivates materialization"
    );
    let mut revive = fabric
        .snapshot()
        .unwrap()
        .packs
        .into_iter()
        .find(|p| p.pack_id == retired_receipt.pack.pack_id)
        .unwrap();
    revive.state = harness_contract::knowledge::KnowledgeObjectState::Active;
    revive.source_corpus_refs = vec![other_receipt.corpus.corpus_id.clone()];
    assert!(matches!(
        knowledge_store.commit_maintenance(
            &knowledge_store.maintenance_inputs().unwrap(),
            &[revive],
            &[]
        ),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    drop(kernel);
    drop(manager);
    assert!(weak.upgrade().is_none());
    let reopened = CognitiveContextManager::new_with_selected_store(
        test_config(&root.path().join("second")),
        None,
        store.clone(),
    )
    .await
    .unwrap();
    assert!(reopened
        .recover_memory_cleanup(&fabric, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    let entities = store.load_entities(&scope).await.unwrap();
    assert!(!entities.iter().any(|e| e.id == "same-scoped-entity"));
    assert_eq!(
        entities
            .iter()
            .find(|e| e.id == "multi-source-entity")
            .unwrap()
            .source_ids,
        vec![other.id.to_string()]
    );
    assert!(entities.iter().any(|e| e.id == "independent-empty-entity"));
    assert_eq!(
        store.load_entities(&foreign_scope).await.unwrap()[0].id,
        "same-scoped-entity"
    );
    assert!(store.load_triples(&scope).await.unwrap().is_empty());
    assert_eq!(
        store.load_triples(&foreign_scope).await.unwrap()[0].id,
        "same-scoped-triple"
    );
    assert!(
        store.get(&retired.id).await.unwrap().is_some(),
        "archive retains evidence; it is not erase"
    );
    // Original command replay is a receipt lookup, not a new epoch or pending.
    MemoryKernel::new(Arc::new(reopened))
        .archive(&ctx, retired.id, "user archive")
        .await
        .unwrap();
    assert_eq!(
        store
            .read_source_fence(&format!("memory:{}", retired.id))
            .unwrap()
            .unwrap()
            .delete_epoch,
        1
    );
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn lifecycle_transaction_rejects_a_changed_body_and_foreign_receipt_identity() {
    use memory::store::{EphemeralMemoryStore, MemoryLifecycleMutation, MemoryStore};
    let store = EphemeralMemoryStore::new();
    let original = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "original source",
    );
    store.insert(&original).await.unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: original.id,
        from: None,
        to: MemoryState::Archived,
        reason: "same command".into(),
        session_id: "s".into(),
        agent_id: "a".into(),
        occurred_at: Utc::now(),
    };
    let stale = MemoryLifecycleMutation::new(&original, None, event.clone()).unwrap();
    let mut changed = original.clone();
    changed.content = "actual concurrent body change".into();
    store
        .update(
            &changed,
            &memory::store::MemoryEntryRevision::of(&original).unwrap(),
        )
        .await
        .unwrap();
    assert!(store.commit_lifecycle(stale).await.is_err());
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    let command = MemoryLifecycleMutation::new(&changed, None, event).unwrap();
    let first = store.commit_lifecycle(command.clone()).await.unwrap();
    assert_eq!(
        store.commit_lifecycle(command.clone()).await.unwrap(),
        first
    );
    let mut forged = command;
    forged.event.agent_id = "foreign".into();
    assert!(store.commit_lifecycle(forged).await.is_err());
    assert_eq!(
        store.pending_memory_cleanup(None, 16).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn legacy_archived_snapshot_cannot_be_reactivated_without_a_new_fence_row() {
    use memory::store::{
        EphemeralMemoryStore, MemoryLifecycleMutation, MemorySourceFenceReader, MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let original = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        "retained legacy snapshot",
    );
    store.insert(&original).await.unwrap();
    let archive = memory::MemoryLifecycleEvent {
        memory_id: original.id,
        from: None,
        to: MemoryState::Archived,
        reason: "historical archive".into(),
        session_id: "original-session".into(),
        agent_id: "original-agent".into(),
        occurred_at: Utc::now(),
    };
    let raw = serde_json::to_string(&vec![archive.clone()]).unwrap();
    store
        .kv_put(&format!("memory_lifecycle:{}", original.id), &raw)
        .await
        .unwrap();
    assert!(
        store
            .read_source_fence(&format!("memory:{}", original.id))
            .unwrap()
            .unwrap()
            .retired
    );
    assert!(store
        .update(
            &original,
            &memory::store::MemoryEntryRevision::of(&original).unwrap()
        )
        .await
        .is_err());
    assert!(store.insert(&original).await.is_err());
    let mut observed = archive;
    observed.from = Some(MemoryState::Archived);
    observed.to = MemoryState::Observed;
    let command = MemoryLifecycleMutation::new(&original, Some(raw.clone()), observed).unwrap();
    assert!(store.commit_lifecycle(command).await.is_err());
    assert_eq!(
        store
            .kv_get(&format!("memory_lifecycle:{}", original.id))
            .await
            .unwrap()
            .unwrap(),
        raw
    );
}

#[tokio::test]
async fn knowledge_required_source_read_failure_is_not_empty_success_or_foreign_failure() {
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::store::{
        EphemeralMemoryStore, MemorySourceFence, MemorySourceFenceReader, MemoryStore,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Reader {
        store: Arc<EphemeralMemoryStore>,
        fail: AtomicBool,
        source: String,
    }
    impl MemorySourceFenceReader for Reader {
        fn read_source_revision(
            &self,
            source: &str,
            budget: usize,
        ) -> memory::store::Result<Option<memory::store::MemorySourceRevisionRead>> {
            if source == self.source && self.fail.load(Ordering::SeqCst) {
                return Err(memory::MemoryError::Store(
                    "injected source read failure".into(),
                ));
            }
            self.store.read_source_revision(source, budget)
        }

        fn read_source_fence(
            &self,
            source: &str,
        ) -> memory::store::Result<Option<MemorySourceFence>> {
            if source == self.source && self.fail.load(Ordering::SeqCst) {
                return Err(memory::MemoryError::Store(
                    "injected source read failure".into(),
                ));
            }
            self.store.read_source_fence(source)
        }
    }
    let store = Arc::new(EphemeralMemoryStore::new());
    let mut own = entry(MemoryLayer::L3, MemorySource::Import, "required rule");
    own.scope = MemoryScope::Project("A".into());
    own.visibility = memory::AgentVisibility::Shared;
    let mut independent = entry(MemoryLayer::L3, MemorySource::Import, "independent rule");
    independent.scope = own.scope.clone();
    independent.visibility = memory::AgentVisibility::Shared;
    let mut foreign = entry(MemoryLayer::L3, MemorySource::Import, "foreign rule");
    foreign.scope = MemoryScope::Project("B".into());
    foreign.visibility = memory::AgentVisibility::Shared;
    let reader = Arc::new(Reader {
        store: store.clone(),
        fail: AtomicBool::new(false),
        source: format!("memory:{}", own.id),
    });
    let fabric = memory::KnowledgeFabric::with_store(Arc::new(
        memory::InMemoryKnowledgeStore::new().with_memory_owner(store.clone()),
    ))
    .with_memory_sources(reader.clone());
    let mut ids = Vec::new();
    for (item, project, governance) in [
        (&own, "A", KnowledgeGovernanceLevel::Blocking),
        (&independent, "A", KnowledgeGovernanceLevel::Required),
        (&foreign, "B", KnowledgeGovernanceLevel::Required),
    ] {
        store.insert(item).await.unwrap();
        let mut document = memory::DocumentContent::new(&item.title, "must retain evidence");
        document.source = Some(format!("memory:{}", item.id));
        document.memory_source_revision =
            Some(memory::store::MemoryEntryRevision::of(&item).unwrap());
        ids.push(
            fabric
                .ingest_document(
                    KnowledgeNamespace::Project(project.into()),
                    KnowledgeActivationPolicy::DefaultForProjectGroup,
                    governance,
                    document,
                )
                .unwrap()
                .pack
                .pack_id,
        );
    }
    let before = fabric.snapshot().unwrap().packs.len();
    reader.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        fabric.search_namespace(&KnowledgeNamespace::Project("A".into()), ""),
        Err(memory::KnowledgeStoreError::MemorySourceRead(_))
    ));
    assert!(matches!(
        fabric.activate("s", "rule", "main", Some("A")),
        Err(memory::KnowledgeStoreError::MemorySourceRead(_))
    ));
    assert_eq!(
        fabric
            .search_namespace(&KnowledgeNamespace::Project("B".into()), "")
            .unwrap()
            .packs[0]
            .pack_id,
        ids[2]
    );
    assert!(fabric
        .activate("s", "rule", "main", Some("B"))
        .unwrap()
        .0
        .active_pack_ids
        .contains(&ids[2]));
    let mut rejected = memory::DocumentContent::new("must not persist", "must retain evidence");
    rejected.source = Some(reader.source.clone());
    rejected.memory_source_revision = Some(memory::store::MemoryEntryRevision::of(&own).unwrap());
    assert!(matches!(
        fabric.ingest_document(
            KnowledgeNamespace::Project("A".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            rejected,
        ),
        Err(memory::KnowledgeStoreError::MemorySourceRead(_))
    ));
    assert_eq!(fabric.snapshot().unwrap().packs.len(), before);
    reader.fail.store(false, Ordering::SeqCst);
    assert_eq!(
        fabric
            .search_namespace(&KnowledgeNamespace::Project("A".into()), "")
            .unwrap()
            .packs
            .len(),
        2
    );
}

#[tokio::test]
async fn later_retirement_epoch_preserves_earlier_pending_and_exact_receipts() {
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::store::{EphemeralMemoryStore, MemorySourceFenceReader, MemoryStore};
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let knowledge =
        Arc::new(memory::InMemoryKnowledgeStore::new().with_memory_owner(store.clone()));
    let fabric = memory::KnowledgeFabric::with_store(knowledge).with_memory_sources(store.clone());
    let mut body = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        "two retirement epochs",
    );
    body.scope = MemoryScope::Project("A".into());
    store.insert(&body).await.unwrap();
    let mut document = memory::DocumentContent::new("source policy", "retain original evidence");
    document.source = Some(format!("memory:{}", body.id));
    document.memory_source_revision = Some(memory::store::MemoryEntryRevision::of(&body).unwrap());
    let original = fabric
        .ingest_document(
            KnowledgeNamespace::Project("A".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document,
        )
        .unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("manager")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager.clone());
    let ctx = MemoryTurnContext::new("epoch-owner", "operator");
    kernel
        .archive(&ctx, body.id, "first retirement")
        .await
        .unwrap();
    kernel
        .archive(&ctx, body.id, "second retirement")
        .await
        .unwrap();
    let pending = store.pending_memory_cleanup(None, 16).await.unwrap();
    assert_eq!(pending.len(), 2);
    let first = pending.iter().find(|p| p.source.delete_epoch == 1).unwrap();
    let second = pending.iter().find(|p| p.source.delete_epoch == 2).unwrap();
    assert_eq!(
        store
            .read_source_fence(&first.source.source_ref)
            .unwrap()
            .unwrap()
            .delete_epoch,
        2
    );
    let receipt1 = fabric.quarantine_memory_source(&first.source).unwrap();
    let receipt2 = fabric.quarantine_memory_source(&second.source).unwrap();
    assert_eq!(receipt1.source, first.source);
    assert_eq!(receipt2.source, second.source);
    assert_eq!(receipt1.pack_ids, vec![original.pack.pack_id]);
    assert!(receipt2.pack_ids.is_empty());
    assert_eq!(
        fabric.quarantine_memory_source(&first.source).unwrap(),
        receipt1
    );
    assert!(store
        .confirm_memory_cleanup(first, Some(&receipt2), false)
        .await
        .is_err());
    assert_eq!(
        store.pending_memory_cleanup(None, 16).await.unwrap(),
        pending
    );
    for bad in [
        memory::store::MemorySourceFence {
            delete_epoch: 0,
            ..first.source.clone()
        },
        memory::store::MemorySourceFence {
            delete_epoch: 3,
            ..first.source.clone()
        },
        memory::store::MemorySourceFence {
            scope_key: MemoryScope::Project("foreign".into()).scope_key(),
            ..first.source.clone()
        },
        memory::store::MemorySourceFence {
            source_ref: format!("memory:{}", uuid::Uuid::new_v4()),
            ..first.source.clone()
        },
    ] {
        assert!(fabric.quarantine_memory_source(&bad).is_err());
    }
    assert!(manager
        .recover_memory_cleanup(&fabric, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(
        store.get(&body.id).await.unwrap().is_some(),
        "archive is not erase"
    );
}

#[tokio::test]
async fn lifecycle_receipt_binds_original_cut_and_distinguishes_later_observations() {
    use memory::store::{EphemeralMemoryStore, MemoryStore};
    let store = EphemeralMemoryStore::new();
    let original = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "original receipt source",
    );
    store.insert(&original).await.unwrap();
    let first_event = memory::MemoryLifecycleEvent {
        memory_id: original.id,
        from: None,
        to: memory::MemoryState::Observed,
        reason: "accepted source observation".into(),
        session_id: "receipt-session".into(),
        agent_id: "receipt-agent".into(),
        occurred_at: chrono::Utc::now(),
    };
    let first =
        memory::store::MemoryLifecycleMutation::new(&original, None, first_event.clone()).unwrap();
    let receipt = store.commit_lifecycle(first.clone()).await.unwrap();
    let history_key = format!("memory_lifecycle:{}", original.id);
    let history = store.kv_get(&history_key).await.unwrap().unwrap();
    for field in [
        "body", "history", "from", "scope", "actor", "memory", "state", "reason",
    ] {
        let mut forged = first.clone();
        match field {
            "body" => forged.expected_entry_digest = "foreign-body".into(),
            "history" => forged.expected_history = Some("[]".into()),
            "from" => forged.event.from = Some(memory::MemoryState::Active),
            "scope" => forged.scope_key = "foreign-scope".into(),
            "actor" => forged.event.agent_id = "foreign-agent".into(),
            "memory" => forged.event.memory_id = uuid::Uuid::new_v4(),
            "state" => forged.event.to = memory::MemoryState::Active,
            "reason" => forged.event.reason = "foreign reason".into(),
            _ => unreachable!(),
        }
        assert!(
            store.commit_lifecycle(forged).await.is_err(),
            "replay must bind original {field}"
        );
        assert_eq!(
            store.kv_get(&history_key).await.unwrap().as_deref(),
            Some(history.as_str())
        );
    }
    let mut second_event = first_event.clone();
    second_event.from = Some(memory::MemoryState::Observed);
    second_event.occurred_at += chrono::Duration::seconds(1);
    let second =
        memory::store::MemoryLifecycleMutation::new(&original, Some(history), second_event.clone())
            .unwrap();
    let second_receipt = store.commit_lifecycle(second).await.unwrap();
    assert_eq!(second_receipt.event, second_event);
    let mut third_event = second_event.clone();
    third_event.occurred_at += chrono::Duration::seconds(1);
    let third = memory::store::MemoryLifecycleMutation::new(
        &original,
        store.kv_get(&history_key).await.unwrap(),
        third_event.clone(),
    )
    .unwrap();
    assert_eq!(
        store.commit_lifecycle(third).await.unwrap().event,
        third_event
    );
    assert_eq!(
        store.commit_lifecycle(first.clone()).await.unwrap(),
        receipt
    );
    let events: Vec<memory::MemoryLifecycleEvent> =
        serde_json::from_str(&store.kv_get(&history_key).await.unwrap().unwrap()).unwrap();
    assert_eq!(events, vec![first_event, second_event, third_event]);
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn knowledge_source_cut_survives_observation_but_refuses_real_replacement() {
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::store::{EphemeralMemoryStore, MemorySourceFenceReader, MemoryStore};
    use memory::{DocumentContent, KnowledgeStore};
    let memory_store = Arc::new(EphemeralMemoryStore::new());
    let knowledge =
        Arc::new(memory::InMemoryKnowledgeStore::new().with_memory_owner(memory_store.clone()));
    let fabric = memory::KnowledgeFabric::with_store(knowledge.clone())
        .with_memory_sources(memory_store.clone());
    let mut original = entry(MemoryLayer::L3, MemorySource::Import, "original source cut");
    original.scope = MemoryScope::Project("snapshot-test".into());
    memory_store.insert(&original).await.unwrap();
    let source_ref = format!("memory:{}", original.id);
    let original_cut = memory::store::MemoryEntryRevision::of(&original).unwrap();
    let mut document =
        DocumentContent::new("source-backed policy", "must retain the original evidence");
    document.source = Some(source_ref.clone());
    document.memory_source_revision = Some(original_cut.clone());
    let first = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document.clone(),
        )
        .unwrap();
    let mut observed = original.clone();
    observed.access_count = 10;
    memory_store.update(&observed, &original_cut).await.unwrap();
    assert!(fabric
        .search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), "")
        .is_ok());
    let prepared = |suffix: &str| {
        let mut receipt = first.clone();
        receipt.corpus.corpus_id = format!("prepared-corpus-{suffix}");
        receipt.pack.pack_id = format!("prepared-pack-{suffix}");
        receipt.pack.source_corpus_refs = vec![receipt.corpus.corpus_id.clone()];
        receipt.canon.canon_id = format!("prepared-canon-{suffix}");
        receipt.canon.pack_id = receipt.pack.pack_id.clone();
        receipt.pack.canon_pack_ref = Some(receipt.canon.canon_id.clone());
        receipt.chunks.clear();
        receipt.corpus.chunk_count = 0;
        receipt.conflicts.clear();
        receipt
    };
    let after_observation = prepared("observation");
    knowledge.save_receipt(&after_observation).unwrap();
    let mut correction = original.clone();
    correction.content = "actual replacement of the authoritative source".into();
    memory_store
        .update(&correction, &original_cut)
        .await
        .unwrap();
    let before = serde_json::to_value(knowledge.snapshot().unwrap()).unwrap();
    assert!(matches!(
        knowledge.save_receipt(&prepared("stale-body")),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before,
        "refusal must commit no corpus/pack/canon/receipt projection"
    );
    assert!(matches!(
        fabric.ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document
        ),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert!(matches!(
        fabric.search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), ""),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        knowledge.save_receipt(&first).unwrap().corpus,
        first.corpus,
        "an immutable old receipt remains its original fact"
    );
    let mut fresh =
        DocumentContent::new("source-backed policy", "must retain the corrected evidence");
    fresh.source = Some(source_ref.clone());
    fresh.memory_source_revision =
        Some(memory::store::MemoryEntryRevision::of(&correction).unwrap());
    let current = fabric
        .ingest_document(
            KnowledgeNamespace::Project("snapshot-test".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            fresh,
        )
        .unwrap();
    assert_ne!(
        current.corpus.corpus_id, first.corpus.corpus_id,
        "a different source cut cannot rebind the original corpus identity"
    );
    assert!(matches!(
        memory_store.read_source_revision(&source_ref, 1),
        Err(memory::MemoryError::SourceReadLimit)
    ));
    let mut detached_ingestion = current.clone();
    detached_ingestion.pack.source_corpus_refs.clear();
    let before_detached = serde_json::to_value(knowledge.snapshot().unwrap()).unwrap();
    assert!(matches!(
        knowledge.save_receipt(&detached_ingestion),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before_detached
    );
    let mut rebound = current.clone();
    rebound.pack.pack_id = "rebind-old-corpus-pack".into();
    rebound.canon.pack_id = rebound.pack.pack_id.clone();
    rebound.canon.canon_id = "rebind-old-corpus-canon".into();
    rebound.pack.canon_pack_ref = Some(rebound.canon.canon_id.clone());
    rebound.corpus.corpus_id = first.corpus.corpus_id.clone();
    rebound.pack.source_corpus_refs = vec![first.corpus.corpus_id.clone()];
    rebound.chunks.clear();
    rebound.corpus.chunk_count = 0;
    rebound.conflicts.clear();
    let before_rebind = serde_json::to_value(knowledge.snapshot().unwrap()).unwrap();
    assert!(matches!(
        knowledge.save_receipt(&rebound),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before_rebind
    );
    let mut detached = after_observation.pack.clone();
    detached.source_corpus_refs.clear();
    assert!(matches!(
        knowledge.commit_maintenance(&knowledge.maintenance_inputs().unwrap(), &[detached], &[]),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
    assert_eq!(
        serde_json::to_value(knowledge.snapshot().unwrap()).unwrap(),
        before_rebind
    );
    let history_key = format!("memory_lifecycle:{}", original.id);
    // Legal JSON with a huge prefix; the source budget must reject before
    // history materialization, rather than merely refusing its later digest.
    memory_store
        .kv_put(
            &history_key,
            &format!(
                "{}[]",
                " ".repeat(memory::knowledge::KNOWLEDGE_READ_MAX_BYTES)
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        fabric.search_namespace(&KnowledgeNamespace::Project("snapshot-test".into()), ""),
        Err(memory::KnowledgeStoreError::ReadLimit)
    ));
    memory_store.kv_delete(&history_key).await.unwrap();
    // Long authorized metadata is bounded by the actual total reservation,
    // not rejected by an invented 4KiB scope/field limit.
    let mut long_scope = original.clone();
    long_scope.id = uuid::Uuid::new_v4();
    long_scope.scope = MemoryScope::Project("\"".repeat(3000));
    memory_store.insert(&long_scope).await.unwrap();
    let long_source = format!("memory:{}", long_scope.id);
    let live = memory_store
        .read_source_revision(&long_source, memory::knowledge::KNOWLEDGE_READ_MAX_BYTES)
        .unwrap()
        .unwrap();
    assert_eq!(
        live.revision.unwrap(),
        memory::store::MemoryEntryRevision::of(&long_scope).unwrap()
    );
    let archive = memory::store::MemoryLifecycleMutation::new(
        &long_scope,
        None,
        memory::MemoryLifecycleEvent {
            memory_id: long_scope.id,
            from: None,
            to: memory::MemoryState::Archived,
            reason: "long original source scope".into(),
            session_id: "s".into(),
            agent_id: "a".into(),
            occurred_at: chrono::Utc::now(),
        },
    )
    .unwrap();
    memory_store.commit_lifecycle(archive).await.unwrap();
    let retired = memory_store
        .read_source_revision(&long_source, memory::knowledge::KNOWLEDGE_READ_MAX_BYTES)
        .unwrap()
        .unwrap();
    assert!(retired.fence.retired);
    assert!(retired.revision.is_none());
    assert!(retired.materialized_bytes > 4096);
    assert!(matches!(
        memory_store.read_source_revision(&long_source, 4096),
        Err(memory::MemoryError::SourceReadLimit)
    ));
    let mut foreign = prepared("foreign-scope");
    foreign.corpus.memory_body_revision = current.corpus.memory_body_revision.clone();
    foreign.corpus.namespace = KnowledgeNamespace::Project("foreign".into());
    foreign.pack.namespace = foreign.corpus.namespace.clone();
    assert!(matches!(
        knowledge.save_receipt(&foreign),
        Err(memory::KnowledgeStoreError::StaleRead)
    ));
}

#[tokio::test]
async fn knowledge_shared_large_source_is_read_once_for_the_original_candidate_frame() {
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::store::{EphemeralMemoryStore, MemorySourceFenceReader, MemoryStore};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Reader {
        owner: Arc<EphemeralMemoryStore>,
        reads: AtomicUsize,
    }
    impl MemorySourceFenceReader for Reader {
        fn read_source_fence(
            &self,
            source: &str,
        ) -> memory::store::Result<Option<memory::store::MemorySourceFence>> {
            self.owner.read_source_fence(source)
        }
        fn read_source_revision(
            &self,
            source: &str,
            budget: usize,
        ) -> memory::store::Result<Option<memory::store::MemorySourceRevisionRead>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.owner.read_source_revision(source, budget)
        }
    }
    let owner = Arc::new(EphemeralMemoryStore::new());
    let mut source = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        &"x".repeat(1024 * 1024),
    );
    source.scope = MemoryScope::Project("shared-source".into());
    source.visibility = AgentVisibility::Shared; // This identity-free namespace query reads public source material.
    owner.insert(&source).await.unwrap();
    let reader = Arc::new(Reader {
        owner: owner.clone(),
        reads: AtomicUsize::new(0),
    });
    let fabric = memory::KnowledgeFabric::with_store(Arc::new(
        memory::InMemoryKnowledgeStore::new().with_memory_owner(owner),
    ))
    .with_memory_sources(reader.clone());
    for index in 0..10 {
        let mut document =
            memory::DocumentContent::new(format!("policy-{index}"), "must retain source evidence");
        document.source = Some(format!("memory:{}", source.id));
        document.memory_source_revision =
            Some(memory::store::MemoryEntryRevision::of(&source).unwrap());
        fabric
            .ingest_document(
                KnowledgeNamespace::Project("shared-source".into()),
                KnowledgeActivationPolicy::DefaultForProjectGroup,
                KnowledgeGovernanceLevel::Required,
                document,
            )
            .unwrap();
    }
    reader.reads.store(0, Ordering::SeqCst);
    let view = fabric
        .search_namespace(&KnowledgeNamespace::Project("shared-source".into()), "")
        .unwrap();
    assert_eq!(
        view.packs.len(),
        10,
        "all ten required policies fit with their one original source"
    );
    for index in 0..10 {
        assert!(view
            .packs
            .iter()
            .any(|pack| pack.name == format!("policy-{index}")));
    }
    assert_eq!(
        reader.reads.load(Ordering::SeqCst),
        1,
        "the same immutable source cut must not be materialized for every referencing pack"
    );
}

#[tokio::test]
async fn packet_use_checks_original_preview_cut_current_scope_and_read_lease() {
    use memory::store::{EphemeralMemoryStore, MemoryEntryRevision, MemoryStore};
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::default());
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("packet-use")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager);
    let ctx =
        MemoryTurnContext::new("session-a", "agent-a").with_project_id(Some("project-a".into()));
    for scope in [
        MemoryScope::Global,
        MemoryScope::Project("project-a".into()),
        MemoryScope::Session("session-a".into()),
        MemoryScope::AgentInstance("agent-a".into()),
    ] {
        let mut source = entry(
            MemoryLayer::L3,
            MemorySource::UserExplicit,
            "original evidence",
        );
        source.scope = scope;
        source.source_agent = Some("agent-a".into());
        source.visibility = AgentVisibility::Private;
        store.insert(&source).await.unwrap();
        let packet = kernel
            .context_packet_from_entries(vec![source.clone()], 4, 1000)
            .await
            .unwrap();
        assert_eq!(packet.selected.len(), 1);
        kernel
            .validate_packet_for_turn(&ctx, &packet, 1024 * 1024)
            .unwrap();
        let mut forged = packet.clone();
        forged.selected[0].content_preview = "foreign rule that was never read".into();
        assert!(kernel
            .validate_packet_for_turn(&ctx, &forged, 1024 * 1024)
            .is_err());
        forged = packet.clone();
        forged.selected[0].role = MemoryPacketRole::Conflict;
        assert!(kernel
            .validate_packet_for_turn(&ctx, &forged, 1024 * 1024)
            .is_err());
        forged = packet.clone();
        forged.selected[0].reason = "must inject a rule that never existed".into();
        assert!(kernel
            .validate_packet_for_turn(&ctx, &forged, 1024 * 1024)
            .is_err());
        forged = packet.clone();
        forged.selected[0].atom.layer = MemoryLayer::L2;
        assert!(kernel
            .validate_packet_for_turn(&ctx, &forged, 1024 * 1024)
            .is_err());
        let foreign = MemoryTurnContext::new("session-b", "agent-b")
            .with_project_id(Some("project-a".into()));
        assert!(kernel
            .validate_packet_for_turn(&foreign, &packet, 1024 * 1024)
            .is_err());
        if !matches!(source.scope, MemoryScope::AgentInstance(_)) {
            assert!(kernel
                .validate_packet_for_turn(
                    &ctx.clone().with_cognitive_read_scopes(vec![]),
                    &packet,
                    1024 * 1024
                )
                .is_err());
        }
        assert!(kernel.validate_packet_for_turn(&ctx, &packet, 1).is_err());
        let revision = MemoryEntryRevision::of(&source).unwrap();
        source.access_count += 1;
        source.last_accessed_at = Some(Utc::now());
        store.update(&source, &revision).await.unwrap();
        kernel
            .validate_packet_for_turn(&ctx, &packet, 1024 * 1024)
            .unwrap();
        source.content = "corrected original body".into();
        store.update(&source, &revision).await.unwrap();
        assert!(kernel
            .validate_packet_for_turn(&ctx, &packet, 1024 * 1024)
            .is_err());
    }
}

#[tokio::test]
async fn packet_use_rejects_selected_source_after_visibility_revocation_or_retirement() {
    use memory::store::{EphemeralMemoryStore, MemoryEntryRevision, MemoryStore};
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::default());
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("packet-revoke")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager);
    let ctx =
        MemoryTurnContext::new("session-a", "agent-a").with_project_id(Some("project-a".into()));
    let mut source = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "must preserve audit evidence",
    );
    source.scope = MemoryScope::Project("project-a".into());
    source.source_agent = Some("agent-b".into());
    source.visibility = AgentVisibility::Shared;
    store.insert(&source).await.unwrap();
    let packet = kernel
        .context_packet_from_entries(vec![source.clone()], 4, 1000)
        .await
        .unwrap();
    kernel
        .validate_packet_for_turn(&ctx, &packet, 1024 * 1024)
        .unwrap();
    let revision = MemoryEntryRevision::of(&source).unwrap();
    source.visibility = AgentVisibility::Private;
    store.update(&source, &revision).await.unwrap();
    assert!(kernel
        .validate_packet_for_turn(&ctx, &packet, 1024 * 1024)
        .is_err());
    let owner =
        MemoryTurnContext::new("session-a", "agent-b").with_project_id(Some("project-a".into()));
    let current = kernel
        .context_packet_from_entries(vec![source.clone()], 4, 1000)
        .await
        .unwrap();
    kernel
        .archive(&owner, source.id, "retire selected source")
        .await
        .unwrap();
    assert!(kernel
        .validate_packet_for_turn(&owner, &current, 1024 * 1024)
        .is_err());
}

#[tokio::test]
async fn extraction_retirement_fences_late_different_id_but_not_an_independent_input() {
    use memory::store::{
        EphemeralMemoryStore, MemoryExtractionOrigin, MemoryLifecycleMutation,
        MemorySourceFenceReader, MemoryStore,
    };
    let store = EphemeralMemoryStore::default();
    let turn = MemoryTurnContext::new("session-a", "agent-a");
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000001";
    let mut source = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "early heuristic",
    );
    source.scope = MemoryScope::Session("session-a".into());
    source.session_id = Some(turn.session_id.clone());
    source.source_agent = Some(turn.agent_id.clone());
    source.tags.push(batch.into());
    let origin = MemoryExtractionOrigin::for_entry(&turn, &source, batch).unwrap();
    store
        .write_extraction(&source, &origin, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .write_extraction(&source, &origin, None)
            .await
            .unwrap(),
        source.id
    );
    let mut sibling = source.clone();
    sibling.id = uuid::Uuid::new_v4();
    sibling.title = "already materialized semantic view".into();
    store
        .write_extraction(&sibling, &origin, None)
        .await
        .unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: source.id,
        from: None,
        to: MemoryState::Archived,
        reason: "source retired while semantic extraction is pending".into(),
        session_id: turn.session_id.clone(),
        agent_id: turn.agent_id.clone(),
        occurred_at: Utc::now(),
    };
    let command = MemoryLifecycleMutation::new(&source, None, event).unwrap();
    let mut forged = serde_json::to_value(&command).unwrap();
    forged["revoke_origin"] = serde_json::Value::Bool(false);
    let decoded: MemoryLifecycleMutation = serde_json::from_value(forged).unwrap();
    assert!(
        decoded.revokes_origin(),
        "external input cannot claim the internal duplicate-projection exemption"
    );
    store.commit_lifecycle(command).await.unwrap();
    let mut late = source.clone();
    late.id = uuid::Uuid::new_v4();
    late.title = "late semantic output has a different ID".into();
    assert!(store.write_extraction(&late, &origin, None).await.is_err());
    assert!(store.get(&late.id).await.unwrap().is_none());
    assert!(
        store.insert(&late).await.is_err(),
        "raw create cannot bypass producer origin binding"
    );
    let read = store
        .read_source_revision(&format!("memory:{}", sibling.id), 1024 * 1024)
        .unwrap()
        .unwrap();
    assert!(
        read.origin_retired,
        "pre-existing sibling view also loses its retired input"
    );
    assert!(read.revision.is_none());
    let foreign = MemoryTurnContext::new("session-b", "agent-b");
    assert!(MemoryExtractionOrigin::for_entry(&foreign, &late, batch).is_err());
    let independent_batch = "extraction-batch:00000000-0000-0000-0000-000000000002";
    late.tags = vec![independent_batch.into()];
    let independent = MemoryExtractionOrigin::for_entry(&turn, &late, independent_batch).unwrap();
    store
        .write_extraction(&late, &independent, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_consumed_source_cut_fences_a_new_extraction_origin_and_target() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation,
        MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let mut parent = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "actual consumed source",
    );
    parent.scope = MemoryScope::Project("project-a".into());
    store.insert(&parent).await.unwrap();
    let original_cut = MemoryEntryRevision::of(&parent).unwrap();
    let turn = MemoryTurnContext::new("session-a", "agent-a")
        .with_project_id(Some("project-a".into()))
        .with_turn_ref(Some("original-turn".into()))
        .with_source_readset(Some(vec![original_cut.clone()]));
    let mut output = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "derived output with a new ID",
    );
    output.scope = parent.scope.clone();
    output.session_id = Some("session-a".into());
    output.source_agent = Some("agent-a".into());
    let first_batch = "extraction-batch:00000000-0000-0000-0000-000000000001";
    output.tags = vec![first_batch.into()];
    let origin = MemoryExtractionOrigin::for_entry(&turn, &output, first_batch).unwrap();
    store
        .write_extraction(&output, &origin, None)
        .await
        .unwrap();
    parent.access_count += 1;
    store.update(&parent, &original_cut).await.unwrap();
    output.id = uuid::Uuid::new_v4();
    store
        .write_extraction(&output, &origin, None)
        .await
        .unwrap();
    parent.content = "changed after the Provider consumed the old body".into();
    store.update(&parent, &original_cut).await.unwrap();
    let new_batch = "extraction-batch:00000000-0000-0000-0000-000000000002";
    output.id = uuid::Uuid::new_v4();
    output.tags = vec![new_batch.into()];
    let late_origin = MemoryExtractionOrigin::for_entry(&turn, &output, new_batch).unwrap();
    assert_ne!(late_origin.key().unwrap(), origin.key().unwrap());
    assert!(store
        .write_extraction(&output, &late_origin, None)
        .await
        .is_err());
    assert!(store.get(&output.id).await.unwrap().is_none());
    let current = MemoryEntryRevision::of(&parent).unwrap();
    let fresh_turn = turn.clone().with_source_readset(Some(vec![current]));
    let fresh = MemoryExtractionOrigin::for_entry(&fresh_turn, &output, new_batch).unwrap();
    store.write_extraction(&output, &fresh, None).await.unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: parent.id,
        from: None,
        to: MemoryState::Archived,
        reason: "source revoked after actual use".into(),
        session_id: "session-a".into(),
        agent_id: "agent-a".into(),
        occurred_at: Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::new(&parent, None, event).unwrap())
        .await
        .unwrap();
    output.id = uuid::Uuid::new_v4();
    assert!(store.write_extraction(&output, &fresh, None).await.is_err());
    assert!(store.get(&output.id).await.unwrap().is_none());
    assert!(
        MemoryExtractionOrigin::for_entry(&turn.with_source_readset(None), &output, new_batch)
            .is_err()
    );
}

#[tokio::test]
async fn original_ancestor_cuts_fence_transitive_derivatives_in_every_layer() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation,
        MemorySourceFenceReader, MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let mut ancestor = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "original ancestor A",
    );
    ancestor.scope = MemoryScope::Project("project-a".into());
    store.insert(&ancestor).await.unwrap();
    let original = MemoryEntryRevision::of(&ancestor).unwrap();
    let turn = MemoryTurnContext::new("session-a", "agent-a")
        .with_project_id(Some("project-a".into()))
        .with_source_readset(Some(vec![original.clone()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000011";
    let mut derivatives = Vec::new();
    for layer in [
        MemoryLayer::L0,
        MemoryLayer::L1,
        MemoryLayer::L2,
        MemoryLayer::L3,
        MemoryLayer::L4,
    ] {
        let mut b = entry(
            layer,
            MemorySource::AutoExtracted,
            "B derives from original A",
        );
        b.scope = ancestor.scope.clone();
        b.session_id = Some("session-a".into());
        b.source_agent = Some("agent-a".into());
        b.tags = vec![batch.into()];
        let origin = MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap();
        store.write_extraction(&b, &origin, None).await.unwrap();
        let b_cut = MemoryEntryRevision::of(&b).unwrap();
        let b_turn = turn.clone().with_source_readset(Some(vec![b_cut]));
        let mut c = b.clone();
        c.id = uuid::Uuid::new_v4();
        c.title = "C derives from B in a later turn".into();
        let c_origin = MemoryExtractionOrigin::for_entry(&b_turn, &c, batch).unwrap();
        store.write_extraction(&c, &c_origin, None).await.unwrap();
        derivatives.push((b, c, c_origin));
    }
    ancestor.content = "corrected original source A".into();
    store.update(&ancestor, &original).await.unwrap();
    for (b, c, origin) in &derivatives {
        for id in [b.id, c.id] {
            assert!(store
                .read_source_revision(&format!("memory:{id}"), 1024 * 1024)
                .unwrap()
                .unwrap()
                .revision
                .is_none());
        }
        let mut late = c.clone();
        late.id = uuid::Uuid::new_v4();
        assert!(store.write_extraction(&late, origin, None).await.is_err());
        assert!(store.get(&late.id).await.unwrap().is_none());
        assert!(
            store.get(&b.id).await.unwrap().is_some(),
            "stale lineage does not fabricate body erasure"
        );
    }
    let current = MemoryEntryRevision::of(&ancestor).unwrap();
    ancestor.content = "original ancestor A content".into();
    store.update(&ancestor, &current).await.unwrap();
    for (b, _, _) in &derivatives {
        assert!(store
            .read_source_revision(&format!("memory:{}", b.id), 1024 * 1024)
            .unwrap()
            .unwrap()
            .revision
            .is_some());
    }
    let event = memory::MemoryLifecycleEvent {
        memory_id: ancestor.id,
        from: None,
        to: MemoryState::Archived,
        reason: "irreversible original source retirement".into(),
        session_id: "session-a".into(),
        agent_id: "agent-a".into(),
        occurred_at: Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::new(&ancestor, None, event).unwrap())
        .await
        .unwrap();
    for (b, c, origin) in derivatives {
        assert!(store
            .read_source_revision(&format!("memory:{}", b.id), 1024 * 1024)
            .unwrap()
            .unwrap()
            .revision
            .is_none());
        let mut late = c;
        late.id = uuid::Uuid::new_v4();
        assert!(store.write_extraction(&late, &origin, None).await.is_err());
    }
    let independent = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "independent source remains usable",
    );
    store.insert(&independent).await.unwrap();
    assert!(store
        .read_source_revision(&format!("memory:{}", independent.id), 1024 * 1024)
        .unwrap()
        .unwrap()
        .revision
        .is_some());
}

#[tokio::test]
async fn shared_original_ancestor_is_materialized_once_in_a_native_source_frame() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemorySourceFenceReader,
        MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let mut a = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "large shared original",
    );
    a.content = "x".repeat(5 * 1024 * 1024);
    a.scope = MemoryScope::Project("project-a".into());
    store.insert(&a).await.unwrap();
    let turn = MemoryTurnContext::new("s", "a")
        .with_project_id(Some("project-a".into()))
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&a).unwrap()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000014";
    let mut parents = Vec::new();
    for name in ["B1", "B2"] {
        let mut b = entry(MemoryLayer::L3, MemorySource::AutoExtracted, name);
        b.scope = a.scope.clone();
        b.session_id = Some("s".into());
        b.source_agent = Some("a".into());
        b.tags = vec![batch.into()];
        store
            .write_extraction(
                &b,
                &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
                None,
            )
            .await
            .unwrap();
        parents.push(MemoryEntryRevision::of(&b).unwrap());
    }
    let mut c = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "shared-ancestor child C",
    );
    c.scope = a.scope;
    c.session_id = Some("s".into());
    c.source_agent = Some("a".into());
    c.tags = vec![batch.into()];
    let origin =
        MemoryExtractionOrigin::for_entry(&turn.with_source_readset(Some(parents)), &c, batch)
            .unwrap();
    store.write_extraction(&c, &origin, None).await.unwrap();
    let read = store
        .read_source_revision(&format!("memory:{}", c.id), 8 * 1024 * 1024)
        .unwrap()
        .unwrap();
    assert!(read.revision.is_some());
    assert!(read.materialized_bytes > 5 * 1024 * 1024);
    assert!(
        read.materialized_bytes < 6 * 1024 * 1024,
        "the 5MiB original is not materialized again for each sibling"
    );
    assert!(store
        .read_source_revision(&format!("memory:{}", c.id), 4 * 1024 * 1024)
        .is_err());
}

#[tokio::test]
async fn retired_derivative_uses_only_its_original_fence_not_a_large_ancestor() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation,
        MemorySourceFenceReader, MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let mut a = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "large original",
    );
    a.content = "x".repeat(1024 * 1024);
    store.insert(&a).await.unwrap();
    let turn = MemoryTurnContext::new("s", "a")
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&a).unwrap()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000015";
    let mut b = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "retired derivative",
    );
    b.session_id = Some("s".into());
    b.source_agent = Some("a".into());
    b.tags = vec![batch.into()];
    store
        .write_extraction(
            &b,
            &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
            None,
        )
        .await
        .unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: b.id,
        from: None,
        to: MemoryState::Archived,
        reason: "independent B retirement".into(),
        session_id: "s".into(),
        agent_id: "a".into(),
        occurred_at: Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::new(&b, None, event).unwrap())
        .await
        .unwrap();
    let read = store
        .read_source_revision(&format!("memory:{}", b.id), 4096)
        .unwrap()
        .unwrap();
    assert!(read.fence.retired);
    assert!(read.revision.is_none());
    assert!(read.materialized_bytes < 4096);
}

#[tokio::test]
async fn explicit_erasure_has_its_own_receipt_and_cannot_recreate_or_confirm_a_foreign_call() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemorySourceFenceReader,
        MemoryStore,
    };
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("memory")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager.clone());
    let ctx = MemoryTurnContext::new("s", "a").with_project_id(Some("project-a".into()));
    let mut source = entry(
        MemoryLayer::L3,
        MemorySource::UserExplicit,
        "body retained for archive only",
    );
    source.scope = MemoryScope::Project("project-a".into());
    kernel.remember(&ctx, source.clone()).await.unwrap();
    let original = store.get(&source.id).await.unwrap().unwrap();
    kernel
        .archive(&ctx, source.id, "archive source")
        .await
        .unwrap();
    assert!(store.get(&source.id).await.unwrap().is_some());
    assert!(store
        .load_verbatim_by_id(&source.id.to_string())
        .await
        .unwrap()
        .is_some());
    let result = kernel
        .erase(&ctx, source.id, "explicit erase")
        .await
        .unwrap();
    let receipt = result.receipt.clone();
    assert!(receipt.body_erased);
    assert!(receipt.source.retired);
    assert_eq!(result.cleanup_confirmed(), Some(false));
    assert_eq!(
        result.may_retain,
        [
            "historical_audit_summaries",
            "immutable_knowledge_evidence",
            "derived_memory_and_historical_projections",
        ]
    );
    assert!(store.get(&source.id).await.unwrap().is_none());
    assert!(store
        .load_verbatim_by_id(&source.id.to_string())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        kernel
            .erase(&ctx, source.id, "explicit erase")
            .await
            .unwrap()
            .receipt,
        receipt
    );
    assert!(kernel
        .erase(
            &MemoryTurnContext::new("s", "foreign").with_project_id(ctx.project_id.clone()),
            source.id,
            "explicit erase"
        )
        .await
        .is_err());
    assert!(kernel
        .erase(&ctx, source.id, "different erase operation")
        .await
        .is_err());
    assert!(kernel
        .erase(&ctx, uuid::Uuid::new_v4(), "never existed")
        .await
        .is_err());
    assert!(kernel.remember(&ctx, source.clone()).await.is_err());
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000017";
    let mut late = original.clone();
    late.id = uuid::Uuid::new_v4();
    late.source = MemorySource::AutoExtracted;
    late.tags = vec![batch.into()];
    let late_ctx = ctx
        .clone()
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&original).unwrap()]));
    assert!(store
        .write_extraction(
            &late,
            &MemoryExtractionOrigin::for_entry(&late_ctx, &late, batch).unwrap(),
            None
        )
        .await
        .is_err());
    let pending = store.pending_memory_cleanup(None, 16).await.unwrap();
    assert_eq!(
        pending.len(),
        2,
        "Archive and Erase are distinct original operations"
    );
    assert!(pending
        .iter()
        .all(|pending| !pending.auxiliary_confirmed && pending.knowledge_receipt_digest.is_none()));
    let fabric = memory::KnowledgeFabric::with_store(Arc::new(
        memory::InMemoryKnowledgeStore::new().with_memory_owner(store.clone()),
    ))
    .with_memory_sources(store.clone());
    assert!(manager
        .recover_memory_cleanup(&fabric, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    let confirmed = kernel
        .erase(&ctx, source.id, "explicit erase")
        .await
        .unwrap();
    assert_eq!(confirmed.receipt, receipt);
    assert_eq!(confirmed.cleanup_confirmed(), Some(true));
    assert!(store
        .memory_cleanup(&receipt.mutation_id)
        .await
        .unwrap()
        .is_some());
    let mut foreign = confirmed.cleanup.clone().unwrap();
    foreign.source.scope_key = "project:foreign".into();
    assert!(memory::store::MemoryErasureResult::observe(receipt.clone(), Some(foreign)).is_err());
    let unknown = memory::store::MemoryErasureResult::observe(receipt.clone(), None).unwrap();
    assert_eq!(unknown.cleanup_confirmed(), None);
    assert!(unknown.receipt.body_erased);
    assert!(
        manager.audit_entries(100).unwrap().iter().any(|event| {
            event.entry_id == source.id.to_string()
                && event.operation == memory::AuditOperation::Create
        }),
        "historical audit survives; body-only receipt must disclose retention"
    );
    assert!(
        store
            .read_source_fence(&format!("memory:{}", source.id))
            .unwrap()
            .unwrap()
            .retired
    );
}

#[tokio::test]
async fn erasure_preserves_original_layer_scope_and_operation_kind_guards() {
    use memory::store::{EphemeralMemoryStore, MemoryLifecycleMutation, MemoryStore};
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let mut source = entry(
        MemoryLayer::L0,
        MemorySource::UserExplicit,
        "protected body",
    );
    source.scope = MemoryScope::Project("A".into());
    source.source_agent = Some("owner".into());
    store.insert(&source).await.unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("memory")),
            None,
            store.clone(),
        )
        .await
        .unwrap()
        .with_write_source(memory::write_guard::WriteSource::Tool),
    );
    let kernel = MemoryKernel::new(manager);
    let ctx = MemoryTurnContext::new("s", "owner").with_project_id(Some("A".into()));
    assert!(kernel
        .erase(&ctx, source.id, "not granted to tools")
        .await
        .is_err());
    let foreign = MemoryTurnContext::new("s", "owner").with_project_id(Some("foreign".into()));
    assert!(kernel
        .erase(&foreign, source.id, "wrong scope")
        .await
        .is_err());
    let archive = MemoryLifecycleMutation::new(
        &source,
        None,
        memory::MemoryLifecycleEvent {
            memory_id: source.id,
            from: None,
            to: MemoryState::Archived,
            reason: "archive only".into(),
            session_id: "s".into(),
            agent_id: "owner".into(),
            occurred_at: Utc::now(),
        },
    )
    .unwrap();
    let mut forged = serde_json::to_value(archive).unwrap();
    forged["erase_body"] = serde_json::json!(true);
    assert!(
        store
            .commit_lifecycle(serde_json::from_value(forged).unwrap())
            .await
            .is_err(),
        "Archive identity cannot be borrowed for a body erase"
    );
    assert!(store.get(&source.id).await.unwrap().is_some());
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn auxiliary_original_cuts_keep_independent_support_and_fence_late_snapshots() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemoryLifecycleMutation,
        MemoryStore,
    };
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let mut a = entry(MemoryLayer::L3, MemorySource::Import, "upstream original");
    a.scope = MemoryScope::Global;
    a.visibility = AgentVisibility::Shared;
    let mut d = entry(MemoryLayer::L3, MemorySource::Import, "independent support");
    d.scope = MemoryScope::Global;
    d.visibility = AgentVisibility::Shared;
    store.insert(&a).await.unwrap();
    store.insert(&d).await.unwrap();
    let a_cut = MemoryEntryRevision::of(&a).unwrap();
    let d_cut = MemoryEntryRevision::of(&d).unwrap();
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000057";
    let turn = MemoryTurnContext::new("original-session", "original-agent")
        .with_source_readset(Some(vec![a_cut.clone()]));
    let mut b = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "derived original",
    );
    b.scope = MemoryScope::Global;
    b.visibility = AgentVisibility::Shared;
    b.session_id = Some(turn.session_id.clone());
    b.source_agent = Some(turn.agent_id.clone());
    b.tags = vec![batch.into()];
    store
        .write_extraction(
            &b,
            &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
            None,
        )
        .await
        .unwrap();
    let backed = memory::entity::Entity {
        id: "original-backed-entity".into(),
        name: "signal".into(),
        entity_type: memory::entity::EntityType::Concept,
        confidence: 1.0,
        frequency: 1,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        source_ids: vec![b.id.to_string(), d.id.to_string()],
        source_type: "memory".into(),
        memory_source_revisions: vec![MemoryEntryRevision::of(&b).unwrap(), d_cut.clone()],
    };
    let mut external = backed.clone();
    external.id = "external-uuid-source".into();
    external.name = "external".into();
    external.source_ids = vec![d.id.to_string()];
    external.source_type = "file".into();
    external.memory_source_revisions.clear();
    let triple = memory::entity::Triple {
        id: "original-backed-triple".into(),
        subject_id: backed.id.clone(),
        predicate: "supported_by".into(),
        object_id: external.id.clone(),
        valid_from: None,
        valid_to: None,
        source: Some(format!("memory:{}", b.id)),
        confidence: 1.0,
        created_at: Utc::now(),
        source_agent: None,
        memory_source_revision: Some(MemoryEntryRevision::of(&b).unwrap()),
    };
    store
        .save_knowledge_graph(
            "global",
            &[backed.clone(), external.clone()],
            &[triple.clone()],
        )
        .await
        .unwrap();
    assert!(
        store
            .save_knowledge_graph("project_P", &[backed.clone()], &[triple.clone()])
            .await
            .is_err(),
        "global use does not grant cross-scope physical duplication"
    );
    let mut washed = backed.clone();
    washed.source_ids.clear();
    washed.memory_source_revisions.clear();
    assert!(store
        .save_entities("global", &[washed.clone()])
        .await
        .is_err());
    washed.source_type = "file".into();
    assert!(
        store.save_entities("global", &[washed]).await.is_err(),
        "same identity cannot shed its original Memory provenance"
    );
    let mut unknown = triple.clone();
    unknown.source = Some(format!("memory:{}", uuid::Uuid::new_v4()));
    assert!(store
        .save_knowledge_graph("global", &[external.clone()], &[unknown])
        .await
        .is_err());
    assert_eq!(store.load_entities("global").await.unwrap().len(), 2);
    assert_eq!(store.load_triples("global").await.unwrap().len(), 1);
    a.content = "a true correction after the auxiliary snapshot".into();
    store.update(&a, &a_cut).await.unwrap();
    let visible = store.load_entities("global").await.unwrap();
    let multi = visible
        .iter()
        .find(|entity| entity.id == backed.id)
        .unwrap();
    assert_eq!(multi.source_ids, vec![d.id.to_string()]);
    assert_eq!(multi.memory_source_revisions, vec![d_cut.clone()]);
    assert!(store.load_triples("global").await.unwrap().is_empty());
    assert!(store
        .save_knowledge_graph("global", &[backed.clone()], &[triple])
        .await
        .is_err());
    // Startup and the actual project consumer use this original global graph.
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("manager")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let mut consumer = MemoryTurnContext::new("consumer-session", "consumer-agent");
    consumer.project_id = Some("P".into());
    let prepared = manager
        .prepare_context_for_turn(&consumer, "signal", &[])
        .await
        .unwrap();
    assert!(prepared.entries.iter().any(|entry| entry.id == d.id));
    assert!(prepared
        .entries
        .iter()
        .all(|entry| entry.id != b.id && entry.title != "KG entity: signal"));
    let event = memory::MemoryLifecycleEvent {
        memory_id: d.id,
        from: None,
        to: MemoryState::Archived,
        reason: "source removed".into(),
        session_id: "erase-owner".into(),
        agent_id: "erase-owner".into(),
        occurred_at: Utc::now(),
    };
    store
        .commit_lifecycle(MemoryLifecycleMutation::erase(&d, None, event).unwrap())
        .await
        .unwrap();
    let prepared = manager
        .prepare_context_for_turn(&consumer, "signal", &[])
        .await
        .unwrap();
    assert!(
        prepared.entries.iter().all(|entry| entry.id != d.id),
        "an accelerator hit cannot return the previously cached retired source"
    );
    let visible = store.load_entities("global").await.unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, external.id);
    let knowledge = memory::KnowledgeFabric::with_store(Arc::new(
        memory::InMemoryKnowledgeStore::new().with_memory_owner(store.clone()),
    ))
    .with_memory_sources(store.clone());
    assert!(manager
        .recover_memory_cleanup(&knowledge, 16)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .pending_memory_cleanup(None, 16)
        .await
        .unwrap()
        .is_empty());
    let visible = store.load_entities("global").await.unwrap();
    assert_eq!(
        visible[0].source_ids,
        vec![d.id.to_string()],
        "an external UUID is not a Memory dependency"
    );
    let mut late = backed;
    late.id = "late-different-entity-id".into();
    assert!(store.save_entities("global", &[late]).await.is_err());
    assert_eq!(
        store.load_entities("global").await.unwrap()[0].id,
        external.id
    );
}

#[tokio::test]
async fn auxiliary_production_filter_reads_shared_large_ancestry_once() {
    use memory::store::{
        EphemeralMemoryStore, MemoryEntryRevision, MemoryExtractionOrigin, MemorySourceFenceReader,
        MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let mut a = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        "large shared original",
    );
    a.scope = MemoryScope::Global;
    a.content = "z".repeat(5 * 1024 * 1024);
    store.insert(&a).await.unwrap();
    let turn = MemoryTurnContext::new("frame-session", "frame-agent")
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&a).unwrap()]));
    let batch = "extraction-batch:00000000-0000-0000-0000-000000000058";
    let mut leaves = Vec::new();
    for title in ["first small leaf", "second small leaf"] {
        let mut b = entry(MemoryLayer::L3, MemorySource::AutoExtracted, title);
        b.scope = MemoryScope::Global;
        b.session_id = Some(turn.session_id.clone());
        b.source_agent = Some(turn.agent_id.clone());
        b.tags = vec![batch.into()];
        store
            .write_extraction(
                &b,
                &MemoryExtractionOrigin::for_entry(&turn, &b, batch).unwrap(),
                None,
            )
            .await
            .unwrap();
        leaves.push(b);
    }
    let entity = memory::entity::Entity {
        id: "shared-large-ancestor".into(),
        name: "shared ancestry".into(),
        entity_type: memory::entity::EntityType::Concept,
        confidence: 1.0,
        frequency: 1,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        source_type: "memory".into(),
        source_ids: leaves.iter().map(|entry| entry.id.to_string()).collect(),
        memory_source_revisions: leaves
            .iter()
            .map(|entry| MemoryEntryRevision::of(entry).unwrap())
            .collect(),
    };
    let (kept, _) =
        memory::store::filter_auxiliary_snapshot(&store, "global", &[entity], &[]).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].source_ids.len(), 2);
    let (frame, spent) = store
        .read_source_revisions(
            &leaves.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            8 * 1024 * 1024,
        )
        .unwrap();
    assert_eq!(frame.len(), 2);
    assert!(
        spent > a.content.len() && spent < 6 * 1024 * 1024,
        "the shared 5MiB source is charged once, with bounded original metadata"
    );
    store
        .save_knowledge_graph("global", &kept, &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn legacy_retirement_adoption_binds_original_history_and_never_reopens_a_scope() {
    use memory::store::{
        EphemeralMemoryStore, MemoryLifecycleMutation, MemorySourceFenceReader, MemoryStore,
    };
    let store = EphemeralMemoryStore::new();
    let original = entry(MemoryLayer::L3, MemorySource::Import, "legacy original");
    store.insert(&original).await.unwrap();
    let event = memory::MemoryLifecycleEvent {
        memory_id: original.id,
        from: None,
        to: MemoryState::Superseded,
        reason: "original retirement".into(),
        session_id: "original-session".into(),
        agent_id: "original-actor".into(),
        occurred_at: Utc::now(),
    };
    let raw = serde_json::to_string(&vec![event.clone()]).unwrap();
    let key = format!("memory_lifecycle:{}", original.id);
    store.kv_put(&key, &raw).await.unwrap();
    let page = store.legacy_lifecycle_page(None, 1).await.unwrap();
    assert_eq!(page.after_id, Some(original.id.to_string()));
    assert_eq!(page.entries.len(), 1);
    assert!(store
        .pending_memory_cleanup(None, 1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .read_source_fence(&format!("memory:{}", original.id))
            .unwrap()
            .unwrap()
            .delete_epoch,
        0
    );
    let command =
        MemoryLifecycleMutation::adopt_legacy_retirement(&page.entries[0].0, &page.entries[0].1)
            .unwrap()
            .unwrap();
    let mut foreign_scope = command.clone();
    foreign_scope.scope_key = "project:foreign".into();
    assert!(store.commit_lifecycle(foreign_scope).await.is_err());
    let mut wrong_actor = command.clone();
    wrong_actor.event.agent_id = "foreign-actor".into();
    assert!(store.commit_lifecycle(wrong_actor).await.is_err());
    let mut changed = event.clone();
    changed.reason = "concurrent original history revision".into();
    let changed = serde_json::to_string(&vec![changed]).unwrap();
    store.kv_put(&key, &changed).await.unwrap();
    assert!(store.commit_lifecycle(command.clone()).await.is_err());
    assert!(store
        .pending_memory_cleanup(None, 1)
        .await
        .unwrap()
        .is_empty());
    store.kv_put(&key, &raw).await.unwrap();
    let receipt = store.commit_lifecycle(command.clone()).await.unwrap();
    assert_eq!(receipt.source.delete_epoch, 1);
    assert!(receipt.source.retired);
    assert!(!receipt.body_erased);
    assert_eq!(receipt.event.agent_id, "original-actor");
    let replay = serde_json::from_value(serde_json::to_value(&command).unwrap()).unwrap();
    assert_eq!(store.commit_lifecycle(replay).await.unwrap(), receipt);
    assert_eq!(
        store.commit_lifecycle(command.clone()).await.unwrap(),
        receipt
    );
    assert!(store
        .legacy_lifecycle_page(None, 1)
        .await
        .unwrap()
        .entries
        .is_empty());
    let mut stripped = serde_json::to_value(&command).unwrap();
    stripped["legacy_adoption"] = false.into();
    assert!(store
        .commit_lifecycle(serde_json::from_value(stripped).unwrap())
        .await
        .is_err());
    assert_eq!(
        store.pending_memory_cleanup(None, 2).await.unwrap().len(),
        1
    );
    assert_eq!(
        store.get(&original.id).await.unwrap().unwrap().content,
        original.content
    );
    let mut held = original.clone();
    held.scope = MemoryScope::LegacyUnresolvedAgent("unclassified".into());
    assert!(
        MemoryLifecycleMutation::adopt_legacy_retirement(&held, &raw)
            .unwrap()
            .is_none()
    );
    let mut wrong_id = event.clone();
    wrong_id.memory_id = uuid::Uuid::new_v4();
    assert!(MemoryLifecycleMutation::adopt_legacy_retirement(
        &original,
        &serde_json::to_string(&vec![wrong_id]).unwrap()
    )
    .is_err());
    let mut revived = event.clone();
    revived.from = Some(event.to);
    revived.to = MemoryState::Observed;
    assert!(MemoryLifecycleMutation::adopt_legacy_retirement(
        &original,
        &serde_json::to_string(&vec![event, revived]).unwrap()
    )
    .is_err());
}

#[tokio::test]
async fn legacy_lifecycle_page_reserves_bytes_and_advances_past_an_unreadable_source() {
    use memory::store::{EphemeralMemoryStore, MemoryStore};
    let store = EphemeralMemoryStore::new();
    let mut huge = entry(MemoryLayer::L3, MemorySource::Import, "unreadable source");
    huge.id = uuid::Uuid::from_u128(1);
    huge.content = "x".repeat(memory::knowledge::KNOWLEDGE_READ_MAX_BYTES + 1);
    let mut independent = entry(MemoryLayer::L3, MemorySource::Import, "independent source");
    independent.id = uuid::Uuid::from_u128(2);
    for e in [&huge, &independent] {
        store.insert(e).await.unwrap();
        store
            .kv_put(&format!("memory_lifecycle:{}", e.id), "[]")
            .await
            .unwrap();
    }
    assert!(store.legacy_lifecycle_page(None, 0).await.is_err());
    assert!(store.legacy_lifecycle_page(None, 257).await.is_err());
    let page = store.legacy_lifecycle_page(None, 1).await.unwrap();
    assert_eq!(page.unavailable_ids, vec![huge.id.to_string()]);
    assert!(page.entries.is_empty());
    assert_eq!(page.after_id, Some(huge.id.to_string()));
    let next = store
        .legacy_lifecycle_page(page.after_id.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(next.entries.len(), 1);
    assert_eq!(next.entries[0].0.id, independent.id);
    assert!(next.unavailable_ids.is_empty());
    assert!(store
        .pending_memory_cleanup(None, 1)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn original_runtime_turn_write_cannot_drop_its_consumed_source_cut() {
    use memory::store::{EphemeralMemoryStore, MemoryEntryRevision, MemoryStore};
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(EphemeralMemoryStore::new());
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("memory")),
            None,
            store.clone(),
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager.clone());
    let mut source = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        "original independent source",
    );
    source.scope = MemoryScope::Global;
    source.visibility = AgentVisibility::Shared;
    store.insert(&source).await.unwrap();
    let original_cut = MemoryEntryRevision::of(&source).unwrap();
    let context = MemoryTurnContext::new("original-provider-session", "original-agent")
        .with_turn_ref(Some("original-provider-turn".into()))
        .with_source_readset(Some(vec![original_cut.clone()]));
    source.content = "actual original correction".into();
    store.update(&source, &original_cut).await.unwrap();
    let stale = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "stale tool-authored note",
    );
    assert!(
        kernel.remember(&context, stale.clone()).await.is_err(),
        "Runtime write must commit against the consumed cut, not just its own new ID"
    );
    assert!(store.get(&stale.id).await.unwrap().is_none());
    let current = context
        .clone()
        .with_source_readset(Some(vec![MemoryEntryRevision::of(&source).unwrap()]));
    let note = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "legitimate current tool note",
    );
    kernel.remember(&current, note.clone()).await.unwrap();
    assert!(store.get(&note.id).await.unwrap().is_some());
    let sibling = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "same-turn independent note action",
    );
    kernel.remember(&current, sibling.clone()).await.unwrap();
    kernel
        .erase(&current, note.id, "erase just one original note action")
        .await
        .unwrap();
    assert!(
        kernel
            .retrieve_visible_entry(&current, sibling.id)
            .await
            .unwrap()
            .is_some(),
        "same-turn sibling action must not share this note's retirement origin"
    );
    let mut persisted = store.get(&sibling.id).await.unwrap().unwrap();
    let original_origin =
        memory::store::MemoryExtractionOrigin::for_turn_entry(&current, &mut persisted).unwrap();
    let mut wrong_target = persisted.clone();
    wrong_target.id = uuid::Uuid::new_v4();
    assert!(store
        .write_extraction(&wrong_target, &original_origin, None)
        .await
        .is_err());
    assert!(store.get(&wrong_target.id).await.unwrap().is_none());
    let mut unknown = current.clone();
    unknown.source_readset = None;
    let missing = entry(
        MemoryLayer::L2,
        MemorySource::UserExplicit,
        "metadata cannot grant independence",
    );
    assert!(kernel.remember(&unknown, missing.clone()).await.is_err());
    assert!(store.get(&missing.id).await.unwrap().is_none());
    kernel
        .archive(&current, source.id, "original source retirement")
        .await
        .unwrap();
    let late = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "new ID paraphrase after retirement",
    );
    assert!(kernel.remember(&current, late.clone()).await.is_err());
    assert!(store.get(&late.id).await.unwrap().is_none());
    let independent = manager
        .create_entry(
            MemoryLayer::L2,
            MemoryCategory::Reference,
            "explicit independent human input",
            "A new independent user assertion.",
            Priority::Normal,
            vec![],
            MemoryScope::Global,
        )
        .await
        .unwrap();
    assert!(store.get(&independent).await.unwrap().is_some());
}

#[tokio::test]
async fn unknown_legacy_extraction_and_broken_lifecycle_are_not_independent_live_sources() {
    use memory::store::{EphemeralMemoryStore, MemorySourceFenceReader, MemoryStore};
    let store = Arc::new(EphemeralMemoryStore::new());
    let old = entry(
        MemoryLayer::L3,
        MemorySource::AutoExtracted,
        "legacy unknown ancestry",
    );
    store.insert(&old).await.unwrap();
    assert!(
        store
            .read_source_revision(
                &format!("memory:{}", old.id),
                memory::knowledge::KNOWLEDGE_READ_MAX_BYTES
            )
            .is_err(),
        "missing extraction origin must remain unknown, not an independent root"
    );
    let mut independent = entry(
        MemoryLayer::L3,
        MemorySource::Import,
        "provable independent source",
    );
    independent.scope = MemoryScope::Global;
    independent.visibility = AgentVisibility::Shared;
    store.insert(&independent).await.unwrap();
    assert!(store
        .read_source_revision(
            &format!("memory:{}", independent.id),
            memory::knowledge::KNOWLEDGE_READ_MAX_BYTES
        )
        .unwrap()
        .unwrap()
        .revision
        .is_some());
    let event = memory::MemoryLifecycleEvent {
        memory_id: independent.id,
        from: None,
        to: MemoryState::Archived,
        reason: "historical retirement".into(),
        session_id: "original-session".into(),
        agent_id: "original-agent".into(),
        occurred_at: Utc::now(),
    };
    let mut revival = event.clone();
    revival.from = Some(MemoryState::Archived);
    revival.to = MemoryState::Observed;
    store
        .kv_put(
            &format!("memory_lifecycle:{}", independent.id),
            &serde_json::to_string(&vec![event, revival]).unwrap(),
        )
        .await
        .unwrap();
    assert!(store
        .read_source_revision(
            &format!("memory:{}", independent.id),
            memory::knowledge::KNOWLEDGE_READ_MAX_BYTES
        )
        .is_err());
    let root = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new_with_selected_store(
            test_config(&root.path().join("memory")),
            None,
            store,
        )
        .await
        .unwrap(),
    );
    let kernel = MemoryKernel::new(manager);
    let ctx = MemoryTurnContext::new("reader", "reader");
    assert!(kernel
        .retrieve_visible_entry(&ctx, independent.id)
        .await
        .is_err());
}
