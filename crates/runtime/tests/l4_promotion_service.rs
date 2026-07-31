#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use harness_contract::{
    core::TaskRisk,
    execution::ExecutionIdentity,
    knowledge::{KnowledgeAuthority, KnowledgeCandidateScope, KnowledgeLineage, KnowledgeNovelty},
    reality::EvidenceRef,
};
use memory::config::{BudgetConfig, LayerConfig, StoreConfig};
use memory::{CognitiveContextManager, MemoryConfig, MemoryLayer};
use runtime::{L4PromotionCandidate, RuntimeServices};

async fn services_with_memory() -> (
    tempfile::TempDir,
    Arc<CognitiveContextManager>,
    Arc<RuntimeServices>,
) {
    let root = tempfile::tempdir().expect("runtime root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let memory = Arc::new(
        CognitiveContextManager::new(MemoryConfig {
            store: StoreConfig {
                sqlite_path: root.path().join("memory.sqlite"),
                blob_dir: root.path().join("memory-blobs"),
                enable_vector_index: false,
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 16_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            layers: LayerConfig {
                l4_enabled: true,
                ..LayerConfig::default()
            },
            ..Default::default()
        })
        .await
        .expect("memory manager"),
    );
    let services = RuntimeServices::builder(root.path(), &workspace)
        .memory_manager(Arc::clone(&memory))
        .build()
        .expect("runtime services");
    (root, memory, services)
}

fn candidate() -> L4PromotionCandidate {
    let graph = ExecutionIdentity::for_task_graph(
        "agent-l4-service",
        "workspace-l4-service",
        "mission-l4-service",
        "task-l4-service",
        "session-l4-service",
        "turn-l4-service",
        "graph-l4-service",
    )
    .expect("task graph identity");
    let execution_identity =
        ExecutionIdentity::for_agent_node(&graph, "agent-l4-service", "verify")
            .expect("agent identity");
    L4PromotionCandidate {
        candidate_id: "candidate-l4-service".to_string(),
        execution_identity,
        scope: KnowledgeCandidateScope::AgentPrivate("agent-l4-service".to_string()),
        title: "Verified architecture decision".to_string(),
        claim: "Use the reviewed Runtime Team instantiation path for durable Team work."
            .to_string(),
        evidence_refs: vec![EvidenceRef::observed("review", "evidence:review:1")],
        authority: KnowledgeAuthority::AgentObservation,
        lineage: KnowledgeLineage::default(),
        novelty: KnowledgeNovelty::New,
        risk: TaskRisk::Low,
        tags: vec!["decision".to_string()],
        producer: "runtime.test".to_string(),
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ms: 1,
    }
}

fn team_candidate(evidence_count: usize) -> L4PromotionCandidate {
    let graph = ExecutionIdentity::for_task_graph(
        "agent-l4-service",
        "workspace-l4-service",
        "mission-l4-service",
        "task-l4-service",
        "session-l4-service",
        "turn-l4-service",
        "graph-l4-service",
    )
    .expect("task graph identity");
    let execution_identity =
        ExecutionIdentity::for_team_node(&graph, "team-l4-service", "synthesis")
            .expect("team identity");
    L4PromotionCandidate {
        candidate_id: format!("candidate-team-evidence-{evidence_count}"),
        execution_identity,
        scope: KnowledgeCandidateScope::Team("team-l4-service".to_string()),
        title: format!("Verified team synthesis {evidence_count}"),
        claim: format!("The team synthesis is supported by {evidence_count} evidence sources."),
        evidence_refs: (0..evidence_count)
            .map(|index| EvidenceRef::observed("team_review", format!("evidence:{index}")))
            .collect(),
        authority: KnowledgeAuthority::TeamSynthesis,
        lineage: KnowledgeLineage::default(),
        novelty: KnowledgeNovelty::New,
        risk: TaskRisk::Low,
        tags: vec!["team-synthesis".to_string()],
        producer: "runtime.test".to_string(),
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ms: 1,
    }
}

#[tokio::test]
async fn runtime_promotion_service_is_idempotent_and_records_lifecycle_before_l4_write() {
    let (_root, memory, services) = services_with_memory().await;
    let first = services
        .l4_promotion_service()
        .validate_and_promote(candidate())
        .await
        .expect("first promotion");
    let second = services
        .l4_promotion_service()
        .validate_and_promote(candidate())
        .await
        .expect("idempotent promotion");

    assert_eq!(first, second);
    let entry = memory
        .get_entry(&first.memory_id)
        .await
        .expect("memory read")
        .expect("promoted L4 entry");
    assert_eq!(entry.layer, MemoryLayer::L4);
    assert!(entry
        .tags
        .iter()
        .any(|tag| tag == "l4_candidate:candidate-l4-service"));

    assert_eq!(
        services
            .l4_promotion_service()
            .lifecycle(&candidate())
            .expect("lifecycle"),
        vec![
            runtime::L4CandidateLifecycle::Proposed,
            runtime::L4CandidateLifecycle::Validated,
            runtime::L4CandidateLifecycle::Promoted,
        ]
    );
}

#[tokio::test]
async fn low_risk_team_knowledge_auto_promotes_only_with_independent_evidence() {
    let (_root, _memory, services) = services_with_memory().await;
    let promoted = services
        .l4_promotion_service()
        .govern(team_candidate(2))
        .await
        .expect("well-supported team knowledge");
    assert_eq!(promoted.state, runtime::L4CandidateLifecycle::Promoted);
    assert!(promoted.approval_id.is_none());

    let pending = services
        .l4_promotion_service()
        .govern(team_candidate(1))
        .await
        .expect("weakly-supported team knowledge enters review");
    assert_eq!(
        pending.state,
        runtime::L4CandidateLifecycle::AwaitingApproval
    );
    assert!(pending.approval_id.is_some());
}
