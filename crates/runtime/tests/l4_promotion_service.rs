#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use memory::config::{BudgetConfig, StoreConfig};
use memory::{CognitiveContextManager, MemoryConfig, MemoryLayer, MemoryScope, Priority};
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
    L4PromotionCandidate {
        candidate_id: "candidate-l4-service".to_string(),
        team_id: "team-l4-service".to_string(),
        graph_id: "team-graph:team-l4-service".to_string(),
        lineage_ref: "team-graph:team-l4-service:verify".to_string(),
        title: "Verified architecture decision".to_string(),
        content: "Use the reviewed Runtime Team instantiation path for durable Team work."
            .to_string(),
        scope: MemoryScope::TeamRun("team-l4-service".to_string()),
        source_evidence_refs: vec!["evidence:review:1".to_string()],
        priority: Priority::High,
        tags: vec!["decision".to_string()],
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
