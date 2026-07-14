#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use memory::store::{sqlite::SqliteStore, MemoryStore};
use memory::{L4PromotionCommand, MemoryLayer, MemoryOrchestrator, MemoryScope, Priority};

#[tokio::test]
async fn only_governed_l4_promotion_crosses_the_shared_persistent_boundary_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("shared-persistent-boundary.db");
    let id = {
        let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_path(&path).unwrap());
        let memory = MemoryOrchestrator::from_store(Default::default(), store, None).unwrap();
        assert!(memory
            .write(
                MemoryLayer::L4,
                memory::MemoryCategory::Shared,
                "direct",
                "must not cross boundary",
                Priority::Normal,
                memory::MemorySource::Import,
                Vec::new(),
                MemoryScope::TeamRun("team:boundary".to_string())
            )
            .await
            .is_err());
        memory
            .promote_l4(L4PromotionCommand {
                candidate_id: "candidate:team-boundary".to_string(),
                promotion_receipt: "receipt:team-boundary".to_string(),
                lineage_ref: "team-graph:boundary".to_string(),
                source_evidence_refs: vec!["evidence:team-boundary".to_string()],
                scope: MemoryScope::TeamRun("team:boundary".to_string()),
                title: "Approved shared conclusion".to_string(),
                content: "A governed team conclusion with durable evidence.".to_string(),
                priority: Priority::High,
                tags: vec!["team".to_string()],
            })
            .await
            .expect("governed promotion")
    };
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_path(&path).unwrap());
    let memory = MemoryOrchestrator::from_store(Default::default(), store, None).unwrap();
    let promoted = memory
        .recall(&id)
        .await
        .unwrap()
        .expect("persisted promoted entry");
    assert_eq!(promoted.layer, MemoryLayer::L4);
    assert_eq!(
        promoted.scope,
        MemoryScope::TeamRun("team:boundary".to_string())
    );
    assert!(promoted
        .tags
        .iter()
        .any(|tag| tag == "l4_promotion_receipt:receipt:team-boundary"));
}
