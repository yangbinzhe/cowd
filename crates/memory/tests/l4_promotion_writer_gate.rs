#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Regression gate for the L4 single-writer boundary.

use std::sync::Arc;

use chrono::Utc;
use memory::store::{sqlite::SqliteStore, MemoryStore};
use memory::{
    AgentVisibility, L4PromotionCommand, MemoryCategory, MemoryEntry, MemoryError, MemoryLayer,
    MemoryOrchestrator, MemoryScope, MemorySource, Priority,
};

fn entry(layer: MemoryLayer) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category: MemoryCategory::Shared,
        priority: Priority::High,
        source: MemorySource::Import,
        title: "governed-team-decision".to_string(),
        content: "Evidence-backed decision retained after Team completion.".to_string(),
        embedding: None,
        tags: vec!["team".to_string()],
        relations: Vec::new(),
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::TeamRun("team-run-writer-gate".to_string()),
        session_id: Some("session-writer-gate".to_string()),
        source_agent: Some("agent-writer-gate".to_string()),
        visibility: AgentVisibility::Shared,
    }
}

#[tokio::test]
async fn ordinary_writers_cannot_bypass_the_governed_l4_promotion_command() {
    let store: Arc<dyn MemoryStore> = Arc::new(SqliteStore::open_in_memory().expect("sqlite"));
    let mut config = memory::MemoryConfig::default();
    config.layers.l4_enabled = true;
    let orchestrator = MemoryOrchestrator::from_store(config, store, None).expect("orchestrator");

    assert!(matches!(
        orchestrator.remember(entry(MemoryLayer::L4)).await,
        Err(MemoryError::WriteDenied { .. })
    ));
    assert!(matches!(
        orchestrator
            .write(
                MemoryLayer::L4,
                MemoryCategory::Shared,
                "attempted-direct-write",
                "must be rejected",
                Priority::Normal,
                MemorySource::Import,
                Vec::new(),
                MemoryScope::TeamRun("team-run-writer-gate".to_string()),
            )
            .await,
        Err(MemoryError::WriteDenied { .. })
    ));

    let id = orchestrator
        .promote_l4(L4PromotionCommand {
            candidate_id: "candidate-writer-gate".to_string(),
            promotion_receipt: "receipt-writer-gate".to_string(),
            lineage_ref: "team-graph:writer-gate".to_string(),
            source_evidence_refs: vec!["evidence:writer-gate".to_string()],
            scope: MemoryScope::TeamRun("team-run-writer-gate".to_string()),
            title: "governed-team-decision".to_string(),
            content: "Evidence-backed decision retained after Team completion.".to_string(),
            priority: Priority::High,
            tags: vec!["governed".to_string()],
        })
        .await
        .expect("typed promotion");
    let promoted = orchestrator
        .recall(&id)
        .await
        .expect("recall")
        .expect("promoted entry");
    assert_eq!(promoted.layer, MemoryLayer::L4);
    assert!(promoted
        .tags
        .iter()
        .any(|tag| tag == "l4_promotion_receipt:receipt-writer-gate"));
}
