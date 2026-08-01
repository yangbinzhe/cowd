#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use chrono::Utc;
use harness_contract::knowledge::{
    KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
};
use memory::config::StoreConfig;
use memory::{
    last_automatic_governance_report, run_automatic_governance, AgentVisibility,
    AutomaticGovernanceMode, CognitiveContextManager, DocumentContent, GovernanceConfig,
    KnowledgeFabric, MaintenanceCandidateFilter, MaintenanceCandidateKind,
    MaintenanceCandidateStatus, MemoryCategory, MemoryConfig, MemoryEntry, MemoryKernel,
    MemoryLayer, MemoryScope, MemorySource, MemoryState, MemoryTurnContext, Priority,
};

fn test_config(path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: path.to_path_buf(),
            blob_dir: path.parent().unwrap().join("blobs"),
            enable_vector_index: false,
            ..StoreConfig::default()
        },
        ..MemoryConfig::default()
    }
}

fn entry(source: MemorySource, title: &str, content: &str) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source,
        title: title.to_string(),
        content: content.to_string(),
        embedding: None,
        tags: vec!["automatic-governance-test".to_string()],
        relations: Vec::new(),
        confidence: 0.8,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("cowd".to_string()),
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::Shared,
    }
}

#[tokio::test]
async fn exact_duplicates_are_archived_once_and_the_decision_stays_applied() {
    let tmp = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("governance.db")))
            .await
            .unwrap(),
    );
    let first = entry(
        MemorySource::Import,
        "Release evidence",
        "Every release retains verified evidence.",
    );
    let duplicate = entry(
        MemorySource::Import,
        " Release evidence ",
        "Every release retains verified evidence.",
    );
    manager.orchestrator().store().insert(&first).await.unwrap();
    manager
        .orchestrator()
        .store()
        .insert(&duplicate)
        .await
        .unwrap();

    let report = run_automatic_governance(
        Arc::clone(&manager),
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();
    assert_eq!(report.auto_applied_duplicates, 1);
    assert_eq!(report.outcome, "succeeded");
    let persisted = last_automatic_governance_report(manager.as_ref())
        .await
        .unwrap()
        .expect("governance report must remain queryable after the run");
    assert_eq!(persisted.run_id, report.run_id);
    assert_eq!(persisted.outcome, "succeeded");
    assert!(persisted.fatal_error.is_none());
    assert_eq!(
        MemoryKernel::new(Arc::clone(&manager))
            .filter_active_entries(manager.list_all_entries().await.unwrap())
            .await
            .len(),
        1
    );
    assert_eq!(
        manager
            .list_memory_maintenance(MaintenanceCandidateFilter {
                kind: Some(MaintenanceCandidateKind::Duplicate),
                status: Some(MaintenanceCandidateStatus::Applied),
                ..MaintenanceCandidateFilter::default()
            })
            .unwrap()
            .len(),
        1
    );

    let repeated = run_automatic_governance(
        Arc::clone(&manager),
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();
    assert_eq!(repeated.auto_applied_duplicates, 0);
    assert_eq!(
        manager
            .list_memory_maintenance(MaintenanceCandidateFilter {
                kind: Some(MaintenanceCandidateKind::Duplicate),
                status: Some(MaintenanceCandidateStatus::Applied),
                ..MaintenanceCandidateFilter::default()
            })
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn authority_resolves_only_unambiguous_conflicts() {
    let tmp = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("authority.db")))
            .await
            .unwrap(),
    );
    let mut inferred = entry(
        MemorySource::AutoExtracted,
        "Runtime provider",
        "Use provider A.",
    );
    inferred.confidence = 0.2;
    inferred.source_agent = Some("agent-researcher".to_string());
    let inferred_id = inferred.id;
    let confirmed = entry(
        MemorySource::UserExplicit,
        "Runtime provider",
        "Use provider B.",
    );
    manager
        .orchestrator()
        .store()
        .insert(&inferred)
        .await
        .unwrap();
    manager
        .orchestrator()
        .store()
        .insert(&confirmed)
        .await
        .unwrap();

    let report = run_automatic_governance(
        Arc::clone(&manager),
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();
    assert_eq!(report.auto_resolved_conflicts, 1);
    assert_eq!(
        MemoryKernel::new(Arc::clone(&manager))
            .lifecycle_events(inferred_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        MemoryState::Superseded
    );

    let mut first = entry(MemorySource::UserExplicit, "Approval mode", "Use pending.");
    first.confidence = 0.2;
    let second = entry(
        MemorySource::UserExplicit,
        "Approval mode",
        "Use continue safely.",
    );
    manager.orchestrator().store().insert(&first).await.unwrap();
    manager
        .orchestrator()
        .store()
        .insert(&second)
        .await
        .unwrap();

    let ambiguous = run_automatic_governance(
        Arc::clone(&manager),
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();
    assert_eq!(ambiguous.auto_resolved_conflicts, 0);
    assert!(ambiguous.pending_human_review >= 1);
    assert!(manager
        .list_memory_maintenance(MaintenanceCandidateFilter {
            kind: Some(MaintenanceCandidateKind::Conflict),
            status: Some(MaintenanceCandidateStatus::Open),
            ..MaintenanceCandidateFilter::default()
        })
        .unwrap()
        .iter()
        .any(|candidate| candidate.entry_ids.contains(&first.id)
            && candidate.entry_ids.contains(&second.id)));
}

#[tokio::test]
async fn fatal_scan_failure_persists_the_current_failed_run_report() {
    let tmp = tempfile::tempdir().unwrap();
    let database = tmp.path().join("governance-failure.db");
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&database))
            .await
            .unwrap(),
    );
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute_batch("DROP TABLE memories")
        .unwrap();

    let error = run_automatic_governance(
        Arc::clone(&manager),
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .expect_err("a missing canonical memory table must fail the run");
    let persisted = last_automatic_governance_report(manager.as_ref())
        .await
        .unwrap()
        .expect("the failed run report must survive independently of the scan table");

    let error = error.to_string();
    assert_eq!(persisted.outcome, "failed");
    assert_eq!(persisted.fatal_error.as_deref(), Some(error.as_str()));
    assert!(persisted.completed_at >= persisted.started_at);
    assert!(persisted
        .errors
        .iter()
        .any(|item| item == &format!("fatal: {error}")));
}

#[tokio::test]
async fn obsolete_open_candidates_leave_the_human_review_queue() {
    let tmp = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("obsolete.db")))
            .await
            .unwrap(),
    );
    let first = entry(MemorySource::Import, "Old import", "Retained evidence.");
    let duplicate = entry(MemorySource::Import, "Old import", "Retained evidence.");
    for item in [&first, &duplicate] {
        manager.orchestrator().store().insert(item).await.unwrap();
    }
    manager
        .scan_memory_maintenance(Default::default())
        .await
        .unwrap();
    let kernel = MemoryKernel::new(Arc::clone(&manager));
    let context = MemoryTurnContext::new("governance-test", "system");
    kernel
        .archive(&context, first.id, "test evidence retired")
        .await
        .unwrap();
    kernel
        .archive(&context, duplicate.id, "test evidence retired")
        .await
        .unwrap();

    let report = run_automatic_governance(
        Arc::clone(&manager),
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();

    assert_eq!(report.auto_dismissed_obsolete, 1);
    assert!(manager
        .list_memory_maintenance(MaintenanceCandidateFilter {
            kind: Some(MaintenanceCandidateKind::Duplicate),
            status: Some(MaintenanceCandidateStatus::Open),
            ..MaintenanceCandidateFilter::default()
        })
        .unwrap()
        .is_empty());
    assert_eq!(manager.list_all_entries().await.unwrap().len(), 2);
}

#[tokio::test]
async fn actionable_knowledge_conflicts_are_counted_as_human_review() {
    let tmp = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("knowledge-conflict.db")))
            .await
            .unwrap(),
    );
    let knowledge = KnowledgeFabric::new();
    let receipt = knowledge.ingest_document(
        KnowledgeNamespace::Project("cowd".to_string()),
        KnowledgeActivationPolicy::DefaultForProjectGroup,
        KnowledgeGovernanceLevel::Required,
        DocumentContent::new(
            "Conflicting retention rule",
            "must retain transient payload\nmust not retain transient payload",
        ),
    );
    assert_eq!(receipt.conflicts.len(), 1);

    let report = run_automatic_governance(
        manager,
        Some(&knowledge),
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();

    assert_eq!(report.pending_human_review, 1);
    assert_eq!(
        report.pending_knowledge_conflict_ids,
        vec![receipt.conflicts[0].conflict_id.clone()]
    );
}

#[tokio::test]
async fn paged_full_scan_detects_duplicates_across_storage_page_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        CognitiveContextManager::new(test_config(&tmp.path().join("paged-governance.db")))
            .await
            .unwrap(),
    );
    let now = Utc::now();
    let mut newest = entry(
        MemorySource::Import,
        "Cross-page duplicate",
        "This exact evidence must be compared across scan pages.",
    );
    newest.updated_at = now;
    newest.created_at = now;
    manager
        .orchestrator()
        .store()
        .insert(&newest)
        .await
        .unwrap();

    for index in 0..749 {
        let mut item = entry(
            MemorySource::Import,
            &format!("Unique memory {index}"),
            &format!("Unique governed evidence {index}"),
        );
        item.updated_at = now - chrono::Duration::seconds(index + 1);
        item.created_at = item.updated_at;
        manager.orchestrator().store().insert(&item).await.unwrap();
    }

    let mut oldest = entry(
        MemorySource::Import,
        " Cross-page duplicate ",
        "This exact evidence must be compared across scan pages.",
    );
    oldest.updated_at = now - chrono::Duration::seconds(1_000);
    oldest.created_at = oldest.updated_at;
    manager
        .orchestrator()
        .store()
        .insert(&oldest)
        .await
        .unwrap();

    let report = run_automatic_governance(
        manager,
        None,
        &GovernanceConfig::default(),
        AutomaticGovernanceMode::Manual,
    )
    .await
    .unwrap();

    assert_eq!(report.scanned_entries, 751);
    assert_eq!(report.active_entries, 751);
    assert_eq!(report.auto_applied_duplicates, 1);
    assert!(
        report.duration_ms < 10_000,
        "a 751-entry governance snapshot took {} ms",
        report.duration_ms
    );
    assert_eq!(report.discovered_by_kind.get("duplicate").copied(), Some(1));
}
