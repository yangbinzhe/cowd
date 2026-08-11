#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use chrono::Utc;
use harness_contract::{
    core::TaskRisk,
    execution::ExecutionIdentity,
    knowledge::{KnowledgeAuthority, KnowledgeCandidateScope, KnowledgeLineage, KnowledgeNovelty},
    reality::EvidenceRef,
    security::{PrincipalAssurance, PrincipalClaims, PrincipalKind},
};
use memory::{
    AgentVisibility, CognitiveContextManager, FactChecker, MemoryCategory, MemoryConfig,
    MemoryEntry, MemoryLayer, MemoryScope, MemorySource, MemoryTurnContext, Priority,
};
use memory::{DocumentCategory, DocumentContent, DocumentIngestor};
use runtime::{
    ApprovalDecisionCommand, ApprovalQueue, L4CandidateLifecycle, L4PromotionCandidate,
    L4PromotionService, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    VerifiedPrincipal,
};
use session::{SessionRecord, UnifiedSessionStore};
use std::sync::Arc;

fn memory_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: memory::config::StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
            blob_dir: sqlite_path.parent().unwrap().join("blobs"),
            enable_vector_index: false,
            cache_capacity: 256,
            ..Default::default()
        },
        budget: memory::config::BudgetConfig {
            context_window: 16_000,
            reserved_system: 2_000,
            reserved_response: 1_000,
            ..Default::default()
        },
        layers: memory::config::LayerConfig {
            l4_enabled: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn memory_entry(
    title: &str,
    content: &str,
    layer: MemoryLayer,
    category: MemoryCategory,
    session_id: Option<String>,
    source_agent: Option<String>,
    tags: Vec<String>,
) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category,
        priority: Priority::High,
        source: MemorySource::Import,
        title: title.to_string(),
        content: content.to_string(),
        embedding: None,
        tags,
        relations: Vec::new(),
        confidence: 0.95,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("cowd-full-capability-eval".to_string()),
        session_id,
        source_agent,
        visibility: AgentVisibility::Shared,
    }
}

fn complex_manufacturing_document() -> DocumentContent {
    DocumentContent {
        title: "Architecture Manufacturing Memory Evaluation".to_string(),
        body: [
            "Architecture: Manufacturing memory and structured evidence evaluation",
            "The GPU shortage eval requires material_shortage_risk, supplier_recovery, order_delivery_risk, and quality gate pass after incident analysis.",
            "Planner, executor, and reviewer agents must share evidence through L4 memory.",
            "Reviewer must see document evidence and structured runtime evidence.",
            "Metric: material_shortage_risk. Expected action: supplier_recovery.",
        ]
        .join("\n"),
        source: Some("mock.complex_doc".to_string()),
        author: Some("user-full-capability-eval".to_string()),
        created_at: Some(Utc::now().to_rfc3339()),
        modified_at: Some(Utc::now().to_rfc3339()),
        language: Some("en-US".to_string()),
    }
}

fn document_text_for_memory(document: &DocumentContent) -> String {
    document.body.clone()
}

fn session_record(session_id: &str) -> SessionRecord {
    let now = Utc::now().to_rfc3339();
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "cowd-eval".to_string(),
        chat_id: "full-capability-eval".to_string(),
        user_id: Some("user-eval".to_string()),
        model: Some("local-eval".to_string()),
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "manual".to_string(),
        metadata_json: Some(
            serde_json::json!({
                "workspace_root": "/tmp/cowd-full-capability-eval",
                "scenario": "document_memory_fact_session_agents_structured_evidence"
            })
            .to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

#[tokio::test]
async fn cowd_full_capability_eval_covers_document_memory_fact_session_agents_and_structured_evidence(
) {
    let tmp = tempfile::TempDir::new().expect("temp dir creates");
    let session_id = "session-full-capability-eval";
    let memory = Arc::new(
        CognitiveContextManager::new(memory_config(&tmp.path().join("memory.db")))
            .await
            .expect("memory manager opens"),
    );
    let sessions = UnifiedSessionStore::open_in_memory().expect("session store opens");
    sessions
        .create_session(&session_record(session_id))
        .await
        .expect("session creates");

    let document = complex_manufacturing_document();
    let ingestion = DocumentIngestor::new().ingest(&document);
    assert!(ingestion.success);
    assert_eq!(ingestion.metadata.category, DocumentCategory::Architecture);
    assert!(ingestion.layer >= 2);

    let document_memory = memory_entry(
        &ingestion.metadata.title,
        &document_text_for_memory(&document),
        MemoryLayer::L3,
        MemoryCategory::Reference,
        Some(session_id.to_string()),
        Some("agent-planner".to_string()),
        vec![
            "complex-doc".to_string(),
            "material_shortage_risk".to_string(),
            "supplier_recovery".to_string(),
        ],
    );
    let document_memory_id = document_memory.id;
    memory
        .remember(document_memory)
        .await
        .expect("complex document memory stores");
    sessions
        .associate_memory(session_id, &document_memory_id.to_string())
        .await
        .expect("session associates document memory");

    let recalled = memory
        .recall("material_shortage_risk supplier_recovery quality gate", 5)
        .await
        .expect("document memory recalls");
    assert!(
        recalled.iter().any(|entry| entry.id == document_memory_id),
        "complex document must be retrievable by extracted manufacturing facts"
    );

    let mut checker = FactChecker::new();
    checker.register_facts("gpu shortage eval", {
        let mut facts = memory::temporal_graph::EntityFacts::default();
        facts.entity_type = Some("manufacturing_incident".to_string());
        facts.parent = Some("supplier_recovery".to_string());
        facts
    });
    let contradiction = memory::temporal_graph::Triple {
        id: "triple-full-capability-conflict".to_string(),
        subject: "gpu shortage eval".to_string(),
        predicate: "child_of".to_string(),
        object: "ignore_supplier_recovery".to_string(),
        confidence: 1.0,
        valid_from: None,
        valid_until: None,
        source_memory_id: Some(document_memory_id),
        source_file: Some("mock.complex_doc".to_string()),
        source_agent: Some("agent-reviewer".to_string()),
    };
    let fact_result = checker.check_triple(&contradiction);
    assert!(!fact_result.is_consistent);
    assert!(fact_result.contradiction.is_some());

    let agent_evidence = [
        serde_json::json!({
            "id": "evidence-planner-doc",
            "node_id": "agent-planner",
            "kind": "complex_document",
            "reference": format!("memory:{document_memory_id}"),
            "summary": "Planner extracted manufacturing facts from complex document."
        }),
        serde_json::json!({
            "id": "evidence-executor-structured",
            "node_id": "agent-executor",
            "kind": "structured_evidence",
            "reference": "structured-evidence:shortage-risk-packet",
            "summary": "Executor linked structured shortage evidence and supplier recovery action."
        }),
        serde_json::json!({
            "id": "evidence-reviewer-fact-check",
            "node_id": "agent-reviewer",
            "kind": "fact_check",
            "reference": "fact-check:triple-full-capability-conflict",
            "summary": "Reviewer detected contradictory supplier recovery status."
        }),
    ];
    let review = serde_json::json!({
        "id": "review-full-capability",
        "node_id": "agent-reviewer",
        "reviewer": "agent-reviewer",
        "verdict": "accept",
        "comment": "All evidence, memory, session, and structured checks are connected."
    });

    let execution_events =
        Arc::new(RuntimeEventStore::open_in_memory().expect("runtime event store opens"));
    for evidence in &agent_evidence {
        execution_events
            .append(RuntimeEventInput {
                stream_id: session_id.to_string(),
                scope: RuntimeEventScope::Agent,
                kind: "agent.evidence".to_string(),
                status: Some("ok".to_string()),
                actor: Some("capability-eval".to_string()),
                refs: vec![
                    RuntimeEventRef {
                        kind: "agent_node".to_string(),
                        id: evidence["node_id"].as_str().unwrap().to_string(),
                    },
                    RuntimeEventRef {
                        kind: "evidence".to_string(),
                        id: evidence["id"].as_str().unwrap().to_string(),
                    },
                ],
                payload: evidence.clone(),
            })
            .expect("agent evidence event appends");
    }
    execution_events
        .append(RuntimeEventInput {
            stream_id: session_id.to_string(),
            scope: RuntimeEventScope::Agent,
            kind: "agent.review".to_string(),
            status: Some("accepted".to_string()),
            actor: Some("agent-reviewer".to_string()),
            refs: vec![RuntimeEventRef {
                kind: "agent_review".to_string(),
                id: review["id"].as_str().unwrap().to_string(),
            }],
            payload: review.clone(),
        })
        .expect("review event appends");

    let timeline = execution_events
        .list_stream(session_id)
        .expect("timeline loads");
    assert_eq!(timeline.len(), 4);
    assert!(timeline.iter().any(|event| event.kind == "agent.evidence"));
    assert!(timeline.iter().any(|event| event.kind == "agent.review"));

    let associated = sessions
        .get_session_memories(session_id)
        .await
        .expect("session memories load");
    assert!(associated.contains(&document_memory_id.to_string()));

    let approval_queue = Arc::new(ApprovalQueue::new(Arc::clone(&execution_events)));
    let l4_promotion = L4PromotionService::new(
        Arc::clone(&execution_events),
        Arc::clone(&approval_queue),
        Some(Arc::clone(&memory)),
    );
    let execution_identity = ExecutionIdentity::for_task_graph(
        "agent-reviewer",
        "cowd-full-capability-eval",
        "mission-full-capability-eval",
        "task-full-capability-eval",
        session_id,
        "turn-full-capability-eval",
        "graph-full-capability-eval",
    )
    .expect("review task graph identity");
    let reviewer_candidate = L4PromotionCandidate {
        candidate_id: "candidate-full-capability-review".to_string(),
        execution_identity,
        scope: KnowledgeCandidateScope::Workspace("cowd-full-capability-eval".to_string()),
        title: "Reviewer accepted full capability eval".to_string(),
        claim: format!(
            "review={} fact_check_consistent={} structured_ref={}",
            review["id"].as_str().unwrap(),
            fact_result.is_consistent,
            "structured-evidence:shortage-risk-packet"
        ),
        evidence_refs: agent_evidence
            .iter()
            .map(|evidence| {
                EvidenceRef::observed(
                    "agent_evidence",
                    evidence["id"].as_str().unwrap().to_string(),
                )
            })
            .collect(),
        authority: KnowledgeAuthority::WorkspaceVerified,
        lineage: KnowledgeLineage::default(),
        novelty: KnowledgeNovelty::New,
        risk: TaskRisk::Medium,
        tags: vec!["review".to_string(), "full-capability".to_string()],
        producer: "runtime.full_capability_eval".to_string(),
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ms: Utc::now().timestamp_millis().max(0) as u64,
    };
    let pending = l4_promotion
        .govern(reviewer_candidate.clone())
        .await
        .expect("workspace candidate enters governance");
    assert_eq!(
        pending.state,
        L4CandidateLifecycle::AwaitingApproval,
        "workspace knowledge must not bypass human approval"
    );
    let principal = VerifiedPrincipal::from_test_claims(PrincipalClaims {
        principal_id: "human:full-capability-reviewer".to_string(),
        kind: PrincipalKind::Human,
        scopes: vec!["workspace".to_string()],
        capabilities: vec!["approval.respond".to_string()],
        assurance: PrincipalAssurance::HumanInteractive,
        issuer: "runtime-test".to_string(),
        issued_at_ms: 1,
        expires_at_ms: None,
        credential_fingerprint: "runtime-test".to_string(),
        credential_epoch: 1,
        profile_revision: 1,
    });
    approval_queue
        .decide(
            &principal,
            ApprovalDecisionCommand {
                approval_id: pending.approval_id.expect("knowledge approval id"),
                approved: true,
                skip: false,
                reason: "verified full capability evidence".to_string(),
                scope: runtime::ApprovalGrantScope::Once,
                actor: harness_contract::policy::ApprovalDecisionActor {
                    kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                    actor_id: "human:full-capability-reviewer".to_string(),
                },
                evidence_refs: vec!["runtime.full_capability_eval".to_string()],
            },
        )
        .expect("knowledge approval succeeds");
    let reviewer_memory = l4_promotion
        .validate_and_promote(reviewer_candidate.clone())
        .await
        .expect("reviewer memory promotes through the governed L4 path");
    assert_eq!(reviewer_memory.lifecycle, L4CandidateLifecycle::Promoted);
    assert_eq!(
        l4_promotion
            .lifecycle(&reviewer_candidate)
            .expect("L4 promotion lifecycle loads"),
        vec![
            L4CandidateLifecycle::Proposed,
            L4CandidateLifecycle::Validated,
            L4CandidateLifecycle::AwaitingApproval,
            L4CandidateLifecycle::Approved,
            L4CandidateLifecycle::Promoted,
        ]
    );

    let shared_l4 = memory
        .orchestrator()
        .team_query(
            "full capability eval",
            Some(&MemoryScope::Project(
                "cowd-full-capability-eval".to_string(),
            )),
            5,
        )
        .await
        .expect("governed shared memory query succeeds");
    assert!(
        shared_l4
            .iter()
            .any(|entry| entry.id.to_string() == reviewer_memory.memory_id),
        "promoted L4 memory must be retrievable through the scoped Runtime-facing recall path"
    );

    let turn = MemoryTurnContext::new(session_id, "agent-reviewer")
        .with_project_id(Some("cowd-full-capability-eval".to_string()))
        .with_team_id(Some("team-full-capability-eval".to_string()));
    let prepared = memory
        .prepare_context_for_turn(
            &turn,
            "material_shortage_risk supplier_recovery quality gate",
            &[],
        )
        .await
        .expect("context prepares");
    assert!(prepared.entries.iter().any(|entry| {
        entry.title.contains("full capability")
            || entry.title.contains("Full capability")
            || entry.content.contains("supplier_recovery")
            || entry.content.contains("Full capability")
            || entry.content.contains("full capability")
    }));
}
