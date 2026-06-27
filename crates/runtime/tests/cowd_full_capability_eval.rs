use chrono::Utc;
use memory::store::session::SessionRecord;
use memory::{
    AgentVisibility, CognitiveContextManager, FactChecker, MemoryCategory, MemoryConfig,
    MemoryEntry, MemoryLayer, MemoryScope, MemorySource, Priority, RuntimeEvent, RuntimeEventScope,
    RuntimeRef, UnifiedSessionStore,
};
use memory::{DocumentCategory, DocumentContent, DocumentIngestor};
use runtime::agent_protocol::{AgentEvidence, AgentReview, ReviewVerdict};

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
    let memory = CognitiveContextManager::new(memory_config(&tmp.path().join("memory.db")))
        .await
        .expect("memory manager opens");
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
        AgentEvidence {
            id: "evidence-planner-doc".to_string(),
            node_id: "agent-planner".to_string(),
            kind: "complex_document".to_string(),
            reference: format!("memory:{document_memory_id}"),
            summary: "Planner extracted manufacturing facts from complex document.".to_string(),
            created_at_ms: 1_000,
        },
        AgentEvidence {
            id: "evidence-executor-structured".to_string(),
            node_id: "agent-executor".to_string(),
            kind: "structured_evidence".to_string(),
            reference: "structured-evidence:shortage-risk-packet".to_string(),
            summary: "Executor linked structured shortage evidence and supplier recovery action."
                .to_string(),
            created_at_ms: 2_000,
        },
        AgentEvidence {
            id: "evidence-reviewer-fact-check".to_string(),
            node_id: "agent-reviewer".to_string(),
            kind: "fact_check".to_string(),
            reference: "fact-check:triple-full-capability-conflict".to_string(),
            summary: "Reviewer detected contradictory supplier recovery status.".to_string(),
            created_at_ms: 3_000,
        },
    ];
    let review = AgentReview {
        id: "review-full-capability".to_string(),
        node_id: "agent-reviewer".to_string(),
        reviewer: "agent-reviewer".to_string(),
        verdict: ReviewVerdict::Accept,
        comment: "All evidence, memory, session, and structured checks are connected.".to_string(),
        created_at_ms: 4_000,
    };

    let mut events = Vec::new();
    for (index, evidence) in agent_evidence.iter().enumerate() {
        let mut event = RuntimeEvent::new(
            session_id,
            index + 1,
            RuntimeEventScope::Agent,
            "agent.evidence",
            serde_json::to_value(evidence).expect("agent evidence serializes"),
            evidence.created_at_ms,
        );
        event.status = Some("ok".to_string());
        event.refs = vec![
            RuntimeRef {
                ref_type: "agent_node".to_string(),
                id: evidence.node_id.clone(),
                label: Some(evidence.kind.clone()),
            },
            RuntimeRef {
                ref_type: "evidence".to_string(),
                id: evidence.id.clone(),
                label: Some(evidence.summary.clone()),
            },
        ];
        sessions
            .append_runtime_event(&event)
            .await
            .expect("agent evidence event appends");
        events.push(event);
    }

    let mut review_event = RuntimeEvent::new(
        session_id,
        10,
        RuntimeEventScope::Agent,
        "agent.review",
        serde_json::to_value(&review).expect("review serializes"),
        review.created_at_ms,
    );
    review_event.status = Some("accepted".to_string());
    review_event.refs = vec![RuntimeRef {
        ref_type: "agent_review".to_string(),
        id: review.id.clone(),
        label: Some("full capability accepted".to_string()),
    }];
    sessions
        .append_runtime_event(&review_event)
        .await
        .expect("review event appends");

    let timeline = sessions
        .timeline_events_page(session_id, 0, 20)
        .await
        .expect("timeline loads");
    assert_eq!(timeline.total, 4);
    assert!(timeline
        .events
        .iter()
        .any(|event| event.kind == "agent.evidence"));
    assert!(timeline
        .events
        .iter()
        .any(|event| event.kind == "agent.review"));

    let associated = sessions
        .get_session_memories(session_id)
        .await
        .expect("session memories load");
    assert!(associated.contains(&document_memory_id.to_string()));

    let reviewer_memory = memory_entry(
        "Reviewer accepted full capability eval",
        &format!(
            "review={} fact_check_consistent={} structured_ref={}",
            review.id, fact_result.is_consistent, "structured-evidence:shortage-risk-packet"
        ),
        MemoryLayer::L4,
        MemoryCategory::Decision,
        Some(session_id.to_string()),
        Some("agent-reviewer".to_string()),
        vec!["review".to_string(), "full-capability".to_string()],
    );
    let reviewer_memory_id = reviewer_memory.id;
    memory
        .remember(reviewer_memory)
        .await
        .expect("reviewer memory stores");

    let prepared = memory
        .prepare_context("full capability eval supplier_recovery", &[], None)
        .await
        .expect("context prepares");
    assert!(prepared
        .entries
        .iter()
        .any(|entry| entry.id == reviewer_memory_id));
    assert!(prepared.entries.iter().any(|entry| {
        entry.title.contains("full capability")
            || entry.title.contains("Full capability")
            || entry.content.contains("supplier_recovery")
            || entry.content.contains("Full capability")
            || entry.content.contains("full capability")
    }));
}
