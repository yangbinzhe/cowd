use chrono::Utc;
use memory::store::session::SessionRecord;
use memory::{
    AgentVisibility, CognitiveContextManager, FactChecker, MemoryCategory, MemoryConfig,
    MemoryEntry, MemoryLayer, MemoryScope, MemorySource, Priority, RuntimeEvent,
    RuntimeEventScope, RuntimeRef, UnifiedSessionStore,
};
use runtime::agent_protocol::{AgentEvidence, AgentReview, ReviewVerdict};
use runtime::doc_ingestion::{DocumentCategory, DocumentIngestor};
use runtime::iacc::{
    IaccIncident, IaccMetricStatus, IaccStore, plan_server_manufacturing_skills,
    run_server_manufacturing_skill,
};
use runtime::platform::feishu::doc::{
    DocumentContent, DocumentElement, DocumentMetadata as FeishuDocumentMetadata, DocumentType,
    ListItem, TableCell, TableRow, TextElement,
};

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
    let text = |value: &str| TextElement {
        text: value.to_string(),
        style: None,
    };
    DocumentContent {
        metadata: FeishuDocumentMetadata {
            token: "doc-full-capability-eval".to_string(),
            doc_type: DocumentType::Doc,
            title: "Architecture Manufacturing Memory Evaluation".to_string(),
            owner_open_id: "user-full-capability-eval".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            is_folder: false,
            parent_token: None,
        },
        elements: vec![
            DocumentElement::Heading {
                level: 1,
                elements: vec![text("Architecture: Manufacturing memory and IACC evaluation")],
                style: None,
            },
            DocumentElement::Paragraph {
                elements: vec![text(
                    "The GPU shortage eval requires material_shortage_risk, supplier_recovery, \
                     order_delivery_risk, and quality gate pass after incident analysis.",
                )],
                style: None,
            },
            DocumentElement::BulletList {
                items: vec![ListItem {
                    elements: vec![text(
                        "Planner, executor, and reviewer agents must share evidence through L4 memory.",
                    )],
                    children: vec![ListItem {
                        elements: vec![text(
                            "Reviewer must see document evidence and structured IACC evidence.",
                        )],
                        children: Vec::new(),
                    }],
                }],
            },
            DocumentElement::Table {
                rows: vec![
                    TableRow {
                        is_header: true,
                        cells: vec![
                            TableCell {
                                content: vec![DocumentElement::Paragraph {
                                    elements: vec![text("Metric")],
                                    style: None,
                                }],
                            },
                            TableCell {
                                content: vec![DocumentElement::Paragraph {
                                    elements: vec![text("Expected action")],
                                    style: None,
                                }],
                            },
                        ],
                    },
                    TableRow {
                        is_header: false,
                        cells: vec![
                            TableCell {
                                content: vec![DocumentElement::Paragraph {
                                    elements: vec![text("material_shortage_risk")],
                                    style: None,
                                }],
                            },
                            TableCell {
                                content: vec![DocumentElement::Paragraph {
                                    elements: vec![text("supplier_recovery")],
                                    style: None,
                                }],
                            },
                        ],
                    },
                ],
            },
        ],
        raw_blocks: vec![serde_json::json!({
            "source": "mock.complex_doc",
            "contains_table": true,
            "contains_nested_list": true
        })],
    }
}

fn document_text_for_memory(document: &DocumentContent) -> String {
    fn collect_element(element: &DocumentElement, out: &mut Vec<String>) {
        match element {
            DocumentElement::Paragraph { elements, .. }
            | DocumentElement::Heading { elements, .. }
            | DocumentElement::CodeBlock { elements, .. }
            | DocumentElement::Quote { elements } => {
                out.extend(elements.iter().map(|item| item.text.clone()));
            }
            DocumentElement::BulletList { items } | DocumentElement::NumberedList { items } => {
                for item in items {
                    out.extend(item.elements.iter().map(|text| text.text.clone()));
                    for child in &item.children {
                        out.extend(child.elements.iter().map(|text| text.text.clone()));
                    }
                }
            }
            DocumentElement::Table { rows } => {
                for row in rows {
                    for cell in &row.cells {
                        for nested in &cell.content {
                            collect_element(nested, out);
                        }
                    }
                }
            }
            DocumentElement::Image { .. } | DocumentElement::Divider => {}
        }
    }

    let mut parts = Vec::new();
    for element in &document.elements {
        collect_element(element, &mut parts);
    }
    parts.join("\n")
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
        metadata_json: Some(serde_json::json!({
            "workspace_root": "/tmp/cowd-full-capability-eval",
            "scenario": "document_memory_fact_session_agents_iacc"
        }).to_string()),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

#[tokio::test]
async fn cowd_full_capability_eval_covers_document_memory_fact_session_agents_and_iacc() {
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

    let iacc = IaccStore::in_memory().expect("iacc store opens");
    iacc.seed_server_manufacturing_domain()
        .expect("manufacturing seed runs");
    let recompute = iacc.recompute_metrics().expect("metrics recompute");
    let shortage_state = recompute
        .metric_states
        .iter()
        .find(|state| state.metric_id == "material_shortage_risk")
        .expect("shortage metric state exists");
    assert_eq!(shortage_state.status, IaccMetricStatus::Critical);

    let attention = recompute
        .attention
        .iter()
        .find(|item| item.title.contains("material_shortage_risk"))
        .expect("shortage attention exists");
    let packet = iacc
        .build_evidence_packet(
            Some(&attention.attention_id),
            Some("Full capability eval GPU shortage"),
        )
        .expect("evidence packet builds");
    let mut incident = IaccIncident::new("Full capability eval GPU shortage");
    incident.attention_id = packet.attention_id.clone();
    incident.evidence_packet_id = Some(packet.packet_id.clone());
    iacc.create_incident(&incident).expect("incident creates");
    let analysis = iacc
        .analyze_incident(&incident.incident_id)
        .expect("incident analyzes");
    assert_eq!(analysis.attribution_candidates[0].cause_type, "supply_constraint");
    assert_eq!(analysis.recommended_actions[0].action_type, "supplier_recovery");
    let gate = iacc
        .evaluate_evidence_quality(&packet.packet_id)
        .expect("quality gate evaluates");
    assert_eq!(gate.decision, "pass");
    let updated_packet = iacc
        .get_evidence_packet(&packet.packet_id)
        .expect("updated packet loads")
        .expect("updated packet exists");

    let skill_plan =
        plan_server_manufacturing_skills(&incident, Some(&analysis), Some(&updated_packet), 3);
    assert!(!skill_plan.planned_agent_nodes.is_empty());
    assert!(
        skill_plan
            .selected_skills
            .iter()
            .any(|skill| skill.input_metric_keys.iter().any(|metric| metric == "material_shortage_risk")
                || skill.output_actions.iter().any(|action| action == "supplier_recovery"))
    );
    let selected_skill = skill_plan
        .selected_skills
        .first()
        .expect("skill plan selects at least one skill");
    let skill_run = run_server_manufacturing_skill(
        &incident,
        selected_skill,
        Some(&analysis),
        Some(&updated_packet),
    );
    assert_eq!(skill_run.status, "completed");
    assert_eq!(
        skill_run.structured_report["quality_gate_status"],
        serde_json::json!("pass")
    );

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
            id: "evidence-executor-iacc".to_string(),
            node_id: "agent-executor".to_string(),
            kind: "iacc_analysis".to_string(),
            reference: format!("iacc:analysis:{}", analysis.analysis_id),
            summary: "Executor generated IACC shortage attribution and supplier recovery action."
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
        comment: "All evidence, memory, session, and IACC checks are connected.".to_string(),
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
    assert!(timeline.events.iter().any(|event| event.kind == "agent.evidence"));
    assert!(timeline.events.iter().any(|event| event.kind == "agent.review"));

    let associated = sessions
        .get_session_memories(session_id)
        .await
        .expect("session memories load");
    assert!(associated.contains(&document_memory_id.to_string()));

    let reviewer_memory = memory_entry(
        "Reviewer accepted full capability eval",
        &format!(
            "review={} fact_check_consistent={} analysis={} gate={}",
            review.id, fact_result.is_consistent, analysis.analysis_id, gate.decision
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
    assert!(prepared.entries.iter().any(|entry| entry.id == reviewer_memory_id));
    assert!(prepared.entries.iter().any(|entry| {
        entry.title.contains("full capability")
            || entry.title.contains("Full capability")
            || entry.content.contains("supplier_recovery")
            || entry.content.contains("Full capability")
            || entry.content.contains("full capability")
    }));
}
