use super::agentic_checkpoint_artifact_content;
use super::host_backend::{
    latest_agentic_content_draft, persist_agentic_content_draft,
    persist_agentic_content_draft_for_scope, resolve_explicit_agentic_content_refs,
    AgenticContentDraftScope,
};
use super::retain_agentic_program_checkpoint;

#[test]
fn worker_wait_requires_an_explicit_successful_inspection() {
    let mut call = explicit_content_commit_call("inspect");
    call.name = harness_contract::agent_action::STATE_INSPECT_TOOL_ID.into();
    call.input = "{}".into();
    let successful = [call.id.clone()].into_iter().collect();
    assert!(!super::host_backend::requested_agentic_worker_wait(
        std::slice::from_ref(&call),
        &successful
    ));
    call.input = r#"{"wait_for_workers":true}"#.into();
    assert!(super::host_backend::requested_agentic_worker_wait(
        std::slice::from_ref(&call),
        &successful
    ));
    assert!(!super::host_backend::requested_agentic_worker_wait(
        std::slice::from_ref(&call),
        &BTreeSet::new()
    ));
    call.name = harness_contract::agent_action::TASK_PUBLISH_TOOL_ID.into();
    assert!(!super::host_backend::requested_agentic_worker_wait(
        std::slice::from_ref(&call),
        &successful
    ));
}

#[tokio::test]
async fn current_program_evidence_survives_allocator_and_reaches_provider_request() {
    let mut program =
        crate::AgenticProgramProjection::empty("program:continuity", "objective:continuity");
    let mut persistent = Vec::new();
    retain_agentic_program_checkpoint(&mut persistent, &program, "SUPERSEDED_AGENT_FINDING".into());
    program.revision = 1;
    let finding = "CURRENT_AGENT_FINDING: gateway consumes the leaf contract";
    retain_agentic_program_checkpoint(&mut persistent, &program, finding.into());
    assert_eq!(persistent.len(), 1);

    // Each invocation constructs a real provider request through the same
    // context allocator and compiler as production, without a paid provider.
    for _ in 0..2 {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let runtime = crate::ConversationRuntime::new(
            Session::new(),
            IdentityRecordingClient {
                requests: Arc::clone(&requests),
            },
            NoopToolExecutor,
            PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
            canonical_host_system_prompt(vec!["Summarize supplied findings.".into()]),
        )
        .without_memory();
        // Independent pinned orientation already suffices for ordinary
        // coverage. It must not crowd out current collaboration truth.
        for index in 0..8 {
            runtime.push_external_context_item(ContextItem::new(
                format!("orientation-{index}"),
                ContextSourceKind::Workspace,
                ContextRole::Instruction,
                "Use the supplied evidence and complete the answer.",
            ));
        }
        for item in super::model_context_for_step(Vec::new(), &persistent) {
            runtime.push_next_model_context_item(item);
        }
        let services = crate::RuntimeServices::in_memory().expect("runtime");
        let (_, result) = submit_test_owned_conversation_turn(
            runtime,
            services,
            "summarize findings",
            &SharedPrompter::none(),
            test_execution_lineage(),
        )
        .await;
        assert!(result.is_ok());
        let requests = requests.lock().expect("requests");
        let request = requests.first().expect("provider request");
        let packets = &request.prompt.contextual_packets;
        assert_eq!(
            packets
                .iter()
                .filter(|packet| packet.content.contains(finding))
                .count(),
            1
        );
        assert!(packets
            .iter()
            .all(|packet| !packet.content.contains("SUPERSEDED_AGENT_FINDING")));
    }
}

fn agentic_content_bridge_ticket(
    graph_id: &str,
    node_id: &str,
    attempt: u32,
) -> NodeExecutionTicket {
    NodeExecutionTicket {
        graph_id: graph_id.to_string(),
        node_id: node_id.to_string(),
        executor_kind: "inline_model".to_string(),
        service_class: harness_contract::execution_graph::ExecutionServiceClass::Interactive,
        attempt,
        idempotency_key: format!("{node_id}:{attempt}"),
        payload_ref: "{}".to_string(),
    }
}

fn explicit_content_commit_call(id: &str) -> ModelToolCall {
    ModelToolCall {
        id: id.to_string(),
        name: harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID.to_string(),
        input: serde_json::json!({
            "title": "authored report",
            "kind": "report",
            "content_ref": "current_message_block:0",
            "evidence_refs": [],
        })
        .to_string(),
        depends_on: Vec::new(),
    }
}

#[tokio::test]
async fn parent_checkpoint_receives_consumable_agent_artifact_content() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown; charset=utf-8".to_string(),
                visibility_scope: "session:parent-checkpoint".to_string(),
                expected_bytes: None,
                original_name: Some("agent-result.md".to_string()),
            },
            b"Agent finding: the dependency direction is valid.",
        )
        .await
        .expect("write agent result");

    let content = agentic_checkpoint_artifact_content(
        services.as_ref(),
        &artifact.selector,
        "session:parent-checkpoint",
    )
    .expect("checkpoint content");
    assert!(content.complete);
    assert_eq!(
        content.text,
        "Agent finding: the dependency direction is valid."
    );
    assert!(
        agentic_checkpoint_artifact_content(
            services.as_ref(),
            &artifact.selector,
            "session:unrelated"
        )
        .is_none(),
        "an artifact's own scope is not authorization for another Session"
    );
}

#[tokio::test]
async fn parent_checkpoint_preview_preserves_full_artifact_retrieval() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let body = format!("BEGIN{}END", "x".repeat(128 * 1024));
    let artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:preview".into(),
                expected_bytes: None,
                original_name: None,
            },
            body.as_bytes(),
        )
        .await
        .expect("write artifact");
    let preview = agentic_checkpoint_artifact_content(
        services.as_ref(),
        &artifact.selector,
        "session:preview",
    )
    .expect("preview");
    assert!(!preview.complete);
    assert!(preview.text.starts_with("BEGIN"));
    assert!(preview.text.ends_with("END"));
    assert!(preview.text.contains(&artifact.selector));
    assert!(preview.text.len() < 66 * 1024);
    let full = services
        .artifact_store()
        .read(&artifact, "session:preview", None)
        .await
        .expect("retrieve complete original");
    assert_eq!(full, body.as_bytes());
}

#[tokio::test]
async fn same_frame_text_and_artifact_commit_resolve_to_durable_content() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let ticket = agentic_content_bridge_ticket("bridge-graph", "bridge-model", 1);
    let message = ConversationMessage::assistant(vec![ContentBlock::Text {
        text: "# Evidence-backed report\n\nThe exact authored body.".to_string(),
    }]);
    let current = persist_agentic_content_draft(services.as_ref(), &ticket, &message)
        .await
        .expect("persist current content")
        .expect("current content ref");
    let mut calls = vec![explicit_content_commit_call("commit-current")];

    resolve_explicit_agentic_content_refs(services.as_ref(), &ticket, &message, &mut calls)
        .await
        .expect("resolve current content");

    let input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("resolved input");
    let content_ref = input["content_ref"].as_str().expect("content ref");
    assert_ne!(content_ref, "current_message_block:0");
    let _retained_draft = current;
    let artifact = services
        .artifact_store()
        .resolve(content_ref)
        .expect("durable artifact");
    let content = services
        .artifact_store()
        .read(&artifact, "execution:bridge-graph", None)
        .await
        .expect("read durable content");
    assert_eq!(
        String::from_utf8(content).expect("UTF-8"),
        "# Evidence-backed report\n\nThe exact authored body."
    );
}

#[tokio::test]
async fn tool_only_artifact_commit_never_selects_a_previous_draft() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let ticket = agentic_content_bridge_ticket("continuation-graph", "model-step-1", 2);
    let older = ConversationMessage::assistant(vec![ContentBlock::Text {
        text: "older draft".to_string(),
    }]);
    persist_agentic_content_draft(services.as_ref(), &ticket, &older)
        .await
        .expect("persist older draft");
    let latest = ConversationMessage::assistant(vec![ContentBlock::Text {
        text: "latest complete draft".to_string(),
    }]);
    let _retained = persist_agentic_content_draft(services.as_ref(), &ticket, &latest)
        .await
        .expect("persist latest draft")
        .expect("latest content ref");
    let mut calls = vec![explicit_content_commit_call("commit-later")];

    resolve_explicit_agentic_content_refs(
        services.as_ref(),
        &ticket,
        &ConversationMessage::assistant(vec![]),
        &mut calls,
    )
    .await
    .expect("resolve prior frame");

    let input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("resolved input");
    assert_eq!(
        input["content_ref"].as_str(),
        Some("current_message_block:0")
    );
}

#[tokio::test]
async fn durable_content_drafts_never_cross_actor_or_attempt_fences() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let actor_a = AgenticContentDraftScope {
        execution_id: "agent-execution".to_string(),
        node_id: "agent-node".to_string(),
        attempt: 3,
        actor_id: "agent-a:run-a".to_string(),
    };
    let actor_b = AgenticContentDraftScope {
        actor_id: "agent-b:run-b".to_string(),
        ..actor_a.clone()
    };
    let next_attempt = AgenticContentDraftScope {
        attempt: 4,
        ..actor_a.clone()
    };
    let content_ref = persist_agentic_content_draft_for_scope(
        services.as_ref(),
        &actor_a,
        "session:scope-test".to_string(),
        "private actor A draft",
    )
    .await
    .expect("persist actor A draft");

    assert_eq!(
        latest_agentic_content_draft(services.as_ref(), &actor_a)
            .expect("read actor A")
            .map(|draft| draft.content_ref),
        Some(content_ref)
    );
    assert_eq!(
        latest_agentic_content_draft(services.as_ref(), &actor_b).expect("read actor B"),
        None
    );
    assert_eq!(
        latest_agentic_content_draft(services.as_ref(), &next_attempt).expect("read next attempt"),
        None
    );
}

#[tokio::test]
async fn explicit_block_publication_excludes_preamble_and_rejects_out_of_range() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let ticket = agentic_content_bridge_ticket("selected-block-graph", "model", 1);
    let body = "<html>正文\r\n</html>\r\n";
    let message = ConversationMessage::assistant(vec![
        ContentBlock::Text {
            text: "I will prepare a report.".into(),
        },
        ContentBlock::Text { text: body.into() },
    ]);
    let mut calls = vec![explicit_content_commit_call("body")];
    calls[0].input = calls[0]
        .input
        .replace("current_message_block:0", "current_message_block:1");
    resolve_explicit_agentic_content_refs(services.as_ref(), &ticket, &message, &mut calls)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&calls[0].input).unwrap();
    let artifact = services
        .artifact_store()
        .resolve(value["content_ref"].as_str().unwrap())
        .unwrap();
    assert_eq!(
        services
            .artifact_store()
            .read(&artifact, "execution:selected-block-graph", None)
            .await
            .unwrap(),
        body.as_bytes()
    );
    let mut missing = vec![explicit_content_commit_call("missing")];
    missing[0].input = missing[0]
        .input
        .replace("current_message_block:0", "current_message_block:999");
    let unchanged = missing[0].input.clone();
    resolve_explicit_agentic_content_refs(services.as_ref(), &ticket, &message, &mut missing)
        .await
        .unwrap();
    assert_eq!(missing[0].input, unchanged);
}

#[tokio::test]
async fn growing_program_checkpoint_materializes_one_relevant_body_and_preserves_originals() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let mut program = crate::AgenticProgramProjection::empty("bounded-checkpoint", "original-goal");
    program.session_id = "bounded-checkpoint-session".into();
    program.objective_summary = "Preserve original evidence and investigate current gaps".into();
    let mut originals = Vec::new();
    for i in 0..80 {
        let body = format!("SOURCE-{i}:{}:EXACT-END-{i}", "evidence ".repeat(10_000));
        let artifact = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: format!("session:{}", program.session_id),
                    expected_bytes: Some(body.len() as u64),
                    original_name: None,
                },
                body.as_bytes(),
            )
            .await
            .unwrap();
        let business_ref = format!("artifact:checkpoint-{i:03}");
        program.artifacts.insert(
            business_ref.clone(),
            crate::AgenticArtifactProjection {
                artifact_ref: business_ref.clone(),
                content_ref: artifact.selector.clone(),
                title: format!("source {i}"),
                kind: "report".into(),
                relates_to: vec![],
                committed_by: "author".into(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        if i == 79 {
            program.final_artifact_ref = Some(business_ref);
        }
        originals.push((artifact, body));
    }
    let raw = super::compact_agentic_program_checkpoint(Some(&services), &program);
    let checkpoint: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(checkpoint["coverage"]["total"]["artifacts"], 80);
    assert_eq!(checkpoint["coverage"]["loaded"]["artifacts"], 32);
    assert_eq!(
        checkpoint["directory_request"],
        serde_json::json!({"name":"state_inspect","input":{}})
    );
    let artifacts = checkpoint["artifacts"].as_array().unwrap();
    assert_eq!(
        artifacts
            .iter()
            .filter(|a| a["content"].is_string())
            .count(),
        1
    );
    assert_eq!(
        artifacts[0]["artifact_ref"],
        program.final_artifact_ref.clone().unwrap()
    );
    assert_eq!(artifacts[0]["content_complete"], false);
    assert!(
        raw.len() < 100 * 1024,
        "checkpoint grew with all source bodies: {}",
        raw.len()
    );
    for (artifact, body) in &originals {
        let bytes = services
            .artifact_store()
            .read(artifact, &format!("session:{}", program.session_id), None)
            .await
            .unwrap();
        assert_eq!(bytes, body.as_bytes());
    }
    program.session_id = "different-private-session".into();
    let denied: serde_json::Value = serde_json::from_str(
        &super::compact_agentic_program_checkpoint(Some(&services), &program),
    )
    .unwrap();
    assert!(denied["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|a| a["content"].is_null()));
}
