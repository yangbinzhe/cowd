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
        binding: None,
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

fn explicit_task_submit_call(id: &str) -> ModelToolCall {
    ModelToolCall {
        binding: None,
        id: id.to_string(),
        name: harness_contract::agent_action::TASK_SUBMIT_TOOL_ID.to_string(),
        input: serde_json::json!({
            "task_ref": "task:bound-report",
            "artifact_refs": [],
            "evidence_refs": ["tool://read-1"],
            "unresolved": [],
            "deliverable": {"block_index": 0, "kind": "report", "title": "Bound report"}
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
async fn one_shot_task_submit_recovers_the_previous_same_attempt_report_draft() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let ticket = agentic_content_bridge_ticket("one-shot-submit-graph", "agent", 1);
    let report = "# Research report\n\nThis is the exact authored report.";
    let input = recover_one_shot_report_draft(
        services.as_ref(),
        &ticket,
        "task:bound-report",
        "execution:one-shot-submit-graph",
        report,
    )
    .await;
    assert_eq!(input.task_ref, "task:bound-report");
}

async fn recover_one_shot_report_draft(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    task_ref: &str,
    visibility_scope: &str,
    report: &str,
) -> harness_contract::agent_action::TaskSubmitInput {
    let previous_message = ConversationMessage::assistant(vec![ContentBlock::Text {
        text: report.to_string(),
    }]);
    persist_agentic_content_draft(services, ticket, &previous_message)
        .await
        .expect("persist report draft");
    let mut calls = vec![explicit_task_submit_call("submit-previous-report")];
    let mut input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("submit input");
    input["task_ref"] = task_ref.into();
    input["evidence_refs"] = serde_json::json!([]);
    calls[0].input = input.to_string();

    resolve_explicit_agentic_content_refs(
        services,
        ticket,
        &ConversationMessage::assistant(vec![]),
        &mut calls,
    )
    .await
    .expect("resolve one-shot submission");

    let input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("resolved input");
    let content_ref = input["deliverable"]["resolved_content_ref"]
        .as_str()
        .expect("Runtime-bound content reference");
    let artifact = services
        .artifact_store()
        .resolve(content_ref)
        .expect("durable report artifact");
    let content = services
        .artifact_store()
        .read(&artifact, visibility_scope, None)
        .await
        .expect("read committed report");
    assert_eq!(String::from_utf8(content).expect("UTF-8"), report);
    assert_eq!(input["task_ref"], task_ref);
    serde_json::from_value(input).expect("typed one-shot submission")
}

#[tokio::test]
async fn durable_content_drafts_never_cross_actor_or_attempt_fences() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let actor_a = AgenticContentDraftScope {
        execution_id: "agent-execution".to_string(),
        node_id: "agent-node".to_string(),
        attempt: 3,
        actor_id: "agent-a:run-a".to_string(),
        task_id: Some("task:a".to_string()),
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
async fn whole_message_commit_selects_the_substantive_body() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let ticket = agentic_content_bridge_ticket("whole-message-graph", "model", 1);
    let body = format!("REPORT-BODY:{}:END", "evidence ".repeat(60));
    let message = ConversationMessage::assistant(vec![
        ContentBlock::Text {
            text: "I will prepare a report.".into(),
        },
        ContentBlock::Text { text: body.clone() },
    ]);
    let mut calls = vec![explicit_content_commit_call("body")];
    calls[0].input = calls[0]
        .input
        .replace("current_message_block:0", "message://current");
    resolve_explicit_agentic_content_refs(services.as_ref(), &ticket, &message, &mut calls)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&calls[0].input).unwrap();
    let content_ref = value["content_ref"].as_str().expect("resolved content ref");
    assert!(content_ref.starts_with("artifact://"), "got {content_ref}");
    let artifact = services.artifact_store().resolve(content_ref).unwrap();
    assert_eq!(
        services
            .artifact_store()
            .read(&artifact, "execution:whole-message-graph", None)
            .await
            .unwrap(),
        body.as_bytes()
    );
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

/// A body written in an earlier step of the same Task is still this Agent's deliverable.
///
/// Runtime resolved only the current response, so a model whose habit is "write the
/// report, then commit a few steps later" could never submit. In d6v10 one Agent wrote a
/// 3795-character synthesis report across three steps and lost all thirteen commits to
/// `content_ref_unresolved`, because no text block and no commit ever shared a step.
#[tokio::test]
async fn a_commit_recovers_the_body_written_in_an_earlier_step_of_the_same_task() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let ticket = agentic_content_bridge_ticket("continuation-graph", "model-step-1", 2);
    let older = ConversationMessage::assistant(vec![ContentBlock::Text {
        text: "# Synthesis report\n\nthe three facts, synthesized".to_string(),
    }]);
    persist_agentic_content_draft(services.as_ref(), &ticket, &older)
        .await
        .expect("persist the report written in an earlier step");
    // This step carries no text at all: the model is committing what it wrote before.
    let mut calls = vec![explicit_content_commit_call("commit-later")];
    calls[0].input = serde_json::json!({
        "title": "authored report",
        "kind": "report",
        "content_ref": "message://current",
        "evidence_refs": [],
    })
    .to_string();
    resolve_explicit_agentic_content_refs(
        services.as_ref(),
        &ticket,
        &ConversationMessage::assistant(vec![]),
        &mut calls,
    )
    .await
    .expect("resolve prior frame");

    let input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("resolved input");
    let content_ref = input["content_ref"]
        .as_str()
        .expect("resolved content ref")
        .to_string();
    assert!(
        content_ref.starts_with("artifact://"),
        "the earlier body must become a durable artifact: {content_ref}"
    );
    let artifact = services
        .artifact_store()
        .resolve(&content_ref)
        .expect("durable artifact");
    let content = services
        .artifact_store()
        .read(&artifact, "execution:continuation-graph", None)
        .await
        .expect("read durable content");
    assert_eq!(
        String::from_utf8(content).expect("UTF-8"),
        "# Synthesis report\n\nthe three facts, synthesized"
    );
}

/// The fallback is pinned to one Task. A body written for one Task is never delivered as
/// another Task's deliverable, which is the protection that keeps the recovery from
/// substituting unrelated content.
#[tokio::test]
async fn the_earlier_body_fallback_never_crosses_a_task_fence() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let ticket = agentic_content_bridge_ticket("continuation-graph", "model-step-1", 2);
    let mut scope = super::host_backend::AgenticContentDraftScope {
        execution_id: "agent-execution".to_string(),
        node_id: "agent-node".to_string(),
        attempt: 1,
        actor_id: "agent-a:run-a".to_string(),
        task_id: Some("task:a".to_string()),
    };
    super::host_backend::persist_agentic_content_draft_for_scope(
        services.as_ref(),
        &scope,
        "session:scope-test".to_string(),
        "body written for task A",
    )
    .await
    .expect("persist task A draft");

    scope.task_id = Some("task:b".to_string());
    let recovered = super::host_backend::latest_agentic_content_draft(services.as_ref(), &scope)
        .expect("read task B drafts");
    assert!(
        recovered.is_none(),
        "a body written for one Task must never be recovered for another"
    );
}

/// A delivered Task nobody is reviewing must be named, not inferred.
///
/// The root used to see these as ordinary `Submitted` rows, identical to one a
/// reviewer was already working. It therefore waited, polled, and gave up with
/// the work delivered and unverified - which is exactly how the run with two
/// `Submitted` Tasks and zero review dispatches ended.
#[tokio::test]
async fn a_submitted_task_with_no_reviewer_is_named_with_its_action() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let mut program = crate::AgenticProgramProjection::empty("awaiting-review", "original-goal");
    program.session_id = "awaiting-review-session".into();
    let base = crate::AgenticTaskProjection {
        task_id: "task:delivered".to_string(),
        team_id: "team:a".to_string(),
        title: "delivered work".to_string(),
        objective: "objective".to_string(),
        acceptance: "accepted evidence".to_string(),
        acceptance_checks: Vec::new(),
        criterion_source: Default::default(),
        required_capabilities: Vec::new(),
        depends_on: Vec::new(),
        status: crate::AgenticTaskStatus::Submitted,
        claimant: Some("agent:author".to_string()),
        claim_generation: 1,
        claim_execution_id: None,
        claimed_at_ms: None,
        lease_expires_at_ms: None,
        claim_renewal_streak: 0,
        active_attempts: Default::default(),
        artifact_refs: vec!["artifact:delivered".to_string()],
        evidence_refs: vec!["tool://receipt".to_string()],
        unresolved: Vec::new(),
        review_reason: None,
        reviewed_by: None,
        failed_attempts: 0,
        review_generation: 0,
        failed_review_attempts: 0,
        last_failure: None,
        replacement_task_refs: Vec::new(),
        supersede_evidence_refs: Vec::new(),
        superseded_reason: None,
        superseded_by: None,
        obligation_refs: Vec::new(),
        purpose: Default::default(),
        execution_requirements: Vec::new(),
        expertise_hints: Vec::new(),
        cancel_requested_by: None,
        cancel_reason_ref: None,
        cancel_evidence_refs: Vec::new(),
        pending_retirement: None,
    };
    // A Task a reviewer already holds is not waiting work.
    let mut under_review = base.clone();
    program.tasks.insert(base.task_id.clone(), base);
    under_review.task_id = "task:under-review".to_string();
    under_review.active_attempts.insert(
        "attempt:review".to_string(),
        crate::agentic::AgenticTaskAttemptProjection {
            execution_id: "run:review".to_string(),
            agent_id: "agent:reviewer".to_string(),
            membership_id: "membership:1".to_string(),
            mode: harness_contract::agent_action::AgentAttemptMode::Review,
            generation: 0,
        },
    );
    program
        .tasks
        .insert(under_review.task_id.clone(), under_review);

    let checkpoint: serde_json::Value = serde_json::from_str(
        &super::compact_agentic_program_checkpoint(Some(&services), &program),
    )
    .unwrap();
    let awaiting = checkpoint["awaiting_independent_review"]
        .as_array()
        .unwrap();
    assert_eq!(awaiting.len(), 1, "{awaiting:?}");
    assert_eq!(awaiting[0]["task_ref"], "task:delivered");
    assert_eq!(awaiting[0]["claimant"], "agent:author");
    let action = &checkpoint["awaiting_review_action"]["instruction"];
    let action = action.as_str().expect("an action must be named");
    assert!(
        action.contains("task:delivered") || action.contains("awaiting_independent_review"),
        "{action}"
    );
    assert!(action.contains("agent_invite"), "{action}");
    assert!(action.contains("different Agent"), "{action}");
}

/// Nothing waiting means no nagging: a checkpoint that always carries the action
/// would train the root to ignore it.
#[tokio::test]
async fn a_program_with_every_task_under_review_carries_no_action() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let mut program = crate::AgenticProgramProjection::empty("all-reviewed", "original-goal");
    program.session_id = "all-reviewed-session".into();
    let mut task = crate::AgenticTaskProjection {
        task_id: "task:1".to_string(),
        team_id: "team:a".to_string(),
        title: "under review".to_string(),
        objective: "objective".to_string(),
        acceptance: "accepted evidence".to_string(),
        acceptance_checks: Vec::new(),
        criterion_source: Default::default(),
        required_capabilities: Vec::new(),
        depends_on: Vec::new(),
        status: crate::AgenticTaskStatus::Submitted,
        claimant: Some("agent:author".to_string()),
        claim_generation: 1,
        claim_execution_id: None,
        claimed_at_ms: None,
        lease_expires_at_ms: None,
        claim_renewal_streak: 0,
        active_attempts: Default::default(),
        artifact_refs: vec!["artifact:1".to_string()],
        evidence_refs: vec![],
        unresolved: Vec::new(),
        review_reason: None,
        reviewed_by: None,
        failed_attempts: 0,
        review_generation: 0,
        failed_review_attempts: 0,
        last_failure: None,
        replacement_task_refs: Vec::new(),
        supersede_evidence_refs: Vec::new(),
        superseded_reason: None,
        superseded_by: None,
        obligation_refs: Vec::new(),
        purpose: Default::default(),
        execution_requirements: Vec::new(),
        expertise_hints: Vec::new(),
        cancel_requested_by: None,
        cancel_reason_ref: None,
        cancel_evidence_refs: Vec::new(),
        pending_retirement: None,
    };
    task.active_attempts.insert(
        "attempt:review".to_string(),
        crate::agentic::AgenticTaskAttemptProjection {
            execution_id: "run:review".to_string(),
            agent_id: "agent:reviewer".to_string(),
            membership_id: "membership:1".to_string(),
            mode: harness_contract::agent_action::AgentAttemptMode::Review,
            generation: 0,
        },
    );
    program.tasks.insert(task.task_id.clone(), task);

    let checkpoint: serde_json::Value = serde_json::from_str(
        &super::compact_agentic_program_checkpoint(Some(&services), &program),
    )
    .unwrap();
    assert!(checkpoint["awaiting_independent_review"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(checkpoint["awaiting_review_action"].is_null());
}
