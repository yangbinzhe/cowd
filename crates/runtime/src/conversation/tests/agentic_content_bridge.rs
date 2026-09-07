use super::host_backend::{
    latest_agentic_content_draft, persist_agentic_content_draft,
    persist_agentic_content_draft_for_scope, resolve_preceding_agentic_content_refs,
    AgenticContentDraftScope,
};
use super::agentic_checkpoint_artifact_content;

fn agentic_content_bridge_ticket(graph_id: &str, node_id: &str, attempt: u32) -> NodeExecutionTicket {
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

fn preceding_content_commit_call(id: &str) -> ModelToolCall {
    ModelToolCall {
        id: id.to_string(),
        name: harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID.to_string(),
        input: serde_json::json!({
            "title": "authored report",
            "kind": "report",
            "content_ref": "preceding_content",
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

    let content = agentic_checkpoint_artifact_content(services.as_ref(), &artifact.selector)
        .expect("checkpoint content");
    assert!(content.complete);
    assert_eq!(
        content.text,
        "Agent finding: the dependency direction is valid."
    );
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
    let mut calls = vec![preceding_content_commit_call("commit-current")];

    resolve_preceding_agentic_content_refs(
        services.as_ref(),
        &ticket,
        Some(&current),
        &mut calls,
    )
    .await
    .expect("resolve current content");

    let input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("resolved input");
    let content_ref = input["content_ref"].as_str().expect("content ref");
    assert_eq!(content_ref, current);
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
async fn tool_only_artifact_commit_uses_latest_text_in_the_same_attempt() {
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
    let expected = persist_agentic_content_draft(services.as_ref(), &ticket, &latest)
        .await
        .expect("persist latest draft")
        .expect("latest content ref");
    let mut calls = vec![preceding_content_commit_call("commit-later")];

    resolve_preceding_agentic_content_refs(services.as_ref(), &ticket, None, &mut calls)
        .await
        .expect("resolve prior frame");

    let input: serde_json::Value = serde_json::from_str(&calls[0].input).expect("resolved input");
    assert_eq!(input["content_ref"].as_str(), Some(expected.as_str()));
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
        latest_agentic_content_draft(services.as_ref(), &next_attempt)
            .expect("read next attempt"),
        None
    );
}
