#![allow(clippy::expect_used, clippy::unwrap_used)]

use runtime::{tool_dispatch::ToolRequest, RuntimeToolExecutionRequest};

/// The Runtime-to-tool boundary must carry only the already-authorized
/// operation context.  A tool receives neither an authenticated principal nor
/// a credential/decision-lease payload from the conversation layer.
#[test]
fn tool_execution_contract_contains_operation_context_but_no_caller_identity() {
    let request = RuntimeToolExecutionRequest::from_tool_request(&ToolRequest {
        tool_use_id: "tool:v0-isolation".to_string(),
        tool_name: "read_file".to_string(),
        input: r#"{\"path\":\"README.md\"}"#.to_string(),
        depends_on: Vec::new(),
    });

    assert_eq!(request.idempotency_key, "tool:v0-isolation");
    assert_eq!(request.tool_use_id, "tool:v0-isolation");
    assert_eq!(request.tool_name, "read_file");
    assert!(request.session_id.is_none());
    assert!(request.model_lease.is_none());
    assert!(request.parent_execution.is_none());
    assert!(request.managed_invocation.is_none());

    let wire = serde_json::to_value(request).expect("tool execution wire contract");
    for forbidden in [
        "principal",
        "credential",
        "token",
        "decision_lease",
        "auth_broker",
        "event_store",
        "control_plane",
    ] {
        assert!(
            wire.get(forbidden).is_none(),
            "tool wire contract must not expose {forbidden}"
        );
    }
}
