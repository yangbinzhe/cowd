#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::fs;
use std::sync::Arc;

use harness_contract::tool::{ToolExecutionAuthorization, ToolIdempotency, ToolPermissionMode};
use serde_json::json;
use tools::ToolHost;

fn test_host(workspace_id: &str, root: &std::path::Path) -> ToolHost {
    ToolHost::builtin(workspace_id, root).with_authorization_lease_verifier(Arc::new(|lease| {
        lease.signature == "integration-signature"
    }))
}

fn authorize(
    lease: &tools::ToolHostLease,
    name: &str,
    input: &serde_json::Value,
) -> ToolExecutionAuthorization {
    let effect = lease.describe_effect(name, input);
    let idempotency_key = (effect.idempotency == ToolIdempotency::IdempotentWithKey)
        .then(|| format!("integration:{name}"));
    ToolExecutionAuthorization {
        request_id: format!("integration:{name}"),
        tool_id: name.to_string(),
        descriptor_hash: effect.descriptor_hash.clone(),
        policy_revision: 1,
        scope: effect.scopes[0].clone(),
        authorization_lease: harness_contract::policy::AuthorizationLease {
            lease_id: "integration-test".to_string(),
            principal_id: "integration".to_string(),
            parent_lease_id: None,
            capability: name.to_string(),
            scopes: effect.scopes.clone(),
            ceiling: effect.required_permission,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: idempotency_key.clone().unwrap_or_default(),
            policy_revision: 1,
            effect_descriptor_hash: effect.descriptor_hash.clone(),
            signature: "integration-signature".to_string(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        },
        timeout_lease: "timeout:30".to_string(),
        idempotency_key,
    }
}

#[test]
fn live_tool_readonly_closure_reads_fixture_and_returns_evidence() {
    let root = std::env::temp_dir().join(format!("cowd-tool-readonly-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temp root");
    let fixture = root.join("evidence.txt");
    fs::write(
        &fixture,
        "component=gpu-a\nrisk=shortage\nrecommendation=dual-source\n",
    )
    .expect("fixture write");

    let host = test_host("readonly-test", &root);
    let lease = host.pin_snapshot();
    let input = json!({"path": fixture, "offset": 0, "limit": 2000});
    let output = lease
        .execute(&authorize(&lease, "read_file", &input), "read_file", &input)
        .expect("readonly read should succeed");
    assert!(output.contains("gpu-a"));
    assert!(output.contains("dual-source"));
}

#[test]
fn live_tool_write_descriptor_requires_runtime_authorization() {
    let root = std::env::temp_dir().join(format!("cowd-tool-deny-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).expect("temp root");
    let target = root.join("blocked.txt");

    let host = test_host("deny-test", &root);
    let input = json!({"action": "write", "path": target, "content": "authorized write"});
    let lease = host.pin_snapshot();
    let effect = lease.describe_effect("edit", &input);
    assert_eq!(
        effect.required_permission,
        ToolPermissionMode::WorkspaceWrite
    );
    let mut denied = authorize(&lease, "edit", &input);
    denied.authorization_lease.ceiling = ToolPermissionMode::ReadOnly;
    let error = lease.execute(&denied, "edit", &input).unwrap_err();
    assert!(matches!(
        error,
        tools::ToolHostError::LeasePermissionDenied {
            authorized: ToolPermissionMode::ReadOnly,
            required: ToolPermissionMode::WorkspaceWrite,
        }
    ));
    assert!(
        !target.exists(),
        "Tools must not execute before Runtime authorizes"
    );
    lease
        .execute(&authorize(&lease, "edit", &input), "edit", &input)
        .expect("the same write must succeed with its required authorization");
    assert_eq!(fs::read_to_string(&target).unwrap(), "authorized write");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn orchestrate_escalates_to_its_writes_while_a_read_program_stays_readonly() {
    let root = std::env::temp_dir().join(format!("cowd-tool-batch-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).expect("temp root");
    let fixture = root.join("batch.txt");
    fs::write(&fixture, "batch evidence").expect("fixture write");

    let host = test_host("batch-test", &root);
    let lease = host.pin_snapshot();

    // A read-only program still reads, and its effect stays ReadOnly.
    let read_program = json!({
        "steps": [{"id": "r", "op": "call", "tool": "read_file", "input": {"path": fixture, "offset": 0, "limit": 2000}}]
    });
    let output = lease
        .execute(
            &authorize(&lease, "tool_orchestrate", &read_program),
            "tool_orchestrate",
            &read_program,
        )
        .expect("a read-only program must read");
    assert!(output.contains("batch evidence"));
    assert_eq!(
        lease
            .describe_effect("tool_orchestrate", &read_program)
            .required_permission,
        ToolPermissionMode::ReadOnly
    );

    // A write sub-call escalates the WHOLE program, so a read lease refuses it
    // before any effect runs - the orchestration never grants an extra
    // permission to its sub-calls.
    let write_program = json!({
        "steps": [{"id": "w", "op": "call", "tool": "edit", "input": {"action": "write", "path": root.join("bad.txt"), "content": "no"}}]
    });
    assert_eq!(
        lease
            .describe_effect("tool_orchestrate", &write_program)
            .required_permission,
        ToolPermissionMode::WorkspaceWrite,
        "a write sub-call must escalate the whole program"
    );
    let mut denied = authorize(&lease, "tool_orchestrate", &write_program);
    denied.authorization_lease.ceiling = ToolPermissionMode::ReadOnly;
    let error = lease
        .execute(&denied, "tool_orchestrate", &write_program)
        .unwrap_err();
    assert!(matches!(
        error,
        tools::ToolHostError::LeasePermissionDenied {
            authorized: ToolPermissionMode::ReadOnly,
            required: ToolPermissionMode::WorkspaceWrite,
        }
    ));
    assert!(!root.join("bad.txt").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn lease_blocks_external_and_control_plane_paths_for_workspace_file_tools() {
    let unique = format!("cowd-tool-boundary-{}", std::process::id());
    let root = std::env::temp_dir().join(&unique);
    let outside = std::env::temp_dir().join(format!("{unique}-outside.txt"));
    fs::create_dir_all(root.join(".cowd")).expect("workspace control plane");
    fs::write(root.join("visible.txt"), "workspace evidence").expect("workspace file");
    fs::write(root.join(".cowd").join("runtime.sqlite"), "control plane").expect("state");
    fs::write(&outside, "outside").expect("outside file");

    let host = test_host("boundary-test", &root);
    let lease = host.pin_snapshot();
    let denied = |name: &str, input: serde_json::Value| {
        let authorization = authorize(&lease, name, &input);
        let error = lease
            .execute(&authorization, name, &input)
            .expect_err("path should be denied");
        assert!(
            error.to_string().contains("workspace") || error.to_string().contains("control-plane"),
            "unexpected error for {name}: {error}"
        );
    };

    denied("read_file", json!({"path": "../outside.txt"}));
    denied(
        "edit",
        json!({"action": "write", "path": outside, "content": "no"}),
    );
    denied(
        "edit",
        json!({"action": "replace", "path": ".cowd/runtime.sqlite", "old_string": "control", "new_string": "no"}),
    );
    denied("glob_search", json!({"pattern": "*.txt", "path": ".cowd"}));
    denied(
        "grep_search",
        json!({"pattern": "control", "path": ".cowd"}),
    );
    denied(
        "workspace_snapshot",
        json!({"roots": ["../"], "includeGit": false}),
    );

    let _ = fs::remove_file(outside);
    let _ = fs::remove_dir_all(root);
}
