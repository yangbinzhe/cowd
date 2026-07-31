#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::fs;

use harness_contract::tool::{ToolExecutionAuthorization, ToolIdempotency, ToolPermissionMode};
use serde_json::json;
use tools::ToolHost;

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

    let host = ToolHost::builtin("readonly-test", &root);
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
    let root = std::env::temp_dir().join(format!("cowd-tool-deny-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temp root");
    let target = root.join("blocked.txt");

    let host = ToolHost::builtin("deny-test", &root);
    let input = json!({"path": target, "content": "must not be written"});
    let effect = host.pin_snapshot().describe_effect("write_file", &input);
    assert_eq!(
        effect.required_permission,
        ToolPermissionMode::WorkspaceWrite
    );
    assert!(
        !target.exists(),
        "Tools must not execute before Runtime authorizes"
    );
}

#[test]
fn live_tool_batch_readonly_accepts_read_tools_and_rejects_write_tools() {
    let root = std::env::temp_dir().join(format!("cowd-tool-batch-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temp root");
    let fixture = root.join("batch.txt");
    fs::write(&fixture, "batch evidence").expect("fixture write");

    let host = ToolHost::builtin("batch-test", &root);
    let lease = host.pin_snapshot();
    let read_batch = json!({
        "calls": [
            {
                "name": "read_file",
                "input": {"path": fixture, "offset": 0, "limit": 2000}
            }
        ],
        "max_concurrency": 2
    });
    let output = lease
        .execute(
            &authorize(&lease, "tool_batch_readonly", &read_batch),
            "tool_batch_readonly",
            &read_batch,
        )
        .expect("readonly batch should accept read_file");
    assert!(output.contains("batch evidence"));

    let write_batch = json!({
        "calls": [{"name": "write_file", "input": {"path": root.join("bad.txt"), "content": "no"}}]
    });
    let error = lease
        .execute(
            &authorize(&lease, "tool_batch_readonly", &write_batch),
            "tool_batch_readonly",
            &write_batch,
        )
        .expect_err("readonly batch should reject write_file before execution");
    assert!(error
        .to_string()
        .contains("only accepts approved read-only tools"));
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

    let host = ToolHost::builtin("boundary-test", &root);
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
    denied("write_file", json!({"path": outside, "content": "no"}));
    denied(
        "edit_file",
        json!({"path": ".cowd/runtime.sqlite", "old_string": "control", "new_string": "no"}),
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
