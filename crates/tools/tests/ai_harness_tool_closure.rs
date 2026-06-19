use std::fs;

use serde_json::json;
use tools::GlobalToolRegistry;

use runtime::permission_enforcer::PermissionEnforcer;
use runtime::{PermissionMode, PermissionPolicy};

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

    let mut registry = GlobalToolRegistry::builtin();
    registry.set_enforcer(PermissionEnforcer::new(
        PermissionPolicy::new(PermissionMode::ReadOnly)
            .with_tool_requirement("read_file", PermissionMode::ReadOnly),
    ));

    let output = registry
        .execute(
            "read_file",
            &json!({
                "path": fixture,
                "offset": 0,
                "limit": 2000
            }),
        )
        .expect("readonly read should succeed");
    assert!(output.contains("gpu-a"));
    assert!(output.contains("dual-source"));
}

#[test]
fn live_tool_permission_deny_blocks_workspace_write_under_readonly_policy() {
    let root = std::env::temp_dir().join(format!("cowd-tool-deny-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temp root");
    let target = root.join("blocked.txt");

    let mut registry = GlobalToolRegistry::builtin();
    registry.set_enforcer(PermissionEnforcer::new(
        PermissionPolicy::new(PermissionMode::ReadOnly)
            .with_tool_requirement("write_file", PermissionMode::WorkspaceWrite),
    ));

    let error = registry
        .execute(
            "write_file",
            &json!({
                "path": target,
                "content": "must not be written"
            }),
        )
        .expect_err("readonly policy should deny write_file");
    assert!(error.contains("requires workspace-write permission"));
}

#[test]
fn live_tool_batch_readonly_accepts_read_tools_and_rejects_write_tools() {
    let root = std::env::temp_dir().join(format!("cowd-tool-batch-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temp root");
    let fixture = root.join("batch.txt");
    fs::write(&fixture, "batch evidence").expect("fixture write");

    let mut registry = GlobalToolRegistry::builtin();
    registry.set_enforcer(PermissionEnforcer::new(
        PermissionPolicy::new(PermissionMode::ReadOnly)
            .with_tool_requirement("tool_batch_readonly", PermissionMode::ReadOnly)
            .with_tool_requirement("read_file", PermissionMode::ReadOnly)
            .with_tool_requirement("write_file", PermissionMode::WorkspaceWrite),
    ));

    let output = registry
        .execute(
            "tool_batch_readonly",
            &json!({
                "calls": [
                    {
                        "name": "read_file",
                        "input": {"path": fixture, "offset": 0, "limit": 2000}
                    }
                ],
                "max_concurrency": 2
            }),
        )
        .expect("readonly batch should accept read_file");
    assert!(output.contains("batch evidence"));

    let error = registry
        .execute(
            "tool_batch_readonly",
            &json!({
                "calls": [
                    {
                        "name": "write_file",
                        "input": {"path": root.join("bad.txt"), "content": "no"}
                    }
                ]
            }),
        )
        .expect_err("readonly batch should reject write_file before execution");
    assert!(error.contains("only accepts approved read-only tools"));
}
