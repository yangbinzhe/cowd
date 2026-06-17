use std::collections::BTreeSet;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_repo(path: &str) -> String {
    std::fs::read_to_string(repo_root().join(path)).expect("source file should read")
}

fn production_part(source: &str) -> &str {
    source.split("#[cfg(test)]").next().unwrap_or(source)
}

#[test]
fn daemon_module_is_only_a_runtime_host_transition_shim() {
    let source = read_repo("crates/cowd-cli/src/daemon/mod.rs");
    assert!(source.contains("delete_by: 0.9.293"));
    assert!(source.contains("pub(crate) use crate::runtime_host::*;"));
    assert!(!source.contains("fn "));
    assert!(!source.contains("struct "));
    assert!(!source.contains("enum "));
}

#[test]
fn runtime_host_owns_gateway_runtime_implementation() {
    let root = repo_root();
    assert!(root
        .join("crates/cowd-cli/src/runtime_host/mod.rs")
        .is_file());
    assert!(root
        .join("crates/cowd-cli/src/runtime_host/commands.rs")
        .is_file());
    let source = production_part(&read_repo("crates/cowd-cli/src/runtime_host/mod.rs")).to_string();
    assert!(source.contains("pub struct RuntimeHostConfig"));
    assert!(source.contains("pub async fn run_gateway_runtime"));
    assert!(!source.contains("pub struct DaemonConfig"));
    assert!(!source.contains("pub async fn run_daemon"));
}

#[test]
fn production_code_does_not_depend_on_daemon_module_except_transition_shim() {
    let files = [
        "crates/cowd-cli/src/main.rs",
        "crates/cowd-cli/src/api_routes.rs",
        "crates/cowd-cli/src/runtime_service.rs",
        "crates/cowd-cli/src/runtime_host/mod.rs",
        "crates/cowd-cli/src/runtime_host/commands.rs",
    ];
    for file in files {
        let full_source = read_repo(file);
        let source = production_part(&full_source);
        assert!(
            !source.contains("crate::daemon::"),
            "{file} must use crate::runtime_host directly"
        );
        assert!(
            !source.contains("DaemonConfig"),
            "{file} must not expose DaemonConfig"
        );
        assert!(
            !source.contains("run_daemon"),
            "{file} must not expose run_daemon"
        );
    }
}

#[test]
fn socket_transition_command_set_is_frozen_until_tui_http_migration() {
    let mut source = String::new();
    source.push_str(production_part(&read_repo(
        "crates/cowd-cli/src/runtime_host/mod.rs",
    )));
    source.push('\n');
    source.push_str(production_part(&read_repo(
        "crates/cowd-cli/src/runtime_host/commands.rs",
    )));

    let mut actual = BTreeSet::new();
    for needle in source.match_indices("Some(\"") {
        let start = needle.0 + "Some(\"".len();
        if let Some(end) = source[start..].find('"') {
            actual.insert(source[start..start + end].to_string());
        }
    }

    let expected = [
        "acquire_session_lease",
        "approval_pending",
        "approval_respond",
        "attach_session",
        "chat",
        "chat_stream",
        "connector_resource_list",
        "connector_resource_promote_memory",
        "connector_resource_revalidate",
        "context_snapshot",
        "create_session",
        "detach_session",
        "ensure_session",
        "list_sessions",
        "memory_status",
        "poll_events",
        "release_session_lease",
        "replay_session",
        "runtime.snapshot",
        "runtime.status",
        "runtime_snapshot",
        "session.attach",
        "session.detach",
        "session.lease.acquire",
        "session.lease.release",
        "session.lifecycle",
        "session.lifecycle.snapshot",
        "session.list",
        "session.replay",
        "status",
        "subscribe_session",
        "task_cancel",
        "task_complete",
        "task_list",
        "task_start",
        "tool_approve",
        "tool_deny",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);
    let transition_header = read_repo("crates/cowd-cli/src/runtime_host/commands.rs");
    assert!(transition_header.contains("delete_by: 0.9.293"));
}

#[test]
fn api_route_direct_dependencies_are_frozen_by_allowlist() {
    let allowlist = read_repo("crates/cowd-cli/src/api_routes/service_transition_allowlist.txt");

    let required_files = [
        "api_routes/agent_routes.rs",
        "api_routes/approval_routes.rs",
        "api_routes/audit_routes.rs",
        "api_routes/connector_routes.rs",
        "api_routes/context_routes.rs",
        "api_routes/cowd_routes.rs",
        "api_routes/matrix_mfg_routes.rs",
        "api_routes/memory_routes.rs",
        "api_routes/message_routes.rs",
        "api_routes/runtime_routes.rs",
        "api_routes/session_routes.rs",
        "api_routes/skill_routes.rs",
        "api_routes/system_routes.rs",
        "api_routes/task_routes.rs",
    ];
    for file in required_files {
        assert!(
            allowlist.contains(&format!("file={file}")),
            "missing route file in service transition allowlist: {file}"
        );
    }

    let required_entries = [
        (
            "api_routes/agent_routes.rs",
            "TaskKernel/AppState",
            "AgentService+TaskService",
            "0.9.296",
        ),
        (
            "api_routes/approval_routes.rs",
            "SmartApprovalGate/AppState",
            "ApprovalService",
            "0.9.296",
        ),
        (
            "api_routes/audit_routes.rs",
            "SmartApprovalGate/AppState",
            "AuditService",
            "0.9.296",
        ),
        (
            "api_routes/connector_routes.rs",
            "CognitiveContextManager",
            "ConnectorService+MemoryService",
            "0.9.296",
        ),
        (
            "api_routes/context_routes.rs",
            "SessionKernel/AppState",
            "ContextService",
            "0.9.296",
        ),
        (
            "api_routes/cowd_routes.rs",
            "MatrixStore::open",
            "MatrixService",
            "0.9.297",
        ),
        (
            "api_routes/matrix_mfg_routes.rs",
            "MatrixStore::open",
            "MatrixService",
            "0.9.297",
        ),
        (
            "api_routes/matrix_mfg_routes.rs",
            "MfgStore::open",
            "MfgService",
            "0.9.298",
        ),
        (
            "api_routes/memory_routes.rs",
            "CognitiveContextManager/AppState",
            "MemoryService",
            "0.9.296",
        ),
        (
            "api_routes/message_routes.rs",
            "SessionKernel",
            "SessionService",
            "0.9.293",
        ),
        (
            "api_routes/runtime_routes.rs",
            "SessionKernel/AppState",
            "RuntimeService",
            "0.9.293",
        ),
        (
            "api_routes/session_routes.rs",
            "SessionKernel/AppState",
            "SessionService",
            "0.9.293",
        ),
        (
            "api_routes/skill_routes.rs",
            "MatrixStore::open",
            "SkillService+MatrixService",
            "0.9.297",
        ),
        (
            "api_routes/system_routes.rs",
            "UnifiedSessionStore/AppState",
            "SystemService",
            "0.9.296",
        ),
        (
            "api_routes/task_routes.rs",
            "TaskKernel/AppState",
            "TaskService",
            "0.9.296",
        ),
    ];
    for (file, dependency, service, delete_by) in required_entries {
        assert!(
            allowlist.contains(&format!("file={file}"))
                && allowlist.contains(&format!("direct_dependency={dependency}"))
                && allowlist.contains(&format!("replacement_service={service}"))
                && allowlist.contains(&format!("delete_by={delete_by}")),
            "missing complete allowlist entry for {file} -> {dependency}"
        );
    }

    for entry in allowlist
        .split("\n\n")
        .filter(|entry| entry.contains("file="))
    {
        for field in [
            "file=",
            "owner=",
            "direct_dependency=",
            "reason=",
            "replacement_service=",
            "delete_by=",
        ] {
            assert!(
                entry.contains(field),
                "allowlist entry missing {field}: {entry}"
            );
        }
    }

    let direct_dependency_files = [
        ("api_routes/message_routes.rs", "SessionKernel"),
        ("api_routes/connector_routes.rs", "CognitiveContextManager"),
        ("api_routes/cowd_routes.rs", "MatrixStore::open"),
        ("api_routes/matrix_mfg_routes.rs", "MatrixStore::open"),
        ("api_routes/matrix_mfg_routes.rs", "MfgStore::open"),
        ("api_routes/skill_routes.rs", "MatrixStore::open"),
    ];
    for (file, dependency) in direct_dependency_files {
        let source = read_repo(&format!("crates/cowd-cli/src/{file}"));
        assert!(
            source.contains(dependency),
            "{file} no longer needs allowlist entry"
        );
        assert!(allowlist.contains(&format!("file={file}")));
        assert!(allowlist.contains(&format!("direct_dependency={dependency}")));
    }

    let scanned_files = [
        "api_routes/agent_routes.rs",
        "api_routes/approval_routes.rs",
        "api_routes/audit_routes.rs",
        "api_routes/connector_routes.rs",
        "api_routes/context_routes.rs",
        "api_routes/cowd_routes.rs",
        "api_routes/matrix_mfg_routes.rs",
        "api_routes/memory_routes.rs",
        "api_routes/message_routes.rs",
        "api_routes/runtime_routes.rs",
        "api_routes/session_routes.rs",
        "api_routes/skill_routes.rs",
        "api_routes/system_routes.rs",
        "api_routes/task_routes.rs",
    ];
    for file in scanned_files {
        let source = read_repo(&format!("crates/cowd-cli/src/{file}"));
        for dependency in [
            "MatrixStore::open",
            "MfgStore::open",
            "SessionKernel",
            "UnifiedSessionStore",
            "CognitiveContextManager",
            "SmartApprovalGate",
            "TaskKernel",
        ] {
            if source.contains(dependency) {
                assert!(
                    allowlist.contains(&format!("file={file}"))
                        && allowlist.contains(&format!("direct_dependency={dependency}")),
                    "{file} uses {dependency} without service transition allowlist"
                );
            }
        }
    }
}
