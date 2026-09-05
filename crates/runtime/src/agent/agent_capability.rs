//! Runtime-owned resolver for team-agent tool and permission capabilities.

use std::collections::BTreeSet;

use harness_contract::agent::AgentCapability;
use harness_contract::agent_action::AGENT_ACTION_TOOL_IDS;

use crate::{PermissionMode, PermissionPolicy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCapabilityRequest {
    pub role_id: String,
    pub allowed_capabilities: Vec<String>,
    pub evidence_duties: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedAgentCapability {
    pub role_id: String,
    pub requested_capabilities: Vec<String>,
    pub allowed_tools: BTreeSet<String>,
    pub evidence_duties: Vec<String>,
    pub permission_mode: PermissionMode,
    pub permission_policy: PermissionPolicy,
    pub capability_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCapabilityHostBinding {
    pub allowed_tools: BTreeSet<String>,
    pub missing_capabilities: Vec<String>,
    pub missing_tool_alternatives: Vec<String>,
}

/// Bind a semantic capability grant to one concrete ToolHost snapshot.
///
/// Most capabilities merely crop an allowlist. Capabilities that promise an
/// externally observable effect (network access, workspace mutation, test
/// execution, or rollback) additionally require at least one physical tool
/// contract that can perform that effect. This prevents admission from
/// advertising a capability that Agent startup would later silently erase.
#[must_use]
pub fn bind_agent_capability_to_host(
    resolved: &ResolvedAgentCapability,
    host_tools: &BTreeSet<String>,
) -> AgentCapabilityHostBinding {
    let allowed_tools = resolved
        .allowed_tools
        .intersection(host_tools)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut missing_capabilities = Vec::new();
    let mut missing_tool_alternatives = BTreeSet::new();
    for capability in &resolved.requested_capabilities {
        let alternatives = required_host_tool_alternatives(capability);
        if alternatives.is_empty()
            || alternatives
                .iter()
                .any(|candidate| host_tools.contains(*candidate))
        {
            continue;
        }
        missing_capabilities.push(capability.clone());
        missing_tool_alternatives.extend(alternatives.iter().map(|tool| (*tool).to_string()));
    }
    AgentCapabilityHostBinding {
        allowed_tools,
        missing_capabilities,
        missing_tool_alternatives: missing_tool_alternatives.into_iter().collect(),
    }
}

pub fn resolve_agent_capability(request: AgentCapabilityRequest) -> ResolvedAgentCapability {
    let requested_capabilities = normalize_capabilities(request.allowed_capabilities);
    let mut allowed_tools = BTreeSet::new();
    let mut permission_mode = PermissionMode::ReadOnly;
    for capability in &requested_capabilities {
        let mapping = capability_mapping(capability);
        permission_mode = strongest_permission(permission_mode, mapping.required_mode);
        allowed_tools.extend(mapping.tools.iter().map(|tool| (*tool).to_string()));
    }
    if allowed_tools.is_empty() {
        allowed_tools.insert("read_file".to_string());
        allowed_tools.insert("grep_search".to_string());
        allowed_tools.insert("glob_search".to_string());
    }
    // Every Runtime-owned Agent receives the same read-only context continuity
    // entry point. The tool itself enforces the exact Agent/Session/Project/
    // Team Binding, so this does not grant broad Memory or Session access.
    allowed_tools.insert("context_retrieve".to_string());
    // Agent-first actions are injected only by the dynamic Program dispatcher
    // after it binds an immutable roster identity. Generic/legacy Agent
    // capability resolution must not advertise those actions without an
    // `agentic_program` context, because the host would correctly reject them
    // as unbound tool inventory drift.
    // Durable raw tool outputs are read-only evidence references resolved by
    // the Runtime ArtifactStore; the tool itself enforces ref authorization.
    allowed_tools.insert("evidence_retrieve".to_string());
    let mut permission_policy = PermissionPolicy::new(permission_mode);
    for tool in &allowed_tools {
        permission_policy =
            permission_policy.with_tool_requirement(tool.clone(), agent_tool_permission(tool));
    }
    let capability_summary = format!(
        "role={} capabilities=[{}] tools=[{}] permission={}",
        request.role_id,
        requested_capabilities.join(","),
        allowed_tools.iter().cloned().collect::<Vec<_>>().join(","),
        permission_mode.as_str()
    );
    ResolvedAgentCapability {
        role_id: request.role_id,
        requested_capabilities,
        allowed_tools,
        evidence_duties: request.evidence_duties,
        permission_mode,
        permission_policy,
        capability_summary,
    }
}

fn normalize_capabilities(values: Vec<String>) -> Vec<String> {
    let mut normalized = values
        .into_iter()
        .flat_map(|value| {
            value
                .split(|ch: char| ch == ',' || ch.is_whitespace())
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(|token| token.replace('-', "_").to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if normalized.is_empty() {
        normalized.push("read".to_string());
    }
    normalized
}

#[derive(Debug, Clone, Copy)]
struct CapabilityMapping {
    tools: &'static [&'static str],
    required_mode: PermissionMode,
}

fn capability_mapping(capability: &str) -> CapabilityMapping {
    match capability {
        "read" => CapabilityMapping {
            tools: &[
                "read_file",
                "read_many",
                "workspace_snapshot",
                "context_retrieve",
            ],
            required_mode: PermissionMode::ReadOnly,
        },
        "search" => CapabilityMapping {
            tools: &[
                "grep_search",
                "grep_many",
                "glob_search",
                "glob_many",
                "tool_search",
                "context_retrieve",
            ],
            required_mode: PermissionMode::ReadOnly,
        },
        "network" | "web" => CapabilityMapping {
            tools: &["web_search", "web_fetch", "tool_search", "mcp_tool"],
            required_mode: PermissionMode::ReadOnly,
        },
        "write" => CapabilityMapping {
            tools: &["write_file", "edit_file", "apply_patch_transaction", "bash"],
            required_mode: PermissionMode::WorkspaceWrite,
        },
        "test" | "status" | "logs" => CapabilityMapping {
            tools: &["bash", "execute_code", "read_file", "grep_search"],
            required_mode: PermissionMode::WorkspaceWrite,
        },
        "rollback" => CapabilityMapping {
            tools: &["bash", "checkpoint_restore", "read_file", "grep_search"],
            required_mode: PermissionMode::DangerFullAccess,
        },
        "tool_call" => CapabilityMapping {
            tools: &["tool_search"],
            required_mode: PermissionMode::ReadOnly,
        },
        "connector_action" => CapabilityMapping {
            tools: &["tool_search", "mcp_tool"],
            required_mode: PermissionMode::DangerFullAccess,
        },
        // Matrix mutations are deliberately supplied by an immutable Skill
        // tool contract. `tool_search` is the bounded discovery entry point;
        // admission additionally requires the selected Skill's concrete
        // matrix tool to exist in the active ToolHost.
        "matrix_write" => CapabilityMapping {
            tools: &["tool_search"],
            required_mode: PermissionMode::WorkspaceWrite,
        },
        _ => CapabilityMapping {
            tools: &["read_file", "grep_search", "glob_search"],
            required_mode: PermissionMode::ReadOnly,
        },
    }
}

/// Check a concrete tool contract against the same capability mapping that
/// produced the Agent allowlist. A tool can legitimately serve more than one
/// capability, so reducing this relationship to one name-derived capability
/// rejects valid least-privilege Bindings.
pub(crate) fn capability_mapping_authorizes_tool(
    capability: AgentCapability,
    tool_ref: &str,
) -> bool {
    let tool = tool_ref
        .rsplit(['/', ':'])
        .next()
        .unwrap_or(tool_ref)
        .to_ascii_lowercase();
    if capability == AgentCapability::Read
        && (AGENT_ACTION_TOOL_IDS.contains(&tool.as_str())
            || matches!(tool.as_str(), "context_retrieve" | "evidence_retrieve"))
    {
        return true;
    }
    capability_mapping(capability.as_str())
        .tools
        .iter()
        .any(|candidate| *candidate == tool)
}

pub(crate) fn runtime_capability_map_contains_tool(tool_ref: &str) -> bool {
    let tool = tool_ref
        .rsplit(['/', ':'])
        .next()
        .unwrap_or(tool_ref)
        .to_ascii_lowercase();
    if AGENT_ACTION_TOOL_IDS.contains(&tool.as_str())
        || matches!(tool.as_str(), "context_retrieve" | "evidence_retrieve")
    {
        return true;
    }
    [
        AgentCapability::Read,
        AgentCapability::Search,
        AgentCapability::Write,
        AgentCapability::Test,
        AgentCapability::Network,
        AgentCapability::ConnectorAction,
        AgentCapability::MatrixWrite,
    ]
    .into_iter()
    .any(|capability| capability_mapping_authorizes_tool(capability, &tool))
}

fn required_host_tool_alternatives(capability: &str) -> &'static [&'static str] {
    match capability {
        "network" | "web" => &["web_search", "web_fetch", "mcp_tool"],
        "write" => &["write_file", "edit_file"],
        "test" => &["bash", "execute_code"],
        "rollback" => &["bash", "checkpoint_restore"],
        "connector_action" => &["mcp_tool"],
        _ => &[],
    }
}

pub(crate) fn agent_tool_permission(tool: &str) -> PermissionMode {
    match tool {
        "write_file" | "edit_file" => PermissionMode::WorkspaceWrite,
        "execute_code" => PermissionMode::ReadOnly,
        "bash" | "checkpoint_restore" | "mcp_tool" => PermissionMode::DangerFullAccess,
        _ => PermissionMode::ReadOnly,
    }
}

fn strongest_permission(left: PermissionMode, right: PermissionMode) -> PermissionMode {
    if permission_rank(right) > permission_rank(left) {
        right
    } else {
        left
    }
}

fn permission_rank(mode: PermissionMode) -> usize {
    usize::from(mode.rank())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_maps_read_search_to_readonly_tools() {
        let resolved = resolve_agent_capability(AgentCapabilityRequest {
            role_id: "researcher".to_string(),
            allowed_capabilities: vec!["read".to_string(), "search".to_string()],
            evidence_duties: vec!["source_notes".to_string()],
        });

        assert_eq!(resolved.permission_mode, PermissionMode::ReadOnly);
        assert!(resolved.allowed_tools.contains("read_file"));
        assert!(resolved.allowed_tools.contains("read_many"));
        assert!(resolved.allowed_tools.contains("workspace_snapshot"));
        assert!(resolved.allowed_tools.contains("grep_search"));
        assert!(resolved.allowed_tools.contains("grep_many"));
        assert!(resolved.allowed_tools.contains("glob_search"));
        assert!(resolved.allowed_tools.contains("glob_many"));
        assert!(resolved.allowed_tools.contains("tool_search"));
        assert!(resolved.allowed_tools.contains("context_retrieve"));
        assert!(!resolved.allowed_tools.contains("task_claim"));
        assert!(!resolved.allowed_tools.contains("message_publish"));
        assert_eq!(resolved.evidence_duties, vec!["source_notes"]);
    }

    #[test]
    fn resolver_escalates_write_and_test_permissions() {
        let resolved = resolve_agent_capability(AgentCapabilityRequest {
            role_id: "implementer".to_string(),
            allowed_capabilities: vec!["read".to_string(), "write".to_string(), "test".to_string()],
            evidence_duties: vec!["diff_summary".to_string()],
        });

        assert_eq!(resolved.permission_mode, PermissionMode::WorkspaceWrite);
        assert!(resolved.allowed_tools.contains("write_file"));
        assert!(resolved.allowed_tools.contains("edit_file"));
        assert!(resolved.allowed_tools.contains("bash"));
        assert!(resolved.allowed_tools.contains("execute_code"));
        assert!(resolved.allowed_tools.contains("context_retrieve"));
        assert_eq!(
            resolved.permission_policy.required_mode_for("write_file"),
            PermissionMode::WorkspaceWrite
        );
        assert_eq!(
            resolved.permission_policy.required_mode_for("bash"),
            PermissionMode::DangerFullAccess
        );
        assert_eq!(
            resolved.permission_policy.required_mode_for("execute_code"),
            PermissionMode::ReadOnly
        );
    }

    #[test]
    fn sandboxed_execute_code_satisfies_test_without_host_shell_authority() {
        let resolved = resolve_agent_capability(AgentCapabilityRequest {
            role_id: "tester".to_string(),
            allowed_capabilities: vec!["read".to_string(), "test".to_string()],
            evidence_duties: vec!["test_report".to_string()],
        });
        let host_tools = ["read_file", "grep_search", "execute_code"]
            .into_iter()
            .map(str::to_string)
            .collect();

        let binding = bind_agent_capability_to_host(&resolved, &host_tools);

        assert!(binding.missing_capabilities.is_empty());
        assert!(binding.missing_tool_alternatives.is_empty());
        assert!(binding.allowed_tools.contains("execute_code"));
        assert!(!binding.allowed_tools.contains("bash"));
    }

    #[test]
    fn resolver_maps_network_to_search_fetch_and_discovery() {
        let resolved = resolve_agent_capability(AgentCapabilityRequest {
            role_id: "external-researcher".to_string(),
            allowed_capabilities: vec!["network".to_string()],
            evidence_duties: vec!["dated_sources".to_string()],
        });

        assert_eq!(resolved.permission_mode, PermissionMode::ReadOnly);
        assert!(resolved.allowed_tools.contains("web_search"));
        assert!(resolved.allowed_tools.contains("web_fetch"));
        assert!(resolved.allowed_tools.contains("tool_search"));
    }

    #[test]
    fn host_binding_rejects_capability_without_physical_effect_tool() {
        let resolved = resolve_agent_capability(AgentCapabilityRequest {
            role_id: "external-researcher".to_string(),
            allowed_capabilities: vec![
                "read".to_string(),
                "search".to_string(),
                "network".to_string(),
            ],
            evidence_duties: Vec::new(),
        });
        let host_tools = ["read_file", "tool_search", "context_retrieve"]
            .into_iter()
            .map(str::to_string)
            .collect();

        let binding = bind_agent_capability_to_host(&resolved, &host_tools);

        assert_eq!(binding.missing_capabilities, vec!["network"]);
        assert_eq!(
            binding.missing_tool_alternatives,
            vec!["mcp_tool", "web_fetch", "web_search"]
        );
        assert!(binding.allowed_tools.contains("read_file"));
        assert!(!binding.allowed_tools.contains("web_search"));
    }

    #[test]
    fn host_binding_keeps_only_attested_tools_when_effect_is_backed() {
        let resolved = resolve_agent_capability(AgentCapabilityRequest {
            role_id: "external-researcher".to_string(),
            allowed_capabilities: vec!["network".to_string()],
            evidence_duties: Vec::new(),
        });
        let host_tools = ["web_search", "context_retrieve", "unknown_tool"]
            .into_iter()
            .map(str::to_string)
            .collect();

        let binding = bind_agent_capability_to_host(&resolved, &host_tools);

        assert!(binding.missing_capabilities.is_empty());
        assert_eq!(
            binding.allowed_tools,
            ["context_retrieve", "web_search"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }
}
