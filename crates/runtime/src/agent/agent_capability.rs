//! Runtime-owned resolver for team-agent tool and permission capabilities.

use std::collections::BTreeSet;

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
    let mut permission_policy = PermissionPolicy::new(permission_mode);
    for tool in &allowed_tools {
        permission_policy =
            permission_policy.with_tool_requirement(tool.clone(), tool_permission(tool));
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
            tools: &["read_file", "glob_search"],
            required_mode: PermissionMode::ReadOnly,
        },
        "search" => CapabilityMapping {
            tools: &["grep_search", "glob_search", "ToolSearch"],
            required_mode: PermissionMode::ReadOnly,
        },
        "web" => CapabilityMapping {
            tools: &["WebSearch"],
            required_mode: PermissionMode::ReadOnly,
        },
        "write" => CapabilityMapping {
            tools: &["write_file", "edit_file"],
            required_mode: PermissionMode::WorkspaceWrite,
        },
        "test" | "status" | "logs" => CapabilityMapping {
            tools: &["bash", "read_file", "grep_search"],
            required_mode: PermissionMode::WorkspaceWrite,
        },
        "rollback" => CapabilityMapping {
            tools: &["bash", "read_file", "grep_search"],
            required_mode: PermissionMode::DangerFullAccess,
        },
        "tool_call" => CapabilityMapping {
            tools: &["ToolSearch"],
            required_mode: PermissionMode::ReadOnly,
        },
        _ => CapabilityMapping {
            tools: &["read_file", "grep_search", "glob_search"],
            required_mode: PermissionMode::ReadOnly,
        },
    }
}

fn tool_permission(tool: &str) -> PermissionMode {
    match tool {
        "write_file" | "edit_file" => PermissionMode::WorkspaceWrite,
        "bash" => PermissionMode::DangerFullAccess,
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
    match mode {
        PermissionMode::ReadOnly => 0,
        PermissionMode::Prompt => 1,
        PermissionMode::WorkspaceWrite => 2,
        PermissionMode::DangerFullAccess | PermissionMode::Allow => 3,
    }
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
        assert!(resolved.allowed_tools.contains("grep_search"));
        assert!(resolved.allowed_tools.contains("ToolSearch"));
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
        assert_eq!(
            resolved.permission_policy.required_mode_for("write_file"),
            PermissionMode::WorkspaceWrite
        );
        assert_eq!(
            resolved.permission_policy.required_mode_for("bash"),
            PermissionMode::DangerFullAccess
        );
    }
}
