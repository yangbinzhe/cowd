#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::tool::{
    ToolDefinition as KernelToolDefinition, ToolDescriptorHealth, ToolDescriptorRef,
    ToolPermissionMode as KernelToolPermissionMode,
};
use plugins::PluginTool;
use serde_json::Value;

use crate::permissions::PermissionMode;

// Re-exports from split modules
pub(crate) use tool_specs::{
    deferred_tool_specs, normalize_tool_name, permission_mode_from_plugin,
};
pub use tool_specs::{mvp_tool_specs, ToolSpec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolManifestEntry {
    pub name: String,
    pub source: ToolSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSource {
    Base,
    Conditional,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolRegistry {
    entries: Vec<ToolManifestEntry>,
}

impl ToolRegistry {
    #[must_use]
    pub fn new(entries: Vec<ToolManifestEntry>) -> Self {
        Self { entries }
    }

    #[must_use]
    pub fn entries(&self) -> &[ToolManifestEntry] {
        &self.entries
    }
}

#[derive(Debug, Clone)]
pub struct ToolCatalog {
    plugin_tools: Vec<PluginTool>,
    runtime_tools: Vec<RuntimeToolDefinition>,
    #[cfg(test)]
    enforcer: Option<crate::permissions::PermissionEnforcer>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
    pub required_permission: PermissionMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

impl ToolCatalog {
    #[must_use]
    pub fn builtin() -> Self {
        Self {
            plugin_tools: Vec::new(),
            runtime_tools: Vec::new(),
            #[cfg(test)]
            enforcer: None,
        }
    }

    pub fn with_plugin_tools(plugin_tools: Vec<PluginTool>) -> Result<Self, String> {
        let builtin_names = mvp_tool_specs()
            .into_iter()
            .map(|spec| spec.name.to_string())
            .collect::<BTreeSet<_>>();
        let mut seen_plugin_names = BTreeSet::new();

        for tool in &plugin_tools {
            let name = tool.definition().name.clone();
            if builtin_names.contains(&name) {
                return Err(format!(
                    "plugin tool `{name}` conflicts with a built-in tool name"
                ));
            }
            if !seen_plugin_names.insert(name.clone()) {
                return Err(format!("duplicate plugin tool name `{name}`"));
            }
        }

        Ok(Self {
            plugin_tools,
            runtime_tools: Vec::new(),
            #[cfg(test)]
            enforcer: None,
        })
    }

    pub fn with_runtime_tools(
        mut self,
        runtime_tools: Vec<RuntimeToolDefinition>,
    ) -> Result<Self, String> {
        let mut seen_names = mvp_tool_specs()
            .into_iter()
            .map(|spec| spec.name.to_string())
            .chain(
                self.plugin_tools
                    .iter()
                    .map(|tool| tool.definition().name.clone()),
            )
            .collect::<BTreeSet<_>>();

        for tool in &runtime_tools {
            if !seen_names.insert(tool.name.clone()) {
                return Err(format!(
                    "runtime tool `{}` conflicts with an existing tool name",
                    tool.name
                ));
            }
        }

        self.runtime_tools = runtime_tools;
        Ok(self)
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_enforcer(mut self, enforcer: crate::permissions::PermissionEnforcer) -> Self {
        self.enforcer = Some(enforcer);
        self
    }

    #[cfg(test)]
    pub fn set_enforcer(&mut self, enforcer: crate::permissions::PermissionEnforcer) {
        self.enforcer = Some(enforcer);
    }

    pub fn normalize_allowed_tools(
        &self,
        values: &[String],
    ) -> Result<Option<BTreeSet<String>>, String> {
        if values.is_empty() {
            return Ok(None);
        }

        let builtin_specs = mvp_tool_specs();
        let canonical_names = builtin_specs
            .iter()
            .map(|spec| spec.name.to_string())
            .chain(
                self.plugin_tools
                    .iter()
                    .map(|tool| tool.definition().name.clone()),
            )
            .chain(self.runtime_tools.iter().map(|tool| tool.name.clone()))
            .collect::<Vec<_>>();
        let mut name_map = canonical_names
            .iter()
            .map(|name| (normalize_tool_name(name), name.clone()))
            .collect::<BTreeMap<_, _>>();

        for (alias, canonical) in [
            ("read", "read_file"),
            ("write", "write_file"),
            ("edit", "edit_file"),
            ("glob", "glob_search"),
            ("grep", "grep_search"),
        ] {
            name_map.insert(alias.to_string(), canonical.to_string());
        }

        let mut allowed = BTreeSet::new();
        for value in values {
            for token in value
                .split(|ch: char| ch == ',' || ch.is_whitespace())
                .filter(|token| !token.is_empty())
            {
                let normalized = normalize_tool_name(token);
                let canonical = name_map.get(&normalized).ok_or_else(|| {
                    format!(
                        "unsupported tool in --allowedTools: {token} (expected one of: {})",
                        canonical_names.join(", ")
                    )
                })?;
                allowed.insert(canonical.clone());
            }
        }

        Ok(Some(allowed))
    }

    #[must_use]
    pub fn definitions(&self, allowed_tools: Option<&BTreeSet<String>>) -> Vec<ToolDefinition> {
        let builtin = mvp_tool_specs()
            .into_iter()
            .filter(|spec| allowed_tools.is_none_or(|allowed| allowed.contains(spec.name)))
            .map(|spec| ToolDefinition {
                name: spec.name.to_string(),
                description: Some(spec.description.to_string()),
                input_schema: spec.input_schema,
            });
        let runtime = self
            .runtime_tools
            .iter()
            .filter(|tool| allowed_tools.is_none_or(|allowed| allowed.contains(tool.name.as_str())))
            .map(|tool| ToolDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema: tool.input_schema.clone(),
            });
        let plugin = self
            .plugin_tools
            .iter()
            .filter(|tool| {
                allowed_tools
                    .is_none_or(|allowed| allowed.contains(tool.definition().name.as_str()))
            })
            .map(|tool| ToolDefinition {
                name: tool.definition().name.clone(),
                description: tool.definition().description.clone(),
                input_schema: tool.definition().input_schema.clone(),
            });
        builtin.chain(runtime).chain(plugin).collect()
    }

    #[must_use]
    pub fn kernel_definitions(
        &self,
        allowed_tools: Option<&BTreeSet<String>>,
    ) -> Vec<KernelToolDefinition> {
        let builtin = mvp_tool_specs()
            .into_iter()
            .filter(|spec| allowed_tools.is_none_or(|allowed| allowed.contains(spec.name)))
            .map(|spec| KernelToolDefinition {
                name: spec.name.to_string(),
                description: Some(spec.description.to_string()),
                input_schema: spec.input_schema,
                required_permission: kernel_permission_mode(spec.required_permission),
            });
        let runtime = self
            .runtime_tools
            .iter()
            .filter(|tool| allowed_tools.is_none_or(|allowed| allowed.contains(tool.name.as_str())))
            .map(|tool| KernelToolDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema: tool.input_schema.clone(),
                required_permission: kernel_permission_mode(tool.required_permission),
            });
        let plugin = self
            .plugin_tools
            .iter()
            .filter(|tool| {
                allowed_tools
                    .is_none_or(|allowed| allowed.contains(tool.definition().name.as_str()))
            })
            .filter_map(|tool| {
                permission_mode_from_plugin(tool.required_permission())
                    .ok()
                    .map(|permission| KernelToolDefinition {
                        name: tool.definition().name.clone(),
                        description: tool.definition().description.clone(),
                        input_schema: tool.definition().input_schema.clone(),
                        required_permission: kernel_permission_mode(permission),
                    })
            });
        builtin.chain(runtime).chain(plugin).collect()
    }

    pub fn permission_specs(
        &self,
        allowed_tools: Option<&BTreeSet<String>>,
    ) -> Result<Vec<(String, PermissionMode)>, String> {
        let builtin = mvp_tool_specs()
            .into_iter()
            .filter(|spec| allowed_tools.is_none_or(|allowed| allowed.contains(spec.name)))
            .map(|spec| (spec.name.to_string(), spec.required_permission));
        let runtime = self
            .runtime_tools
            .iter()
            .filter(|tool| allowed_tools.is_none_or(|allowed| allowed.contains(tool.name.as_str())))
            .map(|tool| (tool.name.clone(), tool.required_permission));
        let plugin = self
            .plugin_tools
            .iter()
            .filter(|tool| {
                allowed_tools
                    .is_none_or(|allowed| allowed.contains(tool.definition().name.as_str()))
            })
            .map(|tool| {
                permission_mode_from_plugin(tool.required_permission())
                    .map(|permission| (tool.definition().name.clone(), permission))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(builtin.chain(runtime).chain(plugin).collect())
    }

    #[must_use]
    pub fn required_permission(&self, name: &str) -> Option<KernelToolPermissionMode> {
        self.permission_specs(None)
            .ok()?
            .into_iter()
            .find_map(|(candidate, permission)| {
                (candidate == name).then_some(kernel_permission_mode(permission))
            })
    }

    #[must_use]
    pub fn has_runtime_tool(&self, name: &str) -> bool {
        self.runtime_tools.iter().any(|tool| tool.name == name)
    }

    #[must_use]
    pub(crate) fn search_ids(&self, query: &str, max_results: usize) -> Vec<String> {
        search_tool_specs(query, max_results.max(1), &self.searchable_tool_specs())
    }

    #[must_use]
    pub(crate) fn descriptor_ref(&self, name: &str) -> Option<ToolDescriptorRef> {
        let definition = self
            .definitions(None)
            .into_iter()
            .find(|item| item.name == name)?;
        let source = if self.runtime_tools.iter().any(|tool| tool.name == name) {
            "runtime"
        } else if self
            .plugin_tools
            .iter()
            .any(|tool| tool.definition().name == name)
        {
            "plugin"
        } else {
            "builtin"
        };
        Some(ToolDescriptorRef {
            canonical_id: definition.name.clone(),
            display_name: definition.name,
            source: source.to_string(),
            schema_hash: value_hash(&definition.input_schema),
            required_permission: self.required_permission(name)?,
            permission_source: format!("{source}_manifest"),
            health: ToolDescriptorHealth::Healthy,
        })
    }

    pub(crate) fn execute_plugin(&self, name: &str, input: &Value) -> Result<String, String> {
        self.plugin_tools
            .iter()
            .find(|tool| tool.definition().name == name)
            .ok_or_else(|| format!("unsupported tool: {name}"))?
            .execute(input)
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub fn execute(&self, name: &str, input: &Value) -> Result<String, String> {
        if mvp_tool_specs().iter().any(|spec| spec.name == name) {
            let host = ToolHost::builtin("tools-catalog-test", std::env::current_dir().unwrap());
            return executor::execute_tool_with_enforcer(
                &host.pin_snapshot(),
                self.enforcer.as_ref(),
                None,
                name,
                input,
            );
        }
        self.execute_plugin(name, input)
    }

    fn searchable_tool_specs(&self) -> Vec<SearchableToolSpec> {
        let builtin = deferred_tool_specs()
            .into_iter()
            .map(|spec| SearchableToolSpec {
                name: spec.name.to_string(),
                description: spec.description.to_string(),
            });
        let runtime = self.runtime_tools.iter().map(|tool| SearchableToolSpec {
            name: tool.name.clone(),
            description: tool.description.clone().unwrap_or_default(),
        });
        let plugin = self.plugin_tools.iter().map(|tool| SearchableToolSpec {
            name: tool.definition().name.clone(),
            description: tool.definition().description.clone().unwrap_or_default(),
        });
        builtin.chain(runtime).chain(plugin).collect()
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        mvp_tool_specs().iter().any(|spec| spec.name == name)
            || self.runtime_tools.iter().any(|tool| tool.name == name)
            || self
                .plugin_tools
                .iter()
                .any(|tool| tool.definition().name == name)
    }

    #[must_use]
    pub fn tool_ids(&self) -> BTreeSet<String> {
        self.definitions(None)
            .into_iter()
            .map(|definition| definition.name)
            .collect()
    }
}

fn kernel_permission_mode(permission: PermissionMode) -> KernelToolPermissionMode {
    match permission {
        PermissionMode::ReadOnly => KernelToolPermissionMode::ReadOnly,
        PermissionMode::WorkspaceWrite => KernelToolPermissionMode::WorkspaceWrite,
        PermissionMode::DangerFullAccess | PermissionMode::Prompt | PermissionMode::Allow => {
            KernelToolPermissionMode::DangerFullAccess
        }
    }
}

fn value_hash(value: &Value) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(value)
        .unwrap_or_default()
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
#[path = "execution/executor.rs"]
pub mod executor;
pub(crate) use executor::*;
#[path = "host.rs"]
pub mod host;
pub use host::{ToolHost, ToolHostError, ToolHostLease, ToolHostSnapshot};
#[path = "execution/bash.rs"]
pub mod bash;
#[path = "state/checkpoint.rs"]
pub mod checkpoint;
#[path = "filesystem/file_ops.rs"]
pub mod file_ops;
#[path = "policy/gates.rs"]
pub mod gates;
#[path = "policy/lane_events.rs"]
pub mod lane_events;
#[path = "policy/lane_policy.rs"]
pub mod lane_policy;
#[path = "state/lsp_client.rs"]
pub mod lsp_client;
#[path = "state/mutation_plan.rs"]
pub mod mutation_plan;
#[path = "filesystem/pdf_extract.rs"]
pub mod pdf_extract;
#[path = "policy/permissions.rs"]
pub mod permissions;
#[path = "execution/prepared.rs"]
pub(crate) mod prepared;
#[path = "execution/sandbox_exec.rs"]
pub mod sandbox_exec;
#[path = "policy/stale_branch.rs"]
pub mod stale_branch;
#[path = "state/tool_cache.rs"]
pub mod tool_cache;
#[path = "state/tool_orchestrator.rs"]
pub mod tool_orchestrator;
#[path = "registry/tool_specs.rs"]
pub mod tool_specs;
#[path = "registry/web_tools.rs"]
pub mod web_tools;

#[cfg(test)]
mod tests {
    use super::*;
    use mcp::McpService;
    use std::sync::Arc;

    #[derive(Debug)]
    struct FakeMcpService {
        name: &'static str,
    }

    impl McpService for FakeMcpService {
        fn list_servers(&self) -> Result<Vec<mcp::McpServerProjection>, mcp::McpServiceError> {
            Ok(vec![self.server(self.name)?])
        }

        fn server(&self, name: &str) -> Result<mcp::McpServerProjection, mcp::McpServiceError> {
            Ok(mcp::McpServerProjection {
                name: name.to_string(),
                transport: mcp::McpTransportKind::ManagedProxy,
                enabled: true,
                status: "ready".to_string(),
                auth_state: None,
            })
        }

        fn health(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
            Ok(serde_json::json!({ "name": self.name }))
        }

        fn reload_config(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
            Ok(serde_json::json!({ "ok": true }))
        }

        fn list_tools(
            &self,
            _server: Option<&str>,
        ) -> Result<Vec<mcp::McpToolProjection>, mcp::McpServiceError> {
            Ok(Vec::new())
        }

        fn list_resources(
            &self,
            _server: Option<&str>,
        ) -> Result<Vec<mcp::McpResourceProjection>, mcp::McpServiceError> {
            Ok(Vec::new())
        }

        fn read_resource(
            &self,
            server: &str,
            uri: &str,
        ) -> Result<mcp::McpResourceProjection, mcp::McpServiceError> {
            Ok(mcp::McpResourceProjection {
                server: server.to_string(),
                uri: uri.to_string(),
                name: None,
                mime_type: None,
                content: None,
            })
        }

        fn call_tool(
            &self,
            request: mcp::McpToolCallRequest,
        ) -> Result<mcp::McpToolCallReceipt, mcp::McpServiceError> {
            Ok(mcp::McpToolCallReceipt {
                server: request.server,
                tool: request.tool,
                ok: true,
                output: serde_json::json!({ "name": self.name }),
            })
        }
    }

    #[test]
    fn mcp_service_is_replaced_by_snapshot_without_changing_pinned_request() {
        let host = ToolHost::new(
            "workspace",
            "/tmp/workspace",
            ToolHostSnapshot::new(
                Arc::new(ToolCatalog::builtin()),
                Arc::new(lsp_client::LspRegistry::new()),
                Some(Arc::new(FakeMcpService { name: "first" })),
            ),
        );
        let pinned = host.pin_snapshot();
        host.replace_snapshot(ToolHostSnapshot::new(
            Arc::new(ToolCatalog::builtin()),
            Arc::new(lsp_client::LspRegistry::new()),
            Some(Arc::new(FakeMcpService { name: "second" })),
        ));
        assert_eq!(
            pinned.snapshot().mcp.as_ref().unwrap().health().unwrap()["name"],
            "first"
        );
        assert_eq!(
            host.pin_snapshot()
                .snapshot()
                .mcp
                .as_ref()
                .unwrap()
                .health()
                .unwrap()["name"],
            "second"
        );
    }
}
