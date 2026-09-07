use std::path::{Path, PathBuf};

use plugins::{PluginHooks, PluginManager, PluginManagerConfig, PluginRegistry};
use runtime::{ConfigLoader, McpServerManager};
use serde_json::json;
use tools::{permissions::PermissionMode as ToolPermissionMode, RuntimeToolDefinition};

use harness_contract::tool::ToolEffectResolverSpec;

pub(crate) type GatewayToolRegistry = tools::ToolCatalog;

pub(crate) struct RuntimeBootstrapState {
    pub(crate) feature_config: runtime::RuntimeFeatureConfig,
    pub(crate) tool_registry: GatewayToolRegistry,
    pub(crate) plugin_registry: PluginRegistry,
}

#[derive(Clone)]
pub(crate) struct RuntimeSessionBootstrapSnapshot {
    pub(crate) feature_config: runtime::RuntimeFeatureConfig,
    pub(crate) tool_registry: GatewayToolRegistry,
    pub(crate) plugin_registry: PluginRegistry,
}

impl RuntimeSessionBootstrapSnapshot {
    #[must_use]
    pub(crate) fn model_context_window(&self, model: &str) -> u32 {
        runtime::model_context_window_with_overrides(
            model,
            Some(self.feature_config.model_context_windows()),
        )
    }
}

impl RuntimeBootstrapState {
    #[must_use]
    pub(crate) fn session_snapshot(&self) -> RuntimeSessionBootstrapSnapshot {
        RuntimeSessionBootstrapSnapshot {
            feature_config: self.feature_config.clone(),
            tool_registry: self.tool_registry.clone(),
            plugin_registry: self.plugin_registry.clone(),
        }
    }
}

pub(crate) fn assemble_runtime_state() -> Result<RuntimeBootstrapState, Box<dyn std::error::Error>>
{
    let cwd = std::env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load()?;
    assemble_runtime_state_with_loader(&cwd, &loader, &runtime_config)
}

pub(crate) fn assemble_runtime_state_with_loader(
    cwd: &Path,
    loader: &ConfigLoader,
    runtime_config: &runtime::RuntimeConfig,
) -> Result<RuntimeBootstrapState, Box<dyn std::error::Error>> {
    let plugin_manager = build_plugin_manager(cwd, loader, runtime_config);
    let plugin_registry = plugin_manager.plugin_registry()?;
    let plugin_hook_config =
        runtime_hook_config_from_plugin_hooks(plugin_registry.aggregated_hooks()?);
    let feature_config = runtime_config
        .feature_config()
        .clone()
        .with_hooks(runtime_config.hooks().merged(&plugin_hook_config));
    let tool_registry =
        GatewayToolRegistry::with_plugin_tools(plugin_registry.aggregated_tools()?)?
            .with_runtime_tools(runtime_capability_tool_definitions())?;
    Ok(RuntimeBootstrapState {
        feature_config,
        tool_registry,
        plugin_registry,
    })
}

pub(crate) fn load_tool_registry_for_current_dir() -> Result<GatewayToolRegistry, String> {
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load().map_err(|error| error.to_string())?;
    let state = assemble_runtime_state_with_loader(&cwd, &loader, &runtime_config)
        .map_err(|error| error.to_string())?;
    let mcp_tools = discover_mcp_tool_definitions_once(&runtime_config)?;
    state.tool_registry.with_runtime_tools(mcp_tools)
}

pub(crate) fn build_plugin_manager(
    cwd: &Path,
    loader: &ConfigLoader,
    runtime_config: &runtime::RuntimeConfig,
) -> PluginManager {
    let plugin_settings = runtime_config.plugins();
    let mut plugin_config = PluginManagerConfig::new(loader.config_home().to_path_buf());
    plugin_config.enabled_plugins = plugin_settings.enabled_plugins().clone();
    // The loader's config home is the authoritative scope for installed-plugin
    // state. Reading the process-global default here made custom profiles and
    // test workspaces silently lose their enabled plugins.
    let state_path = loader.config_home().join("plugin-state.json");
    if let Ok(content) = std::fs::read_to_string(&state_path) {
        if !content.trim().is_empty() {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(map) = val.get("enabledPlugins").and_then(|v| v.as_object()) {
                    for (key, value) in map {
                        if let Some(enabled) = value.as_bool() {
                            plugin_config.enabled_plugins.insert(key.clone(), enabled);
                        }
                    }
                }
            }
        }
    }
    plugin_config.external_dirs = plugin_settings
        .external_directories()
        .iter()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path))
        .collect();
    plugin_config.install_root = plugin_settings
        .install_root()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    plugin_config.registry_path = plugin_settings
        .registry_path()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    plugin_config.bundled_root = plugin_settings
        .bundled_root()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    PluginManager::new(plugin_config)
}

fn resolve_plugin_path(cwd: &Path, config_home: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else if value.starts_with('.') {
        cwd.join(path)
    } else {
        config_home.join(path)
    }
}

fn discover_mcp_tool_definitions_once(
    runtime_config: &runtime::RuntimeConfig,
) -> Result<Vec<RuntimeToolDefinition>, String> {
    let mut manager = McpServerManager::from_runtime_config(runtime_config);
    if manager.server_names().is_empty() {
        return Ok(Vec::new());
    }
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err("cannot perform one-shot MCP discovery inside a Tokio runtime".to_string());
    }
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    let discovery = runtime.block_on(manager.discover_tools_best_effort());
    let mut runtime_tools = discovery
        .tools
        .iter()
        .map(mcp_runtime_tool_definition)
        .collect::<Vec<_>>();
    runtime_tools.extend(mcp_wrapper_tool_definitions());
    runtime
        .block_on(manager.shutdown())
        .map_err(|error| error.to_string())?;
    Ok(runtime_tools)
}

pub(crate) fn runtime_capability_tool_definitions() -> Vec<RuntimeToolDefinition> {
    let mut definitions = vec![
        RuntimeToolDefinition {
            name: "lark_cli_read".to_string(),
            description: Some(
                "Run an official Lark CLI read-only command with the active Cowd Feishu/Lark bot configuration. Pass argv entries after `lark-cli`; Cowd resolves a short-lived tenant token, verifies the CLI-declared risk, isolates credentials, applies timeout/output limits, and redacts secrets. Use this for interactive Lark/Feishu inspection and Base/Bitable reads selected by a lark-* Skill.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "maxItems": 96,
                        "description": "Official lark-cli arguments excluding the executable name, for example [\"im\", \"+chat-list\", \"--as\", \"bot\"]."
                    },
                    "brand": { "type": "string", "enum": ["auto", "feishu", "lark"] },
                    "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 180000 }
                },
                "required": ["args"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.external_read"),
        },
        RuntimeToolDefinition {
            name: "lark_cli_write".to_string(),
            description: Some(
                "Run an official Lark CLI mutating command with the active Cowd Feishu/Lark bot configuration. Cowd verifies the CLI-declared write risk and routes execution through the DangerFullAccess approval/effect fence; credentials remain gateway-owned and are never supplied by the model. Use only when a selected lark-* Skill requires a real create, update, delete, or send operation.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "maxItems": 96,
                        "description": "Official lark-cli arguments excluding the executable name."
                    },
                    "brand": { "type": "string", "enum": ["auto", "feishu", "lark"] },
                    "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 180000 }
                },
                "required": ["args"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::DangerFullAccess,
            effect_resolver: runtime_effect_resolver("runtime.external_danger"),
        },
        RuntimeToolDefinition {
            name: "runtime_config_view".to_string(),
            description: Some(
                "Return a read-only, redacted view of the active model route, context window, permission mode, provider protocol, and runtime policy. Use only when configuration or provider behavior is relevant; secrets, headers, environment values, and configuration paths are never returned.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "detail": {
                        "type": "string",
                        "enum": ["summary", "providers", "policy"],
                        "description": "Optional focused projection. Defaults to summary."
                    }
                },
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.readonly"),
        },
        RuntimeToolDefinition {
            name: "runtime_resource_capabilities".to_string(),
            description: Some(
                "Query a bounded, current capability projection for an attached resource type. Use this only after an attachment is relevant and a narrower parser, skill, plugin, MCP resource, or local command is needed. Returned names are discovery candidates, not proof that inspection succeeded.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "resource_kind": { "type": "string", "enum": ["image", "audio", "video", "pdf", "text", "markdown", "csv", "document", "archive", "code", "binary", "unknown"] },
                    "mime": { "type": "string" },
                    "intent": { "type": "string" }
                },
                "required": ["resource_kind", "intent"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.readonly"),
        },
        RuntimeToolDefinition {
            name: "context_retrieve".to_string(),
            description: Some(
                "Actively retrieve focused context when the automatically assembled packet is incomplete or appears unrelated. Search the current Runtime Binding's Memory, read one authorized Memory by an id returned from search, discover the current actor's own Session catalog with a focused query, page authorized history, or read one exact message by stable id/sequence and block cursor. Follow returned read_request/next_request objects. Evidence references are audit locators, not MCP resources. This tool cannot mutate Memory or cross durable workspace/actor boundaries.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "enum": ["memory", "session_catalog", "session_history"]
                    },
                    "query": {
                        "type": "string",
                        "minLength": 1,
                        "description": "A focused semantic or full-text query. Required for memory search unless memory_id is supplied, session catalog discovery, and related_sessions search."
                    },
                    "memory_id": {
                        "type": "string",
                        "description": "Exact Memory UUID returned by a prior memory search. Valid only with source=memory and always rechecked against the current Runtime Memory Binding."
                    },
                    "scope": {
                        "type": "string",
                        "enum": ["current", "related_sessions", "workspace_sessions", "explicit_session"],
                        "description": "Defaults by source: memory/current, session_catalog/workspace_sessions, session_history/current. explicit_session requires session_id and passes a durable workspace/actor or SessionRelationGraph authorization check."
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Explicit target returned by session_catalog. Valid only with session_history/explicit_session."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 16,
                        "default": 8
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Session catalog page offset."
                    },
                    "before_sequence": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Read an older bounded page ending before this message sequence when query is omitted."
                    },
                    "message_id": {
                        "type": "string",
                        "description": "Read one exact authorized Session message by immutable stable id."
                    },
                    "sequence": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Read one exact authorized Session message by sequence when message_id is unavailable."
                    },
                    "block_cursor": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "First content block to return for exact message retrieval."
                    },
                    "block_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 128,
                        "default": 16,
                        "description": "Maximum content blocks in one exact message page."
                    }
                },
                "required": ["source"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.readonly"),
        },
        RuntimeToolDefinition {
            name: "runtime_capabilities".to_string(),
            description: Some(
                "Return Cowd runtime capability guidance, execution patterns, evidence planning, batch/parallel tool advice, and orchestration suggestions for the current task.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "intent": { "type": "string" },
                    "surface": { "type": "string" },
                    "profile": { "type": "string" },
                    "detail": {
                        "type": "string",
                        "enum": ["summary", "execution_patterns", "agent_catalog", "orchestration_options", "runtime_action_contract", "capability_catalog", "budget_controls", "policy_gates"]
                    }
                },
                "required": ["intent"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.readonly"),
        },
        RuntimeToolDefinition {
            name: "evidence_retrieve".to_string(),
            description: Some(
                "Retrieve selected chunks from an immutable tool:// evidence receipt or artifact:// content reference. Use this before independent review; use a focused query when the content is large.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "evidence_ref": { "type": "string", "description": "tool:// evidence reference or artifact:// content reference" },
                    "query": { "type": "string", "description": "Optional FTS query; omit to read the first chunks" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 16 }
                },
                "required": ["evidence_ref"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.readonly"),
        },
    ];
    definitions.extend(agent_action_tool_definitions());
    definitions
}

fn agent_action_tool_definitions() -> Vec<RuntimeToolDefinition> {
    use harness_contract::agent_action as action;

    vec![
        agent_action_definition::<action::StateInspectInput>(
            action::STATE_INSPECT_TOOL_ID,
            "Inspect the current Program, Team, roster, work, topic and artifact facts. Runtime binds the Objective, Program and actor. Root: leave wait_for_workers=false to continue planning and publishing tasks while members execute; set it true only when ready to yield to workers until their work settles. Do not poll unchanged state. Optional scope_ref=collaboration_patterns returns evidence-backed structural suggestions from previous completed Turns; they are advice, not execution rules or capability grants.",
        ),
        agent_action_definition::<action::TeamCreateInput>(
            action::TEAM_CREATE_TOOL_ID,
            "Create one Team from a semantic name and mission. Runtime creates identity, roster, topic, revision and receipt. Do not submit roles, graph nodes, dependencies or artifact contracts here.",
        ),
        agent_action_definition::<action::AgentInviteInput>(
            action::AGENT_INVITE_TOOL_ID,
            "Invite one Agent into an existing Team by role, mission, and optional semantic capability hints. Prefer portable effect hints read/search/write/test/network; put domain expertise in role and mission. Runtime translates hints and resolves the concrete Agent definition, identity, least-privilege permissions, and execution binding, so unknown domain labels do not grant authority or block dispatch.",
        ),
        agent_action_definition::<action::TaskPublishInput>(
            action::TASK_PUBLISH_TOOL_ID,
            "Publish one bounded semantic Task to a Team work market. Dependencies are existing Task references only. required_capabilities is optional: prefer portable effect hints read/search/write/test/network and keep domain detail in title/objective/acceptance. Runtime owns capability translation, scheduling, leases and physical execution.",
        ),
        agent_action_definition::<action::TaskClaimInput>(
            action::TASK_CLAIM_TOOL_ID,
            "Claim one available Task. Runtime derives the caller identity and checks roster, capabilities, dependencies and lease state.",
        ),
        agent_action_definition::<action::TaskReleaseInput>(
            action::TASK_RELEASE_TOOL_ID,
            "Release the caller's current Task claim with a semantic reason so another eligible Agent can continue.",
        ),
        agent_action_definition::<action::TaskSupersedeInput>(
            action::TASK_SUPERSEDE_TOOL_ID,
            "Retire failed or challenged work in favor of concrete successor Tasks. Supply one replacement Task reference, or multiple references for a split, plus durable evidence and a reason. Retirement preserves failure history and never counts as accepted work.",
        ),
        agent_action_definition::<action::TaskWithdrawInput>(
            action::TASK_WITHDRAW_TOOL_ID,
            "Withdraw an unproductive or invalid Task with a durable reason and optional evidence. Runtime preserves the original obligation, stops further physical attempts safely, and leaves dependent work waiting until an eligible replacement is accepted; withdrawal never fabricates completion.",
        ),
        agent_action_definition::<action::TaskSubmitInput>(
            action::TASK_SUBMIT_TOOL_ID,
            "Submit one claimed Task using committed artifact and evidence references. Long result content must be committed separately, never embedded in this action. unresolved carries honest limitations or follow-up disclosures for the independent reviewer; that reviewer decides whether they block Task acceptance.",
        ),
        agent_action_definition::<action::TaskReviewInput>(
            action::TASK_REVIEW_TOOL_ID,
            "Independently accept, challenge or request rework for one submitted Task using a concise reason and evidence references.",
        ),
        agent_action_definition::<action::MessagePublishInput>(
            action::MESSAGE_PUBLISH_TOOL_ID,
            "Publish a concise Team/Program update or a reference to committed long content. This is shared semantic communication, not private chain-of-thought.",
        ),
        agent_action_definition::<action::ArtifactCommitInput>(
            action::ARTIFACT_COMMIT_TOOL_ID,
            "Commit a staged model content part or existing content reference as a versioned artifact. Use content_ref=preceding_content only when ordinary model content immediately precedes this tool call; never put the content body in JSON.",
        ),
        agent_action_definition::<action::ObjectiveUpdateInput>(
            action::OBJECTIVE_UPDATE_TOOL_ID,
            "Evolve one non-user Objective criterion from durable source material. Add a new criterion, or replace/retire a derived one with a reason and evidence. Runtime protects immutable user intent and rejects a change that would silently remove user obligations.",
        ),
        agent_action_definition::<action::ObjectiveReviewInput>(
            action::OBJECTIVE_REVIEW_TOOL_ID,
            "Record an evidence-backed review of one Objective criterion as satisfied, gapped, or blocked. Use durable result and evidence references plus a reason; Runtime binds the verdict to the current Objective generation and prevents stale review from finalizing a newer plan.",
        ),
        agent_action_definition::<action::MembershipUpdateInput>(
            action::MEMBERSHIP_UPDATE_TOOL_ID,
            "Request one Agent join or leave a Team using the stable Agent and Team references. Runtime checks delegation and active work, then preserves or safely transfers in-flight obligations rather than silently discarding them.",
        ),
        agent_action_definition::<action::TeamUpdateInput>(
            action::TEAM_UPDATE_TOOL_ID,
            "Refine a Team mission from a durable reference or request Team retirement with a reason. Runtime applies roster and active-work safety checks; this action changes organization, not the immutable user goal.",
        ),
        agent_action_definition::<action::ObjectiveCompleteRequestInput>(
            action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID,
            "Request Objective verification using a committed final artifact, evidence references and explicit Objective-level blockers. Accepted Task limitations remain visible disclosures and are not repeated as blockers. The Supervisor verifies terminal invariants without re-litigating independent Task accept verdicts.",
        ),
    ]
}

fn agent_action_definition<T: schemars::JsonSchema>(
    name: &str,
    description: &str,
) -> RuntimeToolDefinition {
    let input_schema = serde_json::to_value(schemars::schema_for!(T))
        .unwrap_or_else(|_| json!({"type": "object", "additionalProperties": false}));
    // Program revisions are a global journal cursor, not a resource-local
    // concurrency token. Exposing that volatile counter to autonomous Agents
    // makes independent Team/Task mutations contend and turns harmless
    // parallel progress into stale-revision retries. Runtime transition
    // validation, attempt ownership and idempotency keys provide the actual
    // mutation safety. The executor continues to accept an explicit fence for
    // trusted non-model callers, but it is intentionally absent from the
    // model-facing contract.
    RuntimeToolDefinition {
        name: name.to_string(),
        description: Some(description.to_string()),
        input_schema,
        // Collaboration-journal mutations do not directly alter the user
        // workspace or external systems. Concrete tools invoked by an Agent
        // retain their own workspace/danger authorization gates.
        required_permission: ToolPermissionMode::ReadOnly,
        effect_resolver: runtime_effect_resolver("runtime.agent_action"),
    }
}

pub(crate) fn mcp_runtime_tool_definition(tool: &runtime::ManagedMcpTool) -> RuntimeToolDefinition {
    let required_permission = permission_mode_for_mcp_tool(&tool.tool);
    RuntimeToolDefinition {
        name: tool.qualified_name.clone(),
        description: Some(
            tool.tool
                .description
                .clone()
                .unwrap_or_else(|| format!("Invoke MCP tool `{}`.", tool.qualified_name)),
        ),
        input_schema: tool
            .tool
            .input_schema
            .clone()
            .unwrap_or_else(|| json!({ "type": "object", "additionalProperties": true })),
        required_permission,
        effect_resolver: mcp_effect_resolver(required_permission),
    }
}

pub(crate) fn mcp_wrapper_tool_definitions() -> Vec<RuntimeToolDefinition> {
    vec![
        RuntimeToolDefinition {
            name: "mcp_tool".to_string(),
            description: Some(
                "Call a configured MCP tool by its qualified name and JSON arguments.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "qualifiedName": { "type": "string" },
                    "arguments": {}
                },
                "required": ["qualifiedName"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::DangerFullAccess,
            effect_resolver: runtime_effect_resolver("runtime.external_danger"),
        },
        RuntimeToolDefinition {
            name: "list_mcp_resources_tool".to_string(),
            description: Some(
                "List MCP resources from one configured server or from every connected server."
                    .to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" }
                },
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.external_read"),
        },
        RuntimeToolDefinition {
            name: "read_mcp_resource_tool".to_string(),
            description: Some("Read a specific MCP resource from a configured server.".to_string()),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" },
                    "uri": { "type": "string" }
                },
                "required": ["server", "uri"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
            effect_resolver: runtime_effect_resolver("runtime.external_read"),
        },
    ]
}

pub(crate) fn runtime_effect_resolver(resolver_id: &str) -> ToolEffectResolverSpec {
    ToolEffectResolverSpec {
        resolver_id: resolver_id.to_string(),
        resolver_version: 1,
    }
}

fn mcp_effect_resolver(permission: ToolPermissionMode) -> ToolEffectResolverSpec {
    let resolver_id = match permission {
        ToolPermissionMode::ReadOnly => "runtime.external_read",
        ToolPermissionMode::WorkspaceWrite => "runtime.external_write",
        ToolPermissionMode::DangerFullAccess => "runtime.external_danger",
    };
    runtime_effect_resolver(resolver_id)
}

fn permission_mode_for_mcp_tool(tool: &runtime::McpTool) -> ToolPermissionMode {
    let read_only = mcp_annotation_flag(tool, "readOnlyHint");
    let destructive = mcp_annotation_flag(tool, "destructiveHint");
    let open_world = mcp_annotation_flag(tool, "openWorldHint");

    if read_only && !destructive && !open_world {
        ToolPermissionMode::ReadOnly
    } else if destructive || open_world {
        ToolPermissionMode::DangerFullAccess
    } else {
        ToolPermissionMode::WorkspaceWrite
    }
}

fn mcp_annotation_flag(tool: &runtime::McpTool, key: &str) -> bool {
    tool.annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn runtime_hook_config_from_plugin_hooks(hooks: PluginHooks) -> runtime::RuntimeHookConfig {
    runtime::RuntimeHookConfig::new(
        hooks.pre_tool_use,
        hooks.post_tool_use,
        hooks.post_tool_use_failure,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_capability_tool_is_always_registered_as_readonly() {
        let tools = runtime_capability_tool_definitions();

        GatewayToolRegistry::builtin()
            .with_runtime_tools(tools.clone())
            .expect("every production runtime tool must have a concrete effect resolver");

        let lark_read = tools
            .iter()
            .find(|tool| tool.name == "lark_cli_read")
            .expect("Lark read tool");
        assert_eq!(lark_read.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(lark_read.input_schema["required"][0], "args");
        assert_eq!(
            lark_read.effect_resolver.resolver_id,
            "runtime.external_read"
        );
        let lark_write = tools
            .iter()
            .find(|tool| tool.name == "lark_cli_write")
            .expect("Lark write tool");
        assert_eq!(
            lark_write.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
        assert_eq!(
            lark_write.effect_resolver.resolver_id,
            "runtime.external_danger"
        );

        let capability_tool = tools
            .iter()
            .find(|tool| tool.name == "runtime_capabilities")
            .expect("runtime capability tool");
        assert_eq!(
            capability_tool.required_permission,
            ToolPermissionMode::ReadOnly
        );
        assert_eq!(capability_tool.input_schema["required"][0], "intent");
        assert!(capability_tool.input_schema["properties"]["detail"]["enum"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "budget_controls")));

        let context_tool = tools
            .iter()
            .find(|tool| tool.name == "context_retrieve")
            .expect("context retrieval tool");
        assert_eq!(
            context_tool.required_permission,
            ToolPermissionMode::ReadOnly
        );
        assert_eq!(context_tool.input_schema["required"][0], "source");
        assert_eq!(
            context_tool.input_schema["required"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert!(context_tool.input_schema["properties"]["source"]["enum"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "session_catalog")));
        assert!(context_tool.input_schema["properties"]["scope"]["enum"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "explicit_session")));
        assert_eq!(
            context_tool.input_schema["properties"]["memory_id"]["type"],
            "string"
        );
        assert!(context_tool
            .description
            .as_deref()
            .is_some_and(|description| description.contains("not MCP resources")));

        for action_id in harness_contract::agent_action::AGENT_ACTION_TOOL_IDS {
            let action = tools
                .iter()
                .find(|tool| tool.name == *action_id)
                .unwrap_or_else(|| panic!("missing Agent action `{action_id}`"));
            assert_eq!(action.required_permission, ToolPermissionMode::ReadOnly);
            assert_eq!(action.effect_resolver.resolver_id, "runtime.agent_action");
            assert_eq!(action.input_schema["additionalProperties"], false);
            assert!(action.input_schema["properties"]["expected_revision"].is_null());
        }
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.effect_resolver.resolver_id == "runtime.agent_action")
                .count(),
            harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.len()
        );

        let evidence_tool = tools
            .iter()
            .find(|tool| tool.name == "evidence_retrieve")
            .expect("evidence retrieval tool");
        assert_eq!(
            evidence_tool.required_permission,
            ToolPermissionMode::ReadOnly
        );
        assert_eq!(evidence_tool.input_schema["required"][0], "evidence_ref");
        assert_eq!(
            evidence_tool.input_schema["properties"]["query"]["type"],
            "string"
        );
        assert_eq!(
            evidence_tool.input_schema["properties"]["limit"]["maximum"],
            16
        );
        assert!(evidence_tool.input_schema["properties"]
            .get("selector")
            .is_none());
    }

    #[test]
    fn mcp_annotations_select_explicit_external_effect_contracts() {
        for (annotations, expected_permission, expected_resolver) in [
            (
                json!({"readOnlyHint": true}),
                ToolPermissionMode::ReadOnly,
                "runtime.external_read",
            ),
            (
                json!({}),
                ToolPermissionMode::WorkspaceWrite,
                "runtime.external_write",
            ),
            (
                json!({"destructiveHint": true}),
                ToolPermissionMode::DangerFullAccess,
                "runtime.external_danger",
            ),
        ] {
            let definition = mcp_runtime_tool_definition(&runtime::ManagedMcpTool {
                server_name: "fixture".to_string(),
                qualified_name: format!("mcp__fixture__{expected_resolver}"),
                raw_name: "fixture".to_string(),
                tool: runtime::McpTool {
                    name: "fixture".to_string(),
                    description: None,
                    input_schema: Some(json!({"type": "object"})),
                    annotations: Some(annotations),
                    meta: None,
                },
            });
            assert_eq!(definition.required_permission, expected_permission);
            assert_eq!(definition.effect_resolver.resolver_id, expected_resolver);
        }
    }

    #[test]
    fn every_gateway_runtime_tool_declares_a_concrete_effect_contract() {
        let mut tools = runtime_capability_tool_definitions();
        tools.extend(mcp_wrapper_tool_definitions());
        let catalog = GatewayToolRegistry::builtin()
            .with_runtime_tools(tools)
            .expect("all gateway runtime tools must register concrete effects");

        for definition in catalog.definitions(None) {
            if !catalog.has_runtime_tool(&definition.name) {
                continue;
            }
            let resolver = catalog.effect_resolver(&definition.name);
            let effect = tools::tool_orchestrator::resolve_registered_tool_effect(
                &resolver,
                &definition.name,
                &json!({}),
                catalog
                    .required_permission(&definition.name)
                    .expect("registered permission"),
            );
            assert_ne!(
                effect.effect_kind,
                harness_contract::tool::ToolEffectKind::Unknown,
                "{} resolved through {}",
                definition.name,
                resolver.resolver_id
            );
        }
    }
}
