use std::sync::{Arc, Mutex, OnceLock};

use runtime::{ConfigLoader, ToolError, ToolExecutor};
use serde::Deserialize;
use tools::permissions::PermissionMode as ToolPermissionMode;
use tools::ToolHost;
#[cfg(test)]
use tools::ToolHostSnapshot;

use crate::lark_cli_tool::{execute_lark_cli_tool, LarkCliToolMode, LarkCliToolRequest};
#[cfg(test)]
use crate::runtime_bootstrap::GatewayToolRegistry;
use crate::{format_tool_result, AllowedToolSet};

#[path = "gateway_tool_executor/context_support.rs"]
mod context_support;
use context_support::*;

#[derive(Debug, Deserialize)]
struct ToolSearchRequest {
    query: String,
    max_results: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct McpToolRequest {
    #[serde(rename = "qualifiedName")]
    qualified_name: Option<String>,
    tool: Option<String>,
    arguments: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ListMcpResourcesRequest {
    server: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadMcpResourceRequest {
    server: String,
    uri: String,
}

fn parse_qualified_mcp_name(qualified: &str) -> Result<(String, String), ToolError> {
    let Some((server, tool)) = qualified
        .strip_prefix("mcp__")
        .and_then(|value| value.split_once("__"))
    else {
        return Err(ToolError::new(format!(
            "invalid MCP tool name `{qualified}`; expected `mcp__server__tool`"
        )));
    };
    if server.is_empty() || tool.is_empty() {
        return Err(ToolError::new(format!(
            "invalid MCP tool name `{qualified}`; server and tool are required"
        )));
    }
    Ok((server.to_string(), tool.to_string()))
}

#[derive(Debug, Deserialize)]
struct RuntimeCapabilitiesRequest {
    intent: String,
    surface: Option<String>,
    profile: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RuntimeConfigViewRequest {
    detail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContextRemainingRequest {
    #[serde(default)]
    detail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeResourceCapabilitiesRequest {
    resource_kind: String,
    mime: Option<String>,
    intent: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceRetrieveToolRequest {
    evidence_ref: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ContextRetrieveSource {
    Memory,
    Program,
    Artifact,
    Fact,
    Matrix,
    SessionCatalog,
    SessionHistory,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ContextRetrieveScope {
    Current,
    RelatedSessions,
    WorkspaceSessions,
    ExplicitSession,
}

#[derive(Debug, Deserialize)]
struct ContextRetrieveRequest {
    source: ContextRetrieveSource,
    #[serde(default)]
    query: Option<String>,
    memory_id: Option<String>,
    content_cursor: Option<String>,
    cursor: Option<String>,
    entry_ref: Option<String>,
    parent_ref: Option<String>,
    scope: Option<ContextRetrieveScope>,
    session_id: Option<String>,
    limit: Option<usize>,
    before_sequence: Option<usize>,
    message_id: Option<String>,
    message_digest: Option<String>,
    sequence: Option<usize>,
    block_cursor: Option<usize>,
    block_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeToolExecutionBinding<'a> {
    action_id: Option<&'a str>,
    session_id: Option<&'a str>,
    authorized_scopes: &'a [String],
    memory_context: Option<&'a memory::MemoryTurnContext>,
    reality_context: Option<&'a harness_contract::agent::AgentDataLease>,
    model_lease: Option<&'a str>,
    parent_execution: Option<&'a harness_contract::execution_graph::ExecutionParentBinding>,
    execution_decision: Option<&'a runtime::RuntimeExecutionDecision>,
    permission_ceiling: harness_contract::policy::PermissionMode,
}

fn is_gateway_runtime_control_tool(tool_name: &str) -> bool {
    is_agent_action_tool(tool_name)
        || matches!(
            tool_name,
            "runtime_config_view"
                | "runtime_resource_capabilities"
                | "runtime_capabilities"
                | "mcp_tool"
                | "list_mcp_resources_tool"
                | "read_mcp_resource_tool"
                | "lark_cli_read"
                | "lark_cli_write"
                | "evidence_retrieve"
                | "artifact_publish"
                | "artifact_materialize"
                | "get_context_remaining"
                | "working_context"
                | "private_note"
        )
}

fn is_agent_action_tool(tool_name: &str) -> bool {
    harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name)
}

fn parse_agent_action(
    tool_name: &str,
    value: serde_json::Value,
) -> Result<harness_contract::agent_action::AgentAction, serde_json::Error> {
    use harness_contract::agent_action as action;

    Ok(match tool_name {
        action::STATE_INSPECT_TOOL_ID => {
            action::AgentAction::StateInspect(serde_json::from_value(value)?)
        }
        action::TEAM_CREATE_TOOL_ID => {
            action::AgentAction::TeamCreate(serde_json::from_value(value)?)
        }
        action::AGENT_INVITE_TOOL_ID => {
            action::AgentAction::AgentInvite(serde_json::from_value(value)?)
        }
        action::TASK_PUBLISH_TOOL_ID => {
            action::AgentAction::TaskPublish(serde_json::from_value(value)?)
        }
        action::TASK_CLAIM_TOOL_ID => {
            action::AgentAction::TaskClaim(serde_json::from_value(value)?)
        }
        action::TASK_RELEASE_TOOL_ID => {
            action::AgentAction::TaskRelease(serde_json::from_value(value)?)
        }
        action::TASK_SUPERSEDE_TOOL_ID => {
            action::AgentAction::TaskSupersede(serde_json::from_value(value)?)
        }
        action::TASK_WITHDRAW_TOOL_ID => {
            action::AgentAction::TaskWithdraw(serde_json::from_value(value)?)
        }
        action::TASK_SUBMIT_TOOL_ID => {
            action::AgentAction::TaskSubmit(serde_json::from_value(value)?)
        }
        action::TASK_REVIEW_TOOL_ID => {
            action::AgentAction::TaskReview(serde_json::from_value(value)?)
        }
        action::MESSAGE_PUBLISH_TOOL_ID => {
            action::AgentAction::MessagePublish(serde_json::from_value(value)?)
        }
        action::ARTIFACT_COMMIT_TOOL_ID => {
            action::AgentAction::ArtifactCommit(serde_json::from_value(value)?)
        }
        action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID => {
            action::AgentAction::ObjectiveCompleteRequest(serde_json::from_value(value)?)
        }
        action::OBJECTIVE_UPDATE_TOOL_ID => {
            action::AgentAction::ObjectiveUpdate(serde_json::from_value(value)?)
        }
        action::OBJECTIVE_REVIEW_TOOL_ID => {
            action::AgentAction::ObjectiveReview(serde_json::from_value(value)?)
        }
        action::MEMBERSHIP_UPDATE_TOOL_ID => {
            action::AgentAction::MembershipUpdate(serde_json::from_value(value)?)
        }
        action::TEAM_UPDATE_TOOL_ID => {
            action::AgentAction::TeamUpdate(serde_json::from_value(value)?)
        }
        _ => {
            return Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown Agent action tool `{tool_name}`"),
            )))
        }
    })
}

fn serialize_agent_action_receipt(
    observation: &harness_contract::agent_action::AgentActionObservation,
    resolved_content_ref: Option<&str>,
) -> Result<String, serde_json::Error> {
    let mut receipt = serde_json::to_value(observation)?;
    if let (Some(content_ref), Some(object)) = (resolved_content_ref, receipt.as_object_mut()) {
        // `content_ref` is Runtime-resolved before the typed action is
        // applied. Echo the physical selector in the receipt so the model can
        // immediately submit/review the exact committed artifact instead of
        // guessing it from the logical `changed_refs` entry.
        object.insert(
            "content_ref".to_string(),
            serde_json::Value::String(content_ref.to_string()),
        );
    }
    serde_json::to_string_pretty(&receipt)
}

fn root_agent_action_actor(
    binding: RuntimeToolExecutionBinding<'_>,
) -> harness_contract::agent_action::AgentActorBinding {
    let session_id = binding.session_id.unwrap_or("standalone-session");
    let turn_id = binding
        .execution_decision
        .and_then(|decision| decision.turn_ref.as_deref())
        .unwrap_or("standalone-turn");
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let required_team_count = binding
        .execution_decision
        .and_then(|decision| decision.collaboration_obligation.as_ref())
        .map_or(0, |obligation| obligation.required_team_count());
    let mut resource_scopes = binding.authorized_scopes.to_vec();
    if let Some(decision) = binding.execution_decision {
        resource_scopes.extend(
            decision
                .strategy
                .understanding
                .required_workspace_evidence_scopes
                .iter()
                .cloned(),
        );
    }
    resource_scopes.sort();
    resource_scopes.dedup();
    let actor_id = format!("root:{session_id}");
    harness_contract::agent_action::AgentActorBinding {
        objective_id,
        program_id,
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_execution_id: binding
            .execution_decision
            .and_then(|decision| decision.execution_graph_ref.clone()),
        required_team_count,
        objective_summary: binding
            .execution_decision
            .map(|decision| decision.user_intent_preview.clone())
            .unwrap_or_default(),
        model_lease: binding.model_lease.unwrap_or("default").to_string(),
        permission_ceiling: Some(binding.permission_ceiling),
        resource_scopes,
        actor_id,
        kind: harness_contract::agent_action::AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    }
}

fn is_gateway_context_tool(tool_name: &str) -> bool {
    tool_name == "context_retrieve"
}

fn effective_runtime_execution_decision(
    request_decision: Option<&runtime::RuntimeExecutionDecision>,
    shared_fallback: Option<runtime::RuntimeExecutionDecision>,
) -> Option<runtime::RuntimeExecutionDecision> {
    request_decision.cloned().or(shared_fallback)
}

pub(crate) struct GatewayToolExecutor {
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    tool_host: Arc<ToolHost>,
    runtime_session_id: Option<String>,
    runtime_memory_context: Option<memory::MemoryTurnContext>,
    runtime_model_lease: Option<String>,
    runtime_permission_ceiling: harness_contract::policy::PermissionMode,
    runtime_execution_decision: Arc<Mutex<Option<runtime::RuntimeExecutionDecision>>>,
    runtime_services: Arc<OnceLock<Arc<runtime::RuntimeServices>>>,
}

include!("gateway_tool_executor/runtime_tools.rs");
include!("gateway_tool_executor/content_publication.rs");
include!("gateway_tool_executor/reality_tools.rs");
include!("gateway_tool_executor/runtime_introspection.rs");
include!("gateway_tool_executor/authorized_execution.rs");
include!("gateway_tool_executor/tool_executor_impl.rs");
include!("gateway_tool_executor/runtime_host_impl.rs");
include!("gateway_tool_executor/evidence_support.rs");
include!("gateway_tool_executor/tests.rs");
use sha2::{Digest, Sha256};
