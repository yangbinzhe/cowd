use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::ExecutionParentBinding;

use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionRequest {
    pub idempotency_key: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: String,
    pub category: ToolSafetyCategory,
    /// Logical parent session that owns this graph node. It is not inferred
    /// from a process-global tool host, because a single Gateway serves many
    /// concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Selected parent model lease. Runtime control tools inherit this binding
    /// for any child AgentTask graph instead of resolving an imaginary default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_lease: Option<String>,
    /// Canonical parent graph/node that issued this tool call. It is supplied
    /// by the Runner, never parsed from model-generated tool JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
}

impl RuntimeToolExecutionRequest {
    #[must_use]
    pub fn from_tool_request(request: &ToolRequest) -> Self {
        Self {
            idempotency_key: request.tool_use_id.clone(),
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            category: ToolSafetyCategory::from_tool_name(&request.tool_name),
            session_id: None,
            model_lease: None,
            parent_execution: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeToolExecutionStatus {
    Executed,
    BlockedPermission,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionOutcome {
    pub tool_use_id: String,
    pub tool_name: String,
    pub status: RuntimeToolExecutionStatus,
    pub category: ToolSafetyCategory,
    pub output: Option<String>,
    pub error: Option<String>,
    pub evidence_ref: String,
}

/// Thin adapter used by the canonical ToolBatch node executor.
///
/// Graph scheduling, dependency resolution, policy, retries, and lifecycle
/// commits are deliberately outside this contract and belong to the Runner.
pub trait RuntimeExecutionHost: Send + Sync {
    fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> RuntimeToolExecutionOutcome;

    /// Return model-facing contracts for a delegated AgentTask's already
    /// authorized tools. The default preserves compatibility for lightweight
    /// hosts, while Gateway supplies its canonical schemas and descriptions.
    fn delegated_tool_definitions(
        &self,
        tool_names: &[String],
    ) -> Vec<crate::ProviderToolDefinition> {
        tool_names
            .iter()
            .map(|name| crate::ProviderToolDefinition {
                name: name.clone(),
                description: Some("Task-authorized runtime tool".to_string()),
                input_schema: serde_json::json!({"type": "object"}),
            })
            .collect()
    }
}
