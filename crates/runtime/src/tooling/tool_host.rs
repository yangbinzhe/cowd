use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::ExecutionParentBinding;

use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;

/// In-process progress sink for long-running tools such as bash (T4). The
/// wrapper keeps the request struct's derives (Debug/PartialEq) valid while
/// serde always skips the callback; only its presence is ever compared.
#[derive(Clone, Default)]
pub struct ToolProgressSink(pub Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>);

impl std::fmt::Debug for ToolProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ToolProgressSink")
            .field(&self.0.is_some())
            .finish()
    }
}

impl PartialEq for ToolProgressSink {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_some() == other.0.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionRequest {
    pub governed_plan_id: String,
    pub governed_plan_revision: u64,
    pub idempotency_key: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: String,
    pub category: ToolSafetyCategory,
    /// Runtime-issued authorization for an ordinary tool effect. Gateway
    /// control tools do not use this field; delegated Agent tools carry it so
    /// the concrete ToolHost can verify the same descriptor before execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
    /// Logical parent session that owns this graph node. It is not inferred
    /// from a process-global tool host, because a single Gateway serves many
    /// concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Runtime-derived evidence/artifact scopes the caller is authorized to
    /// read. Derived from the owning Agent binding and session, never from
    /// model input. Empty means no scoped evidence access.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorized_scopes: Vec<String>,
    /// Exact Memory read binding compiled by the owning ConversationRuntime.
    ///
    /// Context retrieval must not reconstruct Agent/Definition/Team authority
    /// from a Session id at the Gateway boundary. Primary and delegated
    /// runtimes carry the same lease that passive context assembly uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_context: Option<memory::MemoryTurnContext>,
    /// Selected parent model lease. Runtime control tools inherit this binding
    /// for any child AgentTask graph instead of resolving an imaginary default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_lease: Option<String>,
    /// Canonical parent graph/node that issued this tool call. It is supplied
    /// by the Runner, never parsed from model-generated tool JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    /// Immutable execution decision for the owning conversation turn. Runtime
    /// supplies this lease so orchestration validation and Team identity never
    /// consult process-global Gateway state in concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_decision: Option<crate::RuntimeExecutionDecision>,
    /// Runtime-issued paired evaluation traffic. Tool adapters use this to
    /// enforce the evaluation isolation policy rather than trusting a model
    /// supplied session or tool parameter.
    #[serde(default)]
    pub evaluation_isolated: bool,
    /// Runtime-issued Managed Agent invocation fence.  Gateway may execute a
    /// side effect only after Runtime has recorded and claimed the matching
    /// outbox entry; conversational calls always leave this empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_invocation: Option<harness_contract::managed_agent::ManagedAgentInvocationFence>,
    /// Optional per-invocation progress sink used by bash's tokio path (T4).
    /// The callback is in-process only; it is never serialized.
    #[serde(skip)]
    pub tool_progress: ToolProgressSink,
}

impl RuntimeToolExecutionRequest {
    #[must_use]
    pub fn from_tool_request(request: &ToolRequest) -> Self {
        Self {
            governed_plan_id: "offline-unknown".to_string(),
            governed_plan_revision: 0,
            idempotency_key: request.tool_use_id.clone(),
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            category: ToolSafetyCategory::Destructive,
            authorization: None,
            session_id: None,
            authorized_scopes: Vec::new(),
            memory_context: None,
            model_lease: None,
            parent_execution: None,
            execution_decision: None,
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: ToolProgressSink::default(),
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
#[async_trait::async_trait]
pub trait RuntimeExecutionHost: Send + Sync {
    async fn execute_runtime_tool(
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

    /// Return the concrete host descriptor used to authorize a delegated
    /// tool. `None` means that the current host snapshot cannot execute it.
    fn delegated_tool_effect_descriptor(
        &self,
        _tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        None
    }
}
