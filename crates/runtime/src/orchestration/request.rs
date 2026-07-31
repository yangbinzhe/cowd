use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::ExecutionCompletionContract;
use harness_contract::policy::PermissionMode;
use harness_contract::team::{TeamSelectionMode, TeamStrategyBinding};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOrchestrationOperation {
    #[default]
    Inspect,
    Propose,
    Revise,
    Control,
}

impl RuntimeOrchestrationOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Propose => "propose",
            Self::Revise => "revise",
            Self::Control => "control",
        }
    }
}

/// Model-visible semantic recipes. Runtime resolves these into immutable
/// executors, definitions, leases and physical graph identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRecipeId {
    Direct,
    Agent,
    Team,
    Review,
    Synthesis,
    SessionDispatch,
}

impl CapabilityRecipeId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Agent => "agent",
            Self::Team => "team",
            Self::Review => "review",
            Self::Synthesis => "synthesis",
            Self::SessionDispatch => "session_dispatch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticFocus {
    pub focus_id: String,
    pub role_id: String,
    pub objective: String,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    #[serde(default)]
    pub evidence_responsibilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSemanticNode {
    pub node_id: String,
    pub recipe: CapabilityRecipeId,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default = "default_multiplicity")]
    pub multiplicity: u16,
    #[serde(default)]
    pub focuses: Vec<SemanticFocus>,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub input_refs: Vec<String>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
}

const fn default_multiplicity() -> u16 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphMutationProposal {
    pub mutation_id: String,
    #[serde(default)]
    pub target_execution_id: Option<String>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    pub nodes: Vec<GraphSemanticNode>,
    #[serde(default)]
    pub completion: ExecutionCompletionContract,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlKind {
    Pause,
    Resume,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlScope {
    Mission,
    #[default]
    Graph,
    Agent,
    Team,
    Subgraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeControlRequest {
    pub target_execution_id: String,
    pub expected_revision: u64,
    #[serde(default)]
    pub scope: RuntimeControlScope,
    #[serde(default)]
    pub target_node_id: Option<String>,
    pub action: RuntimeControlKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOrchestrationRequest {
    pub intent: String,
    /// Runtime-owned execution binding. Gateway injects the active model.
    #[serde(default)]
    pub model_lease: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation: RuntimeOrchestrationOperation,
    /// Optional graph inspected by the read-only operation.
    #[serde(default)]
    pub inspect_execution_id: Option<String>,
    #[serde(default)]
    pub proposal: Option<GraphMutationProposal>,
    #[serde(default)]
    pub control: Option<RuntimeControlRequest>,
    /// Runtime-owned selection source. Model JSON is always sanitized.
    #[serde(default)]
    pub selection_mode: Option<TeamSelectionMode>,
    #[serde(default)]
    pub strategy_binding: Option<TeamStrategyBinding>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub constraints: RuntimeOrchestrationConstraints,
    #[serde(default)]
    pub surface: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOrchestrationConstraints {
    #[serde(default)]
    pub max_parallel_agents: Option<usize>,
    #[serde(default)]
    pub risk: Option<String>,
    #[serde(default)]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub requires_write: Option<bool>,
    #[serde(default)]
    pub surface_latency_sensitive: Option<bool>,
    #[serde(default = "default_permission_ceiling")]
    pub permission_ceiling: PermissionMode,
}

const fn default_permission_ceiling() -> PermissionMode {
    PermissionMode::ReadOnly
}

impl Default for RuntimeOrchestrationConstraints {
    fn default() -> Self {
        Self {
            max_parallel_agents: None,
            risk: None,
            approval_id: None,
            requires_write: None,
            surface_latency_sensitive: None,
            permission_ceiling: default_permission_ceiling(),
        }
    }
}
