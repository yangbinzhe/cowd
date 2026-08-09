use serde::{Deserialize, Serialize};

use crate::execution_graph::{ExecutionCompletionContract, ExecutionDependencyPolicy};
use crate::input_disposition::ModelInputDispositionBatch;

pub const RUNTIME_ORCHESTRATE_TOOL_ID: &str = "runtime_orchestrate";

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOrchestrationOperation {
    #[default]
    Inspect,
    Propose,
    Revise,
    Control,
    RouteInput,
}

impl RuntimeOrchestrationOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Propose => "propose",
            Self::Revise => "revise",
            Self::Control => "control",
            Self::RouteInput => "route_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelSemanticFocus {
    pub focus_id: String,
    pub role_id: String,
    pub objective: String,
    #[serde(default)]
    pub evidence_responsibilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelGraphSemanticNode {
    pub node_id: String,
    pub recipe: CapabilityRecipeId,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Number of parallel instances Runtime should materialize. Defaults to
    /// one and remains subject to the Runtime concurrency ceiling.
    #[serde(default = "default_multiplicity")]
    #[schemars(range(min = 1, max = 100))]
    pub multiplicity: u16,
    #[serde(default)]
    pub focuses: Vec<ModelSemanticFocus>,
    #[serde(default)]
    pub template: Option<String>,
    /// Explicit destination for `session_dispatch`. Runtime rejects this field
    /// on every other recipe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_session_id: Option<String>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default)]
    pub required_evidence_refs: Vec<String>,
    /// Whether failure prevents the parent execution from completing.
    /// Defaults to true; false is only for optional, cancellable lanes.
    #[serde(default = "default_required")]
    pub required: bool,
    #[serde(default)]
    pub dependency: ExecutionDependencyPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_group: Option<String>,
}

const fn default_multiplicity() -> u16 {
    1
}

const fn default_required() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelGraphMutationProposal {
    pub mutation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    pub nodes: Vec<ModelGraphSemanticNode>,
    #[serde(default)]
    pub completion: ExecutionCompletionContract,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlKind {
    Pause,
    Resume,
    Cancel,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlScope {
    Mission,
    #[default]
    Graph,
    Agent,
    Team,
    Subgraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRuntimeControlRequest {
    pub target_execution_id: String,
    pub expected_revision: u64,
    #[serde(default)]
    pub scope: RuntimeControlScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    pub action: RuntimeControlKind,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRuntimeOrchestrationConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_parallel_agents: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_write: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_latency_sensitive: Option<bool>,
}

/// The complete model-visible contract for `runtime_orchestrate`.
///
/// Authenticated Session identity, model/provider leases, capability grants,
/// resource leases, physical graph IDs, strategy bindings and permission
/// ceilings are deliberately absent. Gateway and Runtime bind them after this
/// contract has been validated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRuntimeOrchestrationInput {
    pub intent: String,
    #[serde(default)]
    pub operation: RuntimeOrchestrationOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspect_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<ModelGraphMutationProposal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ModelRuntimeControlRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_disposition: Option<ModelInputDispositionBatch>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub constraints: ModelRuntimeOrchestrationConstraints,
}

impl ModelRuntimeOrchestrationInput {
    #[must_use]
    pub fn minimal_example() -> Self {
        Self {
            intent: "Review the implementation and return verified evidence".to_string(),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(ModelGraphMutationProposal {
                mutation_id: "review-v1".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![ModelGraphSemanticNode {
                    node_id: "review".to_string(),
                    recipe: CapabilityRecipeId::Review,
                    objective: "Review the implementation against the stated goal".to_string(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: Vec::new(),
                    template: None,
                    target_session_id: None,
                    output_artifacts: vec!["review_report".to_string()],
                    evidence_contract: vec!["findings".to_string(), "evidence".to_string()],
                    required_evidence_refs: Vec::new(),
                    required: true,
                    dependency: ExecutionDependencyPolicy::All,
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                reason: "The objective requires an independently verified result".to_string(),
            }),
            control: None,
            input_disposition: None,
            evidence_refs: Vec::new(),
            constraints: ModelRuntimeOrchestrationConstraints::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_contract_excludes_runtime_owned_fields_and_input_refs() {
        let schema = serde_json::to_string(&schemars::schema_for!(ModelRuntimeOrchestrationInput))
            .expect("serialize schema");
        for forbidden in [
            "input_refs",
            "session_id",
            "model_lease",
            "permission_ceiling",
            "selection_mode",
            "strategy_binding",
            "capabilities",
            "resource_scopes",
            "surface",
            "input_id",
            "request_id",
            "turn_id",
            "task_id",
            "mission_id",
            "graph_id",
            "lease",
        ] {
            assert!(
                !schema.contains(&format!("\"{forbidden}\"")),
                "model schema leaked Runtime-owned field `{forbidden}`"
            );
        }
        assert!(schema.contains("\"target_session_id\""));
    }

    #[test]
    fn route_input_contract_contains_only_semantic_slots() {
        let input: ModelRuntimeOrchestrationInput = serde_json::from_value(serde_json::json!({
            "intent": "route newly arrived work",
            "operation": "route_input",
            "input_disposition": {
                "decisions": [{
                    "input_slots": [0, 1],
                    "action": "add_required_task",
                    "relation": "new_task",
                    "objective": "implement and verify the requested change",
                    "required": true,
                    "confidence_basis_points": 9500,
                    "reason": "both updates describe the same required work"
                }]
            }
        }))
        .expect("semantic route input");
        assert_eq!(input.operation, RuntimeOrchestrationOperation::RouteInput);
        input
            .input_disposition
            .expect("disposition")
            .validate_slots(2)
            .expect("complete slot coverage");
    }

    #[test]
    fn minimal_example_is_valid_model_input() {
        let value =
            serde_json::to_value(ModelRuntimeOrchestrationInput::minimal_example()).expect("value");
        let decoded: ModelRuntimeOrchestrationInput =
            serde_json::from_value(value).expect("typed example");
        assert_eq!(decoded.operation, RuntimeOrchestrationOperation::Propose);
    }

    #[test]
    fn omitted_node_controls_use_the_runtime_safe_defaults() {
        let request: ModelRuntimeOrchestrationInput = serde_json::from_value(serde_json::json!({
            "intent": "Review the implementation",
            "operation": "propose",
            "proposal": {
                "mutation_id": "review-defaults",
                "nodes": [{
                    "node_id": "review",
                    "recipe": "review",
                    "objective": "Review the implementation"
                }],
                "reason": "Independent verification is required"
            }
        }))
        .expect("model request uses serde defaults");
        let node = &request.proposal.expect("proposal").nodes[0];
        assert_eq!(node.multiplicity, 1);
        assert!(node.required);
        assert_eq!(node.dependency, ExecutionDependencyPolicy::All);
    }
}
