use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::{
    CollaborationProgram, ExecutionCompletionContract, ExecutionDependencyPolicy,
};
use harness_contract::input_disposition::ModelInputDispositionBatch;
use harness_contract::orchestration::{
    ModelGraphMutationProposal, ModelGraphSemanticNode, ModelRuntimeOrchestrationConstraints,
    ModelRuntimeOrchestrationInput, ModelSemanticFocus,
};
use harness_contract::policy::PermissionMode;
use harness_contract::team::{TeamSelectionMode, TeamStrategyBinding};

pub use harness_contract::orchestration::{
    CapabilityRecipeId, ModelRuntimeControlRequest as RuntimeControlRequest, RuntimeControlKind,
    RuntimeControlScope, RuntimeOrchestrationOperation,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticFocus {
    pub focus_id: String,
    pub role_id: String,
    pub objective: String,
    pub resource_scopes: Vec<String>,
    pub evidence_responsibilities: Vec<String>,
    /// Runtime-derived role output schema. Model proposals leave this empty
    /// and inherit the enclosing semantic node contract.
    #[serde(default)]
    pub output_contract: Vec<String>,
    /// Runtime-derived role acceptance. Keeping it on the focus prevents a
    /// node-wide contract from being copied onto every distinct Team role.
    #[serde(default)]
    pub output_acceptance: Vec<String>,
}

impl From<ModelSemanticFocus> for SemanticFocus {
    fn from(value: ModelSemanticFocus) -> Self {
        Self {
            focus_id: value.focus_id,
            role_id: value.role_id,
            objective: value.objective,
            resource_scopes: Vec::new(),
            evidence_responsibilities: value.evidence_responsibilities,
            output_contract: Vec::new(),
            output_acceptance: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSemanticNode {
    pub node_id: String,
    pub recipe: CapabilityRecipeId,
    pub objective: String,
    pub depends_on: Vec<String>,
    pub multiplicity: u16,
    pub focuses: Vec<SemanticFocus>,
    pub template: Option<String>,
    pub target_session_id: Option<String>,
    pub output_artifacts: Vec<String>,
    pub evidence_contract: Vec<String>,
    pub required_evidence_refs: Vec<String>,
    /// Runtime-resolved capability scopes. Model input cannot populate this.
    pub resource_scopes: Vec<String>,
    pub required: bool,
    pub dependency: ExecutionDependencyPolicy,
    pub cancellation_group: Option<String>,
}

impl From<ModelGraphSemanticNode> for GraphSemanticNode {
    fn from(value: ModelGraphSemanticNode) -> Self {
        Self {
            node_id: value.node_id,
            recipe: value.recipe,
            objective: value.objective,
            depends_on: value.depends_on,
            multiplicity: value.multiplicity,
            focuses: value.focuses.into_iter().map(Into::into).collect(),
            template: value.template,
            target_session_id: value.target_session_id,
            output_artifacts: value.output_artifacts,
            evidence_contract: value.evidence_contract,
            required_evidence_refs: value.required_evidence_refs,
            resource_scopes: Vec::new(),
            required: value.required,
            dependency: value.dependency,
            cancellation_group: value.cancellation_group,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphMutationProposal {
    pub mutation_id: String,
    pub target_execution_id: Option<String>,
    pub expected_revision: Option<u64>,
    pub nodes: Vec<GraphSemanticNode>,
    pub completion: ExecutionCompletionContract,
    /// Runtime-owned durable collaboration obligations. Model JSON cannot
    /// populate this field directly; the compiler derives it from validated
    /// Team semantic nodes when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collaboration_program: Option<CollaborationProgram>,
    pub reason: String,
}

impl From<ModelGraphMutationProposal> for GraphMutationProposal {
    fn from(value: ModelGraphMutationProposal) -> Self {
        Self {
            mutation_id: value.mutation_id,
            target_execution_id: value.target_execution_id,
            expected_revision: value.expected_revision,
            nodes: value.nodes.into_iter().map(Into::into).collect(),
            completion: value.completion,
            collaboration_program: None,
            reason: value.reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeOrchestrationBinding {
    pub model_lease: Option<String>,
    pub session_id: Option<String>,
    pub lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    pub mission_id: Option<String>,
    pub selection_mode: Option<TeamSelectionMode>,
    pub strategy_binding: Option<TeamStrategyBinding>,
    pub capabilities: Vec<String>,
    pub surface: Option<String>,
    pub permission_ceiling: PermissionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationCommand {
    pub intent: String,
    pub model_lease: Option<String>,
    pub session_id: Option<String>,
    pub lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    pub mission_id: Option<String>,
    pub operation: RuntimeOrchestrationOperation,
    pub inspect_execution_id: Option<String>,
    pub proposal: Option<GraphMutationProposal>,
    pub template_proposal: Option<serde_json::Value>,
    pub control: Option<RuntimeControlRequest>,
    pub input_disposition: Option<ModelInputDispositionBatch>,
    pub selection_mode: Option<TeamSelectionMode>,
    pub strategy_binding: Option<TeamStrategyBinding>,
    pub capabilities: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub constraints: RuntimeOrchestrationConstraints,
    pub surface: Option<String>,
}

impl RuntimeOrchestrationCommand {
    #[must_use]
    pub fn from_model(
        input: ModelRuntimeOrchestrationInput,
        binding: RuntimeOrchestrationBinding,
    ) -> Self {
        Self {
            intent: input.intent,
            model_lease: binding.model_lease,
            session_id: binding.session_id,
            lineage: binding.lineage,
            mission_id: binding.mission_id,
            operation: input.operation,
            inspect_execution_id: input.inspect_execution_id,
            proposal: input.proposal.map(Into::into),
            template_proposal: input.template_proposal,
            control: input.control,
            input_disposition: input.input_disposition,
            selection_mode: binding.selection_mode,
            strategy_binding: binding.strategy_binding,
            capabilities: binding.capabilities,
            evidence_refs: input.evidence_refs,
            constraints: RuntimeOrchestrationConstraints::from_model(
                input.constraints,
                binding.permission_ceiling,
            ),
            surface: binding.surface,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationConstraints {
    pub max_parallel_agents: Option<usize>,
    pub risk: Option<String>,
    pub approval_id: Option<String>,
    pub requires_write: Option<bool>,
    pub surface_latency_sensitive: Option<bool>,
    pub permission_ceiling: PermissionMode,
}

impl RuntimeOrchestrationConstraints {
    fn from_model(
        value: ModelRuntimeOrchestrationConstraints,
        permission_ceiling: PermissionMode,
    ) -> Self {
        Self {
            max_parallel_agents: value.max_parallel_agents,
            risk: value.risk,
            approval_id: None,
            requires_write: value.requires_write,
            surface_latency_sensitive: value.surface_latency_sensitive,
            permission_ceiling,
        }
    }
}

impl Default for RuntimeOrchestrationConstraints {
    fn default() -> Self {
        Self {
            max_parallel_agents: None,
            risk: None,
            approval_id: None,
            requires_write: None,
            surface_latency_sensitive: None,
            permission_ceiling: PermissionMode::ReadOnly,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_binding_is_injected_after_model_validation() {
        let command = RuntimeOrchestrationCommand::from_model(
            ModelRuntimeOrchestrationInput::minimal_example(),
            RuntimeOrchestrationBinding {
                model_lease: Some("provider/model".to_string()),
                session_id: Some("session-1".to_string()),
                lineage: Some(harness_contract::execution_graph::ExecutionGraphLineage {
                    session_id: "session-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    root_task_id: "task-root-1".to_string(),
                    task_id: "task-root-1".to_string(),
                    generation: 1,
                }),
                mission_id: Some("mission-1".to_string()),
                selection_mode: None,
                strategy_binding: None,
                capabilities: vec!["tool:read_file".to_string()],
                surface: Some("webui".to_string()),
                permission_ceiling: PermissionMode::WorkspaceWrite,
            },
        );
        assert_eq!(command.session_id.as_deref(), Some("session-1"));
        assert_eq!(command.model_lease.as_deref(), Some("provider/model"));
        assert_eq!(
            command.constraints.permission_ceiling,
            PermissionMode::WorkspaceWrite
        );
        assert!(command
            .proposal
            .as_ref()
            .expect("proposal")
            .nodes
            .iter()
            .all(|node| node.resource_scopes.is_empty()));
    }
}
