//! Strongly typed disposition of inputs that arrive while a Turn is running.
//!
//! The model chooses semantic work. Runtime binds physical identities and owns
//! materialization; no model-visible field can mint a Session, Task, Team, or
//! execution identity.

use serde::{Deserialize, Serialize};

use crate::{
    execution_graph::{ExecutionCompletionContract, ExecutionDependencyPolicy},
    orchestration::{CapabilityRecipeId, ModelGraphSemanticNode, ModelSemanticFocus},
};

/// Model-visible graph node for input disposition. Physical Session identity
/// is intentionally absent and is injected by Runtime after authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelInputDispositionGraphNode {
    pub node_id: String,
    pub recipe: CapabilityRecipeId,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default = "default_multiplicity")]
    #[schemars(range(min = 1, max = 100))]
    pub multiplicity: u16,
    #[serde(default)]
    pub focuses: Vec<ModelSemanticFocus>,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default)]
    pub required_evidence_refs: Vec<String>,
    #[serde(default = "default_required")]
    pub required: bool,
    #[serde(default)]
    pub dependency: ExecutionDependencyPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_group: Option<String>,
}

impl From<ModelInputDispositionGraphNode> for ModelGraphSemanticNode {
    fn from(value: ModelInputDispositionGraphNode) -> Self {
        Self {
            node_id: value.node_id,
            recipe: value.recipe,
            objective: value.objective,
            depends_on: value.depends_on,
            multiplicity: value.multiplicity,
            focuses: value.focuses,
            template: value.template,
            target_session_id: None,
            output_artifacts: value.output_artifacts,
            evidence_contract: value.evidence_contract,
            required_evidence_refs: value.required_evidence_refs,
            required: value.required,
            dependency: value.dependency,
            cancellation_group: value.cancellation_group,
        }
    }
}

const fn default_multiplicity() -> u16 {
    1
}

/// Model-visible execution shape for one disposition. Runtime owns mutation,
/// execution and revision identities and binds them only after validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelInputDispositionGraphPlan {
    pub nodes: Vec<ModelInputDispositionGraphNode>,
    #[serde(default)]
    pub completion: ExecutionCompletionContract,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputDispositionAction {
    AmendCurrentTurn,
    ReplanCurrentGraph,
    ReplaceCurrentTask,
    AddRequiredTask,
    AddBackgroundTask,
    AddTeamLane,
    AddTaskWithTeam,
    DispatchSession,
    ProgressOrControl,
    Clarify,
}

impl InputDispositionAction {
    #[must_use]
    pub const fn is_structural(self) -> bool {
        matches!(
            self,
            Self::ReplanCurrentGraph
                | Self::ReplaceCurrentTask
                | Self::AddRequiredTask
                | Self::AddBackgroundTask
                | Self::AddTeamLane
                | Self::AddTaskWithTeam
                | Self::DispatchSession
                | Self::Clarify
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputWorkRelation {
    Supplement,
    Replan,
    Progress,
    Background,
    NewTask,
    NewSession,
    Subtask,
    CrossSession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputDispositionSessionTargetMode {
    ExistingAuthorized,
    CreateIsolated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelInputDispositionSessionTarget {
    pub mode: InputDispositionSessionTargetMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelInputDispositionDecision {
    /// Stable zero-based slots supplied by Runtime for this checkpoint. One
    /// decision may group inputs that describe the same unit of work.
    pub input_slots: Vec<u16>,
    pub action: InputDispositionAction,
    pub relation: InputWorkRelation,
    pub objective: String,
    #[serde(default = "default_required")]
    pub required: bool,
    #[schemars(range(min = 0, max = 10000))]
    pub confidence_basis_points: u16,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_plan: Option<ModelInputDispositionGraphPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_target: Option<ModelInputDispositionSessionTarget>,
}

const fn default_required() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelInputDispositionBatch {
    pub decisions: Vec<ModelInputDispositionDecision>,
}

impl ModelInputDispositionBatch {
    pub fn validate_slots(&self, available_slots: usize) -> Result<(), String> {
        if available_slots == 0 {
            return Err("input disposition has no available Runtime slots".to_string());
        }
        if self.decisions.is_empty() {
            return Err("input disposition decisions must not be empty".to_string());
        }
        if self.decisions.len() > 1
            && self
                .decisions
                .iter()
                .any(|decision| decision.action == InputDispositionAction::ReplaceCurrentTask)
        {
            return Err("replace_current_task must own the complete checkpoint batch".to_string());
        }
        let mut covered = vec![false; available_slots];
        for (decision_index, decision) in self.decisions.iter().enumerate() {
            if decision.input_slots.is_empty() {
                return Err(format!(
                    "input disposition decision {decision_index} has no input slots"
                ));
            }
            if decision.objective.trim().is_empty() || decision.reason.trim().is_empty() {
                return Err(format!(
                    "input disposition decision {decision_index} requires objective and reason"
                ));
            }
            decision
                .validate_semantics()
                .map_err(|error| format!("input disposition decision {decision_index}: {error}"))?;
            for slot in &decision.input_slots {
                let slot = usize::from(*slot);
                if slot >= available_slots {
                    return Err(format!(
                        "input disposition slot {slot} is outside 0..{available_slots}"
                    ));
                }
                if std::mem::replace(&mut covered[slot], true) {
                    return Err(format!(
                        "input disposition slot {slot} is covered more than once"
                    ));
                }
            }
        }
        let missing = covered
            .iter()
            .enumerate()
            .filter_map(|(slot, covered)| (!covered).then_some(slot))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "input disposition did not cover slots {}",
                missing
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        Ok(())
    }
}

impl ModelInputDispositionDecision {
    fn validate_semantics(&self) -> Result<(), String> {
        if self.confidence_basis_points > 10_000 {
            return Err("confidence_basis_points must be within 0..=10000".to_string());
        }
        let relation_matches = matches!(
            (self.action, self.relation),
            (
                InputDispositionAction::AmendCurrentTurn,
                InputWorkRelation::Supplement
            ) | (
                InputDispositionAction::ReplanCurrentGraph,
                InputWorkRelation::Replan
            ) | (
                InputDispositionAction::ReplaceCurrentTask,
                InputWorkRelation::NewTask
            ) | (
                InputDispositionAction::AddRequiredTask,
                InputWorkRelation::NewTask | InputWorkRelation::Subtask
            ) | (
                InputDispositionAction::AddBackgroundTask,
                InputWorkRelation::Background
                    | InputWorkRelation::NewTask
                    | InputWorkRelation::Subtask
            ) | (
                InputDispositionAction::AddTeamLane,
                InputWorkRelation::Supplement | InputWorkRelation::Subtask
            ) | (
                InputDispositionAction::AddTaskWithTeam,
                InputWorkRelation::NewTask
                    | InputWorkRelation::Background
                    | InputWorkRelation::Subtask
            ) | (
                InputDispositionAction::DispatchSession,
                InputWorkRelation::NewSession | InputWorkRelation::CrossSession
            ) | (
                InputDispositionAction::ProgressOrControl,
                InputWorkRelation::Progress
            ) | (InputDispositionAction::Clarify, _)
        );
        if !relation_matches {
            return Err("action and work relation are inconsistent".to_string());
        }
        if self.action == InputDispositionAction::AddBackgroundTask && self.required {
            return Err("background work must not block the current turn".to_string());
        }
        if self.action == InputDispositionAction::AddRequiredTask && !self.required {
            return Err("required work must block the current turn".to_string());
        }
        if self.relation == InputWorkRelation::Background && self.required {
            return Err("background relation must not block the current turn".to_string());
        }
        match (&self.action, &self.session_target) {
            (
                InputDispositionAction::DispatchSession,
                Some(ModelInputDispositionSessionTarget {
                    mode: InputDispositionSessionTargetMode::ExistingAuthorized,
                    target_ref: Some(target_ref),
                }),
            ) if !target_ref.trim().is_empty() => {}
            (
                InputDispositionAction::DispatchSession,
                Some(ModelInputDispositionSessionTarget {
                    mode: InputDispositionSessionTargetMode::CreateIsolated,
                    target_ref: None,
                }),
            ) => {}
            (InputDispositionAction::DispatchSession, _) => {
                return Err(
                    "dispatch_session requires an exact existing target_ref or create_isolated"
                        .to_string(),
                );
            }
            (_, None) => {}
            (_, Some(_)) => {
                return Err("session_target is only valid for dispatch_session".to_string());
            }
        }
        let recipes = self
            .graph_plan
            .as_ref()
            .map(|proposal| {
                proposal
                    .nodes
                    .iter()
                    .map(|node| node.recipe)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if self.action != InputDispositionAction::DispatchSession
            && recipes.contains(&CapabilityRecipeId::SessionDispatch)
        {
            return Err(
                "session_dispatch recipe is only valid for dispatch_session action".to_string(),
            );
        }
        if !matches!(
            self.action,
            InputDispositionAction::AddTeamLane | InputDispositionAction::AddTaskWithTeam
        ) && recipes.contains(&CapabilityRecipeId::Team)
        {
            return Err("team recipe requires a typed team action".to_string());
        }
        match self.action {
            InputDispositionAction::AddTeamLane | InputDispositionAction::AddTaskWithTeam => {
                if !recipes.contains(&CapabilityRecipeId::Team) {
                    return Err(
                        "team actions require a graph_plan containing a team recipe".to_string()
                    );
                }
            }
            InputDispositionAction::DispatchSession => {
                if !recipes.contains(&CapabilityRecipeId::SessionDispatch) {
                    return Err(
                        "dispatch_session requires a session_dispatch graph recipe".to_string()
                    );
                }
            }
            InputDispositionAction::ReplaceCurrentTask
            | InputDispositionAction::ReplanCurrentGraph
            | InputDispositionAction::AmendCurrentTurn
            | InputDispositionAction::ProgressOrControl
            | InputDispositionAction::Clarify => {
                if self.graph_plan.is_some() {
                    return Err("action must not include graph_plan".to_string());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInputDispositionInput {
    pub slot: u16,
    pub input_id: String,
    pub request_id: String,
    pub sequence: usize,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInputDispositionScope {
    pub session_id: String,
    pub turn_id: String,
    pub session_generation: u64,
    pub execution_id: String,
    pub expected_graph_revision: u64,
    pub task_id: Option<String>,
    pub mission_id: Option<String>,
    pub inputs: Vec<RuntimeInputDispositionInput>,
}

impl RuntimeInputDispositionScope {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("session_id", self.session_id.as_str()),
            ("turn_id", self.turn_id.as_str()),
            ("execution_id", self.execution_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("Runtime input disposition scope requires {name}"));
            }
        }
        if self.expected_graph_revision == 0 {
            return Err(
                "Runtime input disposition scope requires a committed graph revision".to_string(),
            );
        }
        if self.inputs.is_empty() {
            return Err("Runtime input disposition scope has no inputs".to_string());
        }
        let mut input_ids = std::collections::BTreeSet::new();
        let mut request_ids = std::collections::BTreeSet::new();
        for (expected_slot, input) in self.inputs.iter().enumerate() {
            if usize::from(input.slot) != expected_slot {
                return Err(
                    "Runtime input disposition slots must be contiguous and ordered".to_string(),
                );
            }
            if input.input_id.trim().is_empty() || input.request_id.trim().is_empty() {
                return Err(
                    "Runtime input disposition input identities must not be empty".to_string(),
                );
            }
            if !input_ids.insert(input.input_id.as_str())
                || !request_ids.insert(input.request_id.as_str())
            {
                return Err("Runtime input disposition input identities must be unique".to_string());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputApplicationState {
    Prepared,
    Materializing,
    Applied,
    Failed,
}

impl InputApplicationState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Applied | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInputApplicationReceipt {
    pub disposition_id: String,
    pub leader_input_id: String,
    pub input_ids: Vec<String>,
    pub action: InputDispositionAction,
    pub relation: InputWorkRelation,
    pub state: InputApplicationState,
    pub objective: String,
    pub required: bool,
    /// Materialization attempt count. Runtime permits one initial attempt and
    /// one recovery attempt; further retries require an explicit new input.
    pub attempts: u16,
    pub summary: String,
    #[serde(default)]
    pub task_ids: Vec<String>,
    #[serde(default)]
    pub team_ids: Vec<String>,
    #[serde(default)]
    pub agent_ids: Vec<String>,
    #[serde(default)]
    pub execution_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_session_id: Option<String>,
    #[serde(default)]
    pub target_session_created: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

impl SessionInputApplicationReceipt {
    pub fn validate_shape(&self) -> Result<(), String> {
        if self.disposition_id.trim().is_empty()
            || self.leader_input_id.trim().is_empty()
            || self.input_ids.is_empty()
            || self.objective.trim().is_empty()
            || self.summary.trim().is_empty()
        {
            return Err(
                "application receipt requires disposition, leader, inputs, objective and summary"
                    .to_string(),
            );
        }
        if self.attempts == 0 || self.attempts > 2 {
            return Err("application receipt attempts must be within 1..=2".to_string());
        }
        let mut unique = std::collections::BTreeSet::new();
        if self
            .input_ids
            .iter()
            .any(|input_id| input_id.trim().is_empty() || !unique.insert(input_id))
        {
            return Err("application receipt input_ids must be unique and non-empty".to_string());
        }
        if !unique.contains(&self.leader_input_id) {
            return Err("application receipt leader must belong to input_ids".to_string());
        }
        if self.summary.len() > 2_048
            || self.error.as_ref().is_some_and(|value| value.len() > 4_096)
        {
            return Err(
                "application receipt summary or error exceeds the compact receipt limit"
                    .to_string(),
            );
        }
        if self.state == InputApplicationState::Applied {
            match self.action {
                InputDispositionAction::AddRequiredTask
                | InputDispositionAction::AddBackgroundTask => {
                    if self.task_ids.is_empty() || self.execution_ids.is_empty() {
                        return Err(
                            "applied Task disposition requires Task and execution refs".to_string()
                        );
                    }
                }
                InputDispositionAction::AddTeamLane => {
                    if self.team_ids.is_empty() || self.execution_ids.is_empty() {
                        return Err(
                            "applied Team disposition requires Team and execution refs".to_string()
                        );
                    }
                }
                InputDispositionAction::AddTaskWithTeam => {
                    if self.task_ids.is_empty()
                        || self.team_ids.is_empty()
                        || self.execution_ids.is_empty()
                    {
                        return Err(
                            "applied Task+Team disposition requires Task, Team and execution refs"
                                .to_string(),
                        );
                    }
                }
                InputDispositionAction::DispatchSession => {
                    if self.target_session_id.is_none() || self.execution_ids.is_empty() {
                        return Err(
                            "applied Session dispatch requires target Session and execution refs"
                                .to_string(),
                        );
                    }
                }
                InputDispositionAction::ReplaceCurrentTask => {
                    if self.task_ids.is_empty() || self.execution_ids.is_empty() {
                        return Err(
                            "applied replacement requires cancelled Task and execution refs"
                                .to_string(),
                        );
                    }
                }
                InputDispositionAction::AmendCurrentTurn
                | InputDispositionAction::ReplanCurrentGraph
                | InputDispositionAction::ProgressOrControl
                | InputDispositionAction::Clarify => {}
            }
        } else if self.target_session_created {
            return Err("only an applied receipt can report a created Session".to_string());
        }
        if self.action != InputDispositionAction::DispatchSession
            && (self.target_session_id.is_some() || self.target_session_created)
        {
            return Err("only dispatch_session can carry a target Session".to_string());
        }
        Ok(())
    }

    #[must_use]
    pub fn can_follow(&self, previous: Option<&Self>) -> bool {
        let Some(previous) = previous else {
            return self.state == InputApplicationState::Prepared && self.revision == 0;
        };
        if self.disposition_id != previous.disposition_id
            || self.input_ids != previous.input_ids
            || self.leader_input_id != previous.leader_input_id
            || self.action != previous.action
            || self.relation != previous.relation
            || self.objective != previous.objective
            || self.required != previous.required
            || self.revision != previous.revision.saturating_add(1)
        {
            return false;
        }
        let attempts_valid = self.attempts == previous.attempts
            || (previous.state == InputApplicationState::Failed
                && self.state == InputApplicationState::Prepared
                && self.attempts == previous.attempts.saturating_add(1));
        if !attempts_valid {
            return false;
        }
        matches!(
            (previous.state, self.state),
            (
                InputApplicationState::Prepared,
                InputApplicationState::Materializing
            ) | (
                InputApplicationState::Prepared,
                InputApplicationState::Failed
            ) | (
                InputApplicationState::Materializing,
                InputApplicationState::Applied
            ) | (
                InputApplicationState::Materializing,
                InputApplicationState::Failed
            ) | (
                InputApplicationState::Failed,
                InputApplicationState::Prepared
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision(slots: &[u16]) -> ModelInputDispositionDecision {
        ModelInputDispositionDecision {
            input_slots: slots.to_vec(),
            action: InputDispositionAction::AddRequiredTask,
            relation: InputWorkRelation::NewTask,
            objective: "implement the requested work".to_string(),
            required: true,
            confidence_basis_points: 9_000,
            reason: "the input is independent work".to_string(),
            graph_plan: None,
            session_target: None,
        }
    }

    #[test]
    fn batch_requires_exactly_once_slot_coverage() {
        let valid = ModelInputDispositionBatch {
            decisions: vec![decision(&[0, 1]), decision(&[2])],
        };
        assert!(valid.validate_slots(3).is_ok());

        let duplicate = ModelInputDispositionBatch {
            decisions: vec![decision(&[0, 1]), decision(&[1, 2])],
        };
        assert!(duplicate
            .validate_slots(3)
            .unwrap_err()
            .contains("more than once"));

        let missing = ModelInputDispositionBatch {
            decisions: vec![decision(&[0])],
        };
        assert!(missing
            .validate_slots(2)
            .unwrap_err()
            .contains("did not cover"));
    }

    #[test]
    fn replacement_owns_the_complete_checkpoint() {
        let mut replacement = decision(&[0]);
        replacement.action = InputDispositionAction::ReplaceCurrentTask;
        let batch = ModelInputDispositionBatch {
            decisions: vec![replacement, decision(&[1])],
        };
        assert!(batch
            .validate_slots(2)
            .unwrap_err()
            .contains("complete checkpoint"));
    }

    #[test]
    fn semantic_bounds_are_enforced_at_runtime_not_only_in_schema() {
        let mut invalid_confidence = decision(&[0]);
        invalid_confidence.confidence_basis_points = 10_001;
        assert!(ModelInputDispositionBatch {
            decisions: vec![invalid_confidence],
        }
        .validate_slots(1)
        .unwrap_err()
        .contains("confidence_basis_points"));

        let mut optional_required_work = decision(&[0]);
        optional_required_work.required = false;
        assert!(ModelInputDispositionBatch {
            decisions: vec![optional_required_work],
        }
        .validate_slots(1)
        .unwrap_err()
        .contains("required work"));

        let mut blocking_background = decision(&[0]);
        blocking_background.action = InputDispositionAction::AddBackgroundTask;
        blocking_background.relation = InputWorkRelation::Background;
        assert!(ModelInputDispositionBatch {
            decisions: vec![blocking_background],
        }
        .validate_slots(1)
        .unwrap_err()
        .contains("background work"));
    }

    #[test]
    fn replan_is_compiled_by_the_fresh_model_step_only() {
        let mut replan = decision(&[0]);
        replan.action = InputDispositionAction::ReplanCurrentGraph;
        replan.relation = InputWorkRelation::Replan;
        replan.graph_plan = Some(ModelInputDispositionGraphPlan {
            nodes: Vec::new(),
            completion: ExecutionCompletionContract::default(),
            reason: "stale inline plan".to_string(),
        });
        assert!(ModelInputDispositionBatch {
            decisions: vec![replan],
        }
        .validate_slots(1)
        .unwrap_err()
        .contains("must not include graph_plan"));
    }

    #[test]
    fn disposition_graph_plan_cannot_carry_physical_mutation_identity() {
        let schema = serde_json::to_string(&schemars::schema_for!(ModelInputDispositionGraphPlan))
            .expect("serialize disposition graph schema");
        for forbidden in [
            "mutation_id",
            "target_execution_id",
            "expected_revision",
            "input_refs",
            "target_session_id",
        ] {
            assert!(!schema.contains(&format!("\"{forbidden}\"")));
        }
    }

    #[test]
    fn dispatch_session_uses_semantic_target_modes_only() {
        let mut dispatch = decision(&[0]);
        dispatch.action = InputDispositionAction::DispatchSession;
        dispatch.relation = InputWorkRelation::CrossSession;
        dispatch.graph_plan = Some(ModelInputDispositionGraphPlan {
            nodes: vec![ModelInputDispositionGraphNode {
                node_id: "handoff".to_string(),
                recipe: CapabilityRecipeId::SessionDispatch,
                objective: "continue in the authorized Session".to_string(),
                depends_on: Vec::new(),
                multiplicity: 1,
                focuses: Vec::new(),
                template: None,
                output_artifacts: vec!["handoff_result".to_string()],
                evidence_contract: vec!["target receipt".to_string()],
                required_evidence_refs: Vec::new(),
                required: true,
                dependency: ExecutionDependencyPolicy::All,
                cancellation_group: None,
            }],
            completion: ExecutionCompletionContract::default(),
            reason: "the work belongs to another Session".to_string(),
        });
        dispatch.session_target = Some(ModelInputDispositionSessionTarget {
            mode: InputDispositionSessionTargetMode::ExistingAuthorized,
            target_ref: Some("@session:session-b".to_string()),
        });
        assert!(ModelInputDispositionBatch {
            decisions: vec![dispatch.clone()],
        }
        .validate_slots(1)
        .is_ok());

        dispatch.session_target = Some(ModelInputDispositionSessionTarget {
            mode: InputDispositionSessionTargetMode::CreateIsolated,
            target_ref: None,
        });
        assert!(ModelInputDispositionBatch {
            decisions: vec![dispatch.clone()],
        }
        .validate_slots(1)
        .is_ok());

        dispatch.session_target = None;
        let error = ModelInputDispositionBatch {
            decisions: vec![dispatch],
        }
        .validate_slots(1)
        .unwrap_err();
        assert!(error.contains("dispatch_session requires"));
    }

    #[test]
    fn clarify_is_a_structural_fresh_step_barrier() {
        assert!(InputDispositionAction::Clarify.is_structural());
    }

    #[test]
    fn graph_recipes_cannot_bypass_the_typed_action() {
        let mut plain_task = decision(&[0]);
        plain_task.graph_plan = Some(ModelInputDispositionGraphPlan {
            nodes: vec![ModelInputDispositionGraphNode {
                node_id: "hidden-team".to_string(),
                recipe: CapabilityRecipeId::Team,
                objective: "silently create a Team".to_string(),
                depends_on: Vec::new(),
                multiplicity: 1,
                focuses: Vec::new(),
                template: None,
                output_artifacts: Vec::new(),
                evidence_contract: Vec::new(),
                required_evidence_refs: Vec::new(),
                required: true,
                dependency: ExecutionDependencyPolicy::All,
                cancellation_group: None,
            }],
            completion: ExecutionCompletionContract::default(),
            reason: "invalid hidden Team".to_string(),
        });
        assert!(ModelInputDispositionBatch {
            decisions: vec![plain_task],
        }
        .validate_slots(1)
        .unwrap_err()
        .contains("typed team action"));
    }

    #[test]
    fn runtime_scope_requires_ordered_unique_slots() {
        let mut scope = RuntimeInputDispositionScope {
            session_id: "session-a".to_string(),
            turn_id: "turn-a".to_string(),
            session_generation: 1,
            execution_id: "execution-a".to_string(),
            expected_graph_revision: 1,
            task_id: Some("task-a".to_string()),
            mission_id: None,
            inputs: vec![RuntimeInputDispositionInput {
                slot: 0,
                input_id: "input-a".to_string(),
                request_id: "request-a".to_string(),
                sequence: 1,
                revision: 2,
            }],
        };
        assert!(scope.validate().is_ok());
        scope.inputs[0].slot = 1;
        assert!(scope.validate().is_err());
    }

    fn receipt(state: InputApplicationState, revision: u64) -> SessionInputApplicationReceipt {
        SessionInputApplicationReceipt {
            disposition_id: "disposition-a".to_string(),
            leader_input_id: "input-a".to_string(),
            input_ids: vec!["input-a".to_string()],
            action: InputDispositionAction::AddRequiredTask,
            relation: InputWorkRelation::NewTask,
            state,
            objective: "perform required work".to_string(),
            required: true,
            attempts: 1,
            summary: "durable receipt".to_string(),
            task_ids: Vec::new(),
            team_ids: Vec::new(),
            agent_ids: Vec::new(),
            execution_ids: Vec::new(),
            target_session_id: None,
            target_session_created: false,
            error: None,
            revision,
            updated_at_ms: revision,
        }
    }

    #[test]
    fn receipt_transition_freezes_semantic_identity_and_bounds_recovery() {
        let prepared = receipt(InputApplicationState::Prepared, 0);
        assert!(prepared.can_follow(None));

        let materializing = receipt(InputApplicationState::Materializing, 1);
        assert!(materializing.can_follow(Some(&prepared)));

        let mut changed = materializing.clone();
        changed.objective = "silently changed work".to_string();
        assert!(!changed.can_follow(Some(&prepared)));

        let mut failed = receipt(InputApplicationState::Failed, 2);
        failed.error = Some("transient materialization failure".to_string());
        assert!(failed.can_follow(Some(&materializing)));

        let mut retry = receipt(InputApplicationState::Prepared, 3);
        retry.attempts = 2;
        assert!(retry.can_follow(Some(&failed)));

        let mut third_attempt = receipt(InputApplicationState::Prepared, 4);
        third_attempt.attempts = 3;
        assert!(third_attempt.validate_shape().is_err());
        assert!(!third_attempt.can_follow(Some(&retry)));
    }
}
