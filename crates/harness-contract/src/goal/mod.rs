//! Goal, observation, and intervention contracts for governed execution.
//!
//! These types are pure data contracts. Runtime owns persistence, policy, and
//! graph application; Gateway and surfaces only consume their projections.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::core::MeasureProvenance;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceStatus {
    Open,
    Satisfied,
    Blocked,
    Waived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCriterion {
    pub id: String,
    pub statement: String,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    pub status: AcceptanceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiver: Option<CriterionWaiver>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriterionWaiver {
    pub actor: String,
    pub reason: String,
    pub permission_receipt: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalCompletion {
    Open,
    Satisfied,
    Partial,
    WaitingExternalDecision,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalContract {
    pub id: String,
    pub session_id: String,
    pub objective: String,
    pub criteria: Vec<AcceptanceCriterion>,
    #[serde(default)]
    pub constraints: Vec<String>,
    pub phase: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub unresolved: Vec<String>,
    #[serde(default)]
    pub blockers: Vec<String>,
    pub completion: GoalCompletion,
    pub revision: u64,
    pub user_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRevision {
    pub goal_id: String,
    pub previous_revision: u64,
    pub revision: u64,
    pub reason: String,
    pub user_sequence: u64,
    pub changed_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeObservationKind {
    ToolProgress,
    GraphProgress,
    ContextPressure,
    ProviderProgress,
    UserInput,
    StrategyHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeObservationIdentity {
    pub workspace_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub graph_id: String,
    pub goal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationFreshness {
    pub observed_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_until_ms: Option<u64>,
    pub policy_revision: String,
}

impl ObservationFreshness {
    #[must_use]
    pub fn is_current_at(&self, now_ms: u64, policy_revision: &str) -> bool {
        self.policy_revision == policy_revision
            && self
                .valid_until_ms
                .is_none_or(|valid_until_ms| now_ms <= valid_until_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriterionDelta {
    pub criterion_id: String,
    pub previous: AcceptanceStatus,
    pub current: AcceptanceStatus,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceDelta {
    #[serde(default)]
    pub added: Vec<String>,
    #[serde(default)]
    pub invalidated: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectTerminalClass {
    Completed,
    Failed,
    Cancelled,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectDelta {
    pub effect_id: String,
    pub terminal_class: EffectTerminalClass,
    pub idempotency_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionDeltaKind {
    Opened,
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictDelta {
    pub conflict_id: String,
    pub change: ResolutionDeltaKind,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnknownDelta {
    pub unknown_id: String,
    pub change: ResolutionDeltaKind,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostDelta {
    pub model_steps: u64,
    pub tool_calls: u64,
    pub duration_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InformationGain {
    #[serde(default)]
    pub distinguishing_evidence_refs: Vec<String>,
    #[serde(default)]
    pub resolved_unknown_refs: Vec<String>,
    pub provenance: MeasureProvenance,
}

impl InformationGain {
    #[must_use]
    pub fn is_positive(&self) -> bool {
        self.provenance.supports_automatic_optimization()
            && (!self.distinguishing_evidence_refs.is_empty()
                || !self.resolved_unknown_refs.is_empty())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextDelta {
    pub context_window_tokens: u64,
    pub input_tokens: u64,
    pub pressure_basis_points: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParallelismDelta {
    pub ready_work: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationResultClass {
    Succeeded,
    Partial,
    Failed,
    Informational,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationFailureClass {
    Provider,
    Tool,
    Approval,
    Verification,
    Policy,
    Cancelled,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeObservation {
    pub identity: RuntimeObservationIdentity,
    pub kind: RuntimeObservationKind,
    pub source: String,
    pub source_revision: u64,
    pub freshness: ObservationFreshness,
    pub summary: String,
    /// Stable identity for an observation pattern. Runtime uses it to
    /// distinguish a repeated failed action from unrelated low-progress work.
    pub fingerprint: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    /// Typed execution facts emitted only after canonical ToolHost success.
    /// Display references are never parsed back to reconstruct these facts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_evidence: Vec<crate::context::ObservedEvidence>,
    #[serde(default)]
    pub criterion_deltas: Vec<CriterionDelta>,
    #[serde(default)]
    pub evidence_delta: EvidenceDelta,
    #[serde(default)]
    pub effect_deltas: Vec<EffectDelta>,
    #[serde(default)]
    pub conflict_deltas: Vec<ConflictDelta>,
    #[serde(default)]
    pub unknown_deltas: Vec<UnknownDelta>,
    #[serde(default)]
    pub cost_delta: CostDelta,
    #[serde(default)]
    pub information_gain: InformationGain,
    #[serde(default)]
    pub context_delta: ContextDelta,
    #[serde(default)]
    pub parallelism_delta: ParallelismDelta,
    pub result_class: ObservationResultClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<ObservationFailureClass>,
}

impl RuntimeObservation {
    #[must_use]
    pub fn goal_id(&self) -> &str {
        &self.identity.goal_id
    }

    #[must_use]
    pub fn idempotency_fingerprint(&self) -> String {
        format!(
            "{}:{}:{}",
            self.source, self.source_revision, self.fingerprint
        )
    }

    #[must_use]
    pub fn has_verified_gain(&self) -> bool {
        self.information_gain.is_positive()
    }

    #[must_use]
    pub fn failed(&self) -> bool {
        self.result_class == ObservationResultClass::Failed || self.failure_class.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProgressSnapshot {
    pub goal_id: String,
    pub goal_revision: u64,
    pub observation_count: u64,
    pub criteria: BTreeMap<String, AcceptanceStatus>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub invalidated_evidence_refs: Vec<String>,
    #[serde(default)]
    pub effects: BTreeMap<String, EffectTerminalClass>,
    #[serde(default)]
    pub open_conflicts: Vec<String>,
    #[serde(default)]
    pub open_unknowns: Vec<String>,
    #[serde(default)]
    pub cumulative_cost: CostDelta,
    pub last_observed_at_ms: u64,
    #[serde(default)]
    pub applied_observation_keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeInterventionKind {
    Continue,
    Parallelize,
    Retrieve,
    Replan,
    Switch,
    Synthesize,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIntervention {
    pub goal_id: String,
    pub kind: RuntimeInterventionKind,
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub expected_graph_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInterventionTrace {
    pub identity: RuntimeObservationIdentity,
    pub trigger_observation_keys: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn goal_contract_roundtrips_revisioned_acceptance() {
        let contract = GoalContract {
            id: "goal-1".into(),
            session_id: "session-1".into(),
            objective: "finish the governed task".into(),
            criteria: vec![AcceptanceCriterion {
                id: "criterion-1".into(),
                statement: "produce checked result".into(),
                required_evidence: vec!["evidence:1".into()],
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: vec!["read_only".into()],
            phase: "execution".into(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        assert_eq!(
            serde_json::from_str::<GoalContract>(&serde_json::to_string(&contract).unwrap())
                .unwrap(),
            contract
        );
    }
}
