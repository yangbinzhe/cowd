//! Goal, observation, and intervention contracts for governed execution.
//!
//! These types are pure data contracts. Runtime owns persistence, policy, and
//! graph application; Gateway and surfaces only consume their projections.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::core::MeasureProvenance;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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
    /// A concise, human-readable description.  It is deliberately not a
    /// storage selector: long or evolving source material remains in the
    /// durable content store and is named below.
    pub statement: String,
    /// Canonical content that supplies the criterion's semantic statement.
    /// Runtime validates visibility/readability before accepting this ref;
    /// consumers must never mistake a selector string for the statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_ref: Option<String>,
    /// Original user input, attachments, or approved refinements that justify
    /// this criterion.  This keeps scope changes auditable without copying
    /// unbounded text into the Goal journal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<String>,
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
    Blocked,
    Failed,
    WaitingExternalDecision,
    Cancelled,
}

/// Whether a Goal is the user-visible Objective or a Runtime-local helper.
/// Local Goals may inform their parent but must never be presented as a
/// substitute terminal result for the user request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalScope {
    #[default]
    UserObjective,
    Internal,
}

/// Immutable lineage joining the durable Goal, conversational turn, physical
/// root execution and Agentic Program. These identities are Runtime-owned;
/// model actions can only be authorized against an existing binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalExecutionBinding {
    pub objective_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub root_execution_id: String,
    pub agentic_program_id: String,
}

/// A temporary, non-terminal reason why an Objective cannot presently make
/// progress. It is intentionally distinct from a final Blocked outcome so a
/// durable recovery signal can resume the same Objective without recreating
/// a team or losing already proved work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitDescriptor {
    pub reason_code: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_refs: Vec<String>,
    pub generation: u64,
}

/// A user-sourced participation requirement. Runtime checks actual durable
/// contribution records, not a roster size or a copied number in an Agent
/// action binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipationRequirement {
    pub minimum_team_count: u8,
    pub source_ref: String,
}

/// Durable semantic review metadata. Runtime records identity and the exact
/// Objective specification/evidence versions; the reviewer supplies only a
/// bounded decision and references, never a self-declared independence flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveReviewRecord {
    pub review_id: String,
    pub criterion_ref: String,
    pub spec_revision: u64,
    pub input_manifest_digest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub result_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub reviewer_actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub producer_refs: Vec<String>,
    pub decision: String,
    pub reason_ref: String,
}

/// Durable state of one business obligation.  It is intentionally distinct
/// from an execution-graph node status: a graph may be terminal while the
/// Objective still has an unresolved obligation or is being replanned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveObligationState {
    #[default]
    Open,
    Satisfied,
    Blocked,
    Failed,
    Waived,
}

impl ObjectiveObligationState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Satisfied | Self::Blocked | Self::Failed | Self::Waived
        )
    }
}

/// The executable producer and evidence route required for an Objective
/// obligation.  Runtime resolves these references during admission; model
/// prose and role names are never capability proof.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveProducerContract {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_definition_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_ref: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveEvidenceRequirement {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_artifact_kinds: Vec<String>,
    #[serde(default)]
    pub independent_verifier_required: bool,
    #[serde(default)]
    pub reread_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveObligation {
    pub obligation_id: String,
    #[serde(default = "required_by_default")]
    pub required: bool,
    pub success_predicate: String,
    #[serde(default)]
    pub producer: ObjectiveProducerContract,
    #[serde(default)]
    pub evidence_requirement: ObjectiveEvidenceRequirement,
    #[serde(default)]
    pub state: ObjectiveObligationState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reread_receipts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verifier_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_code: Option<String>,
}

fn required_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveTerminalKind {
    Satisfied,
    PartiallySatisfied,
    Blocked,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveDiagnostic {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obligation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveTerminal {
    pub kind: ObjectiveTerminalKind,
    pub terminal_fence: String,
    pub authority_revision: u64,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ObjectiveDiagnostic>,
    pub committed_at_ms: u64,
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
    #[serde(default)]
    pub scope: GoalScope,
    /// The immutable criterion that represents the full original user input.
    /// Later model refinements may add criteria but cannot retire this anchor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_intent_criterion_id: Option<String>,
    /// Durable Session input/attachment manifest reference. It deliberately
    /// stores a selector rather than copying user text into every event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_intent_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_binding: Option<GoalExecutionBinding>,
    /// Changes only when the business Objective changes; `revision` remains
    /// the generic event/CAS revision and may advance for progress updates.
    #[serde(default)]
    pub spec_revision: u64,
    #[serde(default)]
    pub spec_digest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub review_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reviews: Vec<ObjectiveReviewRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<WaitDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participation_requirement: Option<ParticipationRequirement>,
    /// The only business obligations that can promote the Objective to a
    /// terminal result. Team/graph local terminal facts feed these entries but
    /// never substitute for them.
    #[serde(default)]
    pub obligations: Vec<ObjectiveObligation>,
    /// Runtime-owned bounded semantic recovery cursor.  This is deliberately
    /// additive so older goal streams remain readable and default to no
    /// recovery attempts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<ObjectiveRecoveryState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<ObjectiveTerminal>,
    pub completion: GoalCompletion,
    pub revision: u64,
    pub user_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveRecoveryState {
    pub source_revision: u64,
    pub attempts: u32,
    pub budget: u32,
    pub status: ObjectiveRecoveryStatus,
    pub idempotency_key: String,
    /// Durable graph mutation identity. It lets a restarted executor
    /// distinguish "graph applied, Goal marker pending" from a fresh patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<String>,
    /// The exact required Team obligation that produced the recovery request.
    /// Keeping this identity durable prevents a later observer from selecting
    /// a different retryable Team when several terminal failures coexist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_obligation_id: Option<String>,
    #[serde(default)]
    pub last_diagnostic: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveRecoveryStatus {
    Pending,
    ApplyingGraph,
    GraphApplied,
    Committed,
    Exhausted,
    Failed,
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
                statement_ref: None,
                source_refs: Vec::new(),
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
            scope: GoalScope::Internal,
            user_intent_criterion_id: Some("criterion-1".into()),
            source_intent_ref: Some("session_message:message-1".into()),
            execution_binding: None,
            spec_revision: 1,
            spec_digest: "test-spec".into(),
            review_refs: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        };
        assert_eq!(
            serde_json::from_str::<GoalContract>(&serde_json::to_string(&contract).unwrap())
                .unwrap(),
            contract
        );
    }
}
