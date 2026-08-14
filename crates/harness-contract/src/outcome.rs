//! Canonical execution outcome contracts.
//!
//! Outcomes are durable observations about an execution. They are not policy
//! decisions and cannot promote a provider, strategy, Agent, Team, Tool, or
//! Skill on their own.

use serde::{Deserialize, Serialize};

use crate::{
    reality::{EvidenceCompleteness, EvidenceRef},
    strategy::{ExecutionCandidateKind, StrategyWorkloadFingerprint},
    turn::CancellationReceipt,
};

pub const OUTCOME_SCHEMA_REVISION: u32 = 1;

/// Runtime-owned pipeline state.  This is deliberately independent from
/// [`DeliveryStatus`]: a pipeline can complete while the user's objective is
/// only partially satisfied.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStatus {
    #[default]
    Waiting,
    Completed,
    Failed,
    Cancelled,
}

/// Runtime-derived business delivery state.  Model-generated wording cannot
/// promote this value.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Satisfied,
    Partial,
    Denied,
    #[default]
    Unavailable,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryBranchStatus {
    Completed,
    Failed,
    Cancelled,
    #[default]
    Blocked,
}

/// One terminal branch consumed by the Runtime finally reducer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DeliveryBranchTerminal {
    pub branch_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub status: DeliveryBranchStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_ref: Option<String>,
}

/// Opaque, verified durable reference.  Raw payloads stay behind the evidence
/// store and are never copied into a delivery envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct VerifiedDeliveryReference {
    pub reference_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_execution_id: Option<String>,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum VerifiedEffectStatus {
    Applied,
    NotApplied,
    #[default]
    Uncertain,
}

/// Runtime-attested effect state.  In particular, presentation success must
/// never turn `not_applied` into `applied`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct VerifiedDeliveryEffect {
    pub effect_id: String,
    pub kind: String,
    #[serde(default)]
    pub status: VerifiedEffectStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_execution_id: Option<String>,
}

/// Deterministic obligation coverage computed from RequiredAcceptance and
/// ObservedAcceptance by Runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DeliveryCoverage {
    #[serde(default)]
    pub required_obligation_ids: Vec<String>,
    #[serde(default)]
    pub satisfied_obligation_ids: Vec<String>,
    #[serde(default)]
    pub coverage_basis_points: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DeliveryUnresolved {
    pub unresolved_id: String,
    pub kind: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obligation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DeliveryConflict {
    pub conflict_id: String,
    pub summary: String,
    #[serde(default)]
    pub source_execution_ids: Vec<String>,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UserAnswerFormat {
    HumanText,
    #[default]
    Markdown,
    StrictJson,
    Other,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UserAnswerDetail {
    Concise,
    #[default]
    Balanced,
    Detailed,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UserAnswerEvidencePreference {
    None,
    #[default]
    WhenUseful,
    Required,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UserAnswerCitationPreference {
    None,
    #[default]
    WhenAvailable,
    Required,
}

/// Presentation preferences negotiated from the user request and system
/// capability.  It constrains wording only; it does not alter delivery facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct UserAnswerContract {
    #[serde(default = "default_answer_language")]
    pub language: String,
    #[serde(default)]
    pub format: UserAnswerFormat,
    #[serde(default)]
    pub detail: UserAnswerDetail,
    #[serde(default)]
    pub conclusion_only: bool,
    #[serde(default)]
    pub evidence_preference: UserAnswerEvidencePreference,
    #[serde(default)]
    pub citation_preference: UserAnswerCitationPreference,
    #[serde(default)]
    pub structural_constraints: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other_format: Option<String>,
}

impl Default for UserAnswerContract {
    fn default() -> Self {
        Self {
            language: default_answer_language(),
            format: UserAnswerFormat::default(),
            detail: UserAnswerDetail::default(),
            conclusion_only: false,
            evidence_preference: UserAnswerEvidencePreference::default(),
            citation_preference: UserAnswerCitationPreference::default(),
            structural_constraints: Vec::new(),
            other_format: None,
        }
    }
}

fn default_answer_language() -> String {
    "auto".to_string()
}

/// The only durable fact packet presented to the terminal answer gate.
/// Reducers own construction; answer models receive a read-only serialized
/// view and return an [`AnswerCandidate`] instead of a modified envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DeliveryEnvelope {
    pub envelope_id: String,
    pub revision: u64,
    pub objective_id: String,
    #[serde(default)]
    pub pipeline_status: PipelineStatus,
    #[serde(default)]
    pub delivery_status: DeliveryStatus,
    #[serde(default)]
    pub branch_terminals: Vec<DeliveryBranchTerminal>,
    #[serde(default)]
    pub verified_receipts: Vec<VerifiedDeliveryReference>,
    #[serde(default)]
    pub verified_artifacts: Vec<VerifiedDeliveryReference>,
    #[serde(default)]
    pub verified_effects: Vec<VerifiedDeliveryEffect>,
    #[serde(default)]
    pub coverage: DeliveryCoverage,
    #[serde(default)]
    pub unresolved: Vec<DeliveryUnresolved>,
    #[serde(default)]
    pub conflicts: Vec<DeliveryConflict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<CancellationReceipt>,
    #[serde(default)]
    pub user_answer_contract: UserAnswerContract,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AnswerOrigin {
    ModelDirect,
    TerminalDelegate,
    TeamSynthesizer,
    TerminalNarrator,
    FallbackModel,
    ProgrammaticFallback,
    CancellationReceipt,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AnswerObjectiveScope {
    #[default]
    Root,
    Subtask,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AnswerContentKind {
    #[default]
    UserText,
    StrictJson,
    InternalPacket,
    ToolProtocol,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AnswerValidationStatus {
    #[default]
    Pending,
    Valid,
    Invalid,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AnswerValidation {
    #[serde(default)]
    pub status: AnswerValidationStatus,
    #[serde(default)]
    pub findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_revision: Option<u64>,
}

/// Model- or reducer-produced wording candidate.  It intentionally carries no
/// effect, evidence, coverage, conflict, or cancellation fact fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AnswerCandidate {
    pub candidate_id: String,
    pub origin: AnswerOrigin,
    #[serde(default)]
    pub objective_scope: AnswerObjectiveScope,
    pub source_execution_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed_envelope_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub completed_at_ms: u64,
    pub text: String,
    #[serde(default)]
    pub content_kind: AnswerContentKind,
    #[serde(default)]
    pub terminal_delegate: bool,
    #[serde(default)]
    pub validation: AnswerValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalPresentationState {
    Started,
    Streaming,
    Validating,
    Committed,
    Aborted,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PresentationModelAttempt {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalPresentation {
    pub presentation_id: String,
    pub attempt_id: String,
    pub envelope_id: String,
    pub envelope_revision: u64,
    pub state: TerminalPresentationState,
    pub answer_origin: AnswerOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrator_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrator_provider: Option<String>,
    #[serde(default)]
    pub models_attempted: Vec<PresentationModelAttempt>,
    #[serde(default)]
    pub validation: AnswerValidation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    pub generated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionOutcome {
    pub identity: OutcomeIdentity,
    pub runtime: RuntimeIdentity,
    pub provider: Option<ProviderIdentity>,
    pub strategy: StrategyIdentity,
    pub timing: OutcomeTiming,
    pub usage: OutcomeUsage,
    pub terminal: OutcomeTerminalClass,
    pub quality: OutcomeQuality,
    pub observation: OutcomeObservation,
    /// Non-sensitive facts used by the workload-scoped Strategy projection.
    /// No prompt, path, tool payload, transcript, or business content belongs
    /// in this contract.
    #[serde(default)]
    pub strategy_feedback: OutcomeStrategyFeedback,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
    pub evidence_completeness: EvidenceCompleteness,
    pub schema_revision: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeStrategyFeedback {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload: Option<StrategyWorkloadFingerprint>,
    pub verification_blocked: bool,
    pub context_pressure: bool,
    pub coordination_cost_ms: u64,
    /// Separates ordinary production observations from isolated evaluation
    /// corpora and any future replay/simulation environments.
    pub evaluation_environment: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeIdentity {
    pub execution_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub terminal_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_sample_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_graph_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub workspace_key: String,
    pub runtime_revision: String,
    pub config_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_revision: Option<u64>,
    pub provider_name: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Request-local Provider facts used by Runtime. Each value contains both
    /// state and authority source, so diagnostics do not confuse configured,
    /// probed, and bundled knowledge.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub capabilities: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyIdentity {
    pub decision_id: String,
    pub policy_revision: String,
    pub decision_source: String,
    pub selected_candidate: ExecutionCandidateKind,
    pub selected_pattern: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeTiming {
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub evaluation_tokens: Option<u64>,
    pub tool_calls: u64,
    pub duplicate_tool_calls: u64,
    pub retries: u64,
    pub max_observed_concurrency: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "class", content = "reason")]
pub enum OutcomeTerminalClass {
    Succeeded(String),
    Failed(String),
    Cancelled(String),
    Blocked(String),
    PartialFailure(String),
}

impl OutcomeTerminalClass {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    #[must_use]
    pub const fn class_name(&self) -> &'static str {
        match self {
            Self::Succeeded(_) => "succeeded",
            Self::Failed(_) => "failed",
            Self::Cancelled(_) => "cancelled",
            Self::Blocked(_) => "blocked",
            Self::PartialFailure(_) => "partial_failure",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OutcomeQuality {
    Unknown,
    Estimate {
        value_bp: u16,
        basis: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        calibration_ref: Option<String>,
    },
}

impl OutcomeQuality {
    #[must_use]
    pub fn estimate(
        value_bp: u16,
        basis: impl Into<String>,
        calibration_ref: Option<String>,
    ) -> Self {
        Self::Estimate {
            value_bp: value_bp.min(10_000),
            basis: basis.into(),
            calibration_ref,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeObservation {
    pub source: String,
    pub observed_at_ms: u64,
    pub freshness_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OutcomeSegmentKey {
    #[serde(default)]
    pub workspace_key: String,
    #[serde(default)]
    pub workload_fingerprint_sha256: String,
    #[serde(default)]
    pub evaluation_environment: String,
    pub provider: String,
    pub model: String,
    pub profile: String,
    pub protocol: String,
    pub config_revision: String,
    pub policy_revision: String,
    pub candidate: ExecutionCandidateKind,
}

impl OutcomeSegmentKey {
    #[must_use]
    pub fn from_outcome(outcome: &ExecutionOutcome) -> Self {
        let provider = outcome.provider.as_ref();
        Self {
            workspace_key: outcome.runtime.workspace_key.clone(),
            workload_fingerprint_sha256: outcome
                .strategy_feedback
                .workload
                .as_ref()
                .map(StrategyWorkloadFingerprint::digest)
                .unwrap_or_else(|| "unscoped".to_string()),
            evaluation_environment: if outcome
                .strategy_feedback
                .evaluation_environment
                .trim()
                .is_empty()
            {
                "unknown".to_string()
            } else {
                outcome.strategy_feedback.evaluation_environment.clone()
            },
            provider: provider
                .map(|identity| identity.provider_name.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            model: provider
                .map(|identity| identity.model.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            profile: provider
                .and_then(|identity| identity.profile.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            protocol: provider
                .and_then(|identity| identity.protocol.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            config_revision: outcome.runtime.config_revision.clone(),
            policy_revision: outcome.strategy.policy_revision.clone(),
            candidate: outcome.strategy.selected_candidate,
        }
    }
}

/// Exact lookup key for strategy feedback. Provider profile/protocol remain in
/// the general Outcome segment, while routing feedback is deliberately scoped
/// to the provider/model identity that the next request can resolve before it
/// selects a topology.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StrategyExperienceKey {
    pub workspace_key: String,
    pub workload_fingerprint_sha256: String,
    pub config_revision: String,
    pub provider: String,
    pub model: String,
    pub evaluation_environment: String,
    pub candidate: ExecutionCandidateKind,
}

impl StrategyExperienceKey {
    #[must_use]
    pub fn from_outcome(outcome: &ExecutionOutcome) -> Option<Self> {
        let workload = outcome.strategy_feedback.workload.as_ref()?;
        let provider = outcome.provider.as_ref()?;
        if provider.provider_name.trim().is_empty() || provider.model.trim().is_empty() {
            return None;
        }
        Some(Self {
            workspace_key: outcome.runtime.workspace_key.clone(),
            workload_fingerprint_sha256: workload.digest(),
            config_revision: outcome.runtime.config_revision.clone(),
            provider: provider.provider_name.clone(),
            model: provider.model.clone(),
            evaluation_environment: if outcome
                .strategy_feedback
                .evaluation_environment
                .trim()
                .is_empty()
            {
                "unknown".to_string()
            } else {
                outcome.strategy_feedback.evaluation_environment.clone()
            },
            candidate: outcome.strategy.selected_candidate,
        })
    }

    #[must_use]
    pub fn with_candidate(&self, candidate: ExecutionCandidateKind) -> Self {
        let mut key = self.clone();
        key.candidate = candidate;
        key
    }
}

#[cfg(test)]
mod delivery_contract_tests {
    use super::*;

    #[test]
    fn minimal_delivery_envelope_defaults_fail_closed() {
        let envelope: DeliveryEnvelope = serde_json::from_value(serde_json::json!({
            "envelope_id": "envelope-1",
            "revision": 1,
            "objective_id": "objective-1",
            "created_at_ms": 42
        }))
        .expect("additive delivery fields have durable defaults");

        assert_eq!(envelope.pipeline_status, PipelineStatus::Waiting);
        assert_eq!(envelope.delivery_status, DeliveryStatus::Unavailable);
        assert!(envelope.branch_terminals.is_empty());
        assert!(envelope.verified_effects.is_empty());
        assert_eq!(envelope.user_answer_contract.language, "auto");
    }

    #[test]
    fn answer_candidate_cannot_carry_runtime_delivery_facts() {
        let candidate = AnswerCandidate {
            candidate_id: "candidate-1".to_string(),
            origin: AnswerOrigin::ModelDirect,
            objective_scope: AnswerObjectiveScope::Root,
            source_execution_id: "execution-1".to_string(),
            consumed_envelope_revision: Some(3),
            model: Some("model".to_string()),
            provider: Some("provider".to_string()),
            completed_at_ms: 9,
            text: "answer".to_string(),
            content_kind: AnswerContentKind::UserText,
            terminal_delegate: true,
            validation: AnswerValidation::default(),
        };
        let encoded = serde_json::to_value(candidate).expect("candidate serializes");
        let object = encoded.as_object().expect("candidate is an object");

        for forbidden in [
            "pipeline_status",
            "delivery_status",
            "verified_receipts",
            "verified_effects",
            "coverage",
            "conflicts",
            "cancellation",
        ] {
            assert!(
                !object.contains_key(forbidden),
                "forbidden fact: {forbidden}"
            );
        }
    }

    #[test]
    fn delivery_and_presentation_contracts_have_json_schemas() {
        for schema in [
            serde_json::to_string(&schemars::schema_for!(DeliveryEnvelope))
                .expect("delivery schema serializes"),
            serde_json::to_string(&schemars::schema_for!(AnswerCandidate))
                .expect("candidate schema serializes"),
            serde_json::to_string(&schemars::schema_for!(TerminalPresentation))
                .expect("presentation schema serializes"),
            serde_json::to_string(&schemars::schema_for!(UserAnswerContract))
                .expect("answer contract schema serializes"),
        ] {
            assert!(schema.contains("properties") || schema.contains("oneOf"));
        }
    }
}
