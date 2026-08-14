//! Runtime-owned evolution governance.
//!
//! Candidate construction, evaluation evidence and release approval are one
//! lifecycle.  Gateway and surfaces may project it, but cannot mutate release
//! state or treat the existence of an evaluation artifact as authorization.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use harness_contract::{
    agent::{AgentDefinitionId, AgentDefinitionRevisionRef, RevisionSelector},
    core::TaskRisk,
    evaluation::{
        EvaluationContract, EvaluationMetricDirection, EvaluationPolicyFloor,
        EvaluationStoppingReason,
    },
    reality::EvidenceRef,
    team::{TeamTemplateDefinitionId, TeamTemplateRevisionRef},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventStore,
    RuntimeTransactionEventInput,
};
use crate::{
    AgentRunEvaluation, ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    GlobalApprovalRequest, GlobalApprovalStatus, RuntimeEventRef, RuntimeEventScope,
    VerifiedDecisionLease, VerifiedPrincipal,
};

const CANDIDATE_STREAM_PREFIX: &str = "evolution:candidate:";
const REVIEW_STREAM_PREFIX: &str = "evolution:review:";
const EVALUATION_POLICY_REVIEW_STREAM_PREFIX: &str = "evolution:evaluation-policy-review:";

fn pending_evolution_approval(
    approval_id: String,
    source: ApprovalSource,
    action: String,
    summary: String,
    evidence_refs: Vec<String>,
) -> GlobalApprovalRequest {
    GlobalApprovalRequest {
        approval_id,
        context: harness_contract::policy::ApprovalContext::owned(&source, &action, "evolution"),
        source,
        action,
        summary,
        risk: TaskRisk::High,
        domain: harness_contract::policy::ApprovalDomain::Evolution,
        blocks_execution: false,
        skippable: false,
        allowed_scopes: vec![harness_contract::policy::ApprovalGrantScope::Once],
        evidence_refs,
        timeout_policy: ApprovalTimeoutPolicy::Pending,
        status: GlobalApprovalStatus::Pending,
        decision: None,
        created_at_ms: now_ms(),
        expires_at_ms: None,
        resolved_at_ms: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvolutionCandidateSubject {
    AgentDefinition {
        revision_ref: AgentDefinitionRevisionRef,
    },
    TeamTemplate {
        revision_ref: TeamTemplateRevisionRef,
    },
}

impl EvolutionCandidateSubject {
    #[must_use]
    pub fn subject_ref(&self) -> String {
        match self {
            Self::AgentDefinition { revision_ref } => format!(
                "agent-definition:{}@{}",
                revision_ref.definition_id.as_str(),
                revision_ref.revision
            ),
            Self::TeamTemplate { revision_ref } => format!(
                "team-template:{}@{}",
                revision_ref.template_id.as_str(),
                revision_ref.revision
            ),
        }
    }

    #[must_use]
    pub fn scope_ref(&self) -> String {
        match self {
            Self::AgentDefinition { revision_ref } => {
                revision_ref.definition_id.as_str().to_string()
            }
            Self::TeamTemplate { revision_ref } => revision_ref.template_id.as_str().to_string(),
        }
    }

    /// Logical release target without a revision. Candidate subjects are
    /// revision-specific so evidence remains attributable, whereas pointer
    /// generations and Canary exclusivity are properties of the underlying
    /// Agent/Team definition.
    #[must_use]
    pub fn release_target_ref(&self) -> String {
        match self {
            Self::AgentDefinition { revision_ref } => {
                format!("agent-definition:{}", revision_ref.definition_id.as_str())
            }
            Self::TeamTemplate { revision_ref } => {
                format!("team-template:{}", revision_ref.template_id.as_str())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionCandidateLifecycle {
    Draft,
    Validated,
    EvaluationBlocked,
    EvaluatedEligible,
    EvaluatedIneligible,
    Withdrawn,
    Superseded,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionGovernanceCandidate {
    pub candidate_id: String,
    pub proposal_id: String,
    pub subject: EvolutionCandidateSubject,
    pub baseline_revision: u64,
    /// Immutable baseline contract resolved by Runtime before the candidate
    /// enters governance. Gateway and a candidate artifact never choose this
    /// value, and report binding uses its content digest.
    pub evaluation_contract: EvaluationContract,
    /// Immutable policy floor captured by Runtime at registration. A
    /// candidate never supplies it, and every later release boundary also
    /// checks the currently active floor.
    #[serde(default)]
    pub evaluation_policy_floor: EvaluationPolicyFloor,
    /// Content digest of the immutable scenario bundle verified before
    /// registration. Evaluation must resolve the same bundle before any
    /// provider call so a mutable file cannot silently change the release
    /// workload.
    #[serde(default)]
    pub evaluation_scenario_digest: String,
    pub source_evidence_refs: Vec<EvidenceRef>,
    /// Immutable rollout policy copied to any approved Canary assignment.
    /// A candidate cannot widen traffic or relax observation thresholds after
    /// it is registered.
    #[serde(default)]
    pub canary_policy: CanaryRolloutPolicy,
    pub lifecycle: EvolutionCandidateLifecycle,
    pub comparison_report_ref: Option<String>,
    pub comparison_report_digest: Option<String>,
    pub canary_review_ref: Option<String>,
    pub stable_review_ref: Option<String>,
    /// Latest immutable observation from an active Canary. It is evidence for
    /// a later Stable review, never an authorization by itself.
    pub canary_observation: Option<CanaryObservationReport>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl EvolutionGovernanceCandidate {
    #[must_use]
    pub fn evaluation_contract_digest(&self) -> String {
        self.evaluation_contract.digest()
    }
}

/// Untrusted intent to register an evolution candidate. Callers cannot set a
/// lifecycle, report, review, or release field; Runtime derives those values
/// after the Definition registry has verified both revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCandidateIntent {
    pub candidate_id: String,
    pub proposal_id: String,
    pub subject: EvolutionCandidateSubject,
    pub baseline_revision: u64,
    pub source_evidence_refs: Vec<EvidenceRef>,
    #[serde(default)]
    pub canary_policy: CanaryRolloutPolicy,
}

/// Trusted Runtime-only registration built from a caller intent after the
/// Definition registry has loaded the immutable baseline contract. Keeping
/// this distinct from [`EvolutionCandidateIntent`] prevents Gateway, Surface
/// or model callers from supplying a weaker contract/dimension set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvolutionCandidateRegistration {
    pub candidate_id: String,
    pub proposal_id: String,
    pub subject: EvolutionCandidateSubject,
    pub baseline_revision: u64,
    pub evaluation_contract: EvaluationContract,
    pub evaluation_scenario_digest: String,
    pub source_evidence_refs: Vec<EvidenceRef>,
    pub canary_policy: CanaryRolloutPolicy,
}

/// Candidate-bound Canary traffic and observation policy. Storing this in the
/// candidate makes every rollout review immutable even if defaults change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryRolloutPolicy {
    /// Fraction of eligible default/latest Binding identities routed to the
    /// candidate, in basis points.
    pub traffic_basis_points: u16,
    pub minimum_samples: u32,
    pub minimum_duration_ms: u64,
    pub maximum_duration_ms: u64,
}

impl Default for CanaryRolloutPolicy {
    fn default() -> Self {
        Self {
            traffic_basis_points: 1_000,
            minimum_samples: 10,
            minimum_duration_ms: 60_000,
            maximum_duration_ms: 86_400_000,
        }
    }
}

impl CanaryRolloutPolicy {
    fn validate(&self) -> Result<(), EvolutionGovernanceError> {
        if self.traffic_basis_points == 0 || self.traffic_basis_points > 10_000 {
            return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                "canary traffic basis points must be between 1 and 10000".to_string(),
            ));
        }
        if self.minimum_samples == 0
            || self.minimum_duration_ms == 0
            || self.maximum_duration_ms < self.minimum_duration_ms
        {
            return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                "canary observation thresholds are invalid".to_string(),
            ));
        }
        Ok(())
    }
}

/// Immutable evidence collected while a candidate receives Canary traffic.
/// The Runtime treats missing, premature, or failed observations as
/// ineligible. A Gateway, surface, or candidate artifact cannot manufacture a
/// Stable review without this record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryObservationReport {
    pub report_id: String,
    pub candidate_id: String,
    pub canary_assignment_id: String,
    pub generation: u64,
    pub source_run_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub sample_count: u32,
    pub minimum_samples: u32,
    pub observed_duration_ms: u64,
    pub minimum_duration_ms: u64,
    pub hard_gates_passed: bool,
    pub protected_dimensions_noninferior: bool,
    pub created_at_ms: u64,
}

impl CanaryObservationReport {
    #[must_use]
    pub fn digest(&self) -> String {
        let value = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(value))
    }

    #[must_use]
    pub fn is_eligible(&self) -> bool {
        self.is_well_formed()
            // A terminal lifecycle record alone proves that a run occurred,
            // not that it satisfied the candidate's evaluation contract.
            // Stable promotion therefore always requires durable evidence
            // emitted by the run or its evaluator.
            && !self.evidence_refs.is_empty()
            && self.sample_count >= self.minimum_samples
            && self.observed_duration_ms >= self.minimum_duration_ms
            && self.hard_gates_passed
            && self.protected_dimensions_noninferior
    }

    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        !self.report_id.trim().is_empty()
            && !self.canary_assignment_id.trim().is_empty()
            && !self.source_run_refs.is_empty()
            && self.minimum_samples > 0
            && self.minimum_duration_ms > 0
    }
}

pub type EvaluationDirection = EvaluationMetricDirection;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvolutionComparisonDimension {
    pub metric_id: String,
    pub direction: EvaluationDirection,
    pub baseline: f64,
    pub candidate: f64,
    pub non_inferiority_margin: f64,
    pub sample_count: u32,
    pub minimum_samples: u32,
    pub confidence: f64,
    pub minimum_confidence: f64,
    /// Contract-bound minimum directional mean delta required for a target
    /// metric to count as a useful improvement.
    #[serde(default = "default_minimum_improvement")]
    pub minimum_improvement: f64,
    /// Independent one-sided superiority confidence. Non-inferiority
    /// confidence cannot be reused as proof that a candidate is better.
    #[serde(default)]
    pub superiority_confidence: f64,
    #[serde(default = "default_minimum_superiority_confidence")]
    pub minimum_superiority_confidence: f64,
    pub hard_gate: bool,
    pub protected: bool,
    pub target_improvement: bool,
}

impl EvolutionComparisonDimension {
    fn non_inferior(&self) -> bool {
        if !self.baseline.is_finite()
            || !self.candidate.is_finite()
            || !self.non_inferiority_margin.is_finite()
            || !self.confidence.is_finite()
        {
            return false;
        }
        if self.sample_count < self.minimum_samples || self.confidence < self.minimum_confidence {
            return false;
        }
        match self.direction {
            EvaluationDirection::HigherIsBetter => {
                self.candidate + self.non_inferiority_margin >= self.baseline
            }
            EvaluationDirection::LowerIsBetter => {
                self.candidate - self.non_inferiority_margin <= self.baseline
            }
        }
    }

    fn improved(&self) -> bool {
        if !self.non_inferior()
            || !self.minimum_improvement.is_finite()
            || !self.superiority_confidence.is_finite()
            || !self.minimum_superiority_confidence.is_finite()
            || self.minimum_improvement <= 0.0
            || self.superiority_confidence < self.minimum_superiority_confidence
        {
            return false;
        }
        match self.direction {
            EvaluationDirection::HigherIsBetter => {
                self.candidate - self.baseline >= self.minimum_improvement
            }
            EvaluationDirection::LowerIsBetter => {
                self.baseline - self.candidate >= self.minimum_improvement
            }
        }
    }
}

const fn default_minimum_improvement() -> f64 {
    0.01
}

const fn default_minimum_superiority_confidence() -> f64 {
    0.9
}

/// Immutable report emitted by an evaluation runner.  It is intentionally
/// self-verifying: a candidate can propose a next contract, but cannot weaken
/// the baseline contract that produced this report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvolutionComparisonReportV2 {
    pub report_id: String,
    pub candidate_id: String,
    pub evaluation_contract_digest: String,
    #[serde(default)]
    pub evaluation_policy_digest: String,
    #[serde(default)]
    pub evaluation_scenario_digest: String,
    #[serde(default)]
    pub subject_ref: String,
    #[serde(default)]
    pub environment_fingerprint: String,
    #[serde(default)]
    pub stopping_reason: EvaluationStoppingReason,
    #[serde(default)]
    pub executed_sample_count: u32,
    pub dimensions: Vec<EvolutionComparisonDimension>,
    pub source_run_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub created_at_ms: u64,
}

/// Preflight result produced by the trusted evaluator before registration or
/// execution. It gives Runtime a stable scenario identity and a hard upper
/// bound on paid paired runs without exposing evaluator internals to Gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionEvaluationReadiness {
    pub scenario_bundle_digest: String,
    pub scenario_refs: Vec<String>,
    pub maximum_paired_runs: u32,
}

impl EvolutionComparisonReportV2 {
    #[must_use]
    pub fn digest(&self) -> String {
        let value = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(value))
    }

    #[must_use]
    pub fn is_eligible(&self) -> bool {
        !self.dimensions.is_empty()
            && !self.source_run_refs.is_empty()
            && !self.evidence_refs.is_empty()
            && self
                .dimensions
                .iter()
                .filter(|dimension| dimension.hard_gate || dimension.protected)
                .all(EvolutionComparisonDimension::non_inferior)
            && self
                .dimensions
                .iter()
                .filter(|dimension| dimension.target_improvement)
                .any(EvolutionComparisonDimension::improved)
    }
}

/// Evaluation is a Runtime-owned port. Implementations live in
/// `harness-eval` or another trusted evaluator; Gateway may compose one but
/// cannot calculate, relax, or forge a release verdict from HTTP payloads.
#[async_trait]
pub trait EvolutionEvalRunner: Send + Sync {
    /// Verify immutable inputs and calculate the maximum paid work before any
    /// provider call. Test/replay runners get a deterministic contract-only
    /// fallback; production scenario runners override this with asset digests.
    fn readiness(
        &self,
        contract: &EvaluationContract,
    ) -> Result<EvolutionEvaluationReadiness, String> {
        contract.validate().map_err(|error| error.to_string())?;
        let mut refs = contract.scenario_refs.clone();
        refs.sort();
        let payload =
            serde_json::to_vec(&(contract.digest(), &refs)).map_err(|error| error.to_string())?;
        let maximum_paired_runs = contract
            .metrics
            .iter()
            .map(|metric| match metric.stopping_rule {
                harness_contract::evaluation::EvaluationStoppingRule::FixedSamples => {
                    metric.minimum_samples
                }
                harness_contract::evaluation::EvaluationStoppingRule::Sequential {
                    max_samples,
                    ..
                } => max_samples,
            })
            .max()
            .unwrap_or_default();
        Ok(EvolutionEvaluationReadiness {
            scenario_bundle_digest: format!("sha256:{:x}", Sha256::digest(payload)),
            scenario_refs: refs,
            maximum_paired_runs,
        })
    }

    async fn evaluate(
        &self,
        candidate: &EvolutionGovernanceCandidate,
    ) -> Result<EvolutionComparisonReportV2, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChangeAction {
    PromoteCanary,
    PromoteStable,
    SetDefaultLatest,
    SetDefaultExact,
    Rollback,
    StopCanary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChangeReviewClass {
    Canary,
    Stable,
    Pointer,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChangeReviewStatus {
    Pending,
    Approved,
    Denied,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseChangeReview {
    pub review_id: String,
    pub approval_id: String,
    pub class: ReleaseChangeReviewClass,
    pub action: ReleaseChangeAction,
    pub candidate_id: Option<String>,
    pub subject: EvolutionCandidateSubject,
    pub baseline_revision: Option<u64>,
    pub candidate_revision: Option<u64>,
    pub comparison_report_ref: Option<String>,
    pub comparison_report_digest: Option<String>,
    pub prior_canary_approval_ref: Option<String>,
    pub active_canary_assignment_ref: Option<String>,
    pub observation_report_ref: Option<String>,
    pub observation_report_digest: Option<String>,
    pub expected_selector: Option<RevisionSelector>,
    pub expected_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary_policy: Option<CanaryRolloutPolicy>,
    pub status: ReleaseChangeReviewStatus,
    pub created_at_ms: u64,
}

/// Untrusted intent to request a human-governed release or pointer change.
/// It can create a pending review only; Runtime validates the referenced
/// Definition revision and a verified human lease is still required to make
/// any assignment or pointer effective.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseChangeRequest {
    /// Caller-provided idempotency key. Repeating the same request returns the
    /// same review instead of creating competing approval work.
    pub request_id: String,
    pub subject: EvolutionCandidateSubject,
    pub action: ReleaseChangeAction,
    /// Required for `SetDefaultExact` and `Rollback`; it is the exact target
    /// revision selector, never an arbitrary filesystem path.
    pub selector: Option<RevisionSelector>,
    /// `StopCanary` must name the candidate whose Canary assignment is being
    /// stopped so an unrelated revision cannot cancel it.
    pub candidate_id: Option<String>,
    pub evidence_refs: Vec<EvidenceRef>,
}

/// A policy change is deliberately separate from an Agent/Team candidate.
/// The request may tighten or relax a future workspace floor, but it cannot
/// alter the floor snapshot already bound to an existing candidate without a
/// distinct human-interactive approval transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationPolicyChangeIntent {
    pub request_id: String,
    pub next_policy: EvaluationPolicyFloor,
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationPolicyChangeReview {
    pub review_id: String,
    pub approval_id: String,
    pub previous_policy: EvaluationPolicyFloor,
    pub next_policy: EvaluationPolicyFloor,
    pub expected_policy_digest: String,
    pub status: ReleaseChangeReviewStatus,
    pub created_at_ms: u64,
}

impl EvaluationPolicyChangeReview {
    #[must_use]
    pub fn evidence_digest(&self) -> String {
        evaluation_policy_review_digest(self)
    }

    #[must_use]
    pub fn action_key(&self) -> &'static str {
        evaluation_policy_action_key()
    }

    #[must_use]
    pub fn scope_ref(&self) -> &str {
        &self.previous_policy.policy_id
    }
}

impl ReleaseChangeReview {
    #[must_use]
    pub fn evidence_digest(&self) -> String {
        review_digest(self)
    }

    #[must_use]
    pub fn action_key(&self) -> &'static str {
        release_action_key(self.action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionReleaseAssignment {
    pub assignment_id: String,
    pub review_id: String,
    pub candidate_id: Option<String>,
    pub subject: EvolutionCandidateSubject,
    pub action: ReleaseChangeAction,
    pub selector: Option<RevisionSelector>,
    pub generation: u64,
    pub approval_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary_policy: Option<CanaryRolloutPolicy>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChangeReviewDecision {
    Approve,
    Reject,
    Revise,
}

#[derive(Debug, Error)]
pub enum EvolutionGovernanceError {
    #[error("evolution candidate `{0}` was not found")]
    CandidateNotFound(String),
    #[error("evolution review `{0}` was not found")]
    ReviewNotFound(String),
    #[error("evolution candidate is not eligible for a release review")]
    CandidateNotEligible,
    #[error("evolution report does not satisfy the immutable evaluation contract")]
    IneligibleReport,
    #[error("evolution report contract digest does not match the candidate")]
    ContractDigestMismatch,
    #[error("only an interactive human with evolution.release.manage may decide a release review")]
    HumanCapabilityRequired,
    #[error("stable review requires an approved active canary observation")]
    CanaryPrerequisiteRequired,
    #[error("canary observation does not satisfy the registered release gates")]
    IneligibleCanaryObservation,
    #[error("canary observation is not bound to the candidate's active assignment")]
    CanaryObservationAssignmentMismatch,
    #[error("release review is not pending")]
    ReviewNotPending,
    #[error("release change request is invalid: {0}")]
    InvalidReleaseChangeRequest(String),
    #[error("release generation changed before this review could be decided")]
    ReleaseGenerationChanged,
    #[error("evaluation policy changed before this review could be decided")]
    EvaluationPolicyChanged,
    #[error("an active Canary already exists for this definition subject")]
    ActiveCanaryAlreadyExists,
    #[error("runtime evolution store failure: {0}")]
    Store(String),
}

/// Single Runtime owner for evolution candidate, comparison and review
/// projections.  The event ledger is the source of truth; this service does
/// not keep a JSONL side registry or mutable Gateway cache.
#[derive(Debug)]
pub struct EvolutionGovernanceService {
    event_store: Arc<RuntimeEventStore>,
    approvals: Arc<ApprovalQueue>,
}

impl EvolutionGovernanceService {
    #[must_use]
    pub(crate) fn new(event_store: Arc<RuntimeEventStore>, approvals: Arc<ApprovalQueue>) -> Self {
        Self {
            event_store,
            approvals,
        }
    }

    pub(crate) fn create_candidate(
        &self,
        candidate: EvolutionGovernanceCandidate,
    ) -> Result<EvolutionGovernanceCandidate, EvolutionGovernanceError> {
        candidate.canary_policy.validate()?;
        if candidate.candidate_id.trim().is_empty()
            || candidate.proposal_id.trim().is_empty()
            || candidate.source_evidence_refs.is_empty()
            || candidate
                .source_evidence_refs
                .iter()
                .any(|evidence| !evidence.boundary.can_be_authoritative())
        {
            return Err(EvolutionGovernanceError::Store(
                "candidate id, proposal id, immutable evaluation contract, and authoritative evidence are required"
                    .to_string(),
            ));
        }
        candidate
            .evaluation_policy_floor
            .validate_contract(&candidate.evaluation_contract)
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.evaluation_policy_floor()
            .validate_contract(&candidate.evaluation_contract)
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        let stream = candidate_stream(&candidate.candidate_id);
        let revision = self.stream_revision(&stream)?;
        self.event_store
            .append_batch_if_revision(
                stream,
                revision,
                format!("evolution-candidate-create:{}", candidate.candidate_id),
                vec![event(
                    candidate_stream(&candidate.candidate_id),
                    "evolution.candidate.created",
                    Some("validated"),
                    vec![subject_ref(&candidate.subject)],
                    serde_json::json!({"candidate": candidate}),
                )],
            )
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.candidate(&candidate.candidate_id)
    }

    /// Construct a Draft candidate from caller intent. This is the only
    /// constructor Gateway and composition roots should use; lifecycle and
    /// release-related fields are owned exclusively by Runtime events.
    pub(crate) fn register_candidate(
        &self,
        registration: EvolutionCandidateRegistration,
    ) -> Result<EvolutionGovernanceCandidate, EvolutionGovernanceError> {
        if let Ok(existing) = self.candidate(&registration.candidate_id) {
            let same_registration = existing.proposal_id == registration.proposal_id
                && existing.subject == registration.subject
                && existing.baseline_revision == registration.baseline_revision
                && existing.evaluation_contract == registration.evaluation_contract
                && existing.evaluation_scenario_digest == registration.evaluation_scenario_digest
                && existing.source_evidence_refs == registration.source_evidence_refs
                && existing.canary_policy == registration.canary_policy;
            return if same_registration {
                Ok(existing)
            } else {
                Err(EvolutionGovernanceError::Store(format!(
                    "evolution candidate `{}` is already registered with different immutable inputs",
                    registration.candidate_id
                )))
            };
        }
        let now = now_ms();
        self.create_candidate(EvolutionGovernanceCandidate {
            candidate_id: registration.candidate_id,
            proposal_id: registration.proposal_id,
            subject: registration.subject,
            baseline_revision: registration.baseline_revision,
            evaluation_contract: registration.evaluation_contract,
            evaluation_policy_floor: self.evaluation_policy_floor(),
            evaluation_scenario_digest: registration.evaluation_scenario_digest,
            source_evidence_refs: registration.source_evidence_refs,
            canary_policy: registration.canary_policy,
            lifecycle: EvolutionCandidateLifecycle::Validated,
            comparison_report_ref: None,
            comparison_report_digest: None,
            canary_review_ref: None,
            stable_review_ref: None,
            canary_observation: None,
            created_at_ms: now,
            updated_at_ms: now,
        })
    }

    pub(crate) fn candidate(
        &self,
        candidate_id: &str,
    ) -> Result<EvolutionGovernanceCandidate, EvolutionGovernanceError> {
        let events = self
            .event_store
            .list_stream(&candidate_stream(candidate_id))
            .map_err(EvolutionGovernanceError::Store)?;
        materialize_candidate(events)
            .ok_or_else(|| EvolutionGovernanceError::CandidateNotFound(candidate_id.to_string()))
    }

    pub(crate) fn list_candidates(
        &self,
    ) -> Result<Vec<EvolutionGovernanceCandidate>, EvolutionGovernanceError> {
        let events = self
            .event_store
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, CANDIDATE_STREAM_PREFIX)
            .map_err(EvolutionGovernanceError::Store)?;
        let mut by_stream = BTreeMap::new();
        for event in events {
            if event.stream_id.starts_with("evolution:candidate:") {
                by_stream
                    .entry(event.stream_id.clone())
                    .or_insert_with(Vec::new)
                    .push(event);
            }
        }
        let mut candidates = by_stream
            .into_values()
            .filter_map(|mut stream| {
                stream.sort_by_key(|event| event.sequence);
                materialize_candidate(stream)
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.updated_at_ms.cmp(&left.updated_at_ms));
        Ok(candidates)
    }

    pub(crate) fn recent_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<EvolutionGovernanceCandidate>, EvolutionGovernanceError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut candidates = Vec::new();
        for event in self
            .event_store
            .list_scope(
                RuntimeEventScope::Evolution,
                limit.saturating_mul(16).clamp(16, 512),
            )
            .map_err(EvolutionGovernanceError::Store)?
        {
            let Some(candidate_id) = event.stream_id.strip_prefix(CANDIDATE_STREAM_PREFIX) else {
                continue;
            };
            if seen.insert(candidate_id.to_string()) {
                candidates.push(self.candidate(candidate_id)?);
                if candidates.len() == limit {
                    break;
                }
            }
        }
        Ok(candidates)
    }

    /// All review projections are restored from Runtime events. The result is
    /// intentionally read-only: only typed Runtime commands may append a
    /// review or decision event.
    pub(crate) fn list_reviews(
        &self,
    ) -> Result<Vec<ReleaseChangeReview>, EvolutionGovernanceError> {
        let events = self
            .event_store
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, REVIEW_STREAM_PREFIX)
            .map_err(EvolutionGovernanceError::Store)?;
        let mut by_stream = BTreeMap::new();
        for event in events {
            if event.stream_id.starts_with("evolution:review:") {
                by_stream
                    .entry(event.stream_id.clone())
                    .or_insert_with(Vec::new)
                    .push(event);
            }
        }
        let mut reviews = by_stream
            .into_values()
            .filter_map(materialize_review)
            .map(|review| self.derive_review_status(review))
            .collect::<Result<Vec<_>, _>>()?;
        reviews.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(reviews)
    }

    pub(crate) fn recent_reviews(
        &self,
        limit: usize,
    ) -> Result<Vec<ReleaseChangeReview>, EvolutionGovernanceError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut reviews = Vec::new();
        for event in self
            .event_store
            .list_scope(
                RuntimeEventScope::Evolution,
                limit.saturating_mul(16).clamp(16, 512),
            )
            .map_err(EvolutionGovernanceError::Store)?
        {
            let Some(review_id) = event.stream_id.strip_prefix(REVIEW_STREAM_PREFIX) else {
                continue;
            };
            if seen.insert(review_id.to_string()) {
                reviews.push(self.review(review_id)?);
                if reviews.len() == limit {
                    break;
                }
            }
        }
        Ok(reviews)
    }

    /// List policy reviews from the same Runtime event ledger used for
    /// candidate rollout. Their approval status remains derived from the
    /// decision event, never a mutable Gateway-owned flag.
    pub(crate) fn list_evaluation_policy_reviews(
        &self,
    ) -> Result<Vec<EvaluationPolicyChangeReview>, EvolutionGovernanceError> {
        let events = self
            .event_store
            .replay_scope_stream_prefix(
                RuntimeEventScope::Evolution,
                EVALUATION_POLICY_REVIEW_STREAM_PREFIX,
            )
            .map_err(EvolutionGovernanceError::Store)?;
        let mut by_stream = BTreeMap::new();
        for event in events {
            if event
                .stream_id
                .starts_with("evolution:evaluation-policy-review:")
            {
                by_stream
                    .entry(event.stream_id.clone())
                    .or_insert_with(Vec::new)
                    .push(event);
            }
        }
        let mut reviews = by_stream
            .into_values()
            .filter_map(materialize_evaluation_policy_review)
            .collect::<Vec<_>>();
        reviews.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(reviews)
    }

    pub(crate) fn request_evaluation_policy_change(
        &self,
        intent: EvaluationPolicyChangeIntent,
    ) -> Result<EvaluationPolicyChangeReview, EvolutionGovernanceError> {
        if intent.request_id.trim().is_empty() || intent.evidence_refs.is_empty() {
            return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                "evaluation policy change requires a request id and durable evidence".to_string(),
            ));
        }
        intent.next_policy.validate().map_err(|error| {
            EvolutionGovernanceError::InvalidReleaseChangeRequest(error.to_string())
        })?;
        let previous_policy = self.evaluation_policy_floor();
        if intent.next_policy.policy_id != previous_policy.policy_id
            || intent.next_policy.revision != previous_policy.revision.saturating_add(1)
        {
            return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                "evaluation policy changes must retain policy_id and advance exactly one revision"
                    .to_string(),
            ));
        }
        let review_id = format!("evolution-evaluation-policy-review:{}", intent.request_id);
        if let Some(existing) = self.evaluation_policy_review(&review_id)? {
            return Ok(existing);
        }
        let approval_id = format!("approval:{review_id}");
        let review = EvaluationPolicyChangeReview {
            review_id: review_id.clone(),
            approval_id: approval_id.clone(),
            expected_policy_digest: previous_policy.digest(),
            previous_policy,
            next_policy: intent.next_policy,
            status: ReleaseChangeReviewStatus::Pending,
            created_at_ms: now_ms(),
        };
        let approval = pending_evolution_approval(
            approval_id.clone(),
            ApprovalSource {
                kind: ApprovalSourceKind::Evolution,
                session_id: None,
                agent_id: None,
                team_id: None,
                mission_id: Some("evaluation-policy".to_string()),
                resource_ref: None,
                review_ref: None,
                application: None,
            },
            evaluation_policy_action_key().to_string(),
            format!(
                "Change evaluation policy {} from revision {} to {}",
                review.next_policy.policy_id,
                review.previous_policy.revision,
                review.next_policy.revision
            ),
            approval_evidence_refs(&intent.evidence_refs),
        );
        let approval_stream = format!("approval:{}", approval.approval_id);
        let review_stream = evaluation_policy_review_stream(&review.review_id);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("evaluation-policy-review-create:{}", review.review_id),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: approval_stream.clone(),
                        expected_revision: self.stream_revision(&approval_stream)?,
                    },
                    ExpectedStreamRevision {
                        stream_id: review_stream.clone(),
                        expected_revision: self.stream_revision(&review_stream)?,
                    },
                    ExpectedStreamRevision {
                        stream_id: evaluation_policy_stream(),
                        expected_revision: self.stream_revision(&evaluation_policy_stream())?,
                    },
                ],
                events: vec![
                    RuntimeEventInput {
                        stream_id: approval_stream,
                        scope: RuntimeEventScope::Approval,
                        kind: "approval.submitted".to_string(),
                        status: Some("pending".to_string()),
                        actor: Some("runtime.evolution_governance".to_string()),
                        refs: vec![RuntimeEventRef {
                            kind: "evaluation_policy".to_string(),
                            id: review.next_policy.policy_id.clone(),
                        }],
                        payload: serde_json::json!({"request": approval}),
                    }
                    .into(),
                    event(
                        review_stream,
                        "evolution.evaluation_policy_review.requested",
                        Some("pending"),
                        vec![RuntimeEventRef {
                            kind: "evaluation_policy".to_string(),
                            id: review.next_policy.policy_id.clone(),
                        }],
                        serde_json::json!({"review": review}),
                    ),
                ],
            })
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(review)
    }

    pub(crate) fn decide_evaluation_policy_change(
        &self,
        principal: &VerifiedPrincipal,
        lease: &VerifiedDecisionLease,
        review_id: &str,
        decision: ReleaseChangeReviewDecision,
        reason: String,
    ) -> Result<Option<EvaluationPolicyFloor>, EvolutionGovernanceError> {
        if !principal.is_human_interactive()
            || !principal.has_capability("evolution.release.manage")
        {
            return Err(EvolutionGovernanceError::HumanCapabilityRequired);
        }
        let review = self
            .evaluation_policy_review(review_id)?
            .ok_or_else(|| EvolutionGovernanceError::ReviewNotFound(review_id.to_string()))?;
        if review.status != ReleaseChangeReviewStatus::Pending {
            return Err(EvolutionGovernanceError::ReviewNotPending);
        }
        let scope = evaluation_policy_scope(&review.previous_policy);
        if lease.review_id() != review.review_id
            || lease.action() != evaluation_policy_action_key()
            || lease.scope() != scope
            || lease.evidence_digest() != evaluation_policy_review_digest(&review)
        {
            return Err(EvolutionGovernanceError::HumanCapabilityRequired);
        }
        let approval = self
            .approvals
            .get(&review.approval_id)
            .ok_or_else(|| EvolutionGovernanceError::ReviewNotFound(review.approval_id.clone()))?;
        if approval.status != GlobalApprovalStatus::Pending {
            return Err(EvolutionGovernanceError::ReviewNotPending);
        }
        if self.evaluation_policy_floor().digest() != review.expected_policy_digest {
            return Err(EvolutionGovernanceError::EvaluationPolicyChanged);
        }
        let approved = decision == ReleaseChangeReviewDecision::Approve;
        let approval_stream = format!("approval:{}", review.approval_id);
        let review_stream = evaluation_policy_review_stream(&review.review_id);
        let policy_stream = evaluation_policy_stream();
        let decided_by = principal.claims().principal_id.clone();
        let resolved_at_ms = now_ms();
        let mut events = vec![
            RuntimeEventInput {
                stream_id: approval_stream.clone(),
                scope: RuntimeEventScope::Approval,
                kind: "approval.decided".to_string(),
                status: Some(if approved { "approved" } else { "denied" }.to_string()),
                actor: Some(decided_by.clone()),
                refs: vec![RuntimeEventRef {
                    kind: "evaluation_policy".to_string(),
                    id: review.next_policy.policy_id.clone(),
                }],
                payload: serde_json::json!({
                    "approved": approved,
                    "reason": reason,
                    "message": if approved { format!("approved by {decided_by}") } else { format!("denied by {decided_by}") },
                    "resolved_at_ms": resolved_at_ms,
                }),
            }
            .into(),
            event(
                review_stream.clone(),
                "evolution.evaluation_policy_review.decided",
                Some(if approved { "approved" } else { "denied" }),
                vec![RuntimeEventRef {
                    kind: "evaluation_policy".to_string(),
                    id: review.next_policy.policy_id.clone(),
                }],
                serde_json::json!({"decision": decision, "reason": reason}),
            ),
        ];
        if approved {
            events.push(event(
                policy_stream.clone(),
                "evolution.evaluation_policy.updated",
                Some("active"),
                vec![RuntimeEventRef {
                    kind: "evaluation_policy".to_string(),
                    id: review.next_policy.policy_id.clone(),
                }],
                serde_json::json!({"policy": review.next_policy}),
            ));
        }
        self.event_store
            .append_transaction_with_verified_decision_lease(
                AppendTransactionRequest {
                    transaction_id: format!(
                        "evaluation-policy-review-decide:{}:{:?}",
                        review.review_id, decision
                    ),
                    expected_streams: vec![
                        ExpectedStreamRevision {
                            stream_id: approval_stream.clone(),
                            expected_revision: self.stream_revision(&approval_stream)?,
                        },
                        ExpectedStreamRevision {
                            stream_id: review_stream.clone(),
                            expected_revision: self.stream_revision(&review_stream)?,
                        },
                        ExpectedStreamRevision {
                            stream_id: policy_stream.clone(),
                            expected_revision: self.stream_revision(&policy_stream)?,
                        },
                    ],
                    events,
                },
                lease,
            )
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(approved.then_some(review.next_policy))
    }

    /// Authorized release projections are replayable. Definition stores use
    /// this list to converge after a crash without inventing another release
    /// authority or reading Gateway-owned JSONL state.
    pub(crate) fn release_assignments(
        &self,
    ) -> Result<Vec<EvolutionReleaseAssignment>, EvolutionGovernanceError> {
        let mut assignments = BTreeMap::new();
        for kind in [
            "evolution.release.assignment_authorized",
            "evolution.release_review.decided",
        ] {
            let events = self
                .event_store
                .replay_scope_kind(RuntimeEventScope::Evolution, kind)
                .map_err(EvolutionGovernanceError::Store)?;
            for event in events {
                let Some(value) = event.payload.get("assignment") else {
                    continue;
                };
                let Some(assignment) =
                    serde_json::from_value::<EvolutionReleaseAssignment>(value.clone()).ok()
                else {
                    continue;
                };
                assignments.insert(assignment.assignment_id.clone(), assignment);
            }
        }
        Ok(assignments.into_values().collect())
    }

    pub(crate) fn record_comparison(
        &self,
        report: EvolutionComparisonReportV2,
    ) -> Result<EvolutionGovernanceCandidate, EvolutionGovernanceError> {
        let candidate = self.candidate(&report.candidate_id)?;
        if candidate.evaluation_contract_digest() != report.evaluation_contract_digest {
            return Err(EvolutionGovernanceError::ContractDigestMismatch);
        }
        if candidate.evaluation_policy_floor.digest() != report.evaluation_policy_digest
            || candidate.evaluation_scenario_digest != report.evaluation_scenario_digest
            || candidate.subject.subject_ref() != report.subject_ref
            || report.environment_fingerprint.trim().is_empty()
            || report.executed_sample_count == 0
        {
            return Err(EvolutionGovernanceError::IneligibleReport);
        }
        candidate
            .evaluation_policy_floor
            .validate_contract(&candidate.evaluation_contract)
            .map_err(|_| EvolutionGovernanceError::IneligibleReport)?;
        self.evaluation_policy_floor()
            .validate_contract(&candidate.evaluation_contract)
            .map_err(|_| EvolutionGovernanceError::IneligibleReport)?;
        validate_report_against_contract(&report, &candidate.evaluation_contract)?;
        let eligible = report.is_eligible();
        let stream = candidate_stream(&candidate.candidate_id);
        let revision = self.stream_revision(&stream)?;
        self.event_store
            .append_batch_if_revision(
                stream,
                revision,
                format!(
                    "evolution-comparison:{}:{}",
                    candidate.candidate_id,
                    report.digest()
                ),
                vec![event(
                    candidate_stream(&candidate.candidate_id),
                    "evolution.comparison.recorded",
                    Some(if eligible {
                        "evaluated_eligible"
                    } else {
                        "evaluated_ineligible"
                    }),
                    vec![subject_ref(&candidate.subject)],
                    serde_json::json!({
                        "report": report,
                        "report_ref": format!("evolution-comparison:{}", report.report_id),
                        "report_digest": report.digest(),
                        "eligible": eligible,
                    }),
                )],
            )
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.candidate(&candidate.candidate_id)
    }

    pub(crate) fn record_evaluation_blocked(
        &self,
        candidate_id: &str,
        reason: &str,
    ) -> Result<EvolutionGovernanceCandidate, EvolutionGovernanceError> {
        let candidate = self.candidate(candidate_id)?;
        if matches!(
            candidate.lifecycle,
            EvolutionCandidateLifecycle::EvaluatedEligible
                | EvolutionCandidateLifecycle::EvaluatedIneligible
        ) {
            return Ok(candidate);
        }
        let reason = reason.trim();
        if reason.is_empty() {
            return Err(EvolutionGovernanceError::Store(
                "evaluation blocked reason is required".to_string(),
            ));
        }
        let stream = candidate_stream(candidate_id);
        self.event_store
            .append_batch_if_revision(
                stream.clone(),
                self.stream_revision(&stream)?,
                format!("evolution-evaluation-blocked:{candidate_id}:{reason}"),
                vec![event(
                    stream,
                    "evolution.candidate.evaluation_blocked.v1",
                    Some("evaluation_blocked"),
                    vec![subject_ref(&candidate.subject)],
                    serde_json::json!({"reason": reason}),
                )],
            )
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.candidate(candidate_id)
    }

    pub(crate) fn request_canary_review(
        &self,
        candidate_id: &str,
    ) -> Result<ReleaseChangeReview, EvolutionGovernanceError> {
        let candidate = self.candidate(candidate_id)?;
        self.evaluation_policy_floor()
            .validate_contract(&candidate.evaluation_contract)
            .map_err(|_| EvolutionGovernanceError::IneligibleReport)?;
        if candidate.lifecycle != EvolutionCandidateLifecycle::EvaluatedEligible {
            return Err(EvolutionGovernanceError::CandidateNotEligible);
        }
        let report_ref = candidate
            .comparison_report_ref
            .clone()
            .ok_or(EvolutionGovernanceError::CandidateNotEligible)?;
        let review_id = format!("evolution-review:{}:canary", candidate.candidate_id);
        let approval_id = format!("approval:{review_id}");
        if let Ok(existing) = self.review(&review_id) {
            return Ok(existing);
        }
        let review = ReleaseChangeReview {
            review_id: review_id.clone(),
            approval_id: approval_id.clone(),
            class: ReleaseChangeReviewClass::Canary,
            action: ReleaseChangeAction::PromoteCanary,
            candidate_id: Some(candidate.candidate_id.clone()),
            subject: candidate.subject.clone(),
            baseline_revision: Some(candidate.baseline_revision),
            candidate_revision: subject_revision(&candidate.subject),
            comparison_report_ref: Some(report_ref.clone()),
            comparison_report_digest: candidate.comparison_report_digest.clone(),
            prior_canary_approval_ref: None,
            active_canary_assignment_ref: None,
            observation_report_ref: None,
            observation_report_digest: None,
            expected_selector: None,
            expected_generation: self.current_release_generation(&candidate.subject)?,
            canary_policy: Some(candidate.canary_policy.clone()),
            status: ReleaseChangeReviewStatus::Pending,
            created_at_ms: now_ms(),
        };
        let approval = pending_evolution_approval(
            approval_id.clone(),
            ApprovalSource {
                kind: ApprovalSourceKind::Evolution,
                session_id: None,
                agent_id: None,
                team_id: None,
                mission_id: Some(candidate.candidate_id.clone()),
                resource_ref: None,
                review_ref: None,
                application: None,
            },
            release_action_key(ReleaseChangeAction::PromoteCanary).to_string(),
            format!("Promote {} to Canary", candidate.subject.subject_ref()),
            approval_evidence_refs(&candidate.source_evidence_refs),
        );
        let approval_stream = format!("approval:{}", approval.approval_id);
        let review_stream = review_stream(&review.review_id);
        let candidate_stream = candidate_stream(&candidate.candidate_id);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("evolution-review-create:{}", review.review_id),
                expected_streams: vec![
                    ExpectedStreamRevision { stream_id: approval_stream.clone(), expected_revision: self.stream_revision(&approval_stream)? },
                    ExpectedStreamRevision { stream_id: review_stream.clone(), expected_revision: self.stream_revision(&review_stream)? },
                    ExpectedStreamRevision { stream_id: candidate_stream.clone(), expected_revision: self.stream_revision(&candidate_stream)? },
                ],
                events: vec![
                    RuntimeEventInput {
                        stream_id: approval_stream,
                        scope: RuntimeEventScope::Approval,
                        kind: "approval.submitted".to_string(),
                        status: Some("pending".to_string()),
                        actor: Some("runtime.evolution_governance".to_string()),
                        refs: vec![subject_ref(&candidate.subject)],
                        payload: serde_json::json!({
                            "request": approval,
                            "action": release_action_key(ReleaseChangeAction::PromoteCanary),
                            "summary": format!("Promote {} to Canary", candidate.subject.subject_ref()),
                            "risk": TaskRisk::High,
                            "timeout_policy": ApprovalTimeoutPolicy::Pending,
                        }),
                    }.into(),
                    event(
                        review_stream,
                        "evolution.release_review.requested",
                        Some("pending"),
                        vec![subject_ref(&review.subject)],
                        serde_json::json!({"review": review}),
                    ),
                    event(
                        candidate_stream,
                        "evolution.candidate.canary_review_linked",
                        Some("evaluated_eligible"),
                        vec![subject_ref(&candidate.subject)],
                        serde_json::json!({"review_id": review_id}),
                    ),
                ],
            })
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(review)
    }

    /// Persist a Canary observation against the exact active Runtime release
    /// assignment. This is an evidence write, not a Stable promotion.
    pub(crate) fn record_canary_observation(
        &self,
        observation: CanaryObservationReport,
    ) -> Result<EvolutionGovernanceCandidate, EvolutionGovernanceError> {
        let candidate = self.candidate(&observation.candidate_id)?;
        let assignment = self.active_canary_assignment(&candidate)?;
        if observation.canary_assignment_id != assignment.assignment_id
            || observation.generation != assignment.generation
        {
            return Err(EvolutionGovernanceError::CanaryObservationAssignmentMismatch);
        }
        let policy = assignment
            .canary_policy
            .as_ref()
            .ok_or(EvolutionGovernanceError::CanaryPrerequisiteRequired)?;
        if !observation.is_well_formed()
            || observation.minimum_samples != policy.minimum_samples
            || observation.minimum_duration_ms != policy.minimum_duration_ms
            || observation.observed_duration_ms > policy.maximum_duration_ms
        {
            return Err(EvolutionGovernanceError::IneligibleCanaryObservation);
        }
        let stream = candidate_stream(&candidate.candidate_id);
        let revision = self.stream_revision(&stream)?;
        self.event_store
            .append_batch_if_revision(
                stream,
                revision,
                format!(
                    "evolution-canary-observation:{}:{}",
                    candidate.candidate_id,
                    observation.digest()
                ),
                vec![event(
                    candidate_stream(&candidate.candidate_id),
                    "evolution.canary.observation.recorded",
                    Some("evaluated_eligible"),
                    vec![subject_ref(&candidate.subject)],
                    serde_json::json!({
                        "observation": observation,
                        "observation_ref": format!("evolution-canary-observation:{}", observation.report_id),
                        "observation_digest": observation.digest(),
                    }),
                )],
            )
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.candidate(&candidate.candidate_id)
    }

    /// Rebuild the latest Canary observation from immutable terminal Agent
    /// evaluations. This is a projection-only operation: it never changes a
    /// release assignment and is safe to retry after a process restart.
    pub(crate) fn refresh_canary_observations_from_agent_runs(
        &self,
        evaluations: &[AgentRunEvaluation],
    ) -> Result<Vec<EvolutionGovernanceCandidate>, EvolutionGovernanceError> {
        let mut refreshed = Vec::new();
        for candidate in self.list_candidates()? {
            let EvolutionCandidateSubject::AgentDefinition { revision_ref } = &candidate.subject
            else {
                continue;
            };
            let Ok(assignment) = self.active_canary_assignment(&candidate) else {
                continue;
            };
            let policy = assignment
                .canary_policy
                .as_ref()
                .ok_or(EvolutionGovernanceError::CanaryPrerequisiteRequired)?;
            let mut matching = evaluations
                .iter()
                .filter(|evaluation| {
                    evaluation.definition_id == revision_ref.definition_id.as_str()
                        && evaluation.definition_revision == revision_ref.revision
                        && evaluation.release_channel
                            == Some(harness_contract::agent::ReleaseChannel::Canary)
                        && evaluation.release_assignment_id.as_deref()
                            == Some(assignment.assignment_id.as_str())
                        && evaluation.release_generation == Some(assignment.generation)
                })
                .collect::<Vec<_>>();
            matching.sort_by(|left, right| {
                left.created_at_ms
                    .cmp(&right.created_at_ms)
                    .then_with(|| left.run_id.cmp(&right.run_id))
            });
            let Some(first) = matching.first() else {
                continue;
            };
            let Some(latest) = matching.last() else {
                continue;
            };
            let mut source_run_refs = matching
                .iter()
                .map(|evaluation| format!("agent-run:{}", evaluation.run_id))
                .collect::<Vec<_>>();
            source_run_refs.sort();
            source_run_refs.dedup();
            let mut evidence_refs = matching
                .iter()
                .flat_map(|evaluation| {
                    evaluation.evidence_refs.iter().map(|reference| {
                        EvidenceRef::observed("agent_run_evidence", reference.clone())
                            .with_source(evaluation.evaluation_id.clone())
                    })
                })
                .collect::<Vec<_>>();
            evidence_refs.sort_by(|left, right| {
                (&left.ref_type, &left.id).cmp(&(&right.ref_type, &right.id))
            });
            evidence_refs
                .dedup_by(|left, right| left.ref_type == right.ref_type && left.id == right.id);
            let observation = CanaryObservationReport {
                report_id: format!(
                    "canary-observation:{}:{}",
                    assignment.assignment_id, latest.run_id
                ),
                candidate_id: candidate.candidate_id.clone(),
                canary_assignment_id: assignment.assignment_id.clone(),
                generation: assignment.generation,
                source_run_refs,
                evidence_refs,
                sample_count: matching.len().min(u32::MAX as usize) as u32,
                minimum_samples: policy.minimum_samples,
                observed_duration_ms: latest.created_at_ms.saturating_sub(first.created_at_ms),
                minimum_duration_ms: policy.minimum_duration_ms,
                hard_gates_passed: matching.iter().all(|evaluation| evaluation.is_success()),
                // The pre-Canary comparison establishes the baseline. Live
                // observations can only keep this true when every sampled
                // run passes; a failed run forces human review instead of an
                // optimistic inferred comparison.
                protected_dimensions_noninferior: matching
                    .iter()
                    .all(|evaluation| evaluation.is_success()),
                created_at_ms: latest.created_at_ms,
            };
            if candidate
                .canary_observation
                .as_ref()
                .is_some_and(|existing| existing.digest() == observation.digest())
            {
                continue;
            }
            refreshed.push(self.record_canary_observation(observation)?);
        }
        Ok(refreshed)
    }

    /// Only the Runtime-owned Canary observation gate can create a Stable
    /// review. Gateway merely invokes this typed command and cannot supply a
    /// replacement observation, action, generation, or approval reference.
    pub(crate) fn request_stable_review(
        &self,
        candidate_id: &str,
    ) -> Result<ReleaseChangeReview, EvolutionGovernanceError> {
        let candidate = self.candidate(candidate_id)?;
        let observation = candidate
            .canary_observation
            .clone()
            .ok_or(EvolutionGovernanceError::CanaryPrerequisiteRequired)?;
        if !observation.is_eligible() {
            return Err(EvolutionGovernanceError::IneligibleCanaryObservation);
        }
        let canary = self.active_canary_assignment(&candidate)?;
        if canary.assignment_id != observation.canary_assignment_id
            || canary.generation != observation.generation
        {
            return Err(EvolutionGovernanceError::CanaryObservationAssignmentMismatch);
        }
        let canary_review = self.review(&canary.review_id)?;
        if canary_review.class != ReleaseChangeReviewClass::Canary
            || canary_review.status != ReleaseChangeReviewStatus::Approved
        {
            return Err(EvolutionGovernanceError::CanaryPrerequisiteRequired);
        }
        let review_id = format!(
            "evolution-review:{}:stable:{}",
            candidate.candidate_id, canary.generation
        );
        if let Ok(existing) = self.review(&review_id) {
            return Ok(existing);
        }
        let approval_id = format!("approval:{review_id}");
        let observation_ref = format!("evolution-canary-observation:{}", observation.report_id);
        let review = ReleaseChangeReview {
            review_id: review_id.clone(),
            approval_id: approval_id.clone(),
            class: ReleaseChangeReviewClass::Stable,
            action: ReleaseChangeAction::PromoteStable,
            candidate_id: Some(candidate.candidate_id.clone()),
            subject: candidate.subject.clone(),
            baseline_revision: Some(candidate.baseline_revision),
            candidate_revision: subject_revision(&candidate.subject),
            comparison_report_ref: candidate.comparison_report_ref.clone(),
            comparison_report_digest: candidate.comparison_report_digest.clone(),
            prior_canary_approval_ref: Some(canary.approval_ref.clone()),
            active_canary_assignment_ref: Some(canary.assignment_id.clone()),
            observation_report_ref: Some(observation_ref),
            observation_report_digest: Some(observation.digest()),
            expected_selector: None,
            expected_generation: canary.generation,
            canary_policy: None,
            status: ReleaseChangeReviewStatus::Pending,
            created_at_ms: now_ms(),
        };
        self.append_review_with_approval(&candidate, review.clone())?;
        Ok(review)
    }

    /// Request an explicitly human-governed non-candidate release operation.
    /// Candidate rollout actions deliberately remain unavailable here: those
    /// must pass the immutable comparison -> Canary observation chain above.
    pub(crate) fn request_release_change(
        &self,
        request: ReleaseChangeRequest,
    ) -> Result<ReleaseChangeReview, EvolutionGovernanceError> {
        validate_release_change_request(&request)?;
        if request.action == ReleaseChangeAction::StopCanary {
            let candidate_id = request.candidate_id.as_deref().unwrap_or_default();
            let candidate = self.candidate(candidate_id)?;
            if candidate.subject != request.subject {
                return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                    "stop canary subject does not match the candidate".to_string(),
                ));
            }
            self.active_canary_assignment(&candidate)?;
        }
        if let Some(candidate_id) = request.candidate_id.as_deref() {
            let candidate = self.candidate(candidate_id)?;
            if candidate.subject != request.subject {
                return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                    "candidate does not own the requested release subject".to_string(),
                ));
            }
        }

        let review_id = format!("evolution-review:manual:{}", request.request_id);
        if let Ok(existing) = self.review(&review_id) {
            return Ok(existing);
        }
        let generation = self.current_release_generation(&request.subject)?;
        let review = ReleaseChangeReview {
            review_id: review_id.clone(),
            approval_id: format!("approval:{review_id}"),
            class: match request.action {
                ReleaseChangeAction::Rollback => ReleaseChangeReviewClass::Rollback,
                ReleaseChangeAction::SetDefaultLatest
                | ReleaseChangeAction::SetDefaultExact
                | ReleaseChangeAction::StopCanary => ReleaseChangeReviewClass::Pointer,
                ReleaseChangeAction::PromoteCanary | ReleaseChangeAction::PromoteStable => {
                    return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                        "candidate rollout actions require the evaluation and canary workflow"
                            .to_string(),
                    ));
                }
            },
            action: request.action,
            candidate_id: request.candidate_id,
            subject: request.subject,
            baseline_revision: None,
            candidate_revision: None,
            comparison_report_ref: None,
            comparison_report_digest: None,
            prior_canary_approval_ref: None,
            active_canary_assignment_ref: None,
            observation_report_ref: None,
            observation_report_digest: None,
            expected_selector: request.selector,
            expected_generation: generation,
            canary_policy: None,
            status: ReleaseChangeReviewStatus::Pending,
            created_at_ms: now_ms(),
        };
        self.append_manual_review_with_approval(
            &review,
            approval_evidence_refs(&request.evidence_refs),
        )?;
        Ok(review)
    }

    fn append_review_with_approval(
        &self,
        candidate: &EvolutionGovernanceCandidate,
        review: ReleaseChangeReview,
    ) -> Result<(), EvolutionGovernanceError> {
        let approval = pending_evolution_approval(
            review.approval_id.clone(),
            ApprovalSource {
                kind: ApprovalSourceKind::Evolution,
                session_id: None,
                agent_id: None,
                team_id: None,
                mission_id: Some(candidate.candidate_id.clone()),
                resource_ref: None,
                review_ref: None,
                application: None,
            },
            release_action_key(review.action).to_string(),
            format!("Promote {} to Stable", candidate.subject.subject_ref()),
            approval_evidence_refs(&candidate.source_evidence_refs),
        );
        let approval_stream = format!("approval:{}", approval.approval_id);
        let review_stream = review_stream(&review.review_id);
        let candidate_stream = candidate_stream(&candidate.candidate_id);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("evolution-review-create:{}", review.review_id),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: approval_stream.clone(),
                        expected_revision: self.stream_revision(&approval_stream)?,
                    },
                    ExpectedStreamRevision {
                        stream_id: review_stream.clone(),
                        expected_revision: self.stream_revision(&review_stream)?,
                    },
                    ExpectedStreamRevision {
                        stream_id: candidate_stream.clone(),
                        expected_revision: self.stream_revision(&candidate_stream)?,
                    },
                ],
                events: vec![
                    RuntimeEventInput {
                        stream_id: approval_stream,
                        scope: RuntimeEventScope::Approval,
                        kind: "approval.submitted".to_string(),
                        status: Some("pending".to_string()),
                        actor: Some("runtime.evolution_governance".to_string()),
                        refs: vec![subject_ref(&candidate.subject)],
                        payload: serde_json::json!({
                            "request": approval,
                            "action": release_action_key(review.action),
                            "summary": format!("Promote {} to Stable", candidate.subject.subject_ref()),
                            "risk": TaskRisk::High,
                            "timeout_policy": ApprovalTimeoutPolicy::Pending,
                        }),
                    }
                    .into(),
                    event(
                        review_stream,
                        "evolution.release_review.requested",
                        Some("pending"),
                        vec![subject_ref(&review.subject)],
                        serde_json::json!({"review": review}),
                    ),
                    event(
                        candidate_stream,
                        "evolution.candidate.stable_review_linked",
                        Some("evaluated_eligible"),
                        vec![subject_ref(&candidate.subject)],
                        serde_json::json!({"review_id": review.review_id}),
                    ),
                ],
            })
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(())
    }

    fn append_manual_review_with_approval(
        &self,
        review: &ReleaseChangeReview,
        evidence_refs: Vec<String>,
    ) -> Result<(), EvolutionGovernanceError> {
        let subject_ref = subject_ref(&review.subject);
        let approval = pending_evolution_approval(
            review.approval_id.clone(),
            ApprovalSource {
                kind: ApprovalSourceKind::Evolution,
                session_id: None,
                agent_id: None,
                team_id: None,
                mission_id: review.candidate_id.clone(),
                resource_ref: None,
                review_ref: None,
                application: None,
            },
            release_action_key(review.action).to_string(),
            release_change_summary(review),
            evidence_refs,
        );
        let approval_stream = format!("approval:{}", approval.approval_id);
        let review_stream = review_stream(&review.review_id);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("evolution-review-create:{}", review.review_id),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: approval_stream.clone(),
                        expected_revision: self.stream_revision(&approval_stream)?,
                    },
                    ExpectedStreamRevision {
                        stream_id: review_stream.clone(),
                        expected_revision: self.stream_revision(&review_stream)?,
                    },
                ],
                events: vec![
                    RuntimeEventInput {
                        stream_id: approval_stream,
                        scope: RuntimeEventScope::Approval,
                        kind: "approval.submitted".to_string(),
                        status: Some("pending".to_string()),
                        actor: Some("runtime.evolution_governance".to_string()),
                        refs: vec![subject_ref.clone()],
                        payload: serde_json::json!({
                            "request": approval,
                            "action": release_action_key(review.action),
                            "summary": release_change_summary(review),
                            "risk": TaskRisk::High,
                            "timeout_policy": ApprovalTimeoutPolicy::Pending,
                        }),
                    }
                    .into(),
                    event(
                        review_stream,
                        "evolution.release_review.requested",
                        Some("pending"),
                        vec![subject_ref],
                        serde_json::json!({"review": review}),
                    ),
                ],
            })
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(())
    }

    fn active_canary_assignment(
        &self,
        candidate: &EvolutionGovernanceCandidate,
    ) -> Result<EvolutionReleaseAssignment, EvolutionGovernanceError> {
        let mut assignments = self.release_assignments()?;
        assignments.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.assignment_id.cmp(&right.assignment_id))
        });
        let mut active = None;
        for assignment in assignments {
            if assignment.candidate_id.as_deref() != Some(candidate.candidate_id.as_str())
                || assignment.subject.release_target_ref() != candidate.subject.release_target_ref()
            {
                continue;
            }
            match assignment.action {
                ReleaseChangeAction::PromoteCanary => active = Some(assignment),
                // A later stop is a revocation of the active Canary routing
                // assignment, not merely another historical release event.
                // Treat it as terminal even when the stable candidate itself
                // remains available as an immutable revision.
                ReleaseChangeAction::StopCanary => active = None,
                _ => {}
            }
        }
        active.ok_or(EvolutionGovernanceError::CanaryPrerequisiteRequired)
    }

    /// Deterministically select an active Canary for a normal Runtime
    /// binding. Exact revision pins deliberately bypass experimentation; only
    /// latest/default traffic can enter a candidate release. The assignment
    /// is returned rather than applied here so the Binding compiler records
    /// the immutable provenance on the eventual run.
    pub(crate) fn select_agent_canary_assignment(
        &self,
        definition_id: &AgentDefinitionId,
        selector: &RevisionSelector,
        routing_identity: &str,
    ) -> Result<Option<EvolutionReleaseAssignment>, EvolutionGovernanceError> {
        if matches!(selector, RevisionSelector::ExactApprovedRevision { .. }) {
            return Ok(None);
        }
        let mut selected = Vec::new();
        for candidate in self.list_candidates()? {
            let EvolutionCandidateSubject::AgentDefinition { revision_ref } = &candidate.subject
            else {
                continue;
            };
            if &revision_ref.definition_id != definition_id {
                continue;
            }
            if let Ok(assignment) = self.active_canary_assignment(&candidate) {
                selected.push((candidate, assignment));
            }
        }
        if selected.len() > 1 {
            return Err(EvolutionGovernanceError::ActiveCanaryAlreadyExists);
        }
        let Some((candidate, assignment)) = selected.pop() else {
            return Ok(None);
        };
        let policy = assignment
            .canary_policy
            .as_ref()
            .ok_or(EvolutionGovernanceError::CanaryPrerequisiteRequired)?;
        let digest = Sha256::digest(format!(
            "{}|{}|{}|{}",
            definition_id.as_str(),
            subject_revision(&candidate.subject).unwrap_or_default(),
            assignment.assignment_id,
            routing_identity
        ));
        let bucket = u16::from_be_bytes([digest[0], digest[1]]) % 10_000;
        Ok((bucket < policy.traffic_basis_points).then_some(assignment))
    }

    /// Revalidate Canary provenance when an executable packet reaches the
    /// Agent runtime. A stale packet is rejected after StopCanary or a newer
    /// assignment generation, so old planning output cannot keep admitting
    /// new candidate traffic.
    pub(crate) fn validate_agent_canary_binding(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
        assignment_id: &str,
        generation: u64,
    ) -> Result<(), EvolutionGovernanceError> {
        for candidate in self.list_candidates()? {
            let EvolutionCandidateSubject::AgentDefinition {
                revision_ref: candidate_revision,
            } = &candidate.subject
            else {
                continue;
            };
            if candidate_revision != revision_ref {
                continue;
            }
            let assignment = self.active_canary_assignment(&candidate)?;
            if assignment.assignment_id == assignment_id && assignment.generation == generation {
                return Ok(());
            }
        }
        Err(EvolutionGovernanceError::CanaryPrerequisiteRequired)
    }

    /// Verify that an isolated evaluation packet is tied to a registered
    /// candidate and one of its immutable paired scenarios. This authorizes
    /// no release channel and does not change candidate lifecycle.
    pub(crate) fn validate_agent_evaluation_binding(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
        candidate_id: &str,
        scenario_ref: &str,
    ) -> Result<(), EvolutionGovernanceError> {
        let candidate = self.candidate(candidate_id)?;
        let EvolutionCandidateSubject::AgentDefinition {
            revision_ref: candidate_revision,
        } = &candidate.subject
        else {
            return Err(EvolutionGovernanceError::CanaryPrerequisiteRequired);
        };
        if candidate_revision != revision_ref
            || !candidate
                .evaluation_contract
                .scenario_refs
                .iter()
                .any(|configured| configured == scenario_ref)
            || matches!(
                candidate.lifecycle,
                EvolutionCandidateLifecycle::Withdrawn
                    | EvolutionCandidateLifecycle::Superseded
                    | EvolutionCandidateLifecycle::Archived
            )
        {
            return Err(EvolutionGovernanceError::CanaryPrerequisiteRequired);
        }
        Ok(())
    }

    /// Deterministically select a Team Template Canary using the exact same
    /// ledger and rollout policy as Agent Bindings. The Team graph compiler
    /// consumes the returned assignment and records its chosen revision
    /// before any graph is started; Gateway never performs this routing.
    pub(crate) fn select_team_canary_assignment(
        &self,
        template_id: &TeamTemplateDefinitionId,
        selector: &RevisionSelector,
        routing_identity: &str,
    ) -> Result<Option<EvolutionReleaseAssignment>, EvolutionGovernanceError> {
        if matches!(selector, RevisionSelector::ExactApprovedRevision { .. }) {
            return Ok(None);
        }
        let mut selected = Vec::new();
        for candidate in self.list_candidates()? {
            let EvolutionCandidateSubject::TeamTemplate { revision_ref } = &candidate.subject
            else {
                continue;
            };
            if &revision_ref.template_id != template_id {
                continue;
            }
            if let Ok(assignment) = self.active_canary_assignment(&candidate) {
                selected.push((candidate, assignment));
            }
        }
        if selected.len() > 1 {
            return Err(EvolutionGovernanceError::ActiveCanaryAlreadyExists);
        }
        let Some((candidate, assignment)) = selected.pop() else {
            return Ok(None);
        };
        let policy = assignment
            .canary_policy
            .as_ref()
            .ok_or(EvolutionGovernanceError::CanaryPrerequisiteRequired)?;
        let digest = Sha256::digest(format!(
            "{}|{}|{}|{}",
            template_id.as_str(),
            subject_revision(&candidate.subject).unwrap_or_default(),
            assignment.assignment_id,
            routing_identity
        ));
        let bucket = u16::from_be_bytes([digest[0], digest[1]]) % 10_000;
        Ok((bucket < policy.traffic_basis_points).then_some(assignment))
    }

    /// Fences Team graph admission after a `StopCanary` decision. A graph
    /// already started retains its immutable Template revision; only a newly
    /// admitted Team instantiation is rejected.
    pub(crate) fn validate_team_canary_binding(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
        assignment_id: &str,
        generation: u64,
    ) -> Result<(), EvolutionGovernanceError> {
        for candidate in self.list_candidates()? {
            let EvolutionCandidateSubject::TeamTemplate {
                revision_ref: candidate_revision,
            } = &candidate.subject
            else {
                continue;
            };
            if candidate_revision != revision_ref {
                continue;
            }
            let assignment = self.active_canary_assignment(&candidate)?;
            if assignment.assignment_id == assignment_id && assignment.generation == generation {
                return Ok(());
            }
        }
        Err(EvolutionGovernanceError::CanaryPrerequisiteRequired)
    }

    fn current_release_generation(
        &self,
        subject: &EvolutionCandidateSubject,
    ) -> Result<u64, EvolutionGovernanceError> {
        Ok(self
            .release_assignments()?
            .into_iter()
            .filter(|assignment| {
                assignment.subject.release_target_ref() == subject.release_target_ref()
            })
            .map(|assignment| assignment.generation)
            .max()
            .unwrap_or(0))
    }

    /// The policy floor is Runtime-owned and recovered from the evolution
    /// ledger.  A missing stream means the workspace has never changed its
    /// policy and therefore uses the immutable default.  Store failures are
    /// not silently accepted by a mutating command: every such command reads
    /// another governed stream in the same operation and will fail there.
    #[must_use]
    pub(crate) fn evaluation_policy_floor(&self) -> EvaluationPolicyFloor {
        self.event_store
            .list_stream(&evaluation_policy_stream())
            .ok()
            .and_then(materialize_evaluation_policy)
            .unwrap_or_default()
    }

    fn evaluation_policy_review(
        &self,
        review_id: &str,
    ) -> Result<Option<EvaluationPolicyChangeReview>, EvolutionGovernanceError> {
        let events = self
            .event_store
            .list_stream(&evaluation_policy_review_stream(review_id))
            .map_err(EvolutionGovernanceError::Store)?;
        Ok(materialize_evaluation_policy_review(events))
    }

    /// Review status is derived from the same release stream that guards a
    /// decision. A pending review whose expected generation no longer matches
    /// is visible as `superseded` without writing a second mutable status or
    /// letting a stale approval remain actionable.
    fn derive_review_status(
        &self,
        mut review: ReleaseChangeReview,
    ) -> Result<ReleaseChangeReview, EvolutionGovernanceError> {
        if review.status == ReleaseChangeReviewStatus::Pending
            && self.current_release_generation(&review.subject)? != review.expected_generation
        {
            review.status = ReleaseChangeReviewStatus::Superseded;
        }
        Ok(review)
    }

    pub(crate) fn review(
        &self,
        review_id: &str,
    ) -> Result<ReleaseChangeReview, EvolutionGovernanceError> {
        let events = self
            .event_store
            .list_stream(&review_stream(review_id))
            .map_err(EvolutionGovernanceError::Store)?;
        let review = materialize_review(events)
            .ok_or_else(|| EvolutionGovernanceError::ReviewNotFound(review_id.to_string()))?;
        self.derive_review_status(review)
    }

    pub(crate) fn decide_review(
        &self,
        principal: &VerifiedPrincipal,
        lease: &VerifiedDecisionLease,
        review_id: &str,
        decision: ReleaseChangeReviewDecision,
        reason: String,
    ) -> Result<Option<EvolutionReleaseAssignment>, EvolutionGovernanceError> {
        if !principal.is_human_interactive()
            || !principal.has_capability("evolution.release.manage")
        {
            return Err(EvolutionGovernanceError::HumanCapabilityRequired);
        }
        let review = self.review(review_id)?;
        if review.status != ReleaseChangeReviewStatus::Pending {
            return Err(EvolutionGovernanceError::ReviewNotPending);
        }
        let evidence_digest = review_digest(&review);
        if lease.review_id() != review.review_id
            || lease.action() != release_action_key(review.action)
            || lease.scope() != review.subject.scope_ref()
            || lease.evidence_digest() != evidence_digest
        {
            return Err(EvolutionGovernanceError::HumanCapabilityRequired);
        }
        let approval = self
            .approvals
            .get(&review.approval_id)
            .ok_or_else(|| EvolutionGovernanceError::ReviewNotFound(review.approval_id.clone()))?;
        if approval.status != GlobalApprovalStatus::Pending {
            return Err(EvolutionGovernanceError::ReviewNotPending);
        }
        if self.current_release_generation(&review.subject)? != review.expected_generation {
            return Err(EvolutionGovernanceError::ReleaseGenerationChanged);
        }
        if let Some(candidate_id) = review.candidate_id.as_deref() {
            let candidate = self.candidate(candidate_id)?;
            candidate
                .evaluation_policy_floor
                .validate_contract(&candidate.evaluation_contract)
                .map_err(|_| EvolutionGovernanceError::IneligibleReport)?;
            self.evaluation_policy_floor()
                .validate_contract(&candidate.evaluation_contract)
                .map_err(|_| EvolutionGovernanceError::IneligibleReport)?;
        }
        if decision == ReleaseChangeReviewDecision::Approve
            && review.action == ReleaseChangeAction::PromoteCanary
            && self
                .list_candidates()?
                .into_iter()
                .filter(|candidate| {
                    candidate.subject.release_target_ref() == review.subject.release_target_ref()
                })
                .any(|candidate| {
                    self.active_canary_assignment(&candidate)
                        .map(|assignment| assignment.candidate_id != review.candidate_id)
                        .unwrap_or(false)
                })
        {
            return Err(EvolutionGovernanceError::ActiveCanaryAlreadyExists);
        }
        let approved = decision == ReleaseChangeReviewDecision::Approve;
        let assignment = approved.then(|| EvolutionReleaseAssignment {
            assignment_id: format!("evolution-release:{}:{}", review.review_id, now_ms()),
            review_id: review.review_id.clone(),
            candidate_id: review.candidate_id.clone(),
            subject: review.subject.clone(),
            action: review.action,
            selector: review.expected_selector.clone(),
            generation: review.expected_generation.saturating_add(1),
            approval_ref: review.approval_id.clone(),
            canary_policy: (review.action == ReleaseChangeAction::PromoteCanary)
                .then(|| review.canary_policy.clone())
                .flatten(),
            created_at_ms: now_ms(),
        });
        let review_stream = review_stream(review_id);
        let approval_stream = format!("approval:{}", review.approval_id);
        let approval_revision = self.stream_revision(&approval_stream)?;
        let review_revision = self.stream_revision(&review_stream)?;
        let target_release_stream = release_stream(&review.subject);
        let target_release_revision = assignment
            .as_ref()
            .map(|_| self.stream_revision(&target_release_stream))
            .transpose()?;
        let decided_by = principal.claims().principal_id.clone();
        let resolved_at_ms = now_ms();
        let mut expected_streams = vec![
            ExpectedStreamRevision {
                stream_id: approval_stream.clone(),
                expected_revision: approval_revision,
            },
            ExpectedStreamRevision {
                stream_id: review_stream.clone(),
                expected_revision: review_revision,
            },
        ];
        if let Some(expected_revision) = target_release_revision {
            expected_streams.push(ExpectedStreamRevision {
                stream_id: target_release_stream.clone(),
                expected_revision,
            });
        }
        let mut events = vec![
            RuntimeEventInput {
                stream_id: approval_stream,
                scope: RuntimeEventScope::Approval,
                kind: "approval.decided".to_string(),
                status: Some(if approved { "approved" } else { "denied" }.to_string()),
                actor: Some(decided_by.clone()),
                refs: vec![subject_ref(&review.subject)],
                payload: serde_json::json!({
                    "approved": approved,
                    "reason": reason,
                    "message": if approved { format!("approved by {decided_by}") } else { format!("denied by {decided_by}") },
                    "resolved_at_ms": resolved_at_ms,
                }),
            }
            .into(),
            event(
                review_stream,
                "evolution.release_review.decided",
                Some(if approved { "approved" } else { "denied" }),
                vec![subject_ref(&review.subject)],
                serde_json::json!({"decision": decision, "reason": reason, "assignment": assignment}),
            ),
        ];
        if let Some(assignment) = assignment.as_ref() {
            events.push(event(
                target_release_stream,
                "evolution.release.assignment_authorized",
                Some("authorized"),
                vec![subject_ref(&review.subject)],
                serde_json::json!({"assignment": assignment}),
            ));
        }
        self.event_store
            .append_transaction_with_verified_decision_lease(
                AppendTransactionRequest {
                    transaction_id: format!("evolution-review-decide:{}:{:?}", review_id, decision),
                    expected_streams,
                    events,
                },
                lease,
            )
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))?;
        self.approvals.refresh();
        Ok(assignment)
    }

    fn stream_revision(&self, stream: &str) -> Result<u64, EvolutionGovernanceError> {
        self.event_store
            .stream_revision(stream)
            .map_err(|error| EvolutionGovernanceError::Store(error.to_string()))
    }
}

fn validate_report_against_contract(
    report: &EvolutionComparisonReportV2,
    contract: &EvaluationContract,
) -> Result<(), EvolutionGovernanceError> {
    let mut dimensions = BTreeMap::new();
    for dimension in &report.dimensions {
        if dimensions
            .insert(dimension.metric_id.as_str(), dimension)
            .is_some()
        {
            return Err(EvolutionGovernanceError::Store(
                "comparison report contains duplicate evaluation metrics".to_string(),
            ));
        }
    }
    if dimensions.len() != contract.metrics.len() {
        return Err(EvolutionGovernanceError::Store(
            "comparison report must include exactly the immutable contract metrics".to_string(),
        ));
    }
    for metric in &contract.metrics {
        let Some(dimension) = dimensions.get(metric.metric_id.as_str()) else {
            return Err(EvolutionGovernanceError::Store(format!(
                "comparison report omits contract metric `{}`",
                metric.metric_id
            )));
        };
        let margin_matches = (dimension.non_inferiority_margin - metric.non_inferiority_margin())
            .abs()
            <= f64::EPSILON;
        let confidence_matches =
            (dimension.minimum_confidence - metric.minimum_confidence()).abs() <= f64::EPSILON;
        let improvement_matches =
            (dimension.minimum_improvement - metric.minimum_improvement()).abs() <= f64::EPSILON;
        let superiority_confidence_matches = (dimension.minimum_superiority_confidence
            - metric.minimum_superiority_confidence())
        .abs()
            <= f64::EPSILON;
        if dimension.direction != metric.direction
            || !margin_matches
            || dimension.minimum_samples != metric.minimum_samples
            || !confidence_matches
            || !improvement_matches
            || !superiority_confidence_matches
            || dimension.hard_gate != metric.hard_gate
            || dimension.protected != metric.protected
            || dimension.target_improvement != metric.target_improvement
        {
            return Err(EvolutionGovernanceError::Store(format!(
                "comparison report weakens or relabels immutable metric `{}`",
                metric.metric_id
            )));
        }
    }
    Ok(())
}

fn materialize_candidate(
    mut events: Vec<crate::DurableRuntimeEvent>,
) -> Option<EvolutionGovernanceCandidate> {
    events.sort_by_key(|event| event.sequence);
    let mut candidate: Option<EvolutionGovernanceCandidate> = None;
    for event in events {
        match event.kind.as_str() {
            "evolution.candidate.created" => {
                candidate = event
                    .payload
                    .get("candidate")
                    .and_then(|value| serde_json::from_value(value.clone()).ok());
            }
            "evolution.comparison.recorded" => {
                if let Some(current) = candidate.as_mut() {
                    current.lifecycle = if event
                        .payload
                        .get("eligible")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                    {
                        EvolutionCandidateLifecycle::EvaluatedEligible
                    } else {
                        EvolutionCandidateLifecycle::EvaluatedIneligible
                    };
                    current.comparison_report_ref = event
                        .payload
                        .get("report_ref")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    current.comparison_report_digest = event
                        .payload
                        .get("report_digest")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    current.updated_at_ms = event.created_at_ms;
                }
            }
            "evolution.candidate.evaluation_blocked.v1" => {
                if let Some(current) = candidate.as_mut() {
                    current.lifecycle = EvolutionCandidateLifecycle::EvaluationBlocked;
                    current.updated_at_ms = event.created_at_ms;
                }
            }
            "evolution.candidate.canary_review_linked" => {
                if let Some(current) = candidate.as_mut() {
                    current.canary_review_ref = event
                        .payload
                        .get("review_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    current.updated_at_ms = event.created_at_ms;
                }
            }
            "evolution.canary.observation.recorded" => {
                if let Some(current) = candidate.as_mut() {
                    current.canary_observation = event
                        .payload
                        .get("observation")
                        .and_then(|value| serde_json::from_value(value.clone()).ok());
                    current.updated_at_ms = event.created_at_ms;
                }
            }
            "evolution.candidate.stable_review_linked" => {
                if let Some(current) = candidate.as_mut() {
                    current.stable_review_ref = event
                        .payload
                        .get("review_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    current.updated_at_ms = event.created_at_ms;
                }
            }
            _ => {}
        }
    }
    candidate
}

fn materialize_review(mut events: Vec<crate::DurableRuntimeEvent>) -> Option<ReleaseChangeReview> {
    events.sort_by_key(|event| event.sequence);
    let mut review: Option<ReleaseChangeReview> = None;
    for event in events {
        match event.kind.as_str() {
            "evolution.release_review.requested" => {
                review = event
                    .payload
                    .get("review")
                    .and_then(|value| serde_json::from_value(value.clone()).ok());
            }
            "evolution.release_review.decided" => {
                if let Some(current) = review.as_mut() {
                    current.status = match event.status.as_deref() {
                        Some("approved") => ReleaseChangeReviewStatus::Approved,
                        Some("denied") => ReleaseChangeReviewStatus::Denied,
                        _ => current.status,
                    };
                }
            }
            _ => {}
        }
    }
    review
}

fn materialize_evaluation_policy_review(
    mut events: Vec<crate::DurableRuntimeEvent>,
) -> Option<EvaluationPolicyChangeReview> {
    events.sort_by_key(|event| event.sequence);
    let mut review: Option<EvaluationPolicyChangeReview> = None;
    for event in events {
        match event.kind.as_str() {
            "evolution.evaluation_policy_review.requested" => {
                review = event
                    .payload
                    .get("review")
                    .and_then(|value| serde_json::from_value(value.clone()).ok());
            }
            "evolution.evaluation_policy_review.decided" => {
                if let Some(current) = review.as_mut() {
                    current.status = match event.status.as_deref() {
                        Some("approved") => ReleaseChangeReviewStatus::Approved,
                        Some("denied") => ReleaseChangeReviewStatus::Denied,
                        _ => current.status,
                    };
                }
            }
            _ => {}
        }
    }
    review
}

fn materialize_evaluation_policy(
    mut events: Vec<crate::DurableRuntimeEvent>,
) -> Option<EvaluationPolicyFloor> {
    events.sort_by_key(|event| event.sequence);
    events.into_iter().rev().find_map(|event| {
        (event.kind == "evolution.evaluation_policy.updated")
            .then(|| event.payload.get("policy"))
            .flatten()
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    })
}

fn candidate_stream(candidate_id: &str) -> String {
    format!("evolution:candidate:{candidate_id}")
}
fn review_stream(review_id: &str) -> String {
    format!("evolution:review:{review_id}")
}
fn evaluation_policy_stream() -> String {
    "evolution:evaluation-policy".to_string()
}
fn evaluation_policy_review_stream(review_id: &str) -> String {
    format!("evolution:evaluation-policy-review:{review_id}")
}
fn evaluation_policy_scope(policy: &EvaluationPolicyFloor) -> String {
    policy.policy_id.clone()
}
fn evaluation_policy_action_key() -> &'static str {
    "evolution.evaluation_policy.change"
}
fn release_stream(subject: &EvolutionCandidateSubject) -> String {
    format!("evolution:release:{}", subject.release_target_ref())
}
fn subject_ref(subject: &EvolutionCandidateSubject) -> RuntimeEventRef {
    RuntimeEventRef {
        kind: "evolution_subject".to_string(),
        id: subject.subject_ref(),
    }
}
fn subject_revision(subject: &EvolutionCandidateSubject) -> Option<u64> {
    Some(match subject {
        EvolutionCandidateSubject::AgentDefinition { revision_ref } => revision_ref.revision,
        EvolutionCandidateSubject::TeamTemplate { revision_ref } => revision_ref.revision,
    })
}
fn event(
    stream_id: String,
    kind: &str,
    status: Option<&str>,
    refs: Vec<RuntimeEventRef>,
    payload: serde_json::Value,
) -> RuntimeTransactionEventInput {
    RuntimeEventInput {
        stream_id,
        scope: RuntimeEventScope::Evolution,
        kind: kind.to_string(),
        status: status.map(str::to_string),
        actor: Some("runtime.evolution_governance".to_string()),
        refs,
        payload,
    }
    .into()
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn approval_evidence_refs(evidence_refs: &[EvidenceRef]) -> Vec<String> {
    evidence_refs
        .iter()
        .map(|evidence| format!("{}:{}", evidence.ref_type, evidence.id))
        .collect()
}

fn release_action_key(action: ReleaseChangeAction) -> &'static str {
    match action {
        ReleaseChangeAction::PromoteCanary => "evolution.release.promote_canary",
        ReleaseChangeAction::PromoteStable => "evolution.release.promote_stable",
        ReleaseChangeAction::SetDefaultLatest => "evolution.release.set_default_latest",
        ReleaseChangeAction::SetDefaultExact => "evolution.release.set_default_exact",
        ReleaseChangeAction::Rollback => "evolution.release.rollback",
        ReleaseChangeAction::StopCanary => "evolution.release.stop_canary",
    }
}

fn validate_release_change_request(
    request: &ReleaseChangeRequest,
) -> Result<(), EvolutionGovernanceError> {
    if request.request_id.trim().is_empty() {
        return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
            "request id is required".to_string(),
        ));
    }
    if request.evidence_refs.is_empty() {
        return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
            "evidence refs are required".to_string(),
        ));
    }
    match request.action {
        ReleaseChangeAction::SetDefaultExact | ReleaseChangeAction::Rollback => {
            if !matches!(
                request.selector,
                Some(RevisionSelector::ExactApprovedRevision { .. })
            ) {
                return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                    "exact default and rollback require an exact approved revision selector"
                        .to_string(),
                ));
            }
        }
        ReleaseChangeAction::SetDefaultLatest => {
            if request.selector.is_some() {
                return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                    "latest changes do not accept a selector".to_string(),
                ));
            }
        }
        ReleaseChangeAction::StopCanary => {
            if request.candidate_id.as_deref().is_none_or(str::is_empty) {
                return Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(
                    "stop canary requires its candidate id".to_string(),
                ));
            }
        }
        ReleaseChangeAction::PromoteCanary | ReleaseChangeAction::PromoteStable => {}
    }
    Ok(())
}

fn release_change_summary(review: &ReleaseChangeReview) -> String {
    match review.action {
        ReleaseChangeAction::SetDefaultLatest => {
            format!(
                "Set {} default to latest approved Stable",
                review.subject.subject_ref()
            )
        }
        ReleaseChangeAction::SetDefaultExact => format!(
            "Set {} default to an exact approved revision",
            review.subject.subject_ref()
        ),
        ReleaseChangeAction::Rollback => {
            format!("Rollback {} default pointer", review.subject.subject_ref())
        }
        ReleaseChangeAction::StopCanary => {
            format!("Stop Canary for {}", review.subject.subject_ref())
        }
        ReleaseChangeAction::PromoteCanary => {
            format!("Promote {} to Canary", review.subject.subject_ref())
        }
        ReleaseChangeAction::PromoteStable => {
            format!("Promote {} to Stable", review.subject.subject_ref())
        }
    }
}

fn review_digest(review: &ReleaseChangeReview) -> String {
    let value = serde_json::to_vec(review).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(value))
}

fn evaluation_policy_review_digest(review: &EvaluationPolicyChangeReview) -> String {
    let value = serde_json::to_vec(review).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent::{AgentDefinitionId, DefinitionScope};

    fn service() -> EvolutionGovernanceService {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        EvolutionGovernanceService::new(Arc::clone(&store), Arc::new(ApprovalQueue::new(store)))
    }

    fn candidate() -> EvolutionGovernanceCandidate {
        EvolutionGovernanceCandidate {
            candidate_id: "agent-candidate-v2".to_string(),
            proposal_id: "proposal-agent-v2".to_string(),
            subject: EvolutionCandidateSubject::AgentDefinition {
                revision_ref: AgentDefinitionRevisionRef::new(
                    AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/researcher").unwrap(),
                    2,
                )
                .unwrap(),
            },
            baseline_revision: 1,
            evaluation_contract: harness_contract::evaluation::EvaluationContract {
                scenario_refs: vec!["evolution/protected".to_string()],
                metrics: vec![
                    harness_contract::evaluation::EvaluationMetricSpec {
                        metric_id: "policy_compliance".to_string(),
                        source: harness_contract::evaluation::EvaluationMetricSource::TaskSuccess,
                        unit: "normalized_score".to_string(),
                        direction: EvaluationDirection::HigherIsBetter,
                        non_inferiority_margin_micros: 0,
                        minimum_samples: 10,
                        minimum_confidence_basis_points: 9_500,
                        minimum_improvement_micros: 10_000,
                        minimum_superiority_confidence_basis_points: 9_500,
                        hard_gate: true,
                        protected: true,
                        target_improvement: false,
                        missing_value_policy: harness_contract::evaluation::EvaluationMissingValuePolicy::FailClosed,
                        paired_scenario_refs: vec!["evolution/protected".to_string()],
                        multiplicity_correction: harness_contract::evaluation::EvaluationMultiplicityCorrection::BenjaminiHochberg,
                        stopping_rule: harness_contract::evaluation::EvaluationStoppingRule::FixedSamples,
                    },
                    harness_contract::evaluation::EvaluationMetricSpec {
                        metric_id: "task_success".to_string(),
                        source: harness_contract::evaluation::EvaluationMetricSource::TaskSuccess,
                        unit: "normalized_score".to_string(),
                        direction: EvaluationDirection::HigherIsBetter,
                        non_inferiority_margin_micros: 10_000,
                        minimum_samples: 10,
                        minimum_confidence_basis_points: 9_500,
                        minimum_improvement_micros: 10_000,
                        minimum_superiority_confidence_basis_points: 9_500,
                        hard_gate: false,
                        protected: false,
                        target_improvement: true,
                        missing_value_policy: harness_contract::evaluation::EvaluationMissingValuePolicy::FailClosed,
                        paired_scenario_refs: vec!["evolution/protected".to_string()],
                        multiplicity_correction: harness_contract::evaluation::EvaluationMultiplicityCorrection::BenjaminiHochberg,
                        stopping_rule: harness_contract::evaluation::EvaluationStoppingRule::FixedSamples,
                    },
                ],
            },
            evaluation_policy_floor: EvaluationPolicyFloor::default(),
            evaluation_scenario_digest: "sha256:test-scenarios".to_string(),
            source_evidence_refs: vec![EvidenceRef::observed("agent_run", "baseline-1")],
            canary_policy: CanaryRolloutPolicy {
                traffic_basis_points: 1_000,
                minimum_samples: 10,
                minimum_duration_ms: 60_000,
                maximum_duration_ms: 600_000,
            },
            lifecycle: EvolutionCandidateLifecycle::Draft,
            comparison_report_ref: None,
            comparison_report_digest: None,
            canary_review_ref: None,
            stable_review_ref: None,
            canary_observation: None,
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
        }
    }

    fn eligible_report() -> EvolutionComparisonReportV2 {
        EvolutionComparisonReportV2 {
            report_id: "report-agent-v2".to_string(),
            candidate_id: "agent-candidate-v2".to_string(),
            evaluation_contract_digest: candidate().evaluation_contract_digest(),
            evaluation_policy_digest: candidate().evaluation_policy_floor.digest(),
            evaluation_scenario_digest: candidate().evaluation_scenario_digest,
            subject_ref: candidate().subject.subject_ref(),
            environment_fingerprint: "sha256:test-environment".to_string(),
            stopping_reason: EvaluationStoppingReason::FixedSamplesCompleted,
            executed_sample_count: 20,
            dimensions: vec![
                EvolutionComparisonDimension {
                    metric_id: "policy_compliance".to_string(),
                    direction: EvaluationDirection::HigherIsBetter,
                    baseline: 0.99,
                    candidate: 0.99,
                    non_inferiority_margin: 0.0,
                    sample_count: 20,
                    minimum_samples: 10,
                    confidence: 0.99,
                    minimum_confidence: 0.95,
                    minimum_improvement: 0.01,
                    superiority_confidence: 0.99,
                    minimum_superiority_confidence: 0.95,
                    hard_gate: true,
                    protected: true,
                    target_improvement: false,
                },
                EvolutionComparisonDimension {
                    metric_id: "task_success".to_string(),
                    direction: EvaluationDirection::HigherIsBetter,
                    baseline: 0.70,
                    candidate: 0.82,
                    non_inferiority_margin: 0.01,
                    sample_count: 20,
                    minimum_samples: 10,
                    confidence: 0.99,
                    minimum_confidence: 0.95,
                    minimum_improvement: 0.01,
                    superiority_confidence: 0.99,
                    minimum_superiority_confidence: 0.95,
                    hard_gate: false,
                    protected: false,
                    target_improvement: true,
                },
            ],
            source_run_refs: vec!["eval:paired-1".to_string()],
            evidence_refs: vec![EvidenceRef::observed("evaluation", "paired-1")],
            created_at_ms: now_ms(),
        }
    }

    fn team_candidate() -> EvolutionGovernanceCandidate {
        let mut candidate = candidate();
        candidate.candidate_id = "team-candidate-v2".to_string();
        candidate.subject = EvolutionCandidateSubject::TeamTemplate {
            revision_ref: TeamTemplateRevisionRef::new(
                TeamTemplateDefinitionId::try_from("workspace/cowd/research-team").unwrap(),
                2,
            )
            .unwrap(),
        };
        candidate.canary_policy.traffic_basis_points = 10_000;
        candidate
    }

    #[test]
    fn only_eligible_immutable_report_can_create_human_canary_review() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        let candidate = service
            .record_comparison(eligible_report())
            .expect("comparison");
        assert_eq!(
            candidate.lifecycle,
            EvolutionCandidateLifecycle::EvaluatedEligible
        );
        let review = service
            .request_canary_review(&candidate.candidate_id)
            .expect("review");
        assert_eq!(review.action, ReleaseChangeAction::PromoteCanary);
        assert_eq!(review.status, ReleaseChangeReviewStatus::Pending);
        assert!(service.approvals.get(&review.approval_id).is_some());
    }

    #[test]
    fn target_metric_requires_business_effect_and_independent_superiority() {
        let mut too_small = eligible_report();
        too_small.dimensions[1].candidate = too_small.dimensions[1].baseline + 0.005;
        assert!(!too_small.is_eligible());

        let mut statistically_weak = eligible_report();
        statistically_weak.dimensions[1].superiority_confidence = 0.94;
        assert!(!statistically_weak.is_eligible());

        assert!(eligible_report().is_eligible());
    }

    #[test]
    fn comparison_report_is_bound_to_policy_scenario_subject_and_environment() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        for field in ["policy", "scenario", "subject", "environment"] {
            let mut report = eligible_report();
            match field {
                "policy" => report.evaluation_policy_digest = "sha256:wrong".to_string(),
                "scenario" => report.evaluation_scenario_digest = "sha256:wrong".to_string(),
                "subject" => report.subject_ref = "agent-definition:wrong@2".to_string(),
                "environment" => report.environment_fingerprint.clear(),
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    service.record_comparison(report),
                    Err(EvolutionGovernanceError::IneligibleReport)
                ),
                "{field} binding must fail closed"
            );
        }
    }

    #[test]
    fn direct_candidate_cannot_bypass_evaluation_to_release_review() {
        let service = service();
        let candidate = service.create_candidate(candidate()).expect("candidate");
        assert!(matches!(
            service.request_canary_review(&candidate.candidate_id),
            Err(EvolutionGovernanceError::CandidateNotEligible)
        ));
    }

    #[test]
    fn candidate_registration_is_idempotent_but_rejects_immutable_identity_conflicts() {
        let service = service();
        let candidate = candidate();
        let registration = EvolutionCandidateRegistration {
            candidate_id: candidate.candidate_id,
            proposal_id: candidate.proposal_id,
            subject: candidate.subject,
            baseline_revision: candidate.baseline_revision,
            evaluation_contract: candidate.evaluation_contract,
            evaluation_scenario_digest: candidate.evaluation_scenario_digest,
            source_evidence_refs: candidate.source_evidence_refs,
            canary_policy: candidate.canary_policy,
        };
        let first = service
            .register_candidate(registration.clone())
            .expect("first registration");
        let replay = service
            .register_candidate(registration.clone())
            .expect("idempotent replay");
        assert_eq!(first, replay);

        let mut conflicting = registration;
        conflicting.proposal_id = "proposal-agent-v3".to_string();
        assert!(matches!(
            service.register_candidate(conflicting),
            Err(EvolutionGovernanceError::Store(message))
                if message.contains("different immutable inputs")
        ));
    }

    #[test]
    fn comparison_cannot_omit_or_duplicate_runtime_protected_dimensions() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        let mut omitted = eligible_report();
        omitted
            .dimensions
            .retain(|dimension| dimension.metric_id != "policy_compliance");
        assert!(matches!(
            service.record_comparison(omitted),
            Err(EvolutionGovernanceError::Store(message))
                if message.contains("exactly the immutable contract metrics")
        ));

        let mut duplicate = eligible_report();
        duplicate.dimensions.push(duplicate.dimensions[0].clone());
        assert!(matches!(
            service.record_comparison(duplicate),
            Err(EvolutionGovernanceError::Store(message))
                if message.contains("duplicate evaluation metrics")
        ));
    }

    #[test]
    fn candidate_contract_below_its_runtime_policy_floor_is_rejected() {
        let service = service();
        let mut candidate = candidate();
        candidate.evaluation_policy_floor.minimum_samples = 11;
        assert!(matches!(
            service.create_candidate(candidate),
            Err(EvolutionGovernanceError::Store(message))
                if message.contains("below the active policy floor")
        ));
    }

    #[test]
    fn policy_floor_change_requires_its_own_human_lease_and_becomes_event_sourced() {
        let service = service();
        let mut next_policy = EvaluationPolicyFloor::default();
        next_policy.revision = 2;
        next_policy.minimum_samples = 12;
        let review = service
            .request_evaluation_policy_change(EvaluationPolicyChangeIntent {
                request_id: "raise-minimum-samples".to_string(),
                next_policy: next_policy.clone(),
                evidence_refs: vec![EvidenceRef::observed("audit", "evaluation-policy-2026-07")],
            })
            .expect("policy review");
        assert_eq!(service.evaluation_policy_floor().revision, 1);
        let principal = crate::security::test_human_interactive_principal();
        let lease = crate::security::test_verified_decision_lease(
            &review.review_id,
            review.action_key(),
            review.scope_ref(),
            review.evidence_digest(),
        );
        let applied = service
            .decide_evaluation_policy_change(
                &principal,
                &lease,
                &review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "raise paired confidence evidence floor".to_string(),
            )
            .expect("policy decision")
            .expect("approved policy");
        assert_eq!(applied, next_policy);
        assert_eq!(service.evaluation_policy_floor(), next_policy);
        assert_eq!(
            service.list_evaluation_policy_reviews().expect("reviews")[0].status,
            ReleaseChangeReviewStatus::Approved
        );
        assert!(matches!(
            service.create_candidate(candidate()),
            Err(EvolutionGovernanceError::Store(message))
                if message.contains("below the active policy floor")
        ));
    }

    #[test]
    fn policy_floor_change_cannot_jump_revisions_or_bypass_human_decision() {
        let service = service();
        let mut skipped = EvaluationPolicyFloor::default();
        skipped.revision = 3;
        assert!(matches!(
            service.request_evaluation_policy_change(EvaluationPolicyChangeIntent {
                request_id: "invalid-policy-jump".to_string(),
                next_policy: skipped,
                evidence_refs: vec![EvidenceRef::observed("audit", "invalid")],
            }),
            Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(_))
        ));

        let mut next_policy = EvaluationPolicyFloor::default();
        next_policy.revision = 2;
        let review = service
            .request_evaluation_policy_change(EvaluationPolicyChangeIntent {
                request_id: "policy-denied-with-wrong-lease".to_string(),
                next_policy,
                evidence_refs: vec![EvidenceRef::observed("audit", "policy")],
            })
            .expect("review");
        let principal = crate::security::test_human_interactive_principal();
        let wrong_lease = crate::security::test_verified_decision_lease(
            &review.review_id,
            "evolution.release.promote_canary",
            review.scope_ref(),
            review.evidence_digest(),
        );
        assert!(matches!(
            service.decide_evaluation_policy_change(
                &principal,
                &wrong_lease,
                &review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "wrong action lease".to_string(),
            ),
            Err(EvolutionGovernanceError::HumanCapabilityRequired)
        ));
        assert_eq!(service.evaluation_policy_floor().revision, 1);
    }

    #[test]
    fn release_decision_commits_approval_and_review_under_one_verified_lease() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        let candidate = service
            .record_comparison(eligible_report())
            .expect("comparison");
        let review = service
            .request_canary_review(&candidate.candidate_id)
            .expect("review");
        let principal = crate::security::test_human_interactive_principal();
        let lease = crate::security::test_verified_decision_lease(
            &review.review_id,
            release_action_key(review.action),
            review.subject.scope_ref(),
            review_digest(&review),
        );
        let assignment = service
            .decide_review(
                &principal,
                &lease,
                &review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "verified canary rollout".to_string(),
            )
            .expect("decision")
            .expect("approved assignment");
        assert_eq!(assignment.action, ReleaseChangeAction::PromoteCanary);
        assert_eq!(
            service.review(&review.review_id).unwrap().status,
            ReleaseChangeReviewStatus::Approved
        );
        assert_eq!(
            service.approvals.get(&review.approval_id).unwrap().status,
            GlobalApprovalStatus::Approved
        );
    }

    #[test]
    fn stable_review_requires_an_active_canary_and_qualified_observation() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        let candidate = service
            .record_comparison(eligible_report())
            .expect("comparison");
        assert!(matches!(
            service.request_stable_review(&candidate.candidate_id),
            Err(EvolutionGovernanceError::CanaryPrerequisiteRequired)
        ));

        let canary_review = service
            .request_canary_review(&candidate.candidate_id)
            .expect("canary review");
        let principal = crate::security::test_human_interactive_principal();
        let canary_lease = crate::security::test_verified_decision_lease(
            &canary_review.review_id,
            release_action_key(canary_review.action),
            canary_review.subject.scope_ref(),
            review_digest(&canary_review),
        );
        let canary = service
            .decide_review(
                &principal,
                &canary_lease,
                &canary_review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "enter canary".to_string(),
            )
            .expect("canary decision")
            .expect("canary assignment");

        let observation = CanaryObservationReport {
            report_id: "observation-v2".to_string(),
            candidate_id: candidate.candidate_id.clone(),
            canary_assignment_id: canary.assignment_id.clone(),
            generation: canary.generation,
            source_run_refs: vec!["agent-run:canary-1".to_string()],
            evidence_refs: vec![EvidenceRef::observed("evaluation", "canary-window")],
            sample_count: 40,
            minimum_samples: 10,
            observed_duration_ms: 120_000,
            minimum_duration_ms: 60_000,
            hard_gates_passed: true,
            protected_dimensions_noninferior: true,
            created_at_ms: now_ms(),
        };
        let candidate = service
            .record_canary_observation(observation)
            .expect("qualified observation");
        let stable = service
            .request_stable_review(&candidate.candidate_id)
            .expect("stable review");
        assert_eq!(stable.class, ReleaseChangeReviewClass::Stable);
        assert_eq!(stable.action, ReleaseChangeAction::PromoteStable);
        assert_eq!(
            stable.active_canary_assignment_ref.as_deref(),
            Some(canary.assignment_id.as_str())
        );
        assert!(stable.observation_report_digest.is_some());
    }

    #[test]
    fn stop_canary_invalidates_a_pending_stable_review_by_generation_fence() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        let candidate = service
            .record_comparison(eligible_report())
            .expect("comparison");
        let canary_review = service
            .request_canary_review(&candidate.candidate_id)
            .expect("canary review");
        let principal = crate::security::test_human_interactive_principal();
        let canary_lease = crate::security::test_verified_decision_lease(
            &canary_review.review_id,
            release_action_key(canary_review.action),
            canary_review.subject.scope_ref(),
            review_digest(&canary_review),
        );
        let canary = service
            .decide_review(
                &principal,
                &canary_lease,
                &canary_review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "enter canary".to_string(),
            )
            .expect("canary decision")
            .expect("canary assignment");
        let revision_ref = match &candidate.subject {
            EvolutionCandidateSubject::AgentDefinition { revision_ref } => revision_ref,
            EvolutionCandidateSubject::TeamTemplate { .. } => unreachable!("agent fixture"),
        };
        service
            .validate_agent_canary_binding(revision_ref, &canary.assignment_id, canary.generation)
            .expect("active canary binding is valid");
        service
            .record_canary_observation(CanaryObservationReport {
                report_id: "observation-stop-fence".to_string(),
                candidate_id: candidate.candidate_id.clone(),
                canary_assignment_id: canary.assignment_id.clone(),
                generation: canary.generation,
                source_run_refs: vec!["agent-run:canary-stop".to_string()],
                evidence_refs: vec![EvidenceRef::observed("evaluation", "canary-stop")],
                sample_count: 20,
                minimum_samples: 10,
                observed_duration_ms: 120_000,
                minimum_duration_ms: 60_000,
                hard_gates_passed: true,
                protected_dimensions_noninferior: true,
                created_at_ms: now_ms(),
            })
            .expect("observation");
        let stable = service
            .request_stable_review(&candidate.candidate_id)
            .expect("stable review");
        let stop = service
            .request_release_change(ReleaseChangeRequest {
                request_id: "stop-candidate-v2".to_string(),
                subject: candidate.subject.clone(),
                action: ReleaseChangeAction::StopCanary,
                selector: None,
                candidate_id: Some(candidate.candidate_id.clone()),
                evidence_refs: vec![EvidenceRef::observed("incident", "canary-regression")],
            })
            .expect("stop review");
        let stop_lease = crate::security::test_verified_decision_lease(
            &stop.review_id,
            release_action_key(stop.action),
            stop.subject.scope_ref(),
            review_digest(&stop),
        );
        service
            .decide_review(
                &principal,
                &stop_lease,
                &stop.review_id,
                ReleaseChangeReviewDecision::Approve,
                "stop deteriorating canary".to_string(),
            )
            .expect("stop decision");
        let stable_lease = crate::security::test_verified_decision_lease(
            &stable.review_id,
            release_action_key(stable.action),
            stable.subject.scope_ref(),
            review_digest(&stable),
        );
        assert!(matches!(
            service.decide_review(
                &principal,
                &stable_lease,
                &stable.review_id,
                ReleaseChangeReviewDecision::Approve,
                "must not promote stopped canary".to_string(),
            ),
            Err(EvolutionGovernanceError::ReleaseGenerationChanged)
                | Err(EvolutionGovernanceError::ReviewNotPending)
        ));
        assert!(matches!(
            service.active_canary_assignment(&candidate),
            Err(EvolutionGovernanceError::CanaryPrerequisiteRequired)
        ));
        assert!(matches!(
            service.validate_agent_canary_binding(
                revision_ref,
                &canary.assignment_id,
                canary.generation,
            ),
            Err(EvolutionGovernanceError::CanaryPrerequisiteRequired)
        ));
    }

    #[test]
    fn canary_observation_without_durable_evidence_cannot_open_stable_review() {
        let service = service();
        service.create_candidate(candidate()).expect("candidate");
        let candidate = service
            .record_comparison(eligible_report())
            .expect("comparison");
        let review = service
            .request_canary_review(&candidate.candidate_id)
            .expect("canary review");
        let principal = crate::security::test_human_interactive_principal();
        let lease = crate::security::test_verified_decision_lease(
            &review.review_id,
            release_action_key(review.action),
            review.subject.scope_ref(),
            review_digest(&review),
        );
        let assignment = service
            .decide_review(
                &principal,
                &lease,
                &review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "enter canary".to_string(),
            )
            .expect("canary decision")
            .expect("canary assignment");
        let recorded = service
            .record_canary_observation(CanaryObservationReport {
                report_id: "observation-without-evidence".to_string(),
                candidate_id: candidate.candidate_id.clone(),
                canary_assignment_id: assignment.assignment_id,
                generation: assignment.generation,
                source_run_refs: vec!["agent-run:canary-no-evidence".to_string()],
                evidence_refs: Vec::new(),
                sample_count: 10,
                minimum_samples: 10,
                observed_duration_ms: 60_000,
                minimum_duration_ms: 60_000,
                hard_gates_passed: true,
                protected_dimensions_noninferior: true,
                created_at_ms: now_ms(),
            })
            .expect("projection records the incomplete observation");
        assert!(matches!(
            service.request_stable_review(&recorded.candidate_id),
            Err(EvolutionGovernanceError::IneligibleCanaryObservation)
        ));
    }

    #[test]
    fn only_one_active_canary_is_allowed_for_an_agent_definition() {
        let service = service();
        let first = service
            .create_candidate(candidate())
            .expect("first candidate");
        service
            .record_comparison(eligible_report())
            .expect("first report");
        let first_review = service
            .request_canary_review(&first.candidate_id)
            .expect("first review");
        let principal = crate::security::test_human_interactive_principal();
        let first_lease = crate::security::test_verified_decision_lease(
            &first_review.review_id,
            release_action_key(first_review.action),
            first_review.subject.scope_ref(),
            review_digest(&first_review),
        );
        service
            .decide_review(
                &principal,
                &first_lease,
                &first_review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "first canary".to_string(),
            )
            .expect("first canary decision");

        let mut second = candidate();
        second.candidate_id = "agent-candidate-v3".to_string();
        second.subject = EvolutionCandidateSubject::AgentDefinition {
            revision_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/researcher").unwrap(),
                3,
            )
            .unwrap(),
        };
        let second = service.create_candidate(second).expect("second candidate");
        let mut second_report = eligible_report();
        second_report.report_id = "report-agent-v3".to_string();
        second_report.candidate_id = second.candidate_id.clone();
        second_report.subject_ref = second.subject.subject_ref();
        service
            .record_comparison(second_report)
            .expect("second report");
        let second_review = service
            .request_canary_review(&second.candidate_id)
            .expect("second review");
        let second_lease = crate::security::test_verified_decision_lease(
            &second_review.review_id,
            release_action_key(second_review.action),
            second_review.subject.scope_ref(),
            review_digest(&second_review),
        );
        assert!(matches!(
            service.decide_review(
                &principal,
                &second_lease,
                &second_review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "second canary must not overlap".to_string(),
            ),
            Err(EvolutionGovernanceError::ActiveCanaryAlreadyExists)
        ));
    }

    #[test]
    fn team_canary_routes_deterministically_and_stop_fences_new_graphs() {
        let service = service();
        let candidate = service
            .create_candidate(team_candidate())
            .expect("candidate");
        let mut report = eligible_report();
        report.candidate_id = candidate.candidate_id.clone();
        report.report_id = "report-team-v2".to_string();
        report.evaluation_contract_digest = candidate.evaluation_contract_digest();
        report.subject_ref = candidate.subject.subject_ref();
        service.record_comparison(report).expect("comparison");
        let review = service
            .request_canary_review(&candidate.candidate_id)
            .expect("canary review");
        let principal = crate::security::test_human_interactive_principal();
        let lease = crate::security::test_verified_decision_lease(
            &review.review_id,
            release_action_key(review.action),
            review.subject.scope_ref(),
            review_digest(&review),
        );
        let assignment = service
            .decide_review(
                &principal,
                &lease,
                &review.review_id,
                ReleaseChangeReviewDecision::Approve,
                "team canary".to_string(),
            )
            .expect("decision")
            .expect("assignment");
        let EvolutionCandidateSubject::TeamTemplate { revision_ref } = &candidate.subject else {
            unreachable!("team fixture");
        };
        let selected = service
            .select_team_canary_assignment(
                &revision_ref.template_id,
                &RevisionSelector::LatestApprovedStable,
                "team-routing-identity",
            )
            .expect("select")
            .expect("100% fixture policy would select");
        service
            .validate_team_canary_binding(
                revision_ref,
                &assignment.assignment_id,
                assignment.generation,
            )
            .expect("active Team Canary binding");
        assert_eq!(selected.assignment_id, assignment.assignment_id);
        let stop = service
            .request_release_change(ReleaseChangeRequest {
                request_id: "stop-team-candidate-v2".to_string(),
                subject: candidate.subject.clone(),
                action: ReleaseChangeAction::StopCanary,
                selector: None,
                candidate_id: Some(candidate.candidate_id.clone()),
                evidence_refs: vec![EvidenceRef::observed("incident", "team-canary")],
            })
            .expect("stop review");
        let stop_lease = crate::security::test_verified_decision_lease(
            &stop.review_id,
            release_action_key(stop.action),
            stop.subject.scope_ref(),
            review_digest(&stop),
        );
        service
            .decide_review(
                &principal,
                &stop_lease,
                &stop.review_id,
                ReleaseChangeReviewDecision::Approve,
                "stop Team Canary".to_string(),
            )
            .expect("stop decision");
        assert!(matches!(
            service.validate_team_canary_binding(
                revision_ref,
                &assignment.assignment_id,
                assignment.generation,
            ),
            Err(EvolutionGovernanceError::CanaryPrerequisiteRequired)
        ));
    }

    #[test]
    fn rollback_and_pointer_changes_are_pending_reviews_not_direct_mutations() {
        let service = service();
        let candidate = service.create_candidate(candidate()).expect("candidate");
        assert!(matches!(
            service.request_release_change(ReleaseChangeRequest {
                request_id: "invalid-pointer".to_string(),
                subject: candidate.subject.clone(),
                action: ReleaseChangeAction::SetDefaultExact,
                selector: None,
                candidate_id: None,
                evidence_refs: vec![EvidenceRef::observed("audit", "pointer")],
            }),
            Err(EvolutionGovernanceError::InvalidReleaseChangeRequest(_))
        ));
        let review = service
            .request_release_change(ReleaseChangeRequest {
                request_id: "rollback-agent-v1".to_string(),
                subject: candidate.subject,
                action: ReleaseChangeAction::Rollback,
                selector: Some(RevisionSelector::ExactApprovedRevision { revision: 1 }),
                candidate_id: None,
                evidence_refs: vec![EvidenceRef::observed("incident", "rollback")],
            })
            .expect("rollback review");
        assert_eq!(review.class, ReleaseChangeReviewClass::Rollback);
        assert_eq!(review.status, ReleaseChangeReviewStatus::Pending);
        assert!(service.release_assignments().unwrap().is_empty());
    }
}
