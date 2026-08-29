//! Strategy routing for Cowd AI work kernel.
//!
//! This crate owns deterministic task understanding and execution-mode
//! selection. It does not execute tools, assemble prompts, or mutate task
//! state; later layers consume its `StrategyDecision`.

use crate::core::{
    ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, KernelCapability, MeasureProvenance,
    TaskComplexity, TaskRisk,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDomain {
    Review,
    Bugfix,
    Frontend,
    Backend,
    Docs,
    Release,
    Test,
    Research,
    Architecture,
    Explore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDuration {
    Immediate,
    Short,
    Extended,
    LongRunning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUnderstanding {
    pub domain: TaskDomain,
    pub complexity: TaskComplexity,
    pub risk: TaskRisk,
    pub requires_write: bool,
    pub requires_external_facts: bool,
    #[serde(default)]
    pub requires_tool_evidence: bool,
    /// Explicit bounded workspace evidence targets extracted from the user
    /// request. They are an immutable admission constraint for a root
    /// collaboration decision, not model-authored planning advice.
    #[serde(default)]
    pub required_workspace_evidence_scopes: Vec<String>,
    pub requests_parallelism: bool,
    pub requests_multi_agent: bool,
    /// Explicit number of independently executed Team entities requested by
    /// the user. This structured value is the orchestration authority; prose
    /// cardinality parsing is only the ingress fallback used to populate it.
    #[serde(default)]
    pub required_team_count: u8,
    /// Explicit user contract requiring a managed Agent to invoke Runtime's
    /// follow-up collaboration escalation tool.  This is ingress authority,
    /// not an optional model-planning preference.
    #[serde(default)]
    pub requires_managed_collaboration_escalation: bool,
    #[serde(default)]
    pub forbids_team: bool,
    pub requests_deep_plan: bool,
    pub requests_deliberation: bool,
    pub requests_background: bool,
    pub likely_single_file: bool,
    pub independent_workstreams: u8,
    pub uncertainty: u8,
    pub estimated_duration: TaskDuration,
    /// Typed collaboration reference for "继续/上一组团队" continuations.
    /// Only Runtime resolves it from exact Session/root history; ordinal
    /// text parsing merely proposes the reference.
    #[serde(default)]
    pub collaboration_reference: CollaborationReference,
}

/// Typed continuation reference produced by strategy understanding.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationReference {
    #[default]
    None,
    LatestEligible,
    ExplicitExecution,
    ExplicitTeamSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyProposal {
    pub pattern: ExecutionPattern,
    #[serde(default)]
    pub modifiers: Vec<ExecutionModifier>,
    pub template: Option<String>,
    pub confidence: u8,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDecisionSource {
    Deterministic,
    ModelValidated,
    ExperienceAdapted,
    ResourceAdapted,
}

/// Runtime execution alternatives compared by the deterministic strategy
/// policy. These names describe ownership/topology, not a second execution
/// engine: each candidate still compiles into the canonical ExecutionGraph.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCandidateKind {
    Direct,
    ParallelTools,
    Team,
}

impl ExecutionCandidateKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::ParallelTools => "parallel_tools",
            Self::Team => "team",
        }
    }
}

/// Versioned resource facts used by candidate scoring. Detached consumers get
/// the conservative assumed snapshot; an admitted Runtime turn replaces it
/// with observed provider/tool/team availability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StrategyResourceSnapshot {
    pub version: String,
    pub provider_available: bool,
    pub tools_available: bool,
    pub team_available: bool,
    pub provider_concurrency: u16,
    pub tool_concurrency: u16,
    pub team_slots: u16,
    pub provider_concurrency_penalty_bp: u16,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub provider_effective_limit: u16,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub provider_queue_p95_ms: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub provider_service_p95_ms: u64,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub provider_failure_timeout_upper_bound_bp: u16,
    /// SHA-256 of the effective provider/model profile. The raw provider or
    /// model name is deliberately excluded from public strategy projections.
    #[serde(default)]
    pub provider_profile_fingerprint: String,
    pub sample_source: String,
    pub sample_count: u32,
    pub provenance: MeasureProvenance,
}

impl Default for StrategyResourceSnapshot {
    fn default() -> Self {
        Self {
            version: "strategy-resource-v1".to_string(),
            provider_available: true,
            tools_available: true,
            team_available: true,
            provider_concurrency: 1,
            tool_concurrency: 1,
            team_slots: 2,
            provider_concurrency_penalty_bp: 0,
            provider_effective_limit: 1,
            provider_queue_p95_ms: 0,
            provider_service_p95_ms: 0,
            provider_failure_timeout_upper_bound_bp: 0,
            provider_profile_fingerprint: String::new(),
            sample_source: "assumed-detached-default".to_string(),
            sample_count: 0,
            provenance: MeasureProvenance::Assumed,
        }
    }
}

const fn is_zero_u16(value: &u16) -> bool {
    *value == 0
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Unit-preserving estimate for one strategy candidate.
///
/// Duration, token, quality, and risk fields are intentionally independent.
/// Selection applies hard gates and lexicographic/Pareto comparisons instead
/// of adding unlike units into a synthetic score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionCandidateEstimate {
    pub candidate: ExecutionCandidateKind,
    pub eligible: bool,
    pub estimated_serial_ms: u64,
    pub estimated_critical_path_ms: u64,
    pub startup_overhead_ms: u64,
    pub context_duplication_tokens: u64,
    pub merge_cost_ms: u64,
    pub evidence_overlap_penalty_bp: u16,
    pub provider_concurrency_penalty_bp: u16,
    pub risk_approval_penalty_bp: u16,
    pub expected_quality_lift_bp: i32,
    pub duration_calibration_source: String,
    pub duration_sample_count: u32,
    pub quality_calibration_source: String,
    pub quality_sample_count: u32,
    pub duration_provenance: MeasureProvenance,
    pub token_provenance: MeasureProvenance,
    pub quality_provenance: MeasureProvenance,
    pub risk_provenance: MeasureProvenance,
    pub reasons: Vec<String>,
}

impl ExecutionCandidateEstimate {
    #[must_use]
    pub fn effective_duration_ms(&self) -> u64 {
        self.estimated_critical_path_ms
            .saturating_add(self.startup_overhead_ms)
            .saturating_add(self.merge_cost_ms)
    }

    #[must_use]
    pub const fn duration_optimization_ready(&self) -> bool {
        self.duration_provenance.supports_automatic_optimization()
            && self.duration_sample_count >= 3
    }

    #[must_use]
    pub const fn quality_optimization_ready(&self) -> bool {
        self.quality_provenance.supports_automatic_optimization() && self.quality_sample_count >= 3
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationLiftEstimate {
    pub expected_lift_bp: i16,
    pub coordination_cost_bp: u16,
    pub accepted: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyInput {
    pub prompt: String,
    pub workspace_available: bool,
    pub changed_files: usize,
    pub explicit_write: bool,
    #[serde(default)]
    pub risk_override: Option<TaskRisk>,
    pub experience: Option<StrategyExperienceSummary>,
    /// End-to-end cost observations are bucketed by the topology that
    /// produced them. A Team duration must never be reused as a serial
    /// baseline and divided a second time.
    #[serde(default)]
    pub candidate_costs: BTreeMap<ExecutionCandidateKind, StrategyCandidateCostSummary>,
    pub proposal: Option<StrategyProposal>,
    #[serde(default)]
    pub understanding: Option<TaskUnderstanding>,
    #[serde(default)]
    pub resource_snapshot: StrategyResourceSnapshot,
    /// Expiry-filtered, provenance-checked observations supplied by the
    /// StrategyExperienceStore. They may veto automatic Team only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub negative_benefit_observations: Vec<NegativeBenefitObservation>,
}

impl StrategyInput {
    #[must_use]
    pub fn from_prompt(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            workspace_available: true,
            changed_files: 0,
            explicit_write: false,
            risk_override: None,
            experience: None,
            candidate_costs: BTreeMap::new(),
            proposal: None,
            understanding: None,
            resource_snapshot: StrategyResourceSnapshot::default(),
            negative_benefit_observations: Vec::new(),
        }
    }

    #[must_use]
    pub const fn without_workspace(mut self) -> Self {
        self.workspace_available = false;
        self
    }

    #[must_use]
    pub const fn with_changed_files(mut self, changed_files: usize) -> Self {
        self.changed_files = changed_files;
        self
    }

    #[must_use]
    pub const fn with_explicit_write(mut self, explicit_write: bool) -> Self {
        self.explicit_write = explicit_write;
        self
    }

    #[must_use]
    pub const fn with_risk_override(mut self, risk: TaskRisk) -> Self {
        self.risk_override = Some(risk);
        self
    }

    #[must_use]
    pub fn with_experience(mut self, experience: StrategyExperienceSummary) -> Self {
        self.experience = Some(experience);
        self
    }

    #[must_use]
    pub fn with_proposal(mut self, proposal: StrategyProposal) -> Self {
        self.proposal = Some(proposal);
        self
    }

    #[must_use]
    pub fn with_understanding(mut self, understanding: TaskUnderstanding) -> Self {
        self.understanding = Some(understanding);
        self
    }

    #[must_use]
    pub fn with_resource_snapshot(mut self, snapshot: StrategyResourceSnapshot) -> Self {
        self.resource_snapshot = snapshot;
        self
    }
}

/// Non-sensitive workload identity used to scope strategy experience.
/// It contains responsibility and risk shape, never prompt or path text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyWorkloadFingerprint {
    pub domain: TaskDomain,
    pub complexity: TaskComplexity,
    pub risk: TaskRisk,
    pub requires_write: bool,
    pub likely_single_file: bool,
    pub responsibility_domains: u8,
    pub independent_judgment: bool,
    pub tool_dag_shape: String,
}

impl StrategyWorkloadFingerprint {
    #[must_use]
    pub fn from_understanding(understanding: &TaskUnderstanding, explicit_write: bool) -> Self {
        let input = StrategyInput {
            explicit_write,
            understanding: Some(understanding.clone()),
            ..StrategyInput::from_prompt(String::new())
        };
        Self::from_input(&input, understanding)
    }

    #[must_use]
    pub fn from_input(input: &StrategyInput, understanding: &TaskUnderstanding) -> Self {
        let tool_dag_shape = if understanding.requires_write {
            if understanding.independent_workstreams > 1 {
                "mixed_read_serial_write"
            } else {
                "bounded_serial_write"
            }
        } else if understanding.requires_external_facts
            || understanding.requires_tool_evidence
            || understanding.requests_parallelism
            || understanding.independent_workstreams > 1
        {
            "parallel_idempotent_read"
        } else {
            "direct_read_or_reason"
        };
        Self {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            requires_write: understanding.requires_write || input.explicit_write,
            likely_single_file: understanding.likely_single_file,
            responsibility_domains: understanding.independent_workstreams.max(1),
            independent_judgment: matches!(understanding.risk, TaskRisk::High | TaskRisk::Critical)
                || understanding.requests_multi_agent,
            tool_dag_shape: tool_dag_shape.to_string(),
        }
    }

    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("{:x}", Sha256::digest(bytes))
    }
}

/// A negative paired observation is deliberately one-way: it can prevent an
/// automatic Team choice but can never become positive calibration for any
/// non-Direct candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegativeBenefitObservation {
    pub workload_fingerprint_sha256: String,
    pub provider_profile_fingerprint: String,
    pub baseline_candidate: ExecutionCandidateKind,
    pub baseline_duration_ms: u64,
    pub baseline_quality_score_bp: u16,
    pub team_duration_ms: u64,
    pub team_quality_score_bp: u16,
    pub report_sha256: String,
    pub provenance_ref: String,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

impl NegativeBenefitObservation {
    fn validate(&self) -> Result<(), String> {
        if !is_sha256(&self.workload_fingerprint_sha256)
            || !is_sha256(&self.provider_profile_fingerprint)
            || !is_sha256(&self.report_sha256)
            || self.provenance_ref.trim().is_empty()
            || self.baseline_candidate == ExecutionCandidateKind::Team
            || self.baseline_duration_ms == 0
            || self.team_duration_ms == 0
            || self.expires_at_ms <= self.observed_at_ms
        {
            return Err(
                "negative Team observation has incomplete provenance or bounds".to_string(),
            );
        }
        let quality_delta =
            i32::from(self.team_quality_score_bp) - i32::from(self.baseline_quality_score_bp);
        let speed_channel = self.team_duration_ms.saturating_mul(100)
            <= self.baseline_duration_ms.saturating_mul(80)
            && quality_delta >= -200;
        let quality_channel = quality_delta >= 1_000
            && self.team_duration_ms.saturating_mul(100)
                <= self.baseline_duration_ms.saturating_mul(110);
        if speed_channel || quality_channel {
            return Err("positive Team result cannot be recorded as a negative veto".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyCandidateCostSummary {
    pub sample_count: u32,
    pub average_critical_path_ms: u64,
    pub average_total_tokens: u64,
    pub average_coordination_cost_ms: u64,
    pub calibration_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceSummary {
    pub sample_count: u32,
    pub success_rate_bp: u16,
    pub verification_block_rate_bp: u16,
    pub context_pressure_rate_bp: u16,
    pub multi_agent_lift_rate_bp: u16,
    /// Only provenance-qualified paired evaluations contribute to lift.
    #[serde(default)]
    pub multi_agent_lift_sample_count: u32,
    #[serde(default)]
    pub average_duration_ms: u64,
    #[serde(default)]
    pub average_total_tokens: u64,
    #[serde(default)]
    pub average_coordination_cost_ms: u64,
    #[serde(default)]
    pub actual_cost_sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairedStrategyCalibrationEvidence {
    pub evaluation_ref: String,
    pub corpus_sha256: String,
    pub workspace_revision: String,
    pub provider_account_ref: String,
    pub baseline_pattern: ExecutionPattern,
    pub baseline_duration_ms: u64,
    pub baseline_quality_score_bp: u16,
    pub candidate_duration_ms: u64,
    pub candidate_quality_score_bp: u16,
    pub blind_judge_completed: bool,
    #[serde(default)]
    pub baseline_total_tokens: u64,
    #[serde(default)]
    pub candidate_total_tokens: u64,
    #[serde(default)]
    pub candidate_duplicate_tool_ratio_bp: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_channel: Option<StrategyCalibrationAdmissionChannel>,
    #[serde(default)]
    pub report_sha256: String,
    #[serde(default)]
    pub rubric_sha256: String,
    #[serde(default)]
    pub binary_sha256: String,
    #[serde(default)]
    pub frontend_workspace_revision: String,
    #[serde(default)]
    pub model_revision: String,
    #[serde(default)]
    pub judge_model_revision: String,
    #[serde(default)]
    pub invariant_fingerprint: String,
}

impl PairedStrategyCalibrationEvidence {
    #[must_use]
    pub fn is_provenance_complete(&self) -> bool {
        self.evaluation_ref
            .starts_with("harness_eval.auto_strategy_paired.v1:")
            && is_sha256(&self.corpus_sha256)
            && is_sha256(&self.report_sha256)
            && is_sha256(&self.rubric_sha256)
            && is_sha256(&self.binary_sha256)
            && is_sha256(&self.invariant_fingerprint)
            && !self.workspace_revision.trim().is_empty()
            && !self.frontend_workspace_revision.trim().is_empty()
            && !self.provider_account_ref.trim().is_empty()
            && !self.model_revision.trim().is_empty()
            && !self.judge_model_revision.trim().is_empty()
            && self.baseline_duration_ms > 0
            && self.candidate_duration_ms > 0
            && self.blind_judge_completed
    }

    #[must_use]
    pub fn registered_admission_channel(&self) -> Option<StrategyCalibrationAdmissionChannel> {
        let quality_delta =
            i32::from(self.candidate_quality_score_bp) - i32::from(self.baseline_quality_score_bp);
        let token_gate = quality_delta >= 1_000
            || self.candidate_total_tokens.saturating_mul(10)
                <= self.baseline_total_tokens.saturating_mul(18);
        let speed_channel = self.candidate_duration_ms.saturating_mul(100)
            <= self.baseline_duration_ms.saturating_mul(80)
            && quality_delta >= -200;
        let quality_channel = quality_delta >= 1_000
            && self.candidate_duration_ms.saturating_mul(100)
                <= self.baseline_duration_ms.saturating_mul(110);
        if self.baseline_total_tokens == 0
            || self.candidate_duplicate_tool_ratio_bp >= 1_500
            || !token_gate
        {
            None
        } else if speed_channel {
            Some(StrategyCalibrationAdmissionChannel::Speed)
        } else if quality_channel {
            Some(StrategyCalibrationAdmissionChannel::Quality)
        } else {
            None
        }
    }

    #[must_use]
    pub fn demonstrates_positive_lift(&self) -> bool {
        self.is_provenance_complete()
            && self.admission_channel.is_some()
            && self.admission_channel == self.registered_admission_channel()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyCalibrationAdmissionChannel {
    Speed,
    Quality,
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

const FROZEN_TEAM_CALIBRATION_TASKS: [(&str, &str); 4] = [
    (
        "AS-T01-runtime-gateway-surface",
        "全面审视 crates/runtime、crates/gateway、surfaces/webui 三个独立责任域的策略事件接线、权限边界和用户可见状态，分别给出证据后综合。",
    ),
    (
        "AS-T02-backend-frontend-audit",
        "深度审计 crates/gateway、surfaces/webui、crates/tui 的执行状态一致性与交互契约，每个责任域独立判断并交叉验证。",
    ),
    (
        "AS-T03-runtime-memory-matrix",
        "全面分析 runtime、context/memory、matrix 三个独立域的事实来源、生命周期与证据归并风险，形成带边界的全景结论。",
    ),
    (
        "AS-T04-bounded-implementation-review",
        "完整执行一次受限写入与独立复核，覆盖 runtime 写入责任和 harness-eval 验收责任：只修改 fixtures/auto-strategy-write/target.txt，使其内容精确等于 {{EXPECTED_CONTENT}}（含末尾换行）；必须先读取、写入不同内容、再读取并核对写后摘要，随后用独立验证步骤重新读取目标并核对 change/evidence。不得修改 fixtures/auto-strategy-protected/sentinel.txt 或任何其他路径，最终说明实现、源验证、独立复核、风险与证据。",
    ),
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyCalibrationImportReceipt {
    pub report_sha256: String,
    pub corpus_sha256: String,
    pub rubric_sha256: String,
    pub binary_sha256: String,
    pub workspace_revision: String,
    pub frontend_workspace_revision: String,
    pub provider_account_ref: String,
    pub model_revision: String,
    pub judge_model_revision: String,
    pub invariant_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceRecord {
    pub domain: TaskDomain,
    pub complexity: TaskComplexity,
    pub risk: TaskRisk,
    pub selected_pattern: ExecutionPattern,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_candidate: Option<ExecutionCandidateKind>,
    pub succeeded: bool,
    pub verification_blocked: bool,
    pub context_pressure: bool,
    /// More than one candidate topology executed before terminal completion.
    /// Retained for diagnostics but excluded from candidate cost calibration.
    #[serde(default)]
    pub composite_execution: bool,
    pub multi_agent_positive_lift: bool,
    pub created_at_ms: u64,
    #[serde(default)]
    pub actual_duration_ms: u64,
    #[serde(default)]
    pub actual_input_tokens: u64,
    #[serde(default)]
    pub actual_output_tokens: u64,
    #[serde(default)]
    pub actual_cached_tokens: u64,
    #[serde(default)]
    pub actual_coordination_cost_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_calibration: Option<PairedStrategyCalibrationEvidence>,
}

impl StrategyExperienceRecord {
    #[must_use]
    pub fn from_decision(
        decision: &StrategyDecision,
        succeeded: bool,
        verification_blocked: bool,
        context_pressure: bool,
        multi_agent_positive_lift: bool,
        created_at_ms: u64,
    ) -> Self {
        Self {
            domain: decision.understanding.domain,
            complexity: decision.understanding.complexity,
            risk: decision.understanding.risk,
            selected_pattern: decision.pattern,
            selected_candidate: Some(decision.selected_candidate),
            succeeded,
            verification_blocked,
            context_pressure,
            composite_execution: false,
            multi_agent_positive_lift,
            created_at_ms,
            actual_duration_ms: 0,
            actual_input_tokens: 0,
            actual_output_tokens: 0,
            actual_cached_tokens: 0,
            actual_coordination_cost_ms: 0,
            paired_calibration: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceStore {
    pub records: Vec<StrategyExperienceRecord>,
    #[serde(default)]
    pub trusted_calibration_reports: Vec<StrategyCalibrationImportReceipt>,
    #[serde(default)]
    pub negative_benefit_observations: Vec<NegativeBenefitObservation>,
}

impl StrategyExperienceStore {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
            trusted_calibration_reports: Vec::new(),
            negative_benefit_observations: Vec::new(),
        }
    }

    pub fn record(&mut self, record: StrategyExperienceRecord) {
        let mut record = record;
        record.multi_agent_positive_lift = record
            .paired_calibration
            .as_ref()
            .is_some_and(PairedStrategyCalibrationEvidence::demonstrates_positive_lift);
        self.records.push(record);
    }

    /// Persist a provenance-complete negative Team result. Duplicate report /
    /// workload / provider tuples are idempotent and refresh the observation.
    pub fn record_negative_benefit(
        &mut self,
        observation: NegativeBenefitObservation,
    ) -> Result<(), String> {
        observation.validate()?;
        self.negative_benefit_observations.retain(|existing| {
            existing.report_sha256 != observation.report_sha256
                || existing.workload_fingerprint_sha256 != observation.workload_fingerprint_sha256
                || existing.provider_profile_fingerprint != observation.provider_profile_fingerprint
        });
        self.negative_benefit_observations.push(observation);
        Ok(())
    }

    /// Import one-way veto evidence from a paired evaluator even when its
    /// positive claim gate fails. Isolation, provenance and budget evidence
    /// must still be complete; only the performance/quality benefit claim may
    /// fail.
    pub fn import_negative_benefit_report(
        &mut self,
        report: &serde_json::Value,
    ) -> Result<usize, String> {
        if report["kind"] != "harness_eval.auto_strategy_paired.v1"
            || report
                .pointer("/gate/provenance_complete")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/budget_observation_complete")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/judge_isolation_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/workspace_reset_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/baseline_topology_isolation_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err("negative Team report lacks isolation/provenance gates".to_string());
        }
        let report_sha256 = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(report)
                    .map_err(|error| format!("encode negative Team report: {error}"))?
            )
        );
        let observations = report
            .get("negative_benefit_observations")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "negative Team report has no veto observations".to_string())?;
        let mut imported = 0;
        for value in observations {
            let mut observation: NegativeBenefitObservation = serde_json::from_value(value.clone())
                .map_err(|error| format!("decode negative Team observation: {error}"))?;
            observation.report_sha256.clone_from(&report_sha256);
            self.record_negative_benefit(observation)?;
            imported += 1;
        }
        Ok(imported)
    }

    /// Import only a fully passed, provenance-bound paired evaluation report.
    ///
    /// Ordinary turns cannot mint these records: the report must carry the
    /// frozen corpus identity, release provenance, a passing claim gate, and
    /// per-record fields matching that provenance. Duplicate evaluation refs
    /// are idempotent.
    pub fn import_paired_evaluation_report(
        &mut self,
        report: &serde_json::Value,
    ) -> Result<usize, String> {
        const FROZEN_CORPUS_SHA256: &str =
            "d8dc4ba671dacd7a12b41d0cbe17d1cb4f2d5f5055cb2b9e7cefab2bb8c22e3c";
        const FROZEN_RUBRIC_SHA256: &str =
            "3c2672ad0038c5b63abc6d6f724380d3a339e5921559dcb0b5c39e1a63039eba";
        if report["kind"] != "harness_eval.auto_strategy_paired.v1"
            || report["status"] != "passed"
            || report
                .pointer("/gate/passed")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/claim_allowed")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/workspace_mutation_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/workspace_reset_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/judge_isolation_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/automatic_team_materialization_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/baseline_topology_isolation_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/hard_budget_lease_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || report
                .pointer("/gate/tool_topology_observation_gate")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err("strategy calibration report has no passing claim gate".to_string());
        }
        let provenance = report
            .get("provenance")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| "strategy calibration report has no provenance".to_string())?;
        let provenance_value = |name: &str| {
            provenance
                .get(name)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| format!("strategy calibration provenance lacks `{name}`"))
        };
        let corpus_id = provenance_value("corpus_id")?;
        let corpus_sha256 = provenance_value("corpus_sha256")?;
        let rubric_sha256 = provenance_value("rubric_sha256")?;
        let workspace_revision = provenance_value("workspace_revision")?;
        let frontend_workspace_revision = provenance_value("frontend_workspace_revision")?;
        let backend_source_archive_sha256 = provenance_value("backend_source_archive_sha256")?;
        let frontend_source_archive_sha256 = provenance_value("frontend_source_archive_sha256")?;
        let provider_account_ref = provenance_value("provider_account_ref")?;
        let binary_sha256 = provenance_value("binary_sha256")?;
        let model_revision = provenance_value("provider")?;
        let judge_model_revision = provenance_value("judge_model")?;
        let invariant_fingerprint = provenance_value("condition_invariant_fingerprint")?;
        let invariants = provenance
            .get("condition_invariants")
            .ok_or_else(|| "strategy calibration provenance lacks invariants".to_string())?;
        let computed_invariant_fingerprint = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(invariants)
                    .map_err(|error| format!("encode calibration invariants: {error}"))?
            )
        );
        if corpus_id != "auto-strategy-v1"
            || corpus_sha256 != FROZEN_CORPUS_SHA256
            || rubric_sha256 != FROZEN_RUBRIC_SHA256
            || !is_sha256(binary_sha256)
            || !is_sha256(backend_source_archive_sha256)
            || !is_sha256(frontend_source_archive_sha256)
            || !is_sha256(invariant_fingerprint)
            || invariant_fingerprint != computed_invariant_fingerprint
            || invariants
                .get("permission_mode")
                .and_then(serde_json::Value::as_str)
                != Some("danger-full-access")
            || invariants
                .get("workspace_fixture")
                .and_then(serde_json::Value::as_str)
                != Some("workspace-auto-strategy-frozen")
            || invariants
                .get("mutation_fixture_reset")
                .and_then(serde_json::Value::as_str)
                != Some("per-sample-pristine-full-workspace-sha256")
            || invariants
                .get("tool_catalog")
                .and_then(serde_json::Value::as_str)
                != Some("same-binary-runtime-inspected")
            || invariants
                .get("provider_fallbacks")
                .and_then(serde_json::Value::as_str)
                != Some("disabled")
            || provenance.get("seed").and_then(serde_json::Value::as_u64) != Some(20_260_716)
            || provenance
                .get("temperature_milli")
                .and_then(serde_json::Value::as_u64)
                != Some(0)
            || provenance
                .get("warmup_per_task")
                .and_then(serde_json::Value::as_u64)
                != Some(1)
            || provenance
                .get("repetitions")
                .and_then(serde_json::Value::as_u64)
                .is_none_or(|value| value < 3)
        {
            return Err("strategy calibration provenance is not the frozen evaluator".to_string());
        }
        let scored_samples = report
            .get("samples")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "strategy calibration report has no sample evidence".to_string())?
            .iter()
            .filter(|sample| {
                sample.get("warmup").and_then(serde_json::Value::as_bool) == Some(false)
            })
            .collect::<Vec<_>>();
        if scored_samples.is_empty()
            || scored_samples.iter().any(|sample| {
                sample.get("status").and_then(serde_json::Value::as_str) != Some("completed")
                    || sample
                        .get("workspace_reset_verified")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .pointer("/judge/judge_isolation_verified")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .get("execution_graph_id")
                        .and_then(serde_json::Value::as_str)
                        .is_none()
                    || sample
                        .get("ttft_observed")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .get("usage_observed")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .get("cost_observed")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .get("evaluation_control_observed")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .get("evaluation_budget_observed")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    || sample
                        .get("evaluation_budget_breached")
                        .and_then(serde_json::Value::as_bool)
                        != Some(false)
                    || sample
                        .get("evaluation_token_limit")
                        .and_then(serde_json::Value::as_u64)
                        != Some(12_000)
                    || sample
                        .get("evaluation_tokens_consumed")
                        .and_then(serde_json::Value::as_u64)
                        != Some(
                            sample
                                .get("input_tokens")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0)
                                .saturating_add(
                                    sample
                                        .get("output_tokens")
                                        .and_then(serde_json::Value::as_u64)
                                        .unwrap_or(0),
                                )
                                .saturating_add(
                                    sample
                                        .get("cached_tokens")
                                        .and_then(serde_json::Value::as_u64)
                                        .unwrap_or(0),
                                ),
                        )
                    || sample
                        .get("models_used")
                        .and_then(serde_json::Value::as_array)
                        .is_none_or(|models| {
                            models.is_empty()
                                || models
                                    .iter()
                                    .any(|model| model.as_str() != Some(model_revision))
                        })
                    || sample
                        .pointer("/judge/observed_models")
                        .and_then(serde_json::Value::as_array)
                        .is_none_or(|models| {
                            models.is_empty()
                                || models
                                    .iter()
                                    .any(|model| model.as_str() != Some(judge_model_revision))
                        })
                    || (sample.get("task_id").and_then(serde_json::Value::as_str)
                        == Some("AS-T04-bounded-implementation-review")
                        && (sample
                            .get("workspace_mutation_verified")
                            .and_then(serde_json::Value::as_bool)
                            != Some(true)
                            || sample
                                .get("workspace_changed_paths")
                                .and_then(serde_json::Value::as_array)
                                .is_none_or(|paths| {
                                    paths
                                        != &[serde_json::Value::String(
                                            "fixtures/auto-strategy-write/target.txt".to_string(),
                                        )]
                                })
                            || sample
                                .get("write_attempt_paths")
                                .and_then(serde_json::Value::as_array)
                                .is_none_or(|paths| {
                                    paths
                                        != &[serde_json::Value::String(
                                            "fixtures/auto-strategy-write/target.txt".to_string(),
                                        )]
                                })
                            || sample
                                .get("workspace_mutation_error")
                                .is_some_and(|error| !error.is_null())))
            })
            || report
                .get("task_comparisons")
                .and_then(serde_json::Value::as_array)
                .is_none_or(|comparisons| {
                    comparisons.iter().any(|comparison| {
                        comparison
                            .get("valid_pair_count")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0)
                            < 3
                    })
                })
        {
            return Err(
                "strategy calibration report lacks complete paired sample evidence".to_string(),
            );
        }
        let mut digest_source = report.clone();
        if let Some(object) = digest_source.as_object_mut() {
            object.remove("strategy_calibration_records");
        }
        let report_sha256 = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&digest_source)
                    .map_err(|error| format!("encode calibration report digest: {error}"))?
            )
        );
        let receipt = StrategyCalibrationImportReceipt {
            report_sha256: report_sha256.clone(),
            corpus_sha256: corpus_sha256.to_string(),
            rubric_sha256: rubric_sha256.to_string(),
            binary_sha256: binary_sha256.to_string(),
            workspace_revision: workspace_revision.to_string(),
            frontend_workspace_revision: frontend_workspace_revision.to_string(),
            provider_account_ref: provider_account_ref.to_string(),
            model_revision: model_revision.to_string(),
            judge_model_revision: judge_model_revision.to_string(),
            invariant_fingerprint: invariant_fingerprint.to_string(),
        };
        let provided_records = report
            .get("strategy_calibration_records")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "strategy calibration report has no calibration records".to_string())?;
        if provided_records.is_empty() {
            return Err("passed strategy calibration report has an empty record set".to_string());
        }
        let repetitions = provenance
            .get("repetitions")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| "strategy calibration repetitions are invalid".to_string())?;
        let samples = report
            .get("samples")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "strategy calibration samples are unavailable".to_string())?;
        let comparisons = report
            .get("task_comparisons")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "strategy calibration comparisons are unavailable".to_string())?;
        let mut derived_records = Vec::new();
        for (task_id, prompt) in FROZEN_TEAM_CALIBRATION_TASKS {
            let comparison = comparisons
                .iter()
                .find(|comparison| {
                    comparison
                        .get("task_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(task_id)
                })
                .ok_or_else(|| format!("strategy calibration lacks comparison `{task_id}`"))?;
            let baseline_condition = comparison
                .get("strongest_non_team_baseline")
                .and_then(serde_json::Value::as_str)
                .filter(|condition| matches!(*condition, "direct" | "parallel_tools"))
                .ok_or_else(|| {
                    format!("strategy calibration baseline for `{task_id}` is invalid")
                })?;
            let understanding = understand(&StrategyInput::from_prompt(prompt.to_string()));
            for repetition in 0..repetitions {
                let find_sample = |condition: &str| {
                    samples.iter().find(|sample| {
                        sample.get("warmup").and_then(serde_json::Value::as_bool) == Some(false)
                            && sample.get("task_id").and_then(serde_json::Value::as_str)
                                == Some(task_id)
                            && sample.get("repetition").and_then(serde_json::Value::as_u64)
                                == u64::try_from(repetition).ok()
                            && sample.get("condition").and_then(serde_json::Value::as_str)
                                == Some(condition)
                            && sample.get("status").and_then(serde_json::Value::as_str)
                                == Some("completed")
                    })
                };
                let auto = find_sample("auto").ok_or_else(|| {
                    format!("strategy calibration lacks Auto `{task_id}` repetition {repetition}")
                })?;
                let baseline = find_sample(baseline_condition).ok_or_else(|| {
                    format!(
                        "strategy calibration lacks baseline `{task_id}` repetition {repetition}"
                    )
                })?;
                let metric = |sample: &serde_json::Value, name: &str| {
                    sample.get(name).and_then(serde_json::Value::as_u64)
                };
                let total_tokens = |sample: &serde_json::Value| {
                    metric(sample, "input_tokens")
                        .unwrap_or(0)
                        .saturating_add(metric(sample, "output_tokens").unwrap_or(0))
                        .saturating_add(metric(sample, "cached_tokens").unwrap_or(0))
                };
                let tool_calls = metric(auto, "tool_calls").unwrap_or(0);
                let duplicate_ratio_bp = if tool_calls == 0 {
                    0
                } else {
                    u16::try_from(
                        metric(auto, "duplicate_tool_calls")
                            .unwrap_or(0)
                            .saturating_mul(10_000)
                            / tool_calls,
                    )
                    .unwrap_or(10_000)
                };
                let mut paired_calibration = PairedStrategyCalibrationEvidence {
                    evaluation_ref: format!(
                        "harness_eval.auto_strategy_paired.v1:{corpus_id}:{task_id}:{repetition}"
                    ),
                    corpus_sha256: corpus_sha256.to_string(),
                    workspace_revision: workspace_revision.to_string(),
                    provider_account_ref: provider_account_ref.to_string(),
                    baseline_pattern: if baseline_condition == "direct" {
                        ExecutionPattern::Direct
                    } else {
                        ExecutionPattern::Explore
                    },
                    baseline_duration_ms: metric(baseline, "critical_path_ms").ok_or_else(
                        || format!("baseline `{task_id}` lacks critical-path duration"),
                    )?,
                    baseline_quality_score_bp: metric(baseline, "quality_bp")
                        .and_then(|value| u16::try_from(value).ok())
                        .ok_or_else(|| format!("baseline `{task_id}` lacks quality"))?,
                    candidate_duration_ms: metric(auto, "critical_path_ms")
                        .ok_or_else(|| format!("Auto `{task_id}` lacks critical-path duration"))?,
                    candidate_quality_score_bp: metric(auto, "quality_bp")
                        .and_then(|value| u16::try_from(value).ok())
                        .ok_or_else(|| format!("Auto `{task_id}` lacks quality"))?,
                    blind_judge_completed: true,
                    baseline_total_tokens: total_tokens(baseline),
                    candidate_total_tokens: total_tokens(auto),
                    candidate_duplicate_tool_ratio_bp: duplicate_ratio_bp,
                    admission_channel: None,
                    report_sha256: String::new(),
                    rubric_sha256: String::new(),
                    binary_sha256: String::new(),
                    frontend_workspace_revision: String::new(),
                    model_revision: String::new(),
                    judge_model_revision: String::new(),
                    invariant_fingerprint: String::new(),
                };
                paired_calibration.admission_channel =
                    paired_calibration.registered_admission_channel();
                derived_records.push(StrategyExperienceRecord {
                    domain: understanding.domain,
                    complexity: understanding.complexity,
                    risk: understanding.risk,
                    selected_pattern: ExecutionPattern::Collaborate,
                    selected_candidate: Some(ExecutionCandidateKind::Team),
                    succeeded: true,
                    verification_blocked: false,
                    context_pressure: false,
                    composite_execution: false,
                    multi_agent_positive_lift: false,
                    created_at_ms: 0,
                    actual_duration_ms: paired_calibration.candidate_duration_ms,
                    actual_input_tokens: metric(auto, "input_tokens").unwrap_or(0),
                    actual_output_tokens: metric(auto, "output_tokens").unwrap_or(0),
                    actual_cached_tokens: metric(auto, "cached_tokens").unwrap_or(0),
                    actual_coordination_cost_ms: metric(auto, "merge_cost_ms").unwrap_or(0),
                    paired_calibration: Some(paired_calibration),
                });
            }
        }
        if serde_json::to_value(&derived_records)
            .map_err(|error| format!("encode derived strategy calibration: {error}"))?
            != serde_json::Value::Array(provided_records.clone())
        {
            return Err(
                "strategy calibration records do not match frozen sample-derived metrics"
                    .to_string(),
            );
        }
        let mut prepared = Vec::new();
        for mut record in derived_records {
            let evidence = record
                .paired_calibration
                .as_mut()
                .ok_or_else(|| "strategy calibration record has no paired evidence".to_string())?;
            if !evidence.evaluation_ref.starts_with(&format!(
                "harness_eval.auto_strategy_paired.v1:{corpus_id}:"
            )) || evidence.corpus_sha256 != corpus_sha256
                || evidence.workspace_revision != workspace_revision
                || evidence.provider_account_ref != provider_account_ref
                || evidence.baseline_duration_ms == 0
                || evidence.candidate_duration_ms == 0
                || !evidence.blind_judge_completed
            {
                return Err(
                    "strategy calibration record does not match report provenance".to_string(),
                );
            }
            evidence.report_sha256.clone_from(&report_sha256);
            evidence.rubric_sha256 = rubric_sha256.to_string();
            evidence.binary_sha256 = binary_sha256.to_string();
            evidence.frontend_workspace_revision = frontend_workspace_revision.to_string();
            evidence.model_revision = model_revision.to_string();
            evidence.judge_model_revision = judge_model_revision.to_string();
            evidence.invariant_fingerprint = invariant_fingerprint.to_string();
            let evaluation_ref = evidence.evaluation_ref.clone();
            record.multi_agent_positive_lift = evidence.demonstrates_positive_lift();
            if let Some(existing) = self.records.iter().find(|existing| {
                existing
                    .paired_calibration
                    .as_ref()
                    .is_some_and(|existing| existing.evaluation_ref == evaluation_ref)
            }) {
                if existing != &record {
                    return Err(
                        "strategy calibration evaluation ref conflicts with stored evidence"
                            .to_string(),
                    );
                }
                continue;
            }
            prepared.push(record);
        }
        let imported = prepared.len();
        self.records.extend(prepared);
        if !self
            .trusted_calibration_reports
            .iter()
            .any(|existing| existing.report_sha256 == report_sha256)
        {
            self.trusted_calibration_reports.push(receipt);
        }
        Ok(imported)
    }

    #[must_use]
    pub fn summary_for(
        &self,
        understanding: &TaskUnderstanding,
    ) -> Option<StrategyExperienceSummary> {
        self.summary_for_pattern(understanding, None)
    }

    #[must_use]
    pub fn summary_for_pattern(
        &self,
        understanding: &TaskUnderstanding,
        selected_pattern: Option<ExecutionPattern>,
    ) -> Option<StrategyExperienceSummary> {
        let comparable = self
            .records
            .iter()
            .filter(|record| {
                record.domain == understanding.domain
                    && record.complexity == understanding.complexity
                    && record.risk == understanding.risk
                    && selected_pattern.is_none_or(|pattern| record.selected_pattern == pattern)
            })
            .collect::<Vec<_>>();
        if comparable.is_empty() {
            return None;
        }
        let sample_count = comparable.len() as u32;
        let paired = comparable
            .iter()
            .copied()
            .filter(|record| self.is_trusted_calibration(record))
            .collect::<Vec<_>>();
        let average = |values: Vec<u64>| {
            (!values.is_empty()).then(|| {
                values
                    .iter()
                    .fold(0_u64, |total, value| total.saturating_add(*value))
                    .saturating_div(values.len() as u64)
            })
        };
        Some(StrategyExperienceSummary {
            sample_count,
            success_rate_bp: rate_bp(
                comparable.iter().filter(|record| record.succeeded).count(),
                comparable.len(),
            ),
            verification_block_rate_bp: rate_bp(
                comparable
                    .iter()
                    .filter(|record| record.verification_blocked)
                    .count(),
                comparable.len(),
            ),
            context_pressure_rate_bp: rate_bp(
                comparable
                    .iter()
                    .filter(|record| record.context_pressure)
                    .count(),
                comparable.len(),
            ),
            multi_agent_lift_rate_bp: rate_bp(
                paired
                    .iter()
                    .filter(|record| record.multi_agent_positive_lift)
                    .count(),
                paired.len(),
            ),
            multi_agent_lift_sample_count: paired.len() as u32,
            average_duration_ms: average(
                comparable
                    .iter()
                    .map(|record| record.actual_duration_ms)
                    .filter(|value| *value > 0)
                    .collect(),
            )
            .unwrap_or(0),
            average_total_tokens: average(
                comparable
                    .iter()
                    .map(|record| {
                        record
                            .actual_input_tokens
                            .saturating_add(record.actual_output_tokens)
                            .saturating_add(record.actual_cached_tokens)
                    })
                    .filter(|value| *value > 0)
                    .collect(),
            )
            .unwrap_or(0),
            average_coordination_cost_ms: average(
                comparable
                    .iter()
                    .map(|record| record.actual_coordination_cost_ms)
                    .filter(|value| *value > 0)
                    .collect(),
            )
            .unwrap_or(0),
            actual_cost_sample_count: comparable
                .iter()
                .filter(|record| record.actual_duration_ms > 0)
                .count() as u32,
        })
    }

    #[must_use]
    pub fn cost_summary_for_candidate(
        &self,
        understanding: &TaskUnderstanding,
        candidate: ExecutionCandidateKind,
    ) -> Option<StrategyCandidateCostSummary> {
        #[derive(Debug, Clone, Copy)]
        struct Observation {
            duration_ms: u64,
            total_tokens: u64,
            coordination_ms: u64,
            paired: bool,
        }

        let belongs_to_candidate = |record: &StrategyExperienceRecord| {
            record.selected_candidate.map_or_else(
                || match candidate {
                    ExecutionCandidateKind::Direct => matches!(
                        record.selected_pattern,
                        ExecutionPattern::Direct | ExecutionPattern::Execute
                    ),
                    ExecutionCandidateKind::ParallelTools => {
                        record.selected_pattern == ExecutionPattern::Explore
                    }
                    ExecutionCandidateKind::Team => {
                        record.selected_pattern == ExecutionPattern::Collaborate
                    }
                },
                |selected| selected == candidate,
            )
        };
        let mut observations = self
            .records
            .iter()
            .filter(|record| {
                record.domain == understanding.domain
                    && record.complexity == understanding.complexity
                    && record.risk == understanding.risk
                    && record.succeeded
                    && !record.verification_blocked
                    && !record.composite_execution
                    && record.actual_duration_ms > 0
                    && belongs_to_candidate(record)
            })
            .map(|record| Observation {
                duration_ms: record.actual_duration_ms,
                total_tokens: record
                    .actual_input_tokens
                    .saturating_add(record.actual_output_tokens)
                    .saturating_add(record.actual_cached_tokens),
                coordination_ms: record.actual_coordination_cost_ms,
                paired: self.is_trusted_calibration(record),
            })
            .collect::<Vec<_>>();

        // Imported Team records also carry the strongest non-Team baseline.
        // Materialize that baseline only into its own candidate bucket; do
        // not create a second Team observation or overwrite Direct history.
        if candidate != ExecutionCandidateKind::Team {
            observations.extend(self.records.iter().filter_map(|record| {
                if record.domain != understanding.domain
                    || record.complexity != understanding.complexity
                    || record.risk != understanding.risk
                    || !record.succeeded
                    || record.verification_blocked
                    || !self.is_trusted_calibration(record)
                {
                    return None;
                }
                let evidence = record.paired_calibration.as_ref()?;
                let baseline_candidate = match evidence.baseline_pattern {
                    ExecutionPattern::Direct | ExecutionPattern::Execute => {
                        ExecutionCandidateKind::Direct
                    }
                    ExecutionPattern::Explore => ExecutionCandidateKind::ParallelTools,
                    _ => return None,
                };
                (baseline_candidate == candidate && evidence.baseline_duration_ms > 0).then_some(
                    Observation {
                        duration_ms: evidence.baseline_duration_ms,
                        total_tokens: evidence.baseline_total_tokens,
                        coordination_ms: 0,
                        paired: true,
                    },
                )
            }));
        }
        if observations.is_empty() {
            return None;
        }
        let average = |values: Vec<u64>| {
            values
                .iter()
                .fold(0_u64, |total, value| total.saturating_add(*value))
                .saturating_div(values.len() as u64)
        };
        let paired = observations.iter().any(|observation| observation.paired);
        Some(StrategyCandidateCostSummary {
            sample_count: observations.len() as u32,
            average_critical_path_ms: average(
                observations
                    .iter()
                    .map(|observation| observation.duration_ms)
                    .collect(),
            ),
            average_total_tokens: average(
                observations
                    .iter()
                    .map(|observation| observation.total_tokens)
                    .collect(),
            ),
            average_coordination_cost_ms: average(
                observations
                    .iter()
                    .map(|observation| observation.coordination_ms)
                    .collect(),
            ),
            calibration_source: if paired {
                "strategy-experience-store:paired-and-absolute-cost".to_string()
            } else {
                "strategy-experience-store:absolute-cost".to_string()
            },
        })
    }

    fn is_trusted_calibration(&self, record: &StrategyExperienceRecord) -> bool {
        let Some(evidence) = record.paired_calibration.as_ref() else {
            return false;
        };
        evidence.is_provenance_complete()
            && self.trusted_calibration_reports.iter().any(|receipt| {
                receipt.report_sha256 == evidence.report_sha256
                    && receipt.corpus_sha256 == evidence.corpus_sha256
                    && receipt.rubric_sha256 == evidence.rubric_sha256
                    && receipt.binary_sha256 == evidence.binary_sha256
                    && receipt.workspace_revision == evidence.workspace_revision
                    && receipt.frontend_workspace_revision == evidence.frontend_workspace_revision
                    && receipt.provider_account_ref == evidence.provider_account_ref
                    && receipt.model_revision == evidence.model_revision
                    && receipt.judge_model_revision == evidence.judge_model_revision
                    && receipt.invariant_fingerprint == evidence.invariant_fingerprint
            })
    }

    #[must_use]
    pub fn enrich_input(&self, input: StrategyInput) -> StrategyInput {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                duration.as_millis().min(u128::from(u64::MAX)) as u64
            });
        self.enrich_input_at(input, now_ms)
    }

    #[must_use]
    pub fn enrich_input_at(&self, mut input: StrategyInput, now_ms: u64) -> StrategyInput {
        let understanding = input
            .understanding
            .clone()
            .unwrap_or_else(|| understand(&input));
        input.understanding = Some(understanding.clone());
        let baseline_pattern = StrategyRouter::default().decide(&input).pattern;
        input.experience = self.summary_for_pattern(&understanding, Some(baseline_pattern));
        input.candidate_costs = [
            ExecutionCandidateKind::Direct,
            ExecutionCandidateKind::ParallelTools,
            ExecutionCandidateKind::Team,
        ]
        .into_iter()
        .filter_map(|candidate| {
            self.cost_summary_for_candidate(&understanding, candidate)
                .map(|summary| (candidate, summary))
        })
        .collect();
        input.negative_benefit_observations = self
            .negative_benefit_observations
            .iter()
            .filter(|observation| {
                observation.observed_at_ms <= now_ms && now_ms < observation.expires_at_ms
            })
            .cloned()
            .collect();
        input
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyDecision {
    pub understanding: TaskUnderstanding,
    pub pattern: ExecutionPattern,
    pub modifiers: Vec<ExecutionModifier>,
    pub gates: Vec<ExecutionPolicyGate>,
    pub collaboration_lift: CollaborationLiftEstimate,
    pub source: StrategyDecisionSource,
    pub confidence: u8,
    pub reasons: Vec<String>,
    pub required_capabilities: Vec<KernelCapability>,
    pub policy_version: String,
    pub selected_candidate: ExecutionCandidateKind,
    pub candidate_estimates: Vec<ExecutionCandidateEstimate>,
    pub resource_snapshot: StrategyResourceSnapshot,
}

impl StrategyDecision {
    #[must_use]
    pub fn uses_modifier(&self, modifier: ExecutionModifier) -> bool {
        self.modifiers.contains(&modifier)
    }

    #[must_use]
    pub fn uses_gate(&self, gate: ExecutionPolicyGate) -> bool {
        self.gates.contains(&gate)
    }

    pub fn retarget(
        &mut self,
        pattern: ExecutionPattern,
        reason: impl Into<String>,
    ) -> Result<(), String> {
        if !pattern_supports_required_gates(pattern, &self.understanding) {
            return Err(format!(
                "pattern `{}` cannot preserve the required policy gates",
                pattern.as_str()
            ));
        }
        self.pattern = pattern;
        normalize_modifiers(pattern, &mut self.modifiers);
        self.gates = policy_gates_for(pattern, &self.understanding);
        self.required_capabilities =
            required_capabilities_for(&self.understanding, pattern, &self.modifiers);
        self.source = StrategyDecisionSource::ResourceAdapted;
        self.reasons.push(reason.into());
        Ok(())
    }

    /// Adapt the admitted strategy to concrete tool effects observed after the
    /// provider has produced a ToolBatch. The decision identity remains owned
    /// by Runtime while its capabilities, modifiers and gates are recomputed
    /// from the effects that will actually execute.
    pub fn retarget_for_tool_requirements(
        &mut self,
        pattern: ExecutionPattern,
        requires_external_facts: bool,
        requires_write: bool,
        requests_parallelism: bool,
        reason: impl Into<String>,
    ) -> Result<(), String> {
        self.understanding.requires_external_facts |= requires_external_facts;
        self.understanding.requires_tool_evidence = true;
        self.understanding.requires_write |= requires_write;
        self.understanding.requests_parallelism |= requests_parallelism;
        let reason = reason.into();
        let effective_pattern = if pattern_supports_required_gates(pattern, &self.understanding) {
            pattern
        } else {
            ExecutionPattern::Execute
        };
        self.retarget(effective_pattern, reason)?;
        if effective_pattern != pattern {
            self.reasons.push(format!(
                "tool requirements requested `{}` but retained `{}` so required policy gates remain enforceable",
                pattern.as_str(),
                effective_pattern.as_str()
            ));
        }

        for modifier in [
            requires_external_facts.then_some(ExecutionModifier::WithExternalResearch),
            requires_write.then_some(ExecutionModifier::WithGuardrails),
            requests_parallelism.then_some(ExecutionModifier::Parallel),
        ]
        .into_iter()
        .flatten()
        {
            if effective_pattern.supports_modifier(modifier) && !self.modifiers.contains(&modifier)
            {
                self.modifiers.push(modifier);
            }
        }
        normalize_modifiers(effective_pattern, &mut self.modifiers);
        self.gates = policy_gates_for(effective_pattern, &self.understanding);
        self.required_capabilities =
            required_capabilities_for(&self.understanding, effective_pattern, &self.modifiers);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyPolicy {
    pub enable_parallel_evidence: bool,
    pub enable_multi_agent: bool,
    pub require_verifier_for_complex: bool,
    pub require_guardrails_for_writes: bool,
}

pub const STRATEGY_POLICY_VERSION: &str = "strategy-decision-v5";

impl Default for StrategyPolicy {
    fn default() -> Self {
        Self {
            enable_parallel_evidence: true,
            enable_multi_agent: true,
            require_verifier_for_complex: true,
            require_guardrails_for_writes: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StrategyRouter {
    policy: StrategyPolicy,
}

impl StrategyRouter {
    #[must_use]
    pub fn new(policy: StrategyPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn decide(&self, input: &StrategyInput) -> StrategyDecision {
        let understanding = input
            .understanding
            .clone()
            .unwrap_or_else(|| understand(input));
        let mut modifiers = Vec::new();
        let mut reasons = Vec::new();

        if understanding.requires_external_facts {
            modifiers.push(ExecutionModifier::WithExternalResearch);
            reasons.push("task asks for current or external facts".to_string());
        }

        if understanding.requires_write && self.policy.require_guardrails_for_writes {
            modifiers.push(ExecutionModifier::WithGuardrails);
        }

        if matches!(understanding.risk, TaskRisk::High | TaskRisk::Critical) {
            modifiers.push(ExecutionModifier::WithCheckpoint);
            modifiers.push(ExecutionModifier::WithReviewer);
        }

        if matches!(
            understanding.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        ) && self.policy.require_verifier_for_complex
        {
            modifiers.push(ExecutionModifier::WithVerifier);
            modifiers.push(ExecutionModifier::WithTrace);
        }

        if understanding.requests_multi_agent && self.policy.enable_multi_agent {
            modifiers.push(ExecutionModifier::WithReviewer);
            reasons.push("task explicitly benefits from multiple agents".to_string());
        }

        if understanding.requests_parallelism {
            modifiers.push(ExecutionModifier::Parallel);
        }
        if understanding.requires_write && understanding.likely_single_file {
            modifiers.push(ExecutionModifier::BoundedChange);
        }
        if understanding.requests_background {
            modifiers.push(ExecutionModifier::Background);
        }

        let mut pattern = select_pattern(&understanding, &self.policy, &mut reasons);
        let mut source = StrategyDecisionSource::Deterministic;
        let mut proposed_candidate = None;
        if let Some(proposal) = &input.proposal {
            if proposal_is_executable(proposal, &understanding) {
                pattern = proposal.pattern;
                proposed_candidate = candidate_for_pattern(proposal.pattern);
                modifiers.extend(proposal.modifiers.iter().copied());
                reasons.push(format!("validated model proposal: {}", proposal.rationale));
                source = StrategyDecisionSource::ModelValidated;
            } else {
                reasons.push("model proposal was rejected by contract policy".to_string());
            }
        }
        if let Some(experience) = &input.experience {
            let adapted =
                adapt_pattern_from_experience(pattern, &understanding, experience, &mut reasons);
            if adapted != pattern {
                source = StrategyDecisionSource::ExperienceAdapted;
                pattern = adapted;
            }
        }
        let collaboration_lift =
            estimate_collaboration_lift(&understanding, input.experience.as_ref());
        let mut candidate_estimates = estimate_execution_candidates(
            &understanding,
            input.experience.as_ref(),
            &input.candidate_costs,
            &input.resource_snapshot,
            &self.policy,
        );
        let workload_fingerprint =
            StrategyWorkloadFingerprint::from_input(input, &understanding).digest();
        let negative_team_veto = (!understanding.requests_multi_agent)
            .then(|| {
                input
                    .negative_benefit_observations
                    .iter()
                    .find(|observation| {
                        observation.workload_fingerprint_sha256 == workload_fingerprint
                            && !input
                                .resource_snapshot
                                .provider_profile_fingerprint
                                .is_empty()
                            && observation.provider_profile_fingerprint
                                == input.resource_snapshot.provider_profile_fingerprint
                    })
            })
            .flatten();
        if let Some(observation) = negative_team_veto {
            if let Some(team) = candidate_estimates
                .iter_mut()
                .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            {
                team.eligible = false;
                team.reasons.push(format!(
                    "automatic Team vetoed by workload/profile-scoped negative benefit evidence {}",
                    &observation.report_sha256[..12]
                ));
            }
            reasons.push(
                "automatic Team vetoed by an unexpired, provenance-bound negative benefit observation"
                    .to_string(),
            );
        }
        let structural_team_obligation = automatic_team_is_structurally_required(&understanding);
        if !understanding.requests_multi_agent
            && candidate_estimates.iter().any(|estimate| {
                estimate.candidate == ExecutionCandidateKind::Team
                    && estimate.eligible
                    && (!estimate.duration_optimization_ready()
                        || !estimate.quality_optimization_ready())
            })
        {
            reasons.push(if structural_team_obligation {
                "automatic Team is required by independently verifiable responsibility domains; historical calibration may tune capacity but cannot erase the current objective's ownership obligations"
                    .to_string()
            } else {
                "automatic Team requires calibrated or observed topology evidence when the current objective does not itself require independent ownership"
                    .to_string()
            });
        }
        let selected_candidate = if matches!(source, StrategyDecisionSource::ExperienceAdapted) {
            candidate_for_pattern(pattern).unwrap_or_else(|| {
                select_execution_candidate(
                    &understanding,
                    &candidate_estimates,
                    &input.resource_snapshot,
                )
            })
        } else if let Some(candidate) = proposed_candidate {
            candidate_estimates
                .iter()
                .find(|estimate| estimate.candidate == candidate && estimate.eligible)
                .map_or_else(
                    || {
                        select_execution_candidate(
                            &understanding,
                            &candidate_estimates,
                            &input.resource_snapshot,
                        )
                    },
                    |estimate| estimate.candidate,
                )
        } else {
            select_execution_candidate(
                &understanding,
                &candidate_estimates,
                &input.resource_snapshot,
            )
        };
        if !matches!(
            pattern,
            ExecutionPattern::Deliberate | ExecutionPattern::Supervise
        ) && (!matches!(pattern, ExecutionPattern::Execute)
            || selected_candidate == ExecutionCandidateKind::Team)
            && !matches!(
                source,
                StrategyDecisionSource::ModelValidated | StrategyDecisionSource::ExperienceAdapted
            )
        {
            pattern = pattern_for_candidate(selected_candidate, &understanding);
            reasons.push(format!(
                "unit-preserving strategy policy selected {}",
                selected_candidate.as_str()
            ));
        }
        let explicit_team_cost_warning = understanding.requests_multi_agent
            && selected_candidate == ExecutionCandidateKind::Team
            && candidate_estimates.iter().any(|estimate| {
                estimate.candidate == ExecutionCandidateKind::Team
                    && (estimate.effective_duration_ms() >= estimate.estimated_serial_ms
                        || !estimate.duration_optimization_ready()
                        || !estimate.quality_optimization_ready())
            });
        if pattern == ExecutionPattern::Collaborate && explicit_team_cost_warning {
            reasons.push(
                "explicit Team override retained despite no measured duration advantage or paired quality proof; surface must show the cost warning"
                    .to_string(),
            );
        }
        if !pattern_supports_required_gates(pattern, &understanding) {
            reasons.push(format!(
                "{} cannot represent the required policy gates; using execute",
                pattern.as_str()
            ));
            pattern = ExecutionPattern::Execute;
        }
        let mut confidence = confidence_for(&understanding, pattern);
        if let Some(experience) = &input.experience {
            confidence =
                adapt_confidence_from_experience(confidence, pattern, experience, &mut reasons);
            if experience.verification_block_rate_bp >= 3000
                && !modifiers.contains(&ExecutionModifier::WithVerifier)
                && pattern.supports_modifier(ExecutionModifier::WithVerifier)
            {
                modifiers.push(ExecutionModifier::WithVerifier);
                reasons.push("experience shows verification gaps for comparable tasks".to_string());
            }
        }
        normalize_modifiers(pattern, &mut modifiers);
        let gates = policy_gates_for(pattern, &understanding);
        let required_capabilities = required_capabilities_for(&understanding, pattern, &modifiers);

        StrategyDecision {
            understanding,
            pattern,
            modifiers,
            gates,
            collaboration_lift,
            source,
            confidence,
            reasons,
            required_capabilities,
            policy_version: STRATEGY_POLICY_VERSION.to_string(),
            selected_candidate,
            candidate_estimates,
            resource_snapshot: input.resource_snapshot.clone(),
        }
    }
}

fn estimate_execution_candidates(
    understanding: &TaskUnderstanding,
    experience: Option<&StrategyExperienceSummary>,
    candidate_costs: &BTreeMap<ExecutionCandidateKind, StrategyCandidateCostSummary>,
    resources: &StrategyResourceSnapshot,
    policy: &StrategyPolicy,
) -> Vec<ExecutionCandidateEstimate> {
    let heuristic_serial_ms = match understanding.complexity {
        TaskComplexity::Trivial => 1_500,
        TaskComplexity::Simple => 4_000,
        TaskComplexity::Moderate => 12_000,
        TaskComplexity::Complex => 30_000,
        TaskComplexity::Strategic => 60_000,
    };
    let trusted_cost = |candidate| {
        candidate_costs
            .get(&candidate)
            .filter(|sample| sample.sample_count >= 3 && sample.average_critical_path_ms > 0)
    };
    let direct_cost = trusted_cost(ExecutionCandidateKind::Direct);
    let parallel_cost = trusted_cost(ExecutionCandidateKind::ParallelTools);
    let team_cost = trusted_cost(ExecutionCandidateKind::Team);
    let serial_ms = direct_cost.map_or(heuristic_serial_ms, |sample| {
        sample.average_critical_path_ms
    });
    let risk_penalty = match understanding.risk {
        TaskRisk::Low => 0,
        TaskRisk::Medium => 400,
        TaskRisk::High => 1_500,
        TaskRisk::Critical => 5_000,
    };
    let workstreams = u64::from(understanding.independent_workstreams.max(1));
    let calibrated_quality = experience
        .filter(|sample| sample.multi_agent_lift_sample_count >= 3)
        .map_or(0_i32, |sample| {
            i32::from(sample.multi_agent_lift_rate_bp).saturating_sub(5_000)
        });
    let parallel_eligible = policy.enable_parallel_evidence
        && resources.tools_available
        && (understanding.requests_parallelism
            || understanding.requires_external_facts
            || understanding.requires_tool_evidence
            || (workstreams >= 2 && !understanding.requires_write));
    let team_resource_eligible = policy.enable_multi_agent
        && resources.team_available
        && resources.provider_available
        && resources.team_slots >= 2
        && understanding.risk != TaskRisk::Critical
        && !understanding.forbids_team;
    let team_eligible = team_resource_eligible
        && (understanding.requests_multi_agent
            || (workstreams >= 2
                && (matches!(
                    understanding.complexity,
                    TaskComplexity::Complex | TaskComplexity::Strategic
                ) || workstreams >= 3)));
    let parallel_width = u64::from(
        resources
            .tool_concurrency
            .max(1)
            .min(u16::from(understanding.independent_workstreams.max(2))),
    );
    let parallel_execution_width = if understanding.requires_write {
        1
    } else {
        parallel_width
    };
    let team_width = u64::from(
        resources
            .team_slots
            .max(1)
            .min(u16::from(understanding.independent_workstreams.max(2))),
    );
    // The governed write topology is one bounded implementer followed by a
    // reviewer. It improves verification quality but is not parallel write
    // fan-out, so its critical path must not divide by available Team slots.
    let team_execution_width = if understanding.requires_write {
        1
    } else {
        team_width
    };

    let direct = candidate_estimate(
        ExecutionCandidateKind::Direct,
        resources.provider_available,
        direct_cost.map_or(serial_ms, |sample| sample.average_critical_path_ms),
        serial_ms,
        0,
        0,
        0,
        if workstreams >= 2 { 1_200 } else { 0 },
        resources.provider_concurrency_penalty_bp,
        risk_penalty,
        0,
        vec!["single owner; no delegation or merge cost".to_string()],
    );
    let parallel = candidate_estimate(
        ExecutionCandidateKind::ParallelTools,
        parallel_eligible,
        serial_ms,
        parallel_cost.map_or_else(
            || {
                serial_ms
                    .saturating_add(parallel_execution_width - 1)
                    .saturating_div(parallel_execution_width)
            },
            |sample| sample.average_critical_path_ms,
        ),
        if parallel_cost.is_some() { 0 } else { 300 },
        parallel_execution_width
            .saturating_sub(1)
            .saturating_mul(180),
        if parallel_cost.is_some() { 0 } else { 250 },
        if workstreams >= 2 { 300 } else { 1_800 },
        resources.provider_concurrency_penalty_bp / 2,
        risk_penalty / 2,
        if understanding.requires_external_facts || understanding.requires_tool_evidence {
            250
        } else {
            0
        },
        vec!["parallel idempotent evidence/tool waves".to_string()],
    );
    let team_quality = 800_i32
        .saturating_add(i32::from(understanding.uncertainty).saturating_mul(100))
        .saturating_add(
            if matches!(
                understanding.complexity,
                TaskComplexity::Complex | TaskComplexity::Strategic
            ) {
                800
            } else {
                0
            },
        )
        .saturating_add(calibrated_quality);
    let observed_coordination_ms = team_cost
        .filter(|sample| sample.average_coordination_cost_ms > 0)
        .map_or(1_800, |sample| sample.average_coordination_cost_ms);
    let mut team = candidate_estimate(
        ExecutionCandidateKind::Team,
        team_eligible,
        serial_ms,
        team_cost.map_or_else(
            || {
                serial_ms
                    .saturating_add(team_execution_width - 1)
                    .saturating_div(team_execution_width)
            },
            |sample| sample.average_critical_path_ms,
        ),
        if team_cost.is_some() {
            0
        } else {
            observed_coordination_ms
        },
        team_width.saturating_sub(1).saturating_mul(1_200),
        if team_cost.is_some() { 0 } else { 1_000 },
        if workstreams >= 2 { 500 } else { 4_000 },
        resources.provider_concurrency_penalty_bp,
        risk_penalty,
        team_quality,
        vec![format!(
            "{workstreams} independent responsibility domains; {} admitted Team slots; execution width {}",
            resources.team_slots, team_execution_width,
        )],
    );
    if resources.provider_concurrency_penalty_bp > 0 {
        team.reasons.push(format!(
            "provider queue/capacity pressure adds {}bp to Team cost",
            resources.provider_concurrency_penalty_bp
        ));
    }
    [direct, parallel, team]
        .into_iter()
        .map(|mut estimate| {
            let cost = trusted_cost(estimate.candidate);
            estimate.duration_calibration_source = cost.map_or_else(
                || "assumed-policy-v1".to_string(),
                |sample| sample.calibration_source.clone(),
            );
            estimate.duration_sample_count = cost.map_or(0, |sample| sample.sample_count);
            estimate.duration_provenance = cost.map_or(MeasureProvenance::Assumed, |sample| {
                if sample.sample_count >= 3 {
                    MeasureProvenance::Calibrated
                } else {
                    MeasureProvenance::Observed
                }
            });
            if estimate.candidate == ExecutionCandidateKind::Team {
                if let Some(sample) =
                    experience.filter(|sample| sample.multi_agent_lift_sample_count >= 3)
                {
                    estimate.quality_provenance = MeasureProvenance::Calibrated;
                    estimate.quality_sample_count = sample.multi_agent_lift_sample_count;
                    estimate.quality_calibration_source =
                        "strategy-experience-store:paired-quality-lift".to_string();
                } else {
                    estimate.quality_provenance = MeasureProvenance::Assumed;
                    estimate.quality_calibration_source = "assumed-policy-v1".to_string();
                }
            }
            if cost.is_some() {
                estimate.reasons.push(
                    "critical path is an observed end-to-end value and is not divided again"
                        .to_string(),
                );
            }
            estimate
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn candidate_estimate(
    candidate: ExecutionCandidateKind,
    eligible: bool,
    estimated_serial_ms: u64,
    estimated_critical_path_ms: u64,
    startup_overhead_ms: u64,
    context_duplication_tokens: u64,
    merge_cost_ms: u64,
    evidence_overlap_penalty_bp: u16,
    provider_concurrency_penalty_bp: u16,
    risk_approval_penalty_bp: u16,
    expected_quality_lift_bp: i32,
    reasons: Vec<String>,
) -> ExecutionCandidateEstimate {
    ExecutionCandidateEstimate {
        candidate,
        eligible,
        estimated_serial_ms,
        estimated_critical_path_ms,
        startup_overhead_ms,
        context_duplication_tokens,
        merge_cost_ms,
        evidence_overlap_penalty_bp,
        provider_concurrency_penalty_bp,
        risk_approval_penalty_bp,
        expected_quality_lift_bp,
        duration_calibration_source: String::new(),
        duration_sample_count: 0,
        quality_calibration_source: String::new(),
        quality_sample_count: 0,
        duration_provenance: MeasureProvenance::Assumed,
        token_provenance: MeasureProvenance::Assumed,
        quality_provenance: MeasureProvenance::Unknown,
        risk_provenance: MeasureProvenance::Assumed,
        reasons,
    }
}

fn select_execution_candidate(
    understanding: &TaskUnderstanding,
    estimates: &[ExecutionCandidateEstimate],
    resources: &StrategyResourceSnapshot,
) -> ExecutionCandidateKind {
    if understanding.requests_multi_agent {
        return estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            .filter(|estimate| estimate.eligible)
            .map_or(ExecutionCandidateKind::Direct, |estimate| {
                estimate.candidate
            });
    }
    if understanding.requests_parallelism {
        return estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::ParallelTools)
            .filter(|estimate| estimate.eligible)
            .map_or(ExecutionCandidateKind::Direct, |estimate| {
                estimate.candidate
            });
    }
    // A current objective can itself require independent ownership.  In that
    // case historical calibration may tune capacity, but it cannot erase the
    // need to materialize the accountable workstreams.  This remains a
    // semantic, data-derived decision: no Team name, template name, provider
    // family, or price estimate participates in the predicate.
    if automatic_team_is_structurally_required(understanding) {
        if let Some(team) = estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            .filter(|estimate| estimate.eligible && estimate.expected_quality_lift_bp > 0)
        {
            return team.candidate;
        }
    }
    // Otherwise automatic Team selection requires a genuinely multi-domain
    // topology and calibrated evidence. Duration and quality remain separate
    // dimensions; neither is converted into a synthetic cross-unit score.
    if (matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    ) || understanding.independent_workstreams >= 3)
        && understanding.independent_workstreams >= 2
        && resources.provider_concurrency_penalty_bp < 8_000
    {
        if let Some(team) = estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            .filter(|estimate| {
                estimate.eligible
                    && estimate.duration_optimization_ready()
                    && estimate.quality_optimization_ready()
                    && estimate.expected_quality_lift_bp > 0
                    && (estimate.effective_duration_ms() < estimate.estimated_serial_ms
                        || estimate.expected_quality_lift_bp >= 1_000)
            })
        {
            return team.candidate;
        }
    }
    if understanding.requires_external_facts || understanding.requires_tool_evidence {
        return estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::ParallelTools)
            .filter(|estimate| estimate.eligible)
            .map_or(ExecutionCandidateKind::Direct, |estimate| {
                estimate.candidate
            });
    }
    // In the topology-neutral path, compare only Direct and ParallelTools by
    // effective milliseconds. Context duplication tokens are a stable
    // secondary preference, followed by the candidate enum for deterministic
    // Direct-first ties. Team cannot appear here without the calibrated gate
    // above.
    estimates
        .iter()
        .filter(|estimate| {
            estimate.eligible
                && estimate.candidate != ExecutionCandidateKind::Team
                && estimate.duration_provenance != MeasureProvenance::Unknown
        })
        .min_by_key(|estimate| {
            (
                estimate.effective_duration_ms(),
                estimate.context_duplication_tokens,
                estimate.candidate,
            )
        })
        // No candidate is executable without a provider.  Keep the fallback
        // representationally direct so Runtime can expose the actual provider
        // outage as a blocked direct turn instead of inventing a tool fan-out
        // that cannot make progress.
        .map_or(ExecutionCandidateKind::Direct, |estimate| {
            estimate.candidate
        })
}

/// A task with three or more independently verifiable responsibility domains
/// cannot be truthfully reduced to one owner merely because this exact shape
/// has not appeared in a historical benchmark.  The condition deliberately
/// relies on normalized task semantics, not repository/product role names.
/// Return whether the current objective itself requires independently
/// accountable Team work, regardless of the availability of historical
/// calibration samples.
///
/// This is deliberately part of the normalized strategy contract rather than
/// a Router-local heuristic: every downstream decision that selects a Team
/// template must consume this exact semantic predicate.  Otherwise a Team
/// can be selected for independent evidence work and subsequently compiled
/// with a topology that does not own those responsibilities.
#[must_use]
pub fn automatic_team_is_structurally_required(understanding: &TaskUnderstanding) -> bool {
    !understanding.forbids_team
        && understanding.required_team_count == 0
        && understanding.independent_workstreams >= 3
        && understanding.requires_tool_evidence
}

fn candidate_for_pattern(pattern: ExecutionPattern) -> Option<ExecutionCandidateKind> {
    match pattern {
        ExecutionPattern::Direct | ExecutionPattern::Execute => {
            Some(ExecutionCandidateKind::Direct)
        }
        ExecutionPattern::Explore => Some(ExecutionCandidateKind::ParallelTools),
        ExecutionPattern::Collaborate => Some(ExecutionCandidateKind::Team),
        ExecutionPattern::Deliberate | ExecutionPattern::Supervise => None,
    }
}

fn pattern_for_candidate(
    candidate: ExecutionCandidateKind,
    understanding: &TaskUnderstanding,
) -> ExecutionPattern {
    match candidate {
        ExecutionCandidateKind::Direct if understanding.requires_write => ExecutionPattern::Execute,
        ExecutionCandidateKind::Direct => ExecutionPattern::Direct,
        ExecutionCandidateKind::ParallelTools if understanding.requires_write => {
            ExecutionPattern::Execute
        }
        ExecutionCandidateKind::ParallelTools => ExecutionPattern::Explore,
        ExecutionCandidateKind::Team => ExecutionPattern::Collaborate,
    }
}

fn adapt_pattern_from_experience(
    pattern: ExecutionPattern,
    understanding: &TaskUnderstanding,
    experience: &StrategyExperienceSummary,
    reasons: &mut Vec<String>,
) -> ExecutionPattern {
    if experience.sample_count < 3 {
        return pattern;
    }
    if pattern == ExecutionPattern::Collaborate
        && experience.multi_agent_lift_sample_count >= 3
        && experience.multi_agent_lift_rate_bp < 4000
        && !matches!(understanding.risk, TaskRisk::Critical)
    {
        reasons.push("experience shows low multi-agent lift for comparable tasks".to_string());
        return ExecutionPattern::Execute;
    }
    if matches!(
        pattern,
        ExecutionPattern::Direct | ExecutionPattern::Explore
    ) && experience.verification_block_rate_bp >= 5000
        && matches!(
            understanding.complexity,
            TaskComplexity::Moderate | TaskComplexity::Complex | TaskComplexity::Strategic
        )
    {
        reasons.push(
            "experience shows frequent verification blocks; upgrading to execute".to_string(),
        );
        return ExecutionPattern::Execute;
    }
    pattern
}

fn adapt_confidence_from_experience(
    confidence: u8,
    pattern: ExecutionPattern,
    experience: &StrategyExperienceSummary,
    reasons: &mut Vec<String>,
) -> u8 {
    if experience.sample_count < 3 {
        return confidence;
    }
    if experience.success_rate_bp >= 8500 {
        reasons.push("experience shows high success rate for comparable routing".to_string());
        return confidence.saturating_add(5).min(95);
    }
    if experience.success_rate_bp <= 4500 || experience.context_pressure_rate_bp >= 6000 {
        reasons.push("experience shows degraded outcomes for comparable routing".to_string());
        return confidence.saturating_sub(match pattern {
            ExecutionPattern::Supervise => 0,
            _ => 8,
        });
    }
    confidence
}

#[must_use]
pub fn decide_strategy(input: &StrategyInput) -> StrategyDecision {
    StrategyRouter::default().decide(input)
}

#[must_use]
pub fn understand(input: &StrategyInput) -> TaskUnderstanding {
    let normalized = normalize(&input.prompt);
    let domain = classify_domain(&normalized);
    let forbids_workspace_write = explicitly_forbids_workspace_write(&normalized);
    let requests_artifact = requests_persisted_artifact(&normalized)
        && !explicitly_forbids_persisted_artifact(&normalized)
        && (!forbids_workspace_write || explicitly_requests_new_artifact(&normalized));
    let requires_write = input.explicit_write
        || requests_artifact
        || (contains_any(&normalized, WRITE_TERMS) && !forbids_workspace_write);
    // A user can mention tools only to rule them out for an otherwise direct
    // request. Do not turn an explicit prohibition into an evidence-seeking
    // strategy merely because the word "tool" occurs in the prompt.
    let tool_use_forbidden = explicitly_forbids_tool_use(&normalized);
    let requires_external_facts = requires_external_facts(&normalized) && !tool_use_forbidden;
    let requires_tool_evidence =
        contains_any(&normalized, TOOL_EVIDENCE_TERMS) && !tool_use_forbidden;
    let required_workspace_evidence_scopes = explicit_workspace_evidence_scopes(&input.prompt);
    let requests_parallelism = contains_any(&normalized, PARALLEL_TERMS);
    // A request may mention teams solely to prohibit them. Treating every
    // occurrence of "team" as an affirmative collaboration request turns a
    // user-selected single-agent execution mode into its opposite.
    let forbids_team = explicitly_forbids_collaboration(&normalized);
    let required_team_count = if forbids_team {
        0
    } else {
        explicit_team_count(&normalized)
            .max(u8::from(explicit_team_execution_required(&normalized)))
    };
    let requests_multi_agent =
        (contains_any(&normalized, MULTI_AGENT_TERMS) || required_team_count > 0) && !forbids_team;
    let requires_managed_collaboration_escalation =
        explicit_managed_collaboration_escalation_required(&normalized) && !forbids_team;
    let collaboration_reference = if contains_any(
        &normalized,
        &[
            "继续上一组",
            "继续团队",
            "上一组团队",
            "继续上次",
            "继续处理",
            "continue with the previous",
            "continue the previous team",
            "resume the previous team",
        ],
    ) {
        CollaborationReference::LatestEligible
    } else {
        CollaborationReference::None
    };
    let requests_deep_plan = contains_any(&normalized, DEEP_PLAN_TERMS);
    let requests_deliberation = contains_any(&normalized, DELIBERATION_TERMS);
    let requests_background = contains_any(&normalized, BACKGROUND_TERMS);
    let likely_single_file = contains_any(&normalized, SINGLE_FILE_TERMS)
        || (requires_write
            && !requests_deep_plan
            && !requests_parallelism
            && !requests_multi_agent
            && input.changed_files <= 1);
    let risk = classify_risk(input, &normalized, requires_write);
    let complexity = classify_complexity(
        input,
        domain,
        ComplexitySignals {
            requires_write,
            requires_external_facts,
            requires_tool_evidence,
            requests_parallelism,
            requests_multi_agent,
            requests_deep_plan,
            likely_single_file,
        },
    );

    TaskUnderstanding {
        domain,
        complexity,
        risk,
        requires_write,
        requires_external_facts,
        requires_tool_evidence,
        required_workspace_evidence_scopes,
        requests_parallelism,
        requests_multi_agent,
        required_team_count,
        requires_managed_collaboration_escalation,
        forbids_team,
        requests_deep_plan,
        requests_deliberation,
        requests_background,
        likely_single_file,
        independent_workstreams: independent_workstreams(&normalized),
        uncertainty: uncertainty_score(&normalized, requires_external_facts),
        collaboration_reference,
        estimated_duration: estimate_duration(
            complexity,
            requests_background,
            requests_multi_agent,
        ),
    }
}

/// Extract portable, bounded file targets that the user named explicitly.
/// The extractor intentionally carries only relative workspace paths and
/// performs no repository choice or authorization. Runtime still resolves
/// each scope through `WorkspacePathIdentityResolver` at admission.
fn explicit_workspace_evidence_scopes(prompt: &str) -> Vec<String> {
    let mut scopes = prompt
        .split(|character: char| {
            !(character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '-' | '.'))
        })
        .map(str::trim)
        .filter(|candidate| {
            candidate.contains('/')
                && !candidate.starts_with('/')
                && !candidate.starts_with("../")
                && !candidate.contains("/../")
                && candidate
                    .rsplit_once('/')
                    .is_some_and(|(_, name)| name.contains('.'))
        })
        .map(|candidate| format!("read:{candidate}"))
        .collect::<Vec<_>>();
    scopes.sort();
    scopes.dedup();
    scopes
}

fn explicitly_forbids_collaboration(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要组队",
            "不要团队",
            "不要启动团队",
            "不要启动协作",
            "不要创建团队",
            "不要创建任何团队",
            "不要组建团队",
            "不要新建团队",
            "不要启动任何团队",
            "禁止创建团队",
            "不启动团队",
            "无需团队",
            "不需要团队",
            "单 agent",
            "单agent",
            "单人执行",
            "don't use team",
            "do not use team",
            "do not start a team",
            "single agent",
            "single-agent",
        ],
    )
}

/// Returns whether a prompt contains an unambiguous instruction not to invoke
/// tools for this request. Runtime consumers use the same interpretation when
/// selecting the provider tool schema set, so routing and actual exposure
/// cannot diverge.
#[must_use]
pub fn prompt_explicitly_forbids_tool_use(prompt: &str) -> bool {
    explicitly_forbids_tool_use(&normalize(prompt))
}

fn explicitly_forbids_tool_use(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要调用工具",
            "不要使用工具",
            "不调用工具",
            "不使用工具",
            "无需调用工具",
            "不需要调用工具",
            "无需使用工具",
            "不需要使用工具",
            "禁止调用工具",
            "禁止使用工具",
            "do not call tools",
            "don't call tools",
            "do not use tools",
            "don't use tools",
            "no tool calls",
            "without tools",
        ],
    )
}

fn select_pattern(
    understanding: &TaskUnderstanding,
    policy: &StrategyPolicy,
    reasons: &mut Vec<String>,
) -> ExecutionPattern {
    if understanding.risk == TaskRisk::Critical {
        reasons.push("critical risk requires governed execution".to_string());
        return ExecutionPattern::Execute;
    }
    if understanding.requests_background {
        return ExecutionPattern::Supervise;
    }
    if understanding.requests_deliberation {
        return ExecutionPattern::Deliberate;
    }
    if understanding.requests_multi_agent && policy.enable_multi_agent {
        return ExecutionPattern::Collaborate;
    }
    if understanding.requires_write {
        reasons.push("write work requires the governed execution graph".to_string());
        return ExecutionPattern::Execute;
    }
    if understanding.requests_parallelism && policy.enable_parallel_evidence {
        return ExecutionPattern::Explore;
    }
    if matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    ) || understanding.requests_deep_plan
    {
        return ExecutionPattern::Execute;
    }
    if understanding.requires_external_facts || understanding.requires_tool_evidence {
        return ExecutionPattern::Explore;
    }
    if matches!(
        understanding.complexity,
        TaskComplexity::Trivial | TaskComplexity::Simple
    ) {
        reasons.push("low-risk simple task should avoid over-planning".to_string());
        return ExecutionPattern::Direct;
    }
    ExecutionPattern::Explore
}

fn required_capabilities_for(
    understanding: &TaskUnderstanding,
    pattern: ExecutionPattern,
    modifiers: &[ExecutionModifier],
) -> Vec<KernelCapability> {
    let mut capabilities = vec![
        KernelCapability::StrategyRouting,
        KernelCapability::ContextEpoch,
    ];
    if understanding.requires_write || modifiers.contains(&ExecutionModifier::WithGuardrails) {
        capabilities.push(KernelCapability::ToolTransaction);
    }
    if matches!(
        pattern,
        ExecutionPattern::Explore
            | ExecutionPattern::Execute
            | ExecutionPattern::Deliberate
            | ExecutionPattern::Collaborate
            | ExecutionPattern::Supervise
    ) {
        capabilities.push(KernelCapability::ExecutionGraph);
    }
    if modifiers.contains(&ExecutionModifier::WithVerifier)
        || matches!(
            understanding.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        )
    {
        capabilities.push(KernelCapability::VerificationLedger);
    }
    capabilities.push(KernelCapability::Evaluation);
    capabilities.push(KernelCapability::GrowthLoop);
    capabilities.sort_by_key(|capability| format!("{capability:?}"));
    capabilities.dedup();
    capabilities
}

fn classify_domain(normalized: &str) -> TaskDomain {
    if contains_any(normalized, REVIEW_TERMS) {
        TaskDomain::Review
    } else if contains_any(normalized, BUGFIX_TERMS) {
        TaskDomain::Bugfix
    } else if contains_any(normalized, FRONTEND_TERMS) {
        TaskDomain::Frontend
    } else if contains_any(normalized, RELEASE_TERMS) {
        TaskDomain::Release
    } else if contains_any(normalized, TEST_TERMS) {
        TaskDomain::Test
    } else if contains_any(normalized, RESEARCH_TERMS) {
        TaskDomain::Research
    } else if contains_any(normalized, ARCHITECTURE_TERMS) {
        TaskDomain::Architecture
    } else if contains_any(normalized, DOCS_TERMS) {
        TaskDomain::Docs
    } else if contains_any(normalized, BACKEND_TERMS) {
        TaskDomain::Backend
    } else {
        TaskDomain::Explore
    }
}

fn classify_risk(input: &StrategyInput, normalized: &str, requires_write: bool) -> TaskRisk {
    if let Some(risk) = input.risk_override {
        risk
    } else if contains_any(normalized, CRITICAL_RISK_TERMS) {
        TaskRisk::Critical
    } else if contains_any(normalized, HIGH_RISK_TERMS) || input.changed_files > 20 {
        TaskRisk::High
    } else if requires_write || input.changed_files > 0 {
        TaskRisk::Medium
    } else {
        TaskRisk::Low
    }
}

#[derive(Debug, Clone, Copy)]
struct ComplexitySignals {
    requires_write: bool,
    requires_external_facts: bool,
    requires_tool_evidence: bool,
    requests_parallelism: bool,
    requests_multi_agent: bool,
    requests_deep_plan: bool,
    likely_single_file: bool,
}

fn classify_complexity(
    input: &StrategyInput,
    domain: TaskDomain,
    signals: ComplexitySignals,
) -> TaskComplexity {
    if signals.requests_multi_agent
        || signals.requests_deep_plan
        || contains_many_scopes(&input.prompt)
    {
        return TaskComplexity::Strategic;
    }
    if signals.requests_parallelism
        || input.changed_files > 8
        || matches!(domain, TaskDomain::Architecture | TaskDomain::Release)
    {
        return TaskComplexity::Complex;
    }
    if signals.requires_external_facts
        || signals.requires_tool_evidence
        || input.changed_files > 2
        || matches!(
            domain,
            TaskDomain::Review | TaskDomain::Bugfix | TaskDomain::Backend
        )
    {
        return TaskComplexity::Moderate;
    }
    if signals.requires_write && !signals.likely_single_file {
        return TaskComplexity::Moderate;
    }
    if input.prompt.chars().count() <= 80 {
        TaskComplexity::Simple
    } else {
        TaskComplexity::Moderate
    }
}

fn confidence_for(understanding: &TaskUnderstanding, pattern: ExecutionPattern) -> u8 {
    match (understanding.complexity, pattern) {
        (TaskComplexity::Simple | TaskComplexity::Trivial, ExecutionPattern::Direct) => 88,
        (_, ExecutionPattern::Execute) if understanding.likely_single_file => 84,
        (TaskComplexity::Strategic, ExecutionPattern::Execute | ExecutionPattern::Collaborate) => {
            82
        }
        (_, ExecutionPattern::Supervise | ExecutionPattern::Deliberate) => 80,
        _ => 72,
    }
}

fn contains_many_scopes(prompt: &str) -> bool {
    let normalized = normalize(prompt);
    let count = [
        "gateway",
        "runtime",
        "memory",
        "matrix",
        "session",
        "mission",
        "task",
        "team",
        "agent",
        "tool",
        "provider",
        "surface",
        "connector",
        "skill",
        "mcp",
        "reality",
        "fact",
        "eval",
        "evolution",
        "tui",
        "webui",
        "service",
        "crate",
        "context",
    ]
    .iter()
    .filter(|term| normalized.contains(**term))
    .count();
    count >= 3
}

fn normalize_modifiers(pattern: ExecutionPattern, modifiers: &mut Vec<ExecutionModifier>) {
    let mut seen = std::collections::HashSet::new();
    modifiers.retain(|modifier| pattern.supports_modifier(*modifier) && seen.insert(*modifier));
}

fn policy_gates_for(
    pattern: ExecutionPattern,
    understanding: &TaskUnderstanding,
) -> Vec<ExecutionPolicyGate> {
    ExecutionPolicyGate::ALL
        .iter()
        .copied()
        .filter(|gate| {
            gate.is_required_for(understanding.risk, understanding.requires_write)
                && pattern.supports_gate(*gate)
        })
        .collect()
}

fn pattern_supports_required_gates(
    pattern: ExecutionPattern,
    understanding: &TaskUnderstanding,
) -> bool {
    ExecutionPolicyGate::ALL.iter().copied().all(|gate| {
        !gate.is_required_for(understanding.risk, understanding.requires_write)
            || pattern.supports_gate(gate)
    })
}

fn proposal_is_executable(proposal: &StrategyProposal, understanding: &TaskUnderstanding) -> bool {
    if proposal.confidence < 40 {
        return false;
    }
    if understanding.risk == TaskRisk::Critical && proposal.pattern == ExecutionPattern::Direct {
        return false;
    }
    if understanding.requires_write && proposal.pattern == ExecutionPattern::Direct {
        return false;
    }
    proposal
        .modifiers
        .iter()
        .all(|modifier| proposal.pattern.supports_modifier(*modifier))
        && pattern_supports_required_gates(proposal.pattern, understanding)
}

fn estimate_collaboration_lift(
    understanding: &TaskUnderstanding,
    experience: Option<&StrategyExperienceSummary>,
) -> CollaborationLiftEstimate {
    // A semantic Mission may legitimately propose up to 100 independent
    // workstreams. Compute in a wider type and saturate only at the persisted
    // contract boundary so debug builds cannot overflow on a valid graph.
    let independence = i32::from(understanding.independent_workstreams) * 1_500;
    let verification = i32::from(matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    )) * 1_500;
    let uncertainty = i32::from(understanding.uncertainty) * 100;
    let historical = experience
        .filter(|summary| summary.multi_agent_lift_sample_count >= 3)
        .map_or(0, |summary| {
            i32::from(summary.multi_agent_lift_rate_bp) - 5_000
        });
    let coordination_cost_bp = match understanding.complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => 4_500,
        TaskComplexity::Moderate => 3_000,
        TaskComplexity::Complex => 2_000,
        TaskComplexity::Strategic => 1_500,
    };
    let expected_lift_bp = (independence + verification + uncertainty + historical
        - i32::from(coordination_cost_bp))
    .clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16;
    CollaborationLiftEstimate {
        expected_lift_bp,
        coordination_cost_bp,
        accepted: understanding.independent_workstreams >= 2 && expected_lift_bp > 0,
        reasons: vec![format!(
            "{} independent workstreams; uncertainty {}; coordination cost {}bp",
            understanding.independent_workstreams, understanding.uncertainty, coordination_cost_bp
        )],
    }
}

fn independent_workstreams(normalized: &str) -> u8 {
    let domains = [
        "runtime",
        "gateway",
        "frontend",
        "backend",
        "tui",
        "webui",
        "memory",
        "matrix",
        "test",
        "harness-contract",
        "harness-eval",
        "auth-broker",
        "app-protocol",
    ]
    .iter()
    .filter(|term| normalized.contains(**term))
    .count();
    let explicit_team_handoff = contains_any(
        normalized,
        &[
            "另一个团队",
            "另外一个团队",
            "第二个团队",
            "下一团队",
            "another team",
            "second team",
            "next team",
        ],
    )
    .then_some(2)
    .unwrap_or_default();
    (domains as u8)
        .max(explicit_workstream_count(normalized))
        .max(explicit_team_handoff)
        .clamp(1, 8)
}

fn requests_persisted_artifact(normalized: &str) -> bool {
    const ACTIONS: &[&str] = &[
        "生成", "创建", "制作", "产出", "形成", "保存", "写入", "落盘", "放到", "存入", "build",
        "create", "generate", "save", "write",
    ];
    const ARTIFACTS: &[&str] = &[
        "html",
        "网页",
        "网站",
        "文件",
        "代码",
        "页面",
        "项目",
        "artifact",
        "file",
        "website",
        "webpage",
        "source code",
    ];
    contains_any(normalized, ACTIONS) && contains_any(normalized, ARTIFACTS)
}

/// Returns whether an explicitly requested Team, rather than the parent
/// conversation Agent, owns the persisted artifact. The nearest explicit
/// owner before the artifact action wins. This keeps requests such as
/// "two research Teams, then one Agent writes the report" from silently
/// converting the second research Team into a writer.
#[must_use]
pub fn explicit_team_owns_persisted_artifact(prompt: &str) -> bool {
    let normalized = normalize(prompt);
    if !requests_persisted_artifact(&normalized) {
        return false;
    }

    // Multiple explicitly requested Teams are independent domain owners by
    // default. Collective language such as "两个团队研讨后形成统一方案" does
    // not turn the last Team into a serial file writer. Only an explicit
    // follow-up writer assignment may do that; otherwise the parent execution
    // consumes every Team receipt and owns the final persisted artifact.
    if explicit_team_count(&normalized) > 1
        && !contains_any(
            &normalized,
            &[
                "另一个团队负责",
                "另外一个团队负责",
                "第二个团队负责",
                "第三个团队负责",
                "第四个团队负责",
                "第五个团队负责",
                "第六个团队负责",
                "第七个团队负责",
                "第八个团队负责",
                "最后一个团队负责",
                "下一团队负责",
                "第三个生成",
                "第四个生成",
                "第五个生成",
                "第六个生成",
                "第七个生成",
                "第八个生成",
                "another team writes",
                "second team writes",
                "third team writes",
                "fourth team writes",
                "fifth team writes",
                "sixth team writes",
                "seventh team writes",
                "eighth team writes",
                "final team writes",
                "writer team",
            ],
        )
    {
        return false;
    }

    const ARTIFACT_ACTIONS: &[&str] = &[
        "生成", "创建", "制作", "产出", "形成", "保存", "写入", "落盘", "放到", "存入", "build",
        "create", "generate", "save", "write",
    ];
    const ARTIFACTS: &[&str] = &[
        "html",
        "网页",
        "网站",
        "文件",
        "代码",
        "页面",
        "项目",
        "artifact",
        "file",
        "website",
        "webpage",
        "source code",
    ];
    const TEAM_OWNERS: &[&str] = &["团队", "team"];
    const PARENT_AGENT_OWNERS: &[&str] = &[
        "使用一个智能体",
        "由一个智能体",
        "主智能体",
        "父智能体",
        "主流程",
        "主对话",
        "one agent",
        "an agent",
        "main agent",
        "parent agent",
        "main flow",
    ];

    let artifact_position = ARTIFACTS
        .iter()
        .filter_map(|term| normalized.find(term))
        .min()
        .unwrap_or(normalized.len());
    let action_position = ARTIFACT_ACTIONS
        .iter()
        .filter_map(|term| normalized[..artifact_position].rfind(term))
        .max()
        .unwrap_or(artifact_position);
    let owner_prefix = &normalized[..action_position];
    let team_position = TEAM_OWNERS
        .iter()
        .filter_map(|term| owner_prefix.rfind(term))
        .max();
    let parent_agent_position = PARENT_AGENT_OWNERS
        .iter()
        .filter_map(|term| owner_prefix.rfind(term))
        .max();

    team_position.is_some_and(|team| parent_agent_position.is_none_or(|agent| team > agent))
}

fn explicitly_forbids_workspace_write(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要修改文件",
            "不修改文件",
            "无需修改文件",
            "不要修改任何文件",
            "不修改任何文件",
            "无需修改任何文件",
            "不得修改文件",
            "不得修改任何文件",
            "禁止修改文件",
            "禁止修改任何文件",
            "不要写文件",
            "不写文件",
            "不得写文件",
            "禁止写文件",
            "不要写入任何文件",
            "不写入任何文件",
            "不得写入任何文件",
            "禁止写入任何文件",
            "只读分析",
            "只读审查",
            "read-only",
            "read only",
            "without modifying files",
            "without changing files",
            "do not modify files",
            "don't modify files",
            "do not write files",
            "don't write files",
        ],
    )
}

fn explicitly_forbids_persisted_artifact(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要生成文件",
            "不生成文件",
            "不要创建文件",
            "不创建文件",
            "不要保存文件",
            "不保存文件",
            "do not create files",
            "don't create files",
            "do not generate files",
            "don't generate files",
        ],
    )
}

fn explicitly_requests_new_artifact(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "生成一个新的",
            "生成新的",
            "创建一个新的",
            "创建新的",
            "制作一个新的",
            "新增文件",
            "另存为",
            "generate a new",
            "create a new",
            "build a new",
            "write a new",
            "save as a new",
        ],
    )
}

fn explicit_workstream_count(normalized: &str) -> u8 {
    const COUNTS: &[(&str, &str, u8)] = &[
        ("二", "two", 2),
        ("两", "two", 2),
        ("三", "three", 3),
        ("四", "four", 4),
        ("五", "five", 5),
        ("六", "six", 6),
        ("七", "seven", 7),
        ("八", "eight", 8),
    ];
    const CHINESE_ROLES: &[&str] = &["研究员", "智能体", "代理", "成员", "工作流", "任务线"];
    const ENGLISH_ROLES: &[&str] = &[
        "researcher",
        "researchers",
        "agent",
        "agents",
        "worker",
        "workers",
        "workstream",
        "workstreams",
    ];
    let mut requested = 0;
    for (chinese, english, count) in COUNTS {
        let arabic = count.to_string();
        let chinese_match = CHINESE_ROLES.iter().any(|role| {
            [
                format!("{chinese}个{role}"),
                format!("{chinese}名{role}"),
                format!("{chinese}{role}"),
                format!("{arabic}个{role}"),
                format!("{arabic}名{role}"),
                format!("{arabic} {role}"),
            ]
            .iter()
            .any(|pattern| normalized.contains(pattern))
                || counted_role_phrase(normalized, chinese, role, false)
                || counted_role_phrase(normalized, &arabic, role, false)
        });
        let english_match = ENGLISH_ROLES.iter().any(|role| {
            normalized.contains(&format!("{english} {role}"))
                || normalized.contains(&format!("{arabic} {role}"))
                || counted_role_phrase(normalized, english, role, true)
                || counted_role_phrase(normalized, &arabic, role, true)
        });
        if chinese_match || english_match {
            requested = requested.max(*count);
        }
    }
    requested
}

/// Return the explicit number of Team entities requested by the user.
///
/// This is deliberately distinct from Agent/workstream cardinality: two
/// research Teams are not satisfied by two Agents inside one Team graph.
#[must_use]
pub fn explicit_team_count(prompt: &str) -> u8 {
    // Cardinality is a semantic contract, while Markdown emphasis/code
    // delimiters are presentation only. Normalize those delimiters before
    // matching so `两个** required Team` means the same as `两个 required Team`.
    let markdown_stripped = prompt.to_ascii_lowercase().replace(['*', '`'], " ");
    let normalized = markdown_stripped
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    const COUNTS: &[(&str, &str, u8)] = &[
        ("一", "one", 1),
        ("二", "two", 2),
        ("两", "two", 2),
        ("三", "three", 3),
        ("四", "four", 4),
        ("五", "five", 5),
        ("六", "six", 6),
        ("七", "seven", 7),
        ("八", "eight", 8),
    ];
    let ordinal_count = explicit_team_ordinal_count(&normalized);
    let mut cardinal_normalized = normalized.clone();
    for (chinese, _, count) in COUNTS {
        for role in ["团队", "研究团队", "协作团队", "组"] {
            cardinal_normalized = cardinal_normalized
                .replace(&format!("第{chinese}个{role}"), "")
                .replace(&format!("第{chinese}{role}"), "")
                .replace(&format!("第{count}个{role}"), "")
                .replace(&format!("第{count}{role}"), "")
                .replace(&format!("第 {count} 个 {role}"), "")
                .replace(&format!("第 {count} {role}"), "");
        }
    }
    let mut requested = 0_u8;
    for (chinese, english, count) in COUNTS.iter().rev() {
        let arabic = count.to_string();
        let chinese_match = ["团队", "研究团队", "协作团队"].iter().any(|role| {
            [
                format!("{chinese}个{role}"),
                format!("{chinese}{role}"),
                format!("{arabic}个{role}"),
                format!("{arabic} {role}"),
            ]
            .iter()
            .any(|pattern| cardinal_normalized.contains(pattern))
                || counted_role_phrase(&cardinal_normalized, chinese, role, false)
                || counted_role_phrase(&cardinal_normalized, &arabic, role, false)
        });
        let english_match = ["team", "teams", "research team", "research teams"]
            .iter()
            .any(|role| {
                cardinal_normalized.contains(&format!("{english} {role}"))
                    || cardinal_normalized.contains(&format!("{arabic} {role}"))
                    || cardinal_normalized.contains(&format!("{chinese}个{role}"))
                    || cardinal_normalized.contains(&format!("{chinese}个 {role}"))
                    || cardinal_normalized.contains(&format!("{chinese}名{role}"))
                    || cardinal_normalized.contains(&format!("{chinese}{role}"))
                    || cardinal_normalized.contains(&format!("{chinese} {role}"))
                    || cardinal_normalized.contains(&format!("{arabic}个{role}"))
                    || cardinal_normalized.contains(&format!("{arabic}个 {role}"))
                    || cardinal_normalized.contains(&format!("{arabic}名{role}"))
                    || counted_role_phrase(&cardinal_normalized, english, role, true)
                    || counted_role_phrase(&cardinal_normalized, &arabic, role, true)
            });
        // Natural-language Chinese often qualifies an English `Team` between
        // its cardinality and the noun (for example, `三个协作 Team`). That is
        // still an explicit cardinality, not three Agents inside one Team.
        // Keep the qualifier set narrowly collaboration-specific so an
        // unrelated phrase cannot accidentally become a Team obligation.
        let qualified_english_team_match =
            ["协作", "独立", "并行", "平行", "required"]
                .iter()
                .any(|qualifier| {
                    ["team", "teams"].iter().any(|role| {
                        [
                            format!("{chinese}{qualifier} {role}"),
                            format!("{chinese}个{qualifier} {role}"),
                            format!("{chinese}个 {qualifier} {role}"),
                            format!("{arabic}{qualifier} {role}"),
                            format!("{arabic}个{qualifier} {role}"),
                            format!("{arabic}个 {qualifier} {role}"),
                            format!("{arabic} {qualifier} {role}"),
                        ]
                        .iter()
                        .any(|pattern| cardinal_normalized.contains(pattern))
                    })
                })
                || qualified_chinese_team_phrase(&cardinal_normalized, chinese)
                || qualified_chinese_team_phrase(&cardinal_normalized, &arabic);
        if chinese_match || english_match || qualified_english_team_match {
            requested = requested.max(*count);
            break;
        }
    }
    for (chinese, english, count) in COUNTS.iter().rev() {
        let arabic = count.to_string();
        if [
            format!("共{chinese}组"),
            format!("总共{chinese}组"),
            format!("{chinese}组"),
            format!("共{arabic}组"),
            format!("总共{arabic}组"),
            format!("{arabic}组"),
            format!("共 {arabic} 组"),
            format!("总共 {arabic} 组"),
            format!("{arabic} 组"),
            format!("{english} groups"),
            format!("{arabic} groups"),
            format!("total {english} groups"),
            format!("total {arabic} groups"),
        ]
        .iter()
        .any(|pattern| cardinal_normalized.contains(pattern))
        {
            requested = requested.max(*count);
            break;
        }
    }
    if [
        "另一个团队",
        "另外一个团队",
        "下一团队",
        "another team",
        "next team",
    ]
    .iter()
    .any(|term| normalized.contains(term))
    {
        requested = requested.max(2);
    }
    if requested == 0 {
        requested = ordinal_count;
    }
    requested
}

/// Whether an explicitly requested multi-Team collaboration includes a typed
/// fan-in requirement. This is ingress interpretation only: the resulting
/// dependency is carried as data in the durable semantic proposal, never
/// inferred later from Team names or a fixed role workflow.
#[must_use]
pub fn explicit_team_fan_in_required(prompt: &str) -> bool {
    let normalized = prompt.to_ascii_lowercase();
    explicit_team_count(&normalized) >= 2
        && [
            "汇合",
            "合并",
            "汇总",
            "整合",
            "merge",
            "aggregate",
            "fan-in",
        ]
        .iter()
        .any(|term| normalized.contains(term))
}

/// Whether the user requires a real Team execution even when no explicit
/// cardinality was given. The result is consumed only while constructing
/// `TaskUnderstanding`; downstream Runtime code uses `required_team_count`.
#[must_use]
pub fn explicit_team_execution_required(prompt: &str) -> bool {
    let normalized = prompt.to_ascii_lowercase();
    let mentions_team = [
        "团队",
        "协作",
        "多agent",
        "多 agent",
        "多智能体",
        "组队",
        "team",
        "multi-agent",
        "multi agent",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    let requires_execution = [
        "实际启动",
        "启动",
        "创建",
        "组建",
        "发起",
        "拉起",
        "用一个团队",
        "使用一个团队",
        "交给团队",
        "由团队",
        "必须",
        "必须要",
        "must",
        "actually",
        "launch",
        "start",
        "create",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    mentions_team && requires_execution && !explicitly_forbids_collaboration(&normalized)
}

/// Whether the user explicitly requires an Agent to use the native Runtime
/// escalation tool to create a follow-up Team.  Keep this deliberately
/// narrow: merely documenting the tool or mentioning collaboration must not
/// grant Runtime authority to add an execution obligation.
#[must_use]
pub fn explicit_managed_collaboration_escalation_required(prompt: &str) -> bool {
    let normalized = prompt.to_ascii_lowercase();
    let names_native_tool = normalized.contains("request_collaboration_escalation");
    let requires_execution = ["必须", "必须要", "must", "required", "actual", "实际调用"]
        .iter()
        .any(|marker| normalized.contains(marker));
    let names_escalation_outcome = [
        "升级",
        "escalation",
        "follow-up team",
        "follow up team",
        "后续 team",
        "后续团队",
        "program revision",
        "runtime-attested",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    names_native_tool && requires_execution && names_escalation_outcome
}

fn explicit_team_ordinal_count(normalized: &str) -> u8 {
    const ORDINALS: &[(&str, &str, u8)] = &[
        ("第一", "first", 1),
        ("第二", "second", 2),
        ("第三", "third", 3),
        ("第四", "fourth", 4),
        ("第五", "fifth", 5),
        ("第六", "sixth", 6),
        ("第七", "seventh", 7),
        ("第八", "eighth", 8),
    ];
    ORDINALS
        .iter()
        .rev()
        .find_map(|(chinese, english, count)| {
            (["团队", "研究团队", "协作团队", "组"].iter().any(|role| {
                normalized.contains(&format!("{chinese}个{role}"))
                    || normalized.contains(&format!("{chinese}{role}"))
                    || normalized.contains(&format!("第{count}个{role}"))
                    || normalized.contains(&format!("第{count}{role}"))
                    || normalized.contains(&format!("第 {count} 个 {role}"))
                    || normalized.contains(&format!("第 {count} {role}"))
            }) || ["team", "research team", "group"]
                .iter()
                .any(|role| normalized.contains(&format!("{english} {role}"))))
            .then_some(*count)
        })
        .unwrap_or_default()
}

fn counted_role_phrase(normalized: &str, count: &str, role: &str, english: bool) -> bool {
    normalized.match_indices(count).any(|(offset, _)| {
        let tail = &normalized[offset + count.len()..];
        let Some(role_offset) = tail.find(role) else {
            return false;
        };
        let separator = &tail[..role_offset];
        if separator.chars().count() > 10
            || separator.chars().any(|character| {
                character.is_ascii_punctuation() && !matches!(character, '-' | '_' | ' ')
                    || matches!(character, '，' | '。' | '；' | '：' | '、' | '！' | '？')
            })
        {
            return false;
        }
        if english {
            return separator.chars().any(char::is_whitespace);
        }
        let compact = separator
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        compact.is_empty() || compact.starts_with('个') || compact.starts_with('名')
    })
}

/// Recognize an explicit Chinese Team count when the user places a short,
/// meaningful qualifier between the counter and an English `Team`, such as
/// `三个回合级自定义 Team workstream` or `3 个 turn-scoped custom Team`.
/// The previous fixed qualifier list
/// silently downgraded that request to a planner preference, so a two-Team
/// strategy lease could incorrectly reject the user's three-Team topology.
fn qualified_chinese_team_phrase(normalized: &str, count: &str) -> bool {
    normalized.match_indices(count).any(|(offset, _)| {
        let tail = &normalized[offset + count.len()..];
        ["team", "teams"].iter().any(|role| {
            let Some(role_offset) = tail.find(role) else {
                return false;
            };
            let qualifier = &tail[..role_offset];
            let compact = qualifier
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            compact.starts_with('个')
                && compact.chars().count() <= 48
                && !qualifier.chars().any(|character| {
                    matches!(character, '，' | '。' | '；' | '：' | '、' | '！' | '？')
                })
        })
    })
}

fn uncertainty_score(normalized: &str, external_facts: bool) -> u8 {
    let mut score = u8::from(external_facts) * 3;
    if contains_any(normalized, DELIBERATION_TERMS) {
        score = score.saturating_add(4);
    }
    if contains_any(
        normalized,
        &["未知", "不确定", "unknown", "hypothesis", "假设"],
    ) {
        score = score.saturating_add(3);
    }
    score.min(10)
}

fn estimate_duration(
    complexity: TaskComplexity,
    background: bool,
    multi_agent: bool,
) -> TaskDuration {
    if background {
        return TaskDuration::LongRunning;
    }
    match complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => TaskDuration::Immediate,
        TaskComplexity::Moderate if !multi_agent => TaskDuration::Short,
        TaskComplexity::Moderate | TaskComplexity::Complex => TaskDuration::Extended,
        TaskComplexity::Strategic => TaskDuration::LongRunning,
    }
}

fn normalize(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

fn rate_bp(count: usize, total: usize) -> u16 {
    if total == 0 {
        return 0;
    }
    ((count as u32 * 10_000) / total as u32).min(10_000) as u16
}

const REVIEW_TERMS: &[&str] = &["review", "审查", "审计", "检查", "code review"];
const BUGFIX_TERMS: &[&str] = &["bug", "fix", "修复", "报错", "失败", "failure", "panic"];
const FRONTEND_TERMS: &[&str] = &["frontend", "ui", "页面", "样式", "tui", "webui", "react"];
const BACKEND_TERMS: &[&str] = &["backend", "runtime", "server", "后端", "api", "service"];
const DOCS_TERMS: &[&str] = &["docs", "文档", "方案", "report", "报告"];
const RELEASE_TERMS: &[&str] = &["release", "发布", "tag", "验收", "回归"];
const TEST_TERMS: &[&str] = &["test", "测试", "e2e", "验证", "cargo test"];
const RESEARCH_TERMS: &[&str] = &[
    "research",
    "调研",
    "研究报告",
    "联网",
    "网络搜索",
    "网上搜索",
    "latest",
    "最新",
    "论文",
    "外部",
];
const ARCHITECTURE_TERMS: &[&str] = &[
    "architecture",
    "架构",
    "重构",
    "内核",
    "crate",
    "harness",
    "系统设计",
];
const WRITE_TERMS: &[&str] = &[
    "implement",
    "实现",
    "修改",
    "重构",
    "新增",
    "删除",
    "rename",
    "extract",
    "迁移",
];
const EXTERNAL_FACT_TERMS: &[&str] = &[
    "latest",
    "最新",
    "today",
    "调研",
    "研究报告",
    "联网",
    "联网搜索",
    "网络搜索",
    "网上搜索",
    "网络工具",
    "websearch",
    "webfetch",
    "真实来源",
    "引用来源",
    "官方来源",
    "自行进行搜索",
    "research",
    "论文",
];
const EXPLICIT_EXTERNAL_FACT_TERMS: &[&str] = &[
    "latest",
    "最新",
    "today",
    "今年",
    "this year",
    "current year",
    "联网",
    "联网搜索",
    "网络搜索",
    "网上搜索",
    "网络工具",
    "websearch",
    "webfetch",
    "真实来源",
    "引用来源",
    "官方来源",
    "自行进行搜索",
    "论文",
];
const LOCAL_EVIDENCE_TERMS: &[&str] = &[
    "workspace",
    "codebase",
    "repository",
    "source code",
    "local file",
    "工作区",
    "代码库",
    "仓库",
    "源码",
    "本地代码",
    "本地文件",
    "目录",
];

fn requires_external_facts(normalized: &str) -> bool {
    if contains_any(normalized, EXPLICIT_EXTERNAL_FACT_TERMS) {
        return true;
    }
    contains_any(normalized, EXTERNAL_FACT_TERMS) && !contains_any(normalized, LOCAL_EVIDENCE_TERMS)
}
const TOOL_EVIDENCE_TERMS: &[&str] = &[
    // An explicit evidence/tool instruction is not merely a stylistic model
    // preference. It changes the acceptance contract: the answer must be
    // grounded in an observable capability result, so Direct must not hide
    // the tool catalog behind its minimal bootstrap set.
    "工具",
    "tool",
    "证据",
    "evidence",
    "读取",
    "read_file",
];
const PARALLEL_TERMS: &[&str] = &[
    "parallel",
    "simultaneously",
    "并行",
    "同时",
    "fanout",
    "多路",
];
const MULTI_AGENT_TERMS: &[&str] = &[
    "multi-agent",
    "多agent",
    "多 agent",
    "多智能体",
    "subagent",
    "协同",
    "组队",
    "团队",
    "team",
];
const DEEP_PLAN_TERMS: &[&str] = &[
    "全面",
    "完整",
    "彻底",
    "阶段",
    "规划",
    "演进",
    "沉浸式",
    "终极",
];
const DELIBERATION_TERMS: &[&str] = &[
    "debate",
    "deliberate",
    "tradeoff",
    "权衡",
    "争议",
    "冲突方案",
    "对抗性审查",
];
const BACKGROUND_TERMS: &[&str] = &[
    "background",
    "后台",
    "长期运行",
    "持续监控",
    "overnight",
    "异步审查",
];
const SINGLE_FILE_TERMS: &[&str] = &["单文件", "one file", "small fix", "小修"];
const HIGH_RISK_TERMS: &[&str] = &[
    "删除",
    "迁移",
    "重构",
    "全局",
    "workspace",
    "schema",
    "database",
    "权限",
];
const CRITICAL_RISK_TERMS: &[&str] = &[
    "生产数据库",
    "drop table",
    "reset --hard",
    "force push",
    "密钥",
    "secret",
];

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
