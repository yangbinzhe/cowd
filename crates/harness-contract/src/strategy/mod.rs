//! Strategy routing for Cowd AI work kernel.
//!
//! This crate owns deterministic task understanding and execution-mode
//! selection. It does not execute tools, assemble prompts, or mutate task
//! state; later layers consume its `StrategyDecision`.

use crate::core::{
    ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, KernelCapability, TaskComplexity,
    TaskRisk,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
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
    pub requests_parallelism: bool,
    pub requests_multi_agent: bool,
    #[serde(default)]
    pub forbids_team: bool,
    pub requests_deep_plan: bool,
    pub requests_deliberation: bool,
    pub requests_background: bool,
    pub likely_single_file: bool,
    pub independent_workstreams: u8,
    pub uncertainty: u8,
    pub estimated_duration: TaskDuration,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyResourceSnapshot {
    pub version: String,
    pub provider_available: bool,
    pub tools_available: bool,
    pub team_available: bool,
    pub provider_concurrency: u16,
    pub tool_concurrency: u16,
    pub team_slots: u16,
    pub provider_concurrency_penalty_bp: u16,
    /// SHA-256 of the effective provider/model profile. The raw provider or
    /// model name is deliberately excluded from public strategy projections.
    #[serde(default)]
    pub provider_profile_fingerprint: String,
    pub sample_source: String,
    pub sample_count: u32,
    pub assumed: bool,
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
            provider_profile_fingerprint: String::new(),
            sample_source: "assumed-detached-default".to_string(),
            sample_count: 0,
            assumed: true,
        }
    }
}

/// Integer-only estimate for one strategy candidate. Milliseconds, tokens and
/// basis points keep ordering stable across platforms and serializations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub net_benefit_score: i64,
    pub calibration_source: String,
    pub calibration_sample_count: u32,
    pub assumed: bool,
    pub reasons: Vec<String>,
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
    pub fn from_input(input: &StrategyInput, understanding: &TaskUnderstanding) -> Self {
        let tool_dag_shape = if understanding.requires_write {
            if understanding.independent_workstreams > 1 {
                "mixed_read_serial_write"
            } else {
                "bounded_serial_write"
            }
        } else if understanding.requires_external_facts
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
            return Err("negative Team observation has incomplete provenance or bounds".to_string());
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
        "完整执行一次受限写入与独立复核，覆盖 runtime 写入责任和 harness-eval 验收责任：只修改 fixtures/v546-write/target.txt，使其内容精确等于 {{EXPECTED_CONTENT}}（含末尾换行）；必须先读取、写入不同内容、再读取并核对写后摘要，随后用独立验证步骤重新读取目标并核对 change/evidence。不得修改 fixtures/v546-protected/sentinel.txt 或任何其他路径，最终说明实现、源验证、独立复核、风险与证据。",
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

    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(path, bytes)
    }

    pub fn load_or_default(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self::load(path).unwrap_or_default()
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
                || existing.workload_fingerprint_sha256
                    != observation.workload_fingerprint_sha256
                || existing.provider_profile_fingerprint
                    != observation.provider_profile_fingerprint
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
                != Some("dontAsk")
            || invariants
                .get("workspace_fixture")
                .and_then(serde_json::Value::as_str)
                != Some("workspace-v546-frozen")
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
                                            "fixtures/v546-write/target.txt".to_string(),
                                        )]
                                })
                            || sample
                                .get("write_attempt_paths")
                                .and_then(serde_json::Value::as_array)
                                .is_none_or(|paths| {
                                    paths
                                        != &[serde_json::Value::String(
                                            "fixtures/v546-write/target.txt".to_string(),
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyPolicy {
    pub enable_parallel_evidence: bool,
    pub enable_multi_agent: bool,
    pub require_verifier_for_complex: bool,
    pub require_guardrails_for_writes: bool,
}

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
        );
        let workload_fingerprint =
            StrategyWorkloadFingerprint::from_input(input, &understanding).digest();
        let negative_team_veto = (!understanding.requests_multi_agent)
            .then(|| {
                input.negative_benefit_observations.iter().find(|observation| {
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
                team.net_benefit_score = i64::MIN / 2;
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
        if !understanding.requests_multi_agent
            && candidate_estimates.iter().any(|estimate| {
                estimate.candidate == ExecutionCandidateKind::Team
                    && estimate.eligible
                    && estimate.assumed
            })
        {
            reasons.push(
                "automatic Team requires observed positive net benefit; heuristic-only estimate is insufficient"
                    .to_string(),
            );
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
                "integer cost model selected {}",
                selected_candidate.as_str()
            ));
        }
        let explicit_team_negative_benefit = understanding.requests_multi_agent
            && selected_candidate == ExecutionCandidateKind::Team
            && candidate_estimates.iter().any(|estimate| {
                estimate.candidate == ExecutionCandidateKind::Team && estimate.net_benefit_score < 0
            });
        if pattern == ExecutionPattern::Collaborate && explicit_team_negative_benefit {
            reasons.push(
                "explicit Team override retained despite a negative estimated lift; surface must show the cost warning"
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
            policy_version: "strategy-decision-v5".to_string(),
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
    let parallel_eligible = resources.tools_available
        && (understanding.requests_parallelism
            || understanding.requires_external_facts
            || (workstreams >= 2 && !understanding.requires_write));
    let team_resource_eligible = resources.team_available
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
        if understanding.requires_external_facts {
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
            estimate.calibration_source = cost.map_or_else(
                || "assumed-policy-v1".to_string(),
                |sample| sample.calibration_source.clone(),
            );
            estimate.calibration_sample_count = cost.map_or(0, |sample| sample.sample_count);
            estimate.assumed = cost.is_none();
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
    let saved_ms = estimated_serial_ms
        .saturating_sub(estimated_critical_path_ms)
        .saturating_sub(startup_overhead_ms)
        .saturating_sub(merge_cost_ms);
    let penalty = i64::from(evidence_overlap_penalty_bp)
        .saturating_add(i64::from(provider_concurrency_penalty_bp))
        .saturating_add(i64::from(risk_approval_penalty_bp))
        .saturating_add(i64::try_from(context_duplication_tokens / 4).unwrap_or(i64::MAX));
    let net_benefit_score = if eligible {
        i64::try_from(saved_ms)
            .unwrap_or(i64::MAX)
            .saturating_add(i64::from(expected_quality_lift_bp).saturating_mul(2))
            .saturating_sub(penalty)
    } else {
        i64::MIN / 2
    };
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
        net_benefit_score,
        calibration_source: String::new(),
        calibration_sample_count: 0,
        assumed: true,
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
    // Automatic Team selection is allowed only for a genuinely strategic,
    // multi-domain task.  A high provider-concurrency penalty is a runtime
    // capacity fact: it blocks automatic fan-out, but must not erase an
    // explicit Team request (which is retained with its cost warning).
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
                estimate.eligible && !estimate.assumed && estimate.net_benefit_score > 0
            })
        {
            return team.candidate;
        }
    }
    if understanding.requires_external_facts {
        return estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::ParallelTools)
            .filter(|estimate| estimate.eligible)
            .map_or(ExecutionCandidateKind::Direct, |estimate| {
                estimate.candidate
            });
    }
    estimates
        .iter()
        .filter(|estimate| estimate.eligible)
        .filter(|estimate| {
            estimate.candidate != ExecutionCandidateKind::Team
                || (resources.provider_concurrency_penalty_bp < 8_000
                    && !estimate.assumed
                    && estimate.net_benefit_score > 0)
        })
        .max_by_key(|estimate| {
            (
                estimate.net_benefit_score,
                std::cmp::Reverse(estimate.candidate),
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
    let requires_write = input.explicit_write || contains_any(&normalized, WRITE_TERMS);
    // A user can mention tools only to rule them out for an otherwise direct
    // request. Do not turn an explicit prohibition into an evidence-seeking
    // strategy merely because the word "tool" occurs in the prompt.
    let requires_external_facts =
        contains_any(&normalized, EXTERNAL_FACT_TERMS) && !explicitly_forbids_tool_use(&normalized);
    let requests_parallelism = contains_any(&normalized, PARALLEL_TERMS);
    // A request may mention teams solely to prohibit them. Treating every
    // occurrence of "team" as an affirmative collaboration request turns a
    // user-selected single-agent execution mode into its opposite.
    let forbids_team = explicitly_forbids_collaboration(&normalized);
    let requests_multi_agent = contains_any(&normalized, MULTI_AGENT_TERMS) && !forbids_team;
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
        requests_parallelism,
        requests_multi_agent,
        forbids_team,
        requests_deep_plan,
        requests_deliberation,
        requests_background,
        likely_single_file,
        independent_workstreams: independent_workstreams(&normalized),
        uncertainty: uncertainty_score(&normalized, requires_external_facts),
        estimated_duration: estimate_duration(
            complexity,
            requests_background,
            requests_multi_agent,
        ),
    }
}

fn explicitly_forbids_collaboration(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要组队",
            "不要团队",
            "不要启动团队",
            "不要启动协作",
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
    if understanding.requires_external_facts {
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
        "gateway", "runtime", "tui", "service", "crate", "agent", "context",
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
    let independence = i16::from(understanding.independent_workstreams) * 1_500;
    let verification = i16::from(matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    )) * 1_500;
    let uncertainty = i16::from(understanding.uncertainty) * 100;
    let historical = experience
        .filter(|summary| summary.multi_agent_lift_sample_count >= 3)
        .map_or(0, |summary| summary.multi_agent_lift_rate_bp as i16 - 5_000);
    let coordination_cost_bp = match understanding.complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => 4_500,
        TaskComplexity::Moderate => 3_000,
        TaskComplexity::Complex => 2_000,
        TaskComplexity::Strategic => 1_500,
    };
    let expected_lift_bp =
        independence + verification + uncertainty + historical - coordination_cost_bp as i16;
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
        "app-mfg",
    ]
    .iter()
    .filter(|term| normalized.contains(**term))
    .count();
    domains.clamp(1, 8) as u8
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
const RESEARCH_TERMS: &[&str] = &["research", "调研", "latest", "最新", "论文", "外部"];
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
    "现在",
    "当前",
    "调研",
    "research",
    "web",
    "论文",
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
    "subagent",
    "协同",
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
mod tests {
    use super::*;

    fn with_proven_team_benefit(prompt: &str) -> StrategyInput {
        let mut input = StrategyInput::from_prompt(prompt);
        input.candidate_costs.insert(
            ExecutionCandidateKind::Direct,
            StrategyCandidateCostSummary {
                sample_count: 3,
                average_critical_path_ms: 40_000,
                average_total_tokens: 1_000,
                average_coordination_cost_ms: 0,
                calibration_source: "test:observed-direct".to_string(),
            },
        );
        input.candidate_costs.insert(
            ExecutionCandidateKind::Team,
            StrategyCandidateCostSummary {
                sample_count: 3,
                average_critical_path_ms: 20_000,
                average_total_tokens: 1_200,
                average_coordination_cost_ms: 1_000,
                calibration_source: "test:observed-team".to_string(),
            },
        );
        input
    }

    fn proposal(pattern: ExecutionPattern, modifiers: Vec<ExecutionModifier>) -> StrategyProposal {
        StrategyProposal {
            pattern,
            modifiers,
            template: None,
            confidence: 90,
            rationale: "test proposal".to_string(),
        }
    }

    fn assert_contract_legal(decision: &StrategyDecision) {
        assert!(
            decision
                .modifiers
                .iter()
                .all(|modifier| decision.pattern.supports_modifier(*modifier))
        );
        assert!(
            decision
                .gates
                .iter()
                .all(|gate| decision.pattern.supports_gate(*gate))
        );
    }

    fn paired_calibration(
        id: impl std::fmt::Display,
        demonstrates_positive_lift: bool,
    ) -> PairedStrategyCalibrationEvidence {
        let mut evidence = PairedStrategyCalibrationEvidence {
            evaluation_ref: format!("harness_eval.auto_strategy_paired.v1:{id}"),
            corpus_sha256: "a".repeat(64),
            workspace_revision: "workspace-revision".to_string(),
            provider_account_ref: "provider-account".to_string(),
            baseline_pattern: ExecutionPattern::Direct,
            baseline_duration_ms: 100,
            baseline_quality_score_bp: 8_000,
            candidate_duration_ms: if demonstrates_positive_lift { 80 } else { 120 },
            candidate_quality_score_bp: 8_000,
            blind_judge_completed: true,
            baseline_total_tokens: 100,
            candidate_total_tokens: 150,
            candidate_duplicate_tool_ratio_bp: 0,
            admission_channel: None,
            report_sha256: "b".repeat(64),
            rubric_sha256: "c".repeat(64),
            binary_sha256: "d".repeat(64),
            frontend_workspace_revision: "frontend-revision".to_string(),
            model_revision: "test-model".to_string(),
            judge_model_revision: "test-judge".to_string(),
            invariant_fingerprint: "e".repeat(64),
        };
        evidence.admission_channel = evidence.registered_admission_channel();
        evidence
    }

    #[test]
    fn routes_simple_question_to_direct() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释一下这个函数有什么用"));

        assert_eq!(decision.pattern, ExecutionPattern::Direct);
        assert!(decision.confidence >= 80);
        assert!(!decision.uses_modifier(ExecutionModifier::WithVerifier));
    }

    #[test]
    fn explicit_tool_evidence_and_team_requests_do_not_fall_back_to_direct() {
        let tool = decide_strategy(&StrategyInput::from_prompt(
            "必须通过只读工具读取 Cargo.toml 并提供证据",
        ));
        assert_eq!(tool.pattern, ExecutionPattern::Explore);
        assert!(tool.understanding.requires_external_facts);

        let team = decide_strategy(&StrategyInput::from_prompt(
            "请实际启动协作团队，分别审查 runtime、memory、gateway 后综合结论",
        ));
        assert_eq!(team.pattern, ExecutionPattern::Collaborate);
        assert!(team.understanding.requests_multi_agent);
    }

    #[test]
    fn explicit_tool_prohibition_does_not_create_external_evidence_work() {
        let prompt = "只回答 7 乘以 8 的结果。不要调用工具，不要组队。";
        let decision = decide_strategy(&StrategyInput::from_prompt(prompt));

        assert!(prompt_explicitly_forbids_tool_use(prompt));
        assert_eq!(decision.pattern, ExecutionPattern::Direct);
        assert!(!decision.understanding.requires_external_facts);
        assert!(!decision.understanding.requests_multi_agent);
    }

    #[test]
    fn routes_bounded_write_to_execute_with_bounded_modifier() {
        let decision = decide_strategy(
            &StrategyInput::from_prompt("修复这个单文件小问题")
                .with_explicit_write(true)
                .with_changed_files(1),
        );

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
        assert!(decision.uses_modifier(ExecutionModifier::WithGuardrails));
        assert!(decision.uses_modifier(ExecutionModifier::BoundedChange));
    }

    #[test]
    fn routes_architecture_work_to_execute() {
        let decision = decide_strategy(&with_proven_team_benefit(
            "全面重构 runtime gateway service crate 的架构，做完整阶段规划",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
        assert_eq!(decision.understanding.complexity, TaskComplexity::Strategic);
        assert!(decision.uses_modifier(ExecutionModifier::WithVerifier));
        assert_eq!(decision.policy_version, "strategy-decision-v5");
        assert!(
            decision
                .required_capabilities
                .contains(&KernelCapability::ExecutionGraph)
        );
        assert!(
            decision
                .required_capabilities
                .contains(&KernelCapability::VerificationLedger)
        );
    }

    #[test]
    fn routes_parallel_research_to_explore_with_parallel_modifier() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "并行调研最新 AI harness 实践并汇总",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Explore);
        assert!(decision.uses_modifier(ExecutionModifier::WithExternalResearch));
        assert!(decision.uses_modifier(ExecutionModifier::Parallel));
    }

    #[test]
    fn routes_multi_agent_request_to_collaborate() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "使用多 Agent 协同完成复杂架构分析",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
        assert!(decision.uses_modifier(ExecutionModifier::WithReviewer));
        assert_contract_legal(&decision);
    }

    #[test]
    fn deterministic_candidate_corpus_has_six_cases_per_candidate() {
        let direct = [
            "explain this constant",
            "summarize one paragraph",
            "answer a stable question",
            "clarify this name",
            "describe one function",
            "give a concise definition",
        ];
        let parallel = [
            "parallel read evidence for this API",
            "并行调研当前工具证据",
            "simultaneously inspect independent read-only facts",
            "fanout read-only checks and summarize",
            "多路读取证据但不要组队",
            "parallel research latest references",
        ];
        let team = [
            "全面审查 runtime gateway frontend 三个独立责任域并综合",
            "analyze runtime gateway webui as independent ownership domains",
            "deep architecture review across runtime memory matrix",
            "全面核对 gateway tui webui 的独立职责和验收",
            "plan runtime gateway frontend backend responsibilities with independent judgment",
            "cross-check runtime gateway memory matrix as separate accountable domains",
        ];
        for prompt in direct {
            let decision = decide_strategy(&StrategyInput::from_prompt(prompt));
            assert_eq!(
                decision.selected_candidate,
                ExecutionCandidateKind::Direct,
                "{prompt}"
            );
        }
        for prompt in parallel {
            let decision = decide_strategy(&StrategyInput::from_prompt(prompt));
            assert_eq!(
                decision.selected_candidate,
                ExecutionCandidateKind::ParallelTools,
                "{prompt}"
            );
        }
        for prompt in team {
            let decision = decide_strategy(&with_proven_team_benefit(prompt));
            assert_eq!(
                decision.selected_candidate,
                ExecutionCandidateKind::Team,
                "{prompt}"
            );
            assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
            assert!(!decision.understanding.requests_multi_agent);
        }
    }

    #[test]
    fn candidate_resource_constraints_and_explicit_override_are_deterministic() {
        let prompt = "全面审查 runtime gateway frontend 三个独立责任域并综合";
        let no_team = StrategyResourceSnapshot {
            team_available: false,
            team_slots: 0,
            ..StrategyResourceSnapshot::default()
        };
        let constrained =
            decide_strategy(&StrategyInput::from_prompt(prompt).with_resource_snapshot(no_team));
        assert_ne!(constrained.selected_candidate, ExecutionCandidateKind::Team);

        let provider_constrained = StrategyResourceSnapshot {
            provider_concurrency_penalty_bp: 9_000,
            ..StrategyResourceSnapshot::default()
        };
        let constrained = decide_strategy(
            &StrategyInput::from_prompt(prompt).with_resource_snapshot(provider_constrained),
        );
        assert_ne!(constrained.selected_candidate, ExecutionCandidateKind::Team);

        let explicit = decide_strategy(&StrategyInput::from_prompt(
            "必须启动 Team 分别负责 runtime gateway frontend 并综合",
        ));
        assert_eq!(explicit.selected_candidate, ExecutionCandidateKind::Team);
        assert_eq!(explicit.pattern, ExecutionPattern::Collaborate);
    }

    #[test]
    fn explicit_simple_team_uses_hard_resources_not_auto_benefit_heuristics() {
        let explicit = decide_strategy(&StrategyInput::from_prompt(
            "必须启动 Team，让两个 Agent 分别回答 hello 并汇总。",
        ));
        assert_eq!(explicit.selected_candidate, ExecutionCandidateKind::Team);

        let unavailable = decide_strategy(
            &StrategyInput::from_prompt("必须启动 Team，让两个 Agent 回答 hello。")
                .with_resource_snapshot(StrategyResourceSnapshot {
                    team_available: false,
                    team_slots: 0,
                    ..StrategyResourceSnapshot::default()
                }),
        );
        assert_eq!(
            unavailable.selected_candidate,
            ExecutionCandidateKind::Direct
        );
    }

    #[test]
    fn explicit_team_negative_candidate_benefit_emits_surface_cost_warning() {
        let mut input =
            StrategyInput::from_prompt("必须启动 Team 分别负责 runtime gateway frontend 并综合");
        input.candidate_costs.insert(
            ExecutionCandidateKind::Team,
            StrategyCandidateCostSummary {
                sample_count: 3,
                average_critical_path_ms: 200_000,
                average_total_tokens: 50_000,
                average_coordination_cost_ms: 20_000,
                calibration_source: "test:negative-team".to_string(),
            },
        );
        input.resource_snapshot.provider_concurrency_penalty_bp = 10_000;

        let decision = decide_strategy(&input);
        let team = decision
            .candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            .expect("Team estimate");

        assert_eq!(decision.selected_candidate, ExecutionCandidateKind::Team);
        assert!(team.net_benefit_score < 0);
        assert!(decision.reasons.iter().any(|reason| {
            reason.contains("negative estimated lift")
                && reason.contains("surface must show the cost warning")
        }));
    }

    #[test]
    fn explicit_team_prohibition_blocks_auto_team_for_multiple_domains() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "不要组队，也不要启动多 Agent；只用单一 owner 审查 runtime、gateway、webui 三个责任域。",
        ));

        assert!(decision.understanding.forbids_team);
        assert!(!decision.understanding.requests_multi_agent);
        assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
        assert_ne!(decision.pattern, ExecutionPattern::Collaborate);
    }

    #[test]
    fn every_candidate_records_integer_costs_and_snapshot_provenance() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "全面审查 runtime gateway frontend 三个独立责任域并综合",
        ));
        assert_eq!(decision.candidate_estimates.len(), 3);
        assert_eq!(
            decision
                .candidate_estimates
                .iter()
                .map(|estimate| estimate.candidate)
                .collect::<Vec<_>>(),
            vec![
                ExecutionCandidateKind::Direct,
                ExecutionCandidateKind::ParallelTools,
                ExecutionCandidateKind::Team,
            ]
        );
        assert_eq!(decision.resource_snapshot.version, "strategy-resource-v1");
        assert!(decision.resource_snapshot.assumed);
        assert_eq!(decision.resource_snapshot.sample_count, 0);
    }

    #[test]
    fn candidate_cost_history_is_not_reused_or_divided_across_topologies() {
        let mut input =
            StrategyInput::from_prompt("必须启动 Team 分别负责 runtime gateway frontend 并综合");
        input.candidate_costs.insert(
            ExecutionCandidateKind::Direct,
            StrategyCandidateCostSummary {
                sample_count: 3,
                average_critical_path_ms: 40_000,
                average_total_tokens: 1_000,
                average_coordination_cost_ms: 0,
                calibration_source: "test:direct".to_string(),
            },
        );
        input.candidate_costs.insert(
            ExecutionCandidateKind::Team,
            StrategyCandidateCostSummary {
                sample_count: 3,
                average_critical_path_ms: 30_000,
                average_total_tokens: 1_500,
                average_coordination_cost_ms: 2_000,
                calibration_source: "test:team".to_string(),
            },
        );

        let decision = decide_strategy(&input);
        let direct = decision
            .candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Direct)
            .expect("direct estimate");
        let parallel = decision
            .candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::ParallelTools)
            .expect("parallel estimate");
        let team = decision
            .candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            .expect("team estimate");

        assert_eq!(direct.estimated_critical_path_ms, 40_000);
        assert_eq!(team.estimated_critical_path_ms, 30_000);
        assert_ne!(team.estimated_critical_path_ms, 10_000);
        assert_eq!(direct.calibration_source, "test:direct");
        assert_eq!(team.calibration_source, "test:team");
        assert!(parallel.assumed);
        assert_eq!(parallel.calibration_source, "assumed-policy-v1");
    }

    #[test]
    fn fast_failed_team_runs_never_become_cheap_candidate_cost_calibration() {
        let input =
            StrategyInput::from_prompt("必须启动 Team 分别审查 runtime gateway frontend 并综合");
        let understanding = understand(&input);
        let mut store = StrategyExperienceStore::new();
        for index in 0..3 {
            store.record(StrategyExperienceRecord {
                domain: understanding.domain,
                complexity: understanding.complexity,
                risk: understanding.risk,
                selected_pattern: ExecutionPattern::Collaborate,
                selected_candidate: Some(ExecutionCandidateKind::Team),
                succeeded: false,
                verification_blocked: index == 2,
                context_pressure: false,
                composite_execution: false,
                multi_agent_positive_lift: false,
                created_at_ms: index,
                actual_duration_ms: 10,
                actual_input_tokens: 1,
                actual_output_tokens: 1,
                actual_cached_tokens: 0,
                actual_coordination_cost_ms: 1,
                paired_calibration: None,
            });
        }

        assert!(
            store
                .cost_summary_for_candidate(&understanding, ExecutionCandidateKind::Team)
                .is_none()
        );
        let enriched = store.enrich_input(input);
        assert!(
            !enriched
                .candidate_costs
                .contains_key(&ExecutionCandidateKind::Team)
        );
    }

    #[test]
    fn partial_team_then_successful_fallback_is_not_a_pure_candidate_cost_sample() {
        let input =
            StrategyInput::from_prompt("必须启动 Team 分别审查 runtime gateway frontend 并综合");
        let understanding = understand(&input);
        let mut store = StrategyExperienceStore::new();
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_pattern: ExecutionPattern::Direct,
            selected_candidate: Some(ExecutionCandidateKind::Direct),
            succeeded: true,
            verification_blocked: false,
            context_pressure: false,
            composite_execution: true,
            multi_agent_positive_lift: false,
            created_at_ms: 1,
            actual_duration_ms: 2,
            actual_input_tokens: 1,
            actual_output_tokens: 1,
            actual_cached_tokens: 0,
            actual_coordination_cost_ms: 1,
            paired_calibration: None,
        });

        assert!(
            store
                .cost_summary_for_candidate(&understanding, ExecutionCandidateKind::Direct)
                .is_none()
        );
    }

    #[test]
    fn topology_neutral_bounded_write_review_is_selected_by_cost_not_team_keywords() {
        let prompt = FROZEN_TEAM_CALIBRATION_TASKS
            .iter()
            .find(|(task_id, _)| *task_id == "AS-T04-bounded-implementation-review")
            .map(|(_, prompt)| *prompt)
            .expect("frozen write task");
        let decision = decide_strategy(&StrategyInput::from_prompt(prompt));

        assert!(!decision.understanding.requests_multi_agent);
        assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
    }

    #[test]
    fn negative_team_constraint_is_not_routed_as_collaboration() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "请单人执行这次架构审查，不要启动团队或多 Agent。",
        ));

        assert!(!decision.understanding.requests_multi_agent);
        assert_ne!(decision.pattern, ExecutionPattern::Collaborate);
    }

    #[test]
    fn rejects_model_proposal_with_unsupported_modifier() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释这个函数").with_proposal(
            proposal(
                ExecutionPattern::Execute,
                vec![ExecutionModifier::Background],
            ),
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Direct);
        assert_eq!(decision.source, StrategyDecisionSource::Deterministic);
        assert!(!decision.uses_modifier(ExecutionModifier::Background));
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("rejected by contract policy"))
        );
        assert_contract_legal(&decision);
    }

    #[test]
    fn accepts_model_proposal_with_supported_modifier() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释这个函数").with_proposal(
            proposal(ExecutionPattern::Explore, vec![ExecutionModifier::Parallel]),
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Explore);
        assert_eq!(decision.source, StrategyDecisionSource::ModelValidated);
        assert!(decision.uses_modifier(ExecutionModifier::Parallel));
        assert_contract_legal(&decision);
    }

    #[test]
    fn six_patterns_generate_their_key_policy_gates() {
        use ExecutionPolicyGate::{Approval, Budget, Permission, Risk};

        let direct = decide_strategy(&StrategyInput::from_prompt("解释这个值"));
        let explore = decide_strategy(
            &StrategyInput::from_prompt("收集资料并更新记录")
                .with_explicit_write(true)
                .with_proposal(proposal(ExecutionPattern::Explore, Vec::new())),
        );
        let execute = decide_strategy(
            &StrategyInput::from_prompt("force push secret change").with_explicit_write(true),
        );
        let deliberate = decide_strategy(
            &StrategyInput::from_prompt("对两个方案做 tradeoff").with_changed_files(21),
        );
        let collaborate = decide_strategy(
            &StrategyInput::from_prompt(
                "使用多 Agent 协同分析 runtime gateway memory 的 secret 变更",
            )
            .with_explicit_write(true)
            .with_proposal(proposal(ExecutionPattern::Collaborate, Vec::new())),
        );
        let supervise = decide_strategy(
            &StrategyInput::from_prompt("后台持续监控 secret 变更")
                .with_explicit_write(true)
                .with_proposal(proposal(ExecutionPattern::Supervise, Vec::new())),
        );

        assert_eq!(direct.pattern, ExecutionPattern::Direct);
        assert_eq!(direct.gates, vec![Budget]);
        assert_eq!(explore.pattern, ExecutionPattern::Explore);
        assert_eq!(explore.gates, vec![Budget, Permission]);
        assert_eq!(execute.pattern, ExecutionPattern::Execute);
        assert_eq!(execute.gates, vec![Budget, Permission, Risk, Approval]);
        assert_eq!(deliberate.pattern, ExecutionPattern::Deliberate);
        assert_eq!(deliberate.gates, vec![Budget, Risk]);
        assert_eq!(collaborate.pattern, ExecutionPattern::Collaborate);
        assert_eq!(collaborate.gates, vec![Budget, Permission, Risk, Approval]);
        assert_eq!(supervise.pattern, ExecutionPattern::Supervise);
        assert_eq!(supervise.gates, vec![Budget, Permission, Risk, Approval]);

        for decision in [direct, explore, execute, deliberate, collaborate, supervise] {
            assert_contract_legal(&decision);
        }
    }

    #[test]
    fn same_input_is_stable_across_all_six_patterns() {
        let cases = [
            ("解释一下这个函数有什么用", ExecutionPattern::Direct),
            (
                "调研最新 AI harness 实践并汇总证据",
                ExecutionPattern::Explore,
            ),
            ("实现并修复这个单文件小问题", ExecutionPattern::Execute),
            (
                "权衡两个架构方案并解决冲突方案",
                ExecutionPattern::Deliberate,
            ),
            (
                "使用多 Agent 协同完成复杂架构分析",
                ExecutionPattern::Collaborate,
            ),
            ("后台持续监控这项长期运行任务", ExecutionPattern::Supervise),
        ];

        for (prompt, expected_pattern) in cases {
            let input = StrategyInput::from_prompt(prompt);
            let first = decide_strategy(&input);
            let second = decide_strategy(&input);
            let wire = serde_json::to_value(&first).expect("strategy decision wire payload");

            assert_eq!(first.pattern, expected_pattern, "prompt: {prompt}");
            assert_eq!(first, second, "prompt: {prompt}");
            assert_eq!(wire["pattern"], expected_pattern.as_str());
            assert!(wire.get("mode").is_none());
        }
    }

    #[test]
    fn critical_risk_requires_approval_gate() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "force push 并 reset --hard 清理所有内容",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
        assert!(decision.uses_gate(ExecutionPolicyGate::Risk));
        assert!(decision.uses_gate(ExecutionPolicyGate::Approval));
    }

    #[test]
    fn strategy_experience_can_downgrade_low_lift_multi_agent() {
        let decision = decide_strategy(
            &StrategyInput::from_prompt("使用多 Agent 协同完成复杂架构分析").with_experience(
                StrategyExperienceSummary {
                    sample_count: 5,
                    success_rate_bp: 5000,
                    verification_block_rate_bp: 0,
                    context_pressure_rate_bp: 0,
                    multi_agent_lift_rate_bp: 2000,
                    multi_agent_lift_sample_count: 5,
                    average_duration_ms: 0,
                    average_total_tokens: 0,
                    average_coordination_cost_ms: 0,
                    actual_cost_sample_count: 0,
                },
            ),
        );

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("low multi-agent lift"))
        );
    }

    #[test]
    fn strategy_experience_store_summarizes_comparable_records() {
        let input = StrategyInput::from_prompt("使用多 Agent 协同完成复杂架构分析");
        let understanding = understand(&input);
        let mut store = StrategyExperienceStore::new();
        for index in 0..4 {
            store.record(StrategyExperienceRecord {
                domain: understanding.domain,
                complexity: understanding.complexity,
                risk: understanding.risk,
                selected_pattern: ExecutionPattern::Collaborate,
                selected_candidate: Some(ExecutionCandidateKind::Team),
                succeeded: index < 3,
                verification_blocked: index == 3,
                context_pressure: index >= 2,
                composite_execution: false,
                multi_agent_positive_lift: index == 0,
                created_at_ms: index,
                actual_duration_ms: 100 + index,
                actual_input_tokens: 10,
                actual_output_tokens: 5,
                actual_cached_tokens: 0,
                actual_coordination_cost_ms: 2,
                paired_calibration: Some(paired_calibration(index, index == 0)),
            });
        }

        let summary = store.summary_for(&understanding).expect("summary");

        assert_eq!(summary.sample_count, 4);
        assert_eq!(summary.success_rate_bp, 7500);
        assert_eq!(summary.verification_block_rate_bp, 2500);
        assert_eq!(summary.context_pressure_rate_bp, 5000);
        assert_eq!(summary.multi_agent_lift_rate_bp, 0);
        assert_eq!(summary.multi_agent_lift_sample_count, 0);
        assert_eq!(summary.average_total_tokens, 15);
        assert_eq!(summary.actual_cost_sample_count, 4);
    }

    #[test]
    fn strategy_experience_store_persists_json() {
        let decision = decide_strategy(&StrategyInput::from_prompt("修复这个单文件小问题"));
        let mut store = StrategyExperienceStore::new();
        store.record(StrategyExperienceRecord::from_decision(
            &decision, true, false, false, true, 1,
        ));
        let path = std::env::temp_dir().join(format!(
            "cowd-strategy-experience-{}.json",
            std::process::id()
        ));

        store.save(&path).unwrap();
        let loaded = StrategyExperienceStore::load(&path).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.records[0].selected_pattern, decision.pattern);
    }

    #[test]
    fn paired_calibration_import_is_provenance_gated_and_idempotent() {
        let mut records = Vec::new();
        let mut samples = Vec::new();
        let mut comparisons = Vec::new();
        for (task_id, prompt) in FROZEN_TEAM_CALIBRATION_TASKS {
            let understanding = understand(&StrategyInput::from_prompt(prompt.to_string()));
            comparisons.push(serde_json::json!({
                "task_id": task_id,
                "strongest_non_team_baseline": "direct",
                "valid_pair_count": 3,
            }));
            for repetition in 0..3 {
                records.push(StrategyExperienceRecord {
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
                    actual_duration_ms: 80,
                    actual_input_tokens: 10,
                    actual_output_tokens: 5,
                    actual_cached_tokens: 0,
                    actual_coordination_cost_ms: 2,
                    paired_calibration: Some(PairedStrategyCalibrationEvidence {
                        evaluation_ref: format!(
                            "harness_eval.auto_strategy_paired.v1:auto-strategy-v1:{task_id}:{repetition}"
                        ),
                        corpus_sha256:
                            "d8dc4ba671dacd7a12b41d0cbe17d1cb4f2d5f5055cb2b9e7cefab2bb8c22e3c"
                                .to_string(),
                        workspace_revision: "workspace-revision".to_string(),
                        provider_account_ref: "provider-account".to_string(),
                        baseline_pattern: ExecutionPattern::Direct,
                        baseline_duration_ms: 100,
                        baseline_quality_score_bp: 8_000,
                        candidate_duration_ms: 80,
                        candidate_quality_score_bp: 8_000,
                        blind_judge_completed: true,
                        baseline_total_tokens: 15,
                        candidate_total_tokens: 15,
                        candidate_duplicate_tool_ratio_bp: 0,
                        admission_channel: Some(StrategyCalibrationAdmissionChannel::Speed),
                        report_sha256: String::new(),
                        rubric_sha256: String::new(),
                        binary_sha256: String::new(),
                        frontend_workspace_revision: String::new(),
                        model_revision: String::new(),
                        judge_model_revision: String::new(),
                        invariant_fingerprint: String::new(),
                    }),
                });
                for (condition, critical_path_ms) in
                    [("direct", 100), ("parallel_tools", 110), ("auto", 80)]
                {
                    samples.push(serde_json::json!({
                        "task_id": task_id,
                        "repetition": repetition,
                        "warmup": false,
                        "condition": condition,
                        "status": "completed",
                        "execution_graph_id": format!("graph-{task_id}-{repetition}-{condition}"),
                        "ttft_observed": true,
                        "usage_observed": true,
                        "cost_observed": true,
                        "evaluation_control_observed": true,
                        "evaluation_token_limit": 12_000,
                        "evaluation_tokens_consumed": 15,
                        "evaluation_budget_observed": true,
                        "evaluation_budget_breached": false,
                        "models_used": ["test-model"],
                        "critical_path_ms": critical_path_ms,
                        "quality_bp": 8_000,
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cached_tokens": 0,
                        "merge_cost_ms": if condition == "auto" { 2 } else { 0 },
                        "max_tool_concurrency_observed": if condition == "direct" { 1 } else { 2 },
                        "parallel_tool_batches": if condition == "direct" { 0 } else { 1 },
                        "judge": {
                            "judge_isolation_verified": true,
                            "observed_models": ["test-judge"]
                        },
                        "workspace_reset_verified": true,
                        "workspace_mutation_verified": true,
                        "workspace_changed_paths": if task_id == "AS-T04-bounded-implementation-review" {
                            vec!["fixtures/v546-write/target.txt"]
                        } else {
                            Vec::<&str>::new()
                        },
                        "write_attempt_paths": if task_id == "AS-T04-bounded-implementation-review" {
                            vec!["fixtures/v546-write/target.txt"]
                        } else {
                            Vec::<&str>::new()
                        },
                        "workspace_mutation_error": serde_json::Value::Null,
                    }));
                }
            }
        }
        let condition_invariants = serde_json::json!({
            "permission_mode": "dontAsk",
            "workspace_fixture": "workspace-v546-frozen",
            "mutation_fixture_reset": "per-sample-pristine-full-workspace-sha256",
            "tool_catalog": "same-binary-runtime-inspected",
            "provider_fallbacks": "disabled",
        });
        let invariant_fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&condition_invariants).unwrap())
        );
        let report = serde_json::json!({
            "kind": "harness_eval.auto_strategy_paired.v1",
            "status": "passed",
            "gate": {
                "passed": true,
                "claim_allowed": true,
                "judge_isolation_gate": true,
                "workspace_reset_gate": true,
                "workspace_mutation_gate": true,
                "automatic_team_materialization_gate": true,
                "baseline_topology_isolation_gate": true,
                "hard_budget_lease_gate": true,
                "tool_topology_observation_gate": true
            },
            "provenance": {
                "corpus_id": "auto-strategy-v1",
                "corpus_sha256": "d8dc4ba671dacd7a12b41d0cbe17d1cb4f2d5f5055cb2b9e7cefab2bb8c22e3c",
                "rubric_sha256": "3c2672ad0038c5b63abc6d6f724380d3a339e5921559dcb0b5c39e1a63039eba",
                "workspace_revision": "workspace-revision",
                "frontend_workspace_revision": "frontend-revision",
                "backend_source_archive_sha256": "c".repeat(64),
                "frontend_source_archive_sha256": "d".repeat(64),
                "provider_account_ref": "provider-account",
                "binary_sha256": "b".repeat(64),
                "provider": "test-model",
                "judge_model": "test-judge",
                "condition_invariant_fingerprint": invariant_fingerprint,
                "condition_invariants": condition_invariants,
                "seed": 20_260_716,
                "temperature_milli": 0,
                "warmup_per_task": 1,
                "repetitions": 3,
            },
            "samples": samples,
            "task_comparisons": comparisons,
            "strategy_calibration_records": records,
        });
        let mut store = StrategyExperienceStore::new();
        assert_eq!(store.import_paired_evaluation_report(&report), Ok(12));
        assert_eq!(store.import_paired_evaluation_report(&report), Ok(0));
        assert!(store.records[0].multi_agent_positive_lift);
        let first_understanding = understand(&StrategyInput::from_prompt(
            FROZEN_TEAM_CALIBRATION_TASKS[0].1,
        ));
        assert_eq!(
            store
                .summary_for(&first_understanding)
                .map(|summary| summary.multi_agent_lift_sample_count),
            Some(3)
        );

        let mut rejected = report;
        rejected["gate"]["claim_allowed"] = serde_json::Value::Bool(false);
        assert!(
            StrategyExperienceStore::new()
                .import_paired_evaluation_report(&rejected)
                .is_err()
        );
    }

    #[test]
    fn paired_lift_uses_the_registered_speed_and_quality_channels() {
        let mut speed = paired_calibration("speed", true);
        speed.candidate_quality_score_bp = 7_900;
        speed.admission_channel = speed.registered_admission_channel();
        assert_eq!(
            speed.admission_channel,
            Some(StrategyCalibrationAdmissionChannel::Speed)
        );
        assert!(speed.demonstrates_positive_lift());

        let mut quality = paired_calibration("quality", true);
        quality.candidate_duration_ms = 105;
        quality.candidate_quality_score_bp = 9_000;
        quality.candidate_total_tokens = 250;
        quality.admission_channel = quality.registered_admission_channel();
        assert_eq!(
            quality.admission_channel,
            Some(StrategyCalibrationAdmissionChannel::Quality)
        );
        assert!(quality.demonstrates_positive_lift());

        quality.candidate_duplicate_tool_ratio_bp = 1_500;
        quality.admission_channel = quality.registered_admission_channel();
        assert_eq!(quality.admission_channel, None);
        assert!(!quality.demonstrates_positive_lift());
    }

    #[test]
    fn negative_benefit_is_exact_profile_scoped_expiring_and_veto_only() {
        let prompt = "全面审查 runtime gateway frontend 三个独立责任域并综合";
        let mut input = with_proven_team_benefit(prompt);
        let understanding = understand(&input);
        let workload = StrategyWorkloadFingerprint::from_input(&input, &understanding).digest();
        let profile = "a".repeat(64);
        input.resource_snapshot.provider_profile_fingerprint = profile.clone();
        let observation = NegativeBenefitObservation {
            workload_fingerprint_sha256: workload,
            provider_profile_fingerprint: profile.clone(),
            baseline_candidate: ExecutionCandidateKind::Direct,
            baseline_duration_ms: 40_000,
            baseline_quality_score_bp: 8_500,
            team_duration_ms: 56_000,
            team_quality_score_bp: 7_700,
            report_sha256: "b".repeat(64),
            provenance_ref: "harness_eval.auto_strategy_paired.v1:negative".to_string(),
            observed_at_ms: 1_000,
            expires_at_ms: 2_000,
        };
        let mut store = StrategyExperienceStore::new();
        store.record_negative_benefit(observation).unwrap();

        let candidate_costs = input.candidate_costs.clone();
        let enriched = |now_ms| {
            let mut enriched = store.enrich_input_at(input.clone(), now_ms);
            enriched.candidate_costs.clone_from(&candidate_costs);
            enriched
        };
        let vetoed = decide_strategy(&enriched(1_500));
        assert_ne!(vetoed.selected_candidate, ExecutionCandidateKind::Team);
        assert!(vetoed.reasons.iter().any(|reason| reason.contains("vetoed")));

        let expired = decide_strategy(&enriched(2_000));
        assert_eq!(expired.selected_candidate, ExecutionCandidateKind::Team);

        let mut other_profile = enriched(1_500);
        other_profile.resource_snapshot.provider_profile_fingerprint = "c".repeat(64);
        assert_eq!(
            decide_strategy(&other_profile).selected_candidate,
            ExecutionCandidateKind::Team
        );

        let explicit = StrategyInput::from_prompt(
            "必须启动 Team 分别审查 runtime gateway frontend 并综合",
        );
        assert_eq!(
            decide_strategy(&store.enrich_input_at(explicit, 1_500)).selected_candidate,
            ExecutionCandidateKind::Team
        );
    }

    #[test]
    fn assumed_team_benefit_never_materializes_automatic_team() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "全面审查 runtime gateway frontend 三个独立责任域并综合",
        ));
        assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
        let team = decision
            .candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
            .unwrap();
        assert!(team.assumed);
    }

    #[test]
    fn failed_positive_claim_can_import_only_a_negative_veto() {
        let input = StrategyInput::from_prompt(
            "全面审查 runtime gateway frontend 三个独立责任域并综合",
        );
        let fingerprint =
            StrategyWorkloadFingerprint::from_input(&input, &understand(&input)).digest();
        let report = serde_json::json!({
            "kind": "harness_eval.auto_strategy_paired.v1",
            "status": "failed",
            "gate": {
                "passed": false,
                "claim_allowed": false,
                "provenance_complete": true,
                "budget_observation_complete": true,
                "judge_isolation_gate": true,
                "workspace_reset_gate": true,
                "baseline_topology_isolation_gate": true
            },
            "negative_benefit_observations": [{
                "workload_fingerprint_sha256": fingerprint,
                "provider_profile_fingerprint": "a".repeat(64),
                "baseline_candidate": "direct",
                "baseline_duration_ms": 40_000,
                "baseline_quality_score_bp": 8_500,
                "team_duration_ms": 56_000,
                "team_quality_score_bp": 7_700,
                "report_sha256": "",
                "provenance_ref": "harness_eval.auto_strategy_paired.v1:test",
                "observed_at_ms": 1_000,
                "expires_at_ms": 2_000
            }]
        });
        let mut store = StrategyExperienceStore::new();
        assert!(store.import_paired_evaluation_report(&report).is_err());
        assert_eq!(store.import_negative_benefit_report(&report), Ok(1));
        assert_eq!(store.records.len(), 0);
        assert_eq!(store.negative_benefit_observations.len(), 1);
        assert!(is_sha256(
            &store.negative_benefit_observations[0].report_sha256
        ));
    }
}
