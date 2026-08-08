//! Pure contracts for Skill package inspection, profiling, runtime invocation,
//! and evidence. Implementations live in the `skill` and `runtime` crates.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SKILL_USAGE_RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const SKILL_MAINTENANCE_DRAFT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillKind {
    Document,
    Workflow,
    RuntimePackage,
    BrowserStatic,
    McpServer,
    SidecarService,
    Composite,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillLifecycleStatus {
    Imported,
    Inspected,
    UsablePrompt,
    UsableRuntime,
    Blocked,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillAdapterKind {
    PromptOnly,
    ToolGuided,
    SandboxExec,
    BrowserStatic,
    McpServer,
    SidecarService,
    Composite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillDetectedRuntime {
    Markdown,
    Shell,
    Python,
    Node,
    Go,
    Rust,
    Browser,
    Notebook,
    Mcp,
    Docker,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillEntrypoint {
    pub runtime: SkillDetectedRuntime,
    pub path: String,
    pub adapter: SkillAdapterKind,
    pub command_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRiskSignal {
    pub level: SkillRiskLevel,
    pub kind: String,
    pub evidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInspectionReport {
    pub source_root: String,
    pub detected_files: Vec<String>,
    pub detected_runtimes: Vec<SkillDetectedRuntime>,
    pub entrypoints: Vec<SkillEntrypoint>,
    pub risk_signals: Vec<SkillRiskSignal>,
    pub recommended_adapters: Vec<SkillAdapterKind>,
    pub blocked_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillStructuredDependency {
    pub domain: String,
    #[serde(default)]
    pub required_fact_types: Vec<String>,
    #[serde(default)]
    pub required_metric_keys: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    pub quality_gate: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillCapabilityProfile {
    pub skill_id: String,
    pub name: String,
    pub version: Option<String>,
    pub source_root: String,
    pub package_fingerprint: String,
    pub kind: SkillKind,
    pub lifecycle_status: SkillLifecycleStatus,
    pub adapters: Vec<SkillAdapterKind>,
    pub risk_level: SkillRiskLevel,
    pub entrypoints: Vec<SkillEntrypoint>,
    pub inspection_summary: Vec<String>,
    #[serde(default)]
    pub structured_dependencies: Vec<SkillStructuredDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AgentSkillProfile {
    pub baseline_skill_refs: Vec<String>,
    pub template_skill_refs: Vec<String>,
    pub team_skill_refs: Vec<String>,
    pub task_skill_refs: Vec<String>,
    pub explicit_grants: Vec<String>,
    pub hidden_skill_refs: Vec<String>,
    pub adapter_ceiling: Vec<SkillAdapterKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInvocationEvidence {
    pub skill_id: String,
    pub skill_version: Option<String>,
    pub adapter: SkillAdapterKind,
    pub entrypoint: Option<String>,
    pub outcome: String,
    pub evidence_refs: Vec<String>,
}

/// Exact observation made at the real Runtime Skill page-in boundary.
///
/// These values are deliberately not inferred from a terminal Outcome: a
/// successful turn cannot reveal whether instruction loading hit the cache,
/// missed, loaded a package, or failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillUsageKind {
    Hit,
    Miss,
    Load,
    Failure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillUsageReceipt {
    pub receipt_id: String,
    pub skill_id: String,
    pub skill_revision: String,
    pub adapter: SkillAdapterKind,
    pub usage: SkillUsageKind,
    pub workspace_identity: String,
    pub workload_fingerprint: String,
    pub config_revision: String,
    pub evaluation_environment: String,
    pub execution_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub observed_at_ms: u64,
    pub schema_version: u32,
}

impl SkillUsageReceipt {
    #[must_use]
    pub fn stable_id(
        skill_id: &str,
        skill_revision: &str,
        usage: SkillUsageKind,
        workspace_identity: &str,
        workload_fingerprint: &str,
        config_revision: &str,
        evaluation_environment: &str,
        execution_id: &str,
        session_id: &str,
        turn_id: &str,
    ) -> String {
        let payload = format!(
            "{skill_id}\n{skill_revision}\n{usage:?}\n{workspace_identity}\n\
             {workload_fingerprint}\n{config_revision}\n{evaluation_environment}\n\
             {execution_id}\n{session_id}\n{turn_id}"
        );
        format!("skill-usage-{:x}", Sha256::digest(payload.as_bytes()))
    }

    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageCounts {
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub failures: u64,
}

impl SkillUsageCounts {
    pub fn observe(&mut self, usage: SkillUsageKind) {
        match usage {
            SkillUsageKind::Hit => self.hits = self.hits.saturating_add(1),
            SkillUsageKind::Miss => self.misses = self.misses.saturating_add(1),
            SkillUsageKind::Load => self.loads = self.loads.saturating_add(1),
            SkillUsageKind::Failure => self.failures = self.failures.saturating_add(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillMaintenanceRecommendation {
    Keep,
    Revise,
    Deprecate,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillMaintenanceValidation {
    pub receipt_schema_valid: bool,
    pub evidence_closed: bool,
    pub outcome_association_count: u64,
    pub verified_success_count: u64,
    pub terminal_failure_count: u64,
    pub missing_outcome_count: u64,
    pub notes: Vec<String>,
}

/// Inert maintenance proposal derived only from canonical Receipts and
/// Outcome evidence. It is not a Skill package and carries no executable
/// prompt, entrypoint, tool grant, filesystem path, or installation command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillMaintenanceDraft {
    pub draft_id: String,
    pub skill_id: String,
    pub base_revision: String,
    pub proposed_revision: String,
    pub workspace_identity: String,
    pub workload_fingerprint: String,
    pub config_revision: String,
    pub evaluation_environment: String,
    pub canonical_counts: SkillUsageCounts,
    pub legacy_counts: SkillUsageCounts,
    pub evidence_receipt_ids: Vec<String>,
    pub outcome_refs: Vec<String>,
    pub evidence_digest: String,
    pub target: String,
    pub recommendation: SkillMaintenanceRecommendation,
    pub validation: SkillMaintenanceValidation,
    pub created_at_ms: u64,
    pub schema_version: u32,
}

impl SkillMaintenanceDraft {
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRevisionReviewAction {
    Activate,
    Rollback,
}

impl SkillRevisionReviewAction {
    #[must_use]
    pub const fn action_key(self) -> &'static str {
        match self {
            Self::Activate => "skill.revision.activate",
            Self::Rollback => "skill.revision.rollback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRevisionReviewStatus {
    Pending,
    Approved,
    Denied,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRevisionReviewDecision {
    Approve,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRevisionReview {
    pub review_id: String,
    pub approval_id: String,
    pub action: SkillRevisionReviewAction,
    pub draft_id: Option<String>,
    pub skill_id: String,
    pub target_revision: String,
    pub previous_revision: Option<String>,
    pub evidence_digest: String,
    pub expected_generation: u64,
    pub status: SkillRevisionReviewStatus,
    pub created_at_ms: u64,
}

impl SkillRevisionReview {
    #[must_use]
    pub fn scope_ref(&self) -> String {
        format!("skill:{}", self.skill_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillActivePointer {
    pub skill_id: String,
    pub active_revision: String,
    pub previous_revision: Option<String>,
    pub generation: u64,
    pub source_draft_id: Option<String>,
    pub approval_ref: String,
    pub activated_at_ms: u64,
}

#[cfg(test)]
mod evolution_tests {
    use super::*;

    #[test]
    fn usage_receipt_identity_and_maintenance_digest_are_replay_stable() {
        let id = SkillUsageReceipt::stable_id(
            "review",
            "1.0.0",
            SkillUsageKind::Hit,
            "workspace",
            "workload",
            "config",
            "production",
            "execution",
            "session",
            "turn",
        );
        assert_eq!(
            id,
            SkillUsageReceipt::stable_id(
                "review",
                "1.0.0",
                SkillUsageKind::Hit,
                "workspace",
                "workload",
                "config",
                "production",
                "execution",
                "session",
                "turn",
            )
        );
        let receipt = SkillUsageReceipt {
            receipt_id: id,
            skill_id: "review".to_string(),
            skill_revision: "1.0.0".to_string(),
            adapter: SkillAdapterKind::PromptOnly,
            usage: SkillUsageKind::Hit,
            workspace_identity: "workspace".to_string(),
            workload_fingerprint: "workload".to_string(),
            config_revision: "config".to_string(),
            evaluation_environment: "production".to_string(),
            execution_id: "execution".to_string(),
            session_id: "session".to_string(),
            turn_id: "turn".to_string(),
            observed_at_ms: 1,
            schema_version: SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
        };
        assert_eq!(receipt.digest(), receipt.digest());
    }
}
