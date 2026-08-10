//! Canonical AI policy, approval, grant, and receipt contracts.
//!
//! Runtime owns approval coordination. Gateway and Surfaces only project or
//! submit decisions through these contracts.

use super::{PolicyDecisionKind, SessionExecutionPolicy};
use crate::agent::{AgentPolicyRequirement, AgentSpec};
use crate::core::TaskRisk;
use crate::tool::ToolEffectDescriptor;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyScope {
    Global,
    Session,
    Agent,
    Harness,
    Tool,
    Connector,
    Memory,
    Matrix,
    ExecutionGraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyReceipt {
    pub id: String,
    pub scope: PolicyScope,
    pub decision: PolicyDecisionKind,
    pub reasons: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub source_policy: String,
    pub created_at: DateTime<Utc>,
}

impl PolicyReceipt {
    #[must_use]
    pub fn new(
        scope: PolicyScope,
        decision: PolicyDecisionKind,
        source_policy: impl Into<String>,
    ) -> Self {
        Self {
            id: format!("policy-receipt-{}", uuid::Uuid::new_v4()),
            scope,
            decision,
            reasons: Vec::new(),
            evidence_refs: Vec::new(),
            source_policy: source_policy.into(),
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reasons.push(reason.into());
        self
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, reference: impl Into<String>) -> Self {
        self.evidence_refs.push(reference.into());
        self
    }
}

#[must_use]
pub fn governed_tool_policy_receipts(
    plan_ids: &[String],
    requires_checkpoint: bool,
    requires_human_confirm: bool,
) -> Vec<PolicyReceipt> {
    let mut receipts = Vec::new();
    let decision = if requires_human_confirm {
        PolicyDecisionKind::Ask
    } else {
        PolicyDecisionKind::Allow
    };
    let mut receipt = PolicyReceipt::new(
        PolicyScope::Tool,
        decision,
        "governed_tool_execution_policy",
    );
    for plan_id in plan_ids {
        receipt = receipt.with_evidence_ref(format!("governed_tool_plan:{plan_id}"));
    }
    if requires_human_confirm {
        receipt = receipt.with_reason("critical tool path requires human confirmation");
    } else if requires_checkpoint {
        receipt = receipt.with_reason("write path requires checkpoint receipt");
    } else {
        receipt = receipt.with_reason("governed tool execution is allowed by current policy");
    }
    receipts.push(receipt);
    receipts
}

#[must_use]
pub fn agent_spec_policy_receipts(agent_spec: &AgentSpec) -> Vec<PolicyReceipt> {
    let mut receipts = Vec::new();
    for requirement in &agent_spec.policies {
        let (scope, decision, reason) = match requirement {
            AgentPolicyRequirement::RequiresApproval => (
                PolicyScope::Agent,
                PolicyDecisionKind::Ask,
                "agent contract requires approval",
            ),
            AgentPolicyRequirement::RequiresMatrixEvidence => (
                PolicyScope::Matrix,
                PolicyDecisionKind::Allow,
                "agent contract requires matrix evidence",
            ),
            AgentPolicyRequirement::RequiresVerification => (
                PolicyScope::Harness,
                PolicyDecisionKind::Allow,
                "agent contract requires verification",
            ),
            AgentPolicyRequirement::RequiresWorktreeIsolation => (
                PolicyScope::Agent,
                PolicyDecisionKind::Ask,
                "agent contract requires worktree isolation",
            ),
            AgentPolicyRequirement::RequiresHumanReview => (
                PolicyScope::Agent,
                PolicyDecisionKind::Ask,
                "agent contract requires human review",
            ),
        };
        receipts.push(
            PolicyReceipt::new(scope, decision, "agent_spec_policy")
                .with_reason(reason)
                .with_evidence_ref(format!("agent_spec:{}", agent_spec.id)),
        );
    }
    receipts
}

#[must_use]
pub fn behavior_policy_receipt(
    allow_execution: bool,
    requires_scope_downgrade: bool,
    requires_human_review: bool,
    risks: &[String],
) -> PolicyReceipt {
    let decision = if !allow_execution {
        PolicyDecisionKind::Deny
    } else if requires_human_review || requires_scope_downgrade {
        PolicyDecisionKind::Ask
    } else {
        PolicyDecisionKind::Allow
    };
    let mut receipt = PolicyReceipt::new(PolicyScope::Global, decision, "behavior_policy");
    if risks.is_empty() {
        receipt = receipt.with_reason("behavior policy permits current execution scope");
    } else {
        for risk in risks {
            receipt = receipt.with_reason(risk.clone());
        }
    }
    if requires_scope_downgrade {
        receipt = receipt.with_reason("execution scope should be downgraded before expansion");
    }
    if requires_human_review {
        receipt = receipt.with_reason("behavior policy requires human review");
    }
    receipt
}

/// Runtime owner that raised an approval request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSourceKind {
    Session,
    Agent,
    Team,
    Mission,
    Steward,
    Evolution,
    Application,
}

/// Opaque correlation metadata for an APP-owned typed review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalApplicationSource {
    pub app_id: String,
    pub correlation_schema: String,
    pub decision_capability: String,
}

/// Durable route back to the owner that must consume an approval decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalSource {
    pub kind: ApprovalSourceKind,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub team_id: Option<String>,
    pub mission_id: Option<String>,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub review_ref: Option<String>,
    #[serde(default)]
    pub application: Option<ApprovalApplicationSource>,
}

impl ApprovalSource {
    pub fn validate(&self) -> Result<(), String> {
        match self.kind {
            ApprovalSourceKind::Application => {
                let application = self
                    .application
                    .as_ref()
                    .ok_or_else(|| "application_approval_source_is_incomplete".to_string())?;
                if application.app_id.trim().is_empty()
                    || application.correlation_schema.trim().is_empty()
                    || application.decision_capability.trim().is_empty()
                    || self
                        .review_ref
                        .as_deref()
                        .is_none_or(|review_ref| review_ref.trim().is_empty())
                {
                    return Err("application_approval_source_is_incomplete".to_string());
                }
            }
            _ if self.application.is_some() => {
                return Err("non_application_approval_cannot_include_application_metadata".into());
            }
            _ => {}
        }
        Ok(())
    }

    #[must_use]
    pub fn typed_application(&self) -> Option<&ApprovalApplicationSource> {
        (self.kind == ApprovalSourceKind::Application)
            .then_some(self.application.as_ref())
            .flatten()
    }
}

/// Complete policy context used to match a durable grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalContext {
    pub principal_id: String,
    pub profile_id: String,
    #[serde(default)]
    pub approval_profile: Option<ApprovalProfile>,
    pub workspace_key: String,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub capability: String,
    pub invocation_id: Option<String>,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub strategy_decision_ref: Option<String>,
    #[serde(default)]
    pub source_surface: Option<String>,
    #[serde(default)]
    pub resource_targets: Vec<String>,
    #[serde(default)]
    pub effect: Option<ToolEffectDescriptor>,
    #[serde(default)]
    pub explicit_ask: bool,
    #[serde(default)]
    pub policy_revision: u64,
}

impl ApprovalContext {
    #[must_use]
    pub fn owned(
        source: &ApprovalSource,
        capability: impl Into<String>,
        workspace_key: impl Into<String>,
    ) -> Self {
        Self {
            principal_id: "runtime".to_string(),
            profile_id: "balanced".to_string(),
            approval_profile: Some(ApprovalProfile::Balanced),
            workspace_key: workspace_key.into(),
            session_id: source.session_id.clone(),
            turn_id: None,
            task_id: None,
            capability: capability.into(),
            invocation_id: None,
            execution_id: None,
            strategy_decision_ref: None,
            source_surface: None,
            resource_targets: source.resource_ref.iter().cloned().collect(),
            effect: None,
            explicit_ask: false,
            policy_revision: 0,
        }
    }

    /// Bind a Session-owned approval to the exact execution-policy revision
    /// that evaluated it. Grants issued for an older revision can therefore
    /// never authorize work after a policy change.
    #[must_use]
    pub fn with_execution_policy(mut self, policy: &SessionExecutionPolicy) -> Self {
        self.profile_id = policy.autonomy_profile.as_str().to_string();
        self.approval_profile = Some(policy.approval_profile);
        self.policy_revision = policy.revision;
        self
    }
}

/// Stable, normalized object being approved. This is derived from the request
/// so policy, audit, and Surfaces cannot invent competing subject models.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalSubject {
    pub capability: String,
    pub action: String,
    pub risk: TaskRisk,
    pub resource_targets: Vec<String>,
    pub effect_descriptor_hash: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalTimeoutPolicy {
    Pending,
    AutoDeny,
    ContinueAlternative,
    AutoApproveOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalProfile {
    Supervised,
    Balanced,
    Autonomous,
}

impl Default for ApprovalProfile {
    fn default() -> Self {
        Self::Balanced
    }
}

impl ApprovalProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supervised => "supervised",
            Self::Balanced => "balanced",
            Self::Autonomous => "autonomous",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "supervised" => Some(Self::Supervised),
            "balanced" => Some(Self::Balanced),
            "autonomous" => Some(Self::Autonomous),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDomain {
    #[default]
    Execution,
    Knowledge,
    Skill,
    Evolution,
    Application,
    System,
}

impl ApprovalDomain {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::Knowledge => "knowledge",
            Self::Skill => "skill",
            Self::Evolution => "evolution",
            Self::Application => "application",
            Self::System => "system",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LowRiskTimeoutAction {
    AutoApproveOnce,
    Pending,
}

impl Default for LowRiskTimeoutAction {
    fn default() -> Self {
        Self::AutoApproveOnce
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
    TimedOut,
    Cancelled,
    Superseded,
}

impl ApprovalStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Denied => "denied",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Superseded => "superseded",
        }
    }

    #[must_use]
    pub const fn terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalGrantScope {
    Once,
    Turn,
    Task,
    Session,
    Global,
}

impl ApprovalGrantScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Once => "once",
            Self::Turn => "turn",
            Self::Task => "task",
            Self::Session => "session",
            Self::Global => "global",
        }
    }
}

impl std::str::FromStr for ApprovalGrantScope {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "once" => Ok(Self::Once),
            "turn" => Ok(Self::Turn),
            "task" => Ok(Self::Task),
            "session" => Ok(Self::Session),
            "global" => Ok(Self::Global),
            other => Err(format!("unsupported approval scope: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionActorKind {
    Human,
    Policy,
    StewardAgent,
    TypedOwner,
    TimeoutPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecisionActor {
    pub kind: ApprovalDecisionActorKind,
    pub actor_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub approved: bool,
    pub reason: String,
    pub scope: ApprovalGrantScope,
    pub actor: ApprovalDecisionActor,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub decided_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub approval_id: String,
    pub source: ApprovalSource,
    pub context: ApprovalContext,
    pub action: String,
    pub summary: String,
    pub risk: TaskRisk,
    #[serde(default)]
    pub domain: ApprovalDomain,
    #[serde(default)]
    pub blocks_execution: bool,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub timeout_policy: ApprovalTimeoutPolicy,
    pub status: ApprovalStatus,
    #[serde(default)]
    pub decision: Option<ApprovalDecision>,
    pub created_at_ms: u64,
    pub resolved_at_ms: Option<u64>,
}

impl ApprovalRequest {
    #[must_use]
    pub fn subject(&self) -> ApprovalSubject {
        ApprovalSubject {
            capability: self.context.capability.clone(),
            action: self.action.clone(),
            risk: self.risk,
            resource_targets: self.context.resource_targets.clone(),
            effect_descriptor_hash: self
                .context
                .effect
                .as_ref()
                .map(|effect| effect.descriptor_hash.clone()),
            summary: self.summary.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitApprovalRequest {
    pub source: ApprovalSource,
    pub context: ApprovalContext,
    pub action: String,
    pub summary: String,
    pub risk: TaskRisk,
    #[serde(default)]
    pub domain: ApprovalDomain,
    #[serde(default)]
    pub blocks_execution: bool,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub timeout_policy: ApprovalTimeoutPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecisionCommand {
    pub approval_id: String,
    pub approved: bool,
    pub reason: String,
    pub scope: ApprovalGrantScope,
    pub actor: ApprovalDecisionActor,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecisionReceipt {
    pub approval_id: String,
    pub status: ApprovalStatus,
    pub route_back: ApprovalSource,
    pub message: String,
    pub grant_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalGrantStatus {
    Active,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalGrant {
    pub grant_id: String,
    pub approval_id: String,
    pub scope: ApprovalGrantScope,
    pub principal_id: String,
    pub profile_id: String,
    pub workspace_key: String,
    pub capability: String,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub invocation_id: Option<String>,
    #[serde(default)]
    pub resource_targets: Vec<String>,
    #[serde(default)]
    pub effect_descriptor_hash: Option<String>,
    pub risk_ceiling: TaskRisk,
    #[serde(default)]
    pub policy_revision: u64,
    pub status: ApprovalGrantStatus,
    pub issued_by: ApprovalDecisionActor,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub revoked_at_ms: Option<u64>,
    pub revoke_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn critical_governed_tool_plan_maps_to_ask() {
        let receipts = governed_tool_policy_receipts(&["plan-1".to_string()], true, true);
        assert_eq!(receipts[0].decision, PolicyDecisionKind::Ask);
        assert!(receipts[0].evidence_refs[0].contains("plan-1"));
    }

    #[test]
    fn agent_spec_policy_maps_review_to_ask() {
        let receipts = agent_spec_policy_receipts(&AgentSpec::reviewer());
        assert!(receipts
            .iter()
            .any(|receipt| receipt.decision == PolicyDecisionKind::Ask));
    }
}
