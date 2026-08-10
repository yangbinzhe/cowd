use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod approval;
pub use approval::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionResource {
    File,
    Shell,
    Network,
    Provider,
    Connector,
    Channel,
    Tool,
    Memory,
    Matrix,
    Session,
    Task,
    Approval,
    Config,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionOperation {
    Read,
    Write,
    Delete,
    Execute,
    Send,
    Control,
    Call,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionScope {
    pub resource: PermissionResource,
    pub operation: PermissionOperation,
    pub target: Option<String>,
}

impl PermissionScope {
    #[must_use]
    pub fn new(resource: PermissionResource, operation: PermissionOperation) -> Self {
        Self {
            resource,
            operation,
            target: None,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum PermissionMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyProfileId {
    Cautious,
    Supervised,
    Solo,
    Yolo,
    Stewarded,
}

impl AutonomyProfileId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cautious => "cautious",
            Self::Supervised => "supervised",
            Self::Solo => "solo",
            Self::Yolo => "yolo",
            Self::Stewarded => "stewarded",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cautious" => Some(Self::Cautious),
            "supervised" | "balanced" | "assisted" => Some(Self::Supervised),
            "solo" => Some(Self::Solo),
            "yolo" | "autonomous" => Some(Self::Yolo),
            "stewarded" => Some(Self::Stewarded),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionPolicy {
    AlwaysPauseForHuman,
    PauseOnRisk,
    ContinueWithAudit,
    ContinueUntilBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionExecutionPolicyOrigin {
    ConfigDefault,
    SessionExplicit,
    SurfaceCommand,
    RecoveryReplan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionExecutionPolicy {
    pub autonomy_profile: AutonomyProfileId,
    pub permission_mode: PermissionMode,
    pub approval_profile: ApprovalProfile,
    pub interruption_policy: InterruptionPolicy,
    pub revision: u64,
    pub origin: SessionExecutionPolicyOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateSessionExecutionPolicyRequest {
    pub preset: AutonomyProfileId,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionExecutionPolicyActiveTurn {
    pub state: String,
    pub applied_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionExecutionPolicyResponse {
    pub session_id: String,
    pub policy: SessionExecutionPolicy,
    pub matched_preset: Option<AutonomyProfileId>,
    pub active_turn: SessionExecutionPolicyActiveTurn,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persisted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_to_active_runtime: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applies_after_active_turn: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safe_replay: Option<String>,
}

impl SessionExecutionPolicy {
    #[must_use]
    pub fn from_defaults(
        permission_mode: PermissionMode,
        approval_profile: ApprovalProfile,
    ) -> Self {
        let autonomy_profile = match permission_mode {
            PermissionMode::ReadOnly => AutonomyProfileId::Cautious,
            PermissionMode::WorkspaceWrite => AutonomyProfileId::Supervised,
            PermissionMode::DangerFullAccess => AutonomyProfileId::Solo,
        };
        Self {
            autonomy_profile,
            permission_mode,
            approval_profile,
            interruption_policy: interruption_policy_for(autonomy_profile),
            revision: 1,
            origin: SessionExecutionPolicyOrigin::ConfigDefault,
        }
    }

    #[must_use]
    pub fn from_profile(
        profile: AutonomyProfileId,
        revision: u64,
        origin: SessionExecutionPolicyOrigin,
    ) -> Self {
        Self {
            autonomy_profile: profile,
            permission_mode: permission_mode_for(profile),
            approval_profile: approval_profile_for(profile),
            interruption_policy: interruption_policy_for(profile),
            revision: revision.max(1),
            origin,
        }
    }

    #[must_use]
    pub fn matched_preset(&self) -> Option<AutonomyProfileId> {
        let expected = Self::from_profile(self.autonomy_profile, self.revision, self.origin);
        (self.permission_mode == expected.permission_mode
            && self.approval_profile == expected.approval_profile
            && self.interruption_policy == expected.interruption_policy)
            .then_some(self.autonomy_profile)
    }

    #[must_use]
    pub fn next_revision(&self, origin: SessionExecutionPolicyOrigin) -> Self {
        let mut next = self.clone();
        next.revision = next.revision.saturating_add(1);
        next.origin = origin;
        next
    }

    #[must_use]
    pub fn with_approval_profile(mut self, profile: ApprovalProfile) -> Self {
        self.approval_profile = profile;
        self
    }
}

#[must_use]
pub const fn permission_mode_for(profile: AutonomyProfileId) -> PermissionMode {
    match profile {
        AutonomyProfileId::Cautious => PermissionMode::ReadOnly,
        AutonomyProfileId::Supervised | AutonomyProfileId::Stewarded => {
            PermissionMode::WorkspaceWrite
        }
        AutonomyProfileId::Solo | AutonomyProfileId::Yolo => PermissionMode::DangerFullAccess,
    }
}

#[must_use]
pub const fn approval_profile_for(profile: AutonomyProfileId) -> ApprovalProfile {
    match profile {
        AutonomyProfileId::Cautious => ApprovalProfile::Supervised,
        AutonomyProfileId::Supervised => ApprovalProfile::Balanced,
        AutonomyProfileId::Solo | AutonomyProfileId::Yolo | AutonomyProfileId::Stewarded => {
            ApprovalProfile::Autonomous
        }
    }
}

#[must_use]
pub const fn interruption_policy_for(profile: AutonomyProfileId) -> InterruptionPolicy {
    match profile {
        AutonomyProfileId::Cautious => InterruptionPolicy::AlwaysPauseForHuman,
        AutonomyProfileId::Supervised => InterruptionPolicy::PauseOnRisk,
        AutonomyProfileId::Solo | AutonomyProfileId::Stewarded => {
            InterruptionPolicy::ContinueWithAudit
        }
        AutonomyProfileId::Yolo => InterruptionPolicy::ContinueUntilBlocked,
    }
}

impl PermissionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }

    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::ReadOnly => 0,
            Self::WorkspaceWrite => 1,
            Self::DangerFullAccess => 2,
        }
    }

    #[must_use]
    pub const fn permits(self, required: Self) -> bool {
        self.rank() >= required.rank()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossPlaneRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Secret,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EffectReversibility {
    Reversible,
    Compensatable,
    Irreversible,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EffectExternality {
    Internal,
    Workspace,
    NetworkRead,
    ExternalMutation,
    System,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EffectNovelty {
    Routine,
    NewTarget,
    NewCapability,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EffectBlastRadius {
    Item,
    Workspace,
    ExternalAccount,
    System,
    Unbounded,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectAssessment {
    pub reversibility: EffectReversibility,
    pub externality: EffectExternality,
    pub data_sensitivity: DataClassification,
    pub novelty: EffectNovelty,
    pub blast_radius: EffectBlastRadius,
}

impl Default for EffectAssessment {
    fn default() -> Self {
        Self {
            reversibility: EffectReversibility::Unknown,
            externality: EffectExternality::Unknown,
            data_sensitivity: DataClassification::Internal,
            novelty: EffectNovelty::Unknown,
            blast_radius: EffectBlastRadius::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskAssessment {
    pub level: RiskLevel,
    pub reasons: Vec<String>,
    pub assessed_at: DateTime<Utc>,
}

impl RiskAssessment {
    #[must_use]
    pub fn new(level: RiskLevel) -> Self {
        Self {
            level,
            reasons: Vec::new(),
            assessed_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionKind {
    Allow,
    Deny,
    Ask,
    Defer,
    Escalate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub kind: PolicyDecisionKind,
    pub scope: PermissionScope,
    pub risk: RiskAssessment,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecisionReceipt {
    pub decision: PolicyDecision,
    pub trace_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskGateReceipt {
    pub scope: PermissionScope,
    pub risk: RiskAssessment,
    pub decision: PolicyDecisionKind,
    pub approval_required: bool,
    pub issued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityGapKind {
    MissingLease,
    ScopeMismatch,
    PermissionCeiling,
    ApprovalRequired,
    HardDenied,
    CapabilityUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityGap {
    pub fingerprint: String,
    pub kind: CapabilityGapKind,
    pub capability: String,
    pub requested_scopes: Vec<PermissionScope>,
    pub required_mode: PermissionMode,
    pub active_ceiling: PermissionMode,
    pub parent_ceiling: PermissionMode,
    pub reason: String,
    pub safe_alternatives: Vec<String>,
    pub recoverable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationPath {
    ExistingLease,
    StandingGrant,
    PolicyAutoGrant,
    SafeAlternate,
    HumanApproval,
    HardDeny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationLeaseStatus {
    Active,
    Exhausted,
    Expired,
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationLeaseTransitionKind {
    Issued,
    Consumed,
    Expired,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationLease {
    pub lease_id: String,
    pub principal_id: String,
    pub parent_lease_id: Option<String>,
    pub capability: String,
    pub scopes: Vec<PermissionScope>,
    pub ceiling: PermissionMode,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub max_uses: u32,
    pub remaining_uses: u32,
    pub idempotency_key: String,
    pub signature: String,
    pub status: AuthorizationLeaseStatus,
}

impl AuthorizationLease {
    #[must_use]
    pub fn is_active_at(&self, now_ms: u64) -> bool {
        self.status == AuthorizationLeaseStatus::Active
            && self.remaining_uses > 0
            && now_ms <= self.expires_at_ms
    }

    #[must_use]
    pub fn permits(&self, capability: &str, required: PermissionMode) -> bool {
        self.capability == capability && self.ceiling.permits(required)
    }
}

/// Durable evidence emitted whenever Runtime changes an authorization lease.
///
/// The full snapshot avoids reconstructing TTL, remaining uses, scope, or
/// parent ceilings from tool output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationLeaseTransition {
    pub transition_id: String,
    pub kind: AuthorizationLeaseTransitionKind,
    pub lease: AuthorizationLease,
    pub idempotency_key: Option<String>,
    pub occurred_at_ms: u64,
    pub evidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityAssessment {
    pub assessment_id: String,
    pub capability: String,
    pub effect: EffectAssessment,
    pub requested_scopes: Vec<PermissionScope>,
    pub required_mode: PermissionMode,
    pub active_ceiling: PermissionMode,
    pub parent_ceiling: PermissionMode,
    pub risk: RiskLevel,
    pub path: AuthorizationPath,
    pub lease: Option<AuthorizationLease>,
    pub gap: Option<CapabilityGap>,
    pub evidence_refs: Vec<String>,
    pub assessed_at_ms: u64,
}

#[cfg(test)]
mod session_execution_policy_tests {
    use super::*;

    #[test]
    fn presets_resolve_every_policy_dimension_from_one_source() {
        let cases = [
            (
                AutonomyProfileId::Cautious,
                PermissionMode::ReadOnly,
                ApprovalProfile::Supervised,
                InterruptionPolicy::AlwaysPauseForHuman,
            ),
            (
                AutonomyProfileId::Supervised,
                PermissionMode::WorkspaceWrite,
                ApprovalProfile::Balanced,
                InterruptionPolicy::PauseOnRisk,
            ),
            (
                AutonomyProfileId::Solo,
                PermissionMode::DangerFullAccess,
                ApprovalProfile::Autonomous,
                InterruptionPolicy::ContinueWithAudit,
            ),
            (
                AutonomyProfileId::Yolo,
                PermissionMode::DangerFullAccess,
                ApprovalProfile::Autonomous,
                InterruptionPolicy::ContinueUntilBlocked,
            ),
            (
                AutonomyProfileId::Stewarded,
                PermissionMode::WorkspaceWrite,
                ApprovalProfile::Autonomous,
                InterruptionPolicy::ContinueWithAudit,
            ),
        ];
        for (profile, permission, approval, interruption) in cases {
            let policy = SessionExecutionPolicy::from_profile(
                profile,
                7,
                SessionExecutionPolicyOrigin::SessionExplicit,
            );
            assert_eq!(policy.permission_mode, permission);
            assert_eq!(policy.approval_profile, approval);
            assert_eq!(policy.interruption_policy, interruption);
            assert_eq!(policy.matched_preset(), Some(profile));
            assert_eq!(policy.revision, 7);
        }
    }

    #[test]
    fn a_customized_dimension_is_not_reported_as_a_preset() {
        let mut policy = SessionExecutionPolicy::from_profile(
            AutonomyProfileId::Yolo,
            1,
            SessionExecutionPolicyOrigin::ConfigDefault,
        );
        policy.approval_profile = ApprovalProfile::Supervised;
        assert_eq!(policy.matched_preset(), None);
    }
}
