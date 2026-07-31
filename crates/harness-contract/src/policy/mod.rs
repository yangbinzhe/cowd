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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PermissionMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
    Prompt,
    Allow,
}

impl PermissionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
            Self::Prompt => "prompt",
            Self::Allow => "allow",
        }
    }

    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::ReadOnly => 0,
            Self::WorkspaceWrite => 1,
            Self::DangerFullAccess | Self::Prompt | Self::Allow => 2,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPersistence {
    Once,
    Turn,
    Task,
    Session,
    Always,
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
