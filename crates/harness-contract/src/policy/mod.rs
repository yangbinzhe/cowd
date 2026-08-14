use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    Stewarded,
    Autonomous,
    Yolo,
}

/// Execution boundary derived from an autonomy preset. It is a Runtime-owned
/// contract, never a model-authored switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SandboxPosture {
    ReadOnlySandbox,
    WorkspaceWriteSandbox,
    HostFullAccess,
}

impl Default for SandboxPosture {
    fn default() -> Self {
        Self::ReadOnlySandbox
    }
}

impl SandboxPosture {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnlySandbox => "read-only-sandbox",
            Self::WorkspaceWriteSandbox => "workspace-write-sandbox",
            Self::HostFullAccess => "host-full-access",
        }
    }
}

impl AutonomyProfileId {
    /// Canonical sandbox posture derived from one autonomy profile. This is
    /// the single authoritative mapping; runtime and Gateway code must never
    /// re-derive sandbox behavior from permission ceilings.
    #[must_use]
    pub const fn sandbox_posture(self) -> SandboxPosture {
        match self {
            Self::Cautious => SandboxPosture::ReadOnlySandbox,
            Self::Supervised | Self::Stewarded => SandboxPosture::WorkspaceWriteSandbox,
            Self::Autonomous | Self::Yolo => SandboxPosture::HostFullAccess,
        }
    }
}

impl AutonomyProfileId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cautious => "cautious",
            Self::Supervised => "supervised",
            Self::Stewarded => "stewarded",
            Self::Autonomous => "autonomous",
            Self::Yolo => "yolo",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cautious" => Some(Self::Cautious),
            "supervised" | "balanced" | "assisted" => Some(Self::Supervised),
            // `solo` is accepted only as a legacy CLI input alias for the
            // renamed Autonomous preset. It is never persisted.
            "solo" | "autonomous" => Some(Self::Autonomous),
            "yolo" => Some(Self::Yolo),
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
    pub sandbox_posture: SandboxPosture,
    pub approval_profile: ApprovalProfile,
    pub interruption_policy: InterruptionPolicy,
    pub revision: u64,
    pub origin: SessionExecutionPolicyOrigin,
}

/// Immutable execution-policy snapshot bound to one concrete Session admission.
///
/// Schedules and Tasks retain this value after admission so later policy changes
/// cannot silently widen already admitted work. `permission_ceiling` is the
/// product-owned upper bound; every other effective axis comes from the target
/// Session policy after intersecting that bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionPolicyBinding {
    pub session_id: String,
    pub policy_revision: u64,
    pub autonomy_profile: AutonomyProfileId,
    pub permission_mode: PermissionMode,
    pub sandbox_posture: SandboxPosture,
    pub approval_profile: ApprovalProfile,
    pub interruption_policy: InterruptionPolicy,
    pub permission_ceiling: PermissionMode,
    pub ceiling_digest: String,
    pub policy_digest: String,
}

impl ExecutionPolicyBinding {
    #[must_use]
    pub fn bind(
        session_id: impl Into<String>,
        policy: &SessionExecutionPolicy,
        permission_ceiling: PermissionMode,
    ) -> Self {
        let session_id = session_id.into();
        let ceiling_digest = digest_text(&format!(
            "cowd.execution-policy-ceiling.v1:{}",
            permission_ceiling.as_str()
        ));
        let policy_digest = execution_policy_binding_digest(
            &session_id,
            policy.revision,
            policy.autonomy_profile.as_str(),
            policy.permission_mode.as_str(),
            policy.sandbox_posture.as_str(),
            policy.approval_profile.as_str(),
            interruption_policy_name(policy.interruption_policy),
            &ceiling_digest,
        );
        Self {
            session_id,
            policy_revision: policy.revision,
            autonomy_profile: policy.autonomy_profile,
            permission_mode: policy.permission_mode,
            sandbox_posture: policy.sandbox_posture,
            approval_profile: policy.approval_profile,
            interruption_policy: policy.interruption_policy,
            permission_ceiling,
            ceiling_digest,
            policy_digest,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.session_id.trim().is_empty() {
            return Err("execution policy binding session_id must not be empty");
        }
        if self.policy_revision == 0 {
            return Err("execution policy binding revision must be positive");
        }
        let expected_ceiling = digest_text(&format!(
            "cowd.execution-policy-ceiling.v1:{}",
            self.permission_ceiling.as_str()
        ));
        if self.ceiling_digest != expected_ceiling {
            return Err("execution policy binding ceiling digest does not match");
        }
        let expected_policy = execution_policy_binding_digest(
            &self.session_id,
            self.policy_revision,
            self.autonomy_profile.as_str(),
            self.permission_mode.as_str(),
            self.sandbox_posture.as_str(),
            self.approval_profile.as_str(),
            interruption_policy_name(self.interruption_policy),
            &self.ceiling_digest,
        );
        if self.policy_digest != expected_policy {
            return Err("execution policy binding policy digest does not match");
        }
        Ok(())
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum PolicyTransitionPhase {
    Requested,
    Persisted,
    Freezing,
    Draining,
    Rebinding,
    #[default]
    Stable,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PolicyTransitionReceipt {
    pub transition_id: String,
    pub phase: PolicyTransitionPhase,
    pub desired_revision: u64,
    pub effective_revision: u64,
    pub old_revision_active_attempts: u64,
    pub requested_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionExecutionPolicyState {
    pub effective: SessionExecutionPolicy,
    /// Full desired five-axis policy required to resume a non-terminal
    /// transition after restart. Stable states carry `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired: Option<SessionExecutionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_transition: Option<PolicyTransitionReceipt>,
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
    /// Canonical effective/desired transition state. Surfaces must render
    /// `effective` as authoritative until the pending receipt reaches Stable;
    /// `policy` remains the requested/current convenience projection.
    pub state: SessionExecutionPolicyState,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<PolicyTransitionReceipt>,
}

fn digest_text(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn execution_policy_binding_digest(
    session_id: &str,
    revision: u64,
    autonomy_profile: &str,
    permission_mode: &str,
    sandbox_posture: &str,
    approval_profile: &str,
    interruption_policy: &str,
    ceiling_digest: &str,
) -> String {
    digest_text(&format!(
        "cowd.execution-policy-binding.v1:{session_id}:{revision}:{autonomy_profile}:{permission_mode}:{sandbox_posture}:{approval_profile}:{interruption_policy}:{ceiling_digest}"
    ))
}

const fn interruption_policy_name(policy: InterruptionPolicy) -> &'static str {
    match policy {
        InterruptionPolicy::AlwaysPauseForHuman => "always_pause_for_human",
        InterruptionPolicy::PauseOnRisk => "pause_on_risk",
        InterruptionPolicy::ContinueWithAudit => "continue_with_audit",
        InterruptionPolicy::ContinueUntilBlocked => "continue_until_blocked",
    }
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
            PermissionMode::DangerFullAccess => AutonomyProfileId::Autonomous,
        };
        Self {
            autonomy_profile,
            permission_mode,
            sandbox_posture: autonomy_profile.sandbox_posture(),
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
            sandbox_posture: profile.sandbox_posture(),
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
            && self.sandbox_posture == expected.sandbox_posture
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
        AutonomyProfileId::Autonomous | AutonomyProfileId::Yolo => PermissionMode::DangerFullAccess,
    }
}

#[must_use]
pub const fn approval_profile_for(profile: AutonomyProfileId) -> ApprovalProfile {
    match profile {
        AutonomyProfileId::Cautious => ApprovalProfile::Supervised,
        AutonomyProfileId::Supervised => ApprovalProfile::Balanced,
        AutonomyProfileId::Stewarded | AutonomyProfileId::Autonomous => ApprovalProfile::Autonomous,
        AutonomyProfileId::Yolo => ApprovalProfile::TrustAll,
    }
}

#[must_use]
pub const fn interruption_policy_for(profile: AutonomyProfileId) -> InterruptionPolicy {
    match profile {
        AutonomyProfileId::Cautious => InterruptionPolicy::AlwaysPauseForHuman,
        AutonomyProfileId::Supervised => InterruptionPolicy::PauseOnRisk,
        AutonomyProfileId::Stewarded | AutonomyProfileId::Autonomous => {
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
    /// Exact Session execution-policy revision that authorized this effect.
    /// Zero is accepted only while decoding historical evidence; production
    /// execution must reject an unbound lease.
    #[serde(default)]
    pub policy_revision: u64,
    /// Descriptor hash compiled from the concrete registered Tool effect.
    /// Approval or a broader permission ceiling cannot substitute a different
    /// effect under the same lease.
    #[serde(default)]
    pub effect_descriptor_hash: String,
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
                AutonomyProfileId::Autonomous,
                PermissionMode::DangerFullAccess,
                ApprovalProfile::Autonomous,
                InterruptionPolicy::ContinueWithAudit,
            ),
            (
                AutonomyProfileId::Yolo,
                PermissionMode::DangerFullAccess,
                ApprovalProfile::TrustAll,
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
            assert_eq!(policy.sandbox_posture, profile.sandbox_posture());
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

    #[test]
    fn sandbox_posture_drift_is_not_reported_as_a_preset() {
        let mut policy = SessionExecutionPolicy::from_profile(
            AutonomyProfileId::Yolo,
            1,
            SessionExecutionPolicyOrigin::SessionExplicit,
        );
        assert_eq!(policy.sandbox_posture, SandboxPosture::HostFullAccess);
        policy.sandbox_posture = SandboxPosture::ReadOnlySandbox;
        assert_eq!(policy.matched_preset(), None);
    }

    #[test]
    fn execution_binding_keeps_session_axes_and_treats_ceiling_as_independent() {
        let policy = SessionExecutionPolicy::from_profile(
            AutonomyProfileId::Yolo,
            9,
            SessionExecutionPolicyOrigin::SessionExplicit,
        );
        let binding = ExecutionPolicyBinding::bind("session-a", &policy, PermissionMode::ReadOnly);

        assert_eq!(binding.permission_mode, PermissionMode::DangerFullAccess);
        assert_eq!(binding.sandbox_posture, SandboxPosture::HostFullAccess);
        assert_eq!(binding.permission_ceiling, PermissionMode::ReadOnly);
        assert!(binding.validate().is_ok());
    }

    #[test]
    fn execution_binding_digest_rejects_any_bound_axis_tampering() {
        let policy = SessionExecutionPolicy::from_profile(
            AutonomyProfileId::Supervised,
            4,
            SessionExecutionPolicyOrigin::SessionExplicit,
        );
        let binding =
            ExecutionPolicyBinding::bind("session-a", &policy, PermissionMode::WorkspaceWrite);

        let mut revision = binding.clone();
        revision.policy_revision += 1;
        assert!(revision.validate().is_err());
        let mut posture = binding.clone();
        posture.sandbox_posture = SandboxPosture::ReadOnlySandbox;
        assert!(posture.validate().is_err());
        let mut profile = binding.clone();
        profile.autonomy_profile = AutonomyProfileId::Yolo;
        assert!(profile.validate().is_err());
        let mut ceiling = binding;
        ceiling.permission_ceiling = PermissionMode::ReadOnly;
        assert!(ceiling.validate().is_err());
    }

    #[test]
    fn policy_state_decodes_legacy_stable_snapshot_without_desired_policy() {
        let policy = SessionExecutionPolicy::from_profile(
            AutonomyProfileId::Cautious,
            1,
            SessionExecutionPolicyOrigin::ConfigDefault,
        );
        let value = serde_json::json!({"effective": policy});
        let state: SessionExecutionPolicyState =
            serde_json::from_value(value).expect("legacy state");
        assert!(state.desired.is_none());
        assert!(state.pending_transition.is_none());
    }
}
