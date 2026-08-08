//! Runtime-owned adaptive authorization negotiation.
//!
//! Effect resolvers describe an operation. This module is the sole place that
//! turns that description, the active policy, and a parent ceiling into a
//! consumable execution lease. Tools only validate the resulting lease.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::policy::{
    AuthorizationLease, AuthorizationLeaseStatus, AuthorizationLeaseTransition,
    AuthorizationLeaseTransitionKind, AuthorizationPath, CapabilityAssessment, CapabilityGap,
    CapabilityGapKind, DataClassification, EffectAssessment, EffectBlastRadius, EffectExternality,
    EffectNovelty, EffectReversibility, PermissionMode, PermissionScope, RiskLevel,
};
use harness_contract::tool::{ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind};
use sha2::{Digest, Sha256};

use crate::permissions::{PermissionContext, PermissionPolicy, PermissionPolicyRoute};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequest {
    pub principal_id: String,
    pub capability: String,
    pub input: String,
    pub idempotency_key: String,
    pub effect: ToolEffectDescriptor,
    pub parent_ceiling: PermissionMode,
    pub parent_lease_id: Option<String>,
    pub approval_satisfied: bool,
    pub recovery_scope: String,
    pub context: PermissionContext,
    pub safe_alternatives: Vec<String>,
}

/// Immutable invocation-specific effect compiled once from a registered Tool
/// descriptor and concrete input. Policy, approval, lease issuance and the
/// Tool host all consume this exact value instead of reclassifying the action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveToolAuthorizationDescriptor {
    pub descriptor: ToolEffectDescriptor,
    pub fingerprint: String,
}

/// The exact effect and policy result for one Tool invocation. Execution must
/// carry this pair forward unchanged so approval and the Tool host cannot
/// derive a different permission mode or scope from the same input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveToolAuthorizationAssessment {
    pub effective: EffectiveToolAuthorizationDescriptor,
    pub assessment: CapabilityAssessment,
}

#[derive(Debug, Clone)]
struct LeaseRegistry {
    leases: BTreeMap<String, AuthorizationLease>,
    lease_by_fingerprint: BTreeMap<String, String>,
    consumed_idempotency: BTreeSet<(String, String)>,
    revoked: BTreeSet<String>,
    recovery_claims: BTreeSet<String>,
    transitions: Vec<AuthorizationLeaseTransition>,
}

impl LeaseRegistry {
    fn new() -> Self {
        Self {
            leases: BTreeMap::new(),
            lease_by_fingerprint: BTreeMap::new(),
            consumed_idempotency: BTreeSet::new(),
            revoked: BTreeSet::new(),
            recovery_claims: BTreeSet::new(),
            transitions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthorizationNegotiator {
    registry: Arc<Mutex<LeaseRegistry>>,
    signing_secret: Arc<String>,
}

impl Default for AuthorizationNegotiator {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthorizationNegotiator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(LeaseRegistry::new())),
            signing_secret: Arc::new(uuid::Uuid::new_v4().to_string()),
        }
    }

    #[must_use]
    pub fn compile_effective_descriptor(
        descriptor: &ToolEffectDescriptor,
        input: &str,
    ) -> EffectiveToolAuthorizationDescriptor {
        let mut descriptor = descriptor.clone();
        let assessment = normalized_effect_assessment(&descriptor);
        let required_mode = required_mode_for_effect(&descriptor, &assessment);
        descriptor.assessment = assessment;
        descriptor.required_permission = required_mode;
        let original_hash = descriptor.descriptor_hash.clone();
        let payload = serde_json::json!({
            "tool_id": descriptor.tool_id,
            "registered_descriptor_hash": original_hash,
            "effect_kind": descriptor.effect_kind,
            "idempotency": descriptor.idempotency,
            "scopes": descriptor.scopes,
            "required_mode": descriptor.required_permission,
            "approval_class": descriptor.approval_class,
            "assessment": descriptor.assessment,
            "input_digest": format!("{:x}", Sha256::digest(input.as_bytes())),
        });
        let fingerprint = format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&payload).unwrap_or_default())
        );
        EffectiveToolAuthorizationDescriptor {
            descriptor,
            fingerprint,
        }
    }

    #[must_use]
    pub fn assess(
        &self,
        policy: &PermissionPolicy,
        request: &AuthorizationRequest,
    ) -> CapabilityAssessment {
        self.assess_effective(policy, request).assessment
    }

    #[must_use]
    pub fn assess_effective(
        &self,
        policy: &PermissionPolicy,
        request: &AuthorizationRequest,
    ) -> EffectiveToolAuthorizationAssessment {
        let effective = Self::compile_effective_descriptor(&request.effect, &request.input);
        let mut request = request.clone();
        request.effect = effective.descriptor.clone();
        let assessment = self.assess_compiled(policy, &request);
        EffectiveToolAuthorizationAssessment {
            effective,
            assessment,
        }
    }

    fn assess_compiled(
        &self,
        policy: &PermissionPolicy,
        request: &AuthorizationRequest,
    ) -> CapabilityAssessment {
        let now = now_ms();
        let effective = request.effect.assessment.clone();
        let required_mode = request.effect.required_permission;
        let risk = risk_for_effect(&effective);
        let active_ceiling = policy.active_mode();
        let fingerprint = capability_fingerprint(request, required_mode);

        if request.parent_lease_id.is_some() && required_mode.rank() > request.parent_ceiling.rank()
        {
            return denied_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                fingerprint,
                CapabilityGapKind::PermissionCeiling,
                "requested effect exceeds the parent Mission permission ceiling",
                false,
            );
        }

        let route = policy.route_required_with_context(
            &request.capability,
            &request.input,
            required_mode,
            &request.context,
        );
        if let PermissionPolicyRoute::HardDeny { reason } = &route {
            return denied_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                fingerprint,
                CapabilityGapKind::HardDenied,
                reason,
                false,
            );
        }

        if let Some(lease) = self.consume_existing(&fingerprint, request, required_mode, now) {
            return authorized_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                AuthorizationPath::ExistingLease,
                lease,
                "existing scoped lease",
            );
        }

        if request.approval_satisfied {
            let lease = self.issue_and_consume(
                request,
                required_mode,
                &fingerprint,
                AuthorizationPath::HumanApproval,
                now,
            );
            return authorized_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                AuthorizationPath::HumanApproval,
                lease,
                "approval was satisfied by the governing Mission strategy",
            );
        }

        match route {
            PermissionPolicyRoute::Allow {
                standing_grant,
                reason,
            } => {
                if requires_human_boundary(&effective, risk, request.effect.approval_class)
                    && (!standing_grant
                        || request.effect.approval_class == ToolApprovalClass::Administrator)
                {
                    return gap_assessment(
                        request,
                        effective,
                        required_mode,
                        active_ceiling,
                        risk,
                        fingerprint,
                        AuthorizationPath::HumanApproval,
                        CapabilityGapKind::ApprovalRequired,
                        "the assessed effect crosses a human approval boundary",
                        true,
                    );
                }
                let path = if standing_grant {
                    AuthorizationPath::StandingGrant
                } else {
                    AuthorizationPath::PolicyAutoGrant
                };
                let lease = self.issue_and_consume(request, required_mode, &fingerprint, path, now);
                authorized_assessment(
                    request,
                    effective,
                    required_mode,
                    active_ceiling,
                    risk,
                    path,
                    lease,
                    &reason,
                )
            }
            PermissionPolicyRoute::Ask { reason } => {
                if !request.safe_alternatives.is_empty()
                    && !requires_human_boundary(&effective, risk, request.effect.approval_class)
                {
                    gap_assessment(
                        request,
                        effective,
                        required_mode,
                        active_ceiling,
                        risk,
                        fingerprint,
                        AuthorizationPath::SafeAlternate,
                        CapabilityGapKind::MissingLease,
                        &reason,
                        true,
                    )
                } else {
                    gap_assessment(
                        request,
                        effective,
                        required_mode,
                        active_ceiling,
                        risk,
                        fingerprint,
                        AuthorizationPath::HumanApproval,
                        CapabilityGapKind::ApprovalRequired,
                        &reason,
                        true,
                    )
                }
            }
            PermissionPolicyRoute::HardDeny { .. } => unreachable!("handled before lease lookup"),
        }
    }

    #[must_use]
    pub fn approve_effective(
        &self,
        policy: &PermissionPolicy,
        request: &AuthorizationRequest,
        effective: &EffectiveToolAuthorizationDescriptor,
        approval_ref: &str,
    ) -> CapabilityAssessment {
        let mut request = request.clone();
        request.effect = effective.descriptor.clone();
        let request = &request;
        let now = now_ms();
        let effective = request.effect.assessment.clone();
        let required_mode = request.effect.required_permission;
        let risk = risk_for_effect(&effective);
        let active_ceiling = policy.active_mode();
        let fingerprint = capability_fingerprint(request, required_mode);
        if request.parent_lease_id.is_some() && required_mode.rank() > request.parent_ceiling.rank()
        {
            return denied_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                fingerprint,
                CapabilityGapKind::PermissionCeiling,
                "human approval cannot widen the parent Mission permission ceiling",
                false,
            );
        }
        if let PermissionPolicyRoute::HardDeny { reason } = policy.route_required_with_context(
            &request.capability,
            &request.input,
            required_mode,
            &request.context,
        ) {
            return denied_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                fingerprint,
                CapabilityGapKind::HardDenied,
                &reason,
                false,
            );
        }
        let mut lease = self.issue_and_consume(
            request,
            required_mode,
            &fingerprint,
            AuthorizationPath::HumanApproval,
            now,
        );
        lease.parent_lease_id.clone_from(&request.parent_lease_id);
        authorized_assessment(
            request,
            effective,
            required_mode,
            active_ceiling,
            risk,
            AuthorizationPath::HumanApproval,
            lease,
            &format!("verified human approval `{approval_ref}`"),
        )
    }

    pub fn revoke(&self, lease_id: &str) -> bool {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.revoked.insert(lease_id.to_string());
        let revoked = if let Some(lease) = registry.leases.get_mut(lease_id) {
            lease.status = AuthorizationLeaseStatus::Revoked;
            Some(lease.clone())
        } else {
            None
        };
        if let Some(lease) = revoked {
            registry.transitions.push(lease_transition(
                AuthorizationLeaseTransitionKind::Revoked,
                lease,
                None,
                "authorization lease explicitly revoked",
            ));
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn claim_controlled_recovery(&self, assessment: &CapabilityAssessment) -> bool {
        let Some(gap) = assessment.gap.as_ref() else {
            return false;
        };
        if !gap.recoverable {
            return false;
        }
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovery_claims
            .insert(gap.fingerprint.clone())
    }

    #[must_use]
    pub fn projection(&self) -> Vec<AuthorizationLease> {
        self.reconcile_expirations_at(now_ms());
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .leases
            .values()
            .cloned()
            .collect()
    }

    /// Reconciles TTL expiry using an explicit clock value. Runtime uses the
    /// wall clock; deterministic recovery and tests may replay a recorded
    /// timestamp.
    pub fn reconcile_expirations_at(&self, now: u64) -> usize {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut expired = Vec::new();
        for lease in registry.leases.values_mut() {
            if lease.status == AuthorizationLeaseStatus::Active && now > lease.expires_at_ms {
                lease.status = AuthorizationLeaseStatus::Expired;
                expired.push(lease.clone());
            }
        }
        registry
            .transitions
            .extend(expired.into_iter().map(|lease| {
                lease_transition(
                    AuthorizationLeaseTransitionKind::Expired,
                    lease,
                    None,
                    "authorization lease TTL elapsed",
                )
            }));
        registry
            .leases
            .values()
            .filter(|lease| lease.status == AuthorizationLeaseStatus::Expired)
            .count()
    }

    /// Returns lifecycle evidence produced since the previous drain.
    ///
    /// Conversation Runtime persists these records beside the capability
    /// assessment, while non-conversation callers may expose them through
    /// their own canonical event stream.
    pub fn drain_transitions(&self) -> Vec<AuthorizationLeaseTransition> {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut registry.transitions)
    }

    #[must_use]
    pub fn verify_signature(&self, lease: &AuthorizationLease) -> bool {
        !lease.signature.is_empty() && lease.signature == self.sign(lease)
    }

    fn consume_existing(
        &self,
        fingerprint: &str,
        request: &AuthorizationRequest,
        required_mode: PermissionMode,
        now: u64,
    ) -> Option<AuthorizationLease> {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let lease_id = registry.lease_by_fingerprint.get(fingerprint)?.clone();
        if registry.revoked.contains(&lease_id) {
            return None;
        }
        let idempotency = (lease_id.clone(), request.idempotency_key.clone());
        let already_consumed = registry.consumed_idempotency.contains(&idempotency);
        let (result, consumed_snapshot) = {
            let lease = registry.leases.get_mut(&lease_id)?;
            if already_consumed {
                return Some(execution_snapshot(lease, &request.idempotency_key));
            }
            if !lease.is_active_at(now)
                || !lease.permits(&request.capability, required_mode)
                || !lease_scopes_cover(lease, &request.effect.scopes)
            {
                return None;
            }
            lease.remaining_uses = lease.remaining_uses.saturating_sub(1);
            if lease.remaining_uses == 0 {
                lease.status = AuthorizationLeaseStatus::Exhausted;
            }
            (
                execution_snapshot(lease, &request.idempotency_key),
                lease.clone(),
            )
        };
        registry.consumed_idempotency.insert(idempotency);
        registry.transitions.push(lease_transition(
            AuthorizationLeaseTransitionKind::Consumed,
            consumed_snapshot,
            Some(request.idempotency_key.clone()),
            "existing authorization lease consumed",
        ));
        Some(result)
    }

    fn issue_and_consume(
        &self,
        request: &AuthorizationRequest,
        required_mode: PermissionMode,
        fingerprint: &str,
        path: AuthorizationPath,
        now: u64,
    ) -> AuthorizationLease {
        let max_uses = match path {
            AuthorizationPath::StandingGrant => 64,
            AuthorizationPath::PolicyAutoGrant if required_mode == PermissionMode::ReadOnly => 32,
            _ => 1,
        };
        let ttl_ms = match path {
            AuthorizationPath::HumanApproval => 30 * 60 * 1_000,
            AuthorizationPath::StandingGrant => 60 * 60 * 1_000,
            _ => 15 * 60 * 1_000,
        };
        let lease_id = format!("authorization-lease-{}", uuid::Uuid::new_v4());
        let mut lease = AuthorizationLease {
            lease_id: lease_id.clone(),
            principal_id: request.principal_id.clone(),
            parent_lease_id: request.parent_lease_id.clone(),
            capability: request.capability.clone(),
            scopes: request.effect.scopes.clone(),
            ceiling: required_mode,
            issued_at_ms: now,
            expires_at_ms: now.saturating_add(ttl_ms),
            max_uses,
            remaining_uses: max_uses,
            idempotency_key: request.idempotency_key.clone(),
            signature: String::new(),
            status: AuthorizationLeaseStatus::Active,
        };
        lease.signature = self.sign(&lease);
        let execution_lease = lease.clone();
        let issued_snapshot = lease.clone();
        lease.remaining_uses = lease.remaining_uses.saturating_sub(1);
        if lease.remaining_uses == 0 {
            lease.status = AuthorizationLeaseStatus::Exhausted;
        }
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry
            .lease_by_fingerprint
            .insert(fingerprint.to_string(), lease_id.clone());
        registry
            .consumed_idempotency
            .insert((lease_id.clone(), request.idempotency_key.clone()));
        registry.leases.insert(lease_id, lease.clone());
        registry.transitions.push(lease_transition(
            AuthorizationLeaseTransitionKind::Issued,
            issued_snapshot,
            None,
            "authorization negotiator issued a scoped lease",
        ));
        registry.transitions.push(lease_transition(
            AuthorizationLeaseTransitionKind::Consumed,
            lease,
            Some(request.idempotency_key.clone()),
            "new authorization lease consumed for the assessed request",
        ));
        execution_lease
    }

    fn sign(&self, lease: &AuthorizationLease) -> String {
        let payload = serde_json::to_vec(&serde_json::json!({
            "lease_id": lease.lease_id,
            "principal_id": lease.principal_id,
            "parent_lease_id": lease.parent_lease_id,
            "capability": lease.capability,
            "scopes": lease.scopes,
            "ceiling": lease.ceiling,
            "issued_at_ms": lease.issued_at_ms,
            "expires_at_ms": lease.expires_at_ms,
            "max_uses": lease.max_uses,
        }))
        .unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(self.signing_secret.as_bytes());
        hasher.update(payload);
        format!("sha256:{:x}", hasher.finalize())
    }
}

fn normalized_effect_assessment(effect: &ToolEffectDescriptor) -> EffectAssessment {
    if effect.assessment != EffectAssessment::default() {
        return effect.assessment.clone();
    }
    match effect.effect_kind {
        ToolEffectKind::Read => EffectAssessment {
            reversibility: EffectReversibility::Reversible,
            externality: EffectExternality::Internal,
            data_sensitivity: DataClassification::Internal,
            novelty: EffectNovelty::Routine,
            blast_radius: EffectBlastRadius::Item,
        },
        ToolEffectKind::Write => EffectAssessment {
            reversibility: EffectReversibility::Compensatable,
            externality: EffectExternality::Workspace,
            data_sensitivity: DataClassification::Internal,
            novelty: EffectNovelty::Routine,
            blast_radius: EffectBlastRadius::Workspace,
        },
        ToolEffectKind::Network => EffectAssessment {
            reversibility: EffectReversibility::Reversible,
            externality: EffectExternality::NetworkRead,
            data_sensitivity: DataClassification::Public,
            novelty: EffectNovelty::Routine,
            blast_radius: EffectBlastRadius::Item,
        },
        ToolEffectKind::Process | ToolEffectKind::Package => EffectAssessment {
            reversibility: EffectReversibility::Compensatable,
            externality: EffectExternality::Workspace,
            data_sensitivity: DataClassification::Internal,
            novelty: EffectNovelty::NewTarget,
            blast_radius: EffectBlastRadius::Workspace,
        },
        ToolEffectKind::System | ToolEffectKind::Destructive | ToolEffectKind::Unknown => {
            EffectAssessment {
                reversibility: EffectReversibility::Unknown,
                externality: EffectExternality::System,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::Unknown,
                blast_radius: EffectBlastRadius::System,
            }
        }
    }
}

fn required_mode_for_effect(
    descriptor: &ToolEffectDescriptor,
    assessment: &EffectAssessment,
) -> PermissionMode {
    if matches!(
        assessment.externality,
        EffectExternality::System | EffectExternality::ExternalMutation
    ) || matches!(assessment.reversibility, EffectReversibility::Irreversible)
        || matches!(assessment.data_sensitivity, DataClassification::Secret)
    {
        return PermissionMode::DangerFullAccess;
    }
    if matches!(assessment.externality, EffectExternality::Workspace)
        || matches!(
            descriptor.effect_kind,
            ToolEffectKind::Write | ToolEffectKind::Process | ToolEffectKind::Package
        )
    {
        return PermissionMode::WorkspaceWrite;
    }
    if assessment.externality == EffectExternality::NetworkRead
        && assessment.data_sensitivity == DataClassification::Public
    {
        return PermissionMode::ReadOnly;
    }
    descriptor.required_permission
}

fn risk_for_effect(assessment: &EffectAssessment) -> RiskLevel {
    if assessment.blast_radius >= EffectBlastRadius::System
        || assessment.reversibility == EffectReversibility::Irreversible
    {
        RiskLevel::Critical
    } else if assessment.externality == EffectExternality::ExternalMutation
        || assessment.data_sensitivity == DataClassification::Secret
        || assessment.blast_radius == EffectBlastRadius::ExternalAccount
    {
        RiskLevel::High
    } else if assessment.externality == EffectExternality::Workspace
        || assessment.data_sensitivity == DataClassification::Confidential
        || assessment.novelty == EffectNovelty::NewCapability
    {
        RiskLevel::Medium
    } else {
        RiskLevel::Low
    }
}

fn requires_human_boundary(
    effect: &EffectAssessment,
    risk: RiskLevel,
    approval_class: ToolApprovalClass,
) -> bool {
    approval_class == ToolApprovalClass::Administrator
        || approval_class == ToolApprovalClass::User
        || risk >= RiskLevel::High
        || effect.externality == EffectExternality::ExternalMutation
        || effect.data_sensitivity == DataClassification::Secret
}

fn capability_fingerprint(request: &AuthorizationRequest, required_mode: PermissionMode) -> String {
    let payload = serde_json::json!({
        "principal": request.principal_id,
        "capability": request.capability,
        "scopes": request.effect.scopes,
        "required_mode": required_mode,
        "parent_ceiling": request.parent_ceiling,
        "recovery_scope": request.recovery_scope,
        "effect_descriptor": request.effect.descriptor_hash,
        "input_digest": format!("{:x}", Sha256::digest(request.input.as_bytes())),
    });
    format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&payload).unwrap_or_default())
    )
}

fn lease_scopes_cover(lease: &AuthorizationLease, requested: &[PermissionScope]) -> bool {
    requested.iter().all(|scope| lease.scopes.contains(scope))
}

fn execution_snapshot(lease: &AuthorizationLease, idempotency_key: &str) -> AuthorizationLease {
    let mut snapshot = lease.clone();
    snapshot.idempotency_key = idempotency_key.to_string();
    snapshot.remaining_uses = 1;
    snapshot.status = AuthorizationLeaseStatus::Active;
    snapshot
}

fn lease_transition(
    kind: AuthorizationLeaseTransitionKind,
    lease: AuthorizationLease,
    idempotency_key: Option<String>,
    evidence: &str,
) -> AuthorizationLeaseTransition {
    AuthorizationLeaseTransition {
        transition_id: format!("authorization-transition-{}", uuid::Uuid::new_v4()),
        kind,
        lease,
        idempotency_key,
        occurred_at_ms: now_ms(),
        evidence: evidence.to_string(),
    }
}

fn authorized_assessment(
    request: &AuthorizationRequest,
    effect: EffectAssessment,
    required_mode: PermissionMode,
    active_ceiling: PermissionMode,
    risk: RiskLevel,
    path: AuthorizationPath,
    lease: AuthorizationLease,
    evidence: &str,
) -> CapabilityAssessment {
    CapabilityAssessment {
        assessment_id: format!("capability-assessment-{}", uuid::Uuid::new_v4()),
        capability: request.capability.clone(),
        effect,
        requested_scopes: request.effect.scopes.clone(),
        required_mode,
        active_ceiling,
        parent_ceiling: request.parent_ceiling,
        risk,
        path,
        lease: Some(lease),
        gap: None,
        evidence_refs: vec![evidence.to_string()],
        assessed_at_ms: now_ms(),
    }
}

#[allow(clippy::too_many_arguments)]
fn gap_assessment(
    request: &AuthorizationRequest,
    effect: EffectAssessment,
    required_mode: PermissionMode,
    active_ceiling: PermissionMode,
    risk: RiskLevel,
    fingerprint: String,
    path: AuthorizationPath,
    kind: CapabilityGapKind,
    reason: &str,
    recoverable: bool,
) -> CapabilityAssessment {
    CapabilityAssessment {
        assessment_id: format!("capability-assessment-{}", uuid::Uuid::new_v4()),
        capability: request.capability.clone(),
        effect,
        requested_scopes: request.effect.scopes.clone(),
        required_mode,
        active_ceiling,
        parent_ceiling: request.parent_ceiling,
        risk,
        path,
        lease: None,
        gap: Some(CapabilityGap {
            fingerprint,
            kind,
            capability: request.capability.clone(),
            requested_scopes: request.effect.scopes.clone(),
            required_mode,
            active_ceiling,
            parent_ceiling: request.parent_ceiling,
            reason: reason.to_string(),
            safe_alternatives: request.safe_alternatives.clone(),
            recoverable,
        }),
        evidence_refs: vec![reason.to_string()],
        assessed_at_ms: now_ms(),
    }
}

#[allow(clippy::too_many_arguments)]
fn denied_assessment(
    request: &AuthorizationRequest,
    effect: EffectAssessment,
    required_mode: PermissionMode,
    active_ceiling: PermissionMode,
    risk: RiskLevel,
    fingerprint: String,
    kind: CapabilityGapKind,
    reason: &str,
    recoverable: bool,
) -> CapabilityAssessment {
    gap_assessment(
        request,
        effect,
        required_mode,
        active_ceiling,
        risk,
        fingerprint,
        AuthorizationPath::HardDeny,
        kind,
        reason,
        recoverable,
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use harness_contract::policy::{
        EffectAssessment, EffectBlastRadius, EffectExternality, EffectNovelty, EffectReversibility,
        PermissionOperation, PermissionResource,
    };
    use harness_contract::tool::{ToolApprovalClass, ToolIdempotency, ToolPermissionMode};

    use super::*;

    fn effect(
        required_permission: PermissionMode,
        externality: EffectExternality,
    ) -> ToolEffectDescriptor {
        ToolEffectDescriptor {
            tool_id: "test_tool".to_string(),
            descriptor_hash: "test".to_string(),
            effect_kind: if externality == EffectExternality::Workspace {
                ToolEffectKind::Write
            } else {
                ToolEffectKind::Read
            },
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation: if externality == EffectExternality::Workspace {
                    PermissionOperation::Write
                } else {
                    PermissionOperation::Read
                },
                target: Some("src/lib.rs".to_string()),
            }],
            required_permission,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment {
                reversibility: EffectReversibility::Compensatable,
                externality,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::Routine,
                blast_radius: EffectBlastRadius::Workspace,
            },
        }
    }

    fn request(effect: ToolEffectDescriptor) -> AuthorizationRequest {
        AuthorizationRequest {
            principal_id: "session:test".to_string(),
            capability: "test_tool".to_string(),
            input: "{}".to_string(),
            idempotency_key: "call-1".to_string(),
            effect,
            parent_ceiling: PermissionMode::WorkspaceWrite,
            parent_lease_id: None,
            approval_satisfied: false,
            recovery_scope: "turn:test".to_string(),
            context: PermissionContext::default(),
            safe_alternatives: vec!["return_patch".to_string()],
        }
    }

    #[test]
    fn invocation_fingerprint_never_replaces_the_tool_host_descriptor_identity() {
        let descriptor = effect(PermissionMode::ReadOnly, EffectExternality::Internal);
        let first =
            AuthorizationNegotiator::compile_effective_descriptor(&descriptor, r#"{"path":"a"}"#);
        let second =
            AuthorizationNegotiator::compile_effective_descriptor(&descriptor, r#"{"path":"b"}"#);

        assert_eq!(first.descriptor.descriptor_hash, descriptor.descriptor_hash);
        assert_eq!(
            second.descriptor.descriptor_hash,
            descriptor.descriptor_hash
        );
        assert_ne!(first.fingerprint, second.fingerprint);
    }

    #[test]
    fn low_risk_read_gets_scoped_auto_lease() {
        let mut descriptor = effect(PermissionMode::ReadOnly, EffectExternality::Internal);
        descriptor.assessment.blast_radius = EffectBlastRadius::Item;
        let assessment = AuthorizationNegotiator::new().assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request(descriptor),
        );
        assert_eq!(assessment.path, AuthorizationPath::PolicyAutoGrant);
        assert!(assessment.lease.is_some());
    }

    #[test]
    fn child_cannot_expand_parent_ceiling() {
        let mut request = request(effect(
            PermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        request.parent_ceiling = PermissionMode::ReadOnly;
        request.parent_lease_id = Some("parent:read-only".to_string());
        let assessment = AuthorizationNegotiator::new().assess(
            &PermissionPolicy::new(PermissionMode::DangerFullAccess),
            &request,
        );
        assert_eq!(assessment.path, AuthorizationPath::HardDeny);
        assert_eq!(
            assessment.gap.as_ref().map(|gap| gap.kind),
            Some(CapabilityGapKind::PermissionCeiling)
        );
    }

    #[test]
    fn standing_grant_reuses_lease_without_duplicate_consumption() {
        let rules = crate::RuntimePermissionRuleConfig::new(
            vec!["test_tool(*)".to_string()],
            Vec::new(),
            Vec::new(),
        );
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly).with_permission_rules(&rules);
        let negotiator = AuthorizationNegotiator::new();
        let request = request(effect(
            ToolPermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        let first = negotiator.assess(&policy, &request);
        let second = negotiator.assess(&policy, &request);
        assert_eq!(first.path, AuthorizationPath::StandingGrant);
        assert_eq!(second.path, AuthorizationPath::ExistingLease);
        assert_eq!(
            first.lease.as_ref().map(|lease| lease.lease_id.as_str()),
            second.lease.as_ref().map(|lease| lease.lease_id.as_str())
        );
    }

    #[test]
    fn reversible_workspace_write_gets_a_bounded_policy_lease() {
        let assessment = AuthorizationNegotiator::new().assess(
            &PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            &request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            )),
        );
        let lease = assessment.lease.expect("workspace lease");
        assert_eq!(assessment.path, AuthorizationPath::PolicyAutoGrant);
        assert_eq!(lease.ceiling, PermissionMode::WorkspaceWrite);
        assert_eq!(lease.max_uses, 1);
        assert!(lease.expires_at_ms > lease.issued_at_ms);
        assert!(!lease.signature.is_empty());
    }

    #[test]
    fn external_mutation_requires_human_approval() {
        let mut descriptor = effect(
            PermissionMode::DangerFullAccess,
            EffectExternality::ExternalMutation,
        );
        descriptor.approval_class = ToolApprovalClass::User;
        descriptor.assessment.blast_radius = EffectBlastRadius::ExternalAccount;
        let assessment = AuthorizationNegotiator::new().assess(
            &PermissionPolicy::new(PermissionMode::DangerFullAccess),
            &request(descriptor),
        );
        assert_eq!(assessment.path, AuthorizationPath::HumanApproval);
        assert_eq!(
            assessment.gap.as_ref().map(|gap| gap.kind),
            Some(CapabilityGapKind::ApprovalRequired)
        );
    }

    #[test]
    fn hard_deny_cannot_be_bypassed_by_approval_or_full_access() {
        let rules = crate::RuntimePermissionRuleConfig::new(
            Vec::new(),
            vec!["test_tool(*)".to_string()],
            Vec::new(),
        );
        let policy =
            PermissionPolicy::new(PermissionMode::DangerFullAccess).with_permission_rules(&rules);
        let mut request = request(effect(
            PermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        request.approval_satisfied = true;
        let assessment = AuthorizationNegotiator::new().assess(&policy, &request);
        assert_eq!(assessment.path, AuthorizationPath::HardDeny);
        assert!(assessment.lease.is_none());
        let effective =
            AuthorizationNegotiator::compile_effective_descriptor(&request.effect, &request.input);
        let approved = AuthorizationNegotiator::new().approve_effective(
            &policy,
            &request,
            &effective,
            "approval:attempt",
        );
        assert_eq!(approved.path, AuthorizationPath::HardDeny);
        assert!(approved.lease.is_none());
    }

    #[test]
    fn standing_grant_does_not_cover_a_different_target() {
        let rules = crate::RuntimePermissionRuleConfig::new(
            vec!["test_tool(src/lib.rs)".to_string()],
            Vec::new(),
            Vec::new(),
        );
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly).with_permission_rules(&rules);
        let negotiator = AuthorizationNegotiator::new();
        let mut granted = request(effect(
            PermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        granted.input = r#"{"path":"src/lib.rs"}"#.to_string();
        assert_eq!(
            negotiator.assess(&policy, &granted).path,
            AuthorizationPath::StandingGrant
        );

        let mut outside = granted;
        outside.idempotency_key = "call-2".to_string();
        outside.input = r#"{"path":"src/main.rs"}"#.to_string();
        outside.effect.scopes[0].target = Some("src/main.rs".to_string());
        let assessment = negotiator.assess(&policy, &outside);
        assert_eq!(assessment.path, AuthorizationPath::SafeAlternate);
        assert!(assessment.lease.is_none());
    }

    #[test]
    fn controlled_recovery_is_claimed_once_per_gap_fingerprint() {
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            )),
        );
        assert!(assessment.gap.as_ref().is_some_and(|gap| gap.recoverable));
        assert!(negotiator.claim_controlled_recovery(&assessment));
        assert!(!negotiator.claim_controlled_recovery(&assessment));
    }

    #[test]
    fn lease_signature_and_lifecycle_evidence_survive_consumption() {
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request({
                let mut descriptor = effect(PermissionMode::ReadOnly, EffectExternality::Internal);
                descriptor.assessment.blast_radius = EffectBlastRadius::Item;
                descriptor
            }),
        );
        let lease = assessment.lease.expect("read lease");
        assert!(negotiator.verify_signature(&lease));
        let transitions = negotiator.drain_transitions();
        assert_eq!(transitions.len(), 2);
        assert_eq!(
            transitions[0].kind,
            AuthorizationLeaseTransitionKind::Issued
        );
        assert_eq!(
            transitions[1].kind,
            AuthorizationLeaseTransitionKind::Consumed
        );

        assert!(negotiator.revoke(&lease.lease_id));
        assert_eq!(
            negotiator.drain_transitions()[0].kind,
            AuthorizationLeaseTransitionKind::Revoked
        );
    }

    #[test]
    fn expiration_is_reconciled_without_executing_the_tool() {
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request({
                let mut descriptor = effect(PermissionMode::ReadOnly, EffectExternality::Internal);
                descriptor.assessment.blast_radius = EffectBlastRadius::Item;
                descriptor
            }),
        );
        let lease = assessment.lease.expect("read lease");
        negotiator.drain_transitions();
        negotiator.reconcile_expirations_at(lease.expires_at_ms.saturating_add(1));
        assert_eq!(
            negotiator
                .projection()
                .into_iter()
                .find(|candidate| candidate.lease_id == lease.lease_id)
                .map(|candidate| candidate.status),
            Some(AuthorizationLeaseStatus::Expired)
        );
        assert_eq!(
            negotiator.drain_transitions()[0].kind,
            AuthorizationLeaseTransitionKind::Expired
        );
    }
}
