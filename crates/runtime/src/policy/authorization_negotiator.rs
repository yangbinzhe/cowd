//! Runtime-owned adaptive authorization negotiation.
//!
//! Effect resolvers describe an operation. This module is the sole place that
//! turns that description, the active policy, and a parent ceiling into a
//! consumable execution lease. Tools only validate the resulting lease.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::policy::{
    ApprovalGrant, ApprovalGrantStatus, AuthorizationLease, AuthorizationLeaseStatus,
    AuthorizationLeaseTransition, AuthorizationLeaseTransitionKind, AuthorizationPath,
    CapabilityAssessment, CapabilityGap, CapabilityGapKind, DataClassification, EffectAssessment,
    EffectBlastRadius, EffectExternality, EffectNovelty, EffectReversibility, PermissionMode,
    PermissionScope, RiskLevel,
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
    /// Exact immutable Session policy generation used to compile this request.
    pub policy_revision: u64,
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

pub(crate) const CONTROLLED_RECOVERY_CLAIMED_EVENT: &str =
    "authorization.controlled_recovery_claimed";
pub(crate) const CONTROLLED_RECOVERY_TERMINAL_EVENT: &str =
    "authorization.controlled_recovery_terminal";

/// Durable identity of the single controlled recovery granted inside one
/// exact turn. This is an event payload, not a second recovery state machine.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ControlledRecoveryClaimRecord {
    pub fingerprint: String,
    pub recovery_scope: String,
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub capability: String,
}

/// Terminal evidence committed in the same transaction as the containing
/// graph terminal (and, for Gateway turns, the Session terminal outbox row).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ControlledRecoveryTerminalRecord {
    pub recovery_scope: String,
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub fingerprints: Vec<String>,
}

#[must_use]
pub(crate) fn controlled_recovery_stream_id(
    session_id: &str,
    turn_id: &str,
    execution_id: &str,
) -> String {
    format!("authorization-recovery:{session_id}:turn:{turn_id}:execution:{execution_id}")
}

fn legacy_controlled_recovery_stream_id(session_id: &str, turn_id: &str) -> String {
    format!("authorization-recovery:{session_id}:turn:{turn_id}")
}

fn controlled_recovery_claim_idempotency_key(fingerprint: &str) -> String {
    format!("authorization-recovery-claim:{fingerprint}")
}

fn controlled_recovery_terminal_idempotency_key(
    session_id: &str,
    turn_id: &str,
    execution_id: &str,
) -> String {
    format!("authorization-recovery-terminal:{session_id}:turn:{turn_id}:execution:{execution_id}")
}

fn persisted_controlled_recovery_claim(
    store: &crate::RuntimeEventStore,
    stream_id: &str,
    idempotency_key: &str,
) -> Result<Option<ControlledRecoveryClaimRecord>, String> {
    store
        .event_by_idempotency_key(stream_id, idempotency_key)
        .map_err(|error| error.to_string())?
        .map(|event| {
            serde_json::from_value::<ControlledRecoveryClaimRecord>(event.payload)
                .map_err(|error| format!("decode controlled recovery claim: {error}"))
        })
        .transpose()
}

/// Persists a claim before Runtime tells the model that recovery is available.
/// A stream is owned by one exact execution inside one exact turn; optimistic
/// conflicts between parallel Tool assessments in that execution are retried
/// with stable event identity. Sibling Agent/Team executions never share this
/// terminal key.
pub(crate) fn persist_controlled_recovery_claim(
    store: &crate::RuntimeEventStore,
    record: &ControlledRecoveryClaimRecord,
) -> Result<(), String> {
    let expected_scope = format!("turn:{}", record.turn_id);
    if record.fingerprint.trim().is_empty()
        || record.session_id.trim().is_empty()
        || record.turn_id.trim().is_empty()
        || record.execution_id.trim().is_empty()
        || record.recovery_scope != expected_scope
    {
        return Err(
            "controlled recovery claim requires exact Session/execution/turn identity".to_string(),
        );
    }
    let stream_id =
        controlled_recovery_stream_id(&record.session_id, &record.turn_id, &record.execution_id);
    let idempotency_key = controlled_recovery_claim_idempotency_key(&record.fingerprint);
    for _ in 0..8 {
        if store
            .event_by_idempotency_key(
                &stream_id,
                &controlled_recovery_terminal_idempotency_key(
                    &record.session_id,
                    &record.turn_id,
                    &record.execution_id,
                ),
            )
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("controlled recovery turn is already terminal".to_string());
        }
        if let Some(persisted) =
            persisted_controlled_recovery_claim(store, &stream_id, &idempotency_key)?
        {
            return (persisted == *record)
                .then_some(())
                .ok_or_else(|| "controlled recovery claim identity collision".to_string());
        }
        let revision = store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let request = crate::AppendTransactionRequest {
            transaction_id: idempotency_key.clone(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: revision,
            }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: crate::RuntimeEventScope::Tool,
                    kind: CONTROLLED_RECOVERY_CLAIMED_EVENT.to_string(),
                    status: Some("claimed".to_string()),
                    actor: Some("authorization_negotiator".to_string()),
                    refs: vec![
                        crate::RuntimeEventRef {
                            kind: "capability_gap".to_string(),
                            id: record.fingerprint.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "execution".to_string(),
                            id: record.execution_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "session".to_string(),
                            id: record.session_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: record.turn_id.clone(),
                        },
                    ],
                    payload: serde_json::to_value(record)
                        .map_err(|error| format!("encode controlled recovery claim: {error}"))?,
                },
                idempotency_key: Some(idempotency_key.clone()),
                schema_version: 1,
            }],
        };
        match store.append_transaction(request) {
            Ok(_) => return Ok(()),
            Err(crate::RuntimeEventStoreError::StaleRevision { .. })
            | Err(crate::RuntimeEventStoreError::TransactionConflict { .. }) => {
                if let Some(persisted) =
                    persisted_controlled_recovery_claim(store, &stream_id, &idempotency_key)?
                {
                    return (persisted == *record)
                        .then_some(())
                        .ok_or_else(|| "controlled recovery claim identity collision".to_string());
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("controlled recovery claim persistence conflict retry exhausted".to_string())
}

/// Loads only claims for an unterminated exact turn. Terminal is monotonic: a
/// later malformed/duplicate claim can never reopen a committed turn.
pub(crate) fn load_open_controlled_recovery_claims(
    store: &crate::RuntimeEventStore,
    session_id: &str,
    turn_id: &str,
    execution_id: &str,
) -> Result<Vec<ControlledRecoveryClaimRecord>, String> {
    if execution_id.trim().is_empty() {
        return Err("controlled recovery restore requires an exact execution".to_string());
    }
    let stream_id = controlled_recovery_stream_id(session_id, turn_id, execution_id);
    let exact_events = store.list_stream(&stream_id)?;
    // v0.9.689 used a turn-wide stream. Read that shape only when the exact
    // execution stream does not exist, and filter rather than treating sibling
    // execution records as corruption. New writes never return to this path.
    let (events, exact_stream) = if exact_events.is_empty() {
        (
            store.list_stream(&legacy_controlled_recovery_stream_id(session_id, turn_id))?,
            false,
        )
    } else {
        (exact_events, true)
    };
    let mut claims = BTreeMap::<String, ControlledRecoveryClaimRecord>::new();
    let mut terminal = false;
    for event in events {
        match event.kind.as_str() {
            CONTROLLED_RECOVERY_CLAIMED_EVENT => {
                let claim = serde_json::from_value::<ControlledRecoveryClaimRecord>(event.payload)
                    .map_err(|error| format!("decode controlled recovery claim: {error}"))?;
                if !exact_stream && claim.execution_id != execution_id {
                    continue;
                }
                if claim.session_id != session_id
                    || claim.turn_id != turn_id
                    || claim.execution_id != execution_id
                    || claim.recovery_scope != format!("turn:{turn_id}")
                {
                    return Err(
                        "controlled recovery claim escaped its durable turn stream".to_string()
                    );
                }
                claims.insert(claim.fingerprint.clone(), claim);
            }
            CONTROLLED_RECOVERY_TERMINAL_EVENT => {
                let settled =
                    serde_json::from_value::<ControlledRecoveryTerminalRecord>(event.payload)
                        .map_err(|error| format!("decode controlled recovery terminal: {error}"))?;
                if !exact_stream && settled.execution_id != execution_id {
                    continue;
                }
                if settled.session_id != session_id
                    || settled.turn_id != turn_id
                    || settled.execution_id != execution_id
                    || settled.recovery_scope != format!("turn:{turn_id}")
                {
                    return Err(
                        "controlled recovery terminal escaped its durable turn stream".to_string(),
                    );
                }
                terminal = true;
            }
            _ => {}
        }
    }
    if terminal {
        Ok(Vec::new())
    } else {
        Ok(claims.into_values().collect())
    }
}

pub(crate) fn controlled_recovery_terminal_event(
    record: &ControlledRecoveryTerminalRecord,
) -> Result<crate::RuntimeTransactionEventInput, String> {
    if record.recovery_scope != format!("turn:{}", record.turn_id) {
        return Err("controlled recovery terminal requires an exact turn scope".to_string());
    }
    Ok(crate::RuntimeTransactionEventInput {
        event: crate::RuntimeEventInput {
            stream_id: controlled_recovery_stream_id(
                &record.session_id,
                &record.turn_id,
                &record.execution_id,
            ),
            scope: crate::RuntimeEventScope::Tool,
            kind: CONTROLLED_RECOVERY_TERMINAL_EVENT.to_string(),
            status: Some("terminal".to_string()),
            actor: Some("SynthesizeNodeExecutor".to_string()),
            refs: vec![
                crate::RuntimeEventRef {
                    kind: "execution".to_string(),
                    id: record.execution_id.clone(),
                },
                crate::RuntimeEventRef {
                    kind: "session".to_string(),
                    id: record.session_id.clone(),
                },
                crate::RuntimeEventRef {
                    kind: "turn".to_string(),
                    id: record.turn_id.clone(),
                },
            ],
            payload: serde_json::to_value(record)
                .map_err(|error| format!("encode controlled recovery terminal: {error}"))?,
        },
        idempotency_key: Some(controlled_recovery_terminal_idempotency_key(
            &record.session_id,
            &record.turn_id,
            &record.execution_id,
        )),
        schema_version: 1,
    })
}

/// Admit only the Runtime-owned controlled-recovery terminal through an
/// ExecutionGraph transaction.  Tool scope is otherwise protected from graph
/// executors; this narrow validator preserves that boundary while allowing the
/// recovery claim terminal to commit atomically with the turn terminal.
pub(crate) fn is_controlled_recovery_terminal_event(input: &crate::RuntimeEventInput) -> bool {
    if input.scope != crate::RuntimeEventScope::Tool
        || input.kind != CONTROLLED_RECOVERY_TERMINAL_EVENT
        || input.status.as_deref() != Some("terminal")
    {
        return false;
    }
    let Ok(record) =
        serde_json::from_value::<ControlledRecoveryTerminalRecord>(input.payload.clone())
    else {
        return false;
    };
    record.recovery_scope == format!("turn:{}", record.turn_id)
        && input.stream_id
            == controlled_recovery_stream_id(
                &record.session_id,
                &record.turn_id,
                &record.execution_id,
            )
        && input
            .refs
            .iter()
            .any(|reference| reference.kind == "execution" && reference.id == record.execution_id)
        && input
            .refs
            .iter()
            .any(|reference| reference.kind == "session" && reference.id == record.session_id)
        && input
            .refs
            .iter()
            .any(|reference| reference.kind == "turn" && reference.id == record.turn_id)
}

#[derive(Debug, Clone)]
struct LeaseRegistry {
    leases: BTreeMap<String, AuthorizationLease>,
    lease_by_fingerprint: BTreeMap<String, String>,
    fingerprint_by_lease: BTreeMap<String, String>,
    consumed_idempotency: BTreeSet<(String, String)>,
    revoked: BTreeSet<String>,
    /// One live controlled-recovery opportunity per capability fingerprint.
    /// The value is the exact durable turn scope which owns the claim; this
    /// lets the containing graph terminal release only its own claims.
    recovery_claims: BTreeMap<String, String>,
    transitions: Vec<AuthorizationLeaseTransition>,
    transitions_awaiting_persistence: VecDeque<AuthorizationLeaseTransition>,
}

impl LeaseRegistry {
    fn new() -> Self {
        Self {
            leases: BTreeMap::new(),
            lease_by_fingerprint: BTreeMap::new(),
            fingerprint_by_lease: BTreeMap::new(),
            consumed_idempotency: BTreeSet::new(),
            revoked: BTreeSet::new(),
            recovery_claims: BTreeMap::new(),
            transitions: Vec::new(),
            transitions_awaiting_persistence: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthorizationNegotiator {
    registry: Arc<Mutex<LeaseRegistry>>,
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
                let lease = self.issue_and_consume(
                    request,
                    required_mode,
                    active_ceiling,
                    &fingerprint,
                    path,
                    now,
                );
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
            PermissionPolicyRoute::HardDeny { reason } => denied_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                fingerprint,
                CapabilityGapKind::HardDenied,
                &reason,
                false,
            ),
        }
    }

    #[must_use]
    pub fn approve_effective(
        &self,
        policy: &PermissionPolicy,
        request: &AuthorizationRequest,
        effective: &EffectiveToolAuthorizationDescriptor,
        grant: &ApprovalGrant,
    ) -> CapabilityAssessment {
        let mut request = request.clone();
        request.effect = effective.descriptor.clone();
        let request = &request;
        let effective_descriptor_hash = effective.descriptor.descriptor_hash.clone();
        let now = now_ms();
        let effective = request.effect.assessment.clone();
        let required_mode = request.effect.required_permission;
        let risk = risk_for_effect(&effective);
        let active_ceiling = policy.active_mode();
        let fingerprint = capability_fingerprint(request, required_mode);
        if let Err(reason) = validate_approval_grant(grant, request, &effective_descriptor_hash) {
            return denied_assessment(
                request,
                effective,
                required_mode,
                active_ceiling,
                risk,
                fingerprint,
                CapabilityGapKind::ApprovalRequired,
                &reason,
                true,
            );
        }
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
            active_ceiling,
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
            &format!("verified human approval `{}`", grant.grant_id),
        )
    }

    pub fn revoke(&self, lease_id: &str) -> bool {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revoked = {
            let Some(lease) = registry.leases.get_mut(lease_id) else {
                return false;
            };
            if lease.status == AuthorizationLeaseStatus::Revoked {
                return true;
            }
            lease.status = AuthorizationLeaseStatus::Revoked;
            lease.clone()
        };
        registry.revoked.insert(lease_id.to_string());
        registry.transitions.push(lease_transition(
            AuthorizationLeaseTransitionKind::Revoked,
            revoked,
            None,
            "authorization lease explicitly revoked",
        ));
        true
    }

    #[must_use]
    pub fn claim_controlled_recovery(
        &self,
        assessment: &CapabilityAssessment,
        recovery_scope: &str,
    ) -> bool {
        let Some(gap) = assessment.gap.as_ref() else {
            return false;
        };
        if !gap.recoverable || !recovery_scope.starts_with("turn:") {
            return false;
        }
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.recovery_claims.contains_key(&gap.fingerprint) {
            return false;
        }
        registry
            .recovery_claims
            .insert(gap.fingerprint.clone(), recovery_scope.to_string());
        true
    }

    /// Restores one claim from the exact current turn's durable recovery
    /// stream. Callers must first prove that the turn has no terminal event.
    pub(crate) fn restore_controlled_recovery_claim(
        &self,
        fingerprint: &str,
        recovery_scope: &str,
    ) -> bool {
        if fingerprint.trim().is_empty() || !recovery_scope.starts_with("turn:") {
            return false;
        }
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.recovery_claims.contains_key(fingerprint) {
            return false;
        }
        registry
            .recovery_claims
            .insert(fingerprint.to_string(), recovery_scope.to_string());
        true
    }

    /// Returns the exact claims owned by one turn in stable fingerprint order.
    #[must_use]
    pub(crate) fn controlled_recovery_claims_for_scope(&self, recovery_scope: &str) -> Vec<String> {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovery_claims
            .iter()
            .filter(|(_, scope)| scope.as_str() == recovery_scope)
            .map(|(fingerprint, _)| fingerprint.clone())
            .collect()
    }

    /// Rolls back a hot claim which could not be durably recorded. This is
    /// deliberately distinct from terminal acknowledgement: no lifecycle
    /// completion is asserted.
    pub(crate) fn rollback_unpersisted_controlled_recovery_claim(&self, fingerprint: &str) -> bool {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovery_claims
            .remove(fingerprint)
            .is_some()
    }

    /// Releases process-local claims only after their containing graph/turn
    /// terminal has committed durably. A provider or Tool return is not an
    /// acknowledgement boundary.
    pub(crate) fn acknowledge_controlled_recovery_terminals(
        &self,
        fingerprints: &[String],
    ) -> usize {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fingerprints
            .iter()
            .filter(|fingerprint| registry.recovery_claims.remove(*fingerprint).is_some())
            .count()
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

    /// Legacy volatile handoff used by callers that have not yet connected a
    /// durable acknowledgement. It intentionally does not retain an unbounded
    /// in-flight copy; durable owners must migrate to
    /// [`Self::take_transitions_for_persistence`] before enabling hot cleanup.
    pub fn drain_transitions(&self) -> Vec<AuthorizationLeaseTransition> {
        std::mem::take(
            &mut self
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .transitions,
        )
    }

    /// Transfers new lifecycle evidence into an in-flight persistence set.
    /// The durable writer must call [`Self::acknowledge_persisted_transitions`]
    /// only after every selected transition is committed. Unacknowledged
    /// transitions remain available through
    /// [`Self::transitions_awaiting_persistence`] for exact retry. The durable
    /// owner must apply admission backpressure while persistence is degraded;
    /// this registry never discards unacknowledged evidence to fake a bound.
    pub fn take_transitions_for_persistence(&self) -> Vec<AuthorizationLeaseTransition> {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transitions = std::mem::take(&mut registry.transitions);
        registry
            .transitions_awaiting_persistence
            .extend(transitions.iter().cloned());
        transitions
    }

    /// Returns transition evidence that was handed to a durable writer but has
    /// not yet been acknowledged. The transition id is stable, so a writer can
    /// retry with an idempotent durable key without reconstructing lease state.
    #[must_use]
    pub fn transitions_awaiting_persistence(&self) -> Vec<AuthorizationLeaseTransition> {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .transitions_awaiting_persistence
            .iter()
            .cloned()
            .collect()
    }

    /// Acknowledges transitions that are already durable and releases terminal
    /// lease indexes. Active leases and their idempotency receipts remain hot,
    /// so retries still reuse the same scoped lease while it is valid.
    pub fn acknowledge_persisted_transitions(&self, transition_ids: &[String]) -> usize {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let selected = transition_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if selected.len() == 1
            && registry
                .transitions_awaiting_persistence
                .front()
                .is_some_and(|transition| selected.contains(transition.transition_id.as_str()))
        {
            if let Some(transition) = registry.transitions_awaiting_persistence.pop_front() {
                if transition_ends_hot_lease(&transition) {
                    release_terminal_lease(&mut registry, &transition.lease.lease_id);
                }
                return 1;
            }
            return 0;
        }
        if selected.len() == 1 {
            return 0;
        }
        let mut acknowledged = 0;
        let mut persistence_gap = false;
        let pending = std::mem::take(&mut registry.transitions_awaiting_persistence);
        for transition in pending {
            if !persistence_gap && selected.contains(transition.transition_id.as_str()) {
                acknowledged += 1;
                if transition_ends_hot_lease(&transition) {
                    release_terminal_lease(&mut registry, &transition.lease.lease_id);
                }
            } else {
                persistence_gap = true;
                registry
                    .transitions_awaiting_persistence
                    .push_back(transition);
            }
        }
        acknowledged
    }

    #[must_use]
    pub fn verify_lease_signature(lease: &AuthorizationLease) -> bool {
        !lease.signature.is_empty() && lease.signature == Self::sign(lease)
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
            if !lease.permits(&request.capability, required_mode)
                || !lease_scopes_cover(lease, &request.effect.scopes)
                || lease.principal_id != request.principal_id
                || lease.parent_lease_id != request.parent_lease_id
                || lease.policy_revision == 0
                || lease.policy_revision != request.policy_revision
                || lease.effect_descriptor_hash != request.effect.descriptor_hash
                || now > lease.expires_at_ms
                || matches!(
                    lease.status,
                    AuthorizationLeaseStatus::Expired | AuthorizationLeaseStatus::Revoked
                )
                || !Self::verify_lease_signature(lease)
            {
                return None;
            }
            if already_consumed {
                return Some(execution_snapshot(lease, &request.idempotency_key));
            }
            if !lease.is_active_at(now) {
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
        active_ceiling: PermissionMode,
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
            ceiling: active_ceiling,
            issued_at_ms: now,
            expires_at_ms: now.saturating_add(ttl_ms),
            max_uses,
            remaining_uses: max_uses,
            idempotency_key: request.idempotency_key.clone(),
            policy_revision: request.policy_revision,
            effect_descriptor_hash: request.effect.descriptor_hash.clone(),
            signature: String::new(),
            status: AuthorizationLeaseStatus::Active,
        };
        lease.signature = Self::sign(&lease);
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
            .fingerprint_by_lease
            .insert(lease_id.clone(), fingerprint.to_string());
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

    fn sign(lease: &AuthorizationLease) -> String {
        let payload = serde_json::to_vec(&authorization_lease_signed_payload(lease)).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(process_authorization_signing_secret().as_bytes());
        hasher.update(payload);
        format!("sha256:{:x}", hasher.finalize())
    }
}

/// Commits one authorization lifecycle transition with a stable durable
/// identity. A caller may retry after losing the response: an exact event is
/// accepted, while an idempotency collision with different evidence fails
/// closed. Stream revision races are retried without dropping the transition.
pub(crate) fn persist_authorization_transition(
    store: &crate::RuntimeEventStore,
    stream_id: &str,
    actor: &str,
    transition: &AuthorizationLeaseTransition,
) -> Result<(), String> {
    // The whole read-revision-then-append window runs under the per-stream
    // in-process lock. Parallel Team agents and the parent model stream no
    // longer race the CAS revision; the bounded retry remains only as a
    // safety net for cross-process writers.
    store.with_stream_lock(stream_id, || {
        let payload = serde_json::to_value(transition).map_err(|error| error.to_string())?;
        for attempt in 0..3 {
            if let Some(existing) = store
                .event_by_idempotency_key(stream_id, &transition.transition_id)
                .map_err(|error| error.to_string())?
            {
                if existing.kind == "authorization.lease_transition" && existing.payload == payload
                {
                    return Ok(());
                }
                return Err(format!(
                    "authorization transition idempotency collision: {}",
                    transition.transition_id
                ));
            }
            let expected_revision = store
                .stream_revision(stream_id)
                .map_err(|error| error.to_string())?;
            let request = crate::AppendTransactionRequest {
                transaction_id: format!("authorization-transition:{}", transition.transition_id),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: stream_id.to_string(),
                    expected_revision,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream_id.to_string(),
                        scope: crate::RuntimeEventScope::Tool,
                        kind: "authorization.lease_transition".to_string(),
                        status: Some(format!("{:?}", transition.kind).to_ascii_lowercase()),
                        actor: Some(actor.to_string()),
                        refs: vec![crate::RuntimeEventRef {
                            kind: "authorization_lease".to_string(),
                            id: transition.lease.lease_id.clone(),
                        }],
                        payload: payload.clone(),
                    },
                    idempotency_key: Some(transition.transition_id.clone()),
                    schema_version: 1,
                }],
            };
            match store.append_transaction_locked(request) {
                Ok(_) => return Ok(()),
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => continue,
                Err(crate::RuntimeEventStoreError::TransactionConflict { .. }) if attempt < 2 => {
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        Err(format!(
            "authorization transition persistence retry budget exhausted: {}",
            transition.transition_id
        ))
    })
}

fn transition_ends_hot_lease(transition: &AuthorizationLeaseTransition) -> bool {
    matches!(
        transition.kind,
        AuthorizationLeaseTransitionKind::Expired | AuthorizationLeaseTransitionKind::Revoked
    ) || (transition.kind == AuthorizationLeaseTransitionKind::Consumed
        && transition.lease.status == AuthorizationLeaseStatus::Exhausted)
}

fn release_terminal_lease(registry: &mut LeaseRegistry, lease_id: &str) {
    let Some(lease) = registry.leases.get(lease_id) else {
        return;
    };
    if lease.status == AuthorizationLeaseStatus::Active {
        return;
    }
    registry.leases.remove(lease_id);
    registry.revoked.remove(lease_id);
    let consumed_keys = registry
        .consumed_idempotency
        .range((lease_id.to_string(), String::new())..)
        .take_while(|(candidate_lease_id, _)| candidate_lease_id == lease_id)
        .cloned()
        .collect::<Vec<_>>();
    for key in consumed_keys {
        registry.consumed_idempotency.remove(&key);
    }
    if let Some(fingerprint) = registry.fingerprint_by_lease.remove(lease_id) {
        if registry
            .lease_by_fingerprint
            .get(&fingerprint)
            .is_some_and(|current| current == lease_id)
        {
            registry.lease_by_fingerprint.remove(&fingerprint);
        }
    }
}

fn process_authorization_signing_secret() -> &'static str {
    static SECRET: OnceLock<String> = OnceLock::new();
    SECRET
        .get_or_init(|| uuid::Uuid::new_v4().to_string())
        .as_str()
}

fn validate_approval_grant(
    grant: &ApprovalGrant,
    request: &AuthorizationRequest,
    effective_descriptor_hash: &str,
) -> Result<(), String> {
    if grant.status != ApprovalGrantStatus::Active {
        return Err("approval grant is not active".to_string());
    }
    if grant
        .expires_at_ms
        .is_some_and(|deadline| now_ms() > deadline)
    {
        return Err("approval grant expired".to_string());
    }
    if request.policy_revision == 0 || grant.policy_revision != request.policy_revision {
        return Err("approval grant policy revision mismatch".to_string());
    }
    if grant.capability != request.capability {
        return Err("approval grant capability mismatch".to_string());
    }
    if grant
        .invocation_id
        .as_deref()
        .is_some_and(|invocation_id| invocation_id != request.idempotency_key)
    {
        return Err("approval grant invocation mismatch".to_string());
    }
    if grant.effect_descriptor_hash.as_deref() != Some(effective_descriptor_hash) {
        return Err("approval grant effect descriptor mismatch".to_string());
    }
    if !grant.resource_targets.is_empty()
        && !request.effect.scopes.iter().all(|scope| {
            scope.target.as_ref().is_some_and(|requested| {
                grant
                    .resource_targets
                    .iter()
                    .any(|approved| approved == requested)
            })
        })
    {
        return Err("approval grant resource scope mismatch".to_string());
    }
    Ok(())
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
    let file_observation = descriptor.effect_kind == ToolEffectKind::Read
        && descriptor.idempotency == harness_contract::tool::ToolIdempotency::Idempotent
        && descriptor.required_permission == PermissionMode::ReadOnly
        && descriptor.approval_class == ToolApprovalClass::None
        && !descriptor.uses_network
        && !descriptor.spawns_process
        && !descriptor.mutates_packages
        && !descriptor.mutates_system
        && descriptor
            .scopes
            .iter()
            .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File)
        && descriptor.scopes.iter().all(|scope| {
            scope.operation == harness_contract::policy::PermissionOperation::Read
                && matches!(
                    scope.resource,
                    harness_contract::policy::PermissionResource::File
                        | harness_contract::policy::PermissionResource::Tool
                )
        })
        && assessment.externality == EffectExternality::Workspace
        && assessment.reversibility == EffectReversibility::Reversible
        && assessment.novelty == EffectNovelty::Routine
        && matches!(
            assessment.blast_radius,
            EffectBlastRadius::Item | EffectBlastRadius::Workspace
        )
        && matches!(
            assessment.data_sensitivity,
            DataClassification::Public
                | DataClassification::Internal
                | DataClassification::Confidential
        );
    let internal_control = descriptor.effect_kind == ToolEffectKind::Write
        && descriptor.idempotency == harness_contract::tool::ToolIdempotency::IdempotentWithKey
        && descriptor.required_permission == PermissionMode::ReadOnly
        && descriptor.approval_class == ToolApprovalClass::None
        && !descriptor.scopes.is_empty()
        && descriptor.scopes.iter().all(|scope| {
            scope.resource == harness_contract::policy::PermissionResource::Tool
                && scope.operation == harness_contract::policy::PermissionOperation::Control
        })
        && !descriptor.uses_network
        && !descriptor.spawns_process
        && !descriptor.mutates_packages
        && !descriptor.mutates_system
        && assessment.externality == EffectExternality::Internal
        && assessment.blast_radius == EffectBlastRadius::Item
        && assessment.reversibility == EffectReversibility::Compensatable
        && assessment.novelty == EffectNovelty::Routine
        && assessment.data_sensitivity != DataClassification::Secret;
    let assessed_mode =
        if matches!(
            assessment.externality,
            EffectExternality::System | EffectExternality::ExternalMutation
        ) || matches!(assessment.reversibility, EffectReversibility::Irreversible)
            || matches!(assessment.data_sensitivity, DataClassification::Secret)
        {
            PermissionMode::DangerFullAccess
        } else if (assessment.externality == EffectExternality::Workspace && !file_observation)
            || (!internal_control
                && matches!(
                    descriptor.effect_kind,
                    ToolEffectKind::Write | ToolEffectKind::Process | ToolEffectKind::Package
                ))
        {
            PermissionMode::WorkspaceWrite
        } else if assessment.externality == EffectExternality::NetworkRead
            && assessment.data_sensitivity == DataClassification::Public
        {
            PermissionMode::ReadOnly
        } else {
            descriptor.required_permission
        };

    // Tools owns the concrete effect contract. Runtime may conservatively
    // raise its required permission after risk assessment, but lowering the
    // registered requirement would produce a lease that ToolHost must reject.
    if descriptor.required_permission.rank() >= assessed_mode.rank() {
        descriptor.required_permission
    } else {
        assessed_mode
    }
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
        "parent_lease_id": request.parent_lease_id,
        "policy_revision": request.policy_revision,
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

    fn persist_transitions(
        store: &crate::RuntimeEventStore,
        transitions: &[AuthorizationLeaseTransition],
    ) -> Vec<String> {
        transitions
            .iter()
            .map(|transition| {
                persist_authorization_transition(
                    store,
                    "session:authorization-hot-state-test",
                    "authorization_negotiator_test",
                    transition,
                )
                .expect("persist transition");
                transition.transition_id.clone()
            })
            .collect()
    }

    fn hot_state_counts(
        negotiator: &AuthorizationNegotiator,
    ) -> (usize, usize, usize, usize, usize) {
        let registry = negotiator
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            registry.leases.len(),
            registry.lease_by_fingerprint.len(),
            registry.consumed_idempotency.len(),
            registry.recovery_claims.len(),
            registry.transitions_awaiting_persistence.len(),
        )
    }

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
            policy_revision: 1,
            recovery_scope: "turn:test".to_string(),
            context: PermissionContext::default(),
            safe_alternatives: vec!["return_patch".to_string()],
        }
    }

    fn approval_grant(
        request: &AuthorizationRequest,
        effective: &EffectiveToolAuthorizationDescriptor,
    ) -> ApprovalGrant {
        ApprovalGrant {
            grant_id: "approval-grant:test".to_string(),
            approval_id: "approval:test".to_string(),
            scope: harness_contract::policy::ApprovalGrantScope::Once,
            principal_id: request.principal_id.clone(),
            profile_id: "balanced".to_string(),
            workspace_key: "workspace".to_string(),
            capability: request.capability.clone(),
            session_id: Some("test".to_string()),
            turn_id: None,
            task_id: None,
            invocation_id: Some(request.idempotency_key.clone()),
            resource_targets: Vec::new(),
            effect_descriptor_hash: Some(effective.descriptor.descriptor_hash.clone()),
            risk_ceiling: harness_contract::core::TaskRisk::Critical,
            policy_revision: request.policy_revision,
            status: ApprovalGrantStatus::Active,
            issued_by: harness_contract::policy::ApprovalDecisionActor {
                kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                actor_id: "test".to_string(),
            },
            created_at_ms: 0,
            expires_at_ms: None,
            revoked_at_ms: None,
            revoke_reason: None,
        }
    }

    #[test]
    fn r1_typed_control_effective_floor_preserves_risk_and_lease_boundaries() {
        let resolver = harness_contract::tool::ToolEffectResolverSpec {
            resolver_id: "runtime.agent_action".into(),
            resolver_version: 2,
        };
        let input = serde_json::json!({"task_ref":"task:one"});
        let base = tools::tool_orchestrator::resolve_registered_tool_effect(
            &resolver,
            "task_claim",
            &input,
            PermissionMode::ReadOnly,
        );
        let effective =
            AuthorizationNegotiator::compile_effective_descriptor(&base, &input.to_string());
        assert_eq!(
            effective.descriptor.required_permission,
            PermissionMode::ReadOnly
        );
        assert_eq!(effective.descriptor.descriptor_hash, base.descriptor_hash);
        let mut multiple = base.clone();
        multiple.scopes.push(PermissionScope {
            resource: PermissionResource::Tool,
            operation: PermissionOperation::Control,
            target: Some("second".into()),
        });
        assert_eq!(
            required_mode_for_effect(&multiple, &multiple.assessment),
            PermissionMode::ReadOnly
        );
        for data in [
            DataClassification::Public,
            DataClassification::Internal,
            DataClassification::Confidential,
        ] {
            let mut changed = base.clone();
            changed.assessment.data_sensitivity = data;
            assert_eq!(
                required_mode_for_effect(&changed, &changed.assessment),
                PermissionMode::ReadOnly
            );
            assert_eq!(
                risk_for_effect(&changed.assessment),
                if data == DataClassification::Confidential {
                    RiskLevel::Medium
                } else {
                    RiskLevel::Low
                }
            );
        }
        let mut rejected = Vec::new();
        for kind in [
            ToolEffectKind::Process,
            ToolEffectKind::Package,
            ToolEffectKind::System,
            ToolEffectKind::Destructive,
            ToolEffectKind::Unknown,
        ] {
            let mut changed = base.clone();
            changed.effect_kind = kind;
            if matches!(
                kind,
                ToolEffectKind::System | ToolEffectKind::Destructive | ToolEffectKind::Unknown
            ) {
                changed.assessment = EffectAssessment::default();
            }
            rejected.push(changed);
        }
        let mut empty = base.clone();
        empty.scopes.clear();
        rejected.push(empty);
        for operation in [
            PermissionOperation::Read,
            PermissionOperation::Write,
            PermissionOperation::Call,
            PermissionOperation::Execute,
        ] {
            let mut changed = base.clone();
            changed.scopes[0].operation = operation;
            rejected.push(changed);
        }
        for resource in [
            PermissionResource::File,
            PermissionResource::Network,
            PermissionResource::Provider,
            PermissionResource::Config,
        ] {
            for operation in [PermissionOperation::Read, PermissionOperation::Write] {
                let mut changed = base.clone();
                changed.scopes.push(PermissionScope {
                    resource: resource.clone(),
                    operation,
                    target: Some("target".into()),
                });
                rejected.push(changed);
            }
        }
        for idempotency in [
            ToolIdempotency::Idempotent,
            ToolIdempotency::NonIdempotent,
            ToolIdempotency::Unknown,
        ] {
            let mut changed = base.clone();
            changed.idempotency = idempotency;
            rejected.push(changed);
        }
        for approval in [
            ToolApprovalClass::Policy,
            ToolApprovalClass::User,
            ToolApprovalClass::Administrator,
        ] {
            let mut changed = base.clone();
            changed.approval_class = approval;
            rejected.push(changed);
        }
        for flag in 0..4 {
            let mut changed = base.clone();
            match flag {
                0 => changed.uses_network = true,
                1 => changed.spawns_process = true,
                2 => changed.mutates_packages = true,
                _ => changed.mutates_system = true,
            }
            rejected.push(changed);
        }
        for externality in [
            EffectExternality::Workspace,
            EffectExternality::System,
            EffectExternality::ExternalMutation,
            EffectExternality::NetworkRead,
            EffectExternality::Unknown,
        ] {
            let mut changed = base.clone();
            changed.assessment.externality = externality;
            rejected.push(changed);
        }
        for blast in [
            EffectBlastRadius::Workspace,
            EffectBlastRadius::ExternalAccount,
            EffectBlastRadius::System,
            EffectBlastRadius::Unbounded,
            EffectBlastRadius::Unknown,
        ] {
            let mut changed = base.clone();
            changed.assessment.blast_radius = blast;
            rejected.push(changed);
        }
        for reversibility in [
            EffectReversibility::Reversible,
            EffectReversibility::Irreversible,
            EffectReversibility::Unknown,
        ] {
            let mut changed = base.clone();
            changed.assessment.reversibility = reversibility;
            rejected.push(changed);
        }
        for novelty in [
            EffectNovelty::NewTarget,
            EffectNovelty::NewCapability,
            EffectNovelty::Unknown,
        ] {
            let mut changed = base.clone();
            changed.assessment.novelty = novelty;
            rejected.push(changed);
        }
        let mut defaulted = base.clone();
        defaulted.assessment = EffectAssessment::default();
        rejected.push(defaulted);
        let mut secret = base.clone();
        secret.assessment.data_sensitivity = DataClassification::Secret;
        assert_eq!(
            required_mode_for_effect(&secret, &secret.assessment),
            PermissionMode::DangerFullAccess
        );
        rejected.push(secret);
        for changed in rejected {
            assert_ne!(
                AuthorizationNegotiator::compile_effective_descriptor(&changed, "{}")
                    .descriptor
                    .required_permission,
                PermissionMode::ReadOnly,
                "{changed:?}"
            );
        }
        for floor in [
            PermissionMode::WorkspaceWrite,
            PermissionMode::DangerFullAccess,
        ] {
            let mut changed = base.clone();
            changed.required_permission = floor;
            assert_eq!(
                required_mode_for_effect(&changed, &changed.assessment),
                floor
            );
        }
        let negotiator = AuthorizationNegotiator::new();
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly);
        let mut current = request(base.clone());
        current.capability = base.tool_id.clone();
        current.input = input.to_string();
        current.parent_ceiling = PermissionMode::ReadOnly;
        current.parent_lease_id = Some("parent:ro".into());
        let assessment = negotiator.assess(&policy, &current);
        let lease = assessment
            .lease
            .clone()
            .expect("real negotiator issues RO control lease");
        assert!(AuthorizationNegotiator::verify_lease_signature(&lease));
        assert_eq!(lease.ceiling, PermissionMode::ReadOnly);
        let decision = crate::ToolPolicy
            .authorize(
                &effective,
                &assessment,
                &current.idempotency_key,
                lease.clone(),
                30,
            )
            .unwrap();
        let uses_after_first =
            negotiator.registry.lock().unwrap().leases[&lease.lease_id].remaining_uses;
        let retry = negotiator.assess(&policy, &current).lease.unwrap();
        assert_eq!(retry.lease_id, lease.lease_id);
        assert_eq!(
            negotiator.registry.lock().unwrap().leases[&lease.lease_id].remaining_uses,
            uses_after_first
        );
        let directory = tempfile::tempdir().unwrap();
        let catalog = |version| {
            tools::ToolCatalog::builtin().with_runtime_tools(vec![tools::RuntimeToolDefinition {
                name: "task_claim".into(),
                description: Some("control contract".into()),
                input_schema: serde_json::json!({"type":"object"}),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                    resolver_id: "runtime.agent_action".into(),
                    resolver_version: version,
                },
            }])
        };
        for version in [0, 1, 3, u32::MAX] {
            assert!(catalog(version).is_err());
        }
        let host = tools::ToolHost::new(
            "control-contract",
            directory.path(),
            tools::ToolHostSnapshot::new(
                Arc::new(tools::ToolCatalog::builtin()),
                Arc::new(tools::lsp_client::LspRegistry::default()),
                None,
            ),
        )
        .with_authorization_lease_verifier(Arc::new(
            AuthorizationNegotiator::verify_lease_signature,
        ));
        let old = host.pin_snapshot();
        assert!(old
            .validate_authorization(&decision.authorization, "task_claim", &input)
            .is_err());
        let definitions = old
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "read_file")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        let bound = crate::step_snapshot::BoundStep::bind(
            harness_contract::step::StepIdentity {
                step_id: "control-step".into(),
                session_id: "session:test".into(),
                turn_id: "turn:test".into(),
                execution_id: None,
                actor_ref: None,
                run_ref: None,
                attempt: Some(1),
                epoch: None,
                goal_ref: None,
                program_ref: None,
                task_ref: None,
                build_ref: "control-contract".into(),
            },
            old.clone(),
            &definitions,
            &serde_json::json!({}),
            &serde_json::json!({}),
            BTreeMap::new(),
            i64::MAX,
        )
        .unwrap();
        let binding = crate::step_snapshot::StepCallBinding::provider(Arc::new(bound), "call");
        let persisted: crate::step_snapshot::StepCallBinding =
            serde_json::from_value(serde_json::to_value(&binding).unwrap()).unwrap();
        host.replace_snapshot(tools::ToolHostSnapshot::new(
            Arc::new(catalog(2).unwrap()),
            Arc::new(tools::lsp_client::LspRegistry::default()),
            None,
        ));
        let new = host.pin_snapshot();
        assert_ne!(
            old.snapshot().descriptor_set_hash,
            new.snapshot().descriptor_set_hash
        );
        assert!(persisted.resolve(Some(new.clone())).is_err());
        let historical = binding.resolve(Some(new.clone())).unwrap();
        assert!(historical
            .validate_call(
                &historical.provider_origin("call"),
                "session:test",
                "call",
                "task_claim",
                &current.input,
                1
            )
            .is_err());
        assert!(new
            .validate_authorization(&decision.authorization, "task_claim", &input)
            .is_ok());
        host.replace_snapshot(new.snapshot().clone());
        let repinned = host.pin_snapshot();
        assert_ne!(repinned.revision(), new.revision());
        assert_eq!(
            repinned.describe_effect("task_claim", &input),
            new.describe_effect("task_claim", &input)
        );
        assert_eq!(
            negotiator.assess(&policy, &current).lease.unwrap().lease_id,
            lease.lease_id
        );
        let old_effect = tools::tool_orchestrator::resolve_registered_tool_effect(
            &harness_contract::tool::ToolEffectResolverSpec {
                resolver_id: "runtime.agent_action".into(),
                resolver_version: 1,
            },
            "task_claim",
            &input,
            PermissionMode::ReadOnly,
        );
        let mut old_request = current.clone();
        old_request.effect = old_effect.clone();
        let old_effective =
            AuthorizationNegotiator::compile_effective_descriptor(&old_effect, &current.input);
        assert_ne!(old_effective.fingerprint, effective.fingerprint);
        assert_ne!(
            capability_fingerprint(&old_request, old_effect.required_permission),
            capability_fingerprint(&current, PermissionMode::ReadOnly)
        );
        let old_grant = approval_grant(&old_request, &old_effective);
        assert!(validate_approval_grant(&old_grant, &current, &base.descriptor_hash).is_err());
        assert!(negotiator.assess(&policy, &old_request).lease.is_none());
        let calls = vec![crate::tool_dispatch::ToolRequest {
            tool_use_id: "control-call".into(),
            tool_name: "task_claim".into(),
            input: current.input.clone(),
            depends_on: vec![],
        }];
        let plan = |effect: &ToolEffectDescriptor, pin: &tools::ToolHostLease| {
            crate::governed_tool_plan::GovernedToolCompiler.compile(
                directory.path(),
                &calls,
                |_, _| {
                    Some((
                        effect.clone(),
                        pin.revision(),
                        pin.snapshot().descriptor_set_hash.clone(),
                    ))
                },
            )
        };
        assert!(plan(&old_effect, &old).is_err());
        assert_ne!(
            plan(&base, &old).unwrap().topology_hash,
            plan(&base, &new).unwrap().topology_hash
        );
        let mut approved_old = old_request.clone();
        approved_old.parent_ceiling = PermissionMode::DangerFullAccess;
        let old_assessment = negotiator.approve_effective(
            &PermissionPolicy::new(PermissionMode::DangerFullAccess),
            &approved_old,
            &old_effective,
            &old_grant,
        );
        let old_lease = old_assessment.lease.clone().unwrap();
        let old_authorization = crate::ToolPolicy
            .authorize(
                &old_effective,
                &old_assessment,
                &current.idempotency_key,
                old_lease,
                30,
            )
            .unwrap();
        assert!(new
            .validate_authorization(&old_authorization.authorization, "task_claim", &input)
            .is_err());
        let mut crossed = decision.authorization.clone();
        crossed.authorization_lease = old_authorization.authorization.authorization_lease;
        assert!(new
            .validate_authorization(&crossed, "task_claim", &input)
            .is_err());
        let mut drifted = current.clone();
        drifted.policy_revision += 1;
        let renewed = negotiator.assess(&policy, &drifted).lease.unwrap();
        assert_ne!(renewed.lease_id, lease.lease_id);
        assert_eq!(renewed.policy_revision, drifted.policy_revision);
        assert_eq!(
            negotiator.assess(&policy, &drifted).lease.unwrap().lease_id,
            renewed.lease_id
        );
        let mut reparented = current.clone();
        reparented.parent_lease_id = Some("parent:other".into());
        let reparented_lease = negotiator.assess(&policy, &reparented).lease.unwrap();
        assert_ne!(reparented_lease.lease_id, lease.lease_id);
        assert_eq!(reparented_lease.parent_lease_id, reparented.parent_lease_id);
        for mutation in [
            "revision",
            "parent",
            "descriptor",
            "scope",
            "signature",
            "expired",
            "revoked",
            "exhausted",
        ] {
            let isolated = AuthorizationNegotiator::new();
            let issued = isolated.assess(&policy, &current).lease.unwrap();
            {
                let mut registry = isolated.registry.lock().unwrap();
                let stored = registry.leases.get_mut(&issued.lease_id).unwrap();
                match mutation {
                    "revision" => stored.policy_revision += 1,
                    "parent" => stored.parent_lease_id = Some("parent:foreign".into()),
                    "descriptor" => stored.effect_descriptor_hash.push_str("-foreign"),
                    "scope" => stored.scopes.clear(),
                    "signature" => {}
                    "expired" => stored.expires_at_ms = 0,
                    "revoked" => stored.status = AuthorizationLeaseStatus::Revoked,
                    "exhausted" => {
                        stored.status = AuthorizationLeaseStatus::Exhausted;
                        stored.remaining_uses = 0;
                    }
                    _ => unreachable!(),
                }
                stored.signature = AuthorizationNegotiator::sign(stored);
                if mutation == "signature" {
                    stored.signature.push_str("-foreign");
                }
            }
            let retried = isolated.assess(&policy, &current).lease.unwrap();
            if mutation == "exhausted" {
                assert_eq!(retried.lease_id, issued.lease_id);
                assert_eq!(
                    isolated.registry.lock().unwrap().leases[&issued.lease_id].remaining_uses,
                    0
                );
            } else {
                assert_ne!(retried.lease_id, issued.lease_id, "{mutation}");
            }
        }
        let mut grant = approval_grant(&current, &effective);
        for mutation in 0..4 {
            let mut invalid = grant.clone();
            match mutation {
                0 => invalid.status = ApprovalGrantStatus::Revoked,
                1 => invalid.expires_at_ms = Some(0),
                2 => invalid.policy_revision += 1,
                _ => invalid.invocation_id = Some("wrong-key".into()),
            }
            assert!(negotiator
                .approve_effective(&policy, &current, &effective, &invalid)
                .lease
                .is_none());
        }
        grant.effect_descriptor_hash = Some(old_effect.descriptor_hash.clone());
        assert!(negotiator
            .approve_effective(&policy, &current, &effective, &grant)
            .lease
            .is_none());
        for mutation in 0..7 {
            let mut altered = decision.authorization.clone();
            match mutation {
                0 => altered.authorization_lease.signature.push('x'),
                1 => altered.authorization_lease.status = AuthorizationLeaseStatus::Revoked,
                2 => altered.authorization_lease.expires_at_ms = 0,
                3 => altered.idempotency_key = Some("wrong-key".into()),
                4 => altered.policy_revision += 1,
                5 => altered.descriptor_hash = old_effect.descriptor_hash.clone(),
                _ => altered.authorization_lease.scopes.clear(),
            }
            assert!(
                new.validate_authorization(&altered, "task_claim", &input)
                    .is_err(),
                "{mutation}"
            );
        }
        assert!(crate::ToolPolicy
            .authorize(&effective, &assessment, "wrong-key", lease.clone(), 30)
            .is_err());
        assert!(negotiator.revoke(&lease.lease_id));
        let next = negotiator.assess(&policy, &current).lease.unwrap();
        assert_ne!(next.lease_id, lease.lease_id);
    }

    #[test]
    fn r1_readonly_workspace_observation_preserves_permission_floor() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("a.txt"), "first observation").unwrap();
        std::fs::write(directory.path().join("b.txt"), "second observation").unwrap();
        let host = tools::ToolHost::builtin("readonly-floor", directory.path())
            .with_authorization_lease_verifier(Arc::new(
                AuthorizationNegotiator::verify_lease_signature,
            ));
        let pin = host.pin_snapshot();
        let input = serde_json::json!({"path":"a.txt"});
        let base = pin.describe_effect("read_file", &input);
        assert_eq!(base.assessment.externality, EffectExternality::Workspace);
        assert_eq!(
            base.assessment.reversibility,
            EffectReversibility::Reversible
        );
        assert_eq!(base.assessment.novelty, EffectNovelty::Routine);
        assert_eq!(base.assessment.blast_radius, EffectBlastRadius::Workspace);
        let composite_input = serde_json::json!({"steps":[
            {"id":"first","op":"call","tool":"read_file","input":{"path":"a.txt"}},
            {"id":"second","op":"call","tool":"read_file","input":{"path":"b.txt"}}
        ]});
        let metadata_input = serde_json::json!({"steps":[
            {"id":"first","op":"call","tool":"read_file","input":{"path":"a.txt"}},
            {"id":"metadata","op":"call","tool":"tool_search","input":{"query":"read_file"}}
        ]});
        for (tool, value) in [
            ("read_file", input.clone()),
            ("tool_orchestrate", composite_input),
            ("tool_orchestrate", metadata_input),
        ] {
            let descriptor = pin.describe_effect(tool, &value);
            let effective = AuthorizationNegotiator::compile_effective_descriptor(
                &descriptor,
                &value.to_string(),
            );
            assert_eq!(
                effective.descriptor.required_permission,
                PermissionMode::ReadOnly
            );
            assert_eq!(
                effective.descriptor.descriptor_hash,
                descriptor.descriptor_hash
            );
            assert_eq!(
                pin.describe_effect(tool, &value).descriptor_hash,
                descriptor.descriptor_hash
            );
            let mut current = request(descriptor.clone());
            current.input = value.to_string();
            current.capability = tool.into();
            current.parent_ceiling = PermissionMode::ReadOnly;
            let negotiator = AuthorizationNegotiator::new();
            let evaluated = negotiator
                .assess_effective(&PermissionPolicy::new(PermissionMode::ReadOnly), &current);
            assert_eq!(evaluated.assessment.risk, RiskLevel::Medium);
            let lease = evaluated
                .assessment
                .lease
                .clone()
                .expect("signed RO file observation lease");
            assert!(AuthorizationNegotiator::verify_lease_signature(&lease));
            let authorization = crate::ToolPolicy
                .authorize(
                    &evaluated.effective,
                    &evaluated.assessment,
                    &current.idempotency_key,
                    lease.clone(),
                    30,
                )
                .unwrap()
                .authorization;
            let definitions = pin
                .snapshot()
                .catalog
                .definitions(None)
                .into_iter()
                .filter(|definition| definition.name == tool)
                .map(|definition| crate::ProviderToolDefinition {
                    name: definition.name,
                    description: definition.description,
                    input_schema: definition.input_schema,
                })
                .collect::<Vec<_>>();
            let bound = Arc::new(
                crate::step_snapshot::BoundStep::bind(
                    serde_json::from_value(
                        serde_json::json!({"step_id":"readonly-floor", "session_id":"session:test",
                    "turn_id":"turn:test", "build_ref":"readonly-floor"}),
                    )
                    .unwrap(),
                    pin.clone(),
                    &definitions,
                    &serde_json::json!({}),
                    &serde_json::json!({}),
                    BTreeMap::new(),
                    i64::MAX,
                )
                .unwrap(),
            );
            let binding = crate::step_snapshot::StepCallBinding::provider(bound.clone(), "call-1");
            bound
                .validate_call(
                    &binding.origin,
                    "session:test",
                    "call-1",
                    tool,
                    &current.input,
                    1,
                )
                .unwrap();
            assert!(bound
                .validate_call(
                    &binding.origin,
                    "wrong-session",
                    "call-1",
                    tool,
                    &current.input,
                    1
                )
                .is_err());
            assert!(bound
                .validate_call(
                    &binding.origin,
                    "session:test",
                    "wrong-call",
                    tool,
                    &current.input,
                    1
                )
                .is_err());
            for index in 0..descriptor.scopes.len() {
                let mut altered = authorization.clone();
                altered.authorization_lease.scopes.remove(index);
                altered.authorization_lease.signature =
                    AuthorizationNegotiator::sign(&altered.authorization_lease);
                assert!(
                    pin.validate_authorization(&altered, tool, &value).is_err(),
                    "missing scope {index}"
                );
            }
            for axis in 0..8 {
                let mut altered = authorization.clone();
                match axis {
                    0 => altered.authorization_lease.status = AuthorizationLeaseStatus::Revoked,
                    1 => altered.authorization_lease.expires_at_ms = 0,
                    2 => altered.authorization_lease.remaining_uses = 0,
                    3 => altered.authorization_lease.capability = "foreign".into(),
                    4 => altered.policy_revision += 1,
                    5 => altered.descriptor_hash = "foreign".into(),
                    6 => altered.idempotency_key = Some("foreign".into()),
                    _ => altered.authorization_lease.effect_descriptor_hash = "foreign".into(),
                }
                altered.authorization_lease.signature =
                    AuthorizationNegotiator::sign(&altered.authorization_lease);
                assert!(
                    pin.validate_authorization(&altered, tool, &value).is_err(),
                    "lease axis {axis}"
                );
            }
            let mut forged = authorization.clone();
            forged.authorization_lease.signature.push('x');
            let error = pin
                .validate_authorization(&forged, tool, &value)
                .unwrap_err()
                .to_string();
            assert!(error.contains("signature"), "{error}");
            assert!(pin
                .execute(&authorization, tool, &value)
                .unwrap()
                .contains("first observation"));
            assert!(negotiator.revoke(&lease.lease_id));
            assert_ne!(
                negotiator
                    .assess_effective(&PermissionPolicy::new(PermissionMode::ReadOnly), &current)
                    .assessment
                    .lease
                    .unwrap()
                    .lease_id,
                lease.lease_id
            );
            assert_ne!(
                capability_fingerprint(&current, PermissionMode::ReadOnly),
                capability_fingerprint(&current, PermissionMode::WorkspaceWrite)
            );
        }
        for data in [
            DataClassification::Public,
            DataClassification::Internal,
            DataClassification::Confidential,
        ] {
            for blast in [EffectBlastRadius::Item, EffectBlastRadius::Workspace] {
                let mut changed = base.clone();
                changed.assessment.data_sensitivity = data;
                changed.assessment.blast_radius = blast;
                changed.scopes.push(PermissionScope {
                    resource: PermissionResource::Tool,
                    operation: PermissionOperation::Read,
                    target: Some("metadata".into()),
                });
                assert_eq!(
                    required_mode_for_effect(&changed, &changed.assessment),
                    PermissionMode::ReadOnly
                );
                assert_eq!(risk_for_effect(&changed.assessment), RiskLevel::Medium);
            }
        }
        let mut rejected = Vec::new();
        for kind in [
            ToolEffectKind::Write,
            ToolEffectKind::Process,
            ToolEffectKind::Package,
            ToolEffectKind::Network,
            ToolEffectKind::Unknown,
        ] {
            let mut changed = base.clone();
            changed.effect_kind = kind;
            rejected.push(changed);
        }
        for idempotency in [
            ToolIdempotency::IdempotentWithKey,
            ToolIdempotency::NonIdempotent,
            ToolIdempotency::Unknown,
        ] {
            let mut changed = base.clone();
            changed.idempotency = idempotency;
            rejected.push(changed);
        }
        for approval in [
            ToolApprovalClass::Policy,
            ToolApprovalClass::User,
            ToolApprovalClass::Administrator,
        ] {
            let mut changed = base.clone();
            changed.approval_class = approval;
            rejected.push(changed);
        }
        for axis in 0..4 {
            let mut changed = base.clone();
            match axis {
                0 => changed.uses_network = true,
                1 => changed.spawns_process = true,
                2 => changed.mutates_packages = true,
                _ => changed.mutates_system = true,
            }
            rejected.push(changed);
        }
        for scope in [
            PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Write,
                target: Some("b.txt".into()),
            },
            PermissionScope {
                resource: PermissionResource::Tool,
                operation: PermissionOperation::Control,
                target: Some("control".into()),
            },
            PermissionScope {
                resource: PermissionResource::Tool,
                operation: PermissionOperation::Write,
                target: Some("write".into()),
            },
            PermissionScope {
                resource: PermissionResource::Network,
                operation: PermissionOperation::Read,
                target: Some("remote".into()),
            },
            PermissionScope {
                resource: PermissionResource::Shell,
                operation: PermissionOperation::Execute,
                target: Some("process".into()),
            },
        ] {
            let mut changed = base.clone();
            changed.scopes.push(scope);
            rejected.push(changed);
        }
        let mut empty = base.clone();
        empty.scopes.clear();
        rejected.push(empty);
        let mut tool_only = base.clone();
        tool_only.scopes[0].resource = PermissionResource::Tool;
        rejected.push(tool_only);
        for novelty in [
            EffectNovelty::NewTarget,
            EffectNovelty::NewCapability,
            EffectNovelty::Unknown,
        ] {
            let mut changed = base.clone();
            changed.assessment.novelty = novelty;
            rejected.push(changed);
        }
        for blast in [
            EffectBlastRadius::ExternalAccount,
            EffectBlastRadius::System,
            EffectBlastRadius::Unbounded,
            EffectBlastRadius::Unknown,
        ] {
            let mut changed = base.clone();
            changed.assessment.blast_radius = blast;
            rejected.push(changed);
        }
        for reversibility in [
            EffectReversibility::Compensatable,
            EffectReversibility::Unknown,
            EffectReversibility::Irreversible,
        ] {
            let mut changed = base.clone();
            changed.assessment.reversibility = reversibility;
            rejected.push(changed);
        }
        for changed in rejected {
            assert_eq!(
                required_mode_for_effect(&changed, &changed.assessment),
                if changed.assessment.reversibility == EffectReversibility::Irreversible {
                    PermissionMode::DangerFullAccess
                } else {
                    PermissionMode::WorkspaceWrite
                },
                "{changed:?}"
            );
        }
        for externality in [
            EffectExternality::Unknown,
            EffectExternality::System,
            EffectExternality::ExternalMutation,
        ] {
            let mut changed = base.clone();
            changed.assessment.externality = externality;
            assert_eq!(
                required_mode_for_effect(&changed, &changed.assessment),
                if externality == EffectExternality::Unknown {
                    PermissionMode::ReadOnly
                } else {
                    PermissionMode::DangerFullAccess
                }
            );
        }
        let mut defaulted = base.clone();
        defaulted.assessment = EffectAssessment::default();
        assert_eq!(
            AuthorizationNegotiator::compile_effective_descriptor(&defaulted, "{}")
                .descriptor
                .required_permission,
            PermissionMode::ReadOnly
        );
        for floor in [
            PermissionMode::WorkspaceWrite,
            PermissionMode::DangerFullAccess,
        ] {
            let mut changed = base.clone();
            changed.required_permission = floor;
            assert_eq!(
                required_mode_for_effect(&changed, &changed.assessment),
                floor
            );
        }
        for value in [
            serde_json::json!({"path":".env"}),
            serde_json::json!({"path":".ssh/id_rsa"}),
            serde_json::json!({"path":"credentials.json"}),
            serde_json::json!({"path":"a.txt","contains_secrets":true}),
            serde_json::json!({"path":"a.txt","data_classification":"secret"}),
            serde_json::json!({}),
        ] {
            let sensitive = pin.describe_effect("read_file", &value);
            assert_eq!(
                AuthorizationNegotiator::compile_effective_descriptor(
                    &sensitive,
                    &value.to_string()
                )
                .descriptor
                .required_permission,
                PermissionMode::DangerFullAccess
            );
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
    fn effective_assessment_never_lowers_the_tool_host_permission_contract() {
        let mut descriptor = effect(
            PermissionMode::DangerFullAccess,
            EffectExternality::Workspace,
        );
        descriptor.effect_kind = ToolEffectKind::Process;

        let effective = AuthorizationNegotiator::compile_effective_descriptor(&descriptor, "{}");

        assert_eq!(
            effective.descriptor.required_permission,
            PermissionMode::DangerFullAccess
        );
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
    fn full_trust_session_lease_carries_the_session_ceiling_not_the_effect_floor() {
        let mut descriptor = effect(PermissionMode::WorkspaceWrite, EffectExternality::Workspace);
        descriptor.tool_id = "task_publish".to_string();
        let mut request = request(descriptor);
        request.capability = "task_publish".to_string();
        let assessment = AuthorizationNegotiator::new().assess(
            &PermissionPolicy::new(PermissionMode::DangerFullAccess),
            &request,
        );
        let lease = assessment.lease.expect("full-trust lease");
        assert_eq!(
            lease.ceiling,
            PermissionMode::DangerFullAccess,
            "a YOLO/full-trust session must issue control-plane leases at the session ceiling, never the effect floor"
        );
        assert_eq!(
            assessment.required_mode,
            PermissionMode::WorkspaceWrite,
            "the effect floor must remain visible on the assessment"
        );
        assert!(lease.permits("task_publish", PermissionMode::WorkspaceWrite));
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
        let request = request(effect(
            PermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        let assessment = AuthorizationNegotiator::new().assess(&policy, &request);
        assert_eq!(assessment.path, AuthorizationPath::HardDeny);
        assert!(assessment.lease.is_none());
        let effective =
            AuthorizationNegotiator::compile_effective_descriptor(&request.effect, &request.input);
        let approved = AuthorizationNegotiator::new().approve_effective(
            &policy,
            &request,
            &effective,
            &approval_grant(&request, &effective),
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
        assert!(negotiator.claim_controlled_recovery(&assessment, "turn:test"));
        assert!(!negotiator.claim_controlled_recovery(&assessment, "turn:test"));
    }

    #[test]
    fn durable_controlled_recovery_restores_until_graph_terminal_and_then_acks_exactly() {
        let store = crate::RuntimeEventStore::for_test();
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            )),
        );
        let fingerprint = assessment
            .gap
            .as_ref()
            .expect("recoverable gap")
            .fingerprint
            .clone();
        let record = ControlledRecoveryClaimRecord {
            fingerprint: fingerprint.clone(),
            recovery_scope: "turn:turn-recovery".to_string(),
            session_id: "session-recovery".to_string(),
            turn_id: "turn-recovery".to_string(),
            execution_id: "graph-recovery".to_string(),
            capability: assessment.capability.clone(),
        };
        assert!(negotiator.claim_controlled_recovery(&assessment, &record.recovery_scope));
        persist_controlled_recovery_claim(&store, &record).expect("durable claim");
        persist_controlled_recovery_claim(&store, &record).expect("idempotent claim retry");
        assert_eq!(
            store
                .list_stream(&controlled_recovery_stream_id(
                    &record.session_id,
                    &record.turn_id,
                    &record.execution_id,
                ))
                .expect("claim stream")
                .len(),
            1
        );

        let restarted = AuthorizationNegotiator::new();
        let open = load_open_controlled_recovery_claims(
            &store,
            &record.session_id,
            &record.turn_id,
            &record.execution_id,
        )
        .expect("restart recovery");
        assert_eq!(open, vec![record.clone()]);
        assert!(restarted
            .restore_controlled_recovery_claim(&open[0].fingerprint, &open[0].recovery_scope));
        assert_eq!(hot_state_counts(&restarted).3, 1);

        // Building a terminal or returning from an attempt is not an ACK.
        let terminal = ControlledRecoveryTerminalRecord {
            recovery_scope: record.recovery_scope.clone(),
            session_id: record.session_id.clone(),
            turn_id: record.turn_id.clone(),
            execution_id: record.execution_id.clone(),
            fingerprints: vec![fingerprint.clone()],
        };
        let terminal_event = controlled_recovery_terminal_event(&terminal).expect("terminal event");
        assert_eq!(hot_state_counts(&restarted).3, 1);
        assert_eq!(
            load_open_controlled_recovery_claims(
                &store,
                &record.session_id,
                &record.turn_id,
                &record.execution_id,
            )
            .expect("abort preserves durable claim"),
            vec![record.clone()]
        );

        let stream_id = controlled_recovery_stream_id(
            &record.session_id,
            &record.turn_id,
            &record.execution_id,
        );
        let revision = store.stream_revision(&stream_id).expect("stream revision");
        store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: "graph-recovery-terminal".to_string(),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id,
                    expected_revision: revision,
                }],
                events: vec![terminal_event],
            })
            .expect("graph terminal commit");
        assert!(load_open_controlled_recovery_claims(
            &store,
            &record.session_id,
            &record.turn_id,
            &record.execution_id,
        )
        .expect("settled recovery stream")
        .is_empty());
        assert!(persist_controlled_recovery_claim(&store, &record).is_err());
        assert_eq!(hot_state_counts(&restarted).3, 1);
        assert_eq!(
            restarted.acknowledge_controlled_recovery_terminals(&terminal.fingerprints),
            1
        );
        assert_eq!(hot_state_counts(&restarted).3, 0);
    }

    #[test]
    fn sibling_executions_in_one_turn_commit_distinct_recovery_terminals() {
        let store = crate::RuntimeEventStore::for_test();
        let terminal = |execution_id: &str| ControlledRecoveryTerminalRecord {
            recovery_scope: "turn:shared-turn".to_string(),
            session_id: "shared-session".to_string(),
            turn_id: "shared-turn".to_string(),
            execution_id: execution_id.to_string(),
            fingerprints: Vec::new(),
        };
        let first = terminal("team-one-agent");
        let second = terminal("team-two-agent");
        let first_event = controlled_recovery_terminal_event(&first).expect("first terminal");
        let second_event = controlled_recovery_terminal_event(&second).expect("second terminal");

        assert_ne!(first_event.event.stream_id, second_event.event.stream_id);
        assert_ne!(first_event.idempotency_key, second_event.idempotency_key);
        store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: "parallel-recovery-terminals".to_string(),
                expected_streams: vec![
                    crate::ExpectedStreamRevision {
                        stream_id: first_event.event.stream_id.clone(),
                        expected_revision: 0,
                    },
                    crate::ExpectedStreamRevision {
                        stream_id: second_event.event.stream_id.clone(),
                        expected_revision: 0,
                    },
                ],
                events: vec![first_event, second_event],
            })
            .expect("parallel execution terminals must not collide");
    }

    #[test]
    fn controlled_recovery_without_exact_turn_scope_fails_closed() {
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            )),
        );
        assert!(!negotiator.claim_controlled_recovery(&assessment, "session:test"));
        assert_eq!(hot_state_counts(&negotiator).3, 0);
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
        assert!(AuthorizationNegotiator::verify_lease_signature(&lease));
        assert_eq!(lease.policy_revision, 1);
        assert_eq!(lease.effect_descriptor_hash, "test");
        let mut tampered_revision = lease.clone();
        tampered_revision.policy_revision = 2;
        assert!(!AuthorizationNegotiator::verify_lease_signature(
            &tampered_revision
        ));
        let mut tampered_effect = lease.clone();
        tampered_effect.effect_descriptor_hash = "descriptor-other".to_string();
        assert!(!AuthorizationNegotiator::verify_lease_signature(
            &tampered_effect
        ));
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
    fn durable_ack_releases_terminal_lease_but_preserves_exact_history() {
        let store = crate::RuntimeEventStore::for_test();
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            &request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            )),
        );
        let lease = assessment.lease.expect("one-use workspace lease");
        let transitions = negotiator.take_transitions_for_persistence();
        assert_eq!(transitions.len(), 2);
        assert_eq!(hot_state_counts(&negotiator), (1, 1, 1, 0, 2));

        let transition_ids = persist_transitions(&store, &transitions);
        assert_eq!(persist_transitions(&store, &transitions), transition_ids);
        assert_eq!(
            negotiator.acknowledge_persisted_transitions(std::slice::from_ref(&transition_ids[1])),
            0,
            "a later lifecycle transition cannot be acknowledged across an earlier gap"
        );
        assert_eq!(
            negotiator.acknowledge_persisted_transitions(std::slice::from_ref(&transition_ids[0])),
            1
        );
        assert_eq!(
            negotiator.acknowledge_persisted_transitions(std::slice::from_ref(&transition_ids[1])),
            1
        );
        assert_eq!(hot_state_counts(&negotiator), (0, 0, 0, 0, 0));
        assert!(negotiator.projection().is_empty());

        let durable = store
            .list_stream("session:authorization-hot-state-test")
            .expect("durable authorization history");
        assert_eq!(durable.len(), 2);
        assert_eq!(
            durable
                .iter()
                .filter_map(
                    |event| serde_json::from_value::<AuthorizationLeaseTransition>(
                        event.payload.clone()
                    )
                    .ok()
                    .map(|transition| transition.kind)
                )
                .collect::<Vec<_>>(),
            vec![
                AuthorizationLeaseTransitionKind::Issued,
                AuthorizationLeaseTransitionKind::Consumed,
            ]
        );
        let consumed = durable
            .iter()
            .find_map(|event| {
                serde_json::from_value::<AuthorizationLeaseTransition>(event.payload.clone())
                    .ok()
                    .filter(|transition| {
                        transition.kind == AuthorizationLeaseTransitionKind::Consumed
                    })
            })
            .expect("durable consumed transition");
        assert_eq!(consumed.lease.lease_id, lease.lease_id);
        assert_eq!(consumed.lease.status, AuthorizationLeaseStatus::Exhausted);
    }

    #[test]
    fn active_lease_remains_idempotent_after_transition_ack() {
        let rules = crate::RuntimePermissionRuleConfig::new(
            vec!["test_tool(*)".to_string()],
            Vec::new(),
            Vec::new(),
        );
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly).with_permission_rules(&rules);
        let negotiator = AuthorizationNegotiator::new();
        let request = request(effect(
            PermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        let first = negotiator.assess(&policy, &request);
        let ids = negotiator
            .take_transitions_for_persistence()
            .into_iter()
            .map(|transition| transition.transition_id)
            .collect::<Vec<_>>();
        assert_eq!(negotiator.acknowledge_persisted_transitions(&ids), 2);

        let retry = negotiator.assess(&policy, &request);
        assert_eq!(retry.path, AuthorizationPath::ExistingLease);
        assert_eq!(
            retry.lease.as_ref().map(|lease| lease.lease_id.as_str()),
            first.lease.as_ref().map(|lease| lease.lease_id.as_str())
        );
        assert_eq!(hot_state_counts(&negotiator), (1, 1, 1, 0, 0));
    }

    #[test]
    fn ten_thousand_durable_terminal_acks_leave_no_authorization_history_hot() {
        let negotiator = AuthorizationNegotiator::new();
        let policy = PermissionPolicy::new(PermissionMode::WorkspaceWrite);
        for index in 0..10_000 {
            let mut request = request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            ));
            request.input = format!(r#"{{"path":"generated/{index}.txt"}}"#);
            request.idempotency_key = format!("terminal-call-{index}");
            request.effect.scopes[0].target = Some(format!("generated/{index}.txt"));
            let assessment = negotiator.assess(&policy, &request);
            assert!(assessment.lease.is_some());
        }
        let transitions = negotiator.take_transitions_for_persistence();
        assert_eq!(transitions.len(), 20_000);
        for transition in transitions {
            assert_eq!(
                negotiator.acknowledge_persisted_transitions(std::slice::from_ref(
                    &transition.transition_id
                )),
                1
            );
        }
        assert_eq!(hot_state_counts(&negotiator), (0, 0, 0, 0, 0));
    }

    #[test]
    fn durable_expiry_and_revocation_acks_release_every_lease_index() {
        let active_read = || {
            let negotiator = AuthorizationNegotiator::new();
            let assessment = negotiator.assess(
                &PermissionPolicy::new(PermissionMode::ReadOnly),
                &request({
                    let mut descriptor =
                        effect(PermissionMode::ReadOnly, EffectExternality::Internal);
                    descriptor.assessment.blast_radius = EffectBlastRadius::Item;
                    descriptor
                }),
            );
            let lease = assessment.lease.expect("active reusable read lease");
            let ids = negotiator
                .take_transitions_for_persistence()
                .into_iter()
                .map(|transition| transition.transition_id)
                .collect::<Vec<_>>();
            assert_eq!(negotiator.acknowledge_persisted_transitions(&ids), 2);
            (negotiator, lease)
        };

        let (expired_negotiator, expiring) = active_read();
        expired_negotiator.reconcile_expirations_at(expiring.expires_at_ms.saturating_add(1));
        let expired_ids = expired_negotiator
            .take_transitions_for_persistence()
            .into_iter()
            .map(|transition| transition.transition_id)
            .collect::<Vec<_>>();
        assert_eq!(
            expired_negotiator.acknowledge_persisted_transitions(&expired_ids),
            1
        );
        assert_eq!(hot_state_counts(&expired_negotiator), (0, 0, 0, 0, 0));

        let (revoked_negotiator, revoked) = active_read();
        assert!(revoked_negotiator.revoke(&revoked.lease_id));
        let revoked_ids = revoked_negotiator
            .take_transitions_for_persistence()
            .into_iter()
            .map(|transition| transition.transition_id)
            .collect::<Vec<_>>();
        assert_eq!(
            revoked_negotiator.acknowledge_persisted_transitions(&revoked_ids),
            1
        );
        assert_eq!(hot_state_counts(&revoked_negotiator), (0, 0, 0, 0, 0));
    }

    #[test]
    fn ten_thousand_recovery_claim_terminals_leave_no_fingerprint_hot() {
        let negotiator = AuthorizationNegotiator::new();
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly);
        let mut fingerprints = Vec::with_capacity(10_000);
        for index in 0..10_000 {
            let mut denied = request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            ));
            denied.input = format!(r#"{{"path":"recovery/{index}.txt"}}"#);
            denied.effect.scopes[0].target = Some(format!("recovery/{index}.txt"));
            let assessment = negotiator.assess(&policy, &denied);
            let fingerprint = assessment
                .gap
                .as_ref()
                .expect("recoverable gap")
                .fingerprint
                .clone();
            assert!(negotiator.claim_controlled_recovery(&assessment, "turn:test"));
            fingerprints.push(fingerprint);
        }
        assert_eq!(hot_state_counts(&negotiator).3, 10_000);
        assert_eq!(
            negotiator.acknowledge_controlled_recovery_terminals(&fingerprints),
            10_000
        );
        assert_eq!(hot_state_counts(&negotiator), (0, 0, 0, 0, 0));
    }

    #[test]
    fn stale_lease_ack_cannot_release_a_new_recovery_claim_for_same_fingerprint() {
        let negotiator = AuthorizationNegotiator::new();
        let request = request(effect(
            PermissionMode::WorkspaceWrite,
            EffectExternality::Workspace,
        ));
        assert!(negotiator
            .assess(
                &PermissionPolicy::new(PermissionMode::WorkspaceWrite),
                &request,
            )
            .lease
            .is_some());
        let stale_transition_ids = negotiator
            .take_transitions_for_persistence()
            .into_iter()
            .map(|transition| transition.transition_id)
            .collect::<Vec<_>>();

        let mut denied_request = request.clone();
        denied_request.idempotency_key = "later-recovery-attempt".to_string();
        let denied = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &denied_request,
        );
        assert!(negotiator.claim_controlled_recovery(&denied, "turn:test"));
        assert_eq!(
            negotiator.acknowledge_persisted_transitions(&stale_transition_ids),
            2
        );
        assert!(!negotiator.claim_controlled_recovery(&denied, "turn:test"));
        assert_eq!(hot_state_counts(&negotiator).3, 1);
    }

    #[test]
    fn controlled_recovery_claim_is_released_only_after_terminal_ack() {
        let negotiator = AuthorizationNegotiator::new();
        let assessment = negotiator.assess(
            &PermissionPolicy::new(PermissionMode::ReadOnly),
            &request(effect(
                PermissionMode::WorkspaceWrite,
                EffectExternality::Workspace,
            )),
        );
        let fingerprint = assessment
            .gap
            .as_ref()
            .expect("recoverable gap")
            .fingerprint
            .clone();
        assert!(negotiator.claim_controlled_recovery(&assessment, "turn:test"));
        assert!(!negotiator.claim_controlled_recovery(&assessment, "turn:test"));
        assert_eq!(
            negotiator.acknowledge_controlled_recovery_terminals(&[fingerprint]),
            1
        );
        assert!(negotiator.claim_controlled_recovery(&assessment, "turn:test"));
        assert_eq!(hot_state_counts(&negotiator).3, 1);
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

pub(crate) fn authorization_lease_signed_payload(lease: &AuthorizationLease) -> serde_json::Value {
    serde_json::json!({
        "lease_id":lease.lease_id,"principal_id":lease.principal_id,"parent_lease_id":lease.parent_lease_id,
        "capability":lease.capability,"scopes":lease.scopes,"ceiling":lease.ceiling,
        "issued_at_ms":lease.issued_at_ms,"expires_at_ms":lease.expires_at_ms,"max_uses":lease.max_uses,
        "policy_revision":lease.policy_revision,"effect_descriptor_hash":lease.effect_descriptor_hash,
    })
}

pub(crate) fn validate_historical_authorization_issuer(
    issuer: &crate::DurableRuntimeEvent,
    grant: &harness_contract::tool::ToolExecutionAuthorization,
    receipt_cursor: u64,
) -> Result<(), String> {
    let transition: AuthorizationLeaseTransition = serde_json::from_value(issuer.payload.clone()).map_err(|error| error.to_string())?;
    if issuer.stream_id != format!("authorization-lease:{}", grant.authorization_lease.lease_id)
        || issuer.sequence != 1 || issuer.scope != crate::RuntimeEventScope::Tool || issuer.schema_version != 1
        || issuer.kind != "authorization.lease_transition" || issuer.status.as_deref() != Some("issued")
        || !matches!(issuer.actor.as_deref(), Some("conversation_runtime" | "conversation_runtime.early_tool" | "runtime.process_jsonl"))
        || transition.kind != AuthorizationLeaseTransitionKind::Issued
        || issuer.idempotency_key.as_deref() != Some(transition.transition_id.as_str())
        || issuer.transaction_id != format!("authorization-transition:{}", transition.transition_id)
        || issuer.transaction_index != 0 || issuer.commit_cursor == 0 || issuer.commit_cursor >= receipt_cursor
        || issuer.refs.len() != 1 || issuer.refs[0].kind != "authorization_lease"
        || issuer.refs[0].id != grant.authorization_lease.lease_id
        || transition.lease.signature.is_empty() || transition.lease.signature != grant.authorization_lease.signature
        || authorization_lease_signed_payload(&transition.lease) != authorization_lease_signed_payload(&grant.authorization_lease)
    {
        return Err("historical authorization issuer provenance or immutable grant mismatch".into());
    }
    Ok(())
}

#[cfg(feature = "test-fixtures")]
pub fn persist_authorization_fixture_transitions(
    negotiator: &AuthorizationNegotiator,
    store: &crate::RuntimeEventStore,
) -> Result<(), String> {
    let _ = negotiator.take_transitions_for_persistence();
    for transition in negotiator.transitions_awaiting_persistence() {
        persist_authorization_transition(store, &format!("authorization-lease:{}", transition.lease.lease_id), "conversation_runtime", &transition)?;
        negotiator.acknowledge_persisted_transitions(std::slice::from_ref(&transition.transition_id));
    }
    Ok(())
}
