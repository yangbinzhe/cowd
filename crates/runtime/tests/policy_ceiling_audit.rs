//! ROUND1385 standing-grant vs issued-lease-ceiling audit gate.
//!
//! Test-only file. No product code is modified by this gate.
//! Independent tree: .cowd-worktrees/k04-r1385-policy-ceiling-audit
//! (HEAD 1a291805 + the current cowd-dev WIP diff, applied as source only).
//!
//! Invariant under audit: a lease is only meaningful when its own ceiling can
//! authorize the effect it was issued for. `AuthorizationLease::permits` is the
//! single consumption contract - `ToolPolicy::authorize` and the negotiator's
//! own reuse path (`consume_existing`) both use it - and the durable
//! `AuthorizationLeaseTransition` records the ceiling that was granted.
//!
//! The four stages that must agree:
//!   1. first authorization : `AuthorizationNegotiator::assess` (route + assessment)
//!   2. lease issuance      : `issue_and_consume` (lease.ceiling)
//!   3. ToolPolicy consumption : `ToolPolicy::authorize`
//!   4. lease reuse         : `consume_existing` (same fingerprint)
//!
//! Stage 1 currently claims an authorization that stage 2 cannot back and
//! stages 3/4 reject. These gates pin the invariant, not the policy choice:
//! a fix may either refuse the effect (typed gap, no lease) or raise the lease
//! ceiling, but it may never report an authorization it cannot back.

use harness_contract::policy::{
    ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalGrant, ApprovalGrantScope,
    ApprovalGrantStatus, CapabilityGapKind, DataClassification, EffectAssessment,
    EffectBlastRadius, EffectExternality, EffectNovelty, EffectReversibility, PermissionMode,
    PermissionOperation, PermissionResource, PermissionScope,
};
use harness_contract::tool::{
    ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
};
use runtime::{
    AuthorizationNegotiator, AuthorizationRequest, EffectiveToolAuthorizationDescriptor,
    PermissionContext, PermissionPolicy, RuntimePermissionRuleConfig, ToolPolicy,
};

fn write_effect() -> ToolEffectDescriptor {
    ToolEffectDescriptor {
        tool_id: "test_tool".to_string(),
        descriptor_hash: "sha256:test-tool-write".to_string(),
        effect_kind: ToolEffectKind::Write,
        idempotency: ToolIdempotency::IdempotentWithKey,
        scopes: vec![PermissionScope {
            resource: PermissionResource::File,
            operation: PermissionOperation::Write,
            target: Some("src/lib.rs".to_string()),
        }],
        required_permission: PermissionMode::WorkspaceWrite,
        approval_class: ToolApprovalClass::Policy,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: EffectAssessment {
            reversibility: EffectReversibility::Compensatable,
            externality: EffectExternality::Workspace,
            data_sensitivity: DataClassification::Internal,
            novelty: EffectNovelty::Routine,
            blast_radius: EffectBlastRadius::Workspace,
        },
    }
}

fn read_effect() -> ToolEffectDescriptor {
    let mut descriptor = write_effect();
    descriptor.descriptor_hash = "sha256:test-tool-read".to_string();
    descriptor.effect_kind = ToolEffectKind::Read;
    descriptor.required_permission = PermissionMode::ReadOnly;
    descriptor.scopes = vec![PermissionScope {
        resource: PermissionResource::File,
        operation: PermissionOperation::Read,
        target: Some("src/lib.rs".to_string()),
    }];
    descriptor.assessment.externality = EffectExternality::Internal;
    descriptor.assessment.blast_radius = EffectBlastRadius::Item;
    descriptor
}

fn request(descriptor: ToolEffectDescriptor) -> AuthorizationRequest {
    AuthorizationRequest {
        principal_id: "session:test".to_string(),
        capability: "test_tool".to_string(),
        input: r#"{"path":"src/lib.rs"}"#.to_string(),
        idempotency_key: "call-1".to_string(),
        effect: descriptor,
        parent_ceiling: PermissionMode::DangerFullAccess,
        parent_lease_id: None,
        policy_revision: 1,
        recovery_scope: "turn:test".to_string(),
        context: PermissionContext::default(),
        safe_alternatives: Vec::new(),
    }
}

fn standing_grant_policy(session_ceiling: PermissionMode) -> PermissionPolicy {
    let rules = RuntimePermissionRuleConfig::new(
        vec!["test_tool(src/lib.rs)".to_string()],
        Vec::new(),
        Vec::new(),
    );
    PermissionPolicy::new(session_ceiling).with_permission_rules(&rules)
}

fn approval_grant(
    request: &AuthorizationRequest,
    effective: &EffectiveToolAuthorizationDescriptor,
) -> ApprovalGrant {
    ApprovalGrant {
        grant_id: "approval-grant:test".to_string(),
        approval_id: "approval:test".to_string(),
        scope: ApprovalGrantScope::Once,
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
        issued_by: ApprovalDecisionActor {
            kind: ApprovalDecisionActorKind::Human,
            actor_id: "test".to_string(),
        },
        created_at_ms: 0,
        expires_at_ms: None,
        revoked_at_ms: None,
        revoke_reason: None,
    }
}

/// Stage 1 vs stage 2: a standing grant above the session ceiling must not
/// produce an assessment whose lease cannot authorize its own effect.
#[test]
fn standing_grant_above_the_session_ceiling_never_issues_an_unusable_lease() {
    let policy = standing_grant_policy(PermissionMode::ReadOnly);
    let negotiator = AuthorizationNegotiator::new();
    let request = request(write_effect());
    let evaluated = negotiator.assess_effective(&policy, &request);
    let assessment = evaluated.assessment;

    eprintln!(
        "observed: path={:?} required_mode={:?} active_ceiling={:?} lease_ceiling={:?} gap={:?}",
        assessment.path,
        assessment.required_mode,
        assessment.active_ceiling,
        assessment.lease.as_ref().map(|lease| lease.ceiling),
        assessment.gap.as_ref().map(|gap| gap.kind),
    );

    if let Some(lease) = assessment.lease.as_ref() {
        assert!(
            lease.permits(&assessment.capability, assessment.required_mode),
            "stage 1 reported an authorized assessment (path={:?}) but stage 2 issued a lease \
             (ceiling={:?}) that cannot authorize the assessed effect (required={:?}); \
             stages 3/4 will reject this authorization",
            assessment.path,
            lease.ceiling,
            assessment.required_mode,
        );
    }
}

/// Stage 1 vs stage 3: whatever the negotiator reports as authorized must be
/// consumable by the Tool policy that actually gates execution.
#[test]
fn an_authorized_assessment_is_always_consumable_by_the_tool_policy() {
    let policy = standing_grant_policy(PermissionMode::ReadOnly);
    let negotiator = AuthorizationNegotiator::new();
    let request = request(write_effect());
    let evaluated = negotiator.assess_effective(&policy, &request);
    let assessment = evaluated.assessment;
    let Some(lease) = assessment.lease.clone() else {
        // A refusal is allowed to carry no lease.
        assert!(
            assessment.gap.is_some(),
            "an assessment without a lease must carry a typed gap, not a silent refusal"
        );
        return;
    };

    let decision = ToolPolicy.authorize(
        &evaluated.effective,
        &assessment,
        request.idempotency_key.clone(),
        lease,
        30,
    );
    assert!(
        decision.is_ok(),
        "stage 3 rejected an authorization that stage 1 reported: {:?}",
        decision.err()
    );
}

/// Stage 4: a standing grant lease is long lived and multi use, so an
/// identical follow-up request must reuse it instead of issuing a second one.
#[test]
fn a_standing_grant_lease_is_reused_instead_of_reissued() {
    let policy = standing_grant_policy(PermissionMode::ReadOnly);
    let negotiator = AuthorizationNegotiator::new();
    let request = request(write_effect());
    let first = negotiator.assess(&policy, &request);
    let second = negotiator.assess(&policy, &request);

    eprintln!(
        "observed: first_path={:?} first_lease={:?} second_path={:?} second_lease={:?}",
        first.path,
        first.lease.as_ref().map(|lease| lease.lease_id.clone()),
        second.path,
        second.lease.as_ref().map(|lease| lease.lease_id.clone()),
    );

    match (first.lease.as_ref(), second.lease.as_ref()) {
        (None, _) => assert!(
            first.gap.is_some(),
            "a refusal must carry a typed gap instead of a silent refusal"
        ),
        (Some(first_lease), Some(second_lease)) => assert_eq!(
            first_lease.lease_id, second_lease.lease_id,
            "the same fingerprint was authorized twice under one standing grant; \
             a second lease was issued and the second idempotency key was consumed again"
        ),
        (Some(_), None) => assert!(
            second.gap.is_some(),
            "a reused standing grant must not silently turn into an unexplained refusal"
        ),
    }
}

/// Stage 1 vs stage 2 on the human approval path: an explicit approval above
/// the session ceiling must not produce an unusable lease either.
#[test]
fn human_approval_above_the_session_ceiling_never_issues_an_unusable_lease() {
    let policy = PermissionPolicy::new(PermissionMode::ReadOnly);
    let negotiator = AuthorizationNegotiator::new();
    let request = request(write_effect());
    let effective = AuthorizationNegotiator::compile_effective_descriptor(&request.effect, &request.input);
    let grant = approval_grant(&request, &effective);
    let approved = negotiator.approve_effective(&policy, &request, &effective, &grant);

    eprintln!(
        "observed: path={:?} required_mode={:?} active_ceiling={:?} lease_ceiling={:?} gap={:?}",
        approved.path,
        approved.required_mode,
        approved.active_ceiling,
        approved.lease.as_ref().map(|lease| lease.ceiling),
        approved.gap.as_ref().map(|gap| gap.kind),
    );

    if let Some(lease) = approved.lease.as_ref() {
        assert!(
            lease.permits(&approved.capability, approved.required_mode),
            "an approved assessment (path={:?}) carries a lease (ceiling={:?}) that cannot \
             authorize the approved effect (required={:?})",
            approved.path,
            lease.ceiling,
            approved.required_mode,
        );
    }
}

/// Guard: a standing grant inside the session ceiling keeps working and stays
/// consumable. A fix must not trade a false authorization for a lost grant.
#[test]
fn a_within_ceiling_standing_grant_still_authorizes_and_is_consumable() {
    let policy = standing_grant_policy(PermissionMode::WorkspaceWrite);
    let negotiator = AuthorizationNegotiator::new();
    let request = request(read_effect());
    let evaluated = negotiator.assess_effective(&policy, &request);
    let assessment = evaluated.assessment;

    assert_eq!(assessment.path, harness_contract::policy::AuthorizationPath::StandingGrant);
    let lease = assessment.lease.clone().expect("a within-ceiling grant must issue a lease");
    assert!(lease.permits(&assessment.capability, assessment.required_mode));
    let decision = ToolPolicy.authorize(
        &evaluated.effective,
        &assessment,
        request.idempotency_key.clone(),
        lease,
        30,
    );
    assert!(decision.is_ok(), "{:?}", decision.err());
}

/// Guard: a delegated request can never exceed its parent lease ceiling, even
/// when a standing grant matches. This is the hard cap that must survive any
/// ceiling fix.
#[test]
fn a_delegated_standing_grant_cannot_exceed_the_parent_ceiling() {
    let policy = standing_grant_policy(PermissionMode::DangerFullAccess);
    let negotiator = AuthorizationNegotiator::new();
    let mut request = request(write_effect());
    request.parent_ceiling = PermissionMode::ReadOnly;
    request.parent_lease_id = Some("parent:read-only".to_string());
    let assessment = negotiator.assess(&policy, &request);

    assert!(
        assessment.lease.is_none(),
        "a delegated request above its parent ceiling must not receive a lease"
    );
    assert_eq!(
        assessment.gap.as_ref().map(|gap| gap.kind),
        Some(CapabilityGapKind::PermissionCeiling),
        "the refusal must be a typed permission-ceiling gap"
    );
}

/// Guard: a full trust session keeps issuing control-plane leases at the
/// session ceiling, never at the effect floor. This is the product invariant
/// that the ceiling change (17a75c07) established.
#[test]
fn a_full_trust_session_lease_keeps_the_session_ceiling() {
    let policy = PermissionPolicy::new(PermissionMode::DangerFullAccess);
    let negotiator = AuthorizationNegotiator::new();
    let request = request(write_effect());
    let assessment = negotiator.assess(&policy, &request);
    let lease = assessment.lease.clone().expect("a full trust session issues a lease");
    assert_eq!(
        lease.ceiling,
        PermissionMode::DangerFullAccess,
        "a full trust session must issue control-plane leases at the session ceiling"
    );
}
