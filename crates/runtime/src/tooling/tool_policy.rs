use harness_contract::policy::{AuthorizationLease, CapabilityAssessment};
use harness_contract::tool::{ToolEffectKind, ToolExecutionAuthorization, ToolIdempotency};

use crate::EffectiveToolAuthorizationDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionPolicyDecision {
    pub authorization: ToolExecutionAuthorization,
    pub timeout_secs: u64,
    pub parallel_safe: bool,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ToolPolicyError {
    #[error("tool effect descriptor has no permission scope")]
    MissingScope,
    #[error("authorization lease does not cover tool effect")]
    LeaseScopeMismatch,
    #[error("authorization lease does not permit required mode")]
    PermissionDenied,
    #[error("authorization lease idempotency key does not match request")]
    LeaseIdempotencyMismatch,
}

#[derive(Debug, Clone, Default)]
pub struct ToolPolicy;

impl ToolPolicy {
    pub fn authorize(
        &self,
        effective: &EffectiveToolAuthorizationDescriptor,
        assessment: &CapabilityAssessment,
        request_id: impl Into<String>,
        authorization_lease: AuthorizationLease,
        timeout_secs: u64,
    ) -> Result<ToolExecutionPolicyDecision, ToolPolicyError> {
        let descriptor = &effective.descriptor;
        if assessment.capability != descriptor.tool_id
            || assessment.effect != descriptor.assessment
            || assessment.requested_scopes != descriptor.scopes
            || assessment.required_mode != descriptor.required_permission
            || !authorization_lease.permits(&assessment.capability, assessment.required_mode)
            || authorization_lease.policy_revision == 0
            || authorization_lease.effect_descriptor_hash != descriptor.descriptor_hash
        {
            return Err(ToolPolicyError::PermissionDenied);
        }
        if !assessment
            .requested_scopes
            .iter()
            .all(|scope| authorization_lease.scopes.contains(scope))
        {
            return Err(ToolPolicyError::LeaseScopeMismatch);
        }
        let scope = assessment
            .requested_scopes
            .first()
            .cloned()
            .ok_or(ToolPolicyError::MissingScope)?;
        let request_id = request_id.into();
        let idempotency_key =
            if matches!(descriptor.idempotency, ToolIdempotency::IdempotentWithKey) {
                if authorization_lease.idempotency_key != request_id {
                    return Err(ToolPolicyError::LeaseIdempotencyMismatch);
                }
                Some(request_id.clone())
            } else {
                None
            };
        let parallel_safe = descriptor.idempotency == ToolIdempotency::Idempotent
            && matches!(descriptor.effect_kind, ToolEffectKind::Read);
        Ok(ToolExecutionPolicyDecision {
            authorization: ToolExecutionAuthorization {
                request_id,
                tool_id: descriptor.tool_id.clone(),
                descriptor_hash: descriptor.descriptor_hash.clone(),
                policy_revision: authorization_lease.policy_revision,
                scope,
                authorization_lease,
                timeout_lease: format!("timeout:{timeout_secs}"),
                idempotency_key,
            },
            timeout_secs,
            parallel_safe,
        })
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
        ToolPermissionMode,
    };

    use super::*;

    fn descriptor(permission: ToolPermissionMode) -> ToolEffectDescriptor {
        ToolEffectDescriptor {
            tool_id: "write_file".to_string(),
            descriptor_hash: "hash".to_string(),
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![PermissionScope::new(
                PermissionResource::File,
                PermissionOperation::Write,
            )],
            required_permission: permission,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: harness_contract::policy::EffectAssessment::default(),
        }
    }

    fn assessment(
        descriptor: &ToolEffectDescriptor,
        required_mode: ToolPermissionMode,
    ) -> CapabilityAssessment {
        let effective = effective(descriptor);
        CapabilityAssessment {
            assessment_id: "assessment".to_string(),
            capability: descriptor.tool_id.clone(),
            effect: effective.descriptor.assessment,
            requested_scopes: effective.descriptor.scopes,
            required_mode,
            active_ceiling: ToolPermissionMode::DangerFullAccess,
            parent_ceiling: ToolPermissionMode::DangerFullAccess,
            risk: harness_contract::policy::RiskLevel::Low,
            path: harness_contract::policy::AuthorizationPath::PolicyAutoGrant,
            lease: None,
            gap: None,
            evidence_refs: Vec::new(),
            assessed_at_ms: 0,
        }
    }

    fn lease(descriptor: &ToolEffectDescriptor, ceiling: ToolPermissionMode) -> AuthorizationLease {
        AuthorizationLease {
            lease_id: "lease".to_string(),
            principal_id: "test".to_string(),
            parent_lease_id: None,
            capability: descriptor.tool_id.clone(),
            scopes: descriptor.scopes.clone(),
            ceiling,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: "request".to_string(),
            policy_revision: 1,
            effect_descriptor_hash: descriptor.descriptor_hash.clone(),
            signature: "test-signature".to_string(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        }
    }

    fn effective(descriptor: &ToolEffectDescriptor) -> EffectiveToolAuthorizationDescriptor {
        crate::AuthorizationNegotiator::compile_effective_descriptor(descriptor, "{}")
    }

    #[test]
    fn authorization_never_escalates_active_permission() {
        let error = ToolPolicy
            .authorize(
                &effective(&descriptor(ToolPermissionMode::WorkspaceWrite)),
                &assessment(
                    &descriptor(ToolPermissionMode::WorkspaceWrite),
                    ToolPermissionMode::WorkspaceWrite,
                ),
                "request",
                lease(
                    &descriptor(ToolPermissionMode::WorkspaceWrite),
                    ToolPermissionMode::ReadOnly,
                ),
                30,
            )
            .unwrap_err();
        assert!(matches!(error, ToolPolicyError::PermissionDenied));
    }

    #[test]
    fn write_authorization_has_stable_idempotency_key() {
        let descriptor = descriptor(ToolPermissionMode::WorkspaceWrite);
        let decision = ToolPolicy
            .authorize(
                &effective(&descriptor),
                &assessment(&descriptor, ToolPermissionMode::WorkspaceWrite),
                "request",
                lease(&descriptor, ToolPermissionMode::WorkspaceWrite),
                30,
            )
            .unwrap();
        assert_eq!(
            decision.authorization.idempotency_key.as_deref(),
            Some("request")
        );
        assert!(!decision.parallel_safe);
    }

    #[test]
    fn execution_cannot_use_a_lease_below_the_registered_permission_floor() {
        let descriptor = descriptor(ToolPermissionMode::DangerFullAccess);
        let assessed = assessment(&descriptor, ToolPermissionMode::WorkspaceWrite);
        let error = ToolPolicy
            .authorize(
                &effective(&descriptor),
                &assessed,
                "request",
                lease(&descriptor, ToolPermissionMode::WorkspaceWrite),
                30,
            )
            .expect_err("Runtime must not lower the ToolHost permission contract");
        assert_eq!(error, ToolPolicyError::PermissionDenied);
    }
}
