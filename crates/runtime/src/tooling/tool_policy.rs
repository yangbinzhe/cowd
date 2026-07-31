use harness_contract::policy::AuthorizationLease;
use harness_contract::tool::{
    ToolEffectDescriptor, ToolEffectKind, ToolExecutionAuthorization, ToolIdempotency,
};

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
        descriptor: &ToolEffectDescriptor,
        request_id: impl Into<String>,
        authorization_lease: AuthorizationLease,
        timeout_secs: u64,
    ) -> Result<ToolExecutionPolicyDecision, ToolPolicyError> {
        if !authorization_lease.permits(&descriptor.tool_id, descriptor.required_permission) {
            return Err(ToolPolicyError::PermissionDenied);
        }
        if !descriptor
            .scopes
            .iter()
            .all(|scope| authorization_lease.scopes.contains(scope))
        {
            return Err(ToolPolicyError::LeaseScopeMismatch);
        }
        let scope = descriptor
            .scopes
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
            signature: "test-signature".to_string(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        }
    }

    #[test]
    fn authorization_never_escalates_active_permission() {
        let error = ToolPolicy
            .authorize(
                &descriptor(ToolPermissionMode::WorkspaceWrite),
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
                &descriptor,
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
}
