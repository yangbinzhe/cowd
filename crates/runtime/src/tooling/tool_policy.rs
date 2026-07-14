use harness_contract::tool::{
    ToolEffectDescriptor, ToolEffectKind, ToolExecutionAuthorization, ToolIdempotency,
    ToolPermissionMode,
};

use crate::PermissionMode;

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
    #[error("tool requires {required:?}, active permission is {active:?}")]
    PermissionDenied {
        required: ToolPermissionMode,
        active: PermissionMode,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ToolPolicy;

impl ToolPolicy {
    pub fn authorize(
        &self,
        descriptor: &ToolEffectDescriptor,
        request_id: impl Into<String>,
        active_permission: PermissionMode,
        timeout_secs: u64,
    ) -> Result<ToolExecutionPolicyDecision, ToolPolicyError> {
        if permission_rank(descriptor.required_permission)
            > runtime_permission_rank(active_permission)
        {
            return Err(ToolPolicyError::PermissionDenied {
                required: descriptor.required_permission,
                active: active_permission,
            });
        }
        let scope = descriptor
            .scopes
            .first()
            .cloned()
            .ok_or(ToolPolicyError::MissingScope)?;
        let request_id = request_id.into();
        let idempotency_key = matches!(descriptor.idempotency, ToolIdempotency::IdempotentWithKey)
            .then(|| format!("{request_id}:{}", descriptor.descriptor_hash));
        let parallel_safe = descriptor.idempotency == ToolIdempotency::Idempotent
            && matches!(descriptor.effect_kind, ToolEffectKind::Read);
        Ok(ToolExecutionPolicyDecision {
            authorization: ToolExecutionAuthorization {
                request_id,
                tool_id: descriptor.tool_id.clone(),
                descriptor_hash: descriptor.descriptor_hash.clone(),
                scope,
                permission_lease: format!(
                    "permission:{}:{}",
                    active_permission_label(active_permission),
                    descriptor.descriptor_hash
                ),
                timeout_lease: format!("timeout:{timeout_secs}"),
                idempotency_key,
            },
            timeout_secs,
            parallel_safe,
        })
    }
}

fn permission_rank(mode: ToolPermissionMode) -> u8 {
    match mode {
        ToolPermissionMode::ReadOnly => 0,
        ToolPermissionMode::WorkspaceWrite => 1,
        ToolPermissionMode::DangerFullAccess => 2,
    }
}

fn runtime_permission_rank(mode: PermissionMode) -> u8 {
    match mode {
        PermissionMode::ReadOnly => 0,
        PermissionMode::WorkspaceWrite => 1,
        PermissionMode::DangerFullAccess | PermissionMode::Prompt | PermissionMode::Allow => 2,
    }
}

fn active_permission_label(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::ReadOnly => "read_only",
        PermissionMode::WorkspaceWrite => "workspace_write",
        PermissionMode::DangerFullAccess => "danger_full_access",
        PermissionMode::Prompt => "prompt",
        PermissionMode::Allow => "allow",
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
        }
    }

    #[test]
    fn authorization_never_escalates_active_permission() {
        let error = ToolPolicy
            .authorize(
                &descriptor(ToolPermissionMode::WorkspaceWrite),
                "request",
                PermissionMode::ReadOnly,
                30,
            )
            .unwrap_err();
        assert!(matches!(error, ToolPolicyError::PermissionDenied { .. }));
    }

    #[test]
    fn write_authorization_has_stable_idempotency_key() {
        let decision = ToolPolicy
            .authorize(
                &descriptor(ToolPermissionMode::WorkspaceWrite),
                "request",
                PermissionMode::WorkspaceWrite,
                30,
            )
            .unwrap();
        assert_eq!(
            decision.authorization.idempotency_key.as_deref(),
            Some("request:hash")
        );
        assert!(!decision.parallel_safe);
    }
}
