//! Tool contracts for the Cowd AI harness.

use crate::policy::PermissionScope;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDescriptorHealth {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDescriptorRef {
    pub canonical_id: String,
    pub display_name: String,
    pub source: String,
    pub schema_hash: String,
    pub required_permission: ToolPermissionMode,
    pub permission_source: String,
    pub health: ToolDescriptorHealth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDiscoveryReceipt {
    pub query: String,
    pub catalog_revision: u64,
    pub descriptors: Vec<ToolDescriptorRef>,
    pub activation_candidates: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolActivationStatus {
    Activated,
    Denied,
    Unavailable,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolActivationDecision {
    pub canonical_id: String,
    pub status: ToolActivationStatus,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolActivationReceipt {
    pub catalog_revision: u64,
    pub previous_exposure_revision: u64,
    pub exposure_revision: u64,
    pub decisions: Vec<ToolActivationDecision>,
}

impl ToolActivationReceipt {
    pub fn activated_ids(&self) -> impl Iterator<Item = &str> {
        self.decisions.iter().filter_map(|decision| {
            (decision.status == ToolActivationStatus::Activated)
                .then_some(decision.canonical_id.as_str())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExposureProjection {
    pub catalog_revision: u64,
    pub exposure_revision: u64,
    pub bootstrap_ids: Vec<String>,
    pub active_ids: Vec<String>,
    pub deferred_ids: Vec<String>,
    pub fallback_full: bool,
    pub reason: String,
    pub schema_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffectKind {
    Read,
    Write,
    Network,
    Process,
    Package,
    System,
    Destructive,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolIdempotency {
    Idempotent,
    IdempotentWithKey,
    NonIdempotent,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolApprovalClass {
    None,
    Policy,
    User,
    Administrator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolEffectDescriptor {
    pub tool_id: String,
    pub descriptor_hash: String,
    pub effect_kind: ToolEffectKind,
    pub idempotency: ToolIdempotency,
    pub scopes: Vec<PermissionScope>,
    pub required_permission: ToolPermissionMode,
    pub approval_class: ToolApprovalClass,
    pub uses_network: bool,
    pub spawns_process: bool,
    pub mutates_packages: bool,
    pub mutates_system: bool,
}

/// Declarative resolver selected when a tool is registered.
///
/// The contract carries resolver identity only. Implementations remain in the
/// tool host, so Runtime can pin and audit the descriptor without importing
/// tool implementation code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolEffectResolverSpec {
    pub resolver_id: String,
    pub resolver_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolIntent {
    pub invocation_id: String,
    pub tool_name: String,
    pub normalized_input: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDependency {
    pub invocation_id: String,
    pub depends_on: String,
    pub reason: String,
}

/// Resource demand compiled together with policy and dependency metadata.
/// Counts are weights rather than implementation-specific semaphore permits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDemand {
    pub tool_slots: u32,
    pub process_slots: u32,
    pub network_slots: u32,
    pub cpu_weight: u32,
    pub memory_bytes: u64,
    pub scopes: Vec<ResourceScopeDemand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceAccess {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceScopeDemand {
    pub key: String,
    pub access: ResourceAccess,
}

impl Default for ResourceDemand {
    fn default() -> Self {
        Self {
            tool_slots: 1,
            process_slots: 0,
            network_slots: 0,
            cpu_weight: 1,
            memory_bytes: 0,
            scopes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernedToolInvocation {
    pub contract_version: u32,
    pub invocation_id: String,
    pub intent: ToolIntent,
    pub effect: ToolEffectDescriptor,
    pub resource_demand: ResourceDemand,
    /// Dependencies explicitly declared by the model/provider response.
    pub explicit_dependencies: Vec<ToolDependency>,
    /// Deterministic safety dependencies compiled by Runtime (for example,
    /// overlapping write scopes). These are executable policy, not display
    /// annotations.
    #[serde(default)]
    pub compiled_dependencies: Vec<ToolDependency>,
    pub catalog_revision: u64,
    pub descriptor_set_hash: String,
    pub idempotency_key: String,
}

/// Stable, implementation-neutral projection emitted by Runtime's sole
/// governed tool compiler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernedToolPlanProjection {
    pub contract_version: u32,
    pub plan_id: String,
    pub revision: u64,
    pub catalog_revision: u64,
    pub invocations: Vec<GovernedToolInvocation>,
    pub dependencies: Vec<ToolDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionAuthorization {
    pub request_id: String,
    pub tool_id: String,
    pub descriptor_hash: String,
    pub scope: PermissionScope,
    pub permission_lease: String,
    pub timeout_lease: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPermissionMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl ToolPermissionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WorkspaceWrite => "workspace_write",
            Self::DangerFullAccess => "danger_full_access",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
    pub required_permission: ToolPermissionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionStatus {
    Succeeded,
    Failed,
    Denied,
    Cancelled,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolUnavailableReason {
    MissingDependency,
    ServerDisconnected,
    CapabilityUnsupported,
    Timeout,
    Misconfigured,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionReceipt {
    #[serde(default)]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub plan_revision: Option<u64>,
    #[serde(default)]
    pub invocation_id: Option<String>,
    pub tool_name: String,
    pub status: ToolExecutionStatus,
    pub permission: ToolPermissionMode,
    pub evidence_refs: Vec<String>,
    pub error: Option<String>,
    pub unavailable_reason: Option<ToolUnavailableReason>,
}

impl ToolExecutionReceipt {
    #[must_use]
    pub fn succeeded(tool_name: impl Into<String>, permission: ToolPermissionMode) -> Self {
        Self {
            plan_id: None,
            plan_revision: None,
            invocation_id: None,
            tool_name: tool_name.into(),
            status: ToolExecutionStatus::Succeeded,
            permission,
            evidence_refs: Vec::new(),
            error: None,
            unavailable_reason: None,
        }
    }

    #[must_use]
    pub fn failed(
        tool_name: impl Into<String>,
        permission: ToolPermissionMode,
        error: impl Into<String>,
    ) -> Self {
        Self {
            plan_id: None,
            plan_revision: None,
            invocation_id: None,
            tool_name: tool_name.into(),
            status: ToolExecutionStatus::Failed,
            permission,
            evidence_refs: Vec::new(),
            error: Some(error.into()),
            unavailable_reason: None,
        }
    }

    #[must_use]
    pub fn unavailable(
        tool_name: impl Into<String>,
        permission: ToolPermissionMode,
        reason: ToolUnavailableReason,
        error: impl Into<String>,
    ) -> Self {
        Self {
            plan_id: None,
            plan_revision: None,
            invocation_id: None,
            tool_name: tool_name.into(),
            status: ToolExecutionStatus::Unavailable,
            permission,
            evidence_refs: Vec::new(),
            error: Some(error.into()),
            unavailable_reason: Some(reason),
        }
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, evidence_ref: impl Into<String>) -> Self {
        self.evidence_refs.push(evidence_ref.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{PermissionOperation, PermissionResource};

    #[test]
    fn activation_receipt_exposes_only_accepted_canonical_ids() {
        let receipt = ToolActivationReceipt {
            catalog_revision: 7,
            previous_exposure_revision: 2,
            exposure_revision: 3,
            decisions: vec![
                ToolActivationDecision {
                    canonical_id: "read_file".to_string(),
                    status: ToolActivationStatus::Activated,
                    reason: "allowed".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "write_file".to_string(),
                    status: ToolActivationStatus::Denied,
                    reason: "read-only lease".to_string(),
                },
            ],
        };

        assert_eq!(
            receipt.activated_ids().collect::<Vec<_>>(),
            vec!["read_file"]
        );
    }

    #[test]
    fn effect_and_authorization_contracts_have_stable_wire_names() {
        let scope = PermissionScope::new(PermissionResource::File, PermissionOperation::Write);
        let descriptor = ToolEffectDescriptor {
            tool_id: "write_file".to_string(),
            descriptor_hash: "sha256:descriptor".to_string(),
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![scope.clone()],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        };
        let authorization = ToolExecutionAuthorization {
            request_id: "request-1".to_string(),
            tool_id: descriptor.tool_id.clone(),
            descriptor_hash: descriptor.descriptor_hash.clone(),
            scope,
            permission_lease: "permission-1".to_string(),
            timeout_lease: "timeout-1".to_string(),
            idempotency_key: Some("write-1".to_string()),
        };

        assert_eq!(
            serde_json::to_value(descriptor).unwrap()["effect_kind"],
            "write"
        );
        assert_eq!(
            serde_json::to_value(authorization).unwrap()["idempotency_key"],
            "write-1"
        );
    }
}
