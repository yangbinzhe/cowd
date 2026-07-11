//! Tool transaction planning for Cowd AI work kernel.

use std::collections::BTreeSet;

use crate::core::{AiKernelError, AiKernelResult};
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
    #[must_use]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAccessMode {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOperation {
    pub id: String,
    pub tool_name: String,
    pub access: ToolAccessMode,
    pub risk: ToolRisk,
    pub path: Option<String>,
}

impl ToolOperation {
    #[must_use]
    pub fn read(tool_name: impl Into<String>, path: Option<String>) -> Self {
        Self::new(tool_name, ToolAccessMode::Read, ToolRisk::Low, path)
    }

    #[must_use]
    pub fn write(tool_name: impl Into<String>, risk: ToolRisk, path: Option<String>) -> Self {
        Self::new(tool_name, ToolAccessMode::Write, risk, path)
    }

    fn new(
        tool_name: impl Into<String>,
        access: ToolAccessMode,
        risk: ToolRisk,
        path: Option<String>,
    ) -> Self {
        Self {
            id: format!("tool-op-{}", uuid::Uuid::new_v4()),
            tool_name: tool_name.into(),
            access,
            risk,
            path: path.map(normalize_path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTransactionPlan {
    pub id: String,
    pub batches: Vec<Vec<ToolOperation>>,
    pub requires_checkpoint: bool,
    pub requires_human_confirm: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTransactionReceipt {
    pub transaction_id: String,
    pub completed_operations: usize,
    pub failed_operations: usize,
    pub checkpoint_created: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ToolTransactionPlanner;

impl ToolTransactionPlanner {
    pub fn plan(&self, operations: Vec<ToolOperation>) -> AiKernelResult<ToolTransactionPlan> {
        detect_write_conflicts(&operations)?;
        let requires_checkpoint = operations.iter().any(|operation| {
            operation.access == ToolAccessMode::Write
                && matches!(
                    operation.risk,
                    ToolRisk::Medium | ToolRisk::High | ToolRisk::Critical
                )
        });
        let requires_human_confirm = operations
            .iter()
            .any(|operation| operation.risk == ToolRisk::Critical);
        let warnings = operations
            .iter()
            .filter(|operation| {
                operation.access == ToolAccessMode::Write && operation.path.is_none()
            })
            .map(|operation| format!("write operation {} has no path", operation.tool_name))
            .collect();

        let mut read_batch = Vec::new();
        let mut batches = Vec::new();
        for operation in operations {
            match operation.access {
                ToolAccessMode::Read => read_batch.push(operation),
                ToolAccessMode::Write => {
                    if !read_batch.is_empty() {
                        batches.push(std::mem::take(&mut read_batch));
                    }
                    batches.push(vec![operation]);
                }
            }
        }
        if !read_batch.is_empty() {
            batches.push(read_batch);
        }

        Ok(ToolTransactionPlan {
            id: format!("tool-tx-{}", uuid::Uuid::new_v4()),
            batches,
            requires_checkpoint,
            requires_human_confirm,
            warnings,
        })
    }
}

impl ToolTransactionPlan {
    #[must_use]
    pub fn receipt(
        &self,
        completed_operations: usize,
        failed_operations: usize,
        checkpoint_created: bool,
    ) -> ToolTransactionReceipt {
        ToolTransactionReceipt {
            transaction_id: self.id.clone(),
            completed_operations,
            failed_operations,
            checkpoint_created,
        }
    }
}

fn detect_write_conflicts(operations: &[ToolOperation]) -> AiKernelResult<()> {
    let mut seen = BTreeSet::new();
    for operation in operations
        .iter()
        .filter(|operation| operation.access == ToolAccessMode::Write)
    {
        let Some(path) = &operation.path else {
            continue;
        };
        if !seen.insert(path.clone()) {
            return Err(AiKernelError::Conflict(format!(
                "multiple write operations target {path}"
            )));
        }
    }
    Ok(())
}

fn normalize_path(path: String) -> String {
    path.trim().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{PermissionOperation, PermissionResource};

    #[test]
    fn read_operations_share_a_parallel_batch() {
        let plan = ToolTransactionPlanner
            .plan(vec![
                ToolOperation::read("read_file", Some("a.rs".to_string())),
                ToolOperation::read("grep", None),
            ])
            .unwrap();

        assert_eq!(plan.batches.len(), 1);
        assert_eq!(plan.batches[0].len(), 2);
        assert!(!plan.requires_checkpoint);
    }

    #[test]
    fn writes_are_serialized_and_checkpointed() {
        let plan = ToolTransactionPlanner
            .plan(vec![
                ToolOperation::read("read_file", Some("a.rs".to_string())),
                ToolOperation::write("apply_patch", ToolRisk::High, Some("a.rs".to_string())),
                ToolOperation::write("apply_patch", ToolRisk::Medium, Some("b.rs".to_string())),
            ])
            .unwrap();

        assert_eq!(plan.batches.len(), 3);
        assert!(plan.requires_checkpoint);
    }

    #[test]
    fn same_path_write_conflict_is_rejected() {
        let error = ToolTransactionPlanner
            .plan(vec![
                ToolOperation::write("apply_patch", ToolRisk::Medium, Some("a.rs".to_string())),
                ToolOperation::write("write_file", ToolRisk::Medium, Some("a.rs".to_string())),
            ])
            .unwrap_err();

        assert_eq!(error.kind(), "conflict");
    }

    #[test]
    fn critical_operation_requires_human_confirm() {
        let plan = ToolTransactionPlanner
            .plan(vec![ToolOperation::write(
                "danger",
                ToolRisk::Critical,
                Some("db.sqlite".to_string()),
            )])
            .unwrap();

        assert!(plan.requires_human_confirm);
    }

    #[test]
    fn receipt_reports_observed_checkpoint_not_planned_checkpoint() {
        let plan = ToolTransactionPlanner
            .plan(vec![ToolOperation::write(
                "write_file",
                ToolRisk::High,
                Some("a.rs".to_string()),
            )])
            .unwrap();

        assert!(plan.requires_checkpoint);
        assert!(!plan.receipt(0, 1, false).checkpoint_created);
        assert!(plan.receipt(1, 0, true).checkpoint_created);
    }

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
