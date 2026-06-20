use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use ai_policy::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionResource {
    File,
    Shell,
    Network,
    Provider,
    Connector,
    Channel,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
    Prompt,
    Allow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyReceipt {
    pub decision: PolicyDecision,
    pub trace_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskGateReceipt {
    pub scope: PermissionScope,
    pub risk: RiskAssessment,
    pub decision: PolicyDecisionKind,
    pub approval_required: bool,
    pub issued_at: DateTime<Utc>,
}
