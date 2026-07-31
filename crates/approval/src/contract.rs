use serde::{Deserialize, Serialize};

pub use harness_contract::policy::ApprovalPersistence;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: String,
    pub scope: String,
    pub summary: String,
    pub risk_level: String,
    pub requested_by: String,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub request_id: String,
    pub approved: bool,
    pub persistence: ApprovalPersistence,
    pub reason: Option<String>,
    pub decided_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalReceipt {
    pub request: ApprovalRequest,
    pub decision: ApprovalDecision,
    pub trace_id: Option<String>,
}
