use serde::{Deserialize, Serialize};

use crate::contract::ApprovalPersistence;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApprovalCacheKey {
    pub scope: String,
    pub target: Option<String>,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalCacheDecision {
    pub approved: bool,
    pub persistence: ApprovalPersistence,
    pub reason: Option<String>,
}
