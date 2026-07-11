use harness_contract::core::{ExecutionPattern, ExecutionPolicyGate};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::execution_core::ProtocolRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationApprovalRequirement {
    pub action: String,
    pub session_id: Option<String>,
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationDecision {
    pub selected_pattern: ExecutionPattern,
    pub selected_template: Option<String>,
    #[serde(default)]
    pub selected_protocol: Option<ProtocolRef>,
    pub reason: String,
    pub policy_gates: Vec<ExecutionPolicyGate>,
    pub validation_findings: Vec<String>,
    #[serde(default)]
    pub required_approval: Option<RuntimeOrchestrationApprovalRequirement>,
    pub budget: Value,
    pub permission: Value,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationResult {
    pub request_id: String,
    pub status: String,
    #[serde(default)]
    pub protocol: Option<ProtocolRef>,
    pub decision: RuntimeOrchestrationDecision,
    pub execution: Value,
    pub evidence: Value,
    pub next_model_guidance: String,
}
