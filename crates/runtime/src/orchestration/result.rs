use harness_contract::core::ExecutionMode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationDecision {
    pub selected_mode: ExecutionMode,
    pub selected_template: Option<String>,
    pub reason: String,
    pub policy_gates: Vec<String>,
    pub budget: Value,
    pub permission: Value,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationResult {
    pub request_id: String,
    pub status: String,
    pub decision: RuntimeOrchestrationDecision,
    pub execution: Value,
    pub evidence: Value,
    pub next_model_guidance: String,
}
