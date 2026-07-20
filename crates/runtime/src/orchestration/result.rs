use harness_contract::core::{ExecutionPattern, ExecutionPolicyGate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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
    pub decision: RuntimeOrchestrationDecision,
    pub execution: Value,
    pub evidence: Value,
    pub next_model_guidance: String,
}

impl RuntimeOrchestrationResult {
    /// Return the bounded receipt that an invoking model needs to continue a
    /// turn. The complete compilation/run projection remains the durable raw
    /// tool result; feeding that recursive graph payload into the next model
    /// request makes a successful team unnecessarily expensive and can stall
    /// the parent turn behind provider admission limits.
    #[must_use]
    pub fn model_receipt(&self) -> Value {
        let execution = &self.execution;
        let terminal_result_ref = execution
            .get("terminal_result_ref")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let terminal_summary = terminal_result_ref
            .as_deref()
            .and_then(decode_terminal_summary)
            .map(|value| truncate_chars(&value, 12_000));
        let terminal_result_kind = terminal_result_ref.as_deref().map_or("none", |reference| {
            if reference.starts_with("assistant_json:") {
                "assistant_json"
            } else {
                "opaque_reference"
            }
        });
        let report = execution.get("report").map(|report| {
            json!({
                "completed": report.get("completed"),
                "failed": report.get("failed"),
                "blocked": report.get("blocked"),
                "waiting": report.get("waiting"),
            })
        });
        json!({
            "schema_version": 1,
            "receipt_id": format!("runtime-orchestration-receipt:{}", self.request_id),
            "request_id": self.request_id,
            "status": self.status,
            "decision": {
                "selected_pattern": self.decision.selected_pattern,
                "selected_template": self.decision.selected_template,
                "reason": self.decision.reason,
                "status": self.decision.status,
                "validation_findings": self.decision.validation_findings,
            },
            "execution": {
                "type": execution.get("type"),
                "status": execution.get("status"),
                "graph_id": self.evidence.get("graph_id"),
                "report": report,
                "terminal_result_available": terminal_result_ref.is_some(),
                "terminal_result_kind": terminal_result_kind,
                "focus_overlap_assessment": execution.get("focus_overlap_assessment"),
            },
            "team_id": self.evidence.get("team_id"),
            "working_state_verified": self.evidence.get("working_state_verified"),
            "focus_overlap_verified": self.evidence.get("focus_overlap_verified"),
            "focus_overlap_exceeded": self.evidence.get("focus_overlap_exceeded"),
            "evidence": {
                "action": self.evidence.get("action"),
                "compiled": self.evidence.get("compiled"),
                "strategy_lease_id": self
                    .evidence
                    .pointer("/strategy_lease/lease_id"),
                "accepted": self.evidence.get("accepted"),
                "executed": self.evidence.get("executed"),
                "reused": self.evidence.get("reused"),
                "graph_id": self.evidence.get("graph_id"),
                "team_id": self.evidence.get("team_id"),
                "working_state_verified": self.evidence.get("working_state_verified"),
                "focus_overlap_verified": self.evidence.get("focus_overlap_verified"),
                "focus_overlap_exceeded": self.evidence.get("focus_overlap_exceeded"),
            },
            "terminal_summary": terminal_summary,
            "next_model_guidance": self.next_model_guidance,
        })
    }
}

fn decode_terminal_summary(reference: &str) -> Option<String> {
    let encoded = reference.strip_prefix("assistant_json:")?;
    serde_json::from_str::<String>(encoded).ok()
}

fn truncate_chars(value: &str, limit: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= limit {
        return value.to_string();
    }
    let kept = chars.into_iter().take(limit).collect::<String>();
    format!(
        "{kept}\n...[terminal synthesis truncated; retrieve durable execution evidence for full detail]"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::core::ExecutionPattern;

    fn result(execution: Value) -> RuntimeOrchestrationResult {
        RuntimeOrchestrationResult {
            request_id: "runtime-orch-1".to_string(),
            status: "completed".to_string(),
            decision: RuntimeOrchestrationDecision {
                selected_pattern: ExecutionPattern::Collaborate,
                selected_template: Some("research_synthesis".to_string()),
                reason: "independent evidence lanes".to_string(),
                policy_gates: Vec::new(),
                validation_findings: Vec::new(),
                required_approval: None,
                budget: Value::Null,
                permission: Value::Null,
                status: "completed".to_string(),
            },
            execution,
            evidence: json!({"accepted": true, "executed": true, "graph_id": "graph-1"}),
            next_model_guidance: "synthesize from the terminal result".to_string(),
        }
    }

    #[test]
    fn model_receipt_omits_recursive_projection_and_keeps_terminal_summary() {
        let terminal = serde_json::to_string(&"checked conclusion".repeat(4_000)).unwrap();
        let receipt = result(json!({
            "type": "execution_graph_run",
            "status": "completed",
            "terminal_result_ref": format!("assistant_json:{terminal}"),
            "report": {"completed": 8, "failed": 0, "blocked": 0, "waiting": 0},
            "projection": {"very_large": "x".repeat(200_000)},
        }))
        .model_receipt();

        assert_eq!(receipt["status"], "completed");
        assert_eq!(receipt["execution"]["type"], "execution_graph_run");
        assert!(receipt["execution"].get("projection").is_none());
        assert!(receipt["execution"].get("terminal_result_ref").is_none());
        assert!(receipt["terminal_summary"]
            .as_str()
            .is_some_and(|value| value.contains("terminal synthesis truncated")));
        assert!(serde_json::to_string(&receipt).unwrap().len() < 20_000);
    }
}
