use harness_contract::core::{ExecutionPattern, ExecutionPolicyGate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStateSnapshot {
    pub snapshot_generation: u64,
    pub target_execution_id: Option<String>,
    pub graph: Option<harness_contract::execution_graph::ExecutionGraphProjection>,
    pub child_graphs: Vec<harness_contract::execution_graph::ExecutionGraphProjection>,
    pub capability_recipes: Vec<String>,
    pub team_templates: Vec<String>,
    pub permission_ceiling: harness_contract::policy::PermissionMode,
    pub pending_approvals: usize,
    pub execution_health: Value,
    pub team_board_revisions: BTreeMap<String, u64>,
    pub unresolved_conflicts: Vec<String>,
    pub artifact_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationApprovalRequirement {
    pub action: String,
    pub session_id: Option<String>,
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryHint {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationDecision {
    pub selected_pattern: ExecutionPattern,
    pub selected_template: Option<String>,
    pub reason: String,
    pub policy_gates: Vec<ExecutionPolicyGate>,
    pub validation_findings: Vec<String>,
    /// Successful policy adjustments applied during compilation (P14-F4).
    /// Kept separate from `validation_findings` so a rejected decision never
    /// mixes "repaired" notes into the failure reasons the model must read.
    #[serde(default)]
    pub adjustments: Vec<String>,
    #[serde(default)]
    pub required_approval: Option<RuntimeOrchestrationApprovalRequirement>,
    #[serde(default)]
    pub recovery_hints: Vec<RecoveryHint>,
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
        let runtime_snapshot = (self.status == "inspected").then(|| {
            json!({
                "snapshot_generation": execution.get("snapshot_generation"),
                "target_execution_id": execution.get("target_execution_id"),
                "graph": execution.get("graph"),
                "child_graphs": execution.get("child_graphs"),
                "capability_recipes": execution.get("capability_recipes"),
                "team_templates": execution.get("team_templates"),
                "permission_ceiling": execution.get("permission_ceiling"),
                "pending_approvals": execution.get("pending_approvals"),
                "execution_health": execution.get("execution_health"),
                "team_board_revisions": execution.get("team_board_revisions"),
                "unresolved_conflicts": execution.get("unresolved_conflicts"),
                "artifact_refs": execution.get("artifact_refs"),
            })
        });
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
        let delivery_envelope = execution.pointer("/projection/delivery_envelope").cloned();
        let terminal_presentation = execution
            .pointer("/projection/terminal_presentation")
            .cloned();
        let team_terminals = execution.get("team_terminals").cloned();
        let collaboration_program = execution.get("collaboration_program").cloned();
        let collaboration_diagnostics = execution.get("collaboration_diagnostics").cloned();
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
                "adjustments": self.decision.adjustments,
                "required_approval": self.decision.required_approval,
                "recovery_hints": self.decision.recovery_hints,
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
            "team_ids": self.evidence.get("team_ids"),
            "runtime_snapshot": runtime_snapshot,
            "working_state_verified": self.evidence.get("working_state_verified"),
            "focus_overlap_verified": self.evidence.get("focus_overlap_verified"),
            "focus_overlap_exceeded": self.evidence.get("focus_overlap_exceeded"),
            "committed_write": self.evidence.get("committed_write"),
            "committed_write_paths": self.evidence.get("committed_write_paths"),
            "write_attempt_paths": self.evidence.get("write_attempt_paths"),
            "child_usage": self.evidence.get("child_usage"),
            "evidence": {
                "operation": self.evidence.get("operation"),
                "compiled": self.evidence.get("compiled"),
                "strategy_lease_id": self.evidence.get("strategy_lease_id"),
                "accepted": self.evidence.get("accepted"),
                "executed": self.evidence.get("executed"),
                "reused": self.evidence.get("reused"),
                "graph_id": self.evidence.get("graph_id"),
                "team_id": self.evidence.get("team_id"),
                "team_ids": self.evidence.get("team_ids"),
                "working_state_verified": self.evidence.get("working_state_verified"),
                "focus_overlap_verified": self.evidence.get("focus_overlap_verified"),
                "focus_overlap_exceeded": self.evidence.get("focus_overlap_exceeded"),
                "committed_write": self.evidence.get("committed_write"),
                "committed_write_paths": self.evidence.get("committed_write_paths"),
                "write_attempt_paths": self.evidence.get("write_attempt_paths"),
                "child_usage": self.evidence.get("child_usage"),
            },
            "terminal_summary": terminal_summary,
            "delivery_envelope": delivery_envelope,
            "terminal_presentation": terminal_presentation,
            "team_terminals": team_terminals,
            "collaboration_program": collaboration_program,
            "collaboration_diagnostics": collaboration_diagnostics,
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
                adjustments: Vec::new(),
                required_approval: None,
                recovery_hints: Vec::new(),
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
        let mut outcome = result(json!({
            "type": "execution_graph_run",
            "status": "completed",
            "terminal_result_ref": format!("assistant_json:{terminal}"),
            "report": {"completed": 8, "failed": 0, "blocked": 0, "waiting": 0},
            "projection": {"very_large": "x".repeat(200_000)},
        }));
        outcome.evidence = json!({
            "accepted": true,
            "executed": true,
            "graph_id": "graph-1",
            "committed_write": true,
            "committed_write_paths": ["reports/final.html"],
            "write_attempt_paths": ["reports/final.html"],
            "child_usage": {
                "input_tokens": 120,
                "output_tokens": 30,
                "cached_tokens": 10,
                "tool_calls": 4,
                "duplicate_tool_calls": 0
            }
        });
        let receipt = outcome.model_receipt();

        assert_eq!(receipt["status"], "completed");
        assert_eq!(receipt["execution"]["type"], "execution_graph_run");
        assert!(receipt["execution"].get("projection").is_none());
        assert!(receipt["execution"].get("terminal_result_ref").is_none());
        assert!(receipt["terminal_summary"]
            .as_str()
            .is_some_and(|value| value.contains("terminal synthesis truncated")));
        assert_eq!(receipt["committed_write"], true);
        assert_eq!(receipt["committed_write_paths"][0], "reports/final.html");
        assert_eq!(receipt["child_usage"]["tool_calls"], 4);
        assert!(serde_json::to_string(&receipt).unwrap().len() < 20_000);
    }

    #[test]
    fn model_receipt_preserves_bounded_typed_team_terminals() {
        let outcome = result(json!({
            "type": "execution_graph_run",
            "status": "completed",
            "terminal_result_ref": "assistant_json:\"checked\"",
            "team_terminals": [{
                "team_id": "team-a",
                "terminal_summary": "checked",
                "delivery_envelope": {"envelope_id": "envelope-a"},
                "terminal_presentation": {"presentation_id": "presentation-a"}
            }]
        }));

        let receipt = outcome.model_receipt();

        assert_eq!(receipt["team_terminals"][0]["team_id"], "team-a");
        assert!(receipt["execution"].get("team_terminals").is_none());
    }

    #[test]
    fn model_receipt_preserves_program_owned_failure_diagnostic() {
        let outcome = result(json!({
            "type": "execution_graph_run",
            "status": "failed",
            "collaboration_program": {
                "program_id": "program-a",
                "lifecycle": "failed",
                "completed_required_instance_ids": []
            },
            "collaboration_diagnostics": [{
                "code": "team_execution_not_completed",
                "program_id": "program-a",
                "team_instance_id": "audit:1",
                "semantic_node_id": "audit",
                "execution_node_id": "graph-audit",
                "node_status": "failed",
                "failure_kind": "provider_timeout",
                "failure_message": "provider deadline elapsed",
                "retryable": true,
                "evidence_refs": [],
                "next_action": "inspect_collaboration_terminal_diagnostic"
            }]
        }));

        let receipt = outcome.model_receipt();

        assert_eq!(receipt["collaboration_program"]["lifecycle"], "failed");
        assert_eq!(
            receipt["collaboration_diagnostics"][0]["failure_kind"],
            "provider_timeout"
        );
        assert!(receipt["execution"]
            .get("collaboration_diagnostics")
            .is_none());
    }
}
