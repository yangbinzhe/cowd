use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::execution_core::tool_dag::ToolDagPlan;
use crate::execution_scheduler::{ExecutionBatchMode, ToolSchedule};
use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionRequest {
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: String,
    pub category: ToolSafetyCategory,
}

impl RuntimeToolExecutionRequest {
    #[must_use]
    pub fn from_tool_request(request: &ToolRequest) -> Self {
        Self {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            category: ToolSafetyCategory::from_tool_name(&request.tool_name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeToolExecutionStatus {
    Executed,
    BlockedPermission,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionOutcome {
    pub tool_use_id: String,
    pub tool_name: String,
    pub status: RuntimeToolExecutionStatus,
    pub category: ToolSafetyCategory,
    pub output: Option<String>,
    pub error: Option<String>,
    pub evidence_ref: String,
}

pub trait RuntimeToolExecutionHost {
    fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> RuntimeToolExecutionOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActionExecutionReceipt {
    pub action: String,
    pub status: String,
    pub execution_id: String,
    pub schedule: ToolSchedule,
    pub tool_results: Vec<RuntimeToolExecutionOutcome>,
    pub evidence_refs: Vec<String>,
    pub events: Vec<Value>,
    pub context_injection: Vec<Value>,
    pub next_model_guidance: String,
}

impl RuntimeActionExecutionReceipt {
    #[must_use]
    pub fn blocked_missing_executor(action: &str, dag: &ToolDagPlan) -> Self {
        Self {
            action: action.to_string(),
            status: "blocked_missing_executor".to_string(),
            execution_id: format!("runtime-action-{}", uuid::Uuid::new_v4()),
            schedule: dag.safety_summary.schedule.clone(),
            tool_results: Vec::new(),
            evidence_refs: Vec::new(),
            events: vec![json!({
                "kind": "runtime.tool_dag.blocked",
                "status": "blocked_missing_executor",
                "reason": "runtime action requires an attached RuntimeToolExecutionHost"
            })],
            context_injection: vec![json!({
                "type": "runtime_action_guidance",
                "status": "blocked_missing_executor",
                "guidance": "Attach a runtime tool execution host or fall back to model-native tool calls; do not claim the DAG executed."
            })],
            next_model_guidance:
                "A RuntimeToolExecutionHost is not attached, so this action did not execute. Use model-native tools or retry through a gateway/conversation runtime that can inject a host."
                    .to_string(),
        }
    }
}

#[must_use]
pub fn execute_tool_dag_with_host(
    action: &str,
    dag: &ToolDagPlan,
    host: &dyn RuntimeToolExecutionHost,
) -> RuntimeActionExecutionReceipt {
    let requests = dag.to_tool_requests();
    let mut outcomes = Vec::new();
    let mut events = Vec::new();
    let mut evidence_refs = Vec::new();

    for batch in &dag.safety_summary.schedule.batches {
        let executable = matches!(
            batch.mode,
            ExecutionBatchMode::ParallelRead | ExecutionBatchMode::Wave
        );
        for index in &batch.indices {
            let Some(request) = requests.get(*index) else {
                continue;
            };
            let runtime_request = RuntimeToolExecutionRequest::from_tool_request(request);
            if !executable || runtime_request.category != ToolSafetyCategory::ReadOnly {
                let evidence_ref = format!("runtime-tool:{}:blocked", runtime_request.tool_use_id);
                outcomes.push(RuntimeToolExecutionOutcome {
                    tool_use_id: runtime_request.tool_use_id,
                    tool_name: runtime_request.tool_name,
                    status: RuntimeToolExecutionStatus::BlockedPermission,
                    category: runtime_request.category,
                    output: None,
                    error: Some(format!(
                        "tool category {:?} requires explicit permission gate",
                        runtime_request.category
                    )),
                    evidence_ref: evidence_ref.clone(),
                });
                evidence_refs.push(evidence_ref);
                continue;
            }
            let outcome = host.execute_runtime_tool(&runtime_request);
            evidence_refs.push(outcome.evidence_ref.clone());
            outcomes.push(outcome);
        }
    }

    let executed_count = outcomes
        .iter()
        .filter(|outcome| outcome.status == RuntimeToolExecutionStatus::Executed)
        .count();
    let blocked_count = outcomes
        .iter()
        .filter(|outcome| outcome.status == RuntimeToolExecutionStatus::BlockedPermission)
        .count();
    let failed_count = outcomes
        .iter()
        .filter(|outcome| outcome.status == RuntimeToolExecutionStatus::Failed)
        .count();
    let status = if failed_count > 0 {
        "failed"
    } else if executed_count > 0 && blocked_count > 0 {
        "degraded_permission_blocked"
    } else if executed_count > 0 {
        "executed"
    } else if blocked_count > 0 {
        "blocked_permission"
    } else {
        "degraded_empty_dag"
    };

    events.push(json!({
        "kind": "runtime.tool_dag.executed",
        "status": status,
        "action": action,
        "dag_id": dag.dag_id,
        "executed_count": executed_count,
        "blocked_count": blocked_count,
        "failed_count": failed_count,
    }));

    RuntimeActionExecutionReceipt {
        action: action.to_string(),
        status: status.to_string(),
        execution_id: format!("runtime-action-{}", uuid::Uuid::new_v4()),
        schedule: dag.safety_summary.schedule.clone(),
        tool_results: outcomes,
        evidence_refs,
        events,
        context_injection: vec![json!({
            "type": "runtime_tool_dag_observation",
            "status": status,
            "dag_id": dag.dag_id,
            "guidance": "Use executed tool outputs and evidence refs before requesting more tools."
        })],
        next_model_guidance:
            "Use the executed tool outputs and evidence refs; avoid repeating overlapping reads."
                .to_string(),
    }
}
