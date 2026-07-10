use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::execution_core::tool_dag::ToolDagPlan;
use crate::execution_scheduler::{ExecutionBatchMode, ToolSchedule};
use crate::orchestration::request::RuntimeOrchestrationRequest;
use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;
use crate::CollaborationDecision;

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

pub trait RuntimeExecutionHost: Sync {
    fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> RuntimeToolExecutionOutcome;

    fn start_runtime_team(
        &self,
        _request: &RuntimeOrchestrationRequest,
        _decision: &CollaborationDecision,
    ) -> Option<Result<Value, String>> {
        None
    }
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
                "reason": "runtime action requires an attached RuntimeExecutionHost"
            })],
            context_injection: vec![json!({
                "type": "runtime_action_guidance",
                "status": "blocked_missing_executor",
                "guidance": "Attach a runtime tool execution host or fall back to model-native tool calls; do not claim the DAG executed."
            })],
            next_model_guidance:
                "A RuntimeExecutionHost is not attached, so this action did not execute. Use model-native tools or retry through a gateway/conversation runtime that can inject a host."
                    .to_string(),
        }
    }
}

#[must_use]
pub fn execute_tool_dag_with_host(
    action: &str,
    dag: &ToolDagPlan,
    host: &dyn RuntimeExecutionHost,
) -> RuntimeActionExecutionReceipt {
    let requests = dag.to_tool_requests();
    let runtime_requests = requests
        .iter()
        .map(RuntimeToolExecutionRequest::from_tool_request)
        .collect::<Vec<_>>();
    let mut outcomes = vec![None; requests.len()];
    let mut successful_ids = HashSet::new();
    let mut remaining = requests.len();
    let mut events = Vec::new();

    while remaining > 0 {
        let mut made_progress = false;

        for batch in &dag.safety_summary.schedule.batches {
            let ready = batch
                .indices
                .iter()
                .copied()
                .filter(|index| {
                    outcomes.get(*index).is_some_and(Option::is_none)
                        && requests.get(*index).is_some_and(|request| {
                            request
                                .depends_on
                                .iter()
                                .all(|dependency| successful_ids.contains(dependency))
                        })
                })
                .collect::<Vec<_>>();
            if ready.is_empty() {
                continue;
            }

            let parallel_safe = matches!(
                batch.mode,
                ExecutionBatchMode::ParallelRead | ExecutionBatchMode::LimitedNetwork
            ) && batch.max_concurrency > 1
                && ready.len() > 1
                && ready.iter().all(|index| {
                    requests[*index].depends_on.is_empty()
                        && is_parallel_safe_category(runtime_requests[*index].category)
                });

            let batch_outcomes = if parallel_safe {
                execute_parallel_batch(host, &runtime_requests, &ready, batch.max_concurrency)
            } else {
                ready
                    .iter()
                    .map(|index| (*index, execute_or_block(host, &runtime_requests[*index])))
                    .collect()
            };

            for (index, outcome) in batch_outcomes {
                if outcomes[index].is_some() {
                    continue;
                }
                if outcome.status == RuntimeToolExecutionStatus::Executed {
                    successful_ids.insert(requests[index].tool_use_id.clone());
                }
                outcomes[index] = Some(outcome);
                remaining -= 1;
                made_progress = true;
            }
        }

        if !made_progress {
            break;
        }
    }

    for (index, outcome) in outcomes.iter_mut().enumerate() {
        if outcome.is_none() {
            *outcome = Some(unresolved_dependency_outcome(
                &runtime_requests[index],
                &requests[index].depends_on,
            ));
        }
    }

    let outcomes = outcomes.into_iter().flatten().collect::<Vec<_>>();
    let evidence_refs = outcomes
        .iter()
        .map(|outcome| outcome.evidence_ref.clone())
        .collect::<Vec<_>>();

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

fn is_parallel_safe_category(category: ToolSafetyCategory) -> bool {
    matches!(
        category,
        ToolSafetyCategory::ReadOnly | ToolSafetyCategory::Network
    )
}

fn execute_or_block(
    host: &dyn RuntimeExecutionHost,
    request: &RuntimeToolExecutionRequest,
) -> RuntimeToolExecutionOutcome {
    if is_parallel_safe_category(request.category) {
        host.execute_runtime_tool(request)
    } else {
        let evidence_ref = format!("runtime-tool:{}:blocked", request.tool_use_id);
        RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: RuntimeToolExecutionStatus::BlockedPermission,
            category: request.category,
            output: None,
            error: Some(format!(
                "tool category {:?} requires explicit permission gate",
                request.category
            )),
            evidence_ref,
        }
    }
}

fn execute_parallel_batch(
    host: &dyn RuntimeExecutionHost,
    requests: &[RuntimeToolExecutionRequest],
    indices: &[usize],
    max_concurrency: usize,
) -> Vec<(usize, RuntimeToolExecutionOutcome)> {
    let concurrency = max_concurrency.max(1).min(indices.len());
    let mut outcomes = Vec::with_capacity(indices.len());

    for chunk in indices.chunks(concurrency) {
        let mut chunk_outcomes = std::thread::scope(|scope| {
            let handles = chunk
                .iter()
                .map(|index| {
                    let index = *index;
                    let request = &requests[index];
                    (
                        index,
                        request,
                        scope.spawn(move || host.execute_runtime_tool(request)),
                    )
                })
                .collect::<Vec<_>>();

            handles
                .into_iter()
                .map(|(index, request, handle)| {
                    let outcome = handle
                        .join()
                        .unwrap_or_else(|_| runtime_host_panic_outcome(request));
                    (index, outcome)
                })
                .collect::<Vec<_>>()
        });
        outcomes.append(&mut chunk_outcomes);
    }

    outcomes
}

fn runtime_host_panic_outcome(
    request: &RuntimeToolExecutionRequest,
) -> RuntimeToolExecutionOutcome {
    RuntimeToolExecutionOutcome {
        tool_use_id: request.tool_use_id.clone(),
        tool_name: request.tool_name.clone(),
        status: RuntimeToolExecutionStatus::Failed,
        category: request.category,
        output: None,
        error: Some("runtime execution host panicked".to_string()),
        evidence_ref: format!("runtime-tool:{}:failed", request.tool_use_id),
    }
}

fn unresolved_dependency_outcome(
    request: &RuntimeToolExecutionRequest,
    dependencies: &[String],
) -> RuntimeToolExecutionOutcome {
    RuntimeToolExecutionOutcome {
        tool_use_id: request.tool_use_id.clone(),
        tool_name: request.tool_name.clone(),
        status: RuntimeToolExecutionStatus::Failed,
        category: request.category,
        output: None,
        error: Some(format!(
            "tool could not be scheduled because dependencies were unresolved: {}",
            dependencies.join(", ")
        )),
        evidence_ref: format!("runtime-tool:{}:failed", request.tool_use_id),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::execution_core::tool_dag::{ToolDagEdge, ToolDagEdgeKind, ToolDagTask};

    #[derive(Default)]
    struct TrackingHost {
        active_reads: AtomicUsize,
        peak_reads: AtomicUsize,
        active_network: AtomicUsize,
        peak_network: AtomicUsize,
        calls: Mutex<Vec<String>>,
    }

    impl RuntimeExecutionHost for TrackingHost {
        fn execute_runtime_tool(
            &self,
            request: &RuntimeToolExecutionRequest,
        ) -> RuntimeToolExecutionOutcome {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request.tool_use_id.clone());

            if request.tool_use_id.contains("fail") {
                return RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: RuntimeToolExecutionStatus::Failed,
                    category: request.category,
                    output: None,
                    error: Some("synthetic failure".to_string()),
                    evidence_ref: format!("test:tool:{}:failed", request.tool_use_id),
                };
            }

            let (active, peak) = match request.category {
                ToolSafetyCategory::ReadOnly => (&self.active_reads, &self.peak_reads),
                ToolSafetyCategory::Network => (&self.active_network, &self.peak_network),
                ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Destructive => {
                    panic!("permission-gated tools must not reach the host")
                }
            };
            let active_now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(active_now, Ordering::SeqCst);
            let delay_ms = if request.tool_use_id.contains("slow") {
                60
            } else {
                15
            };
            std::thread::sleep(Duration::from_millis(delay_ms));
            active.fetch_sub(1, Ordering::SeqCst);

            RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(format!("output:{}", request.tool_use_id)),
                error: None,
                evidence_ref: format!("test:tool:{}", request.tool_use_id),
            }
        }
    }

    #[test]
    fn parallel_read_and_network_batches_merge_in_original_order() {
        let dag = ToolDagPlan::new(
            vec![
                task("read-slow", "read_file"),
                task("network-slow", "web_search"),
                task("read-fast", "grep_search"),
                task("network-fast", "web_fetch"),
            ],
            Vec::new(),
        );
        let host = TrackingHost::default();

        let receipt = execute_tool_dag_with_host("parallel evidence", &dag, &host);

        assert!(host.peak_reads.load(Ordering::SeqCst) > 1);
        assert!(host.peak_network.load(Ordering::SeqCst) > 1);
        assert_eq!(
            receipt
                .tool_results
                .iter()
                .map(|outcome| outcome.tool_use_id.as_str())
                .collect::<Vec<_>>(),
            vec!["read-slow", "network-slow", "read-fast", "network-fast"]
        );
        assert!(receipt
            .tool_results
            .iter()
            .all(|outcome| outcome.status == RuntimeToolExecutionStatus::Executed));
    }

    #[test]
    fn dependency_tasks_are_serial_and_permission_gated_tasks_never_execute() {
        let dag = ToolDagPlan::new(
            vec![
                task("child", "read_file"),
                task("write", "write_file"),
                task("root", "read_file"),
                task("destroy", "bash"),
                task("grandchild", "web_search"),
            ],
            vec![edge("root", "child"), edge("child", "grandchild")],
        );
        let host = TrackingHost::default();

        let receipt = execute_tool_dag_with_host("dependency evidence", &dag, &host);

        let calls = host
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(calls, vec!["root", "child", "grandchild"]);
        assert_eq!(host.peak_reads.load(Ordering::SeqCst), 1);
        assert_eq!(host.peak_network.load(Ordering::SeqCst), 1);
        assert_eq!(
            receipt
                .tool_results
                .iter()
                .map(|outcome| outcome.tool_use_id.as_str())
                .collect::<Vec<_>>(),
            vec!["child", "write", "root", "destroy", "grandchild"]
        );
        assert_eq!(
            receipt.tool_results[1].status,
            RuntimeToolExecutionStatus::BlockedPermission
        );
        assert_eq!(
            receipt.tool_results[3].status,
            RuntimeToolExecutionStatus::BlockedPermission
        );
    }

    #[test]
    fn failed_dependency_blocks_descendants() {
        let dag = ToolDagPlan::new(
            vec![task("root-fail", "read_file"), task("child", "read_file")],
            vec![edge("root-fail", "child")],
        );
        let host = TrackingHost::default();

        let receipt = execute_tool_dag_with_host("dependency failure", &dag, &host);
        let calls = host
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        assert_eq!(calls, vec!["root-fail"]);
        assert_eq!(
            receipt.tool_results[0].status,
            RuntimeToolExecutionStatus::Failed
        );
        assert_eq!(
            receipt.tool_results[1].status,
            RuntimeToolExecutionStatus::Failed
        );
        assert!(receipt.tool_results[1]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("dependencies were unresolved")));
    }

    fn task(id: &str, tool_name: &str) -> ToolDagTask {
        ToolDagTask {
            id: id.to_string(),
            tool_name: tool_name.to_string(),
            input: json!({"id": id}),
            purpose: format!("run {id}"),
            expected_output: format!("output:{id}"),
        }
    }

    fn edge(from: &str, to: &str) -> ToolDagEdge {
        ToolDagEdge {
            from: from.to_string(),
            to: to.to_string(),
            kind: ToolDagEdgeKind::DataDependency,
        }
    }
}
