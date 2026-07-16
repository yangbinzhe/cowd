//! Conservative execution scheduler for model-requested tool calls.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use memory::{SessionDomainEvent, SessionDomainRef, SessionDomainScope};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::execution_core::RuntimeExecutionDecision;
use crate::tool_dispatch::ToolRequest;
use crate::tool_execution_plan::{ToolExecutionMode, ToolExecutionPlan};
use crate::tool_orchestrator::ToolSafetyCategory;

/// Bounded fan-out for idempotent code-reading/research calls.  A single
/// model response may legitimately contain dozens of independent reads, but
/// an unbounded sentinel leaked the batch size as a concurrency policy and could turn a
/// large prompt into unbounded process, file-descriptor, or provider load.
/// Runtime still preserves all eligible parallelism up to this explicit cap.
pub const MAX_PARALLEL_READ_CONCURRENCY: usize = 32;
/// Default per-turn fan-out. The maximum is kept as a separately named
/// contract so a future Runtime configuration can raise it without changing
/// scheduler semantics or allowing unbounded task creation.
pub const DEFAULT_PARALLEL_READ_CONCURRENCY: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBatchMode {
    ParallelRead,
    LimitedWrite,
    LimitedNetwork,
    SerialDestructive,
    Wave,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBatch {
    pub mode: ExecutionBatchMode,
    pub indices: Vec<usize>,
    pub max_concurrency: usize,
    pub reason: String,
    #[serde(default)]
    pub scope_groups: Vec<ExecutionScopeGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionScopeGroup {
    pub scope: String,
    pub indices: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSchedule {
    pub batches: Vec<ExecutionBatch>,
}

impl ToolSchedule {
    #[must_use]
    pub fn parallel_read_indices(&self) -> Vec<usize> {
        self.batches
            .iter()
            .filter(|batch| batch.mode == ExecutionBatchMode::ParallelRead)
            .flat_map(|batch| batch.indices.iter().copied())
            .collect()
    }

    #[must_use]
    pub fn remaining_indices(&self) -> Vec<usize> {
        self.batches
            .iter()
            .filter(|batch| batch.mode != ExecutionBatchMode::ParallelRead)
            .flat_map(|batch| batch.indices.iter().copied())
            .collect()
    }

    #[must_use]
    pub fn to_runtime_event(
        &self,
        session_id: impl Into<String>,
        sequence: usize,
        created_at_ms: u64,
        requests: &[ToolRequest],
    ) -> SessionDomainEvent {
        let payload = serde_json::json!({
            "batch_count": self.batches.len(),
            "batches": self.batches,
            "tool_count": requests.len(),
        });
        let mut event = SessionDomainEvent::new(
            session_id,
            sequence,
            SessionDomainScope::Tool,
            "tool.schedule.created",
            payload,
            created_at_ms,
        );
        event.status = Some("planned".to_string());
        event.refs = requests
            .iter()
            .map(|request| SessionDomainRef {
                ref_type: "tool_call".to_string(),
                id: request.tool_use_id.clone(),
                label: Some(request.tool_name.clone()),
            })
            .collect();
        event
    }
}

#[must_use]
pub fn schedule_tool_requests(requests: &[ToolRequest]) -> ToolSchedule {
    let plan = ToolExecutionPlan::from_requests(requests);
    schedule_tool_execution_plan(requests, &plan)
}

#[must_use]
pub fn schedule_tool_execution_plan(
    requests: &[ToolRequest],
    plan: &ToolExecutionPlan,
) -> ToolSchedule {
    let mut parallel_read = Vec::new();
    let mut limited_write = Vec::new();
    let mut limited_network = Vec::new();
    let mut serial_destructive = Vec::new();
    let id_to_index = requests
        .iter()
        .enumerate()
        .map(|(index, request)| (request.tool_use_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut depths = HashMap::new();
    let mut dependency_waves = BTreeMap::<usize, Vec<usize>>::new();

    for (index, task) in plan.tasks.iter().enumerate() {
        let depth =
            task_dependency_depth(index, plan, &id_to_index, &mut depths, &mut BTreeSet::new());
        if task.execution_mode == ToolExecutionMode::Wave || depth > 0 {
            dependency_waves
                .entry(depth.max(1))
                .or_default()
                .push(index);
            continue;
        }
        match task.safety_category {
            ToolSafetyCategory::ReadOnly => parallel_read.push(index),
            ToolSafetyCategory::Network => limited_network.push(index),
            ToolSafetyCategory::WriteLocal => limited_write.push(index),
            ToolSafetyCategory::Destructive => serial_destructive.push(index),
        }
    }

    let mut batches = Vec::new();
    push_batch(
        &mut batches,
        ExecutionBatchMode::ParallelRead,
        parallel_read,
        DEFAULT_PARALLEL_READ_CONCURRENCY,
        "independent read-only tools run concurrently within the Runtime fan-out cap",
        requests,
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::LimitedNetwork,
        limited_network,
        ToolSafetyCategory::Network.max_concurrency(),
        "network tools are rate limited",
        requests,
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::LimitedWrite,
        limited_write,
        ToolSafetyCategory::WriteLocal.max_concurrency(),
        "local mutation tools require resource-aware limits",
        requests,
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::SerialDestructive,
        serial_destructive,
        ToolSafetyCategory::Destructive.max_concurrency(),
        "runtime side-effect tools are serialized",
        requests,
    );
    for (depth, indices) in dependency_waves {
        push_batch(
            &mut batches,
            ExecutionBatchMode::Wave,
            indices,
            1,
            &format!("dependency wave {depth} runs only after its prerequisite wave"),
            requests,
        );
    }

    ToolSchedule { batches }
}

fn task_dependency_depth(
    index: usize,
    plan: &ToolExecutionPlan,
    id_to_index: &BTreeMap<&str, usize>,
    memo: &mut HashMap<usize, usize>,
    visiting: &mut BTreeSet<usize>,
) -> usize {
    if let Some(depth) = memo.get(&index) {
        return *depth;
    }
    // A cycle or unknown dependency is conservatively held behind one wave;
    // the execution plan/validator reports the invalid graph separately.
    if !visiting.insert(index) {
        return 1;
    }
    let depth = plan.tasks.get(index).map_or(1, |task| {
        task.depends_on
            .iter()
            .map(|dependency| {
                id_to_index
                    .get(dependency.as_str())
                    .map_or(0, |dependency_index| {
                        task_dependency_depth(*dependency_index, plan, id_to_index, memo, visiting)
                    })
            })
            .max()
            .map_or(0, |depth| depth.saturating_add(1))
    });
    visiting.remove(&index);
    memo.insert(index, depth);
    depth
}

#[must_use]
pub fn schedule_tool_requests_for_decision(
    requests: &[ToolRequest],
    decision: &RuntimeExecutionDecision,
) -> ToolSchedule {
    let plan = ToolExecutionPlan::from_requests(requests);
    schedule_tool_execution_plan_for_decision(requests, &plan, decision)
}

#[must_use]
pub fn schedule_tool_execution_plan_for_decision(
    requests: &[ToolRequest],
    plan: &ToolExecutionPlan,
    _decision: &RuntimeExecutionDecision,
) -> ToolSchedule {
    // A strategy's Parallel modifier is a planning preference, not a safety
    // switch.  Independent, read-only calls are already safe to run in
    // parallel; requiring the modifier made ordinary multi-read model turns
    // serial for no safety benefit. Dependencies, safety category and scope
    // policy below remain the authority for every side effect.
    schedule_tool_execution_plan(requests, plan)
}

fn push_batch(
    batches: &mut Vec<ExecutionBatch>,
    mode: ExecutionBatchMode,
    indices: Vec<usize>,
    max_concurrency: usize,
    reason: &str,
    requests: &[ToolRequest],
) {
    if !indices.is_empty() {
        let scope_groups = build_scope_groups(&indices, requests);
        batches.push(ExecutionBatch {
            mode,
            indices,
            max_concurrency,
            reason: reason.to_string(),
            scope_groups,
        });
    }
}

fn build_scope_groups(indices: &[usize], requests: &[ToolRequest]) -> Vec<ExecutionScopeGroup> {
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for index in indices {
        let Some(request) = requests.get(*index) else {
            continue;
        };
        groups
            .entry(request_scope(request))
            .or_default()
            .push(*index);
    }
    groups
        .into_iter()
        .map(|(scope, indices)| ExecutionScopeGroup { scope, indices })
        .collect()
}

fn request_scope(request: &ToolRequest) -> String {
    let input = serde_json::from_str::<Value>(&request.input).unwrap_or(Value::Null);
    match request.tool_name.as_str() {
        "read_file" | "write_file" | "edit_file" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| format!("file:{path}"))
            .unwrap_or_else(|| "file:unknown".to_string()),
        "read_many" => "files:batch".to_string(),
        "glob_search" | "grep_search" | "glob_many" | "grep_many" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| format!("directory:{path}"))
            .unwrap_or_else(|| "workspace:.".to_string()),
        "workspace_snapshot" | "tool_batch_readonly" => "workspace:.".to_string(),
        tool => format!("tool:{tool}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use harness_contract::core::ExecutionModifier;

    use crate::execution_core::build_runtime_execution_decision;

    fn request(id: &str, name: &str, input: &str) -> ToolRequest {
        ToolRequest {
            tool_use_id: id.to_string(),
            tool_name: name.to_string(),
            input: input.to_string(),
            depends_on: Vec::new(),
        }
    }

    fn decision_with_modifiers(modifiers: &[ExecutionModifier]) -> RuntimeExecutionDecision {
        let mut decision = build_runtime_execution_decision("explain this function", None);
        decision.strategy.modifiers = modifiers.to_vec();
        decision
    }

    #[test]
    fn scheduler_groups_read_network_write_and_destructive_tools() {
        let schedule = schedule_tool_requests(&[
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("net-1", "WebSearch", r#"{"query":"rust"}"#),
            request(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
            ),
            request("shell-1", "bash", r#"{"command":"rm -rf target"}"#),
        ]);

        assert_eq!(schedule.batches.len(), 4);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(schedule.batches[0].indices, vec![0]);
        assert_eq!(schedule.batches[1].mode, ExecutionBatchMode::LimitedNetwork);
        assert_eq!(schedule.batches[1].indices, vec![1]);
        assert_eq!(schedule.batches[2].mode, ExecutionBatchMode::LimitedWrite);
        assert_eq!(schedule.batches[2].indices, vec![2]);
        assert_eq!(
            schedule.batches[3].mode,
            ExecutionBatchMode::SerialDestructive
        );
        assert_eq!(schedule.batches[3].indices, vec![3]);
    }

    #[test]
    fn scheduler_keeps_dependency_tasks_in_wave_batch() {
        let mut req = request("edit-1", "edit_file", r#"{"path":"src/lib.rs"}"#);
        req.depends_on.push("read-1".to_string());
        let schedule = schedule_tool_requests(&[
            request("read-1", "read_file", r#"{"path":"src/lib.rs"}"#),
            req,
        ]);

        assert_eq!(schedule.batches.len(), 2);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(schedule.batches[0].indices, vec![0]);
        assert_eq!(schedule.batches[1].mode, ExecutionBatchMode::Wave);
        assert_eq!(schedule.batches[1].indices, vec![1]);
        assert_eq!(schedule.remaining_indices(), vec![1]);
    }

    #[test]
    fn scheduler_treats_runtime_capabilities_as_parallel_readonly() {
        let schedule = schedule_tool_requests(&[
            request(
                "cap-1",
                "runtime_capabilities",
                r#"{"intent":"检查 README 是否反映最新架构"}"#,
            ),
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
        ]);

        assert_eq!(schedule.batches.len(), 1);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(schedule.batches[0].indices, vec![0, 1]);
    }

    #[test]
    fn scheduler_keeps_large_idempotent_read_batches_parallel_but_bounded() {
        let requests = (0..48)
            .map(|index| {
                request(
                    &format!("read-{index}"),
                    "read_file",
                    &format!(r#"{{"path":"src/{index}.rs"}}"#),
                )
            })
            .collect::<Vec<_>>();

        let schedule = schedule_tool_requests(&requests);

        assert_eq!(schedule.batches.len(), 1);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(schedule.batches[0].indices.len(), 48);
        assert_eq!(
            schedule.batches[0].max_concurrency,
            DEFAULT_PARALLEL_READ_CONCURRENCY
        );
    }

    #[test]
    fn scheduler_reuses_registered_tool_classification_from_execution_plan() {
        let requests = vec![request("plugin-read", "company_catalog_lookup", "{}")];
        let plan = ToolExecutionPlan::from_requests_with_classifier(&requests, |name, _| {
            (name == "company_catalog_lookup").then_some(ToolSafetyCategory::ReadOnly)
        });
        let decision = decision_with_modifiers(&[ExecutionModifier::Parallel]);

        let schedule = schedule_tool_execution_plan_for_decision(&requests, &plan, &decision);

        assert_eq!(schedule.batches.len(), 1);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
    }

    #[test]
    fn schedule_runtime_event_refs_all_tools() {
        let requests = vec![
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("net-1", "WebSearch", r#"{"query":"rust"}"#),
        ];
        let schedule = schedule_tool_requests(&requests);
        let event = schedule.to_runtime_event("session-1", 9, 123, &requests);

        assert_eq!(event.scope, SessionDomainScope::Tool);
        assert_eq!(event.kind, "tool.schedule.created");
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.payload["batch_count"], 2);
    }

    #[test]
    fn scheduler_projects_resource_scope_groups() {
        let schedule = schedule_tool_requests(&[
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("grep-1", "grep_search", r#"{"pattern":"fn","path":"src"}"#),
            request(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
            ),
        ]);

        let parallel = &schedule.batches[0];
        assert_eq!(parallel.mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(parallel.scope_groups.len(), 2);
        assert_eq!(parallel.scope_groups[0].scope, "directory:src");
        assert_eq!(parallel.scope_groups[0].indices, vec![1]);
        assert_eq!(parallel.scope_groups[1].scope, "file:README.md");
        assert_eq!(parallel.scope_groups[1].indices, vec![0]);

        let write = &schedule.batches[1];
        assert_eq!(write.mode, ExecutionBatchMode::LimitedWrite);
        assert_eq!(write.scope_groups[0].scope, "file:src/lib.rs");
    }

    #[test]
    fn decision_scheduler_parallelizes_independent_reads_without_parallel_modifier() {
        let requests = [
            request(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
            ),
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("net-1", "WebSearch", r#"{"query":"rust"}"#),
            request("shell-1", "bash", r#"{"command":"rm -rf target"}"#),
        ];
        let decision = decision_with_modifiers(&[]);

        let schedule = schedule_tool_requests_for_decision(&requests, &decision);

        assert_eq!(schedule.batches.len(), 4);
        assert_eq!(
            schedule
                .batches
                .iter()
                .map(|batch| batch.indices.clone())
                .collect::<Vec<_>>(),
            vec![vec![1], vec![2], vec![0], vec![3]]
        );
        assert_eq!(
            schedule
                .batches
                .iter()
                .map(|batch| batch.mode)
                .collect::<Vec<_>>(),
            vec![
                ExecutionBatchMode::ParallelRead,
                ExecutionBatchMode::LimitedNetwork,
                ExecutionBatchMode::LimitedWrite,
                ExecutionBatchMode::SerialDestructive,
            ]
        );
        assert_eq!(
            schedule.batches[0].max_concurrency,
            DEFAULT_PARALLEL_READ_CONCURRENCY
        );
    }

    #[test]
    fn decision_scheduler_reuses_grouped_concurrency_with_parallel_modifier() {
        let requests = [
            request(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
            ),
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("net-1", "WebSearch", r#"{"query":"rust"}"#),
            request("read-2", "grep_search", r#"{"pattern":"fn"}"#),
        ];
        let decision = decision_with_modifiers(&[ExecutionModifier::Parallel]);

        let schedule = schedule_tool_requests_for_decision(&requests, &decision);
        let existing = schedule_tool_requests(&requests);

        assert_eq!(schedule, existing);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(schedule.batches[0].indices, vec![1, 3]);
        assert_eq!(
            schedule.batches[0].max_concurrency,
            DEFAULT_PARALLEL_READ_CONCURRENCY
        );
        assert_eq!(schedule.batches[1].indices, vec![2]);
        assert_eq!(schedule.batches[2].indices, vec![0]);
    }
}
