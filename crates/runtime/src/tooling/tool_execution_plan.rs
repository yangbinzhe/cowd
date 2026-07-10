//! Explainable tool execution planning for batched tool requests.

use harness_contract::core::{ExecutionModifier, ExecutionPolicyGate, TaskRisk};
use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::execution_core::{RuntimeCompileTarget, RuntimeExecutionDecision};
use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::{ToolSafetyCategory, ToolSafetyRegistry};

pub const TOOL_EXECUTION_PLAN_CONTRACT_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionMode {
    ParallelRead,
    LimitedParallel,
    SerialDestructive,
    Wave,
}

impl ToolExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParallelRead => "parallel_read",
            Self::LimitedParallel => "limited_parallel",
            Self::SerialDestructive => "serial_destructive",
            Self::Wave => "wave",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPurity {
    ReadOnlyIdempotent,
    LocalMutation,
    Network,
    RuntimeSideEffect,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResourceScope {
    pub kind: String,
    pub paths: Vec<String>,
    pub network: bool,
    pub unknown: bool,
}

impl ToolResourceScope {
    fn workspace() -> Self {
        Self {
            kind: "workspace".to_string(),
            paths: vec![".".to_string()],
            network: false,
            unknown: false,
        }
    }

    fn paths(paths: Vec<String>) -> Self {
        Self {
            kind: "paths".to_string(),
            paths,
            network: false,
            unknown: false,
        }
    }

    fn network() -> Self {
        Self {
            kind: "network".to_string(),
            paths: Vec::new(),
            network: true,
            unknown: false,
        }
    }

    fn runtime() -> Self {
        Self {
            kind: "runtime".to_string(),
            paths: Vec::new(),
            network: false,
            unknown: false,
        }
    }

    fn unknown() -> Self {
        Self {
            kind: "unknown".to_string(),
            paths: Vec::new(),
            network: false,
            unknown: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolConflict {
    pub tool_call_id: String,
    pub kind: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionPlanTask {
    pub contract_version: u32,
    pub tool_call_id: String,
    pub tool_name: String,
    pub idempotency_key: String,
    pub model_visible_name: String,
    pub can_parallelize: bool,
    pub safety_category: ToolSafetyCategory,
    pub purity: ToolPurity,
    pub resource_scope: ToolResourceScope,
    pub authority_set: Vec<String>,
    pub side_effect_class: String,
    pub output_budget_class: String,
    pub conflicts: Vec<ToolConflict>,
    pub reason: String,
    pub execution_mode: ToolExecutionMode,
    pub depends_on: Vec<String>,
    pub max_concurrency: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionPlan {
    pub plan_id: String,
    pub task_count: usize,
    pub parallel_read_count: usize,
    pub limited_count: usize,
    pub destructive_count: usize,
    pub wave_count: usize,
    pub tasks: Vec<ToolExecutionPlanTask>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionPolicyValidationReport {
    pub allowed: bool,
    pub findings: Vec<String>,
    pub lease_id: String,
    pub requires_approval: bool,
    pub requires_checkpoint: bool,
    pub approval_satisfied: bool,
    pub checkpoint_created: bool,
}

impl ToolExecutionPlan {
    #[must_use]
    pub fn from_requests(requests: &[ToolRequest]) -> Self {
        Self::from_requests_with_classifier(requests, |_, _| None)
    }

    #[must_use]
    pub fn from_requests_with_classifier(
        requests: &[ToolRequest],
        classify_registered_tool: impl Fn(&str, &str) -> Option<ToolSafetyCategory>,
    ) -> Self {
        let registry = ToolSafetyRegistry::global();
        let mut parallel_read_count = 0;
        let mut limited_count = 0;
        let mut destructive_count = 0;
        let mut wave_count = 0;

        let mut tasks = requests
            .iter()
            .map(|request| {
                let builtin_category =
                    registry.classify_request(&request.tool_name, &request.input);
                let safety_category = if builtin_category == ToolSafetyCategory::WriteLocal {
                    classify_registered_tool(&request.tool_name, &request.input)
                        .unwrap_or(builtin_category)
                } else {
                    builtin_category
                };
                let analysis = analyze_request(request, safety_category);
                let execution_mode = if !request.depends_on.is_empty() {
                    wave_count += 1;
                    ToolExecutionMode::Wave
                } else {
                    match safety_category {
                        ToolSafetyCategory::ReadOnly => {
                            parallel_read_count += 1;
                            ToolExecutionMode::ParallelRead
                        }
                        ToolSafetyCategory::Destructive => {
                            destructive_count += 1;
                            ToolExecutionMode::SerialDestructive
                        }
                        ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Network => {
                            limited_count += 1;
                            ToolExecutionMode::LimitedParallel
                        }
                    }
                };

                ToolExecutionPlanTask {
                    contract_version: TOOL_EXECUTION_PLAN_CONTRACT_VERSION,
                    tool_call_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    idempotency_key: tool_plan_idempotency_key(request),
                    model_visible_name: model_visible_tool_name(&request.tool_name),
                    can_parallelize: matches!(
                        execution_mode,
                        ToolExecutionMode::ParallelRead | ToolExecutionMode::LimitedParallel
                    ),
                    safety_category,
                    purity: analysis.purity,
                    resource_scope: analysis.resource_scope,
                    authority_set: analysis.authority_set,
                    side_effect_class: analysis.side_effect_class,
                    output_budget_class: analysis.output_budget_class,
                    conflicts: Vec::new(),
                    reason: analysis.reason,
                    execution_mode,
                    depends_on: request.depends_on.clone(),
                    max_concurrency: match execution_mode {
                        ToolExecutionMode::Wave => 8,
                        _ => safety_category.max_concurrency(),
                    },
                }
            })
            .collect::<Vec<_>>();
        annotate_conflicts(&mut tasks);
        for task in &mut tasks {
            task.can_parallelize = task.conflicts.is_empty()
                && matches!(
                    task.execution_mode,
                    ToolExecutionMode::ParallelRead | ToolExecutionMode::LimitedParallel
                );
        }

        Self {
            plan_id: format!("tool-plan-{}", Uuid::new_v4()),
            task_count: tasks.len(),
            parallel_read_count,
            limited_count,
            destructive_count,
            wave_count,
            tasks,
        }
    }

    #[must_use]
    pub fn validate_against_execution_decision(
        &self,
        decision: &RuntimeExecutionDecision,
    ) -> ToolExecutionPolicyValidationReport {
        let mut findings = Vec::new();
        if !decision.executable {
            findings.push("execution_decision_not_executable".to_string());
            return ToolExecutionPolicyValidationReport {
                allowed: false,
                findings,
                lease_id: decision.lease.lease_id.clone(),
                requires_approval: false,
                requires_checkpoint: false,
                approval_satisfied: false,
                checkpoint_created: false,
            };
        }

        let governed_tasks = self
            .tasks
            .iter()
            .filter(|task| !uses_inner_runtime_validator(&task.tool_name))
            .collect::<Vec<_>>();
        let has_network = governed_tasks
            .iter()
            .any(|task| task.safety_category == ToolSafetyCategory::Network);
        let has_mutation = governed_tasks.iter().any(|task| is_mutation(task));
        let high_or_critical = matches!(decision.risk(), TaskRisk::High | TaskRisk::Critical);
        let requires_approval = has_mutation
            && decision.risk() == TaskRisk::Critical
            && decision.gates().contains(&ExecutionPolicyGate::Approval);
        let requires_checkpoint = has_mutation
            && high_or_critical
            && decision
                .modifiers()
                .contains(&ExecutionModifier::WithCheckpoint);

        if governed_tasks.iter().any(|task| {
            !compile_target_allows_category(decision.compile_target, task.safety_category)
        }) {
            push_finding(&mut findings, "tool_category_not_allowed_by_compile_target");
        }

        if has_network
            && !decision
                .modifiers()
                .contains(&ExecutionModifier::WithExternalResearch)
        {
            push_finding(&mut findings, "network_requires_with_external_research");
        }

        if decision.compile_target == RuntimeCompileTarget::ExecutionGraph && has_mutation {
            if !decision.gates().contains(&ExecutionPolicyGate::Permission) {
                push_finding(&mut findings, "mutation_requires_permission_gate");
            }
            if !decision
                .modifiers()
                .contains(&ExecutionModifier::WithGuardrails)
            {
                push_finding(&mut findings, "mutation_requires_with_guardrails");
            }

            if high_or_critical && !decision.gates().contains(&ExecutionPolicyGate::Risk) {
                push_finding(&mut findings, "high_risk_mutation_requires_risk_gate");
            }
            if decision.risk() == TaskRisk::Critical
                && !decision.gates().contains(&ExecutionPolicyGate::Approval)
            {
                push_finding(&mut findings, "critical_mutation_requires_approval_gate");
            }
            if high_or_critical
                && !decision
                    .modifiers()
                    .contains(&ExecutionModifier::WithCheckpoint)
            {
                push_finding(&mut findings, "high_risk_mutation_requires_with_checkpoint");
            }
        }

        if has_mutation
            && decision
                .modifiers()
                .contains(&ExecutionModifier::BoundedChange)
            && !has_single_known_mutation_path(&governed_tasks)
        {
            push_finding(
                &mut findings,
                "bounded_change_requires_single_known_path_scope",
            );
        }

        ToolExecutionPolicyValidationReport {
            allowed: findings.is_empty(),
            findings,
            lease_id: decision.lease.lease_id.clone(),
            requires_approval,
            requires_checkpoint,
            approval_satisfied: false,
            checkpoint_created: false,
        }
    }

    #[must_use]
    pub fn to_runtime_event(
        &self,
        session_id: impl Into<String>,
        sequence: usize,
        created_at_ms: u64,
    ) -> RuntimeEvent {
        let payload = serde_json::json!({
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "parallel_read_count": self.parallel_read_count,
            "limited_count": self.limited_count,
            "destructive_count": self.destructive_count,
            "wave_count": self.wave_count,
            "tasks": self.tasks,
        });
        let mut event = RuntimeEvent::new(
            session_id,
            sequence,
            RuntimeEventScope::Tool,
            "tool.execution_plan.created",
            payload,
            created_at_ms,
        );
        event.status = Some("planned".to_string());
        event.span_id = Some(self.plan_id.clone());
        event.refs = self
            .tasks
            .iter()
            .map(|task| RuntimeRef {
                ref_type: "tool_call".to_string(),
                id: task.tool_call_id.clone(),
                label: Some(task.tool_name.clone()),
            })
            .collect();
        event
    }
}

fn compile_target_allows_category(
    compile_target: RuntimeCompileTarget,
    category: ToolSafetyCategory,
) -> bool {
    match compile_target {
        RuntimeCompileTarget::InlineModel => category == ToolSafetyCategory::ReadOnly,
        RuntimeCompileTarget::EvidenceGraph
        | RuntimeCompileTarget::DeliberationGraph
        | RuntimeCompileTarget::TeamGraph
        | RuntimeCompileTarget::MissionGraph => matches!(
            category,
            ToolSafetyCategory::ReadOnly | ToolSafetyCategory::Network
        ),
        RuntimeCompileTarget::ExecutionGraph => true,
    }
}

fn is_mutation(task: &ToolExecutionPlanTask) -> bool {
    matches!(
        task.safety_category,
        ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Destructive
    )
}

fn uses_inner_runtime_validator(tool_name: &str) -> bool {
    let normalized = tool_name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    matches!(
        normalized.as_str(),
        "runtimecapabilities" | "runtimeorchestrate"
    )
}

fn has_single_known_mutation_path(tasks: &[&ToolExecutionPlanTask]) -> bool {
    let mut bounded_path: Option<&str> = None;
    for task in tasks.iter().copied().filter(|task| is_mutation(task)) {
        if task.resource_scope.unknown || task.resource_scope.kind != "paths" {
            return false;
        }
        let [path] = task.resource_scope.paths.as_slice() else {
            return false;
        };
        if path.is_empty() || path == "." {
            return false;
        }
        if bounded_path.is_some_and(|known| known != path) {
            return false;
        }
        bounded_path = Some(path);
    }
    bounded_path.is_some()
}

fn push_finding(findings: &mut Vec<String>, finding: &str) {
    if !findings.iter().any(|existing| existing == finding) {
        findings.push(finding.to_string());
    }
}

struct ToolRequestAnalysis {
    purity: ToolPurity,
    resource_scope: ToolResourceScope,
    authority_set: Vec<String>,
    side_effect_class: String,
    output_budget_class: String,
    reason: String,
}

fn analyze_request(
    request: &ToolRequest,
    safety_category: ToolSafetyCategory,
) -> ToolRequestAnalysis {
    let input = serde_json::from_str::<Value>(&request.input).unwrap_or(Value::Null);
    let purity = match safety_category {
        ToolSafetyCategory::ReadOnly => ToolPurity::ReadOnlyIdempotent,
        ToolSafetyCategory::WriteLocal => ToolPurity::LocalMutation,
        ToolSafetyCategory::Network => ToolPurity::Network,
        ToolSafetyCategory::Destructive => ToolPurity::RuntimeSideEffect,
    };
    let resource_scope = resource_scope_for(&request.tool_name, &input, safety_category);
    let authority_set = match safety_category {
        ToolSafetyCategory::ReadOnly => vec!["workspace.read".to_string()],
        ToolSafetyCategory::WriteLocal => vec!["workspace.write".to_string()],
        ToolSafetyCategory::Network => vec!["network".to_string()],
        ToolSafetyCategory::Destructive => vec!["runtime.control".to_string()],
    };
    let side_effect_class = match purity {
        ToolPurity::ReadOnlyIdempotent => "none",
        ToolPurity::LocalMutation => "local_mutation",
        ToolPurity::Network => "network",
        ToolPurity::RuntimeSideEffect => "runtime_side_effect",
        ToolPurity::Unknown => "unknown",
    }
    .to_string();
    let output_budget_class = match request.tool_name.as_str() {
        "read_many" | "grep_many" | "glob_many" | "tool_batch_readonly" => "batch",
        "workspace_snapshot" => "summary",
        _ => match safety_category {
            ToolSafetyCategory::ReadOnly => "normal",
            _ => "mutation",
        },
    }
    .to_string();
    let reason = format!(
        "{} tool planned as {} with {} resource scope",
        side_effect_class,
        ToolExecutionMode::from_safety_and_deps(safety_category, !request.depends_on.is_empty())
            .as_str(),
        resource_scope.kind
    );

    ToolRequestAnalysis {
        purity,
        resource_scope,
        authority_set,
        side_effect_class,
        output_budget_class,
        reason,
    }
}

impl ToolExecutionMode {
    fn from_safety_and_deps(safety_category: ToolSafetyCategory, has_deps: bool) -> Self {
        if has_deps {
            return Self::Wave;
        }
        match safety_category {
            ToolSafetyCategory::ReadOnly => Self::ParallelRead,
            ToolSafetyCategory::Destructive => Self::SerialDestructive,
            ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Network => Self::LimitedParallel,
        }
    }
}

fn resource_scope_for(
    tool_name: &str,
    input: &Value,
    safety_category: ToolSafetyCategory,
) -> ToolResourceScope {
    match tool_name {
        "read_file" | "write_file" | "edit_file" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| ToolResourceScope::paths(vec![normalize_resource_path(path)]))
            .unwrap_or_else(ToolResourceScope::unknown),
        "read_many" => ToolResourceScope::paths(extract_array_paths(input, "files", "path")),
        "grep_search" | "glob_search" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| ToolResourceScope::paths(vec![normalize_resource_path(path)]))
            .unwrap_or_else(ToolResourceScope::workspace),
        "grep_many" => {
            let paths = extract_array_paths(input, "searches", "path");
            if paths.is_empty() {
                ToolResourceScope::workspace()
            } else {
                ToolResourceScope::paths(paths)
            }
        }
        "glob_many" => {
            let paths = extract_array_paths(input, "patterns", "path");
            if paths.is_empty() {
                ToolResourceScope::workspace()
            } else {
                ToolResourceScope::paths(paths)
            }
        }
        "workspace_snapshot" | "tool_batch_readonly" => ToolResourceScope::workspace(),
        "WebFetch" | "WebSearch" | "web_fetch" | "web_search" => ToolResourceScope::network(),
        _ => match safety_category {
            ToolSafetyCategory::ReadOnly => ToolResourceScope::workspace(),
            ToolSafetyCategory::Network => ToolResourceScope::network(),
            ToolSafetyCategory::Destructive => ToolResourceScope::runtime(),
            ToolSafetyCategory::WriteLocal => ToolResourceScope::unknown(),
        },
    }
}

fn extract_array_paths(input: &Value, array_key: &str, path_key: &str) -> Vec<String> {
    input
        .get(array_key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get(path_key).and_then(Value::as_str))
                .map(normalize_resource_path)
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_resource_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        ".".to_string()
    } else {
        trimmed.replace('\\', "/")
    }
}

fn tool_plan_idempotency_key(request: &ToolRequest) -> String {
    let mut hasher = Sha256::new();
    hasher.update(request.tool_name.as_bytes());
    hasher.update([0]);
    hasher.update(request.input.as_bytes());
    hasher.update([0]);
    for dependency in &request.depends_on {
        hasher.update(dependency.as_bytes());
        hasher.update([0]);
    }
    let digest = format!("{:x}", hasher.finalize());
    format!("tool-plan-task:v{TOOL_EXECUTION_PLAN_CONTRACT_VERSION}:{digest}")
}

fn model_visible_tool_name(tool_name: &str) -> String {
    tool_name.trim().replace('_', " ")
}

fn annotate_conflicts(tasks: &mut [ToolExecutionPlanTask]) {
    for left in 0..tasks.len() {
        for right in (left + 1)..tasks.len() {
            if let Some((kind, reason)) = conflict_between(&tasks[left], &tasks[right]) {
                let right_id = tasks[right].tool_call_id.clone();
                let left_id = tasks[left].tool_call_id.clone();
                tasks[left].conflicts.push(ToolConflict {
                    tool_call_id: right_id,
                    kind: kind.clone(),
                    reason: reason.clone(),
                });
                tasks[right].conflicts.push(ToolConflict {
                    tool_call_id: left_id,
                    kind,
                    reason,
                });
            }
        }
    }
}

fn conflict_between(
    left: &ToolExecutionPlanTask,
    right: &ToolExecutionPlanTask,
) -> Option<(String, String)> {
    if left.purity == ToolPurity::ReadOnlyIdempotent
        && right.purity == ToolPurity::ReadOnlyIdempotent
    {
        return None;
    }
    if left.resource_scope.unknown || right.resource_scope.unknown {
        return Some((
            "unknown_resource".to_string(),
            "unknown write/runtime resource scope requires conservative scheduling".to_string(),
        ));
    }
    if left.safety_category == ToolSafetyCategory::Destructive
        || right.safety_category == ToolSafetyCategory::Destructive
    {
        return Some((
            "runtime_side_effect".to_string(),
            "destructive/runtime side-effect tools must be serialized".to_string(),
        ));
    }
    for left_path in &left.resource_scope.paths {
        for right_path in &right.resource_scope.paths {
            if paths_overlap(left_path, right_path) {
                return Some((
                    "path_overlap".to_string(),
                    format!("resource paths overlap: {left_path} <-> {right_path}"),
                ));
            }
        }
    }
    None
}

fn paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || left == "."
        || right == "."
        || left.starts_with(&format!("{right}/"))
        || right.starts_with(&format!("{left}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::execution_core::build_runtime_execution_decision;

    fn request(id: &str, tool_name: &str, depends_on: Vec<String>) -> ToolRequest {
        request_with_input(id, tool_name, "{}", depends_on)
    }

    fn request_with_input(
        id: &str,
        tool_name: &str,
        input: &str,
        depends_on: Vec<String>,
    ) -> ToolRequest {
        ToolRequest {
            tool_use_id: id.to_string(),
            tool_name: tool_name.to_string(),
            input: input.to_string(),
            depends_on,
        }
    }

    fn execution_decision(
        compile_target: RuntimeCompileTarget,
        risk: TaskRisk,
        modifiers: &[ExecutionModifier],
        gates: &[ExecutionPolicyGate],
    ) -> RuntimeExecutionDecision {
        let mut decision = build_runtime_execution_decision("explain this function", None);
        decision.compile_target = compile_target;
        decision.strategy.understanding.risk = risk;
        decision.strategy.modifiers = modifiers.to_vec();
        decision.strategy.gates = gates.to_vec();
        decision.lease.lease_id = "lease-test".to_string();
        decision.executable = true;
        decision.blocked_reasons.clear();
        decision
    }

    #[test]
    fn tool_contract_plan_classifies_parallel_limited_and_destructive_tools() {
        let plan = ToolExecutionPlan::from_requests(&[
            request("read-1", "read", Vec::new()),
            request("write-1", "write", Vec::new()),
            request("rm-1", "rm", Vec::new()),
        ]);

        assert_eq!(plan.task_count, 3);
        assert_eq!(plan.parallel_read_count, 1);
        assert_eq!(plan.limited_count, 1);
        assert_eq!(plan.destructive_count, 1);
        assert_eq!(
            plan.tasks[0].execution_mode,
            ToolExecutionMode::ParallelRead
        );
        assert_eq!(
            plan.tasks[0].contract_version,
            TOOL_EXECUTION_PLAN_CONTRACT_VERSION
        );
        assert!(plan.tasks[0]
            .idempotency_key
            .starts_with("tool-plan-task:v2:"));
        assert_eq!(plan.tasks[0].model_visible_name, "read");
        assert!(!plan.tasks[0].can_parallelize);
        assert_eq!(
            plan.tasks[2].execution_mode,
            ToolExecutionMode::SerialDestructive
        );
        assert!(!plan.tasks[2].can_parallelize);
    }

    #[test]
    fn dependency_tasks_are_planned_as_wave_tasks() {
        let plan = ToolExecutionPlan::from_requests(&[request(
            "write-2",
            "write",
            vec!["read-1".to_string()],
        )]);

        assert_eq!(plan.wave_count, 1);
        assert_eq!(plan.tasks[0].execution_mode, ToolExecutionMode::Wave);
        assert_eq!(plan.tasks[0].max_concurrency, 8);
    }

    #[test]
    fn tool_contract_readonly_batch_can_parallelize_without_conflicts() {
        let plan = ToolExecutionPlan::from_requests(&[
            request("read-1", "read_file", Vec::new()),
            request("read-2", "grep_search", Vec::new()),
        ]);

        assert!(plan.tasks.iter().all(|task| task.can_parallelize));
        assert!(plan
            .tasks
            .iter()
            .all(|task| task.contract_version == TOOL_EXECUTION_PLAN_CONTRACT_VERSION));
    }

    #[test]
    fn plan_event_refs_all_tool_calls() {
        let plan = ToolExecutionPlan::from_requests(&[
            request("read-1", "read", Vec::new()),
            request("write-1", "write", Vec::new()),
        ]);
        let event = plan.to_runtime_event("session-1", 7, 123);

        assert_eq!(event.scope, RuntimeEventScope::Tool);
        assert_eq!(event.kind, "tool.execution_plan.created");
        assert_eq!(event.status.as_deref(), Some("planned"));
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.payload["task_count"], 2);
        assert_eq!(event.payload["tasks"][0]["contract_version"], 2);
        assert!(event.payload["tasks"][0]["idempotency_key"]
            .as_str()
            .unwrap()
            .starts_with("tool-plan-task:v2:"));
    }

    #[test]
    fn plan_records_resource_scope_and_authority_metadata() {
        let plan = ToolExecutionPlan::from_requests(&[
            ToolRequest {
                tool_use_id: "read-1".to_string(),
                tool_name: "read_file".to_string(),
                input: r#"{"path":"src/lib.rs"}"#.to_string(),
                depends_on: Vec::new(),
            },
            ToolRequest {
                tool_use_id: "web-1".to_string(),
                tool_name: "WebSearch".to_string(),
                input: r#"{"query":"latest rust"}"#.to_string(),
                depends_on: Vec::new(),
            },
        ]);

        assert_eq!(plan.tasks[0].purity, ToolPurity::ReadOnlyIdempotent);
        assert_eq!(plan.tasks[0].resource_scope.kind, "paths");
        assert_eq!(plan.tasks[0].resource_scope.paths, vec!["src/lib.rs"]);
        assert_eq!(plan.tasks[0].authority_set, vec!["workspace.read"]);
        assert_eq!(plan.tasks[0].output_budget_class, "normal");
        assert_eq!(plan.tasks[1].purity, ToolPurity::Network);
        assert_eq!(plan.tasks[1].resource_scope.kind, "network");
        assert_eq!(plan.tasks[1].authority_set, vec!["network"]);
    }

    #[test]
    fn plan_marks_overlapping_write_conflicts() {
        let plan = ToolExecutionPlan::from_requests(&[
            ToolRequest {
                tool_use_id: "write-1".to_string(),
                tool_name: "write_file".to_string(),
                input: r#"{"path":"src/lib.rs","content":"a"}"#.to_string(),
                depends_on: Vec::new(),
            },
            ToolRequest {
                tool_use_id: "edit-1".to_string(),
                tool_name: "edit_file".to_string(),
                input: r#"{"path":"src/lib.rs","old_string":"a","new_string":"b"}"#.to_string(),
                depends_on: Vec::new(),
            },
        ]);

        assert_eq!(plan.tasks[0].conflicts.len(), 1);
        assert!(!plan.tasks[0].can_parallelize);
        assert_eq!(plan.tasks[0].conflicts[0].tool_call_id, "edit-1");
        assert_eq!(plan.tasks[0].conflicts[0].kind, "path_overlap");
        assert_eq!(plan.tasks[1].conflicts[0].tool_call_id, "write-1");
    }

    #[test]
    fn read_only_batch_tools_get_batch_budget_class() {
        let plan = ToolExecutionPlan::from_requests(&[ToolRequest {
            tool_use_id: "batch-1".to_string(),
            tool_name: "tool_batch_readonly".to_string(),
            input: r#"{"calls":[]}"#.to_string(),
            depends_on: Vec::new(),
        }]);

        assert_eq!(plan.tasks[0].safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(plan.tasks[0].output_budget_class, "batch");
        assert!(plan.tasks[0].conflicts.is_empty());
    }

    #[test]
    fn compile_targets_enforce_normal_tool_categories() {
        let read_plan =
            ToolExecutionPlan::from_requests(&[request("read-1", "read_file", Vec::new())]);
        let network_plan =
            ToolExecutionPlan::from_requests(&[request("network-1", "WebSearch", Vec::new())]);
        let write_plan = ToolExecutionPlan::from_requests(&[request_with_input(
            "write-1",
            "write_file",
            r#"{"path":"src/lib.rs","content":"x"}"#,
            Vec::new(),
        )]);

        let inline = execution_decision(
            RuntimeCompileTarget::InlineModel,
            TaskRisk::Low,
            &[ExecutionModifier::WithExternalResearch],
            &[],
        );
        assert!(
            read_plan
                .validate_against_execution_decision(&inline)
                .allowed
        );
        assert_eq!(
            network_plan
                .validate_against_execution_decision(&inline)
                .findings,
            vec!["tool_category_not_allowed_by_compile_target"]
        );

        let evidence = execution_decision(
            RuntimeCompileTarget::EvidenceGraph,
            TaskRisk::Low,
            &[ExecutionModifier::WithExternalResearch],
            &[],
        );
        assert!(
            read_plan
                .validate_against_execution_decision(&evidence)
                .allowed
        );
        assert!(
            network_plan
                .validate_against_execution_decision(&evidence)
                .allowed
        );
        assert_eq!(
            write_plan
                .validate_against_execution_decision(&evidence)
                .findings,
            vec!["tool_category_not_allowed_by_compile_target"]
        );

        let execution = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Medium,
            &[ExecutionModifier::WithGuardrails],
            &[ExecutionPolicyGate::Permission],
        );
        assert!(
            write_plan
                .validate_against_execution_decision(&execution)
                .allowed
        );

        for compile_target in [
            RuntimeCompileTarget::DeliberationGraph,
            RuntimeCompileTarget::TeamGraph,
            RuntimeCompileTarget::MissionGraph,
        ] {
            let decision = execution_decision(
                compile_target,
                TaskRisk::Low,
                &[ExecutionModifier::WithExternalResearch],
                &[],
            );
            assert!(
                read_plan
                    .validate_against_execution_decision(&decision)
                    .allowed
            );
            assert!(
                network_plan
                    .validate_against_execution_decision(&decision)
                    .allowed
            );
            assert_eq!(
                write_plan
                    .validate_against_execution_decision(&decision)
                    .findings,
                vec!["tool_category_not_allowed_by_compile_target"],
                "{compile_target:?}"
            );
        }
    }

    #[test]
    fn execution_graph_mutation_requires_typed_gates_and_modifiers() {
        let plan = ToolExecutionPlan::from_requests(&[request_with_input(
            "write-1",
            "write_file",
            r#"{"path":"src/lib.rs","content":"x"}"#,
            Vec::new(),
        )]);

        let medium = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Medium,
            &[],
            &[],
        );
        assert_eq!(
            plan.validate_against_execution_decision(&medium).findings,
            vec![
                "mutation_requires_permission_gate",
                "mutation_requires_with_guardrails",
            ]
        );

        let high = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::High,
            &[ExecutionModifier::WithGuardrails],
            &[ExecutionPolicyGate::Permission],
        );
        assert_eq!(
            plan.validate_against_execution_decision(&high).findings,
            vec![
                "high_risk_mutation_requires_risk_gate",
                "high_risk_mutation_requires_with_checkpoint",
            ]
        );

        let critical = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Critical,
            &[
                ExecutionModifier::WithGuardrails,
                ExecutionModifier::WithCheckpoint,
            ],
            &[ExecutionPolicyGate::Permission, ExecutionPolicyGate::Risk],
        );
        assert_eq!(
            plan.validate_against_execution_decision(&critical).findings,
            vec!["critical_mutation_requires_approval_gate"]
        );

        let fully_gated = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Critical,
            &[
                ExecutionModifier::WithGuardrails,
                ExecutionModifier::WithCheckpoint,
            ],
            &[
                ExecutionPolicyGate::Permission,
                ExecutionPolicyGate::Risk,
                ExecutionPolicyGate::Approval,
            ],
        );
        assert!(
            plan.validate_against_execution_decision(&fully_gated)
                .allowed
        );
    }

    #[test]
    fn network_tools_require_external_research_modifier() {
        let plan =
            ToolExecutionPlan::from_requests(&[request("network-1", "WebSearch", Vec::new())]);
        let without_research =
            execution_decision(RuntimeCompileTarget::EvidenceGraph, TaskRisk::Low, &[], &[]);

        assert_eq!(
            plan.validate_against_execution_decision(&without_research)
                .findings,
            vec!["network_requires_with_external_research"]
        );

        let with_research = execution_decision(
            RuntimeCompileTarget::EvidenceGraph,
            TaskRisk::Low,
            &[ExecutionModifier::WithExternalResearch],
            &[],
        );
        assert!(
            plan.validate_against_execution_decision(&with_research)
                .allowed
        );
    }

    #[test]
    fn registered_read_only_tool_metadata_overrides_unknown_tool_fallback() {
        let plan = ToolExecutionPlan::from_requests_with_classifier(
            &[request("plugin-read", "company_catalog_lookup", Vec::new())],
            |name, _| (name == "company_catalog_lookup").then_some(ToolSafetyCategory::ReadOnly),
        );

        assert_eq!(plan.tasks[0].safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(
            plan.tasks[0].execution_mode,
            ToolExecutionMode::ParallelRead
        );
        let decision =
            execution_decision(RuntimeCompileTarget::EvidenceGraph, TaskRisk::Low, &[], &[]);
        assert!(plan.validate_against_execution_decision(&decision).allowed);
    }

    #[test]
    fn bounded_change_requires_one_known_mutation_path_scope() {
        let decision = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Medium,
            &[
                ExecutionModifier::BoundedChange,
                ExecutionModifier::WithGuardrails,
            ],
            &[ExecutionPolicyGate::Permission],
        );
        let one_path = ToolExecutionPlan::from_requests(&[
            request_with_input(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
                Vec::new(),
            ),
            request_with_input(
                "edit-1",
                "edit_file",
                r#"{"path":"src/lib.rs","old_string":"x","new_string":"y"}"#,
                Vec::new(),
            ),
        ]);
        assert!(
            one_path
                .validate_against_execution_decision(&decision)
                .allowed
        );

        let multiple_paths = ToolExecutionPlan::from_requests(&[
            request_with_input(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
                Vec::new(),
            ),
            request_with_input(
                "write-2",
                "write_file",
                r#"{"path":"src/main.rs","content":"y"}"#,
                Vec::new(),
            ),
        ]);
        assert_eq!(
            multiple_paths
                .validate_against_execution_decision(&decision)
                .findings,
            vec!["bounded_change_requires_single_known_path_scope"]
        );

        let unknown_path =
            ToolExecutionPlan::from_requests(&[request("custom-1", "custom_mutation", Vec::new())]);
        assert_eq!(
            unknown_path
                .validate_against_execution_decision(&decision)
                .findings,
            vec!["bounded_change_requires_single_known_path_scope"]
        );
    }

    #[test]
    fn runtime_entry_tools_defer_to_their_inner_validators() {
        let plan = ToolExecutionPlan::from_requests(&[
            request("capabilities-1", "RuntimeCapabilities", Vec::new()),
            request("orchestrate-1", "runtime_orchestrate", Vec::new()),
        ]);
        let decision =
            execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);

        assert!(plan.validate_against_execution_decision(&decision).allowed);
    }

    #[test]
    fn non_executable_decision_is_rejected_with_serializable_lease_report() {
        let plan = ToolExecutionPlan::from_requests(&[request("read-1", "read_file", Vec::new())]);
        let mut decision =
            execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);
        decision.executable = false;

        let report = plan.validate_against_execution_decision(&decision);
        let wire = serde_json::to_value(&report).expect("validation report serializes");

        assert!(!report.allowed);
        assert_eq!(report.findings, vec!["execution_decision_not_executable"]);
        assert_eq!(report.lease_id, "lease-test");
        assert_eq!(wire["allowed"], false);
        assert_eq!(wire["findings"][0], "execution_decision_not_executable");
        assert_eq!(wire["lease_id"], "lease-test");
    }

    #[test]
    fn plan_uses_request_level_bash_classification() {
        let plan = ToolExecutionPlan::from_requests(&[
            request_with_input(
                "bash-read",
                "bash",
                r#"{"command":"git status"}"#,
                Vec::new(),
            ),
            request_with_input(
                "bash-write",
                "bash",
                r#"{"command":"mkdir target/new"}"#,
                Vec::new(),
            ),
        ]);

        assert_eq!(plan.tasks[0].safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(
            plan.tasks[1].safety_category,
            ToolSafetyCategory::WriteLocal
        );
    }
}
