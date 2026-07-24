//! Canonical governed plan for batched tool requests.

use harness_contract::core::{ExecutionModifier, ExecutionPolicyGate, TaskRisk};
use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
use harness_contract::tool::{
    GovernedToolInvocation, GovernedToolPlanProjection, ResourceAccess, ResourceDemand,
    ResourceScopeDemand, ToolDependency, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
    ToolIntent,
};
use memory::{SessionDomainEvent, SessionDomainRef, SessionDomainScope};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::execution_core::{RuntimeCompileTarget, RuntimeExecutionDecision};
use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;

pub const GOVERNED_TOOL_PLAN_CONTRACT_VERSION: u32 = 3;
pub const DEFAULT_PARALLEL_TOOL_CONCURRENCY: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernedToolExecutionMode {
    ParallelRead,
    LimitedParallel,
    SerialDestructive,
    Wave,
}

impl GovernedToolExecutionMode {
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
pub struct GovernedToolPlanTask {
    pub contract_version: u32,
    pub tool_call_id: String,
    pub tool_name: String,
    pub normalized_input: Value,
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
    pub execution_mode: GovernedToolExecutionMode,
    pub depends_on: Vec<String>,
    pub max_concurrency: usize,
    pub effect: ToolEffectDescriptor,
    pub resource_demand: ResourceDemand,
    pub catalog_revision: u64,
    pub descriptor_set_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernedExecutionBatchMode {
    ParallelRead,
    SerialStrategy,
    LimitedWrite,
    LimitedNetwork,
    SerialDestructive,
    DependencyWave,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedExecutionScopeGroup {
    pub scope: String,
    pub indices: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedExecutionBatch {
    pub mode: GovernedExecutionBatchMode,
    pub indices: Vec<usize>,
    pub max_concurrency: usize,
    pub reason: String,
    pub scope_groups: Vec<GovernedExecutionScopeGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedToolPlan {
    pub plan_id: String,
    pub revision: u64,
    pub catalog_revision: u64,
    pub task_count: usize,
    pub parallel_read_count: usize,
    pub limited_count: usize,
    pub destructive_count: usize,
    pub wave_count: usize,
    pub tasks: Vec<GovernedToolPlanTask>,
    pub batches: Vec<GovernedExecutionBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedToolPolicyValidationReport {
    pub allowed: bool,
    pub findings: Vec<String>,
    pub lease_id: String,
    pub requires_approval: bool,
    pub requires_checkpoint: bool,
    pub approval_satisfied: bool,
    pub checkpoint_created: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GovernedToolCompiler;

impl GovernedToolCompiler {
    #[must_use]
    pub fn compile(
        &self,
        requests: &[ToolRequest],
        decision: Option<&RuntimeExecutionDecision>,
        describe_registered_tool: impl Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    ) -> GovernedToolPlan {
        GovernedToolPlan::compile(requests, decision, describe_registered_tool)
    }
}

impl GovernedToolPlan {
    /// Conservative fallback retained for isolated tests and offline planning.
    /// Production callers must use `GovernedToolCompiler` with a pinned
    /// registration descriptor.
    #[must_use]
    pub fn from_requests(requests: &[ToolRequest]) -> Self {
        #[cfg(test)]
        {
            return Self::compile(requests, None, |name, input| {
                Some((fixture_effect(name, input), 1, "test-fixture".to_string()))
            });
        }
        #[cfg(not(test))]
        Self::compile(requests, None, |name, input| {
            Some((
                unknown_effect(name, input),
                0,
                "offline-unknown".to_string(),
            ))
        })
    }

    fn compile(
        requests: &[ToolRequest],
        decision: Option<&RuntimeExecutionDecision>,
        describe_registered_tool: impl Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    ) -> Self {
        let mut parallel_read_count = 0;
        let mut limited_count = 0;
        let mut destructive_count = 0;
        let mut wave_count = 0;

        let mut tasks = requests
            .iter()
            .map(|request| {
                let normalized_input =
                    serde_json::from_str::<Value>(&request.input).unwrap_or(Value::Null);
                let (effect, catalog_revision, descriptor_set_hash) =
                    describe_registered_tool(&request.tool_name, &normalized_input).unwrap_or_else(
                        || {
                            (
                                unknown_effect(&request.tool_name, &normalized_input),
                                0,
                                "missing-descriptor".to_string(),
                            )
                        },
                    );
                let safety_category = ToolSafetyCategory::from_effect(&effect);
                let analysis = analyze_request(request, &effect, safety_category);
                let execution_mode = if !request.depends_on.is_empty() {
                    wave_count += 1;
                    GovernedToolExecutionMode::Wave
                } else {
                    match safety_category {
                        ToolSafetyCategory::ReadOnly => {
                            parallel_read_count += 1;
                            GovernedToolExecutionMode::ParallelRead
                        }
                        ToolSafetyCategory::Destructive => {
                            destructive_count += 1;
                            GovernedToolExecutionMode::SerialDestructive
                        }
                        ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Network => {
                            limited_count += 1;
                            GovernedToolExecutionMode::LimitedParallel
                        }
                    }
                };

                GovernedToolPlanTask {
                    contract_version: GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
                    tool_call_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    normalized_input,
                    idempotency_key: tool_plan_idempotency_key(request),
                    model_visible_name: model_visible_tool_name(&request.tool_name),
                    can_parallelize: matches!(
                        execution_mode,
                        GovernedToolExecutionMode::ParallelRead
                            | GovernedToolExecutionMode::LimitedParallel
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
                        GovernedToolExecutionMode::Wave => 8,
                        _ => safety_category.max_concurrency(),
                    },
                    resource_demand: resource_demand_from_effect(&effect),
                    effect,
                    catalog_revision,
                    descriptor_set_hash,
                }
            })
            .collect::<Vec<_>>();
        annotate_conflicts(&mut tasks);
        for task in &mut tasks {
            task.can_parallelize = task.conflicts.is_empty()
                && matches!(
                    task.execution_mode,
                    GovernedToolExecutionMode::ParallelRead
                        | GovernedToolExecutionMode::LimitedParallel
                );
        }

        let catalog_revision = tasks
            .iter()
            .map(|task| task.catalog_revision)
            .max()
            .unwrap_or(0);
        let mut plan = Self {
            plan_id: format!("tool-plan-{}", Uuid::new_v4()),
            revision: 1,
            catalog_revision,
            task_count: tasks.len(),
            parallel_read_count,
            limited_count,
            destructive_count,
            wave_count,
            tasks,
            batches: Vec::new(),
        };
        plan.batches = build_execution_batches(&plan, requests, decision);
        plan
    }

    #[must_use]
    pub fn projection(&self) -> GovernedToolPlanProjection {
        let invocations = self
            .tasks
            .iter()
            .map(|task| GovernedToolInvocation {
                contract_version: task.contract_version,
                invocation_id: task.tool_call_id.clone(),
                intent: ToolIntent {
                    invocation_id: task.tool_call_id.clone(),
                    tool_name: task.tool_name.clone(),
                    normalized_input: task.normalized_input.clone(),
                },
                effect: task.effect.clone(),
                resource_demand: task.resource_demand.clone(),
                explicit_dependencies: task
                    .depends_on
                    .iter()
                    .map(|depends_on| ToolDependency {
                        invocation_id: task.tool_call_id.clone(),
                        depends_on: depends_on.clone(),
                        reason: "model_explicit_dependency".to_string(),
                    })
                    .collect(),
                catalog_revision: task.catalog_revision,
                descriptor_set_hash: task.descriptor_set_hash.clone(),
                idempotency_key: task.idempotency_key.clone(),
            })
            .collect::<Vec<_>>();
        let dependencies = invocations
            .iter()
            .flat_map(|invocation| invocation.explicit_dependencies.clone())
            .collect();
        GovernedToolPlanProjection {
            contract_version: GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
            plan_id: self.plan_id.clone(),
            revision: self.revision,
            catalog_revision: self.catalog_revision,
            invocations,
            dependencies,
        }
    }

    pub fn apply_execution_decision(
        &mut self,
        requests: &[ToolRequest],
        decision: &RuntimeExecutionDecision,
    ) {
        self.revision = self.revision.saturating_add(1);
        self.batches = build_execution_batches(self, requests, Some(decision));
    }

    #[must_use]
    pub fn validate_against_execution_decision(
        &self,
        decision: &RuntimeExecutionDecision,
    ) -> GovernedToolPolicyValidationReport {
        let mut findings = Vec::new();
        if !decision.executable {
            findings.push("execution_decision_not_executable".to_string());
            return GovernedToolPolicyValidationReport {
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

        GovernedToolPolicyValidationReport {
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
    ) -> SessionDomainEvent {
        let payload = serde_json::json!({
            "plan_id": self.plan_id,
            "plan_revision": self.revision,
            "catalog_revision": self.catalog_revision,
            "task_count": self.task_count,
            "parallel_read_count": self.parallel_read_count,
            "limited_count": self.limited_count,
            "destructive_count": self.destructive_count,
            "wave_count": self.wave_count,
            "tasks": self.tasks,
            "batches": self.batches,
        });
        let mut event = SessionDomainEvent::new(
            session_id,
            sequence,
            SessionDomainScope::Tool,
            "tool.execution_plan.created",
            payload,
            created_at_ms,
        );
        event.status = Some("planned".to_string());
        event.span_id = Some(self.plan_id.clone());
        event.refs = self
            .tasks
            .iter()
            .map(|task| SessionDomainRef {
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
        RuntimeCompileTarget::EvidenceGraph => matches!(
            category,
            ToolSafetyCategory::ReadOnly | ToolSafetyCategory::Network
        ),
        RuntimeCompileTarget::ExecutionGraph => true,
    }
}

fn is_mutation(task: &GovernedToolPlanTask) -> bool {
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

fn has_single_known_mutation_path(tasks: &[&GovernedToolPlanTask]) -> bool {
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

fn unknown_effect(tool_name: &str, input: &Value) -> ToolEffectDescriptor {
    let target = input
        .get("path")
        .or_else(|| input.get("url"))
        .or_else(|| input.get("target"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut hasher = Sha256::new();
    hasher.update(tool_name.as_bytes());
    hasher.update(serde_json::to_vec(input).unwrap_or_default());
    ToolEffectDescriptor {
        tool_id: tool_name.to_string(),
        descriptor_hash: format!("{:x}", hasher.finalize()),
        effect_kind: ToolEffectKind::Unknown,
        idempotency: ToolIdempotency::Unknown,
        scopes: vec![PermissionScope {
            resource: PermissionResource::Tool,
            operation: PermissionOperation::Call,
            target,
        }],
        required_permission: harness_contract::tool::ToolPermissionMode::DangerFullAccess,
        approval_class: harness_contract::tool::ToolApprovalClass::User,
        uses_network: true,
        spawns_process: true,
        mutates_packages: false,
        mutates_system: false,
    }
}

pub(crate) fn resource_demand_from_effect(effect: &ToolEffectDescriptor) -> ResourceDemand {
    let mut scopes = effect
        .scopes
        .iter()
        .filter_map(|scope| {
            scope.target.clone().map(|key| ResourceScopeDemand {
                key,
                access: if scope.operation == PermissionOperation::Read {
                    ResourceAccess::Read
                } else {
                    ResourceAccess::Write
                },
            })
        })
        .collect::<Vec<_>>();
    scopes.sort_by(|left, right| left.key.cmp(&right.key));
    scopes.dedup();
    ResourceDemand {
        tool_slots: 1,
        process_slots: u32::from(effect.spawns_process),
        network_slots: u32::from(effect.uses_network),
        cpu_weight: if effect.spawns_process { 2 } else { 1 },
        memory_bytes: 0,
        scopes,
    }
}

fn build_execution_batches(
    plan: &GovernedToolPlan,
    requests: &[ToolRequest],
    decision: Option<&RuntimeExecutionDecision>,
) -> Vec<GovernedExecutionBatch> {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    let id_to_index = requests
        .iter()
        .enumerate()
        .map(|(index, request)| (request.tool_use_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut memo = HashMap::new();
    let mut waves = BTreeMap::<usize, Vec<usize>>::new();
    for index in 0..plan.tasks.len() {
        let depth = dependency_depth(index, plan, &id_to_index, &mut memo, &mut BTreeSet::new());
        waves.entry(depth).or_default().push(index);
    }
    let direct = decision.is_some_and(|decision| {
        decision.strategy.selected_candidate
            == harness_contract::strategy::ExecutionCandidateKind::Direct
    });
    let mut batches = Vec::new();
    for (depth, indices) in waves {
        if direct {
            for index in indices {
                push_execution_batch(
                    &mut batches,
                    GovernedExecutionBatchMode::SerialStrategy,
                    vec![index],
                    1,
                    format!("direct strategy dependency depth {depth}"),
                    plan,
                );
            }
            continue;
        }

        let mut reads = Vec::new();
        let mut writes = Vec::new();
        let mut network = Vec::new();
        let mut destructive = Vec::new();
        for index in indices {
            match plan.tasks[index].safety_category {
                ToolSafetyCategory::ReadOnly => reads.push(index),
                ToolSafetyCategory::WriteLocal => writes.push(index),
                ToolSafetyCategory::Network => network.push(index),
                ToolSafetyCategory::Destructive => destructive.push(index),
            }
        }
        let mode_for = |normal, dependency| if depth == 0 { normal } else { dependency };
        push_execution_batch(
            &mut batches,
            mode_for(
                GovernedExecutionBatchMode::ParallelRead,
                GovernedExecutionBatchMode::DependencyWave,
            ),
            reads,
            DEFAULT_PARALLEL_TOOL_CONCURRENCY,
            format!("governed read wave {depth}"),
            plan,
        );
        push_execution_batch(
            &mut batches,
            mode_for(
                GovernedExecutionBatchMode::LimitedNetwork,
                GovernedExecutionBatchMode::DependencyWave,
            ),
            network,
            3,
            format!("governed network wave {depth}"),
            plan,
        );
        push_execution_batch(
            &mut batches,
            mode_for(
                GovernedExecutionBatchMode::LimitedWrite,
                GovernedExecutionBatchMode::DependencyWave,
            ),
            writes,
            4,
            format!("governed mutation wave {depth}"),
            plan,
        );
        for index in destructive {
            push_execution_batch(
                &mut batches,
                GovernedExecutionBatchMode::SerialDestructive,
                vec![index],
                1,
                format!("governed destructive wave {depth}"),
                plan,
            );
        }
    }
    batches
}

fn dependency_depth<'a>(
    index: usize,
    plan: &GovernedToolPlan,
    id_to_index: &std::collections::BTreeMap<&'a str, usize>,
    memo: &mut std::collections::HashMap<usize, usize>,
    visiting: &mut std::collections::BTreeSet<usize>,
) -> usize {
    if let Some(depth) = memo.get(&index) {
        return *depth;
    }
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
                        dependency_depth(*dependency_index, plan, id_to_index, memo, visiting)
                    })
            })
            .max()
            .map_or(0, |depth| depth.saturating_add(1))
    });
    visiting.remove(&index);
    memo.insert(index, depth);
    depth
}

fn push_execution_batch(
    batches: &mut Vec<GovernedExecutionBatch>,
    mode: GovernedExecutionBatchMode,
    indices: Vec<usize>,
    max_concurrency: usize,
    reason: String,
    plan: &GovernedToolPlan,
) {
    if indices.is_empty() {
        return;
    }
    let mut groups = std::collections::BTreeMap::<String, Vec<usize>>::new();
    for index in &indices {
        let task = &plan.tasks[*index];
        let scope = if task.resource_scope.unknown {
            "unknown".to_string()
        } else if task.resource_scope.network {
            "network".to_string()
        } else if task.resource_scope.paths.is_empty() {
            task.resource_scope.kind.clone()
        } else {
            task.resource_scope.paths.join("|")
        };
        groups.entry(scope).or_default().push(*index);
    }
    batches.push(GovernedExecutionBatch {
        mode,
        indices,
        max_concurrency,
        reason,
        scope_groups: groups
            .into_iter()
            .map(|(scope, indices)| GovernedExecutionScopeGroup { scope, indices })
            .collect(),
    });
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
    effect: &ToolEffectDescriptor,
    safety_category: ToolSafetyCategory,
) -> ToolRequestAnalysis {
    let purity = match (effect.effect_kind, effect.idempotency) {
        (ToolEffectKind::Read, ToolIdempotency::Idempotent) => ToolPurity::ReadOnlyIdempotent,
        (ToolEffectKind::Write, _) => ToolPurity::LocalMutation,
        (ToolEffectKind::Network, _) => ToolPurity::Network,
        (
            ToolEffectKind::Process
            | ToolEffectKind::Package
            | ToolEffectKind::System
            | ToolEffectKind::Destructive,
            _,
        ) => ToolPurity::RuntimeSideEffect,
        _ => ToolPurity::Unknown,
    };
    let resource_scope = resource_scope_from_effect(effect);
    let authority_set = effect
        .scopes
        .iter()
        .map(|scope| format!("{:?}.{:?}", scope.resource, scope.operation).to_ascii_lowercase())
        .collect();
    let side_effect_class = match purity {
        ToolPurity::ReadOnlyIdempotent => "none",
        ToolPurity::LocalMutation => "local_mutation",
        ToolPurity::Network => "network",
        ToolPurity::RuntimeSideEffect => "runtime_side_effect",
        ToolPurity::Unknown => "unknown",
    }
    .to_string();
    let output_budget_class = if request
        .tool_name
        .eq_ignore_ascii_case("tool_batch_readonly")
    {
        "batch"
    } else if effect.effect_kind == ToolEffectKind::Read {
        "normal"
    } else {
        "mutation"
    }
    .to_string();
    let reason = format!(
        "{} tool planned as {} with {} resource scope",
        side_effect_class,
        GovernedToolExecutionMode::from_safety_and_deps(
            safety_category,
            !request.depends_on.is_empty()
        )
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

impl GovernedToolExecutionMode {
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

pub(crate) fn resource_scope_from_effect(effect: &ToolEffectDescriptor) -> ToolResourceScope {
    if effect.effect_kind == ToolEffectKind::Unknown {
        return ToolResourceScope::unknown();
    }
    if effect.uses_network {
        return ToolResourceScope::network();
    }
    if matches!(
        effect.effect_kind,
        ToolEffectKind::Process
            | ToolEffectKind::Package
            | ToolEffectKind::System
            | ToolEffectKind::Destructive
    ) {
        return ToolResourceScope::runtime();
    }
    let mut paths = effect
        .scopes
        .iter()
        .filter_map(|scope| scope.target.as_deref())
        .map(normalize_resource_path)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        ToolResourceScope::workspace()
    } else {
        ToolResourceScope::paths(paths)
    }
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
    format!("tool-plan-task:v{GOVERNED_TOOL_PLAN_CONTRACT_VERSION}:{digest}")
}

fn model_visible_tool_name(tool_name: &str) -> String {
    tool_name.trim().replace('_', " ")
}

fn annotate_conflicts(tasks: &mut [GovernedToolPlanTask]) {
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
    left: &GovernedToolPlanTask,
    right: &GovernedToolPlanTask,
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
fn fixture_effect(tool_name: &str, input: &Value) -> ToolEffectDescriptor {
    let category =
        crate::classify_tool_request(tool_name, &serde_json::to_string(input).unwrap_or_default());
    let mut effect = unknown_effect(tool_name, input);
    let normalized = tool_name.trim().replace('-', "_").to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "todo_write" | "todowrite" | "runtime_orchestrate" | "runtimeorchestrate"
    ) {
        effect.effect_kind = ToolEffectKind::System;
        effect.idempotency = ToolIdempotency::IdempotentWithKey;
        effect.uses_network = false;
        effect.spawns_process = false;
        effect.scopes = vec![PermissionScope {
            resource: PermissionResource::Tool,
            operation: PermissionOperation::Control,
            target: None,
        }];
        return effect;
    }
    effect.effect_kind = match category {
        ToolSafetyCategory::ReadOnly => ToolEffectKind::Read,
        ToolSafetyCategory::WriteLocal => ToolEffectKind::Write,
        ToolSafetyCategory::Network => ToolEffectKind::Network,
        ToolSafetyCategory::Destructive => ToolEffectKind::Destructive,
    };
    effect.idempotency = if category == ToolSafetyCategory::ReadOnly {
        ToolIdempotency::Idempotent
    } else {
        ToolIdempotency::IdempotentWithKey
    };
    effect.uses_network = category == ToolSafetyCategory::Network;
    effect.spawns_process = category == ToolSafetyCategory::Destructive;
    effect.required_permission = match category {
        ToolSafetyCategory::ReadOnly => harness_contract::tool::ToolPermissionMode::ReadOnly,
        ToolSafetyCategory::WriteLocal => {
            harness_contract::tool::ToolPermissionMode::WorkspaceWrite
        }
        ToolSafetyCategory::Network | ToolSafetyCategory::Destructive => {
            harness_contract::tool::ToolPermissionMode::DangerFullAccess
        }
    };
    effect.approval_class = if category == ToolSafetyCategory::ReadOnly {
        harness_contract::tool::ToolApprovalClass::None
    } else {
        harness_contract::tool::ToolApprovalClass::Policy
    };
    effect.scopes = vec![PermissionScope {
        resource: match category {
            ToolSafetyCategory::ReadOnly | ToolSafetyCategory::WriteLocal => {
                PermissionResource::File
            }
            ToolSafetyCategory::Network => PermissionResource::Network,
            ToolSafetyCategory::Destructive => PermissionResource::Tool,
        },
        operation: match category {
            ToolSafetyCategory::ReadOnly => PermissionOperation::Read,
            ToolSafetyCategory::WriteLocal => PermissionOperation::Write,
            ToolSafetyCategory::Network => PermissionOperation::Call,
            ToolSafetyCategory::Destructive => PermissionOperation::Execute,
        },
        target: input
            .get("path")
            .or_else(|| input.get("file"))
            .or_else(|| input.get("file_path"))
            .or_else(|| input.get("url"))
            .and_then(Value::as_str)
            .map(str::to_string),
    }];
    effect
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
        let plan = GovernedToolPlan::from_requests(&[
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
            GovernedToolExecutionMode::ParallelRead
        );
        assert_eq!(
            plan.tasks[0].contract_version,
            GOVERNED_TOOL_PLAN_CONTRACT_VERSION
        );
        assert!(plan.tasks[0]
            .idempotency_key
            .starts_with("tool-plan-task:v3:"));
        assert_eq!(plan.tasks[0].model_visible_name, "read");
        assert!(!plan.tasks[0].can_parallelize);
        assert_eq!(
            plan.tasks[2].execution_mode,
            GovernedToolExecutionMode::SerialDestructive
        );
        assert!(!plan.tasks[2].can_parallelize);
    }

    #[test]
    fn dependency_tasks_are_planned_as_wave_tasks() {
        let plan = GovernedToolPlan::from_requests(&[request(
            "write-2",
            "write",
            vec!["read-1".to_string()],
        )]);

        assert_eq!(plan.wave_count, 1);
        assert_eq!(
            plan.tasks[0].execution_mode,
            GovernedToolExecutionMode::Wave
        );
        assert_eq!(plan.tasks[0].max_concurrency, 8);
    }

    #[test]
    fn tool_contract_readonly_batch_can_parallelize_without_conflicts() {
        let plan = GovernedToolPlan::from_requests(&[
            request("read-1", "read_file", Vec::new()),
            request("read-2", "grep_search", Vec::new()),
        ]);

        assert!(plan.tasks.iter().all(|task| task.can_parallelize));
        assert!(plan
            .tasks
            .iter()
            .all(|task| task.contract_version == GOVERNED_TOOL_PLAN_CONTRACT_VERSION));
    }

    #[test]
    fn plan_event_refs_all_tool_calls() {
        let plan = GovernedToolPlan::from_requests(&[
            request("read-1", "read", Vec::new()),
            request("write-1", "write", Vec::new()),
        ]);
        let event = plan.to_runtime_event("session-1", 7, 123);

        assert_eq!(event.scope, SessionDomainScope::Tool);
        assert_eq!(event.kind, "tool.execution_plan.created");
        assert_eq!(event.status.as_deref(), Some("planned"));
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.payload["task_count"], 2);
        assert_eq!(event.payload["tasks"][0]["contract_version"], 3);
        assert!(event.payload["tasks"][0]["idempotency_key"]
            .as_str()
            .unwrap()
            .starts_with("tool-plan-task:v3:"));
    }

    #[test]
    fn plan_records_resource_scope_and_authority_metadata() {
        let plan = GovernedToolPlan::from_requests(&[
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
        assert_eq!(plan.tasks[0].authority_set, vec!["file.read"]);
        assert_eq!(plan.tasks[0].output_budget_class, "normal");
        assert_eq!(plan.tasks[1].purity, ToolPurity::Network);
        assert_eq!(plan.tasks[1].resource_scope.kind, "network");
        assert_eq!(plan.tasks[1].authority_set, vec!["network.call"]);
    }

    #[test]
    fn read_file_accepts_the_registered_file_path_argument_alias() {
        let plan = GovernedToolPlan::from_requests(&[ToolRequest {
            tool_use_id: "read-file-path".to_string(),
            tool_name: "read_file".to_string(),
            input: r#"{"file_path":"crates/runtime/src/lib.rs"}"#.to_string(),
            depends_on: Vec::new(),
        }]);

        assert_eq!(plan.tasks[0].purity, ToolPurity::ReadOnlyIdempotent);
        assert_eq!(plan.tasks[0].resource_scope.kind, "paths");
        assert_eq!(
            plan.tasks[0].resource_scope.paths,
            vec!["crates/runtime/src/lib.rs"]
        );
    }

    #[test]
    fn plan_marks_overlapping_write_conflicts() {
        let plan = GovernedToolPlan::from_requests(&[
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
    fn independent_file_writes_remain_parallelizable() {
        let plan = GovernedToolPlan::from_requests(&[
            ToolRequest {
                tool_use_id: "write-a".to_string(),
                tool_name: "write_file".to_string(),
                input: r#"{"path":"src/a.rs","content":"a"}"#.to_string(),
                depends_on: Vec::new(),
            },
            ToolRequest {
                tool_use_id: "write-b".to_string(),
                tool_name: "write_file".to_string(),
                input: r#"{"path":"src/b.rs","content":"b"}"#.to_string(),
                depends_on: Vec::new(),
            },
        ]);

        assert!(plan.tasks.iter().all(|task| task.conflicts.is_empty()));
        assert!(plan.tasks.iter().all(|task| task.can_parallelize));
    }

    #[test]
    fn read_only_batch_tools_get_batch_budget_class() {
        let plan = GovernedToolPlan::from_requests(&[ToolRequest {
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
            GovernedToolPlan::from_requests(&[request("read-1", "read_file", Vec::new())]);
        let network_plan =
            GovernedToolPlan::from_requests(&[request("network-1", "WebSearch", Vec::new())]);
        let write_plan = GovernedToolPlan::from_requests(&[request_with_input(
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
    }

    #[test]
    fn execution_graph_mutation_requires_typed_gates_and_modifiers() {
        let plan = GovernedToolPlan::from_requests(&[request_with_input(
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
            GovernedToolPlan::from_requests(&[request("network-1", "WebSearch", Vec::new())]);
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
        let plan = GovernedToolCompiler.compile(
            &[request("plugin-read", "company_catalog_lookup", Vec::new())],
            None,
            |name, input| {
                let mut effect = fixture_effect(name, input);
                effect.effect_kind = ToolEffectKind::Read;
                effect.idempotency = ToolIdempotency::Idempotent;
                effect.required_permission = harness_contract::tool::ToolPermissionMode::ReadOnly;
                effect.uses_network = false;
                effect.spawns_process = false;
                Some((effect, 1, "plugin-test".to_string()))
            },
        );

        assert_eq!(plan.tasks[0].safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(
            plan.tasks[0].execution_mode,
            GovernedToolExecutionMode::ParallelRead
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
        let one_path = GovernedToolPlan::from_requests(&[
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

        let multiple_paths = GovernedToolPlan::from_requests(&[
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
            GovernedToolPlan::from_requests(&[request("custom-1", "custom_mutation", Vec::new())]);
        assert_eq!(
            unknown_path
                .validate_against_execution_decision(&decision)
                .findings,
            vec!["bounded_change_requires_single_known_path_scope"]
        );
    }

    #[test]
    fn runtime_entry_tools_defer_to_their_inner_validators() {
        let plan = GovernedToolPlan::from_requests(&[
            request("capabilities-1", "RuntimeCapabilities", Vec::new()),
            request("orchestrate-1", "runtime_orchestrate", Vec::new()),
        ]);
        let decision =
            execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);

        assert!(plan.validate_against_execution_decision(&decision).allowed);
    }

    #[test]
    fn runtime_state_updates_do_not_claim_a_workspace_write_scope() {
        let plan = GovernedToolPlan::from_requests(&[
            request("todo-1", "TodoWrite", Vec::new()),
            request("orchestrate-1", "runtime_orchestrate", Vec::new()),
        ]);

        assert!(plan
            .tasks
            .iter()
            .all(|task| task.resource_scope.kind == "runtime"));
        assert!(plan
            .tasks
            .iter()
            .all(|task| task.resource_scope.paths.is_empty()));
    }

    #[test]
    fn non_executable_decision_is_rejected_with_serializable_lease_report() {
        let plan = GovernedToolPlan::from_requests(&[request("read-1", "read_file", Vec::new())]);
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
        let plan = GovernedToolPlan::from_requests(&[
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
