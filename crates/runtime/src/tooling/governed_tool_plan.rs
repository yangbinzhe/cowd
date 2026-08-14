//! Fail-closed compiler for governed tool dependency graphs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;

use harness_contract::core::{ExecutionModifier, ExecutionPolicyGate, TaskRisk};
use harness_contract::policy::PermissionOperation;
#[cfg(test)]
use harness_contract::policy::{PermissionResource, PermissionScope};
use harness_contract::tool::{
    GovernedToolInvocation, GovernedToolPlanProjection, ResourceAccess, ResourceDemand,
    ResourceScopeDemand, ToolDependency, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
    ToolIntent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::execution_core::{RuntimeCompileTarget, RuntimeExecutionDecision};
use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;
use crate::{RuntimeSessionEvent, RuntimeSessionEventKind, RuntimeSessionEventRef};

pub const GOVERNED_TOOL_PLAN_CONTRACT_VERSION: u32 = 4;
pub const DEFAULT_PARALLEL_TOOL_CONCURRENCY: usize = 42;

/// Effective default parallel tool concurrency (P11). Operators may override
/// the 42 default with `COWD_TOOL_PARALLEL_CEILING`; the dynamic elevation to
/// explicit proposal width is preserved.
#[must_use]
pub fn default_parallel_tool_concurrency() -> usize {
    std::env::var("COWD_TOOL_PARALLEL_CEILING")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=256).contains(value))
        .unwrap_or(DEFAULT_PARALLEL_TOOL_CONCURRENCY)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernedToolExecutionMode {
    ParallelRead,
    LimitedParallel,
    SerialDestructive,
    DependencyReady,
}

impl GovernedToolExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParallelRead => "parallel_read",
            Self::LimitedParallel => "limited_parallel",
            Self::SerialDestructive => "serial_destructive",
            Self::DependencyReady => "dependency_ready",
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernedToolPlanTask {
    pub original_call_index: usize,
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
    pub invocation: GovernedToolInvocation,
    pub predecessors: Vec<usize>,
    pub successors: Vec<usize>,
    pub indegree: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidatedGovernedToolDag {
    pub plan_id: String,
    pub revision: u64,
    pub catalog_revision: u64,
    pub topology_hash: String,
    pub task_count: usize,
    pub parallel_read_count: usize,
    pub limited_count: usize,
    pub destructive_count: usize,
    pub tasks: Vec<GovernedToolPlanTask>,
    pub topological_order: Vec<usize>,
}

pub type GovernedToolPlan = ValidatedGovernedToolDag;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedToolCompileRejection {
    pub tool_call_id: String,
    pub tool_name: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernedToolCompilation {
    pub plan: Option<ValidatedGovernedToolDag>,
    pub rejected: Vec<GovernedToolCompileRejection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GovernedToolCompileError {
    #[error("tool call at index {index} has an empty task id")]
    EmptyTaskId { index: usize },
    #[error("tool call at index {index} has non-canonical task id `{task_id}`: {reason}")]
    InvalidTaskId {
        index: usize,
        task_id: String,
        reason: String,
    },
    #[error("duplicate tool task id `{task_id}` at indices {first_index} and {duplicate_index}")]
    DuplicateTaskId {
        task_id: String,
        first_index: usize,
        duplicate_index: usize,
    },
    #[error("tool task `{task_id}` depends on unknown task `{dependency_id}`")]
    UnknownDependency {
        task_id: String,
        dependency_id: String,
    },
    #[error("tool task `{task_id}` depends on itself")]
    SelfDependency { task_id: String },
    #[error("tool dependency graph contains a cycle involving {task_ids:?}")]
    Cycle { task_ids: Vec<String> },
    #[error("registered descriptor is missing for tool task `{task_id}` (`{tool_name}`)")]
    MissingDescriptor { task_id: String, tool_name: String },
    #[error("tool task `{task_id}` has invalid normalized scope `{scope}`: {reason}")]
    InvalidNormalizedScope {
        task_id: String,
        scope: String,
        reason: String,
    },
    #[error(
        "tool task `{task_id}` depends on rejected task `{dependency_id}`: {rejection_reason}"
    )]
    DependencyReferencesRejectedTask {
        task_id: String,
        dependency_id: String,
        rejection_reason: String,
    },
    #[error("tool task `{task_id}` has invalid JSON input: {reason}")]
    InvalidInput { task_id: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedToolPolicyValidationReport {
    pub allowed: bool,
    pub findings: Vec<String>,
    pub lease_id: String,
    pub requires_approval: bool,
    pub requires_checkpoint: bool,
    pub checkpoint_created: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GovernedToolCompiler;

impl GovernedToolCompiler {
    pub fn compile(
        &self,
        workspace_root: &Path,
        requests: &[ToolRequest],
        describe_registered_tool: impl Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    ) -> Result<ValidatedGovernedToolDag, GovernedToolCompileError> {
        ValidatedGovernedToolDag::compile(workspace_root, requests, describe_registered_tool)
    }

    /// Compile every valid node while isolating malformed nodes, their
    /// descendants, and cycle-affected subgraphs. Graph identity errors remain
    /// whole-batch failures because no unambiguous dependency graph exists.
    pub fn compile_partial<F>(
        &self,
        workspace_root: &Path,
        requests: &[ToolRequest],
        describe_registered_tool: F,
    ) -> Result<GovernedToolCompilation, GovernedToolCompileError>
    where
        F: Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    {
        let id_to_index = validate_task_ids(requests)?;
        let mut rejected = BTreeMap::<usize, String>::new();

        for (index, request) in requests.iter().enumerate() {
            for dependency in &request.depends_on {
                if dependency == &request.tool_use_id {
                    rejected.entry(index).or_insert_with(|| {
                        GovernedToolCompileError::SelfDependency {
                            task_id: request.tool_use_id.clone(),
                        }
                        .to_string()
                    });
                } else if !id_to_index.contains_key(dependency.as_str()) {
                    rejected.entry(index).or_insert_with(|| {
                        GovernedToolCompileError::UnknownDependency {
                            task_id: request.tool_use_id.clone(),
                            dependency_id: dependency.clone(),
                        }
                        .to_string()
                    });
                }
            }

            let normalized_input = match serde_json::from_str::<Value>(&request.input) {
                Ok(input) => canonical_json(input),
                Err(error) => {
                    rejected.entry(index).or_insert_with(|| {
                        GovernedToolCompileError::InvalidInput {
                            task_id: request.tool_use_id.clone(),
                            reason: error.to_string(),
                        }
                        .to_string()
                    });
                    continue;
                }
            };
            let Some((effect, _, _)) =
                describe_registered_tool(&request.tool_name, &normalized_input)
            else {
                rejected.entry(index).or_insert_with(|| {
                    GovernedToolCompileError::MissingDescriptor {
                        task_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                    }
                    .to_string()
                });
                continue;
            };
            if let Err((scope, reason)) = normalized_resource_scope(&effect, workspace_root) {
                rejected.entry(index).or_insert_with(|| {
                    GovernedToolCompileError::InvalidNormalizedScope {
                        task_id: request.tool_use_id.clone(),
                        scope,
                        reason,
                    }
                    .to_string()
                });
            }
        }

        propagate_rejected_dependencies(requests, &id_to_index, &mut rejected);

        let mut indegree = vec![0_usize; requests.len()];
        let mut successors = vec![Vec::<usize>::new(); requests.len()];
        for (index, request) in requests.iter().enumerate() {
            if rejected.contains_key(&index) {
                continue;
            }
            for dependency in &request.depends_on {
                let dependency_index = id_to_index[dependency.as_str()];
                if rejected.contains_key(&dependency_index) {
                    continue;
                }
                indegree[index] = indegree[index].saturating_add(1);
                successors[dependency_index].push(index);
            }
        }
        let mut ready = indegree
            .iter()
            .enumerate()
            .filter_map(|(index, degree)| {
                (!rejected.contains_key(&index) && *degree == 0).then_some(index)
            })
            .collect::<VecDeque<_>>();
        let mut visited = BTreeSet::new();
        while let Some(index) = ready.pop_front() {
            if !visited.insert(index) {
                continue;
            }
            for successor in successors[index].iter().copied() {
                indegree[successor] = indegree[successor].saturating_sub(1);
                if indegree[successor] == 0 {
                    ready.push_back(successor);
                }
            }
        }
        for index in 0..requests.len() {
            if !rejected.contains_key(&index) && !visited.contains(&index) {
                rejected.insert(
                    index,
                    "tool dependency cycle or a dependency blocked by that cycle".to_string(),
                );
            }
        }

        let accepted_indices = (0..requests.len())
            .filter(|index| !rejected.contains_key(index))
            .collect::<Vec<_>>();
        let accepted_requests = accepted_indices
            .iter()
            .map(|index| requests[*index].clone())
            .collect::<Vec<_>>();
        let plan = if accepted_requests.is_empty() {
            None
        } else {
            let mut plan = ValidatedGovernedToolDag::compile(
                workspace_root,
                &accepted_requests,
                |name, input| describe_registered_tool(name, input),
            )?;
            for task in &mut plan.tasks {
                task.original_call_index = accepted_indices[task.original_call_index];
            }
            Some(plan)
        };
        let rejected = rejected
            .into_iter()
            .map(|(index, reason)| GovernedToolCompileRejection {
                tool_call_id: requests[index].tool_use_id.clone(),
                tool_name: requests[index].tool_name.clone(),
                reason,
            })
            .collect();
        Ok(GovernedToolCompilation { plan, rejected })
    }
}

fn propagate_rejected_dependencies(
    requests: &[ToolRequest],
    id_to_index: &BTreeMap<&str, usize>,
    rejected: &mut BTreeMap<usize, String>,
) {
    loop {
        let mut changed = false;
        for (index, request) in requests.iter().enumerate() {
            if rejected.contains_key(&index) {
                continue;
            }
            if let Some((dependency, reason)) = request.depends_on.iter().find_map(|dependency| {
                let dependency_index = id_to_index.get(dependency.as_str()).copied()?;
                rejected
                    .get(&dependency_index)
                    .map(|reason| (dependency.clone(), reason.clone()))
            }) {
                rejected.insert(
                    index,
                    GovernedToolCompileError::DependencyReferencesRejectedTask {
                        task_id: request.tool_use_id.clone(),
                        dependency_id: dependency,
                        rejection_reason: reason,
                    }
                    .to_string(),
                );
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
}

impl ValidatedGovernedToolDag {
    /// Test-only fixture compiler. Production has no descriptor fallback.
    #[cfg(test)]
    #[must_use]
    pub fn from_requests(requests: &[ToolRequest]) -> Self {
        let workspace = std::env::current_dir().expect("test workspace");
        Self::compile(&workspace, requests, |name, input| {
            Some((fixture_effect(name, input), 1, "test-fixture".to_string()))
        })
        .expect("fixture requests form a valid governed tool DAG")
    }

    fn compile(
        workspace_root: &Path,
        requests: &[ToolRequest],
        describe_registered_tool: impl Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    ) -> Result<Self, GovernedToolCompileError> {
        let id_to_index = validate_task_ids(requests)?;
        validate_dependencies(requests, &id_to_index)?;

        let mut parallel_read_count = 0;
        let mut limited_count = 0;
        let mut destructive_count = 0;
        let mut rejected = BTreeMap::<String, String>::new();
        let mut first_rejection = None;
        let mut prepared = Vec::with_capacity(requests.len());

        for request in requests {
            let normalized_input = match serde_json::from_str::<Value>(&request.input) {
                Ok(input) => canonical_json(input),
                Err(error) => {
                    let reason = error.to_string();
                    rejected.insert(
                        request.tool_use_id.clone(),
                        format!("invalid JSON input: {reason}"),
                    );
                    first_rejection.get_or_insert_with(|| GovernedToolCompileError::InvalidInput {
                        task_id: request.tool_use_id.clone(),
                        reason,
                    });
                    continue;
                }
            };
            let Some((effect, catalog_revision, descriptor_set_hash)) =
                describe_registered_tool(&request.tool_name, &normalized_input)
            else {
                rejected.insert(
                    request.tool_use_id.clone(),
                    format!("missing descriptor for `{}`", request.tool_name),
                );
                first_rejection.get_or_insert_with(|| {
                    GovernedToolCompileError::MissingDescriptor {
                        task_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                    }
                });
                continue;
            };
            let resource_scope = match normalized_resource_scope(&effect, workspace_root) {
                Ok(scope) => scope,
                Err((scope, reason)) => {
                    rejected.insert(
                        request.tool_use_id.clone(),
                        format!("invalid normalized scope `{scope}`: {reason}"),
                    );
                    first_rejection.get_or_insert_with(|| {
                        GovernedToolCompileError::InvalidNormalizedScope {
                            task_id: request.tool_use_id.clone(),
                            scope,
                            reason,
                        }
                    });
                    continue;
                }
            };
            prepared.push((
                normalized_input,
                effect,
                catalog_revision,
                descriptor_set_hash,
                resource_scope,
            ));
        }

        if !rejected.is_empty() {
            for request in requests {
                for dependency in &request.depends_on {
                    if let Some(reason) = rejected.get(dependency) {
                        return Err(GovernedToolCompileError::DependencyReferencesRejectedTask {
                            task_id: request.tool_use_id.clone(),
                            dependency_id: dependency.clone(),
                            rejection_reason: reason.clone(),
                        });
                    }
                }
            }
            return Err(first_rejection.expect("rejected task records a typed compile error"));
        }

        let mut predecessors = vec![Vec::<usize>::new(); requests.len()];
        let mut successors = vec![Vec::<usize>::new(); requests.len()];
        for (index, request) in requests.iter().enumerate() {
            let mut dependency_indices = request
                .depends_on
                .iter()
                .map(|dependency| id_to_index[dependency.as_str()])
                .collect::<Vec<_>>();
            dependency_indices.sort_unstable();
            dependency_indices.dedup();
            predecessors[index] = dependency_indices.clone();
            for dependency_index in dependency_indices {
                successors[dependency_index].push(index);
            }
        }
        for dependent_indices in &mut successors {
            dependent_indices.sort_unstable();
            dependent_indices.dedup();
        }
        let topological_order =
            deterministic_topological_order(requests, &predecessors, &successors)?;

        let mut tasks = Vec::with_capacity(requests.len());
        for (original_call_index, (request, prepared)) in requests.iter().zip(prepared).enumerate()
        {
            let (normalized_input, effect, catalog_revision, descriptor_set_hash, resource_scope) =
                prepared;
            let safety_category = ToolSafetyCategory::from_effect(&effect);
            let analysis =
                analyze_request(request, &effect, safety_category, resource_scope.clone());
            let execution_mode = GovernedToolExecutionMode::from_safety_and_deps(
                safety_category,
                !predecessors[original_call_index].is_empty(),
            );
            match safety_category {
                ToolSafetyCategory::ReadOnly => parallel_read_count += 1,
                ToolSafetyCategory::Destructive => destructive_count += 1,
                ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Network => {
                    limited_count += 1;
                }
            }
            let resource_demand = resource_demand_from_effect(&effect);
            let invocation = GovernedToolInvocation {
                contract_version: GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
                invocation_id: request.tool_use_id.clone(),
                intent: ToolIntent {
                    invocation_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    normalized_input: normalized_input.clone(),
                },
                effect: effect.clone(),
                resource_demand: resource_demand.clone(),
                explicit_dependencies: request
                    .depends_on
                    .iter()
                    .map(|depends_on| ToolDependency {
                        invocation_id: request.tool_use_id.clone(),
                        depends_on: depends_on.clone(),
                        reason: "model_explicit_dependency".to_string(),
                    })
                    .collect(),
                compiled_dependencies: Vec::new(),
                catalog_revision,
                descriptor_set_hash: descriptor_set_hash.clone(),
                idempotency_key: tool_plan_idempotency_key(request),
            };
            tasks.push(GovernedToolPlanTask {
                original_call_index,
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
                resource_scope,
                authority_set: analysis.authority_set,
                side_effect_class: analysis.side_effect_class,
                output_budget_class: analysis.output_budget_class,
                conflicts: Vec::new(),
                reason: analysis.reason,
                execution_mode,
                depends_on: request.depends_on.clone(),
                max_concurrency: safety_category.max_concurrency(),
                effect,
                resource_demand,
                catalog_revision,
                descriptor_set_hash,
                invocation,
                predecessors: predecessors[original_call_index].clone(),
                successors: successors[original_call_index].clone(),
                indegree: predecessors[original_call_index].len(),
            });
        }
        annotate_conflicts(&mut tasks);
        compile_conflict_dependencies(&mut tasks, &topological_order);
        for task in &mut tasks {
            // Deterministic conflicts are executable dependency edges now.
            // Keeping the entire task non-parallel merely because it has one
            // ordered conflict would also serialize it against unrelated
            // resources and throw away valid concurrency.
            task.can_parallelize = task.safety_category != ToolSafetyCategory::Destructive;
        }
        let predecessors = tasks
            .iter()
            .map(|task| task.predecessors.clone())
            .collect::<Vec<_>>();
        let successors = tasks
            .iter()
            .map(|task| task.successors.clone())
            .collect::<Vec<_>>();
        let topological_order =
            deterministic_topological_order(requests, &predecessors, &successors)?;

        let catalog_revision = tasks
            .iter()
            .map(|task| task.catalog_revision)
            .max()
            .unwrap_or(0);
        let topology_hash = deterministic_topology_hash(&tasks, &topological_order);
        Ok(Self {
            plan_id: format!("tool-dag-{}", Uuid::new_v4()),
            revision: 1,
            catalog_revision,
            topology_hash,
            task_count: tasks.len(),
            parallel_read_count,
            limited_count,
            destructive_count,
            tasks,
            topological_order,
        })
    }

    #[must_use]
    pub fn projection(&self) -> GovernedToolPlanProjection {
        let invocations = self
            .tasks
            .iter()
            .map(|task| task.invocation.clone())
            .collect::<Vec<_>>();
        let dependencies = invocations
            .iter()
            .flat_map(|invocation| {
                invocation
                    .explicit_dependencies
                    .iter()
                    .chain(&invocation.compiled_dependencies)
                    .cloned()
            })
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
        let requires_approval =
            has_mutation && decision.gates().contains(&ExecutionPolicyGate::Approval);
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
                push_finding(&mut findings, "critical_mutation_requires_approval");
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
            checkpoint_created: false,
        }
    }

    #[must_use]
    pub fn to_runtime_event(
        &self,
        session_id: impl Into<String>,
        sequence: usize,
        created_at_ms: u64,
    ) -> RuntimeSessionEvent {
        let payload = serde_json::json!({
            "plan_id": self.plan_id,
            "plan_revision": self.revision,
            "catalog_revision": self.catalog_revision,
            "topology_hash": self.topology_hash,
            "task_count": self.task_count,
            "parallel_read_count": self.parallel_read_count,
            "limited_count": self.limited_count,
            "destructive_count": self.destructive_count,
            "tasks": self.tasks,
            "topological_order": self.topological_order,
        });
        let mut event = RuntimeSessionEvent::new(
            session_id,
            sequence,
            RuntimeSessionEventKind::ToolExecutionPlanCreated,
            payload,
            created_at_ms,
        );
        event.status = Some("planned".to_string());
        event.span_id = Some(self.plan_id.clone());
        event.refs = self
            .tasks
            .iter()
            .map(|task| RuntimeSessionEventRef {
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

#[cfg(test)]
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
        assessment: harness_contract::policy::EffectAssessment::default(),
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

fn validate_task_ids<'a>(
    requests: &'a [ToolRequest],
) -> Result<BTreeMap<&'a str, usize>, GovernedToolCompileError> {
    let mut ids = BTreeMap::new();
    for (index, request) in requests.iter().enumerate() {
        let task_id = request.tool_use_id.trim();
        if task_id.is_empty() {
            return Err(GovernedToolCompileError::EmptyTaskId { index });
        }
        if task_id != request.tool_use_id {
            return Err(GovernedToolCompileError::InvalidTaskId {
                index,
                task_id: request.tool_use_id.clone(),
                reason: "leading or trailing whitespace is not allowed".to_string(),
            });
        }
        if let Some(first_index) = ids.insert(task_id, index) {
            return Err(GovernedToolCompileError::DuplicateTaskId {
                task_id: task_id.to_string(),
                first_index,
                duplicate_index: index,
            });
        }
    }
    Ok(ids)
}

fn validate_dependencies(
    requests: &[ToolRequest],
    id_to_index: &BTreeMap<&str, usize>,
) -> Result<(), GovernedToolCompileError> {
    for request in requests {
        for dependency in &request.depends_on {
            if dependency == &request.tool_use_id {
                return Err(GovernedToolCompileError::SelfDependency {
                    task_id: request.tool_use_id.clone(),
                });
            }
            if !id_to_index.contains_key(dependency.as_str()) {
                return Err(GovernedToolCompileError::UnknownDependency {
                    task_id: request.tool_use_id.clone(),
                    dependency_id: dependency.clone(),
                });
            }
        }
    }
    Ok(())
}

fn deterministic_topological_order(
    requests: &[ToolRequest],
    predecessors: &[Vec<usize>],
    successors: &[Vec<usize>],
) -> Result<Vec<usize>, GovernedToolCompileError> {
    let mut remaining_indegree = predecessors.iter().map(Vec::len).collect::<Vec<_>>();
    let mut ready = remaining_indegree
        .iter()
        .enumerate()
        .filter_map(|(index, indegree)| (*indegree == 0).then_some(index))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(requests.len());
    while let Some(index) = ready.pop_first() {
        order.push(index);
        for successor in &successors[index] {
            remaining_indegree[*successor] = remaining_indegree[*successor].saturating_sub(1);
            if remaining_indegree[*successor] == 0 {
                ready.insert(*successor);
            }
        }
    }
    if order.len() == requests.len() {
        return Ok(order);
    }
    let task_ids = remaining_indegree
        .iter()
        .enumerate()
        .filter_map(|(index, indegree)| {
            (*indegree > 0).then(|| requests[index].tool_use_id.clone())
        })
        .collect();
    Err(GovernedToolCompileError::Cycle { task_ids })
}

fn deterministic_topology_hash(
    tasks: &[GovernedToolPlanTask],
    topological_order: &[usize],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(GOVERNED_TOOL_PLAN_CONTRACT_VERSION.to_be_bytes());
    for index in topological_order {
        let task = &tasks[*index];
        hasher.update(task.original_call_index.to_be_bytes());
        hash_text(&mut hasher, &task.tool_call_id);
        hash_text(&mut hasher, &task.tool_name);
        hash_text(
            &mut hasher,
            &serde_json::to_string(&task.normalized_input).unwrap_or_default(),
        );
        hash_text(&mut hasher, &task.effect.descriptor_hash);
        hash_text(&mut hasher, &task.descriptor_set_hash);
        hasher.update(task.catalog_revision.to_be_bytes());
        for predecessor in &task.predecessors {
            hasher.update(predecessor.to_be_bytes());
        }
        hasher.update([0xff]);
        for successor in &task.successors {
            hasher.update(successor.to_be_bytes());
        }
        hasher.update([0xfe]);
    }
    format!("{:x}", hasher.finalize())
}

fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_json).collect()),
        Value::Object(object) => {
            let sorted = object
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        scalar => scalar,
    }
}

struct ToolRequestAnalysis {
    purity: ToolPurity,
    authority_set: Vec<String>,
    side_effect_class: String,
    output_budget_class: String,
    reason: String,
}

fn analyze_request(
    request: &ToolRequest,
    effect: &ToolEffectDescriptor,
    safety_category: ToolSafetyCategory,
    resource_scope: ToolResourceScope,
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
        authority_set,
        side_effect_class,
        output_budget_class,
        reason,
    }
}

impl GovernedToolExecutionMode {
    fn from_safety_and_deps(safety_category: ToolSafetyCategory, has_deps: bool) -> Self {
        if has_deps {
            return Self::DependencyReady;
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

fn normalized_resource_scope(
    effect: &ToolEffectDescriptor,
    workspace_root: &Path,
) -> Result<ToolResourceScope, (String, String)> {
    let mut scope = resource_scope_from_effect(effect);
    let descriptor_declares_workspace_scope = scope.kind == "workspace";
    if scope.unknown {
        return Err((
            "unknown".to_string(),
            "registered descriptors must declare a concrete effect scope".to_string(),
        ));
    }
    for path in &mut scope.paths {
        *path = normalize_workspace_relative_scope_path(path, workspace_root)?;
        if path == "." {
            if descriptor_declares_workspace_scope {
                continue;
            }
            if effect.effect_kind == ToolEffectKind::Read {
                continue;
            }
            return Err((
                path.clone(),
                "a concrete resource scope cannot normalize to the workspace root".to_string(),
            ));
        }
        validate_relative_scope_path(path)?;
    }
    if !descriptor_declares_workspace_scope
        && effect.effect_kind == ToolEffectKind::Read
        && scope.paths.iter().any(|path| path == ".")
    {
        return Ok(ToolResourceScope::workspace());
    }
    scope.paths.sort();
    scope.paths.dedup();
    Ok(scope)
}

fn normalize_workspace_relative_scope_path(
    path: &str,
    workspace_root: &Path,
) -> Result<String, (String, String)> {
    use std::path::Path;

    let normalized = normalize_resource_path(path);
    let normalized_path = Path::new(&normalized);
    if !normalized_path.is_absolute() {
        return Ok(normalized);
    }
    let relative = normalized_path.strip_prefix(workspace_root).map_err(|_| {
        (
            normalized.clone(),
            "absolute scope path is outside the active workspace".to_string(),
        )
    })?;
    Ok(normalize_resource_path(&relative.to_string_lossy()))
}

fn validate_relative_scope_path(path: &str) -> Result<(), (String, String)> {
    use std::path::{Component, Path};

    if path.is_empty() || path.contains('\0') {
        return Err((
            path.to_string(),
            "scope path is empty or contains NUL".to_string(),
        ));
    }
    let path_value = Path::new(path);
    if path_value.is_absolute() {
        return Err((
            path.to_string(),
            "scope path must be workspace-relative".to_string(),
        ));
    }
    if path
        .split('/')
        .next()
        .is_some_and(|component| component.ends_with(':'))
    {
        return Err((
            path.to_string(),
            "scope path must not contain a platform drive prefix".to_string(),
        ));
    }
    for component in path_value.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            return Err((
                path.to_string(),
                "scope path escapes the workspace or has a platform prefix".to_string(),
            ));
        }
    }
    Ok(())
}

fn normalize_resource_path(path: &str) -> String {
    let replaced = path.trim().replace('\\', "/");
    if replaced.is_empty() {
        ".".to_string()
    } else {
        let absolute = replaced.starts_with('/');
        let components = replaced
            .split('/')
            .filter(|component| !component.is_empty() && *component != ".")
            .collect::<Vec<_>>();
        let normalized = components.join("/");
        match (absolute, normalized.is_empty()) {
            (true, true) => "/".to_string(),
            (true, false) => format!("/{normalized}"),
            (false, true) => ".".to_string(),
            (false, false) => normalized,
        }
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

/// Turn symmetric conflict facts into deterministic DAG edges. Edges always
/// follow the explicit dependency topology, so compiling safety cannot create
/// a cycle or reorder a model-declared predecessor.
fn compile_conflict_dependencies(
    tasks: &mut [GovernedToolPlanTask],
    explicit_topological_order: &[usize],
) {
    let mut rank = vec![usize::MAX; tasks.len()];
    for (position, index) in explicit_topological_order.iter().copied().enumerate() {
        rank[index] = position;
    }
    let mut edges = Vec::new();
    for left in 0..tasks.len() {
        for conflict in &tasks[left].conflicts {
            let Some(right) = tasks
                .iter()
                .position(|task| task.tool_call_id == conflict.tool_call_id)
            else {
                continue;
            };
            if left >= right {
                continue;
            }
            let (predecessor, successor) = if rank[left] <= rank[right] {
                (left, right)
            } else {
                (right, left)
            };
            edges.push((
                predecessor,
                successor,
                conflict.kind.clone(),
                conflict.reason.clone(),
            ));
        }
    }
    for (predecessor, successor, kind, reason) in edges {
        if !tasks[successor].predecessors.contains(&predecessor) {
            tasks[successor].predecessors.push(predecessor);
            tasks[successor].predecessors.sort_unstable();
            tasks[successor].indegree = tasks[successor].predecessors.len();
        }
        if !tasks[predecessor].successors.contains(&successor) {
            tasks[predecessor].successors.push(successor);
            tasks[predecessor].successors.sort_unstable();
        }
        let predecessor_id = tasks[predecessor].tool_call_id.clone();
        if !tasks[successor].depends_on.contains(&predecessor_id) {
            tasks[successor].depends_on.push(predecessor_id.clone());
        }
        if !tasks[successor]
            .invocation
            .compiled_dependencies
            .iter()
            .any(|dependency| dependency.depends_on == predecessor_id)
        {
            let invocation_id = tasks[successor].tool_call_id.clone();
            tasks[successor]
                .invocation
                .compiled_dependencies
                .push(ToolDependency {
                    invocation_id,
                    depends_on: predecessor_id,
                    reason: format!("compiled_conflict:{kind}:{reason}"),
                });
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
pub(crate) fn fixture_effect(tool_name: &str, input: &Value) -> ToolEffectDescriptor {
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

    fn compile_fixture(
        requests: &[ToolRequest],
    ) -> Result<ValidatedGovernedToolDag, GovernedToolCompileError> {
        let workspace = std::env::current_dir().expect("test workspace");
        GovernedToolCompiler.compile(&workspace, requests, |name, input| {
            Some((fixture_effect(name, input), 7, "fixture-set".to_string()))
        })
    }

    #[test]
    fn compiler_rejects_empty_and_duplicate_task_ids() {
        assert_eq!(
            compile_fixture(&[request("", "read_file", Vec::new())]),
            Err(GovernedToolCompileError::EmptyTaskId { index: 0 })
        );
        assert_eq!(
            compile_fixture(&[
                request("same", "read_file", Vec::new()),
                request("same", "grep_search", Vec::new()),
            ]),
            Err(GovernedToolCompileError::DuplicateTaskId {
                task_id: "same".to_string(),
                first_index: 0,
                duplicate_index: 1,
            })
        );
    }

    #[test]
    fn compiler_rejects_unknown_and_self_dependencies() {
        assert_eq!(
            compile_fixture(&[request("task", "read_file", vec!["missing".to_string()],)]),
            Err(GovernedToolCompileError::UnknownDependency {
                task_id: "task".to_string(),
                dependency_id: "missing".to_string(),
            })
        );
        assert_eq!(
            compile_fixture(&[request("task", "read_file", vec!["task".to_string()],)]),
            Err(GovernedToolCompileError::SelfDependency {
                task_id: "task".to_string(),
            })
        );
    }

    #[test]
    fn compiler_rejects_two_and_three_node_cycles() {
        let two = compile_fixture(&[
            request("a", "read_file", vec!["b".to_string()]),
            request("b", "read_file", vec!["a".to_string()]),
        ]);
        assert_eq!(
            two,
            Err(GovernedToolCompileError::Cycle {
                task_ids: vec!["a".to_string(), "b".to_string()],
            })
        );

        let three = compile_fixture(&[
            request("a", "read_file", vec!["c".to_string()]),
            request("b", "read_file", vec!["a".to_string()]),
            request("c", "read_file", vec!["b".to_string()]),
        ]);
        assert_eq!(
            three,
            Err(GovernedToolCompileError::Cycle {
                task_ids: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            })
        );
    }

    #[test]
    fn compiler_validates_diamond_fork_join_and_independent_chains() {
        let dag = compile_fixture(&[
            request("root", "read_file", Vec::new()),
            request("left", "read_file", vec!["root".to_string()]),
            request("right", "grep_search", vec!["root".to_string()]),
            request(
                "join",
                "read_file",
                vec!["right".to_string(), "left".to_string()],
            ),
            request("other-root", "read_file", Vec::new()),
            request("other-leaf", "read_file", vec!["other-root".to_string()]),
        ])
        .expect("valid DAG");

        assert_eq!(dag.tasks[0].successors, vec![1, 2]);
        assert_eq!(dag.tasks[3].predecessors, vec![1, 2]);
        assert_eq!(dag.tasks[3].indegree, 2);
        assert_eq!(dag.tasks[4].successors, vec![5]);
        assert_eq!(dag.topological_order, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn compiler_rejects_missing_descriptor_invalid_scope_and_rejected_dependency() {
        let workspace = std::env::current_dir().expect("test workspace");
        let missing = GovernedToolCompiler.compile(
            &workspace,
            &[request("missing", "not_registered", Vec::new())],
            |_name, _input| None,
        );
        assert_eq!(
            missing,
            Err(GovernedToolCompileError::MissingDescriptor {
                task_id: "missing".to_string(),
                tool_name: "not_registered".to_string(),
            })
        );

        let invalid_scope = GovernedToolCompiler.compile(
            &workspace,
            &[request_with_input(
                "escape",
                "read_file",
                r#"{"path":"../secret"}"#,
                Vec::new(),
            )],
            |name, input| Some((fixture_effect(name, input), 1, "scope-test".to_string())),
        );
        assert!(matches!(
            invalid_scope,
            Err(GovernedToolCompileError::InvalidNormalizedScope { task_id, .. })
                if task_id == "escape"
        ));

        let dependent = GovernedToolCompiler.compile(
            &workspace,
            &[
                request("rejected", "not_registered", Vec::new()),
                request("dependent", "read_file", vec!["rejected".to_string()]),
            ],
            |name, input| {
                (name == "read_file").then(|| {
                    (
                        fixture_effect(name, input),
                        1,
                        "dependency-test".to_string(),
                    )
                })
            },
        );
        assert!(matches!(
            dependent,
            Err(GovernedToolCompileError::DependencyReferencesRejectedTask {
                task_id,
                dependency_id,
                ..
            }) if task_id == "dependent" && dependency_id == "rejected"
        ));
    }

    #[test]
    fn partial_compiler_isolates_invalid_nodes_and_their_descendants() {
        let workspace = std::env::current_dir().expect("test workspace");
        let compilation = GovernedToolCompiler
            .compile_partial(
                &workspace,
                &[
                    request("invalid", "not_registered", Vec::new()),
                    request("blocked", "read_file", vec!["invalid".to_string()]),
                    request("independent", "read_file", Vec::new()),
                ],
                |name, input| {
                    (name == "read_file")
                        .then(|| (fixture_effect(name, input), 1, "partial-test".to_string()))
                },
            )
            .expect("graph identity remains valid");

        let plan = compilation
            .plan
            .expect("independent node remains executable");
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].tool_call_id, "independent");
        assert_eq!(plan.tasks[0].original_call_index, 2);
        assert_eq!(
            compilation
                .rejected
                .iter()
                .map(|rejection| rejection.tool_call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["invalid", "blocked"]
        );
    }

    #[test]
    fn partial_compiler_isolates_cycles_but_keeps_independent_nodes() {
        let workspace = std::env::current_dir().expect("test workspace");
        let compilation = GovernedToolCompiler
            .compile_partial(
                &workspace,
                &[
                    request("cycle-a", "read_file", vec!["cycle-b".to_string()]),
                    request("cycle-b", "read_file", vec!["cycle-a".to_string()]),
                    request("independent", "read_file", Vec::new()),
                ],
                |name, input| {
                    Some((
                        fixture_effect(name, input),
                        1,
                        "partial-cycle-test".to_string(),
                    ))
                },
            )
            .expect("graph identity remains valid");

        let plan = compilation
            .plan
            .expect("independent node remains executable");
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].tool_call_id, "independent");
        assert_eq!(plan.tasks[0].original_call_index, 2);
        assert_eq!(compilation.rejected.len(), 2);
        assert!(compilation
            .rejected
            .iter()
            .all(|rejection| rejection.reason.contains("cycle")));
    }

    #[test]
    fn partial_compiler_rejects_ambiguous_graph_identity_as_a_batch() {
        let workspace = std::env::current_dir().expect("test workspace");
        let error = GovernedToolCompiler
            .compile_partial(
                &workspace,
                &[
                    request("same", "read_file", Vec::new()),
                    request("same", "read_file", Vec::new()),
                ],
                |name, input| {
                    Some((
                        fixture_effect(name, input),
                        1,
                        "partial-identity-test".to_string(),
                    ))
                },
            )
            .expect_err("duplicate ids make the graph ambiguous");

        assert!(matches!(
            error,
            GovernedToolCompileError::DuplicateTaskId {
                task_id,
                first_index: 0,
                duplicate_index: 1,
            } if task_id == "same"
        ));
    }

    #[test]
    fn compiler_rejects_invalid_json_before_any_execution_contract_exists() {
        let result = compile_fixture(&[request_with_input(
            "invalid-json",
            "read_file",
            "{",
            Vec::new(),
        )]);

        assert!(matches!(
            result,
            Err(GovernedToolCompileError::InvalidInput { task_id, .. })
                if task_id == "invalid-json"
        ));
    }

    #[test]
    fn topology_hash_is_deterministic_and_canonicalizes_json_objects() {
        let first = compile_fixture(&[request_with_input(
            "read",
            "read_file",
            r#"{"path":"src/lib.rs","options":{"b":2,"a":1}}"#,
            Vec::new(),
        )])
        .expect("first DAG");
        let second = compile_fixture(&[request_with_input(
            "read",
            "read_file",
            r#"{"options":{"a":1,"b":2},"path":"src/lib.rs"}"#,
            Vec::new(),
        )])
        .expect("second DAG");

        assert_eq!(first.topology_hash, second.topology_hash);
        assert_ne!(first.plan_id, second.plan_id);
    }

    #[test]
    fn invalid_graph_never_reaches_lifecycle_or_effect_hooks() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let lifecycle_events = AtomicUsize::new(0);
        let tool_calls = AtomicUsize::new(0);
        let effect_intents = AtomicUsize::new(0);
        let result = compile_fixture(&[request("task", "read_file", vec!["task".to_string()])]);

        assert!(result.is_err());
        assert_eq!(lifecycle_events.load(Ordering::Relaxed), 0);
        assert_eq!(tool_calls.load(Ordering::Relaxed), 0);
        assert_eq!(effect_intents.load(Ordering::Relaxed), 0);
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
            .starts_with("tool-plan-task:v4:"));
        assert_eq!(plan.tasks[0].model_visible_name, "read");
        assert!(plan.tasks[0].can_parallelize);
        assert_eq!(
            plan.tasks[2].execution_mode,
            GovernedToolExecutionMode::SerialDestructive
        );
        assert!(!plan.tasks[2].can_parallelize);
    }

    #[test]
    fn dependency_tasks_record_typed_adjacency() {
        let plan = GovernedToolPlan::from_requests(&[
            request("read-1", "read", Vec::new()),
            request("write-2", "write", vec!["read-1".to_string()]),
        ]);

        assert_eq!(plan.tasks[0].successors, vec![1]);
        assert_eq!(plan.tasks[1].predecessors, vec![0]);
        assert_eq!(plan.tasks[1].indegree, 1);
        assert_eq!(plan.topological_order, vec![0, 1]);
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

        assert_eq!(event.kind.scope(), session::SessionDomainScope::Tool);
        assert_eq!(
            event.kind,
            RuntimeSessionEventKind::ToolExecutionPlanCreated
        );
        assert_eq!(event.status.as_deref(), Some("planned"));
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.payload["task_count"], 2);
        assert_eq!(
            event.payload["tasks"][0]["contract_version"],
            GOVERNED_TOOL_PLAN_CONTRACT_VERSION
        );
        assert!(event.payload["tasks"][0]["idempotency_key"]
            .as_str()
            .unwrap()
            .starts_with("tool-plan-task:v4:"));
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
                tool_name: "web_search".to_string(),
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
    fn readonly_workspace_root_scope_is_canonical_and_writes_still_fail_closed() {
        let workspace = std::env::current_dir().expect("test workspace");
        let readonly = GovernedToolCompiler
            .compile(
                &workspace,
                &[request_with_input(
                    "glob-root",
                    "glob_search",
                    r#"{"pattern":"**/*.rs","path":"."}"#,
                    Vec::new(),
                )],
                |name, input| Some((fixture_effect(name, input), 1, "workspace-root".to_string())),
            )
            .expect("read-only workspace root is a valid governed scope");
        assert_eq!(readonly.tasks[0].resource_scope.kind, "workspace");
        assert_eq!(readonly.tasks[0].resource_scope.paths, vec!["."]);

        let write = GovernedToolCompiler.compile(
            &workspace,
            &[request_with_input(
                "write-root",
                "write_file",
                r#"{"path":".","content":"no"}"#,
                Vec::new(),
            )],
            |name, input| Some((fixture_effect(name, input), 1, "workspace-root".to_string())),
        );
        assert!(matches!(
            write,
            Err(GovernedToolCompileError::InvalidNormalizedScope { task_id, .. })
                if task_id == "write-root"
        ));
    }

    #[test]
    fn readonly_absolute_scope_is_relativized_only_inside_the_workspace() {
        let workspace = std::env::current_dir().expect("workspace");
        let inside = workspace.join("crates/runtime/src");
        let input = serde_json::json!({
            "pattern": "**/*.rs",
            "path": inside,
        })
        .to_string();
        let plan = GovernedToolCompiler
            .compile(
                &workspace,
                &[request_with_input(
                    "glob-inside",
                    "glob_search",
                    &input,
                    Vec::new(),
                )],
                |name, input| {
                    Some((
                        fixture_effect(name, input),
                        1,
                        "workspace-absolute".to_string(),
                    ))
                },
            )
            .expect("absolute paths inside the workspace should be relativized");
        assert_eq!(
            plan.tasks[0].resource_scope.paths,
            vec!["crates/runtime/src"]
        );

        let outside = std::env::temp_dir().join("cowd-outside-workspace");
        let outside_input = serde_json::json!({
            "pattern": "**/*.rs",
            "path": outside,
        })
        .to_string();
        let rejected = GovernedToolCompiler.compile(
            &workspace,
            &[request_with_input(
                "glob-outside",
                "glob_search",
                &outside_input,
                Vec::new(),
            )],
            |name, input| {
                Some((
                    fixture_effect(name, input),
                    1,
                    "workspace-absolute".to_string(),
                ))
            },
        );
        assert!(matches!(
            rejected,
            Err(GovernedToolCompileError::InvalidNormalizedScope { task_id, .. })
                if task_id == "glob-outside"
        ));
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
        assert!(plan.tasks[0].can_parallelize);
        assert_eq!(plan.tasks[0].conflicts[0].tool_call_id, "edit-1");
        assert_eq!(plan.tasks[0].conflicts[0].kind, "path_overlap");
        assert_eq!(plan.tasks[1].conflicts[0].tool_call_id, "write-1");
        assert_eq!(plan.tasks[0].successors, vec![1]);
        assert_eq!(plan.tasks[1].predecessors, vec![0]);
        assert_eq!(plan.tasks[1].depends_on, vec!["write-1"]);
        assert_eq!(
            plan.tasks[1].invocation.compiled_dependencies[0].depends_on,
            "write-1"
        );
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
            GovernedToolPlan::from_requests(&[request("network-1", "web_search", Vec::new())]);
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
        let approved_delivery = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Medium,
            &[ExecutionModifier::WithGuardrails],
            &[
                ExecutionPolicyGate::Permission,
                ExecutionPolicyGate::Approval,
            ],
        );
        let approved_delivery_report =
            write_plan.validate_against_execution_decision(&approved_delivery);
        assert!(approved_delivery_report.allowed);
        assert!(approved_delivery_report.requires_approval);
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
            vec!["critical_mutation_requires_approval"]
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
            GovernedToolPlan::from_requests(&[request("network-1", "web_search", Vec::new())]);
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
        let workspace = std::env::current_dir().expect("test workspace");
        let plan = GovernedToolCompiler
            .compile(
                &workspace,
                &[request("plugin-read", "company_catalog_lookup", Vec::new())],
                |name, input| {
                    let mut effect = fixture_effect(name, input);
                    effect.effect_kind = ToolEffectKind::Read;
                    effect.idempotency = ToolIdempotency::Idempotent;
                    effect.required_permission =
                        harness_contract::tool::ToolPermissionMode::ReadOnly;
                    effect.uses_network = false;
                    effect.spawns_process = false;
                    Some((effect, 1, "plugin-test".to_string()))
                },
            )
            .expect("registered descriptor compiles");

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
    fn registered_dynamic_write_metadata_drives_governance_without_name_heuristics() {
        let workspace = std::env::current_dir().expect("test workspace");
        let plan = GovernedToolCompiler
            .compile(
                &workspace,
                &[request_with_input(
                    "plugin-write",
                    "company_report_publisher",
                    r#"{"path":"reports/final.md"}"#,
                    Vec::new(),
                )],
                |name, input| {
                    let mut effect = fixture_effect(name, input);
                    effect.effect_kind = ToolEffectKind::Write;
                    effect.idempotency = ToolIdempotency::IdempotentWithKey;
                    effect.required_permission =
                        harness_contract::tool::ToolPermissionMode::WorkspaceWrite;
                    effect.approval_class = harness_contract::tool::ToolApprovalClass::Policy;
                    effect.scopes = vec![PermissionScope {
                        resource: PermissionResource::File,
                        operation: PermissionOperation::Write,
                        target: Some("reports/final.md".to_string()),
                    }];
                    Some((effect, 9, "dynamic-write-descriptor".to_string()))
                },
            )
            .expect("registered dynamic write descriptor compiles");

        assert_eq!(
            plan.tasks[0].safety_category,
            ToolSafetyCategory::WriteLocal
        );
        let evidence_only = execution_decision(
            RuntimeCompileTarget::EvidenceGraph,
            TaskRisk::Medium,
            &[],
            &[],
        );
        assert!(
            !plan
                .validate_against_execution_decision(&evidence_only)
                .allowed
        );

        let approved_delivery = execution_decision(
            RuntimeCompileTarget::ExecutionGraph,
            TaskRisk::Medium,
            &[ExecutionModifier::WithGuardrails],
            &[
                ExecutionPolicyGate::Permission,
                ExecutionPolicyGate::Approval,
            ],
        );
        let report = plan.validate_against_execution_decision(&approved_delivery);
        assert!(report.allowed);
        assert!(report.requires_approval);
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
            request("todo-1", "todo_write", Vec::new()),
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
