//! Fail-closed compiler for governed tool dependency graphs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;

#[cfg(test)]
use harness_contract::core::{ExecutionModifier, ExecutionPolicyGate, TaskRisk};
#[cfg(test)]
use harness_contract::policy::PermissionScope;
use harness_contract::policy::{PermissionOperation, PermissionResource};
#[cfg(any(test, feature = "test-fixtures"))]
use harness_contract::tool::ToolIntent;
use harness_contract::tool::{
    GovernedToolInvocation, GovernedToolPlanProjection, ResourceAccess, ResourceDemand,
    ResourceScopeDemand, ToolDependency, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[cfg(test)]
use crate::execution_core::RuntimeCompileTarget;
use crate::execution_core::RuntimeExecutionDecision;
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

    fn internal_transaction() -> Self {
        Self {
            kind: "internal_transaction".to_string(),
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

/// Same-compilation invocation and its original live executor owner.
#[derive(Clone)]
pub struct OriginalPinnedInvocation {
    invocation: harness_contract::tool::GovernedToolInvocation,
    lease: Option<std::sync::Arc<tools::ToolHostLease>>,
    executor: std::sync::Weak<dyn crate::ToolExecutor>,
}
impl OriginalPinnedInvocation {
    pub(crate) fn new(invocation: harness_contract::tool::GovernedToolInvocation,
        lease: Option<std::sync::Arc<tools::ToolHostLease>>, executor: std::sync::Arc<dyn crate::ToolExecutor>) -> Self {
        Self { invocation, lease, executor: std::sync::Arc::downgrade(&executor) }
    }
    pub(crate) fn invocation(&self) -> &harness_contract::tool::GovernedToolInvocation { &self.invocation }
    pub(crate) fn lease(&self) -> Option<&std::sync::Arc<tools::ToolHostLease>> { self.lease.as_ref() }
    pub(crate) fn executor(&self) -> Option<std::sync::Arc<dyn crate::ToolExecutor>> { self.executor.upgrade() }
}
impl std::fmt::Debug for OriginalPinnedInvocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalPinnedInvocation").field("invocation", &self.invocation).finish_non_exhaustive()
    }
}
impl PartialEq for OriginalPinnedInvocation {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.executor, &other.executor)
            && match (&self.lease, &other.lease) { (Some(a), Some(b)) => std::sync::Arc::ptr_eq(a, b), (None, None) => true, _ => false }
            && self.invocation == other.invocation
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernedToolPlanTask {
    #[serde(skip)]
    pub(crate) original_pin: Option<std::sync::Arc<OriginalPinnedInvocation>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_binding: Option<crate::step_snapshot::StepCallBinding>,
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
    #[error("governed tool compiler invariant failed: {reason}")]
    CompilationInvariant { reason: String },
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

struct PreparedToolNode {
    invocation: GovernedToolInvocation,
    resource_scope: ToolResourceScope,
}

#[cfg(any(test, feature = "test-fixtures"))]
fn prepare_fixture_node(
    workspace_root: &Path,
    request: &ToolRequest,
    describe: &impl Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
) -> Result<PreparedToolNode, GovernedToolCompileError> {
    let normalized_input = serde_json::from_str::<Value>(&request.input)
        .map(canonical_json)
        .map_err(|error| GovernedToolCompileError::InvalidInput {
            task_id: request.tool_use_id.clone(),
            reason: error.to_string(),
        })?;
    let (effect, catalog_revision, descriptor_set_hash) =
        describe(&request.tool_name, &normalized_input).ok_or_else(|| {
            GovernedToolCompileError::MissingDescriptor {
                task_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
            }
        })?;
    let resource_scope =
        normalized_resource_scope(&effect, workspace_root).map_err(|(scope, reason)| {
            GovernedToolCompileError::InvalidNormalizedScope {
                task_id: request.tool_use_id.clone(),
                scope,
                reason,
            }
        })?;
    let invocation = GovernedToolInvocation {
        origin: None,
        contract_version: GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
        invocation_id: request.tool_use_id.clone(),
        intent: ToolIntent {
            invocation_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            normalized_input,
        },
        resource_demand: resource_demand_from_effect(&effect),
        effect,
        explicit_dependencies: request
            .depends_on
            .iter()
            .map(|dependency| ToolDependency {
                invocation_id: request.tool_use_id.clone(),
                depends_on: dependency.clone(),
                reason: "model_explicit_dependency".into(),
            })
            .collect(),
        compiled_dependencies: Vec::new(),
        catalog_revision,
        descriptor_set_hash,
        idempotency_key: tool_plan_idempotency_key(request),
    };
    Ok(PreparedToolNode {
        invocation,
        resource_scope,
    })
}

impl GovernedToolCompiler {
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn compile(
        &self,
        workspace_root: &Path,
        requests: &[ToolRequest],
        describe_registered_tool: impl Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    ) -> Result<ValidatedGovernedToolDag, GovernedToolCompileError> {
        let ids = validate_task_ids(requests)?;
        validate_dependencies(requests, &ids)?;
        let attempts = requests
            .iter()
            .map(|request| prepare_fixture_node(workspace_root, request, &describe_registered_tool))
            .collect::<Vec<_>>();
        for request in requests {
            for dependency in &request.depends_on {
                if let Err(error) = &attempts[ids[dependency.as_str()]] {
                    return Err(GovernedToolCompileError::DependencyReferencesRejectedTask {
                        task_id: request.tool_use_id.clone(),
                        dependency_id: dependency.clone(),
                        rejection_reason: error.to_string(),
                    });
                }
            }
        }
        let prepared = attempts.into_iter().collect::<Result<Vec<_>, _>>()?;
        ValidatedGovernedToolDag::from_prepared(requests, prepared)
    }

    /// Consume the exact Step/ToolHost preparation. Dispatch retains its live
    /// authorization, source, path and attempt checks; compilation only adds DAG edges.
    pub fn compile_prepared_partial(
        &self,
        workspace_root: &Path,
        requests: &[ToolRequest],
        prepared: &[GovernedToolInvocation],
        admission_rejections: &BTreeMap<String, String>,
    ) -> Result<GovernedToolCompilation, GovernedToolCompileError> {
        let mut by_id = BTreeMap::new();
        for invocation in prepared {
            if !requests
                .iter()
                .any(|request| request.tool_use_id == invocation.invocation_id)
                || by_id
                    .insert(invocation.invocation_id.as_str(), invocation)
                    .is_some()
            {
                return Err(GovernedToolCompileError::CompilationInvariant {
                    reason: "prepared invocation has duplicate or foreign identity".into(),
                });
            }
        }
        self.compile_partial_nodes(requests, admission_rejections, |request| {
            let invocation = *by_id.get(request.tool_use_id.as_str()).ok_or_else(|| {
                GovernedToolCompileError::MissingDescriptor {
                    task_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                }
            })?;
            if invocation.intent.invocation_id != request.tool_use_id
                || invocation.intent.tool_name != request.tool_name
                || invocation.effect.tool_id != invocation.intent.tool_name
                || invocation
                    .explicit_dependencies
                    .iter()
                    .any(|dependency| dependency.invocation_id != request.tool_use_id)
                || invocation
                    .explicit_dependencies
                    .iter()
                    .map(|dependency| &dependency.depends_on)
                    .collect::<BTreeSet<_>>()
                    != request.depends_on.iter().collect::<BTreeSet<_>>()
                || invocation
                    .compiled_dependencies
                    .iter()
                    .any(|dependency| dependency.invocation_id != request.tool_use_id)
            {
                return Err(GovernedToolCompileError::CompilationInvariant {
                    reason: format!(
                        "prepared invocation binding changed for {}",
                        request.tool_use_id
                    ),
                });
            }
            let resource_scope = normalized_resource_scope(&invocation.effect, workspace_root)
                .map_err(
                    |(scope, reason)| GovernedToolCompileError::InvalidNormalizedScope {
                        task_id: request.tool_use_id.clone(),
                        scope,
                        reason,
                    },
                )?;
            Ok(PreparedToolNode {
                invocation: (*invocation).clone(),
                resource_scope,
            })
        })
    }

    /// Compile every valid node while isolating malformed nodes, their
    /// descendants, and cycle-affected subgraphs. Graph identity errors remain
    /// whole-batch failures because no unambiguous dependency graph exists.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn compile_partial<F>(
        &self,
        workspace_root: &Path,
        requests: &[ToolRequest],
        describe_registered_tool: F,
    ) -> Result<GovernedToolCompilation, GovernedToolCompileError>
    where
        F: Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    {
        self.compile_partial_with_rejections(
            workspace_root,
            requests,
            &BTreeMap::new(),
            describe_registered_tool,
        )
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn compile_partial_with_rejections<F>(
        &self,
        workspace_root: &Path,
        requests: &[ToolRequest],
        admission_rejections: &BTreeMap<String, String>,
        describe_registered_tool: F,
    ) -> Result<GovernedToolCompilation, GovernedToolCompileError>
    where
        F: Fn(&str, &Value) -> Option<(ToolEffectDescriptor, u64, String)>,
    {
        self.compile_partial_nodes(requests, admission_rejections, |request| {
            prepare_fixture_node(workspace_root, request, &describe_registered_tool)
        })
    }

    fn compile_partial_nodes(
        &self,
        requests: &[ToolRequest],
        admission_rejections: &BTreeMap<String, String>,
        prepare: impl Fn(&ToolRequest) -> Result<PreparedToolNode, GovernedToolCompileError>,
    ) -> Result<GovernedToolCompilation, GovernedToolCompileError> {
        let id_to_index = validate_task_ids(requests)?;
        let mut rejected = requests
            .iter()
            .enumerate()
            .filter_map(|(index, request)| {
                admission_rejections
                    .get(&request.tool_use_id)
                    .map(|reason| (index, reason.clone()))
            })
            .collect::<BTreeMap<_, _>>();

        let mut prepared_nodes = BTreeMap::new();
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

            if !rejected.contains_key(&index) {
                match prepare(request) {
                    Ok(node) => {
                        prepared_nodes.insert(index, node);
                    }
                    Err(error) => {
                        rejected.insert(index, error.to_string());
                    }
                }
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
            let nodes = accepted_indices
                .iter()
                .map(|index| {
                    prepared_nodes.remove(index).ok_or_else(|| {
                        GovernedToolCompileError::CompilationInvariant {
                            reason: "accepted node has no original preparation".into(),
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut plan = ValidatedGovernedToolDag::from_prepared(&accepted_requests, nodes)?;
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
        GovernedToolCompiler
            .compile(&workspace, requests, |name, input| {
                Some((fixture_effect(name, input), 1, "test-fixture".to_string()))
            })
            .expect("fixture requests form a valid governed tool DAG")
    }

    fn from_prepared(
        requests: &[ToolRequest],
        prepared: Vec<PreparedToolNode>,
    ) -> Result<Self, GovernedToolCompileError> {
        let id_to_index = validate_task_ids(requests)?;
        validate_dependencies(requests, &id_to_index)?;
        if prepared.len() != requests.len() {
            return Err(GovernedToolCompileError::CompilationInvariant {
                reason: "prepared graph cardinality changed".into(),
            });
        }
        let mut parallel_read_count = 0;
        let mut limited_count = 0;
        let mut destructive_count = 0;

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
            let PreparedToolNode {
                invocation,
                resource_scope,
            } = prepared;
            let normalized_input = invocation.intent.normalized_input.clone();
            let effect = invocation.effect.clone();
            let catalog_revision = invocation.catalog_revision;
            let descriptor_set_hash = invocation.descriptor_set_hash.clone();
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
            let resource_demand = invocation.resource_demand.clone();
            let idempotency_key = invocation.idempotency_key.clone();
            tasks.push(GovernedToolPlanTask {
                original_pin: None,
                step_binding: None,
                original_call_index,
                contract_version: GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
                tool_call_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                normalized_input,
                idempotency_key,
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

    /// Prepare concrete-effect safeguards. The decision supplies only the
    /// trace lease: predicted topology, confidence, risk and modifiers neither
    /// authorize nor refuse a tool. Each invocation still needs its original
    /// current-policy authorization and resource admission at dispatch.
    #[must_use]
    pub fn validate_against_execution_decision(
        &self,
        decision: &RuntimeExecutionDecision,
    ) -> GovernedToolPolicyValidationReport {
        use harness_contract::tool::ToolApprovalClass;

        let mut findings = Vec::new();
        let mut requires_approval = false;
        let mut requires_checkpoint = false;
        for task in self
            .tasks
            .iter()
            .filter(|task| !uses_inner_runtime_validator(&task.tool_name))
        {
            let effect = &task.invocation.effect;
            if task.effect != *effect {
                push_finding(&mut findings, "prepared_effect_identity_mismatch");
            }
            // Unknown effects remain conservative. A favourable strategy can
            // never turn them into reads or suppress an actual approval.
            requires_approval |= effect.approval_class != ToolApprovalClass::None
                || effect.effect_kind == ToolEffectKind::Unknown;
            // A read-only bounded process has no file mutation to back up.
            // This is only a checkpoint decision: the dispatchers route every
            // such effect through the original bounded-process owner, which
            // verifies the live authorization, lease and sandbox. No generic
            // host execution may consume this exception.
            let bounded_read_only = tools::tool_orchestrator::effect_is_bounded_read_only_compute(effect);
            // Runtime-only control writes have no workspace state to checkpoint.
            let internal_control = harness_contract::tool::is_internal_control_effect(
                effect.effect_kind, effect.idempotency, effect.required_permission,
                effect.approval_class, &effect.scopes, effect.uses_network,
                effect.spawns_process, effect.mutates_packages, effect.mutates_system,
            ) && effect.assessment.externality == harness_contract::policy::EffectExternality::Internal;
            requires_checkpoint |= !bounded_read_only && !internal_control && (matches!(
                effect.effect_kind,
                ToolEffectKind::Write
                    | ToolEffectKind::Process
                    | ToolEffectKind::Package
                    | ToolEffectKind::System
                    | ToolEffectKind::Destructive
                    | ToolEffectKind::Unknown
            ) || effect.spawns_process
                || effect.mutates_packages
                || effect.mutates_system);
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

fn uses_inner_runtime_validator(tool_name: &str) -> bool {
    let normalized = tool_name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    normalized == "runtimecapabilities"
        || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .any(|action| {
                action
                    .chars()
                    .filter(char::is_ascii_alphanumeric)
                    .collect::<String>()
                    == normalized
            })
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
    let output_budget_class = if request.tool_name.eq_ignore_ascii_case("tool_orchestrate") {
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
        let mut scope = ToolResourceScope::unknown();
        scope.network = effect.uses_network;
        return scope;
    }
    let file_scopes = effect
        .scopes
        .iter()
        .filter(|scope| scope.resource == PermissionResource::File)
        .collect::<Vec<_>>();
    let file_accesses = crate::tool_host::effect_file_accesses(effect);
    let untargeted_file = effect.spawns_process
        || file_scopes.iter().any(|scope| {
            scope
                .target
                .as_deref()
                .is_none_or(|target| target.trim().is_empty())
        });
    if effect.uses_network {
        if file_accesses.is_empty() {
            return ToolResourceScope::network();
        }
        let mut scope = if untargeted_file {
            ToolResourceScope::workspace()
        } else {
            let mut paths = file_accesses
                .iter()
                .map(|(path, _)| normalize_resource_path(path))
                .collect::<Vec<_>>();
            paths.sort();
            paths.dedup();
            ToolResourceScope::paths(paths)
        };
        scope.network = true;
        return scope;
    }
    if untargeted_file {
        return ToolResourceScope::workspace();
    }
    // Runtime-owned control actions commit through their own transactional
    // aggregate. They do not mutate a workspace path, and the aggregate
    // service already owns CAS/serialization. Treating a pathless Tool.Control
    // scope as `workspace:.` creates false DAG dependencies: one rejected
    // action then blocks otherwise independent sibling actions. The exact
    // descriptor tuple below is the contract for an internally serialized,
    // idempotent mutation; unknown/system effects remain conservative.
    if effect.effect_kind == ToolEffectKind::Write
        && effect.idempotency == ToolIdempotency::IdempotentWithKey
        && !effect.spawns_process
        && !effect.mutates_packages
        && !effect.mutates_system
        && !effect.scopes.is_empty()
        && effect.scopes.iter().all(|scope| {
            scope.resource == PermissionResource::Tool
                && scope.operation == PermissionOperation::Control
        })
    {
        return ToolResourceScope::internal_transaction();
    }
    if file_accesses.is_empty()
        && matches!(
            effect.effect_kind,
            ToolEffectKind::Process
                | ToolEffectKind::Package
                | ToolEffectKind::System
                | ToolEffectKind::Destructive
        )
    {
        return ToolResourceScope::runtime();
    }
    let mut paths = if file_accesses.is_empty() {
        effect
            .scopes
            .iter()
            .filter_map(|scope| scope.target.as_deref())
            .map(normalize_resource_path)
            .collect::<Vec<_>>()
    } else {
        file_accesses
            .iter()
            .map(|(path, _)| normalize_resource_path(path))
            .collect::<Vec<_>>()
    };
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
    for (path, _) in crate::tool_host::effect_file_accesses(effect) {
        let normalized = normalize_workspace_relative_scope_path(&path, workspace_root)?;
        if normalized != "." {
            validate_relative_scope_path(&normalized)?;
        }
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
        let mut workspace_scope = ToolResourceScope::workspace();
        workspace_scope.network = scope.network;
        return Ok(workspace_scope);
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

pub(crate) fn validate_relative_scope_path(path: &str) -> Result<(), (String, String)> {
    use std::path::{Component, Path};

    if path.is_empty() || path.contains('\0') {
        return Err((
            path.to_string(),
            "scope path is empty or contains NUL".to_string(),
        ));
    }
    let normalized = path.replace('\\', "/");
    let path = normalized.as_str();
    let path_value = Path::new(path);
    if path_value.is_absolute() {
        return Err((
            path.to_string(),
            "scope path must be workspace-relative".to_string(),
        ));
    }
    if (path.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && path.as_bytes().get(1) == Some(&b':'))
        || path
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

pub(crate) fn normalize_resource_path(path: &str) -> String {
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

#[cfg(test)]
mod relative_scope_grammar_tests {
    #[test]
    fn drive_relative_scopes_are_rejected_before_normalization() {
        for path in [
            "C:secret",
            "c:../secret",
            "C:/secret",
            "C:\\secret",
            "\\\\server\\share",
            "/secret",
            "../secret",
            "volume:/secret",
        ] {
            assert!(super::validate_relative_scope_path(path).is_err(), "{path}");
        }
        assert!(super::validate_relative_scope_path("src/a.rs").is_ok());
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
    if harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .any(|action| *action == normalized)
    {
        effect.effect_kind = if normalized == harness_contract::agent_action::STATE_INSPECT_TOOL_ID
        {
            ToolEffectKind::Read
        } else {
            ToolEffectKind::Write
        };
        effect.idempotency = if effect.effect_kind == ToolEffectKind::Read {
            ToolIdempotency::Idempotent
        } else {
            ToolIdempotency::IdempotentWithKey
        };
        effect.uses_network = false;
        effect.spawns_process = false;
        effect.scopes = vec![PermissionScope {
            resource: PermissionResource::Tool,
            operation: if effect.effect_kind == ToolEffectKind::Read {
                PermissionOperation::Read
            } else {
                PermissionOperation::Control
            },
            target: None,
        }];
        return effect;
    }
    if matches!(
        normalized.as_str(),
        "todo_write" | "todowrite" | "runtime_capabilities"
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

    #[test]
    fn r1_composite_effect_mixed_scope_keeps_local_paths_and_network() {
        let workspace = std::env::current_dir().unwrap().canonicalize().unwrap();
        let lease = tools::ToolHost::builtin("mixed-runtime-scope", &workspace).pin_snapshot();
        let mixed = serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"src/shared.rs","content":"x"}},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid/no-transport","prompt":"summarize"}}
        ]});
        let effect = lease.describe_effect("tool_orchestrate", &mixed);
        let scope = resource_scope_from_effect(&effect);
        assert_eq!(scope.kind, "paths");
        assert_eq!(scope.paths, vec![".cowd/checkpoints", "src/shared.rs"]);
        assert!(scope.network && !scope.unknown);
        assert_eq!(
            normalized_resource_scope(&effect, &workspace).unwrap(),
            scope
        );
        let requests = [
            request_with_input("mixed", "tool_orchestrate", &mixed.to_string(), Vec::new()),
            request_with_input(
                "same",
                "edit",
                r#"{"action":"write","path":"src/shared.rs","content":"y"}"#,
                Vec::new(),
            ),
            request_with_input(
                "different",
                "edit",
                r#"{"action":"write","path":"src/different.rs","content":"z"}"#,
                Vec::new(),
            ),
            request_with_input(
                "independent-read",
                "read_file",
                r#"{"path":"src/independent.rs"}"#,
                Vec::new(),
            ),
        ];
        let plan = GovernedToolCompiler
            .compile(&workspace, &requests, |tool, input| {
                Some((
                    lease.describe_effect(tool, input),
                    lease.revision(),
                    lease.snapshot().descriptor_set_hash.clone(),
                ))
            })
            .unwrap();
        assert_eq!(plan.tasks[0].resource_scope, scope);
        assert_eq!(plan.tasks[0].conflicts.len(), 2);
        assert_eq!(plan.tasks[0].conflicts[0].tool_call_id, "same");
        assert_eq!(plan.tasks[0].conflicts[0].kind, "path_overlap");
        assert_eq!(plan.tasks[0].conflicts[1].tool_call_id, "different");
        assert_eq!(plan.tasks[0].conflicts[1].kind, "path_overlap");
        assert_eq!(plan.tasks[1].predecessors, vec![0]);
        assert_eq!(
            plan.tasks[1].invocation.compiled_dependencies[0].depends_on,
            "mixed"
        );
        assert_eq!(plan.tasks[2].predecessors, vec![0, 1]);
        assert!(plan.tasks[3].conflicts.is_empty() && plan.tasks[3].predecessors.is_empty());
        let network = lease.describe_effect("web_fetch", &mixed["steps"][1]["input"]);
        assert_eq!(
            resource_scope_from_effect(&network),
            ToolResourceScope::network()
        );
        assert_eq!(
            normalized_resource_scope(&network, &workspace).unwrap(),
            ToolResourceScope::network()
        );
        let mut absolute = effect.clone();
        for file in absolute
            .scopes
            .iter_mut()
            .filter(|scope| scope.resource == PermissionResource::File)
        {
            file.target = Some(
                workspace
                    .join(file.target.as_deref().unwrap())
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        assert_eq!(
            normalized_resource_scope(&absolute, &workspace).unwrap(),
            scope
        );
        let mut escaped = effect.clone();
        for file in escaped
            .scopes
            .iter_mut()
            .filter(|scope| scope.resource == PermissionResource::File)
        {
            file.target = Some("../outside".into());
        }
        assert!(normalized_resource_scope(&escaped, &workspace).is_err());
        for target in [None, Some(String::new())] {
            let mut untargeted = effect.clone();
            untargeted.scopes.push(PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Write,
                target,
            });
            let scope = resource_scope_from_effect(&untargeted);
            assert_eq!(scope.kind, "workspace");
            assert_eq!(scope.paths, vec!["."]);
            assert!(scope.network && !scope.unknown);
            assert_eq!(
                normalized_resource_scope(&untargeted, &workspace).unwrap(),
                scope
            );
        }
        let mut unknown = effect.clone();
        unknown.effect_kind = ToolEffectKind::Unknown;
        let scope = resource_scope_from_effect(&unknown);
        assert!(scope.unknown && scope.network && scope.paths.is_empty());
        assert!(normalized_resource_scope(&unknown, &workspace).is_err());
        let mut root_read = effect.clone();
        root_read.effect_kind = ToolEffectKind::Read;
        root_read
            .scopes
            .iter_mut()
            .filter(|scope| scope.resource == PermissionResource::File)
            .for_each(|scope| scope.target = Some(workspace.to_string_lossy().into_owned()));
        let scope = normalized_resource_scope(&root_read, &workspace).unwrap();
        assert_eq!(scope.kind, "workspace");
        assert_eq!(scope.paths, vec!["."]);
        assert!(scope.network);
        let sandbox = lease.describe_effect(
            "execute_code",
            &serde_json::json!({"language":"python","code":"print(1)"}),
        );
        let scope = resource_scope_from_effect(&sandbox);
        assert_eq!(scope.kind, "workspace");
        assert_eq!(scope.paths, vec!["."]);
        assert!(!scope.network && !scope.unknown);
        let mut process_mixed = effect.clone();
        process_mixed.effect_kind = ToolEffectKind::Process;
        process_mixed.uses_network = false;
        process_mixed
            .scopes
            .retain(|scope| scope.resource == PermissionResource::File);
        assert_eq!(
            resource_scope_from_effect(&process_mixed),
            ToolResourceScope::paths(vec![".cowd/checkpoints".into(), "src/shared.rs".into()])
        );
        assert!(!process_mixed.spawns_process);
        process_mixed.spawns_process = true;
        let process_scope = resource_scope_from_effect(&process_mixed);
        assert_eq!(process_scope.kind, "workspace");
        assert_eq!(process_scope.paths, vec!["."]);
        assert!(!process_scope.network && !process_scope.unknown);
        assert_eq!(
            normalized_resource_scope(&process_mixed, &workspace).unwrap(),
            process_scope
        );
        process_mixed.uses_network = true;
        let network_process_scope = resource_scope_from_effect(&process_mixed);
        assert_eq!(network_process_scope.kind, "workspace");
        assert_eq!(network_process_scope.paths, vec!["."]);
        assert!(network_process_scope.network && !network_process_scope.unknown);
        assert_eq!(
            normalized_resource_scope(&process_mixed, &workspace).unwrap(),
            network_process_scope
        );
        process_mixed.scopes.push(PermissionScope {
            resource: PermissionResource::File,
            operation: PermissionOperation::Write,
            target: Some("../escaped-child".into()),
        });
        assert!(normalized_resource_scope(&process_mixed, &workspace).is_err());
    }

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
    fn identity_rejection_precedes_any_descriptor_resolution() {
        let resolutions = std::cell::Cell::new(0);
        let workspace = tempfile::tempdir().unwrap();
        let describe = |name: &str, input: &Value| {
            resolutions.set(resolutions.get() + 1);
            Some((fixture_effect(name, input), 1, "fixture".into()))
        };
        let invalid = request("task", "read_file", vec!["task".to_string()]);
        assert!(GovernedToolCompiler
            .compile(workspace.path(), &[invalid], describe)
            .is_err());
        assert_eq!(
            resolutions.get(),
            0,
            "self dependency must reject before calling the resolver"
        );
        let valid = request("valid", "read_file", Vec::new());
        assert!(GovernedToolCompiler
            .compile(workspace.path(), &[valid], describe)
            .is_ok());
        assert_eq!(
            resolutions.get(),
            1,
            "the independent positive must actually resolve"
        );
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
                "edit",
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
                tool_name: "edit".to_string(),
                input: r#"{"path":"src/lib.rs","content":"a"}"#.to_string(),
                depends_on: Vec::new(),
            },
            ToolRequest {
                tool_use_id: "edit-1".to_string(),
                tool_name: "edit".to_string(),
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
                tool_name: "edit".to_string(),
                input: r#"{"path":"src/a.rs","content":"a"}"#.to_string(),
                depends_on: Vec::new(),
            },
            ToolRequest {
                tool_use_id: "write-b".to_string(),
                tool_name: "edit".to_string(),
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
            tool_name: "tool_orchestrate".to_string(),
            input: r#"{"calls":[]}"#.to_string(),
            depends_on: Vec::new(),
        }]);

        assert_eq!(plan.tasks[0].safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(plan.tasks[0].output_budget_class, "batch");
        assert!(plan.tasks[0].conflicts.is_empty());
    }

    #[test]
    fn bounded_read_only_process_needs_no_mutation_checkpoint_but_writes_still_do() {
        let workspace = tempfile::tempdir().unwrap();
        let lease = tools::ToolHost::builtin("checkpoint-process-contract", workspace.path()).pin_snapshot();
        let input = serde_json::json!({"language":"python", "code":"print(1)", "input_files":["src/a.rs"]});
        let request = ToolRequest { tool_use_id:"process".into(), tool_name:"execute_code".into(),
            input:input.to_string(), depends_on:Vec::new() };
        let invocation = lease.prepare_governed_invocation(&request.tool_use_id, &request.tool_name, &input, &[]);
        assert!(tools::tool_orchestrator::effect_is_bounded_read_only_compute(&invocation.effect));
        let mut plan = GovernedToolCompiler.compile_prepared_partial(workspace.path(), &[request], &[invocation], &Default::default())
            .unwrap().plan.unwrap();
        let decision = execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);
        let safe = plan.validate_against_execution_decision(&decision);
        assert!(safe.allowed && !safe.requires_checkpoint, "{safe:?}");

        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        let write = PermissionScope { resource:PermissionResource::File, operation:PermissionOperation::Write,
            target:Some("out.txt".into()) };
        plan.tasks[0].invocation.effect.scopes.push(write);
        plan.tasks[0].effect = plan.tasks[0].invocation.effect.clone();
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint,
            "a process with a write scope must not bypass the checkpoint boundary");
        plan.tasks[0].invocation.effect.uses_network = true;
        plan.tasks[0].effect = plan.tasks[0].invocation.effect.clone();
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint,
            "a networked process must remain conservative");
    }

    #[test]
    fn checkpoint_uses_effect_scope_for_internal_control_and_keeps_other_writes() {
        use harness_contract::policy::{EffectExternality, PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{ToolApprovalClass, ToolEffectResolverSpec, ToolPermissionMode};
        let input = serde_json::json!({"actions": []});
        let effect = tools::tool_orchestrator::resolve_registered_tool_effect(
            &ToolEffectResolverSpec { resolver_id: "runtime.agent_action".into(), resolver_version: 2 },
            "agent_action_batch", &input, ToolPermissionMode::ReadOnly,
        );
        let request = request_with_input("batch", "agent_action_batch", &input.to_string(), Vec::new());
        let workspace = std::env::current_dir().unwrap();
        let mut plan = GovernedToolCompiler.compile(&workspace, &[request], |_, _|
            Some((effect.clone(), 1, "original-control".to_string()))).unwrap();
        let decision = execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);
        assert!(plan.validate_against_execution_decision(&decision).allowed);
        assert!(!plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let original = plan.tasks[0].invocation.effect.clone();
        let mut changed = original.clone();
        changed.scopes.push(PermissionScope { resource: PermissionResource::File,
            operation: PermissionOperation::Write, target: Some("out.txt".into()) });
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let mut changed = original.clone();
        changed.assessment.externality = EffectExternality::Unknown;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let mut changed = original.clone();
        changed.uses_network = true;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let mut changed = original.clone();
        changed.scopes.clear();
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let mut changed = original.clone();
        changed.spawns_process = true;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let mut changed = original.clone();
        changed.required_permission = ToolPermissionMode::WorkspaceWrite;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(plan.validate_against_execution_decision(&decision).requires_checkpoint);
        let mut changed = original.clone();
        changed.effect_kind = ToolEffectKind::Unknown;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        let unknown = plan.validate_against_execution_decision(&decision);
        assert!(unknown.requires_approval && unknown.requires_checkpoint);
        let mut changed = original.clone();
        changed.approval_class = ToolApprovalClass::Policy;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        let approval = plan.validate_against_execution_decision(&decision);
        assert!(approval.requires_approval && approval.requires_checkpoint);
        let mut changed = original.clone();
        changed.effect_kind = ToolEffectKind::Network;
        changed.uses_network = true;
        plan.tasks[0].invocation.effect = changed.clone();
        plan.tasks[0].effect = changed;
        assert!(!plan.validate_against_execution_decision(&decision).requires_checkpoint);
        plan.tasks[0].invocation.effect = original;
        assert!(!plan.validate_against_execution_decision(&decision).allowed);
    }

    #[test]
    fn prediction_cannot_refuse_concrete_read_network_or_write_effects() {
        let plans = [
            GovernedToolPlan::from_requests(&[request("read", "read_file", Vec::new())]),
            GovernedToolPlan::from_requests(&[request("network", "web_search", Vec::new())]),
            GovernedToolPlan::from_requests(&[request_with_input(
                "write",
                "edit",
                r#"{"path":"src/lib.rs","content":"x"}"#,
                Vec::new(),
            )]),
        ];
        for target in [
            RuntimeCompileTarget::InlineModel,
            RuntimeCompileTarget::EvidenceGraph,
            RuntimeCompileTarget::ExecutionGraph,
        ] {
            for risk in [TaskRisk::Low, TaskRisk::Critical] {
                let mut decision = execution_decision(target, risk, &[], &[]);
                decision.executable = false;
                for plan in &plans {
                    let report = plan.validate_against_execution_decision(&decision);
                    assert!(report.allowed, "{report:?}");
                    assert!(report.findings.is_empty());
                    assert_eq!(report.lease_id, "lease-test");
                }
                assert!(
                    !plans[0]
                        .validate_against_execution_decision(&decision)
                        .requires_checkpoint
                );
                assert!(
                    plans[2]
                        .validate_against_execution_decision(&decision)
                        .requires_checkpoint
                );
            }
        }
    }

    #[test]
    fn concrete_unknown_and_destructive_effects_keep_safeguards_without_strategy_gates() {
        for kind in [
            ToolEffectKind::Unknown,
            ToolEffectKind::Destructive,
            ToolEffectKind::Process,
        ] {
            let compiled = GovernedToolCompiler.compile(
                &std::env::current_dir().unwrap(),
                &[request("effect", "registered-effect", Vec::new())],
                |name, input| {
                    let mut effect = fixture_effect(name, input);
                    effect.effect_kind = kind;
                    effect.approval_class = harness_contract::tool::ToolApprovalClass::User;
                    Some((effect, 1, "registered-set".to_string()))
                },
            );
            if kind == ToolEffectKind::Unknown {
                assert!(
                    matches!(compiled, Err(GovernedToolCompileError::InvalidNormalizedScope { ref scope, .. }) if scope == "unknown")
                );
                continue;
            }
            let plan = compiled.unwrap();
            let decision =
                execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);
            let report = plan.validate_against_execution_decision(&decision);
            assert!(report.allowed);
            assert!(report.requires_checkpoint);
            assert!(report.requires_approval);
            // Preparing safeguards is not a fabricated checkpoint or grant.
            assert!(!report.checkpoint_created);
            let mut corrupt = plan.clone();
            corrupt.tasks[0].effect.effect_kind = ToolEffectKind::Read;
            let refused = corrupt.validate_against_execution_decision(&decision);
            assert!(!refused.allowed);
            assert_eq!(refused.findings, ["prepared_effect_identity_mismatch"]);
        }
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
        let evidence_report = plan.validate_against_execution_decision(&evidence_only);
        assert!(evidence_report.allowed);
        assert!(evidence_report.requires_approval);
        assert!(evidence_report.requires_checkpoint);

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
    fn bounded_change_is_a_planning_hint_not_a_negative_effect_authority() {
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
                "edit",
                r#"{"path":"src/lib.rs","content":"x"}"#,
                Vec::new(),
            ),
            request_with_input(
                "edit-1",
                "edit",
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
                "edit",
                r#"{"path":"src/lib.rs","content":"x"}"#,
                Vec::new(),
            ),
            request_with_input(
                "write-2",
                "edit",
                r#"{"path":"src/main.rs","content":"y"}"#,
                Vec::new(),
            ),
        ]);
        assert!(
            multiple_paths
                .validate_against_execution_decision(&decision)
                .allowed
        );

        let unknown_path =
            GovernedToolPlan::from_requests(&[request("custom-1", "custom_mutation", Vec::new())]);
        assert!(
            unknown_path
                .validate_against_execution_decision(&decision)
                .allowed
        );
    }

    #[test]
    fn runtime_entry_tools_defer_to_their_inner_validators() {
        let plan = GovernedToolPlan::from_requests(&[
            request("capabilities-1", "RuntimeCapabilities", Vec::new()),
            request("publish-1", "task_publish", Vec::new()),
        ]);
        let decision =
            execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);

        assert!(plan.validate_against_execution_decision(&decision).allowed);
    }

    #[test]
    fn runtime_state_updates_do_not_claim_a_workspace_write_scope() {
        let plan = GovernedToolPlan::from_requests(&[
            request("todo-1", "todo_write", Vec::new()),
            request("publish-1", "task_publish", Vec::new()),
        ]);

        assert_eq!(plan.tasks[0].resource_scope.kind, "runtime");
        assert_eq!(plan.tasks[1].resource_scope.kind, "internal_transaction");
        assert!(plan
            .tasks
            .iter()
            .all(|task| task.resource_scope.paths.is_empty()));
    }

    #[test]
    fn independent_agent_actions_remain_parallel_and_failure_isolated() {
        let plan = GovernedToolPlan::from_requests(&[
            request("publish-1", "task_publish", Vec::new()),
            request("publish-2", "task_publish", Vec::new()),
            request("publish-3", "task_publish", Vec::new()),
        ]);

        assert!(plan.tasks.iter().all(|task| {
            task.resource_scope.kind == "internal_transaction"
                && task.predecessors.is_empty()
                && task.depends_on.is_empty()
                && task.can_parallelize
        }));
        assert!(plan.tasks.iter().all(|task| task.conflicts.is_empty()));
    }

    #[test]
    fn speculative_non_executable_decision_does_not_deny_an_authorizable_read() {
        let plan = GovernedToolPlan::from_requests(&[request("read-1", "read_file", Vec::new())]);
        let mut decision =
            execution_decision(RuntimeCompileTarget::InlineModel, TaskRisk::Low, &[], &[]);
        decision.executable = false;
        let report = plan.validate_against_execution_decision(&decision);
        let wire = serde_json::to_value(&report).unwrap();
        assert_eq!(wire["allowed"], true);
        assert_eq!(wire["findings"], serde_json::json!([]));
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

#[cfg(test)]
#[path = "governed_tool_plan_prepared_tests.rs"]
mod prepared_contract_tests;
