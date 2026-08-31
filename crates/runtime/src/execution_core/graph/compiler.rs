use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionGraphValidationError,
    ExecutionNodeKind, ExecutionNodeSpec, ExecutionWorkContract, ExecutionWorkRole,
};
use thiserror::Error;

use crate::execution_core::{
    ModelWorkCompileError, ModelWorkGraphCompiler, ModelWorkNode, ModelWorkPlan,
    RuntimeCompileTarget,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCompileRequest {
    pub objective: String,
    pub payload_ref: String,
    pub target: RuntimeCompileTarget,
    /// Canonical locks/leases inherited by executable nodes. Read-only model,
    /// verify and synthesize nodes intentionally do not acquire mutation locks.
    pub resource_scopes: Vec<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExecutionCompileError {
    #[error(transparent)]
    InvalidGraph(#[from] ExecutionGraphValidationError),
    #[error(transparent)]
    InvalidModelWork(#[from] ModelWorkCompileError),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ExecutionGraphCompiler;

impl ExecutionGraphCompiler {
    /// Compile the durable bootstrap graph for a provider-driven conversation turn.
    ///
    /// Target-specific guard nodes persist the initial constraints while the model
    /// node remains responsible for compiling the provider-selected continuation.
    pub fn compile_conversation_turn(
        &self,
        request: ExecutionCompileRequest,
    ) -> Result<ExecutionGraph, ExecutionCompileError> {
        let mut graph = ExecutionGraph::new(request.objective);
        let target = request.target;
        let model = node(
            &graph.id,
            "model",
            ExecutionNodeKind::InlineModel,
            "inline_model",
            &request.payload_ref,
            &[],
        );
        graph.nodes = match target {
            RuntimeCompileTarget::InlineModel => vec![model],
            RuntimeCompileTarget::EvidenceGraph => vec![
                target_guard(
                    &graph.id,
                    "evidence-constraint",
                    &request.payload_ref,
                    "evidence_read_before_synthesis",
                    &[],
                ),
                model,
            ],
            RuntimeCompileTarget::ExecutionGraph => vec![
                target_guard(
                    &graph.id,
                    "policy-constraint",
                    &request.payload_ref,
                    "permission_and_policy_gate_required",
                    &[],
                ),
                target_guard(
                    &graph.id,
                    "resource-constraint",
                    &request.payload_ref,
                    "mutation_resources_must_be_leased",
                    &request.resource_scopes,
                ),
                model,
            ],
        };
        graph.edges = dependencies(&graph.nodes);
        compile_model_work_graph(graph)
    }

    pub fn compile(
        &self,
        request: ExecutionCompileRequest,
    ) -> Result<ExecutionGraph, ExecutionCompileError> {
        let mut graph = ExecutionGraph::new(request.objective);
        let execution_scopes = request.resource_scopes;
        match request.target {
            RuntimeCompileTarget::InlineModel => {
                graph.nodes = vec![
                    node(
                        &graph.id,
                        "model",
                        ExecutionNodeKind::InlineModel,
                        "inline_model",
                        &request.payload_ref,
                        &[],
                    ),
                    node(
                        &graph.id,
                        "verify",
                        ExecutionNodeKind::Verify,
                        "verify",
                        &request.payload_ref,
                        &[],
                    ),
                    node(
                        &graph.id,
                        "synthesize",
                        ExecutionNodeKind::Synthesize,
                        "synthesize",
                        &request.payload_ref,
                        &[],
                    ),
                ];
                graph.edges = dependencies(&graph.nodes);
            }
            RuntimeCompileTarget::EvidenceGraph => {
                graph.nodes = vec![
                    node(
                        &graph.id,
                        "model",
                        ExecutionNodeKind::InlineModel,
                        "inline_model",
                        &request.payload_ref,
                        &[],
                    ),
                    node(
                        &graph.id,
                        "tool-evidence",
                        ExecutionNodeKind::ToolBatch,
                        "tool_batch",
                        &request.payload_ref,
                        &execution_scopes,
                    ),
                    node(
                        &graph.id,
                        "synthesize",
                        ExecutionNodeKind::Synthesize,
                        "synthesize",
                        &request.payload_ref,
                        &[],
                    ),
                ];
                graph.edges = dependencies(&graph.nodes);
            }
            RuntimeCompileTarget::ExecutionGraph => {
                graph.nodes = vec![
                    node(
                        &graph.id,
                        "model",
                        ExecutionNodeKind::InlineModel,
                        "inline_model",
                        &request.payload_ref,
                        &[],
                    ),
                    node(
                        &graph.id,
                        "tool-execution",
                        ExecutionNodeKind::ToolBatch,
                        "tool_batch",
                        &request.payload_ref,
                        &execution_scopes,
                    ),
                    node(
                        &graph.id,
                        "verify",
                        ExecutionNodeKind::Verify,
                        "verify",
                        &request.payload_ref,
                        &[],
                    ),
                    node(
                        &graph.id,
                        "synthesize",
                        ExecutionNodeKind::Synthesize,
                        "synthesize",
                        &request.payload_ref,
                        &[],
                    ),
                ];
                graph.edges = dependencies(&graph.nodes);
            }
        }
        compile_model_work_graph(graph)
    }
}

fn target_guard(
    graph_id: &str,
    suffix: &str,
    payload_ref: &str,
    criterion: &str,
    resource_scopes: &[String],
) -> ExecutionNodeSpec {
    let mut guard = node(
        graph_id,
        suffix,
        ExecutionNodeKind::Verify,
        super::executors::CompileTargetGuardExecutor::KIND,
        payload_ref,
        resource_scopes,
    );
    guard.acceptance.criteria.push(criterion.to_string());
    guard
}

fn node(
    graph_id: &str,
    suffix: &str,
    kind: ExecutionNodeKind,
    executor_kind: &str,
    payload_ref: &str,
    resource_scopes: &[String],
) -> ExecutionNodeSpec {
    let id = format!("{graph_id}:{suffix}");
    ExecutionNodeSpec {
        id: id.clone(),
        kind,
        payload_ref: payload_ref.to_string(),
        executor_kind: executor_kind.to_string(),
        idempotency_key: format!("{id}:attempt"),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: resource_scopes.to_vec(),
        work: Some(ExecutionWorkContract::new(work_role(kind))),
    }
}

const fn work_role(kind: ExecutionNodeKind) -> ExecutionWorkRole {
    match kind {
        ExecutionNodeKind::ToolBatch | ExecutionNodeKind::Materialize => ExecutionWorkRole::Tool,
        ExecutionNodeKind::Verify | ExecutionNodeKind::Approval => ExecutionWorkRole::Verify,
        ExecutionNodeKind::Synthesize => ExecutionWorkRole::Synthesize,
        ExecutionNodeKind::AgentTask | ExecutionNodeKind::Subgraph => {
            ExecutionWorkRole::EvidenceAnalyze
        }
        ExecutionNodeKind::InlineModel
        | ExecutionNodeKind::SessionDispatch
        | ExecutionNodeKind::Timer => ExecutionWorkRole::Plan,
    }
}

fn dependencies(nodes: &[ExecutionNodeSpec]) -> Vec<ExecutionEdge> {
    nodes
        .windows(2)
        .map(|pair| ExecutionEdge {
            from: pair[0].id.clone(),
            to: pair[1].id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        })
        .collect()
}

fn compile_model_work_graph(
    graph: ExecutionGraph,
) -> Result<ExecutionGraph, ExecutionCompileError> {
    let revision = graph.revision;
    let service_class = graph.service_class;
    let parent_execution = graph.parent_execution.clone();
    let orchestration = graph.orchestration.clone();
    let node_statuses = graph.node_statuses.clone();
    let node_results = graph.node_results.clone();
    let recovery_cursor = graph.recovery_cursor.clone();
    let mut dependencies = std::collections::BTreeMap::<String, Vec<String>>::new();
    for edge in graph
        .edges
        .iter()
        .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn)
    {
        dependencies
            .entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }
    let nodes = graph
        .nodes
        .into_iter()
        .map(|node| {
            let work = node
                .work
                .clone()
                .unwrap_or_else(|| ExecutionWorkContract::new(work_role(node.kind)));
            ModelWorkNode {
                id: node.id.clone(),
                role: work.role,
                kind: node.kind,
                executor_kind: node.executor_kind,
                payload_ref: node.payload_ref,
                depends_on: dependencies.remove(&node.id).unwrap_or_default(),
                required: work.required,
                dependency: work.dependency,
                cancellation_group: work.cancellation_group,
                required_evidence_refs: work.required_evidence_refs,
                context_view_ref: work.context_view_ref,
                model_profile: work.model_profile,
                reasoning_effort: work.reasoning_effort,
                expected_input_tokens: work.expected_input_tokens,
                expected_output_tokens: work.expected_output_tokens,
                expected_duration_ms: work.expected_duration_ms,
                acceptance: node.acceptance,
                retry_policy: node.retry_policy,
                resource_scopes: node.resource_scopes,
            }
        })
        .collect();
    let mut compiled = ModelWorkGraphCompiler.compile(ModelWorkPlan {
        objective: graph.objective,
        graph_id: Some(graph.id),
        nodes,
    })?;
    compiled.revision = revision;
    compiled.service_class = service_class;
    compiled.parent_execution = parent_execution;
    compiled.orchestration = orchestration;
    compiled.node_statuses = node_statuses;
    compiled.node_results = node_results;
    compiled.recovery_cursor = recovery_cursor;
    Ok(compiled)
}

#[cfg(test)]
mod metadata_tests {
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionServiceClass,
    };

    use super::{compile_model_work_graph, node};

    #[test]
    fn model_work_compilation_preserves_canonical_graph_metadata() {
        let mut graph = ExecutionGraph::new("preserve metadata");
        graph.revision = 7;
        graph.service_class = ExecutionServiceClass::Maintenance;
        graph.recovery_cursor.commit_cursor = 13;
        graph.nodes.push(node(
            &graph.id,
            "model",
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "{}",
            &[],
        ));

        let compiled = compile_model_work_graph(graph).expect("compile");

        assert_eq!(compiled.revision, 7);
        assert_eq!(compiled.service_class, ExecutionServiceClass::Maintenance);
        assert_eq!(compiled.recovery_cursor.commit_cursor, 13);
    }
}
