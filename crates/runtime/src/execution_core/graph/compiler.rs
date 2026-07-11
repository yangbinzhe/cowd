use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionEdgeKind, ExecutionGraph,
    ExecutionGraphValidationError, ExecutionNodeKind, ExecutionNodeSpec,
};
use thiserror::Error;

use crate::execution_core::RuntimeCompileTarget;

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
    #[error("execution capability `{capability}` is unavailable until version `{available_in}`")]
    CapabilityUnavailable {
        capability: &'static str,
        available_in: &'static str,
    },
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
        match target {
            RuntimeCompileTarget::InlineModel
            | RuntimeCompileTarget::EvidenceGraph
            | RuntimeCompileTarget::ExecutionGraph => {}
            RuntimeCompileTarget::DeliberationGraph => {
                return Err(ExecutionCompileError::CapabilityUnavailable {
                    capability: "deliberate",
                    available_in: "V6",
                });
            }
            RuntimeCompileTarget::TeamGraph => {
                return Err(ExecutionCompileError::CapabilityUnavailable {
                    capability: "collaborate",
                    available_in: "V5",
                });
            }
            RuntimeCompileTarget::MissionGraph => {
                return Err(ExecutionCompileError::CapabilityUnavailable {
                    capability: "supervise",
                    available_in: "V8",
                });
            }
        };
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
            _ => unreachable!("unavailable targets returned above"),
        };
        graph.edges = dependencies(&graph.nodes);
        validate_execution_graph(&graph)?;
        Ok(graph)
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
            RuntimeCompileTarget::DeliberationGraph => {
                return Err(ExecutionCompileError::CapabilityUnavailable {
                    capability: "deliberate",
                    available_in: "V6",
                });
            }
            RuntimeCompileTarget::TeamGraph => {
                return Err(ExecutionCompileError::CapabilityUnavailable {
                    capability: "collaborate",
                    available_in: "V5",
                });
            }
            RuntimeCompileTarget::MissionGraph => {
                return Err(ExecutionCompileError::CapabilityUnavailable {
                    capability: "supervise",
                    available_in: "V8",
                });
            }
        }
        validate_execution_graph(&graph)?;
        Ok(graph)
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
