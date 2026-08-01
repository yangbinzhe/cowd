use std::collections::BTreeSet;

use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionEdgeKind, ExecutionGraph,
    ExecutionGraphValidationError, ExecutionNodeSpec, ExecutionWorkContract,
};
use thiserror::Error;

use super::graph::ModelWorkPlan;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ModelWorkCompileError {
    #[error("duplicate model work node `{0}`")]
    DuplicateNode(String),
    #[error("model work dependency `{dependency}` for `{node}` is absent")]
    MissingDependency { node: String, dependency: String },
    #[error(transparent)]
    InvalidGraph(#[from] ExecutionGraphValidationError),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ModelWorkGraphCompiler;

impl ModelWorkGraphCompiler {
    pub fn compile(&self, plan: ModelWorkPlan) -> Result<ExecutionGraph, ModelWorkCompileError> {
        let mut graph = ExecutionGraph::new(plan.objective);
        if let Some(graph_id) = plan.graph_id {
            graph.id = graph_id;
        }
        let mut ids = BTreeSet::new();
        for node in &plan.nodes {
            if !ids.insert(node.id.clone()) {
                return Err(ModelWorkCompileError::DuplicateNode(node.id.clone()));
            }
        }
        for node in &plan.nodes {
            for dependency in &node.depends_on {
                if !ids.contains(dependency) {
                    return Err(ModelWorkCompileError::MissingDependency {
                        node: node.id.clone(),
                        dependency: dependency.clone(),
                    });
                }
                graph.edges.push(ExecutionEdge {
                    from: dependency.clone(),
                    to: node.id.clone(),
                    kind: ExecutionEdgeKind::DependsOn,
                });
            }
            let mut contract = ExecutionWorkContract::new(node.role);
            contract.required = node.required;
            contract.dependency = node.dependency.clone();
            contract.cancellation_group = node.cancellation_group.clone();
            contract.required_evidence_refs = node.required_evidence_refs.clone();
            contract.context_view_ref = node.context_view_ref.clone();
            contract.model_profile = node.model_profile.clone();
            contract.reasoning_effort = node.reasoning_effort.clone();
            contract.expected_input_tokens = node.expected_input_tokens;
            contract.expected_output_tokens = node.expected_output_tokens;
            contract.expected_duration_ms = node.expected_duration_ms;
            graph.nodes.push(ExecutionNodeSpec {
                id: node.id.clone(),
                kind: node.kind,
                payload_ref: node.payload_ref.clone(),
                executor_kind: node.executor_kind.clone(),
                idempotency_key: format!("{}:attempt", node.id),
                lease_ref: None,
                acceptance: node.acceptance.clone(),
                retry_policy: node.retry_policy.clone(),
                resource_scopes: node.resource_scopes.clone(),
                work: Some(contract),
            });
        }
        validate_execution_graph(&graph)?;
        Ok(graph)
    }
}
