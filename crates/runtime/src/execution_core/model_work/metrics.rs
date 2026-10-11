use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionWorkGraphProjection, project_work_graph,
};

#[must_use]
pub fn model_work_metrics(graph: &ExecutionGraph) -> Option<ExecutionWorkGraphProjection> {
    project_work_graph(graph)
}
