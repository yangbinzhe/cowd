use harness_contract::execution_graph::{
    project_work_graph, ExecutionGraph, ExecutionWorkGraphProjection,
};

#[must_use]
pub fn model_work_metrics(graph: &ExecutionGraph) -> Option<ExecutionWorkGraphProjection> {
    project_work_graph(graph)
}
