use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionGraphProjection, ExecutionNodeSpec,
};

use super::*;

impl AgentService {
    pub(crate) async fn list_execution_graphs(
        &self,
        task_service: &TaskService,
    ) -> Result<serde_json::Value, String> {
        let graphs = task_service.execution_graphs().await?;
        Ok(serde_json::json!({
            "kind": "execution_graphs",
            "graphs": graphs,
        }))
    }

    pub(crate) async fn execution_graph(
        &self,
        task_service: &TaskService,
        task_id: &str,
    ) -> Result<Option<ExecutionGraphProjection>, String> {
        task_service.execution_graph(task_id).await
    }

    /// Register a task's canonical execution graph through Runtime's only
    /// commit service. Gateway validates HTTP DTOs and caches the resulting
    /// read-only projection; it never advances node state.
    pub(crate) async fn register_execution_graph(
        &self,
        task_service: &TaskService,
        task_id: &str,
        objective: Option<String>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
    ) -> Result<ExecutionGraphProjection, String> {
        task_service
            .register_execution_graph(
                task_id,
                objective,
                nodes,
                edges,
                vec![harness_contract::reality::EvidenceRef::new(
                    "gateway_command",
                    format!("register-graph:{task_id}"),
                )
                .with_source("gateway.agent_service")],
            )
            .await
    }
}
