use harness_contract::execution_graph::ExecutionGraphProjection;

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
}
