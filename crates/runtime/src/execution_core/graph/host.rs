use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphProjection,
};
use serde::{Deserialize, Serialize};

use super::{ExecutionGraphRunner, ExecutionRunReport, ExecutionRunnerError};

/// The only stateful entry point callers may use to drive an execution graph.
///
/// Callers may compile or inspect graphs, but they must not advance node state
/// themselves. RuntimeServices owns one implementation and injects this trait
/// into Gateway adapters and background supervisors.
#[async_trait]
pub trait ExecutionGraphHost: Send + Sync {
    async fn submit_graph(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError>;

    async fn command_graph(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError>;

    async fn graph_projection(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraphProjection, ExecutionRunnerError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphHostReceipt {
    pub graph: ExecutionGraphProjection,
    pub run: Option<ExecutionRunReport>,
}

#[async_trait]
impl ExecutionGraphHost for ExecutionGraphRunner {
    async fn submit_graph(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError> {
        if !matches!(
            command,
            ExecutionGraphCommand::Start { expected_revision }
                if expected_revision == graph.revision
        ) {
            return Err(ExecutionRunnerError::InvalidStartCommand);
        }
        let graph_id = graph.id.clone();
        let run = self.start(graph).await?;
        let graph = self.projection(&graph_id).await?;
        Ok(ExecutionGraphHostReceipt {
            graph,
            run: Some(run),
        })
    }

    async fn command_graph(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraphHostReceipt, ExecutionRunnerError> {
        self.command(graph_id, command).await?;
        Ok(ExecutionGraphHostReceipt {
            graph: self.projection(graph_id).await?,
            run: None,
        })
    }

    async fn graph_projection(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraphProjection, ExecutionRunnerError> {
        self.projection(graph_id).await
    }
}
