use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphProjection,
};
use serde::{Deserialize, Serialize};

use super::ExecutionRunnerError;

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
    pub graph_id: String,
    pub admission_id: String,
    pub accepted_revision: u64,
    pub queue_partition: u16,
    pub accepted_at_ms: u64,
}
