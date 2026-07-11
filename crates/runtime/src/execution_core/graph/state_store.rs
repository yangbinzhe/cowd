use std::sync::Arc;

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphProjection, ExecutionNodeProjection, ExecutionNodeStatus,
};
use thiserror::Error;

use crate::runtime_event_store::{RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError};

use super::events::ExecutionGraphEvent;

#[derive(Debug, Error)]
pub enum ExecutionStateStoreError {
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
    #[error("execution graph event payload is invalid: {0}")]
    InvalidPayload(#[from] serde_json::Error),
    #[error("execution graph `{0}` does not exist")]
    NotFound(String),
    #[error("execution graph `{graph_id}` stream is corrupt: {reason}")]
    Corrupt { graph_id: String, reason: String },
    #[error("execution state blocking task failed: {0}")]
    BlockingTask(String),
}

#[derive(Clone)]
pub struct ExecutionGraphStateStore {
    event_store: Arc<RuntimeEventStore>,
}

impl ExecutionGraphStateStore {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self { event_store }
    }

    pub fn load(&self, graph_id: &str) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        let events = self.event_store.list_stream(graph_id).map_err(|error| {
            ExecutionStateStoreError::Corrupt {
                graph_id: graph_id.to_string(),
                reason: error,
            }
        })?;
        if events.is_empty() {
            return Err(ExecutionStateStoreError::NotFound(graph_id.to_string()));
        }
        let mut projected = None;
        for record in events {
            if record.scope != RuntimeEventScope::ExecutionGraph {
                return Err(ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason: format!("unexpected scope {:?}", record.scope),
                });
            }
            let event: ExecutionGraphEvent = serde_json::from_value(record.payload)?;
            let mut graph = event.graph().clone();
            if graph.id != graph_id || graph.revision != record.sequence {
                return Err(ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason: format!(
                        "event identity/revision mismatch: graph={} revision={}, stream_sequence={}",
                        graph.id, graph.revision, record.sequence
                    ),
                });
            }
            if projected.as_ref().is_some_and(|previous: &ExecutionGraph| {
                graph.revision != previous.revision.saturating_add(1)
            }) {
                return Err(ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason: "non-contiguous graph revision".to_string(),
                });
            }
            graph.recovery_cursor.commit_cursor = record.commit_cursor;
            projected = Some(graph);
        }
        projected.ok_or_else(|| ExecutionStateStoreError::NotFound(graph_id.to_string()))
    }

    pub async fn load_async(
        &self,
        graph_id: impl Into<String>,
    ) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        let store = self.clone();
        let graph_id = graph_id.into();
        tokio::task::spawn_blocking(move || store.load(&graph_id))
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }

    pub fn graph_ids(&self) -> Result<Vec<String>, ExecutionStateStoreError> {
        self.event_store
            .stream_ids_for_scope(RuntimeEventScope::ExecutionGraph)
            .map_err(ExecutionStateStoreError::EventStore)
    }

    pub async fn graph_ids_async(&self) -> Result<Vec<String>, ExecutionStateStoreError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.graph_ids())
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }

    pub fn nonterminal_graph_ids(&self) -> Result<Vec<String>, ExecutionStateStoreError> {
        let mut graph_ids = Vec::new();
        for graph_id in self.graph_ids()? {
            let graph = self.load(&graph_id)?;
            if !graph_is_terminal(&graph) {
                graph_ids.push(graph_id);
            }
        }
        Ok(graph_ids)
    }

    pub async fn nonterminal_graph_ids_async(
        &self,
    ) -> Result<Vec<String>, ExecutionStateStoreError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.nonterminal_graph_ids())
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }

    pub fn projection(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraphProjection, ExecutionStateStoreError> {
        let graph = self.load(graph_id)?;
        Ok(ExecutionGraphProjection {
            graph_id: graph.id.clone(),
            revision: graph.revision,
            objective: graph.objective.clone(),
            nodes: graph
                .nodes
                .iter()
                .map(|node| {
                    let result = graph.node_results.get(&node.id);
                    ExecutionNodeProjection {
                        node_id: node.id.clone(),
                        kind: node.kind,
                        status: graph.node_statuses[&node.id],
                        executor_kind: node.executor_kind.clone(),
                        result_ref: result.and_then(|result| result.result_ref.clone()),
                        evidence_refs: result
                            .map(|result| result.evidence_refs.clone())
                            .unwrap_or_default(),
                    }
                })
                .collect(),
            commit_cursor: graph.recovery_cursor.commit_cursor,
            terminal_result_ref: graph
                .nodes
                .iter()
                .rev()
                .filter_map(|node| graph.node_results.get(&node.id))
                .find_map(|result| result.result_ref.clone()),
        })
    }

    pub async fn projection_async(
        &self,
        graph_id: impl Into<String>,
    ) -> Result<ExecutionGraphProjection, ExecutionStateStoreError> {
        let store = self.clone();
        let graph_id = graph_id.into();
        tokio::task::spawn_blocking(move || store.projection(&graph_id))
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }
}

fn graph_is_terminal(graph: &ExecutionGraph) -> bool {
    !graph.node_statuses.is_empty()
        && graph
            .node_statuses
            .values()
            .copied()
            .all(ExecutionNodeStatus::is_terminal)
}
