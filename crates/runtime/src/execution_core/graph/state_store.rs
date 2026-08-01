use std::sync::Arc;

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphProjection, ExecutionNodeProjection, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::runtime_event_store::{RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError};

use super::{commit_service::execution_lineage_stream_id, events::ExecutionGraphEvent};
use crate::execution_core::hot_state::{HotExecutionGraphRegistry, RuntimeHotStatePlane};

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
    hot_graphs: Arc<HotExecutionGraphRegistry>,
    verify_durable_revision: bool,
}

/// Durable, reverse lookup entry for one graph that was registered under a
/// parent graph node. The canonical parent binding remains on the child graph;
/// this event-backed index only makes root-to-descendant projections O(tree)
/// instead of O(all execution graphs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionChildLink {
    pub parent_execution_id: String,
    pub parent_node_id: String,
    pub child_execution_id: String,
    pub child_objective: String,
}

impl ExecutionGraphStateStore {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        let mut store =
            Self::with_hot_state(event_store, Arc::new(RuntimeHotStatePlane::default()));
        store.verify_durable_revision = true;
        store
    }

    #[must_use]
    pub fn with_hot_state(
        event_store: Arc<RuntimeEventStore>,
        hot_state: Arc<RuntimeHotStatePlane>,
    ) -> Self {
        Self {
            event_store,
            hot_graphs: Arc::clone(hot_state.graphs()),
            verify_durable_revision: false,
        }
    }

    pub fn load(&self, graph_id: &str) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        if !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(graph_id) {
                return Ok((*graph).clone());
            }
        }
        let permit = self.hot_graphs.recovery_permit(graph_id);
        if !permit.is_leader() && !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(graph_id) {
                return Ok((*graph).clone());
            }
            drop(permit);
            return self.load(graph_id);
        }
        if !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(graph_id) {
                return Ok((*graph).clone());
            }
        }
        let events = self.event_store.list_stream(graph_id).map_err(|error| {
            ExecutionStateStoreError::Corrupt {
                graph_id: graph_id.to_string(),
                reason: error,
            }
        })?;
        if events.is_empty() {
            return Err(ExecutionStateStoreError::NotFound(graph_id.to_string()));
        }
        let checkpoint_index = events
            .iter()
            .rposition(|event| {
                event.kind == "execution_graph.planned"
                    || event.kind == "execution_graph.checkpoint"
            })
            .unwrap_or(0);
        let mut projected = None;
        for record in events.into_iter().skip(checkpoint_index) {
            if record.scope != RuntimeEventScope::ExecutionGraph {
                return Err(ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason: format!("unexpected scope {:?}", record.scope),
                });
            }
            let event: ExecutionGraphEvent = serde_json::from_value(record.payload)?;
            let mut graph = event.project(projected.take()).map_err(|reason| {
                ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason,
                }
            })?;
            if graph.id != graph_id || graph.revision != record.sequence {
                return Err(ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason: format!(
                        "event identity/revision mismatch: graph={} revision={}, stream_sequence={}",
                        graph.id, graph.revision, record.sequence
                    ),
                });
            }
            graph.recovery_cursor.commit_cursor = record.commit_cursor;
            projected = Some(graph);
        }
        let graph =
            projected.ok_or_else(|| ExecutionStateStoreError::NotFound(graph_id.to_string()))?;
        self.hot_graphs.record_recovery();
        self.hot_graphs.publish(graph.clone());
        Ok(graph)
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
        let stream_ids = self
            .event_store
            .stream_ids_for_scope(RuntimeEventScope::ExecutionGraph)
            .map_err(ExecutionStateStoreError::EventStore)?;
        let mut graph_ids = Vec::new();
        for stream_id in stream_ids {
            let events = self.event_store.list_stream(&stream_id).map_err(|reason| {
                ExecutionStateStoreError::Corrupt {
                    graph_id: stream_id.clone(),
                    reason,
                }
            })?;
            // Older releases incorrectly wrote Session strategy and live-state
            // evidence with `ExecutionGraph` scope. A canonical graph stream
            // always starts with its planned snapshot, so retain malformed
            // graph streams for corruption reporting while excluding those
            // legacy non-graph streams from graph enumeration.
            if events.first().is_some_and(|event| {
                event.scope == RuntimeEventScope::ExecutionGraph
                    && event.kind == "execution_graph.planned"
            }) {
                graph_ids.push(stream_id);
            }
        }
        Ok(graph_ids)
    }

    pub async fn graph_ids_async(&self) -> Result<Vec<String>, ExecutionStateStoreError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.graph_ids())
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }

    pub fn child_links(
        &self,
        parent_execution_id: &str,
    ) -> Result<Vec<ExecutionChildLink>, ExecutionStateStoreError> {
        let stream_id = execution_lineage_stream_id(parent_execution_id);
        let mut links = self
            .event_store
            .list_stream(&stream_id)
            .map_err(|reason| ExecutionStateStoreError::Corrupt {
                graph_id: parent_execution_id.to_string(),
                reason,
            })?
            .into_iter()
            .filter(|event| {
                event.scope == RuntimeEventScope::Relation
                    && event.kind == "execution.lineage.child_registered.v1"
            })
            .map(|event| serde_json::from_value::<ExecutionChildLink>(event.payload))
            .collect::<Result<Vec<_>, _>>()?;
        if links
            .iter()
            .any(|link| link.parent_execution_id != parent_execution_id)
        {
            return Err(ExecutionStateStoreError::Corrupt {
                graph_id: parent_execution_id.to_string(),
                reason: "execution lineage event belongs to another parent".to_string(),
            });
        }
        links.sort_by(|left, right| left.child_execution_id.cmp(&right.child_execution_id));
        links.dedup_by(|left, right| left.child_execution_id == right.child_execution_id);
        Ok(links)
    }

    pub async fn child_links_async(
        &self,
        parent_execution_id: impl Into<String>,
    ) -> Result<Vec<ExecutionChildLink>, ExecutionStateStoreError> {
        let store = self.clone();
        let parent_execution_id = parent_execution_id.into();
        tokio::task::spawn_blocking(move || store.child_links(&parent_execution_id))
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
            service_class: graph.service_class,
            parent_execution: graph.parent_execution.clone(),
            orchestration: graph.orchestration.clone(),
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
                        payload_ref: node.payload_ref.clone(),
                        acceptance: node.acceptance.clone(),
                        resource_scopes: node.resource_scopes.clone(),
                        result_ref: result.and_then(|result| result.result_ref.clone()),
                        summary: result.and_then(|result| result.summary.clone()),
                        failure: result.and_then(|result| result.failure.clone()),
                        evidence_refs: result
                            .map(|result| result.evidence_refs.clone())
                            .unwrap_or_default(),
                        usage: result
                            .map(|result| result.usage.clone())
                            .unwrap_or_default(),
                    }
                })
                .collect(),
            edges: graph
                .edges
                .iter()
                .map(
                    |edge| harness_contract::execution_graph::ExecutionEdgeProjection {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        kind: edge.kind,
                    },
                )
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

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{ExecutionNodeKind, ExecutionNodeSpec};

    use super::*;
    use crate::execution_core::graph::ExecutionCommitService;
    use crate::execution_core::hot_state::RuntimeHotStatePlane;

    #[test]
    fn shared_hot_plane_recovers_once_then_serves_memory() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let write_plane = Arc::new(RuntimeHotStatePlane::default());
        let commit = ExecutionCommitService::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&write_plane),
        );
        let mut graph = ExecutionGraph::new("hot graph");
        graph.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "{}",
        ));
        let registered = commit.register_graph(graph).unwrap().graph;

        let read_plane = Arc::new(RuntimeHotStatePlane::default());
        let store = ExecutionGraphStateStore::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&read_plane),
        );
        assert_eq!(store.load(&registered.id).unwrap().revision, 1);
        assert_eq!(store.load(&registered.id).unwrap().revision, 1);
        let metrics = read_plane.metrics().snapshot();
        assert_eq!(metrics.graph_recoveries, 1);
        assert!(metrics.graph_hits >= 1);
    }
}
