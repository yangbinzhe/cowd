use std::sync::Arc;

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphProjection, ExecutionNodeProjection,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::runtime_event_store::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeEventStoreError, RuntimeTransactionEventInput,
};

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
    pub(crate) fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.event_store.subscribe_commits()
    }

    fn team_governance_stream(graph_id: &str) -> String {
        format!("team-projection-governance:{graph_id}")
    }

    pub fn team_projection_quarantine(
        &self,
        graph_id: &str,
    ) -> Result<Option<serde_json::Value>, ExecutionStateStoreError> {
        let events = self
            .event_store
            .list_stream(&Self::team_governance_stream(graph_id))
            .map_err(|reason| ExecutionStateStoreError::Corrupt {
                graph_id: graph_id.to_string(),
                reason,
            })?;
        Ok(events
            .into_iter()
            .rev()
            .find(|event| event.kind == "team.projection.quarantined")
            .map(|event| event.payload))
    }

    pub fn quarantine_team_projection(
        &self,
        graph_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, ExecutionStateStoreError> {
        if let Some(existing) = self.team_projection_quarantine(graph_id)? {
            return Ok(existing);
        }
        let evidence_id = format!("team-projection-quarantine:{graph_id}");
        let payload = serde_json::json!({
            "graph_id": graph_id,
            "state": "quarantined",
            "reason": reason,
            "evidence_id": evidence_id,
        });
        let event = RuntimeEventInput {
            stream_id: Self::team_governance_stream(graph_id),
            scope: RuntimeEventScope::Team,
            kind: "team.projection.quarantined".to_string(),
            status: Some("quarantined".to_string()),
            actor: Some("runtime.team_projection_governance".to_string()),
            refs: vec![RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: graph_id.to_string(),
            }],
            payload: payload.clone(),
        };
        match self.event_store.append_batch_if_revision(
            Self::team_governance_stream(graph_id),
            0,
            format!("team-projection-quarantine:{graph_id}"),
            vec![RuntimeTransactionEventInput {
                event,
                idempotency_key: Some(format!("team-projection-quarantine:{graph_id}")),
                schema_version: 1,
            }],
        ) {
            Ok(_) => Ok(payload),
            Err(RuntimeEventStoreError::StaleRevision { .. }) => self
                .team_projection_quarantine(graph_id)?
                .ok_or_else(|| ExecutionStateStoreError::Corrupt {
                    graph_id: graph_id.to_string(),
                    reason: "Team projection quarantine raced without a durable winner".to_string(),
                }),
            Err(error) => Err(ExecutionStateStoreError::EventStore(error)),
        }
    }

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

    pub fn load_snapshot(
        &self,
        graph_id: &str,
    ) -> Result<Arc<ExecutionGraph>, ExecutionStateStoreError> {
        if !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(graph_id) {
                return Ok(graph);
            }
        }
        let permit = self.hot_graphs.recovery_permit(graph_id);
        if !permit.is_leader() && !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(graph_id) {
                return Ok(graph);
            }
            drop(permit);
            return self.load_snapshot(graph_id);
        }
        if !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(graph_id) {
                return Ok(graph);
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
        let graph = project_graph_events(graph_id, events)?;
        let graph = Arc::new(graph);
        self.hot_graphs.record_recovery();
        self.hot_graphs.publish_snapshot(Arc::clone(&graph));
        Ok(graph)
    }

    pub fn load(&self, graph_id: &str) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        self.load_snapshot(graph_id)
            .map(|graph| graph.as_ref().clone())
    }

    pub async fn load_snapshot_async(
        &self,
        graph_id: impl Into<String>,
    ) -> Result<Arc<ExecutionGraph>, ExecutionStateStoreError> {
        let store = self.clone();
        let graph_id = graph_id.into();
        tokio::task::spawn_blocking(move || store.load_snapshot(&graph_id))
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
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
            .stream_ids_for_scope_kind_at_sequence(
                RuntimeEventScope::ExecutionGraph,
                "execution_graph.planned",
                1,
            )
            .map_err(ExecutionStateStoreError::EventStore)
    }

    pub fn graph_ids_page(
        &self,
        after: Option<(u64, String)>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, ExecutionStateStoreError> {
        self.event_store
            .stream_ids_for_scope_kind_at_sequence_page(
                RuntimeEventScope::ExecutionGraph,
                "execution_graph.planned",
                1,
                after,
                limit,
            )
            .map_err(ExecutionStateStoreError::EventStore)
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
        Ok(self
            .event_store
            .latest_stream_statuses_for_scope_kind_at_sequence(
                RuntimeEventScope::ExecutionGraph,
                "execution_graph.planned",
                1,
            )?
            .into_iter()
            .filter(|(_, status)| {
                !matches!(
                    status.as_deref(),
                    Some("completed" | "failed" | "blocked" | "cancelled")
                )
            })
            .map(|(graph_id, _)| graph_id)
            .collect())
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
            lineage: graph.lineage.clone(),
            orchestration: graph.orchestration.clone(),
            delivery_envelope: graph.delivery_envelope.clone(),
            terminal_presentation: graph.terminal_presentation.clone(),
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
                        payload_ref: format!("execution-payload:{}", node.id),
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
                        work: node
                            .work
                            .as_ref()
                            .map(harness_contract::execution_graph::ExecutionWorkProjection::from),
                        work_state: node
                            .work
                            .as_ref()
                            .map(|_| graph.work_states.get(&node.id).cloned().unwrap_or_default()),
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
            work: harness_contract::execution_graph::project_work_graph(&graph),
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

fn project_graph_events(
    graph_id: &str,
    events: Vec<crate::DurableRuntimeEvent>,
) -> Result<ExecutionGraph, ExecutionStateStoreError> {
    let checkpoint_index = events
        .iter()
        .rposition(|event| {
            event.kind == "execution_graph.planned"
                || event.kind == "execution_graph.checkpoint"
                || is_legacy_full_graph_snapshot(event)
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
        let event = decode_execution_graph_event(&record).map_err(|reason| {
            ExecutionStateStoreError::Corrupt {
                graph_id: graph_id.to_string(),
                reason,
            }
        })?;
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
    projected.ok_or_else(|| ExecutionStateStoreError::NotFound(graph_id.to_string()))
}

fn is_legacy_full_graph_snapshot(record: &crate::DurableRuntimeEvent) -> bool {
    matches!(
        record.kind.as_str(),
        "execution_graph.node_transitioned"
            | "execution_graph.node_transitioned_and_replanned"
            | "execution_graph.command_applied"
            | "execution_graph.replanned"
            | "execution_graph.recovered"
    ) && record.payload.get("graph").is_some()
}

fn decode_execution_graph_event(
    record: &crate::DurableRuntimeEvent,
) -> Result<ExecutionGraphEvent, String> {
    match serde_json::from_value::<ExecutionGraphEvent>(record.payload.clone()) {
        Ok(event) => Ok(event),
        Err(_error) if is_legacy_full_graph_snapshot(record) => {
            let graph = record
                .payload
                .get("graph")
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "execution graph event {} at sequence {} has no graph snapshot",
                        record.kind, record.sequence
                    )
                })
                .and_then(|value| {
                    serde_json::from_value(value).map_err(|graph_error| {
                        format!(
                            "execution graph event {} at sequence {} has an invalid legacy snapshot: {graph_error}",
                            record.kind, record.sequence
                        )
                    })
                })?;
            Ok(ExecutionGraphEvent::Checkpoint {
                cause: format!("upcast:{}", record.kind),
                graph,
            })
        }
        Err(error) => Err(format!(
            "execution graph event {} at sequence {} is invalid: {error}",
            record.kind, record.sequence
        )),
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{ExecutionNodeKind, ExecutionNodeSpec};

    use super::*;
    use crate::execution_core::graph::ExecutionCommitService;
    use crate::execution_core::hot_state::RuntimeHotStatePlane;
    use crate::RuntimeEventInput;

    #[test]
    fn shared_hot_plane_recovers_once_then_serves_memory() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let write_plane = Arc::new(RuntimeHotStatePlane::default());
        let commit = ExecutionCommitService::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&write_plane),
        );
        let mut graph = ExecutionGraph::new("hot graph");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
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

    #[test]
    fn legacy_full_graph_delta_event_is_upcast_as_checkpoint() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let commit = ExecutionCommitService::new(Arc::clone(&event_store));
        let mut graph = ExecutionGraph::new("legacy graph");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "{}",
        ));
        let registered = commit.register_graph(graph).unwrap().graph;
        let mut legacy_snapshot = registered.clone();
        legacy_snapshot.revision = 2;
        event_store
            .append(RuntimeEventInput {
                stream_id: registered.id.clone(),
                scope: RuntimeEventScope::ExecutionGraph,
                kind: "execution_graph.node_transitioned".to_string(),
                status: Some("running".to_string()),
                actor: Some("legacy-runtime".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "event": "node_transitioned",
                    "node_id": registered.nodes[0].id.clone(),
                    "from": "planned",
                    "to": "running",
                    "result": null,
                    "graph": legacy_snapshot,
                }),
            })
            .unwrap();

        let store = ExecutionGraphStateStore::new(event_store);
        assert_eq!(store.load(&registered.id).unwrap().revision, 2);
    }

    #[test]
    fn team_projection_quarantine_is_idempotent() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let store = ExecutionGraphStateStore::new(Arc::clone(&event_store));

        let first = store
            .quarantine_team_projection("invalid-team", "missing assignment")
            .unwrap();
        let second = store
            .quarantine_team_projection("invalid-team", "another read")
            .unwrap();

        assert_eq!(first, second);
        let events = event_store
            .list_stream("team-projection-governance:invalid-team")
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "team.projection.quarantined");
    }

    #[test]
    fn state_store_projection_keeps_node_payloads_opaque() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let commit = ExecutionCommitService::new(Arc::clone(&event_store));
        let mut graph = ExecutionGraph::new("opaque payload projection");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "private-prompt-and-runtime-binding",
        );
        node.id = "opaque-node".to_string();
        graph.nodes.push(node);
        let registered = commit.register_graph(graph).expect("register graph").graph;
        let projection = ExecutionGraphStateStore::new(event_store)
            .projection(&registered.id)
            .expect("public projection");
        assert_eq!(
            projection.nodes[0].payload_ref,
            "execution-payload:opaque-node"
        );
        assert!(!serde_json::to_string(&projection)
            .expect("serialize public projection")
            .contains("private-prompt-and-runtime-binding"));
    }
}
