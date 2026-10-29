use std::sync::Arc;

use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphProjection};
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
    /// A graph id that no stream carries, together with **where** it was looked
    /// up. The caller is the part that cannot be recovered later: a run id passed
    /// where a graph id belongs produces exactly this error, and without the
    /// location every investigation starts by grepping for call sites instead of
    /// reading one line.
    #[error("execution graph `{graph_id}` does not exist (looked up at {location})")]
    NotFound { graph_id: String, location: String },
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

/// The store keys on the graph id alone, while a claim records the id the Agent
/// was admitted under - an agent-run id (`run:<graph_id>`). Looking that up here
/// can never succeed: one live run failed the same `task_review` one hundred and
/// twenty-two times on exactly that mismatch while the graph existed the whole
/// time.
///
/// Normalizing at the store entry, rather than at each call site, is the point.
/// Two call sites (`review_evidence.rs` and `review_evidence/results.rs`) did not
/// normalize, and that is where the 28 failures in the live run came from. A rule
/// that depends on every caller remembering it is not a rule.
///
/// `runner:` and `runs:` are separate namespaces, not run ids, so they are left
/// alone. The error keeps the id that was asked for: a message that renames the
/// id cannot be matched against what the model or the caller actually saw.
#[must_use]
pub fn graph_id_for_lookup(graph_id: &str) -> &str {
    graph_id.strip_prefix("run:").unwrap_or(graph_id)
}

impl ExecutionGraphStateStore {
    pub(crate) fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.event_store.subscribe_commits()
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
        let key = graph_id_for_lookup(graph_id);
        if let Some(graph) = self.hot_graphs.get(key) {
            if !self.verify_durable_revision
                || self.event_store.stream_revision(key)? == graph.revision
            {
                return Ok(graph);
            }
        }
        let permit = self.hot_graphs.recovery_permit(key);
        if !permit.is_leader() && !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(key) {
                return Ok(graph);
            }
            drop(permit);
            return self.load_snapshot(graph_id);
        }
        if !self.verify_durable_revision {
            if let Some(graph) = self.hot_graphs.get(key) {
                return Ok(graph);
            }
        }
        let head = self.event_store.stream_revision(key)?;
        if head == 0 {
            return Err(ExecutionStateStoreError::NotFound {
                graph_id: graph_id.into(),
                location: "<unspecified>".into(),
            });
        }
        let corrupt = |reason: String| ExecutionStateStoreError::Corrupt {
            graph_id: graph_id.into(),
            reason,
        };
        let validate_snapshot = |graph: &ExecutionGraph, record: &crate::DurableRuntimeEvent| {
            if graph.id != key || graph.revision != record.sequence {
                return Err(corrupt("graph identity/revision mismatch".into()));
            }
            let lineage = graph
                .lineage
                .as_ref()
                .ok_or_else(|| corrupt("graph lineage missing".into()))?;
            lineage
                .validate()
                .map_err(|reason| corrupt(reason.into()))?;
            let binding = record
                .activity_binding()
                .ok_or_else(|| corrupt("graph activity binding missing".into()))?;
            let parent_activity = graph.parent_execution.as_ref().map(|parent| {
                format!(
                    "activity:execution:{}:node:{}",
                    parent.execution_id, parent.node_id
                )
            });
            if binding.root_execution_id != key
                || binding.activity_id != format!("activity:execution:{key}")
                || binding.node_id.is_some()
                || binding.session_id != lineage.session_id
                || binding.turn_id != lineage.turn_id
                || binding.root_task_id != lineage.root_task_id
                || binding.task_id != lineage.task_id
                || binding.generation != lineage.generation
                || binding.revision != graph.revision
                || binding.fence != graph.revision
                || binding.parent_activity_id != parent_activity
                || binding.initiator_activity_id != parent_activity
            {
                return Err(corrupt("graph activity binding/lineage mismatch".into()));
            }
            Ok(())
        };
        let mut through = head;
        let mut projected = None;
        while through > 0 && projected.is_none() {
            let page = self
                .event_store
                .list_stream_before(key, through, 100, 1024 * 1024)
                .map_err(&corrupt)?;
            if page.is_empty() {
                return Err(corrupt("graph checkpoint search has a sequence gap".into()));
            }
            for record in page {
                if record.stream_id != key
                    || record.scope != RuntimeEventScope::ExecutionGraph
                    || record.sequence != through
                {
                    return Err(corrupt("graph checkpoint search is not continuous".into()));
                }
                through = through
                    .checked_sub(1)
                    .ok_or_else(|| corrupt("invalid graph sequence".into()))?;
                if !matches!(
                    record.kind.as_str(),
                    "execution_graph.planned" | "execution_graph.checkpoint"
                ) {
                    continue;
                }
                let mut graph = decode_execution_graph_event(&self.event_store, &record)
                    .and_then(|event| event.project(None))
                    .map_err(&corrupt)?;
                validate_snapshot(&graph, &record)?;
                graph.recovery_cursor.commit_cursor = record.commit_cursor;
                projected = Some(graph);
                break;
            }
        }
        let mut graph = projected.ok_or_else(|| corrupt("no supported graph checkpoint".into()))?;
        while graph.revision < head {
            let page = self
                .event_store
                .list_stream_after(key, graph.revision, head, 100, 1024 * 1024)
                .map_err(&corrupt)?;
            if page.is_empty() {
                return Err(corrupt("graph tail has a sequence gap".into()));
            }
            for record in page {
                if record.stream_id != key
                    || record.sequence > head
                    || record.sequence
                        != graph
                            .revision
                            .checked_add(1)
                            .ok_or_else(|| corrupt("graph sequence overflow".into()))?
                {
                    return Err(corrupt("graph tail is not continuous".into()));
                }
                let event =
                    decode_execution_graph_event(&self.event_store, &record).map_err(&corrupt)?;
                graph = event.project(Some(graph)).map_err(&corrupt)?;
                validate_snapshot(&graph, &record)?;
                graph.recovery_cursor.commit_cursor = record.commit_cursor;
            }
        }
        let graph = Arc::new(graph);
        self.hot_graphs.record_recovery();
        self.hot_graphs.publish_snapshot(Arc::clone(&graph));
        Ok(graph)
    }

    #[track_caller]
    pub fn load(&self, graph_id: &str) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        self.load_at(graph_id, &std::panic::Location::caller().to_string())
    }

    /// Same lookup, with the caller recorded so a miss says where it came from.
    pub(crate) fn load_at(
        &self,
        graph_id: &str,
        location: &str,
    ) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        self.load_snapshot(graph_id)
            .map(|graph| graph.as_ref().clone())
            .or_else(|error| match error {
                ExecutionStateStoreError::NotFound { graph_id, .. } => {
                    Err(ExecutionStateStoreError::NotFound {
                        graph_id,
                        location: location.to_string(),
                    })
                }
                other => Err(other),
            })
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

    /// `#[track_caller]` is a no-op on an async fn, so the location this records is
    /// the store entry, not the call site. The message says so instead of implying a
    /// caller it never saw. Capturing the real caller would mean adding a location
    /// parameter to every async call site - signature pollution for observability.
    pub async fn load_async(
        &self,
        graph_id: impl Into<String>,
    ) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        let store = self.clone();
        let graph_id = graph_id.into();
        let location = format!(
            "{} (async: store entry, not the caller)",
            std::panic::Location::caller()
        );
        tokio::task::spawn_blocking(move || store.load_at(&graph_id, &location))
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }

    /// Read after a durable event without trusting a not-yet-published hot
    /// snapshot. EventStore notification precedes the writer's cache publish;
    /// a consumer must not acknowledge that notification using stale state.
    /// The indexed stream-head check avoids replay when the cache is current.
    pub(crate) async fn load_current_async(
        &self,
        graph_id: impl Into<String>,
    ) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        let mut current = self.clone();
        current.verify_durable_revision = true;
        current.load_async(graph_id).await
    }

    fn load_current(&self, graph_id: &str) -> Result<ExecutionGraph, ExecutionStateStoreError> {
        let mut current = self.clone();
        current.verify_durable_revision = true;
        current.load(graph_id)
    }

    pub(crate) async fn graph_ids_page_async(
        &self,
        after: Option<(u64, String)>,
    ) -> Result<Vec<(String, u64)>, ExecutionStateStoreError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.graph_ids_page(after, 100))
            .await
            .map_err(|error| ExecutionStateStoreError::BlockingTask(error.to_string()))?
    }

    pub(crate) fn lease_starts_page(
        &self,
        graph_id: &str,
        after: u64,
        head: u64,
    ) -> Result<
        (
            u64,
            Vec<(String, super::events::ExecutionNodeBinding, (u64, u64, u32))>,
        ),
        ExecutionStateStoreError,
    > {
        self.binding_starts_page(graph_id, after, head)
    }

    fn binding_history_page(
        &self,
        graph: &ExecutionGraph,
        after: u64,
        head: u64,
    ) -> Result<Vec<(crate::DurableRuntimeEvent, ExecutionGraphEvent)>, ExecutionStateStoreError>
    {
        let graph_id = graph.id.as_str();
        let corrupt = |reason: String| ExecutionStateStoreError::Corrupt {
            graph_id: graph_id.into(),
            reason,
        };
        let lineage = graph
            .lineage
            .as_ref()
            .ok_or_else(|| corrupt("lease start lineage missing".into()))?;
        let parent = graph.parent_execution.as_ref().map(|parent| {
            format!(
                "activity:execution:{}:node:{}",
                parent.execution_id, parent.node_id
            )
        });
        let records = self
            .event_store
            .list_stream_after(graph_id, after, head, 100, 1024 * 1024)
            .map_err(&corrupt)?;
        if records.is_empty() && after < head {
            return Err(corrupt("lease start history gap".into()));
        }
        let mut cursor = after;
        let mut authenticated = Vec::new();
        for record in records {
            if record.stream_id != graph_id
                || record.sequence != cursor + 1
                || record.sequence > head
                || record.scope != RuntimeEventScope::ExecutionGraph
                || record.schema_version != 2
                || record.actor.as_deref() != Some("execution_commit_service")
            {
                return Err(corrupt("lease start source mismatch".into()));
            }
            let activity = record
                .activity_binding()
                .ok_or_else(|| corrupt("lease start activity missing".into()))?;
            if activity.root_execution_id != graph_id
                || activity.node_id.is_some()
                || activity.activity_id != format!("activity:execution:{graph_id}")
                || activity.session_id != lineage.session_id
                || activity.turn_id != lineage.turn_id
                || activity.task_id != lineage.task_id
                || activity.root_task_id != lineage.root_task_id
                || activity.generation != lineage.generation
                || activity.revision != record.sequence
                || activity.fence != record.sequence
                || activity.parent_activity_id != parent
                || activity.initiator_activity_id != parent
            {
                return Err(corrupt("lease start activity identity mismatch".into()));
            }
            cursor = record.sequence;
            let event =
                decode_execution_graph_event(&self.event_store, &record).map_err(&corrupt)?;
            authenticated.push((record, event));
        }
        Ok(authenticated)
    }

    pub(crate) fn binding_starts_page(
        &self,
        graph_id: &str,
        after: u64,
        head: u64,
    ) -> Result<
        (
            u64,
            Vec<(String, super::events::ExecutionNodeBinding, (u64, u64, u32))>,
        ),
        ExecutionStateStoreError,
    > {
        let graph = self.load(graph_id)?;
        let records = self.binding_history_page(&graph, after, head)?;
        let mut cursor = after;
        let mut starts = Vec::new();
        for (record, event) in records {
            cursor = record.sequence;
            let bindings = match event {
                ExecutionGraphEvent::NodeTransitioned {
                    node_id,
                    binding: Some(binding),
                    ..
                } => std::collections::BTreeMap::from([(node_id, binding)]),
                ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings,
                ExecutionGraphEvent::Checkpoint {
                    cause, bindings, ..
                } if matches!(
                    cause.as_str(),
                    "execution_graph.nodes_started" | "execution_graph.node_transitioned"
                ) =>
                {
                    bindings
                }
                _ => std::collections::BTreeMap::new(),
            };
            for (node_id, binding) in bindings {
                starts.push((
                    node_id,
                    binding,
                    (
                        record.sequence,
                        record.commit_cursor,
                        record.transaction_index,
                    ),
                ));
            }
        }
        Ok((cursor, starts))
    }

    /// A read-only fact for the original bound ToolBatch cancellation owner.
    /// Replay every authenticated record: a terminal snapshot cannot erase a start.
    pub(crate) fn confirm_cancelled_bound_child_never_admitted(
        &self,
        expected: &ExecutionGraph,
    ) -> Result<(), ExecutionStateStoreError> {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        let corrupt = |reason: &str| ExecutionStateStoreError::Corrupt {
            graph_id: expected.id.clone(),
            reason: reason.into(),
        };
        super::executors::agent_tool::decode_bound_agent_tool_graph(expected)
            .map_err(|_| corrupt("never-admitted original bound child identity invalid"))?;
        let fence = self.event_store.source_mutation_fence()?;
        let head = self.event_store.stream_revision(&expected.id)?;
        if head < 2 {
            return Err(corrupt("never-admitted cancellation missing"));
        }
        let mut cursor = 0;
        let mut state: Option<ExecutionGraph> = None;
        let mut cancelled = false;
        while cursor < head {
            for (record, event) in self.binding_history_page(expected, cursor, head)? {
                let node = &expected.nodes[0];
                let command = match &event {
                    ExecutionGraphEvent::Planned { .. }
                        if record.sequence == 1
                            && record.transaction_id == format!("{}:planned", expected.id) =>
                    {
                        None
                    }
                    ExecutionGraphEvent::CommandApplied { command, .. }
                        if state.is_some()
                            && !cancelled
                            && matches!(command.as_str(), "start" | "cancel")
                            && record.transaction_id
                                == format!(
                                    "{}:command:{}:{}",
                                    expected.id, command, record.sequence
                                ) =>
                    {
                        Some(command.as_str())
                    }
                    ExecutionGraphEvent::NodeTransitioned {
                        node_id,
                        from: ExecutionNodeStatus::Planned,
                        to: ExecutionNodeStatus::Ready,
                        result: None,
                        binding: None,
                        ..
                    } if state.is_some()
                        && !cancelled
                        && node_id == &node.id
                        && state.as_ref().and_then(|g| g.node_statuses.get(node_id))
                            == Some(&ExecutionNodeStatus::Planned)
                        && record.transaction_id
                            == format!(
                                "{}:{}:{}:{}",
                                expected.id,
                                node.id,
                                record.sequence,
                                ExecutionNodeStatus::Ready as u8
                            ) =>
                    {
                        None
                    }
                    ExecutionGraphEvent::Checkpoint {
                        cause,
                        bindings,
                        lease_closures,
                        ..
                    } if state.is_some()
                        && !cancelled
                        && bindings.is_empty()
                        && lease_closures.is_empty()
                        && cause == "execution_graph.command_applied" =>
                    {
                        if record.transaction_id
                            == format!("{}:command:cancel:{}", expected.id, record.sequence)
                        {
                            Some("cancel")
                        } else if record.transaction_id
                            == format!("{}:command:start:{}", expected.id, record.sequence)
                        {
                            Some("start")
                        } else {
                            return Err(corrupt("never-admitted checkpoint command unknown"));
                        }
                    }
                    ExecutionGraphEvent::Checkpoint {
                        cause,
                        bindings,
                        lease_closures,
                        ..
                    } if state.is_some()
                        && !cancelled
                        && bindings.is_empty()
                        && lease_closures.is_empty()
                        && cause == "execution_graph.node_transitioned"
                        && state.as_ref().and_then(|g| g.node_statuses.get(&node.id))
                            == Some(&ExecutionNodeStatus::Planned)
                        && record.transaction_id
                            == format!(
                                "{}:{}:{}:{}",
                                expected.id,
                                node.id,
                                record.sequence,
                                ExecutionNodeStatus::Ready as u8
                            ) =>
                    {
                        None
                    }
                    _ => {
                        return Err(corrupt(
                            "never-admitted history contains admission or ambiguous mutation",
                        ))
                    }
                };
                let mut projected = event
                    .project(state.clone())
                    .map_err(|_| corrupt("never-admitted history projection failed"))?;
                if projected.revision != record.sequence
                    || projected
                        .recovery_cursor
                        .node_attempts
                        .values()
                        .any(|attempt| *attempt != 0)
                    || projected
                        .recovery_cursor
                        .node_attempts
                        .keys()
                        .any(|key| key != &node.id)
                    || !projected.node_results.is_empty()
                    || record.transaction_index != 0
                    || record.idempotency_key.as_deref()
                        != Some(format!("{}:revision:{}", expected.id, record.sequence).as_str())
                {
                    return Err(corrupt("never-admitted history contains attempt or result"));
                }
                // Only original dependency readiness and the exact Cancel may change status.
                let ready_transition = matches!(
                    event,
                    ExecutionGraphEvent::NodeTransitioned { .. }
                ) || matches!(&event, ExecutionGraphEvent::Checkpoint { cause, .. } if cause == "execution_graph.node_transitioned");
                let wanted = if command == Some("cancel") {
                    ExecutionNodeStatus::Cancelled
                } else if ready_transition {
                    ExecutionNodeStatus::Ready
                } else {
                    state
                        .as_ref()
                        .and_then(|g| g.node_statuses.get(&node.id))
                        .copied()
                        .unwrap_or(ExecutionNodeStatus::Planned)
                };
                if projected.node_statuses.len() != 1
                    || projected.node_statuses.get(&node.id) != Some(&wanted)
                {
                    return Err(corrupt("never-admitted node status mismatch"));
                }
                let mut identity = projected.clone();
                identity.revision = expected.revision;
                identity.node_statuses = expected.node_statuses.clone();
                identity.node_results = expected.node_results.clone();
                identity.recovery_cursor = expected.recovery_cursor.clone();
                if identity != *expected {
                    return Err(corrupt("never-admitted original graph identity changed"));
                }
                projected.recovery_cursor.commit_cursor = record.commit_cursor;
                state = Some(projected);
                cancelled |= command == Some("cancel");
                cursor = record.sequence;
            }
        }
        if !cancelled
            || self.event_store.stream_revision(&expected.id)? != head
            || self.event_store.source_mutation_fence()? != fence
        {
            return Err(corrupt("never-admitted source head or fence changed"));
        }
        // Also reject a stale/foreign hot view rather than relying on it as proof.
        if self.load_current(&expected.id)?
            != state.ok_or_else(|| corrupt("never-admitted history missing"))?
        {
            return Err(corrupt("never-admitted current projection mismatch"));
        }
        if self.event_store.stream_revision(&expected.id)? != head
            || self.event_store.source_mutation_fence()? != fence
        {
            return Err(corrupt("never-admitted source head or fence changed"));
        }
        Ok(())
    }

    pub(crate) fn committed_binding_result(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &super::events::ExecutionNodeBinding,
    ) -> Result<
        Option<(
            harness_contract::execution_graph::ExecutionNodeResult,
            crate::DurableRuntimeEvent,
        )>,
        ExecutionStateStoreError,
    > {
        let (original_ticket, _) = self.binding_start_ticket(graph, node, binding)?;
        let corrupt = |reason: String| ExecutionStateStoreError::Corrupt {
            graph_id: graph.id.clone(),
            reason,
        };
        let mut state: Option<ExecutionGraph> = None;
        let mut after = 0;
        let mut started = false;
        while after < graph.revision {
            let page = self
                .event_store
                .list_stream_after(&graph.id, after, graph.revision, 100, 1024 * 1024)
                .map_err(&corrupt)?;
            if page.is_empty() {
                return Err(corrupt("committed attempt history gap".into()));
            }
            for record in page {
                if record.stream_id != graph.id
                    || record.sequence != after + 1
                    || record.scope != RuntimeEventScope::ExecutionGraph
                    || record.schema_version != 2
                    || record.actor.as_deref() != Some("execution_commit_service")
                {
                    return Err(corrupt("committed attempt source mismatch".into()));
                }
                after = record.sequence;
                let event =
                    decode_execution_graph_event(&self.event_store, &record).map_err(&corrupt)?;
                let next = event.project(state.clone()).map_err(&corrupt)?;
                let original = match &event {
                    ExecutionGraphEvent::NodeTransitioned {
                        node_id,
                        binding: Some(original),
                        ..
                    } if node_id == node => Some(original),
                    ExecutionGraphEvent::NodesStarted { bindings, .. }
                    | ExecutionGraphEvent::Checkpoint { bindings, .. } => bindings.get(node),
                    _ => None,
                };
                if original == Some(binding) {
                    started = true;
                }
                let result_event = match &event {
                    ExecutionGraphEvent::NodeTransitioned {
                        node_id,
                        result: Some(_),
                        ..
                    }
                    | ExecutionGraphEvent::NodeTransitionedAndReplanned { node_id, .. } => {
                        node_id == node
                    }
                    ExecutionGraphEvent::NodesTransitioned { node_ids, .. } => {
                        node_ids.iter().any(|id| id == node)
                    }
                    candidate if matches!(candidate, ExecutionGraphEvent::CommandApplied { command, .. }
                        if matches!(command.as_str(), "cancel" | "cancel_node"))
                        || matches!(candidate, ExecutionGraphEvent::Checkpoint { cause, .. }
                            if cause == "execution_graph.command_applied"
                                && ["cancel", "cancel_node"].iter().any(|command|
                                    record.transaction_id == format!("{}:command:{}:{}", graph.id, command, record.sequence))) => {
                        let stream = super::events::ExecutionNodeBinding::effect_stream_id(&original_ticket);
                        let intent = self.event_store.event_by_idempotency_key(&stream, &format!("{}:intent", original_ticket.idempotency_key)).map_err(|error| corrupt(error.to_string()))?;
                        let receipt = self.event_store.event_by_idempotency_key(&stream, &format!("{}:receipt", original_ticket.idempotency_key)).map_err(|error| corrupt(error.to_string()))?;
                        if let Some(receipt) = receipt.as_ref() {
                            let outcome = super::events::ExecutionNodeBinding::validate_effect_records(
                                &original_ticket, intent.as_ref(), Some(receipt), record.commit_cursor).map_err(&corrupt)?;
                            receipt.transaction_id == record.transaction_id
                                && receipt.commit_cursor == record.commit_cursor
                                && receipt.transaction_index > record.transaction_index
                                && outcome.as_ref().is_some_and(|outcome| next.node_results.get(node) == Some(&outcome.result))
                        } else { false }
                    }
                    ExecutionGraphEvent::Checkpoint { cause, .. } => matches!(
                        cause.as_str(),
                        "execution_graph.node_transitioned"
                            | "execution_graph.node_transitioned_and_replanned"
                            | "execution_graph.nodes_transitioned"
                    ),
                    _ => false,
                };
                if started
                    && result_event
                    && state.as_ref().is_some_and(|previous| {
                        previous.node_statuses.get(node)
                            == Some(
                                &harness_contract::execution_graph::ExecutionNodeStatus::Running,
                            )
                            && previous.recovery_cursor.node_attempts.get(node)
                                == Some(&binding.attempt)
                    })
                    && next.recovery_cursor.node_attempts.get(node) == Some(&binding.attempt)
                {
                    if let Some(result) = next.node_results.get(node) {
                        if next.node_statuses.get(node) == Some(&result.status)
                            && super::runner::validate_outcome(
                                node,
                                &super::registry::NodeExecutionOutcome::new(result.clone()),
                            )
                            .is_ok()
                        {
                            return Ok(Some((result.clone(), record)));
                        }
                    }
                }
                state = Some(next);
            }
        }
        Ok(None)
    }

    pub(crate) fn binding_retirement_record(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &super::events::ExecutionNodeBinding,
    ) -> Result<Option<crate::DurableRuntimeEvent>, ExecutionStateStoreError> {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        self.binding_start_ticket(graph, node, binding)?;
        let corrupt = |reason: String| ExecutionStateStoreError::Corrupt {
            graph_id: graph.id.clone(),
            reason,
        };
        let mut current: Option<ExecutionGraph> = None;
        let mut after = 0;
        let mut retired = None;
        let position = self
            .binding_start_position(graph, node, binding)?
            .ok_or_else(|| corrupt("retirement original start missing".into()))?;
        while after < graph.revision {
            let page = self
                .event_store
                .list_stream_after(&graph.id, after, graph.revision, 100, 1024 * 1024)
                .map_err(&corrupt)?;
            if page.is_empty() {
                return Err(corrupt("binding retirement history gap".into()));
            }
            for record in page {
                if record.sequence != after + 1
                    || record.stream_id != graph.id
                    || record.scope != RuntimeEventScope::ExecutionGraph
                    || record.schema_version != 2
                    || record.actor.as_deref() != Some("execution_commit_service")
                {
                    return Err(corrupt("binding retirement source mismatch".into()));
                }
                after = record.sequence;
                let event =
                    decode_execution_graph_event(&self.event_store, &record).map_err(&corrupt)?;
                let next = event.project(current.clone()).map_err(&corrupt)?;
                let command = matches!(&event,ExecutionGraphEvent::CommandApplied { command,.. } if matches!(command.as_str(),"pause"|"cancel"|"cancel_node"))
                    || matches!(&event,ExecutionGraphEvent::Checkpoint { cause,.. } if cause == "execution_graph.command_applied");
                if record.sequence > position.0
                    && command
                    && current.as_ref().is_some_and(|previous| {
                        matches!(
                            previous.node_statuses.get(node),
                            Some(
                                ExecutionNodeStatus::Running
                                    | ExecutionNodeStatus::WaitingInput
                                    | ExecutionNodeStatus::WaitingApproval
                                    | ExecutionNodeStatus::WaitingExternal
                                    | ExecutionNodeStatus::Paused
                                    | ExecutionNodeStatus::Ready
                            )
                        ) && previous.recovery_cursor.node_attempts.get(node)
                            == Some(&binding.attempt)
                    })
                    && next.recovery_cursor.node_attempts.get(node) == Some(&binding.attempt)
                    && (next.node_statuses.get(node) == Some(&ExecutionNodeStatus::Cancelled)
                        || (current
                            .as_ref()
                            .and_then(|previous| previous.node_statuses.get(node))
                            == Some(&ExecutionNodeStatus::Running)
                            && next.node_statuses.get(node) == Some(&ExecutionNodeStatus::Paused)))
                {
                    retired = Some(record);
                }
                current = Some(next);
            }
        }
        Ok(retired)
    }
    pub(crate) fn binding_ready_command(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &super::events::ExecutionNodeBinding,
    ) -> Result<bool, ExecutionStateStoreError> {
        self.binding_start_ticket(graph, node, binding)?;
        let corrupt = |reason: String| ExecutionStateStoreError::Corrupt {
            graph_id: graph.id.clone(),
            reason,
        };
        let mut state: Option<ExecutionGraph> = None;
        let mut after = 0;
        let mut admitted = false;
        while after < graph.revision {
            let page = self
                .event_store
                .list_stream_after(&graph.id, after, graph.revision, 100, 1024 * 1024)
                .map_err(&corrupt)?;
            if page.is_empty() {
                return Err(corrupt("readmission history gap".into()));
            }
            for record in page {
                if record.sequence != after + 1 || record.stream_id != graph.id {
                    return Err(corrupt("readmission sequence mismatch".into()));
                }
                after = record.sequence;
                let event =
                    decode_execution_graph_event(&self.event_store, &record).map_err(&corrupt)?;
                let next = event.project(state.clone()).map_err(&corrupt)?;
                let command = matches!(&event, ExecutionGraphEvent::CommandApplied { .. })
                    || matches!(&event,ExecutionGraphEvent::Checkpoint { cause,.. } if cause == "execution_graph.command_applied");
                if command && next.recovery_cursor.node_attempts.get(node) == Some(&binding.attempt)
                    && next.node_statuses.get(node) == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Ready)
                    && state.as_ref().is_some_and(|previous| matches!(previous.node_statuses.get(node),
                        Some(harness_contract::execution_graph::ExecutionNodeStatus::Paused
                            | harness_contract::execution_graph::ExecutionNodeStatus::WaitingInput
                            | harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval
                            | harness_contract::execution_graph::ExecutionNodeStatus::WaitingExternal))) { admitted = true; }
                state = Some(next);
            }
        }
        Ok(admitted
            && graph.node_statuses.get(node)
                == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Ready))
    }

    pub(crate) fn current_binding(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        attempt: u32,
    ) -> Result<super::events::ExecutionNodeBinding, ExecutionStateStoreError> {
        let mut after = 0;
        let mut found = None;
        while after < graph.revision {
            let (next, starts) = self.binding_starts_page(&graph.id, after, graph.revision)?;
            for (id, binding, _) in starts {
                if id == node && binding.attempt == attempt && found.replace(binding).is_some() {
                    return Err(ExecutionStateStoreError::Corrupt {
                        graph_id: graph.id.clone(),
                        reason: "duplicate attempt binding".into(),
                    });
                }
            }
            after = next;
        }
        found.ok_or_else(|| ExecutionStateStoreError::Corrupt {
            graph_id: graph.id.clone(),
            reason: format!("original binding missing for {node}/{attempt}"),
        })
    }

    pub(crate) fn binding_start_ticket(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        binding: &super::events::ExecutionNodeBinding,
    ) -> Result<
        (
            super::registry::NodeExecutionTicket,
            harness_contract::execution_graph::ExecutionNodeSpec,
        ),
        ExecutionStateStoreError,
    > {
        let corrupt = |reason: String| ExecutionStateStoreError::Corrupt {
            graph_id: graph.id.clone(),
            reason,
        };
        let lineage = graph
            .lineage
            .as_ref()
            .ok_or_else(|| corrupt("start lineage missing".into()))?;
        let parent = graph.parent_execution.as_ref().map(|parent| {
            format!(
                "activity:execution:{}:node:{}",
                parent.execution_id, parent.node_id
            )
        });
        let mut after = 0;
        let mut state = None;
        let mut found = None;
        while after < graph.revision {
            let page = self
                .event_store
                .list_stream_after(&graph.id, after, graph.revision, 100, 1024 * 1024)
                .map_err(&corrupt)?;
            if page.is_empty() {
                return Err(corrupt("original start history gap".into()));
            }
            for record in page {
                let activity = record
                    .activity_binding()
                    .ok_or_else(|| corrupt("original start activity missing".into()))?;
                if record.stream_id != graph.id
                    || record.sequence != after + 1
                    || record.sequence > graph.revision
                    || record.scope != RuntimeEventScope::ExecutionGraph
                    || record.schema_version != 2
                    || record.actor.as_deref() != Some("execution_commit_service")
                    || activity.root_execution_id != graph.id
                    || activity.node_id.is_some()
                    || activity.activity_id != format!("activity:execution:{}", graph.id)
                    || activity.session_id != lineage.session_id
                    || activity.turn_id != lineage.turn_id
                    || activity.root_task_id != lineage.root_task_id
                    || activity.task_id != lineage.task_id
                    || activity.generation != lineage.generation
                    || activity.revision != record.sequence
                    || activity.fence != record.sequence
                    || activity.parent_activity_id != parent
                    || activity.initiator_activity_id != parent
                {
                    return Err(corrupt("original start source identity mismatch".into()));
                }
                after = record.sequence;
                let event =
                    decode_execution_graph_event(&self.event_store, &record).map_err(&corrupt)?;
                let original = match &event {
                    ExecutionGraphEvent::NodeTransitioned {
                        node_id: candidate,
                        binding: Some(value),
                        ..
                    } if candidate == node_id => Some(value),
                    ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.get(node_id),
                    ExecutionGraphEvent::Checkpoint {
                        cause, bindings, ..
                    } if matches!(
                        cause.as_str(),
                        "execution_graph.nodes_started" | "execution_graph.node_transitioned"
                    ) =>
                    {
                        bindings.get(node_id)
                    }
                    _ => None,
                };
                let next = event.project(state.clone()).map_err(&corrupt)?;
                if next.id != graph.id || next.revision != record.sequence {
                    return Err(corrupt("original start projection mismatch".into()));
                }
                if original == Some(binding) {
                    if found.is_some() {
                        return Err(corrupt("duplicate original binding start".into()));
                    }
                    if let Some(previous) = state.as_ref() {
                        binding
                            .validate_start(previous, node_id)
                            .map_err(&corrupt)?;
                    }
                    let spec = next
                        .nodes
                        .iter()
                        .find(|spec| spec.id == node_id)
                        .ok_or_else(|| corrupt("original start spec missing".into()))?
                        .clone();
                    let ticket = super::registry::NodeExecutionTicket {
                        graph_id: next.id.clone(),
                        node_id: node_id.into(),
                        executor_kind: binding.executor_kind.clone(),
                        service_class: next.service_class,
                        attempt: binding.attempt,
                        idempotency_key: binding.ticket_idempotency_key.clone(),
                        payload_ref: spec.payload_ref.clone(),
                    };
                    super::events::ExecutionNodeBinding::validate_ticket(
                        &ticket,
                        &next,
                        &spec,
                        binding.attempt,
                    )
                    .map_err(&corrupt)?;
                    found = Some((ticket, spec));
                }
                state = Some(next);
            }
        }
        found.ok_or_else(|| corrupt("canonical original binding start missing".into()))
    }

    pub(crate) fn binding_start_position(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        binding: &super::events::ExecutionNodeBinding,
    ) -> Result<Option<(u64, u64, u32)>, ExecutionStateStoreError> {
        let mut after = 0;
        let mut found = None;
        while after < graph.revision {
            let (next, starts) = self.binding_starts_page(&graph.id, after, graph.revision)?;
            for (candidate_node, candidate_binding, position) in starts {
                if candidate_node == node_id && candidate_binding == *binding {
                    if found.replace(position).is_some() {
                        return Err(ExecutionStateStoreError::Corrupt {
                            graph_id: graph.id.clone(),
                            reason: "duplicate exact lease binding start".into(),
                        });
                    }
                }
            }
            after = next;
        }
        Ok(found)
    }

    pub(crate) fn binding_lease_closure(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        binding: &super::events::ExecutionNodeBinding,
        start: (u64, u64, u32),
    ) -> Result<bool, ExecutionStateStoreError> {
        let key = super::events::ExecutionBindingLeaseClosure::key(graph, node_id, binding);
        let Some(record) = self.event_store.event_by_idempotency_key(&graph.id, &key)? else {
            return Ok(false);
        };
        let resolved = self
            .event_store
            .resolve_read_record(record.clone())
            .map_err(|reason| ExecutionStateStoreError::Corrupt {
                graph_id: graph.id.clone(),
                reason,
            })?;
        let closure = super::events::ExecutionBindingLeaseClosure::from_record(
            &resolved, graph, node_id, binding,
        )
        .map_err(|reason| ExecutionStateStoreError::Corrupt {
            graph_id: graph.id.clone(),
            reason,
        })?;
        Ok(closure.is_some_and(|closure| {
            closure.matches(graph, node_id, binding, start)
                && (record.commit_cursor, record.transaction_index) > (start.1, start.2)
        }))
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
        // A Surface reducer may run directly after the durable commit wakeup.
        let graph = self.load_current(graph_id)?;
        Ok(harness_contract::execution_graph::project_execution_graph(
            &graph,
        ))
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

fn decode_execution_graph_event(
    store: &RuntimeEventStore,
    record: &crate::DurableRuntimeEvent,
) -> Result<ExecutionGraphEvent, String> {
    let event: ExecutionGraphEvent = serde_json::from_value(store.resolve_graph_record(record)?)
        .map_err(|_| "invalid current graph event".to_string())?;
    if event.kind() != record.kind {
        return Err("graph kind does not match content".into());
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::execution_core::graph::ExecutionCommitService;
    use crate::execution_core::hot_state::RuntimeHotStatePlane;
    use crate::runtime_event_store::*;
    use crate::RuntimeEventInput;

    #[derive(Debug, Clone)]
    enum GraphReadFault {
        Replace(u64, crate::DurableRuntimeEvent),
        Missing(u64),
        Duplicate(u64),
        Swap(u64),
    }

    struct GraphFaultBackend {
        inner: EphemeralRuntimeEventStore,
        fault: Mutex<Option<GraphReadFault>>,
        source_view: Mutex<(String, u64)>,
        forward_only: Mutex<bool>,
        hits: std::sync::atomic::AtomicUsize,
        append_on_read: Mutex<Option<(Arc<ExecutionCommitService>, ExecutionGraph)>>,
        reads: Mutex<Vec<(bool, u64, u64, usize)>>,
    }

    impl std::fmt::Debug for GraphFaultBackend {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("GraphFaultBackend")
                .finish_non_exhaustive()
        }
    }

    impl GraphFaultBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: EphemeralRuntimeEventStore::new(),
                fault: Mutex::new(None),
                source_view: Mutex::new((uuid::Uuid::new_v4().to_string(), 0)),
                forward_only: Mutex::new(false),
                hits: std::sync::atomic::AtomicUsize::new(0),
                append_on_read: Mutex::new(None),
                reads: Mutex::new(Vec::new()),
            })
        }

        fn set_fault(&self, fault: Option<GraphReadFault>, forward_only: bool) {
            let mut view = self.source_view.lock().unwrap();
            view.1 = view.1.checked_add(1).expect("source view epoch overflow");
            *self.fault.lock().unwrap() = fault;
            *self.forward_only.lock().unwrap() = forward_only;
            self.hits.store(0, std::sync::atomic::Ordering::SeqCst);
        }

        fn page(
            &self,
            mut records: Vec<crate::DurableRuntimeEvent>,
            descending: bool,
            after: u64,
            through: u64,
        ) -> Vec<crate::DurableRuntimeEvent> {
            self.reads
                .lock()
                .unwrap()
                .push((descending, after, through, records.len()));
            if descending && *self.forward_only.lock().unwrap() {
                return records;
            }
            let fault = self.fault.lock().unwrap().clone();
            let target = match &fault {
                Some(
                    GraphReadFault::Replace(sequence, _)
                    | GraphReadFault::Missing(sequence)
                    | GraphReadFault::Duplicate(sequence)
                    | GraphReadFault::Swap(sequence),
                ) => *sequence,
                None => return records,
            };
            if let Some(position) = records.iter().position(|record| {
                record.scope == RuntimeEventScope::ExecutionGraph && record.sequence == target
            }) {
                self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match fault.unwrap() {
                    GraphReadFault::Replace(_, record) => records[position] = record,
                    GraphReadFault::Missing(_) => {
                        records.remove(position);
                    }
                    GraphReadFault::Duplicate(_) => {
                        records.insert(position, records[position].clone());
                    }
                    GraphReadFault::Swap(_) => {
                        let other = if position + 1 < records.len() {
                            position + 1
                        } else {
                            position.checked_sub(1).unwrap()
                        };
                        records.swap(position, other);
                    }
                }
            }
            records
        }
    }

    macro_rules! forward_graph_test_ports {
        ($(fn $name:ident(&self $(, $argument:ident: $kind:ty)*) -> $result:ty;)*) => {
            $(fn $name(&self $(, $argument: $kind)*) -> $result {
                self.inner.$name($($argument),*)
            })*
        };
    }

    #[derive(Default)]
    struct GraphArtifactReads {
        inner: crate::EphemeralArtifactRepository,
        records: Mutex<BTreeMap<String, usize>>,
    }

    impl crate::ArtifactMetadataRepository for GraphArtifactReads {
        fn record(&self, artifact_id: &str) -> Result<Option<crate::ArtifactRecord>, String> {
            *self
                .records
                .lock()
                .unwrap()
                .entry(artifact_id.into())
                .or_default() += 1;
            self.inner.record(artifact_id)
        }

        forward_graph_test_ports! {
            fn reserve_attempt(&self, object: &crate::ArtifactObjectRecord, quota: u64, token: &str, publication_id: &str) -> Result<crate::ArtifactObjectRecord, String>;
            fn reservation(&self, token: &str) -> Result<Option<crate::ArtifactReservation>, String>;
            fn publish_attempt(&self, record: &crate::ArtifactRecord, generation: &str, token: &str, pin: Option<(&str, u64)>) -> Result<(), String>;
            fn release_attempt(&self, token: &str, sha256: &str, generation: &str) -> Result<bool, String>;
            fn active_attempts_page(&self, after: Option<&str>, limit: usize) -> Result<Vec<crate::ArtifactReservation>, String>;
            fn prepared_high_water(&self) -> Result<Option<String>, String>;
            fn prepared_page(&self, after: Option<&str>, through: &str, limit: usize) -> Result<crate::PreparedArtifactPage, String>;
            fn retire_prepared(&self, candidate: &crate::PreparedArtifactCandidate) -> Result<crate::PreparedArtifactRetirement, String>;
            fn adopt_live(&self, expected: &crate::ArtifactRecord) -> Result<(), String>;
            fn prepared_readable(&self, expected: &crate::ArtifactRecord, origin: &crate::PreparedArtifactOrigin) -> Result<bool, String>;
            fn claim_object_deletion(&self, sha256: &str, generation: &str) -> Result<bool, String>;
            fn finish_object_deletion(&self, sha256: &str, generation: &str) -> Result<(), String>;
            fn object(&self, sha256: &str) -> Result<Option<crate::ArtifactObjectRecord>, String>;
            fn checkpoint_object_metadata(&self, sha256: &str) -> Result<Option<(crate::ArtifactObjectRecord, String)>, String>;
            fn catalog_page(&self, query: crate::ArtifactCatalogQuery) -> Result<crate::ArtifactCatalogPage, String>;
            fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(), String>;
            fn remove_record(&self, artifact_id: &str) -> Result<(), String>;
            fn unreferenced_objects_before(&self, before_ms: u64, limit: usize) -> Result<Vec<crate::ArtifactObjectRecord>, String>;
            fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String>;
            fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String>;
            fn is_pinned(&self, artifact_id: &str, at_ms: u64) -> Result<bool, String>;
            fn stats(&self, at_ms: u64) -> Result<crate::ArtifactStoreStats, String>;
        }
    }

    impl RuntimeEventStoreBackend for GraphFaultBackend {
        fn bounded_event_by_idempotency_key(
            &self, stream: &str, key: &str, through: u64, max_bytes: usize,
        ) -> RuntimeEventStoreResult<crate::RuntimeBoundedEventLookup> {
            let mut result = self.inner.bounded_event_by_idempotency_key(stream, key, through, max_bytes)?;
            if let crate::RuntimeBoundedEventLookup::Record { record, encoded_bytes } = &mut result {
                if record.scope == RuntimeEventScope::ExecutionGraph {
                    let mut viewed = self.page(vec![record.as_ref().clone()], false, 0, through);
                    if viewed.len() != 1 {
                        return Err(RuntimeEventStoreError::Corrupt("bounded original witness missing or duplicated".into()));
                    }
                    let current = viewed.pop().unwrap();
                    if serde_json::to_vec(&current)?.len() != *encoded_bytes {
                        return Err(RuntimeEventStoreError::Corrupt("bounded original view extent changed".into()));
                    }
                    *record = Box::new(current);
                }
            }
            Ok(result)
        }

        fn source_mutation_fence(&self) -> RuntimeEventStoreResult<crate::RuntimeSourceMutationFence> {
            let view = self.source_view.lock().unwrap();
            let inner = self.inner.source_mutation_fence()?;
            Ok(crate::RuntimeSourceMutationFence {
                incarnation: serde_json::to_string(&(&view.0, view.1, inner.incarnation))?,
                mutation_epoch: inner.mutation_epoch,
            })
        }

        fn compare_and_put_projection_checkpoint_fenced(
            &self, id: &str, cursor: u64, expected: u64,
            payload: &serde_json::Value, at: u64, source: &crate::RuntimeSourceMutationFence,
        ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
            let view = self.source_view.lock().unwrap();
            let inner = self.inner.source_mutation_fence()?;
            let current = crate::RuntimeSourceMutationFence {
                incarnation: serde_json::to_string(&(&view.0, view.1, &inner.incarnation))?,
                mutation_epoch: inner.mutation_epoch,
            };
            if current != *source {
                return Err(RuntimeEventStoreError::Corrupt("source view fence changed".into()));
            }
            self.inner.compare_and_put_projection_checkpoint_fenced(
                id, cursor, expected, payload, at, &inner,
            )
        }

        fn position_page(
            &self,
            scope: RuntimeEventScope,
            identity: &str,
            kind: Option<&str>,
            after: Option<(u64, u32)>,
            through_cursor: u64,
            max_events: usize,
            max_bytes: usize,
        ) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
            let records = self.inner.position_page(
                scope,
                identity,
                kind,
                after,
                through_cursor,
                max_events,
                max_bytes,
            )?;
            Ok(self.page(
                records,
                false,
                after.map_or(0, |position| position.0),
                through_cursor,
            ))
        }

        fn list_stream_before(
            &self,
            stream_id: &str,
            through_sequence: u64,
            max_events: usize,
            max_bytes: usize,
        ) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
            let pending = self.append_on_read.lock().unwrap().take();
            if let Some((writer, graph)) = pending {
                writer
                    .apply_command(
                        &graph,
                        &ExecutionGraphCommand::Start {
                            expected_revision: graph.revision,
                        },
                    )
                    .unwrap();
            }
            let records = self.inner.list_stream_before(
                stream_id,
                through_sequence,
                max_events,
                max_bytes,
            )?;
            Ok(self.page(records, true, 0, through_sequence))
        }

        fn list_stream_after(
            &self,
            stream_id: &str,
            after_sequence: u64,
            through_sequence: u64,
            max_events: usize,
            max_bytes: usize,
        ) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
            let records = self.inner.list_stream_after(
                stream_id,
                after_sequence,
                through_sequence,
                max_events,
                max_bytes,
            )?;
            Ok(self.page(records, false, after_sequence, through_sequence))
        }

        fn event_by_idempotency_key(&self, stream_id: &str, idempotency_key: &str) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
            let Some(record) = self.inner.event_by_idempotency_key(stream_id, idempotency_key)? else {
                return Ok(None);
            };
            if record.scope != RuntimeEventScope::ExecutionGraph {
                return Ok(Some(record));
            }
            let mut records = self.page(vec![record], false, 0, u64::MAX);
            if records.len() > 1 {
                return Err(RuntimeEventStoreError::Corrupt("duplicate exact original record".into()));
            }
            Ok(records.pop())
        }

        forward_graph_test_ports! {
            fn append(&self, input: RuntimeEventInput) -> Result<crate::DurableRuntimeEvent, String>;
            fn recent_event_lines(&self, stream_id: &str, references: &[RuntimeEventRef], through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String>;
            fn prepared_content_root(&self, origin: &crate::PreparedArtifactOrigin, artifact: &harness_contract::context::ArtifactRef) -> RuntimeEventStoreResult<crate::PreparedContentRoot>;
            fn transaction_stream_revision_exact(&self, transaction_id: &str, stream_id: &str) -> RuntimeEventStoreResult<Option<crate::CommittedStreamRevision>>;
            fn append_transaction(&self, request: AppendTransactionRequest) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn append_transaction_with_terminal(&self, request: AppendTransactionRequest, terminal: SessionTerminalInput) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn consume_verified_decision_lease(&self, lease_id: &str, principal_id: &str, review_id: &str, action: &str, scope: &str, evidence_digest: &str, credential_epoch: u64, consumed_at_ms: u64) -> RuntimeEventStoreResult<()>;
            fn append_transaction_with_verified_decision_lease(&self, request: AppendTransactionRequest, lease: &crate::VerifiedDecisionLease) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn append_batch_if_revision(&self, stream_id: String, expected_revision: u64, transaction_id: String, events: Vec<RuntimeTransactionEventInput>) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn events_after_cursor(&self, cursor: u64, max_commits: usize) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>>;
            fn projection_scan_page(&self, cursor: u64, interest: &RuntimeProjectionInterest, max_commits: usize, max_events: usize, max_bytes: usize) -> RuntimeEventStoreResult<RuntimeProjectionScanPage>;
            fn projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>>;
            fn projection_checkpoints_with_prefix(&self, prefix: &str) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>>;
            fn put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn compare_and_put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn compare_and_repair_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
            fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64>;
            fn list_stream(&self, stream_id: &str) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn count_stream_kinds(&self, stream_id: &str, kinds: &[&str], through_sequence: u64) -> Result<u64, String>;
            fn list_stream_page_desc(&self, stream_id: &str, limit: usize, offset: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn stream_event_count(&self, stream_id: &str) -> Result<usize, String>;
            fn execution_events_for_session(&self, session_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn events_for_root_execution(&self, root_execution_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn events_for_root_execution_kind(&self, root_execution_id: &str, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn events_for_activity(&self, activity_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope(&self, scope: RuntimeEventScope, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope_page_asc(&self, scope: RuntimeEventScope, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope_stream_prefix_page_asc(&self, scope: RuntimeEventScope, stream_prefix: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope_kind_page_asc(&self, scope: RuntimeEventScope, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn stream_ids_for_scope(&self, scope: RuntimeEventScope) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence_page(&self, scope: RuntimeEventScope, kind: &str, sequence: u64, after: Option<(u64, String)>, limit: usize) -> RuntimeEventStoreResult<Vec<(String, u64)>>;
            fn latest_stream_statuses_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>>;
            fn all_events(&self, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64>;
            fn latest_for_stream(&self, stream_id: &str) -> Result<Option<crate::DurableRuntimeEvent>, String>;
            fn latest_for_stream_kind(&self, stream_id: &str, kind: &str) -> Result<Option<crate::DurableRuntimeEvent>, String>;
            fn enqueue_session_terminal(&self, terminal_id: &str, message_id: &str, session_id: &str, commit_cursor: u64, payload_ref: &str) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn claim_session_terminals(&self, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal(&self, terminal_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool>;
            fn session_terminal_for_execution(&self, session_id: &str, execution_id: &str, turn_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn materialized_session_terminals_after(&self, session_id: &str, after_commit_cursor: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth>;
            fn blocked_session_terminals(&self, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn retry_session_terminal(&self, terminal_id: &str, actor: &str, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn adopt_session_terminal_fence(&self, request: &RuntimeSessionTerminalFenceAdoption) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn ack_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn suppress_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn fail_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, class: RuntimeSessionOutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        }
    }

    struct CanonicalGraphFixture {
        root: PathBuf,
        events: Arc<RuntimeEventStore>,
        backend: Arc<GraphFaultBackend>,
        artifacts: Arc<crate::ArtifactStore>,
        artifact_reads: Arc<GraphArtifactReads>,
        writer: Arc<ExecutionCommitService>,
        initial: ExecutionGraph,
        expected: ExecutionGraph,
        records: Vec<crate::DurableRuntimeEvent>,
        checkpoints: Vec<crate::DurableRuntimeEvent>,
        bindings: Vec<(String, super::super::events::ExecutionNodeBinding)>,
    }

    impl CanonicalGraphFixture {
        fn new() -> Self {
            let root = PathBuf::from(
                std::env::var_os("COWD_R1_06_FIXTURE_ROOT")
                    .expect("explicit private plan fixture root"),
            )
            .join(format!("graph-canonical-07-{}", uuid::Uuid::new_v4()));
            assert!(
                root.is_absolute()
                    && root.starts_with(
                        "/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel"
                    )
            );
            Self::new_at(root)
        }

        fn new_at(root: PathBuf) -> Self {
            std::fs::create_dir_all(&root).unwrap();
            let backend = GraphFaultBackend::new();
            let events = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
            let artifact_reads = Arc::new(GraphArtifactReads::default());
            let artifacts = Arc::new(
                crate::ArtifactStore::new(
                    root.join("artifacts"),
                    artifact_reads.clone(),
                    crate::ArtifactStoreConfig::default(),
                )
                .unwrap(),
            );
            events.bind_artifact_store(artifacts.clone()).unwrap();
            let writer = Arc::new(ExecutionCommitService::new(events.clone()));
            let mut graph = ExecutionGraph::new("canonical blob objective ".repeat(16 * 1024));
            crate::test_support::attach_execution_graph_lineage(&mut graph);
            for index in 0..4 {
                graph.nodes.push(ExecutionNodeSpec::new(
                    ExecutionNodeKind::InlineModel,
                    "inline_model",
                    format!("{{\"node\":{index}}}"),
                ));
            }
            let initial = writer.register_graph(graph).unwrap().graph;
            let mut graph = initial.clone();
            let mut bindings = Vec::new();
            let mut checkpoints = Vec::new();
            for attempt in 1..=2 {
                let node = graph.nodes[(attempt - 1) as usize].clone();
                graph = writer
                    .transition_node(
                        &graph,
                        &node.id,
                        harness_contract::execution_graph::ExecutionNodeStatus::Ready,
                        None,
                        Vec::new(),
                    )
                    .unwrap()
                    .graph;
                let binding = super::super::events::ExecutionNodeBinding {
                    executor_kind: node.executor_kind.clone(),
                    ticket_idempotency_key: node.idempotency_key.clone(),
                    attempt: 1,
                    resource_lease_refs: vec![format!("resource:canonical-{attempt}")],
                    scope_lease_ref: Some(format!("scope:canonical-{attempt}")),
                    worktree_lease_ref: Some(format!("worktree:canonical-{attempt}")),
                };
                let ticket = super::super::registry::NodeExecutionTicket {
                    graph_id: graph.id.clone(),
                    node_id: node.id.clone(),
                    executor_kind: node.executor_kind.clone(),
                    service_class: graph.service_class,
                    attempt: 1,
                    idempotency_key: node.idempotency_key.clone(),
                    payload_ref: node.payload_ref.clone(),
                };
                graph = writer
                    .bind_and_start_nodes(
                        &graph,
                        BTreeMap::from([(node.id.clone(), binding.clone())]),
                        vec![ticket],
                    )
                    .unwrap()
                    .graph;
                bindings.push((node.id, binding));
                for _ in 0..128 {
                    let record = events.latest_for_stream(&graph.id).unwrap().unwrap();
                    if record.kind == "execution_graph.checkpoint" {
                        match decode_execution_graph_event(&events, &record).unwrap() {
                            ExecutionGraphEvent::Checkpoint {
                                cause,
                                graph: mut snapshot,
                                ..
                            } => {
                                snapshot.recovery_cursor.commit_cursor = record.commit_cursor;
                                assert_eq!(snapshot, graph);
                                assert!(!cause.is_empty());
                            }
                            _ => panic!("actual checkpoint kind must decode as checkpoint"),
                        }
                        checkpoints.push(record);
                        break;
                    }
                    graph = writer
                        .apply_command(
                            &graph,
                            &ExecutionGraphCommand::Start {
                                expected_revision: graph.revision,
                            },
                        )
                        .unwrap()
                        .graph;
                }
                assert_eq!(
                    checkpoints.len(),
                    attempt as usize,
                    "actual adaptive checkpoint must occur"
                );
            }
            let latest = checkpoints.last().unwrap().sequence;
            for _ in 0..5 {
                graph = writer
                    .apply_command(
                        &graph,
                        &ExecutionGraphCommand::Start {
                            expected_revision: graph.revision,
                        },
                    )
                    .unwrap()
                    .graph;
                assert_ne!(
                    events.latest_for_stream(&graph.id).unwrap().unwrap().kind,
                    "execution_graph.checkpoint"
                );
            }
            let records = events.list_stream(&graph.id).unwrap();
            assert!(latest < graph.revision);
            assert_eq!(
                records.last().unwrap().commit_cursor,
                graph.recovery_cursor.commit_cursor
            );
            assert!(records
                .iter()
                .filter(|record| record.sequence > latest)
                .all(|record| record.kind != "execution_graph.checkpoint"));
            Self {
                root,
                events,
                backend,
                artifacts,
                artifact_reads,
                writer,
                initial,
                expected: graph,
                records,
                checkpoints,
                bindings,
            }
        }

        fn cold(&self) -> (ExecutionGraphStateStore, Arc<RuntimeHotStatePlane>) {
            let plane = Arc::new(RuntimeHotStatePlane::default());
            let mut store =
                ExecutionGraphStateStore::with_hot_state(self.events.clone(), plane.clone());
            store.verify_durable_revision = true;
            (store, plane)
        }

        async fn projection(
            &self,
            services: &crate::RuntimeServices,
        ) -> Result<harness_contract::projection::ExecutionProjection, crate::RuntimeServicesError> {
            for request_index in 0..16 {
                let fresh = (request_index > 0).then(|| self.services());
                let current = fresh.as_deref().unwrap_or(services);
                let result = crate::execution_projection::snapshot(current, &self.expected.id, &self.context(current)).await;
                if fresh.is_some() {
                    current.shutdown_execution().await;
                    current.shutdown_maintenance().await;
                }
                let projection = result?;
                if !projection.health.iter().any(|entity| entity.kind == "history_verification" && entity.status.as_deref() == Some("incomplete")) {
                    return Ok(projection);
                }
                assert!(!projection.terminal_axes.execution.coverage_complete);
                assert_eq!(projection.terminal_axes.execution.phase, harness_contract::projection::TerminalExecutionPhase::Unknown);
                assert_eq!(projection.terminal_axes.execution.settlement, harness_contract::projection::TerminalEffectSettlement::Unknown);
                assert!(current.execution_projection_cache_stats().2 == 0);
            }
            panic!("original canonical fixture did not complete within sixteen bounded cold requests");
        }

        fn assert_rejected_without_publish(&self) {
            let before = self.events.list_stream(&self.expected.id).unwrap();
            let cursor = self.events.durable_commit_cursor().unwrap();
            for old_hot in [false, true] {
                let (store, plane) = self.cold();
                if old_hot {
                    assert!(plane.graphs().publish(self.initial.clone()));
                }
                let metrics = plane.metrics().snapshot();
                assert!(matches!(
                    store.load_snapshot(&self.expected.id),
                    Err(ExecutionStateStoreError::Corrupt { .. })
                ));
                assert!(store.load_current(&self.expected.id).is_err());
                assert_eq!(
                    plane.metrics().snapshot().graph_publishes,
                    metrics.graph_publishes
                );
                assert_eq!(
                    plane.metrics().snapshot().graph_recoveries,
                    metrics.graph_recoveries
                );
                assert_eq!(
                    plane
                        .graphs()
                        .get(&self.expected.id)
                        .map(|graph| graph.as_ref().clone()),
                    old_hot.then(|| self.initial.clone())
                );
            }
            assert_eq!(self.events.list_stream(&self.expected.id).unwrap(), before);
            assert_eq!(self.events.durable_commit_cursor().unwrap(), cursor);
        }

        fn blob_path(&self, record: &crate::DurableRuntimeEvent) -> PathBuf {
            let artifact: harness_contract::context::ArtifactRef =
                serde_json::from_value(record.payload["body"]["artifact"].clone()).unwrap();
            assert!(artifact.bytes > 256 * 1024);
            let digest = artifact.sha256.strip_prefix("sha256:").unwrap();
            let directory = self
                .root
                .join("artifacts/objects")
                .join(&digest[..2])
                .join(digest);
            let files = std::fs::read_dir(directory)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            assert_eq!(files.len(), 1);
            files[0].clone()
        }

        fn changed_body(
            &self,
            record: &crate::DurableRuntimeEvent,
            change: impl FnOnce(&mut serde_json::Value),
        ) -> crate::DurableRuntimeEvent {
            let mut value = self.events.resolve_graph_record(record).unwrap();
            change(&mut value);
            let binding = record.activity_binding().unwrap();
            let mut altered = record.clone();
            altered.payload = self
                .events
                .prepare_content(
                    &format!("session:{}", binding.session_id),
                    &format!("negative-fixture:{}", uuid::Uuid::new_v4()),
                    &value,
                    &crate::PreparedArtifactOrigin::EventBody {
                        stream_id: record.stream_id.clone(),
                        idempotency_key: record.idempotency_key.clone().unwrap(),
                        kind: record.kind.clone(),
                    },
                )
                .unwrap();
            altered.payload["_runtime_activity_binding"] = serde_json::to_value(binding).unwrap();
            altered
        }

        fn services(&self) -> Arc<crate::RuntimeServices> {
            crate::RuntimeServices::builder(
                self.root.join(format!("home-{}", uuid::Uuid::new_v4())),
                &self.root,
            )
            .runtime_event_store(self.events.clone())
            .artifact_store(self.artifacts.clone())
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
            .build()
            .unwrap()
        }

        fn context(
            &self,
            services: &crate::RuntimeServices,
        ) -> harness_contract::projection::ProjectionQueryContext {
            harness_contract::projection::ProjectionQueryContext {
                principal: "test".into(),
                workspace_id: services.workspace_key().into(),
                session_scopes: Vec::new(),
                mission_scopes: Vec::new(),
                visibility_grants: vec![
                    format!("workspace:{}", services.workspace_key()),
                    "resource:*".into(),
                ],
                detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
                authorization_revision: 1,
            }
        }

        fn add_started_checkpoint_and_successor(&mut self) -> crate::DurableRuntimeEvent {
            let interval = self.checkpoints[1].sequence - self.checkpoints[0].sequence;
            let target = self.checkpoints[1].sequence + interval;
            let node = self.expected.nodes[2].clone();
            self.expected = self
                .writer
                .transition_node(
                    &self.expected,
                    &node.id,
                    harness_contract::execution_graph::ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            while self.expected.revision + 1 < target {
                self.expected = self
                    .writer
                    .apply_command(
                        &self.expected,
                        &ExecutionGraphCommand::Start {
                            expected_revision: self.expected.revision,
                        },
                    )
                    .unwrap()
                    .graph;
            }
            assert_eq!(self.expected.revision + 1, target);
            let binding = super::super::events::ExecutionNodeBinding {
                executor_kind: node.executor_kind.clone(),
                ticket_idempotency_key: node.idempotency_key.clone(),
                attempt: 1,
                resource_lease_refs: vec!["resource:checkpoint-start".into()],
                scope_lease_ref: Some("scope:checkpoint-start".into()),
                worktree_lease_ref: Some("worktree:checkpoint-start".into()),
            };
            let ticket = super::super::registry::NodeExecutionTicket {
                graph_id: self.expected.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: self.expected.service_class,
                attempt: 1,
                idempotency_key: node.idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            self.expected = self
                .writer
                .bind_and_start_nodes(
                    &self.expected,
                    BTreeMap::from([(node.id.clone(), binding.clone())]),
                    vec![ticket],
                )
                .unwrap()
                .graph;
            let started = self
                .events
                .latest_for_stream(&self.expected.id)
                .unwrap()
                .unwrap();
            match decode_execution_graph_event(&self.events, &started).unwrap() {
                ExecutionGraphEvent::Checkpoint {
                    cause, bindings, ..
                } => {
                    assert_eq!(cause, "execution_graph.nodes_started");
                    assert_eq!(
                        bindings,
                        BTreeMap::from([(node.id.clone(), binding.clone())])
                    );
                }
                _ => panic!("real bind/start must have actually triggered a checkpoint"),
            }
            self.bindings.push((node.id, binding));
            for _ in 0..128 {
                self.expected = self
                    .writer
                    .apply_command(
                        &self.expected,
                        &ExecutionGraphCommand::Start {
                            expected_revision: self.expected.revision,
                        },
                    )
                    .unwrap()
                    .graph;
                let record = self
                    .events
                    .latest_for_stream(&self.expected.id)
                    .unwrap()
                    .unwrap();
                if record.kind == "execution_graph.checkpoint" {
                    self.checkpoints.push(started.clone());
                    self.checkpoints.push(record);
                    break;
                }
            }
            assert_eq!(
                self.checkpoints.len(),
                4,
                "real successor checkpoint required"
            );
            for _ in 0..5 {
                self.expected = self
                    .writer
                    .apply_command(
                        &self.expected,
                        &ExecutionGraphCommand::Start {
                            expected_revision: self.expected.revision,
                        },
                    )
                    .unwrap()
                    .graph;
            }
            assert!(self.checkpoints.last().unwrap().sequence < self.expected.revision);
            self.records = self.events.list_stream(&self.expected.id).unwrap();
            started
        }
    }

    // A synthetic shape witness, not a reconstruction of the lost 1090 event.
    // Complete construction synchronously before entering the cold reader.
    fn observed_result_fixture(root: PathBuf) -> CanonicalGraphFixture {
        use harness_contract::context::{
            EvidenceTargetIdentity, ObservedAcceptance, ObservedEvidence,
            ObservedEvidenceProvenance,
        };
        use harness_contract::execution_graph::{
            ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
        };

        let mut fixture = CanonicalGraphFixture::new_at(root);
        let node_id = fixture.bindings[0].0.clone();
        assert_eq!(
            fixture.expected.node_statuses[&node_id],
            ExecutionNodeStatus::Running
        );
        let result = ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: None,
            wait_class: None,
            summary: Some("synthetic reader shape only".into()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: ExecutionUsage {
                input_tokens: 17,
                output_tokens: 5,
                duration_ms: 23,
                observed_acceptance: ObservedAcceptance {
                    satisfied_criteria: vec!["synthetic-criterion".into()],
                    observed_evidence: vec![ObservedEvidence {
                        obligation_id: "synthetic-obligation".into(),
                        target: EvidenceTargetIdentity::Network {
                            endpoint: "https://reader-witness.invalid/not-executed".into(),
                        },
                        observed_at_sequence: 7,
                        tool_name: "synthetic-reader-no-tool-dispatch".into(),
                        provenance: ObservedEvidenceProvenance::RetainedReplay,
                        evidence_ref: None,
                        model_observation: None,
                        workspace_prior_state: None,
                    }],
                    unresolved_obligation_ids: Vec::new(),
                },
                ..ExecutionUsage::default()
            },
            finished_at_ms: 123_456,
        };
        fixture.expected = fixture
            .writer
            .transition_node(
                &fixture.expected,
                &node_id,
                ExecutionNodeStatus::Completed,
                Some(result),
                Vec::new(),
            )
            .unwrap()
            .graph;
        fixture.records = fixture.events.list_stream(&fixture.expected.id).unwrap();
        fixture
    }

    #[test]
    fn r1_1090_cold_graph_reader_preserves_observed_result_shape() {
        use harness_contract::context::{EvidenceTargetIdentity, ObservedEvidenceProvenance};
        use harness_contract::execution_graph::ExecutionNodeStatus;

        // The owner outlives every store/artifact handle; never use the old plan tree.
        let root = tempfile::tempdir_in("/media/yi/Datas/workspace/.cowd-build/tmp-main").unwrap();
        let fixture = observed_result_fixture(root.path().to_path_buf());
        let node_id = &fixture.bindings[0].0;
        let tail = fixture.records.last().unwrap();
        assert_eq!(tail.kind, "execution_graph.node_transitioned");
        assert_eq!(tail.sequence, fixture.expected.revision);
        assert_eq!(tail.stream_id, fixture.expected.id);
        let body = fixture.events.resolve_graph_record(tail).unwrap();
        assert_eq!(body["event"], "node_transitioned");
        assert!(
            body["delta"].is_object(),
            "must hit a real Delta tail, not a checkpoint"
        );
        assert_eq!(body["delta"]["revision"], tail.sequence);
        let nested = &body["delta"]["node_result_updates"][node_id]["usage"]["observed_acceptance"]
            ["observed_evidence"];
        assert_eq!(nested.as_array().unwrap().len(), 1);
        assert_eq!(nested[0]["obligation_id"], "synthetic-obligation");
        assert_eq!(
            nested[0]["target"]["endpoint"],
            "https://reader-witness.invalid/not-executed"
        );

        let before = fixture.events.list_stream(&fixture.expected.id).unwrap();
        let cursor = fixture.events.durable_commit_cursor().unwrap();
        let assert_positive = || {
            let (store, plane) = fixture.cold();
            assert!(plane.graphs().get(&fixture.expected.id).is_none());
            let graph = store.load(&fixture.expected.id).unwrap();
            assert_eq!(graph.id, tail.stream_id);
            assert_eq!(graph.revision, tail.sequence);
            assert_eq!(graph.recovery_cursor.commit_cursor, tail.commit_cursor);
            assert_eq!(graph.node_statuses[node_id], ExecutionNodeStatus::Completed);
            let result = &graph.node_results[node_id];
            assert_eq!(result.status, ExecutionNodeStatus::Completed);
            assert_eq!(result.finished_at_ms, 123_456);
            assert_eq!(
                result.summary.as_deref(),
                Some("synthetic reader shape only")
            );
            assert!(
                result.result_ref.is_none()
                    && result.wait_class.is_none()
                    && result.failure.is_none()
            );
            assert!(result.evidence_refs.is_empty());
            assert_eq!(
                (
                    result.usage.input_tokens,
                    result.usage.output_tokens,
                    result.usage.duration_ms
                ),
                (17, 5, 23)
            );
            assert!(result.usage.provider_usage_observed.is_none());
            let observed = &result.usage.observed_acceptance;
            assert_eq!(observed.satisfied_criteria, ["synthetic-criterion"]);
            assert!(observed.unresolved_obligation_ids.is_empty());
            assert_eq!(observed.observed_evidence.len(), 1);
            let evidence = &observed.observed_evidence[0];
            assert_eq!(evidence.obligation_id, "synthetic-obligation");
            assert_eq!(
                evidence.target,
                EvidenceTargetIdentity::Network {
                    endpoint: "https://reader-witness.invalid/not-executed".into(),
                }
            );
            assert_eq!(evidence.observed_at_sequence, 7);
            assert_eq!(evidence.tool_name, "synthetic-reader-no-tool-dispatch");
            assert_eq!(
                evidence.provenance,
                ObservedEvidenceProvenance::RetainedReplay
            );
            assert!(
                evidence.evidence_ref.is_none()
                    && evidence.model_observation.is_none()
                    && evidence.workspace_prior_state.is_none()
            );
            assert_eq!(graph, fixture.expected); // Secondary whole-graph roundtrip oracle.
            assert_eq!(plane.metrics().snapshot().graph_recoveries, 1);
            assert_eq!(plane.metrics().snapshot().graph_publishes, 1);
        };
        assert_positive();

        let mut wrong_kind = tail.clone();
        wrong_kind.kind = "execution_graph.checkpoint".into();
        let mut foreign_identity = tail.clone();
        foreign_identity.payload["_runtime_activity_binding"]["activity_id"] =
            "activity:execution:foreign-reader-witness".into();
        for altered in [wrong_kind, foreign_identity] {
            fixture
                .backend
                .set_fault(Some(GraphReadFault::Replace(tail.sequence, altered)), false);
            let (store, plane) = fixture.cold();
            assert!(plane.graphs().get(&fixture.expected.id).is_none());
            assert!(matches!(
                store.load(&fixture.expected.id),
                Err(ExecutionStateStoreError::Corrupt { .. })
            ));
            assert!(
                fixture
                    .backend
                    .hits
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
            );
            assert_eq!(plane.metrics().snapshot().graph_recoveries, 0);
            assert_eq!(plane.metrics().snapshot().graph_publishes, 0);
            assert!(plane.graphs().get(&fixture.expected.id).is_none());
            assert_eq!(
                fixture.events.list_stream(&fixture.expected.id).unwrap(),
                before
            );
            assert_eq!(fixture.events.durable_commit_cursor().unwrap(), cursor);
        }
        fixture.backend.set_fault(None, false);
        assert_positive();
        assert_eq!(
            fixture.events.list_stream(&fixture.expected.id).unwrap(),
            before
        );
        assert_eq!(fixture.events.durable_commit_cursor().unwrap(), cursor);
    }

    #[test]
    fn r1_bounded_history_graph_canonical_checkpoint_and_tail_fail_closed() {
        let fixture = CanonicalGraphFixture::new();
        let (store, plane) = fixture.cold();
        let recovered = store.load_snapshot(&fixture.expected.id).unwrap();
        assert_eq!(recovered.as_ref(), &fixture.expected);
        assert_eq!(
            recovered.recovery_cursor.commit_cursor,
            fixture.records.last().unwrap().commit_cursor
        );
        assert_eq!(plane.metrics().snapshot().graph_recoveries, 1);
        assert_eq!(plane.metrics().snapshot().graph_publishes, 1);
        assert_eq!(
            store.load_snapshot(&fixture.expected.id).unwrap().as_ref(),
            &fixture.expected
        );
        assert_eq!(plane.metrics().snapshot().graph_recoveries, 1);
        let (rebuilt, rebuilt_plane) = fixture.cold();
        assert_eq!(
            rebuilt
                .load_snapshot(&fixture.expected.id)
                .unwrap()
                .as_ref(),
            &fixture.expected
        );
        assert_eq!(rebuilt_plane.metrics().snapshot().graph_publishes, 1);

        let latest = fixture.checkpoints.last().unwrap();
        let mut bad_tail = fixture.records.last().unwrap().clone();
        bad_tail.kind = "execution_graph.unknown".into();
        fixture.backend.set_fault(
            Some(GraphReadFault::Replace(bad_tail.sequence, bad_tail)),
            true,
        );
        fixture.assert_rejected_without_publish();
        assert!(
            fixture
                .backend
                .hits
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        fixture.backend.set_fault(None, false);
        assert_eq!(
            fixture
                .events
                .stream_revision(&fixture.expected.id)
                .unwrap(),
            fixture.expected.revision
        );
        assert!(latest.sequence < fixture.expected.revision);
    }

    #[test]
    fn r1_graph_canonical_covered_body_is_not_resolved_but_history_still_rejects() {
        let fixture = CanonicalGraphFixture::new();
        let old = &fixture.checkpoints[0];
        let latest = fixture.checkpoints.last().unwrap();
        let old_id = old.payload["body"]["artifact"]["selector"]
            .as_str()
            .unwrap()
            .strip_prefix("artifact://")
            .unwrap();
        let latest_id = latest.payload["body"]["artifact"]["selector"]
            .as_str()
            .unwrap()
            .strip_prefix("artifact://")
            .unwrap();
        assert_ne!(old_id, latest_id);
        let old_path = fixture.blob_path(old);
        std::fs::remove_file(&old_path).unwrap();
        assert!(!old_path.exists());
        fixture.artifact_reads.records.lock().unwrap().clear();
        fixture.backend.reads.lock().unwrap().clear();
        let (store, _) = fixture.cold();
        assert_eq!(
            store.load_snapshot(&fixture.expected.id).unwrap().as_ref(),
            &fixture.expected
        );
        let reads = fixture.artifact_reads.records.lock().unwrap().clone();
        assert_eq!(reads.get(old_id).copied().unwrap_or(0), 0);
        assert!(reads.get(latest_id).copied().unwrap_or(0) > 0);
        let pages = fixture.backend.reads.lock().unwrap();
        assert!(pages.iter().any(|(descending, _, _, count)| *descending
            && *count > (fixture.expected.revision - latest.sequence + 1) as usize));
        drop(pages);
        assert!(decode_execution_graph_event(&fixture.events, old).is_err());
        assert!(
            fixture
                .artifact_reads
                .records
                .lock()
                .unwrap()
                .get(old_id)
                .copied()
                .unwrap_or(0)
                > 0
        );
    }

    #[test]
    fn r1_graph_canonical_latest_real_blob_missing_length_and_digest_fail_closed() {
        let fixture = CanonicalGraphFixture::new();
        let old = &fixture.checkpoints[0];
        let latest = fixture.checkpoints.last().unwrap();
        let path = fixture.blob_path(latest);
        let bytes = std::fs::read(&path).unwrap();
        assert!(decode_execution_graph_event(&fixture.events, old).is_ok());
        std::fs::remove_file(&path).unwrap();
        assert!(!path.exists());
        fixture.assert_rejected_without_publish();
        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            bytes.len() as u64 - 1
        );
        fixture.assert_rejected_without_publish();
        let mut corrupted = bytes.clone();
        corrupted[0] ^= 1;
        std::fs::write(&path, &corrupted).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes.len() as u64);
        fixture.assert_rejected_without_publish();
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            fixture.cold().0.load(&fixture.expected.id).unwrap(),
            fixture.expected
        );
    }

    #[test]
    fn r1_graph_canonical_latest_schema_kind_ref_and_identity_negative_matrix() {
        let fixture = CanonicalGraphFixture::new();
        let latest = fixture.checkpoints.last().unwrap();
        let mut cases = Vec::new();
        let mut record = latest.clone();
        record.schema_version = 1;
        cases.push(("record-schema", record));
        let mut record = latest.clone();
        record.payload["content_schema"] = 1.into();
        cases.push(("content-schema", record));
        let mut record = latest.clone();
        record.kind = "execution_graph.planned".into();
        cases.push(("kind-mismatch", record));
        cases.push((
            "unknown-body",
            fixture.changed_body(latest, |value| value["event"] = "unknown".into()),
        ));
        let invalid_json = vec![b'x'; 300 * 1024];
        let artifact = fixture
            .artifacts
            .write_pre_pinned_blocking(
                &format!(
                    "session:{}",
                    fixture.expected.lineage.as_ref().unwrap().session_id
                ),
                "negative-valid-hash-invalid-json",
                &invalid_json,
                &crate::PreparedArtifactOrigin::EventBody {
                    stream_id: latest.stream_id.clone(),
                    idempotency_key: latest.idempotency_key.clone().unwrap(),
                    kind: latest.kind.clone(),
                },
            )
            .unwrap();
        let mut record = latest.clone();
        record.payload["sha256"] = artifact.sha256.clone().into();
        record.payload["bytes"] = artifact.bytes.into();
        record.payload["body"] = serde_json::json!({"kind":"artifact_ref", "artifact":artifact});
        cases.push(("valid-hash-invalid-json", record));
        for field in ["id", "revision", "lineage"] {
            let altered = fixture.changed_body(latest, |value| match field {
                "id" => value["graph"]["id"] = "foreign-graph".into(),
                "revision" => value["graph"]["revision"] = (latest.sequence + 1).into(),
                _ => value["graph"]["lineage"]["session_id"] = "foreign-session".into(),
            });
            assert!(
                fixture.events.resolve_graph_record(&altered).is_ok(),
                "hash-consistent semantic fault: {field}"
            );
            cases.push((field, altered));
        }
        cases.push((
            "lineage-missing",
            fixture.changed_body(latest, |value| {
                value["graph"]["lineage"] = serde_json::Value::Null
            }),
        ));
        for field in [
            "root_execution_id",
            "session_id",
            "turn_id",
            "root_task_id",
            "task_id",
            "activity_id",
            "parent_activity_id",
            "initiator_activity_id",
            "node_id",
            "generation",
            "revision",
            "fence",
        ] {
            let mut record = latest.clone();
            record.payload["_runtime_activity_binding"][field] =
                if matches!(field, "generation" | "revision" | "fence") {
                    999999.into()
                } else {
                    "foreign-identity".into()
                };
            cases.push((field, record));
        }
        let mut record = latest.clone();
        record
            .payload
            .as_object_mut()
            .unwrap()
            .remove("_runtime_activity_binding");
        cases.push(("binding-missing", record));
        let mut record = latest.clone();
        record.stream_id = "foreign-stream".into();
        cases.push(("stream", record));
        let mut record = latest.clone();
        record.scope = RuntimeEventScope::Goal;
        cases.push(("scope", record));
        for field in ["selector", "visibility_scope", "bytes", "sha256"] {
            let mut record = latest.clone();
            record.payload["body"]["artifact"][field] = match field {
                "selector" => "artifact://missing-negative-fixture".into(),
                "bytes" => (latest.payload["bytes"].as_u64().unwrap() + 1).into(),
                "sha256" => format!("sha256:{}", "0".repeat(64)).into(),
                _ => "session:foreign".into(),
            };
            cases.push((field, record.clone()));
            if matches!(field, "bytes" | "sha256") {
                record.payload[field] = record.payload["body"]["artifact"][field].clone();
                cases.push(("ref-envelope-agree-but-not-real-object", record));
            }
        }
        let mut record = latest.clone();
        record.payload["body"] = serde_json::json!({"kind":"artifact_ref"});
        cases.push(("ref-missing", record));
        for (name, record) in cases {
            assert!(
                decode_execution_graph_event(&fixture.events, &fixture.checkpoints[0]).is_ok(),
                "older checkpoint valid for {name}"
            );
            fixture.backend.set_fault(
                Some(GraphReadFault::Replace(latest.sequence, record)),
                false,
            );
            fixture.assert_rejected_without_publish();
            assert!(
                fixture
                    .backend
                    .hits
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0,
                "fault-hit: {name}"
            );
        }
        fixture.backend.set_fault(None, false);
        assert_eq!(
            fixture.cold().0.load(&fixture.expected.id).unwrap(),
            fixture.expected
        );
    }

    #[test]
    fn r1_graph_canonical_tail_first_middle_last_and_missing_checkpoint_fail_closed() {
        let fixture = CanonicalGraphFixture::new();
        let checkpoint = fixture.checkpoints.last().unwrap().sequence;
        for sequence in [checkpoint + 1, checkpoint + 3, fixture.expected.revision] {
            let original = fixture
                .records
                .iter()
                .find(|record| record.sequence == sequence)
                .unwrap();
            let mut wrong_stream = original.clone();
            wrong_stream.stream_id = "foreign-stream".into();
            let mut wrong_kind = original.clone();
            wrong_kind.kind = "execution_graph.checkpoint".into();
            let mut wrong_schema = original.clone();
            wrong_schema.schema_version = 1;
            let mut wrong_content_schema = original.clone();
            wrong_content_schema.payload["content_schema"] = 1.into();
            let mut wrong_scope = original.clone();
            wrong_scope.scope = RuntimeEventScope::Goal;
            let mut wrong_body = original.clone();
            wrong_body.payload["sha256"] = format!("sha256:{}", "0".repeat(64)).into();
            let wrong_revision = fixture.changed_body(original, |value| {
                value["delta"]["revision"] = (sequence + 1).into()
            });
            let unknown_body =
                fixture.changed_body(original, |value| value["event"] = "unknown".into());
            let mut missing_ref = original.clone();
            missing_ref.payload["body"] = serde_json::json!({"kind":"artifact_ref"});
            let mut wrong_sequence = original.clone();
            wrong_sequence.sequence += 1;
            let faults = vec![
                GraphReadFault::Missing(sequence),
                GraphReadFault::Duplicate(sequence),
                GraphReadFault::Swap(sequence),
                GraphReadFault::Replace(sequence, wrong_stream),
                GraphReadFault::Replace(sequence, wrong_kind),
                GraphReadFault::Replace(sequence, wrong_schema),
                GraphReadFault::Replace(sequence, wrong_body),
                GraphReadFault::Replace(sequence, wrong_revision),
                GraphReadFault::Replace(sequence, missing_ref),
                GraphReadFault::Replace(sequence, wrong_sequence),
                GraphReadFault::Replace(sequence, wrong_content_schema),
                GraphReadFault::Replace(sequence, wrong_scope),
                GraphReadFault::Replace(sequence, unknown_body),
            ];
            for fault in faults {
                for forward_only in [false, true] {
                    fixture.backend.set_fault(Some(fault.clone()), forward_only);
                    fixture.assert_rejected_without_publish();
                    assert!(
                        fixture
                            .backend
                            .hits
                            .load(std::sync::atomic::Ordering::SeqCst)
                            > 0,
                        "fault-hit: {fault:?}, forward={forward_only}"
                    );
                }
            }
        }
        fixture
            .backend
            .set_fault(Some(GraphReadFault::Missing(checkpoint)), false);
        fixture.assert_rejected_without_publish();
        assert!(
            fixture
                .backend
                .hits
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        fixture.backend.set_fault(None, false);
        assert_eq!(
            fixture.cold().0.load(&fixture.expected.id).unwrap(),
            fixture.expected
        );
    }

    #[test]
    fn r1_graph_canonical_frozen_head_paging_and_budget_boundaries_are_honest() {
        let fixture = CanonicalGraphFixture::new();
        let head = fixture.expected.revision;
        fixture.backend.reads.lock().unwrap().clear();
        *fixture.backend.append_on_read.lock().unwrap() =
            Some((fixture.writer.clone(), fixture.expected.clone()));
        let (store, plane) = fixture.cold();
        let frozen = store.load_snapshot(&fixture.expected.id).unwrap();
        assert_eq!(frozen.as_ref(), &fixture.expected);
        assert_eq!(
            fixture
                .events
                .stream_revision(&fixture.expected.id)
                .unwrap(),
            head + 1
        );
        assert_eq!(
            plane.graphs().get(&fixture.expected.id).unwrap().revision,
            head
        );
        let reads = fixture.backend.reads.lock().unwrap().clone();
        assert!(reads.iter().any(|(_, _, through, _)| *through == head));
        assert!(reads.iter().all(|(_, _, through, _)| *through <= head));
        let current = store.load_current(&fixture.expected.id).unwrap();
        assert_eq!(current.revision, head + 1);
        assert_eq!(
            current.recovery_cursor.commit_cursor,
            fixture
                .events
                .latest_for_stream(&fixture.expected.id)
                .unwrap()
                .unwrap()
                .commit_cursor
        );
        let mut after = 0;
        let mut records = Vec::new();
        loop {
            let page = fixture
                .events
                .list_stream_after(&fixture.expected.id, after, head, 7, 1024 * 1024)
                .unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 7);
            for record in &page {
                assert_eq!(record.sequence, after + 1);
                after = record.sequence;
            }
            records.extend(page);
        }
        assert_eq!(records, fixture.records);
        let error = fixture
            .events
            .list_stream_after(&fixture.expected.id, 0, head, 1, 1)
            .unwrap_err();
        assert!(error.starts_with("HistoryBudgetExceeded:"));
        assert!(fixture
            .events
            .list_stream_before(&fixture.expected.id, u64::MAX, 1, 1024)
            .is_err());
        assert!(fixture
            .events
            .list_stream_after(&fixture.expected.id, u64::MAX, head, 1, 1024)
            .is_err());
        assert!(fixture
            .events
            .list_stream_after(&fixture.expected.id, head, head, 1, 1024)
            .unwrap()
            .is_empty());
        assert!(fixture
            .events
            .list_stream_after(&fixture.expected.id, 0, head, 0, 1024)
            .unwrap()
            .is_empty());
        assert!(fixture
            .events
            .list_stream_after(&fixture.expected.id, 0, head, 1, 0)
            .unwrap()
            .is_empty());
        let small = crate::ArtifactStore::for_test(
            fixture.root.join("object-budget"),
            crate::ArtifactStoreConfig {
                compact_threshold_bytes: 1,
                max_object_bytes: 1024,
                ..crate::ArtifactStoreConfig::default()
            },
        )
        .unwrap();
        assert!(matches!(
            small.write_pre_pinned_blocking(
                "session:budget",
                "negative-oversize",
                &vec![0; 1025],
                &crate::PreparedArtifactOrigin::EventBody {
                    stream_id: "budget".into(),
                    idempotency_key: "negative-oversize".into(),
                    kind: "execution_graph.registered".into()
                }
            ),
            Err(crate::ArtifactError::ObjectTooLarge)
        ));
        assert_eq!(
            plane.graphs().get(&fixture.expected.id).unwrap().revision,
            head + 1
        );
    }

    #[tokio::test]
    async fn r1_graph_canonical_binding_conflict_uses_real_history_consumer() {
        let fixture = CanonicalGraphFixture::new();
        let latest = fixture.checkpoints.last().unwrap();
        let (node_id, binding) = &fixture.bindings[0];
        for field in [
            "executor_kind",
            "resource_lease_refs",
            "scope_lease_ref",
            "worktree_lease_ref",
        ] {
            let mut conflict = binding.clone();
            match field {
                "executor_kind" => conflict.executor_kind = "negative-foreign-executor".into(),
                "resource_lease_refs" => conflict.resource_lease_refs.clear(),
                "scope_lease_ref" => conflict.scope_lease_ref = None,
                _ => conflict.worktree_lease_ref = Some("worktree:negative-conflict".into()),
            }
            let altered = fixture.changed_body(latest, |value| {
                value["bindings"] =
                    serde_json::to_value(BTreeMap::from([(node_id.clone(), conflict)])).unwrap()
            });
            // Each injected read fault advances the backend source-view epoch
            // (GraphFaultBackend::set_fault), so the schema6 projection-history
            // checkpoint published while diagnosing the previous field is bound
            // to a stale source view. The product must refuse to reuse it
            // ("checkpoint source mutation fence changed"), so drop it here and
            // let this field be diagnosed from a cold history that matches the
            // current source view.
            fixture
                .events
                .delete_projection_checkpoint(&format!(
                    "projection-history:{}",
                    fixture.expected.id
                ))
                .unwrap();
            fixture.backend.set_fault(
                Some(GraphReadFault::Replace(latest.sequence, altered)),
                false,
            );
            assert_eq!(
                fixture.cold().0.load(&fixture.expected.id).unwrap(),
                fixture.expected
            );
            let services = fixture.services();
            let error = fixture.projection(&services)
            .await
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("current Graph binding fact changed"),
                "{field}: {error}"
            );
            assert!(
                fixture
                    .backend
                    .hits
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
            );
            assert!(fixture
                .events
                .projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
                .unwrap()
                .is_none_or(|saved| saved.payload["schema"] == 6 && saved.payload["incomplete"] == true));
        }
    }

    #[tokio::test]
    async fn r1_graph_canonical_history_bindings_and_unreceipted_effect_are_not_replaced_by_snapshot(
    ) {
        use harness_contract::projection::TerminalEffectSettlement;
        let fixture = CanonicalGraphFixture::new();
        for (node_id, binding) in &fixture.bindings {
            let node = fixture
                .expected
                .nodes
                .iter()
                .find(|node| &node.id == node_id)
                .unwrap();
            let ticket = super::super::registry::NodeExecutionTicket {
                graph_id: fixture.expected.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: fixture.expected.service_class,
                attempt: binding.attempt,
                idempotency_key: binding.ticket_idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            assert!(matches!(
                fixture.writer.inspect_execution_effect(&ticket).unwrap(),
                super::super::commit_service::ExecutionEffectState::Uncertain
            ));
            assert!(fixture
                .events
                .latest_for_stream_kind(
                    &super::super::events::ExecutionNodeBinding::effect_stream_id(&ticket),
                    "execution.effect.receipt"
                )
                .unwrap()
                .is_none());
        }
        let services = fixture.services();
        let projection = fixture.projection(&services)
        .await
        .unwrap();
        assert_eq!(
            projection.terminal_axes.execution.settlement,
            TerminalEffectSettlement::Unsettled
        );
        for (node_id, binding) in &fixture.bindings {
            for reference in binding
                .resource_lease_refs
                .iter()
                .chain(binding.scope_lease_ref.iter())
                .chain(binding.worktree_lease_ref.iter())
            {
                assert!(
                    projection
                        .terminal_axes
                        .execution
                        .evidence_refs
                        .contains(reference),
                    "preserve {reference}"
                );
            }
            let node = fixture
                .expected
                .nodes
                .iter()
                .find(|node| &node.id == node_id)
                .unwrap();
            let ticket = super::super::registry::NodeExecutionTicket {
                graph_id: fixture.expected.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: fixture.expected.service_class,
                attempt: binding.attempt,
                idempotency_key: binding.ticket_idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            assert!(projection
                .terminal_axes
                .execution
                .evidence_refs
                .contains(&super::super::events::ExecutionNodeBinding::effect_stream_id(&ticket)));
        }
        let checkpoint = fixture
            .events
            .projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
            .unwrap()
            .unwrap();
        let session = fixture
            .expected
            .lineage
            .as_ref()
            .unwrap()
            .session_id
            .clone();
        let history = fixture
            .events
            .resolve_content(
                &format!("session:{session}"),
                &checkpoint.payload["body"],
                &crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: checkpoint.projection_id.clone(),
                    source_cursor: checkpoint.source_cursor,
                },
            )
            .unwrap();
        let history = serde_json::json!({"state": crate::execution_projection::own843_checkpoint_state(
            checkpoint.payload["schema"].as_u64().unwrap(), history,
        )});
        for (node_id, binding) in &fixture.bindings {
            let key = format!(
                "{node_id}:{}:{}",
                binding.attempt, binding.ticket_idempotency_key
            );
            assert_eq!(
                history["state"]["bindings"][&key],
                serde_json::json!([node_id, binding])
            );
        }
        let first_binding_record = fixture
            .records
            .iter()
            .find(
                |record| match decode_execution_graph_event(&fixture.events, record).unwrap() {
                    ExecutionGraphEvent::NodesStarted { bindings, .. }
                    | ExecutionGraphEvent::Checkpoint { bindings, .. } => {
                        bindings.get(&fixture.bindings[0].0) == Some(&fixture.bindings[0].1)
                    }
                    _ => false,
                },
            )
            .unwrap();
        let mut corrupt = first_binding_record.clone();
        corrupt.payload["sha256"] = format!("sha256:{}", "0".repeat(64)).into();
        fixture.backend.set_fault(
            Some(GraphReadFault::Replace(corrupt.sequence, corrupt)),
            false,
        );
        assert_eq!(
            fixture.cold().0.load(&fixture.expected.id).unwrap(),
            fixture.expected
        );
        fixture
            .events
            .delete_projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
            .unwrap();
        let fresh_services = fixture.services();
        assert!(fixture.projection(&fresh_services)
        .await
        .is_err());
        assert!(
            fixture
                .backend
                .hits
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        assert!(fixture
            .events
            .projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
            .unwrap()
            .is_none_or(|saved| saved.payload["schema"] == 6 && saved.payload["incomplete"] == true));
    }

    #[tokio::test]
    async fn r1_graph_canonical_start_checkpoint_binding_and_covered_history_body_are_distinct() {
        let mut fixture = CanonicalGraphFixture::new();
        let started = fixture.add_started_checkpoint_and_successor();
        for (node_id, binding) in &fixture.bindings {
            let node = fixture
                .expected
                .nodes
                .iter()
                .find(|node| &node.id == node_id)
                .unwrap();
            let ticket = super::super::registry::NodeExecutionTicket {
                graph_id: fixture.expected.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: fixture.expected.service_class,
                attempt: binding.attempt,
                idempotency_key: binding.ticket_idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            assert!(matches!(
                fixture.writer.inspect_execution_effect(&ticket).unwrap(),
                super::super::commit_service::ExecutionEffectState::Uncertain
            ));
            assert!(fixture
                .events
                .latest_for_stream_kind(
                    &super::super::events::ExecutionNodeBinding::effect_stream_id(&ticket),
                    "execution.effect.receipt"
                )
                .unwrap()
                .is_none());
        }
        let latest = fixture.checkpoints.last().unwrap();
        match decode_execution_graph_event(&fixture.events, latest).unwrap() {
            ExecutionGraphEvent::Checkpoint { bindings, .. } => assert!(bindings.is_empty()),
            _ => panic!("successor must be an actual checkpoint"),
        }
        let services = fixture.services();
        let projection = fixture.projection(&services)
        .await
        .unwrap();
        assert!(projection
            .terminal_axes
            .execution
            .evidence_refs
            .contains(&"resource:checkpoint-start".into()));
        let started_id = started.payload["body"]["artifact"]["selector"]
            .as_str()
            .unwrap()
            .strip_prefix("artifact://")
            .unwrap();
        let path = fixture.blob_path(&started);
        std::fs::remove_file(&path).unwrap();
        fixture.artifact_reads.records.lock().unwrap().clear();
        assert_eq!(
            fixture.cold().0.load(&fixture.expected.id).unwrap(),
            fixture.expected
        );
        assert_eq!(
            fixture
                .artifact_reads
                .records
                .lock()
                .unwrap()
                .get(started_id)
                .copied()
                .unwrap_or(0),
            0
        );
        fixture
            .events
            .delete_projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
            .unwrap();
        let fresh_services = fixture.services();
        assert!(fixture.projection(&fresh_services)
        .await
        .is_err());
        assert!(
            fixture
                .artifact_reads
                .records
                .lock()
                .unwrap()
                .get(started_id)
                .copied()
                .unwrap_or(0)
                > 0
        );
        assert!(fixture
            .events
            .projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
            .unwrap()
            .is_none_or(|saved| saved.payload["schema"] == 6 && saved.payload["incomplete"] == true));
    }

    #[test]
    fn r1_graph_canonical_checkpoint_required_binding_fields_are_not_invented() {
        let fixture = CanonicalGraphFixture::new();
        let latest = fixture.checkpoints.last().unwrap();
        let (node_id, binding) = &fixture.bindings[0];
        for field in [
            "executor_kind",
            "attempt",
            "ticket_idempotency_key",
            "resource_lease_refs",
        ] {
            let altered = fixture.changed_body(latest, |value| {
                let mut missing = serde_json::to_value(binding).unwrap();
                missing.as_object_mut().unwrap().remove(field);
                value["bindings"] =
                    serde_json::to_value(BTreeMap::from([(node_id.clone(), missing)])).unwrap();
            });
            assert!(fixture.events.resolve_graph_record(&altered).is_ok());
            fixture.backend.set_fault(
                Some(GraphReadFault::Replace(latest.sequence, altered)),
                false,
            );
            fixture.assert_rejected_without_publish();
            assert!(
                fixture
                    .backend
                    .hits
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0,
                "fault-hit {field}"
            );
        }
    }


    #[tokio::test]
    async fn own843_graph_source_view_requires_its_own_fence_and_real_fault_reads() {
        let fixture = CanonicalGraphFixture::new();
        let source = fixture.backend.source_mutation_fence().unwrap();
        let inner = fixture.backend.inner.source_mutation_fence().unwrap();
        assert_ne!(source.incarnation, inner.incarnation);
        let original = fixture.records[0].clone();
        let key = original.idempotency_key.as_deref().unwrap();
        let mut changed = original.clone();
        changed.payload["sha256"] = serde_json::json!("sha256:view-replaced");
        fixture.backend.set_fault(
            Some(GraphReadFault::Replace(changed.sequence, changed.clone())),
            false,
        );
        let current = fixture.backend.source_mutation_fence().unwrap();
        assert_ne!(source, current);
        let observed = fixture
            .events
            .event_by_idempotency_key(&original.stream_id, key)
            .unwrap()
            .unwrap();
        assert_eq!(observed, changed);
        assert!(
            fixture
                .backend
                .hits
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        assert!(fixture
            .backend
            .compare_and_put_projection_checkpoint_fenced(
                "843-view-fence",
                original.commit_cursor,
                0,
                &serde_json::json!({}),
                0,
                &source,
            )
            .is_err());
        assert!(fixture
            .backend
            .projection_checkpoint("843-view-fence")
            .unwrap()
            .is_none());
        fixture.backend.set_fault(None, false);
        let restored = fixture.backend.source_mutation_fence().unwrap();
        assert_ne!(restored, source);
        assert_ne!(restored, current);
        assert!(fixture
            .backend
            .compare_and_put_projection_checkpoint_fenced(
                "843-view-fence",
                original.commit_cursor,
                0,
                &serde_json::json!({}),
                0,
                &inner,
            )
            .is_err());
        fixture
            .backend
            .compare_and_put_projection_checkpoint_fenced(
                "843-view-fence",
                original.commit_cursor,
                0,
                &serde_json::json!({}),
                0,
                &restored,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn own843_previously_folded_original_metadata_change_is_rejected_on_resume() {
        let fixture = CanonicalGraphFixture::new();
        let services = fixture.services();
        let first = crate::execution_projection::snapshot(
            &services,
            &fixture.expected.id,
            &fixture.context(&services),
        )
        .await
        .unwrap();
        assert!(first
            .health
            .iter()
            .any(|entity| entity.kind == "history_verification"
                && entity.status.as_deref() == Some("incomplete")));
        assert!(!first.terminal_axes.execution.coverage_complete);
        let mut changed = fixture.records[0].clone();
        changed.payload["sha256"] = format!("sha256:{}", "0".repeat(64)).into();
        fixture.backend.set_fault(
            Some(GraphReadFault::Replace(changed.sequence, changed)),
            false,
        );
        let error = fixture.projection(&services).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("verified original record changed"),
            "{error}"
        );
        assert!(
            fixture
                .backend
                .hits
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        let saved = fixture
            .events
            .projection_checkpoint(&format!("projection-history:{}", fixture.expected.id))
            .unwrap()
            .unwrap();
        assert_eq!(saved.payload["incomplete"], true);
    }

    #[tokio::test]
    async fn own843_foreign_prefix_owner_cannot_supply_current_history() {
        let original = CanonicalGraphFixture::new();
        let original_services = original.services();
        let first = crate::execution_projection::snapshot(
            &original_services,
            &original.expected.id,
            &original.context(&original_services),
        )
        .await
        .unwrap();
        assert!(first
            .health
            .iter()
            .any(|entity| entity.status.as_deref() == Some("incomplete")));
        let saved = original
            .events
            .projection_checkpoint(&format!("projection-history:{}", original.expected.id))
            .unwrap()
            .unwrap();
        let foreign = CanonicalGraphFixture::new();
        let foreign_services = foreign.services();
        let result = foreign.events.compare_and_put_projection_checkpoint(
            &format!("projection-history:{}", foreign.expected.id),
            saved.source_cursor,
            0,
            &saved.payload,
            0,
        );
        if result.is_ok() {
            assert!(foreign.projection(&foreign_services).await.is_err());
        } else {
            assert!(foreign
                .events
                .projection_checkpoint(&format!("projection-history:{}", foreign.expected.id))
                .unwrap()
                .is_none());
        }
    }

    fn replace_never_admitted_record_body(
        events: &RuntimeEventStore,
        record: &mut crate::DurableRuntimeEvent,
        event: ExecutionGraphEvent,
    ) {
        let binding = record
            .activity_binding()
            .expect("retain original source identity");
        let activity = record.payload["_runtime_activity_binding"].clone();
        record.kind = event.kind().into();
        record.payload = events
            .prepare_content(
                &format!("session:{}", binding.session_id),
                &format!("graph:{}:revision:{}", record.stream_id, record.sequence),
                &serde_json::to_value(&event).unwrap(),
                &crate::PreparedArtifactOrigin::EventBody {
                    stream_id: record.stream_id.clone(),
                    idempotency_key: record.idempotency_key.clone().unwrap(),
                    kind: record.kind.clone(),
                },
            )
            .unwrap();
        record.payload["_runtime_activity_binding"] = activity;
        assert_eq!(record.activity_binding(), Some(binding));
        assert_eq!(
            decode_execution_graph_event(events, record).unwrap().kind(),
            event.kind(),
            "fault body must satisfy original content decoder first"
        );
    }

    fn never_admitted_fixture() -> (
        tempfile::TempDir,
        Arc<GraphFaultBackend>,
        Arc<RuntimeEventStore>,
        Arc<ExecutionCommitService>,
        ExecutionGraph,
        ExecutionGraph,
    ) {
        let root = tempfile::tempdir().unwrap();
        let request =
            super::super::executors::agent_tool::carrier_437_tests::real_request(root.path());
        let mut expected =
            super::super::executors::agent_tool::carrier_437_tests::graph_for(request);
        crate::test_support::attach_execution_graph_lineage(&mut expected);
        let backend = GraphFaultBackend::new();
        let events = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
        let writer = Arc::new(ExecutionCommitService::new(events.clone()));
        let registered = writer.register_graph(expected.clone()).unwrap().graph;
        (root, backend, events, writer, expected, registered)
    }

    #[test]
    fn never_admitted_cancel_wins_is_read_only_and_old_start_cas_loses() {
        let (_root, _backend, events, writer, expected, registered) = never_admitted_fixture();
        let cancelled = writer
            .apply_command(
                &registered,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: registered.revision,
                    reason: "original call stopped".into(),
                },
            )
            .unwrap()
            .graph;
        let before = events.list_stream(&expected.id).unwrap();
        for _ in 0..2 {
            writer
                .confirm_cancelled_bound_child_never_admitted(&expected)
                .unwrap();
        }
        assert_eq!(
            events.list_stream(&expected.id).unwrap(),
            before,
            "read fact must not publish receipt or attempt"
        );
        assert!(cancelled.recovery_cursor.node_attempts.is_empty());
        let error = writer
            .apply_command(
                &registered,
                &ExecutionGraphCommand::Start {
                    expected_revision: registered.revision,
                },
            )
            .err()
            .expect("stale snapshot Start must reject");
        assert!(
            matches!(&error, super::super::commit_service::ExecutionCommitError::EventStore(RuntimeEventStoreError::InvalidTransaction(reason)) if reason == "event idempotency key already belongs to another transaction"),
            "{error:?}"
        );
        let error = writer
            .apply_command(
                &cancelled,
                &ExecutionGraphCommand::Start {
                    expected_revision: registered.revision,
                },
            )
            .err()
            .expect("old revision must fail original command CAS");
        assert!(
            matches!(
                error,
                super::super::commit_service::ExecutionCommitError::StaleRevision { .. }
            ),
            "{error:?}"
        );
        assert_eq!(
            events.list_stream(&expected.id).unwrap(),
            before,
            "both losing starts publish nothing"
        );
    }

    #[test]
    fn never_admitted_dependency_ready_is_not_a_started_attempt() {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        let (_root, _backend, _events, writer, expected, registered) = never_admitted_fixture();
        let started = writer
            .apply_command(
                &registered,
                &ExecutionGraphCommand::Start {
                    expected_revision: registered.revision,
                },
            )
            .unwrap()
            .graph;
        let ready = writer
            .transition_node(
                &started,
                &expected.nodes[0].id,
                ExecutionNodeStatus::Ready,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        assert_eq!(
            ready.recovery_cursor.node_attempts[&expected.nodes[0].id],
            0
        );
        let cancelled = writer
            .apply_command(
                &ready,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: ready.revision,
                    reason: "before admission".into(),
                },
            )
            .unwrap()
            .graph;
        writer
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .unwrap();
        assert_eq!(
            cancelled.node_statuses[&expected.nodes[0].id],
            ExecutionNodeStatus::Cancelled
        );
    }

    #[test]
    fn never_admitted_original_checkpoints_and_multiple_pages_are_authenticated() {
        let (_root, _backend, events, writer, expected, registered) = never_admitted_fixture();
        let mut current = registered;
        for _ in 0..105 {
            current = writer
                .apply_command(
                    &current,
                    &ExecutionGraphCommand::Start {
                        expected_revision: current.revision,
                    },
                )
                .unwrap()
                .graph;
        }
        writer
            .apply_command(
                &current,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: current.revision,
                    reason: "cancel before any admission".into(),
                },
            )
            .unwrap();
        let records = events.list_stream(&expected.id).unwrap();
        assert!(records.len() > 100, "must cross original bounded page");
        assert!(
            records
                .iter()
                .any(|record| record.kind == "execution_graph.checkpoint"),
            "must hit real writer checkpoint"
        );
        writer
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .unwrap();
        assert_eq!(events.list_stream(&expected.id).unwrap(), records);
    }

    #[test]
    fn never_admitted_history_gap_foreign_identity_and_checkpoint_fail_closed() {
        let (_root, backend, events, writer, expected, registered) = never_admitted_fixture();
        writer
            .apply_command(
                &registered,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: registered.revision,
                    reason: "cancel".into(),
                },
            )
            .unwrap();
        let reader = ExecutionGraphStateStore::new(events.clone());
        reader
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .unwrap();
        let records = events.list_stream(&expected.id).unwrap();
        let cancel = records.last().unwrap().clone();
        let mut foreign = cancel.clone();
        foreign.actor = Some("foreign".into());
        let mut unknown = cancel.clone();
        unknown.transaction_id = "foreign:command:cancel:2".into();
        let projected = reader.load_current(&expected.id).unwrap();
        let mut ambiguous = cancel.clone();
        replace_never_admitted_record_body(
            &events,
            &mut ambiguous,
            ExecutionGraphEvent::Checkpoint {
                cause: "execution_graph.recovered".into(),
                graph: projected,
                bindings: BTreeMap::new(),
                lease_closures: vec![],
            },
        );
        for (fault, predicate) in [
            (GraphReadFault::Missing(1), "lease start source mismatch"),
            (GraphReadFault::Duplicate(1), "lease start source mismatch"),
            (
                GraphReadFault::Replace(cancel.sequence, foreign),
                "lease start source mismatch",
            ),
            (
                GraphReadFault::Replace(cancel.sequence, unknown),
                "admission or ambiguous mutation",
            ),
            (
                GraphReadFault::Replace(cancel.sequence, ambiguous),
                "admission or ambiguous mutation",
            ),
        ] {
            backend.set_fault(Some(fault), true);
            let error = reader
                .confirm_cancelled_bound_child_never_admitted(&expected)
                .unwrap_err();
            assert!(
                error.to_string().contains(predicate),
                "target {predicate}, actual {error}"
            );
            assert!(
                backend.hits.load(std::sync::atomic::Ordering::SeqCst) > 0,
                "fault must be read"
            );
        }
        backend.set_fault(None, false);
        for variant in 0..4 {
            let mut wrong = expected.clone();
            match variant {
                0 => wrong
                    .parent_execution
                    .as_mut()
                    .unwrap()
                    .node_id
                    .push_str("foreign"),
                1 => wrong.nodes[0].payload_ref.push(' '),
                2 => wrong.id.push_str("foreign"),
                _ => wrong
                    .lineage
                    .as_mut()
                    .unwrap()
                    .session_id
                    .push_str("foreign"),
            }
            assert!(reader
                .confirm_cancelled_bound_child_never_admitted(&wrong)
                .is_err());
        }
        reader
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .unwrap();
    }

    #[test]
    fn never_admitted_historical_start_cannot_be_erased_by_terminal_snapshot() {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        let (_root, backend, events, writer, expected, registered) = never_admitted_fixture();
        let node = &expected.nodes[0];
        let ready = writer
            .transition_node(
                &registered,
                &node.id,
                ExecutionNodeStatus::Ready,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        let binding = super::super::events::ExecutionNodeBinding {
            executor_kind: node.executor_kind.clone(),
            ticket_idempotency_key: node.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let running = writer
            .bind_and_start_node(&ready, &node.id, binding)
            .unwrap()
            .graph;
        assert_eq!(running.recovery_cursor.node_attempts[&node.id], 1);
        let mut cancelled = writer
            .apply_command(
                &running,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: running.revision,
                    reason: "after admission".into(),
                },
            )
            .unwrap()
            .graph;
        cancelled.recovery_cursor.node_attempts.clear();
        let mut record = events.list_stream(&expected.id).unwrap().pop().unwrap();
        replace_never_admitted_record_body(
            &events,
            &mut record,
            ExecutionGraphEvent::Checkpoint {
                cause: "execution_graph.command_applied".into(),
                graph: cancelled,
                bindings: BTreeMap::new(),
                lease_closures: vec![],
            },
        );
        assert!(
            matches!(decode_execution_graph_event(&events, &record).unwrap(), ExecutionGraphEvent::Checkpoint { graph, .. } if graph.recovery_cursor.node_attempts.is_empty()),
            "injected terminal snapshot must actually conceal original attempt"
        );
        backend.set_fault(
            Some(GraphReadFault::Replace(record.sequence, record)),
            false,
        );
        let reader = ExecutionGraphStateStore::new(events);
        let error = reader
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .unwrap_err();
        assert!(
            backend.hits.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "production bounded reader must consume actual replaced terminal record"
        );
        assert!(
            error
                .to_string()
                .contains("admission or ambiguous mutation"),
            "{error}"
        );
    }

    #[test]
    fn never_admitted_head_change_during_final_read_rejects() {
        let (_root, backend, events, writer, expected, registered) = never_admitted_fixture();
        let cancelled = writer
            .apply_command(
                &registered,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: registered.revision,
                    reason: "before read".into(),
                },
            )
            .unwrap()
            .graph;
        *backend.append_on_read.lock().unwrap() = Some((writer, cancelled.clone()));
        let reader = ExecutionGraphStateStore::new(events.clone());
        assert!(reader
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .is_err());
        assert_eq!(
            events.stream_revision(&expected.id).unwrap(),
            cancelled.revision + 1,
            "actual concurrent append must hit"
        );
    }

    #[test]
    fn shared_hot_plane_recovers_once_then_serves_memory() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
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

    #[tokio::test]
    async fn event_causal_read_cannot_consume_a_commit_using_an_older_hot_snapshot() {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        let events = Arc::new(RuntimeEventStore::for_test());
        let writer = ExecutionCommitService::new(Arc::clone(&events));
        let reader_plane = Arc::new(RuntimeHotStatePlane::default());
        let reader = ExecutionGraphStateStore::with_hot_state(
            Arc::clone(&events),
            Arc::clone(&reader_plane),
        );
        let mut graph = ExecutionGraph::new("causal terminal read");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "{}",
        ));
        let registered = writer.register_graph(graph).unwrap().graph;
        let initial = reader.load(&registered.id).unwrap();
        let mut changes = reader.subscribe_commits();
        let committed = writer
            .transition_node(
                &registered,
                &registered.nodes[0].id,
                ExecutionNodeStatus::Cancelled,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        changes
            .changed()
            .await
            .expect("durable commit notification");
        // Separate hot planes deterministically retain the window that exists
        // between EventStore commit publication and the graph writer's publish.
        assert_eq!(
            reader.load(&registered.id).unwrap().revision,
            initial.revision
        );
        assert_eq!(
            reader.projection(&registered.id).unwrap().revision,
            committed.revision,
            "public projection cannot publish stale state at a new event cursor"
        );
        let recovered = reader.load_current_async(&registered.id).await.unwrap();
        assert_eq!(recovered.revision, committed.revision);
        assert_eq!(
            recovered.node_statuses[&registered.nodes[0].id],
            ExecutionNodeStatus::Cancelled
        );
        let recoveries = reader_plane.metrics().snapshot().graph_recoveries;
        reader.load_current_async(&registered.id).await.unwrap();
        assert_eq!(
            reader_plane.metrics().snapshot().graph_recoveries,
            recoveries,
            "current hot state uses indexed revision validation, not full journal replay"
        );
    }

    #[test]
    fn r1_graph_canonical_legacy_format_rejected_and_unknown_tail_fails_closed() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
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
            .unwrap_err();

        assert_eq!(event_store.stream_revision(&registered.id).unwrap(), 1);
        let binding = event_store
            .latest_for_stream(&registered.id)
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        let payload = event_store
            .prepare_content(
                &format!("session:{}", binding.session_id),
                "bad-current-tail",
                &serde_json::json!({"event":"unknown"}),
                &crate::PreparedArtifactOrigin::EventBody {
                    stream_id: registered.id.clone(),
                    idempotency_key: "bad-current-tail".into(),
                    kind: "execution_graph.node_transitioned".into(),
                },
            )
            .unwrap();
        event_store
            .inject_content_transaction_fault(crate::AppendTransactionRequest {
                transaction_id: "bad-current-tail".into(),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: registered.id.clone(),
                    expected_revision: 1,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: registered.id.clone(),
                        scope: RuntimeEventScope::ExecutionGraph,
                        kind: "execution_graph.node_transitioned".into(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload,
                    }
                    .with_activity_binding(binding)
                    .unwrap(),
                    idempotency_key: Some("bad-current-tail".into()),
                    schema_version: 2,
                }],
            })
            .unwrap();

        let store = ExecutionGraphStateStore::new(event_store);
        assert!(store.load(&registered.id).is_err());
    }

    #[test]
    fn state_store_projection_keeps_node_payloads_opaque() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
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

#[cfg(test)]
mod not_found_location_tests {
    use super::*;

    /// A missing graph has to say where it was looked up.
    ///
    /// The failure this project chased for three runs was one error text -
    /// "execution graph `run:agentic-graph:<id>` does not exist" - repeated over a
    /// hundred times with no hint of which of six call sites produced it, so each
    /// investigation started by grepping for call sites instead of reading one
    /// line. The location is part of the error now.
    #[test]
    fn a_missing_graph_names_the_call_site_that_looked_it_up() {
        let store = ExecutionGraphStateStore::new(Arc::new(RuntimeEventStore::for_test()));
        let error = store.load("agentic-graph:absent").expect_err("absent");
        let rendered = error.to_string();
        assert!(
            rendered.contains("agentic-graph:absent"),
            "the id must stay in the message: {rendered}"
        );
        assert!(
            rendered.contains("looked up at"),
            "the call site must be in the message: {rendered}"
        );
        assert!(
            rendered.contains("state_store.rs"),
            "the call site must name a file: {rendered}"
        );
    }

    /// The store normalizes a run id at its own entry, so a call site that forgets
    /// the translation still resolves. Two call sites did forget
    /// (`review_evidence.rs`, `review_evidence/results.rs`), and that is where the
    /// 28 failures of one live run came from while the graph existed the whole time.
    ///
    /// Proved by which stream the store reads: a non-graph event is planted under the
    /// graph id. If the entry normalized, the store finds it and reports the scope
    /// mismatch; if it did not, the run-id stream is empty and the store reports
    /// NotFound.
    #[test]
    fn the_store_normalizes_a_run_id_at_its_own_entry_not_at_the_call_site() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        event_store
            .append(crate::runtime_event_store::RuntimeEventInput {
                stream_id: "agentic-graph:entry-normalization".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "probe".to_string(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("plant");
        let store = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let error = store
            .load("run:agentic-graph:entry-normalization")
            .expect_err("the stream is not a graph stream");
        let rendered = error.to_string();
        assert!(
            rendered.contains("corrupt"),
            "the store must have read the graph-id stream: {rendered}"
        );
    }

    /// The error keeps the id that was asked for. A message that renames the id
    /// cannot be matched against what the model or the caller actually saw, and the
    /// whole value of this error is that it names the thing that was looked up.
    #[test]
    fn the_error_keeps_the_id_that_was_asked_for() {
        let store = ExecutionGraphStateStore::new(Arc::new(RuntimeEventStore::for_test()));
        let error = store
            .load("run:agentic-graph:asked-for")
            .expect_err("absent");
        assert!(
            error.to_string().contains("run:agentic-graph:asked-for"),
            "the message must name the id that was asked for: {error}"
        );
    }

    /// `runner:` and `runs:` are separate namespaces, not run ids. Normalizing them
    /// would merge three different identity spaces into one.
    #[test]
    fn runner_and_runs_are_not_run_ids() {
        assert_eq!(
            graph_id_for_lookup("run:agentic-graph:abc"),
            "agentic-graph:abc"
        );
        assert_eq!(graph_id_for_lookup("runner:abc"), "runner:abc");
        assert_eq!(graph_id_for_lookup("runs:abc"), "runs:abc");
        assert_eq!(
            graph_id_for_lookup("agentic-graph:abc"),
            "agentic-graph:abc"
        );
    }
}
