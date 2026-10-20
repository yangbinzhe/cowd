use super::*;
use std::collections::{BTreeMap, BTreeSet};

use crate::execution_core::graph::{
    ExecutionCommitService, ExecutionGraphRunner, ExecutionGraphStateStore, ExecutionNodeBinding,
    ExecutionResourceManager, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError, NodeExecutorRegistry, ScopeLockManager, WorktreeLeaseManager,
};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeResult,
    ExecutionNodeSpec, ExecutionNodeStatus,
};

fn registered() -> (AgentActionService, ExecutionCommitService, ExecutionGraph) {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let commits = ExecutionCommitService::new(store);
    let mut graph = ExecutionGraph::new("Native historical NodeEffect source validation")
        .with_lineage(ExecutionGraphLineage {
            session_id: "session-465".into(),
            turn_id: "turn-465".into(),
            root_task_id: "task-465".into(),
            task_id: "task-465".into(),
            generation: 1,
        });
    for identity in ["first", "second"] {
        graph.nodes.push(ExecutionNodeSpec {
            id: identity.into(),
            kind: ExecutionNodeKind::InlineModel,
            payload_ref: format!("payload:{identity}"),
            executor_kind: "history-test".into(),
            idempotency_key: format!("{identity}:attempt"),
            lease_ref: None,
            acceptance: Default::default(),
            retry_policy: Default::default(),
            resource_scopes: Vec::new(),
            work: None,
        });
    }
    let graph = commits.register_graph(graph).unwrap().graph;
    (service, commits, graph)
}

fn start(
    service: &AgentActionService,
    commits: &ExecutionCommitService,
    graph: ExecutionGraph,
    node_id: &str,
) -> (ExecutionGraph, NodeExecutionTicket) {
    let graph = commits
        .transition_node(
            &graph,
            node_id,
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let node = graph.nodes.iter().find(|node| node.id == node_id).unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: node.executor_kind.clone(),
        ticket_idempotency_key: node.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let admitted = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node_id.into(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: 1,
        idempotency_key: node.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    let graph = commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([(node_id.into(), binding.clone())]),
            vec![admitted.clone()],
        )
        .unwrap()
        .graph;
    let (ticket, _) = ExecutionGraphStateStore::new(Arc::clone(&service.store))
        .binding_start_ticket(&graph, node_id, &binding)
        .unwrap();
    assert_eq!(ticket, admitted);
    (graph, ticket)
}

struct HistoryExecutor {
    status: ExecutionNodeStatus,
}

#[async_trait::async_trait]
impl NodeExecutor for HistoryExecutor {
    fn kind(&self) -> &str {
        "history-test"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: context.node.executor_kind,
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: self.status,
            result_ref: None,
            wait_class: None,
            summary: Some("historical source fixture".into()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        }))
    }
}

async fn completed_fixture() -> (AgentActionService, ExecutionCommitService, ExecutionGraph) {
    let (service, commits, graph, _directory, _runner) =
        executed_fixture(ExecutionNodeStatus::Completed).await;
    (service, commits, graph)
}

async fn executed_fixture(
    status: ExecutionNodeStatus,
) -> (
    AgentActionService,
    ExecutionCommitService,
    ExecutionGraph,
    tempfile::TempDir,
    ExecutionGraphRunner,
) {
    let (service, commits, graph) = registered();
    let directory = tempfile::tempdir().unwrap();
    let registry = Arc::new(NodeExecutorRegistry::new());
    registry
        .register(Arc::new(HistoryExecutor { status }))
        .unwrap();
    let state = ExecutionGraphStateStore::new(Arc::clone(&service.store));
    let runner = ExecutionGraphRunner::new(
        registry,
        state.clone(),
        commits.clone(),
        Arc::new(ExecutionResourceManager::new([])),
        Arc::new(ScopeLockManager::persistent(directory.path().join("scopes")).unwrap()),
        Arc::new(WorktreeLeaseManager::open(directory.path().join("worktrees.json")).unwrap()),
        "history-workspace",
        directory.path(),
    );
    runner.run_until_quiescent(&graph.id).await.unwrap();
    let graph = state.load(&graph.id).unwrap();
    assert!(graph
        .node_statuses
        .values()
        .all(|observed| *observed == status));
    (service, commits, graph, directory, runner)
}

fn validate(
    service: &AgentActionService,
    commits: &ExecutionCommitService,
    graph: &ExecutionGraph,
    expected: BTreeSet<String>,
) -> Result<BTreeMap<String, u64>, String> {
    let mut pinned = BTreeMap::new();
    service.validate_native_effect_history(graph, commits, expected, &mut |stream| {
        let head = service
            .store
            .stream_revision(stream)
            .map_err(|error| error.to_string())?;
        pinned.insert(stream.to_string(), head);
        Ok(head)
    })?;
    Ok(pinned)
}

#[tokio::test]
async fn r1_465_native_history_accepts_original_atomic_results_and_pins_exact_streams() {
    let (service, commits, graph) = completed_fixture().await;
    let state = ExecutionGraphStateStore::new(Arc::clone(&service.store));
    let expected = graph
        .nodes
        .iter()
        .map(|node| {
            let binding = state.current_binding(&graph, &node.id, 1).unwrap();
            let (ticket, _) = state
                .binding_start_ticket(&graph, &node.id, &binding)
                .unwrap();
            ExecutionNodeBinding::effect_stream_id(&ticket)
        })
        .collect::<BTreeSet<_>>();
    let cursor = service.store.durable_commit_cursor().unwrap();
    let pinned = validate(&service, &commits, &graph, expected.clone()).unwrap();
    assert_eq!(
        pinned,
        expected.into_iter().map(|stream| (stream, 2)).collect()
    );
    assert_eq!(service.store.durable_commit_cursor().unwrap(), cursor);
}

#[tokio::test]
async fn r1_465_native_history_rejects_unfinished_and_removed_or_changed_historical_nodes() {
    let (service, commits, graph) = registered();
    let (graph, _) = start(&service, &commits, graph, "first");
    let error = validate(&service, &commits, &graph, BTreeSet::new()).unwrap_err();
    assert!(
        error.contains("historical attempt is unfinished"),
        "{error}"
    );
    let (service, commits, graph) = completed_fixture().await;
    let cursor = service.store.durable_commit_cursor().unwrap();
    let mut removed = graph.clone();
    removed.nodes.retain(|node| node.id != "first");
    let error = validate(&service, &commits, &removed, BTreeSet::new()).unwrap_err();
    assert!(error.contains("unmodelled historical node"), "{error}");
    let mut changed = graph.clone();
    changed
        .nodes
        .iter_mut()
        .find(|node| node.id == "first")
        .unwrap()
        .payload_ref = "foreign".into();
    assert!(validate(&service, &commits, &changed, BTreeSet::new()).is_err());
    assert_eq!(service.store.durable_commit_cursor().unwrap(), cursor);
}

#[tokio::test]
async fn r1_465_native_history_rejects_unmodelled_effect_and_foreign_expected_attempt() {
    let (service, commits, graph) = completed_fixture().await;
    let state = ExecutionGraphStateStore::new(Arc::clone(&service.store));
    let binding = state.current_binding(&graph, "first", 1).unwrap();
    let (ticket, _) = state
        .binding_start_ticket(&graph, "first", &binding)
        .unwrap();
    let mut foreign = ticket.clone();
    foreign.attempt += 1;
    let expected = BTreeSet::from([ExecutionNodeBinding::effect_stream_id(&foreign)]);
    assert!(validate(&service, &commits, &graph, expected).is_err());
    let orphan_stream = format!(
        "{}orphan",
        ExecutionNodeBinding::effect_stream_prefix(&graph.id)
    );
    service
        .store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "orphan-465".into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: orphan_stream.clone(),
                expected_revision: 0,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: orphan_stream,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.intent".into(),
                    status: Some("inflight".into()),
                    actor: Some(ticket.executor_kind.clone()),
                    refs: Vec::new(),
                    payload: serde_json::to_value(&ticket).unwrap(),
                },
                schema_version: 1,
                idempotency_key: Some("orphan-465:intent".into()),
            }],
        })
        .unwrap();
    let cursor = service.store.durable_commit_cursor().unwrap();
    let error = validate(&service, &commits, &graph, BTreeSet::new()).unwrap_err();
    assert!(error.contains("unmodelled historical effect"), "{error}");
    assert_eq!(service.store.durable_commit_cursor().unwrap(), cursor);
}

#[tokio::test]
async fn r1_475_native_history_wait_then_real_cancel_preserves_original_receipts() {
    let (service, commits, graph, _directory, runner) =
        executed_fixture(ExecutionNodeStatus::WaitingInput).await;
    let state = ExecutionGraphStateStore::new(Arc::clone(&service.store));
    let mut receipts = Vec::new();
    for node in &graph.nodes {
        let binding = state.current_binding(&graph, &node.id, 1).unwrap();
        let (ticket, _) = state
            .binding_start_ticket(&graph, &node.id, &binding)
            .unwrap();
        let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
        receipts.push(
            service
                .store
                .event_by_idempotency_key(&stream, &format!("{}:receipt", ticket.idempotency_key))
                .unwrap()
                .unwrap(),
        );
    }
    let cancelled = runner
        .command(
            &graph.id,
            harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "withdraw original work".into(),
            },
        )
        .await
        .unwrap();
    assert!(cancelled
        .node_statuses
        .values()
        .all(|status| *status == ExecutionNodeStatus::Cancelled));
    let cursor = service.store.durable_commit_cursor().unwrap();
    assert_eq!(
        validate(&service, &commits, &cancelled, BTreeSet::new())
            .unwrap()
            .len(),
        2
    );
    for receipt in receipts {
        let current = service
            .store
            .event_by_idempotency_key(
                &receipt.stream_id,
                receipt.idempotency_key.as_deref().unwrap(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(current, receipt);
    }
    assert_eq!(service.store.durable_commit_cursor().unwrap(), cursor);
}

#[tokio::test]
async fn r1_475_native_history_missing_closure_rejects_even_without_os_handles() {
    let (service, commits, graph) = registered();
    let (running, ticket) = start(&service, &commits, graph, "first");
    let outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: None,
        wait_class: None,
        summary: None,
        evidence_refs: Vec::new(),
        failure: None,
        usage: Default::default(),
        finished_at_ms: 1,
    });
    let graph = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    let cursor = service.store.durable_commit_cursor().unwrap();
    let error = validate(&service, &commits, &graph, BTreeSet::new()).unwrap_err();
    assert!(
        error.contains("callback or physical closure missing"),
        "{error}"
    );
    assert_eq!(service.store.durable_commit_cursor().unwrap(), cursor);
}

struct HeldHistoryExecutor {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    cancelled: tokio::sync::watch::Sender<bool>,
    aborts: std::sync::Mutex<BTreeSet<String>>,
}

#[async_trait::async_trait]
impl NodeExecutor for HeldHistoryExecutor {
    fn kind(&self) -> &str {
        "history-test"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        HistoryExecutor {
            status: ExecutionNodeStatus::Completed,
        }
        .start(context)
        .await
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let mut cancelled = self.cancelled.subscribe();
        self.entered.send(()).unwrap();
        loop {
            let is_cancelled = *cancelled.borrow_and_update();
            if is_cancelled {
                break;
            }
            cancelled.changed().await.unwrap();
        }
        Err(NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason: "cancelled by original control owner".into(),
        })
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancelled.send_replace(true);
        Ok(())
    }

    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), NodeExecutorError> {
        self.aborts.lock().unwrap().insert(ticket.node_id.clone());
        Ok(())
    }
}

#[tokio::test]
async fn r1_475_native_history_real_cancel_closes_intent_only_without_inventing_receipt() {
    let (service, commits, graph) = registered();
    let directory = tempfile::tempdir().unwrap();
    let registry = Arc::new(NodeExecutorRegistry::new());
    let (entered, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let (cancelled, _) = tokio::sync::watch::channel(false);
    let executor = Arc::new(HeldHistoryExecutor {
        entered,
        cancelled,
        aborts: std::sync::Mutex::new(BTreeSet::new()),
    });
    registry.register(executor.clone()).unwrap();
    let state = ExecutionGraphStateStore::new(Arc::clone(&service.store));
    let runner = ExecutionGraphRunner::new(
        registry,
        state.clone(),
        commits.clone(),
        Arc::new(ExecutionResourceManager::new([])),
        Arc::new(ScopeLockManager::persistent(directory.path().join("scopes")).unwrap()),
        Arc::new(WorktreeLeaseManager::open(directory.path().join("worktrees.json")).unwrap()),
        "history-workspace",
        directory.path(),
    );
    let driver = tokio::spawn({
        let runner = runner.clone();
        let identity = graph.id.clone();
        async move { runner.run_until_quiescent(&identity).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for _ in 0..2 {
            observed.recv().await.unwrap();
        }
    })
    .await
    .unwrap();
    let running = state.load(&graph.id).unwrap();
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        runner.command(
            &graph.id,
            harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                expected_revision: running.revision,
                reason: "withdraw running original attempt".into(),
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let report = tokio::time::timeout(std::time::Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap();
    if let Err(error) = report {
        assert!(
            error
                .to_string()
                .contains("cancelled by original control owner"),
            "{error}"
        );
    }
    assert!(cancelled
        .node_statuses
        .values()
        .all(|status| *status == ExecutionNodeStatus::Cancelled));
    assert_eq!(
        *executor.aborts.lock().unwrap(),
        cancelled.nodes.iter().map(|node| node.id.clone()).collect()
    );
    let cursor = service.store.durable_commit_cursor().unwrap();
    let pinned = validate(&service, &commits, &cancelled, BTreeSet::new()).unwrap();
    assert_eq!(pinned.len(), 2);
    assert!(pinned.values().all(|head| *head == 1));
    for node in &cancelled.nodes {
        let binding = state.current_binding(&cancelled, &node.id, 1).unwrap();
        let (ticket, _) = state
            .binding_start_ticket(&cancelled, &node.id, &binding)
            .unwrap();
        assert!(service
            .store
            .event_by_idempotency_key(
                &ExecutionNodeBinding::effect_stream_id(&ticket),
                &format!("{}:receipt", ticket.idempotency_key),
            )
            .unwrap()
            .is_none());
    }
    assert_eq!(service.store.durable_commit_cursor().unwrap(), cursor);
}
