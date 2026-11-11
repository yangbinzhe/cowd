use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};
use harness_contract::context::{ObservedAcceptance, RequiredAcceptance};
use harness_contract::execution_graph::{
    DependencyPredicate, ExecutionDependencyPolicy, ExecutionEdge, ExecutionEdgeKind,
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
    ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
    ExecutionWorkContract, ExecutionWorkRole,
};
use harness_contract::{context::EvidenceAccessRef, reality::EvidenceRef};

use super::*;
use crate::execution_core::RuntimeCompileTarget;
use crate::runtime_event_store::{RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};
use tokio::sync::Notify;

fn test_graph(objective: impl Into<String>) -> ExecutionGraph {
    let graph = ExecutionGraph::new(objective);
    let graph_id = graph.id.clone();
    let task_id = format!("test-task:{graph_id}");
    graph.with_lineage(ExecutionGraphLineage {
        session_id: "test-session".to_string(),
        turn_id: format!("test-turn:{graph_id}"),
        root_task_id: task_id.clone(),
        task_id,
        generation: 1,
    })
}

struct TestExecutor {
    fail_nodes: Vec<String>,
    delay: Duration,
    running: AtomicUsize,
    max_running: AtomicUsize,
    recoveries: AtomicUsize,
    calls: Mutex<Vec<String>>,
    held_outcome: Option<(Arc<Notify>, Arc<Notify>)>,
}

impl TestExecutor {
    fn new(fail_nodes: Vec<String>, delay: Duration) -> Self {
        Self {
            fail_nodes,
            delay,
            running: AtomicUsize::new(0),
            max_running: AtomicUsize::new(0),
            recoveries: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
            held_outcome: None,
        }
    }

    fn with_held_outcome(mut self, entered: Arc<Notify>, release: Arc<Notify>) -> Self {
        self.held_outcome = Some((entered, release));
        self
    }
}

#[async_trait]
impl NodeExecutor for TestExecutor {
    fn kind(&self) -> &str {
        "test"
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.payload_ref.is_empty() {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "empty payload".to_string(),
            });
        }
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref.clone(),
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let current = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_running.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if let Some((entered, release)) = self.held_outcome.as_ref() {
            entered.notify_one();
            release.notified().await;
        }
        self.running.fetch_sub(1, Ordering::SeqCst);
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ticket.node_id.clone());
        if self.fail_nodes.contains(&ticket.node_id) {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "injected failure".to_string(),
            });
        }
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }

    async fn recover(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.recoveries.fetch_add(1, Ordering::SeqCst);
        Ok(ticket.clone())
    }
}

struct StartFailExecutor;

struct BindingIdentityExecutor {
    fault: usize,
    hits: AtomicUsize,
    starts: AtomicUsize,
    polls: AtomicUsize,
    cancels: AtomicUsize,
    calls: Mutex<Vec<String>>,
}

impl BindingIdentityExecutor {
    fn new(fault: usize) -> Self {
        Self {
            fault,
            hits: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl NodeExecutor for BindingIdentityExecutor {
    fn kind(&self) -> &str {
        "test"
    }
    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let mut ticket = NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: "test".into(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key.clone(),
            payload_ref: context.node.payload_ref.clone(),
        };
        if context.node.id == "bad" && self.fault != 0 {
            self.hits.fetch_add(1, Ordering::SeqCst);
            match self.fault {
                1 => ticket.attempt = 0,
                2 => ticket.idempotency_key.clear(),
                3 => ticket.executor_kind = "foreign".into(),
                4 => ticket.node_id = "foreign".into(),
                5 => ticket.graph_id = "foreign".into(),
                6 => ticket.payload_ref = "foreign".into(),
                7 => ticket.service_class = ExecutionServiceClass::Background,
                8 => {
                    return Err(NodeExecutorError::Start {
                        node_id: context.node.id,
                        reason: "start fault-hit".into(),
                    })
                }
                _ => {}
            }
        }
        Ok(ticket)
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.calls.lock().unwrap().push(ticket.node_id.clone());
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }
    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn recover(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(ticket.clone())
    }
}

#[tokio::test]
async fn r1_bounded_history_first_start_binding_and_effect_identity_are_checked() {
    for fault in 1..=10 {
        for width in [1, 2] {
            let temporary = tempfile::tempdir().unwrap();
            let events = Arc::new(RuntimeEventStore::for_test());
            let state = ExecutionGraphStateStore::new(events.clone());
            let commits = ExecutionCommitService::new(events.clone());
            let registry = Arc::new(NodeExecutorRegistry::new());
            let executor = Arc::new(BindingIdentityExecutor::new(fault));
            registry.register(executor.clone()).unwrap();
            let resources = Arc::new(ExecutionResourceManager::new([(
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 4, 4).unwrap(),
            )]));
            let runner = ExecutionGraphRunner::new(
                registry,
                state.clone(),
                commits.clone(),
                resources.clone(),
                Arc::new(
                    ScopeLockManager::persistent(temporary.path().join("scope-locks")).unwrap(),
                ),
                Arc::new(WorktreeLeaseManager::open(temporary.path().join("leases.json")).unwrap()),
                "binding-identity-workspace",
                temporary.path(),
            );
            let mut graph = test_graph("first start identity");
            let mut bad = node("bad");
            bad.kind = ExecutionNodeKind::Materialize;
            if width == 2 {
                let mut good = node("a-good");
                good.kind = ExecutionNodeKind::Materialize;
                graph.nodes.push(good);
            }
            graph.nodes.push(bad);
            let registered = runner.register(graph).await.unwrap();
            let mut ready = runner
                .pump_snapshot(&registered.id, &Default::default())
                .await
                .unwrap()
                .ready;
            ready.sort_by(|left, right| left.id.cmp(&right.id));
            if fault == 9 {
                let bad = registered
                    .nodes
                    .iter()
                    .find(|node| node.id == "bad")
                    .unwrap();
                let ticket = NodeExecutionTicket {
                    graph_id: registered.id.clone(),
                    node_id: bad.id.clone(),
                    executor_kind: bad.executor_kind.clone(),
                    service_class: registered.service_class,
                    attempt: 1,
                    idempotency_key: bad.idempotency_key.clone(),
                    payload_ref: bad.payload_ref.clone(),
                };
                let effect_stream = ExecutionNodeBinding::effect_stream_id(&ticket);
                events
                    .inject_content_transaction_fault(crate::AppendTransactionRequest {
                        transaction_id: "bad-existing-intent".into(),
                        expected_streams: vec![crate::ExpectedStreamRevision {
                            stream_id: effect_stream.clone(),
                            expected_revision: 0,
                        }],
                        events: vec![crate::RuntimeTransactionEventInput {
                            event: RuntimeEventInput {
                                stream_id: effect_stream,
                                scope: RuntimeEventScope::ExecutionNode,
                                kind: "execution.effect.intent".into(),
                                status: Some("inflight".into()),
                                actor: Some("test".into()),
                                refs: vec![crate::RuntimeEventRef {
                                    kind: "execution_node".into(),
                                    id: "bad".into(),
                                }],
                                payload: serde_json::json!({"negative":"corrupt intent"}),
                            },
                            schema_version: 1,
                            idempotency_key: Some("idempotency:bad:intent".into()),
                        }],
                    })
                    .unwrap();
            }
            if fault == 10 {
                let current = state.load(&registered.id).unwrap();
                let mut overflow = current.clone();
                overflow
                    .recovery_cursor
                    .node_attempts
                    .insert("bad".into(), u32::MAX);
                commits
                    .commit_recovery(&current, overflow, Vec::new(), Vec::new())
                    .unwrap();
            }
            let before = state.load(&registered.id).unwrap();
            let cursor = events.durable_commit_cursor().unwrap();
            let result = runner.prepare_ready_wave(&registered.id, ready).await;
            assert!(
                result.is_err(),
                "first-start fault-hit {fault}, width {width}"
            );
            if fault == 9 {
                assert!(result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("intent identity field missing"));
            }
            assert_eq!(
                executor.hits.load(Ordering::SeqCst),
                usize::from(fault != 10)
            );
            assert_eq!(
                executor.starts.load(Ordering::SeqCst),
                if fault == 10 { width - 1 } else { width }
            );
            assert_eq!(executor.polls.load(Ordering::SeqCst), 0);
            assert_eq!(executor.cancels.load(Ordering::SeqCst), 0);
            assert_eq!(state.load(&registered.id).unwrap(), before);
            assert_eq!(events.durable_commit_cursor().unwrap(), cursor);
            let counts = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
            assert_eq!(counts.active_leases, 0);
            assert_eq!(counts.queued_waiters, 0);
        }
    }
    let events = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(events.clone());
    let mut graph = test_graph("single and wave admission exactness");
    graph.nodes.push(node("one"));
    graph.nodes.push(node("two"));
    let mut graph = commits.register_graph(graph).unwrap().graph;
    for node_id in ["one", "two"] {
        graph = commits
            .transition_node(
                &graph,
                node_id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
    }
    let executor = BindingIdentityExecutor::new(0);
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(graph.clone()),
            node: graph.nodes[0].clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let before = events.durable_commit_cursor().unwrap();
    for field in ["attempt", "key", "executor"] {
        let mut bad = binding.clone();
        match field {
            "attempt" => bad.attempt = 0,
            "key" => bad.ticket_idempotency_key.clear(),
            _ => bad.executor_kind = "foreign".into(),
        }
        assert!(commits
            .bind_and_start_node(&graph, "one", bad.clone())
            .is_err());
        assert!(commits
            .bind_and_start_nodes(&graph, BTreeMap::from([("one".into(), bad)]), Vec::new())
            .is_err());
        assert_eq!(events.durable_commit_cursor().unwrap(), before);
    }
    for field in ["graph", "node", "attempt", "payload", "class", "duplicate"] {
        let mut bad = ticket.clone();
        match field {
            "graph" => bad.graph_id = "foreign".into(),
            "node" => bad.node_id = "two".into(),
            "attempt" => bad.attempt = 0,
            "payload" => bad.payload_ref = "foreign".into(),
            "class" => bad.service_class = ExecutionServiceClass::Background,
            _ => {}
        }
        let tickets = if field == "duplicate" {
            vec![bad.clone(), bad]
        } else {
            vec![bad]
        };
        assert!(commits
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([("one".into(), binding.clone())]),
                tickets
            )
            .is_err());
        assert_eq!(events.durable_commit_cursor().unwrap(), before);
    }
    let mut overflow = graph.clone();
    overflow
        .recovery_cursor
        .node_attempts
        .insert("one".into(), u32::MAX);
    assert!(commits
        .bind_and_start_node(&overflow, "one", binding.clone())
        .is_err());
    graph = commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([("one".into(), binding.clone())]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    let outcome = executor.poll_or_await(&ticket).await.unwrap();
    assert_eq!(executor.polls.load(Ordering::SeqCst), 1);
    let mut foreign = ticket.clone();
    foreign.node_id = "two".into();
    assert!(commits.commit_execution_effect(&foreign, &outcome).is_err());
    assert!(commits
        .transition_nodes(
            &graph,
            vec![super::commit_service::ExecutionTerminalCommit {
                node_id: "one".into(),
                outcome: outcome.clone(),
                effect_ticket: Some(foreign),
            }]
        )
        .is_err());
    let graph = commits
        .transition_execution_outcome(&graph, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    let mut retry = ticket.clone();
    retry.attempt = 2;
    assert!(matches!(
        commits.inspect_execution_effect(&retry).unwrap(),
        ExecutionEffectState::Fresh
    ));
    assert_ne!(
        ExecutionNodeBinding::effect_stream_id(&ticket),
        ExecutionNodeBinding::effect_stream_id(&retry)
    );
    assert_eq!(
        events
            .stream_revision(&ExecutionNodeBinding::effect_stream_id(&ticket))
            .unwrap(),
        2
    );
    let other = executor
        .start(NodeExecutionContext {
            graph: Arc::new(graph.clone()),
            node: graph.nodes[1].clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let running = commits
        .bind_and_start_node(
            &graph,
            "two",
            ExecutionNodeBinding {
                executor_kind: other.executor_kind.clone(),
                ticket_idempotency_key: other.idempotency_key.clone(),
                attempt: 1,
                resource_lease_refs: Vec::new(),
                scope_lease_ref: None,
                worktree_lease_ref: None,
            },
        )
        .unwrap()
        .graph;
    events
        .inject_content_transaction_fault(crate::AppendTransactionRequest {
            transaction_id: "receipt-only-negative".into(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: ExecutionNodeBinding::effect_stream_id(&other),
                expected_revision: 0,
            }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: ExecutionNodeBinding::effect_stream_id(&other),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.receipt".into(),
                    status: Some("completed".into()),
                    actor: Some("test".into()),
                    refs: vec![crate::RuntimeEventRef {
                        kind: "execution_node".into(),
                        id: "two".into(),
                    }],
                    payload: serde_json::json!({"negative":"receipt-only corrupt fact"}),
                },
                schema_version: 1,
                idempotency_key: Some(format!("{}:receipt", other.idempotency_key)),
            }],
        })
        .unwrap();
    assert!(
        commits
            .inspect_execution_effect(&other)
            .unwrap_err()
            .to_string()
            .contains("has no intent"),
        "receipt-only fault-hit"
    );
    let cursor = events.durable_commit_cursor().unwrap();
    assert!(commits
        .commit_execution_effect(&other, &NodeExecutionOutcome::new(completed_result("two")))
        .is_err());
    assert!(commits
        .transition_nodes(
            &running,
            vec![super::commit_service::ExecutionTerminalCommit {
                node_id: "two".into(),
                outcome: NodeExecutionOutcome::new(completed_result("two")),
                effect_ticket: Some(other.clone()),
            }]
        )
        .is_err());
    assert_eq!(events.durable_commit_cursor().unwrap(), cursor);
    events
        .inject_content_transaction_fault(crate::AppendTransactionRequest {
            transaction_id: "foreign-node-intent-negative".into(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: ExecutionNodeBinding::effect_stream_id(&other),
                expected_revision: 1,
            }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: ExecutionNodeBinding::effect_stream_id(&other),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.intent".into(),
                    status: Some("inflight".into()),
                    actor: Some("test".into()),
                    refs: vec![crate::RuntimeEventRef {
                        kind: "execution_node".into(),
                        id: "two".into(),
                    }],
                    payload: serde_json::to_value(&ticket).unwrap(),
                },
                schema_version: 1,
                idempotency_key: Some(format!("{}:intent", other.idempotency_key)),
            }],
        })
        .unwrap();
    assert!(
        commits
            .inspect_execution_effect(&other)
            .unwrap_err()
            .to_string()
            .contains("intent ticket mismatch"),
        "foreign intent actual lookup fault-hit"
    );
    let cursor = events.durable_commit_cursor().unwrap();
    assert!(commits
        .commit_execution_effect(&other, &NodeExecutionOutcome::new(completed_result("two")))
        .is_err());
    assert!(commits
        .transition_nodes(
            &running,
            vec![super::commit_service::ExecutionTerminalCommit {
                node_id: "two".into(),
                outcome: NodeExecutionOutcome::new(completed_result("two")),
                effect_ticket: Some(other),
            }]
        )
        .is_err());
    assert_eq!(events.durable_commit_cursor().unwrap(), cursor);
}

struct StartRaceExecutor {
    start_entered: Notify,
    release_start: Notify,
    poll_calls: AtomicUsize,
    cancel_calls: AtomicUsize,
    cancelled: AtomicBool,
}

struct ReplanRaceExecutor {
    poll_entered: Notify,
    release_poll: Notify,
    poll_calls: AtomicUsize,
    cancel_calls: AtomicUsize,
    fail_outcome: bool,
}

struct CancelNestedRunnerExecutor {
    runner: OnceLock<ExecutionGraphRunner>,
    poll_entered: Notify,
    release_poll: Notify,
    cancel_entered: Notify,
    release_cancel: Notify,
    nested_completed: AtomicBool,
}

struct PostCommitExecutor {
    after_commits: AtomicUsize,
    after_aborts: AtomicUsize,
    invalid_domain_event: bool,
    poll_error: bool,
}

struct BlockingPostCommitExecutor {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct PermanentPendingExecutor {
    cancel_calls: AtomicUsize,
}

#[async_trait]
impl NodeExecutor for PermanentPendingExecutor {
    fn kind(&self) -> &str {
        "permanent_pending"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        std::future::pending().await
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Executes a nested graph from a parent node. Runtime orchestration uses the
/// same pattern through ToolBatch, so this protects the Runner from holding
/// its coordination gate across re-entrant executor work.
struct ReentrantRunnerExecutor {
    runner: OnceLock<ExecutionGraphRunner>,
}

#[async_trait]
impl NodeExecutor for ReentrantRunnerExecutor {
    fn kind(&self) -> &str {
        "reentrant_runner"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:reentrant", context.node.idempotency_key),
            payload_ref: context.node.payload_ref.clone(),
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let runner = self
            .runner
            .get()
            .expect("runner is installed for reentrant executor");
        let mut nested = test_graph("nested execution from tool-like node");
        let mut nested_tool = node("nested-tool");
        nested_tool.resource_scopes = vec!["read:fixtures/shared.txt".to_string()];
        nested.nodes.push(nested_tool);
        runner
            .start(nested)
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }
}

#[async_trait]
impl NodeExecutor for CancelNestedRunnerExecutor {
    fn kind(&self) -> &str {
        "cancel_nested_runner"
    }

    fn supports_resumable_pause(&self) -> bool {
        false
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:cancel-nested", context.node.idempotency_key),
            payload_ref: context.node.payload_ref.clone(),
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.poll_entered.notify_one();
        self.release_poll.notified().await;
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }

    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.runner
            .get()
            .expect("runner is installed for nested cancellation")
            .start(test_graph("nested graph during parent cancellation"))
            .await
            .map_err(|error| NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        self.nested_completed.store(true, Ordering::SeqCst);
        self.cancel_entered.notify_one();
        self.release_cancel.notified().await;
        Ok(())
    }
}

struct PayloadScopedBackend {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl super::executors::ScopedNodeBackend for PayloadScopedBackend {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }
}

struct PayloadScopedResolver {
    payload_ref: String,
    backend: Arc<PayloadScopedBackend>,
}

impl super::executors::ScopedNodeBackendResolver for PayloadScopedResolver {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        (ticket.payload_ref == self.payload_ref)
            .then(|| Arc::clone(&self.backend) as Arc<dyn ScopedNodeBackend>)
    }
}

#[async_trait]
impl NodeExecutor for PostCommitExecutor {
    fn kind(&self) -> &str {
        "post_commit"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        if self.poll_error {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "preview stream failed before commit".to_string(),
            });
        }
        let mut outcome = NodeExecutionOutcome::new(completed_result(&ticket.node_id));
        if self.invalid_domain_event {
            outcome
                .domain_events
                .push(crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("side-effect:{}", ticket.node_id),
                        scope: crate::RuntimeEventScope::AgentDefinition,
                        kind: "test.side_effect".to_string(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::Value::Null,
                    },
                    idempotency_key: Some(format!("protected:{}", ticket.idempotency_key)),
                    schema_version: 1,
                });
        }
        Ok(outcome)
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.after_commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn after_abort(
        &self,
        _ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), NodeExecutorError> {
        self.after_aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for BlockingPostCommitExecutor {
    fn kind(&self) -> &str {
        "blocking_post_commit"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let mut successor = node("successor");
        successor.executor_kind = "test".to_string();
        Ok(
            NodeExecutionOutcome::new(completed_result(&ticket.node_id)).with_replan(
                ExecutionGraphReplan {
                    nodes: vec![successor.clone()],
                    edges: vec![ExecutionEdge {
                        from: ticket.node_id.clone(),
                        to: successor.id,
                        kind: ExecutionEdgeKind::DependsOn,
                    }],
                    reason: "post-commit coordination regression".to_string(),
                    framework_authored: false,
                },
            ),
        )
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for ReplanRaceExecutor {
    fn kind(&self) -> &str {
        "replan_race"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.poll_calls.fetch_add(1, Ordering::SeqCst);
        self.poll_entered.notify_one();
        self.release_poll.notified().await;
        let mut result = completed_result(&ticket.node_id);
        if self.fail_outcome {
            result.status = ExecutionNodeStatus::Failed;
        }
        Ok(
            NodeExecutionOutcome::new(result).with_replan(
                ExecutionGraphReplan {
                    nodes: vec![node("late-tool")],
                    edges: vec![ExecutionEdge {
                        from: ticket.node_id.clone(),
                        to: "late-tool".to_string(),
                        kind: ExecutionEdgeKind::DependsOn,
                    }],
                    reason: "late dynamic tool".to_string(),
                    framework_authored: false,
                },
            ),
        )
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        self.release_poll.notify_one();
        Ok(())
    }
}

impl StartRaceExecutor {
    fn new() -> Self {
        Self {
            start_entered: Notify::new(),
            release_start: Notify::new(),
            poll_calls: AtomicUsize::new(0),
            cancel_calls: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl NodeExecutor for StartRaceExecutor {
    fn kind(&self) -> &str {
        "start_race"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.start_entered.notify_one();
        self.release_start.notified().await;
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.poll_calls.fetch_add(1, Ordering::SeqCst);
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }

    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct EvidenceExecutor {
    evidence_id: Option<String>,
}

#[async_trait]
impl NodeExecutor for EvidenceExecutor {
    fn kind(&self) -> &str {
        "evidence_test"
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
            executor_kind: self.kind().to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let evidence_refs = self
            .evidence_id
            .as_ref()
            .map(|id| {
                vec![EvidenceAccessRef::durable(
                    EvidenceRef::observed("evidence", id),
                    format!("sha:{id}"),
                    1,
                    "text/plain",
                    format!("evidence://{id}"),
                    "workspace",
                )]
            })
            .unwrap_or_default();
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!("source-result:{}", ticket.node_id)),
            wait_class: None,
            summary: Some("source completed".to_string()),
            evidence_refs,
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        }))
    }
}

struct TerminalBackend {
    calls: Arc<AtomicUsize>,
}

struct TerminalResolver {
    graph_id: String,
    backend: Arc<TerminalBackend>,
}

impl super::executors::SynthesizeBackendResolver for TerminalResolver {
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn super::executors::SynthesizeBackend>> {
        (ticket.graph_id == self.graph_id)
            .then(|| Arc::clone(&self.backend) as Arc<dyn super::executors::SynthesizeBackend>)
    }
}

#[async_trait]
impl super::executors::SynthesizeBackend for TerminalBackend {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!("terminal:{}", ticket.graph_id)),
            wait_class: None,
            summary: Some("terminal completed".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        }))
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for StartFailExecutor {
    fn kind(&self) -> &str {
        "start_fail"
    }

    fn validate(&self, _node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Err(NodeExecutorError::Start {
            node_id: context.node.id,
            reason: "intentional start failure".to_string(),
        })
    }

    async fn poll_or_await(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        unreachable!("a failed start must never be polled")
    }
}

fn harness() -> (
    Arc<NodeExecutorRegistry>,
    ExecutionGraphStateStore,
    ExecutionCommitService,
) {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    (
        Arc::new(NodeExecutorRegistry::new()),
        ExecutionGraphStateStore::new(Arc::clone(&event_store)),
        ExecutionCommitService::new(event_store),
    )
}

fn test_runner(
    registry: Arc<NodeExecutorRegistry>,
    state: ExecutionGraphStateStore,
    commits: ExecutionCommitService,
) -> ExecutionGraphRunner {
    test_runner_with_agent_limit(registry, state, commits, 4)
}

fn test_runner_with_agent_limit(
    registry: Arc<NodeExecutorRegistry>,
    state: ExecutionGraphStateStore,
    commits: ExecutionCommitService,
    agent_limit: usize,
) -> ExecutionGraphRunner {
    let workspace_id = format!("test-{}", uuid::Uuid::new_v4());
    let leases = WorktreeLeaseManager::open(
        std::env::temp_dir()
            .join("cowd-execution-graph-tests")
            .join(&workspace_id)
            .join("leases.json"),
    )
    .expect("worktree leases");
    ExecutionGraphRunner::new(
        registry,
        state,
        commits,
        Arc::new(ExecutionResourceManager::new([
            (
                ExecutionResourceKind::Provider,
                ResourceQuota::new(1, 4, 8).expect("provider quota"),
            ),
            (
                ExecutionResourceKind::Agent,
                ResourceQuota::new(1, agent_limit, agent_limit).expect("agent quota"),
            ),
            (
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 4, 8).expect("tool quota"),
            ),
        ])),
        Arc::new(
            ScopeLockManager::persistent(
                std::env::temp_dir()
                    .join("cowd-execution-graph-tests")
                    .join(&workspace_id)
                    .join("scope-locks"),
            )
            .unwrap(),
        ),
        Arc::new(leases),
        workspace_id,
        std::env::temp_dir(),
    )
}

fn node(id: &str) -> ExecutionNodeSpec {
    ExecutionNodeSpec {
        id: id.to_string(),
        kind: ExecutionNodeKind::ToolBatch,
        payload_ref: format!("payload:{id}"),
        executor_kind: "test".to_string(),
        idempotency_key: format!("idempotency:{id}"),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: Vec::new(),
        work: None,
    }
}

fn agent_intent_payload(graph_id: &str, node_id: &str, deadline_at_ms: u64) -> String {
    serde_json::to_string(&harness_contract::agent::AgentTaskIntent {
        selected_agent_id: None,
        definition_ref: None,
        granted_capabilities: Vec::new(),
        principal_id: "test".to_string(),
        source_turn_id: format!("turn:{graph_id}"),
        run_id: format!("run:{graph_id}:{node_id}"),
        task_id: format!("task:{graph_id}"),
        root_task_id: format!("task:{graph_id}"),
        parent_task_id: None,
        session_id: "test-session".to_string(),
        mission_id: "test-mission".to_string(),
        team_id: None,
        graph_id: graph_id.to_string(),
        node_id: node_id.to_string(),
        attempt: 1,
        expected_graph_revision: 0,
        objective: format!("execute {node_id}"),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        acceptance: Vec::new(),
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: Vec::new(),
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "test".to_string(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            format!("budget:{graph_id}:{node_id}"),
            format!("agent:{node_id}"),
            "agent",
            1_000,
            deadline_at_ms,
            1,
        ),
        deadline_at_ms,
        managed_invocation: None,
        idempotency_key: format!("idempotency:{graph_id}:{node_id}"),
        agentic_binding: None,
    })
    .expect("serialize test AgentTask intent")
}

fn completed_result(id: &str) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some(format!("result:{id}")),
        wait_class: None,
        summary: Some(format!("result {id}")),
        evidence_refs: Vec::new(),
        failure: None,
        usage: Default::default(),
        finished_at_ms: 1,
    }
}

#[tokio::test]
async fn supervisor_runs_one_hundred_graphs_with_bounded_cross_key_parallelism() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(10)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        128,
        8,
        Duration::from_secs(2),
    ));

    let mut graph_ids = Vec::new();
    for index in 0..100 {
        let mut graph = test_graph(format!("parallel graph {index}"));
        graph.id = format!("parallel-graph-{index}");
        graph.nodes.push(node(&format!("node-{index}")));
        let receipt = supervisor
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        graph_ids.push(receipt.graph_id);
    }

    let mut waiters = tokio::task::JoinSet::new();
    for graph_id in graph_ids {
        let supervisor = Arc::clone(&supervisor);
        waiters.spawn(async move {
            supervisor
                .wait_for_quiescence(&graph_id)
                .await
                .expect("graph reaches quiescence");
        });
    }
    while let Some(result) = waiters.join_next().await {
        result.unwrap();
    }

    let max_running = executor.max_running.load(Ordering::SeqCst);
    assert!(
        max_running > 1,
        "different graph keys must run concurrently"
    );
    assert!(
        max_running <= 8,
        "supervisor concurrency must respect its configured upper bound"
    );
    assert_eq!(executor.calls.lock().unwrap().len(), 100);
    assert_eq!(supervisor.health().tracked_keys, 0);
    let shutdown = supervisor.shutdown().await;
    assert_eq!(shutdown.remaining_keys, 0);
    assert_eq!(shutdown.forced_aborts, 0);
}

#[tokio::test]
async fn resource_pressure_keeps_a_ready_graph_pump_alive_until_a_lease_releases() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(100)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner_with_agent_limit(
            registry,
            state.clone(),
            commits,
            1,
        )),
        8,
        2,
        Duration::from_secs(2),
    ));
    let deadline_at_ms = crate::tool_invocation::now_ms().saturating_add(5_000);
    let mut first = test_graph("first graph holds the only Agent lease");
    first.id = "resource-pressure-first".to_string();
    let mut first_node = node("first-agent");
    first_node.kind = ExecutionNodeKind::AgentTask;
    first_node.payload_ref = agent_intent_payload(&first.id, &first_node.id, deadline_at_ms);
    first.nodes.push(first_node);
    supervisor
        .submit_graph(
            first,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("first graph admitted");
    tokio::time::timeout(Duration::from_secs(1), async {
        while executor.running.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first graph acquires the Agent lease");

    let mut deferred = test_graph("second graph waits for the Agent lease");
    deferred.id = "resource-pressure-deferred".to_string();
    let deferred_id = deferred.id.clone();
    let mut deferred_node = node("deferred-agent");
    deferred_node.kind = ExecutionNodeKind::AgentTask;
    deferred_node.payload_ref =
        agent_intent_payload(&deferred.id, &deferred_node.id, deadline_at_ms);
    deferred.nodes.push(deferred_node);
    supervisor
        .submit_graph(
            deferred,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("deferred graph admitted");

    let report = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor.wait_for_quiescence(&deferred_id),
    )
    .await
    .expect("resource release wakes the deferred graph")
    .expect("deferred graph reaches quiescence");
    assert_eq!(report.completed, 1);
    assert_eq!(
        state
            .load(&deferred_id)
            .expect("deferred graph state")
            .node_statuses["deferred-agent"],
        ExecutionNodeStatus::Completed
    );
    supervisor.shutdown().await;
}

#[tokio::test]
async fn continuation_retry_returns_the_existing_root_to_the_execution_runner() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(Vec::new(), Duration::ZERO)))
        .expect("test executor");
    let runner = test_runner(registry, state.clone(), commits);
    let binding = crate::session_continuation::compile_continuation_binding(
        &crate::session_continuation::ContinuationCandidate {
            source_session_id: "continuation-session".to_string(),
            source_turn_id: "continuation-turn".to_string(),
            source_root_id: "continuation-source-root".to_string(),
            team_set_ref: "agentic_program:continuation-source-program".to_string(),
            delivery_revision: 7,
            result_refs: vec!["agentic_program:continuation-source-program".to_string()],
            handoff_id: None,
        },
        "continuation-ingress",
        7,
        harness_contract::turn::ContinuationAuthorization::Authorized,
        1,
    )
    .expect("binding");
    let mut first = test_graph("first continuation root");
    first.id = "continuation-existing-root".to_string();
    first.nodes.push(node("continuation-first-node"));
    first.continuation_binding = Some(binding.clone());
    let registered = runner.register(first).await.expect("first registration");
    assert_eq!(registered.id, "continuation-existing-root");

    let mut replay = test_graph("replayed continuation root");
    replay.id = "continuation-replay-must-not-exist".to_string();
    replay.nodes.push(node("continuation-replay-node"));
    replay.continuation_binding = Some(binding);
    let replayed = runner
        .register(replay)
        .await
        .expect("replayed registration");
    assert_eq!(replayed.id, "continuation-existing-root");
    assert!(state.load("continuation-replay-must-not-exist").is_err());
}

#[tokio::test]
async fn two_root_teams_overlap_through_real_supervisor_and_agent_resource_quota() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(75)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        8,
        2,
        Duration::from_secs(2),
    ));

    for index in 0..2 {
        let mut graph = test_graph(format!("root Team {index}"));
        graph.id = format!("root-team-overlap-{index}");
        let node_id = format!("root-team-agent-{index}");
        let mut agent = node(&node_id);
        agent.kind = ExecutionNodeKind::AgentTask;
        agent.payload_ref = serde_json::to_string(&harness_contract::agent::AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: None,
            granted_capabilities: Vec::new(),
            principal_id: "test".to_string(),
            source_turn_id: format!("root-team-turn-{index}"),
            run_id: format!("root-team-run-{index}"),
            task_id: format!("root-team-task-{index}"),
            root_task_id: format!("root-team-task-{index}"),
            parent_task_id: None,
            session_id: "root-team-session".to_string(),
            mission_id: "root-team-mission".to_string(),
            team_id: Some(format!("root-team-{index}")),
            graph_id: graph.id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            expected_graph_revision: 0,
            objective: format!("execute root Team {index}"),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "test".to_string(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                format!("root-team-budget-{index}"),
                format!("root-team-agent-{index}"),
                "agent",
                1_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            idempotency_key: format!("root-team-idempotency-{index}"),
            agentic_binding: None,
        })
        .expect("serialize AgentTaskIntent");
        graph.nodes.push(agent);
        supervisor
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("root Team admitted");
    }
    for index in 0..2 {
        supervisor
            .wait_for_quiescence(&format!("root-team-overlap-{index}"))
            .await
            .expect("root Team settles");
    }
    assert_eq!(
        executor.max_running.load(Ordering::SeqCst),
        2,
        "two independent root Teams must overlap when graph and Agent capacity allow it"
    );
    supervisor.shutdown().await;
}

#[tokio::test]
async fn durable_agent_deadline_cancels_permanent_branch_and_unblocks_finally() {
    let (registry, state, commits) = harness();
    let pending = Arc::new(PermanentPendingExecutor {
        cancel_calls: AtomicUsize::new(0),
    });
    registry.register(pending.clone()).unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("deadline keeps finally reachable");
    let graph_id = graph.id.clone();
    let deadline_at_ms = crate::tool_invocation::now_ms().saturating_add(50);
    let mut stuck = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        pending.kind(),
        agent_intent_payload(&graph_id, "stuck-agent", deadline_at_ms),
    );
    stuck.id = "stuck-agent".to_string();
    stuck.idempotency_key = "stuck-agent-attempt".to_string();
    let sibling = node("healthy-sibling");
    let mut finally = node("finally");
    finally.work = Some(harness_contract::execution_graph::ExecutionWorkContract {
        role: harness_contract::execution_graph::ExecutionWorkRole::Synthesize,
        dependency: harness_contract::execution_graph::ExecutionDependencyPolicy::Finally,
        ..harness_contract::execution_graph::ExecutionWorkContract::new(
            harness_contract::execution_graph::ExecutionWorkRole::Synthesize,
        )
    });
    graph.edges = vec![
        ExecutionEdge {
            from: stuck.id.clone(),
            to: finally.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: sibling.id.clone(),
            to: finally.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    graph.nodes = vec![stuck, sibling, finally];

    tokio::time::timeout(Duration::from_secs(1), runner.start(graph))
        .await
        .expect("durable deadline prevents a permanent branch hang")
        .expect("graph reaches quiescence");
    let graph = state.load(&graph_id).unwrap();
    assert_eq!(
        graph.node_statuses["stuck-agent"],
        ExecutionNodeStatus::Cancelled
    );
    assert_eq!(
        graph.node_results["stuck-agent"]
            .failure
            .as_ref()
            .map(|failure| failure.kind.as_str()),
        Some("execution_deadline_exceeded")
    );
    assert_eq!(
        graph.node_statuses["healthy-sibling"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        graph.node_statuses["finally"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(pending.cancel_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn evidence_ready_reviewer_runs_on_failed_facts_and_never_rewrites_predecessor() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("evidence ready reviewer");
    let graph_id = graph.id.clone();
    let mut source = node("source");
    source.kind = ExecutionNodeKind::AgentTask;
    let mut reviewer = node("reviewer");
    reviewer.work = Some(ExecutionWorkContract {
        role: ExecutionWorkRole::CrossCheck,
        dependency: ExecutionDependencyPolicy::EvidenceReady {
            predicate: DependencyPredicate::EvidenceReady {
                minimum: 1,
                required_fact_kinds: vec![
                    TerminalFactKind::ObservedEvidence,
                    TerminalFactKind::AcceptanceVerdict,
                ],
                accepted_execution_statuses: vec![ExecutionNodeStatus::Failed],
                accepted_acceptance_verdicts: vec![AcceptanceVerdict::Unsatisfied],
                require_committed_effect: false,
            },
            cancel_remaining: false,
        },
        ..ExecutionWorkContract::new(ExecutionWorkRole::CrossCheck)
    });
    graph.edges = vec![ExecutionEdge {
        from: "source".to_string(),
        to: "reviewer".to_string(),
        kind: ExecutionEdgeKind::DependsOn,
    }];
    graph.nodes = vec![source, reviewer];
    let graph = runner.register(graph).await.expect("registered");
    let graph = commits
        .transition_node(
            &graph,
            "source",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "source",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    commits
        .transition_node(
            &graph,
            "source",
            ExecutionNodeStatus::Failed,
            Some(ExecutionNodeResult {
                status: ExecutionNodeStatus::Failed,
                result_ref: Some("agent-return:run".to_string()),
                wait_class: None,
                summary: Some("implementer failed but evidence is durable".to_string()),
                evidence_refs: vec![EvidenceAccessRef::durable(
                    EvidenceRef::observed("evidence", "proof-1"),
                    "sha",
                    1,
                    "text/plain",
                    "evidence://proof-1",
                    "workspace",
                )],
                failure: None,
                usage: ExecutionUsage {
                    required_acceptance: RequiredAcceptance {
                        criteria: vec!["ok".to_string()],
                        evidence_obligations: Vec::new(),
                    },
                    observed_acceptance: ObservedAcceptance {
                        satisfied_criteria: Vec::new(),
                        observed_evidence: Vec::new(),
                        unresolved_obligation_ids: vec!["ok".to_string()],
                    },
                    acceptance_evaluation: Some(
                        harness_contract::acceptance::AcceptanceEvaluation {
                            evaluator_revision:
                                crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
                            contract_digest: "fixture-contract".to_string(),
                            receipt_set_digest: "fixture-receipts".to_string(),
                            derived_obligations: Vec::new(),
                            verdict: AcceptanceVerdict::Unsatisfied,
                        },
                    ),
                    ..ExecutionUsage::default()
                },
                finished_at_ms: 1,
            }),
            Vec::new(),
        )
        .unwrap();
    runner
        .run_until_quiescent(&graph_id)
        .await
        .expect("reviewer quiesces");
    let settled = state.load(&graph_id).unwrap();
    assert_eq!(settled.node_statuses["source"], ExecutionNodeStatus::Failed);
    assert_eq!(
        settled.node_statuses["reviewer"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        settled.node_results["source"].status,
        ExecutionNodeStatus::Failed
    );
}

#[tokio::test]
async fn supervisor_coalesces_same_key_wakes_and_reclaims_the_slot() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(50)));
    registry.register(executor.clone()).unwrap();
    let supervisor = Arc::new(crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        64,
        8,
        Duration::from_secs(2),
    ));
    let mut graph = test_graph("same key");
    graph.id = "same-key-graph".to_string();
    graph.nodes.push(node("same-key-node"));
    supervisor
        .submit_graph(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .unwrap();

    let mut waiters = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let supervisor = Arc::clone(&supervisor);
        waiters.spawn(async move {
            supervisor
                .wait_for_quiescence("same-key-graph")
                .await
                .expect("same key waiter");
        });
    }
    while let Some(result) = waiters.join_next().await {
        result.unwrap();
    }

    assert_eq!(executor.max_running.load(Ordering::SeqCst), 1);
    assert_eq!(
        executor.calls.lock().unwrap().as_slice(),
        &["same-key-node".to_string()]
    );
    assert_eq!(supervisor.health().tracked_keys, 0);
    supervisor.shutdown().await;
}

#[tokio::test]
async fn graph_host_returns_after_admission_before_slow_execution_finishes() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(200)));
    registry.register(executor).unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        8,
        1,
        Duration::from_secs(2),
    );
    let mut graph = test_graph("admission only");
    graph.id = "admission-only-graph".to_string();
    graph.nodes.push(node("slow-node"));
    let receipt = supervisor
        .submit_graph(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(receipt.graph_id, "admission-only-graph");
    let projection = supervisor.projection(&receipt.graph_id).await.unwrap();
    assert!(
        projection
            .nodes
            .iter()
            .any(|node| !node.status.is_terminal()),
        "admission must not wait for the slow node to finish"
    );
    supervisor
        .wait_for_quiescence(&receipt.graph_id)
        .await
        .unwrap();
    supervisor.shutdown().await;
}

#[tokio::test]
async fn supervisor_shutdown_cancels_owned_work_and_zeroes_owner_health() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        8,
        1,
        Duration::from_secs(2),
    );
    let started = Arc::new(Notify::new());
    let started_work = Arc::clone(&started);
    supervisor
        .spawn_owned(
            "shutdown-test",
            Box::pin(async move {
                started_work.notify_one();
                std::future::pending::<()>().await;
            }),
        )
        .await
        .unwrap();
    started.notified().await;

    let report = supervisor.shutdown().await;
    let owner = report.owners.get("shutdown-test").unwrap();
    assert_eq!(owner.active, 0);
    assert_eq!(owner.aborted, 1);
    assert_eq!(report.forced_aborts, 0);
    assert_eq!(report.remaining_keys, 0);
}

#[tokio::test]
async fn supervisor_reaps_aborted_join_handles_through_its_owned_reaper() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::with_limits(
        Arc::new(test_runner(registry, state, commits)),
        1,
        1,
        Duration::from_secs(2),
    );
    let worker = tokio::spawn(std::future::pending::<()>());

    supervisor.reap_join_handle("join-handle-reaper-test", worker);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if supervisor
                .health()
                .owners
                .get("join-handle-reaper-test")
                .is_some_and(|owner| owner.completed == 1 && owner.active == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted join handle is reaped");

    let report = supervisor.shutdown().await;
    let owner = report.owners.get("join-handle-reaper-test").unwrap();
    assert_eq!(owner.submitted, 1);
    assert_eq!(owner.completed, 1);
    assert_eq!(owner.active, 0);
    assert_eq!(report.forced_aborts, 0);
}

#[test]
fn compiler_exposes_the_three_canonical_bootstrap_targets() {
    let compiler = ExecutionGraphCompiler;
    for target in [
        RuntimeCompileTarget::InlineModel,
        RuntimeCompileTarget::EvidenceGraph,
        RuntimeCompileTarget::ExecutionGraph,
    ] {
        let graph = compiler
            .compile(ExecutionCompileRequest {
                objective: "solve".to_string(),
                payload_ref: "payload:turn".to_string(),
                target,
                resource_scopes: Vec::new(),
            })
            .expect("available target");
        assert!(!graph.nodes.is_empty());
    }
}

#[test]
fn conversation_compile_targets_have_distinct_initial_dags_and_constraints() {
    let compiler = ExecutionGraphCompiler;
    let compile = |target| {
        compiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: "turn".to_string(),
                payload_ref: "durable-turn-payload".to_string(),
                target,
                resource_scopes: vec!["write:src/lib.rs".to_string()],
            })
            .unwrap()
    };
    let inline = compile(RuntimeCompileTarget::InlineModel);
    let evidence = compile(RuntimeCompileTarget::EvidenceGraph);
    let execution = compile(RuntimeCompileTarget::ExecutionGraph);

    assert_eq!(inline.nodes.len(), 2);
    assert_eq!(evidence.nodes.len(), 3);
    assert_eq!(execution.nodes.len(), 3);
    for (graph, suffixes) in [
        (&inline, vec!["resource-constraint", "model"]),
        (
            &evidence,
            vec!["evidence-constraint", "resource-constraint", "model"],
        ),
        (
            &execution,
            vec!["policy-constraint", "resource-constraint", "model"],
        ),
    ] {
        let mut bound = graph.clone();
        compiler
            .bind_conversation_graph_identity(&mut bound, "ingress-graph", None, Some("request"))
            .unwrap();
        assert_eq!(bound.id, "ingress-graph");
        for (node, suffix) in bound.nodes.iter().zip(&suffixes) {
            assert_eq!(node.id, format!("ingress-graph:{suffix}"));
            assert_eq!(node.idempotency_key, format!("request:{suffix}"));
        }
        assert_eq!(bound.edges.len(), suffixes.len() - 1);
        for (edge, pair) in bound.edges.iter().zip(suffixes.windows(2)) {
            assert_eq!(edge.from, format!("ingress-graph:{}", pair[0]));
            assert_eq!(edge.to, format!("ingress-graph:{}", pair[1]));
            assert_eq!(edge.kind, ExecutionEdgeKind::DependsOn);
        }
        assert_eq!(
            bound
                .nodes
                .iter()
                .map(|node| &node.resource_scopes)
                .collect::<Vec<_>>(),
            graph
                .nodes
                .iter()
                .map(|node| &node.resource_scopes)
                .collect::<Vec<_>>()
        );
    }
    assert!(evidence.nodes[0]
        .acceptance
        .criteria
        .contains(&"evidence_read_before_synthesis".to_string()));
    for graph in [&inline, &evidence, &execution] {
        assert_eq!(
            graph
                .nodes
                .iter()
                .filter(|node| !node.resource_scopes.is_empty())
                .count(),
            1
        );
        assert!(graph.nodes.iter().any(|node| {
            node.kind == ExecutionNodeKind::Verify
                && node.executor_kind == super::executors::CompileTargetGuardExecutor::KIND
                && node.resource_scopes == ["write:src/lib.rs"]
                && node
                    .acceptance
                    .criteria
                    .contains(&"mutation_resources_must_be_leased".to_string())
        }));
        assert!(graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::InlineModel)
            .all(|node| node.resource_scopes.is_empty()));
        assert_eq!(
            graph.nodes.last().unwrap().kind,
            ExecutionNodeKind::InlineModel
        );
        assert_eq!(graph.edges.len(), graph.nodes.len() - 1);
        for (edge, pair) in graph.edges.iter().zip(graph.nodes.windows(2)) {
            assert_eq!(edge.from, pair[0].id);
            assert_eq!(edge.to, pair[1].id);
            assert_eq!(edge.kind, ExecutionEdgeKind::DependsOn);
        }
    }
}

#[test]
fn conversation_compilation_preserves_read_only_and_empty_delegation_boundaries() {
    for target in [
        RuntimeCompileTarget::InlineModel,
        RuntimeCompileTarget::EvidenceGraph,
        RuntimeCompileTarget::ExecutionGraph,
    ] {
        for scopes in [Vec::new(), vec!["read:input.txt".to_string()]] {
            let graph = ExecutionGraphCompiler
                .compile_conversation_turn(ExecutionCompileRequest {
                    objective: "inspect".into(),
                    payload_ref: "payload:turn".into(),
                    target,
                    resource_scopes: scopes.clone(),
                })
                .unwrap();
            let retained = graph
                .nodes
                .iter()
                .flat_map(|node| node.resource_scopes.iter().cloned())
                .collect::<Vec<_>>();
            assert_eq!(retained, scopes);
        }
    }
}

#[test]
fn compiler_applies_resource_scopes_only_to_side_effect_nodes() {
    let graph = ExecutionGraphCompiler
        .compile(ExecutionCompileRequest {
            objective: "update one file".to_string(),
            payload_ref: "payload:turn".to_string(),
            target: RuntimeCompileTarget::ExecutionGraph,
            resource_scopes: vec!["write:src/lib.rs".to_string(), "worktree:.".to_string()],
        })
        .expect("execution graph");

    for node in graph.nodes {
        if node.kind == ExecutionNodeKind::ToolBatch {
            assert_eq!(node.resource_scopes, ["write:src/lib.rs", "worktree:."]);
        } else {
            assert!(node.resource_scopes.is_empty());
        }
    }
}

#[test]
fn registry_rejects_ambiguous_duplicate_executor_binding() {
    let registry = NodeExecutorRegistry::new();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    assert!(matches!(
        registry.register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        ))),
        Err(NodeExecutorError::DuplicateExecutor(kind)) if kind == "test"
    ));
}

#[tokio::test]
async fn executor_start_failure_never_persists_running_or_binding() {
    let (registry, state, commits) = harness();
    registry.register(Arc::new(StartFailExecutor)).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("start failure");
    let mut failing = node("start-failure");
    failing.executor_kind = "start_fail".to_string();
    graph.nodes.push(failing);
    let graph_id = graph.id.clone();

    let report = runner
        .start(graph)
        .await
        .expect("start failure is isolated to the failing node");
    assert_eq!(report.failed, 1);
    let persisted = state.load(&graph_id).expect("persisted graph");
    assert_eq!(
        persisted.node_statuses["start-failure"],
        ExecutionNodeStatus::Failed
    );
}

#[tokio::test]
async fn cancel_queued_during_start_commits_before_poll_and_prevents_side_effects() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(StartRaceExecutor::new());
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("cancel startup race");
    let mut race_node = node("race");
    race_node.executor_kind = "start_race".to_string();
    graph.nodes.push(race_node);
    let graph_id = graph.id.clone();
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(&graph, "race", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;

    let run = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move { runner.run_until_quiescent(&graph_id).await })
    };
    executor.start_entered.notified().await;
    let command = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Cancel {
                        expected_revision: graph.revision + 1,
                        reason: "startup race".to_string(),
                    },
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    executor.release_start.notify_one();

    let cancelled = command.await.unwrap().unwrap();
    run.await.unwrap().unwrap();
    assert_eq!(
        cancelled.node_statuses["race"],
        ExecutionNodeStatus::Cancelled
    );
    assert!(executor.cancelled.load(Ordering::SeqCst));
    assert_eq!(executor.cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["race"],
        ExecutionNodeStatus::Cancelled
    );
}

#[tokio::test]
async fn aborted_cancel_releases_intent_and_nested_child_graph_can_progress() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(CancelNestedRunnerExecutor {
        runner: OnceLock::new(),
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
        cancel_entered: Notify::new(),
        release_cancel: Notify::new(),
        nested_completed: AtomicBool::new(false),
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    executor
        .runner
        .set(runner.clone())
        .unwrap_or_else(|_| panic!("runner is installed once"));

    let mut graph = test_graph("abort cancellation intent");
    let mut candidate = node("cancel-probe");
    candidate.executor_kind = executor.kind().to_string();
    graph.nodes.push(candidate);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();

    let cancel = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Cancel {
                        expected_revision: running.revision,
                        reason: "abort command future".to_string(),
                    },
                )
                .await
        })
    };
    executor.cancel_entered.notified().await;
    assert!(
        executor.nested_completed.load(Ordering::SeqCst),
        "executor.cancel must advance a nested graph without the parent coordination lock"
    );

    // Make the parent result wait on the in-flight command intent, then abort
    // that command. CommandIntentOwner::drop must wake the old waiter.
    executor.release_poll.notify_one();
    tokio::task::yield_now().await;
    cancel.abort();
    let _ = cancel.await;
    // Aborting the command waiter does not end the original cancel future.
    // Let that same physical operation finish before expecting a graph commit.
    assert_eq!(state.load(&graph_id).unwrap().node_statuses["cancel-probe"], ExecutionNodeStatus::Running);
    executor.release_cancel.notify_one();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("old intent waiter is released")
        .expect("run task joins")
        .expect("parent graph can finish after command abort");

    let completed = state.load(&graph_id).unwrap();
    assert_eq!(
        completed.node_statuses["cancel-probe"],
        ExecutionNodeStatus::Completed
    );
    let cancelled = tokio::time::timeout(
        Duration::from_secs(1),
        runner.command(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: completed.revision,
                reason: "subsequent command".to_string(),
            },
        ),
    )
    .await
    .expect("subsequent command is not stuck")
    .expect("subsequent command is accepted");
    assert_eq!(
        cancelled.node_statuses["cancel-probe"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn active_non_resumable_executor_rejects_pause_without_cancelling() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(CancelNestedRunnerExecutor {
        runner: OnceLock::new(),
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
        cancel_entered: Notify::new(),
        release_cancel: Notify::new(),
        nested_completed: AtomicBool::new(false),
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    executor
        .runner
        .set(runner.clone())
        .unwrap_or_else(|_| panic!("runner is installed once"));

    let mut graph = test_graph("non-resumable active pause");
    let mut candidate = node("agent-like");
    candidate.executor_kind = executor.kind().to_string();
    graph.nodes.push(candidate);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();
    assert!(matches!(
        runner
            .command(
                &graph_id,
                ExecutionGraphCommand::Pause {
                    expected_revision: running.revision,
                    reason: "unsupported active pause".to_string(),
                },
            )
            .await,
        Err(ExecutionRunnerError::Commit(
            ExecutionCommitError::InvalidCommand(_)
        ))
    ));
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["agent-like"],
        ExecutionNodeStatus::Running
    );
    executor.release_poll.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn pause_queued_during_start_commits_before_poll_and_prevents_side_effects() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(StartRaceExecutor::new());
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("pause startup race");
    let mut race_node = node("race");
    race_node.executor_kind = "start_race".to_string();
    graph.nodes.push(race_node);
    let graph_id = graph.id.clone();
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(&graph, "race", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;

    let run = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move { runner.run_until_quiescent(&graph_id).await })
    };
    executor.start_entered.notified().await;
    let command = {
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        tokio::spawn(async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Pause {
                        expected_revision: graph.revision + 1,
                        reason: "startup race".to_string(),
                    },
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    executor.release_start.notify_one();

    let paused = command.await.unwrap().unwrap();
    run.await.unwrap().unwrap();
    assert_eq!(paused.node_statuses["race"], ExecutionNodeStatus::Paused);
    assert!(executor.cancelled.load(Ordering::SeqCst));
    assert_eq!(executor.cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["race"],
        ExecutionNodeStatus::Paused
    );
}

#[test]
fn worktree_scope_rejects_absolute_parent_and_symlink_escape() {
    let workspace = tempfile::tempdir().unwrap();
    let child = workspace.path().join("child");
    std::fs::create_dir(&child).unwrap();
    assert_eq!(
        validate_worktree_path(workspace.path(), "child").unwrap(),
        child.canonicalize().unwrap()
    );
    assert!(validate_worktree_path(workspace.path(), "/tmp").is_err());
    assert!(validate_worktree_path(workspace.path(), "../outside").is_err());

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/tmp", workspace.path().join("escape")).unwrap();
        assert!(validate_worktree_path(workspace.path(), "escape").is_err());
    }
}

#[test]
fn terminal_replan_is_one_transaction_and_survives_projection_restart() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("atomic terminal replan");
    graph.nodes.push(node("model"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "model",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "model",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let before_revision = graph.revision;
    let added = node("tool");
    let receipt = commits
        .transition_node_with_replan(
            &graph,
            "model",
            completed_result("model"),
            Vec::new(),
            vec![added],
            vec![ExecutionEdge {
                from: "model".to_string(),
                to: "tool".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            }],
            "model requested tool".to_string(),
            false,
        )
        .unwrap();

    assert_eq!(receipt.graph.revision, before_revision + 1);
    assert_eq!(receipt.transaction.event_ids.len(), 2);
    drop(commits);
    let projected = ExecutionGraphStateStore::new(event_store)
        .load(&graph.id)
        .unwrap();
    assert_eq!(projected.revision, before_revision + 1);
    assert_eq!(
        projected.node_statuses["model"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        projected.node_statuses["tool"],
        ExecutionNodeStatus::Planned
    );
    assert!(projected
        .nodes
        .iter()
        .any(|candidate| candidate.id == "tool"));
}

#[test]
fn malformed_session_terminal_cannot_commit_a_graph_without_an_outbox_row() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("terminal must be typed before atomic commit");
    graph.nodes.push(node("terminal"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let terminal_stream = format!("session-terminal:{}", graph.id);
    let invalid = crate::runtime_event_store::RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: terminal_stream.clone(),
            scope: RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".to_string(),
            status: Some("pending_delivery".to_string()),
            actor: Some("SynthesizeNodeExecutor".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"terminal_id":"missing_fields"}),
        },
        idempotency_key: Some("malformed-terminal".to_string()),
        schema_version: 1,
    };
    let revision = graph.revision;
    assert!(commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![invalid],
        )
        .is_err());
    assert_eq!(event_store.stream_revision(&graph.id).unwrap(), revision);
    assert_eq!(event_store.stream_revision(&terminal_stream).unwrap(), 0);
    assert!(event_store
        .session_terminal("missing_fields")
        .unwrap()
        .is_none());
}

#[test]
fn non_agentic_terminal_cannot_borrow_another_graphs_session() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("terminal Session belongs to Graph lineage");
    graph.nodes.push(node("terminal"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let terminal_stream = format!("session-terminal:{}", graph.id);
    let event = crate::runtime_event_store::RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: terminal_stream.clone(),
            scope: RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".into(),
            status: Some("pending_delivery".into()),
            actor: Some("SynthesizeNodeExecutor".into()),
            refs: vec![],
            payload: serde_json::json!({
                "terminal_id": "terminal:foreign-session",
                "message_id": "message:foreign-session",
                "session_id": "foreign-session",
                "execution_id": graph.id,
                "turn_id": graph.lineage.as_ref().unwrap().turn_id,
                "request_id": "request:foreign-session",
                "session_generation": 1,
                "input_sequence": 1,
                "input_claim_owner": "claim:owner",
                "input_claim_token": "claim:token",
                "input_claim_revision": 1,
                "payload_ref": "artifact://foreign-session",
            }),
        },
        idempotency_key: Some("terminal:foreign-session".into()),
        schema_version: 1,
    };
    let revision = graph.revision;
    let rejected = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![event.clone()],
        )
        .err()
        .expect("foreign Session cannot be committed by this Graph");
    assert!(rejected
        .to_string()
        .contains("Session terminal does not match its Graph, Session and Turn"));
    assert_eq!(event_store.stream_revision(&graph.id).unwrap(), revision);
    assert_eq!(event_store.stream_revision(&terminal_stream).unwrap(), 0);
    assert!(event_store
        .session_terminal("terminal:foreign-session")
        .unwrap()
        .is_none());
    let mut local = event;
    local.event.payload["terminal_id"] = serde_json::json!("terminal:local-session");
    local.event.payload["message_id"] = serde_json::json!("message:local-session");
    local.event.payload["session_id"] = serde_json::json!("test-session");
    local.event.payload["request_id"] = serde_json::json!("request:local-session");
    local.idempotency_key = Some("terminal:local-session".into());
    local.event.payload["controlled_recovery_claim_fingerprints"] = serde_json::json!([]);
    let terminal = prepare_graph_terminal_body(&event_store, &mut local);
    let terminal_payload = local.event.payload.clone();
    let request = crate::AppendTransactionRequest {
        transaction_id: "local-terminal-closed-matrix".into(),
        expected_streams: vec![crate::ExpectedStreamRevision {
            stream_id: terminal_stream.clone(),
            expected_revision: 0,
        }],
        events: vec![local.clone()],
    };
    let cursor = event_store.durable_commit_cursor().unwrap();
    let mut rejected_count = 0;
    for field in [
        "session_generation",
        "input_sequence",
        "input_claim_owner",
        "input_claim_token",
        "input_claim_revision",
        "payload_ref",
        "controlled_recovery_claim_fingerprints",
        "request_id",
        "execution_id",
        "turn_id",
    ] {
        for replacement in [
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::json!("foreign")),
        ] {
            let mut changed = request.clone();
            let object = changed.events[0].event.payload.as_object_mut().unwrap();
            match replacement {
                Some(value) => {
                    object.insert(field.into(), value);
                }
                None => {
                    object.remove(field);
                }
            }
            assert!(
                event_store
                    .append_transaction_with_terminal(changed, terminal.clone())
                    .is_err(),
                "{field}"
            );
            rejected_count += 1;
            assert_eq!(event_store.durable_commit_cursor().unwrap(), cursor);
            assert!(event_store
                .session_terminal(&terminal.terminal_id)
                .unwrap()
                .is_none());
        }
    }
    let mut extra = request;
    extra.events[0].event.payload["unexpected"] = serde_json::json!(true);
    assert!(event_store
        .append_transaction_with_terminal(extra, terminal.clone())
        .is_err());
    assert_eq!(rejected_count, 30);
    let receipt = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![local.clone()],
        )
        .expect("same Graph and Session may commit a fenced terminal");
    let duplicate = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![local],
        )
        .unwrap();
    assert!(duplicate.transaction.duplicate);
    assert_eq!(
        duplicate.transaction.commit_cursor,
        receipt.transaction.commit_cursor
    );
    assert_eq!(
        event_store.list_stream(&terminal_stream).unwrap()[0].payload,
        terminal_payload
    );
    assert!(event_store
        .session_terminal("terminal:local-session")
        .unwrap()
        .is_some());
}

fn prepare_graph_terminal_body(
    events: &Arc<RuntimeEventStore>,
    input: &mut crate::RuntimeTransactionEventInput,
) -> crate::SessionTerminalInput {
    let mut terminal: crate::SessionTerminalInput =
        serde_json::from_value(input.event.payload.clone()).unwrap();
    let root = std::path::PathBuf::from(
        std::env::var_os("COWD_R1_06_FIXTURE_ROOT").expect("explicit private plan fixture root"),
    )
    .join(format!("terminal-matrix-{}", uuid::Uuid::new_v4()));
    assert!(
        root.is_absolute()
            && root.starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
    );
    let artifacts = Arc::new(
        crate::ArtifactStore::for_test(&root, crate::ArtifactStoreConfig::default()).unwrap(),
    );
    let origin = crate::PreparedArtifactOrigin::TerminalBody {
        terminal_id: terminal.terminal_id.clone(),
        session_id: terminal.session_id.clone(),
        execution_id: terminal.execution_id.clone().unwrap(),
        turn_id: terminal.turn_id.clone().unwrap(),
        event_stream: input.event.stream_id.clone(),
        event_key: input.idempotency_key.clone().unwrap(),
    };
    let bytes = serde_json::to_vec(&serde_json::json!({"schema_version":crate::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,"text":"current fenced terminal"})).unwrap();
    let reference = artifacts
        .write_pre_pinned_with_media_blocking(
            &format!("session:{}", terminal.session_id),
            &terminal.terminal_id,
            "application/vnd.cowd.session-terminal+json",
            &bytes,
            &origin,
        )
        .unwrap();
    events.bind_artifact_store(artifacts).unwrap();
    terminal.payload_ref = crate::encode_session_terminal_artifact_ref(&reference).unwrap();
    input.event.payload = serde_json::to_value(&terminal).unwrap();
    terminal
}

#[test]
fn two_session_terminals_cannot_share_one_graph_transition() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("one terminal per graph transition");
    graph.nodes.push(node("terminal"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let terminal_events = ["first", "second"]
        .into_iter()
        .map(
            |name| crate::runtime_event_store::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("session-terminal:{name}"),
                    scope: RuntimeEventScope::SessionInput,
                    kind: "runtime.session.terminal_requested".to_string(),
                    status: Some("pending_delivery".to_string()),
                    actor: Some("SynthesizeNodeExecutor".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({
                        "terminal_id": format!("terminal:{name}"),
                        "message_id": format!("message:{name}"),
                        "session_id": "test-session",
                        "execution_id": graph.id,
                        "turn_id": graph.lineage.as_ref().unwrap().turn_id,
                        "request_id": format!("request:{name}"),
                        "session_generation": 1,
                        "input_sequence": 1,
                        "input_claim_owner": "claim:owner",
                        "input_claim_token": "claim:token",
                        "input_claim_revision": 1,
                        "controlled_recovery_claim_fingerprints": [],
                        "payload_ref": format!("artifact://{name}"),
                    }),
                },
                idempotency_key: Some(format!("terminal:{name}")),
                schema_version: 1,
            },
        )
        .collect();
    let revision = graph.revision;
    let rejected = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            terminal_events,
        )
        .err()
        .expect("multiple complete terminal inputs must be rejected");
    assert!(rejected
        .to_string()
        .contains("multiple Session terminal inputs"));
    assert_eq!(event_store.stream_revision(&graph.id).unwrap(), revision);
    assert!(event_store
        .session_terminal("terminal:first")
        .unwrap()
        .is_none());
    assert!(event_store
        .session_terminal("terminal:second")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn agentic_root_terminal_requires_real_a_verdict_and_refuses_supplied_verdict() {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
        MessagePublishInput,
    };

    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("A must exist before B");
    graph.nodes.push(node("terminal"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let lineage = graph.lineage.as_ref().unwrap();
    let objective_id =
        harness_contract::agent_action::root_objective_id(&lineage.session_id, &lineage.turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let actions = crate::AgentActionService::new(Arc::clone(&event_store));
    let observation = actions
        .apply(&AgentActionEnvelope {
            action_id: "open-root-program-before-b".into(),
            actor: AgentActorBinding {
                original_execution: None,
                objective_id,
                program_id: program_id.clone(),
                session_id: lineage.session_id.clone(),
                turn_id: lineage.turn_id.clone(),
                root_execution_id: Some(graph.id.clone()),
                required_team_count: 0,
                exact_team_count: None,
                objective_summary: "A must exist before B".into(),
                model_lease: "test".into(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: vec![],
                actor_id: "root".into(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            expected_revision: None,
            action: AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: format!("topic:{program_id}"),
                summary: Some("work began".into()),
                content_ref: None,
                refs: vec![],
                recipients: vec![],
                intent: None,
                issue_dispositions: vec![],
            }),
        })
        .unwrap();
    assert_eq!(observation.status, AgentActionStatus::Applied);
    let terminal_stream = format!("session-terminal:{}", graph.id);
    let terminal_event = |payload| crate::runtime_event_store::RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: terminal_stream.clone(),
            scope: RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".into(),
            status: Some("pending_delivery".into()),
            actor: Some("SynthesizeNodeExecutor".into()),
            refs: vec![],
            payload,
        },
        idempotency_key: Some("no-a-terminal".into()),
        schema_version: 1,
    };
    let payload = serde_json::json!({
        "terminal_id": "terminal:no-a",
        "message_id": "message:no-a",
        "session_id": lineage.session_id,
        "execution_id": graph.id,
        "turn_id": lineage.turn_id,
        "request_id": "request:no-a",
        "session_generation": 1,
        "input_sequence": 1,
        "input_claim_owner": "claim:owner",
        "input_claim_token": "claim:token",
        "input_claim_revision": 1,
        "payload_ref": "artifact://terminal",
    });
    let revision = graph.revision;
    let rejected = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![terminal_event(payload.clone())],
        )
        .err()
        .expect("root B without A must be rejected");
    assert!(rejected
        .to_string()
        .contains("no committed Objective verdict"));
    let mut forged = payload.clone();
    forged["objective_verdict"] = serde_json::json!({"kind":"satisfied"});
    let rejected = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![terminal_event(forged)],
        )
        .err()
        .expect("caller-supplied A must be rejected");
    assert!(rejected
        .to_string()
        .contains("cannot supply its own Objective verdict"));
    let mut complete_forgery = payload;
    complete_forgery["objective_verdict"] = serde_json::json!({
        "program_id":program_id,"program_stream_revision":1,"goal_stream_revision":1,
        "goal_spec_revision":1,"goal_spec_digest":"forged","a_commit_cursor":1,
        "proof_input_digests":[],"verdict":{"kind":"satisfied"}
    });
    let rejected = commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed_result("terminal")),
            vec![terminal_event(complete_forgery)],
        )
        .err()
        .unwrap();
    assert!(rejected
        .to_string()
        .contains("cannot supply its own Objective verdict"));
    assert_eq!(event_store.stream_revision(&graph.id).unwrap(), revision);
    assert_eq!(event_store.stream_revision(&terminal_stream).unwrap(), 0);
    assert!(event_store
        .session_terminal("terminal:no-a")
        .unwrap()
        .is_none());


}

#[test]
fn graph_admission_rejects_missing_canonical_business_lineage_without_panicking() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(event_store);
    let error = match commits.register_graph(ExecutionGraph::new("missing lineage")) {
        Ok(_) => panic!("unscoped graph must be rejected"),
        Err(error) => error,
    };

    assert!(matches!(error, ExecutionCommitError::InvalidCommand(_)));
    assert!(error.to_string().contains("canonical business lineage"));
}

#[tokio::test]
async fn cancel_wins_over_inflight_dynamic_replan_without_partial_graph_mutation() {
    // R02 collect-and-close positive control: a cancellation that arrives
    // while the original task is in flight stops further dispatch, collects
    // every fact that had happened by that point, and closes the node with a
    // named terminal state carrying that evidence - with no partial graph
    // mutation and no re-dispatch of the original attempt.
    let (registry, state, commits) = harness();
    let executor = Arc::new(ReplanRaceExecutor {
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
        poll_calls: AtomicUsize::new(0),
        cancel_calls: AtomicUsize::new(0),
        fail_outcome: false,
    });
    registry.register(executor.clone()).unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("command versus replan");
    let mut model = node("model");
    model.executor_kind = "replan_race".to_string();
    graph.nodes.push(model);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();
    assert_eq!(running.node_statuses["model"], ExecutionNodeStatus::Running);
    let cancelled = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: running.revision,
                reason: "operator superseded replan".to_string(),
            },
        )
        .await
        .unwrap();
    run.await.unwrap().unwrap();

    assert_eq!(
        cancelled.node_statuses["model"],
        ExecutionNodeStatus::Cancelled
    );
    let projected = state.load(&graph_id).unwrap();
    assert_eq!(
        projected.node_statuses["model"],
        ExecutionNodeStatus::Cancelled
    );
    // No partial graph mutation: the collected replan is never admitted and
    // the durable head moved exactly twice - one command transaction plus
    // the cancelled owner's lease-closure checkpoint, nothing else.
    assert!(!projected
        .nodes
        .iter()
        .any(|candidate| candidate.id == "late-tool"));
    assert!(!projected
        .edges
        .iter()
        .any(|edge| edge.to == "late-tool"));
    assert_eq!(cancelled.revision, running.revision + 2);
    // The named terminal state carries the collected facts: the original
    // computed result and the collected-unapplied dynamic replan, so the
    // durable answer holds the aggregation summary.
    let closure = cancelled
        .node_results
        .get("model")
        .expect("a collected cancellation closure must carry its node result");
    assert_eq!(closure.status, ExecutionNodeStatus::Cancelled);
    let summary = closure
        .summary
        .as_deref()
        .expect("the cancelled answer must carry the collected changes summary");
    assert!(summary.contains("collected"), "{summary}");
    assert!(summary.contains("Completed"), "{summary}");
    assert!(summary.contains("late-tool"), "{summary}");
    assert!(summary.contains("collected unapplied"), "{summary}");
    assert_eq!(
        closure.result_ref.as_deref(),
        Some("result:model"),
        "the collected fact keeps the original computed result reference"
    );
    // The same-ticket effect receipt closes with the closure: recovery and
    // every downstream reader consume one decision from one source.
    let attempt = *projected
        .recovery_cursor
        .node_attempts
        .get("model")
        .expect("collected cancellation keeps the original attempt");
    let binding = state
        .current_binding(&projected, "model", attempt)
        .expect("original binding");
    let ticket = NodeExecutionTicket {
        graph_id: graph_id.clone(),
        node_id: "model".to_string(),
        executor_kind: binding.executor_kind.clone(),
        service_class: projected.service_class,
        attempt: binding.attempt,
        idempotency_key: binding.ticket_idempotency_key.clone(),
        payload_ref: "payload:model".to_string(),
    };
    assert!(
        matches!(
            commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Completed(collected)
                if collected.result.status == ExecutionNodeStatus::Cancelled
        ),
        "the collected closure must commit the same-ticket effect receipt"
    );
    // Lease and retirement state is part of the collected facts: the binding
    // lease closure checkpoint is durable and no owner stays retained.
    let position = state
        .binding_start_position(&projected, "model", &binding)
        .expect("original binding start")
        .expect("collected cancellation keeps the original binding start");
    assert!(
        state
            .binding_lease_closure(&projected, "model", &binding, position)
            .expect("lease closure checkpoint"),
        "the collected cancellation must close the original binding leases"
    );
    assert!(!runner.has_retained_shutdown_owners().await);
    // No re-dispatch: the original attempt polled exactly once.
    assert_eq!(executor.poll_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.cancel_calls.load(Ordering::SeqCst), 1);

    // Negative control 1: a cancellation with no original facts at all keeps
    // the plain Cancelled semantics - status flip only, no aggregated result,
    // a single command revision.
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("cancel without facts");
    graph.nodes.push(node("idle"));
    let graph_id = graph.id.clone();
    runner.register(graph).await.unwrap();
    runner
        .pump_snapshot(&graph_id, &Default::default())
        .await
        .unwrap();
    let ready = state.load(&graph_id).unwrap();
    assert_eq!(ready.node_statuses["idle"], ExecutionNodeStatus::Ready);
    let cancelled = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: ready.revision,
                reason: "queued without facts".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(cancelled.node_statuses["idle"], ExecutionNodeStatus::Cancelled);
    assert!(
        cancelled.node_results.get("idle").is_none(),
        "a factless cancellation must not fabricate a collected result"
    );
    assert_eq!(cancelled.revision, ready.revision + 1);

    // Negative control 2: a finished outcome that only the original owner
    // can retire (a Failed result carrying a replan) stays unknown to this
    // command. The rejected command commits nothing: the graph is neither
    // half-written nor re-dispatched, and the original owner still settles
    // its single decision afterwards.
    let (registry, state, commits) = harness();
    let executor = Arc::new(ReplanRaceExecutor {
        poll_entered: Notify::new(),
        release_poll: Notify::new(),
        poll_calls: AtomicUsize::new(0),
        cancel_calls: AtomicUsize::new(0),
        fail_outcome: true,
    });
    registry.register(executor.clone()).unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("uncollectable versus replan");
    let mut model = node("model");
    model.executor_kind = "replan_race".to_string();
    graph.nodes.push(model);
    let graph_id = graph.id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.start(graph).await })
    };
    executor.poll_entered.notified().await;
    let running = state.load(&graph_id).unwrap();
    let rejected = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: running.revision,
                reason: "uncollectable outcome probe".to_string(),
            },
        )
        .await
        .expect_err("a Failed outcome with a replan belongs to its original owner");
    assert!(
        matches!(&rejected,
            ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { node_id, .. })
                if node_id == "model"),
        "{rejected}"
    );
    let intact = state.load(&graph_id).unwrap();
    assert_eq!(
        intact.revision, running.revision,
        "the rejected command must commit no partial mutation"
    );
    assert!(!intact
        .nodes
        .iter()
        .any(|candidate| candidate.id == "late-tool"));
    // The graph still converges through the original owner's own single
    // decision, and the cancelled probe never forced a re-dispatch.
    let report = run
        .await
        .unwrap()
        .expect("the original owner settles the graph after the unknown probe");
    assert_eq!(report.failed, 1);
    // The owner's own single decision adds the replan; its late successor is
    // blocked behind the Failed predecessor exactly like any other dependent
    // work - the probe committed nothing itself.
    assert_eq!(report.blocked, 1);
    let settled = state.load(&graph_id).unwrap();
    assert_eq!(settled.node_statuses["model"], ExecutionNodeStatus::Failed);
    assert_eq!(
        settled.node_statuses["late-tool"],
        ExecutionNodeStatus::Blocked
    );
    assert_eq!(executor.poll_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execution_graph_host_admits_and_supervisor_drives_the_same_graph() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let supervisor =
        crate::RuntimeExecutionSupervisor::new(Arc::new(test_runner(registry, state, commits)));
    let mut graph = test_graph("host submission");
    graph.nodes.push(node("host-node"));
    let graph_id = graph.id.clone();

    let (receipt, report) = supervisor
        .submit_and_wait(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("host submission");

    assert_eq!(receipt.graph_id, graph_id);
    let projection = supervisor.projection(&graph_id).await.unwrap();
    assert_eq!(projection.nodes[0].status, ExecutionNodeStatus::Completed);
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn nested_graph_submission_from_executor_never_deadlocks_runner_coordination() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(ReentrantRunnerExecutor {
        runner: OnceLock::new(),
    });
    registry.register(executor.clone()).unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    assert!(executor.runner.set(runner.clone()).is_ok());
    let mut graph = test_graph("parent submits nested graph");
    let mut node = node("orchestrate");
    node.kind = ExecutionNodeKind::AgentTask;
    node.executor_kind = "reentrant_runner".to_string();
    node.payload_ref = agent_intent_payload(&graph.id, &node.id, u64::MAX);
    node.resource_scopes = vec!["write:fixtures/shared.txt".to_string()];
    graph.nodes.push(node);
    let graph_id = graph.id.clone();

    let report = tokio::time::timeout(Duration::from_secs(1), runner.start(graph))
        .await
        .expect("nested graph submission must not wait on the parent coordination lock")
        .expect("parent graph completes");

    assert_eq!(report.completed, 1);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["orchestrate"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn rejects_missing_executor_before_persisting_graph() {
    let (registry, state, commits) = harness();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("missing executor");
    graph.nodes.push(node("missing"));
    let graph_id = graph.id.clone();

    assert!(matches!(
        runner.start(graph).await,
        Err(ExecutionRunnerError::Executor(
            NodeExecutorError::Unavailable { .. }
        ))
    ));
    assert!(matches!(
        state.load(&graph_id),
        Err(ExecutionStateStoreError::NotFound { .. })
    ));
}

#[tokio::test]
async fn executes_independent_ready_nodes_concurrently_then_dependency() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(30)));
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("parallel wave");
    graph.nodes = vec![node("a"), node("b"), node("join")]
        .into_iter()
        .map(|mut node| {
            let mut work = harness_contract::execution_graph::ExecutionWorkContract::new(
                harness_contract::execution_graph::ExecutionWorkRole::Tool,
            );
            work.expected_duration_ms = 30;
            node.work = Some(work);
            node
        })
        .collect();
    graph.edges = vec![
        ExecutionEdge {
            from: "a".to_string(),
            to: "join".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: "b".to_string(),
            to: "join".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    let graph_id = graph.id.clone();

    let report = runner.start(graph).await.expect("run graph");
    assert_eq!(report.completed, 3);
    assert!(executor.max_running.load(Ordering::SeqCst) >= 2);
    let calls = executor
        .calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let join_index = calls.iter().position(|node| node == "join").unwrap();
    assert!(calls.iter().position(|node| node == "a").unwrap() < join_index);
    assert!(calls.iter().position(|node| node == "b").unwrap() < join_index);
    let final_graph = state.load(&graph_id).unwrap();
    assert!(final_graph.recovery_cursor.commit_cursor > 0);
    let work = harness_contract::execution_graph::project_work_graph(&final_graph)
        .expect("work graph metrics");
    assert_eq!(work.width, 2);
    assert_eq!(work.depth, 2);
    assert!(work.actual_serial_ms >= 90);
    assert!(work.actual_critical_path_ms < work.actual_serial_ms);
    assert!(work
        .actual_speedup_basis_points
        .is_some_and(|value| value > 10_000));
}

#[tokio::test]
async fn failure_blocks_only_dependent_branch() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            vec!["fail".to_string()],
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("failure propagation");
    graph.nodes = vec![node("fail"), node("dependent"), node("independent")];
    graph.edges = vec![ExecutionEdge {
        from: "fail".to_string(),
        to: "dependent".to_string(),
        kind: ExecutionEdgeKind::DependsOn,
    }];
    let graph_id = graph.id.clone();

    let report = runner.start(graph).await.expect("run graph");
    assert_eq!(report.failed, 1);
    assert_eq!(report.blocked, 1);
    assert_eq!(report.completed, 1);
    let final_graph = state.load(&graph_id).unwrap();
    assert_eq!(
        final_graph.node_statuses,
        BTreeMap::from([
            ("dependent".to_string(), ExecutionNodeStatus::Blocked),
            ("fail".to_string(), ExecutionNodeStatus::Failed),
            ("independent".to_string(), ExecutionNodeStatus::Completed),
        ])
    );
}

#[tokio::test]
async fn isolation_rejects_foreign_attempt_before_touching_current_resource_lease() {
    let temporary = tempfile::tempdir().unwrap();
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(Vec::new(), Duration::ZERO)))
        .unwrap();
    let resources = Arc::new(ExecutionResourceManager::new([(
        ExecutionResourceKind::Tool,
        ResourceQuota::new(1, 1, 1).unwrap(),
    )]));
    let runner = ExecutionGraphRunner::new(
        registry,
        state.clone(),
        commits.clone(),
        resources.clone(),
        Arc::new(ScopeLockManager::persistent(temporary.path().join("scope-locks")).unwrap()),
        Arc::new(WorktreeLeaseManager::open(temporary.path().join("leases.json")).unwrap()),
        "isolation-ticket-workspace",
        temporary.path(),
    );
    let mut graph = test_graph("an old panic cannot release a current attempt");
    let mut work = node("current-attempt");
    work.kind = ExecutionNodeKind::Materialize;
    graph.nodes.push(work);
    let graph = runner.register(graph).await.unwrap();
    let ready = runner
        .pump_snapshot(&graph.id, &Default::default())
        .await
        .unwrap()
        .ready;
    let mut prepared = runner.prepare_ready_wave(&graph.id, ready).await.unwrap();
    assert_eq!(prepared.len(), 1);
    let prepared = prepared.pop().unwrap();
    let binding = prepared.binding.unwrap();
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: prepared.node.id,
        executor_kind: binding.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: binding.attempt,
        idempotency_key: binding.ticket_idempotency_key.clone(),
        payload_ref: prepared.node.payload_ref,
    };
    let before = state.load(&graph.id).unwrap();
    assert_eq!(
        before.node_statuses[&ticket.node_id],
        ExecutionNodeStatus::Running
    );
    assert_eq!(
        resources
            .snapshot(&ExecutionResourceKind::Tool)
            .unwrap()
            .active_leases,
        1
    );
    let mut foreign = binding.clone();
    foreign.attempt += 1;
    let rejected = runner
        .isolate_node_failure(
            &graph.id,
            &ticket.node_id,
            Some(&foreign),
            "foreign panic".into(),
        )
        .await;
    let mut wrong_lease = binding.clone();
    wrong_lease.resource_lease_refs = vec!["foreign-lease".into()];
    let rejected_lease = runner
        .isolate_node_failure(
            &graph.id,
            &ticket.node_id,
            Some(&wrong_lease),
            "foreign lease".into(),
        )
        .await;
    let after = state.load(&graph.id).unwrap();
    let retained = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
    runner
        .isolate_node_failure(
            &graph.id,
            &ticket.node_id,
            Some(&binding),
            "actual owner failure".into(),
        )
        .await
        .unwrap();
    let drained = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
    assert!(matches!(
        rejected,
        Err(ExecutionRunnerError::CommandSuperseded { .. })
    ));
    assert!(matches!(
        rejected_lease,
        Err(ExecutionRunnerError::CommandSuperseded { .. })
    ));
    assert_eq!(after, before);
    assert_eq!(retained.active_leases, 1);
    assert_eq!(drained.active_leases, 0);
    assert_eq!(drained.queued_waiters, 0);
    assert_eq!(
        state.load(&graph.id).unwrap().node_statuses[&ticket.node_id],
        ExecutionNodeStatus::Failed
    );
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
}

#[tokio::test]
async fn delayed_old_outcome_cannot_complete_or_release_a_resumed_attempt() {
    assert_resumed_attempt_ignores_old_response(OldAttemptResponse::HeldSuccess).await;
}

#[tokio::test]
async fn delayed_old_poll_error_cannot_fail_or_release_a_resumed_attempt() {
    assert_resumed_attempt_ignores_old_response(OldAttemptResponse::HeldFailure).await;
}

#[tokio::test(start_paused = true)]
async fn queued_old_terminal_wave_cannot_complete_or_release_a_resumed_attempt() {
    assert_resumed_attempt_ignores_old_response(OldAttemptResponse::QueuedTerminal).await;
}

#[tokio::test]
async fn old_prepared_node_cannot_poll_or_release_a_resumed_attempt() {
    assert_resumed_attempt_ignores_old_response(OldAttemptResponse::NotStarted).await;
}

enum OldAttemptResponse {
    HeldSuccess,
    HeldFailure,
    QueuedTerminal,
    NotStarted,
}

async fn assert_resumed_attempt_ignores_old_response(response: OldAttemptResponse) {
    let poll_fails = matches!(response, OldAttemptResponse::HeldFailure);
    let queued_terminal = matches!(response, OldAttemptResponse::QueuedTerminal);
    let not_started = matches!(response, OldAttemptResponse::NotStarted);
    let temporary = tempfile::tempdir().unwrap();
    let (registry, state, commits) = harness();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let failures = if poll_fails {
        vec!["resumed-node".to_string()]
    } else {
        Vec::new()
    };
    let executor = Arc::new(
        TestExecutor::new(failures, Duration::ZERO)
            .with_held_outcome(entered.clone(), release.clone()),
    );
    registry.register(executor.clone()).unwrap();
    let resources = Arc::new(ExecutionResourceManager::new([(
        ExecutionResourceKind::Tool,
        ResourceQuota::new(1, 1, 1).unwrap(),
    )]));
    let runner = ExecutionGraphRunner::new(
        registry,
        state.clone(),
        commits,
        resources.clone(),
        Arc::new(ScopeLockManager::persistent(temporary.path().join("scope-locks")).unwrap()),
        Arc::new(WorktreeLeaseManager::open(temporary.path().join("leases.json")).unwrap()),
        "late-response-workspace",
        temporary.path(),
    );
    let mut graph = test_graph("old response after original Pause and Resume");
    let mut work = node("resumed-node");
    work.kind = ExecutionNodeKind::Materialize;
    graph.nodes.push(work);
    let graph = runner.register(graph).await.unwrap();
    let ready = runner
        .pump_snapshot(&graph.id, &Default::default())
        .await
        .unwrap()
        .ready;
    let mut initial = runner.prepare_ready_wave(&graph.id, ready).await.unwrap();
    let old = initial.pop().unwrap();
    let old_binding = old.binding.clone().unwrap();
    let mut execution = Some(Box::pin({
        let runner = runner.clone();
        let graph_id = graph.id.clone();
        async move {
            runner
                .execute_prepared_pump_node(&graph_id, old, Arc::new(Notify::new()))
                .await
        }
    }));
    let pending = if not_started {
        None
    } else {
        Some(tokio::spawn(execution.take().unwrap()))
    };
    if !not_started {
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
    }
    if queued_terminal {
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while runner.pending_terminal_count(&graph.id).await == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(runner.pending_terminal_count(&graph.id).await, 1);
    }
    let running = state.load(&graph.id).unwrap();
    let paused = runner
        .command(
            &graph.id,
            ExecutionGraphCommand::Pause {
                expected_revision: running.revision,
                reason: "explicit operator pause".into(),
            },
        )
        .await
        .unwrap();
    runner
        .command(
            &graph.id,
            ExecutionGraphCommand::Resume {
                expected_revision: paused.revision,
            },
        )
        .await
        .unwrap();
    let ready = runner
        .pump_snapshot(&graph.id, &Default::default())
        .await
        .unwrap()
        .ready;
    let mut current = runner.prepare_ready_wave(&graph.id, ready).await.unwrap();
    let current = current.pop().unwrap();
    let current_binding = current.binding.clone().unwrap();
    assert!(current_binding.attempt > old_binding.attempt);
    assert_ne!(
        current_binding.resource_lease_refs,
        old_binding.resource_lease_refs
    );
    let before = state.load(&graph.id).unwrap();
    let held = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
    if !queued_terminal && !not_started {
        release.notify_one();
    }
    let outcome = match pending {
        Some(pending) => tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap(),
        None => tokio::time::timeout(Duration::from_secs(2), execution.take().unwrap())
            .await
            .unwrap(),
    };
    let after = state.load(&graph.id).unwrap();
    let retained = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
    let cleanup = runner
        .command(
            &graph.id,
            ExecutionGraphCommand::Cancel {
                expected_revision: after.revision,
                reason: "fixture owner cleanup".into(),
            },
        )
        .await;
    drop(current);
    drop(runner);
    let drained = resources.snapshot(&ExecutionResourceKind::Tool).unwrap();
    eprintln!(
        "V3_LATE_ATTEMPT_WITNESS {}",
        serde_json::json!({
            "poll_fails": poll_fails, "queued_terminal": queued_terminal, "not_started": not_started,
            "old_attempt": old_binding.attempt, "current_attempt": current_binding.attempt,
            "revision_before": before.revision, "revision_after": after.revision,
            "held_active": held.active_leases, "retained_active": retained.active_leases,
            "drained_active": drained.active_leases,
        })
    );
    assert_eq!(drained.active_leases, 0);
    assert_eq!(drained.queued_waiters, 0);
    cleanup.unwrap();
    assert_eq!(held.active_leases, 1);
    assert_eq!(
        after, before,
        "an old physical response cannot commit a current attempt outcome"
    );
    assert_eq!(retained.active_leases, 1);
    if not_started {
        assert!(executor.calls.lock().unwrap().is_empty());
    }
    outcome.unwrap();
}

#[tokio::test]
async fn pause_resume_and_cancel_are_revision_checked() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("commands");
    graph.nodes.push(node("a"));
    let graph_id = graph.id.clone();
    let mut graph = commits.register_graph(graph).unwrap().graph;
    graph = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;

    let paused = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Pause {
                expected_revision: graph.revision,
                reason: "operator".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(paused.node_statuses["a"], ExecutionNodeStatus::Paused);
    assert!(matches!(
        runner
            .command(
                &graph_id,
                ExecutionGraphCommand::Cancel {
                    expected_revision: graph.revision,
                    reason: "stale".to_string(),
                },
            )
            .await,
        Err(ExecutionRunnerError::Commit(
            ExecutionCommitError::StaleRevision { .. }
        ))
    ));
    let resumed = runner
        .command(
            &graph_id,
            ExecutionGraphCommand::Resume {
                expected_revision: paused.revision,
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.node_statuses["a"], ExecutionNodeStatus::Ready);
    runner.run_until_quiescent(&graph_id).await.unwrap();
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["a"],
        ExecutionNodeStatus::Completed
    );
}


#[test]
fn duplicate_transition_returns_original_transaction_without_duplicate_events() {
    let (_registry, state, commits) = harness();
    let mut graph = test_graph("idempotent commit");
    graph.nodes.push(node("a"));
    let graph = commits.register_graph(graph).unwrap().graph;

    let first = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap();
    let duplicate = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap();

    assert!(!first.transaction.duplicate);
    assert!(duplicate.transaction.duplicate);
    assert_eq!(
        first.transaction.commit_cursor,
        duplicate.transaction.commit_cursor
    );
    assert_eq!(state.load(&graph.id).unwrap().revision, 2);
}

fn r1_ref_fixture() -> (
    Arc<RuntimeEventStore>,
    Arc<crate::ArtifactStore>,
    ExecutionCommitService,
    ExecutionGraph,
) {
    let root = std::path::PathBuf::from(
        std::env::var_os("COWD_R1_06_FIXTURE_ROOT").expect("explicit private plan fixture root"),
    )
    .join(format!("graph-ref-{}", uuid::Uuid::new_v4()));
    assert!(
        root.is_absolute()
            && root.starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
    );
    let events = Arc::new(RuntimeEventStore::for_test());
    let artifacts = Arc::new(
        crate::ArtifactStore::for_test(
            &root,
            crate::ArtifactStoreConfig {
                gc_high_water_bytes: 1,
                gc_low_water_bytes: 0,
                orphan_grace_ms: 0,
                ..crate::ArtifactStoreConfig::default()
            },
        )
        .unwrap(),
    );
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let commits = ExecutionCommitService::new(events.clone());
    let mut graph = test_graph("large graph snapshot ".repeat(4096));
    graph.nodes.push(node("a"));
    let graph = commits.register_graph(graph).unwrap().graph;
    (events, artifacts, commits, graph)
}

#[test]
fn r1_event_replay_ref_write_retains_source_cas() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    let (events, artifacts, commits, graph) = r1_ref_fixture();
    let goals = crate::execution_core::GoalStore::new(events.clone());
    let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
        "id":"cas-goal","session_id":"test-session","objective":"preserve current source CAS",
        "criteria":[{"id":"checked","statement":"verified","status":"open"}],"phase":"execution","scope":"internal",
        "spec_revision":1,"spec_digest":"current","completion":"open","revision":1,"user_sequence":1
    })).unwrap();
    goals.create(goal).unwrap();
    let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
        "identity":{"workspace_id":"workspace","session_id":"test-session","graph_id":graph.id,"goal_id":"cas-goal","node_id":"a"},
        "kind":"tool_progress","source":"source-cas","source_revision":1,"freshness":{"observed_at_ms":1,"policy_revision":"current"},
        "summary":"large real observation ".repeat(4096),"fingerprint":"cas","result_class":"succeeded"
    })).unwrap();
    let prepared = goals
        .observation_event(&observation, "cas-observation".into())
        .unwrap();
    let artifact: harness_contract::context::ArtifactRef = serde_json::from_value(
        prepared.event.payload["observation_body"]["body"]["artifact"].clone(),
    )
    .unwrap();
    assert!(!artifacts
        .read_blocking(&artifact, "session:test-session", None)
        .unwrap()
        .is_empty());
    let mut newer = observation.clone();
    newer.source_revision = 2;
    newer.fingerprint = "interleaving".into();
    goals.record_observation(newer).unwrap();
    let source_head = events.stream_revision("goal:cas-goal").unwrap();
    assert!(commits
        .transition_node(
            &graph,
            "a",
            ExecutionNodeStatus::Ready,
            None,
            vec![prepared]
        )
        .is_err());
    assert_eq!(events.stream_revision(&graph.id).unwrap(), 1);
    assert_eq!(
        events.stream_revision("goal:cas-goal").unwrap(),
        source_head
    );
    let fresh = goals
        .observation_event(&observation, "cas-observation".into())
        .unwrap();
    commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, vec![fresh])
        .unwrap();
    assert_eq!(
        goals
            .projection("cas-goal")
            .unwrap()
            .unwrap()
            .progress
            .applied_observation_keys
            .len(),
        2
    );
    assert_eq!(
        ExecutionGraphStateStore::new(events)
            .load(&graph.id)
            .unwrap()
            .revision,
        2
    );
}

#[test]
fn r1_event_replay_current_retry_is_idempotent_and_rejects_legacy_transactions() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    let (events, _artifacts, commits, graph) = r1_ref_fixture();
    let first = commits
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap();
    let original = events.list_stream(&graph.id).unwrap();
    let duplicate = ExecutionCommitService::new(events.clone())
        .transition_node(&graph, "a", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap();
    assert!(duplicate.transaction.duplicate);
    assert_eq!(
        duplicate.transaction.commit_cursor,
        first.transaction.commit_cursor
    );
    assert_eq!(events.list_stream(&graph.id).unwrap(), original);
    let current = original.last().unwrap();
    let mut old = crate::RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: graph.id.clone(),
            scope: RuntimeEventScope::ExecutionGraph,
            kind: current.kind.clone(),
            status: current.status.clone(),
            actor: current.actor.clone(),
            refs: current.refs.clone(),
            payload: current.payload.clone(),
        },
        idempotency_key: current.idempotency_key.clone(),
        schema_version: 1,
    };
    assert!(events
        .append_batch_if_revision(graph.id.clone(), 2, "reject-old-schema", vec![old.clone()])
        .is_err());
    old.schema_version = 999;
    assert!(events
        .append_batch_if_revision(graph.id.clone(), 2, "reject-unknown-schema", vec![old])
        .is_err());
    assert_eq!(events.list_stream(&graph.id).unwrap(), original);
    assert_eq!(
        ExecutionGraphStateStore::new(events)
            .load(&graph.id)
            .unwrap()
            .revision,
        2
    );
}

#[test]
fn r1_content_gc_committed_ref_survives_crash_before_pin_promotion() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    let (events, artifacts, commits, graph) = r1_ref_fixture();
    let event = events.latest_for_stream(&graph.id).unwrap().unwrap();
    let reference: harness_contract::context::ArtifactRef =
        serde_json::from_value(event.payload["body"]["artifact"].clone()).unwrap();
    drop(commits);
    assert_eq!(artifacts.collect_garbage(100).unwrap().removed_objects, 0);
    let bytes = artifacts
        .read_blocking(&reference, "session:test-session", None)
        .unwrap();
    assert_eq!(bytes.len() as u64, reference.bytes);
    assert!(artifacts
        .delete(&reference, "session:test-session")
        .is_err());
    let recovered = ExecutionGraphStateStore::new(events.clone())
        .load(&graph.id)
        .unwrap();
    assert_eq!(recovered.objective, graph.objective);
    let retried = ExecutionCommitService::new(events)
        .transition_node(
            &recovered,
            "a",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap();
    assert_eq!(retried.graph.revision, 2);
    assert_eq!(artifacts.collect_garbage(100).unwrap().removed_objects, 0);
    assert_eq!(
        artifacts
            .read_blocking(&reference, "session:test-session", None)
            .unwrap(),
        bytes
    );
}

#[test]
fn r1_content_gc_prepared_failure_reconciles_real_source_cas() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    let (events, artifacts, commits, graph) = r1_ref_fixture();
    let goals = crate::execution_core::GoalStore::new(events.clone());
    let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
        "id":"reconcile-goal","session_id":"test-session","objective":"failed source CAS must release only unreachable prepared content",
        "criteria":[{"id":"checked","statement":"verified","status":"open"}],"phase":"execution","scope":"internal",
        "spec_revision":1,"spec_digest":"current","completion":"open","revision":1,"user_sequence":1
    })).unwrap();
    goals.create(goal).unwrap();
    let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
        "identity":{"workspace_id":"workspace","session_id":"test-session","graph_id":graph.id,"goal_id":"reconcile-goal","node_id":"a"},
        "kind":"tool_progress","source":"source-cas","source_revision":1,"freshness":{"observed_at_ms":1,"policy_revision":"current"},
        "summary":"discard only this failed observation ".repeat(4096),"fingerprint":"failed-cas","result_class":"succeeded"
    })).unwrap();
    let prepared = goals
        .observation_event(&observation, "failed-cas-observation".into())
        .unwrap();
    let reference: harness_contract::context::ArtifactRef = serde_json::from_value(
        prepared.event.payload["observation_body"]["body"]["artifact"].clone(),
    )
    .unwrap();
    let origin = crate::PreparedArtifactOrigin::EventBody {
        stream_id: "goal:reconcile-goal".into(),
        idempotency_key: "failed-cas-observation".into(),
        kind: "goal.observation".into(),
    };
    assert_eq!(
        events
            .resolve_content(
                "session:test-session",
                &prepared.event.payload["observation_body"],
                &origin
            )
            .unwrap(),
        serde_json::to_value(&observation).unwrap()
    );
    assert!(artifacts.resolve(&reference.selector).is_err());
    let source = prepared.event.payload["goal_source"]["sequence"]
        .as_u64()
        .unwrap();
    let mut newer = observation.clone();
    newer.source_revision = 2;
    newer.fingerprint = "committed-interleaving".into();
    goals.record_observation(newer).unwrap();
    let actual = events.stream_revision("goal:reconcile-goal").unwrap();
    assert!(actual > source);
    let error = commits
        .transition_node(
            &graph,
            "a",
            ExecutionNodeStatus::Ready,
            None,
            vec![prepared],
        )
        .err()
        .expect("the actual source CAS must reject this prepared write");
    assert!(
        matches!(error,ExecutionCommitError::EventStore(crate::RuntimeEventStoreError::StaleRevision { ref stream_id, expected, actual: current }) if stream_id == "goal:reconcile-goal" && expected == source && current == actual)
    );
    assert!(events
        .event_by_idempotency_key("goal:reconcile-goal", "failed-cas-observation")
        .unwrap()
        .is_none());
    assert_eq!(events.stream_revision(&graph.id).unwrap(), 1);
    let committed = events.latest_for_stream(&graph.id).unwrap().unwrap();
    let roots = events.quiesced_content_roots();
    drop(goals);
    drop(commits);
    drop(events);
    let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
    let report = roots
        .reconcile_prepared_batch(
            &mut artifacts,
            true,
            "ephemeral:real-cas",
            "quiesced-real-cas",
        )
        .unwrap();
    assert!(report.inventory_pass_complete);
    assert_eq!(report.blocked, 0);
    assert!(report.retired_records >= 1);
    assert!(report.referenced_canonical >= 1);
    assert!(artifacts
        .read_prepared_blocking(&reference, "session:test-session", &origin)
        .is_err());
    let original: harness_contract::context::ArtifactRef =
        serde_json::from_value(committed.payload["body"]["artifact"].clone()).unwrap();
    let original_origin = crate::PreparedArtifactOrigin::EventBody {
        stream_id: committed.stream_id,
        idempotency_key: committed.idempotency_key.unwrap(),
        kind: committed.kind,
    };
    assert!(!artifacts
        .read_prepared_blocking(&original, "session:test-session", &original_origin)
        .unwrap()
        .is_empty());
    assert!(
        artifacts
            .reconcile_quiesced_orphans(true, 100)
            .unwrap()
            .removed_objects
            >= 1
    );
    let retry = roots
        .reconcile_prepared_batch(
            &mut artifacts,
            true,
            "ephemeral:real-cas",
            "quiesced-real-cas",
        )
        .unwrap();
    assert!(retry.inventory_pass_complete);
    assert_eq!(retry.retired_records, 0);
}

#[test]
fn r1_content_gc_all_submission_facades_revalidate_retired_slots() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    let (events, artifacts, _commits, graph) = r1_ref_fixture();
    let goals = crate::execution_core::GoalStore::new(events.clone());
    let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
        "id":"facade-goal","session_id":"test-session","objective":"checkpoint owner ".repeat(2048),
        "criteria":[{"id":"checked","statement":"verified","status":"open"}],"phase":"execution","scope":"internal",
        "spec_revision":1,"spec_digest":"current","completion":"open","revision":1,"user_sequence":1
    })).unwrap();
    goals.create(goal).unwrap();
    let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
        "identity":{"workspace_id":"workspace","session_id":"test-session","graph_id":graph.id,"goal_id":"facade-goal","node_id":"a"},
        "kind":"tool_progress","source":"facade-test","source_revision":1,"freshness":{"observed_at_ms":1,"policy_revision":"current"},
        "summary":"full prepared body ".repeat(2048),"fingerprint":"facade-body","result_class":"succeeded"
    })).unwrap();
    let prepared = goals
        .observation_event(&observation, "facade-body".into())
        .unwrap();
    let reference: crate::ArtifactRef = serde_json::from_value(
        prepared.event.payload["observation_body"]["body"]["artifact"].clone(),
    )
    .unwrap();
    let record = artifacts
        .repository()
        .record(reference.selector.strip_prefix("artifact://").unwrap())
        .unwrap()
        .unwrap();
    let object = artifacts
        .repository()
        .object(&reference.sha256)
        .unwrap()
        .unwrap();
    assert_eq!(
        artifacts
            .repository()
            .retire_prepared(&crate::PreparedArtifactCandidate {
                record,
                generation: object.generation
            })
            .unwrap(),
        crate::PreparedArtifactRetirement::Removed
    );
    let request = crate::AppendTransactionRequest {
        transaction_id: "facade-revalidation".into(),
        expected_streams: vec![crate::ExpectedStreamRevision {
            stream_id: "goal:facade-goal".into(),
            expected_revision: 1,
        }],
        events: vec![prepared.clone()],
    };
    assert!(events.append(prepared.event.clone()).is_err());
    assert!(events.append_transaction(request.clone()).is_err());
    assert!(events.append_transaction_locked(request.clone()).is_err());
    assert!(events
        .append_batch_if_revision("goal:facade-goal", 1, "facade-batch", vec![prepared])
        .is_err());
    let terminal = crate::SessionTerminalInput {
        terminal_id: "facade-terminal".into(),
        message_id: "facade-message".into(),
        session_id: "test-session".into(),
        execution_id: Some(graph.id.clone()),
        turn_id: Some("facade-turn".into()),
        request_id: Some("facade-request".into()),
        session_generation: Some(1),
        input_sequence: Some(1),
        input_claim_owner: Some("facade-owner".into()),
        input_claim_token: Some("facade-token".into()),
        input_claim_revision: Some(1),
        controlled_recovery_claim_fingerprints: Vec::new(),
        payload_ref: crate::encode_session_terminal_artifact_ref(&reference).unwrap(),
    };
    assert!(events
        .append_transaction_with_terminal(request.clone(), terminal)
        .is_err());
    let lease = crate::security::test_verified_decision_lease(
        "facade-review",
        "facade-action",
        "facade-scope",
        "facade-evidence",
    );
    assert!(events
        .append_transaction_with_verified_decision_lease(request, &lease)
        .is_err());
    assert_eq!(events.stream_revision("goal:facade-goal").unwrap(), 1);
    assert!(events
        .session_terminal("facade-terminal")
        .unwrap()
        .is_none());
    let checkpoint = events
        .projection_checkpoint("goal-projection:facade-goal")
        .unwrap()
        .unwrap();
    let reference: crate::ArtifactRef =
        serde_json::from_value(checkpoint.payload["body"]["body"]["artifact"].clone()).unwrap();
    let record = artifacts
        .repository()
        .record(reference.selector.strip_prefix("artifact://").unwrap())
        .unwrap()
        .unwrap();
    let object = artifacts
        .repository()
        .object(&reference.sha256)
        .unwrap()
        .unwrap();
    assert_eq!(
        artifacts
            .repository()
            .retire_prepared(&crate::PreparedArtifactCandidate {
                record,
                generation: object.generation
            })
            .unwrap(),
        crate::PreparedArtifactRetirement::Removed
    );
    assert!(events
        .put_projection_checkpoint(
            &checkpoint.projection_id,
            checkpoint.source_cursor,
            &checkpoint.payload,
            0
        )
        .is_err());
    assert!(events
        .compare_and_put_projection_checkpoint(
            &checkpoint.projection_id,
            checkpoint.source_cursor,
            checkpoint.revision,
            &checkpoint.payload,
            0
        )
        .is_err());
    assert!(events
        .compare_and_repair_projection_checkpoint(
            &checkpoint.projection_id,
            checkpoint.source_cursor,
            checkpoint.revision,
            &checkpoint.payload,
            0
        )
        .is_err());
    assert_eq!(
        events
            .projection_checkpoint(&checkpoint.projection_id)
            .unwrap(),
        Some(checkpoint)
    );
}

#[tokio::test]
async fn recovery_requeues_retryable_running_node_and_preserves_waiting_approval() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::ZERO));
    registry.register(executor.clone()).unwrap();
    let mut graph = test_graph("unbound Running cannot be inferred never-started");
    let mut item = node("retryable");
    item.retry_policy.max_attempts = 2;
    graph.nodes = vec![item, node("approval")];
    let mut graph = commits.register_graph(graph).unwrap().graph;
    for id in ["retryable", "approval"] {
        graph = commits
            .transition_node(&graph, id, ExecutionNodeStatus::Ready, None, Vec::new())
            .unwrap()
            .graph;
        graph = commits
            .transition_node(&graph, id, ExecutionNodeStatus::Running, None, Vec::new())
            .unwrap()
            .graph;
    }
    graph = commits
        .transition_node(
            &graph,
            "approval",
            ExecutionNodeStatus::WaitingApproval,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let runner = test_runner(registry, state.clone(), commits);
    assert!(runner.recover_graph(&graph.id).await.is_err());
    assert_eq!(state.load(&graph.id).unwrap(), graph);
    assert_eq!(
        graph.node_statuses["approval"],
        ExecutionNodeStatus::WaitingApproval
    );
    assert_eq!(executor.recoveries.load(Ordering::SeqCst), 0);
    assert!(executor.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn restart_installs_fresh_scoped_resolver_and_executes_from_persistent_payload() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("resolver restart");
    let mut durable = node("durable");
    durable.executor_kind = "durable_scoped".to_string();
    graph.nodes.push(durable);
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let graph_id = graph.id.clone();
    drop(commits);
    let state = ExecutionGraphStateStore::new(Arc::clone(&event_store));
    let commits = ExecutionCommitService::new(event_store);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(super::executors::ScopedNodeExecutor::new("durable_scoped"));
    let calls = Arc::new(AtomicUsize::new(0));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: "payload:durable".to_string(),
        backend: Arc::new(PayloadScopedBackend {
            calls: Arc::clone(&calls),
        }),
    }));
    registry
        .register(executor as Arc<dyn NodeExecutor>)
        .unwrap();
    test_runner(registry.clone(), state.clone(), commits.clone())
        .recover_graph(&graph_id)
        .await
        .unwrap();
    test_runner(registry, state.clone(), commits)
        .run_until_quiescent(&graph_id)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.load(&graph_id).unwrap().node_statuses["durable"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn restart_replays_completed_effect_receipt_without_provider_or_tool_reexecution() {
    let (events, graph, ticket, original_calls) =
        committed_inline_attempt_450("completed atomic result restart").await;
    let state = ExecutionGraphStateStore::new(events.clone());
    let commits = ExecutionCommitService::new(events);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let backend = Arc::new(PayloadScopedBackend {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: ticket.payload_ref.clone(),
        backend: backend.clone(),
    }));
    registry.register(executor).unwrap();
    let runner = recovery_runner_475(registry, state.clone(), commits, &graph.id);
    let recovered = runner.recover_graph(&graph.id).await.unwrap();
    assert_eq!(recovered.nodes[0].payload_ref, "payload:durable");
    assert_eq!(
        recovered.node_statuses["durable"],
        ExecutionNodeStatus::Completed
    );
    runner.run_until_quiescent(&graph.id).await.unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(original_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_blocks_inflight_effect_without_receipt_as_typed_uncertain() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(BindingIdentityExecutor::new(0));
    registry.register(executor.clone()).unwrap();
    let mut graph = test_graph("uncertain intent cannot be rerun");
    graph.nodes.push(node("durable"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(graph.clone()),
            node: graph.nodes[0].clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let scopes = manual_scope_locks_475(&graph.id);
    let owner = acquire_fixture_owner_475(&scopes, &commits, &graph, "durable", &binding);
    let graph = commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([("durable".into(), binding)]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    drop(owner);
    let runner = recovery_runner_475(registry, state.clone(), commits.clone(), &graph.id);
    let error = runner.recover_graph(&graph.id).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("original outcome is not durably committed"));
    assert_eq!(state.load(&graph.id).unwrap(), graph);
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert_eq!(executor.polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn process_side_effect_hook_runs_only_after_graph_commit() {
    for (
        invalid_domain_event,
        poll_error,
        expected_run_ok,
        expected_after_commits,
        expected_after_aborts,
    ) in [
        (false, false, true, 1, 0),
        (true, false, false, 0, 1),
        // Poll failure is a failed-node report rather than Runner corruption,
        // but any already-streamed preview still has to be aborted.
        (false, true, true, 0, 1),
    ] {
        let (registry, state, commits) = harness();
        let executor = Arc::new(PostCommitExecutor {
            after_commits: AtomicUsize::new(0),
            after_aborts: AtomicUsize::new(0),
            invalid_domain_event,
            poll_error,
        });
        registry.register(executor.clone()).unwrap();
        let runner = test_runner(registry, state, commits);
        let mut graph = test_graph("post commit boundary");
        let mut terminal = node("terminal");
        terminal.executor_kind = "post_commit".to_string();
        graph.nodes.push(terminal);
        let result = runner.start(graph).await;
        assert_eq!(result.is_ok(), expected_run_ok, "{result:?}");
        assert_eq!(
            executor.after_commits.load(Ordering::SeqCst),
            expected_after_commits
        );
        assert_eq!(
            executor.after_aborts.load(Ordering::SeqCst),
            expected_after_aborts,
            "a preview-producing outcome that cannot commit must be explicitly aborted"
        );
    }
}

#[tokio::test]
async fn post_commit_callback_never_holds_graph_coordination_lock() {
    let (registry, state, commits) = harness();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    registry
        .register(Arc::new(BlockingPostCommitExecutor {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }))
        .unwrap();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = Arc::new(test_runner(registry, state.clone(), commits));
    let mut graph = test_graph("post commit coordination");
    let mut root = node("root");
    root.executor_kind = "blocking_post_commit".to_string();
    graph.nodes.push(root);
    let graph_id = graph.id.clone();
    let run = {
        let runner = Arc::clone(&runner);
        tokio::spawn(async move { runner.start(graph).await })
    };

    entered.notified().await;
    let revision = state.load(&graph_id).unwrap().revision;
    let command = runner.command(
        &graph_id,
        ExecutionGraphCommand::Pause {
            expected_revision: revision,
            reason: "exercise coordination while callback is pending".to_string(),
        },
    );
    let paused = tokio::time::timeout(Duration::from_millis(100), command)
        .await
        .expect("post-commit callback must not block graph command coordination")
        .expect("pause should commit while callback remains pending");
    assert!(paused.revision > revision);

    release.notify_one();
    let report = run.await.unwrap().unwrap();
    assert!(report.completed >= 1);
    assert!(state
        .load(&graph_id)
        .unwrap()
        .node_statuses
        .contains_key("successor"));
}

#[tokio::test]
async fn workspace_root_scope_is_a_valid_hierarchical_lock_scope() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state, commits);
    let mut graph = test_graph("workspace root scope");
    let mut root = node("root");
    root.resource_scopes = vec!["read:.".to_string()];
    graph.nodes.push(root);

    let report = runner.start(graph).await.unwrap();
    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
}

#[tokio::test]
async fn conflicting_effect_leaf_scopes_are_deferred_without_deadlocking_the_wave() {
    let (registry, state, commits) = harness();
    let executor = Arc::new(TestExecutor::new(Vec::new(), Duration::from_millis(20)));
    registry.register(executor.clone()).unwrap();
    let supervisor = crate::RuntimeExecutionSupervisor::new(Arc::new(test_runner(
        registry,
        state.clone(),
        commits,
    )));
    let mut graph = test_graph("conflicting effect leaves");
    for id in ["first", "second"] {
        let mut item = node(id);
        item.kind = ExecutionNodeKind::Materialize;
        item.resource_scopes = vec!["write:.".to_string()];
        graph.nodes.push(item);
    }
    let graph_id = graph.id.clone();

    let (_, report) = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor.submit_and_wait(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        ),
    )
    .await
    .expect("a conflicting sibling must defer instead of blocking wave admission")
    .expect("serialized effect leaves complete");

    let terminal = state.load(&graph_id).unwrap();
    assert_eq!(
        report.completed, 2,
        "report={report:?} statuses={:?} results={:?}",
        terminal.node_statuses, terminal.node_results
    );
    assert_eq!(report.failed, 0);
    assert_eq!(executor.max_running.load(Ordering::SeqCst), 1);
    supervisor.shutdown().await;
}

#[tokio::test]
async fn workspace_absolute_scope_is_normalized_before_locking() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state, commits);
    let mut graph = test_graph("workspace absolute scope");
    let mut root = node("root");
    root.resource_scopes = vec![format!("read:{}", std::env::temp_dir().display())];
    graph.nodes.push(root);

    let report = runner.start(graph).await.unwrap();
    assert_eq!(report.completed, 1);
    assert_eq!(report.blocked, 0);
}

#[tokio::test]
async fn compile_target_guard_skips_scope_locks_but_verify_and_effect_leaves_acquire_them() {
    for (kind, executor_kind, skips_lock) in [
        (
            ExecutionNodeKind::Verify,
            super::executors::CompileTargetGuardExecutor::KIND,
            true,
        ),
        (ExecutionNodeKind::Verify, "test", false),
        (ExecutionNodeKind::Materialize, "test", false),
    ] {
        let fixture = LeaseRecoveryFixture423::new();
        fixture
            .registry
            .register(Arc::new(super::executors::CompileTargetGuardExecutor))
            .unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(fixture.root.path())
                .unwrap();
        let request = ScopeLockRequest {
            scope: ScopedResource::workspace(resolver.workspace_id()).unwrap(),
            mode: ScopeLockMode::Write,
        };
        let held = fixture
            .scopes
            .try_acquire([request.clone()])
            .unwrap()
            .unwrap();
        let mut graph = test_graph("guard versus effect lock ownership");
        let mut spec = node("scoped");
        spec.kind = kind;
        spec.executor_kind = executor_kind.into();
        spec.resource_scopes = vec!["write:.".into()];
        spec.acceptance.criteria = vec!["mutation_resources_must_be_leased".into()];
        graph.nodes.push(spec);
        let graph = fixture.runner.register(graph).await.unwrap();
        let ready = fixture
            .runner
            .pump_snapshot(&graph.id, &Default::default())
            .await
            .unwrap()
            .ready;
        let mut prepared = fixture
            .runner
            .prepare_ready_wave(&graph.id, ready)
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        let prepared = prepared.pop().unwrap();
        if skips_lock {
            let binding = prepared
                .binding
                .as_ref()
                .expect("guard admits despite the held path lock");
            assert!(binding.scope_lease_ref.is_none());
            assert!(binding.worktree_lease_ref.is_none());
            assert!(binding.resource_lease_refs.is_empty());
            fixture
                .runner
                .execute_prepared_pump_node(&graph.id, prepared, Arc::new(Notify::new()))
                .await
                .unwrap();
            assert_eq!(
                fixture.state.load(&graph.id).unwrap().node_statuses["scoped"],
                ExecutionNodeStatus::Completed
            );
            drop(held);
        } else {
            assert!(prepared.binding.is_none());
            assert!(matches!(
                fixture
                    .runner
                    .execute_prepared_pump_node(&graph.id, prepared, Arc::new(Notify::new()))
                    .await,
                Err(ExecutionRunnerError::ResourceDeferred { .. })
            ));
            assert_eq!(
                fixture.state.load(&graph.id).unwrap().node_statuses["scoped"],
                ExecutionNodeStatus::Ready
            );
            drop(held);
            let ready = fixture
                .runner
                .pump_snapshot(&graph.id, &Default::default())
                .await
                .unwrap()
                .ready;
            let mut prepared = fixture
                .runner
                .prepare_ready_wave(&graph.id, ready)
                .await
                .unwrap();
            assert_eq!(prepared.len(), 1);
            let prepared = prepared.pop().unwrap();
            assert!(prepared.binding.as_ref().unwrap().scope_lease_ref.is_some());
            assert!(fixture
                .scopes
                .try_acquire([request.clone()])
                .unwrap()
                .is_none());
            fixture
                .runner
                .execute_prepared_pump_node(&graph.id, prepared, Arc::new(Notify::new()))
                .await
                .unwrap();
            assert_eq!(
                fixture.state.load(&graph.id).unwrap().node_statuses["scoped"],
                ExecutionNodeStatus::Completed
            );
        }
        assert!(fixture.scopes.try_acquire([request]).unwrap().is_some());
    }
}

#[tokio::test]
async fn invalid_resource_scope_becomes_a_durable_node_blocker() {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(TestExecutor::new(
            Vec::new(),
            Duration::from_millis(1),
        )))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("invalid resource scope");
    let mut root = node("root");
    root.resource_scopes = vec!["read:/outside-workspace".to_string()];
    graph.nodes.push(root);
    let graph_id = graph.id.clone();

    let report = runner.start(graph).await.unwrap();
    assert_eq!(report.blocked, 1);
    let graph = state.load(&graph_id).unwrap();
    assert_eq!(graph.node_statuses["root"], ExecutionNodeStatus::Blocked);
    assert_eq!(
        graph.node_results["root"].failure.as_ref().unwrap().kind,
        "resource_acquisition_failed"
    );
}

#[tokio::test]
async fn compile_target_guard_still_rejects_invalid_resource_scopes() {
    for scope in ["read:../outside", "write:../outside", "worktree:../outside"] {
        let fixture = LeaseRecoveryFixture423::new();
        fixture
            .registry
            .register(Arc::new(super::executors::CompileTargetGuardExecutor))
            .unwrap();
        let mut graph = test_graph("guard validates without leasing");
        let mut guard = node("guard");
        guard.kind = ExecutionNodeKind::Verify;
        guard.executor_kind = super::executors::CompileTargetGuardExecutor::KIND.into();
        guard.acceptance.criteria = vec!["mutation_resources_must_be_leased".into()];
        guard.resource_scopes = vec![scope.into()];
        graph.nodes.push(guard);
        let graph_id = graph.id.clone();
        let report = fixture.runner.start(graph).await.unwrap();
        assert_eq!(report.blocked, 1);
        let graph = fixture.state.load(&graph_id).unwrap();
        assert_eq!(graph.node_statuses["guard"], ExecutionNodeStatus::Blocked);
        assert_eq!(
            graph.node_results["guard"].failure.as_ref().unwrap().kind,
            "resource_acquisition_failed"
        );
        assert_eq!(fixture.scopes.active_lease_count().unwrap(), 0);
    }
}

async fn run_verify_graph(evidence_id: Option<&str>, required: &str) -> (ExecutionGraph, usize) {
    let (registry, state, commits) = harness();
    registry
        .register(Arc::new(EvidenceExecutor {
            evidence_id: evidence_id.map(str::to_string),
        }))
        .unwrap();
    registry
        .register(Arc::new(super::executors::VerifyNodeExecutor::new(
            state.clone(),
        )))
        .unwrap();
    let synthesize = Arc::new(super::executors::SynthesizeNodeExecutor::new());
    registry
        .register(Arc::clone(&synthesize) as Arc<dyn NodeExecutor>)
        .unwrap();

    let mut graph = test_graph("verify gate");
    let source = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "evidence_test", "source");
    let mut verify = ExecutionNodeSpec::new(
        ExecutionNodeKind::Verify,
        super::executors::VerifyNodeExecutor::KIND,
        "verify",
    );
    verify.acceptance.required_evidence = vec![required.to_string()];
    let synth = ExecutionNodeSpec::new(
        ExecutionNodeKind::Synthesize,
        super::executors::SynthesizeNodeExecutor::KIND,
        "synthesize",
    );
    graph.edges = vec![
        ExecutionEdge {
            from: source.id.clone(),
            to: verify.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: verify.id.clone(),
            to: synth.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    graph.nodes = vec![source, verify, synth];
    let graph_id = graph.id.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    synthesize.install_resolver(Arc::new(TerminalResolver {
        graph_id: graph_id.clone(),
        backend: Arc::new(TerminalBackend {
            calls: Arc::clone(&calls),
        }),
    }));
    let runner = test_runner(registry, state.clone(), commits);
    runner.start(graph).await.expect("verify graph runs");
    (state.load(&graph_id).unwrap(), calls.load(Ordering::SeqCst))
}

#[tokio::test]
async fn verify_missing_evidence_blocks_synthesize() {
    let (graph, synthesize_calls) = run_verify_graph(None, "required-proof").await;
    let verify = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Verify)
        .unwrap();
    let synth = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Synthesize)
        .unwrap();
    assert_eq!(
        graph.node_statuses[&verify.id],
        ExecutionNodeStatus::Blocked
    );
    assert_eq!(
        graph.node_results[&verify.id]
            .failure
            .as_ref()
            .map(|failure| failure.kind.as_str()),
        Some("missing_evidence")
    );
    assert_eq!(graph.node_statuses[&synth.id], ExecutionNodeStatus::Blocked);
    assert_eq!(synthesize_calls, 0);
    assert!(
        harness_contract::execution_graph::project_execution_graph(&graph)
            .terminal_result_ref
            .is_none()
    );
}

#[tokio::test]
async fn verify_satisfied_evidence_allows_exactly_one_terminal_synthesis() {
    let (graph, synthesize_calls) =
        run_verify_graph(Some("required-proof"), "required-proof").await;
    let verify = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Verify)
        .unwrap();
    let synth = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Synthesize)
        .unwrap();
    assert_eq!(
        graph.node_statuses[&verify.id],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        graph.node_statuses[&synth.id],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(synthesize_calls, 1);
    assert_eq!(
        harness_contract::execution_graph::project_execution_graph(&graph)
            .terminal_result_ref
            .as_deref(),
        Some(format!("terminal:{}", graph.id).as_str())
    );
}

#[test]
fn graph_enumeration_excludes_legacy_non_graph_execution_scope_streams() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut graph = test_graph("canonical graph");
    graph.nodes.push(node("canonical"));
    let graph = commits
        .register_graph(graph)
        .expect("graph registers")
        .graph;
    event_store
        .append(RuntimeEventInput {
            stream_id: "session:legacy-strategy".to_string(),
            scope: RuntimeEventScope::ExecutionGraph,
            kind: "runtime.strategy.selected".to_string(),
            status: Some("running".to_string()),
            actor: Some("legacy".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"decision_id": "legacy"}),
        })
        .expect("legacy event records");

    let state = ExecutionGraphStateStore::new(event_store);
    assert_eq!(
        state.graph_ids().expect("graph ids"),
        vec![graph.id.clone()]
    );
    assert_eq!(
        state
            .nonterminal_graph_ids()
            .expect("nonterminal graph ids"),
        vec![graph.id]
    );
}

#[test]
fn graph_enumeration_is_cursor_paginated_without_full_stream_scan() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(Arc::clone(&event_store));
    let mut expected = Vec::new();
    for objective in ["team page one", "team page two", "team page three"] {
        let mut graph = test_graph(objective);
        graph.nodes.push(node(objective));
        expected.push(
            commits
                .register_graph(graph)
                .expect("graph registers")
                .graph
                .id,
        );
    }
    let state = ExecutionGraphStateStore::new(event_store);
    let first = state.graph_ids_page(None, 2).expect("first page");
    assert_eq!(
        first.iter().map(|(id, _)| id).collect::<Vec<_>>(),
        expected.iter().take(2).collect::<Vec<_>>()
    );
    let (last_id, last_cursor) = first.last().expect("first page cursor");
    let second = state
        .graph_ids_page(Some((*last_cursor, last_id.clone())), 2)
        .expect("second page");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, expected[2]);
}

/// A Program id contains a colon, so the parked subject has five colon-separated
/// parts. The C4 anchor used to require exactly four, which rejected every barrier
/// resolution unconditionally: run #6 parked at Program revision 17, the Program
/// advanced to 29 with all three Tasks accepted, and the resolver's own record shows
/// the release refused as "correlation does not match the parked subject" - which is
/// why an earlier fix that only changed which revision the correlation carried
/// changed nothing.
#[tokio::test]
async fn a_barrier_resolution_anchors_on_a_program_id_that_contains_a_colon() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let program_id = "program:deadbeef".to_string();
    let root_id = "root-wait-colon-program".to_string();
    let graph = {
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("colon root");
        graph.id = root_id.clone();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: format!("session:{program_id}"),
            turn_id: format!("turn:{program_id}"),
            root_task_id: format!("task:{program_id}"),
            task_id: format!("task:{program_id}"),
            generation: 1,
        });
        let wait = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify,
            crate::execution_core::graph::executors::AgenticProgramWaitExecutor::KIND,
            serde_json::to_string(
                &crate::execution_core::graph::executors::AgenticProgramWaitRequest {
                    program_id: program_id.clone(),
                    root_execution_id: root_id.clone(),
                    deadline_at_ms: None,
                },
            )
            .expect("wait request"),
        );
        graph.nodes.push(wait);
        graph
    };
    services
        .commit_service()
        .register_graph(graph)
        .expect("register root");

    let parked = format!("agentic-program-wait:{program_id}:revision:17");
    assert_eq!(
        parked.split(':').count(),
        5,
        "the Program id itself carries a colon, so the parked subject is not four parts"
    );

    // A resolution at a later revision must still be anchored on the parked subject.
    let correlation = format!(
        "agentic-program-wait:{program_id}:{}:17",
        "root-wait-colon-program:program-wait"
    );
    let anchored = parked
        .rsplit_once(":revision:")
        .is_some_and(|(subject, parked_revision)| {
            correlation.starts_with(&format!("{subject}:"))
                && correlation.ends_with(&format!(":{parked_revision}"))
        });
    assert!(
        anchored,
        "a five-part parked subject must still anchor a resolution: parked={parked} correlation={correlation}"
    );
}

/// Only `AgentTask` nodes read a durable deadline from their payload, so every
/// other kind used to be awaited with no bound at all. Run t2 is the cost: one
/// `inline_model` node stopped making progress, the whole Runtime wrote no
/// further event for ten minutes, the session worker kept renewing its lease the
/// entire time, and shutdown had to force-abort the turn.
#[tokio::test]
async fn a_node_with_no_durable_deadline_is_still_bounded() {
    let (registry, state, commits) = harness();
    let pending = Arc::new(PermanentPendingExecutor {
        cancel_calls: AtomicUsize::new(0),
    });
    registry.register(pending.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("a bounded node is never a permanent hang");
    let graph_id = graph.id.clone();
    // An `inline_model` node is exactly the kind that carries no deadline.
    let mut stuck = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        pending.kind(),
        "{}".to_string(),
    );
    stuck.id = "stuck-model".to_string();
    stuck.idempotency_key = "stuck-model-attempt".to_string();
    graph.nodes = vec![stuck];

    // The ceiling is an operator bound, not a per-node contract, so the test
    // shortens it rather than waiting out the default.
    struct Ceiling;
    impl Drop for Ceiling {
        fn drop(&mut self) {
            std::env::remove_var("COWD_NODE_EXECUTION_CEILING_SECS");
        }
    }
    let _ceiling = Ceiling;
    std::env::set_var("COWD_NODE_EXECUTION_CEILING_SECS", "1");

    tokio::time::timeout(Duration::from_secs(10), runner.start(graph))
        .await
        .expect("a node with no durable deadline must still terminate")
        .expect("graph reaches quiescence");
    let graph = state.load(&graph_id).unwrap();
    assert_eq!(
        graph.node_statuses["stuck-model"],
        ExecutionNodeStatus::Cancelled,
        "the bounded ceiling must cancel a node that never returns"
    );
    assert_eq!(
        graph.node_results["stuck-model"]
            .failure
            .as_ref()
            .map(|failure| failure.kind.as_str()),
        Some("execution_deadline_exceeded")
    );
    assert!(pending.cancel_calls.load(Ordering::SeqCst) >= 1);
}

struct LeaseRecoveryFixture423 {
    root: tempfile::TempDir,
    events: Arc<RuntimeEventStore>,
    state: ExecutionGraphStateStore,
    commits: ExecutionCommitService,
    scopes: Arc<ScopeLockManager>,
    worktrees: Arc<WorktreeLeaseManager>,
    resources: Arc<ExecutionResourceManager>,
    registry: Arc<NodeExecutorRegistry>,
    runner: ExecutionGraphRunner,
}

impl LeaseRecoveryFixture423 {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let events = Arc::new(RuntimeEventStore::for_test());
        let state = ExecutionGraphStateStore::new(events.clone());
        let commits = ExecutionCommitService::new(events.clone());
        let scopes = Arc::new(ScopeLockManager::persistent(root.path().join("scopes")).unwrap());
        let worktrees =
            Arc::new(WorktreeLeaseManager::open(root.path().join("worktrees.json")).unwrap());
        let resources = Arc::new(ExecutionResourceManager::new([(
            ExecutionResourceKind::Tool,
            ResourceQuota::new(1, 2, 2).unwrap(),
        )]));
        let registry = Arc::new(NodeExecutorRegistry::new());
        registry
            .register(Arc::new(TestExecutor::new(Vec::new(), Duration::ZERO)))
            .unwrap();
        let runner = ExecutionGraphRunner::new(
            registry.clone(),
            state.clone(),
            commits.clone(),
            resources.clone(),
            scopes.clone(),
            worktrees.clone(),
            "lease-workspace-423",
            root.path(),
        );
        Self {
            root,
            events,
            state,
            commits,
            scopes,
            worktrees,
            resources,
            registry,
            runner,
        }
    }

    async fn terminal_with_uncommitted_release(
        &self,
    ) -> (
        ExecutionGraph,
        ExecutionNodeBinding,
        ScopeLockLease,
        ExecutionResourceLease,
        super::resources::BindingOwnerFence,
    ) {
        let mut graph = test_graph("exact original lease recovery");
        let mut spec = node("lease-node-423");
        spec.kind = ExecutionNodeKind::Materialize;
        graph.nodes.push(spec);
        let graph = self.commits.register_graph(graph).unwrap().graph;
        let graph = self
            .commits
            .transition_node(
                &graph,
                "lease-node-423",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let scope = self
            .scopes
            .try_acquire([ScopeLockRequest {
                scope: ScopedResource::Resource {
                    namespace: "lease-423".into(),
                    key: "reader".into(),
                },
                mode: ScopeLockMode::Read,
            }])
            .unwrap()
            .unwrap();
        let resource = self
            .resources
            .acquire(ExecutionResourceKind::Tool, None)
            .await
            .unwrap();
        let binding = ExecutionNodeBinding {
            executor_kind: "test".into(),
            ticket_idempotency_key: "idempotency:lease-node-423:1".into(),
            attempt: 1,
            resource_lease_refs: vec![resource.projection_reference()],
            scope_lease_ref: Some(scope.projection_reference()),
            worktree_lease_ref: None,
        };
        let key = super::events::ExecutionBindingLeaseClosure::fence_key(
            &graph,
            "lease-node-423",
            &binding,
        );
        let fence = acquire_fixture_owner_475(
            &self.scopes,
            &self.commits,
            &graph,
            "lease-node-423",
            &binding,
        );
        let graph = self
            .commits
            .bind_and_start_node(&graph, "lease-node-423", binding.clone())
            .unwrap()
            .graph;
        let graph = self
            .commits
            .transition_node(
                &graph,
                "lease-node-423",
                ExecutionNodeStatus::Completed,
                Some(completed_result("lease-node-423")),
                Vec::new(),
            )
            .unwrap()
            .graph;
        (graph, binding, scope, resource, fence)
    }
}

#[tokio::test]
async fn r1_423_normal_release_is_durable_across_manager_restart() {
    let fixture = LeaseRecoveryFixture423::new();
    let mut graph = test_graph("normal lease release survives restart");
    let backend = Arc::new(PayloadScopedBackend {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: "payload:normal-lease-423".into(),
        backend: backend.clone(),
    }));
    fixture.registry.register(executor).unwrap();
    let mut spec = node("normal-lease-423");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = "inline_model".into();
    spec.resource_scopes = vec!["read:.".into()];
    graph.nodes.push(spec);
    let graph_id = graph.id.clone();
    fixture.runner.start(graph).await.unwrap();
    let graph = fixture.state.load(&graph_id).unwrap();
    let mut after = 0;
    let mut starts = Vec::new();
    while after < graph.revision {
        let (next, page) = fixture
            .state
            .lease_starts_page(&graph_id, after, graph.revision)
            .unwrap();
        starts.extend(page);
        after = next;
    }
    assert_eq!(starts.len(), 1);
    let (node_id, binding, position) = &starts[0];
    assert!(fixture
        .state
        .binding_lease_closure(&graph, node_id, binding, *position)
        .unwrap());
    let restarted_scopes =
        ScopeLockManager::persistent(fixture.root.path().join("scopes")).unwrap();
    let restarted_resources = ExecutionResourceManager::new([]);
    assert!(binding.scope_lease_ref.is_some());
    assert!(binding.resource_lease_refs.is_empty());
    assert!(restarted_resources
        .snapshot(&ExecutionResourceKind::Tool)
        .is_err());
    let restarted =
        ExecutionGraphRecovery::new(&fixture.state, &fixture.commits, &fixture.registry)
            .recover_with_leases(
                &graph_id,
                &restarted_scopes,
                &fixture.worktrees,
                "lease-workspace-423",
            )
            .await
            .unwrap();
    assert_eq!(restarted.revision, graph.revision);
    assert!(fixture
        .state
        .binding_lease_closure(&restarted, node_id, binding, *position)
        .unwrap());
    let key = super::events::ExecutionBindingLeaseClosure::fence_key(&graph, node_id, binding);
    let (ticket, spec) = fixture
        .state
        .binding_start_ticket(&graph, node_id, binding)
        .unwrap();
    assert!(restarted_scopes
        .acquire_binding_owner(&key, || fixture
            .commits
            .validate_unadmitted_binding_start(&graph, &spec, &ticket)
            .map_err(|error| error.to_string()))
        .is_err());
}

#[tokio::test]
async fn r1_423b_owner_loss_without_physical_auth_never_releases_or_requeues() {
    let fixture = LeaseRecoveryFixture423::new();
    let (graph, binding, scope, resource, fence) =
        fixture.terminal_with_uncommitted_release().await;
    let position = fixture
        .state
        .binding_start_position(&graph, "lease-node-423", &binding)
        .unwrap()
        .unwrap();
    let peer = ScopeLockManager::persistent(fixture.root.path().join("scopes")).unwrap();
    let recovery = ExecutionGraphRecovery::new(&fixture.state, &fixture.commits, &fixture.registry);
    assert!(recovery
        .recover_with_leases(&graph.id, &peer, &fixture.worktrees, "lease-workspace-423")
        .await
        .is_err());
    scope.try_release().unwrap();
    drop(resource);
    drop(fence);
    let error = recovery
        .recover_with_leases(&graph.id, &peer, &fixture.worktrees, "lease-workspace-423")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("quiescence unproven"));
    assert_eq!(
        fixture.state.load(&graph.id).unwrap().revision,
        graph.revision
    );
    assert!(!fixture
        .state
        .binding_lease_closure(&graph, "lease-node-423", &binding, position)
        .unwrap());
}

#[tokio::test]
async fn r1_423b_typed_normal_capability_rejects_wrong_binding_and_release_error() {
    let fixture = LeaseRecoveryFixture423::new();
    let (graph, binding, scope, resource, mut owner) =
        fixture.terminal_with_uncommitted_release().await;
    let position = fixture
        .state
        .binding_start_position(&graph, "lease-node-423", &binding)
        .unwrap()
        .unwrap();
    let mut resource = Some(resource);
    let mut scope = Some(scope);
    let mut worktree = None;
    let mut wrong = binding.clone();
    wrong.attempt += 1;
    assert!(owner
        .release_original(
            &graph,
            "lease-node-423",
            &wrong,
            position,
            &mut resource,
            &mut scope,
            &mut worktree,
            &fixture.worktrees
        )
        .is_err());
    assert!(resource.is_some() && scope.is_some());
    scope.as_mut().unwrap().fail_release_for_test();
    assert!(owner
        .release_original(
            &graph,
            "lease-node-423",
            &binding,
            position,
            &mut resource,
            &mut scope,
            &mut worktree,
            &fixture.worktrees
        )
        .is_err());
    assert!(!fixture
        .state
        .binding_lease_closure(&graph, "lease-node-423", &binding, position)
        .unwrap());
    let peer = ScopeLockManager::persistent(fixture.root.path().join("scopes")).unwrap();
    let key =
        super::events::ExecutionBindingLeaseClosure::fence_key(&graph, "lease-node-423", &binding);
    assert!(peer.try_recover_binding_owner(&key).unwrap().is_none());
    assert!(owner
        .release_original(
            &graph,
            "lease-node-423",
            &binding,
            position,
            &mut resource,
            &mut scope,
            &mut worktree,
            &fixture.worktrees
        )
        .is_err());
}

#[tokio::test]
async fn r1_423b_typed_owner_proof_survives_checkpoint_and_rejects_forged_source() {
    let fixture = LeaseRecoveryFixture423::new();
    let (mut graph, binding, scope, resource, mut owner) =
        fixture.terminal_with_uncommitted_release().await;
    while graph.revision + 1 < 256 {
        graph = fixture
            .commits
            .commit_recovery(&graph, graph.clone(), Vec::new(), Vec::new())
            .unwrap()
            .graph;
    }
    let position = fixture
        .state
        .binding_start_position(&graph, "lease-node-423", &binding)
        .unwrap()
        .unwrap();
    let mut resource = Some(resource);
    let mut scope = Some(scope);
    let mut worktree = None;
    let released = owner
        .release_original(
            &graph,
            "lease-node-423",
            &binding,
            position,
            &mut resource,
            &mut scope,
            &mut worktree,
            &fixture.worktrees,
        )
        .unwrap();
    let closed = fixture
        .commits
        .commit_binding_lease_closure(&graph, &released)
        .unwrap()
        .graph;
    let key = binding.lease_closure_key(&closed, "lease-node-423");
    let record = fixture
        .events
        .event_by_idempotency_key(&closed.id, &key)
        .unwrap()
        .unwrap();
    assert_eq!(record.kind, "execution_graph.checkpoint");
    let mut resolved = fixture.events.resolve_read_record(record).unwrap();
    assert!(binding
        .validate_lease_closure(
            &closed,
            "lease-node-423",
            (position.1, position.2),
            &resolved
        )
        .unwrap());
    resolved.actor = Some("untrusted".into());
    assert!(binding
        .validate_lease_closure(
            &closed,
            "lease-node-423",
            (position.1, position.2),
            &resolved
        )
        .is_err());
}

#[tokio::test]
async fn r1_423_no_lease_binding_does_not_require_owner_file() {
    let fixture = LeaseRecoveryFixture423::new();
    let mut graph = test_graph("no lease obligation");
    graph.nodes.push(node("no-lease-423"));
    let graph_id = graph.id.clone();
    fixture.runner.start(graph).await.unwrap();
    let graph = fixture.state.load(&graph_id).unwrap();
    let starts = fixture
        .state
        .binding_starts_page(&graph_id, 0, graph.revision)
        .unwrap()
        .1;
    assert_eq!(starts.len(), 1);
    let (node, binding, position) = &starts[0];
    assert!(
        binding.resource_lease_refs.is_empty()
            && binding.scope_lease_ref.is_none()
            && binding.worktree_lease_ref.is_none()
    );
    assert!(fixture
        .state
        .binding_lease_closure(&graph, node, binding, *position)
        .unwrap());
    assert!(fixture
        .root
        .path()
        .join("scopes")
        .join("binding-owners-v1")
        .exists());
}

#[tokio::test]
async fn r1_423_worktree_recovery_requires_exact_owner_and_real_release_not_ttl() {
    for mode in ["normal", "wrong_workspace", "reclaimed"] {
        let fixture = LeaseRecoveryFixture423::new();
        let mut graph = test_graph("worktree owner recovery");
        let mut spec = node("worktree-423");
        spec.kind = ExecutionNodeKind::Materialize;
        graph.nodes.push(spec);
        let graph = fixture.commits.register_graph(graph).unwrap().graph;
        let graph = fixture
            .commits
            .transition_node(
                &graph,
                "worktree-423",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let path = fixture.root.path().join("user-worktree");
        std::fs::create_dir_all(&path).unwrap();
        let lease = fixture
            .worktrees
            .acquire(WorktreeLeaseRequest {
                workspace_id: if mode == "wrong_workspace" {
                    "other"
                } else {
                    "lease-workspace-423"
                }
                .into(),
                task_id: graph.id.clone(),
                owner_id: "worktree-423".into(),
                path: path.clone(),
                ownership: WorktreeOwnership::UserManaged,
                ttl: Duration::from_secs(60),
            })
            .unwrap();
        let lease_id = lease.record().lease_id;
        let binding = ExecutionNodeBinding {
            executor_kind: "test".into(),
            ticket_idempotency_key: "idempotency:worktree-423:1".into(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: Some(lease_id.to_string()),
        };
        let key = super::events::ExecutionBindingLeaseClosure::fence_key(
            &graph,
            "worktree-423",
            &binding,
        );
        let fence = acquire_fixture_owner_475(
            &fixture.scopes,
            &fixture.commits,
            &graph,
            "worktree-423",
            &binding,
        );
        let graph = fixture
            .commits
            .bind_and_start_node(&graph, "worktree-423", binding.clone())
            .unwrap()
            .graph;
        let graph = fixture
            .commits
            .transition_node(
                &graph,
                "worktree-423",
                ExecutionNodeStatus::Completed,
                Some(completed_result("worktree-423")),
                Vec::new(),
            )
            .unwrap()
            .graph;
        drop(lease);
        drop(fence);
        if mode == "reclaimed" {
            fixture.worktrees.reclaim_expired(u64::MAX).unwrap();
        }
        let restarted =
            WorktreeLeaseManager::open(fixture.root.path().join("worktrees.json")).unwrap();
        let result =
            ExecutionGraphRecovery::new(&fixture.state, &fixture.commits, &fixture.registry)
                .recover_with_leases(
                    &graph.id,
                    &fixture.scopes,
                    &restarted,
                    "lease-workspace-423",
                )
                .await;
        assert!(result.is_err());
        assert!(fixture
            .events
            .event_by_idempotency_key(
                &graph.id,
                &binding.lease_closure_key(&graph, "worktree-423")
            )
            .unwrap()
            .is_none());
        if mode == "normal" {
            assert_eq!(
                restarted.record(lease_id).unwrap().unwrap().status,
                WorktreeLeaseStatus::Active
            );
            assert!(path.exists());
        }
    }
}


struct LeaseAfterCommitFailure423b;

#[async_trait]
impl NodeExecutor for LeaseAfterCommitFailure423b {
    fn kind(&self) -> &str {
        "after-commit-failure-423b"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().into(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key.clone(),
            payload_ref: context.node.payload_ref.clone(),
        })
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        Ok(NodeExecutionOutcome::new(completed_result(&ticket.node_id)))
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        Err(NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason: "original after_commit not done".into(),
        })
    }
}

#[tokio::test]
async fn r1_423b_failed_after_commit_pins_worktree_and_owner_fence() {
    let fixture = LeaseRecoveryFixture423::new();
    fixture
        .registry
        .register(Arc::new(LeaseAfterCommitFailure423b))
        .unwrap();
    let mut graph = test_graph("failed after_commit cannot release resources");
    let mut spec = node("callback-423b");
    spec.kind = ExecutionNodeKind::Materialize;
    spec.executor_kind = "after-commit-failure-423b".into();
    std::fs::create_dir_all(fixture.root.path().join("callback-worktree")).unwrap();
    spec.resource_scopes = vec!["worktree:callback-worktree".into()];
    graph.nodes.push(spec);
    let graph_id = graph.id.clone();
    let _result = tokio::time::timeout(Duration::from_secs(2), fixture.runner.start(graph)).await;
    let graph = fixture.state.load(&graph_id).unwrap();
    let active = fixture.worktrees.active_records().unwrap();
    assert_eq!(active.len(), 1);
    let mut after = 0;
    let mut binding = None;
    while after < graph.revision {
        let (next, starts) = fixture
            .state
            .lease_starts_page(&graph_id, after, graph.revision)
            .unwrap();
        for (node, value, _) in starts {
            if node == "callback-423b" {
                binding = Some(value);
            }
        }
        after = next;
    }
    let binding = binding.unwrap();
    let key =
        super::events::ExecutionBindingLeaseClosure::fence_key(&graph, "callback-423b", &binding);
    let peer = ScopeLockManager::persistent(fixture.root.path().join("scopes")).unwrap();
    assert!(peer.try_recover_binding_owner(&key).unwrap().is_none());
    assert!(fixture
        .events
        .event_by_idempotency_key(
            &graph_id,
            &binding.lease_closure_key(&graph, "callback-423b")
        )
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn r1_423b_builtin_inline_model_crash_before_lease_commit_recovers_real_worktree() {
    let fixture = LeaseRecoveryFixture423::new();
    let backend = Arc::new(PayloadScopedBackend {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: "model-only-423b".into(),
        backend: backend.clone(),
    }));
    fixture.registry.register(executor.clone()).unwrap();
    let mut graph = test_graph("builtin model-only original receipt then crash");
    let mut spec = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "model-only-423b",
    );
    spec.id = "inline-crash-423b".into();
    spec.idempotency_key = "inline-crash-423b:1".into();
    graph.nodes.push(spec.clone());
    let graph = fixture.commits.register_graph(graph).unwrap().graph;
    let graph = fixture
        .commits
        .transition_node(
            &graph,
            &spec.id,
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let path = fixture.root.path().join("inline-worktree");
    std::fs::create_dir_all(&path).unwrap();
    let lease = fixture
        .worktrees
        .acquire(WorktreeLeaseRequest {
            workspace_id: "lease-workspace-423".into(),
            task_id: graph.id.clone(),
            owner_id: spec.id.clone(),
            path,
            ownership: WorktreeOwnership::UserManaged,
            ttl: Duration::from_secs(60),
        })
        .unwrap();
    let lease_id = lease.record().lease_id;
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(graph.clone()),
            node: spec.clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: "inline_model".into(),
        ticket_idempotency_key: spec.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: Some(lease_id.to_string()),
    };
    let key = super::events::ExecutionBindingLeaseClosure::fence_key(&graph, &spec.id, &binding);
    let fence = acquire_fixture_owner_475(
        &fixture.scopes,
        &fixture.commits,
        &graph,
        &spec.id,
        &binding,
    );
    executor.prepare_original_binding(&ticket).unwrap();
    let graph = fixture
        .commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([(spec.id.clone(), binding.clone())]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    let outcome = executor.poll_or_await(&ticket).await.unwrap();
    let graph = fixture
        .commits
        .transition_execution_outcome(&graph, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    drop(lease);
    drop(fence);
    assert_eq!(
        fixture.worktrees.record(lease_id).unwrap().unwrap().status,
        WorktreeLeaseStatus::Active
    );
    let restarted = ScopeLockManager::persistent(fixture.root.path().join("scopes")).unwrap();
    let recovered =
        ExecutionGraphRecovery::new(&fixture.state, &fixture.commits, &fixture.registry)
            .recover_with_leases(
                &graph.id,
                &restarted,
                &fixture.worktrees,
                "lease-workspace-423",
            )
            .await
            .unwrap();
    assert_eq!(
        fixture.worktrees.record(lease_id).unwrap().unwrap().status,
        WorktreeLeaseStatus::Released
    );
    let position = fixture
        .state
        .binding_start_position(&recovered, &spec.id, &binding)
        .unwrap()
        .unwrap();
    assert!(fixture
        .state
        .binding_lease_closure(&recovered, &spec.id, &binding, position)
        .unwrap());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn r1_450_node_effect_attempt_namespace_and_source_identity_are_exact() {
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(event_store.clone());
    let mut graph = test_graph("450 exact attempt addressing");
    graph.nodes.push(node("exact"));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "exact",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let executor = BindingIdentityExecutor::new(0);
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(graph.clone()),
            node: graph.nodes[0].clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: ticket.attempt,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let running = commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([("exact".into(), binding)]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    let outcome = executor.poll_or_await(&ticket).await.unwrap();
    let committed = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
    assert!(stream.starts_with("execution-node-effect:"));
    let events = event_store.list_stream(&stream).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[1].transaction_id,
        event_store
            .list_stream(&graph.id)
            .unwrap()
            .last()
            .unwrap()
            .transaction_id
    );
    assert!(event_store
        .list_stream(&format!("execution-effect:{}", ticket.idempotency_key))
        .unwrap()
        .is_empty());
    let through = event_store.durable_commit_cursor().unwrap();
    assert!(ExecutionNodeBinding::validate_effect_records(
        &ticket,
        Some(&events[0]),
        Some(&events[1]),
        through
    )
    .unwrap()
    .is_some());
    for attempt in [2, 3, u32::MAX] {
        let mut retry = ticket.clone();
        retry.attempt = attempt;
        let address = ExecutionNodeBinding::effect_stream_id(&retry);
        assert_ne!(address, stream);
        assert_eq!(retry.idempotency_key, ticket.idempotency_key);
        assert!(matches!(
            commits.inspect_execution_effect(&retry).unwrap(),
            ExecutionEffectState::Fresh
        ));
        assert!(commits
            .validate_fresh_node_admission(&committed, &committed.nodes[0], &retry)
            .is_err());
        assert!(ExecutionNodeBinding::validate_effect_records(
            &retry,
            Some(&events[0]),
            Some(&events[1]),
            through
        )
        .is_err());
        let mut intent = events[0].clone();
        let mut receipt = events[1].clone();
        intent.stream_id = address.clone();
        receipt.stream_id = address;
        assert!(ExecutionNodeBinding::validate_effect_records(
            &retry,
            Some(&intent),
            Some(&receipt),
            through
        )
        .unwrap_err()
        .contains("intent ticket mismatch"));
    }
    assert_eq!(executor.polls.load(Ordering::SeqCst), 1);
}

async fn committed_inline_attempt_450(
    title: &str,
) -> (
    Arc<RuntimeEventStore>,
    ExecutionGraph,
    NodeExecutionTicket,
    Arc<AtomicUsize>,
) {
    let (events, graph, ticket, outcome, calls) = running_inline_attempt_450(title).await;
    let commits = ExecutionCommitService::new(events.clone());
    let graph = commits
        .transition_execution_outcome(&graph, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    (events, graph, ticket, calls)
}

pub(super) async fn running_inline_attempt_450(
    title: &str,
) -> (
    Arc<RuntimeEventStore>,
    ExecutionGraph,
    NodeExecutionTicket,
    NodeExecutionOutcome,
    Arc<AtomicUsize>,
) {
    let events = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(events.clone());
    let mut graph = test_graph(title);
    let mut spec = node("durable");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = "inline_model".into();
    graph.nodes.push(spec);
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = ScopedNodeExecutor::new("inline_model");
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: "payload:durable".into(),
        backend: Arc::new(PayloadScopedBackend {
            calls: calls.clone(),
        }),
    }));
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(graph.clone()),
            node: graph.nodes[0].clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let scopes = manual_scope_locks_475(&graph.id);
    let _owner = acquire_fixture_owner_475(&scopes, &commits, &graph, "durable", &binding);
    executor.prepare_original_binding(&ticket).unwrap();
    let graph = commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([("durable".into(), binding)]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    let outcome = executor.poll_or_await(&ticket).await.unwrap();
    (events, graph, ticket, outcome, calls)
}




#[tokio::test]
async fn r1_450_atomic_replan_metadata_retry_and_response_loss_keep_one_outcome() {
    let (events, running, ticket, mut outcome, calls) =
        running_inline_attempt_450("450 atomic metadata").await;
    let commits = ExecutionCommitService::new(events.clone());
    outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
        "envelope_id":"450-envelope","revision":1,"objective_id":"450-objective","created_at_ms":1
    })).unwrap());
    outcome.terminal_presentation = Some(serde_json::from_value(serde_json::json!({
        "presentation_id":"450-presentation","attempt_id":"450-attempt","envelope_id":"450-envelope","envelope_revision":1,
        "state":"committed","answer_origin":"runtime_verified_fallback","generated_at_ms":1
    })).unwrap());
    let tail = node("450-tail");
    outcome.replan = Some(ExecutionGraphReplan {
        nodes: vec![tail],
        edges: vec![ExecutionEdge {
            from: "durable".into(),
            to: "450-tail".into(),
            kind: ExecutionEdgeKind::DependsOn,
        }],
        reason: "450 original replan".into(),
        framework_authored: true,
    });
    outcome
        .domain_events
        .push(crate::RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: "450-business".into(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "fixture.business".into(),
                status: None,
                actor: Some("test".into()),
                refs: Vec::new(),
                payload: serde_json::json!({"original":true}),
            },
            schema_version: 1,
            idempotency_key: Some("450-business-once".into()),
        });
    let sources = commits
        .execution_outcome_sources(&running, &ticket, &outcome)
        .unwrap();
    let concurrent = commits
        .commit_recovery(&running, running.clone(), Vec::new(), Vec::new())
        .unwrap()
        .graph;
    assert!(commits
        .transition_execution_outcome(&running, &ticket, &outcome, &sources)
        .is_err());
    assert_eq!(
        ExecutionGraphStateStore::new(events.clone())
            .load(&running.id)
            .unwrap(),
        concurrent
    );
    assert_eq!(events.stream_revision("450-business").unwrap(), 0);
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    let lost = Arc::new(AtomicUsize::new(0));
    commits.inject_node_outcome_response_loss_for_test(&ticket.node_id, lost.clone());
    let receipt = commits
        .transition_execution_outcome(&concurrent, &ticket, &outcome, &sources)
        .unwrap();
    assert_eq!(lost.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.graph.delivery_envelope, outcome.delivery_envelope);
    assert_eq!(
        receipt.graph.terminal_presentation,
        outcome.terminal_presentation
    );
    assert_eq!(receipt.graph.node_results["durable"], outcome.result);
    assert_eq!(
        receipt.graph.node_statuses["450-tail"],
        ExecutionNodeStatus::Planned
    );
    let primary = events
        .event_by_idempotency_key(
            &ExecutionNodeBinding::effect_stream_id(&ticket),
            &format!("{}:receipt", ticket.idempotency_key),
        )
        .unwrap()
        .unwrap();
    let business = events
        .event_by_idempotency_key("450-business", "450-business-once")
        .unwrap()
        .unwrap();
    assert_eq!(primary.transaction_id, receipt.transaction.transaction_id);
    assert_eq!(business.transaction_id, primary.transaction_id);
    assert_eq!(events.stream_revision("450-business").unwrap(), 1);
}

#[tokio::test]
async fn r601_observation_and_original_goal_cut_cannot_commit_mixed_revisions() {
    let (events, running, ticket, mut outcome, _) =
        running_inline_attempt_450("601 mixed observation cut").await;
    let goals = crate::execution_core::GoalStore::new(events.clone());
    let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
        "id":"601-goal","session_id":"test-session","objective":"retain original source fence",
        "criteria":[{"id":"checked","statement":"verified","status":"open"}],
        "phase":"execution","scope":"internal","spec_revision":1,"spec_digest":"current",
        "completion":"open","revision":1,"user_sequence":1
    })).unwrap();
    goals.create(goal).unwrap();
    let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
        "identity":{"workspace_id":"workspace","session_id":"test-session","graph_id":running.id,"goal_id":"601-goal","node_id":ticket.node_id},
        "kind":"tool_progress","source":"601","source_revision":1,
        "freshness":{"observed_at_ms":1,"policy_revision":"current"},
        "summary":"source interleaving","fingerprint":"601-first","result_class":"succeeded"
    })).unwrap();
    let source = goals.projection("601-goal").unwrap().unwrap();
    let mut intervening = observation.clone();
    intervening.fingerprint = "601-intervening".into();
    goals.record_observation(intervening).unwrap();
    let latest = goals.projection("601-goal").unwrap().unwrap();
    assert!(latest.stream_revision > source.stream_revision);
    assert_eq!(latest.goal, source.goal);
    outcome.source_revisions.insert("goal:601-goal".into(), source.stream_revision);
    outcome.domain_events.push(goals.observation_event_at(&latest, &observation, "601-original".into()).unwrap());
    let commits = ExecutionCommitService::new(events.clone());
    let error = commits.execution_outcome_sources(&running, &ticket, &outcome).unwrap_err();
    assert!(error.to_string().contains("observation source conflicts with causal read-set"), "{error}");
    assert_eq!(ExecutionGraphStateStore::new(events.clone()).load(&running.id).unwrap(), running);
    assert!(events.event_by_idempotency_key("goal:601-goal", "601-original").unwrap().is_none());
    assert!(matches!(commits.inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
    // Re-evaluating against the same latest source used to build the event
    // permits the original transaction; its source is still protected by CAS.
    outcome.source_revisions.insert("goal:601-goal".into(), latest.stream_revision);
    let sources = commits.execution_outcome_sources(&running, &ticket, &outcome).unwrap();
    let mut later = observation.clone();
    later.fingerprint = "601-after-prepare".into();
    goals.record_observation(later).unwrap();
    let error = commits.transition_execution_outcome(&running, &ticket, &outcome, &sources).err().expect("stale Goal cut must be rejected");
    assert!(matches!(error, ExecutionCommitError::EventStore(crate::RuntimeEventStoreError::StaleRevision { ref stream_id, .. }) if stream_id == "goal:601-goal"));
    assert_eq!(ExecutionGraphStateStore::new(events.clone()).load(&running.id).unwrap(), running);
    assert!(events.event_by_idempotency_key("goal:601-goal", "601-original").unwrap().is_none());
    let final_source = goals.projection("601-goal").unwrap().unwrap();
    outcome.source_revisions.insert("goal:601-goal".into(), final_source.stream_revision);
    outcome.domain_events[0] = goals.observation_event_at(&final_source, &observation, "601-original".into()).unwrap();
    let sources = commits.execution_outcome_sources(&running, &ticket, &outcome).unwrap();
    let receipt = commits.transition_execution_outcome(&running, &ticket, &outcome, &sources).unwrap();
    assert_eq!(receipt.graph.node_results[&ticket.node_id], outcome.result);
    assert!(events.event_by_idempotency_key("goal:601-goal", "601-original").unwrap().is_some());
}

#[tokio::test]
async fn r1_450_readonly_cas_and_reserved_domain_never_commit_partial_node_truth() {
    let (events, running, ticket, mut outcome, calls) =
        running_inline_attempt_450("450 readonly cut").await;
    let commits = ExecutionCommitService::new(events.clone());
    let original = outcome.clone();
    outcome
        .domain_events
        .push(crate::RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: ExecutionNodeBinding::effect_stream_id(&ticket),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.effect.receipt".into(),
                status: Some("completed".into()),
                actor: Some("inline_model".into()),
                refs: Vec::new(),
                payload: serde_json::json!({"forged":true}),
            },
            schema_version: 1,
            idempotency_key: Some("forged".into()),
        });
    assert!(commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .err()
        .expect("reserved domain must be rejected")
        .to_string()
        .contains("reserved"));
    events
        .append(RuntimeEventInput {
            stream_id: "450-read-source".into(),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "fixture.source".into(),
            status: None,
            actor: Some("test".into()),
            refs: Vec::new(),
            payload: serde_json::json!({"revision":1}),
        })
        .unwrap();
    assert!(commits
        .transition_execution_outcome(
            &running,
            &ticket,
            &original,
            &BTreeMap::from([("450-read-source".into(), 0)])
        )
        .is_err());
    assert_eq!(
        ExecutionGraphStateStore::new(events.clone())
            .load(&running.id)
            .unwrap(),
        running
    );
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct PublicationGate521 {
    inner: TestExecutor,
    held_node: String,
    entered: Notify,
    release: Notify,
    starts: Mutex<Vec<String>>,
    polls: Mutex<Vec<String>>,
}

impl PublicationGate521 {
    fn new(held_node: &str) -> Self {
        Self {
            inner: TestExecutor::new(Vec::new(), Duration::ZERO),
            held_node: held_node.into(),
            entered: Notify::new(),
            release: Notify::new(),
            starts: Mutex::new(Vec::new()),
            polls: Mutex::new(Vec::new()),
        }
    }

    fn count(&self, calls: &Mutex<Vec<String>>, node_id: &str) -> usize {
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|id| id.as_str() == node_id)
            .count()
    }
}

#[async_trait]
impl NodeExecutor for PublicationGate521 {
    fn kind(&self) -> &str {
        self.inner.kind()
    }

    fn validate(&self, spec: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        self.inner.validate(spec)
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.starts.lock().unwrap().push(context.node.id.clone());
        self.inner.start(context).await
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.polls.lock().unwrap().push(ticket.node_id.clone());
        self.inner.poll_or_await(ticket).await
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        if ticket.node_id == self.held_node {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}

struct PublicationFixture521 {
    events: Arc<RuntimeEventStore>,
    state: ExecutionGraphStateStore,
    commits: ExecutionCommitService,
    runner: ExecutionGraphRunner,
    executor: Arc<PublicationGate521>,
}

impl PublicationFixture521 {
    fn new(held_node: &str) -> Self {
        let events = Arc::new(RuntimeEventStore::for_test());
        let state = ExecutionGraphStateStore::new(events.clone());
        let commits = ExecutionCommitService::new(events.clone());
        let registry = Arc::new(NodeExecutorRegistry::new());
        let executor = Arc::new(PublicationGate521::new(held_node));
        registry.register(executor.clone()).unwrap();
        let runner = test_runner(registry, state.clone(), commits.clone());
        Self {
            events,
            state,
            commits,
            runner,
            executor,
        }
    }

    async fn spawn_node(
        &self,
        graph_id: &str,
        node_id: &str,
    ) -> tokio::task::JoinHandle<Result<(), ExecutionRunnerError>> {
        let snapshot = self
            .runner
            .pump_snapshot(graph_id, &Default::default())
            .await
            .unwrap();
        let ready = snapshot
            .ready
            .into_iter()
            .filter(|spec| spec.id == node_id)
            .collect();
        let mut prepared = self
            .runner
            .prepare_ready_wave(graph_id, ready)
            .await
            .unwrap();
        assert_eq!(
            prepared.len(),
            1,
            "original node {node_id} must be admitted"
        );
        let prepared = prepared.pop().unwrap();
        assert!(prepared.binding.is_some());
        assert_eq!(
            self.runner
                .state_store()
                .load(graph_id)
                .unwrap()
                .node_statuses
                .get(node_id),
            Some(&ExecutionNodeStatus::Running),
        );
        let runner = self.runner.clone();
        let graph_id = graph_id.to_string();
        tokio::spawn(async move {
            runner
                .execute_prepared_pump_node(&graph_id, prepared, Arc::new(Notify::new()))
                .await
        })
    }

    fn assert_closure(&self, graph: &ExecutionGraph, node_id: &str, expected: bool) {
        let starts = self
            .state
            .binding_starts_page(&graph.id, 0, graph.revision)
            .unwrap()
            .1;
        let (_, binding, position) = starts.iter().find(|(id, _, _)| id == node_id).unwrap();
        assert_eq!(
            self.state
                .binding_lease_closure(graph, node_id, binding, *position)
                .unwrap(),
            expected,
            "original publication/lease closure for {node_id}"
        );
    }
}

async fn join_publication521(driver: tokio::task::JoinHandle<Result<(), ExecutionRunnerError>>) {
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

fn publication_edge521(from: &str, to: &str) -> ExecutionEdge {
    ExecutionEdge {
        from: from.into(),
        to: to.into(),
        kind: ExecutionEdgeKind::DependsOn,
    }
}

#[tokio::test]
async fn r1_521_ready_snapshot_original_closure_race_is_typed_stale_without_marker_or_intent() {
    let fixture = PublicationFixture521::new("predecessor");
    let mut graph = test_graph("521 Ready snapshot versus original callback closure");
    graph.nodes = vec![node("predecessor"), node("unadmitted")];
    let registered = fixture.runner.register(graph).await.unwrap();
    let driver = fixture.spawn_node(&registered.id, "predecessor").await;
    tokio::time::timeout(Duration::from_secs(5), fixture.executor.entered.notified())
        .await
        .unwrap();
    fixture
        .runner
        .pump_snapshot(&registered.id, &Default::default())
        .await
        .unwrap();
    let ready = fixture.state.load(&registered.id).unwrap();
    assert_eq!(
        ready.node_statuses["unadmitted"],
        ExecutionNodeStatus::Ready
    );
    fixture.assert_closure(&ready, "predecessor", false);
    let spec = ready
        .nodes
        .iter()
        .find(|spec| spec.id == "unadmitted")
        .unwrap()
        .clone();
    let ticket = fixture
        .executor
        .start(NodeExecutionContext {
            graph: Arc::new(ready.clone()),
            node: spec.clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    fixture
        .commits
        .validate_unadmitted_binding_start(&ready, &spec, &ticket)
        .unwrap();

    fixture.executor.release.notify_one();
    join_publication521(driver).await;
    let fresh = fixture.state.load(&registered.id).unwrap();
    assert!(fresh.revision > ready.revision);
    fixture.assert_closure(&fresh, "predecessor", true);
    assert_eq!(
        fresh.node_statuses["unadmitted"],
        ExecutionNodeStatus::Ready
    );
    let cursor = fixture.events.durable_commit_cursor().unwrap();
    let assert_stale = |error| match error {
        super::commit_service::ExecutionCommitError::EventStore(
            crate::RuntimeEventStoreError::StaleRevision {
                stream_id,
                expected,
                actual,
            },
        ) => {
            assert_eq!(stream_id, ready.id);
            assert_eq!(expected, ready.revision);
            assert_eq!(actual, fresh.revision);
        }
        other => panic!("legal Graph race must retain typed EventStore::StaleRevision: {other:?}"),
    };
    assert_stale(
        fixture
            .commits
            .validate_unadmitted_binding_start(&ready, &spec, &ticket)
            .unwrap_err(),
    );
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: ticket.attempt,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let root = tempfile::tempdir().unwrap();
    let scopes = ScopeLockManager::persistent(root.path()).unwrap();
    let key = super::events::ExecutionBindingLeaseClosure::fence_key(&ready, &spec.id, &binding);
    let mut typed_error = None;
    let rejected = scopes.acquire_binding_owner(&key, || {
        fixture
            .commits
            .validate_unadmitted_binding_start(&ready, &spec, &ticket)
            .map_err(|error| {
                let message = error.to_string();
                typed_error = Some(error);
                message
            })
    });
    assert!(rejected.is_err());
    assert_stale(typed_error.expect("same-inode pre-marker checker must run"));
    for entry in std::fs::read_dir(root.path().join("binding-owners-v1")).unwrap() {
        assert_eq!(std::fs::metadata(entry.unwrap().path()).unwrap().len(), 0);
    }
    assert_eq!(
        fixture
            .events
            .stream_revision(&ExecutionNodeBinding::effect_stream_id(&ticket))
            .unwrap(),
        0
    );
    assert_eq!(fixture.events.durable_commit_cursor().unwrap(), cursor);
    assert_eq!(fixture.state.load(&ready.id).unwrap(), fresh);
    assert_eq!(
        fixture
            .executor
            .count(&fixture.executor.polls, "unadmitted"),
        0
    );
    fixture
        .commits
        .validate_unadmitted_binding_start(&fresh, &spec, &ticket)
        .unwrap();
    join_publication521(fixture.spawn_node(&ready.id, "unadmitted").await).await;
    assert_eq!(
        fixture
            .executor
            .count(&fixture.executor.polls, "unadmitted"),
        1
    );
}

#[tokio::test]
async fn r1_521_deterministic_successor_cannot_prepare_or_poll_before_original_callback_closure() {
    let fixture = PublicationFixture521::new("predecessor");
    let mut graph = test_graph("521 deterministic publication barrier");
    let successor = node("successor");
    graph.nodes = vec![node("predecessor"), successor.clone()];
    graph.edges = vec![publication_edge521("predecessor", "successor")];
    let registered = fixture.runner.register(graph).await.unwrap();
    let driver = fixture.spawn_node(&registered.id, "predecessor").await;
    tokio::time::timeout(Duration::from_secs(5), fixture.executor.entered.notified())
        .await
        .unwrap();
    let pending = fixture.state.load(&registered.id).unwrap();
    assert_eq!(
        pending.node_statuses["predecessor"],
        ExecutionNodeStatus::Completed
    );
    fixture.assert_closure(&pending, "predecessor", false);
    let snapshot = fixture
        .runner
        .pump_snapshot(&registered.id, &Default::default())
        .await
        .unwrap();
    assert!(!snapshot.ready.iter().any(|spec| spec.id == "successor"));
    let prepared = fixture
        .runner
        .prepare_ready_wave(&registered.id, vec![successor])
        .await
        .unwrap();
    assert!(
        prepared.is_empty(),
        "caller-supplied candidate must not bypass publication"
    );
    assert_eq!(
        fixture
            .executor
            .count(&fixture.executor.starts, "successor"),
        0
    );
    assert_eq!(
        fixture.executor.count(&fixture.executor.polls, "successor"),
        0
    );
    let still_pending = fixture.state.load(&registered.id).unwrap();
    fixture.assert_closure(&still_pending, "predecessor", false);
    assert_eq!(
        fixture
            .state
            .binding_starts_page(&registered.id, 0, still_pending.revision)
            .unwrap()
            .1
            .len(),
        1
    );

    fixture.executor.release.notify_one();
    join_publication521(driver).await;
    fixture.assert_closure(
        &fixture.state.load(&registered.id).unwrap(),
        "predecessor",
        true,
    );
    join_publication521(fixture.spawn_node(&registered.id, "successor").await).await;
    let completed = fixture.state.load(&registered.id).unwrap();
    assert_eq!(
        completed.node_statuses["successor"],
        ExecutionNodeStatus::Completed
    );
    fixture.assert_closure(&completed, "successor", true);
    assert_eq!(
        fixture
            .executor
            .count(&fixture.executor.starts, "successor"),
        1
    );
    assert_eq!(
        fixture.executor.count(&fixture.executor.polls, "successor"),
        1
    );
}

#[tokio::test]
async fn r1_521_any_and_quorum_use_closed_winners_without_waiting_for_nonwinning_publication() {
    for minimum in [1, 2] {
        let fixture = PublicationFixture521::new("nonwinner");
        let mut graph = test_graph(format!("521 closed winners minimum {minimum}"));
        let mut merge = node("merge");
        merge.work = Some(ExecutionWorkContract {
            dependency: if minimum == 1 {
                ExecutionDependencyPolicy::Any {
                    cancel_remaining: false,
                }
            } else {
                ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: false,
                }
            },
            ..ExecutionWorkContract::new(ExecutionWorkRole::CrossCheck)
        });
        graph.nodes = vec![
            node("nonwinner"),
            node("winner-a"),
            node("winner-b"),
            merge.clone(),
        ];
        graph.edges = vec![
            publication_edge521("nonwinner", "merge"),
            publication_edge521("winner-a", "merge"),
            publication_edge521("winner-b", "merge"),
        ];
        let registered = fixture.runner.register(graph).await.unwrap();
        let nonwinner = fixture.spawn_node(&registered.id, "nonwinner").await;
        tokio::time::timeout(Duration::from_secs(5), fixture.executor.entered.notified())
            .await
            .unwrap();
        let pending = fixture.state.load(&registered.id).unwrap();
        assert_eq!(
            pending.node_statuses["nonwinner"],
            ExecutionNodeStatus::Completed
        );
        fixture.assert_closure(&pending, "nonwinner", false);
        let snapshot = fixture
            .runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert!(!snapshot.ready.iter().any(|spec| spec.id == "merge"));
        assert!(fixture
            .runner
            .prepare_ready_wave(&registered.id, vec![merge.clone()])
            .await
            .unwrap()
            .is_empty());
        join_publication521(fixture.spawn_node(&registered.id, "winner-a").await).await;
        if minimum == 2 {
            let snapshot = fixture
                .runner
                .pump_snapshot(&registered.id, &Default::default())
                .await
                .unwrap();
            assert!(
                !snapshot.ready.iter().any(|spec| spec.id == "merge"),
                "one closed winner plus one unclosed Completed status is not quorum two"
            );
            assert!(fixture
                .runner
                .prepare_ready_wave(&registered.id, vec![merge])
                .await
                .unwrap()
                .is_empty());
            join_publication521(fixture.spawn_node(&registered.id, "winner-b").await).await;
        }
        let winners = fixture.state.load(&registered.id).unwrap();
        fixture.assert_closure(&winners, "winner-a", true);
        if minimum == 2 {
            fixture.assert_closure(&winners, "winner-b", true);
        } else {
            assert_eq!(
                fixture.executor.count(&fixture.executor.starts, "winner-b"),
                0
            );
        }
        fixture.assert_closure(&winners, "nonwinner", false);
        join_publication521(fixture.spawn_node(&registered.id, "merge").await).await;
        let merged = fixture.state.load(&registered.id).unwrap();
        assert_eq!(
            merged.node_statuses["merge"],
            ExecutionNodeStatus::Completed
        );
        fixture.assert_closure(&merged, "merge", true);
        fixture.assert_closure(&merged, "nonwinner", false);
        assert_eq!(fixture.executor.count(&fixture.executor.polls, "merge"), 1);
        fixture.executor.release.notify_one();
        join_publication521(nonwinner).await;
        if minimum == 1 {
            join_publication521(fixture.spawn_node(&registered.id, "winner-b").await).await;
        }
        fixture.assert_closure(
            &fixture.state.load(&registered.id).unwrap(),
            "nonwinner",
            true,
        );
    }
}

#[tokio::test]
async fn r1_521_unknown_failed_original_publication_cannot_use_completed_status_as_closed_winner() {
    for minimum in [1, 2] {
        let events = Arc::new(RuntimeEventStore::for_test());
        let state = ExecutionGraphStateStore::new(events.clone());
        let commits = ExecutionCommitService::new(events);
        let registry = Arc::new(NodeExecutorRegistry::new());
        let failed = Arc::new(FailingCallback475 {
            inner: PostCommitExecutor {
                after_commits: AtomicUsize::new(0),
                after_aborts: AtomicUsize::new(0),
                invalid_domain_event: false,
                poll_error: false,
            },
        });
        let healthy = Arc::new(TestExecutor::new(Vec::new(), Duration::ZERO));
        registry.register(failed.clone()).unwrap();
        registry.register(healthy.clone()).unwrap();
        let runner = test_runner(registry, state.clone(), commits);
        let mut graph = test_graph("521 unknown failed publication is not a closed winner");
        let mut unknown = node("unknown");
        unknown.kind = ExecutionNodeKind::InlineModel;
        unknown.executor_kind = "post_commit".into();
        let mut merge = node("merge");
        merge.work = Some(ExecutionWorkContract {
            dependency: if minimum == 1 {
                ExecutionDependencyPolicy::Any {
                    cancel_remaining: false,
                }
            } else {
                ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: false,
                }
            },
            ..ExecutionWorkContract::new(ExecutionWorkRole::CrossCheck)
        });
        graph.nodes = vec![unknown, merge.clone()];
        graph.edges = vec![publication_edge521("unknown", "merge")];
        if minimum == 2 {
            graph.nodes.push(node("closed-winner"));
            graph
                .edges
                .push(publication_edge521("closed-winner", "merge"));
        }
        let registered = runner.register(graph).await.unwrap();
        for node_id in if minimum == 2 {
            vec!["closed-winner", "unknown"]
        } else {
            vec!["unknown"]
        } {
            let ready = runner
                .pump_snapshot(&registered.id, &Default::default())
                .await
                .unwrap()
                .ready;
            let ready = ready
                .into_iter()
                .filter(|spec| spec.id == node_id)
                .collect();
            let mut prepared = runner
                .prepare_ready_wave(&registered.id, ready)
                .await
                .unwrap();
            assert_eq!(prepared.len(), 1);
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                runner.execute_prepared_pump_node(
                    &registered.id,
                    prepared.pop().unwrap(),
                    Arc::new(Notify::new()),
                ),
            )
            .await
            .unwrap();
            if node_id == "unknown" {
                assert!(
                    result.is_err(),
                    "real callback failed; must not manufacture closure"
                );
            } else {
                result.unwrap();
            }
        }
        let durable = state.load(&registered.id).unwrap();
        assert_eq!(
            durable.node_statuses["unknown"],
            ExecutionNodeStatus::Completed
        );
        let starts = state
            .binding_starts_page(&registered.id, 0, durable.revision)
            .unwrap()
            .1;
        for (node_id, binding, position) in starts {
            assert_eq!(
                state
                    .binding_lease_closure(&durable, &node_id, &binding, position)
                    .unwrap(),
                node_id == "closed-winner"
            );
        }
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert!(!snapshot.ready.iter().any(|spec| spec.id == "merge"));
        assert!(runner
            .prepare_ready_wave(&registered.id, vec![merge])
            .await
            .unwrap()
            .is_empty());
        assert!(!healthy.calls.lock().unwrap().iter().any(|id| id == "merge"));
        assert_eq!(failed.inner.after_commits.load(Ordering::SeqCst), 1);
        assert_eq!(failed.inner.after_aborts.load(Ordering::SeqCst), 0);
        assert!(
            runner.recover_graph(&registered.id).await.is_err(),
            "unknown original owner must remain fail-closed"
        );
        assert_eq!(
            state.load(&registered.id).unwrap().node_statuses["unknown"],
            ExecutionNodeStatus::Completed
        );
    }
}

#[test]
fn r1_450_public_outcome_serde_cannot_import_internal_domain_events() {
    let mut value =
        serde_json::to_value(NodeExecutionOutcome::new(completed_result("public"))).unwrap();
    value["domain_events"] = serde_json::json!([{"event":{"kind":"execution.effect.receipt","payload":{"trusted":true}}}]);
    let decoded: NodeExecutionOutcome = serde_json::from_value(value).unwrap();
    assert!(decoded.domain_events.is_empty());
    assert!(serde_json::to_value(decoded)
        .unwrap()
        .get("domain_events")
        .is_none());
}

#[tokio::test]
async fn r1_450_host_transcript_atomic_root_restart_and_forgery_contract() {
    let (events, running, ticket, mut outcome, calls) =
        running_inline_attempt_450("450 Host publication").await;
    let workspace = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
        workspace.path().join("blobs"),
    ));
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let commits =
        ExecutionCommitService::new(events.clone()).with_artifact_store(artifacts.clone());
    let lineage = running.lineage.as_ref().unwrap();
    let empty_digest =
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string();
    let messages = vec![
        serde_json::json!({"role":"assistant","blocks":[{"Text":{"text":"canonical output"}}],"usage":null}),
    ];
    let publication = commits
        .prepare_host_transcript(
            &ticket,
            &lineage.session_id,
            &lineage.turn_id,
            0,
            empty_digest.clone(),
            messages.clone(),
            None,
        )
        .unwrap();
    let publication_key = publication.idempotency_key.clone().unwrap();
    assert_eq!(
        publication.event.payload["body"]["body"]["kind"],
        "artifact_ref"
    );
    ExecutionCommitService::attach_host_transcript(&mut outcome, publication).unwrap();
    let receipt = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap();
    let document = commits.committed_host_transcript(&ticket).unwrap();
    assert_eq!(document.messages, messages);
    assert_eq!(document.ticket, ticket);
    assert_eq!(document.root_control_team_count, None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut records = events
        .list_stream(&running.id)
        .unwrap()
        .into_iter()
        .chain(
            events
                .list_stream(&format!("{}:node:{}", running.id, ticket.node_id))
                .unwrap(),
        )
        .chain(
            events
                .list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket))
                .unwrap(),
        )
        .filter(|event| event.transaction_id == receipt.transaction.transaction_id)
        .collect::<Vec<_>>();
    records.sort_by_key(|event| event.transaction_index);
    let request = crate::AppendTransactionRequest {
        transaction_id: receipt.transaction.transaction_id.clone(),
        expected_streams: receipt
            .transaction
            .stream_revisions
            .iter()
            .map(|source| crate::ExpectedStreamRevision {
                stream_id: source.stream_id.clone(),
                expected_revision: source.expected_revision,
            })
            .collect(),
        events: records
            .into_iter()
            .map(|event| crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: event.stream_id,
                    scope: event.scope,
                    kind: event.kind,
                    status: event.status,
                    actor: event.actor,
                    refs: event.refs,
                    payload: event.payload,
                },
                idempotency_key: event.idempotency_key,
                schema_version: event.schema_version,
            })
            .collect(),
    };
    let position = request
        .events
        .iter()
        .position(|event| event.idempotency_key.as_deref() == Some(publication_key.as_str()))
        .unwrap();
    ExecutionCommitService::validate_host_transcript_publication(&events, &request, position)
        .unwrap();
    let live = artifacts
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.host-transcript+json".into(),
                visibility_scope: format!("session:{}", lineage.session_id),
                expected_bytes: None,
                original_name: None,
            },
            &serde_json::to_vec(&document).unwrap(),
        )
        .await
        .unwrap();
    let mut forged = request.clone();
    forged.events[position].event.payload["body"]["body"]["artifact"] =
        serde_json::to_value(live).unwrap();
    assert!(
        ExecutionCommitService::validate_host_transcript_publication(&events, &forged, position)
            .is_err()
    );
    for field in ["attempt", "session_id", "turn_id", "ticket_digest"] {
        let mut forged = request.clone();
        forged.events[position].event.payload[field] = serde_json::json!("foreign");
        assert!(
            ExecutionCommitService::validate_host_transcript_publication(
                &events, &forged, position
            )
            .is_err(),
            "{field}"
        );
    }
    let mut forged = request.clone();
    forged.events[position].idempotency_key = Some("host-transcript:foreign".into());
    assert!(
        ExecutionCommitService::validate_host_transcript_publication(&events, &forged, position)
            .is_err()
    );
    let mut forged = request.clone();
    forged.events[position].event.payload["body"]["body"] =
        serde_json::json!({"kind":"inline","value":document});
    assert!(
        ExecutionCommitService::validate_host_transcript_publication(&events, &forged, position)
            .is_err()
    );
    let mut forged = request.clone();
    forged
        .events
        .retain(|event| event.event.scope != RuntimeEventScope::ExecutionGraph);
    let position = forged
        .events
        .iter()
        .position(|event| event.event.kind == "execution.host.transcript")
        .unwrap();
    assert!(
        ExecutionCommitService::validate_host_transcript_publication(&events, &forged, position)
            .is_err()
    );
}

#[tokio::test]
async fn r1_450_host_missing_publication_and_oversize_never_become_empty_success() {
    let (events, running, ticket, outcome, _) =
        running_inline_attempt_450("450 missing publication").await;
    let workspace = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
        workspace.path().join("blobs"),
    ));
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let commits = ExecutionCommitService::new(events).with_artifact_store(artifacts);
    let lineage = running.lineage.as_ref().unwrap();
    let digest =
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string();
    let unknown = vec![
        serde_json::json!({"role":"assistant","blocks":[],"usage":null,"unexpected_authority":true}),
    ];
    assert!(commits
        .prepare_host_transcript(
            &ticket,
            &lineage.session_id,
            &lineage.turn_id,
            0,
            digest.clone(),
            unknown,
            None
        )
        .is_err());
    let oversized = vec![
        serde_json::json!({"role":"assistant","blocks":[{"Text":{"text":"x".repeat(512*1024)}}],"usage":null}),
    ];
    assert!(commits
        .prepare_host_transcript(
            &ticket,
            &lineage.session_id,
            &lineage.turn_id,
            0,
            digest,
            oversized,
            None
        )
        .is_err());
    commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap();
    assert!(commits
        .committed_host_transcript(&ticket)
        .unwrap_err()
        .to_string()
        .contains("canonical publication missing"));
}

struct WaitPublicationBackend450 {
    polls: AtomicUsize,
    publications: AtomicUsize,
}

#[async_trait]
impl ScopedNodeBackend for WaitPublicationBackend450 {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        let mut result = completed_result(&ticket.node_id);
        result.status = ExecutionNodeStatus::WaitingInput;
        result.result_ref = Some("450-original-wait".into());
        Ok(NodeExecutionOutcome::new(result))
    }
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.publications.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct WaitPublicationResolver450(Arc<WaitPublicationBackend450>);

impl super::executors::ScopedNodeBackendResolver for WaitPublicationResolver450 {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        (ticket.payload_ref == "payload:durable")
            .then(|| self.0.clone() as Arc<dyn ScopedNodeBackend>)
    }
}

#[tokio::test]
async fn r1_450_original_wait_after_ready_is_reconciled_not_borrowed_for_new_attempt() {
    let events = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(events.clone());
    let state = ExecutionGraphStateStore::new(events);
    let mut graph = test_graph("450 wait historical recovery");
    let mut spec = node("durable");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = "inline_model".into();
    graph.nodes.push(spec);
    let graph = commits.register_graph(graph).unwrap().graph;
    let ready = commits
        .transition_node(
            &graph,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let backend = Arc::new(WaitPublicationBackend450 {
        polls: AtomicUsize::new(0),
        publications: AtomicUsize::new(0),
    });
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(WaitPublicationResolver450(backend.clone())));
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(ready.clone()),
            node: ready.nodes[0].clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let scopes = manual_scope_locks_475(&ready.id);
    let owner = acquire_fixture_owner_475(&scopes, &commits, &ready, "durable", &binding);
    executor.prepare_original_binding(&ticket).unwrap();
    let running = commits
        .bind_and_start_nodes(
            &ready,
            BTreeMap::from([("durable".into(), binding)]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    let outcome = executor.poll_or_await(&ticket).await.unwrap();
    let waiting = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    assert_eq!(
        waiting.node_statuses["durable"],
        ExecutionNodeStatus::WaitingInput
    );
    let ready = commits
        .transition_node(
            &waiting,
            "durable",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let registry = Arc::new(NodeExecutorRegistry::new());
    registry.register(executor).unwrap();
    drop(owner);
    let runner = recovery_runner_475(registry, state.clone(), commits.clone(), &ready.id);
    let recovered = runner.recover_graph(&ready.id).await.unwrap();
    assert_eq!(recovered.node_statuses, ready.node_statuses);
    assert!(recovered.revision > ready.revision);
    assert_eq!(runner.recover_graph(&ready.id).await.unwrap(), recovered);
    assert_eq!(backend.polls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.publications.load(Ordering::SeqCst), 1);
    let mut retry = ticket.clone();
    retry.attempt = 2;
    assert!(matches!(
        commits.inspect_execution_effect(&retry).unwrap(),
        ExecutionEffectState::Fresh
    ));
    assert!(commits
        .validate_fresh_node_admission(&ready, &ready.nodes[0], &retry)
        .is_err());
}

#[tokio::test]
async fn r1_450_explicit_resume_admits_fresh_namespace_without_reusing_uncertain_result() {
    let (events, running, ticket, _discarded, calls) =
        running_inline_attempt_450("450 explicit Resume").await;
    let commits = ExecutionCommitService::new(events.clone());
    let paused = commits
        .apply_command(
            &running,
            &ExecutionGraphCommand::Pause {
                expected_revision: running.revision,
                reason: "operator".into(),
            },
        )
        .unwrap()
        .graph;
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: ticket.payload_ref.clone(),
        backend: Arc::new(PayloadScopedBackend {
            calls: calls.clone(),
        }),
    }));
    registry.register(executor).unwrap();
    let runner = recovery_runner_475(
        registry,
        ExecutionGraphStateStore::new(events.clone()),
        commits.clone(),
        &paused.id,
    );
    let retired = runner.recover_graph(&paused.id).await.unwrap();
    let ready = commits
        .apply_command(
            &retired,
            &ExecutionGraphCommand::Resume {
                expected_revision: retired.revision,
            },
        )
        .unwrap()
        .graph;
    let mut retry = ticket.clone();
    retry.attempt = 2;
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert!(commits
        .validate_fresh_node_admission(&ready, &ready.nodes[0], &retry)
        .is_ok());
    let binding = ExecutionNodeBinding {
        executor_kind: retry.executor_kind.clone(),
        ticket_idempotency_key: retry.idempotency_key.clone(),
        attempt: 2,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let scopes = manual_scope_locks_475(&ready.id);
    let _owner = acquire_fixture_owner_475(&scopes, &commits, &ready, "durable", &binding);
    commits
        .bind_and_start_nodes(
            &ready,
            BTreeMap::from([("durable".into(), binding)]),
            vec![retry.clone()],
        )
        .unwrap();
    assert!(matches!(
        commits.inspect_execution_effect(&retry).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert_ne!(
        ExecutionNodeBinding::effect_stream_id(&ticket),
        ExecutionNodeBinding::effect_stream_id(&retry)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn r1_450_split_completed_receipt_cannot_restore_running_or_reexecute() {
    let (events, running, ticket, outcome, calls) =
        running_inline_attempt_450("450 corrupt split fixture").await;
    let commits = ExecutionCommitService::new(events.clone());
    commits.commit_execution_effect(&ticket, &outcome).unwrap();
    let effect_stream = ExecutionNodeBinding::effect_stream_id(&ticket);
    let effect_before = events.list_stream(&effect_stream).unwrap();
    assert_eq!(effect_before.len(), 2);
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Completed(_)
    ));
    let cursor = events.durable_commit_cursor().unwrap();
    let state = ExecutionGraphStateStore::new(events.clone());
    let registry = Arc::new(NodeExecutorRegistry::new());
    let backend = Arc::new(PayloadScopedBackend {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: ticket.payload_ref.clone(),
        backend: backend.clone(),
    }));
    registry.register(executor).unwrap();
    let runner = recovery_runner_475(registry, state.clone(), commits.clone(), &running.id);
    match runner.recover_graph(&running.id).await.err().expect("split receipt must not restore Running") {
        super::recovery::ExecutionRecoveryError::LeaseReconciliation(reason) => assert_eq!(reason,
            format!("{}/durable/1: original outcome is not durably committed; provider/Host state unknown; no reexecute or Ready admission",running.id)),
        other => panic!("expected exact missing atomic Graph-result rejection, got {other}"),
    }
    assert_eq!(state.load(&running.id).unwrap(), running);
    assert_eq!(events.list_stream(&effect_stream).unwrap(), effect_before);
    assert_eq!(events.durable_commit_cursor().unwrap(), cursor);
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Completed(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn r1_450_existing_lease_closure_does_not_replace_original_result_authentication() {
    let fixture = LeaseRecoveryFixture423::new();
    let (graph, binding, scope, resource, mut fence) =
        fixture.terminal_with_uncommitted_release().await;
    let position = fixture
        .state
        .binding_start_position(&graph, "lease-node-423", &binding)
        .unwrap()
        .unwrap();
    let mut scope = Some(scope);
    let mut resource = Some(resource);
    let mut worktree = None;
    let released = fence
        .release_original(
            &graph,
            "lease-node-423",
            &binding,
            position,
            &mut resource,
            &mut scope,
            &mut worktree,
            &fixture.worktrees,
        )
        .unwrap();
    let closed = fixture
        .commits
        .commit_binding_lease_closure(&graph, &released)
        .unwrap()
        .graph;
    drop(released);
    drop(fence);
    let peer = ScopeLockManager::persistent(fixture.root.path().join("scopes")).unwrap();
    assert!(
        ExecutionGraphRecovery::new(&fixture.state, &fixture.commits, &fixture.registry)
            .recover_with_leases(&closed.id, &peer, &fixture.worktrees, "lease-workspace-423")
            .await
            .is_err()
    );
    assert_eq!(fixture.state.load(&closed.id).unwrap(), closed);
}

#[tokio::test]
async fn r1_450_ordinary_host_running_without_container_lease_is_not_requeued() {
    let (registry, state, commits) = harness();
    let mut graph = test_graph("450 unknown ordinary Host assembly");
    let mut spec = node("ordinary-tools");
    spec.kind = ExecutionNodeKind::ToolBatch;
    spec.executor_kind = "tool_batch".into();
    spec.payload_ref=crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
        "session_id":"test-session","calls":[{"id":"ordinary-call","name":"read_file","input":"{}","depends_on":[]}],"step_snapshots":{}
    })).unwrap();
    graph.nodes.push(spec);
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(
            &graph,
            "ordinary-tools",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: "ordinary-tools".into(),
        executor_kind: "tool_batch".into(),
        service_class: graph.service_class,
        attempt: 1,
        idempotency_key: graph.nodes[0].idempotency_key.clone(),
        payload_ref: graph.nodes[0].payload_ref.clone(),
    };
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let scopes = manual_scope_locks_475(&graph.id);
    let owner = acquire_fixture_owner_475(&scopes, &commits, &graph, "ordinary-tools", &binding);
    let graph = commits
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([("ordinary-tools".into(), binding)]),
            Vec::new(),
        )
        .unwrap()
        .graph;
    let backend = Arc::new(PayloadScopedBackend {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: ticket.payload_ref.clone(),
        backend: backend.clone(),
    }));
    registry.register(executor).unwrap();
    drop(owner);
    let runner = recovery_runner_475(registry, state.clone(), commits.clone(), &graph.id);
    let error = runner.recover_graph(&graph.id).await.unwrap_err();
    match error {
        super::recovery::ExecutionRecoveryError::LeaseReconciliation(reason) => assert_eq!(reason,
            format!("{}/ordinary-tools/1: original outcome is not durably committed; provider/Host state unknown; no reexecute or Ready admission",graph.id)),
        other => panic!("expected exact original provider/Host Unknown, got {other}"),
    }
    assert_eq!(state.load(&graph.id).unwrap(), graph);
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Fresh
    ));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn r1_450_synthesize_missing_terminal_publication_is_not_after_commit_success() {
    for terminal_publication in [false, true] {
        let events = Arc::new(RuntimeEventStore::for_test());
        let commits = ExecutionCommitService::new(events.clone());
        let state = ExecutionGraphStateStore::new(events.clone());
        let mut graph = test_graph("450 synthesize original publication");
        let mut spec = node("terminal");
        spec.kind = ExecutionNodeKind::Synthesize;
        spec.executor_kind = "synthesize".into();
        graph.nodes.push(spec);
        let graph = commits.register_graph(graph).unwrap().graph;
        let graph = commits
            .transition_node(
                &graph,
                "terminal",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(super::executors::SynthesizeNodeExecutor::new());
        executor.install_resolver(Arc::new(TerminalResolver {
            graph_id: graph.id.clone(),
            backend: Arc::new(TerminalBackend {
                calls: calls.clone(),
            }),
        }));
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node: graph.nodes[0].clone(),
                attempt: 1,
            })
            .await
            .unwrap();
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let scopes = manual_scope_locks_475(&graph.id);
        let owner = acquire_fixture_owner_475(&scopes, &commits, &graph, "terminal", &binding);
        let graph = commits
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([("terminal".into(), binding)]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        let mut outcome = executor.poll_or_await(&ticket).await.unwrap();
        if terminal_publication {
            outcome.terminal_presentation = Some(
                serde_json::from_value(serde_json::json!({
                    "presentation_id":"450-synthesize","attempt_id":"450-attempt",
                    "envelope_id":"450-envelope","envelope_revision":1,"state":"committed",
                    "answer_origin":"runtime_verified_fallback","generated_at_ms":1
                }))
                .unwrap(),
            );
        }
        let committed = commits
            .transition_execution_outcome(&graph, &ticket, &outcome, &BTreeMap::new())
            .unwrap()
            .graph;
        drop(owner);
        let registry = Arc::new(NodeExecutorRegistry::new());
        registry.register(executor).unwrap();
        let runner = recovery_runner_475(registry, state.clone(), commits, &graph.id);
        let recovered = runner.recover_graph(&graph.id).await;
        assert!(recovered.is_err());
        assert_eq!(state.load(&graph.id).unwrap(), committed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            events
                .stream_revision(&format!(
                    "session:{}",
                    committed.lineage.as_ref().unwrap().session_id
                ))
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn r1_450_host_append_rejects_coordinated_activity_forgery_against_original_start() {
    let (events, start, ticket, mut outcome, calls) =
        running_inline_attempt_450("450 Host original start anchor").await;
    let workspace = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
        workspace.path().join("blobs"),
    ));
    events.bind_artifact_store(artifacts.clone()).unwrap();
    let commits = ExecutionCommitService::new(events.clone()).with_artifact_store(artifacts);
    let running = commits
        .commit_recovery(&start, start.clone(), Vec::new(), Vec::new())
        .unwrap()
        .graph;
    let lineage = start.lineage.as_ref().unwrap();
    let publication = commits.prepare_host_transcript(&ticket,&lineage.session_id,&lineage.turn_id,0,
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        vec![serde_json::json!({"role":"assistant","blocks":[{"Text":{"text":"start-authenticated"}}],"usage":null})],None).unwrap();
    assert_eq!(
        publication.event.activity_binding().unwrap().revision,
        start.revision
    );
    ExecutionCommitService::attach_host_transcript(&mut outcome, publication).unwrap();
    let request = commits
        .execution_outcome_request_for_test(&running, &ticket, &outcome)
        .unwrap();
    let cursor = events.durable_commit_cursor().unwrap();
    let node_stream = format!("{}:node:{}", running.id, ticket.node_id);
    let node_head = events.stream_revision(&node_stream).unwrap();
    for (field, value) in [
        ("generation", serde_json::json!(lineage.generation + 1)),
        ("task_id", serde_json::json!("forged-task")),
        ("root_task_id", serde_json::json!("forged-root")),
        (
            "parent_activity_id",
            serde_json::json!("activity:forged-parent"),
        ),
        (
            "initiator_activity_id",
            serde_json::json!("activity:forged-initiator"),
        ),
        ("agent_run_id", serde_json::json!("forged-run")),
    ] {
        let mut forged = request.clone();
        let mut stamped = 0;
        for input in &mut forged.events {
            if matches!(
                input.event.kind.as_str(),
                "execution.host.transcript" | "execution_node.transitioned"
            ) || input.event.scope == RuntimeEventScope::ExecutionGraph
            {
                input.event.payload["_runtime_activity_binding"][field] = value.clone();
                stamped += 1;
            }
        }
        assert_eq!(stamped, 3);
        let position = forged
            .events
            .iter()
            .position(|input| input.event.kind == "execution.host.transcript")
            .unwrap();
        let error = ExecutionCommitService::validate_host_transcript_publication(
            &events, &forged, position,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("authenticated original start"),
            "{field}: {error}"
        );
        assert!(events.append_transaction(forged).is_err(), "{field}");
        assert_eq!(events.durable_commit_cursor().unwrap(), cursor);
        assert_eq!(events.stream_revision(&node_stream).unwrap(), node_head);
        assert_eq!(
            events
                .stream_revision(&ExecutionNodeBinding::effect_stream_id(&ticket))
                .unwrap(),
            1
        );
    }
    let committed = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    assert!(committed.revision > start.revision);
    assert_eq!(
        commits
            .committed_host_transcript(&ticket)
            .unwrap()
            .messages
            .len(),
        1
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn r1_450b_synthesize_message_metadata_pairs_exact_terminal_or_original_replan() {
    for terminal in [false, true] {
        let events = Arc::new(RuntimeEventStore::for_test());
        let root = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            root.path().join("blobs"),
        ));
        events.bind_artifact_store(artifacts.clone()).unwrap();
        let commits = ExecutionCommitService::new(events.clone()).with_artifact_store(artifacts);
        let mut graph = test_graph("450b original Synthesize publication");
        let mut spec = node("450b-terminal");
        spec.kind = ExecutionNodeKind::Synthesize;
        spec.executor_kind = "synthesize".into();
        graph.nodes.push(spec);
        let graph = commits.register_graph(graph).unwrap().graph;
        let graph = commits
            .transition_node(
                &graph,
                "450b-terminal",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(super::executors::SynthesizeNodeExecutor::new());
        executor.install_resolver(Arc::new(TerminalResolver {
            graph_id: graph.id.clone(),
            backend: Arc::new(TerminalBackend {
                calls: calls.clone(),
            }),
        }));
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node: graph.nodes[0].clone(),
                attempt: 1,
            })
            .await
            .unwrap();
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let scopes = manual_scope_locks_475(&graph.id);
        let owner = acquire_fixture_owner_475(&scopes, &commits, &graph, "450b-terminal", &binding);
        let running = commits
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([("450b-terminal".into(), binding.clone())]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        let mut outcome = executor.poll_or_await(&ticket).await.unwrap();
        let text = "450b exact durable terminal";
        let completion = terminal.then_some(harness_contract::goal::GoalCompletion::Failed);
        let messages = if terminal {
            use sha2::{Digest, Sha256};
            outcome.result.result_ref = Some(format!(
                "turn-result:{}:{:x}",
                ticket.graph_id,
                Sha256::digest(text.as_bytes())
            ));
            vec![
                serde_json::json!({"role":"assistant","blocks":[{"Text":{"text":text}}],"usage":null}),
            ]
        } else {
            outcome.replan = Some(ExecutionGraphReplan {
                nodes: vec![node("450b-continuation")],
                edges: vec![ExecutionEdge {
                    from: ticket.node_id.clone(),
                    to: "450b-continuation".into(),
                    kind: ExecutionEdgeKind::DependsOn,
                }],
                reason: "original input barrier continuation".into(),
                framework_authored: true,
            });
            Vec::new()
        };
        let lineage = running.lineage.as_ref().unwrap();
        let digest =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string();
        assert!(commits
            .prepare_synthesize_transcript(
                &ticket,
                &lineage.session_id,
                &lineage.turn_id,
                0,
                digest.clone(),
                messages.clone(),
                Some(harness_contract::goal::GoalCompletion::Open)
            )
            .is_err());
        let publication = commits
            .prepare_synthesize_transcript(
                &ticket,
                &lineage.session_id,
                &lineage.turn_id,
                0,
                digest,
                messages.clone(),
                completion,
            )
            .unwrap();
        ExecutionCommitService::attach_host_transcript(&mut outcome, publication).unwrap();
        let mut forged = outcome.clone();
        if terminal {
            forged.result.result_ref = Some("turn-result:forged".into());
        } else {
            forged.replan = None;
        }
        let request = commits
            .execution_outcome_request_for_test(&running, &ticket, &forged)
            .unwrap();
        let cursor = events.durable_commit_cursor().unwrap();
        assert!(events.append_transaction(request).is_err());
        assert_eq!(events.durable_commit_cursor().unwrap(), cursor);
        let committed = commits
            .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
            .unwrap()
            .graph;
        drop(owner);
        let document = commits.committed_host_transcript(&ticket).unwrap();
        assert_eq!(document.terminal_completion, completion);
        assert_eq!(document.messages, messages);
        assert_eq!(document.root_control_team_count, None);
        let registry = Arc::new(NodeExecutorRegistry::new());
        registry.register(executor).unwrap();
        let runner = recovery_runner_475(
            registry,
            ExecutionGraphStateStore::new(events.clone()),
            commits,
            &running.id,
        );
        let closed = runner.recover_graph(&running.id).await.unwrap();
        assert_eq!(closed.node_statuses, committed.node_statuses);
        assert_eq!(closed.node_results, committed.node_results);
        let state = ExecutionGraphStateStore::new(events.clone());
        let position = state
            .binding_start_position(&closed, &ticket.node_id, &binding)
            .unwrap()
            .unwrap();
        assert!(state
            .binding_lease_closure(&closed, &ticket.node_id, &binding, position)
            .unwrap());
        assert_eq!(runner.recover_graph(&running.id).await.unwrap(), closed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            events
                .stream_revision(&format!("session-terminal:{}", running.id))
                .unwrap(),
            0
        );
    }
}

fn manual_scope_locks_475(graph_id: &str) -> Arc<ScopeLockManager> {
    use sha2::{Digest, Sha256};
    Arc::new(
        ScopeLockManager::persistent(
            std::env::temp_dir()
                .join("cowd475-manual-owner")
                .join(format!("{:x}", Sha256::digest(graph_id.as_bytes()))),
        )
        .unwrap(),
    )
}

fn recovery_runner_475(
    registry: Arc<NodeExecutorRegistry>,
    state: ExecutionGraphStateStore,
    commits: ExecutionCommitService,
    graph_id: &str,
) -> ExecutionGraphRunner {
    let scopes = manual_scope_locks_475(graph_id);
    let path = std::env::temp_dir()
        .join("cowd475-manual-worktrees")
        .join(uuid::Uuid::new_v4().to_string());
    ExecutionGraphRunner::new(
        registry,
        state,
        commits,
        Arc::new(ExecutionResourceManager::new([])),
        scopes,
        Arc::new(WorktreeLeaseManager::open(path.join("leases.json")).unwrap()),
        "475-manual",
        std::env::temp_dir(),
    )
}

fn fixture_ticket_475(
    graph: &ExecutionGraph,
    node: &str,
    binding: &ExecutionNodeBinding,
) -> NodeExecutionTicket {
    let spec = graph.nodes.iter().find(|spec| spec.id == node).unwrap();
    NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node.into(),
        executor_kind: binding.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: binding.attempt,
        idempotency_key: binding.ticket_idempotency_key.clone(),
        payload_ref: spec.payload_ref.clone(),
    }
}

fn acquire_fixture_owner_475(
    scopes: &ScopeLockManager,
    commits: &ExecutionCommitService,
    graph: &ExecutionGraph,
    node: &str,
    binding: &ExecutionNodeBinding,
) -> super::resources::BindingOwnerFence {
    let ticket = fixture_ticket_475(graph, node, binding);
    let spec = graph.nodes.iter().find(|spec| spec.id == node).unwrap();
    let key = super::events::ExecutionBindingLeaseClosure::fence_key(graph, node, binding);
    scopes
        .acquire_binding_owner(&key, || {
            commits
                .validate_unadmitted_binding_start(graph, spec, &ticket)
                .map_err(|error| error.to_string())
        })
        .unwrap()
}

#[tokio::test]
async fn r1_475_pending_callback_cancel_never_closes_empty_binding_early() {
    let events = Arc::new(RuntimeEventStore::for_test());
    let state = ExecutionGraphStateStore::new(events.clone());
    let commits = ExecutionCommitService::new(events.clone());
    let registry = Arc::new(NodeExecutorRegistry::new());
    registry
        .register(Arc::new(TestExecutor::new(Vec::new(), Duration::ZERO)))
        .unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    registry
        .register(Arc::new(BlockingPostCommitExecutor {
            entered: entered.clone(),
            release: release.clone(),
        }))
        .unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("475 positive callback gate");
    let mut spec = node("callback475");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = "blocking_post_commit".into();
    graph.nodes.push(spec);
    let graph_id = graph.id.clone();
    let driver = tokio::spawn({
        let runner = runner.clone();
        async move { runner.start(graph).await }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let durable = state.load(&graph_id).unwrap();
    assert_eq!(
        durable.node_statuses["callback475"],
        ExecutionNodeStatus::Completed
    );
    let (_, starts) = state
        .binding_starts_page(&graph_id, 0, durable.revision)
        .unwrap();
    let (_, binding, position) = starts
        .into_iter()
        .find(|(node, _, _)| node == "callback475")
        .unwrap();
    assert!(
        binding.resource_lease_refs.is_empty()
            && binding.scope_lease_ref.is_none()
            && binding.worktree_lease_ref.is_none()
    );
    assert!(!state
        .binding_lease_closure(&durable, "callback475", &binding, position)
        .unwrap());
    let mut cancel = tokio::spawn({
        let runner = runner.clone();
        let graph_id = graph_id.clone();
        async move {
            runner
                .command(
                    &graph_id,
                    ExecutionGraphCommand::Cancel {
                        expected_revision: durable.revision,
                        reason: "concurrent cancel".into(),
                    },
                )
                .await
        }
    });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut cancel)
        .await
        .is_err());
    let pending = state.load(&graph_id).unwrap();
    assert!(!state
        .binding_lease_closure(&pending, "callback475", &binding, position)
        .unwrap());
    release.notify_one();
    match tokio::time::timeout(Duration::from_secs(5), cancel)
        .await
        .unwrap()
        .unwrap()
    {
        Ok(_) => {}
        Err(ExecutionRunnerError::Commit(
            super::commit_service::ExecutionCommitError::StaleRevision { .. }
            | super::commit_service::ExecutionCommitError::EventStore(
                crate::RuntimeEventStoreError::StaleRevision { .. },
            ),
        )) => {}
        Err(error) => panic!("unexpected cancellation error: {error}"),
    }
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let closed = state.load(&graph_id).unwrap();
    assert!(state
        .binding_lease_closure(&closed, "callback475", &binding, position)
        .unwrap());
    assert_eq!(
        closed.node_statuses["callback475"],
        ExecutionNodeStatus::Completed
    );
}

struct FailingCallback475 {
    inner: PostCommitExecutor,
}

#[tokio::test]
async fn r1_704_runtime_cancel_tree_retries_original_publication_before_terminal_commit() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let executor = Arc::new(RecoverableCallback704 {
        inner: PostCommitExecutor {
            after_commits: AtomicUsize::new(0),
            after_aborts: AtomicUsize::new(0),
            invalid_domain_event: false,
            poll_error: false,
        },
        publish: AtomicBool::new(false),
        polls: AtomicUsize::new(0),
        recovery: None,
    });
    services
        .executor_registry()
        .register(executor.clone())
        .unwrap();
    let mut graph = test_graph("704 production Runtime cancellation route");
    for name in ["original704", "successor704"] {
        let mut spec = node(name);
        spec.kind = ExecutionNodeKind::InlineModel;
        spec.executor_kind = executor.kind().into();
        graph.nodes.push(spec);
    }
    graph
        .edges
        .push(publication_edge521("original704", "successor704"));
    let graph = services
        .execution_supervisor()
        .register_graph(graph)
        .await
        .unwrap();
    assert!(services
        .execution_supervisor()
        .drive_registered(&graph.id)
        .await
        .is_err());
    let before = services.graph_state_store().load(&graph.id).unwrap();
    let (_, binding, position) = services
        .graph_state_store()
        .binding_starts_page(&graph.id, 0, before.revision)
        .unwrap()
        .1
        .into_iter()
        .next()
        .unwrap();
    let error = services
        .cancel_execution_tree(&graph.id, "704 cancel original")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("704 original publication"));
    assert_eq!(
        services.graph_state_store().load(&graph.id).unwrap(),
        before
    );
    executor.publish.store(true, Ordering::SeqCst);
    let cancelled = services
        .cancel_execution_tree(&graph.id, "704 cancel original")
        .await
        .unwrap();
    assert_eq!(cancelled, vec![graph.id.clone()]);
    let after = services.graph_state_store().load(&graph.id).unwrap();
    assert_eq!(
        after.node_statuses["original704"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        after.node_statuses["successor704"],
        ExecutionNodeStatus::Cancelled
    );
    assert!(services
        .graph_state_store()
        .binding_lease_closure(&after, "original704", &binding, position)
        .unwrap());
    assert_eq!(executor.polls.load(Ordering::SeqCst), 1);
    assert!(services
        .cancel_execution_tree(&graph.id, "704 idempotent cancel")
        .await
        .unwrap()
        .is_empty());
    services.shutdown_execution().await;
}

struct RecoverableCallback704 {
    inner: PostCommitExecutor,
    publish: AtomicBool,
    polls: AtomicUsize,
    recovery: Option<CallbackRecovery704>,
}

struct CallbackRecovery704 {
    events: Arc<RuntimeEventStore>,
    cancelled: bool,
    crash: Option<(bool, Arc<Notify>)>,
}

impl CallbackRecovery704 {
    async fn finish(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        if let Some((false, entered)) = &self.crash {
            entered.notify_one();
            std::future::pending::<()>().await;
        }
        let stream = format!(
            "callback704:{}:{}:{}",
            ticket.graph_id, ticket.node_id, ticket.attempt
        );
        self.events
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!("{stream}:finish"),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: stream.clone(),
                    expected_revision: 0,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: stream.clone(),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "test.callback.receipt".into(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::json!({"ticket": ticket, "cancelled": self.cancelled}),
                    },
                    idempotency_key: Some(format!("{stream}:receipt")),
                    schema_version: 1,
                }],
            })
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        if let Some((true, entered)) = &self.crash {
            entered.notify_one();
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "704 callback receipt durable but acknowledgement lost".into(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl NodeExecutor for RecoverableCallback704 {
    fn kind(&self) -> &str {
        match &self.recovery {
            Some(recovery) if recovery.cancelled => "synthesize",
            Some(_) => "inline_model",
            None => self.inner.kind(),
        }
    }
    fn validate(&self, spec: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        self.inner.validate(spec)
    }
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        let mut ticket = self.inner.start(context).await?;
        ticket.executor_kind = self.kind().into();
        Ok(ticket)
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self
            .recovery
            .as_ref()
            .is_some_and(|recovery| recovery.cancelled)
        {
            let mut result = completed_result(&ticket.node_id);
            result.status = ExecutionNodeStatus::Cancelled;
            result.result_ref = None;
            result.failure = Some(harness_contract::execution_graph::ExecutionFailure {
                kind: "terminal_presentation_cancelled".into(),
                message: "terminal presentation cancelled".into(),
                retryable: false,
                evidence_refs: Vec::new(),
            });
            return Ok(NodeExecutionOutcome::new(result));
        }
        self.inner.poll_or_await(ticket).await
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        if let Some(recovery) = &self.recovery {
            self.inner.after_commits.fetch_add(1, Ordering::SeqCst);
            if recovery.cancelled {
                return Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: "cancelled synthesis has no transcript to commit".into(),
                });
            }
            return recovery.finish(ticket).await;
        }
        if !self.publish.load(Ordering::SeqCst) {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "704 original publication not yet available".into(),
            });
        }
        self.inner.after_commit(ticket).await
    }

    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        reason: &str,
    ) -> Result<(), NodeExecutorError> {
        if let Some(recovery) = &self.recovery {
            self.inner.after_aborts.fetch_add(1, Ordering::SeqCst);
            if !recovery.cancelled {
                return Err(NodeExecutorError::Cancel {
                    node_id: ticket.node_id.clone(),
                    reason: "committed publication must not become abort".into(),
                });
            }
            return recovery.finish(ticket).await;
        }
        self.inner.after_abort(ticket, reason).await
    }
}

#[tokio::test]
async fn r1_704_hot_recovery_finishes_original_callback_without_stealing_or_reexecuting() {
    let events = Arc::new(RuntimeEventStore::for_test());
    let state = ExecutionGraphStateStore::new(events.clone());
    let commits = ExecutionCommitService::new(events);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(RecoverableCallback704 {
        inner: PostCommitExecutor {
            after_commits: AtomicUsize::new(0),
            after_aborts: AtomicUsize::new(0),
            invalid_domain_event: false,
            poll_error: false,
        },
        publish: AtomicBool::new(false),
        polls: AtomicUsize::new(0),
        recovery: None,
    });
    registry.register(executor.clone()).unwrap();
    let workspace_id = format!("704-hot-{}", uuid::Uuid::new_v4());
    let resource_root = std::env::temp_dir()
        .join("cowd704-hot-recovery")
        .join(&workspace_id);
    let scope_locks = Arc::new(ScopeLockManager::persistent(resource_root.join("scopes")).unwrap());
    let worktree_leases = Arc::new(WorktreeLeaseManager::open(resource_root.join("leases.json")).unwrap());
    let resources = Arc::new(ExecutionResourceManager::new([
        (ExecutionResourceKind::Provider, ResourceQuota::new(1, 4, 8).unwrap()),
        (ExecutionResourceKind::Agent, ResourceQuota::new(1, 4, 4).unwrap()),
        (ExecutionResourceKind::Tool, ResourceQuota::new(1, 4, 8).unwrap()),
    ]));
    let runner = ExecutionGraphRunner::new(
        registry.clone(), state.clone(), commits.clone(), resources,
        scope_locks.clone(), worktree_leases.clone(), workspace_id.clone(),
        std::env::temp_dir(),
    );
    let mut graph = test_graph("704 original hot callback recovery");
    let mut spec = node("retained704");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = executor.kind().into();
    graph.nodes.push(spec);
    let id = graph.id.clone();
    assert!(runner.start(graph).await.is_err());
    let before = state.load(&id).unwrap();
    assert_eq!(
        before.node_statuses["retained704"],
        ExecutionNodeStatus::Completed
    );
    let (_, binding, position) = state
        .binding_starts_page(&id, 0, before.revision)
        .unwrap()
        .1
        .into_iter()
        .next()
        .unwrap();
    assert!(runner.recover_graph(&id).await.is_err());
    assert_eq!(state.load(&id).unwrap(), before);
    executor.publish.store(true, Ordering::SeqCst);
    assert!(ExecutionGraphRecovery::new(&state, &commits, &registry)
        .recover_with_leases(
            &id,
            &scope_locks,
            &worktree_leases,
            &workspace_id
        )
        .await
        .is_err());
    // A generic Poll can also mean that the receipt was written and its
    // acknowledgement lost. Changing this fixture flag is not proof that
    // the original callback is safe to run again.
    assert!(matches!(runner.recover_graph(&id).await,
        Err(super::recovery::ExecutionRecoveryError::LeaseReconciliation(_))));
    let retained = state.load(&id).unwrap();
    assert_eq!(retained, before);
    assert!(!state.binding_lease_closure(&retained, "retained704", &binding, position).unwrap());
    assert_eq!(executor.polls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.inner.after_commits.load(Ordering::SeqCst), 0);
    let stale = runner
        .command(
            &id,
            ExecutionGraphCommand::Cancel {
                expected_revision: retained.revision.checked_sub(1).expect("registered graph has a prior revision"),
                reason: "704 stale command must not bypass CAS".into(),
            },
        )
        .await;
    assert!(matches!(stale, Err(ExecutionRunnerError::Commit(
        ExecutionCommitError::StaleRevision { .. }
    ))));
    assert_eq!(state.load(&id).unwrap(), retained);
    assert!(runner.recover_graph("foreign-704").await.is_err());
}

async fn cold_callback_recovery_704(after_effect: bool) {
    for cancelled in [false, true] {
        let backend = Arc::new(crate::runtime_event_store::EphemeralRuntimeEventStore::new());
        let events = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
        let state = ExecutionGraphStateStore::new(events.clone());
        let commits = ExecutionCommitService::new(events.clone());
        let entered = Arc::new(Notify::new());
        let executor = Arc::new(RecoverableCallback704 {
            inner: PostCommitExecutor {
                after_commits: AtomicUsize::new(0),
                after_aborts: AtomicUsize::new(0),
                invalid_domain_event: false,
                poll_error: false,
            },
            publish: AtomicBool::new(true),
            polls: AtomicUsize::new(0),
            recovery: Some(CallbackRecovery704 {
                events: events.clone(),
                cancelled,
                crash: Some((after_effect, entered.clone())),
            }),
        });
        let registry = Arc::new(NodeExecutorRegistry::new());
        registry.register(executor.clone()).unwrap();
        let mut graph = test_graph("704 cold original callback recovery");
        let mut spec = node("cold704");
        spec.kind = if cancelled {
            ExecutionNodeKind::Synthesize
        } else {
            ExecutionNodeKind::InlineModel
        };
        spec.executor_kind = executor.kind().into();
        graph.nodes.push(spec);
        let graph_id = graph.id.clone();
        let runner =
            recovery_runner_475(registry.clone(), state.clone(), commits.clone(), &graph_id);
        let driver = tokio::spawn({
            let runner = runner.clone();
            async move { runner.start(graph).await }
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let before = state.load(&graph_id).unwrap();
        let (_, binding, position) = state
            .binding_starts_page(&graph_id, 0, before.revision)
            .unwrap()
            .1
            .into_iter()
            .next()
            .unwrap();
        let ticket = fixture_ticket_475(&before, "cold704", &binding);
        let receipt_stream = format!("callback704:{graph_id}:cold704:{}", binding.attempt);
        let original_effect = events
            .list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket))
            .unwrap();
        assert_eq!(original_effect.len(), 2);
        assert!(matches!(
            commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Completed(_)
        ));
        assert_eq!(
            before.node_statuses["cold704"],
            if cancelled {
                ExecutionNodeStatus::Cancelled
            } else {
                ExecutionNodeStatus::Completed
            }
        );
        assert!(!state
            .binding_lease_closure(&before, "cold704", &binding, position)
            .unwrap());
        assert_eq!(
            events.list_stream(&receipt_stream).unwrap().len(),
            usize::from(after_effect)
        );
        assert_eq!(executor.polls.load(Ordering::SeqCst), 1);
        let scopes = manual_scope_locks_475(&graph_id);
        assert_eq!(
            commits
                .binding_callback_disposition(&before, "cold704", &binding)
                .unwrap(),
            if cancelled {
                super::commit_service::CallbackDisposition::Abort
            } else {
                super::commit_service::CallbackDisposition::Commit
            }
        );
        assert_eq!(
            executor.inner.after_commits.load(Ordering::SeqCst),
            usize::from(!cancelled)
        );
        assert_eq!(
            executor.inner.after_aborts.load(Ordering::SeqCst),
            usize::from(cancelled)
        );
        let fence =
            super::events::ExecutionBindingLeaseClosure::fence_key(&before, "cold704", &binding);
        assert!(scopes.try_recover_binding_owner(&fence).unwrap().is_none());
        if after_effect {
            let error = tokio::time::timeout(Duration::from_secs(5), driver)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert!(error.to_string().contains("acknowledgement lost"));
        } else {
            driver.abort();
            assert!(driver.await.unwrap_err().is_cancelled());
        }
        let old_registry = Arc::downgrade(&registry);
        let old_executor = Arc::downgrade(&executor);
        let old_events = Arc::downgrade(&events);
        drop(runner);
        drop(registry);
        drop(executor);
        drop(commits);
        drop(state);
        drop(events);
        if !after_effect {
            // Cancelling the observer is not process death: the pending original
            // callback still owns its fence and has not published a receipt.
            let retained_events = old_events.upgrade().expect("original callback retains its event owner");
            let retained_executor = old_executor.upgrade().expect("original executor remains alive");
            assert!(retained_events.list_stream(&receipt_stream).unwrap().is_empty());
            assert_eq!(retained_events.list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap(), original_effect);
            let retained_state = ExecutionGraphStateStore::new(retained_events.clone());
            let retained_graph = retained_state.load(&graph_id).unwrap();
            assert!(!retained_state.binding_lease_closure(&retained_graph, "cold704", &binding, position).unwrap());
            assert_eq!(retained_executor.polls.load(Ordering::SeqCst), 1);
            assert!(scopes.try_recover_binding_owner(&fence).unwrap().is_none());
            continue;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while old_registry.upgrade().is_some()
                || old_executor.upgrade().is_some()
                || old_events.upgrade().is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let owner = scopes
            .try_recover_binding_owner(&fence)
            .unwrap()
            .expect("original owner must be lost before rebuilding");
        drop(owner);
        drop(scopes);

        let events = Arc::new(RuntimeEventStore::from_backend(backend));
        let state = ExecutionGraphStateStore::new(events.clone());
        let commits = ExecutionCommitService::new(events.clone());
        let registry = Arc::new(NodeExecutorRegistry::new());
        let executor = Arc::new(RecoverableCallback704 {
            inner: PostCommitExecutor {
                after_commits: AtomicUsize::new(0),
                after_aborts: AtomicUsize::new(0),
                invalid_domain_event: false,
                poll_error: false,
            },
            publish: AtomicBool::new(true),
            polls: AtomicUsize::new(0),
            recovery: Some(CallbackRecovery704 {
                events: events.clone(),
                cancelled,
                crash: None,
            }),
        });
        registry.register(executor.clone()).unwrap();
        let runner = recovery_runner_475(registry, state.clone(), commits.clone(), &graph_id);
        let recovered =
            tokio::time::timeout(Duration::from_secs(5), runner.recover_graph(&graph_id))
                .await
                .unwrap()
                .unwrap();
        assert!(state
            .binding_lease_closure(&recovered, "cold704", &binding, position)
            .unwrap());
        assert_eq!(recovered.node_statuses, before.node_statuses);
        assert_eq!(
            recovered.recovery_cursor.node_attempts,
            before.recovery_cursor.node_attempts
        );
        assert_eq!(
            events
                .list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket))
                .unwrap(),
            original_effect
        );
        assert_eq!(events.list_stream(&receipt_stream).unwrap().len(), 1);
        assert_eq!(executor.polls.load(Ordering::SeqCst), 0);
        assert_eq!(
            executor.inner.after_commits.load(Ordering::SeqCst),
            usize::from(!cancelled)
        );
        assert_eq!(
            executor.inner.after_aborts.load(Ordering::SeqCst),
            usize::from(cancelled)
        );
        let stale = ExecutionNodeBinding {
            attempt: binding.attempt + 1,
            ..binding.clone()
        };
        assert!(commits
            .binding_callback_disposition(&recovered, "cold704", &stale)
            .is_err());
        assert_eq!(runner.recover_graph(&graph_id).await.unwrap(), recovered);
        assert_eq!(events.list_stream(&receipt_stream).unwrap().len(), 1);
        assert_eq!(
            executor.inner.after_commits.load(Ordering::SeqCst),
            usize::from(!cancelled)
        );
        assert_eq!(
            executor.inner.after_aborts.load(Ordering::SeqCst),
            usize::from(cancelled)
        );
        assert!(runner
            .command(
                &graph_id,
                ExecutionGraphCommand::Cancel {
                    expected_revision: before.revision,
                    reason: "late original notification".into(),
                }
            )
            .await
            .is_err());
        assert_eq!(state.load(&graph_id).unwrap(), recovered);
    }
}

#[tokio::test]
async fn r1_704_cold_recovery_after_result_before_callback_preserves_original_binding() {
    cold_callback_recovery_704(false).await;
}

#[tokio::test]
async fn r1_704_cold_recovery_after_callback_before_confirmation_is_idempotent() {
    cold_callback_recovery_704(true).await;
}

#[async_trait]
impl NodeExecutor for FailingCallback475 {
    fn kind(&self) -> &str {
        "post_commit"
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        self.inner.validate(node)
    }
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.inner.start(context).await
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.inner.poll_or_await(ticket).await
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.inner.after_commits.fetch_add(1, Ordering::SeqCst);
        Err(NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason: "475 publication failed".into(),
        })
    }
    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), NodeExecutorError> {
        self.inner.after_aborts.fetch_add(1, Ordering::SeqCst);
        Err(NodeExecutorError::Cancel {
            node_id: ticket.node_id.clone(),
            reason: "must not mask committed publication".into(),
        })
    }
}

#[tokio::test]
async fn r1_475_failed_callback_cancel_cannot_replace_it_with_abort_or_closure() {
    let events = Arc::new(RuntimeEventStore::for_test());
    let state = ExecutionGraphStateStore::new(events.clone());
    let commits = ExecutionCommitService::new(events);
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(FailingCallback475 {
        inner: PostCommitExecutor {
            after_commits: AtomicUsize::new(0),
            after_aborts: AtomicUsize::new(0),
            invalid_domain_event: false,
            poll_error: false,
        },
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits);
    let mut graph = test_graph("475 failed callback");
    let mut spec = node("failed-callback475");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = "post_commit".into();
    graph.nodes.push(spec);
    let id = graph.id.clone();
    assert!(runner.start(graph).await.is_err());
    let graph = state.load(&id).unwrap();
    assert_eq!(
        graph.node_statuses["failed-callback475"],
        ExecutionNodeStatus::Completed
    );
    let (_, starts) = state.binding_starts_page(&id, 0, graph.revision).unwrap();
    let (_, binding, position) = starts.into_iter().next().unwrap();
    assert!(!state
        .binding_lease_closure(&graph, "failed-callback475", &binding, position)
        .unwrap());
    let error = runner
        .command(
            &id,
            ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "must keep publication".into(),
            },
        )
        .await
        .err()
        .expect("failed publication must block cancel release");
    assert!(error.to_string().contains("475 publication failed"));
    assert_eq!(executor.inner.after_aborts.load(Ordering::SeqCst), 0);
    assert!(executor.inner.after_commits.load(Ordering::SeqCst) > 0);
    assert!(!state
        .binding_lease_closure(
            &state.load(&id).unwrap(),
            "failed-callback475",
            &binding,
            position
        )
        .unwrap());
}

#[tokio::test]
async fn r1_475_closed_empty_binding_restart_needs_no_old_executor_resolver() {
    let fixture = LeaseRecoveryFixture423::new();
    let mut graph = test_graph("475 no old resolver");
    graph.nodes.push(node("closed-empty475"));
    let id = graph.id.clone();
    fixture.runner.start(graph).await.unwrap();
    let graph = fixture.state.load(&id).unwrap();
    let (node, binding, position) = fixture
        .state
        .binding_starts_page(&id, 0, graph.revision)
        .unwrap()
        .1
        .into_iter()
        .next()
        .unwrap();
    assert!(fixture
        .state
        .binding_lease_closure(&graph, &node, &binding, position)
        .unwrap());
    let empty_registry = NodeExecutorRegistry::new();
    let recovered = ExecutionGraphRecovery::new(&fixture.state, &fixture.commits, &empty_registry)
        .recover_with_leases(
            &id,
            &fixture.scopes,
            &fixture.worktrees,
            "lease-workspace-423",
        )
        .await
        .unwrap();
    assert_eq!(recovered, graph);
}

#[tokio::test]
async fn r1_475_intent_only_command_crash_recovers_empty_binding_without_completion_receipt() {
    let (events, running, ticket, _, calls) =
        running_inline_attempt_450("475 actual command crash").await;
    let commits = ExecutionCommitService::new(events.clone());
    let state = ExecutionGraphStateStore::new(events.clone());
    let binding = state
        .current_binding(&running, "durable", ticket.attempt)
        .unwrap();
    assert_eq!(
        commits
            .binding_callback_disposition(&running, "durable", &binding)
            .unwrap(),
        super::commit_service::CallbackDisposition::Pending
    );
    let cancelled = commits
        .apply_command(
            &running,
            &ExecutionGraphCommand::Cancel {
                expected_revision: running.revision,
                reason: "actual canonical cancel".into(),
            },
        )
        .unwrap()
        .graph;
    assert_eq!(
        commits
            .binding_callback_disposition(&cancelled, "durable", &binding)
            .unwrap(),
        super::commit_service::CallbackDisposition::Abort
    );
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: ticket.payload_ref.clone(),
        backend: Arc::new(PayloadScopedBackend {
            calls: calls.clone(),
        }),
    }));
    registry.register(executor).unwrap();
    let runner = recovery_runner_475(registry, state.clone(), commits.clone(), &cancelled.id);
    let closed = runner.recover_graph(&cancelled.id).await.unwrap();
    assert_eq!(
        closed.node_statuses["durable"],
        ExecutionNodeStatus::Cancelled
    );
    let (_, starts) = state
        .binding_starts_page(&closed.id, 0, closed.revision)
        .unwrap();
    let (_, binding, position) = starts.into_iter().next().unwrap();
    assert!(state
        .binding_lease_closure(&closed, "durable", &binding, position)
        .unwrap());
    assert_eq!(
        events
            .stream_revision(&ExecutionNodeBinding::effect_stream_id(&ticket))
            .unwrap(),
        1
    );
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert_eq!(runner.recover_graph(&closed.id).await.unwrap(), closed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn r1_475_orphan_admission_same_inode_cas_retry_response_loss_polls_once() {
    use std::os::unix::fs::MetadataExt;
    let events = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(events.clone());
    let mut graph = test_graph("475 orphan retry");
    let mut spec = node("orphan475");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = "inline_model".into();
    graph.nodes.push(spec.clone());
    let graph = commits.register_graph(graph).unwrap().graph;
    let ready = commits
        .transition_node(
            &graph,
            &spec.id,
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = ScopedNodeExecutor::new("inline_model");
    executor.install_resolver(Arc::new(PayloadScopedResolver {
        payload_ref: spec.payload_ref.clone(),
        backend: Arc::new(PayloadScopedBackend {
            calls: calls.clone(),
        }),
    }));
    let ticket = executor
        .start(NodeExecutionContext {
            graph: Arc::new(ready.clone()),
            node: spec.clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let root = tempfile::tempdir().unwrap();
    let scopes = ScopeLockManager::persistent(root.path()).unwrap();
    let owner = acquire_fixture_owner_475(&scopes, &commits, &ready, &spec.id, &binding);
    let path = std::fs::read_dir(root.path().join("binding-owners-v1"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let inode = std::fs::metadata(&path).unwrap().ino();
    let fresh = commits
        .commit_recovery(&ready, ready.clone(), Vec::new(), Vec::new())
        .unwrap()
        .graph;
    assert!(commits
        .bind_and_start_nodes(
            &ready,
            BTreeMap::from([(spec.id.clone(), binding.clone())]),
            vec![ticket.clone()]
        )
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(owner);
    let mut owner = acquire_fixture_owner_475(&scopes, &commits, &fresh, &spec.id, &binding);
    assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
    let losses = Arc::new(AtomicUsize::new(0));
    commits.inject_binding_start_response_loss_for_test(&fresh.id, losses.clone());
    executor.prepare_original_binding(&ticket).unwrap();
    let running = commits
        .bind_and_start_nodes(
            &fresh,
            BTreeMap::from([(spec.id.clone(), binding.clone())]),
            vec![ticket.clone()],
        )
        .unwrap()
        .graph;
    assert_eq!(losses.load(Ordering::SeqCst), 1);
    let outcome = executor.poll_or_await(&ticket).await.unwrap();
    let graph = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap()
        .graph;
    executor.after_commit(&ticket).await.unwrap();
    let state = ExecutionGraphStateStore::new(events.clone());
    let position = state
        .binding_start_position(&graph, &spec.id, &binding)
        .unwrap()
        .unwrap();
    let worktrees = WorktreeLeaseManager::open(root.path().join("worktrees.json")).unwrap();
    let proof = owner
        .release_original(
            &graph, &spec.id, &binding, position, &mut None, &mut None, &mut None, &worktrees,
        )
        .unwrap();
    let closed = commits
        .commit_binding_lease_closure(&graph, &proof)
        .unwrap()
        .graph;
    drop(proof);
    drop(owner);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let key = super::events::ExecutionBindingLeaseClosure::fence_key(&closed, &spec.id, &binding);
    assert!(scopes
        .acquire_binding_owner(&key, || commits
            .validate_unadmitted_binding_start(&closed, &spec, &ticket)
            .map_err(|error| error.to_string()))
        .is_err());
    assert_eq!(std::fs::metadata(path).unwrap().ino(), inode);
}

#[tokio::test]
async fn r1_475_wait_result_then_ready_or_paused_cancel_keeps_original_receipt() {
    for phase in ["waiting", "ready", "paused"] {
        let events = Arc::new(RuntimeEventStore::for_test());
        let commits = ExecutionCommitService::new(events.clone());
        let mut graph = test_graph("475 waiting retirement");
        let mut spec = node("durable");
        spec.kind = ExecutionNodeKind::InlineModel;
        spec.executor_kind = "inline_model".into();
        graph.nodes.push(spec.clone());
        let graph = commits.register_graph(graph).unwrap().graph;
        let ready = commits
            .transition_node(
                &graph,
                "durable",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let backend = Arc::new(WaitPublicationBackend450 {
            polls: AtomicUsize::new(0),
            publications: AtomicUsize::new(0),
        });
        let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
        executor.install_resolver(Arc::new(WaitPublicationResolver450(backend.clone())));
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(ready.clone()),
                node: spec,
                attempt: 1,
            })
            .await
            .unwrap();
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let scopes = manual_scope_locks_475(&ready.id);
        let owner = acquire_fixture_owner_475(&scopes, &commits, &ready, "durable", &binding);
        executor.prepare_original_binding(&ticket).unwrap();
        let running = commits
            .bind_and_start_nodes(
                &ready,
                BTreeMap::from([("durable".into(), binding.clone())]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        let outcome = executor.poll_or_await(&ticket).await.unwrap();
        let waiting = commits
            .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
            .unwrap()
            .graph;
        let receipt = events
            .event_by_idempotency_key(
                &ExecutionNodeBinding::effect_stream_id(&ticket),
                &format!("{}:receipt", ticket.idempotency_key),
            )
            .unwrap()
            .unwrap();
        let before = if phase == "waiting" {
            waiting
        } else {
            let ready = commits
                .transition_node(
                    &waiting,
                    "durable",
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            if phase == "paused" {
                commits
                    .apply_command(
                        &ready,
                        &ExecutionGraphCommand::Pause {
                            expected_revision: ready.revision,
                            reason: "pause after ready".into(),
                        },
                    )
                    .unwrap()
                    .graph
            } else {
                ready
            }
        };
        assert_eq!(
            before.node_statuses["durable"],
            match phase {
                "ready" => ExecutionNodeStatus::Ready,
                "paused" => ExecutionNodeStatus::Paused,
                _ => ExecutionNodeStatus::WaitingInput,
            }
        );
        let cancelled = commits
            .apply_command(
                &before,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: before.revision,
                    reason: "cancel wait".into(),
                },
            )
            .unwrap()
            .graph;
        let state = ExecutionGraphStateStore::new(events.clone());
        let retirement = state
            .binding_retirement_record(&cancelled, "durable", &binding)
            .unwrap()
            .unwrap();
        assert!(retirement.commit_cursor > receipt.commit_cursor);
        assert_eq!(
            commits
                .binding_callback_disposition(&cancelled, "durable", &binding)
                .unwrap(),
            super::commit_service::CallbackDisposition::Commit
        );
        drop(owner);
        let registry = Arc::new(NodeExecutorRegistry::new());
        registry.register(executor).unwrap();
        let runner = recovery_runner_475(registry, state.clone(), commits, &cancelled.id);
        let closed = runner.recover_graph(&cancelled.id).await.unwrap();
        let position = state
            .binding_start_position(&closed, "durable", &binding)
            .unwrap()
            .unwrap();
        assert!(state
            .binding_lease_closure(&closed, "durable", &binding, position)
            .unwrap());
        assert_eq!(
            closed.node_statuses["durable"],
            ExecutionNodeStatus::Cancelled
        );
        assert_eq!(
            events
                .event_by_idempotency_key(
                    &ExecutionNodeBinding::effect_stream_id(&ticket),
                    &format!("{}:receipt", ticket.idempotency_key)
                )
                .unwrap()
                .unwrap(),
            receipt
        );
        assert_eq!(
            events
                .stream_revision(&ExecutionNodeBinding::effect_stream_id(&ticket))
                .unwrap(),
            2
        );
        assert_eq!(backend.polls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.publications.load(Ordering::SeqCst), 1);
    }
}

struct ControlledDrain475 {
    drained: AtomicBool,
    abort_fails: bool,
    aborts: AtomicUsize,
    polls: AtomicUsize,
}

#[async_trait]
impl NodeExecutor for ControlledDrain475 {
    fn kind(&self) -> &str {
        "controlled_drain475"
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
            node_id: context.node.id.clone(),
            executor_kind: self.kind().into(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: format!("{}:{}", context.node.idempotency_key, context.attempt),
            payload_ref: context.node.payload_ref,
        })
    }
    async fn poll_or_await(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
    fn cancellation_requires_quiescence(&self, _ticket: &NodeExecutionTicket) -> bool {
        true
    }
    async fn cancel_bounded(
        &self,
        _ticket: &NodeExecutionTicket,
        _budget: Duration,
    ) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        if self.drained.load(Ordering::SeqCst) {
            Ok(())
        } else {
            std::future::pending().await
        }
    }
    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), NodeExecutorError> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        if self.abort_fails {
            Err(NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: "475 after_abort failed".into(),
            })
        } else {
            Ok(())
        }
    }
}

async fn assert_actual_cancel_gate_475(abort_fails: bool, initially_drained: bool) {
    let events = Arc::new(RuntimeEventStore::for_test());
    let state = ExecutionGraphStateStore::new(events.clone());
    let commits = ExecutionCommitService::new(events.clone());
    let registry = Arc::new(NodeExecutorRegistry::new());
    let executor = Arc::new(ControlledDrain475 {
        drained: AtomicBool::new(initially_drained),
        abort_fails,
        aborts: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
    });
    registry.register(executor.clone()).unwrap();
    let runner = test_runner(registry, state.clone(), commits.clone());
    let mut graph = test_graph("475 real admitted cancellation gate");
    let mut spec = node("drain475");
    spec.kind = ExecutionNodeKind::InlineModel;
    spec.executor_kind = executor.kind().into();
    graph.nodes.push(spec);
    let graph = runner.register(graph).await.unwrap();
    let snapshot = runner
        .pump_snapshot(&graph.id, &std::collections::BTreeSet::new())
        .await
        .unwrap();
    let prepared = runner
        .prepare_ready_wave(&graph.id, snapshot.ready)
        .await
        .unwrap();
    assert_eq!(prepared.len(), 1);
    drop(prepared);
    let running = state.load(&graph.id).unwrap();
    assert_eq!(
        running.node_statuses["drain475"],
        ExecutionNodeStatus::Running
    );
    let (_, binding, position) = state
        .binding_starts_page(&graph.id, 0, running.revision)
        .unwrap()
        .1
        .into_iter()
        .next()
        .unwrap();
    if !initially_drained {
        let ticket = fixture_ticket_475(&running, "drain475", &binding);
        let effect_stream = ExecutionNodeBinding::effect_stream_id(&ticket);
        let original_effect = events.list_stream(&effect_stream).unwrap();
        assert_eq!(original_effect.len(), 1, "original intent exists without a receipt");
        let error = runner
            .command(
                &graph.id,
                ExecutionGraphCommand::Cancel {
                    expected_revision: running.revision,
                    reason: "original cancel".into(),
                },
            )
            .await
            .err()
            .expect("bounded Ok must not mean physical drain");
        assert!(error.to_string().contains("without physical quiescence"));
        assert_eq!(state.load(&graph.id).unwrap(), running);
        assert!(!state
            .binding_lease_closure(&running, "drain475", &binding, position)
            .unwrap());
        assert_eq!(executor.aborts.load(Ordering::SeqCst), 0);
        assert!(runner.recover_graph(&graph.id).await.is_err());
        executor.drained.store(true, Ordering::SeqCst);
        let retry = runner.command(&graph.id, ExecutionGraphCommand::Cancel {
            expected_revision: running.revision,
            reason: "unconfirmed original drain".into(),
        }).await;
        assert!(retry.is_err(), "a changed fixture flag is not an original drain receipt");
        let retained = state.load(&graph.id).unwrap();
        assert_eq!(retained, running);
        assert!(!state.binding_lease_closure(&retained, "drain475", &binding, position).unwrap());
        assert_eq!(events.list_stream(&effect_stream).unwrap(), original_effect);
        assert!(matches!(commits.inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        assert_eq!(executor.aborts.load(Ordering::SeqCst), 0);
        assert_eq!(executor.polls.load(Ordering::SeqCst), 0);
        return;
    }
    let result = runner
        .command(
            &graph.id,
            ExecutionGraphCommand::Cancel {
                expected_revision: running.revision,
                reason: "confirmed original drain".into(),
            },
        )
        .await;
    let cancelled = state.load(&graph.id).unwrap();
    assert_eq!(
        cancelled.node_statuses["drain475"],
        ExecutionNodeStatus::Cancelled
    );
    if abort_fails {
        assert!(result
            .err()
            .expect("after_abort failure must block closure")
            .to_string()
            .contains("475 after_abort failed"));
        assert!(!state
            .binding_lease_closure(&cancelled, "drain475", &binding, position)
            .unwrap());
        assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
        let recovery_error = runner.recover_graph(&graph.id).await.unwrap_err();
        assert!(
            recovery_error
                .to_string()
                .contains("475 after_abort failed"),
            "{recovery_error}"
        );
        assert_eq!(state.load(&graph.id).unwrap(), cancelled);
        assert!(!state
            .binding_lease_closure(&cancelled, "drain475", &binding, position)
            .unwrap());
    } else {
        result.unwrap();
        assert!(state
            .binding_lease_closure(&cancelled, "drain475", &binding, position)
            .unwrap());
        assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
    }
    let ticket = fixture_ticket_475(&cancelled, "drain475", &binding);
    assert_eq!(
        events
            .stream_revision(&ExecutionNodeBinding::effect_stream_id(&ticket))
            .unwrap(),
        1
    );
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert_eq!(executor.polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn r1_475_bounded_cancel_ok_requires_positive_original_drain_before_closure() {
    assert_actual_cancel_gate_475(false, false).await;
    assert_actual_cancel_gate_475(false, true).await;
}

#[tokio::test]
async fn r1_475_command_after_abort_failure_keeps_cancelled_binding_unclosed() {
    assert_actual_cancel_gate_475(true, false).await;
    assert_actual_cancel_gate_475(true, true).await;
}

#[test]
fn r1_498_source_revisions_are_private_and_default_empty() {
    let mut outcome = NodeExecutionOutcome::new(completed_result("498-private-source"));
    assert!(outcome.source_revisions.is_empty());
    outcome
        .source_revisions
        .insert("goal:498-private".into(), 4);
    let mut encoded = serde_json::to_value(&outcome).unwrap();
    assert!(encoded.get("source_revisions").is_none());
    encoded["source_revisions"] = serde_json::json!({"goal:498-private":99});
    let decoded: NodeExecutionOutcome = serde_json::from_value(encoded).unwrap();
    assert!(decoded.source_revisions.is_empty());
    assert!(decoded.domain_events.is_empty());
}

fn append_source_498(events: &RuntimeEventStore, stream: &str) {
    events
        .append(RuntimeEventInput {
            stream_id: stream.into(),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "fixture.source".into(),
            status: None,
            actor: Some("test".into()),
            refs: Vec::new(),
            payload: serde_json::json!({"source":stream}),
        })
        .unwrap();
}

#[tokio::test]
async fn r1_498_source_revisions_union_preserves_readonly_cuts_and_rejects_conflict() {
    let (events, running, ticket, mut outcome, calls) =
        running_inline_attempt_450("498 producer source union").await;
    let commits = ExecutionCommitService::new(events.clone());
    outcome.source_revisions = BTreeMap::from([
        ("goal:498-producer".into(), 0),
        ("498-primary-cut".into(), 0),
        ("498-index-cut".into(), 0),
    ]);
    let mut late = BTreeMap::from([("498-caller-cut".into(), 0)]);
    let captured = commits
        .execution_outcome_sources(&running, &ticket, &outcome)
        .unwrap();
    for (stream, revision) in &outcome.source_revisions {
        assert_eq!(captured.get(stream), Some(revision));
    }
    late.insert("goal:498-producer".into(), 1);
    let error = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &late)
        .err()
        .expect("producer source conflict must fail");
    assert!(error.to_string().contains("outcome source conflicts"));
    assert_eq!(
        events.stream_revision(&running.id).unwrap(),
        running.revision
    );
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    late.insert("goal:498-producer".into(), 0);
    let receipt = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &late)
        .unwrap();
    for stream in [
        "goal:498-producer",
        "498-primary-cut",
        "498-index-cut",
        "498-caller-cut",
    ] {
        let source = receipt
            .transaction
            .stream_revisions
            .iter()
            .find(|source| source.stream_id == stream)
            .expect("every read-only source is part of the original transaction");
        assert_eq!(source.expected_revision, 0);
        assert_eq!(source.committed_revision, 0);
        assert!(events.list_stream(stream).unwrap().is_empty());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn r1_498_stale_producer_source_rejects_empty_and_late_async_callers() {
    for late_caller in [false, true] {
        let (events, running, ticket, mut outcome, calls) =
            running_inline_attempt_450("498 stale original producer").await;
        let commits = Arc::new(ExecutionCommitService::new(events.clone()));
        outcome
            .source_revisions
            .insert("goal:498-original-read".into(), 0);
        let captured = commits
            .execution_outcome_sources(&running, &ticket, &outcome)
            .unwrap();
        append_source_498(&events, "goal:498-original-read");
        let late = if late_caller {
            let mut late = captured;
            late.insert("goal:498-original-read".into(), 1);
            late
        } else {
            BTreeMap::new()
        };
        assert!(commits
            .transition_execution_outcome_async(running.clone(), ticket.clone(), outcome, late,)
            .await
            .is_err());
        assert_eq!(
            ExecutionGraphStateStore::new(events.clone())
                .load(&running.id)
                .unwrap(),
            running
        );
        assert!(matches!(
            commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Uncertain
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn r1_498_replan_preserves_original_source_and_response_loss_duplicate() {
    let (events, running, ticket, mut outcome, calls) =
        running_inline_attempt_450("498 replan source duplicate").await;
    let commits = ExecutionCommitService::new(events.clone());
    outcome
        .source_revisions
        .insert("goal:498-replan-source".into(), 0);
    let tail = node("498-source-tail");
    outcome.replan = Some(ExecutionGraphReplan {
        nodes: vec![tail],
        edges: vec![ExecutionEdge {
            from: "durable".into(),
            to: "498-source-tail".into(),
            kind: ExecutionEdgeKind::DependsOn,
        }],
        reason: "498 original source".into(),
        framework_authored: true,
    });
    let losses = Arc::new(AtomicUsize::new(0));
    commits.inject_node_outcome_response_loss_for_test(&ticket.node_id, losses.clone());
    let receipt = commits
        .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
        .unwrap();
    assert_eq!(losses.load(Ordering::SeqCst), 1);
    assert_eq!(
        receipt.graph.node_statuses["498-source-tail"],
        ExecutionNodeStatus::Planned
    );
    let mut records = Vec::new();
    for source in &receipt.transaction.stream_revisions {
        records.extend(
            events
                .list_stream(&source.stream_id)
                .unwrap()
                .into_iter()
                .filter(|event| event.transaction_id == receipt.transaction.transaction_id),
        );
    }
    records.sort_by_key(|event| event.transaction_index);
    let request = crate::AppendTransactionRequest {
        transaction_id: receipt.transaction.transaction_id.clone(),
        expected_streams: receipt
            .transaction
            .stream_revisions
            .iter()
            .map(|source| crate::ExpectedStreamRevision {
                stream_id: source.stream_id.clone(),
                expected_revision: source.expected_revision,
            })
            .collect(),
        events: records
            .into_iter()
            .map(|record| crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: record.stream_id,
                    scope: record.scope,
                    kind: record.kind,
                    status: record.status,
                    actor: record.actor,
                    refs: record.refs,
                    payload: record.payload,
                },
                schema_version: record.schema_version,
                idempotency_key: record.idempotency_key,
            })
            .collect(),
    };
    append_source_498(&events, "goal:498-replan-source");
    let graph_head = events.stream_revision(&running.id).unwrap();
    let repeated = events.append_transaction(request.clone()).unwrap();
    assert!(repeated.duplicate);
    assert_eq!(events.stream_revision(&running.id).unwrap(), graph_head);
    let mut forged = request;
    forged
        .expected_streams
        .iter_mut()
        .find(|source| source.stream_id == "goal:498-replan-source")
        .unwrap()
        .expected_revision = 1;
    assert!(events.append_transaction(forged).is_err());
    assert_eq!(events.stream_revision(&running.id).unwrap(), graph_head);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

async fn running_source_wave_498() -> (
    Arc<RuntimeEventStore>,
    ExecutionGraph,
    Vec<super::commit_service::ExecutionTerminalCommit>,
) {
    let events = Arc::new(RuntimeEventStore::for_test());
    let commits = ExecutionCommitService::new(events.clone());
    let mut graph = test_graph("498 multi-outcome source wave");
    for name in ["498-first", "498-second"] {
        let mut spec = node(name);
        spec.kind = ExecutionNodeKind::InlineModel;
        spec.executor_kind = "inline_model".into();
        graph.nodes.push(spec);
    }
    let mut graph = commits.register_graph(graph).unwrap().graph;
    for name in ["498-first", "498-second"] {
        graph = commits
            .transition_node(&graph, name, ExecutionNodeStatus::Ready, None, Vec::new())
            .unwrap()
            .graph;
    }
    let scopes = manual_scope_locks_475(&graph.id);
    let mut owners = Vec::new();
    let mut tickets = Vec::new();
    let mut bindings = BTreeMap::new();
    let mut executors = Vec::new();
    for spec in &graph.nodes {
        let executor = ScopedNodeExecutor::new("inline_model");
        executor.install_resolver(Arc::new(PayloadScopedResolver {
            payload_ref: spec.payload_ref.clone(),
            backend: Arc::new(PayloadScopedBackend {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        }));
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node: spec.clone(),
                attempt: 1,
            })
            .await
            .unwrap();
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        owners.push(acquire_fixture_owner_475(
            &scopes, &commits, &graph, &spec.id, &binding,
        ));
        executor.prepare_original_binding(&ticket).unwrap();
        bindings.insert(spec.id.clone(), binding);
        tickets.push(ticket);
        executors.push(executor);
    }
    graph = commits
        .bind_and_start_nodes(&graph, bindings, tickets.clone())
        .unwrap()
        .graph;
    let mut transitions = Vec::new();
    for (executor, ticket) in executors.iter().zip(tickets) {
        let outcome = executor.poll_or_await(&ticket).await.unwrap();
        transitions.push(super::commit_service::ExecutionTerminalCommit {
            node_id: ticket.node_id.clone(),
            outcome,
            effect_ticket: Some(ticket),
        });
    }
    drop(owners);
    (events, graph, transitions)
}

#[tokio::test]
async fn r1_498_terminal_wave_unions_all_producer_sources_without_events() {
    let (events, running, mut transitions) = running_source_wave_498().await;
    transitions[0].outcome.source_revisions = BTreeMap::from([
        ("goal:498-shared-wave".into(), 0),
        ("498-first-read".into(), 0),
    ]);
    transitions[1].outcome.source_revisions = BTreeMap::from([
        ("goal:498-shared-wave".into(), 0),
        ("498-second-read".into(), 0),
    ]);
    let commits = Arc::new(ExecutionCommitService::new(events.clone()));
    let receipt = commits
        .transition_nodes_async(running, transitions)
        .await
        .unwrap();
    for stream in ["goal:498-shared-wave", "498-first-read", "498-second-read"] {
        assert!(receipt
            .transaction
            .stream_revisions
            .iter()
            .any(|source| { source.stream_id == stream && source.expected_revision == 0 }));
        assert_eq!(events.stream_revision(stream).unwrap(), 0);
    }
    assert_eq!(
        receipt.graph.node_statuses["498-first"],
        ExecutionNodeStatus::Completed
    );
    assert_eq!(
        receipt.graph.node_statuses["498-second"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn r1_498_terminal_wave_conflict_and_stale_producer_leave_all_results_pending() {
    for stale in [false, true] {
        let (events, running, mut transitions) = running_source_wave_498().await;
        transitions[0]
            .outcome
            .source_revisions
            .insert("goal:498-wave-cut".into(), 0);
        transitions[1]
            .outcome
            .source_revisions
            .insert("goal:498-wave-cut".into(), if stale { 0 } else { 1 });
        if stale {
            append_source_498(&events, "goal:498-wave-cut");
        }
        let commits = ExecutionCommitService::new(events.clone());
        assert!(commits.transition_nodes(&running, transitions).is_err());
        assert_eq!(
            ExecutionGraphStateStore::new(events.clone())
                .load(&running.id)
                .unwrap(),
            running
        );
        for spec in &running.nodes {
            assert!(!running.node_results.contains_key(&spec.id));
        }
    }
}

#[tokio::test]
async fn r1_498_original_graph_read_cut_cannot_be_silently_rebased() {
    let (events, running, ticket, mut outcome, calls) =
        running_inline_attempt_450("498 pinned Graph producer").await;
    let commits = ExecutionCommitService::new(events.clone());
    outcome
        .source_revisions
        .insert(running.id.clone(), running.revision);
    let advanced = commits
        .commit_recovery(&running, running.clone(), Vec::new(), Vec::new())
        .unwrap()
        .graph;
    let error = commits
        .transition_execution_outcome(&advanced, &ticket, &outcome, &BTreeMap::new())
        .err()
        .expect("an explicit producer Graph cut cannot be refreshed");
    assert!(error.to_string().contains("Graph source conflicts"));
    assert_eq!(
        ExecutionGraphStateStore::new(events.clone())
            .load(&running.id)
            .unwrap(),
        advanced
    );
    assert!(matches!(
        commits.inspect_execution_effect(&ticket).unwrap(),
        ExecutionEffectState::Uncertain
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct UnadmittedBuiltinFixture534 {
    services: Arc<crate::RuntimeServices>,
    runner: ExecutionGraphRunner,
    ready: ExecutionGraph,
    spec: ExecutionNodeSpec,
    ticket: NodeExecutionTicket,
    calls: Arc<AtomicUsize>,
}

impl UnadmittedBuiltinFixture534 {
    async fn new() -> Self {
        Self::new_wave(false).await
    }

    async fn new_wave(with_sibling: bool) -> Self {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let mut graph = test_graph("534 original builtin unadmitted rollback");
        let mut spec = node("builtin534");
        spec.executor_kind = "tool_batch".into();
        spec.payload_ref = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"test-session",
            "calls":[],
            "step_snapshots":{}
        }))
        .unwrap();
        graph.nodes.push(spec.clone());
        if with_sibling {
            let mut sibling = spec.clone();
            sibling.id = "builtin534-sibling".into();
            sibling.idempotency_key.push_str(":sibling");
            graph.nodes.push(sibling);
        }
        let calls = Arc::new(AtomicUsize::new(0));
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(PayloadScopedResolver {
                payload_ref: spec.payload_ref.clone(),
                backend: Arc::new(PayloadScopedBackend {
                    calls: calls.clone(),
                }),
            }));
        let registered = services.executor_registry().get("tool_batch").unwrap();
        let original: Arc<dyn NodeExecutor> = services.tool_batch_executor().clone();
        assert!(Arc::ptr_eq(&registered, &original));
        let runner = test_runner(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
        );
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.ready.len(), if with_sibling { 2 } else { 1 });
        let ready = services.graph_state_store().load(&registered.id).unwrap();
        let ticket = services
            .tool_batch_executor()
            .start(NodeExecutionContext {
                graph: Arc::new(ready.clone()),
                node: spec.clone(),
                attempt: ExecutionNodeBinding::next_attempt(&ready, &spec.id).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(ticket.attempt, 1);
        Self {
            services,
            runner,
            ready,
            spec,
            ticket,
            calls,
        }
    }

    fn assert_no_canonical_start_or_intent(&self) {
        let state = self.services.graph_state_store();
        let current = state.load(&self.ready.id).unwrap();
        assert_eq!(current, self.ready);
        assert_eq!(
            current.node_statuses[&self.spec.id],
            ExecutionNodeStatus::Ready
        );
        assert_eq!(
            ExecutionNodeBinding::next_attempt(&current, &self.spec.id).unwrap(),
            1
        );
        assert!(state
            .binding_starts_page(&current.id, 0, current.revision)
            .unwrap()
            .1
            .is_empty());
        assert_eq!(
            self.services
                .event_store()
                .stream_revision(&ExecutionNodeBinding::effect_stream_id(&self.ticket))
                .unwrap(),
            0
        );
        assert_eq!(self.calls.load(Ordering::SeqCst), 0);
    }

    async fn admit(&self) -> super::runner::ExecutionPreparedPumpNode {
        let mut prepared = self
            .runner
            .prepare_ready_wave(&self.ready.id, vec![self.spec.clone()])
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        let prepared = prepared.pop().unwrap();
        let binding = prepared.binding.as_ref().expect("original start binding");
        assert_eq!(binding.attempt, self.ticket.attempt);
        assert!(self
            .services
            .commit_service()
            .binding_start_was_admitted(&self.ready.id, &self.ticket, binding)
            .unwrap());
        assert_eq!(
            self.runner
                .active_binding_for_ticket_for_test(&self.ticket)
                .await,
            Some(binding.clone())
        );
        assert_eq!(self.calls.load(Ordering::SeqCst), 0);
        prepared
    }

    async fn execute_once(&self, prepared: super::runner::ExecutionPreparedPumpNode) {
        self.runner
            .execute_prepared_pump_node(&self.ready.id, prepared, Arc::new(Notify::new()))
            .await
            .unwrap();
        let graph = self
            .services
            .graph_state_store()
            .load(&self.ready.id)
            .unwrap();
        assert_eq!(
            graph.node_statuses[&self.spec.id],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(self.calls.load(Ordering::SeqCst), 1);
        assert!(self
            .runner
            .active_binding_for_ticket_for_test(&self.ticket)
            .await
            .is_none());
        let starts = self
            .services
            .graph_state_store()
            .binding_starts_page(&graph.id, 0, graph.revision)
            .unwrap()
            .1;
        assert_eq!(starts.len(), 1);
        let (node_id, binding, position) = &starts[0];
        assert_eq!(node_id, &self.spec.id);
        assert_eq!(binding.attempt, 1);
        assert!(self
            .services
            .graph_state_store()
            .binding_lease_closure(&graph, node_id, binding, *position)
            .unwrap());
    }
}

#[tokio::test]
async fn r1_534_builtin_start_before_append_failure_retries_same_runtime_attempt_polls_once() {
    let fixture = UnadmittedBuiltinFixture534::new().await;
    let hits = Arc::new(AtomicUsize::new(0));
    fixture
        .services
        .commit_service()
        .inject_binding_start_before_append_failure_for_test(&fixture.ready.id, hits.clone());
    let error = fixture
        .runner
        .prepare_ready_wave(&fixture.ready.id, vec![fixture.spec.clone()])
        .await
        .err()
        .expect("original atomic start append must fail before writing");
    assert!(error
        .to_string()
        .contains("534 injected binding-start failure before atomic append"));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    fixture.assert_no_canonical_start_or_intent();
    assert!(fixture
        .runner
        .active_binding_for_ticket_for_test(&fixture.ticket)
        .await
        .is_none());
    assert!(fixture
        .services
        .tool_batch_executor()
        .binding_observation(&fixture.ticket)
        .is_none());
    let prepared = fixture.admit().await;
    fixture.execute_once(prepared).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn r1_534_builtin_start_response_loss_keeps_admitted_original_observation() {
    let fixture = UnadmittedBuiltinFixture534::new().await;
    let losses = Arc::new(AtomicUsize::new(0));
    fixture
        .services
        .commit_service()
        .inject_binding_start_response_loss_for_test(&fixture.ready.id, losses.clone());
    let prepared = fixture.admit().await;
    assert_eq!(losses.load(Ordering::SeqCst), 1);
    let observation = fixture
        .services
        .tool_batch_executor()
        .binding_observation(&fixture.ticket)
        .expect("admitted original observation retained");
    assert!(fixture
        .services
        .executor_registry()
        .observe_builtin_binding(&fixture.ticket)
        .is_err());
    let retained = fixture
        .services
        .tool_batch_executor()
        .binding_observation(&fixture.ticket)
        .unwrap();
    assert!(Arc::ptr_eq(&observation, &retained));
    let deferred = fixture
        .runner
        .prepare_ready_wave(&fixture.ready.id, vec![fixture.spec.clone()])
        .await
        .unwrap();
    assert!(deferred.is_empty());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.execute_once(prepared).await;
}

#[tokio::test]
async fn r1_534_builtin_entered_unavailable_cancel_cleanup_cannot_reset_observation() {
    let fixture = UnadmittedBuiltinFixture534::new().await;
    let mut ticket = fixture.ticket.clone();
    ticket.node_id = "534-unavailable".into();
    ticket.payload_ref = crate::step_snapshot::encode_persisted_tool_batch(
        &serde_json::json!({"session_id":"test-session",
            "calls":[{"id":"534-unavailable","name":"read_file","input":"{}","depends_on":[]}],
            "step_snapshots":{}}),
    )
    .unwrap();
    let registry = fixture.services.executor_registry();
    let owner = fixture.services.tool_batch_executor();
    let observation = registry.observe_builtin_binding(&ticket).unwrap().unwrap();
    assert!(matches!(
        owner.poll_or_await(&ticket).await,
        Err(NodeExecutorError::Unavailable { .. })
    ));
    let _ = registry.rollback_unadmitted_builtin_binding(
        &ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(Arc::ptr_eq(
        &observation,
        &owner.binding_observation(&ticket).unwrap()
    ));
    assert!(registry.observe_builtin_binding(&ticket).is_err());
    assert!(observation.witness().is_none());
    owner
        .cancel_bounded(&ticket, Duration::from_millis(10))
        .await
        .unwrap();
    owner.cancellation_finalized(&ticket);
    let _ = registry.rollback_unadmitted_builtin_binding(
        &ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(registry.observe_builtin_binding(&ticket).is_err());
    assert!(observation.witness().is_none());
    registry.forget_builtin_binding(&ticket);
    let _ = registry.rollback_unadmitted_builtin_binding(
        &ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(registry.observe_builtin_binding(&ticket).is_err());
    assert!(matches!(
        owner.poll_or_await(&ticket).await,
        Err(NodeExecutorError::Cancel { .. })
    ));
    assert!(observation.witness().is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn r1_534_builtin_cancelled_never_polled_cleanup_cannot_reset_sticky_binding() {
    let fixture = UnadmittedBuiltinFixture534::new().await;
    let registry = fixture.services.executor_registry();
    let owner = fixture.services.tool_batch_executor();
    let observation = registry
        .observe_builtin_binding(&fixture.ticket)
        .unwrap()
        .unwrap();
    owner
        .cancel_bounded(&fixture.ticket, Duration::from_millis(10))
        .await
        .unwrap();
    let witness = observation.witness().unwrap();
    assert!(witness.fact().never_polled);
    owner.cancellation_finalized(&fixture.ticket);
    let _ = registry.rollback_unadmitted_builtin_binding(
        &fixture.ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(registry.observe_builtin_binding(&fixture.ticket).is_err());
    registry.forget_builtin_binding(&fixture.ticket);
    let _ = registry.rollback_unadmitted_builtin_binding(
        &fixture.ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(registry.observe_builtin_binding(&fixture.ticket).is_err());
    assert!(matches!(
        owner.poll_or_await(&fixture.ticket).await,
        Err(NodeExecutorError::Cancel { .. })
    ));
    assert!(observation.witness().unwrap().fact().never_polled);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn r1_534_builtin_rollback_rejects_cloned_observation_arc_and_wrong_ticket() {
    let fixture = UnadmittedBuiltinFixture534::new().await;
    let registry = fixture.services.executor_registry();
    let owner = fixture.services.tool_batch_executor();
    let observation = registry
        .observe_builtin_binding(&fixture.ticket)
        .unwrap()
        .unwrap();
    let copied = Arc::new((*observation).clone());
    assert!(!Arc::ptr_eq(&observation, &copied));
    let _ = registry.rollback_unadmitted_builtin_binding(
        &fixture.ticket,
        &copied,
        &fixture.services.commit_service(),
    );
    assert!(Arc::ptr_eq(
        &observation,
        &owner.binding_observation(&fixture.ticket).unwrap()
    ));
    let mut wrong = fixture.ticket.clone();
    wrong.idempotency_key.push_str(":wrong");
    let _ = registry.rollback_unadmitted_builtin_binding(
        &wrong,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(Arc::ptr_eq(
        &observation,
        &owner.binding_observation(&fixture.ticket).unwrap()
    ));
    assert!(registry.observe_builtin_binding(&fixture.ticket).is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    owner
        .cancel_bounded(&fixture.ticket, Duration::from_millis(10))
        .await
        .unwrap();
    owner.cancellation_finalized(&fixture.ticket);
    registry.forget_builtin_binding(&fixture.ticket);
}

#[tokio::test]
async fn r1_534_builtin_wave_observation_failure_rolls_back_only_preceding_staging() {
    let fixture = UnadmittedBuiltinFixture534::new_wave(true).await;
    let sibling = fixture
        .ready
        .nodes
        .iter()
        .find(|spec| spec.id == "builtin534-sibling")
        .unwrap()
        .clone();
    let ticket = fixture
        .services
        .tool_batch_executor()
        .start(NodeExecutionContext {
            graph: Arc::new(fixture.ready.clone()),
            node: sibling.clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    let registry = fixture.services.executor_registry();
    let owner = fixture.services.tool_batch_executor();
    let original = registry.observe_builtin_binding(&ticket).unwrap().unwrap();
    let error = fixture
        .runner
        .prepare_ready_wave(&fixture.ready.id, vec![fixture.spec.clone(), sibling])
        .await
        .err()
        .expect("second original observation must reject duplicate staging");
    assert!(error
        .to_string()
        .contains("original ToolBatch observation already admitted"));
    fixture.assert_no_canonical_start_or_intent();
    assert!(owner.binding_observation(&fixture.ticket).is_none());
    assert!(fixture
        .runner
        .active_binding_for_ticket_for_test(&fixture.ticket)
        .await
        .is_none());
    assert!(fixture
        .runner
        .active_binding_for_ticket_for_test(&ticket)
        .await
        .is_none());
    assert!(Arc::ptr_eq(
        &original,
        &owner.binding_observation(&ticket).unwrap()
    ));
    let prepared = fixture.admit().await;
    fixture.execute_once(prepared).await;
    assert!(Arc::ptr_eq(
        &original,
        &owner.binding_observation(&ticket).unwrap()
    ));
    owner
        .cancel_bounded(&ticket, Duration::from_millis(10))
        .await
        .unwrap();
    owner.cancellation_finalized(&ticket);
    registry.forget_builtin_binding(&ticket);
}

#[tokio::test]
async fn r1_534_builtin_completed_entered_callback_cleanup_cannot_reset_original_binding() {
    let fixture = UnadmittedBuiltinFixture534::new().await;
    let prepared = fixture.admit().await;
    let registry = fixture.services.executor_registry();
    let owner = fixture.services.tool_batch_executor();
    let observation = owner.binding_observation(&fixture.ticket).unwrap();
    fixture.execute_once(prepared).await;
    assert!(observation.witness().is_none());
    let _ = registry.rollback_unadmitted_builtin_binding(
        &fixture.ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(registry.observe_builtin_binding(&fixture.ticket).is_err());
    owner.cancellation_finalized(&fixture.ticket);
    registry.forget_builtin_binding(&fixture.ticket);
    let _ = registry.rollback_unadmitted_builtin_binding(
        &fixture.ticket,
        &observation,
        &fixture.services.commit_service(),
    );
    assert!(registry.observe_builtin_binding(&fixture.ticket).is_err());
    assert!(matches!(
        owner.poll_or_await(&fixture.ticket).await,
        Err(NodeExecutorError::Cancel { .. })
    ));
    assert!(observation.witness().is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

/// The original terminal custody belongs to the original Session terminal.
///
/// A delegated leaf publishes its own terminal and never owns the original
/// Session terminal, so a published terminal completion alone must not demand
/// that custody. The original Session terminal keeps demanding it.
#[test]
fn a8_original_terminal_custody_is_required_only_for_the_original_session_terminal() {
    let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some("a8-delegated-terminal".to_string()),
        wait_class: None,
        summary: Some("delegated leaf terminal".to_string()),
        evidence_refs: Vec::new(),
        failure: None,
        usage: Default::default(),
        finished_at_ms: 1,
    });
    // A delegated leaf terminal: no original Session terminal was requested.
    assert!(!super::runner::requests_original_session_terminal(&outcome));
    outcome
        .domain_events
        .push(crate::runtime_event_store::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: "session-terminal:request-a8".to_string(),
                scope: crate::RuntimeEventScope::SessionInput,
                kind: "runtime.session.terminal_requested".to_string(),
                status: Some("pending_delivery".to_string()),
                actor: Some("SynthesizeNodeExecutor".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            },
            idempotency_key: Some("a8-session-terminal".to_string()),
            schema_version: 1,
        });
    // The original Session terminal keeps its custody requirement, and the
    // custody error keeps its original wording.
    assert!(super::runner::requests_original_session_terminal(&outcome));
    let error = super::runner::missing_original_terminal_custody(&outcome)
        .expect("the original Session terminal keeps its custody requirement");
    assert_eq!(
        error.to_string(),
        "runtime execution driver failed: original terminal handoff is unconfirmed"
    );
    // A delegated leaf terminal never carries that custody.
    let mut leaf = NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some("a8-delegated-terminal".to_string()),
        wait_class: None,
        summary: Some("delegated leaf terminal".to_string()),
        evidence_refs: Vec::new(),
        failure: None,
        usage: Default::default(),
        finished_at_ms: 1,
    });
    leaf.domain_events.push(crate::runtime_event_store::RuntimeTransactionEventInput {
        event: crate::RuntimeEventInput {
            stream_id: "execution-node:leaf".to_string(),
            scope: crate::RuntimeEventScope::ExecutionNode,
            kind: "execution.node.result".to_string(),
            status: Some("completed".to_string()),
            actor: Some("synthesize".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({}),
        },
        idempotency_key: Some("a8-leaf-node".to_string()),
        schema_version: 1,
    });
    assert!(super::runner::missing_original_terminal_custody(&leaf).is_none());
}

/// A delegated leaf publishes its own terminal.
///
/// The leaf terminal carries a terminal completion and never carries the
/// original Session terminal, because only a root Session ingress asks for
/// that one. The runner must not read a leaf terminal as a lost original
/// terminal handover, while the original Session terminal keeps demanding its
/// custody.
struct A8LeafTerminalBackend {
    commits: ExecutionCommitService,
    session_id: String,
    turn_id: String,
    original_session_terminal: bool,
}

struct A8LeafTerminalResolver {
    graph_id: String,
    backend: Arc<A8LeafTerminalBackend>,
}

impl super::executors::SynthesizeBackendResolver for A8LeafTerminalResolver {
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn super::executors::SynthesizeBackend>> {
        (ticket.graph_id == self.graph_id)
            .then(|| Arc::clone(&self.backend) as Arc<dyn super::executors::SynthesizeBackend>)
    }
}

#[async_trait]
impl super::executors::SynthesizeBackend for A8LeafTerminalBackend {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        use sha2::{Digest, Sha256};
        let text = "a8 delegated leaf terminal";
        let messages = vec![serde_json::json!({
            "role":"assistant","blocks":[{"Text":{"text":text}}],"usage":null
        })];
        let publication = self
            .commits
            .prepare_synthesize_transcript(
                ticket,
                &self.session_id,
                &self.turn_id,
                0,
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
                messages,
                Some(harness_contract::goal::GoalCompletion::Partial),
            )
            .map_err(|error| error.to_string())?;
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!(
                "turn-result:{}:{:x}",
                ticket.graph_id,
                Sha256::digest(text.as_bytes())
            )),
            wait_class: None,
            summary: Some("delegated leaf terminal".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        });
        if self.original_session_terminal {
            outcome
                .domain_events
                .push(crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("session-terminal:{}", self.turn_id),
                        scope: crate::RuntimeEventScope::SessionInput,
                        kind: "runtime.session.terminal_requested".to_string(),
                        status: Some("pending_delivery".to_string()),
                        actor: Some("SynthesizeNodeExecutor".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({}),
                    },
                    idempotency_key: Some("a8-session-terminal".to_string()),
                    schema_version: 1,
                });
        }
        ExecutionCommitService::attach_host_transcript(&mut outcome, publication)
            .map_err(|error| error.to_string())?;
        Ok(outcome)
    }

    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn a8_delegated_leaf_terminal_is_not_a_lost_original_session_terminal() {
    for original_session_terminal in [false, true] {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let root = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(root.path().join("blobs")));
        event_store.bind_artifact_store(artifacts.clone()).unwrap();
        let commits = ExecutionCommitService::new(Arc::clone(&event_store)).with_artifact_store(artifacts);
        let state = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let registry = Arc::new(NodeExecutorRegistry::new());
        let synthesize = Arc::new(super::executors::SynthesizeNodeExecutor::new());
        registry
            .register(Arc::clone(&synthesize) as Arc<dyn NodeExecutor>)
            .unwrap();
        let mut graph = test_graph("a8 delegated leaf terminal");
        let mut spec = node("leaf-terminal");
        spec.kind = ExecutionNodeKind::Synthesize;
        spec.executor_kind = "synthesize".into();
        graph.nodes.push(spec);
        let graph_id = graph.id.clone();
        let lineage = graph.lineage.clone().unwrap();
        synthesize.install_resolver(Arc::new(A8LeafTerminalResolver {
            graph_id: graph_id.clone(),
            backend: Arc::new(A8LeafTerminalBackend {
                commits: commits.clone(),
                session_id: lineage.session_id.clone(),
                turn_id: lineage.turn_id.clone(),
                original_session_terminal,
            }),
        }));
        let runner = test_runner(registry, state.clone(), commits);
        let _ = runner.start(graph).await;
        let committed = state.load(&graph_id).expect("graph state");
        let status = committed
            .node_statuses
            .get("leaf-terminal")
            .copied()
            .unwrap_or(ExecutionNodeStatus::Planned);
        if original_session_terminal {
            // The original Session terminal keeps demanding its custody: this
            // shape never completes as a plain leaf terminal.
            assert_ne!(
                status,
                ExecutionNodeStatus::Completed,
                "the original Session terminal custody must still be required"
            );
            // The refused terminal leaves no durable completed receipt of its
            // own: it either carries its failure or it was never committed.
            let recorded = committed.node_results.get("leaf-terminal");
            assert!(
                recorded
                    .map(|result| {
                        result.failure.as_ref().is_some_and(|failure| !failure.message.is_empty())
                    })
                    .unwrap_or(true),
                "the refused original Session terminal must not carry a durable completed receipt"
            );
        } else {
            // The delegated leaf's own terminal is not a lost original Session
            // terminal handover.
            assert_eq!(status, ExecutionNodeStatus::Completed);
            assert!(committed.node_results["leaf-terminal"]
                .failure
                .as_ref()
                .is_none());
        }
    }
}
