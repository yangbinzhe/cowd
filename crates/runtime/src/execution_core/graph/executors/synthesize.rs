use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use harness_contract::execution_graph::ExecutionNodeSpec;
use harness_contract::outcome::{AnswerOrigin, TerminalPresentationState};

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};

#[async_trait]
pub trait SynthesizeBackend: Send + Sync {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String>;
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
        Ok(())
    }
    async fn after_abort(
        &self,
        _ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

pub trait SynthesizeBackendResolver: Send + Sync {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>>;
}

/// The sole publisher of a graph terminal candidate, resolved from its ticket.
pub struct SynthesizeNodeExecutor {
    resolvers: RwLock<Vec<Arc<dyn SynthesizeBackendResolver>>>,
    #[cfg(any(test, feature = "test-fixtures"))]
    terminal_observers: std::sync::Mutex<
        std::collections::HashMap<
            String,
            tokio::sync::oneshot::Sender<(
                NodeExecutionTicket,
                crate::SessionTerminalInput,
                tokio::sync::oneshot::Sender<()>,
            )>,
        >,
    >,
}

impl SynthesizeNodeExecutor {
    pub const KIND: &'static str = "synthesize";
    #[must_use]
    pub fn new() -> Self {
        Self {
            resolvers: RwLock::new(Vec::new()),
            #[cfg(any(test, feature = "test-fixtures"))]
            terminal_observers: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
    pub fn install_resolver(&self, resolver: Arc<dyn SynthesizeBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn observe_terminal_candidate_for_test(
        &self,
        graph_id: &str,
    ) -> Result<
        tokio::sync::oneshot::Receiver<(
            NodeExecutionTicket,
            crate::SessionTerminalInput,
            tokio::sync::oneshot::Sender<()>,
        )>,
        String,
    > {
        if graph_id.is_empty() {
            return Err("terminal candidate requires an exact graph".into());
        }
        let mut observers = self
            .terminal_observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observers.contains_key(graph_id) {
            return Err("duplicate terminal candidate observer".into());
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        observers.insert(graph_id.into(), sender);
        Ok(receiver)
    }

    #[must_use]
    pub(crate) fn is_bound(&self, ticket: &NodeExecutionTicket) -> bool {
        self.resolve(ticket).is_some()
    }

    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        self.resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(ticket))
    }
}
impl Default for SynthesizeNodeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeExecutor for SynthesizeNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == Self::KIND {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Synthesize must use canonical synthesize executor".into(),
            })
        }
    }
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.into(),
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
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        let outcome = match backend.synthesize(ticket).await {
            Ok(outcome) => outcome,
            Err(reason) if reason == "terminal_presentation_cancelled" => {
                return Ok(NodeExecutionOutcome::new(
                    harness_contract::execution_graph::ExecutionNodeResult {
                        status: harness_contract::execution_graph::ExecutionNodeStatus::Cancelled,
                        result_ref: None,
                        wait_class: None,
                        summary: None,
                        evidence_refs: Vec::new(),
                        failure: Some(harness_contract::execution_graph::ExecutionFailure {
                            kind: "terminal_presentation_cancelled".into(),
                            message: reason,
                            retryable: false,
                            evidence_refs: Vec::new(),
                        }),
                        usage: Default::default(),
                        finished_at_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis()
                            .min(u128::from(u64::MAX))
                            as u64,
                    },
                ));
            }
            Err(reason) => {
                return Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })
            }
        };
        validate_terminal_candidate(ticket, &outcome).map_err(|reason| {
            NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            }
        })?;
        #[cfg(any(test, feature = "test-fixtures"))]
        if let Some(terminal) = outcome
            .domain_events
            .iter()
            .find(|event| event.event.kind == "runtime.session.terminal_requested")
        {
            let observer = self
                .terminal_observers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&ticket.graph_id);
            if let Some(observer) = observer {
                let terminal =
                    serde_json::from_value(terminal.event.payload.clone()).map_err(|error| {
                        NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason: format!("terminal candidate observer decode: {error}"),
                        }
                    })?;
                let (release, released) = tokio::sync::oneshot::channel();
                if observer.send((ticket.clone(), terminal, release)).is_ok() {
                    let _ = released.await;
                }
            }
        }
        Ok(outcome)
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .after_commit(ticket)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        reason: &str,
    ) -> Result<(), NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .after_abort(ticket, reason)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
}

fn validate_terminal_candidate(
    ticket: &NodeExecutionTicket,
    outcome: &NodeExecutionOutcome,
) -> Result<(), String> {
    let Some(presentation) = outcome.terminal_presentation.as_ref() else {
        return Ok(());
    };
    if presentation.answer_origin != AnswerOrigin::TeamSynthesizer {
        return Ok(());
    }
    let envelope = outcome.delivery_envelope.as_ref().ok_or_else(|| {
        "TeamSynthesizer presentation has no Runtime-authored delivery envelope".to_string()
    })?;
    if !ticket.payload_ref.starts_with("team:")
        || presentation.envelope_id != envelope.envelope_id
        || presentation.envelope_revision != envelope.revision
        || presentation.state != TerminalPresentationState::Validating
        || presentation
            .source_execution_id
            .as_deref()
            .is_none_or(str::is_empty)
        || presentation.committed_at_ms.is_some()
        || outcome
            .result
            .result_ref
            .as_deref()
            .is_none_or(|reference| !reference.starts_with("assistant_json:"))
        || outcome
            .result
            .summary
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
    {
        return Err(
            "TeamSynthesizer presentation is not bound to a complete envelope-consuming terminal Agent candidate"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
    };
    use harness_contract::outcome::{
        AnswerValidation, DeliveryCoverage, DeliveryEnvelope, DeliveryStatus, PipelineStatus,
        TerminalPresentation, UserAnswerContract,
    };

    use super::*;
    use crate::execution_core::graph::commit_service::CallbackDisposition;

    fn ticket() -> NodeExecutionTicket {
        NodeExecutionTicket {
            graph_id: "graph".to_string(),
            node_id: "synthesize".to_string(),
            executor_kind: SynthesizeNodeExecutor::KIND.to_string(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "synthesize:1".to_string(),
            payload_ref: "team:team-1".to_string(),
        }
    }

    struct ReadinessBackend {
        calls: std::sync::atomic::AtomicUsize,
    }

    struct RetirementCancellationBackend {
        reason: &'static str,
        abort_fails: bool,
        aborts: std::sync::atomic::AtomicUsize,
        commits: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SynthesizeBackend for RetirementCancellationBackend {
        async fn synthesize(
            &self,
            _ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, String> {
            Err(self.reason.into())
        }
        async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err("cancelled synthesis has no transcript to commit".into())
        }
        async fn after_abort(
            &self,
            _ticket: &NodeExecutionTicket,
            _reason: &str,
        ) -> Result<(), String> {
            self.aborts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.abort_fails {
                Err("abort not settled".into())
            } else {
                Ok(())
            }
        }
    }

    struct RetirementCancellationResolver {
        expected: NodeExecutionTicket,
        backend: Arc<RetirementCancellationBackend>,
    }

    impl SynthesizeBackendResolver for RetirementCancellationResolver {
        fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
            (ticket == &self.expected).then(|| self.backend.clone() as Arc<dyn SynthesizeBackend>)
        }
    }

    #[tokio::test]
    async fn r1_synthesize_retirement_cancellation_is_exact_and_not_business_success() {
        for (reason, abort_fails) in [
            ("terminal_presentation_cancelled", false),
            ("terminal_presentation_cancelled", true),
            ("effect completion uncertain", false),
            ("prefix terminal_presentation_cancelled", false),
        ] {
            let executor = SynthesizeNodeExecutor::new();
            let expected = ticket();
            let backend = Arc::new(RetirementCancellationBackend {
                reason,
                abort_fails,
                aborts: std::sync::atomic::AtomicUsize::new(0),
                commits: std::sync::atomic::AtomicUsize::new(0),
            });
            executor.install_resolver(Arc::new(RetirementCancellationResolver {
                expected: expected.clone(),
                backend: backend.clone(),
            }));
            let mut foreign = expected.clone();
            foreign.attempt += 1;
            assert!(matches!(
                executor.poll_or_await(&foreign).await,
                Err(NodeExecutorError::Unavailable { .. })
            ));
            let result = executor.poll_or_await(&expected).await;
            if reason == "terminal_presentation_cancelled" {
                let outcome = result.unwrap();
                assert_eq!(outcome.result.status, ExecutionNodeStatus::Cancelled);
                assert!(outcome.result.result_ref.is_none());
                assert!(outcome.terminal_presentation.is_none());
                assert!(outcome.delivery_envelope.is_none());
                assert!(outcome.domain_events.is_empty());
                assert!(outcome.replan.is_none());
                let unexpected_evidence = harness_contract::context::EvidenceAccessRef::unavailable(
                    harness_contract::context::EvidenceRef::observed("test", "unexpected"),
                    "text/plain",
                    "test",
                );
                assert!(
                    CallbackDisposition::from_result(
                        &expected.node_id,
                        &outcome.result,
                    ) == CallbackDisposition::Abort
                );
                for mutation in [
                    "kind",
                    "evidence",
                    "failure-evidence",
                    "result",
                    "retryable",
                    "status",
                ] {
                    let mut result = outcome.result.clone();
                    match mutation {
                        "kind" => {
                            result.failure.as_mut().unwrap().kind = "foreign_cancelled".into()
                        }
                        "evidence" => result.evidence_refs.push(unexpected_evidence.clone()),
                        "failure-evidence" => result
                            .failure
                            .as_mut()
                            .unwrap()
                            .evidence_refs
                            .push(unexpected_evidence.clone()),
                        "result" => result.result_ref = Some("transcript:unexpected".into()),
                        "retryable" => result.failure.as_mut().unwrap().retryable = true,
                        "status" => result.status = ExecutionNodeStatus::Completed,
                        _ => unreachable!(),
                    }
                    assert!(
                        CallbackDisposition::from_result(
                            &expected.node_id,
                            &result,
                        ) == CallbackDisposition::Commit,
                        "{mutation}"
                    );
                }
            } else {
                assert!(matches!(result, Err(NodeExecutorError::Poll { .. })));
            }
            assert_eq!(backend.aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(backend.commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn r1_synthesize_retirement_cancel_commits_original_effect_receipt_atomically() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
        };
        for abort_fails in [false, true] {
            let services = crate::RuntimeServices::in_memory().unwrap();
            let mut graph = ExecutionGraph::new("Original synthesis cancellation receipt");
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "retirement-session".into(),
                turn_id: "retirement-turn".into(),
                root_task_id: "retirement-task".into(),
                task_id: "retirement-task".into(),
                generation: 1,
            });
            let node = ExecutionNodeSpec::new(
                ExecutionNodeKind::Synthesize,
                "synthesize",
                "retirement-cancel",
            );
            let expected = NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: node.id.clone(),
                executor_kind: "synthesize".into(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: node.idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            graph.nodes.push(node);
            let backend = Arc::new(RetirementCancellationBackend {
                reason: "terminal_presentation_cancelled",
                abort_fails,
                aborts: std::sync::atomic::AtomicUsize::new(0),
                commits: std::sync::atomic::AtomicUsize::new(0),
            });
            services.synthesize_executor().install_resolver(Arc::new(
                RetirementCancellationResolver {
                    expected: expected.clone(),
                    backend: backend.clone(),
                },
            ));
            let completed = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                services.execution_supervisor().submit_and_wait_terminal(
                    graph,
                    ExecutionGraphCommand::Start {
                        expected_revision: 0,
                    },
                ),
            )
            .await
            .unwrap();
            if abort_fails {
                assert!(completed
                    .unwrap_err()
                    .to_string()
                    .contains("abort not settled"));
            } else {
                completed.unwrap();
            }
            assert_eq!(backend.commits.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(backend.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
            let graph = services
                .graph_state_store()
                .load(&expected.graph_id)
                .unwrap();
            assert_eq!(
                graph.node_statuses[&expected.node_id],
                ExecutionNodeStatus::Cancelled
            );
            assert!(graph.terminal_presentation.is_none());
            let starts = services
                .graph_state_store()
                .binding_starts_page(&graph.id, 0, graph.revision)
                .unwrap()
                .1;
            assert_eq!(starts.len(), 1);
            let (node_id, binding, position) = &starts[0];
            assert_eq!(
                services
                    .graph_state_store()
                    .binding_lease_closure(&graph, node_id, binding, *position)
                    .unwrap(),
                !abort_fails
            );
            let events = services
                .event_store()
                .list_stream(
                    &crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(
                        &expected,
                    ),
                )
                .unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].kind, "execution.effect.intent");
            assert_eq!(events[1].kind, "execution.effect.receipt");
            let graph_events = services
                .event_store()
                .list_stream(&expected.graph_id)
                .unwrap();
            let node_events = services
                .event_store()
                .list_stream(&format!("{}:node:{}", expected.graph_id, expected.node_id))
                .unwrap();
            let mut cancelled = node_events.iter().filter(|event| {
                event.kind == "execution_node.transitioned" && event.payload["to"] == "cancelled"
            });
            let transition = cancelled.next().unwrap();
            assert!(cancelled.next().is_none());
            assert_eq!(events[1].transaction_id, transition.transaction_id);
            assert_eq!(events[1].commit_cursor, transition.commit_cursor);
            assert!(graph_events.iter().any(|event| {
                event.transaction_id == transition.transaction_id
                    && event.commit_cursor == transition.commit_cursor
            }));
            assert_eq!(events[1].payload["result"]["status"], "cancelled");
            assert!(
                matches!(services.commit_service().inspect_execution_effect(&expected).unwrap(),
            crate::execution_core::graph::ExecutionEffectState::Completed(outcome)
                if outcome.result.status == ExecutionNodeStatus::Cancelled)
            );
            // Custody closure positive: once the original retirement receipt is
            // durably committed, the callback disposition is the canonical Abort,
            // so `complete_callback_observed_until` can release. The fence is
            // unchanged: an unconfirmed original still projects Pending below.
            assert_eq!(
                services
                    .commit_service()
                    .binding_callback_disposition(&graph, node_id.as_str(), binding)
                    .unwrap(),
                CallbackDisposition::Abort
            );
            services.shutdown_maintenance().await;
        }
    }

    struct PendingSynthesisBackend {
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl SynthesizeBackend for PendingSynthesisBackend {
        async fn synthesize(
            &self,
            _ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, String> {
            self.release.notified().await;
            Err("terminal_presentation_cancelled".into())
        }
        async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
            Ok(())
        }
        async fn after_abort(
            &self,
            _ticket: &NodeExecutionTicket,
            _reason: &str,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct PendingSynthesisResolver {
        expected: NodeExecutionTicket,
        backend: Arc<PendingSynthesisBackend>,
    }

    impl SynthesizeBackendResolver for PendingSynthesisResolver {
        fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
            (ticket == &self.expected).then(|| self.backend.clone() as Arc<dyn SynthesizeBackend>)
        }
    }

    #[tokio::test]
    async fn r1_synthesize_unconfirmed_original_still_refuses_release() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
        };
        let services = crate::RuntimeServices::in_memory().unwrap();
        let mut graph = ExecutionGraph::new("Unconfirmed synthesis custody");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "pending-session".into(),
            turn_id: "pending-turn".into(),
            root_task_id: "pending-task".into(),
            task_id: "pending-task".into(),
            generation: 1,
        });
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Synthesize,
            "synthesize",
            "pending-original",
        );
        let expected = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: "synthesize".into(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        graph.nodes.push(node);
        let release = Arc::new(tokio::sync::Notify::new());
        let backend = Arc::new(PendingSynthesisBackend { release: release.clone() });
        services.synthesize_executor().install_resolver(Arc::new(
            PendingSynthesisResolver { expected: expected.clone(), backend },
        ));
        let graph_id = graph.id.clone();
        let node_id = expected.node_id.clone();
        let supervisor = Arc::clone(services.execution_supervisor());
        let driver = tokio::spawn(async move {
            supervisor
                .submit_and_wait_terminal(graph, ExecutionGraphCommand::Start { expected_revision: 0 })
                .await
        });
        let reached_pending = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = services.graph_state_store().load(&graph_id).unwrap();
                if snapshot.node_statuses.get(&node_id) == Some(&ExecutionNodeStatus::Running) {
                    let starts = services
                        .graph_state_store()
                        .binding_starts_page(&graph_id, 0, snapshot.revision)
                        .unwrap()
                        .1;
                    if let Some((_, binding, _)) = starts.first() {
                        if services
                            .commit_service()
                            .binding_callback_disposition(&snapshot, &node_id, binding)
                            .unwrap()
                            == CallbackDisposition::Pending
                        {
                            return;
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(reached_pending.is_ok(), "original binding never reached started-but-unconfirmed Pending");
        let snapshot = services.graph_state_store().load(&graph_id).unwrap();
        let starts = services
            .graph_state_store()
            .binding_starts_page(&graph_id, 0, snapshot.revision)
            .unwrap()
            .1;
        let (_, binding, _) = starts.first().unwrap();
        assert_eq!(
            services
                .commit_service()
                .binding_callback_disposition(&snapshot, &node_id, binding)
                .unwrap(),
            CallbackDisposition::Pending
        );
        release.notify_waiters();
        driver.abort();
        services.shutdown_maintenance().await;
    }

    #[async_trait::async_trait]
    impl SynthesizeBackend for ReadinessBackend {
        async fn synthesize(
            &self,
            _ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, String> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(outcome())
        }
    }

    struct ExactReadinessResolver {
        expected: NodeExecutionTicket,
        backend: std::sync::Weak<ReadinessBackend>,
    }

    impl SynthesizeBackendResolver for ExactReadinessResolver {
        fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
            if ticket != &self.expected {
                return None;
            }
            self.backend
                .upgrade()
                .map(|backend| backend as Arc<dyn SynthesizeBackend>)
        }
    }

    #[tokio::test]
    async fn r1_synthesize_readiness_requires_exact_ticket_without_dispatch() {
        let executor = SynthesizeNodeExecutor::new();
        let mut expected = ticket();
        expected.attempt = 7;
        expected.service_class =
            harness_contract::execution_graph::ExecutionServiceClass::Background;
        assert!(!executor.is_bound(&expected));
        let backend = Arc::new(ReadinessBackend {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        executor.install_resolver(Arc::new(ExactReadinessResolver {
            expected: expected.clone(),
            backend: Arc::downgrade(&backend),
        }));
        assert!(executor.is_bound(&expected));
        for field in [
            "graph",
            "node",
            "executor",
            "service",
            "attempt",
            "idempotency",
            "payload",
        ] {
            let mut foreign = expected.clone();
            match field {
                "graph" => foreign.graph_id.push_str(":foreign"),
                "node" => foreign.node_id.push_str(":foreign"),
                "executor" => foreign.executor_kind = "tool_batch".into(),
                "service" => {
                    foreign.service_class =
                        harness_contract::execution_graph::ExecutionServiceClass::Foreground
                }
                "attempt" => foreign.attempt += 1,
                "idempotency" => foreign.idempotency_key.push_str(":foreign"),
                "payload" => foreign.payload_ref.push_str(":foreign"),
                _ => unreachable!(),
            }
            assert!(!executor.is_bound(&foreign), "{field}");
        }
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(executor.poll_or_await(&expected).await.unwrap(), outcome());
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn r1_synthesize_readiness_does_not_cache_expired_backend() {
        let executor = SynthesizeNodeExecutor::new();
        let expected = ticket();
        let backend = Arc::new(ReadinessBackend {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        executor.install_resolver(Arc::new(ExactReadinessResolver {
            expected: expected.clone(),
            backend: Arc::downgrade(&backend),
        }));
        assert!(executor.is_bound(&expected));
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(backend);
        assert!(!executor.is_bound(&expected));
        let replacement = Arc::new(ReadinessBackend {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        executor.install_resolver(Arc::new(ExactReadinessResolver {
            expected: expected.clone(),
            backend: Arc::downgrade(&replacement),
        }));
        assert!(executor.is_bound(&expected));
        assert_eq!(
            replacement.calls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    fn envelope() -> DeliveryEnvelope {
        DeliveryEnvelope {
            envelope_id: "delivery:graph:4".to_string(),
            revision: 4,
            objective_id: "graph".to_string(),
            pipeline_status: PipelineStatus::Completed,
            delivery_status: DeliveryStatus::Partial,
            branch_terminals: Vec::new(),
            verified_receipts: Vec::new(),
            verified_artifacts: Vec::new(),
            verified_effects: Vec::new(),
            workspace_materializations: Vec::new(),
            coverage: DeliveryCoverage::default(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
            cancellation: None,
            user_answer_contract: UserAnswerContract::default(),
            created_at_ms: 1,
        }
    }

    fn outcome() -> NodeExecutionOutcome {
        let envelope = envelope();
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("assistant_json:\"answer\"".to_string()),
            wait_class: None,
            summary: Some("answer".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: 1,
        });
        outcome.terminal_presentation = Some(TerminalPresentation {
            presentation_id: "presentation".to_string(),
            attempt_id: "candidate".to_string(),
            envelope_id: envelope.envelope_id.clone(),
            envelope_revision: envelope.revision,
            state: TerminalPresentationState::Validating,
            answer_origin: AnswerOrigin::TeamSynthesizer,
            source_execution_id: Some("agent-run".to_string()),
            narrator_model: Some("model".to_string()),
            narrator_provider: Some("provider".to_string()),
            models_attempted: Vec::new(),
            validation: AnswerValidation::default(),
            fallback_reason: None,
            generated_at_ms: 1,
            committed_at_ms: None,
        });
        outcome.delivery_envelope = Some(envelope);
        outcome
    }

    #[test]
    fn team_synthesizer_requires_matching_delivery_envelope() {
        let mut outcome = outcome();
        assert!(validate_terminal_candidate(&ticket(), &outcome).is_ok());

        outcome.delivery_envelope = None;
        assert!(validate_terminal_candidate(&ticket(), &outcome).is_err());
    }

    #[test]
    fn mechanical_reduction_without_presentation_is_not_promoted() {
        let mut outcome = outcome();
        outcome.result.result_ref = Some("delivery-envelope: delivery:graph:4".to_string());
        outcome.terminal_presentation = None;
        assert!(validate_terminal_candidate(&ticket(), &outcome).is_ok());
    }
}
