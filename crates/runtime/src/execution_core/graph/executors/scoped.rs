use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use harness_contract::execution_graph::ExecutionNodeSpec;

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};

#[async_trait]
pub trait ScopedNodeBackend: Send + Sync {
    /// The executor must retain this future through cancellation until the
    /// physical operation and its effect receipt have finished.
    fn retains_physical_execution(&self) -> bool {
        false
    }
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError>;
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        Ok(())
    }
}

pub trait ScopedNodeBackendResolver: Send + Sync {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>>;
}

type PhysicalKey = (String, String, u32, String);
#[derive(Clone)]
enum PhysicalOutcome {
    Node(NodeExecutionOutcome),
    Tool(crate::RuntimeToolExecutionOutcome),
}
type PhysicalWork = Shared<BoxFuture<'static, Result<PhysicalOutcome, NodeExecutorError>>>;

/// A fixed executor that reconstructs its backend through durable-ticket resolvers.
pub struct ScopedNodeExecutor {
    kind: &'static str,
    resolvers: RwLock<Vec<Arc<dyn ScopedNodeBackendResolver>>>,
    physical: Mutex<BTreeMap<PhysicalKey, Option<PhysicalWork>>>,
}

impl ScopedNodeExecutor {
    #[must_use]
    pub fn new(kind: &'static str) -> Self {
        Self {
            kind,
            resolvers: RwLock::new(Vec::new()),
            physical: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn install_resolver(&self, resolver: Arc<dyn ScopedNodeBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
    }

    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        self.resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(ticket))
    }

    /// Whether the exact durable node can currently be reconstructed.
    /// Startup recovery uses this before waking a graph so a process-level
    /// producer cannot race Session-scoped resolver installation.
    #[must_use]
    pub(crate) fn is_bound(&self, ticket: &NodeExecutionTicket) -> bool {
        self.resolve(ticket).is_some()
    }

    fn physical_key(ticket: &NodeExecutionTicket, leaf: &str) -> PhysicalKey {
        (
            ticket.graph_id.clone(),
            ticket.node_id.clone(),
            ticket.attempt,
            leaf.into(),
        )
    }

    fn owns_key(ticket: &NodeExecutionTicket, key: &PhysicalKey) -> bool {
        key.0 == ticket.graph_id && key.1 == ticket.node_id && key.2 == ticket.attempt
    }

    /// Retain only an already-bound host leaf, never its orchestration caller.
    pub(crate) async fn execute_physical_tool(
        &self,
        ticket: &NodeExecutionTicket,
        invocation_id: &str,
        operation: BoxFuture<
            'static,
            Result<crate::RuntimeToolExecutionOutcome, NodeExecutorError>,
        >,
    ) -> Result<crate::RuntimeToolExecutionOutcome, NodeExecutorError> {
        if invocation_id.is_empty() || ticket.executor_kind != self.kind {
            return Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "physical leaf has no owning ticket or invocation".into(),
            });
        }
        let key = Self::physical_key(ticket, invocation_id);
        let work = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if physical
                .get(&Self::physical_key(ticket, ""))
                .is_some_and(Option::is_none)
            {
                return Err(NodeExecutorError::Cancel {
                    node_id: ticket.node_id.clone(),
                    reason: "cancelled before physical leaf admission".into(),
                });
            }
            physical
                .entry(key.clone())
                .or_insert_with(|| {
                    Some(
                        async move { operation.await.map(PhysicalOutcome::Tool) }
                            .boxed()
                            .shared(),
                    )
                })
                .clone()
                .expect("leaf entry is an execution")
        };
        let outcome = work.await;
        // The leaf itself committed its effect/read receipt before resolving.
        // Do not retain large tool bodies until the containing model turn ends.
        self.physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        match outcome? {
            PhysicalOutcome::Tool(outcome) => Ok(outcome),
            PhysicalOutcome::Node(_) => Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "physical leaf identity collides with node execution".into(),
            }),
        }
    }
}

#[async_trait]
impl NodeExecutor for ScopedNodeExecutor {
    fn kind(&self) -> &str {
        self.kind
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == self.kind {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: format!("node must use canonical {} executor", self.kind),
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
            executor_kind: self.kind.to_string(),
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
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        if !backend.retains_physical_execution() {
            return backend.execute(ticket).await;
        }
        let key = Self::physical_key(ticket, "");
        let work = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            physical
                .entry(key)
                .or_insert_with(|| {
                    let ticket = ticket.clone();
                    Some(
                        async move { backend.execute(&ticket).await.map(PhysicalOutcome::Node) }
                            .boxed()
                            .shared(),
                    )
                })
                .clone()
        };
        match work {
            Some(work) => match work.await? {
                PhysicalOutcome::Node(outcome) => Ok(outcome),
                PhysicalOutcome::Tool(_) => Err(NodeExecutorError::Invalid {
                    node_id: ticket.node_id.clone(),
                    reason: "node identity collides with physical leaf".into(),
                }),
            },
            None => Err(NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: "cancelled before physical execution".into(),
            }),
        }
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.cancel(ticket).await?;
        self.physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key, _| !Self::owns_key(ticket, key));
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        backend.after_commit(ticket).await
    }
    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let pending = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            physical
                .entry(Self::physical_key(ticket, ""))
                .or_insert(None);
            physical
                .iter()
                .filter(|(key, _)| Self::owns_key(ticket, key))
                .filter_map(|(_, work)| work.clone())
                .collect::<Vec<_>>()
        };
        futures::future::join_all(pending).await;
        Ok(())
    }
    fn cancellation_requires_quiescence(&self, _ticket: &NodeExecutionTicket) -> bool {
        // Admission can race this query. Even an InlineModel with no leaf yet
        // must atomically close leaf admission and drain through cancel().
        true
    }
    async fn drain_physical_execution(&self) {
        let pending = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(key, work)| (key.clone(), work.clone()))
            .collect::<Vec<_>>();
        futures::future::join_all(pending.into_iter().map(|(key, work)| async move {
            if let Some(work) = work {
                let _ = work.await;
            }
            self.physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&key);
        }))
        .await;
    }
    fn cancellation_finalized(&self, ticket: &NodeExecutionTicket) {
        let mut physical = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A graph command waiter may itself be abandoned. Finalization of its
        // intent must not drop a still-running physical future.
        if physical.iter().any(|(key, work)| {
            Self::owns_key(ticket, key) && work.as_ref().is_some_and(|work| work.peek().is_none())
        }) {
            return;
        }
        physical.retain(|key, _| !Self::owns_key(ticket, key));
    }
    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        _: &str,
    ) -> Result<(), NodeExecutorError> {
        self.cancel(ticket).await?;
        self.cancellation_finalized(ticket);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::{ExecutionNodeResult, ExecutionNodeStatus};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct PhysicalBackend {
        started: AtomicBool,
        completed: AtomicBool,
        release: tokio::sync::Notify,
    }
    #[async_trait]
    impl ScopedNodeBackend for PhysicalBackend {
        fn retains_physical_execution(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            self.started.store(true, Ordering::Release);
            self.release.notified().await;
            self.completed.store(true, Ordering::Release);
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                summary: None,
                evidence_refs: vec![],
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }
    struct Resolver(Arc<PhysicalBackend>);
    impl ScopedNodeBackendResolver for Resolver {
        fn resolve(&self, _: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
            Some(self.0.clone())
        }
    }
    fn ticket() -> NodeExecutionTicket {
        NodeExecutionTicket {
            graph_id: "physical-graph".into(),
            node_id: "physical-node".into(),
            executor_kind: "tool_batch".into(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "physical-request".into(),
            payload_ref: "physical-payload".into(),
        }
    }
    #[tokio::test]
    async fn tool_and_early_model_owners_drain_abandoned_leaf_before_unlocking() {
        for kind in ["inline_model", "tool_batch"] {
            let executor = Arc::new(ScopedNodeExecutor::new(kind));
            let mut request = ticket();
            request.executor_kind = kind.into();
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("physical-write.txt");
            let started = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let caller = {
                let executor = executor.clone();
                let request = request.clone();
                let started = started.clone();
                let release = release.clone();
                let path = path.clone();
                tokio::spawn(async move {
                    executor
                        .execute_physical_tool(
                            &request,
                            "leaf",
                            Box::pin(async move {
                                std::fs::write(&path, "started").unwrap();
                                started.notify_one();
                                release.notified().await;
                                std::fs::write(&path, "finished").unwrap();
                                Ok(crate::RuntimeToolExecutionOutcome {
                                    tool_use_id: "leaf".into(),
                                    tool_name: "write_file".into(),
                                    status: crate::RuntimeToolExecutionStatus::Executed,
                                    category: crate::ToolSafetyCategory::WriteLocal,
                                    output: None,
                                    error: None,
                                    evidence_ref: "fixture".into(),
                                    observed_evidence: vec![],
                                })
                            }),
                        )
                        .await
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
                .await
                .unwrap();
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "started");
            assert!(executor.cancellation_requires_quiescence(&request));
            let mut cancel = Box::pin(executor.cancel(&request));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut cancel)
                    .await
                    .is_err()
            );
            drop(cancel);
            executor.cancellation_finalized(&request);
            let late = executor
                .execute_physical_tool(
                    &request,
                    "late",
                    Box::pin(async { panic!("cancelled leaf must not start") }),
                )
                .await;
            assert!(late.is_err());
            release.notify_one();
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                executor.drain_physical_execution(),
            )
            .await
            .unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "finished");
            assert!(executor.physical.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn cancelled_waiter_is_drained_by_original_executor_before_finalization() {
        let backend = Arc::new(PhysicalBackend {
            started: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        let executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        executor.install_resolver(Arc::new(Resolver(backend.clone())));
        let waiter = {
            let executor = executor.clone();
            tokio::spawn(async move { executor.poll_or_await(&ticket()).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !backend.started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(executor.physical.lock().unwrap().len(), 1);
        assert!(executor.cancellation_requires_quiescence(&ticket()));
        let request = ticket();
        let mut cancellation = Box::pin(executor.cancel(&request));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut cancellation)
                .await
                .is_err()
        );
        assert!(!backend.completed.load(Ordering::Acquire));
        // Abandoning the cancellation waiter must not let its finalizer erase
        // the physical future. A later owner still has to drain it.
        drop(cancellation);
        executor.cancellation_finalized(&request);
        assert_eq!(executor.physical.lock().unwrap().len(), 1);
        backend.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), executor.cancel(&request))
            .await
            .unwrap()
            .unwrap();
        assert!(backend.completed.load(Ordering::Acquire));
        executor.cancellation_finalized(&request);
        assert!(executor.physical.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn shutdown_drain_finishes_abandoned_effect_and_releases_retained_owner() {
        let backend = Arc::new(PhysicalBackend {
            started: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        let executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        executor.install_resolver(Arc::new(Resolver(backend.clone())));
        let waiter = {
            let executor = executor.clone();
            tokio::spawn(async move { executor.poll_or_await(&ticket()).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !backend.started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let mut drain = Box::pin(executor.drain_physical_execution());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain)
                .await
                .is_err()
        );
        assert!(!backend.completed.load(Ordering::Acquire));
        backend.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), drain)
            .await
            .unwrap();
        assert!(backend.completed.load(Ordering::Acquire));
        assert!(executor.physical.lock().unwrap().is_empty());
        assert_eq!(
            Arc::strong_count(&backend),
            2,
            "only test and resolver remain"
        );
    }

    #[tokio::test]
    async fn cancellation_before_poll_never_starts_physical_work() {
        let backend = Arc::new(PhysicalBackend {
            started: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        let executor = ScopedNodeExecutor::new("tool_batch");
        executor.install_resolver(Arc::new(Resolver(backend.clone())));
        executor.cancel(&ticket()).await.unwrap();
        assert!(executor.poll_or_await(&ticket()).await.is_err());
        assert!(!backend.started.load(Ordering::Acquire));
        executor.cancellation_finalized(&ticket());
        assert!(executor.physical.lock().unwrap().is_empty());
    }
}
