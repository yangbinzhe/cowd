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

#[derive(Clone, Default)]
struct PhysicalEntry {
    work: Option<PhysicalWork>,
    publication_completed: bool,
    observation: Option<Arc<super::agent_tool::NoDispatchObservation>>,
    closed: bool,
    original_binding: bool,
}

/// A fixed executor that reconstructs its backend through durable-ticket resolvers.
pub struct ScopedNodeExecutor {
    kind: &'static str,
    resolvers: RwLock<Vec<Arc<dyn ScopedNodeBackendResolver>>>,
    physical: Mutex<BTreeMap<PhysicalKey, PhysicalEntry>>,
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

    pub(crate) fn observe_binding(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<Arc<super::agent_tool::NoDispatchObservation>, NodeExecutorError> {
        if self.kind != "tool_batch" || ticket.executor_kind != self.kind {
            return Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "not the builtin ToolBatch owner".into(),
            });
        }
        let observation = super::agent_tool::NoDispatchObservation::new(ticket)?;
        let mut physical = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::physical_key(ticket, "");
        if physical.contains_key(&key) {
            return Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "original ToolBatch observation already admitted".into(),
            });
        }
        physical.insert(
            key,
            PhysicalEntry {
                observation: Some(observation.clone()),
                original_binding: true,
                ..Default::default()
            },
        );
        Ok(observation)
    }

    pub(crate) fn rollback_unadmitted_binding(
        &self,
        ticket: &NodeExecutionTicket,
        observation: &Arc<super::agent_tool::NoDispatchObservation>,
    ) -> Result<(), NodeExecutorError> {
        let mut physical = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::physical_key(ticket, "");
        let original = physical.get(&key).is_some_and(|entry| {
            entry.original_binding
                && !entry.closed
                && entry.work.is_none()
                && observation.ticket_matches(ticket)
                && entry
                    .observation
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, observation))
        });
        let has_leaf = physical
            .keys()
            .any(|candidate| candidate != &key && Self::owns_key(ticket, candidate));
        if !original || has_leaf || !observation.abandon_unadmitted() {
            return Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "unadmitted rollback cannot discard an entered or foreign original owner"
                    .into(),
            });
        }
        physical.remove(&key);
        Ok(())
    }

    pub(crate) fn binding_observation(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<super::agent_tool::NoDispatchObservation>> {
        self.physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&Self::physical_key(ticket, ""))
            .and_then(|entry| entry.observation.clone())
            .filter(|observation| observation.ticket_matches(ticket))
    }

    pub(crate) fn forget_closed_binding(&self, ticket: &NodeExecutionTicket) {
        let mut physical = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::physical_key(ticket, "");
        if physical.get(&key).is_some_and(|entry| {
            entry.closed && entry.work.as_ref().is_none_or(|work| work.peek().is_some())
        }) {
            if let Some(entry) = physical.get_mut(&key) {
                entry.work = None;
                entry.observation = None;
                entry.publication_completed = false;
            }
        }
    }

    fn close_entry(entry: &mut PhysicalEntry) {
        entry.closed = true;
        if let Some(observation) = &entry.observation {
            observation.close();
        }
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
                .is_some_and(|entry| entry.closed)
            {
                return Err(NodeExecutorError::Cancel {
                    node_id: ticket.node_id.clone(),
                    reason: "cancelled before physical leaf admission".into(),
                });
            }
            physical
                .entry(key.clone())
                .or_insert_with(|| PhysicalEntry {
                    work: Some(
                        async move { operation.await.map(PhysicalOutcome::Tool) }
                            .boxed()
                            .shared(),
                    ),
                    ..Default::default()
                })
                .work
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

/// Bounded cancellation of the physical work owned by `ticket` (PLAN W3 / R-02).
///
/// `cancel` waits for the physical work to quiesce indefinitely, which is right
/// for a real write but must not block turn terminalization forever. After
/// `budget` we stop waiting, but we do NOT drop the physical future: its
/// scope/resource lease lives inside it, so dropping it would release a lock
/// mid-write. Instead we keep the lease alive and drive the retained future to
/// completion on a detached task, so the physical owner releases its own
/// admission when it truly finishes.
async fn cancel_scoped_bounded(
    executor: &ScopedNodeExecutor,
    ticket: &NodeExecutionTicket,
    budget: std::time::Duration,
) -> Result<(), NodeExecutorError> {
    let pending = {
        let mut physical = executor
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = physical
            .entry(ScopedNodeExecutor::physical_key(ticket, ""))
            .or_default();
        ScopedNodeExecutor::close_entry(entry);
        physical
            .iter()
            .filter(|(key, _)| ScopedNodeExecutor::owns_key(ticket, key))
            .filter_map(|(_, entry)| entry.work.clone())
            .collect::<Vec<_>>()
    };
    let retained = pending.clone();
    match tokio::time::timeout(budget, futures::future::join_all(pending)).await {
        Ok(_) => Ok(()),
        Err(_) => {
            let orphaned = retained.len() as u64;
            for work in retained {
                tokio::spawn(async move {
                    let _ = work.await;
                });
            }
            crate::execution_core::performance::observe_count(
                "scoped_cancel_orphaned_total",
                orphaned,
            );
            Ok(())
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
        {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = physical.get_mut(&Self::physical_key(ticket, "")) {
                if entry.closed
                    || entry
                        .observation
                        .as_ref()
                        .is_some_and(|observation| !observation.enter())
                {
                    return Err(NodeExecutorError::Cancel {
                        node_id: ticket.node_id.clone(),
                        reason: "original physical admission closed".into(),
                    });
                }
            }
        }
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        if !backend.retains_physical_execution() {
            let observation = {
                let mut physical = self
                    .physical
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let entry = physical.entry(Self::physical_key(ticket, "")).or_default();
                if entry.closed
                    || entry
                        .observation
                        .as_ref()
                        .is_some_and(|observation| !observation.enter())
                {
                    return Err(NodeExecutorError::Cancel {
                        node_id: ticket.node_id.clone(),
                        reason: "original physical admission closed".into(),
                    });
                }
                entry.observation.clone()
            };
            return super::agent_tool::finish_observed_outcome(
                ticket,
                observation.as_deref(),
                backend.execute(ticket).await,
            );
        }
        let key = Self::physical_key(ticket, "");
        let work: Option<PhysicalWork> = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = physical.entry(key).or_default();
            if entry.closed
                || entry
                    .observation
                    .as_ref()
                    .is_some_and(|observation| !observation.enter())
            {
                return Err(NodeExecutorError::Cancel {
                    node_id: ticket.node_id.clone(),
                    reason: "original physical admission closed".into(),
                });
            }
            let observation = entry.observation.clone();
            entry
                .work
                .get_or_insert_with(|| {
                    let ticket = ticket.clone();
                    async move {
                        super::agent_tool::finish_observed_outcome(
                            &ticket,
                            observation.as_deref(),
                            backend.execute(&ticket).await,
                        )
                        .map(PhysicalOutcome::Node)
                    }
                    .boxed()
                    .shared()
                })
                .clone()
                .into()
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
        let published = {
            let physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = physical.get(&Self::physical_key(ticket, ""));
            if entry.is_some_and(|entry| {
                entry
                    .observation
                    .as_ref()
                    .is_some_and(|observation| !observation.ticket_matches(ticket))
            }) {
                return Err(NodeExecutorError::Invalid {
                    node_id: ticket.node_id.clone(),
                    reason: "publication retry does not match original physical ticket".into(),
                });
            }
            entry.is_some_and(|entry| {
                entry.original_binding
                    && entry.publication_completed
                    && entry
                        .observation
                        .as_ref()
                        .is_some_and(|observation| observation.ticket_matches(ticket))
            })
        };
        // The success path must not abandon an in-flight physical write: wait
        // for it. Bounded cancellation is reserved for cancel/abort paths, where
        // stopping is the point (PLAN W3 / R-02).
        self.cancel(ticket).await?;
        self.physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key, entry| !Self::owns_key(ticket, key) || entry.original_binding);
        if published {
            return Ok(());
        }
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        backend.after_commit(ticket).await?;
        if let Some(entry) = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&Self::physical_key(ticket, ""))
            .filter(|entry| {
                entry.original_binding
                    && entry
                        .observation
                        .as_ref()
                        .is_some_and(|observation| observation.ticket_matches(ticket))
            })
        {
            entry.publication_completed = true;
        }
        Ok(())
    }
    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let pending = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = physical.entry(Self::physical_key(ticket, "")).or_default();
            Self::close_entry(entry);
            physical
                .iter()
                .filter(|(key, _)| Self::owns_key(ticket, key))
                .filter_map(|(_, entry)| entry.work.clone())
                .collect::<Vec<_>>()
        };
        futures::future::join_all(pending).await;
        Ok(())
    }

    async fn cancel_bounded(
        &self,
        ticket: &NodeExecutionTicket,
        budget: std::time::Duration,
    ) -> Result<(), NodeExecutorError> {
        cancel_scoped_bounded(self, ticket, budget).await
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
            .map(|(key, entry)| (key.clone(), entry.work.clone()))
            .collect::<Vec<_>>();
        futures::future::join_all(pending.into_iter().map(|(key, work)| async move {
            if let Some(work) = work {
                let _ = work.await;
            }
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = physical.get_mut(&key) {
                Self::close_entry(entry);
                if entry.original_binding {
                    entry.work = None;
                } else {
                    physical.remove(&key);
                }
            }
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
            Self::owns_key(ticket, key)
                && work.work.as_ref().is_some_and(|work| work.peek().is_none())
        }) {
            return;
        }
        physical.retain(|key, entry| !Self::owns_key(ticket, key) || entry.original_binding);
    }
    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        _: &str,
    ) -> Result<(), NodeExecutorError> {
        self.cancel_bounded(ticket, std::time::Duration::from_secs(5))
            .await?;
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
                wait_class: None,
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

    struct Publication704 {
        calls: std::sync::atomic::AtomicUsize,
        allowed: AtomicBool,
    }

    #[async_trait]
    impl ScopedNodeBackend for Publication704 {
        async fn execute(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            panic!("publication recovery cannot execute a tool");
        }

        async fn after_commit(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<(), NodeExecutorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.allowed.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: "704 publication pending".into(),
                })
            }
        }
    }

    struct PublicationResolver704(std::sync::Weak<Publication704>);

    impl ScopedNodeBackendResolver for PublicationResolver704 {
        fn resolve(&self, _: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
            self.0
                .upgrade()
                .map(|backend| backend as Arc<dyn ScopedNodeBackend>)
        }
    }

    fn publication_ticket_704() -> NodeExecutionTicket {
        let mut request = ticket();
        request.payload_ref =
            crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
                "session_id": "704-session",
                "step_snapshots": {},
                "calls": [{"id":"704-read","name":"read_file","input":"{}","depends_on":[]}]
            }))
            .unwrap();
        request
    }

    #[tokio::test]
    async fn r1_704_published_original_callback_survives_dropped_host_without_reexecution() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let request = publication_ticket_704();
        executor.observe_binding(&request).unwrap();
        let backend = Arc::new(Publication704 {
            calls: std::sync::atomic::AtomicUsize::new(0),
            allowed: AtomicBool::new(true),
        });
        executor.install_resolver(Arc::new(PublicationResolver704(Arc::downgrade(&backend))));
        executor.after_commit(&request).await.unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        drop(backend);
        assert!(!executor.is_bound(&request));
        executor.after_commit(&request).await.unwrap();
        for field in ["payload", "idempotency", "attempt", "graph", "node"] {
            let mut foreign = request.clone();
            match field {
                "payload" => foreign.payload_ref.push(' '),
                "idempotency" => foreign.idempotency_key.push_str("-foreign"),
                "attempt" => foreign.attempt += 1,
                "graph" => foreign.graph_id.push_str("-foreign"),
                _ => foreign.node_id.push_str("-foreign"),
            }
            assert!(executor.after_commit(&foreign).await.is_err(), "{field}");
        }
        executor.forget_closed_binding(&request);
        assert!(executor.after_commit(&request).await.is_err());
    }

    #[tokio::test]
    async fn r1_704_failed_publication_is_not_cached_and_cold_owner_must_rebind() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let request = publication_ticket_704();
        executor.observe_binding(&request).unwrap();
        let backend = Arc::new(Publication704 {
            calls: std::sync::atomic::AtomicUsize::new(0),
            allowed: AtomicBool::new(false),
        });
        executor.install_resolver(Arc::new(PublicationResolver704(Arc::downgrade(&backend))));
        assert!(executor.after_commit(&request).await.is_err());
        assert!(executor.after_commit(&request).await.is_err());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
        drop(backend);
        assert!(executor.after_commit(&request).await.is_err());
        let rebound = Arc::new(Publication704 {
            calls: std::sync::atomic::AtomicUsize::new(0),
            allowed: AtomicBool::new(true),
        });
        executor.install_resolver(Arc::new(PublicationResolver704(Arc::downgrade(&rebound))));
        executor.after_commit(&request).await.unwrap();
        assert_eq!(rebound.calls.load(Ordering::SeqCst), 1);
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
                                    tool_name: "edit".into(),
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

    #[cfg(test)]
    mod nodispatch_520_tests {
        use super::*;

        fn original_batch_ticket() -> NodeExecutionTicket {
            let mut original = ticket();
            original.payload_ref =
                crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
                    "session_id": "session-520",
                    "calls": [
                        {"id": "refused-520", "name": "read_file", "input": "{}", "depends_on": []},
                        {"id": "dispatched-520", "name": "read_file", "input": "{}", "depends_on": []}
                    ],
                    "step_snapshots": {}
                }))
                .unwrap();
            original
        }

        #[tokio::test]
        async fn nodispatch_520_cancel_before_first_poll_prevents_backend_and_leaf() {
            let original = original_batch_ticket();
            let backend = Arc::new(PhysicalBackend {
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                release: tokio::sync::Notify::new(),
            });
            let executor = ScopedNodeExecutor::new("tool_batch");
            executor.install_resolver(Arc::new(Resolver(backend.clone())));
            let observation = executor.observe_binding(&original).unwrap();
            executor.cancel(&original).await.unwrap();
            assert!(matches!(
                executor.poll_or_await(&original).await,
                Err(NodeExecutorError::Cancel { .. })
            ));
            assert!(matches!(
                executor
                    .execute_physical_tool(
                        &original,
                        "dispatched-520",
                        Box::pin(async { panic!("closed owner must not poll a physical leaf") })
                    )
                    .await,
                Err(NodeExecutorError::Cancel { .. })
            ));
            assert!(!backend.started.load(Ordering::Acquire));
            assert!(!backend.completed.load(Ordering::Acquire));
            let witness = observation.witness().unwrap();
            assert!(witness.fact().never_polled);
            assert_eq!(witness.fact().calls.len(), 2);
            assert!(witness
                .fact()
                .calls
                .iter()
                .all(|call| call.reason == "cancelled_before_first_poll"));
            witness.authenticate(&original, None).unwrap();
        }

        #[tokio::test]
        async fn nodispatch_520_polled_unavailable_owner_cannot_become_never_polled() {
            let original = original_batch_ticket();
            let executor = ScopedNodeExecutor::new("tool_batch");
            let observation = executor.observe_binding(&original).unwrap();
            assert!(matches!(
                executor.poll_or_await(&original).await,
                Err(NodeExecutorError::Unavailable { .. })
            ));
            executor.cancel(&original).await.unwrap();
            executor.drain_physical_execution().await;
            executor.cancellation_finalized(&original);
            assert!(observation.witness().is_none());
            assert!(!observation.enter());
        }

        #[tokio::test]
        async fn nodispatch_520_repeated_cancel_cleanup_cannot_remint_never_polled() {
            for entered in [false, true] {
                let original = original_batch_ticket();
                let executor = ScopedNodeExecutor::new("tool_batch");
                let observation = executor.observe_binding(&original).unwrap();
                if entered {
                    assert!(matches!(
                        executor.poll_or_await(&original).await,
                        Err(NodeExecutorError::Unavailable { .. })
                    ));
                }
                executor.cancel(&original).await.unwrap();
                let initial = observation.witness();
                assert_eq!(initial.is_some(), !entered);
                for _round in 0..3 {
                    executor
                        .cancel_bounded(&original, std::time::Duration::from_secs(1))
                        .await
                        .unwrap();
                    executor.drain_physical_execution().await;
                    executor.cancellation_finalized(&original);
                    executor.forget_closed_binding(&original);
                    executor.cancel(&original).await.unwrap();
                    assert_eq!(observation.witness(), initial);
                    assert!(executor.binding_observation(&original).is_none());
                    assert!(
                        executor.observe_binding(&original).is_err(),
                        "cleanup must retain the original closed admission tombstone"
                    );
                    assert!(matches!(
                        executor.poll_or_await(&original).await,
                        Err(NodeExecutorError::Cancel { .. })
                    ));
                }
            }
        }

        #[tokio::test]
        async fn nodispatch_520_mixed_finish_excludes_real_dispatched_read() {
            let original = original_batch_ticket();
            let executor = ScopedNodeExecutor::new("tool_batch");
            let observation = executor.observe_binding(&original).unwrap();
            assert!(observation.enter());
            observation.refuse("refused-520", "admission refused".into());
            let request = crate::RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: "dispatched-520".into(),
                    tool_name: "read_file".into(),
                    input: "{}".into(),
                    depends_on: Vec::new(),
                },
            );
            observation.uncertain(&request);
            let workspace = tempfile::tempdir().unwrap();
            let path = workspace.path().join("physical-read-520.txt");
            std::fs::write(&path, "actual dispatched bytes").unwrap();
            let polled = Arc::new(AtomicBool::new(false));
            let polled_by_leaf = polled.clone();
            let leaf = executor
                .execute_physical_tool(
                    &original,
                    "dispatched-520",
                    Box::pin(async move {
                        polled_by_leaf.store(true, Ordering::Release);
                        let output = std::fs::read_to_string(&path).unwrap();
                        Ok(crate::RuntimeToolExecutionOutcome {
                            tool_use_id: "dispatched-520".into(),
                            tool_name: "read_file".into(),
                            status: crate::RuntimeToolExecutionStatus::Executed,
                            category: crate::ToolSafetyCategory::ReadOnly,
                            output: Some(output),
                            error: None,
                            evidence_ref: String::new(),
                            observed_evidence: Vec::new(),
                        })
                    }),
                )
                .await
                .unwrap();
            assert!(polled.load(Ordering::Acquire));
            assert_eq!(leaf.status, crate::RuntimeToolExecutionStatus::Executed);
            assert_eq!(leaf.output.as_deref(), Some("actual dispatched bytes"));
            observation.refuse(
                "dispatched-520",
                "late refusal must not hide dispatch".into(),
            );
            let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Failed,
                result_ref: None,
                wait_class: None,
                summary: Some("one refused call and one completed read".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            });
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap();
            assert!(!witness.fact().never_polled);
            assert_eq!(witness.fact().calls.len(), 1);
            assert_eq!(witness.fact().calls[0].call_id, "refused-520");
            assert_eq!(witness.fact().calls[0].reason, "admission refused");
            witness
                .authenticate(&original, Some(&outcome.result))
                .unwrap();
            executor.cancel(&original).await.unwrap();
            assert_eq!(observation.witness().as_ref(), Some(witness));
        }
    }
}
