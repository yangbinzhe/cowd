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
    /// Nonblocking signal to this exact physical backend, never a fresh resolver.
    fn request_stop(&self, _ticket: &NodeExecutionTicket) {}
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError>;
    /// Read only an already known original durable outcome; never execute.
    async fn recover_known_outcome(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        Ok(None)
    }
    /// Reconcile only the original bound backend; never resolve a new one.
    async fn reconcile_original_custody(
        &self, _ticket: &NodeExecutionTicket,
        _early: &[(String, crate::conversation::EarlyToolDispatchResult)],
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        Ok(None)
    }
    /// Authenticate the original per-call transactions before their handles retire.
    async fn confirm_original_early_release(
        &self, _ticket: &NodeExecutionTicket,
        _workers: &[(String, crate::conversation::EarlyToolDispatchResult)],
    ) -> Result<bool, NodeExecutorError> {
        Ok(false)
    }
    /// Reassess only the already executed original backend; never dispatch.
    async fn revalidate_goal_source(
        &self,
        _ticket: &NodeExecutionTicket,
        _outcome: &NodeExecutionOutcome,
        _stale_stream: &str,
        _expected: u64,
        _actual: u64,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        Ok(None)
    }
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
type PhysicalDriver = Shared<BoxFuture<'static, Result<(), String>>>;
pub(crate) type OriginalEarlyWorker = Shared<BoxFuture<'static, Result<crate::conversation::EarlyToolDispatchResult, String>>>;
tokio::task_local! { static ORIGINAL_EARLY_WORKER: (String, Arc<()>); }

#[derive(Clone)]
struct RetainedEarlyWorker {
    result: OriginalEarlyWorker,
    abort: tokio::task::AbortHandle,
    nonce: Arc<()>,
}

/// A successful drain of the original ticket's physical entry.  This is an
/// in-process owner handoff, not a durable receipt or an execution grant.
pub(crate) struct OriginalPhysicalClose {
    ticket: NodeExecutionTicket,
    observation: Arc<super::agent_tool::NoDispatchObservation>,
    original_child: Option<Arc<OriginalAgentChildCall>>,
}

impl OriginalPhysicalClose {
    pub(crate) fn matches_original_ticket(&self, ticket: &NodeExecutionTicket) -> bool {
        &self.ticket == ticket && self.observation.ticket_matches(ticket)
    }
    pub(crate) fn observation_weak(&self) -> std::sync::Weak<super::agent_tool::NoDispatchObservation> {
        Arc::downgrade(&self.observation)
    }
    pub(crate) fn matches_observation(&self, observation: &std::sync::Weak<super::agent_tool::NoDispatchObservation>) -> bool {
        std::sync::Weak::ptr_eq(observation, &Arc::downgrade(&self.observation))
    }
    pub(crate) fn verify_member_custody(
        &self, ticket: &NodeExecutionTicket, call_id: &str,
        expected: &[(usize, Arc<crate::tool_host::OriginalMemberSettlement>)],
    ) -> Result<(), String> {
        if !self.matches_original_ticket(ticket) { return Err("original physical close ticket differs".into()); }
        self.observation.verify_closed_member_custody(ticket, call_id, expected)
    }
    pub(crate) fn original_agent_child(&self) -> Option<&OriginalAgentChildCall> {
        self.original_child.as_deref()
    }
    pub(crate) fn original_refusal(&self, request: &crate::RuntimeToolExecutionRequest) -> Option<crate::RuntimeToolExecutionOutcome> {
        self.observation.refusal(request)
    }
}

tokio::task_local! {
    static ORIGINAL_PHYSICAL_CLOSE: Arc<OriginalPhysicalClose>;
}

pub(crate) fn current_original_physical_close(
    ticket: &NodeExecutionTicket,
) -> Option<Arc<OriginalPhysicalClose>> {
    ORIGINAL_PHYSICAL_CLOSE.try_with(|close| {
        close.matches_original_ticket(ticket)
            .then(|| Arc::clone(close))
    }).ok().flatten()
}

/// Operation-scoped handoff held by the existing retained physical owner.
/// Its private fields originate in the live caller, never deserialization.
#[derive(Clone)]
pub(crate) struct OriginalAgentChildCall {
    pub(crate) request: Arc<crate::RuntimeToolExecutionRequest>,
    pub(crate) parent_ticket: NodeExecutionTicket,
    pub(crate) packet: harness_contract::agent::AgentTaskPacket,
    pub(crate) store: Arc<crate::RuntimeEventStore>,
    pub(crate) provider_capture: Option<Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>,
    pub(crate) stop: Option<crate::bound_tool_batch::BoundToolStop>,
    pub(crate) process_members: Option<Arc<crate::agent_in_process_worker::ProcessMemberCustody>>,
    pub(crate) release_proof: Arc<std::sync::Mutex<Option<Arc<crate::tool_host::OriginalEffectReleaseProof>>>>,
    child_payload: String,
}
pub(crate) struct PendingAgentChildCall {
    executor: Arc<ScopedNodeExecutor>,
    key: PhysicalKey,
    call: Arc<OriginalAgentChildCall>,
}
impl Drop for PendingAgentChildCall {
    fn drop(&mut self) {
        let mut physical = self.executor.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if physical.get(&self.key).and_then(|entry| entry.original_child.as_ref())
            .is_some_and(|call| Arc::ptr_eq(call, &self.call)) { physical.remove(&self.key); }
    }
}

#[derive(Clone, Default)]
struct PhysicalEntry {
    work: Option<PhysicalWork>,
    publication_completed: bool,
    observation: Option<Arc<super::agent_tool::NoDispatchObservation>>,
    original_child: Option<Arc<OriginalAgentChildCall>>,
    closed: bool,
    original_binding: bool,
    backend: Option<Arc<dyn ScopedNodeBackend>>,
    original_ticket: Option<NodeExecutionTicket>,
    custody_reconciling: bool,
    early_workers: BTreeMap<String, RetainedEarlyWorker>,
    early_release_proven: bool,
    driver: Option<PhysicalDriver>,
    stop_requested: bool,
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

    #[cfg(test)]
    pub(crate) fn original_unknown_worker_for_test(
        &self, ticket: &NodeExecutionTicket, call_id: &str,
    ) -> Option<(crate::conversation::EarlyToolFailure, Arc<()>)> {
        let physical = self.physical.lock().ok()?;
        let entry = physical.get(&Self::physical_key(ticket, ""))?;
        if entry.closed || entry.stop_requested || !entry.custody_reconciling
            || entry.original_ticket.as_ref() != Some(ticket) || entry.backend.is_none()
            || entry.work.as_ref().is_none_or(|work| work.peek().is_none()) { return None; }
        let worker = entry.early_workers.get(call_id)?;
        match worker.result.peek()? {
            Ok(crate::conversation::EarlyToolDispatchResult::Failed(failure))
                if failure.tool_call_id == call_id && failure.owner_ticket.as_ref() == Some(ticket)
                    && failure.kind == crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown =>
                        Some((failure.clone(), Arc::clone(&worker.nonce))),
            Ok(crate::conversation::EarlyToolDispatchResult::Executed(_))
            | Ok(crate::conversation::EarlyToolDispatchResult::Deferred(_))
            | Ok(crate::conversation::EarlyToolDispatchResult::Failed(_))
            | Err(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn retains_original_early_worker_for_test(
        &self, ticket: &NodeExecutionTicket, call_id: &str, nonce: &Arc<()>,
    ) -> bool {
        self.physical.lock().ok().and_then(|physical| physical.get(&Self::physical_key(ticket, ""))
            .filter(|entry| entry.original_ticket.as_ref() == Some(ticket))
            .and_then(|entry| entry.early_workers.get(call_id))
            .map(|worker| Arc::ptr_eq(&worker.nonce, nonce)
                && matches!(worker.result.peek(), Some(Ok(crate::conversation::EarlyToolDispatchResult::Failed(failure)))
                    if failure.kind == crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown
                        && failure.tool_call_id == call_id && failure.owner_ticket.as_ref() == Some(ticket))))
            .unwrap_or(false)
    }

    fn physical_key(ticket: &NodeExecutionTicket, leaf: &str) -> PhysicalKey {
        (
            ticket.graph_id.clone(),
            ticket.node_id.clone(),
            ticket.attempt,
            leaf.into(),
        )
    }

    /// Register the original early producer before it can execute any work.
    pub(crate) fn retain_original_early_worker(
        &self, ticket: &NodeExecutionTicket, call_id: &str,
        work: crate::conversation::EarlyToolDispatchFuture,
    ) -> Result<(OriginalEarlyWorker, tokio::task::AbortHandle), NodeExecutorError> {
        let invalid = |reason: &str| NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: reason.into(),
        };
        if call_id.is_empty() || ticket.executor_kind != self.kind { return Err(invalid("original early worker identity differs")); }
        let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = physical.get_mut(&Self::physical_key(ticket, ""))
            .ok_or_else(|| invalid("original early worker owner unavailable"))?;
        if entry.closed || entry.custody_reconciling || entry.stop_requested
            || entry.original_ticket.as_ref() != Some(ticket) || entry.early_workers.contains_key(call_id)
        { return Err(invalid("original early worker admission closed or identity reused")); }
        let (start, started) = tokio::sync::oneshot::channel::<()>();
        let nonce = Arc::new(());
        let task_nonce = Arc::clone(&nonce);
        let task_call_id = call_id.to_owned();
        let worker = tokio::spawn(async move {
            started.await.map_err(|_| "original early worker admission cancelled".to_string())?;
            Ok::<_, String>(ORIGINAL_EARLY_WORKER.scope((task_call_id, task_nonce), work).await)
        });
        let abort = worker.abort_handle();
        let result: OriginalEarlyWorker = async move {
            worker.await.map_err(|error| format!("original early worker join unconfirmed: {error}"))?
        }.boxed().shared();
        entry.early_workers.insert(call_id.to_owned(), RetainedEarlyWorker { result: result.clone(), abort: abort.clone(), nonce });
        drop(physical);
        start.send(()).map_err(|_| invalid("original early worker start unconfirmed"))?;
        Ok((result, abort))
    }

    pub(crate) fn retain_original_agent_child(
        self: &Arc<Self>, graph_id: &str, node_id: &str, payload: &str,
        request: Arc<crate::RuntimeToolExecutionRequest>, parent_ticket: NodeExecutionTicket,
        packet: harness_contract::agent::AgentTaskPacket, store: Arc<crate::RuntimeEventStore>,
        provider_capture: Option<Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>,
        stop: Option<crate::bound_tool_batch::BoundToolStop>,
        process_members: Option<Arc<crate::agent_in_process_worker::ProcessMemberCustody>>,
    ) -> Result<PendingAgentChildCall, String> {
        if self.kind != "tool_batch" || graph_id.is_empty() || node_id.is_empty() || payload.is_empty() {
            return Err("invalid original child call".into());
        }
        let key = (graph_id.to_owned(), node_id.to_owned(), 0, String::new());
        let call = Arc::new(OriginalAgentChildCall { request, parent_ticket, packet, store, provider_capture, stop, process_members, release_proof: Default::default(), child_payload: payload.to_owned() });
        let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if physical.contains_key(&key) || physical.keys().any(|k| k.0 == graph_id && k.1 == node_id && k.2 > 0) {
            return Err("original child call already admitted".into());
        }
        physical.insert(key.clone(), PhysicalEntry { original_child: Some(Arc::clone(&call)), ..Default::default() });
        Ok(PendingAgentChildCall { executor: Arc::clone(self), key, call })
    }

    pub(crate) fn original_agent_child(&self, ticket: &NodeExecutionTicket) -> Option<Arc<OriginalAgentChildCall>> {
        let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = physical.get(&Self::physical_key(ticket, ""))?;
        if entry.closed || !entry.original_binding || ticket.attempt != 1
            || entry.observation.as_ref().is_none_or(|obs| !obs.ticket_matches(ticket)) { return None; }
        entry.original_child.as_ref().filter(|call| call.child_payload == ticket.payload_ref).cloned()
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
        let pending = (ticket.graph_id.clone(), ticket.node_id.clone(), 0, String::new());
        let original_child = if ticket.attempt == 1 {
            physical.get(&pending).and_then(|entry| entry.original_child.clone())
                .filter(|call| call.child_payload == ticket.payload_ref)
        } else { None };
        if let Some(child) = &original_child {
            if let Some(members) = &child.process_members {
                members.bind(ticket, Arc::clone(&observation)).map_err(|reason| NodeExecutorError::Invalid {
                    node_id: ticket.node_id.clone(), reason,
                })?;
            }
            let owner = child.request.tool_progress.original_fresh_effect();
            let hook = child.request.tool_progress.original_governed_hook();
            if owner.as_ref().is_err() || owner.as_ref().is_ok_and(Option::is_some) || hook.as_ref().is_err() {
                // The child did not create the original intent; never mint a
                // zero-history witness while its original effect is retained.
                observation.uncertain(&child.request);
            } else if hook.as_ref().is_ok_and(Option::is_some) {
                // A hook is not itself an effect. Keep a distinct pending
                // marker until the same child owner commits a checked fact.
                observation.hold_inherited_hook(&child.request);
            }
        }
        if original_child.is_some() { physical.remove(&pending); }
        physical.insert(
            key,
            PhysicalEntry {
                observation: Some(observation.clone()), original_child,
                original_ticket: Some(ticket.clone()), original_binding: true,
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
            entry.closed
                && entry.observation.as_ref().is_none_or(|observation| observation.original_custody_allows_forget(entry.publication_completed))
                && entry.work.as_ref().is_none_or(|work| work.peek().is_some())
                && entry
                    .driver
                    .as_ref()
                    .is_none_or(|driver| matches!(driver.peek(), Some(Ok(()))))
        }) {
            if let Some(entry) = physical.get_mut(&key) {
                entry.work = None;
                entry.backend = None;
                entry.driver = None;
                entry.observation = None;
                entry.original_child = None;
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

    fn retain_driver(entry: &mut PhysicalEntry) {
        if entry.driver.is_none() {
            if let Some(work) = entry.work.as_ref().filter(|work| work.peek().is_none()) {
                let work = work.clone();
                let handle = tokio::spawn(async move {
                    let _ = work.await;
                });
                entry.driver = Some(
                    async move {
                        handle
                            .await
                            .map_err(|_| "original physical driver join failed".to_owned())
                    }
                    .boxed()
                    .shared(),
                );
            }
        }
    }

    fn owns_key(ticket: &NodeExecutionTicket, key: &PhysicalKey) -> bool {
        key.0 == ticket.graph_id && key.1 == ticket.node_id && key.2 == ticket.attempt
    }

    fn close_work(
        &self,
        ticket: &NodeExecutionTicket,
        stop: bool,
    ) -> (Vec<PhysicalWork>, Vec<PhysicalDriver>) {
        let (pending, drivers, backends) = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Self::close_entry(physical.entry(Self::physical_key(ticket, "")).or_default());
            let mut pending = Vec::new();
            let mut drivers = Vec::new();
            let mut backends = Vec::new();
            for (key, entry) in physical
                .iter_mut()
                .filter(|(key, _)| Self::owns_key(ticket, key))
            {
                Self::close_entry(entry);
                if stop && !entry.stop_requested {
                    entry.stop_requested = true;
                    if let Some(backend) = &entry.backend {
                        backends.push(backend.clone());
                    }
                }
                let _ = key;
                if let Some(work) = &entry.work {
                    pending.push(work.clone());
                }
                Self::retain_driver(entry);
                if let Some(driver) = &entry.driver {
                    drivers.push(driver.clone());
                }
            }
            (pending, drivers, backends)
        };
        // Backend hooks can consult their own locks. Never call under physical lock.
        for backend in backends {
            backend.request_stop(ticket);
        }
        (pending, drivers)
    }

    pub(crate) async fn close_and_drain(
        &self,
        ticket: &NodeExecutionTicket,
        stop: bool,
    ) -> Result<(), NodeExecutorError> {
        // Snapshot both under the admission lock. A result becoming available
        // cannot erase an owned driver while this waiter is between the awaits.
        let (pending, drivers) = self.close_work(ticket, stop);
        let early = {
            let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            physical.get(&Self::physical_key(ticket, ""))
                .filter(|entry| entry.original_ticket.as_ref() == Some(ticket))
                .map(|entry| entry.early_workers.values().cloned().collect::<Vec<_>>()).unwrap_or_default()
        };
        if stop { for worker in &early { worker.abort.abort(); } }
        for result in futures::future::join_all(early.into_iter().map(|worker| worker.result)).await {
            result.map_err(|reason| NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason,
            })?;
        }
        for result in futures::future::join_all(drivers).await {
            result.map_err(|reason| NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: format!("original physical driver join failed: {reason}"),
            })?;
        }
        if pending.iter().any(|work| work.peek().is_none()) {
            return Err(NodeExecutorError::Uncertain {
                node_id: ticket.node_id.clone(),
                reason: "original physical work has no completed driver".into(),
            });
        }
        self.physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key, entry| {
                !Self::owns_key(ticket, key)
                    || key.3.is_empty()
                    || entry
                        .work
                        .as_ref()
                        .is_some_and(|work| work.peek().is_none())
                    || entry
                        .driver
                        .as_ref()
                        .is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
            });
        Ok(())
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
        self.execute_physical_tool_for_call(ticket, invocation_id, None, operation).await
    }

    pub(crate) async fn execute_physical_tool_for_call(
        &self, ticket: &NodeExecutionTicket, invocation_id: &str, call_id: Option<&str>,
        operation: BoxFuture<'static, Result<crate::RuntimeToolExecutionOutcome, NodeExecutorError>>,
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
                .is_none_or(|entry| entry.original_ticket.as_ref() != Some(ticket) || entry.closed
                    || entry.stop_requested || (entry.custody_reconciling
                    && !call_id.and_then(|call| entry.early_workers.get(call).map(|worker| (call, worker)))
                        .is_some_and(|(call, worker)| ORIGINAL_EARLY_WORKER.try_with(|(original_call, nonce)|
                            original_call == call && Arc::ptr_eq(nonce, &worker.nonce)).unwrap_or(false))))
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
        // Result availability is not a driver join. A bounded drain may be
        // abandoned, so the original entry must retain its handle independently
        // of both this caller and the drain waiter until a real join succeeds.
        let mut physical = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if physical.get(&key).is_some_and(|entry| {
            entry
                .driver
                .as_ref()
                .is_none_or(|driver| matches!(driver.peek(), Some(Ok(()))))
        }) {
            physical.remove(&key);
        }
        drop(physical);
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
/// completion on its retained driver, so the physical owner releases its own
/// admission when it truly finishes. The original entry also retains a join
/// future; repeated cancellation cannot create unowned duplicate drivers, and
/// a bounded wait expiry is an error/Pending rather than a successful drain.
async fn cancel_scoped_bounded(
    executor: &ScopedNodeExecutor,
    ticket: &NodeExecutionTicket,
    budget: std::time::Duration,
) -> Result<(), NodeExecutorError> {
    match tokio::time::timeout(budget, executor.close_and_drain(ticket, true)).await {
        Ok(result) => result,
        Err(_) => Err(NodeExecutorError::Cancel {
            node_id: ticket.node_id.clone(),
            reason: "physical cancellation remains pending after deadline".into(),
        }),
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

    async fn recover_known_outcome(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        if ticket.executor_kind != self.kind {
            return Err(NodeExecutorError::Recover {
                node_id: ticket.node_id.clone(),
                reason: "known outcome executor kind mismatch".into(),
            });
        }
        let Some(backend) = self.resolve(ticket) else {
            return Ok(None);
        };
        backend.recover_known_outcome(ticket).await
    }

    async fn reconcile_original_custody(
        &self, ticket: &NodeExecutionTicket,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        let key = Self::physical_key(ticket, "");
        let (original, early) = {
            let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(entry) = physical.get_mut(&key) else { return Ok(None); };
            if entry.original_ticket.as_ref() != Some(ticket) || entry.stop_requested || entry.closed
            { return Ok(None); }
            if entry.work.as_ref().is_none_or(|work| work.peek().is_none()) { return Ok(None); }
            // Close admission under the same lock before checking every
            // originally registered physical producer.
            entry.custody_reconciling = true;
            let backend = entry.backend.clone();
            let early = entry.early_workers.iter().map(|(call, worker)| (call.clone(), worker.result.clone())).collect::<Vec<_>>();
            (backend, early)
        };
        let original = original.ok_or_else(|| NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: "original scoped backend unavailable".into(),
        })?;
        let mut early_results = Vec::with_capacity(early.len());
        for (call, worker) in early {
            let result = worker.await.map_err(|reason| NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason })?;
            early_results.push((call, result));
        }
        {
            let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if physical.iter().filter(|(item, _)| Self::owns_key(ticket, item)).any(|(_, entry)| {
                entry.work.as_ref().is_some_and(|work| work.peek().is_none())
                    || entry.driver.as_ref().is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
                    || entry.early_workers.values().any(|worker| !matches!(worker.result.peek(), Some(Ok(_))))
            }) { return Ok(None); }
        }
        let result = original.reconcile_original_custody(ticket, &early_results).await?;
        let still_original = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key).is_some_and(|entry| entry.custody_reconciling && !entry.closed && !entry.stop_requested
                && entry.original_ticket.as_ref() == Some(ticket)
                && entry.backend.as_ref().is_some_and(|backend| Arc::ptr_eq(backend, &original)));
        if !still_original {
            return Err(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original scoped backend changed while reconciling".into(),
            });
        }
        Ok(result)
    }

    fn prepare_original_binding(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        if ticket.executor_kind != self.kind { return Err(NodeExecutorError::Invalid { node_id: ticket.node_id.clone(), reason: "original physical ticket kind differs".into() }); }
        let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::physical_key(ticket, "");
        if let Some(entry) = physical.get(&key) {
            if entry.original_binding && entry.original_ticket.as_ref() == Some(ticket) && !entry.closed && !entry.stop_requested { return Ok(()); }
            return Err(NodeExecutorError::Invalid { node_id: ticket.node_id.clone(), reason: "original physical binding already exists".into() });
        }
        physical.insert(key, PhysicalEntry { original_ticket: Some(ticket.clone()), ..Default::default() });
        Ok(())
    }

    fn rollback_original_binding(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::physical_key(ticket, "");
        let Some(entry) = physical.get(&key) else { return Ok(()); };
        if entry.original_binding { return Ok(()); }
        if entry.original_ticket.as_ref() != Some(ticket) || entry.work.is_some() || entry.driver.is_some()
            || !entry.early_workers.is_empty() || entry.closed || entry.stop_requested {
            return Err(NodeExecutorError::Invalid { node_id: ticket.node_id.clone(), reason: "original unadmitted physical binding differs".into() });
        }
        physical.remove(&key);
        Ok(())
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
            let entry = physical.get_mut(&Self::physical_key(ticket, ""))
                .ok_or_else(|| NodeExecutorError::Cancel { node_id: ticket.node_id.clone(), reason: "original physical binding unavailable".into() })?;
            if entry.original_ticket.as_ref() != Some(ticket) || entry.closed || entry.stop_requested || entry.custody_reconciling
                || entry.observation.as_ref().is_some_and(|observation| !observation.enter()) {
                return Err(NodeExecutorError::Cancel { node_id: ticket.node_id.clone(), reason: "original physical admission closed".into() });
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
                let entry = physical.get_mut(&Self::physical_key(ticket, "")).ok_or_else(|| NodeExecutorError::Cancel { node_id: ticket.node_id.clone(), reason: "original physical binding unavailable".into() })?;
                if entry.closed || entry.custody_reconciling
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
                if entry.original_ticket.as_ref().is_some_and(|original| original != ticket) {
                    return Err(NodeExecutorError::Invalid {
                        node_id: ticket.node_id.clone(), reason: "original physical ticket differs".into(),
                    });
                }
                entry.original_ticket = Some(ticket.clone());
                if entry.original_binding
                    && entry.observation.as_ref().is_some_and(|o| o.ticket_matches(ticket))
                    && entry.backend.is_none()
                {
                    entry.backend = Some(backend.clone());
                }
                entry.observation.clone()
            };
            #[cfg(feature = "test-fixtures")]
            let trace_completion = std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some()
                && crate::host::decode_tool_batch(&ticket.payload_ref).is_ok_and(|(calls, _)|
                    calls.iter().any(|call| call.id == "recovery-complete" && call.name == "objective_complete_request"));
            let result = backend.execute(ticket).await;
            #[cfg(feature = "test-fixtures")]
            if trace_completion { eprintln!("rc_completion_stage scoped_backend_returned"); }
            let result = super::agent_tool::finish_observed_outcome(ticket, observation.as_deref(), result)
                .and_then(|mut outcome| {
                    if let Some(child) = self.original_agent_child(ticket) {
                        outcome.attach_original_agent_child(child).map_err(|reason| NodeExecutorError::Invalid {
                            node_id: ticket.node_id.clone(), reason,
                        })?;
                    }
                    Ok(outcome)
                });
            #[cfg(feature = "test-fixtures")]
            if trace_completion { eprintln!("rc_completion_stage scoped_finish_returned ok={}", result.is_ok()); }
            return result;
        }
        let key = Self::physical_key(ticket, "");
        let work: Option<PhysicalWork> = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = physical.get_mut(&key).ok_or_else(|| NodeExecutorError::Cancel { node_id: ticket.node_id.clone(), reason: "original physical binding unavailable".into() })?;
            if entry.closed || entry.custody_reconciling
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
            if entry.original_ticket.as_ref().is_some_and(|original| original != ticket) {
                return Err(NodeExecutorError::Invalid {
                    node_id: ticket.node_id.clone(), reason: "original physical ticket differs".into(),
                });
            }
            let observation = entry.observation.clone();
            if entry.work.is_none() {
                entry.backend = Some(backend.clone());
                entry.original_ticket = Some(ticket.clone());
            }
            entry
                .work
                .get_or_insert_with(|| {
                    let ticket = ticket.clone();
                    async move {
                        let result = std::panic::AssertUnwindSafe(backend.execute(&ticket))
                            .catch_unwind()
                            .await
                            .unwrap_or_else(|_| {
                                Err(NodeExecutorError::Uncertain {
                                    node_id: ticket.node_id.clone(),
                                    reason: "original retained backend panicked".into(),
                                })
                            });
                        super::agent_tool::finish_observed_outcome(
                            &ticket,
                            observation.as_deref(),
                            result,
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
                PhysicalOutcome::Node(mut outcome) => {
                    if let Some(child) = self.original_agent_child(ticket) {
                        outcome.attach_original_agent_child(child).map_err(|reason| NodeExecutorError::Invalid {
                            node_id: ticket.node_id.clone(), reason,
                        })?;
                    }
                    Ok(outcome)
                },
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
    async fn revalidate_goal_source(
        &self,
        ticket: &NodeExecutionTicket,
        outcome: &NodeExecutionOutcome,
        stale_stream: &str,
        expected: u64,
        actual: u64,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        let original = {
            let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            physical.get(&Self::physical_key(ticket, "")).filter(|entry| {
                entry.original_binding && !entry.closed
                    && entry.observation.as_ref().is_some_and(|o| o.ticket_matches(ticket))
            }).and_then(|entry| entry.backend.clone())
        };
        match original {
            Some(backend) => backend.revalidate_goal_source(ticket, outcome, stale_stream, expected, actual).await,
            None => Ok(None),
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
        let original_close_source = {
            let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            physical.get(&Self::physical_key(ticket, "")).and_then(|entry| {
                if !entry.original_binding || entry.closed { return None; }
                let observation = entry.observation.as_ref()?;
                if !observation.ticket_matches(ticket) { return None; }
                let child = entry.original_child.clone();
                if child.as_ref().is_some_and(|child| child.child_payload != ticket.payload_ref) { return None; }
                Some((Arc::clone(observation), Arc::clone(entry.backend.as_ref()?), child))
            })
        };
        // The success path must not abandon an in-flight physical write: wait
        // for it. Bounded cancellation is reserved for cancel/abort paths, where
        // stopping is the point (PLAN W3 / R-02).
        #[cfg(feature = "test-fixtures")]
        let traced = super::super::runner::k05_completion_trace_enabled(ticket);
        #[cfg(feature = "test-fixtures")]
        if traced { super::super::runner::k05_completion_trace(ticket, "physical_drain_enter", None, None); }
        let drained = self.close_and_drain(ticket, false).await;
        #[cfg(feature = "test-fixtures")]
        if traced { super::super::runner::k05_completion_trace(ticket, "physical_drain_return", None, Some(drained.is_ok())); }
        drained?;
        self.physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key, entry| !Self::owns_key(ticket, key) || entry.original_binding || !entry.early_workers.is_empty());
        if published {
            return Ok(());
        }
        let original = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&Self::physical_key(ticket, ""))
            .and_then(|entry| entry.backend.clone());
        let backend = original.or_else(|| self.resolve(ticket)).ok_or_else(|| {
            NodeExecutorError::Unavailable {
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            }
        })?;
        let close = original_close_source.and_then(|(observation, original_backend, original_child)| {
            if !Arc::ptr_eq(&backend, &original_backend) { return None; }
            let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = physical.get(&Self::physical_key(ticket, ""))?;
            if !entry.original_binding || !entry.closed
                || entry.observation.as_ref().is_none_or(|actual| !Arc::ptr_eq(actual, &observation))
                || entry.backend.as_ref().is_none_or(|actual| !Arc::ptr_eq(actual, &original_backend))
                || match (&entry.original_child, &original_child) {
                    (Some(actual), Some(original)) => !Arc::ptr_eq(actual, original),
                    (None, None) => false,
                    _ => true,
                }
                || physical.iter().filter(|(key, _)| Self::owns_key(ticket, key)).any(|(_, item)| {
                    !item.closed
                        || item.work.as_ref().is_some_and(|work| work.peek().is_none())
                        || item.driver.as_ref().is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
                }) {
                return None;
            }
            Some(Arc::new(OriginalPhysicalClose { ticket: ticket.clone(), observation, original_child }))
        });
        #[cfg(feature = "test-fixtures")]
        if traced { super::super::runner::k05_completion_trace(ticket, "backend_after_commit_enter", None, Some(close.is_some())); }
        let published = if let Some(close) = close {
            ORIGINAL_PHYSICAL_CLOSE.scope(close, backend.after_commit(ticket)).await
        } else {
            backend.after_commit(ticket).await
        };
        #[cfg(feature = "test-fixtures")]
        if traced { super::super::runner::k05_completion_trace(ticket, "backend_after_commit_return", None, Some(published.is_ok())); }
        published?;
        let early = {
            let physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match physical.get(&Self::physical_key(ticket, "")) {
                Some(entry) if !entry.early_workers.is_empty() && entry.original_ticket.as_ref() == Some(ticket)
                    && entry.backend.as_ref().is_some_and(|original| Arc::ptr_eq(original, &backend)) => {
                    let mut results = Vec::with_capacity(entry.early_workers.len());
                    for (call, worker) in &entry.early_workers {
                        let Some(Ok(result)) = worker.result.peek() else {
                            return Err(NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason: "original early worker result unavailable".into() });
                        };
                        results.push((call.clone(), result.clone(), Arc::clone(&worker.nonce)));
                    }
                    results
                },
                Some(entry) if !entry.early_workers.is_empty() => return Err(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(), reason: "original early worker owner differs".into(),
                }),
                Some(_) | None => Vec::new(),
            }
        };
        if !early.is_empty() {
            let values = early.iter().map(|(call, result, _)| (call.clone(), result.clone())).collect::<Vec<_>>();
            if !backend.confirm_original_early_release(ticket, &values).await? {
                return Err(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(), reason: "original early transaction release unconfirmed".into(),
                });
            }
            let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = physical.get_mut(&Self::physical_key(ticket, ""))
                .ok_or_else(|| NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason: "original early owner disappeared".into() })?;
            if entry.original_ticket.as_ref() != Some(ticket)
                || entry.backend.as_ref().is_none_or(|original| !Arc::ptr_eq(original, &backend))
                || entry.early_workers.len() != early.len()
                || early.iter().any(|(call, _, nonce)| entry.early_workers.get(call).is_none_or(|worker| !Arc::ptr_eq(&worker.nonce, nonce)))
            { return Err(NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason: "original early owner changed during release".into() }); }
            entry.early_release_proven = true;
        }
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
            entry.backend = None;
        }
        Ok(())
    }
    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.close_and_drain(ticket, true).await
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
    async fn drain_physical_execution(&self) -> Result<(), NodeExecutorError> {
        let pending = {
            let mut physical = self
                .physical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let parents = physical
                .keys()
                .map(|key| (key.0.clone(), key.1.clone(), key.2, String::new()))
                .collect::<Vec<_>>();
            for parent in parents {
                Self::close_entry(physical.entry(parent).or_default());
            }
            for entry in physical.values_mut() {
                Self::close_entry(entry);
                Self::retain_driver(entry);
            }
            physical
                .iter()
                .map(|(key, entry)| {
                    // Never repoll a Shared work that panicked. With an original
                    // driver present, only its real JoinHandle answers for exit.
                    let completed_without_driver = entry.driver.is_none()
                        && entry.work.as_ref().is_none_or(|work| work.peek().is_some());
                    (key.clone(), entry.driver.clone(), completed_without_driver,
                        entry.early_workers.values().map(|worker| worker.result.clone()).collect::<Vec<_>>())
                })
                .collect::<Vec<_>>()
        };
        let joined =
            futures::future::join_all(pending.into_iter().map(|(key, driver, done, early)| async move {
                let joined = match driver {
                    Some(driver) => driver.await.is_ok(),
                    None => done,
                };
                let early_joined = match tokio::time::timeout(super::super::PHYSICAL_CLOSURE_BUDGET,
                    futures::future::join_all(early)).await {
                    Ok(results) => results.into_iter().all(|result| result.is_ok()),
                    Err(_) => false,
                };
                (key, joined && early_joined)
            }))
            .await;
        let mut physical = self
            .physical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut uncertain = None;
        for (key, joined) in joined {
            if !joined {
                uncertain.get_or_insert_with(|| key.1.clone());
                continue;
            }
            if let Some(entry) = physical.get_mut(&key) {
                // Check driver first, so a poisoned work is never inspected
                // after its driver reported a failed join.
                if entry
                    .driver
                    .as_ref()
                    .is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
                    || entry
                        .work
                        .as_ref()
                        .is_some_and(|work| work.peek().is_none())
                {
                    uncertain.get_or_insert_with(|| key.1.clone());
                    continue;
                }
                if entry.original_binding || key.3.is_empty() {
                    entry.work = None;
                } else {
                    physical.remove(&key);
                }
            }
        }
        let pending = |entry: &PhysicalEntry| {
            entry
                .driver
                .as_ref()
                .is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
                || entry
                    .work
                    .as_ref()
                    .is_some_and(|work| work.peek().is_none())
                || !entry.early_workers.is_empty()
        };
        let removable = physical
            .iter()
            .filter(|(key, entry)| {
                key.3.is_empty()
                    && !entry.original_binding
                    && entry.closed
                    && !physical.iter().any(|(other, entry)| {
                        other.0 == key.0 && other.1 == key.1 && other.2 == key.2 && pending(entry)
                    })
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in removable {
            physical.remove(&key);
        }
        if let Some((key, _)) = physical.iter().find(|(_, entry)| pending(entry)) {
            uncertain.get_or_insert_with(|| key.1.clone());
        }
        if let Some(node_id) = uncertain {
            return Err(NodeExecutorError::Uncertain {
                node_id,
                reason: "original physical drain could not prove every driver join".into(),
            });
        }
        // A joined physical driver does not settle its original packet decision.
        // Preserve the same-ticket entry and report that distinct Unknown to the
        // existing shutdown owner; do not retry or synthesize a receipt here.
        if let Some((key, _)) = physical.iter().find(|(_, entry)| {
            entry.observation.as_ref().is_some_and(|observation| observation.packet_scope_is_unresolved())
        }) {
            return Err(NodeExecutorError::Uncertain {
                node_id: key.1.clone(),
                reason: "original packet scope transaction remains unknown after physical drain".into(),
            });
        }
        if let Some((key, _)) = physical.iter().find(|(_, entry)| {
            entry.observation.as_ref().is_some_and(|observation| observation.member_custody_is_unresolved())
        }) {
            return Err(NodeExecutorError::Uncertain {
                node_id: key.1.clone(),
                reason: "original member custody remains unresolved after physical drain".into(),
            });
        }
        Ok(())
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
                && (work.work.as_ref().is_some_and(|work| work.peek().is_none())
                    || work
                        .driver
                        .as_ref()
                        .is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
                    || !work.early_workers.is_empty())
        }) {
            return;
        }
        physical.retain(|key, entry| !Self::owns_key(ticket, key) || entry.original_binding);
    }
    fn original_binding_released(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let mut physical = self.physical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::physical_key(ticket, "");
        let Some(entry) = physical.get_mut(&key) else { return Ok(()); };
        if entry.early_workers.is_empty() { return Ok(()); }
        if entry.original_ticket.as_ref() != Some(ticket) || !entry.closed
            || entry.early_workers.values().any(|worker| !matches!(worker.result.peek(), Some(Ok(_))))
            || entry.work.as_ref().is_some_and(|work| work.peek().is_none())
            || entry.driver.as_ref().is_some_and(|driver| !matches!(driver.peek(), Some(Ok(()))))
        {
            return Err(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original early workers are not physically closed".into(),
            });
        }
        if !entry.early_release_proven {
            return Err(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original early worker transaction release is unconfirmed".into(),
            });
        }
        entry.early_workers.clear();
        if !entry.original_binding { physical.remove(&key); }
        Ok(())
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

    #[tokio::test]
    async fn original_leaf_admission_rejects_missing_foreign_and_closed_root() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let original = publication_ticket_704();
        let ran = Arc::new(AtomicBool::new(false));
        let attempt = |flag: Arc<AtomicBool>| Box::pin(async move {
            flag.store(true, Ordering::SeqCst);
            Err(NodeExecutorError::Invalid {
                node_id: "physical-node".into(),
                reason: "unauthorized leaf was executed".into(),
            })
        }) as BoxFuture<'static, Result<crate::RuntimeToolExecutionOutcome, NodeExecutorError>>;
        assert!(executor.execute_physical_tool(&original, "missing", attempt(Arc::clone(&ran))).await.is_err());
        executor.observe_binding(&original).unwrap();
        let mut foreign = original.clone();
        foreign.idempotency_key.push_str("-foreign");
        assert!(executor.execute_physical_tool(&foreign, "foreign", attempt(Arc::clone(&ran))).await.is_err());
        executor.close_and_drain(&original, true).await.unwrap();
        assert!(executor.execute_physical_tool(&original, "closed", attempt(Arc::clone(&ran))).await.is_err());
        assert!(executor.poll_or_await(&original).await.is_err());
        assert!(!ran.load(Ordering::SeqCst));
        let other = ScopedNodeExecutor::new("tool_batch");
        other.prepare_original_binding(&original).unwrap();
        other.rollback_original_binding(&original).unwrap();
        assert!(other.poll_or_await(&original).await.is_err());
        assert!(other.execute_physical_tool(&original, "rolled-back", attempt(Arc::clone(&ran))).await.is_err());
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn original_early_worker_can_enter_its_own_late_leaf_after_seal() {
        let executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        let original = ticket();
        executor.prepare_original_binding(&original).unwrap();
        let (release, ready) = tokio::sync::oneshot::channel();
        let executor_for_worker = Arc::clone(&executor);
        let original_for_worker = original.clone();
        let worker = Box::pin(async move {
            ready.await.unwrap();
            let foreign = executor_for_worker.execute_physical_tool_for_call(&original_for_worker, "leaf:foreign-hash", Some("foreign"),
                Box::pin(async { panic!("foreign early leaf ran") })).await;
            assert!(foreign.is_err());
            let own = executor_for_worker.execute_physical_tool_for_call(&original_for_worker, "leaf:request-hash", Some("original-call"),
                Box::pin(async { Ok(crate::RuntimeToolExecutionOutcome {
                    pending_agent_batch: None, tool_use_id: "original-call".into(), tool_name: "read_file".into(),
                    status: crate::RuntimeToolExecutionStatus::Executed, category: crate::ToolSafetyCategory::ReadOnly,
                    output: None, error: None, evidence_ref: String::new(), observed_evidence: vec![],
                }) })).await.expect("same original worker retains late leaf admission");
            assert_eq!(own.tool_use_id, "original-call");
            crate::conversation::EarlyToolDispatchResult::Deferred(crate::conversation::EarlyToolDeferral {
                tool_call_id: "original-call".into(), reason: "test completion".into(), ready_at_ms: 0,
                original_undispatched: None,
            })
        });
        let (joined, _) = executor.retain_original_early_worker(&original, "original-call", worker).unwrap();
        executor.physical.lock().unwrap().get_mut(&ScopedNodeExecutor::physical_key(&original, "")).unwrap().custody_reconciling = true;
        release.send(()).unwrap();
        assert!(matches!(joined.await.unwrap(), crate::conversation::EarlyToolDispatchResult::Deferred(_)));
    }

    #[tokio::test]
    async fn r188_full_queue_consumer_panic_and_dropped_observer_retain_original_producer() {
        struct State {
            entered: tokio::sync::Notify,
            panicked: tokio::sync::Notify,
            release: tokio::sync::Notify,
            released: std::sync::atomic::AtomicUsize,
            calls: std::sync::atomic::AtomicUsize,
        }
        struct Lease(Arc<State>);
        impl Drop for Lease {
            fn drop(&mut self) {
                self.0.released.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Structured(Arc<State>);
        #[async_trait]
        impl ScopedNodeBackend for Structured {
            fn retains_physical_execution(&self) -> bool {
                true
            }
            async fn execute(
                &self,
                ticket: &NodeExecutionTicket,
            ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
                self.0.calls.fetch_add(1, Ordering::SeqCst);
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                let state = self.0.clone();
                let lease = Lease(state.clone());
                let producer = crate::conversation::ProviderExecution::new(
                    async move {
                        let _lease = lease;
                        tx.send(1).await.unwrap();
                        assert_eq!(tx.capacity(), 0);
                        state.entered.notify_one();
                        // Receiver must be dropped on unwind, otherwise this blocks.
                        assert!(tx.send(2).await.is_err());
                        state.release.notified().await;
                        Ok(crate::conversation::ProviderLocalExit::Completed)
                    },
                    crate::CancellationToken::new(),
                );
                let consumer_state = self.0.clone();
                let (consumer, result) = producer
                    .drive(async move {
                        consumer_state.entered.notified().await;
                        // Keep the actual receiver in the future being unwound.
                        assert!(!rx.is_closed());
                        consumer_state.panicked.notify_one();
                        let _ = &mut rx;
                        panic!("controlled consumer panic");
                    })
                    .await;
                assert!(consumer.is_err());
                result.unwrap();
                Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: "controlled consumer failed".into(),
                })
            }
        }
        struct StructuredResolver(Arc<State>);
        impl ScopedNodeBackendResolver for StructuredResolver {
            fn resolve(&self, _: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
                Some(Arc::new(Structured(self.0.clone())))
            }
        }
        let state = Arc::new(State {
            entered: Default::default(),
            panicked: Default::default(),
            release: Default::default(),
            released: Default::default(),
            calls: Default::default(),
        });
        let executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
        executor.install_resolver(Arc::new(StructuredResolver(state.clone())));
        let mut request = ticket();
        request.executor_kind = "inline_model".into();
        executor.prepare_original_binding(&request).unwrap();
        let caller = {
            let executor = executor.clone();
            let request = request.clone();
            tokio::spawn(async move { executor.poll_or_await(&request).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), state.panicked.notified())
            .await
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(state.released.load(Ordering::SeqCst), 0);
        // Timeout must retain the same work/driver rather than drop its lease.
        assert!(executor
            .cancel_bounded(&request, std::time::Duration::from_millis(10))
            .await
            .is_err());
        assert_eq!(state.released.load(Ordering::SeqCst), 0);
        state.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), executor.cancel(&request))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.released.load(Ordering::SeqCst), 1);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);
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
            executor.prepare_original_binding(&request).unwrap();
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
                                    pending_agent_batch: None,
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
            .unwrap()
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
        executor.prepare_original_binding(&ticket()).unwrap();
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
        executor.prepare_original_binding(&ticket()).unwrap();
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
            .unwrap()
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
            executor.drain_physical_execution().await.unwrap();
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
                    executor.drain_physical_execution().await.unwrap();
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
                            pending_agent_batch: None,
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

#[cfg(test)]
mod controlled_stop_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Backend {
        entered: AtomicBool,
        stops: AtomicUsize,
        release: tokio::sync::Notify,
    }
    #[async_trait]
    impl ScopedNodeBackend for Backend {
        fn retains_physical_execution(&self) -> bool {
            true
        }
        fn request_stop(&self, _: &NodeExecutionTicket) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }
        async fn execute(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            self.entered.store(true, Ordering::Release);
            self.release.notified().await;
            Ok(NodeExecutionOutcome::new(
                harness_contract::execution_graph::ExecutionNodeResult {
                    status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                    result_ref: None,
                    wait_class: None,
                    summary: None,
                    evidence_refs: vec![],
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }
    struct Resolver(Arc<Backend>);
    impl ScopedNodeBackendResolver for Resolver {
        fn resolve(&self, _: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
            Some(self.0.clone())
        }
    }
    fn ticket() -> NodeExecutionTicket {
        NodeExecutionTicket {
            graph_id: "controlled-child".into(),
            node_id: "original-leaf".into(),
            executor_kind: "tool_batch".into(),
            service_class: Default::default(),
            attempt: 3,
            idempotency_key: "original-call".into(),
            payload_ref: "{}".into(),
        }
    }
    #[tokio::test]
    async fn physical_drain_work_panic_uses_failed_driver_without_repolling_poisoned_work() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let request = ticket();
        executor.prepare_original_binding(&request).unwrap();
        let (release, ready) = tokio::sync::oneshot::channel();
        let mut leaf = Box::pin(executor.execute_physical_tool(
            &request,
            "panic-leaf",
            Box::pin(async move {
                ready.await.unwrap();
                panic!("controlled physical work panic");
            }),
        ));
        assert!(futures::poll!(&mut leaf).is_pending());
        let mut drain = Box::pin(executor.drain_physical_execution());
        assert!(futures::poll!(&mut drain).is_pending());
        drop(leaf);
        release.send(()).unwrap();
        let result = std::panic::AssertUnwindSafe(drain)
            .catch_unwind()
            .await
            .expect("physical drain must not repoll poisoned Shared work");
        assert!(
            matches!(result, Err(NodeExecutorError::Uncertain { ref node_id, .. }) if node_id == "original-leaf")
        );
        let driver = executor
            .physical
            .lock()
            .unwrap()
            .get(&ScopedNodeExecutor::physical_key(&request, "panic-leaf"))
            .unwrap()
            .driver
            .clone()
            .unwrap();
        assert!(matches!(driver.peek(), Some(Err(_))));
        let retry = std::panic::AssertUnwindSafe(executor.drain_physical_execution())
            .catch_unwind()
            .await;
        assert!(matches!(
            retry,
            Ok(Err(NodeExecutorError::Uncertain { .. }))
        ));
        assert!(
            executor
                .physical
                .lock()
                .unwrap()
                .get(&ScopedNodeExecutor::physical_key(&request, ""))
                .unwrap()
                .closed
        );
    }

    #[tokio::test]
    async fn physical_drain_business_error_with_real_successful_join_is_quiescent() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let request = ticket();
        executor.prepare_original_binding(&request).unwrap();
        let (release, ready) = tokio::sync::oneshot::channel();
        let mut leaf = Box::pin(executor.execute_physical_tool(
            &request,
            "failed-business",
            Box::pin(async move {
                ready.await.unwrap();
                Err(NodeExecutorError::Poll {
                    node_id: "original-leaf".into(),
                    reason: "business rejected".into(),
                })
            }),
        ));
        assert!(futures::poll!(&mut leaf).is_pending());
        let mut drain = Box::pin(executor.drain_physical_execution());
        assert!(futures::poll!(&mut drain).is_pending());
        let driver = executor
            .physical
            .lock()
            .unwrap()
            .get(&ScopedNodeExecutor::physical_key(
                &request,
                "failed-business",
            ))
            .unwrap()
            .driver
            .clone()
            .unwrap();
        drop(leaf);
        release.send(()).unwrap();
        drain.await.unwrap();
        assert!(matches!(driver.peek(), Some(Ok(()))));
        assert!(executor.physical.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn shutdown_snapshot_closes_parent_and_retains_driver_before_wait() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let request = ticket();
        executor.prepare_original_binding(&request).unwrap();
        let (release, ready) = tokio::sync::oneshot::channel();
        let mut leaf = Box::pin(executor.execute_physical_tool(
            &request,
            "shutdown-leaf",
            Box::pin(async move {
                ready.await.unwrap();
                Ok(crate::RuntimeToolExecutionOutcome {
                    pending_agent_batch: None,
                    tool_use_id: "shutdown-leaf".into(),
                    tool_name: "edit".into(),
                    status: crate::RuntimeToolExecutionStatus::Executed,
                    category: crate::ToolSafetyCategory::WriteLocal,
                    output: None,
                    error: None,
                    evidence_ref: "receipt".into(),
                    observed_evidence: vec![],
                })
            }),
        ));
        assert!(futures::poll!(&mut leaf).is_pending());
        let mut drain = Box::pin(executor.drain_physical_execution());
        assert!(futures::poll!(&mut drain).is_pending());
        let driver = {
            let physical = executor.physical.lock().unwrap();
            assert!(
                physical
                    .get(&ScopedNodeExecutor::physical_key(&request, ""))
                    .unwrap()
                    .closed
            );
            physical
                .get(&ScopedNodeExecutor::physical_key(&request, "shutdown-leaf"))
                .unwrap()
                .driver
                .clone()
                .expect("shutdown owns original driver before await")
        };
        assert!(executor
            .execute_physical_tool(
                &request,
                "late-leaf",
                Box::pin(async { panic!("shutdown admitted new leaf") })
            )
            .await
            .is_err());
        release.send(()).unwrap();
        leaf.await.unwrap();
        drain.await.unwrap();
        assert!(matches!(driver.peek(), Some(Ok(()))));
        assert!(executor.physical.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn completed_leaf_retains_original_driver_until_real_join() {
        let executor = ScopedNodeExecutor::new("tool_batch");
        let request = ticket();
        executor.prepare_original_binding(&request).unwrap();
        let (release_work, work_ready) = tokio::sync::oneshot::channel();
        let mut caller = Box::pin(executor.execute_physical_tool(
            &request,
            "joined-leaf",
            Box::pin(async move {
                work_ready.await.unwrap();
                Ok(crate::RuntimeToolExecutionOutcome {
                    pending_agent_batch: None,
                    tool_use_id: "joined-leaf".into(),
                    tool_name: "edit".into(),
                    status: crate::RuntimeToolExecutionStatus::Executed,
                    category: crate::ToolSafetyCategory::WriteLocal,
                    output: None,
                    error: None,
                    evidence_ref: "committed-leaf".into(),
                    observed_evidence: vec![],
                })
            }),
        ));
        assert!(futures::poll!(&mut caller).is_pending());
        let key = ScopedNodeExecutor::physical_key(&request, "joined-leaf");
        let (release_driver, driver_ready) = tokio::sync::oneshot::channel();
        let handle = {
            let mut physical = executor.physical.lock().unwrap();
            let entry = physical.get_mut(&key).unwrap();
            let work = entry.work.clone().unwrap();
            let handle = tokio::spawn(async move {
                let _ = work.await;
                // A completed shared result does not imply this task has returned.
                let _ = driver_ready.await;
            });
            entry.driver = Some(
                async move { handle.await.map_err(|error| error.to_string()) }
                    .boxed()
                    .shared(),
            );
            entry.driver.clone().unwrap()
        };
        let mut drain = Box::pin(executor.close_and_drain(&request, true));
        assert!(futures::poll!(&mut drain).is_pending());
        release_work.send(()).unwrap();
        caller.await.unwrap();
        assert!(
            executor.physical.lock().unwrap().contains_key(&key),
            "the original caller must retain the not-yet-joined leaf driver"
        );
        assert!(futures::poll!(&mut drain).is_pending());
        drop(drain);
        assert!(executor.physical.lock().unwrap().contains_key(&key));
        assert!(handle.peek().is_none());
        release_driver.send(()).unwrap();
        executor.close_and_drain(&request, true).await.unwrap();
        assert!(matches!(handle.peek(), Some(Ok(()))));
        assert!(!executor.physical.lock().unwrap().contains_key(&key));
    }

    #[tokio::test]
    async fn normal_publication_never_stops_but_cancel_retains_original_physical_work() {
        for cancel in [false, true] {
            let backend = Arc::new(Backend {
                entered: AtomicBool::new(false),
                stops: AtomicUsize::new(0),
                release: Default::default(),
            });
            let executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
            executor.install_resolver(Arc::new(Resolver(backend.clone())));
            executor.prepare_original_binding(&ticket()).unwrap();
            let runner = {
                let executor = executor.clone();
                tokio::spawn(async move { executor.poll_or_await(&ticket()).await })
            };
            while !backend.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            let mut foreign = ticket();
            foreign.attempt += 1;
            executor.cancel(&foreign).await.unwrap();
            assert_eq!(backend.stops.load(Ordering::SeqCst), 0);
            if cancel {
                assert!(matches!(
                    executor
                        .cancel_bounded(&ticket(), std::time::Duration::from_millis(5))
                        .await,
                    Err(NodeExecutorError::Cancel { .. })
                ));
                assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
                assert!(executor
                    .physical
                    .lock()
                    .unwrap()
                    .get(&ScopedNodeExecutor::physical_key(&ticket(), ""))
                    .unwrap()
                    .work
                    .as_ref()
                    .unwrap()
                    .peek()
                    .is_none());
                backend.release.notify_one();
                executor
                    .cancel_bounded(&ticket(), std::time::Duration::from_secs(1))
                    .await
                    .unwrap();
                assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
            } else {
                let original = ticket();
                let mut publication = Box::pin(executor.after_commit(&original));
                assert!(tokio::time::timeout(
                    std::time::Duration::from_millis(5),
                    &mut publication
                )
                .await
                .is_err());
                assert_eq!(backend.stops.load(Ordering::SeqCst), 0);
                backend.release.notify_one();
                publication.await.unwrap();
                assert_eq!(backend.stops.load(Ordering::SeqCst), 0);
            }
            runner.await.unwrap().unwrap();
        }
    }
}
