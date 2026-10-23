use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use async_trait::async_trait;
use futures::FutureExt;
use harness_contract::agent::{
    AgentBindingSnapshot, AgentCommand, AgentCommandReceipt, AgentCommandRejectReason,
    AgentCommandRequest, AgentLifecycleEvent, AgentReturnPacket, AgentStatus, AgentTaskPacket,
    AgentTerminalStatus, RevisionSelector,
};
use harness_contract::execution::ExecutionIdentity;
use harness_contract::execution_graph::{
    ExecutionEdgeKind, ExecutionNodeKind, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};

use crate::execution_core::graph::executors::{AgentTaskBackend, AgentTaskBackendResolver};
use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope,
};
use crate::{
    project_self_models, AgentLifecyclePhase, AgentRunEvaluation, AgentSelfModel, CowdEvent,
    CowdExecutionContext, CowdExecutionLineage, ProviderRegistry, RuntimeEventStore,
    RuntimeServices,
};
use sha2::{Digest, Sha256};

use crate::agent_catalog::AgentCatalog;
use crate::agent_model_selector::{AgentModelSelection, AgentModelSelector};
use crate::agent_result_validator::validate_agent_return;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};

#[path = "input_journal.rs"]
mod input_journal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agentic_binding: Option<harness_contract::agent::AgenticExecutionBinding>,
    pub execution_identity: ExecutionIdentity,
    pub run_id: String,
    pub agent_id: String,
    pub task_id: String,
    pub root_task_id: String,
    pub session_id: String,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub backend: AgentBackendKind,
    pub status: AgentStatus,
    pub revision: u64,
    pub model: Option<String>,
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<AgentBindingSnapshot>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub failure: Option<String>,
}

/// A verified Agent record exported by the pre-V4 upgrade coordinator.
///
/// Legacy files alone are intentionally not accepted: they did not carry the
/// complete workspace/session/graph/node binding required by the canonical
/// runtime. The coordinator must provide that binding and, for a terminal
/// record, its canonical result before this importer will write lifecycle
/// truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAgentStateRecord {
    pub source_ref: String,
    pub snapshot: AgentRunSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returned: Option<AgentReturnPacket>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAgentImportReport {
    pub source_id: String,
    pub duplicate: bool,
    pub imported_agent_ids: Vec<String>,
    pub blocked_agent_ids: Vec<String>,
}

impl AgentRunSnapshot {
    #[must_use]
    pub fn handle(&self) -> AgentRunHandle {
        AgentRunHandle {
            run_id: self.run_id.clone(),
            agent_id: self.agent_id.clone(),
            backend: self.backend,
            revision: self.revision,
            status: self.status,
        }
    }
}

#[async_trait]
pub trait AgentRuntimeBackend: Send + Sync {
    fn kind(&self) -> AgentBackendKind;
    fn capabilities(&self) -> AgentBackendCapabilities;
    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String>;
    async fn command(
        &self,
        _handle: &AgentRunHandle,
        _request: &AgentCommandRequest,
    ) -> Result<(), AgentCommandRejectReason> {
        Err(AgentCommandRejectReason::UnsupportedByBackend)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAgentEvent {
    snapshot: AgentRunSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<AgentCommandReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    returned: Option<AgentReturnPacket>,
}

#[derive(Default)]
struct AgentRunRecord {
    snapshot: Option<AgentRunSnapshot>,
    receipts: BTreeMap<String, AgentCommandReceipt>,
    returned: Option<AgentReturnPacket>,
    retained: Option<Arc<RetainedAgentRun>>,
    retiring: Vec<Arc<RetainedAgentRun>>,
    release_after_completion: bool,
}

struct RetainedAgentRun {
    packet_sha256: String,
    run_key: (String, String, String, u32),
    /// The identity of the immutable packet of this run, as the run facts
    /// must carry it. It is bound to this run by the packet digest and the run
    /// key, so a fact of another attempt can never borrow it.
    identity_refs: Vec<RuntimeEventRef>,
    result: tokio::sync::watch::Sender<Option<Result<AgentReturnPacket, String>>>,
    join: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    pending_exit: Mutex<Option<RunExitFact>>,
    exit_committed: AtomicBool,
    /// The exit phase of this run, decided exactly once. The run owner and a
    /// cancellation path both publish `agent.local_exit` for the same run, so
    /// they must not disagree about the phase: the first observer that knows
    /// the physical exit decides it and every later publisher reuses it.
    exit_phase: Mutex<Option<LocalRunPhase>>,
    /// The exit evidence the run owner has produced so far. A cancellation
    /// path mirrors it here so it can commit the same authenticated exit fact
    /// when the run owner can no longer reach its own publication point.
    exit_evidence: Mutex<Option<RunExitFact>>,
    /// The single-writer claim of the authenticated exit publication. Exactly
    /// one publisher may write `agent.local_exit` for this run; every later
    /// attempt observes the committed fact instead of writing a second one.
    exit_publisher_claimed: AtomicBool,
}

impl RetainedAgentRun {
    fn physically_exited(&self) -> bool {
        self.exit_committed.load(Ordering::Acquire)
            && self
                .pending_exit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(RunExitFact::positive)
    }

    fn locally_settled(&self) -> bool {
        self.result.borrow().is_some() && self.physically_exited()
    }

    /// Claim the single authenticated exit publication of this run.
    fn claim_exit_publication(&self) -> bool {
        self.exit_publisher_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn exit_publication_claimed(&self) -> bool {
        self.exit_publisher_claimed.load(Ordering::Acquire)
    }

    /// Decide the exit phase of this run exactly once.
    /// Decide the exit phase of this run exactly once.
    ///
    /// Only a **positive** phase may decide it. An unproven exit is not a
    /// decision: committing it would fix the run as "physically unproven" for
    /// good and would stop the run owner from publishing the real exit fact
    /// it may still be able to prove.
    fn decide_exit_phase(&self, proposed: LocalRunPhase) -> LocalRunPhase {
        let mut decided = self
            .exit_phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match decided.as_ref() {
            Some(phase) => phase.clone(),
            None => {
                if proposed.positive_phase() {
                    *decided = Some(proposed.clone());
                }
                proposed
            }
        }
    }

    fn mirror_exit_evidence(&self, evidence: &RunExitFact) {
        *self
            .exit_evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(evidence.clone());
    }

    fn mirrored_exit_evidence(&self) -> Option<RunExitFact> {
        self.exit_evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether the run owner task ended, published or not. A run owner that
    /// ended without publishing its exit can no longer commit one, so the
    /// cancellation path has to do it.
    async fn owner_ended(&self) -> bool {
        let mut join = self.join.lock().await;
        join.as_ref().is_some_and(|handle| handle.is_finished())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LocalRunPhase {
    Admitted,
    NotStarted,
    BackendPending,
    LocalReturned,
    ProcessReaped,
    /// The run was cancelled and its physical exit is proven: the owned
    /// process group is empty, so the cancelled worker cannot produce another
    /// effect. A cancellation may publish this phase only when that exit is
    /// observed, never on the strength of the cancellation request alone.
    Cancelled,
    Unknown,
}

impl LocalRunPhase {
    /// Whether this phase can carry a positive (proven) exit fact.
    ///
    /// `Admitted`, `BackendPending` and `Unknown` cannot: a run in one of
    /// those phases has not proven that its physical resources are gone.
    fn positive_phase(&self) -> bool {
        matches!(
            self,
            LocalRunPhase::NotStarted
                | LocalRunPhase::Cancelled
                | LocalRunPhase::LocalReturned
                | LocalRunPhase::ProcessReaped
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RunExitFact {
    schema_version: u32,
    agent_id: String,
    run_id: String,
    graph_id: String,
    node_id: String,
    attempt: u32,
    packet_sha256: String,
    backend_packet_sha256: Option<String>,
    owner_epoch: String,
    backend: AgentBackendKind,
    phase: LocalRunPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    process_envelope: Option<crate::agent_process_jsonl_adapter::ProcessPhysicalEnvelope>,
}

fn run_packet_sha256(packet: &AgentTaskPacket) -> Result<String, String> {
    serde_json::to_vec(packet)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|error| error.to_string())
}

fn run_cancellation_key(packet: &AgentTaskPacket) -> (String, String, String, u32) {
    (
        packet.run_id().into(),
        packet.graph_id().into(),
        packet.node_id().into(),
        packet.attempt,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunExitEvidence {
    fact: RunExitFact,
}

impl RunExitFact {
    fn matches(&self, packet: &AgentTaskPacket, packet_sha256: &str) -> Result<bool, String> {
        Ok(self.schema_version == 1
            && self.agent_id == packet.agent_id()
            && self.run_id == packet.run_id()
            && self.graph_id == packet.graph_id()
            && self.node_id == packet.node_id()
            && self.attempt == packet.attempt
            && self.packet_sha256 == packet_sha256
            && self.backend == backend_from_packet(packet)?
            && uuid::Uuid::parse_str(&self.owner_epoch).is_ok()
            && self.backend_packet_sha256.as_ref().is_none_or(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            && self.process_envelope.as_ref().is_none_or(|envelope| {
                self.backend == AgentBackendKind::ProcessJsonl
                    && self.backend_packet_sha256.as_ref().is_some_and(|backend| {
                        envelope.authentic_for(packet, packet_sha256, backend)
                    })
            }))
    }

    fn positive(&self) -> bool {
        matches!(self.phase, LocalRunPhase::NotStarted)
            || (self.backend_packet_sha256.is_some()
                && (matches!(self.phase, LocalRunPhase::Cancelled)
                    || matches!(
                        (&self.backend, &self.phase),
                        (AgentBackendKind::InProcess, LocalRunPhase::LocalReturned)
                            | (AgentBackendKind::ProcessJsonl, LocalRunPhase::ProcessReaped)
                    )))
    }
}

/// The durable reason a run was cancelled by command. The run owner and the
/// cancelling authority must record the same reason: a cancellation is one
/// fact with one reason, whoever happens to write it.
pub(crate) const CANCELLED_BY_COMMAND: &str = "agent cancelled by command";

pub(crate) fn confirmed_run_exit(
    store: &RuntimeEventStore,
    packet: &AgentTaskPacket,
) -> Result<Option<(RunExitEvidence, Vec<ExpectedStreamRevision>)>, String> {
    let lifecycle = authenticated_run_lifecycle(store, packet)?;
    Ok(lifecycle
        .exit
        .filter(RunExitFact::positive)
        .map(|fact| (RunExitEvidence { fact }, lifecycle.sources)))
}

pub(crate) struct AuthenticatedRunLifecycle {
    exit: Option<RunExitFact>,
    backend: Option<RunExitFact>,
    pub(crate) sources: Vec<ExpectedStreamRevision>,
}

impl AuthenticatedRunLifecycle {
    pub(crate) fn closed_for_governed_effects(&self) -> bool {
        self.exit.as_ref().is_some_and(|fact| {
            fact.positive()
                && (fact.backend == AgentBackendKind::InProcess || self.no_direct_effects())
        })
    }

    pub(crate) fn no_direct_effects(&self) -> bool {
        self.exit
            .as_ref()
            .is_some_and(|fact| fact.phase == LocalRunPhase::NotStarted)
            || self
                .backend
                .as_ref()
                .and_then(|fact| fact.process_envelope.as_ref())
                .is_some_and(|envelope| envelope.host_only())
    }

    pub(crate) fn semantic_fact(&self) -> serde_json::Value {
        serde_json::json!({"envelope":self.backend.as_ref().and_then(|fact| fact.process_envelope.as_ref()),
            "not_started":self.exit.as_ref().is_some_and(|fact| fact.phase == LocalRunPhase::NotStarted),
            "no_direct_effects":self.no_direct_effects()})
    }
}

pub(crate) fn authenticated_run_lifecycle(
    store: &RuntimeEventStore,
    packet: &AgentTaskPacket,
) -> Result<AuthenticatedRunLifecycle, String> {
    let stream_id = agent_stream_id(packet.agent_id());
    let revision = store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    let mut records = Vec::new();
    for kind in [
        "agent.run_admitted",
        "agent.backend_admitted",
        "agent.local_exit",
    ] {
        let key = format!("{kind}:{}", packet.run_id());
        if let Some(record) = store
            .event_by_idempotency_key(&stream_id, &key)
            .map_err(|error| error.to_string())?
        {
            records.push(Arc::new(record));
        }
    }
    let lifecycle = authenticate_run_lifecycle_records(packet, revision, &records)?;
    if store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?
        != revision
    {
        return Err("Agent exit source changed during authentication".into());
    }
    Ok(lifecycle)
}

pub(crate) fn projection_run_closed_from_records(
    packet: &AgentTaskPacket,
    revision: u64,
    records: &[Arc<crate::DurableRuntimeEvent>],
) -> Result<bool, String> {
    let lifecycle = authenticate_run_lifecycle_records(packet, revision, records)?;
    Ok(lifecycle.closed_for_governed_effects())
}

fn authenticate_run_lifecycle_records(
    packet: &AgentTaskPacket,
    revision: u64,
    records: &[Arc<crate::DurableRuntimeEvent>],
) -> Result<AuthenticatedRunLifecycle, String> {
    let stream_id = agent_stream_id(packet.agent_id());
    let packet_sha256 = run_packet_sha256(packet)?;
    let kinds = [
        "agent.run_admitted",
        "agent.backend_admitted",
        "agent.local_exit",
    ];
    if records.len() > kinds.len()
        || records
            .iter()
            .any(|record| !kinds.contains(&record.kind.as_str()))
        || kinds
            .iter()
            .any(|kind| records.iter().filter(|record| record.kind == *kind).count() > 1)
    {
        return Err("ambiguous Agent exit lifecycle source records".into());
    }
    let mut admission: Option<RunExitFact> = None;
    let mut backend_admission: Option<RunExitFact> = None;
    let mut exited = None;
    let mut previous_sequence = 0;
    let mut previous_cursor = 0;
    for kind in kinds {
        let key = format!("{kind}:{}", packet.run_id());
        let Some(event) = records.iter().find(|record| record.kind == kind) else {
            continue;
        };
        if event.sequence > revision {
            // The run stream advanced while this lifecycle was read: the
            // reader holds a stale view of the stream, not an untrusted one.
            // The caller reads the lifecycle again and decides on the fresh
            // view, so this is a retryable source change rather than a trust
            // violation.
            return Err("Agent exit source changed during authentication".into());
        }
        if event.scope != RuntimeEventScope::Agent
            || event.actor.as_deref() != Some("agent_runtime")
            || event.stream_id != stream_id
            || event.kind != kind
            || event.sequence <= previous_sequence
            || event.commit_cursor <= previous_cursor
            || event.transaction_index != 0
        {
            return Err("untrusted or out-of-order Agent exit lifecycle event".into());
        }
        previous_sequence = event.sequence;
        previous_cursor = event.commit_cursor;
        let fact: RunExitFact =
            serde_json::from_value(event.payload.clone()).map_err(|error| error.to_string())?;
        if !fact.matches(packet, &packet_sha256)? {
            return Err("Agent exit schema or exact identity mismatch".into());
        }
        if event.schema_version != 1
            || event.idempotency_key.as_deref() != Some(key.as_str())
            || event.transaction_id
                != format!(
                    "{}:{}:{}:{}",
                    event.kind, fact.run_id, fact.owner_epoch, fact.packet_sha256
                )
        {
            return Err("Agent exit original transaction identity mismatch".into());
        }
        if !event
            .refs
            .iter()
            .any(|reference| reference.kind == "run" && reference.id == fact.run_id)
        {
            return Err("Agent exit lifecycle run reference missing".into());
        }
        match kind {
            "agent.run_admitted" => {
                if admission.is_some()
                    || fact.phase != LocalRunPhase::Admitted
                    || fact.backend_packet_sha256.is_some()
                    || fact.process_envelope.is_some()
                {
                    return Err("ambiguous Agent run admission".into());
                }
                admission = Some(fact);
            }
            "agent.backend_admitted" => {
                let Some(original) = admission.as_ref() else {
                    return Err("Agent backend admission lacks original run admission".into());
                };
                if original.owner_epoch != fact.owner_epoch
                    || backend_admission.is_some()
                    || exited.is_some()
                    || fact.phase != LocalRunPhase::BackendPending
                    || fact.backend_packet_sha256.is_none()
                {
                    return Err("Agent backend admission identity or phase mismatch".into());
                }
                backend_admission = Some(fact);
            }
            "agent.local_exit" => {
                let Some(original) = admission.as_ref() else {
                    return Err("Agent exit lacks original admission".into());
                };
                if original.owner_epoch != fact.owner_epoch || exited.is_some() {
                    return Err("Agent exit owner epoch mismatch or duplicate exit".into());
                }
                if let Some(backend) = backend_admission.as_ref() {
                    if backend.backend_packet_sha256 != fact.backend_packet_sha256
                        || backend.process_envelope != fact.process_envelope
                    {
                        return Err("Agent exit backend packet mismatch".into());
                    }
                } else if fact.phase != LocalRunPhase::NotStarted
                    || fact.backend_packet_sha256.is_some()
                    || fact.process_envelope.is_some()
                {
                    return Err("Agent exit lacks exact backend admission".into());
                }
                if matches!(
                    fact.phase,
                    LocalRunPhase::Admitted | LocalRunPhase::BackendPending
                ) {
                    return Err("Agent exit has no local exit phase".into());
                }
                exited = Some(fact);
            }
            _ => unreachable!(),
        }
    }
    Ok(AuthenticatedRunLifecycle {
        exit: exited,
        backend: backend_admission,
        sources: vec![ExpectedStreamRevision {
            stream_id,
            expected_revision: revision,
        }],
    })
}

#[derive(Clone)]
struct RegisteredAgentBackend {
    backend: Arc<dyn AgentRuntimeBackend>,
    observation_authority: bool,
    process_exit_owner: Option<Arc<crate::ProcessJsonlAdapter>>,
}

/// Single workspace-scoped owner of agent lifecycle state. The event store is
/// canonical; original records also retain process-local work to actual exit.
pub struct AgentRuntime {
    event_store: Arc<RuntimeEventStore>,
    selector: AgentModelSelector,
    catalog: Arc<AgentCatalog>,
    records: Arc<RwLock<BTreeMap<String, AgentRunRecord>>>,
    graph_agent_ids: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
    backends: Arc<RwLock<BTreeMap<AgentBackendKind, RegisteredAgentBackend>>>,
    process_jsonl_adapter: Arc<RwLock<Option<Arc<crate::ProcessJsonlAdapter>>>>,
    services: Arc<RwLock<Option<Weak<RuntimeServices>>>>,
    pending_cancellations: Arc<Mutex<BTreeSet<(String, String, String, u32)>>>,
    run_locks: Arc<Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>>,
    lifecycle_locks: Arc<Mutex<BTreeMap<String, Weak<Mutex<()>>>>>,
    closing: Arc<AtomicBool>,
    owner_epoch: Arc<String>,
    #[cfg(test)]
    exit_append_failures: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    exit_append_failed: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    command_append_failures: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    cancellation_append_failures: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    cancellation_append_failed: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    backend_append_failures: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    backend_append_failed: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    run_admission_gate: Arc<Mutex<Option<Arc<ProcessRunAdmissionGate>>>>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ProcessRunAdmissionGate {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

impl AgentRuntime {
    #[cfg(test)]
    pub(crate) fn validate_process_fixture_boundary(
        &self,
        packet: &AgentTaskPacket,
    ) -> Result<(), String> {
        self.process_jsonl_adapter
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .validate_packet_command(packet)
    }
    #[cfg(test)]
    pub(crate) fn hold_next_process_run_admission(&self) -> Arc<ProcessRunAdmissionGate> {
        let gate = Arc::new(ProcessRunAdmissionGate::default());
        *self.run_admission_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    #[cfg(test)]
    pub(crate) fn process_fixture_exit_gate(
        &self,
        unknown: bool,
    ) -> Arc<crate::agent_process_jsonl_adapter::ProcessExitGate> {
        self.process_jsonl_adapter
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .fixture_exit_gate(unknown)
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn install_process_fixture_scratch(
        &self,
        root: std::path::PathBuf,
    ) -> Result<(), String> {
        self.process_jsonl_adapter
            .read()
            .map_err(|_| "Process adapter poisoned")?
            .as_ref()
            .ok_or("Process adapter missing")?
            .install_fixture_scratch(root)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn process_fixture_scratch(
        &self,
        packet: &AgentTaskPacket,
    ) -> Result<std::path::PathBuf, String> {
        self.process_jsonl_adapter
            .read()
            .map_err(|_| "Process adapter poisoned")?
            .as_ref()
            .ok_or("Process adapter missing")?
            .fixture_scratch_directory(packet)
    }

    fn shared_owner(&self) -> Self {
        Self {
            event_store: self.event_store.clone(),
            selector: self.selector.clone(),
            catalog: self.catalog.clone(),
            records: self.records.clone(),
            graph_agent_ids: self.graph_agent_ids.clone(),
            backends: self.backends.clone(),
            process_jsonl_adapter: self.process_jsonl_adapter.clone(),
            services: self.services.clone(),
            pending_cancellations: self.pending_cancellations.clone(),
            run_locks: self.run_locks.clone(),
            lifecycle_locks: self.lifecycle_locks.clone(),
            closing: self.closing.clone(),
            owner_epoch: self.owner_epoch.clone(),
            #[cfg(test)]
            exit_append_failures: self.exit_append_failures.clone(),
            #[cfg(test)]
            command_append_failures: self.command_append_failures.clone(),
            #[cfg(test)]
            cancellation_append_failures: self.cancellation_append_failures.clone(),
            #[cfg(test)]
            cancellation_append_failed: self.cancellation_append_failed.clone(),
            #[cfg(test)]
            exit_append_failed: self.exit_append_failed.clone(),
            #[cfg(test)]
            backend_append_failures: self.backend_append_failures.clone(),
            #[cfg(test)]
            backend_append_failed: self.backend_append_failed.clone(),
            #[cfg(test)]
            run_admission_gate: self.run_admission_gate.clone(),
        }
    }

    #[must_use]
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        provider_registry: Arc<ProviderRegistry>,
    ) -> Self {
        let runtime = Self {
            event_store,
            selector: AgentModelSelector::new(provider_registry),
            catalog: Arc::new(AgentCatalog::new()),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            graph_agent_ids: Arc::new(RwLock::new(BTreeMap::new())),
            backends: Arc::new(RwLock::new(BTreeMap::new())),
            process_jsonl_adapter: Arc::new(RwLock::new(None)),
            services: Arc::new(RwLock::new(None)),
            pending_cancellations: Arc::new(Mutex::new(BTreeSet::new())),
            run_locks: Arc::new(Mutex::new(BTreeMap::new())),
            lifecycle_locks: Arc::new(Mutex::new(BTreeMap::new())),
            closing: Arc::new(AtomicBool::new(false)),
            owner_epoch: Arc::new(uuid::Uuid::new_v4().to_string()),
            #[cfg(test)]
            exit_append_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            command_append_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            cancellation_append_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            cancellation_append_failed: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            exit_append_failed: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            backend_append_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            backend_append_failed: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            run_admission_gate: Arc::new(Mutex::new(None)),
        };
        runtime.restore_projection();
        runtime
    }

    async fn acquire_run_lock(&self, agent_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self
                .run_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(agent_id).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(agent_id.to_string(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    fn lifecycle_lock(&self, agent_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .lifecycle_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(agent_id).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(Mutex::new(()));
            locks.insert(agent_id.to_string(), Arc::downgrade(&lock));
            lock
        }
    }

    #[cfg(test)]
    fn retained_lock_counts(&self) -> (usize, usize) {
        let run_locks = {
            let mut locks = self
                .run_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            locks.len()
        };
        let lifecycle_locks = {
            let mut locks = self
                .lifecycle_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            locks.len()
        };
        (run_locks, lifecycle_locks)
    }

    pub fn bind_services(&self, services: Arc<RuntimeServices>) {
        *self
            .services
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(&services));
    }

    #[must_use]
    pub fn services(&self) -> Option<Arc<RuntimeServices>> {
        self.services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    #[must_use]
    pub fn catalog(&self) -> &Arc<AgentCatalog> {
        &self.catalog
    }

    pub fn register_backend(&self, backend: Arc<dyn AgentRuntimeBackend>) {
        let kind = backend.kind();
        self.backends
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                kind,
                RegisteredAgentBackend {
                    backend,
                    observation_authority: false,
                    process_exit_owner: None,
                },
            );
    }

    /// Register a Runtime-owned backend whose observations originate from
    /// the canonical ToolHost receipt chain. Kept crate-private so external
    /// backend plugins cannot self-promote model-authored acceptance.
    pub(crate) fn register_observation_authority_backend(
        &self,
        backend: Arc<dyn AgentRuntimeBackend>,
    ) {
        let kind = backend.kind();
        self.backends
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                kind,
                RegisteredAgentBackend {
                    backend,
                    observation_authority: true,
                    process_exit_owner: None,
                },
            );
    }

    /// Install the one Runtime-owned ProcessJsonl bridge. Its command
    /// manifests are keyed by immutable `command_ref` plus digest, not by a
    /// mutable Agent instance id.
    pub(crate) fn register_runtime_process_jsonl_backend(
        &self,
        adapter: Arc<crate::ProcessJsonlAdapter>,
    ) {
        self.backends
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                AgentBackendKind::ProcessJsonl,
                RegisteredAgentBackend {
                    backend: adapter.clone(),
                    observation_authority: true,
                    process_exit_owner: Some(adapter.clone()),
                },
            );
        *self
            .process_jsonl_adapter
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(adapter);
    }

    pub(crate) fn register_process_jsonl_command(
        &self,
        spec: crate::ProcessJsonlSpec,
    ) -> Result<(), String> {
        let adapter = self
            .process_jsonl_adapter
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                "ProcessJsonl Runtime bridge is not installed for this RuntimeServices instance"
                    .to_string()
            })?;
        adapter.register_command(spec)
    }

    #[must_use]
    pub fn list(&self) -> Vec<AgentRunSnapshot> {
        let mut runs = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter_map(|record| record.snapshot.clone())
            .collect::<Vec<_>>();
        runs.sort_by(|left, right| {
            left.updated_at_ms
                .cmp(&right.updated_at_ms)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        runs
    }

    /// Original run truth retains the immutable typed Program/member scope.
    /// No display metadata, role names or cross-Program graph scans participate.
    pub(crate) fn running_agentic_members(&self, program_id: &str) -> BTreeSet<String> {
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|record| {
                record
                    .retained
                    .as_ref()
                    .is_some_and(|run| !run.physically_exited())
                    || record
                        .snapshot
                        .as_ref()
                        .is_some_and(|run| !run.status.is_terminal())
            })
            .filter_map(|record| record.snapshot.as_ref())
            .filter_map(|run| run.agentic_binding.as_ref())
            .filter(|binding| binding.program_id == program_id)
            .map(|binding| binding.agent_id.clone())
            .collect()
    }

    #[must_use]
    pub fn list_for_graphs(&self, graph_ids: &BTreeSet<String>) -> Vec<AgentRunSnapshot> {
        let agent_ids = {
            let index = self
                .graph_agent_ids
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            graph_ids
                .iter()
                .filter_map(|graph_id| index.get(graph_id))
                .flat_map(|ids| ids.iter().cloned())
                .collect::<BTreeSet<_>>()
        };
        let mut runs = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(agent_id, _)| agent_ids.contains(*agent_id))
            .filter_map(|(_, record)| record.snapshot.as_ref())
            .cloned()
            .collect::<Vec<_>>();
        runs.sort_by(|left, right| {
            left.updated_at_ms
                .cmp(&right.updated_at_ms)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        runs
    }

    #[must_use]
    pub fn get(&self, agent_id: &str) -> Option<AgentRunSnapshot> {
        let cached = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .and_then(|record| record.snapshot.clone());
        cached.or_else(|| {
            self.latest_durable_record(agent_id)
                .and_then(|record| record.snapshot)
        })
    }

    /// Return the canonical terminal packet retained with an Agent lifecycle
    /// event. Consumers may project it, but cannot mutate graph state through
    /// this read API.
    #[must_use]
    pub fn terminal_return(&self, agent_id: &str) -> Option<AgentReturnPacket> {
        let cached = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .and_then(|record| record.returned.clone());
        cached.or_else(|| {
            self.latest_agent_payload_at_head(agent_id, |payload| payload.returned.is_some())
                .ok()?
                .0
                .and_then(|payload| payload.returned)
        })
    }

    fn latest_durable_record(&self, agent_id: &str) -> Option<AgentRunRecord> {
        match self.latest_durable_record_at_head(agent_id) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(agent_id, %error, "Agent lifecycle cold lookup failed closed");
                None
            }
        }
    }

    fn latest_durable_record_at_head(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentRunRecord>, String> {
        let (payload, head) = self.latest_agent_payload_at_head(agent_id, |_| true)?;
        Ok(payload.map(|mut payload| {
            payload.snapshot.revision = head;
            let mut receipts = BTreeMap::new();
            if let Some(receipt) = payload.receipt {
                receipts.insert(receipt.command_id.clone(), receipt);
            }
            AgentRunRecord {
                snapshot: Some(payload.snapshot),
                returned: payload.returned,
                receipts,
                retained: None,
                retiring: Vec::new(),
                release_after_completion: false,
            }
        }))
    }

    fn latest_agent_payload_at_head(
        &self,
        agent_id: &str,
        select: impl Fn(&PersistedAgentEvent) -> bool,
    ) -> Result<(Option<PersistedAgentEvent>, u64), String> {
        let stream_id = agent_stream_id(agent_id);
        let head = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let mut through = head;
        while through > 0 {
            let start = through.saturating_sub(128);
            let mut cursor = start;
            let mut latest = None;
            while cursor < through {
                let page = self.event_store.list_stream_after(
                    &stream_id,
                    cursor,
                    through,
                    128,
                    256 * 1024,
                )?;
                let last = page
                    .last()
                    .ok_or("Agent lifecycle keyset page has an unexpected gap")?
                    .sequence;
                if last <= cursor || last > through {
                    return Err(
                        "Agent lifecycle keyset did not advance inside its fixed head".into(),
                    );
                }
                for event in page {
                    if event.payload.get("snapshot").is_none() {
                        continue;
                    }
                    let payload: PersistedAgentEvent =
                        serde_json::from_value(event.payload).map_err(|error| error.to_string())?;
                    if payload.snapshot.agent_id != agent_id
                        || payload.snapshot.revision != event.sequence
                    {
                        return Err("Agent lifecycle snapshot stream identity mismatch".into());
                    }
                    if select(&payload) {
                        latest = Some(payload);
                    }
                }
                cursor = last;
            }
            if latest.is_some() {
                return Ok((latest, head));
            }
            through = start;
        }
        Ok((None, head))
    }

    /// Restore a verified durable run projection during Runtime recovery.
    ///
    /// This is intentionally a projection recovery API, not an execution API:
    /// callers cannot attach a backend or mutate graph state through it.
    pub fn restore_verified_run(&self, snapshot: AgentRunSnapshot) -> Result<(), String> {
        if [
            snapshot.run_id.as_str(),
            snapshot.agent_id.as_str(),
            snapshot.task_id.as_str(),
            snapshot.session_id.as_str(),
            snapshot.graph_id.as_str(),
            snapshot.node_id.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err("recovered AgentRuntime snapshot has an empty binding".into());
        }
        self.persist_snapshot(snapshot, "agent.recovered", "recovered", None, None)
            .map(|_| ())
    }

    /// Reconcile replayed runs according to their backend's recovery contract.
    ///
    /// Native runs are Runtime-owned: their graph/effect idempotency keys are
    /// durable, so a lost in-memory future is returned to `Prepared` and the
    /// owning graph may resume it after its Session resolver is rebound.
    /// ProcessJsonl runs cross a process boundary and currently expose no
    /// resume handle, so an admitted process run remains explicitly blocked
    /// for effect reconciliation instead of being silently re-executed.
    pub fn block_unrecoverable_replayed_runs(&self) -> Result<Vec<String>, String> {
        let snapshots = self.list();
        let mut blocked = Vec::new();
        for mut snapshot in snapshots.into_iter().filter(|snapshot| {
            !snapshot.status.is_terminal() && snapshot.status != AgentStatus::Prepared
        }) {
            if snapshot.backend == AgentBackendKind::InProcess {
                snapshot.status = AgentStatus::Prepared;
                snapshot.updated_at_ms = now_ms();
                snapshot.failure = None;
                self.persist_snapshot(
                    snapshot,
                    "agent.requeued_recovery",
                    "Runtime-owned backend will resume from the durable graph binding",
                    None,
                    None,
                )?;
                continue;
            }
            snapshot.status = AgentStatus::Blocked;
            snapshot.updated_at_ms = now_ms();
            snapshot.failure = Some("backend handle is unavailable after runtime restart".into());
            let agent_id = snapshot.agent_id.clone();
            self.persist_snapshot(
                snapshot,
                "agent.blocked_recovery",
                "backend handle is unavailable after runtime restart",
                None,
                None,
            )?;
            blocked.push(agent_id);
        }
        Ok(blocked)
    }

    /// Import a coordinator-verified legacy Agent snapshot set exactly once.
    ///
    /// This transaction owns both the import marker and every imported Agent
    /// stream, so a crash cannot leave a marker without an Agent projection or
    /// import only a subset of the supplied records. Active legacy records are
    /// deliberately blocked because an old process/session handle cannot be
    /// proved recoverable by a new runtime.
    pub fn import_legacy_state_records(
        &self,
        source_id: impl Into<String>,
        records: Vec<LegacyAgentStateRecord>,
    ) -> Result<LegacyAgentImportReport, String> {
        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err("legacy Agent import source_id must not be empty".into());
        }
        let marker_stream = legacy_import_stream_id(&source_id);
        let existing_marker = self
            .event_store
            .list_stream(&marker_stream)
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|event| event.kind == "agent.legacy_imported");
        if let Some(marker) = existing_marker {
            return Ok(LegacyAgentImportReport {
                source_id,
                duplicate: true,
                imported_agent_ids: marker.payload["imported_agent_ids"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
                blocked_agent_ids: marker.payload["blocked_agent_ids"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
            });
        }

        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: marker_stream.clone(),
            expected_revision: self
                .event_store
                .stream_revision(&marker_stream)
                .map_err(|error| error.to_string())?,
        }];
        let mut events = Vec::with_capacity(records.len().saturating_add(1));
        let mut imported = Vec::with_capacity(records.len());
        let mut blocked = Vec::new();
        let mut snapshots = Vec::with_capacity(records.len());
        let mut seen_agents = std::collections::BTreeSet::new();

        for record in records {
            validate_legacy_record(&record)?;
            if !seen_agents.insert(record.snapshot.agent_id.clone()) {
                return Err(format!(
                    "legacy Agent import contains duplicate agent_id {}",
                    record.snapshot.agent_id
                ));
            }

            let stream_id = agent_stream_id(&record.snapshot.agent_id);
            let mut snapshot = record.snapshot;
            if !snapshot.status.is_terminal() {
                snapshot.status = AgentStatus::Blocked;
                snapshot.failure =
                    Some("legacy active backend handle is not recoverable by AgentRuntime".into());
                blocked.push(snapshot.agent_id.clone());
            }
            snapshot.updated_at_ms = now_ms();
            snapshot.revision = self
                .event_store
                .stream_revision(&stream_id)
                .map_err(|error| error.to_string())?
                .saturating_add(1);
            expected_streams.push(ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: snapshot.revision.saturating_sub(1),
            });
            let payload = PersistedAgentEvent {
                snapshot: snapshot.clone(),
                receipt: None,
                returned: record.returned,
            };
            events.push(
                RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Agent,
                    kind: "agent.legacy_imported".into(),
                    status: Some("legacy Agent state imported".into()),
                    actor: Some("upgrade_coordinator".into()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "run".into(),
                            id: snapshot.run_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "legacy_source".into(),
                            id: record.source_ref,
                        },
                    ],
                    payload: serde_json::to_value(payload).map_err(|error| error.to_string())?,
                }
                .into(),
            );
            imported.push(snapshot.agent_id.clone());
            snapshots.push(snapshot);
        }
        events.push(
            RuntimeEventInput {
                stream_id: marker_stream.clone(),
                scope: RuntimeEventScope::Recovery,
                kind: "agent.legacy_imported".into(),
                status: Some("legacy Agent import complete".into()),
                actor: Some("upgrade_coordinator".into()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "source_id": source_id.clone(),
                    "imported_agent_ids": imported.clone(),
                    "blocked_agent_ids": blocked.clone(),
                }),
            }
            .into(),
        );
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("legacy-agent-import:{source_id}"),
                expected_streams,
                events,
            })
            .map_err(|error| error.to_string())?;
        self.restore_projection();
        Ok(LegacyAgentImportReport {
            source_id,
            duplicate: false,
            imported_agent_ids: imported,
            blocked_agent_ids: blocked,
        })
    }

    #[must_use]
    pub fn events(&self, agent_id: &str) -> Vec<AgentLifecycleEvent> {
        let stream_id = agent_stream_id(agent_id);
        self.event_store
            .list_stream(&stream_id)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|event| {
                let payload = serde_json::from_value::<PersistedAgentEvent>(event.payload).ok()?;
                Some(AgentLifecycleEvent {
                    event_id: event.event_id,
                    agent_id: payload.snapshot.agent_id,
                    revision: payload.snapshot.revision,
                    status: payload.snapshot.status,
                    kind: event.kind,
                    message: event.status.unwrap_or_default(),
                    created_at_ms: event.created_at_ms,
                })
            })
            .collect()
    }

    pub async fn execute_task(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String> {
        let run_guard = self.acquire_run_lock(packet.agent_id()).await;
        let admitted_packet = self.original_run_packet(&packet)?;
        let digest = run_packet_sha256(&admitted_packet)?;
        let retained = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(packet.agent_id())
            .and_then(|record| {
                record
                    .retiring
                    .iter()
                    .chain(record.retained.iter())
                    .find(|run| run.packet_sha256 == digest)
                    .cloned()
                    .or_else(|| record.retained.clone())
            });
        if let Some(retained) = retained {
            if retained.packet_sha256 == digest {
                drop(run_guard);
                return Self::wait_retained(&retained).await;
            }
            if !retained.physically_exited() {
                return Err("Agent still owns a physical run".into());
            }
        }
        if let Some(existing) = self.get(packet.agent_id()) {
            if existing.run_id == packet.run_id() {
                if confirmed_run_exit(&self.event_store, &admitted_packet)?.is_some() {
                    return self
                        .latest_agent_payload_at_head(packet.agent_id(), |event| {
                            event.snapshot.run_id == packet.run_id()
                                && event.snapshot.graph_id == packet.graph_id()
                                && event.snapshot.node_id == packet.node_id()
                                && event.snapshot.attempt == packet.attempt
                                && event.returned.is_some()
                        })?
                        .0
                        .and_then(|event| event.returned)
                        .ok_or_else(|| {
                            "exact exited Agent run lacks a canonical return packet".to_string()
                        });
                }
                return Err(
                    "original Agent run requires reconciliation; no exact durable exit".into(),
                );
            }
            if existing.run_id != packet.run_id() && !existing.status.is_terminal() {
                return Err(format!(
                    "agent {} already owns an active run",
                    packet.agent_id()
                ));
            }
        }
        if self
            .event_store
            .event_by_idempotency_key(
                &agent_stream_id(packet.agent_id()),
                &format!("agent.run_admitted:{}", packet.run_id()),
            )
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("original Agent admission cannot be replayed as a new owner epoch".into());
        }
        let (result, _) = tokio::sync::watch::channel(None);
        let retained = Arc::new(RetainedAgentRun {
            packet_sha256: digest.clone(),
            run_key: run_cancellation_key(&packet),
            identity_refs: identity_refs(
                &packet.assignment.execution_identity,
                packet.agent_id(),
                packet.run_id(),
            ),
            result,
            join: tokio::sync::Mutex::new(None),
            pending_exit: Mutex::new(None),
            exit_committed: AtomicBool::new(false),
            exit_phase: Mutex::new(None),
            exit_evidence: Mutex::new(None),
            exit_publisher_claimed: AtomicBool::new(false),
        });
        let evidence = RunExitFact {
            schema_version: 1,
            agent_id: packet.agent_id().into(),
            run_id: packet.run_id().into(),
            graph_id: packet.graph_id().into(),
            node_id: packet.node_id().into(),
            attempt: packet.attempt,
            packet_sha256: digest.clone(),
            backend_packet_sha256: None,
            owner_epoch: self.owner_epoch.as_ref().clone(),
            backend: backend_from_packet(&packet)?,
            phase: LocalRunPhase::Admitted,
            process_envelope: None,
        };
        {
            let mut records = self
                .records
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.closing.load(Ordering::Acquire) {
                drop(records);
                return self.reject_unstarted_run(
                    &admitted_packet,
                    "AgentRuntime admission is fenced for shutdown",
                );
            }
            if records
                .values()
                .flat_map(|record| record.retained.iter().chain(record.retiring.iter()))
                .filter(|run| !run.locally_settled())
                .count()
                >= 1024
            {
                drop(records);
                return self.reject_unstarted_run(
                    &admitted_packet,
                    "AgentRuntime retained run capacity exhausted",
                );
            }
            let record = records.entry(packet.agent_id().into()).or_default();
            if let Some(previous) = record.retained.take() {
                let finishing = previous.result.borrow().is_none();
                if finishing {
                    record.retiring.push(previous);
                }
            }
            record.retained = Some(retained.clone());
            record.release_after_completion = false;
            let runtime = self.shared_owner();
            let owned_run = retained.clone();
            let handle = tokio::spawn(async move {
                let agent_id = packet.agent_id().to_string();
                let result = std::panic::AssertUnwindSafe(
                    runtime.execute_task_owned(packet, run_guard, evidence, &owned_run),
                )
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err("original Agent run owner completion panicked".into()));
                owned_run.result.send_replace(Some(result));
                {
                    let mut records = runtime
                        .records
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(record) = records.get_mut(&agent_id) {
                        record.retiring.retain(|run| !Arc::ptr_eq(run, &owned_run));
                    }
                }
                runtime.release_completed_record(&agent_id);
            });
            *retained
                .join
                .try_lock()
                .expect("new retained run join is uncontended") = Some(handle);
        }
        Self::wait_retained(&retained).await
    }

    async fn wait_retained(run: &RetainedAgentRun) -> Result<AgentReturnPacket, String> {
        let mut receiver = run.result.subscribe();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            receiver
                .changed()
                .await
                .map_err(|_| "Agent run completion channel closed".to_string())?;
        }
    }

    fn original_run_packet(&self, supplied: &AgentTaskPacket) -> Result<AgentTaskPacket, String> {
        let Some(services) = self.services() else {
            return Ok(supplied.clone());
        };
        let graph = match services.graph_state_store().load(supplied.graph_id()) {
            Ok(graph) => graph,
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => {
                return Ok(supplied.clone())
            }
            Err(error) => return Err(error.to_string()),
        };
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == supplied.node_id() && node.kind == ExecutionNodeKind::AgentTask)
            .ok_or("original Agent admission node missing")?;
        let original: AgentTaskPacket =
            serde_json::from_str(&node.payload_ref).map_err(|error| error.to_string())?;
        if original.assignment != supplied.assignment
            || original.attempt != supplied.attempt
            || original.expected_graph_revision != supplied.expected_graph_revision
            || original.binding != supplied.binding
        {
            return Err("Agent run does not match its immutable graph admission".into());
        }
        Ok(original)
    }

    fn reject_unstarted_run(
        &self,
        packet: &AgentTaskPacket,
        reason: &str,
    ) -> Result<AgentReturnPacket, String> {
        let mut evidence = RunExitFact {
            schema_version: 1,
            agent_id: packet.agent_id().into(),
            run_id: packet.run_id().into(),
            graph_id: packet.graph_id().into(),
            node_id: packet.node_id().into(),
            attempt: packet.attempt,
            packet_sha256: run_packet_sha256(packet)?,
            backend_packet_sha256: None,
            owner_epoch: self.owner_epoch.as_ref().clone(),
            backend: backend_from_packet(packet)?,
            phase: LocalRunPhase::Admitted,
            process_envelope: None,
        };
        self.persist_run_fact(&evidence, "agent.run_admitted")?;
        evidence.phase = LocalRunPhase::NotStarted;
        self.persist_run_fact(&evidence, "agent.local_exit")?;
        Err(reason.to_string())
    }

    pub(crate) fn fence_run_admission(&self) {
        let _records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.closing.store(true, Ordering::Release);
    }

    fn release_completed_record(&self, agent_id: &str) {
        let removed = {
            let mut records = self
                .records
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !records.get(agent_id).is_some_and(|record| {
                record.release_after_completion
                    && record.retiring.is_empty()
                    && record
                        .retained
                        .as_ref()
                        .is_none_or(|run| run.locally_settled())
            }) {
                return;
            }
            records.remove(agent_id)
        };
        if let Some(snapshot) = removed.and_then(|record| record.snapshot) {
            let mut index = self
                .graph_agent_ids
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(agents) = index.get_mut(&snapshot.graph_id) {
                agents.remove(agent_id);
                if agents.is_empty() {
                    index.remove(&snapshot.graph_id);
                }
            }
        }
    }

    pub(crate) async fn drain_retained_runs(&self) -> Result<(), String> {
        self.fence_run_admission();
        let runs = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .flat_map(|record| {
                record
                    .retained
                    .iter()
                    .chain(record.retiring.iter())
                    .cloned()
            })
            .collect::<Vec<_>>();
        let mut errors = Vec::new();
        for run in runs {
            let mut join = run.join.lock().await;
            if let Some(handle) = join.as_mut() {
                if let Err(error) = handle.await {
                    errors.push(format!("Agent physical run join failed: {error}"));
                }
            }
            join.take();
            if let Some(Err(error)) = run.result.borrow().as_ref() {
                errors.push(format!(
                    "Agent run owner did not complete normally: {error}"
                ));
            }
            if run
                .pending_exit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_none_or(|evidence| !evidence.positive())
            {
                errors.push("Agent physical run finished without authenticated local exit".into());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Whether this exact run has been cancelled.
    ///
    /// The pending set carries a cancellation that arrived before the backend
    /// was admitted; the durable snapshot is the committed truth of a
    /// cancellation the Runtime already accepted. This observation never
    /// consumes the pending intent, so the run owner and a cancellation path
    /// see the same answer.
    fn cancellation_observed(&self, packet: &AgentTaskPacket) -> bool {
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&run_cancellation_key(packet))
            || self
                .get(packet.agent_id())
                .is_some_and(|snapshot| snapshot.status == AgentStatus::Cancelled)
    }

    /// The identity refs of the exact run this fact belongs to.
    ///
    /// The identity is proven, never assumed: the fact must belong to the
    /// immutable packet of a retained run, and the refs are the ones that
    /// packet carries. A fact whose exact run cannot be proven keeps the run
    /// ref alone.
    fn run_identity_refs(&self, evidence: &RunExitFact) -> Vec<RuntimeEventRef> {
        let mut refs = vec![RuntimeEventRef {
            kind: "run".into(),
            id: evidence.run_id.clone(),
        }];
        let exact = {
            let records = self
                .records
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            records.get(&evidence.agent_id).and_then(|record| {
                record
                    .retained
                    .iter()
                    .chain(record.retiring.iter())
                    .find(|run| {
                        run.run_key
                            == (
                                evidence.run_id.clone(),
                                evidence.graph_id.clone(),
                                evidence.node_id.clone(),
                                evidence.attempt,
                            )
                            && run.packet_sha256 == evidence.packet_sha256
                    })
                    .map(|run| run.identity_refs.clone())
            })
        };
        if let Some(identity) = exact {
            refs.extend(identity);
        }
        refs
    }

    fn persist_run_fact(&self, evidence: &RunExitFact, kind: &str) -> Result<(), String> {
        #[cfg(test)]
        if kind == "agent.backend_admitted"
            && self
                .backend_append_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            self.backend_append_failed.notify_one();
            return Err("test injected original Agent backend admission append failure".into());
        }
        #[cfg(test)]
        if kind == "agent.local_exit"
            && self
                .exit_append_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            self.exit_append_failed.notify_one();
            return Err("test injected original Agent exit append failure".into());
        }
        let lock = self.lifecycle_lock(&evidence.agent_id);
        let _guard = lock
            .lock()
            .map_err(|_| "Agent lifecycle lock poisoned".to_string())?;
        let stream_id = agent_stream_id(&evidence.agent_id);
        let fact_key = format!("{kind}:{}", evidence.run_id);
        if let Some(event) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &fact_key)
            .map_err(|error| error.to_string())?
        {
            if event.kind != kind || event.actor.as_deref() != Some("agent_runtime") {
                return Err("untrusted original Agent run fact replay".into());
            }
            let existing: RunExitFact =
                serde_json::from_value(event.payload).map_err(|error| error.to_string())?;
            if existing != *evidence {
                return Err("conflicting original Agent run fact".into());
            }
            self.refresh_run_fact_revision(evidence)?;
            return Ok(());
        }
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let run_refs = self.run_identity_refs(evidence);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!(
                    "{kind}:{}:{}:{}",
                    evidence.run_id, evidence.owner_epoch, evidence.packet_sha256
                ),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: revision,
                }],
                events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::Agent,
                        kind: kind.into(),
                        status: None,
                        actor: Some("agent_runtime".into()),
                        refs: run_refs,
                        payload: serde_json::to_value(evidence)
                            .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(fact_key),
                    schema_version: 1,
                }],
            })
            .map_err(|error| error.to_string())?;
        self.refresh_run_fact_revision(evidence)?;
        Ok(())
    }

    fn refresh_run_fact_revision(&self, evidence: &RunExitFact) -> Result<(), String> {
        let revision = self
            .event_store
            .stream_revision(&agent_stream_id(&evidence.agent_id))
            .map_err(|error| error.to_string())?;
        if let Some(snapshot) = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&evidence.agent_id)
            .and_then(|record| record.snapshot.as_mut())
        {
            if snapshot.run_id == evidence.run_id {
                snapshot.revision = snapshot.revision.max(revision);
            }
        }
        Ok(())
    }

    async fn persist_run_fact_until_durable(&self, evidence: &RunExitFact, kind: &str) {
        let mut retry_delay = std::time::Duration::from_millis(100);
        loop {
            match self.persist_run_fact(evidence, kind) {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(run_id = %evidence.run_id, %error, kind, "retaining original Agent run fact until durable commit");
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay
                        .saturating_mul(2)
                        .min(std::time::Duration::from_secs(5));
                }
            }
        }
    }

    /// Publish the authenticated local exit of a run exactly once.
    ///
    /// `agent.local_exit` is written through the same authenticated channel,
    /// the same idempotency key and the same stream as every other run fact.
    /// The single-writer claim keeps a repeated cancellation from producing a
    /// second exit fact with a different phase.
    async fn publish_run_exit(&self, retained: &RetainedAgentRun, evidence: &RunExitFact) {
        if !evidence.positive() {
            return;
        }
        if !retained.claim_exit_publication() {
            // Another publisher already owns this run's authenticated exit.
            // Its fact carries the same identity and the same decided phase.
            return;
        }
        *retained
            .pending_exit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(evidence.clone());
        self.persist_run_fact_until_durable(evidence, "agent.local_exit")
            .await;
        retained.exit_committed.store(true, Ordering::Release);
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(
                evidence.run_id.clone(),
                evidence.graph_id.clone(),
                evidence.node_id.clone(),
                evidence.attempt,
            ));
    }

    async fn execute_task_owned(
        &self,
        packet: AgentTaskPacket,
        run_guard: tokio::sync::OwnedMutexGuard<()>,
        mut evidence: RunExitFact,
        retained: &RetainedAgentRun,
    ) -> Result<AgentReturnPacket, String> {
        #[cfg(test)]
        if evidence.backend == AgentBackendKind::ProcessJsonl {
            let gate = self.run_admission_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        self.persist_run_fact_until_durable(&evidence, "agent.run_admitted")
            .await;
        evidence.phase = LocalRunPhase::NotStarted;
        // Mirror the evidence so a cancellation path can commit the same
        // authenticated exit fact when this owner can no longer reach its own
        // publication point.
        retained.mirror_exit_evidence(&evidence);
        let result = std::panic::AssertUnwindSafe(self.execute_task_locked(
            packet.clone(),
            &mut evidence,
            retained,
        ))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err("Agent backend owner panicked; physical exit unconfirmed".into()));
        if evidence.phase == LocalRunPhase::BackendPending {
            evidence.phase = LocalRunPhase::Unknown;
        }
        self.publish_run_exit(retained, &evidence).await;
        // The next opportunity may choose this same Agent. Never await its
        // dispatch while retaining the previous run's exclusive lock.
        drop(run_guard);
        if let Some(services) = self.services() {
            // Why a Submitted Task can sit with no reviewer: this reconcile is
            // the only thing that admits Review work after an agent finishes,
            // and one live run ended with two Tasks `Submitted`, zero review
            // dispatches, and not one `worker-exit-reconcile` action anywhere in
            // the journal. Each branch below says which condition held, so the
            // next run names the cause instead of leaving it to be inferred.
            let has_agentic_binding = packet.agentic_binding.is_some();
            let reason = match &result {
                Ok(returned) => returned
                    .failure
                    .as_deref()
                    .unwrap_or("Agent execution ended without a durable task submission or review"),
                Err(error) => error.as_str(),
            };
            if let Err(error) = services
                .settle_abandoned_agentic_attempt(&packet, reason)
                .await
            {
                tracing::warn!(run_id = packet.run_id(), %error, "Agent attempt settlement awaits durable recovery");
            }
            if let Some(binding) = packet.agentic_binding.as_ref() {
                match services
                    .dispatch_ready_agentic_work(&binding.program_id)
                    .await
                {
                    Ok(receipts) => tracing::info!(
                        run_id = packet.run_id(),
                        program_id = %binding.program_id,
                        dispatched = receipts.len(),
                        "worker-exit reconcile ran after an agent finished"
                    ),
                    Err(error) => {
                        tracing::warn!(run_id = packet.run_id(), %error, "ready Agent work awaits recovery after worker exit")
                    }
                }
            } else {
                tracing::info!(
                    run_id = packet.run_id(),
                    has_agentic_binding,
                    "worker-exit reconcile skipped: this agent carries no Agentic binding, so no Review work can be admitted"
                );
            }
        } else {
            tracing::info!(
                run_id = packet.run_id(),
                "worker-exit reconcile skipped: no RuntimeServices are bound to this agent"
            );
        }
        result
    }

    async fn execute_task_locked(
        &self,
        packet: AgentTaskPacket,
        exit_evidence: &mut RunExitFact,
        retained: &RetainedAgentRun,
    ) -> Result<AgentReturnPacket, String> {
        let original_packet = self.original_run_packet(&packet)?;
        let packet = self.attach_predecessor_context(packet).await?;
        let packet = self.ensure_runtime_binding(packet)?;
        let backend_kind = backend_from_packet(&packet)?;
        ensure_team_backend_trusted(&packet, backend_kind)?;
        if backend_kind == AgentBackendKind::ProcessJsonl {
            if let Some(adapter) = self
                .process_jsonl_adapter
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                adapter.validate_packet_command(&packet)?;
            }
        }
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&run_cancellation_key(&packet))
        {
            let returned = cancelled_return(
                &packet,
                "agent cancelled before provider/backend admission".to_string(),
            );
            self.persist_snapshot(
                AgentRunSnapshot {
                    agentic_binding: packet.agentic_binding.clone(),
                    execution_identity: packet.assignment.execution_identity.clone(),
                    run_id: packet.run_id().to_string(),
                    agent_id: packet.agent_id().to_string(),
                    task_id: packet.task_id().to_string(),
                    root_task_id: packet.assignment.root_task_id.clone(),
                    session_id: packet.session_id().to_string(),
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    attempt: packet.attempt,
                    expected_graph_revision: packet.expected_graph_revision,
                    backend: backend_kind,
                    status: AgentStatus::Cancelled,
                    revision: 0,
                    model: None,
                    provider: None,
                    binding: packet.binding.clone(),
                    started_at_ms: now_ms(),
                    updated_at_ms: now_ms(),
                    failure: returned.failure.clone(),
                },
                "agent.cancelled",
                "cancelled before backend admission",
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "Runtime failed to materialize Agent Binding".to_string())?;
        // ProcessJsonl owns its external model lifecycle. Requiring a local
        // ProviderRegistry entry here would silently make a configured
        // process Agent unusable; it is not a safe model fallback. Runtime
        // records the frozen profile as a configuration identity and labels
        // the provider honestly as external_process.
        let selection = match backend_kind {
            AgentBackendKind::ProcessJsonl => AgentModelSelection {
                model: binding.model_policy.profile.clone(),
                provider: "external_process".to_string(),
                registry_revision: 0,
            },
            AgentBackendKind::InProcess => {
                match self.selector.select(nonempty(&packet.model_lease)) {
                    Ok(selection) => selection,
                    Err(error) => {
                        let failure = error.to_string();
                        let returned = blocked_return(&packet, failure.clone());
                        self.persist_snapshot(
                            AgentRunSnapshot {
                                agentic_binding: packet.agentic_binding.clone(),
                                execution_identity: packet.assignment.execution_identity.clone(),
                                run_id: packet.run_id().to_string(),
                                agent_id: packet.agent_id().to_string(),
                                task_id: packet.task_id().to_string(),
                                root_task_id: packet.assignment.root_task_id.clone(),
                                session_id: packet.session_id().to_string(),
                                graph_id: packet.graph_id().to_string(),
                                node_id: packet.node_id().to_string(),
                                attempt: packet.attempt,
                                expected_graph_revision: packet.expected_graph_revision,
                                backend: backend_kind,
                                status: AgentStatus::Blocked,
                                revision: 0,
                                model: None,
                                provider: None,
                                binding: packet.binding.clone(),
                                started_at_ms: now_ms(),
                                updated_at_ms: now_ms(),
                                failure: Some(failure.clone()),
                            },
                            "agent.blocked",
                            &failure,
                            None,
                            Some(returned.clone()),
                        )?;
                        return Ok(returned);
                    }
                }
            }
        };
        if backend_kind != AgentBackendKind::ProcessJsonl
            && !binding.model_policy.allowed_models.is_empty()
            && !binding
                .model_policy
                .allowed_models
                .contains(&selection.model)
        {
            let failure = format!(
                "selected model `{}` is outside Binding model policy for {}@{}",
                selection.model,
                binding.definition_ref.definition_id.as_str(),
                binding.definition_ref.revision
            );
            let returned = blocked_return(&packet, failure.clone());
            self.persist_snapshot(
                AgentRunSnapshot {
                    agentic_binding: packet.agentic_binding.clone(),
                    execution_identity: packet.assignment.execution_identity.clone(),
                    run_id: packet.run_id().to_string(),
                    agent_id: packet.agent_id().to_string(),
                    task_id: packet.task_id().to_string(),
                    root_task_id: packet.assignment.root_task_id.clone(),
                    session_id: packet.session_id().to_string(),
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    attempt: packet.attempt,
                    expected_graph_revision: packet.expected_graph_revision,
                    backend: backend_kind,
                    status: AgentStatus::Blocked,
                    revision: 0,
                    model: Some(selection.model),
                    provider: Some(selection.provider),
                    binding: packet.binding.clone(),
                    started_at_ms: now_ms(),
                    updated_at_ms: now_ms(),
                    failure: Some(failure.clone()),
                },
                "agent.blocked",
                &failure,
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        let snapshot = AgentRunSnapshot {
            agentic_binding: packet.agentic_binding.clone(),
            execution_identity: packet.assignment.execution_identity.clone(),
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            root_task_id: packet.assignment.root_task_id.clone(),
            session_id: packet.session_id().to_string(),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            backend: backend_kind,
            status: AgentStatus::Prepared,
            revision: 0,
            model: Some(selection.model.clone()),
            provider: Some(selection.provider.clone()),
            binding: packet.binding.clone(),
            started_at_ms: now_ms(),
            updated_at_ms: now_ms(),
            failure: None,
        };
        self.persist_snapshot(snapshot, "agent.prepared", "prepared", None, None)?;
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&run_cancellation_key(&packet))
        {
            let returned = cancelled_return(
                &packet,
                "agent cancelled after prepare and before backend admission".to_string(),
            );
            let mut cancelled = self.get(packet.agent_id()).ok_or_else(|| {
                format!(
                    "prepared agent projection `{}` is missing",
                    packet.agent_id()
                )
            })?;
            cancelled.status = AgentStatus::Cancelled;
            cancelled.updated_at_ms = now_ms();
            cancelled.failure = returned.failure.clone();
            self.persist_snapshot(
                cancelled,
                "agent.cancelled",
                "cancelled before backend admission",
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        let mut running = self.get(packet.agent_id()).ok_or_else(|| {
            format!(
                "prepared agent projection `{}` is missing",
                packet.agent_id()
            )
        })?;
        if running.status == AgentStatus::Cancelled {
            let returned = cancelled_return(
                &packet,
                "agent cancelled during backend admission".to_string(),
            );
            running.failure = returned.failure.clone();
            running.updated_at_ms = now_ms();
            self.persist_snapshot(
                running,
                "agent.terminal",
                "cancelled during backend admission",
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        running.status = AgentStatus::Running;
        running.updated_at_ms = now_ms();
        self.persist_snapshot(running.clone(), "agent.running", "running", None, None)?;

        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&backend_kind)
            .cloned();
        let observation_authority = backend
            .as_ref()
            .is_some_and(|registered| registered.observation_authority);
        let process_exit_owner = backend
            .as_ref()
            .and_then(|registered| registered.process_exit_owner.clone());
        let claim_heartbeat = self
            .services()
            .map(|services| {
                crate::agentic::start_agentic_claim_heartbeat(Arc::downgrade(&services), &packet)
            })
            .transpose()?
            .flatten();
        let mut process_prepared = match &process_exit_owner {
            Some(adapter) => match adapter
                .prepare_packet_launch(&original_packet, packet.clone(), &selection)
                .await
            {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    let _ = adapter.take_local_exit_evidence(&packet);
                    return Err(error);
                }
            },
            None => None,
        };
        if backend.is_some() {
            exit_evidence.backend_packet_sha256 = Some(run_packet_sha256(&packet)?);
            exit_evidence.phase = LocalRunPhase::BackendPending;
            exit_evidence.process_envelope = process_prepared
                .as_ref()
                .map(|prepared| prepared.envelope().clone());
            self.persist_run_fact_until_durable(exit_evidence, "agent.backend_admitted")
                .await;
            exit_evidence.phase = LocalRunPhase::Unknown;
            // The backend is admitted: from here the exit evidence carries the
            // exact backend identity and envelope a cancellation path needs to
            // commit the same authenticated exit fact.
            retained.mirror_exit_evidence(exit_evidence);
        }
        let cancelled_before_start = self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&run_cancellation_key(&packet))
            || self
                .get(packet.agent_id())
                .is_some_and(|snapshot| snapshot.status == AgentStatus::Cancelled);
        let mut returned = if cancelled_before_start {
            drop(process_prepared.take());
            if let Some(adapter) = &process_exit_owner {
                let _ = adapter.take_local_exit_evidence(&packet);
            }
            exit_evidence.phase = LocalRunPhase::NotStarted;
            cancelled_return(&packet, "cancelled before backend invocation".into())
        } else {
            match backend {
            Some(registered) => match if let Some(prepared) = process_prepared.take() {
                process_exit_owner.as_ref().ok_or("prepared Process owner missing")?.execute_prepared(prepared).await
            } else { registered.backend.execute(packet.clone(), selection).await } {
                Ok(returned) => returned,
                Err(error) => failed_return(&packet, error),
            },
            None => blocked_return(
                &packet,
                format!(
                    "agent backend {backend_kind:?} is not installed for this RuntimeServices instance"
                ),
            ),
        }
        };
        let cancelled = self.cancellation_observed(&packet);
        if !cancelled_before_start && exit_evidence.backend_packet_sha256.is_some() {
            if backend_kind == AgentBackendKind::InProcess {
                exit_evidence.phase = retained.decide_exit_phase(if cancelled {
                    LocalRunPhase::Cancelled
                } else {
                    LocalRunPhase::LocalReturned
                });
            } else {
                let mut proposed = LocalRunPhase::Unknown;
                let scope_closed = process_exit_owner.as_ref().and_then(|adapter| {
                    adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt)
                });
                if let Some(witness) = process_exit_owner
                    .as_ref()
                    .and_then(|adapter| adapter.local_exit_evidence(&packet))
                {
                    if witness.matches_packet(&packet) {
                        if witness.is_reaped() {
                            proposed = if cancelled {
                                if scope_closed == Some(true) {
                                    LocalRunPhase::Cancelled
                                } else {
                                    LocalRunPhase::Unknown
                                }
                            } else {
                                LocalRunPhase::ProcessReaped
                            };
                        } else if witness.is_not_started() {
                            proposed = LocalRunPhase::NotStarted;
                        }
                    }
                }
                exit_evidence.phase = retained.decide_exit_phase(proposed);
                if exit_evidence.phase.positive_phase() {
                    if let Some(adapter) = &process_exit_owner {
                        let _ = adapter.take_local_exit_evidence(&packet);
                    }
                }
            }
        } else {
            exit_evidence.phase = retained.decide_exit_phase(exit_evidence.phase.clone());
        }
        drop(claim_heartbeat);
        if !observation_authority {
            // Extension/process backends may return business output, but they
            // cannot mint Runtime observation truth. Only the crate-private
            // canonical ToolHost-backed registration path has that authority.
            // Keep the same legacy-to-typed terminal bridge as the trusted
            // path: an empty typed carrier does not erase a frozen legacy
            // criterion from an older packet. Otherwise an externally
            // registered backend could turn a required acceptance contract
            // into an empty (and therefore satisfied) one merely by lacking
            // the typed field.
            let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
                &packet.required_acceptance,
                &packet.acceptance,
            );
            let receipt_snapshot =
                crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                    required,
                    Vec::new(),
                    Vec::new(),
                );
            let (observed, evaluation) =
                crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(
                    receipt_snapshot,
                );
            returned.observed_acceptance = observed;
            returned.acceptance_evaluation = Some(evaluation);
            returned.acceptance.clear();
            returned.runtime_observed_resource_scopes.clear();
        }
        // The Runtime terminal boundary canonicalizes every backend return.
        // Existing envelopes are treated as transport data only; facts are
        // retained but an untrusted backend cannot mint a verdict.
        let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
            &packet.required_acceptance,
            &packet.acceptance,
        );
        let receipt_snapshot =
            crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                required,
                returned.observed_acceptance.satisfied_criteria.clone(),
                returned.observed_acceptance.observed_evidence.clone(),
            );
        let (observed, evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(receipt_snapshot);
        returned.observed_acceptance = observed;
        returned.acceptance_evaluation = Some(evaluation);
        // A cancel/shutdown command is durable lifecycle truth. Backends may
        // observe the interruption as a transport/process error, but they may
        // not overwrite a committed cancellation with `failed` or `completed`.
        let cancellation_requested = self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&run_cancellation_key(&packet));
        if cancellation_requested
            || self
                .get(packet.agent_id())
                .is_some_and(|snapshot| snapshot.status == AgentStatus::Cancelled)
        {
            returned.status = AgentTerminalStatus::Cancelled;
            returned.outcome.clear();
            returned.failure = Some(CANCELLED_BY_COMMAND.into());
        }
        if returned.status == AgentTerminalStatus::Completed
            && backend_from_packet(&packet)? == AgentBackendKind::InProcess
            && !self
                .pending_agent_inputs(packet.agent_id(), packet.run_id())?
                .is_empty()
        {
            returned.status = AgentTerminalStatus::Failed;
            returned.failure = Some("accepted Agent inputs remain without a committed model-consumption receipt; reconcile the original run".into());
        }
        if let Err(error) = validate_agent_return(&packet, &returned) {
            let missing_acceptance = packet
                .acceptance
                .iter()
                .filter(|criterion| {
                    !returned
                        .observed_acceptance
                        .satisfied_criteria
                        .contains(criterion)
                })
                .cloned()
                .collect::<Vec<_>>();
            // A backend result that fails the Runtime contract is a terminal
            // failure, but committed effects, observed evidence, artifacts
            // and the Runtime acceptance evaluation are facts that must
            // survive the verdict. They are never rewritten to empty.
            returned.status = AgentTerminalStatus::Failed;
            returned.outcome.clear();
            let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
                &packet.required_acceptance,
                &packet.acceptance,
            );
            let receipt_snapshot =
                crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                    required,
                    returned.observed_acceptance.satisfied_criteria.clone(),
                    returned.observed_acceptance.observed_evidence.clone(),
                );
            let (observed, evaluation) =
                crate::acceptance_evaluator::AcceptanceEvaluator::framework_invalid(
                    receipt_snapshot,
                );
            returned.observed_acceptance = observed;
            returned.acceptance_evaluation = Some(evaluation);
            returned.failure = Some(format!(
                "Runtime rejected Agent terminal result: {error}; missing_acceptance={missing_acceptance:?}; runtime_change_receipts={}; observed_evidence_count={}; unresolved_obligations={:?}",
                returned.runtime_change_receipts.len(),
                returned.observed_acceptance.observed_evidence.len(),
                returned.observed_acceptance.unresolved_obligation_ids,
            ));
        }
        let mut terminal = self.get(packet.agent_id()).ok_or_else(|| {
            format!(
                "running agent projection `{}` is missing",
                packet.agent_id()
            )
        })?;
        terminal.status = terminal_status(returned.status);
        terminal.updated_at_ms = now_ms();
        terminal.failure = returned.failure.clone();
        let services = self.services();
        let path_resolver = services
            .as_ref()
            .map(|services| services.path_identity_resolver().as_ref());
        let evaluation = AgentRunEvaluation::from_terminal(
            &packet,
            &returned,
            path_resolver,
            terminal.updated_at_ms,
        );
        let refresh_canary_observation = evaluation.as_ref().is_some_and(|evaluation| {
            evaluation.release_channel == Some(harness_contract::agent::ReleaseChannel::Canary)
        });
        let started_at_ms = terminal.started_at_ms;
        let completed_at_ms = terminal.updated_at_ms;
        self.persist_snapshot_with_evaluation(
            terminal,
            "agent.terminal",
            "terminal",
            None,
            Some(returned.clone()),
            evaluation,
        )?;
        self.record_agent_outcome(&packet, &returned, started_at_ms, completed_at_ms)?;
        if refresh_canary_observation {
            if let Some(services) = self.services() {
                if let Err(error) = services.refresh_evolution_canary_observations() {
                    // The terminal run is already durably committed. Canary
                    // observation is a replayable projection and will be
                    // rebuilt before any Stable-review request, so do not
                    // turn a completed user task into a false failure here.
                    tracing::warn!(
                        agent_id = %packet.agent_id(),
                        run_id = %packet.run_id(),
                        error = %error,
                        "failed to refresh replayable Canary observation"
                    );
                }
            }
        }
        Ok(returned)
    }

    fn record_agent_outcome(
        &self,
        packet: &AgentTaskPacket,
        returned: &AgentReturnPacket,
        started_at_ms: u64,
        completed_at_ms: u64,
    ) -> Result<(), String> {
        let Some(services) = self.services() else {
            return Ok(());
        };
        let identity = &packet.assignment.execution_identity;
        let turn_id = identity
            .turn_id()
            .ok_or_else(|| "Agent outcome has no canonical turn identity".to_string())?;
        let terminal = match returned.status {
            AgentTerminalStatus::Completed => {
                harness_contract::outcome::OutcomeTerminalClass::Succeeded(returned.outcome.clone())
            }
            AgentTerminalStatus::Failed => harness_contract::outcome::OutcomeTerminalClass::Failed(
                returned
                    .failure
                    .clone()
                    .unwrap_or_else(|| "failed".to_string()),
            ),
            AgentTerminalStatus::Cancelled => {
                harness_contract::outcome::OutcomeTerminalClass::Cancelled(
                    returned
                        .failure
                        .clone()
                        .unwrap_or_else(|| "cancelled".to_string()),
                )
            }
            AgentTerminalStatus::Blocked => {
                harness_contract::outcome::OutcomeTerminalClass::PartialFailure(
                    returned.failure.clone().unwrap_or_else(|| {
                        "agent blocked; committed evidence retained".to_string()
                    }),
                )
            }
        };
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "Agent outcome has no immutable binding".to_string())?;
        let evidence_refs = returned
            .evidence_refs
            .iter()
            .map(|reference| reference.evidence_ref.clone())
            .collect::<Vec<_>>();
        let evidence_completeness = if evidence_refs.is_empty() {
            harness_contract::reality::EvidenceCompleteness::None
        } else {
            harness_contract::reality::EvidenceCompleteness::Partial
        };
        let outcome = harness_contract::outcome::ExecutionOutcome {
            identity: harness_contract::outcome::OutcomeIdentity {
                execution_id: returned.run_id.clone(),
                session_id: returned.session_id.clone(),
                turn_id: turn_id.to_string(),
                terminal_generation: u64::from(returned.attempt).saturating_add(1),
                execution_scope: harness_contract::outcome::OutcomeExecutionScope::Agent,
                paired_sample_id: None,
                task_id: Some(returned.task_id.clone()),
                mission_id: Some(returned.mission_id.clone()),
                agent_id: Some(returned.agent_id.clone()),
                team_id: returned.team_id.clone(),
                execution_graph_ref: Some(returned.graph_id.clone()),
            },
            runtime: harness_contract::outcome::RuntimeIdentity {
                workspace_key: identity.workspace_id().to_string(),
                runtime_revision: env!("CARGO_PKG_VERSION").to_string(),
                config_revision: format!("agent-binding:{}", binding.binding_digest),
                build: Default::default(),
            },
            provider: (!returned.provider.is_empty() || !returned.model.is_empty()).then(|| {
                harness_contract::outcome::ProviderIdentity {
                    registry_revision: None,
                    provider_name: returned.provider.clone(),
                    model: returned.model.clone(),
                    profile: None,
                    protocol: None,
                    capabilities: std::collections::BTreeMap::new(),
                }
            }),
            strategy: harness_contract::outcome::StrategyIdentity {
                decision_id: binding.binding_digest.clone(),
                policy_revision: format!(
                    "agent-definition:{}@{}",
                    binding.definition_ref.definition_id.as_str(),
                    binding.definition_ref.revision
                ),
                decision_source: "runtime.agent_binding".to_string(),
                selected_candidate: if returned.team_id.is_some() {
                    harness_contract::strategy::ExecutionCandidateKind::Team
                } else {
                    harness_contract::strategy::ExecutionCandidateKind::Direct
                },
                selected_pattern: "agent".to_string(),
            },
            timing: harness_contract::outcome::OutcomeTiming {
                started_at_ms,
                completed_at_ms,
                duration_ms: completed_at_ms.saturating_sub(started_at_ms),
            },
            usage: harness_contract::outcome::OutcomeUsage {
                input_tokens: Some(returned.input_tokens),
                output_tokens: Some(returned.output_tokens),
                cached_tokens: Some(returned.cached_tokens),
                evaluation_tokens: None,
                tool_calls: returned.tool_calls,
                duplicate_tool_calls: returned.duplicate_tool_calls,
                retries: u64::from(returned.attempt),
                max_observed_concurrency: returned.max_tool_concurrency_observed.max(1),
            },
            terminal,
            quality: harness_contract::outcome::OutcomeQuality::Unknown,
            observation: harness_contract::outcome::OutcomeObservation {
                source: "runtime.agent_terminal".to_string(),
                observed_at_ms: completed_at_ms,
                freshness_ms: 0,
            },
            strategy_feedback: harness_contract::outcome::OutcomeStrategyFeedback {
                evaluation_environment: if returned.session_id.starts_with("evolution-eval:") {
                    "evolution_evaluation".to_string()
                } else {
                    "production".to_string()
                },
                ..Default::default()
            },
            evidence_refs,
            evidence_completeness,
            schema_revision: harness_contract::outcome::OUTCOME_SCHEMA_REVISION,
        };
        services.outcome_service().record_terminal(&outcome)?;
        Ok(())
    }

    /// Immutable per-run evidence, written only with a terminal lifecycle
    /// event. This projection deliberately groups by Definition revision and
    /// environment instead of mutable instance reputation.
    #[must_use]
    pub fn evaluations(&self) -> Vec<AgentRunEvaluation> {
        let Ok(events) = self
            .event_store
            .replay_scope_kind(RuntimeEventScope::Evolution, "agent.run_evaluated")
        else {
            return Vec::new();
        };
        let mut evaluations = events
            .into_iter()
            .filter_map(|event| event.payload.get("evaluation").cloned())
            .filter_map(|value| serde_json::from_value::<AgentRunEvaluation>(value).ok())
            .collect::<Vec<_>>();
        evaluations.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.evaluation_id.cmp(&right.evaluation_id))
        });
        evaluations
    }

    #[must_use]
    pub fn self_models(&self) -> Vec<AgentSelfModel> {
        project_self_models(self.evaluations())
    }

    /// Persist a bounded lifecycle progress marker for an active Agent.
    ///
    /// Provider deltas are intentionally not written one-by-one. Backends use
    /// this for state transitions such as execution admission and first model
    /// output, which makes a live team observable without turning telemetry
    /// into transcript or event-store noise.
    pub fn record_progress(&self, agent_id: &str, kind: &str, message: &str) -> Result<(), String> {
        let Some(mut snapshot) = self.get(agent_id) else {
            return Err(format!("agent {agent_id} does not exist"));
        };
        if snapshot.status.is_terminal() {
            return Ok(());
        }
        snapshot.updated_at_ms = now_ms();
        self.persist_snapshot(snapshot, kind, message, None, None)
            .map(|_| ())
    }

    /// Verify the immutable Runtime Binding persisted with an AgentTask node.
    /// New executable packets are compiled before graph registration; Runtime
    /// intentionally rejects an unbound payload rather than selecting a
    /// default Definition during execution or recovery.
    fn ensure_runtime_binding(
        &self,
        mut packet: AgentTaskPacket,
    ) -> Result<AgentTaskPacket, String> {
        packet
            .assignment
            .validate()
            .map_err(|error| error.to_string())?;
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "unbound AgentTaskPacket is not executable; compile AgentTaskIntent before graph registration"
                .to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet.agent_id() != binding.instance.instance_id {
            return Err(
                "AgentTaskPacket agent_id must equal its Binding instance identity".to_string(),
            );
        }
        if binding.data_lease.session_id != packet.session_id()
            || binding.data_lease.task_id != packet.task_id()
            || binding.data_lease.team_id.as_deref() != packet.team_id()
        {
            return Err("AgentTaskPacket binding data lease does not match task identity".into());
        }
        if let Some(services) = self.services() {
            services
                .validate_agent_binding_release(binding)
                .map_err(|error| {
                    format!("AgentTaskPacket original Binding authority is not runnable: {error}")
                })?;
            let resolved = if binding.evaluation.is_some() {
                services
                    .validate_agent_evaluation_binding(binding)
                    .map_err(|error| {
                        format!("AgentTaskPacket evaluation Binding is not runnable: {error}")
                    })?;
                services
                    .definition_registry()
                    .resolve_agent_canary(&binding.definition_ref)
                    .map_err(|error| {
                        format!(
                            "AgentTaskPacket evaluation Binding cannot resolve its candidate revision: {error}"
                        )
                    })?
            } else if binding.release.as_ref().is_some_and(|release| {
                release.channel == harness_contract::agent::ReleaseChannel::Canary
            }) {
                services
                    .definition_registry()
                    .resolve_agent_canary(&binding.definition_ref)
                    .map_err(|error| {
                        format!(
                            "AgentTaskPacket Canary Binding cannot resolve its revision: {error}"
                        )
                    })?
            } else {
                services
                    .definition_registry()
                    .resolve_agent(
                        &binding.definition_ref.definition_id,
                        RevisionSelector::ExactApprovedRevision {
                            revision: binding.definition_ref.revision,
                        },
                    )
                    .map_err(|error| format!("AgentTaskPacket Binding is not runnable: {error}"))?
            };
            if resolved.revision.content_digest != binding.definition_digest
                || resolved.agent_markdown != binding.instructions
            {
                return Err(
                    "AgentTaskPacket Binding content does not match the approved Definition"
                        .to_string(),
                );
            }
            verify_binding_against_definition(binding, &resolved.revision.manifest)?;
            let session_id = packet.session_id().to_string();
            if let Some(agentic) = packet.agentic_binding.as_mut() {
                let task_ref = match &agentic.focus {
                    harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                    | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
                        Some(task_ref)
                    }
                    _ => None,
                };
                if let Some(task_ref) = task_ref {
                    let program = services
                        .agent_action_service()
                        .project(&agentic.program_id)
                        .map_err(|error| {
                            format!("AgentTaskPacket purpose source unavailable: {error}")
                        })?;
                    if program.session_id != session_id {
                        return Err(
                            "AgentTaskPacket purpose source belongs to another Session".into()
                        );
                    }
                    let task = program
                        .tasks
                        .get(task_ref)
                        .ok_or("AgentTaskPacket purpose source Task missing")?;
                    if agentic
                        .task_purpose
                        .is_some_and(|purpose| purpose != task.purpose)
                    {
                        return Err(
                            "AgentTaskPacket purpose differs from its immutable Task".into()
                        );
                    }
                    agentic.task_purpose = Some(task.purpose);
                }
            }
        }
        Ok(packet)
    }

    /// Derive bounded peer context from the canonical graph projection before a
    /// dependent AgentTask starts. Graph edges remain the scheduling truth;
    /// this method only makes already-committed predecessor results visible to
    /// the next Agent. It has no graph mutation authority and is shared by
    /// protocol and generic Team graphs.
    async fn attach_predecessor_context(
        &self,
        mut packet: AgentTaskPacket,
    ) -> Result<AgentTaskPacket, String> {
        let Some(services) = self.services() else {
            return Ok(packet);
        };
        let Ok(graph) = services
            .graph_state_store()
            .load_async(packet.graph_id().to_string())
            .await
        else {
            // Isolated AgentRuntime tests and external adapters may execute
            // standalone packets. They have no graph peer context to attach.
            return Ok(packet);
        };
        let mut predecessor_ids = BTreeSet::new();
        let predecessors = graph
            .edges
            .iter()
            .filter(|edge| {
                edge.to == packet.node_id()
                    && matches!(
                        edge.kind,
                        ExecutionEdgeKind::DependsOn
                            | ExecutionEdgeKind::Produces
                            | ExecutionEdgeKind::Verifies
                    )
            })
            .filter_map(|edge| {
                graph
                    .nodes
                    .iter()
                    .find(|node| node.id == edge.from && node.kind == ExecutionNodeKind::AgentTask)
            })
            .filter(|node| predecessor_ids.insert(node.id.clone()))
            .collect::<Vec<_>>();
        if predecessors.is_empty() {
            return Ok(packet);
        }

        let mut sections = Vec::new();
        for predecessor in predecessors {
            let predecessor_packet: AgentTaskPacket =
                serde_json::from_str(&predecessor.payload_ref).map_err(|error| {
                    format!(
                        "predecessor AgentTask packet {} is invalid: {error}",
                        predecessor.id
                    )
                })?;
            let result = graph.node_results.get(&predecessor.id).ok_or_else(|| {
                format!(
                    "completed predecessor {} has no durable graph result",
                    predecessor.id
                )
            })?;
            let role = predecessor_packet.assignment.role_id.as_str();
            let upstream_outcome = if result.status == ExecutionNodeStatus::Completed {
                result.summary.clone().unwrap_or_else(|| {
                    format!("completed upstream result {}", predecessor_packet.run_id())
                })
            } else {
                format!(
                    "UNRESOLVED: upstream role did not complete: {}",
                    result
                        .failure
                        .as_ref()
                        .map(|failure| failure.message.clone())
                        .unwrap_or_else(|| "no durable terminal summary".to_string())
                )
            };
            sections.push(canonical_predecessor_section(role, &upstream_outcome));
        }
        if !sections.is_empty() {
            packet.objective.push_str(
                "\n\n## Canonical upstream results\nUse completed peer results as evidence. Entries marked UNRESOLVED are failed or blocked peer lanes, not evidence; preserve them as explicit gaps rather than treating them as facts. Reconcile contradictions explicitly.\n\n",
            );
            packet.objective.push_str(&sections.join("\n\n"));
        }
        Ok(packet)
    }

    pub async fn command(&self, request: AgentCommandRequest) -> AgentCommandReceipt {
        if request.command == AgentCommand::SendInput {
            return self.command_durable_input(request).await;
        }
        if let Some(receipt) = self.reject_reserved_input_command_id(&request) {
            return receipt;
        }
        if let Some(receipt) = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&request.agent_id)
            .and_then(|record| record.receipts.get(&request.command_id).cloned())
        {
            return receipt;
        }
        let Some(snapshot) = self.get(&request.agent_id) else {
            return self.reject_command(
                request,
                AgentStatus::Blocked,
                AgentCommandRejectReason::NotFound,
                "agent not found",
            );
        };
        if snapshot.revision != request.expected_revision {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::StaleRevision,
                "agent revision does not match",
            );
        }
        if snapshot.status.is_terminal() {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::Terminal,
                "agent is terminal",
            );
        }
        if matches!(request.command, AgentCommand::SendInput) && request.input.is_none() {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                "send_input requires input",
            );
        }
        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&snapshot.backend)
            .cloned();
        let Some(backend) = backend else {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::UnsupportedByBackend,
                "agent backend is unavailable",
            );
        };
        // The revision a cancellation was accepted at, when this command was a
        // cancellation. Only a durable revision may be named by a receipt.
        let mut cancellation_revision: Option<u64> = None;
        if matches!(
            request.command,
            AgentCommand::Cancel | AgentCommand::Shutdown
        ) {
            // A cancellation is durable before the child is torn down. The
            // caller that observes the death of the worker must already see
            // the run as cancelled: the cancellation is the Runtime's own
            // decision, the death of the process is only its consequence.
            // The physical exit stays a separate fact, proven by whoever can
            // prove it.
            let mut cancelling = snapshot.clone();
            cancelling.status = AgentStatus::Cancelled;
            cancelling.failure = Some(CANCELLED_BY_COMMAND.into());
            cancelling.updated_at_ms = now_ms();
            // A cancellation is accepted only when its own snapshot is durable.
            // The failure is never swallowed: when the snapshot cannot be
            // persisted the worker is not torn down and the caller is told
            // that the cancellation was not accepted.
            let cancellation = match self.persist_snapshot(
                cancelling.clone(),
                "agent.cancellation_requested",
                "cancellation accepted",
                Some(AgentCommandReceipt {
                    command_id: request.command_id.clone(),
                    agent_id: request.agent_id.clone(),
                    accepted_revision: 0,
                    status: AgentStatus::Cancelled,
                    accepted: true,
                    reject_reason: None,
                    message: "cancellation accepted".into(),
                }),
                None,
            ) {
                Ok(receipt) => receipt,
                Err(error) => {
                // The run may have ended while the cancellation was being
                // accepted. A cancellation of a run that already reached a
                // terminal state is not a rejection of the caller's intent:
                // the run is gone either way, and the family converges.
                let current = self.current_run_snapshot(&snapshot.agent_id);
                let (status, reason) = match current
                    .as_ref()
                    .filter(|current| current.run_id == snapshot.run_id)
                    .filter(|current| current.status.is_terminal())
                {
                    Some(current) => (
                        current.status,
                        AgentCommandRejectReason::Terminal,
                    ),
                    None => (
                        snapshot.status,
                        if error.contains("stale") {
                            AgentCommandRejectReason::StaleRevision
                        } else {
                            AgentCommandRejectReason::InvalidInput
                        },
                    ),
                };
                return self.reject_command(
                    request,
                    status,
                    reason,
                    &format!("agent cancellation was not accepted: {error}"),
                );
                }
            };
            // The revision the cancellation was accepted at is the one that is
            // durable: the commit service wrote it with the snapshot.
            cancellation_revision = Some(cancellation.accepted_revision);
            // The authenticated exit of the cancelled run is published next to
            // the kill, not after it: the owner publishes when it returns, and
            // the cancelling authority covers the case where the owner can no
            // longer publish one. Both write the same fact under the same
            // single-writer claim, and only a proven exit may claim it.
            let handle = cancelling.handle();
            let (command, ()) = tokio::join!(
                backend.backend.command(&handle, &request),
                self.commit_cancelled_exit(&cancelling),
            );
            if let Err(reason) = command {
                return self.reject_command(
                    request,
                    snapshot.status,
                    reason,
                    "agent backend rejected command",
                );
            }
        } else if let Err(reason) = backend.backend.command(&snapshot.handle(), &request).await {
            return self.reject_command(
                request,
                snapshot.status,
                reason,
                "agent backend rejected command",
            );
        }
        // The trailing receipt is written against the latest legal revision of
        // this run, not against the snapshot the command started from: the
        // cancellation, the exit fact and the agent's own return may all have
        // moved the run forward while the command was in flight. Reusing the
        // stale snapshot would make a legal cancellation look like a rejected
        // one.
        let cancellation_accepted = matches!(
            request.command,
            AgentCommand::Cancel | AgentCommand::Shutdown
        );
        let mut updated = self
            .current_run_snapshot(&snapshot.agent_id)
            .filter(|current| current.run_id == snapshot.run_id)
            .unwrap_or_else(|| snapshot.clone());
        if !updated.status.is_terminal() {
            updated.status = match request.command {
                AgentCommand::Pause => AgentStatus::Paused,
                AgentCommand::Resume => AgentStatus::Running,
                AgentCommand::Cancel | AgentCommand::Shutdown => AgentStatus::Cancelled,
                AgentCommand::SendInput | AgentCommand::Interrupt => updated.status,
            };
        }
        updated.updated_at_ms = now_ms();
        if cancellation_accepted {
            // A cancellation must leave one authenticated exit fact even when
            // the run owner can no longer reach its own publication point.
            self.commit_cancelled_exit(&updated).await;
        }
        let durable_revision = if cancellation_accepted {
            cancellation_revision.unwrap_or_else(|| updated.revision.saturating_add(1))
        } else {
            updated.revision.saturating_add(1)
        };
        let receipt = AgentCommandReceipt {
            command_id: request.command_id,
            agent_id: updated.agent_id.clone(),
            accepted_revision: durable_revision,
            status: updated.status,
            accepted: true,
            reject_reason: None,
            message: "command accepted".into(),
        };
        self.persist_snapshot(
            updated,
            "agent.command",
            "command accepted",
            Some(receipt.clone()),
            None,
        )
        .unwrap_or_else(|error| {
            if cancellation_accepted {
                // The cancellation itself is durable; only the trailing command
                // record failed. The receipt still reports the accepted
                // cancellation and names the trailing failure instead of
                // pretending the cancellation was never accepted.
                AgentCommandReceipt {
                    accepted: true,
                    // The receipt names the revision that is durable (the
                    // cancellation), never a revision that was never written.
                    accepted_revision: durable_revision,
                    message: format!(
                        "agent cancellation accepted; the trailing command record failed: {error}"
                    ),
                    ..receipt
                }
            } else {
                AgentCommandReceipt {
                    accepted: false,
                    reject_reason: Some(AgentCommandRejectReason::InvalidInput),
                    message: error,
                    ..receipt
                }
            }
        })
    }

    /// The latest durable snapshot of a run, or `None` when the agent owns no
    /// run. The trailing command receipt is written against this snapshot so
    /// it never reuses a revision that the run has already left behind.
    fn current_run_snapshot(&self, agent_id: &str) -> Option<AgentRunSnapshot> {
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .and_then(|record| record.snapshot.clone())
    }

    /// Commit the authenticated exit fact of a cancelled run.
    ///
    /// The run owner publishes `agent.local_exit` when it returns from the
    /// backend. A cancelled worker can leave that owner waiting on a physical
    /// teardown that never finishes, and the lifecycle then never closes: the
    /// run is reported as "unclosed" instead of "cancelled". Publication still
    /// requires the exact backend's completed local-exit witness and a live
    /// observation that its owned group is empty. Reaping the OS leader alone
    /// does not prove that the local protocol driver has finished.
    async fn commit_cancelled_exit(&self, snapshot: &AgentRunSnapshot) {
        let Some(retained) = self.retained_run_for(snapshot) else {
            return;
        };
        if retained.exit_publication_claimed() {
            return;
        }
        let adapter = self
            .process_jsonl_adapter
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let deadline = tokio::time::Instant::now() + CANCELLED_EXIT_BOUND;
        loop {
            // The run owner owns the exit fact whenever it can publish it.
            if retained.exit_publication_claimed() {
                return;
            }
            let scope_closed = adapter
                .as_ref()
                .and_then(|adapter| adapter.owned_process_group_is_empty(&snapshot.run_id, snapshot.attempt))
                == Some(true);
            let witnessed = retained
                .mirrored_exit_evidence()
                .filter(|evidence| {
                    evidence.packet_sha256 == retained.packet_sha256
                        && evidence.agent_id == snapshot.agent_id
                        && evidence.run_id == snapshot.run_id
                        && evidence.graph_id == snapshot.graph_id
                        && evidence.node_id == snapshot.node_id
                        && evidence.attempt == snapshot.attempt
                })
                .and_then(|evidence| evidence.backend_packet_sha256)
                .and_then(|backend_digest| {
                    adapter.as_ref().and_then(|adapter| {
                        adapter.local_exit_evidence_for_run(
                            &snapshot.run_id,
                            snapshot.attempt,
                            &backend_digest,
                        )
                    })
                })
                .is_some_and(|witness| witness.is_reaped());
            if scope_closed && witnessed {
                self.reclaim_cancelled_run_scope(snapshot).await;
                self.publish_cancelled_exit(&retained, LocalRunPhase::Cancelled)
                    .await;
                return;
            }
            // The run owner ended without publishing its exit: it can no
            // longer commit one. An unproven physical exit must stay
            // unproven: publishing an unknown exit fact here would fix the
            // run as unproven for good and would block the one writer that
            // could still prove it.
            if retained.owner_ended().await {
                tracing::warn!(
                    run_id = %snapshot.run_id,
                    "cancelled agent run ended without a proven physical exit; the exit stays unproven"
                );
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                // The run owner is still alive and may still prove the exit.
                // It stays the single authority for this fact.
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// Close the private physical scope of a cancelled run.
    ///
    /// The scope belongs to the run, not to the side that observes its exit.
    /// The cancelling authority closes it by identity, so the scope is closed
    /// even when the run owner already reaped the process and never reached
    /// its own cleanup. A failure to close the scope never rewrites the exit
    /// fact: the exit stays proven and the scope stays honestly unfinished.
    async fn reclaim_cancelled_run_scope(&self, snapshot: &AgentRunSnapshot) {
        let adapter = self
            .process_jsonl_adapter
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(adapter) = adapter else {
            return;
        };
        if let Err(error) = adapter.reclaim_run_scope(&snapshot.run_id, snapshot.attempt) {
            tracing::warn!(
                run_id = %snapshot.run_id,
                attempt = snapshot.attempt,
                %error,
                "cancelled agent run kept its physical scope open"
            );
        }
    }

    /// Publish the cancelled exit of a run through the authenticated channel.
    async fn publish_cancelled_exit(&self, retained: &RetainedAgentRun, phase: LocalRunPhase) {
        if !phase.positive_phase() {
            // An unproven exit is never committed.
            return;
        }
        let Some(mut evidence) = retained.mirrored_exit_evidence() else {
            // The run never reached an authenticated admission: the run owner
            // owns the "never started" exit fact.
            return;
        };
        evidence.phase = retained.decide_exit_phase(phase);
        if !evidence.phase.positive_phase() {
            return;
        }
        self.publish_run_exit(retained, &evidence).await;
    }

    /// The retained run that owns this exact run identity, if any.
    fn retained_run_for(&self, snapshot: &AgentRunSnapshot) -> Option<Arc<RetainedAgentRun>> {
        let key = (
            snapshot.run_id.clone(),
            snapshot.graph_id.clone(),
            snapshot.node_id.clone(),
            snapshot.attempt,
        );
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&snapshot.agent_id)
            .and_then(|record| {
                record
                    .retained
                    .iter()
                    .chain(record.retiring.iter())
                    .find(|run| run.run_key == key)
                    .cloned()
            })
    }

    fn reject_command(
        &self,
        request: AgentCommandRequest,
        status: AgentStatus,
        reason: AgentCommandRejectReason,
        message: &str,
    ) -> AgentCommandReceipt {
        let receipt = AgentCommandReceipt {
            command_id: request.command_id,
            agent_id: request.agent_id.clone(),
            accepted_revision: request.expected_revision,
            status,
            accepted: false,
            reject_reason: Some(reason),
            message: message.into(),
        };
        if let Some(snapshot) = self.get(&request.agent_id) {
            let _ = self.persist_snapshot(
                snapshot,
                "agent.command_rejected",
                message,
                Some(receipt.clone()),
                None,
            );
        }
        receipt
    }

    fn persist_snapshot(
        &self,
        snapshot: AgentRunSnapshot,
        kind: &str,
        message: &str,
        receipt: Option<AgentCommandReceipt>,
        returned: Option<AgentReturnPacket>,
    ) -> Result<AgentCommandReceipt, String> {
        self.persist_snapshot_with_evaluation(snapshot, kind, message, receipt, returned, None)
    }

    fn persist_snapshot_with_evaluation(
        &self,
        snapshot: AgentRunSnapshot,
        kind: &str,
        message: &str,
        receipt: Option<AgentCommandReceipt>,
        returned: Option<AgentReturnPacket>,
        evaluation: Option<AgentRunEvaluation>,
    ) -> Result<AgentCommandReceipt, String> {
        self.persist_snapshot_with_input(
            snapshot, kind, message, receipt, returned, evaluation, None,
        )
    }

    fn persist_snapshot_with_input(
        &self,
        mut snapshot: AgentRunSnapshot,
        kind: &str,
        message: &str,
        mut receipt: Option<AgentCommandReceipt>,
        returned: Option<AgentReturnPacket>,
        evaluation: Option<AgentRunEvaluation>,
        input_event: Option<(
            crate::runtime_event_store::RuntimeTransactionEventInput,
            u64,
        )>,
    ) -> Result<AgentCommandReceipt, String> {
        validate_snapshot_identity(&snapshot)?;
        let lifecycle_lock = self.lifecycle_lock(&snapshot.agent_id);
        let _lifecycle_guard = lifecycle_lock
            .lock()
            .map_err(|_| "AgentRuntime lifecycle lock poisoned".to_string())?;
        if snapshot.status == AgentStatus::Completed
            && snapshot.backend == AgentBackendKind::InProcess
            && !self
                .pending_agent_inputs(&snapshot.agent_id, &snapshot.run_id)?
                .is_empty()
        {
            return Err("Agent success commit is fenced by an accepted, unconsumed input; original run reconciliation required".into());
        }
        let current = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&snapshot.agent_id)
            .and_then(|record| record.snapshot.clone());
        match current {
            Some(current) if current.run_id == snapshot.run_id => {
                if current.execution_identity != snapshot.execution_identity {
                    return Err(format!(
                        "Agent {} attempted to change immutable execution identity",
                        snapshot.agent_id
                    ));
                }
                if current.status.is_terminal() && !snapshot.status.is_terminal() {
                    return Err(format!(
                        "Agent lifecycle cannot regress terminal {:?} to {:?}",
                        current.status, snapshot.status
                    ));
                }
                if snapshot.revision != current.revision {
                    return Err(format!(
                        "Agent lifecycle transition is stale for {}: expected revision {}, actual {}",
                        snapshot.agent_id, snapshot.revision, current.revision
                    ));
                }
            }
            Some(current) if !current.status.is_terminal() => {
                return Err(format!(
                    "Agent {} already owns active run {}",
                    snapshot.agent_id, current.run_id
                ));
            }
            Some(_) | None if snapshot.revision != 0 => {
                return Err(format!(
                    "Agent lifecycle initial transition for {} must start at revision 0",
                    snapshot.agent_id
                ));
            }
            Some(_) | None => {}
        }
        let stream_id = agent_stream_id(&snapshot.agent_id);
        snapshot.revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?
            .saturating_add(1);
        if let Some(receipt) = receipt.as_mut().filter(|receipt| receipt.accepted) {
            receipt.accepted_revision = snapshot.revision;
        }
        let payload = PersistedAgentEvent {
            snapshot: snapshot.clone(),
            receipt: receipt.clone(),
            returned: returned.clone(),
        };
        let candidate_event = returned
            .as_ref()
            .and_then(|returned| {
                crate::knowledge_candidate_projector::agent_terminal_candidate(&snapshot, returned)
            })
            .map(crate::knowledge_candidate_projector::candidate_proposal_event)
            .transpose()?;
        let activity_generation = self
            .services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
            .and_then(|services| {
                services
                    .graph_state_store()
                    .projection(&snapshot.graph_id)
                    .ok()
            })
            .and_then(|projection| projection.lineage)
            .map_or_else(
                || u64::from(snapshot.attempt.max(1)),
                |lineage| lineage.generation,
            );
        let agent_event = RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: RuntimeEventScope::Agent,
            kind: kind.into(),
            status: Some(message.into()),
            actor: Some("agent_runtime".into()),
            refs: snapshot_identity_refs(&snapshot),
            payload: serde_json::to_value(payload).map_err(|error| error.to_string())?,
        }
        .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
            root_execution_id: snapshot.graph_id.clone(),
            session_id: snapshot.session_id.clone(),
            turn_id: snapshot
                .execution_identity
                .turn_id()
                .unwrap_or(snapshot.run_id.as_str())
                .to_string(),
            root_task_id: snapshot.root_task_id.clone(),
            task_id: snapshot.task_id.clone(),
            activity_id: format!(
                "activity:execution:{}:node:{}",
                snapshot.graph_id, snapshot.node_id
            ),
            node_id: Some(snapshot.node_id.clone()),
            parent_activity_id: Some(format!("activity:execution:{}", snapshot.graph_id)),
            initiator_activity_id: Some(format!("activity:execution:{}", snapshot.graph_id)),
            team_run_id: snapshot.execution_identity.team_run_id().map(str::to_owned),
            agent_instance_id: Some(snapshot.agent_id.clone()),
            agent_run_id: Some(snapshot.run_id.clone()),
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            parallel_group_id: None,
            revision: snapshot.revision.max(1),
            fence: snapshot.expected_graph_revision.max(1),
            generation: activity_generation,
        })
        .map_err(|error| error.to_string())?;
        if let Some((input_event, expected_revision)) = input_event {
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "agent-input:{}:{}:{}",
                        snapshot.run_id, snapshot.revision, input_event.event.kind
                    ),
                    expected_streams: vec![
                        ExpectedStreamRevision {
                            stream_id: stream_id.clone(),
                            expected_revision: snapshot.revision.saturating_sub(1),
                        },
                        ExpectedStreamRevision {
                            stream_id: input_event.event.stream_id.clone(),
                            expected_revision,
                        },
                    ],
                    events: vec![agent_event.into(), input_event],
                })
                .map_err(|error| error.to_string())?;
        } else if let Some(evaluation) = evaluation {
            let evaluation_stream = agent_evaluation_stream(&evaluation.run_id);
            let evaluation_revision = self
                .event_store
                .stream_revision(&evaluation_stream)
                .map_err(|error| error.to_string())?;
            let mut expected_streams = vec![
                ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: snapshot.revision.saturating_sub(1),
                },
                ExpectedStreamRevision {
                    stream_id: evaluation_stream.clone(),
                    expected_revision: evaluation_revision,
                },
            ];
            let mut events = vec![
                agent_event.into(),
                RuntimeEventInput {
                    stream_id: evaluation_stream,
                    scope: RuntimeEventScope::Evolution,
                    kind: "agent.run_evaluated".to_string(),
                    status: Some(if evaluation.is_success() {
                        "completed".to_string()
                    } else {
                        "failed".to_string()
                    }),
                    actor: Some("runtime.agent_evaluation".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "run".to_string(),
                            id: evaluation.run_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "agent_definition".to_string(),
                            id: format!(
                                "{}@{}",
                                evaluation.definition_id, evaluation.definition_revision
                            ),
                        },
                        RuntimeEventRef {
                            kind: "binding".to_string(),
                            id: evaluation.binding_digest.clone(),
                        },
                    ],
                    payload: serde_json::json!({"evaluation": evaluation}),
                }
                .into(),
            ];
            if let Some(candidate_event) = candidate_event {
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: candidate_event.event.stream_id.clone(),
                    expected_revision: self
                        .event_store
                        .stream_revision(&candidate_event.event.stream_id)
                        .map_err(|error| error.to_string())?,
                });
                events.push(candidate_event);
            }
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "agent-terminal-evaluation:{}:{}",
                        snapshot.run_id, evaluation.evaluation_id
                    ),
                    expected_streams,
                    events,
                })
                .map_err(|error| error.to_string())?;
        } else if let Some(candidate_event) = candidate_event {
            let candidate_stream = candidate_event.event.stream_id.clone();
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "agent-terminal-knowledge:{}:{}",
                        snapshot.run_id, snapshot.revision
                    ),
                    expected_streams: vec![
                        ExpectedStreamRevision {
                            stream_id: stream_id.clone(),
                            expected_revision: snapshot.revision.saturating_sub(1),
                        },
                        ExpectedStreamRevision {
                            stream_id: candidate_stream.clone(),
                            expected_revision: self
                                .event_store
                                .stream_revision(&candidate_stream)
                                .map_err(|error| error.to_string())?,
                        },
                    ],
                    events: vec![agent_event.into(), candidate_event],
                })
                .map_err(|error| error.to_string())?;
        } else {
            // A controlled failure of the cancellation snapshot proves that a
            // cancellation which cannot be persisted is never accepted.
            #[cfg(test)]
            if kind == "agent.command"
                && self
                    .command_append_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(
                    "test injected original Agent trailing command record append failure".into(),
                );
            }
            #[cfg(test)]
            if kind == "agent.cancellation_requested"
                && self
                    .cancellation_append_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                self.cancellation_append_failed.notify_one();
                return Err(
                    "test injected original Agent cancellation snapshot append failure".into(),
                );
            }
            self.event_store
                .append(agent_event)
                .map_err(|error| error.to_string())?;
        }
        self.publish_live_lifecycle(&snapshot, kind, message, returned.as_ref());
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = records.entry(snapshot.agent_id.clone()).or_default();
        let previous_graph_id = record
            .snapshot
            .as_ref()
            .map(|previous| previous.graph_id.clone());
        record.snapshot = Some(snapshot.clone());
        if returned.is_some() {
            record.returned = returned;
        }
        if let Some(receipt) = receipt {
            record
                .receipts
                .insert(receipt.command_id.clone(), receipt.clone());
            drop(records);
            self.update_graph_index(
                &snapshot.agent_id,
                previous_graph_id.as_deref(),
                &snapshot.graph_id,
            );
            return Ok(receipt);
        }
        drop(records);
        self.update_graph_index(
            &snapshot.agent_id,
            previous_graph_id.as_deref(),
            &snapshot.graph_id,
        );
        Ok(AgentCommandReceipt {
            command_id: format!("event:{}:{}", snapshot.agent_id, snapshot.revision),
            agent_id: snapshot.agent_id,
            accepted_revision: snapshot.revision,
            status: snapshot.status,
            accepted: true,
            reject_reason: None,
            message: message.into(),
        })
    }

    fn publish_live_lifecycle(
        &self,
        snapshot: &AgentRunSnapshot,
        kind: &str,
        message: &str,
        returned: Option<&AgentReturnPacket>,
    ) {
        let Some((phase, status)) = live_lifecycle_phase(kind, snapshot.status) else {
            return;
        };
        let Some(services) = self.services() else {
            return;
        };
        let Some((root_execution_id, parent_bus)) =
            services.resolve_active_execution_bus(&snapshot.graph_id)
        else {
            return;
        };
        let identity = &snapshot.execution_identity;
        let summary = returned
            .and_then(|returned| {
                returned
                    .failure
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| {
                        (!returned.outcome.trim().is_empty()).then_some(returned.outcome.as_str())
                    })
            })
            .or_else(|| (!message.trim().is_empty() && message != status).then_some(message))
            .map(|value| bounded_lifecycle_summary(value, 240));
        parent_bus.emit(CowdEvent::RelatedExecution {
            lineage: CowdExecutionLineage {
                parent_execution_id: root_execution_id,
                graph_id: snapshot.graph_id.clone(),
                node_id: snapshot.node_id.clone(),
                team_id: identity.team_run_id().map(str::to_owned),
                agent_id: Some(snapshot.agent_id.clone()),
            },
            event: Box::new(CowdEvent::ExecutionScoped {
                context: CowdExecutionContext {
                    execution_id: snapshot.run_id.clone(),
                    session_id: snapshot.session_id.clone(),
                    turn_id: identity.turn_id().unwrap_or(&snapshot.run_id).to_string(),
                },
                activity_binding: None,
                event: Box::new(CowdEvent::AgentLifecycle {
                    run_id: snapshot.run_id.clone(),
                    agent_id: snapshot.agent_id.clone(),
                    role: snapshot
                        .binding
                        .as_ref()
                        .and_then(|binding| binding.instance.role_slot_id.clone()),
                    phase,
                    status: status.to_string(),
                    summary,
                }),
            }),
        });
    }

    fn restore_projection(&self) {
        const RESTORE_PAGE_SIZE: usize = 512;
        let mut after_position = None;
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let Ok(events) = self.event_store.list_scope_page_asc(
                RuntimeEventScope::Agent,
                after_position,
                RESTORE_PAGE_SIZE,
            ) else {
                return;
            };
            if events.is_empty() {
                break;
            }
            let page_is_complete = events.len() < RESTORE_PAGE_SIZE;
            after_position = events
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            for event in events {
                if event.payload.get("snapshot").is_none() {
                    if let Some(agent_id) = event.stream_id.strip_prefix("agent:") {
                        if let Some(snapshot) = records
                            .get_mut(agent_id)
                            .and_then(|record| record.snapshot.as_mut())
                        {
                            snapshot.revision = snapshot.revision.max(event.sequence);
                        }
                    }
                    continue;
                }
                let Ok(payload) = serde_json::from_value::<PersistedAgentEvent>(event.payload)
                else {
                    if let Some(agent_id) = event.stream_id.strip_prefix("agent:") {
                        records.remove(agent_id);
                    }
                    continue;
                };
                if payload.snapshot.status.is_terminal() {
                    // Terminal history remains in the event store and is loaded
                    // only by exact lookup. Keeping it in the active projection
                    // makes restart memory proportional to all historical Agents.
                    records.remove(&payload.snapshot.agent_id);
                    continue;
                }
                let record = records
                    .entry(payload.snapshot.agent_id.clone())
                    .or_default();
                record.snapshot = Some(payload.snapshot);
                if let Some(receipt) = payload.receipt {
                    record.receipts.insert(receipt.command_id.clone(), receipt);
                }
                if payload.returned.is_some() {
                    record.returned = payload.returned;
                }
            }
            if page_is_complete {
                break;
            }
        }
        let index = records
            .iter()
            .filter_map(|(agent_id, record)| {
                record
                    .snapshot
                    .as_ref()
                    .map(|snapshot| (agent_id.clone(), snapshot.graph_id.clone()))
            })
            .fold(
                BTreeMap::<String, BTreeSet<String>>::new(),
                |mut index, (agent_id, graph_id)| {
                    index.entry(graph_id).or_default().insert(agent_id);
                    index
                },
            );
        drop(records);
        *self
            .graph_agent_ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = index;
    }

    fn update_graph_index(&self, agent_id: &str, previous_graph_id: Option<&str>, graph_id: &str) {
        let mut index = self
            .graph_agent_ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous_graph_id) = previous_graph_id.filter(|previous| *previous != graph_id)
        {
            if let Some(agent_ids) = index.get_mut(previous_graph_id) {
                agent_ids.remove(agent_id);
                if agent_ids.is_empty() {
                    index.remove(previous_graph_id);
                }
            }
        }
        index
            .entry(graph_id.to_string())
            .or_default()
            .insert(agent_id.to_string());
    }
}

#[async_trait]
impl AgentTaskBackend for AgentRuntime {
    async fn execute(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String> {
        self.execute_task(packet).await
    }

    async fn cancel(&self, packet: &AgentTaskPacket) -> Result<(), String> {
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(run_cancellation_key(packet));
        let Some(snapshot) = self.get(packet.agent_id()) else {
            return Ok(());
        };
        if snapshot.run_id != packet.run_id()
            || snapshot.graph_id != packet.graph_id()
            || snapshot.node_id != packet.node_id()
            || snapshot.attempt != packet.attempt
        {
            self.pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&run_cancellation_key(packet));
            return Err("Agent cancellation does not match the current exact run".into());
        }
        if snapshot.status.is_terminal() {
            self.pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&run_cancellation_key(packet));
            return Ok(());
        }
        // The cancellation continues on the latest legal revision: the run
        // may move forward while the command is in flight, and that is a
        // retryable condition, not a rejection of the caller's intent.
        let mut attempt = 0;
        loop {
            attempt += 1;
            let Some(current) = self.get(packet.agent_id()) else {
                self.pending_cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&run_cancellation_key(packet));
                return Ok(());
            };
            if current.run_id != packet.run_id()
                || current.graph_id != packet.graph_id()
                || current.node_id != packet.node_id()
                || current.attempt != packet.attempt
            {
                // The agent now owns a different run. This cancellation named
                // an exact run, so it is not satisfied by the new one.
                self.pending_cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&run_cancellation_key(packet));
                return Err("Agent cancellation does not match the current exact run".into());
            }
            if current.status.is_terminal() {
                // The named run already reached a terminal state: the
                // cancellation is moot, and its pending key is released.
                self.pending_cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&run_cancellation_key(packet));
                return Ok(());
            }
            let receipt = self
                .command(AgentCommandRequest {
                    // The command id names the revision the cancellation is
                    // issued against. A duplicate cancellation of the same
                    // revision replays its own receipt, while a cancellation
                    // that continues on a newer revision is a new command and
                    // never inherits the rejection of an older one.
                    command_id: format!(
                        "graph-cancel:{}:{}:r{}",
                        packet.graph_id(),
                        packet.node_id(),
                        current.revision
                    ),
                    agent_id: packet.agent_id().to_string(),
                    expected_revision: current.revision,
                    command: AgentCommand::Cancel,
                    input: None,
                })
                .await;
            if receipt.accepted
                || receipt.reject_reason == Some(AgentCommandRejectReason::Terminal)
                || self.get(packet.agent_id()).is_some_and(|current| {
                    // Only the run this cancellation names may be reported as
                    // cancelled. A newer run that happens to be cancelled is
                    // not the evidence for this one.
                    current.status == AgentStatus::Cancelled
                        && current.run_id == packet.run_id()
                        && current.graph_id == packet.graph_id()
                        && current.node_id == packet.node_id()
                        && current.attempt == packet.attempt
                })
            {
                return Ok(());
            }
            if receipt.reject_reason == Some(AgentCommandRejectReason::StaleRevision)
                && attempt < CANCEL_REVISION_ATTEMPTS
            {
                // The run moved to a newer revision: continue on it.
                continue;
            }
            // A cancellation that was durably accepted stays pending even when
            // the physical operation failed: the run keeps its reclaim
            // responsibility. A cancellation that was never accepted is not
            // pending at all, and the pending key must not be able to cancel
            // the run behind the caller's back.
            let durably_cancelled = self.get(packet.agent_id()).is_some_and(|current| {
                current.status == AgentStatus::Cancelled
                    && current.run_id == packet.run_id()
                    && current.graph_id == packet.graph_id()
                    && current.node_id == packet.node_id()
                    && current.attempt == packet.attempt
            });
            if !durably_cancelled {
                self.pending_cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&run_cancellation_key(packet));
            }
            return Err(format!(
                "AgentRuntime cancel rejected for {}: {}",
                packet.agent_id(),
                receipt.message
            ));
        }
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&run_cancellation_key(packet));
        Ok(())
    }


    fn terminal_committed(&self, packet: &AgentTaskPacket) {
        {
            let mut records = self
                .records
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(record) = records.get_mut(packet.agent_id()) else {
                return;
            };
            if record.snapshot.as_ref().is_none_or(|snapshot| {
                snapshot.run_id != packet.run_id()
                    || snapshot.graph_id != packet.graph_id()
                    || snapshot.node_id != packet.node_id()
                    || snapshot.attempt != packet.attempt
            }) {
                return;
            }
            record.release_after_completion = true;
        }
        self.release_completed_record(packet.agent_id());
    }

    fn cancellation_finalized(&self, packet: &AgentTaskPacket) {
        let key = run_cancellation_key(packet);
        let owned = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(packet.agent_id())
            .is_some_and(|record| {
                record
                    .retained
                    .iter()
                    .chain(record.retiring.iter())
                    .any(|run| run.run_key == key && !run.physically_exited())
            });
        if owned {
            return;
        }
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
    }
}

pub struct AgentRuntimeResolver {
    runtime: Arc<AgentRuntime>,
}

impl AgentRuntimeResolver {
    #[must_use]
    pub fn new(runtime: Arc<AgentRuntime>) -> Self {
        Self { runtime }
    }
}

impl AgentTaskBackendResolver for AgentRuntimeResolver {
    fn resolve(&self, _packet: &AgentTaskPacket) -> Option<Arc<dyn AgentTaskBackend>> {
        Some(Arc::clone(&self.runtime) as Arc<dyn AgentTaskBackend>)
    }
}

/// The bounded time a cancellation waits for the run owner to publish its
/// own exit fact before it commits the cancelled exit itself.
const CANCELLED_EXIT_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

/// How many times a cancellation continues on a newer legal revision of its
/// run before the caller is told the cancellation was not accepted.
const CANCEL_REVISION_ATTEMPTS: u32 = 4;

fn agent_stream_id(agent_id: &str) -> String {
    format!("agent:{agent_id}")
}

fn agent_evaluation_stream(run_id: &str) -> String {
    format!("agent-evaluation:{run_id}")
}

fn legacy_import_stream_id(source_id: &str) -> String {
    format!("agent-migration:{source_id}")
}

fn validate_snapshot_identity(snapshot: &AgentRunSnapshot) -> Result<(), String> {
    snapshot
        .execution_identity
        .validate()
        .map_err(|error| error.to_string())?;
    if snapshot.execution_identity.kind()
        != harness_contract::execution::ExecutionIdentityKind::AgentNode
        || snapshot.execution_identity.task_id() != Some(snapshot.task_id.as_str())
        || snapshot.execution_identity.session_id() != Some(snapshot.session_id.as_str())
        || snapshot.execution_identity.graph_id() != Some(snapshot.graph_id.as_str())
        || snapshot.execution_identity.agent_run_id() != Some(snapshot.run_id.as_str())
        || snapshot.execution_identity.node_id() != Some(snapshot.node_id.as_str())
    {
        return Err(format!(
            "Agent {} snapshot conflicts with its canonical execution identity",
            snapshot.agent_id
        ));
    }
    Ok(())
}

fn identity_refs(
    identity: &ExecutionIdentity,
    agent_id: &str,
    run_id: &str,
) -> Vec<RuntimeEventRef> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "principal".to_string(),
            id: identity.principal_id().to_string(),
        },
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: identity.workspace_id().to_string(),
        },
        RuntimeEventRef {
            kind: "agent_instance".to_string(),
            id: agent_id.to_string(),
        },
        RuntimeEventRef {
            kind: "agent_run".to_string(),
            id: run_id.to_string(),
        },
    ];
    for (kind, id) in [
        ("mission", identity.mission_id()),
        ("task", identity.task_id()),
        ("session", identity.session_id()),
        ("turn", identity.turn_id()),
        ("execution_graph", identity.graph_id()),
        ("team_run", identity.team_run_id()),
        ("node", identity.node_id()),
    ] {
        if let Some(id) = id {
            refs.push(RuntimeEventRef {
                kind: kind.to_string(),
                id: id.to_string(),
            });
        }
    }
    refs
}

fn snapshot_identity_refs(snapshot: &AgentRunSnapshot) -> Vec<RuntimeEventRef> {
    identity_refs(
        &snapshot.execution_identity,
        &snapshot.agent_id,
        &snapshot.run_id,
    )
}

fn validate_legacy_record(record: &LegacyAgentStateRecord) -> Result<(), String> {
    let snapshot = &record.snapshot;
    validate_snapshot_identity(snapshot)?;
    if record.source_ref.trim().is_empty()
        || [
            snapshot.run_id.as_str(),
            snapshot.agent_id.as_str(),
            snapshot.task_id.as_str(),
            snapshot.session_id.as_str(),
            snapshot.graph_id.as_str(),
            snapshot.node_id.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
    {
        return Err("legacy Agent record lacks a verified canonical binding".into());
    }
    if snapshot.status.is_terminal() {
        let Some(returned) = record.returned.as_ref() else {
            return Err(format!(
                "legacy terminal Agent {} lacks its canonical return packet",
                snapshot.agent_id
            ));
        };
        if returned.run_id != snapshot.run_id
            || returned.agent_id != snapshot.agent_id
            || returned.task_id != snapshot.task_id
            || returned.session_id != snapshot.session_id
            || snapshot.execution_identity.mission_id() != Some(returned.mission_id.as_str())
            || returned.graph_id != snapshot.graph_id
            || returned.node_id != snapshot.node_id
            || returned.attempt != snapshot.attempt
            || returned.expected_graph_revision != snapshot.expected_graph_revision
            || terminal_status(returned.status) != snapshot.status
        {
            return Err(format!(
                "legacy terminal Agent {} return binding does not match its snapshot",
                snapshot.agent_id
            ));
        }
    } else if record.returned.is_some() {
        return Err(format!(
            "legacy active Agent {} must not carry a terminal return packet",
            snapshot.agent_id
        ));
    }
    Ok(())
}

fn backend_from_packet(packet: &AgentTaskPacket) -> Result<AgentBackendKind, String> {
    let binding = packet.binding.as_ref().ok_or_else(|| {
        "AgentTaskPacket has no Runtime-compiled Binding; backend selection is undefined"
            .to_string()
    })?;
    match &binding.executor {
        harness_contract::agent::AgentExecutorPolicy::CowdNative => Ok(AgentBackendKind::InProcess),
        harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. } => {
            Ok(AgentBackendKind::ProcessJsonl)
        }
        harness_contract::agent::AgentExecutorPolicy::McpBacked { .. } => Err(
            "McpBacked Agent definitions have no Runtime AgentTask backend; route their tools through a Cowd-native Agent instead".to_string(),
        ),
        harness_contract::agent::AgentExecutorPolicy::ManualReview => Err(
            "ManualReview Agent definitions require the approval workflow and cannot execute as an AgentTask backend".to_string(),
        ),
    }
}

fn ensure_team_backend_trusted(
    packet: &AgentTaskPacket,
    backend_kind: AgentBackendKind,
) -> Result<(), String> {
    if packet.team_id().is_some()
        && !matches!(
            backend_kind,
            AgentBackendKind::InProcess | AgentBackendKind::ProcessJsonl
        )
    {
        return Err("Team Agent execution requires a Runtime-owned backend".to_string());
    }
    Ok(())
}

fn verify_binding_against_definition(
    binding: &AgentBindingSnapshot,
    manifest: &harness_contract::agent::AgentDefinitionManifest,
) -> Result<(), String> {
    if binding.executor != manifest.executor {
        return Err("AgentTaskPacket Binding executor differs from the Definition".to_string());
    }
    if binding.model_policy != manifest.model_policy {
        return Err("AgentTaskPacket Binding model policy differs from the Definition".to_string());
    }
    if binding.effective_capabilities.iter().any(|capability| {
        !manifest
            .capability_contract
            .capability_ceiling
            .contains(capability)
    }) {
        return Err(
            "AgentTaskPacket Binding expands the Definition capability ceiling".to_string(),
        );
    }
    if binding
        .skill_refs
        .iter()
        .any(|skill| !manifest.capability_contract.skill_refs.contains(skill))
    {
        return Err("AgentTaskPacket Binding exposes an undeclared Skill".to_string());
    }
    if binding.tool_contract_refs.iter().any(|tool_ref| {
        !crate::agent::binding::tool_contract_is_authorized(
            tool_ref,
            &binding.effective_capabilities,
        )
    }) {
        return Err(
            "AgentTaskPacket Binding exposes a Tool outside its effective capability grant"
                .to_string(),
        );
    }
    if binding
        .data_lease
        .read_scopes
        .iter()
        .any(|scope| !manifest.cognitive_policy.read_scopes.contains(scope))
        || binding.data_lease.write_mode
            == harness_contract::agent::CognitiveWriteMode::CandidateOnly
            && manifest.cognitive_policy.write_mode
                == harness_contract::agent::CognitiveWriteMode::None
    {
        return Err("AgentTaskPacket Binding expands the Definition cognitive policy".to_string());
    }
    let expected_digest = binding_digest(binding)?;
    if binding.binding_digest != expected_digest {
        return Err("AgentTaskPacket Binding digest is invalid".to_string());
    }
    Ok(())
}

fn binding_digest(binding: &AgentBindingSnapshot) -> Result<String, String> {
    let mut unsigned = binding.clone();
    unsigned.binding_digest.clear();
    serde_json::to_string(&unsigned)
        .map(|encoded| format!("{:x}", Sha256::digest(encoded.as_bytes())))
        .map_err(|error| format!("failed to encode Agent Binding: {error}"))
}

fn canonical_predecessor_section(role: &str, outcome: &str) -> String {
    format!("### Upstream {role}\n{outcome}")
}

fn live_lifecycle_phase(
    kind: &str,
    status: AgentStatus,
) -> Option<(AgentLifecyclePhase, &'static str)> {
    match kind {
        "agent.running" => Some((AgentLifecyclePhase::Started, "running")),
        "agent.provider.first_output" => Some((AgentLifecyclePhase::FirstOutput, "running")),
        "agent.acceptance.evaluated" => Some((AgentLifecyclePhase::Evaluating, "running")),
        "agent.cancelled" => Some((AgentLifecyclePhase::Cancelled, "cancelled")),
        "agent.blocked" => Some((AgentLifecyclePhase::Blocked, "blocked")),
        "agent.terminal" => match status {
            AgentStatus::Completed => Some((AgentLifecyclePhase::Completed, "completed")),
            AgentStatus::Failed => Some((AgentLifecyclePhase::Failed, "failed")),
            AgentStatus::Cancelled => Some((AgentLifecyclePhase::Cancelled, "cancelled")),
            AgentStatus::Blocked => Some((AgentLifecyclePhase::Blocked, "blocked")),
            _ => None,
        },
        _ => None,
    }
}

fn bounded_lifecycle_summary(value: &str, max_chars: usize) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        compact
    } else {
        compact
            .chars()
            .take(max_chars.saturating_sub(1))
            .chain(std::iter::once('…'))
            .collect()
    }
}

fn terminal_status(status: AgentTerminalStatus) -> AgentStatus {
    match status {
        AgentTerminalStatus::Completed => AgentStatus::Completed,
        AgentTerminalStatus::Failed => AgentStatus::Failed,
        AgentTerminalStatus::Cancelled => AgentStatus::Cancelled,
        AgentTerminalStatus::Blocked => AgentStatus::Blocked,
    }
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.trim().is_empty() && value != "default" && value != "auto").then_some(value)
}

fn blocked_return(packet: &AgentTaskPacket, failure: String) -> AgentReturnPacket {
    return_packet(
        packet,
        AgentTerminalStatus::Blocked,
        String::new(),
        Some(failure),
    )
}

fn failed_return(packet: &AgentTaskPacket, failure: String) -> AgentReturnPacket {
    return_packet(
        packet,
        AgentTerminalStatus::Failed,
        String::new(),
        Some(failure),
    )
}

fn cancelled_return(packet: &AgentTaskPacket, failure: String) -> AgentReturnPacket {
    return_packet(
        packet,
        AgentTerminalStatus::Cancelled,
        String::new(),
        Some(failure),
    )
}

fn return_packet(
    packet: &AgentTaskPacket,
    status: AgentTerminalStatus,
    outcome: String,
    failure: Option<String>,
) -> AgentReturnPacket {
    AgentReturnPacket {
        run_id: packet.run_id().to_string(),
        agent_id: packet.agent_id().to_string(),
        task_id: packet.task_id().to_string(),
        session_id: packet.session_id().to_string(),
        mission_id: packet.mission_id().to_string(),
        team_id: packet.team_id().map(str::to_owned),
        graph_id: packet.graph_id().to_string(),
        node_id: packet.node_id().to_string(),
        attempt: packet.attempt,
        expected_graph_revision: packet.expected_graph_revision,
        status,
        outcome,
        answer_candidate: None,
        observed_acceptance: Default::default(),
        acceptance_evaluation: None,
        acceptance: Vec::new(),
        evidence_refs: Vec::new(),
        changes: Vec::new(),
        runtime_change_receipts: Vec::new(),
        conflicts: Vec::new(),
        unresolved: Vec::new(),
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        cached_tokens: 0,
        model: String::new(),
        provider: String::new(),
        tool_calls: 0,
        duplicate_tool_calls: 0,
        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        runtime_write_attempt_paths: Vec::new(),
        runtime_observed_resource_scopes: Vec::new(),
        failure,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{ProviderConfig, ProvidersConfig};
    use harness_contract::agent::{AgentCapability, AgentDefinitionId, DefinitionScope};
    use harness_contract::context::ChildExecutionBudgetReservation;

    #[test]
    fn own834_governed_exit_keeps_process_envelope_requirement() {
        for backend in [AgentBackendKind::InProcess, AgentBackendKind::ProcessJsonl] {
            for phase in [
                LocalRunPhase::Admitted,
                LocalRunPhase::BackendPending,
                LocalRunPhase::LocalReturned,
                LocalRunPhase::ProcessReaped,
                LocalRunPhase::Cancelled,
                LocalRunPhase::NotStarted,
                LocalRunPhase::Unknown,
            ] {
                for backend_digest in [None, Some("a".repeat(64))] {
                    let fact = RunExitFact {
                        schema_version: 1,
                        agent_id: "agent".into(),
                        run_id: "run".into(),
                        graph_id: "graph".into(),
                        node_id: "node".into(),
                        attempt: 1,
                        packet_sha256: "b".repeat(64),
                        backend_packet_sha256: backend_digest.clone(),
                        owner_epoch: uuid::Uuid::new_v4().to_string(),
                        backend,
                        phase: phase.clone(),
                        process_envelope: None,
                    };
                    // Without a process envelope only an in-process run has
                    // no direct effects, so only it closes on its own exit.
                    let expected = phase == LocalRunPhase::NotStarted
                        || (backend_digest.is_some()
                            && backend == AgentBackendKind::InProcess
                            && matches!(
                                phase,
                                LocalRunPhase::LocalReturned | LocalRunPhase::Cancelled
                            ));
                    let lifecycle = AuthenticatedRunLifecycle {
                        exit: Some(fact.clone()),
                        backend: Some(fact),
                        sources: Vec::new(),
                    };
                    assert_eq!(lifecycle.closed_for_governed_effects(), expected);
                    assert_eq!(
                        lifecycle.no_direct_effects(),
                        phase == LocalRunPhase::NotStarted,
                    );
                }
            }
        }
        let missing = AuthenticatedRunLifecycle {
            exit: None,
            backend: None,
            sources: Vec::new(),
        };
        assert!(!missing.closed_for_governed_effects());
        assert!(!missing.no_direct_effects());
    }

    #[test]
    fn canonical_predecessor_section_preserves_complete_role_result() {
        let outcome = format!(
            "verified {} COMPLETE_INVESTIGATOR_TAIL",
            "evidence ".repeat(4_000)
        );
        let section = canonical_predecessor_section("investigator", &outcome);

        assert!(section.starts_with("### Upstream investigator\nverified"));
        assert!(section.contains("COMPLETE_INVESTIGATOR_TAIL"));
        assert!(!section.contains("truncated"));
        assert_eq!(section.chars().count(), outcome.chars().count() + 26);
    }

    struct CompletedBackend(AgentBackendKind);

    struct HeldExitBackend {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: std::sync::atomic::AtomicUsize,
        returned_status: Option<AgentTerminalStatus>,
        panic_on_return: bool,
        reject_commands: AtomicBool,
    }

    #[async_trait]
    impl AgentRuntimeBackend for HeldExitBackend {
        fn kind(&self) -> AgentBackendKind {
            AgentBackendKind::InProcess
        }
        fn capabilities(&self) -> AgentBackendCapabilities {
            AgentBackendCapabilities::in_process()
        }
        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
            assert!(
                !self.panic_on_return,
                "controlled backend panic is not exit evidence"
            );
            match self.returned_status {
                Some(status) => {
                    let mut returned = CompletedBackend(AgentBackendKind::InProcess)
                        .execute(packet, selection)
                        .await?;
                    returned.status = status;
                    Ok(returned)
                }
                None => Err("controlled ordinary local backend error".into()),
            }
        }
        async fn command(
            &self,
            _: &AgentRunHandle,
            _: &AgentCommandRequest,
        ) -> Result<(), AgentCommandRejectReason> {
            if self.reject_commands.load(Ordering::SeqCst) {
                Err(AgentCommandRejectReason::UnsupportedByBackend)
            } else {
                Ok(())
            }
        }
    }

    fn held_exit_runtime(
        status: Option<AgentTerminalStatus>,
        panic_on_return: bool,
    ) -> (Arc<AgentRuntime>, Arc<HeldExitBackend>) {
        let runtime = Arc::new(AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        ));
        let backend = Arc::new(HeldExitBackend {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            returned_status: status,
            panic_on_return,
            reject_commands: AtomicBool::new(false),
        });
        runtime.register_observation_authority_backend(backend.clone());
        (runtime, backend)
    }

    /// T1: a normally completed Process run keeps its positive local exit.
    #[cfg(unix)]
    #[tokio::test]
    async fn completed_process_run_keeps_a_positive_local_exit() {
        let (_temporary, _services, adapter, packet) =
            crate::agent_process_jsonl_adapter::tests::driver_exit_fixture();
        let adapter = Arc::new(adapter);
        let runtime = Arc::new(AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        ));
        runtime.register_runtime_process_jsonl_backend(adapter.clone());
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        let returned = tokio::time::timeout(std::time::Duration::from_secs(120), caller)
            .await
            .expect("completed run must return")
            .unwrap()
            .unwrap();
        assert_eq!(returned.status, AgentTerminalStatus::Completed);
        let (evidence, _) = confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .expect("a completed run keeps its authenticated local exit");
        assert!(evidence.fact.positive());
        assert_eq!(evidence.fact.phase, LocalRunPhase::ProcessReaped);
    }

    /// T2 and T4: a cancelled Process run whose owner can no longer publish
    /// still leaves exactly one authenticated cancelled local exit.
    #[cfg(unix)]
    /// N4: a cancellation whose own snapshot cannot be persisted is not
    /// accepted. The worker is not torn down, the run does not claim to be
    /// cancelled, and no exit fact is published for a cancellation that never
    /// happened.
    #[tokio::test]
    async fn r1_900_a_cancellation_that_cannot_be_persisted_is_not_accepted() {
        let (_temporary, _services, adapter, packet) =
            crate::agent_process_jsonl_adapter::tests::driver_cancel_fixture();
        let adapter = Arc::new(adapter);
        let runtime = Arc::new(AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        ));
        runtime.register_runtime_process_jsonl_backend(adapter.clone());
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt) != Some(false) {
            assert!(
                std::time::Instant::now() < deadline,
                "the process child never owned a live process group"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        runtime
            .cancellation_append_failures
            .store(1, Ordering::SeqCst);
        let rejected = AgentTaskBackend::cancel(runtime.as_ref(), &packet).await;
        assert!(
            rejected.is_err(),
            "a cancellation that cannot be persisted is not accepted: {rejected:?}"
        );
        let snapshot = runtime.get(packet.agent_id()).expect("the run still exists");
        assert_ne!(
            snapshot.status,
            AgentStatus::Cancelled,
            "an unpersisted cancellation never claims the run as cancelled"
        );
        assert_ne!(
            snapshot.failure.as_deref(),
            Some(CANCELLED_BY_COMMAND),
            "an unpersisted cancellation never claims the cancel reason"
        );
        assert_eq!(
            runtime.cancellation_append_failures.load(Ordering::SeqCst),
            0,
            "the injected failure is consumed exactly once"
        );
        let stream = agent_stream_id(packet.agent_id());
        let exits = runtime
            .event_store
            .list_stream(&stream)
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.kind == "agent.local_exit"
                    && event.idempotency_key.as_deref()
                        == Some(format!("agent.local_exit:{}", packet.run_id()).as_str())
            })
            .count();
        assert_eq!(exits, 0, "no exit fact is published for a rejected cancellation");
        // The cancellation converges once it is durable again: the caller is
        // either told the cancellation was accepted, or that the run already
        // reached a terminal state. It is never told the cancellation was
        // accepted while nothing was durable.
        let converged = AgentTaskBackend::cancel(runtime.as_ref(), &packet).await;
        assert!(
            converged.is_ok(),
            "the cancellation converges once it is durable: first={rejected:?} second={converged:?}"
        );
        let snapshot = runtime.get(packet.agent_id()).expect("the run still exists");
        assert!(
            snapshot.status.is_terminal(),
            "the cancelled run reaches a terminal state: {:?}",
            snapshot.status
        );
        let _ = caller.await;
    }

    /// N4: when the cancellation fact is durable but the trailing command
    /// record cannot be written, the receipt still names a revision that
    /// exists, the same command replays the same answer, and a restarted
    /// Runtime finds the same run identity.
    #[tokio::test]
    async fn r1_900_c_a_failed_trailing_record_never_names_an_unwritten_revision() {
        let (_temporary, _services, adapter, packet) =
            crate::agent_process_jsonl_adapter::tests::driver_cancel_fixture();
        let adapter = Arc::new(adapter);
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = Arc::new(AgentRuntime::new(
            Arc::clone(&store),
            configured_registry(),
        ));
        runtime.register_runtime_process_jsonl_backend(adapter.clone());
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt) != Some(false) {
            assert!(
                std::time::Instant::now() < deadline,
                "the process child never owned a live process group"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let snapshot = runtime.get(packet.agent_id()).expect("the run exists");
        runtime.command_append_failures.store(1, Ordering::SeqCst);
        let request = AgentCommandRequest {
            command_id: "r1-900-c-cancel".into(),
            agent_id: packet.agent_id().to_string(),
            expected_revision: snapshot.revision,
            command: AgentCommand::Cancel,
            input: None,
        };
        let receipt = runtime.command(request.clone()).await;
        assert!(receipt.accepted, "the cancellation itself was accepted: {receipt:?}");
        let stream = agent_stream_id(packet.agent_id());
        let cancellation_revision = store
            .list_stream(&stream)
            .unwrap()
            .into_iter()
            .find(|event| event.kind == "agent.cancellation_requested")
            .and_then(|event| {
                event
                    .payload
                    .get("snapshot")
                    .and_then(|snapshot| snapshot.get("revision"))
                    .and_then(serde_json::Value::as_u64)
            })
            .expect("the cancellation fact is durable");
        assert_eq!(
            receipt.accepted_revision,
            cancellation_revision,
            "the receipt names the revision the cancellation was written at"
        );
        assert!(
            receipt.message.contains("trailing command record failed"),
            "the receipt names the trailing failure: {receipt:?}"
        );
        // The same command replays the same answer.
        let replay = runtime.command(request).await;
        assert_eq!(replay.accepted_revision, receipt.accepted_revision);
        assert_eq!(replay.accepted, receipt.accepted);
        // A restarted Runtime finds the same run and its identity.
        let restarted = AgentRuntime::new(Arc::clone(&store), configured_registry());
        let recovered = restarted.get(packet.agent_id()).expect("the run is recovered");
        assert_eq!(recovered.run_id, packet.run_id());
        assert_eq!(recovered.status, AgentStatus::Cancelled);
        let _ = caller.await;
    }

    /// N4: a cancellation whose own snapshot could not be persisted is never
    /// accepted, and the pending key it left behind must not cancel the run
    /// afterwards. The worker is released to return normally and the run must
    /// end on its own result, not as a cancellation nobody accepted.
    #[tokio::test]
    async fn r1_900_d_an_unaccepted_cancellation_never_cancels_the_run_later() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("r1-900-d-unaccepted-cancel");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        runtime
            .cancellation_append_failures
            .store(1, Ordering::SeqCst);
        let rejected = AgentTaskBackend::cancel(runtime.as_ref(), &packet).await;
        assert!(
            rejected.is_err(),
            "a cancellation that cannot be persisted is not accepted: {rejected:?}"
        );
        assert!(
            !runtime
                .pending_cancellations
                .lock()
                .unwrap()
                .contains(&run_cancellation_key(&packet)),
            "an unaccepted cancellation is not pending"
        );
        // The worker returns normally: nothing cancelled it.
        backend.release.notify_one();
        let returned = tokio::time::timeout(std::time::Duration::from_secs(30), caller)
            .await
            .expect("the run must return")
            .unwrap()
            .unwrap();
        assert_eq!(
            returned.status,
            AgentTerminalStatus::Completed,
            "an unaccepted cancellation must not change the outcome"
        );
        let snapshot = runtime.get(packet.agent_id()).expect("the run exists");
        assert_ne!(
            snapshot.status,
            AgentStatus::Cancelled,
            "the run was never cancelled: {:?}",
            snapshot.status
        );
        assert_ne!(
            snapshot.failure.as_deref(),
            Some(CANCELLED_BY_COMMAND),
            "the run never claims a cancellation nobody accepted"
        );
    }

    /// N4: the trailing command receipt is written against the latest legal
    /// revision of the run, not against the revision the command started
    /// from. The cancellation and the trailing record are two durable steps.
    #[tokio::test]
    async fn r1_900_b_the_trailing_receipt_follows_the_latest_legal_revision() {
        let (_temporary, _services, adapter, packet) =
            crate::agent_process_jsonl_adapter::tests::driver_cancel_fixture();
        let adapter = Arc::new(adapter);
        let runtime = Arc::new(AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        ));
        runtime.register_runtime_process_jsonl_backend(adapter.clone());
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt) != Some(false) {
            assert!(
                std::time::Instant::now() < deadline,
                "the process child never owned a live process group"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let snapshot = runtime.get(packet.agent_id()).expect("the run exists");
        let receipt = runtime
            .command(AgentCommandRequest {
                command_id: "r1-900-b-cancel".into(),
                agent_id: packet.agent_id().to_string(),
                expected_revision: snapshot.revision,
                command: AgentCommand::Cancel,
                input: None,
            })
            .await;
        assert!(receipt.accepted, "the cancellation is accepted: {receipt:?}");
        let stream = agent_stream_id(packet.agent_id());
        let events = runtime.event_store.list_stream(&stream).unwrap();
        let revision_of = |kind: &str| -> u64 {
            events
                .iter()
                .find(|event| event.kind == kind)
                .and_then(|event| event.payload.get("snapshot"))
                .and_then(|snapshot| snapshot.get("revision"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| panic!("{kind} event with a revision"))
        };
        let cancellation_revision = revision_of("agent.cancellation_requested");
        let command_revision = revision_of("agent.command");
        assert!(
            command_revision > cancellation_revision,
            "the trailing record is written against the next legal revision"
        );
        let current = runtime
            .event_store
            .stream_revision(&stream)
            .expect("the agent stream revision");
        assert_eq!(
            current, command_revision,
            "the run ends on the revision the trailing record was written against"
        );
        assert_eq!(
            receipt.accepted_revision, current,
            "the receipt names the revision it was accepted at"
        );
        let _ = caller.await;
    }

    #[tokio::test]
    async fn cancelled_process_run_publishes_one_cancelled_local_exit() {
        let (_temporary, _services, adapter, packet) =
            crate::agent_process_jsonl_adapter::tests::driver_cancel_fixture();
        let adapter = Arc::new(adapter);
        let runtime = Arc::new(AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        ));
        runtime.register_runtime_process_jsonl_backend(adapter.clone());
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        // The worker owns a live process group while it runs.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt) != Some(false) {
            assert!(
                std::time::Instant::now() < deadline,
                "the process child never owned a live process group"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // T4: a repeated cancellation must not produce a second exit fact.
        for _ in 0..2 {
            AgentTaskBackend::cancel(runtime.as_ref(), &packet)
                .await
                .expect("cancellation must be accepted");
        }
        let stream = agent_stream_id(packet.agent_id());
        let exits = runtime
            .event_store
            .list_stream(&stream)
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.idempotency_key.as_deref()
                    == Some(format!("agent.local_exit:{}", packet.run_id()).as_str())
            })
            .count();
        assert_eq!(exits, 1, "a cancelled run keeps exactly one local exit");
        let (evidence, _) = confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .expect("a cancelled run keeps its authenticated local exit");
        assert_eq!(evidence.fact.phase, LocalRunPhase::Cancelled);
        assert!(evidence.fact.positive());
        let lifecycle = authenticated_run_lifecycle(&runtime.event_store, &packet).unwrap();
        assert!(
            lifecycle.closed_for_governed_effects(),
            "a proven cancelled exit closes the governed effects of this run"
        );
        // The owner of this run cannot reach its own publication point; the
        // cancellation committed the fact instead.
        caller.abort();
        let _ = caller.await;
    }

    /// T3: the owned process group is the physical exit proof, and an
    /// unproven exit never claims that the worker is gone.
    #[cfg(unix)]
    #[tokio::test]
    async fn owned_process_group_observation_refuses_an_unproven_exit() {
        let (_temporary, _services, adapter, packet) =
            crate::agent_process_jsonl_adapter::tests::driver_cancel_fixture();
        let adapter = Arc::new(adapter);
        assert_eq!(
            adapter.owned_process_group_is_empty("run:unknown", 0),
            None,
            "an unknown run has no observable process group"
        );
        let run = adapter.clone();
        let supplied = packet.clone();
        let worker = tokio::spawn(async move {
            run.execute(
                supplied,
                AgentModelSelection {
                    model: "test-model".into(),
                    provider: "test".into(),
                    registry_revision: 0,
                },
            )
            .await
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while adapter
            .owned_process_group_is_empty(&packet.run_id(), packet.attempt)
            .is_none()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the process child never owned a process group"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt),
            Some(false),
            "a live worker keeps its owned process group occupied"
        );
        let handle = crate::agent_run_handle::AgentRunHandle {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            backend: AgentBackendKind::ProcessJsonl,
            revision: 1,
            status: AgentStatus::Running,
        };
        adapter
            .command(
                &handle,
                &AgentCommandRequest {
                    command_id: "cancel-owned-group".into(),
                    agent_id: packet.agent_id().to_string(),
                    expected_revision: 1,
                    command: AgentCommand::Cancel,
                    input: None,
                },
            )
            .await
            .expect("the cancellation must reach the live process");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt) != Some(true) {
            assert!(
                std::time::Instant::now() < deadline,
                "the cancelled worker never left its owned process group"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            adapter.owned_process_group_is_empty(&packet.run_id(), packet.attempt),
            Some(true),
            "a reaped worker leaves an empty owned process group"
        );
        worker.abort();
        let _ = worker.await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_prepared_admission_failure_never_launches_or_rebinds() {
        for cancel in [false, true] {
            let (_temporary, _services, adapter, packet) =
                crate::agent_process_jsonl_adapter::tests::driver_exit_fixture();
            let adapter = Arc::new(adapter);
            let runtime = Arc::new(AgentRuntime::new(
                Arc::new(RuntimeEventStore::for_test()),
                configured_registry(),
            ));
            runtime.register_runtime_process_jsonl_backend(adapter.clone());
            runtime
                .backend_append_failures
                .store(1000, Ordering::SeqCst);
            let supplied = packet.clone();
            let owner = runtime.clone();
            let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                runtime.backend_append_failed.notified(),
            )
            .await
            .unwrap();
            assert_eq!(adapter.fixture_spawn_count(), 0);
            assert_eq!(adapter.fixture_prepare_count(), 1);
            let stream = agent_stream_id(packet.agent_id());
            assert!(runtime
                .event_store
                .event_by_idempotency_key(
                    &stream,
                    &format!("agent.backend_admitted:{}", packet.run_id())
                )
                .unwrap()
                .is_none());
            let pending = runtime
                .records
                .read()
                .unwrap()
                .get(packet.agent_id())
                .unwrap()
                .retained
                .clone()
                .unwrap();
            assert!(!pending.physically_exited());
            let binding = packet.binding.as_ref().unwrap();
            let (command_ref, command_digest) = match &binding.executor {
                harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                    command_ref,
                    command_digest,
                } => (command_ref, command_digest),
                _ => panic!("driver fixture must have a real approved Process definition"),
            };
            let changed = crate::ProcessJsonlSpec::new(command_ref, "another-worker.py", vec![]);
            assert_ne!(&changed.command_digest, command_digest);
            assert!(adapter.register_command(changed).is_err());
            if cancel {
                let snapshot = runtime.get(packet.agent_id()).unwrap();
                assert!(
                    runtime
                        .command(AgentCommandRequest {
                            command_id: "cancel-during-backend-admission-retry".into(),
                            agent_id: packet.agent_id().into(),
                            expected_revision: snapshot.revision,
                            command: AgentCommand::Cancel,
                            input: None,
                        })
                        .await
                        .accepted
                );
            }
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            assert_eq!(
                adapter.fixture_spawn_count(),
                0,
                "caller abort must not release or execute the carrier"
            );
            runtime.backend_append_failures.store(0, Ordering::SeqCst);
            let returned = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                runtime.execute_task(packet.clone()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(adapter.fixture_spawn_count(), usize::from(!cancel));
            assert_eq!(
                adapter.fixture_prepare_count(),
                1,
                "admission retry must retain the same prepared carrier"
            );
            let (exit, _) = confirmed_run_exit(&runtime.event_store, &packet)
                .unwrap()
                .unwrap();
            assert_eq!(
                exit.fact.phase,
                if cancel {
                    LocalRunPhase::NotStarted
                } else {
                    LocalRunPhase::ProcessReaped
                }
            );
            let admission = runtime
                .event_store
                .event_by_idempotency_key(
                    &stream,
                    &format!("agent.backend_admitted:{}", packet.run_id()),
                )
                .unwrap()
                .unwrap();
            let admitted: RunExitFact = serde_json::from_value(admission.payload.clone()).unwrap();
            assert_eq!(admitted.process_envelope, exit.fact.process_envelope);
            assert_eq!(
                admitted.backend_packet_sha256,
                exit.fact.backend_packet_sha256
            );
            assert!(admitted.process_envelope.is_some());
            assert!(!admission.payload.to_string().contains("environment_refs"));
            assert!(admission.payload.get("argv").is_none());
            let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
            restarted.register_runtime_process_jsonl_backend(adapter.clone());
            assert_eq!(
                restarted.execute_task(packet.clone()).await.unwrap(),
                returned
            );
            assert_eq!(adapter.fixture_spawn_count(), usize::from(!cancel));
            if cancel {
                continue;
            }
            let events = [
                "agent.run_admitted",
                "agent.backend_admitted",
                "agent.local_exit",
            ]
            .map(|kind| {
                runtime
                    .event_store
                    .event_by_idempotency_key(&stream, &format!("{kind}:{}", packet.run_id()))
                    .unwrap()
                    .unwrap()
            });
            for defect in [
                "legacy",
                "version",
                "enforcement",
                "original_packet_sha256",
                "backend_packet_sha256",
                "command_ref",
                "command_digest",
                "workspace",
                "workspace_writable",
                "network_enabled",
                "policy_revision",
                "policy_digest",
                "network_restrictions_digest",
                "private_network_allowed",
                "hardened",
                "protected_roots_denied",
                "inherited_fds_closed",
                "environment_allowlist_enforced",
                "piped_stdio",
                "control_scratch",
                "digest",
                "exit-mismatch",
            ] {
                let negative = RuntimeEventStore::for_test();
                let mut copied = events.clone();
                for event in &mut copied[1..] {
                    if defect == "legacy" {
                        event
                            .payload
                            .as_object_mut()
                            .unwrap()
                            .remove("process_envelope");
                    } else if defect == "exit-mismatch" {
                        if event.kind == "agent.local_exit" {
                            event
                                .payload
                                .as_object_mut()
                                .unwrap()
                                .remove("process_envelope");
                        }
                    } else {
                        let value = &mut event.payload["process_envelope"][defect];
                        *value = match defect {
                            "version" => serde_json::json!(999),
                            "policy_revision" => serde_json::json!(999),
                            "workspace" | "control_scratch" => serde_json::json!("/foreign/root"),
                            _ if value.is_boolean() => serde_json::json!(!value.as_bool().unwrap()),
                            _ => serde_json::json!("foreign"),
                        };
                    }
                }
                for event in copied {
                    let revision = negative.stream_revision(&stream).unwrap();
                    negative
                        .append_transaction(AppendTransactionRequest {
                            transaction_id: event.transaction_id,
                            expected_streams: vec![ExpectedStreamRevision {
                                stream_id: stream.clone(),
                                expected_revision: revision,
                            }],
                            events: vec![
                                crate::runtime_event_store::RuntimeTransactionEventInput {
                                    event: RuntimeEventInput {
                                        stream_id: stream.clone(),
                                        scope: event.scope,
                                        kind: event.kind,
                                        status: event.status,
                                        actor: event.actor,
                                        refs: event.refs,
                                        payload: event.payload,
                                    },
                                    idempotency_key: event.idempotency_key,
                                    schema_version: event.schema_version,
                                },
                            ],
                        })
                        .unwrap();
                }
                if defect == "legacy" {
                    assert!(confirmed_run_exit(&negative, &packet).unwrap().is_some());
                    assert!(!authenticated_run_lifecycle(&negative, &packet)
                        .unwrap()
                        .no_direct_effects());
                } else {
                    assert!(
                        authenticated_run_lifecycle(&negative, &packet).is_err(),
                        "explicit test-only negative injection: {defect}"
                    );
                }
            }
            runtime.drain_retained_runs().await.unwrap();
        }

        for freeze in [false, true] {
            let fixture =
                crate::agentic::execution::reviewed_goal_fixture::PhysicalProcessFixture::new(
                    crate::config::AgentExecutorSandboxProfile::WorkspaceReadWrite,
                    if freeze {
                        "prepared-real-freeze"
                    } else {
                        "prepared-real-revision"
                    },
                )
                .await;
            let runtime = fixture.services.agent_runtime();
            runtime
                .backend_append_failures
                .store(1000, Ordering::SeqCst);
            let worker = fixture
                .dispatch(
                    "prepared-real-transition",
                    vec!["read:.".into()],
                    false,
                    false,
                    false,
                )
                .await;
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                runtime.backend_append_failed.notified(),
            )
            .await
            .unwrap();
            let adapter = runtime
                .process_jsonl_adapter
                .read()
                .unwrap()
                .clone()
                .unwrap();
            assert_eq!(adapter.fixture_prepare_count(), 1);
            assert_eq!(adapter.fixture_spawn_count(), 0);
            assert!(!worker.directory().join("started.json").exists());
            worker.withdraw().await;
            if freeze {
                fixture.services.freeze_session_execution_policy_admission(
                    worker.packet.session_id(),
                    "physical-transition",
                );
            } else {
                let control = fixture
                    .services
                    .session_execution_policy_control(worker.packet.session_id())
                    .unwrap();
                let original = control.snapshot();
                control
                    .replace(
                        harness_contract::policy::SessionExecutionPolicy::from_profile(
                            harness_contract::policy::AutonomyProfileId::Cautious,
                            original.revision + 1,
                            harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
                        ),
                    )
                    .unwrap();
            }
            runtime.backend_append_failures.store(0, Ordering::SeqCst);
            worker.wait_terminal().await;
            let reason = worker.terminal_failure();
            assert!(
                reason.contains("physical_grant")
                    && (reason.contains("frozen") || reason.contains("revision")),
                "{reason}"
            );
            let (exit, _) = confirmed_run_exit(fixture.services.event_store(), &worker.packet)
                .unwrap()
                .unwrap();
            assert_eq!(exit.fact.phase, LocalRunPhase::NotStarted);
            assert!(
                exit.fact.process_envelope.is_some(),
                "post-admission NotStarted must retain the same prepared facts"
            );
            assert_eq!(adapter.fixture_spawn_count(), 0);
            assert_eq!(adapter.fixture_prepare_count(), 1);
            assert!(
                !worker.directory().exists(),
                "discarded unspawned carrier owns exact scratch cleanup"
            );
            if freeze {
                assert!(fixture
                    .services
                    .unfreeze_session_execution_policy_admission(
                        worker.packet.session_id(),
                        "physical-transition"
                    ));
            }
            println!(
                "PHYSICAL_PREPARED_TRANSITION freeze={freeze} same_carrier=true actual_spawn=0"
            );
        }
    }

    #[tokio::test]
    async fn exact_exit_ack_and_caller_drop_retain_the_original_run() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("exact-exit-drop");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        let snapshot = runtime.get(packet.agent_id()).unwrap();
        let receipt = runtime
            .command(AgentCommandRequest {
                command_id: "ack-only".into(),
                agent_id: packet.agent_id().into(),
                expected_revision: snapshot.revision,
                command: AgentCommand::Cancel,
                input: None,
            })
            .await;
        assert!(receipt.accepted);
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        AgentTaskBackend::terminal_committed(runtime.as_ref(), &packet);
        assert!(runtime
            .records
            .read()
            .unwrap()
            .get(packet.agent_id())
            .unwrap()
            .retained
            .is_some());
        backend.release.notify_one();
        let returned = runtime.execute_task(packet.clone()).await.unwrap();
        assert_eq!(returned.status, AgentTerminalStatus::Cancelled);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let (evidence, _) = confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .unwrap();
        assert_eq!(evidence.fact.phase, LocalRunPhase::Cancelled);
        let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
        assert_eq!(
            confirmed_run_exit(&restarted.event_store, &packet)
                .unwrap()
                .unwrap()
                .0,
            evidence
        );
        assert_eq!(
            restarted.execute_task(packet.clone()).await.unwrap().status,
            AgentTerminalStatus::Cancelled
        );
        assert_ne!(restarted.owner_epoch, runtime.owner_epoch);
    }

    #[tokio::test]
    async fn projection_426_cut_reader_reuses_authentication_without_future_event_invariants() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("projection-cut-426");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        let cut = runtime.event_store.durable_commit_cursor().unwrap();
        backend.release.notify_one();
        assert_eq!(
            caller.await.unwrap().unwrap().status,
            AgentTerminalStatus::Completed
        );
        let stream = agent_stream_id(packet.agent_id());
        let head = runtime.event_store.stream_revision(&stream).unwrap();
        let records = [
            "agent.run_admitted",
            "agent.backend_admitted",
            "agent.local_exit",
        ]
        .iter()
        .map(|kind| {
            Arc::new(
                runtime
                    .event_store
                    .event_by_idempotency_key(&stream, &format!("{kind}:{}", packet.run_id()))
                    .unwrap()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
        assert!(records.last().unwrap().commit_cursor > cut);
        let at_cut = records
            .iter()
            .filter(|record| record.commit_cursor <= cut)
            .cloned()
            .collect::<Vec<_>>();
        assert!(!projection_run_closed_from_records(&packet, head, &at_cut).unwrap());
        assert!(projection_run_closed_from_records(&packet, head, &records).unwrap());
        let mut forged = records.clone();
        let mut exit = (*forged.pop().unwrap()).clone();
        exit.payload["owner_epoch"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
        forged.push(Arc::new(exit));
        assert!(projection_run_closed_from_records(&packet, head, &forged).is_err());
        let mut duplicate = records.clone();
        duplicate.push(records.last().unwrap().clone());
        assert!(projection_run_closed_from_records(&packet, head, &duplicate).is_err());
    }

    #[tokio::test]
    async fn exact_exit_finalization_and_stale_cancel_cannot_erase_live_run_intent() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("exact-exit-finalization");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        backend.reject_commands.store(true, Ordering::SeqCst);
        assert!(AgentTaskBackend::cancel(runtime.as_ref(), &packet)
            .await
            .is_ok());
        assert!(runtime
            .pending_cancellations
            .lock()
            .unwrap()
            .contains(&run_cancellation_key(&packet)));
        AgentTaskBackend::cancellation_finalized(runtime.as_ref(), &packet);
        let mut stale = packet.clone();
        stale.attempt += 1;
        assert!(AgentTaskBackend::cancel(runtime.as_ref(), &stale)
            .await
            .is_err());
        AgentTaskBackend::cancellation_finalized(runtime.as_ref(), &stale);
        assert!(runtime
            .pending_cancellations
            .lock()
            .unwrap()
            .contains(&run_cancellation_key(&packet)));
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        backend.release.notify_one();
        assert_eq!(
            caller.await.unwrap().unwrap().status,
            AgentTerminalStatus::Cancelled
        );
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_some());
        assert!(!runtime
            .pending_cancellations
            .lock()
            .unwrap()
            .contains(&run_cancellation_key(&packet)));
    }

    #[tokio::test]
    async fn exact_exit_failed_cancelled_and_local_errors_are_not_remote_exit_claims() {
        for status in [
            Some(AgentTerminalStatus::Failed),
            Some(AgentTerminalStatus::Cancelled),
            None,
        ] {
            let (runtime, backend) = held_exit_runtime(status, false);
            let packet = task("exact-exit-errors");
            let owner = runtime.clone();
            let supplied = packet.clone();
            let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
            backend.entered.notified().await;
            assert!(confirmed_run_exit(&runtime.event_store, &packet)
                .unwrap()
                .is_none());
            let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
            assert!(restarted.execute_task(packet.clone()).await.is_err());
            assert!(confirmed_run_exit(&restarted.event_store, &packet)
                .unwrap()
                .is_none());
            backend.release.notify_one();
            caller.await.unwrap().unwrap();
            let (fact, sources) = confirmed_run_exit(&runtime.event_store, &packet)
                .unwrap()
                .unwrap();
            assert_eq!(fact.fact.phase, LocalRunPhase::LocalReturned);
            let mut wrong = packet.clone();
            wrong.attempt += 1;
            assert!(confirmed_run_exit(&runtime.event_store, &wrong).is_err());
            wrong = packet.clone();
            wrong.objective.push_str("foreign digest");
            assert!(confirmed_run_exit(&runtime.event_store, &wrong).is_err());
            wrong = packet.clone();
            wrong.assignment = crate::test_support::agent_assignment(
                Some(packet.assignment.definition_ref.clone()),
                packet.agent_id(),
                "foreign-run",
                packet.task_id(),
                packet.session_id(),
                packet.mission_id(),
                packet.team_id(),
                packet.graph_id(),
                packet.node_id(),
            );
            assert!(confirmed_run_exit(&runtime.event_store, &wrong)
                .unwrap()
                .is_none());
            runtime
                .event_store
                .append(RuntimeEventInput {
                    stream_id: agent_stream_id(packet.agent_id()),
                    scope: RuntimeEventScope::Agent,
                    kind: "test.source_advanced".into(),
                    status: None,
                    actor: Some("test".into()),
                    refs: Vec::new(),
                    payload: serde_json::json!({}),
                })
                .unwrap();
            let stale = runtime
                .event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: "exact-exit-stale-source".into(),
                    expected_streams: sources,
                    events: vec![RuntimeEventInput {
                        stream_id: agent_stream_id(packet.agent_id()),
                        scope: RuntimeEventScope::Agent,
                        kind: "test.must_not_commit".into(),
                        status: None,
                        actor: Some("test".into()),
                        refs: Vec::new(),
                        payload: serde_json::json!({}),
                    }
                    .into()],
                });
            assert!(stale.is_err());
            assert!(runtime
                .event_store
                .list_stream(&agent_stream_id(packet.agent_id()))
                .unwrap()
                .iter()
                .all(|event| event.kind != "test.must_not_commit"));
        }
    }

    #[tokio::test]
    async fn exact_exit_drain_survives_an_interrupted_wait_and_fences_admission() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("exact-exit-drain");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        caller.abort();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            runtime.drain_retained_runs()
        )
        .await
        .is_err());
        assert!(runtime
            .execute_task(task("fenced-new-run"))
            .await
            .unwrap_err()
            .contains("shutdown"));
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        backend.release.notify_one();
        runtime.drain_retained_runs().await.unwrap();
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_some());
        runtime.drain_retained_runs().await.unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exact_exit_append_failure_retains_the_original_fact_until_retry() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("exact-exit-append-failure");
        runtime.exit_append_failures.store(1000, Ordering::SeqCst);
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        caller.abort();
        backend.release.notify_one();
        runtime.exit_append_failed.notified().await;
        let retained = runtime
            .records
            .read()
            .unwrap()
            .get(packet.agent_id())
            .unwrap()
            .retained
            .clone()
            .unwrap();
        let original = retained.pending_exit.lock().unwrap().clone().unwrap();
        assert_eq!(original.phase, LocalRunPhase::LocalReturned);
        assert!(!retained.exit_committed.load(Ordering::Acquire));
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            runtime.drain_retained_runs()
        )
        .await
        .is_err());
        runtime.exit_append_failures.store(0, Ordering::SeqCst);
        runtime.drain_retained_runs().await.unwrap();
        let (proof, _) = confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .unwrap();
        assert_eq!(proof.fact, original);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            runtime
                .event_store
                .list_stream(&agent_stream_id(packet.agent_id()))
                .unwrap()
                .iter()
                .filter(|event| event.kind == "agent.local_exit")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn exact_exit_panic_and_unwitnessed_process_return_stay_unknown() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), true);
        let packet = task("exact-exit-panic");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        backend.release.notify_one();
        assert!(caller.await.unwrap().is_err());
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        assert!(runtime.drain_retained_runs().await.is_err());
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        runtime.register_backend(Arc::new(CompletedBackend(AgentBackendKind::ProcessJsonl)));
        let mut packet = task("exact-exit-process-no-os-owner");
        packet.binding.as_mut().unwrap().executor =
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                command_ref: "test/no-os-owner".into(),
                command_digest: "a".repeat(64),
            };
        runtime.execute_task(packet.clone()).await.unwrap();
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        assert!(runtime.drain_retained_runs().await.is_err());
    }

    #[tokio::test]
    async fn exact_exit_explicit_not_started_is_durable_without_a_backend() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            Arc::new(ProviderRegistry::empty()),
        );
        let packet = task("exact-exit-not-started");
        runtime.execute_task(packet.clone()).await.unwrap();
        let (fact, _) = confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .unwrap();
        assert_eq!(fact.fact.phase, LocalRunPhase::NotStarted);
        assert!(fact.fact.backend_packet_sha256.is_none());
        let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
        assert_eq!(
            confirmed_run_exit(&restarted.event_store, &packet)
                .unwrap()
                .unwrap()
                .0,
            fact
        );
    }

    #[tokio::test]
    async fn exact_exit_foreign_epoch_and_unknown_schema_cannot_extend_a_real_witness() {
        for mutation in ["owner_epoch", "schema_version", "node_id"] {
            let runtime = AgentRuntime::new(
                Arc::new(RuntimeEventStore::for_test()),
                configured_registry(),
            );
            runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
                AgentBackendKind::InProcess,
            )));
            let packet = task("exact-exit-forged-suffix");
            runtime.execute_task(packet.clone()).await.unwrap();
            let (fact, _) = confirmed_run_exit(&runtime.event_store, &packet)
                .unwrap()
                .unwrap();
            let mut invalid = serde_json::to_value(fact.fact.clone()).unwrap();
            invalid[mutation] = if mutation == "schema_version" {
                serde_json::json!(999)
            } else {
                serde_json::json!("foreign")
            };
            let invalid: RunExitFact = serde_json::from_value(invalid).unwrap();
            assert!(runtime
                .persist_run_fact(&invalid, "agent.local_exit")
                .is_err());
            assert_eq!(
                confirmed_run_exit(&runtime.event_store, &packet)
                    .unwrap()
                    .unwrap()
                    .0,
                fact
            );
        }
    }

    #[tokio::test]
    async fn exact_exit_point_lookup_rejects_out_of_order_or_foreign_indexed_facts() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let packet = task("exact-exit-indexed-negative");
        runtime.execute_task(packet.clone()).await.unwrap();
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_some());
        let stream = agent_stream_id(packet.agent_id());
        let mut original = Vec::new();
        for kind in [
            "agent.run_admitted",
            "agent.backend_admitted",
            "agent.local_exit",
        ] {
            original.push(
                runtime
                    .event_store
                    .event_by_idempotency_key(&stream, &format!("{kind}:{}", packet.run_id()))
                    .unwrap()
                    .unwrap(),
            );
        }
        for defect in ["order", "epoch", "backend_digest", "actor", "phase"] {
            let negative_store = RuntimeEventStore::for_test();
            let mut events = original.clone();
            match defect {
                "order" => events.swap(1, 2),
                "epoch" => {
                    events[2].payload["owner_epoch"] = serde_json::json!("foreign-epoch");
                    events[2].transaction_id = format!(
                        "agent.local_exit:{}:foreign-epoch:{}",
                        packet.run_id(),
                        run_packet_sha256(&packet).unwrap()
                    );
                }
                "backend_digest" => {
                    events[2].payload["backend_packet_sha256"] = serde_json::json!("f".repeat(64))
                }
                "actor" => events[2].actor = Some("untrusted_supervisor".into()),
                "phase" => events[2].payload["phase"] = serde_json::json!("admitted"),
                _ => unreachable!(),
            }
            for event in events {
                let revision = negative_store.stream_revision(&stream).unwrap();
                negative_store
                    .append_transaction(AppendTransactionRequest {
                        transaction_id: event.transaction_id,
                        expected_streams: vec![ExpectedStreamRevision {
                            stream_id: stream.clone(),
                            expected_revision: revision,
                        }],
                        events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                            event: RuntimeEventInput {
                                stream_id: stream.clone(),
                                scope: event.scope,
                                kind: event.kind,
                                status: event.status,
                                actor: event.actor,
                                refs: event.refs,
                                payload: event.payload,
                            },
                            idempotency_key: event.idempotency_key,
                            schema_version: event.schema_version,
                        }],
                    })
                    .unwrap();
            }
            assert!(
                confirmed_run_exit(&negative_store, &packet).is_err(),
                "{defect}"
            );
        }
    }

    #[tokio::test]
    async fn exact_exit_legacy_terminal_is_not_a_local_witness() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        let packet = task("exact-exit-legacy");
        let returned = CompletedBackend(AgentBackendKind::InProcess)
            .execute(
                packet.clone(),
                AgentModelSelection {
                    model: "fast".into(),
                    provider: "test".into(),
                    registry_revision: 1,
                },
            )
            .await
            .unwrap();
        runtime
            .import_legacy_state_records(
                "exact-exit-legacy-source",
                vec![LegacyAgentStateRecord {
                    source_ref: "legacy/exact-exit".into(),
                    snapshot: legacy_snapshot(&packet, AgentStatus::Completed),
                    returned: Some(returned),
                }],
            )
            .unwrap();
        assert!(confirmed_run_exit(&runtime.event_store, &packet)
            .unwrap()
            .is_none());
        let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
        assert!(confirmed_run_exit(&restarted.event_store, &packet)
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn exact_exit_physical_sequences_fence_old_commands_and_receipts() {
        let (runtime, backend) = held_exit_runtime(Some(AgentTerminalStatus::Completed), false);
        let packet = task("exact-exit-command-revisions");
        let owner = runtime.clone();
        let supplied = packet.clone();
        let caller = tokio::spawn(async move { owner.execute_task(supplied).await });
        backend.entered.notified().await;
        let stream = agent_stream_id(packet.agent_id());
        let running = runtime
            .event_store
            .latest_for_stream_kind(&stream, "agent.running")
            .unwrap()
            .unwrap();
        assert!(runtime.get(packet.agent_id()).unwrap().revision > running.sequence);
        let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
        assert_eq!(
            restarted.get(packet.agent_id()).unwrap().revision,
            runtime.event_store.stream_revision(&stream).unwrap()
        );
        let stale = runtime
            .command(AgentCommandRequest {
                command_id: "before-physical-sequence".into(),
                agent_id: packet.agent_id().into(),
                expected_revision: running.sequence,
                command: AgentCommand::Cancel,
                input: None,
            })
            .await;
        assert!(!stale.accepted);
        assert_eq!(
            stale.reject_reason,
            Some(AgentCommandRejectReason::StaleRevision)
        );
        let receipt = runtime
            .command(AgentCommandRequest {
                command_id: "current-physical-sequence".into(),
                agent_id: packet.agent_id().into(),
                expected_revision: runtime.get(packet.agent_id()).unwrap().revision,
                command: AgentCommand::Cancel,
                input: None,
            })
            .await;
        assert!(receipt.accepted);
        let command = runtime
            .event_store
            .latest_for_stream_kind(&stream, "agent.command")
            .unwrap()
            .unwrap();
        let payload: PersistedAgentEvent = serde_json::from_value(command.payload).unwrap();
        assert_eq!(receipt.accepted_revision, command.sequence);
        assert_eq!(payload.receipt.unwrap().accepted_revision, command.sequence);
        assert_eq!(
            runtime.get(packet.agent_id()).unwrap().revision,
            command.sequence
        );
        backend.release.notify_one();
        caller.await.unwrap().unwrap();
        assert!(runtime.get(packet.agent_id()).unwrap().revision > receipt.accepted_revision);
    }

    #[tokio::test]
    async fn exact_exit_cold_lookup_crosses_arbitrary_physical_pages_at_a_fixed_head() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let packet = task("exact-exit-keyset");
        runtime.execute_task(packet.clone()).await.unwrap();
        let stream = agent_stream_id(packet.agent_id());
        for index in 0..400 {
            runtime
                .event_store
                .append(RuntimeEventInput {
                    stream_id: stream.clone(),
                    scope: RuntimeEventScope::Agent,
                    kind: "test.physical_page_tail".into(),
                    status: None,
                    actor: Some("test".into()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"index": index}),
                })
                .unwrap();
        }
        let head = runtime.event_store.stream_revision(&stream).unwrap();
        let changed = AtomicBool::new(false);
        let (found, pinned) = runtime
            .latest_agent_payload_at_head(packet.agent_id(), |_| {
                if !changed.swap(true, Ordering::SeqCst) {
                    let mut snapshot = runtime.get(packet.agent_id()).unwrap();
                    snapshot.status = AgentStatus::Cancelled;
                    runtime
                        .persist_snapshot(
                            snapshot,
                            "agent.command_rejected",
                            "test concurrent logical head",
                            None,
                            None,
                        )
                        .unwrap();
                }
                true
            })
            .unwrap();
        assert!(changed.load(Ordering::SeqCst));
        assert_eq!(pinned, head);
        assert_eq!(found.unwrap().snapshot.status, AgentStatus::Completed);
        let restarted = AgentRuntime::new(runtime.event_store.clone(), configured_registry());
        let current = restarted.get(packet.agent_id()).unwrap();
        assert_eq!(current.status, AgentStatus::Cancelled);
        assert_eq!(
            current.revision,
            runtime.event_store.stream_revision(&stream).unwrap()
        );
        assert!(restarted.terminal_return(packet.agent_id()).is_some());
    }

    struct ClaimLifetimeBackend {
        kind: AgentBackendKind,
        services: Arc<RuntimeServices>,
        claim: harness_contract::agent_action::AgentActionEnvelope,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        claim_first: bool,
    }

    #[async_trait]
    impl AgentRuntimeBackend for ClaimLifetimeBackend {
        fn kind(&self) -> AgentBackendKind {
            self.kind
        }
        fn capabilities(&self) -> AgentBackendCapabilities {
            CompletedBackend(self.kind).capabilities()
        }
        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            if self.claim_first {
                assert_eq!(
                    self.services
                        .submit_agent_action(&self.claim)
                        .await
                        .unwrap()
                        .status,
                    harness_contract::agent_action::AgentActionStatus::Applied
                );
            }
            self.entered.notify_one();
            self.release.notified().await;
            if self.claim_first {
                CompletedBackend(self.kind).execute(packet, selection).await
            } else {
                Err("backend failed before choosing work".into())
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn common_agent_lifetime_renews_and_settles_native_and_process_opportunities() {
        use harness_contract::agent::{AgenticExecutionBinding, AgenticExecutionFocus};
        use harness_contract::agent_action::*;
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeSpec,
        };
        for kind in [AgentBackendKind::InProcess, AgentBackendKind::ProcessJsonl] {
            for claim_first in [false, true] {
                let services = RuntimeServices::in_memory().unwrap();
                let actions = services.agent_action_service();
                let mut actor = AgentActorBinding {
                    objective_id: "lifetime-objective".into(),
                    program_id: "lifetime-program".into(),
                    session_id: "session-1".into(),
                    turn_id: "lifetime-turn".into(),
                    root_execution_id: None,
                    required_team_count: 1,
                    exact_team_count: None,
                    objective_summary: "preserve worker truth".into(),
                    model_lease: "fast".into(),
                    permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                    resource_scopes: vec![],
                    actor_id: "root".into(),
                    kind: AgentActorKind::Root,
                    execution_id: None,
                    team_id: None,
                    agent_id: None,
                };
                let apply = |id: &str, actor: &AgentActorBinding, action| {
                    actions
                        .apply(&AgentActionEnvelope {
                            action_id: id.into(),
                            actor: actor.clone(),
                            expected_revision: None,
                            action,
                        })
                        .unwrap()
                };
                let team = apply(
                    "team",
                    &actor,
                    AgentAction::TeamCreate(TeamCreateInput {
                        name: "Readers".into(),
                        mission: "read evidence".into(),
                        objective: None,
                    }),
                )
                .changed_refs[0]
                    .clone();
                let member = apply("member", &actor, AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
                    "team_ref":team,"role":"Reader","mission":"inspect evidence","required_capabilities":["read"]
                })).unwrap())).changed_refs[0].clone();
                let work = apply("task", &actor, AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
                    "team_ref":team,"title":"Evidence","objective":"inspect","acceptance":"cited source"
                })).unwrap())).changed_refs[0].clone();
                let mut packet = task("lifetime");
                packet.agentic_binding = Some(AgenticExecutionBinding {
                    program_id: "lifetime-program".into(),
                    agent_id: member.clone(),
                    membership_id: format!("membership:{member}:{team}"),
                    team_id: team.clone(),
                    task_team_id: team.clone(),
                    source_spec_revision: 1,
                    task_purpose: None,
                    focus: AgenticExecutionFocus::TaskExecute {
                        task_ref: work.clone(),
                    },
                });
                // Bind an actually approved Definition: attaching services
                // enables the production content/digest validator, so the
                // standalone test packet's invented binding is not authority.
                let registry = services.definition_registry();
                let base = registry
                    .resolve_agent(
                        &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
                        RevisionSelector::LatestApprovedStable,
                    )
                    .unwrap();
                let mut manifest = base.revision.manifest.clone();
                manifest.definition_id =
                    AgentDefinitionId::new(DefinitionScope::Workspace, "tests/lifetime").unwrap();
                manifest.model_policy.profile = "test".into();
                manifest.model_policy.allowed_models = vec!["fast".into()];
                manifest.model_policy.fallback_allowed = false;
                if kind == AgentBackendKind::ProcessJsonl {
                    manifest.executor =
                        harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                            command_ref: "test/lifetime".into(),
                            command_digest: "a".repeat(64),
                        };
                }
                let stored = registry
                    .agents()
                    .store_revision(manifest, &base.agent_markdown)
                    .unwrap();
                registry
                    .agents()
                    .record_release_assignment(&harness_contract::agent::ReleaseAssignment {
                        scope: DefinitionScope::Workspace,
                        revision_ref: stored.revision.revision_ref.clone(),
                        channel: harness_contract::agent::ReleaseChannel::Stable,
                        status: harness_contract::agent::ReleaseAssignmentStatus::Active,
                        authorization:
                            harness_contract::agent::ReleaseAuthorization::HumanApproval {
                                approval_ref: "test-fixture:lifetime".into(),
                            },
                        content_digest: stored.revision.content_digest,
                    })
                    .unwrap();
                let mut request = crate::AgentBindingRequest::new(
                    stored.revision.revision_ref.definition_id,
                    RevisionSelector::LatestApprovedStable,
                    packet.agent_id(),
                    packet.session_id(),
                    packet.task_id(),
                );
                request.team_id = Some(team.clone());
                request.granted_capabilities = vec![AgentCapability::Read];
                let binding = crate::AgentBindingCompiler::new(Arc::clone(registry))
                    .compile(request)
                    .unwrap()
                    .snapshot;
                packet.assignment = crate::test_support::agent_assignment(
                    Some(binding.definition_ref.clone()),
                    packet.agent_id(),
                    packet.run_id(),
                    packet.task_id(),
                    packet.session_id(),
                    packet.mission_id(),
                    Some(&team),
                    packet.graph_id(),
                    packet.node_id(),
                );
                packet.binding = Some(binding);
                let mut graph = ExecutionGraph::new("lifetime");
                graph.id = packet.graph_id().into();
                graph.lineage = Some(ExecutionGraphLineage {
                    session_id: packet.session_id().into(),
                    turn_id: "lifetime-turn".into(),
                    root_task_id: packet.task_id().into(),
                    task_id: packet.task_id().into(),
                    generation: 1,
                });
                let mut node = ExecutionNodeSpec::new(
                    ExecutionNodeKind::AgentTask,
                    "agent_task",
                    serde_json::to_string(&packet).unwrap(),
                );
                node.id = packet.node_id().into();
                graph
                    .node_statuses
                    .insert(node.id.clone(), ExecutionNodeStatus::Planned);
                graph.nodes.push(node);
                services.commit_service().register_graph(graph).unwrap();
                actor.kind = AgentActorKind::Supervisor;
                actor.actor_id = "runtime.program-supervisor".into();
                actor.execution_id = Some(packet.graph_id().into());
                assert_eq!(
                    apply(
                        "dispatch",
                        &actor,
                        AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                            task_ref: work.clone(),
                            execution_id: packet.graph_id().into(),
                            agent_ref: member.clone(),
                            membership_id: format!("membership:{member}:{team}"),
                            mode: AgentAttemptMode::Execute,
                            generation: 0,
                        })
                    )
                    .status,
                    AgentActionStatus::Applied
                );
                actor.kind = AgentActorKind::Agent;
                actor.actor_id = member.clone();
                actor.agent_id = Some(member);
                actor.team_id = Some(team);
                let entered = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(tokio::sync::Notify::new());
                let runtime = Arc::new(AgentRuntime::new(
                    services.event_store().clone(),
                    configured_registry(),
                ));
                runtime.bind_services(services.clone());
                let normalized = runtime.ensure_runtime_binding(packet.clone()).unwrap();
                assert_eq!(
                    normalized.agentic_binding.as_ref().unwrap().task_purpose,
                    Some(harness_contract::agent_action::TaskPurpose::Delivery)
                );
                let mut tampered = packet.clone();
                tampered.agentic_binding.as_mut().unwrap().task_purpose =
                    Some(harness_contract::agent_action::TaskPurpose::Exploration);
                assert!(runtime
                    .ensure_runtime_binding(tampered)
                    .unwrap_err()
                    .contains("purpose differs"));
                runtime.register_backend(Arc::new(ClaimLifetimeBackend {
                    kind,
                    services: services.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                    claim_first,
                    claim: AgentActionEnvelope {
                        action_id: "agent-claim".into(),
                        actor,
                        expected_revision: None,
                        action: AgentAction::TaskClaim(TaskClaimInput {
                            task_ref: work.clone(),
                            reason: None,
                        }),
                    },
                }));
                let executing = runtime.clone();
                let run_packet = packet.clone();
                let mut worker =
                    tokio::spawn(async move { executing.execute_task(run_packet).await });
                tokio::select! {
                    _ = entered.notified() => {},
                    early = &mut worker => panic!("{kind:?} did not enter backend: {early:?}"),
                }
                let before = actions.project("lifetime-program").unwrap();
                let logical_member = &packet.agentic_binding.as_ref().unwrap().agent_id;
                assert!(runtime
                    .running_agentic_members("lifetime-program")
                    .contains(logical_member));
                assert!(runtime
                    .running_agentic_members("foreign-program")
                    .is_empty());
                let recovered =
                    AgentRuntime::new(services.event_store().clone(), configured_registry());
                assert!(
                    recovered
                        .running_agentic_members("lifetime-program")
                        .contains(logical_member),
                    "typed busy identity survives cold journal reconstruction without display metadata"
                );
                let mut commits = services.event_store().subscribe_commits();
                // The heartbeat reads the graph on a blocking worker. A
                // fixed number of yields is not evidence that it finished,
                // particularly while other tests occupy those workers.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    tokio::time::advance(std::time::Duration::from_secs(31)).await;
                    // Allow the actual blocking read and the newly scheduled
                    // interval to initialize, then observe its durable event.
                    tokio::time::resume();
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        commits.changed(),
                    )
                    .await;
                    tokio::time::pause();
                    if !claim_first
                        || actions.project("lifetime-program").unwrap().revision > before.revision
                    {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "{kind:?} real heartbeat journal commit timed out"
                    );
                }
                let during = actions.project("lifetime-program").unwrap();
                if claim_first {
                    assert!(
                        during.revision > before.revision,
                        "{kind:?} must renew its real claim"
                    );
                    assert_eq!(during.tasks[&work].claim_generation, 1);
                } else {
                    assert_eq!(
                        during.revision, before.revision,
                        "Runtime must not claim for the Agent"
                    );
                }
                release.notify_one();
                worker.await.unwrap().unwrap();
                let after = actions.project("lifetime-program").unwrap();
                if kind == AgentBackendKind::InProcess {
                    assert!(runtime
                        .running_agentic_members("lifetime-program")
                        .is_empty());
                    assert!(!after.tasks[&work]
                        .active_attempts
                        .contains_key(packet.graph_id()));
                    assert_eq!(after.tasks[&work].failed_attempts, 1);
                    assert_eq!(after.tasks[&work].claim_generation, 1);
                } else {
                    assert!(runtime
                        .running_agentic_members("lifetime-program")
                        .contains(logical_member));
                    assert!(confirmed_run_exit(&runtime.event_store, &packet)
                        .unwrap()
                        .is_none());
                    assert!(after.tasks[&work]
                        .active_attempts
                        .contains_key(packet.graph_id()));
                    assert_eq!(
                        after.tasks[&work].active_attempts.len(),
                        during.tasks[&work].active_attempts.len()
                    );
                    assert_eq!(after.tasks[&work].failed_attempts, 0);
                    assert_eq!(
                        after.tasks[&work].claim_generation,
                        during.tasks[&work].claim_generation
                    );
                }
                tokio::time::advance(std::time::Duration::from_secs(60)).await;
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    actions.project("lifetime-program").unwrap().revision,
                    after.revision,
                    "backend return must stop the common lease keeper"
                );
            }
        }
    }

    #[async_trait]
    impl AgentRuntimeBackend for CompletedBackend {
        fn kind(&self) -> AgentBackendKind {
            self.0
        }

        fn capabilities(&self) -> AgentBackendCapabilities {
            match self.0 {
                AgentBackendKind::InProcess => AgentBackendCapabilities::in_process(),
                AgentBackendKind::ProcessJsonl => AgentBackendCapabilities::process_jsonl(),
            }
        }

        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            Ok(AgentReturnPacket {
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                session_id: packet.session_id().to_string(),
                mission_id: packet.mission_id().to_string(),
                team_id: packet.team_id().map(ToString::to_string),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: "completed".into(),
                answer_candidate: None,
                observed_acceptance: harness_contract::context::ObservedAcceptance {
                    satisfied_criteria: vec!["verified".into()],
                    observed_evidence: Vec::new(),
                    unresolved_obligation_ids: Vec::new(),
                },
                acceptance_evaluation: None,
                acceptance: vec!["verified".into()],
                evidence_refs: Vec::new(),
                changes: Vec::new(),
                runtime_change_receipts: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 3,
                output_tokens: 2,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                cached_tokens: 0,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 0,
                duplicate_tool_calls: 0,
                max_tool_concurrency_observed: 0,
                parallel_tool_batches: 0,
                runtime_write_attempt_paths: Vec::new(),
                runtime_observed_resource_scopes: Vec::new(),
                failure: None,
            })
        }

        async fn command(
            &self,
            _handle: &AgentRunHandle,
            _request: &AgentCommandRequest,
        ) -> Result<(), AgentCommandRejectReason> {
            Ok(())
        }
    }

    fn configured_registry() -> Arc<ProviderRegistry> {
        Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".into(),
                    ProviderConfig {
                        name: "test".into(),
                        base_url: "https://example.test/v1".into(),
                        api_key: "test".into(),
                        models: vec!["fast".into()],
                        protocol: Some("responses".into()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("valid provider registry"),
        )
    }

    fn test_binding(agent_id: &str) -> AgentBindingSnapshot {
        AgentBindingSnapshot {
            binding_id: format!("binding:{agent_id}"),
            definition_ref: harness_contract::agent::AgentDefinitionRevisionRef {
                definition_id: AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct")
                    .expect("builtin id"),
                revision: 1,
            },
            definition_digest: "a".repeat(64),
            instructions: "# Test\n\nComplete the test task.\n".to_string(),
            instance: harness_contract::agent::AgentInstanceRef {
                instance_id: format!("instance:{agent_id}"),
                role_slot_id: None,
            },
            executor: harness_contract::agent::AgentExecutorPolicy::CowdNative,
            model_policy: harness_contract::agent::AgentModelPolicy {
                profile: "test".to_string(),
                allowed_models: vec!["fast".to_string()],
                fallback_allowed: true,
            },
            effective_capabilities: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            skill_tool_pins: None,
            skill_tool_refs: None,
            tool_contract_refs: Vec::new(),
            data_lease: harness_contract::agent::AgentDataLease {
                session_id: "session-1".to_string(),
                task_id: "task-1".to_string(),
                team_id: None,
                read_scopes: vec![harness_contract::agent::CognitiveReadScope::Session],
                write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
                fact_boundaries: Vec::new(),
                fact_refs: Vec::new(),
                matrix_snapshot_refs: Vec::new(),
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "b".repeat(64),
        }
    }

    #[test]
    fn stable_and_process_bindings_reject_unknown_skill_pins_before_definition_or_launch() {
        let services = RuntimeServices::in_memory().unwrap();
        services.replace_skill_catalog(
            crate::RuntimeSkillCatalog::default().with_instruction_source(Arc::new(
                crate::skill::test_support::Instructions(vec![], vec![]),
            )),
        );
        let runtime = AgentRuntime::new(services.event_store().clone(), configured_registry());
        runtime.bind_services(services.clone());
        for process in [false, true] {
            let mut packet = task("revoked-skill");
            let binding = packet.binding.as_mut().unwrap();
            binding.skill_refs = vec!["review".into()];
            binding.skill_tool_pins = Some(vec![crate::skill::test_support::pin(
                "review",
                None,
                "OLD",
                &["read_file"],
            )]);
            binding.skill_tool_refs = Some(vec!["read_file".into()]);
            binding.tool_contract_refs = vec!["read_file".into()];
            if process {
                binding.executor = harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                    command_ref: "external/worker".into(),
                    command_digest: "a".repeat(64),
                };
            }
            let error = runtime.ensure_runtime_binding(packet).unwrap_err();
            assert!(
                error.contains("unit approval does not authenticate this pin"),
                "{error}"
            );
        }
    }

    fn task(agent_id: &str) -> AgentTaskPacket {
        let binding = test_binding(agent_id);
        let instance_id = binding.instance.instance_id.clone();
        let definition_ref = binding.definition_ref.clone();
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                Some(definition_ref),
                &instance_id,
                &format!("run-{agent_id}"),
                "task-1",
                "session-1",
                "mission-1",
                None,
                "graph-1",
                "node-1",
            ),
            attempt: 1,
            expected_graph_revision: 1,
            policy_revision: 1,
            objective: "verify lifecycle".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: vec!["verified".into()],
            cohort_prompt_package: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "fast".into(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget-1",
                instance_id,
                "agent",
                1_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: Some(binding),
            managed_invocation: None,
            idempotency_key: format!("idempotency-{agent_id}"),
            agentic_binding: None,
        }
    }

    fn team_task(agent_id: &str, team_run_id: &str) -> AgentTaskPacket {
        let mut packet = task(agent_id);
        let definition_ref = packet.assignment.definition_ref.clone();
        packet.assignment = crate::test_support::agent_assignment(
            Some(definition_ref),
            packet.agent_id(),
            packet.run_id(),
            packet.task_id(),
            packet.session_id(),
            packet.mission_id(),
            Some(team_run_id),
            packet.graph_id(),
            packet.node_id(),
        );
        if let Some(binding) = packet.binding.as_mut() {
            binding.data_lease.team_id = Some(team_run_id.to_string());
        }
        packet
    }

    #[test]
    fn process_backend_uses_the_explicit_runtime_bridge_kind() {
        let mut packet = team_task("external-team", "team-1");
        packet.binding.as_mut().expect("binding").executor =
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                command_ref: "external/worker".to_string(),
                command_digest: "a".repeat(64),
            };

        let backend = backend_from_packet(&packet).expect("bound ProcessJsonl backend");
        assert_eq!(backend, AgentBackendKind::ProcessJsonl);
        assert!(ensure_team_backend_trusted(&packet, backend).is_ok());
    }

    #[test]
    fn mcp_and_manual_review_definitions_are_not_agent_task_backends() {
        let mut packet = task("unsupported-backend");
        for (executor, expected) in [
            (
                harness_contract::agent::AgentExecutorPolicy::McpBacked {
                    server_ref: "mcp:research".to_string(),
                    tool_prefixes: vec!["research_".to_string()],
                },
                "McpBacked Agent definitions have no Runtime AgentTask backend",
            ),
            (
                harness_contract::agent::AgentExecutorPolicy::ManualReview,
                "ManualReview Agent definitions require the approval workflow",
            ),
        ] {
            packet.binding.as_mut().expect("binding").executor = executor;
            assert!(backend_from_packet(&packet)
                .expect_err("unsupported definition must not start an AgentTask backend")
                .contains(expected));
        }
    }

    #[tokio::test]
    async fn process_agent_does_not_require_a_local_provider_model() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store, Arc::new(ProviderRegistry::empty()));
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::ProcessJsonl,
        )));
        let mut packet = team_task("external-process", "team-1");
        let binding = packet.binding.as_mut().expect("binding");
        binding.executor = harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
            command_ref: "external/worker".to_string(),
            command_digest: "a".repeat(64),
        };
        binding.model_policy.profile = "external-model-profile".to_string();
        binding.model_policy.allowed_models = vec!["external-model-profile".to_string()];
        packet.model_lease = "not-registered-locally".to_string();

        let returned = runtime.execute_task(packet.clone()).await.expect("run");

        assert_eq!(
            returned.status,
            AgentTerminalStatus::Failed,
            "the fake Process backend has no ToolHost receipts, so Runtime must reject its claimed acceptance: {returned:#?}"
        );
        assert_eq!(returned.model, "external-model-profile");
        assert_eq!(returned.provider, "external_process");
        assert!(returned
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("non-satisfied Runtime acceptance")));
        assert_eq!(
            runtime.get(packet.agent_id()).expect("projection").status,
            AgentStatus::Failed
        );
    }

    #[tokio::test]
    async fn graph_cancel_before_agent_poll_persists_terminal_cancelled_without_backend_work() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store, configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let packet = task("cancel-before-poll");

        AgentTaskBackend::cancel(&runtime, &packet)
            .await
            .expect("queue graph cancellation");
        let returned = runtime
            .execute_task(packet.clone())
            .await
            .expect("cancelled return");

        assert_eq!(returned.status, AgentTerminalStatus::Cancelled);
        assert_eq!(
            runtime
                .get(packet.agent_id())
                .expect("cancelled projection")
                .status,
            AgentStatus::Cancelled
        );
        assert!(runtime
            .events(packet.agent_id())
            .iter()
            .any(|event| event.status == AgentStatus::Cancelled));
    }

    #[tokio::test]
    async fn public_backend_registration_cannot_self_promote_observation_truth() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store, configured_registry());
        runtime.register_backend(Arc::new(CompletedBackend(AgentBackendKind::InProcess)));

        let returned = runtime
            .execute_task(task("untrusted-observation"))
            .await
            .expect("terminal packet");

        assert_eq!(returned.status, AgentTerminalStatus::Failed);
        assert!(returned.observed_acceptance.observed_evidence.is_empty());
        assert!(returned
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("non-satisfied Runtime acceptance verdict")));
    }

    #[tokio::test]
    async fn rejected_backend_result_persists_a_terminal_failed_projection() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store, configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let mut packet = team_task("invalid-acceptance", "team-1");
        packet.acceptance = vec!["evidence".to_string()];
        packet.output_acceptance = vec![harness_contract::agent::OutputAcceptanceRequirement {
            criterion: "evidence".to_string(),
            check: harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence {
                scopes: vec!["read:src".to_string()],
            },
        }];

        let returned = runtime
            .execute_task(packet.clone())
            .await
            .expect("validation failure is a terminal Agent result");

        assert_eq!(returned.status, AgentTerminalStatus::Failed);
        assert!(returned
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("Runtime rejected Agent terminal result")));
        assert_eq!(returned.input_tokens, 3);
        assert_eq!(returned.output_tokens, 2);
        assert_eq!(
            returned
                .acceptance_evaluation
                .as_ref()
                .expect("terminal rejection retains evaluator output")
                .verdict,
            harness_contract::acceptance::AcceptanceVerdict::FrameworkInvalid
        );
        // This backend did not execute a governed tool, so the failed result
        // must preserve that empty evidence set rather than fabricate the
        // historical `read_file` fixture. The framework-invalid verdict and
        // receipt snapshot above remain durable facts.
        assert!(returned.observed_acceptance.observed_evidence.is_empty());
        assert_eq!(
            runtime
                .get(packet.agent_id())
                .expect("terminal projection")
                .status,
            AgentStatus::Failed
        );
        assert!(runtime
            .events(packet.agent_id())
            .iter()
            .any(|event| event.status == AgentStatus::Failed));
    }

    #[test]
    fn stale_prepare_or_running_snapshot_cannot_overwrite_terminal_cancellation() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store, configured_registry());
        let packet = task("cancel-cas");
        let prepared = legacy_snapshot(&packet, AgentStatus::Prepared);
        runtime
            .persist_snapshot(prepared, "agent.prepared", "prepared", None, None)
            .expect("prepare");
        let stale_prepared = runtime.get(packet.agent_id()).expect("prepared projection");
        let mut cancelled = stale_prepared.clone();
        cancelled.status = AgentStatus::Cancelled;
        runtime
            .persist_snapshot(cancelled, "agent.command", "cancelled", None, None)
            .expect("cancel");

        let mut stale_running = stale_prepared;
        stale_running.status = AgentStatus::Running;
        assert!(runtime
            .persist_snapshot(stale_running, "agent.running", "running", None, None)
            .is_err());
        assert_eq!(
            runtime.get(packet.agent_id()).expect("terminal").status,
            AgentStatus::Cancelled
        );
    }

    fn legacy_snapshot(packet: &AgentTaskPacket, status: AgentStatus) -> AgentRunSnapshot {
        AgentRunSnapshot {
            agentic_binding: packet.agentic_binding.clone(),
            execution_identity: packet.assignment.execution_identity.clone(),
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            root_task_id: packet.assignment.root_task_id.clone(),
            session_id: packet.session_id().to_string(),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            backend: AgentBackendKind::InProcess,
            status,
            revision: 0,
            model: Some("fast".into()),
            provider: Some("test".into()),
            binding: None,
            started_at_ms: 1,
            updated_at_ms: 1,
            failure: None,
        }
    }

    #[test]
    fn graph_scoped_agent_lookup_uses_replayable_index() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store, configured_registry());
        let packet = task("graph-indexed");
        runtime
            .restore_verified_run(legacy_snapshot(&packet, AgentStatus::Completed))
            .expect("restore indexed Agent");

        assert_eq!(
            runtime
                .list_for_graphs(&BTreeSet::from(["graph-1".to_string()]))
                .iter()
                .map(|snapshot| snapshot.agent_id.as_str())
                .collect::<Vec<_>>(),
            vec![packet.agent_id()],
        );
        assert!(runtime
            .list_for_graphs(&BTreeSet::from(["graph-other".to_string()]))
            .is_empty());
    }

    #[tokio::test]
    async fn legacy_import_is_atomic_idempotent_and_blocks_unrecoverable_active_runs() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        let active = task("legacy-active");
        let completed = task("legacy-completed");
        let returned = return_packet(
            &completed,
            AgentTerminalStatus::Completed,
            "completed before upgrade".into(),
            None,
        );
        let report = runtime
            .import_legacy_state_records(
                "upgrade-manifest-sha256",
                vec![
                    LegacyAgentStateRecord {
                        source_ref: "legacy/active.json".into(),
                        snapshot: legacy_snapshot(&active, AgentStatus::Running),
                        returned: None,
                    },
                    LegacyAgentStateRecord {
                        source_ref: "legacy/completed.json".into(),
                        snapshot: legacy_snapshot(&completed, AgentStatus::Completed),
                        returned: Some(returned.clone()),
                    },
                ],
            )
            .expect("import succeeds");
        assert!(!report.duplicate);
        assert_eq!(report.imported_agent_ids.len(), 2);
        assert_eq!(
            report.blocked_agent_ids,
            vec![active.agent_id().to_string()]
        );
        assert_eq!(
            runtime
                .get(active.agent_id())
                .expect("active projection")
                .status,
            AgentStatus::Blocked
        );
        let replay = runtime.execute_task(completed.clone()).await;
        let refusal = replay
            .err()
            .expect("a legacy imported terminal has no exact durable exit");
        assert!(
            refusal.contains("no exact durable exit"),
            "the legacy replay is refused for the missing durable exit: {refusal}"
        );
        let imported = store
            .list_stream(&agent_stream_id(completed.agent_id()))
            .expect("legacy import stream")
            .into_iter()
            .find(|event| event.kind == "agent.legacy_imported")
            .expect("the legacy import stays a durable historical fact");
        let imported: PersistedAgentEvent =
            serde_json::from_value(imported.payload).expect("legacy import payload");
        assert_eq!(imported.returned, Some(returned.clone()));

        let replayed = AgentRuntime::new(Arc::clone(&store), configured_registry());
        assert_eq!(
            replayed
                .get(completed.agent_id())
                .expect("terminal replay")
                .status,
            AgentStatus::Completed
        );
        let duplicate = replayed
            .import_legacy_state_records("upgrade-manifest-sha256", Vec::new())
            .expect("duplicate marker");
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.imported_agent_ids.len(), 2);
    }

    #[test]
    fn legacy_import_rejects_unbound_records_without_partial_writes() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        let valid = task("legacy-valid");
        let mut invalid_snapshot = legacy_snapshot(&task("legacy-invalid"), AgentStatus::Running);
        invalid_snapshot.graph_id.clear();
        runtime
            .import_legacy_state_records(
                "bad-upgrade-manifest",
                vec![
                    LegacyAgentStateRecord {
                        source_ref: "legacy/valid.json".into(),
                        snapshot: legacy_snapshot(&valid, AgentStatus::Running),
                        returned: None,
                    },
                    LegacyAgentStateRecord {
                        source_ref: "legacy/invalid.json".into(),
                        snapshot: invalid_snapshot,
                        returned: None,
                    },
                ],
            )
            .expect_err("unbound records are rejected");
        assert!(runtime.get(valid.agent_id()).is_none());
        assert!(store
            .list_stream(&legacy_import_stream_id("bad-upgrade-manifest"))
            .expect("marker stream")
            .is_empty());
    }

    #[tokio::test]
    async fn terminal_lifecycle_replays_from_the_event_store() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let packet = task("agent-replay");

        let returned = runtime.execute_task(packet.clone()).await.expect("run");
        assert_eq!(returned.status, AgentTerminalStatus::Completed);
        assert_eq!(
            runtime.get(packet.agent_id()).unwrap().status,
            AgentStatus::Completed
        );
        assert_eq!(runtime.events(packet.agent_id()).len(), 3);
        let terminal = store
            .list_stream(&agent_stream_id(packet.agent_id()))
            .expect("agent event stream")
            .into_iter()
            .last()
            .expect("terminal agent event");
        for (kind, id) in [
            ("principal", "test.principal"),
            ("workspace", "test-workspace"),
            ("mission", packet.mission_id()),
            ("task", packet.task_id()),
            ("session", packet.session_id()),
            ("turn", "test-turn"),
            ("execution_graph", packet.graph_id()),
            ("agent_instance", packet.agent_id()),
            ("agent_run", packet.run_id()),
            ("node", packet.node_id()),
        ] {
            assert!(
                terminal
                    .refs
                    .iter()
                    .any(|reference| reference.kind == kind && reference.id == id),
                "terminal Agent event must retain {kind}:{id}"
            );
        }
        let evaluations = runtime.evaluations();
        assert_eq!(evaluations.len(), 1);
        assert_eq!(evaluations[0].definition_revision, 1);
        assert_eq!(evaluations[0].binding_digest, "b".repeat(64));
        assert_eq!(runtime.self_models().len(), 1);
        AgentTaskBackend::terminal_committed(&runtime, &packet);
        assert!(
            !runtime
                .records
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(packet.agent_id()),
            "a graph-committed terminal Agent must leave the active hot projection"
        );
        assert_eq!(
            runtime
                .get(packet.agent_id())
                .map(|snapshot| snapshot.status),
            Some(AgentStatus::Completed),
            "exact history lookup must rehydrate from the durable stream"
        );
        let replayed_return = runtime
            .execute_task(packet.clone())
            .await
            .expect("replay result");
        assert_eq!(replayed_return, returned);
        assert_eq!(runtime.events(packet.agent_id()).len(), 3);

        let restored = AgentRuntime::new(store, configured_registry());
        assert_eq!(restored.evaluations().len(), 1);
        assert_eq!(restored.self_models()[0].run_count, 1);
        let snapshot = restored.get(packet.agent_id()).expect("replayed snapshot");
        assert_eq!(snapshot.status, AgentStatus::Completed);
        assert_eq!(snapshot.graph_id, packet.graph_id());
        assert_eq!(snapshot.node_id, packet.node_id());
        assert_eq!(
            restored
                .execute_task(packet)
                .await
                .expect("restored return"),
            returned
        );
    }

    #[test]
    fn team_agent_snapshot_refs_preserve_the_team_lineage() {
        let packet = team_task("team-lineage", "team-run-1");
        let snapshot = legacy_snapshot(&packet, AgentStatus::Running);
        let refs = snapshot_identity_refs(&snapshot);
        assert!(refs
            .iter()
            .any(|reference| reference.kind == "team_run" && reference.id == "team-run-1"));
        assert!(refs.iter().any(|reference| {
            reference.kind == "agent_instance" && reference.id == packet.agent_id()
        }));
    }

    #[test]
    fn public_lifecycle_projection_is_derived_from_canonical_agent_events() {
        assert_eq!(
            live_lifecycle_phase("agent.running", AgentStatus::Running),
            Some((AgentLifecyclePhase::Started, "running"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.provider.first_output", AgentStatus::Running),
            Some((AgentLifecyclePhase::FirstOutput, "running"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.acceptance.evaluated", AgentStatus::Running),
            Some((AgentLifecyclePhase::Evaluating, "running"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.terminal", AgentStatus::Completed),
            Some((AgentLifecyclePhase::Completed, "completed"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.terminal", AgentStatus::Failed),
            Some((AgentLifecyclePhase::Failed, "failed"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.prepared", AgentStatus::Prepared),
            None,
            "prepared is durable internal truth, not duplicate public start noise"
        );
    }

    #[tokio::test]
    async fn command_receipt_is_revisioned_and_idempotent() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let packet = task("agent-command");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                agentic_binding: packet.agentic_binding.clone(),
                execution_identity: packet.assignment.execution_identity.clone(),
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                session_id: packet.session_id().to_string(),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                backend: AgentBackendKind::InProcess,
                status: AgentStatus::Running,
                revision: 0,
                model: Some("fast".into()),
                provider: Some("test".into()),
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore");
        let revision = runtime.get(packet.agent_id()).unwrap().revision;
        let command = AgentCommandRequest {
            command_id: "command-1".into(),
            agent_id: packet.agent_id().to_string(),
            expected_revision: revision,
            command: AgentCommand::Interrupt,
            input: None,
        };
        let first = runtime.command(command.clone()).await;
        let duplicate = runtime.command(command).await;
        assert!(first.accepted);
        assert_eq!(first, duplicate);
        assert_eq!(runtime.events(packet.agent_id()).len(), 2);
    }

    #[tokio::test]
    async fn durable_input_replays_exact_payload_and_consumption_is_transactional() {
        use harness_contract::turn::{
            InputSourceKind, SessionInputEnvelope, TurnId, TurnInputCheckpoint,
        };
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = AgentRuntime::new(store.clone(), configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend(
            AgentBackendKind::InProcess,
        )));
        let packet = task("durable-input");
        runtime
            .restore_verified_run(legacy_snapshot(&packet, AgentStatus::Running))
            .unwrap();
        let request = AgentCommandRequest {
            command_id: "supplement".into(),
            agent_id: packet.agent_id().into(),
            expected_revision: runtime.get(packet.agent_id()).unwrap().revision,
            command: AgentCommand::SendInput,
            input: Some(harness_contract::agent::AgentInput::UserSupplement(
                "durable requirement".into(),
            )),
        };
        let receipt = runtime.command(request.clone()).await;
        assert!(receipt.accepted, "{receipt:?}");
        assert_eq!(runtime.command(request.clone()).await, receipt);
        let mut different_command = request.clone();
        different_command.command = AgentCommand::Cancel;
        assert!(!runtime.command(different_command).await.accepted);
        let mut premature_terminal = runtime.get(packet.agent_id()).unwrap();
        premature_terminal.status = AgentStatus::Completed;
        assert!(runtime
            .persist_snapshot(
                premature_terminal,
                "agent.terminal",
                "must not lose input",
                None,
                None
            )
            .is_err());
        let mut conflicting = request.clone();
        conflicting.input = Some(harness_contract::agent::AgentInput::UserSupplement(
            "other payload".into(),
        ));
        assert!(!runtime.command(conflicting).await.accepted);
        drop(runtime);
        let restored = AgentRuntime::new(store.clone(), configured_registry());
        assert_eq!(
            restored
                .pending_agent_inputs(packet.agent_id(), packet.run_id())
                .unwrap(),
            vec![request.clone()]
        );
        assert!(restored
            .pending_agent_inputs(packet.agent_id(), "different-run")
            .unwrap()
            .is_empty());
        let inbox = crate::SessionInputStream::new(packet.session_id());
        let turn = TurnId::from_string("input-checkpoint");
        inbox.set_active_turn(Some(turn.clone()));
        inbox.admit(
            SessionInputEnvelope::text(
                packet.session_id(),
                InputSourceKind::Agent,
                "durable requirement",
            )
            .with_source_ref(format!("agent-input:{}", packet.run_id()))
            .with_source_message_id("supplement"),
            inbox.runtime_state(),
        );
        let records =
            inbox.consume_for_checkpoint(&turn, TurnInputCheckpoint::BeforeProviderRequest, 32);
        assert_eq!(records.len(), 1);
        let mut forged = records.clone();
        forged[0].envelope.content = "forged".into();
        assert!(restored
            .agent_input_consumption_events(&packet, &forged)
            .is_err());
        let events = restored
            .agent_input_consumption_events(&packet, &records)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(store
            .append_transaction(AppendTransactionRequest {
                transaction_id: "input-consumption-fault".into(),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: "test-model-node".into(),
                        expected_revision: 1
                    },
                    ExpectedStreamRevision {
                        stream_id: events[0].event.stream_id.clone(),
                        expected_revision: 1
                    }
                ],
                events: events.clone(),
            })
            .is_err());
        assert_eq!(
            restored
                .pending_agent_inputs(packet.agent_id(), packet.run_id())
                .unwrap()
                .len(),
            1
        );
        store
            .append_transaction(AppendTransactionRequest {
                transaction_id: "input-consumption-commit".into(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: events[0].event.stream_id.clone(),
                    expected_revision: 1,
                }],
                events,
            })
            .unwrap();
        drop(restored);
        let recovered = AgentRuntime::new(store, configured_registry());
        assert!(recovered
            .pending_agent_inputs(packet.agent_id(), packet.run_id())
            .unwrap()
            .is_empty());
        assert_eq!(recovered.command(request).await, receipt);
    }

    struct PendingInputBackend {
        kind: AgentBackendKind,
        entered: Arc<tokio::sync::Notify>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl AgentRuntimeBackend for PendingInputBackend {
        fn kind(&self) -> AgentBackendKind {
            self.kind
        }
        fn capabilities(&self) -> AgentBackendCapabilities {
            CompletedBackend(self.kind).capabilities()
        }
        async fn execute(
            &self,
            _packet: AgentTaskPacket,
            _selection: AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            unreachable!("command-only fault fixture")
        }
        async fn command(
            &self,
            _handle: &AgentRunHandle,
            _request: &AgentCommandRequest,
        ) -> Result<(), AgentCommandRejectReason> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn abandoned_input_delivery_retains_native_intent_and_never_resends_uncertain_process() {
        for kind in [AgentBackendKind::InProcess, AgentBackendKind::ProcessJsonl] {
            let store = Arc::new(RuntimeEventStore::for_test());
            let runtime = Arc::new(AgentRuntime::new(store.clone(), configured_registry()));
            let entered = Arc::new(tokio::sync::Notify::new());
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            runtime.register_observation_authority_backend(Arc::new(PendingInputBackend {
                kind,
                entered: entered.clone(),
                calls: calls.clone(),
            }));
            let packet = task("abandoned-input");
            let mut snapshot = legacy_snapshot(&packet, AgentStatus::Running);
            snapshot.backend = kind;
            runtime.restore_verified_run(snapshot).unwrap();
            let request = AgentCommandRequest {
                command_id: "original-input".into(),
                agent_id: packet.agent_id().into(),
                expected_revision: runtime.get(packet.agent_id()).unwrap().revision,
                command: AgentCommand::SendInput,
                input: Some(harness_contract::agent::AgentInput::UserSupplement(
                    "keep this".into(),
                )),
            };
            let waiter = {
                let runtime = runtime.clone();
                let request = request.clone();
                tokio::spawn(async move { runtime.command(request).await })
            };
            entered.notified().await;
            waiter.abort();
            let _ = waiter.await;
            drop(runtime);
            let recovered = AgentRuntime::new(store, configured_registry());
            recovered.register_observation_authority_backend(Arc::new(PendingInputBackend {
                kind,
                entered,
                calls: calls.clone(),
            }));
            let receipt = recovered.command(request.clone()).await;
            assert_eq!(receipt.accepted, kind == AgentBackendKind::InProcess);
            if kind == AgentBackendKind::ProcessJsonl {
                assert!(receipt.message.contains("reconcile"));
            }
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                recovered
                    .pending_agent_inputs(packet.agent_id(), packet.run_id())
                    .unwrap(),
                vec![request]
            );
        }
    }

    #[test]
    fn progress_markers_preserve_running_lifecycle_without_becoming_terminal() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        let packet = task("agent-progress");
        runtime
            .restore_verified_run(legacy_snapshot(&packet, AgentStatus::Running))
            .expect("restore running agent");

        runtime
            .record_progress(
                packet.agent_id(),
                "agent.provider.first_output",
                "provider emitted the first output",
            )
            .expect("progress is durable");

        assert_eq!(
            runtime
                .get(packet.agent_id())
                .expect("agent projection")
                .status,
            AgentStatus::Running
        );
        let events = runtime.events(packet.agent_id());
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].kind, "agent.provider.first_output");
    }

    #[tokio::test]
    async fn unavailable_model_is_recorded_as_blocked_not_running() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            Arc::new(ProviderRegistry::empty()),
        );
        let packet = task("agent-blocked");
        let returned = runtime
            .execute_task(packet.clone())
            .await
            .expect("blocked return");
        assert_eq!(returned.status, AgentTerminalStatus::Blocked);
        let snapshot = runtime.get(packet.agent_id()).expect("blocked snapshot");
        assert_eq!(snapshot.status, AgentStatus::Blocked);
        assert!(snapshot.failure.is_some());
        assert_eq!(runtime.events(packet.agent_id()).len(), 1);
    }

    #[tokio::test]
    async fn agent_locks_are_keyed_and_reclaimed_after_the_last_holder() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );

        let first_run = runtime.acquire_run_lock("agent-a").await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                runtime.acquire_run_lock("agent-a")
            )
            .await
            .is_err(),
            "the same agent key must remain serialized"
        );
        let other_run = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            runtime.acquire_run_lock("agent-b"),
        )
        .await
        .expect("different agent keys must not share a global run lock");

        let lifecycle_a = runtime.lifecycle_lock("agent-a");
        let lifecycle_a_again = runtime.lifecycle_lock("agent-a");
        let lifecycle_b = runtime.lifecycle_lock("agent-b");
        assert!(Arc::ptr_eq(&lifecycle_a, &lifecycle_a_again));
        assert!(!Arc::ptr_eq(&lifecycle_a, &lifecycle_b));

        drop(lifecycle_a);
        drop(lifecycle_a_again);
        drop(lifecycle_b);
        drop(other_run);
        drop(first_run);
        assert_eq!(runtime.retained_lock_counts(), (0, 0));
    }

    #[test]
    fn restart_recovery_blocks_runs_without_a_recovered_handle() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        let packet = task("agent-recovery");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                agentic_binding: packet.agentic_binding.clone(),
                execution_identity: packet.assignment.execution_identity.clone(),
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                session_id: packet.session_id().to_string(),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                backend: AgentBackendKind::ProcessJsonl,
                status: AgentStatus::Running,
                revision: 0,
                model: Some("fast".into()),
                provider: Some("test".into()),
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore");

        assert_eq!(
            runtime.block_unrecoverable_replayed_runs().expect("block"),
            vec![packet.agent_id().to_string()]
        );
        let snapshot = runtime.get(packet.agent_id()).expect("blocked run");
        assert_eq!(snapshot.status, AgentStatus::Blocked);
        assert!(snapshot
            .failure
            .as_deref()
            .unwrap_or_default()
            .contains("backend handle"));
    }

    #[test]
    fn restart_recovery_leaves_prepared_runs_reclaimable() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        let packet = task("agent-prepared-recovery");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                agentic_binding: packet.agentic_binding.clone(),
                execution_identity: packet.assignment.execution_identity.clone(),
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                session_id: packet.session_id().to_string(),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                backend: AgentBackendKind::ProcessJsonl,
                status: AgentStatus::Prepared,
                revision: 0,
                model: Some("fast".into()),
                provider: Some("test".into()),
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore");

        assert!(runtime
            .block_unrecoverable_replayed_runs()
            .expect("recovery sweep")
            .is_empty());
        assert_eq!(
            runtime.get(packet.agent_id()).expect("prepared run").status,
            AgentStatus::Prepared
        );
    }

    #[test]
    fn restart_recovery_requeues_runtime_owned_runs() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::for_test()),
            configured_registry(),
        );
        let packet = task("agent-native-recovery");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                agentic_binding: packet.agentic_binding.clone(),
                execution_identity: packet.assignment.execution_identity.clone(),
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                session_id: packet.session_id().to_string(),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                backend: AgentBackendKind::InProcess,
                status: AgentStatus::Running,
                revision: 0,
                model: Some("fast".into()),
                provider: Some("test".into()),
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: Some("stale transient error".into()),
            })
            .expect("restore");

        assert!(runtime
            .block_unrecoverable_replayed_runs()
            .expect("recovery sweep")
            .is_empty());
        let snapshot = runtime.get(packet.agent_id()).expect("requeued run");
        assert_eq!(snapshot.status, AgentStatus::Prepared);
        assert_eq!(snapshot.failure, None);
    }
}
