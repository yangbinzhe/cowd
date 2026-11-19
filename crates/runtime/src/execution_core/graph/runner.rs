use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, RwLock, Weak};
use std::time::Duration;
use futures::{future::{BoxFuture, Shared}, FutureExt};

use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionGraph, ExecutionGraphCommand, ExecutionGraphValidationError,
    ExecutionNodeResult, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{futures::OwnedNotified, oneshot, Mutex, Notify, OwnedMutexGuard};

use super::commit_service::{
    CallbackDisposition, ExecutionCommitError, ExecutionCommitService, ExecutionEffectState,
    ExecutionTerminalCommit,
};
use super::events::{ExecutionBindingLeaseClosure, ExecutionNodeBinding};
use super::recovery::{ExecutionGraphRecovery, ExecutionRecoveryError};

use super::registry::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError, NodeExecutorRegistry,
};
use super::resources::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager,
    ResourceAdmissionDecision, ResourceAdmissionRequest, ResourceWaitReason, ScopeLockError,
    ScopeLockLease, ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
    WorktreeLease, WorktreeLeaseManager, WorktreeLeaseRequest, WorktreeOwnership,
};
use super::state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};
use super::override_custody::{
    release_silent_override_custody, seal_override_custody_for_graph, unix_now_ms,
    OverrideCustody, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS,
};
use super::PHYSICAL_CLOSURE_BUDGET;

// Keep this list aligned with the proof reader's explicitly retryable source
// races. Other validation errors remain failures, including forged evidence.
fn retryable_proof_source_change(error: &ExecutionCommitError) -> bool {
    let ExecutionCommitError::InvalidCommand(reason) = error else {
        return false;
    };
    matches!(reason.as_str(),
        "proof source changed while paging; retry current inputs"
        | "proof source changed at final page boundary; retry current inputs"
        | "proof Program source changed; retry current inputs"
        | "proof physical lineage changed during discovery; retry current inputs")
        || reason.starts_with("proof Program source changed; retry current inputs; advanced=")
        || reason.starts_with("proof source changed during validation; retry current inputs; source=")
}

fn original_terminal_retryable_rollback(error: &ExecutionCommitError, outcome: &NodeExecutionOutcome) -> bool {
    let ExecutionCommitError::EventStore(crate::RuntimeEventStoreError::CheckedAttemptRolledBack(cause)) = error else { return false; };
    if outcome.original_terminal_owner.is_none() || outcome.original_synthesize_replan.is_none() { return false; }
    let Some(terminal) = outcome.domain_events.iter().find(|event| event.event.kind == "runtime.session.terminal_requested")
        .and_then(|event| serde_json::from_value::<crate::SessionTerminalInput>(event.event.payload.clone()).ok()) else { return false; };
    let Some(turn) = terminal.turn_id.as_deref() else { return false; };
    let objective = harness_contract::agent_action::root_objective_id(&terminal.session_id, turn);
    let program = format!("agentic-program:{}", harness_contract::agent_action::program_id_for_objective(&objective));
    matches!(cause.as_ref(), crate::RuntimeEventStoreError::SessionAttemptsUnsettled)
        || matches!(cause.as_ref(), crate::RuntimeEventStoreError::StaleRevision { stream_id, .. } if *stream_id == program)
}

// The semantic owner may rebuild post-tool observations and the next plan,
// but cannot rewrite the physical outcome or discard any other read fence.
fn accepts_goal_revalidation(
    original: &NodeExecutionOutcome,
    candidate: &NodeExecutionOutcome,
    original_sources: &BTreeMap<String, u64>,
    candidate_sources: &BTreeMap<String, u64>,
    stream: &str,
    expected: u64,
    actual: u64,
) -> bool {
    stream.starts_with("goal:")
        && actual > expected
        && original_sources.get(stream) == Some(&expected)
        && candidate_sources.get(stream) == Some(&actual)
        && original.result == candidate.result
        && original.delivery_envelope == candidate.delivery_envelope
        && original.terminal_presentation == candidate.terminal_presentation
        && original.no_dispatch == candidate.no_dispatch
        && original_sources.iter().all(|(key, value)| {
            key == stream || candidate_sources.get(key) == Some(value)
        })
}

// Opt-in diagnostic for the original synthetic completion call. The writer
// is deliberately separate: tracing must never perform persistent I/O while
// the graph or shared active-owner lock is held.
#[cfg(feature = "test-fixtures")]
pub(super) fn k05_completion_trace_enabled(ticket: &NodeExecutionTicket) -> bool {
    std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some()
        && crate::host::decode_tool_batch(&ticket.payload_ref).is_ok_and(|(calls, _)| {
            calls.iter().any(|call| {
                (call.id == "recovery-complete" && call.name == "objective_complete_request")
                    || (call.id == "d-objective-review" && call.name == "objective_review")
            })
        })
}

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) fn k05_trace_emit(line: String) {
    use std::sync::{mpsc, OnceLock};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SENDER: OnceLock<Option<mpsc::SyncSender<String>>> = OnceLock::new();
    static DROPPED: AtomicU64 = AtomicU64::new(0);
    let sender = SENDER.get_or_init(|| {
        let (sender, receiver) = mpsc::sync_channel::<String>(128);
        std::thread::Builder::new().name("k05-stage-observer".into()).spawn(move || {
            while let Ok(line) = receiver.recv() { eprintln!("{line}"); }
        }).ok().map(|_| sender)
    });
    let line = format!("{line} dropped={}", DROPPED.load(Ordering::Relaxed));
    if sender.as_ref().is_none_or(|sender| sender.try_send(line).is_err()) {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "test-fixtures")]
pub(crate) fn k05_completion_trace(ticket: &NodeExecutionTicket, stage: &'static str, revision: Option<u64>, ok: Option<bool>) {
    use sha2::{Digest, Sha256};
    let call = if crate::host::decode_tool_batch(&ticket.payload_ref).is_ok_and(|(calls, _)|
        calls.iter().any(|call| call.id == "d-objective-review" && call.name == "objective_review")) {
        "d-objective-review"
    } else { "recovery-complete" };
    k05_trace_emit(format!(
        "k05_runner_stage graph_sha256={:x} node_sha256={:x} attempt={} call={} stage={} revision={:?} ok={:?}",
        Sha256::digest(ticket.graph_id.as_bytes()), Sha256::digest(ticket.node_id.as_bytes()),
        ticket.attempt, call, stage, revision, ok,
    ));
}

#[cfg(feature = "test-fixtures")]
fn k05_completion_cas_class(
    error: &ExecutionCommitError,
) -> (&'static str, Option<&str>, Option<u64>, Option<u64>) {
    match error {
        ExecutionCommitError::EventStore(crate::runtime_event_store::RuntimeEventStoreError::StaleRevision { stream_id, expected, actual }) =>
            ("event_store_stale_revision", Some(stream_id.as_str()), Some(*expected), Some(*actual)),
        ExecutionCommitError::StaleRevision { graph_id, expected, actual } =>
            ("graph_stale_revision", Some(graph_id.as_str()), Some(*expected), Some(*actual)),
        ExecutionCommitError::EventStore(_) => ("event_store_other", None, None, None),
        ExecutionCommitError::EffectAdmissionUnknown { .. } => ("effect_admission_unknown", None, None, None),
        ExecutionCommitError::Transition(_) => ("transition", None, None, None),
        ExecutionCommitError::Serialization(_) => ("serialization", None, None, None),
        ExecutionCommitError::InvalidCommand(_) => ("invalid_command", None, None, None),
        ExecutionCommitError::AlreadyAppliedSame { .. } => ("already_applied_same", None, None, None),
        ExecutionCommitError::GraphStreamCollision(_) => ("graph_stream_collision", None, None, None),
        // The source moved under the proof: the owner is still writing, so the
        // closure re-proves inside its budget. The same wait the recovery seam
        // takes, named the same way.
        ExecutionCommitError::PhysicalClosureSourceMoved { stream_id } =>
            ("physical_closure_source_moved", Some(stream_id.as_str()), None, None),
        ExecutionCommitError::MissingDomainIdempotency(_) => ("missing_domain_idempotency", None, None, None),
        ExecutionCommitError::ProtectedDomainScope(_) => ("protected_domain_scope", None, None, None),
        ExecutionCommitError::BlockingTask(_) => ("blocking_task", None, None, None),
        ExecutionCommitError::InvalidReplan(_) => ("invalid_replan", None, None, None),
    }
}

#[cfg(feature = "test-fixtures")]
fn k05_completion_cas_error(ticket: &NodeExecutionTicket, error: &ExecutionCommitError) {
    use sha2::{Digest, Sha256};
    let (category, stream, expected, actual) = k05_completion_cas_class(error);
    let source_kind = stream.map(|id| {
        if id == ticket.graph_id.as_str() { "graph" }
        else if id.starts_with("execution-agent-receipts:") { "agent_receipts" }
        else if id.starts_with("execution-effect:") { "effect" }
        else if id.starts_with("agentic-program:") { "program" }
        else if id.starts_with("goal:") { "goal" }
        else if id.starts_with("session:") { "session" }
        else { "other" }
    }).unwrap_or("unknown");
    let source_sha = stream.map(|id| format!("{:x}", Sha256::digest(id.as_bytes())));
    k05_trace_emit(format!(
        "k05_runner_cas_error graph_sha256={:x} node_sha256={:x} attempt={} category={} source_kind={} source_sha256={:?} expected={:?} actual={:?} same_graph={}",
        Sha256::digest(ticket.graph_id.as_bytes()), Sha256::digest(ticket.node_id.as_bytes()),
        ticket.attempt, category, source_kind, source_sha, expected, actual,
        stream == Some(ticket.graph_id.as_str()),
    ));
}

// Local fixture diagnostic only; no payloads or authority are reconstructed.
#[cfg(feature = "test-fixtures")]
fn build16_callback_trace(graph: &str, node: &str, attempt: Option<u32>, phase: &str, detail: &str) {
    if std::env::var_os("COWD_BUILD16_CALLBACK_TRACE").as_deref() == Some(std::ffi::OsStr::new("1"))
        && graph.starts_with("agent-tool-batch:")
    {
        let detail: String = detail.chars().take(512).collect();
        eprintln!("BUILD16_CALLBACK graph={graph} node={node} attempt={attempt:?} phase={phase} detail={detail:?}");
    }
}

#[cfg(feature = "test-fixtures")]
fn build16_runner_error_kind(error: &ExecutionRunnerError) -> &'static str {
    match error {
        ExecutionRunnerError::Executor(_) => "executor",
        ExecutionRunnerError::Commit(_) => "commit",
        ExecutionRunnerError::State(_) => "state",
        ExecutionRunnerError::CommandSuperseded { .. } => "superseded",
        ExecutionRunnerError::TerminalWaveCleanup { .. } => "terminal_wave_cleanup",
        ExecutionRunnerError::Driver(_) => "driver",
        ExecutionRunnerError::SharedOriginal(original) => build16_runner_error_kind(original),
        _ => "other",
    }
}

#[derive(Debug, Error)]
pub enum ExecutionRunnerError {
    #[error(transparent)]
    InvalidGraph(#[from] ExecutionGraphValidationError),
    #[error(transparent)]
    Executor(#[from] NodeExecutorError),
    #[error(transparent)]
    Commit(#[from] ExecutionCommitError),
    #[error(transparent)]
    State(#[from] ExecutionStateStoreError),
    #[error("execution node task failed to join: {0}")]
    Join(String),
    #[error("executor returned illegal outcome `{status:?}` for node `{node_id}`")]
    IllegalOutcome {
        node_id: String,
        status: ExecutionNodeStatus,
    },
    #[error("execution graph projection is missing node `{0}`")]
    NodeMissing(String),
    #[error("new execution graphs must be submitted with the Start command")]
    InvalidStartCommand,
    #[error("execution resource acquisition failed for node `{node_id}`: {reason}")]
    Resource { node_id: String, reason: String },
    #[error("execution resource admission deferred for node `{node_id}`: {reason}")]
    ResourceDeferred { node_id: String, reason: String },
    #[error("unadmitted execution graph `{graph_id}` changed during resource preparation")]
    AdmissionRaced { graph_id: String },
    #[error("execution node `{node_id}` exceeded its durable deadline `{deadline_at_ms}`")]
    DeadlineExceeded {
        node_id: String,
        deadline_at_ms: u64,
    },
    #[error("execution mutation is blocked: {0}")]
    MutationBlocked(String),
    #[error("execution node `{node_id}` was superseded by a graph command")]
    CommandSuperseded { node_id: String },
    #[error("runtime execution supervisor is unavailable: {0}")]
    SupervisorUnavailable(String),
    #[error("terminal wave failed: {reason}; {phase} remains unresolved: {cleanup}")]
    TerminalWaveCleanup {
        reason: String,
        phase: &'static str,
        #[source]
        cleanup: Box<ExecutionRunnerError>,
    },
    #[error("runtime execution driver failed: {0}")]
    Driver(String),
    #[error(transparent)]
    SharedOriginal(Arc<ExecutionRunnerError>),
}

/// The original terminal custody belongs to the original Session terminal.
///
/// Only a root Session ingress asks for that terminal, so only that request
/// can leave the original terminal without a live owner. A delegated leaf
/// publishes its own terminal and never owns the original Session terminal,
/// so a published terminal completion alone must not demand that custody.
pub(super) fn requests_original_session_terminal(outcome: &NodeExecutionOutcome) -> bool {
    outcome.domain_events.iter()
        .any(|event| event.event.kind == "runtime.session.terminal_requested")
}

/// The custody error of a committed Synthesize outcome whose original Session
/// terminal has no live owner. A delegated leaf terminal never asks for that
/// custody, so it never carries this error.
pub(super) fn missing_original_terminal_custody(
    outcome: &NodeExecutionOutcome,
) -> Option<ExecutionRunnerError> {
    requests_original_session_terminal(outcome)
        .then(|| ExecutionRunnerError::Driver("original terminal handoff is unconfirmed".into()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRunReport {
    pub graph_id: String,
    pub revision: u64,
    pub completed: usize,
    pub failed: usize,
    pub blocked: usize,
    pub cancelled: usize,
    pub waiting: usize,
}

#[derive(Debug)]
pub(crate) struct ExecutionGraphPumpSnapshot {
    pub(crate) ready: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
    pub(crate) report: ExecutionRunReport,
}

pub(crate) struct ExecutionPreparedPumpNode {
    pub(crate) node: harness_contract::execution_graph::ExecutionNodeSpec,
    pub(crate) binding: Option<ExecutionNodeBinding>,
    startup_error: Option<ExecutionRunnerError>,
}

impl ExecutionPreparedPumpNode {
    pub(crate) fn has_startup_error(&self) -> bool { self.startup_error.is_some() }
}

// The receiver stays with the original handoff if an observing pump is dropped.
pub(crate) struct OriginalPumpTask {
    result: StdMutex<OriginalPumpResult>,
    ready: Notify,
    panicked: std::sync::atomic::AtomicBool,
    source_conflict: std::sync::atomic::AtomicBool,
}

enum OriginalPumpResult {
    Pending,
    Finished(Result<(), Arc<ExecutionRunnerError>>),
    Closed,
}

pub(crate) struct OriginalPumpSender(Option<Arc<OriginalPumpTask>>);

impl OriginalPumpSender {
    pub(crate) fn send(mut self, value: Result<(), ExecutionRunnerError>) -> Result<(), Result<(), ExecutionRunnerError>> {
        let Some(task) = self.0.take() else { return Err(value); };
        let mut state = task.result.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, OriginalPumpResult::Pending) { return Err(value); }
        *state = OriginalPumpResult::Finished(value.map_err(Arc::new));
        drop(state);
        task.ready.notify_waiters();
        Ok(())
    }
}

impl Drop for OriginalPumpSender {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            let mut state = task.result.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if matches!(*state, OriginalPumpResult::Pending) { *state = OriginalPumpResult::Closed; }
            drop(state);
            task.ready.notify_waiters();
        }
    }
}

fn original_shared_error(error: &Arc<ExecutionRunnerError>) -> ExecutionRunnerError {
    match error.as_ref() {
        ExecutionRunnerError::InvalidGraph(error) => ExecutionRunnerError::InvalidGraph(error.clone()),
        ExecutionRunnerError::Executor(error) => ExecutionRunnerError::Executor(error.clone()),
        ExecutionRunnerError::Join(reason) => ExecutionRunnerError::Join(reason.clone()),
        ExecutionRunnerError::IllegalOutcome { node_id, status } => ExecutionRunnerError::IllegalOutcome { node_id: node_id.clone(), status: *status },
        ExecutionRunnerError::NodeMissing(node) => ExecutionRunnerError::NodeMissing(node.clone()),
        ExecutionRunnerError::InvalidStartCommand => ExecutionRunnerError::InvalidStartCommand,
        ExecutionRunnerError::Resource { node_id, reason } => ExecutionRunnerError::Resource { node_id: node_id.clone(), reason: reason.clone() },
        ExecutionRunnerError::ResourceDeferred { node_id, reason } => ExecutionRunnerError::ResourceDeferred { node_id: node_id.clone(), reason: reason.clone() },
        ExecutionRunnerError::AdmissionRaced { graph_id } => ExecutionRunnerError::AdmissionRaced { graph_id: graph_id.clone() },
        ExecutionRunnerError::DeadlineExceeded { node_id, deadline_at_ms } => ExecutionRunnerError::DeadlineExceeded { node_id: node_id.clone(), deadline_at_ms: *deadline_at_ms },
        ExecutionRunnerError::MutationBlocked(reason) => ExecutionRunnerError::MutationBlocked(reason.clone()),
        ExecutionRunnerError::CommandSuperseded { node_id } => ExecutionRunnerError::CommandSuperseded { node_id: node_id.clone() },
        ExecutionRunnerError::SupervisorUnavailable(reason) => ExecutionRunnerError::SupervisorUnavailable(reason.clone()),
        ExecutionRunnerError::Driver(reason) => ExecutionRunnerError::Driver(reason.clone()),
        ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision { graph_id, expected, actual }) =>
            ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision {
                graph_id: graph_id.clone(), expected: *expected, actual: *actual,
            }),
        ExecutionRunnerError::Commit(_) | ExecutionRunnerError::State(_) | ExecutionRunnerError::TerminalWaveCleanup { .. } => ExecutionRunnerError::SharedOriginal(Arc::clone(error)),
        ExecutionRunnerError::SharedOriginal(original) => ExecutionRunnerError::SharedOriginal(Arc::clone(original)),
    }
}

impl OriginalPumpTask {
    pub(crate) fn new() -> (Arc<Self>, OriginalPumpSender) {
        let task = Arc::new(Self { result: StdMutex::new(OriginalPumpResult::Pending), ready: Notify::new(), panicked: std::sync::atomic::AtomicBool::new(false), source_conflict: std::sync::atomic::AtomicBool::new(false) });
        (Arc::clone(&task), OriginalPumpSender(Some(task)))
    }

    pub(crate) fn mark_panicked(&self) {
        self.panicked.store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn was_panicked(&self) -> bool {
        self.panicked.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn requires_original_cleanup(&self) -> bool {
        self.was_panicked() || self.source_conflict.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn classify(&self, node_id: &str) -> Option<Result<(), ExecutionRunnerError>> {
        let result = match self.result.lock() {
            Ok(result) => result,
            Err(_) => return Some(Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: node_id.into(), reason: "original pump result state unavailable".into(),
            }))),
        };
        match &*result {
            OriginalPumpResult::Finished(Ok(())) => Some(Ok(())),
            OriginalPumpResult::Finished(Err(error)) => Some(Err(original_shared_error(error))),
            OriginalPumpResult::Closed => Some(Err(ExecutionRunnerError::Executor(
                NodeExecutorError::OriginalCustodyUnknown {
                    node_id: node_id.into(), reason: "original pump task exited without its result".into(),
                }))),
            OriginalPumpResult::Pending => None,
        }
    }

    pub(crate) async fn observe(&self, node_id: &str) -> Result<(), ExecutionRunnerError> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.classify(node_id) { return result; }
            notified.await;
        }
    }
}

fn unknown_dependency_nodes(graph: &ExecutionGraph, unknown: &BTreeSet<String>) -> BTreeSet<String> {
    let mut prohibited = unknown.clone();
    loop {
        let mut changed = false;
        for edge in &graph.edges {
            if edge.kind.is_dependency() && prohibited.contains(&edge.from) {
                changed |= prohibited.insert(edge.to.clone());
            }
        }
        if !changed { return prohibited; }
    }
}

#[derive(Default)]
struct CancelOutcomeHandoff {
    state: StdMutex<CancelOutcomeState>,
    ready: Notify,
}
impl CancelOutcomeHandoff {
    fn closing_deadline(&self, proposed: std::time::Instant) -> Result<std::time::Instant, ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        let deadline = state.closing_deadline.map_or(proposed, |original| original.min(proposed));
        state.closing_deadline = Some(deadline);
        Ok(deadline)
    }
}
#[derive(Default)]
struct CancelOutcomeState {
    polled: bool,
    finished: bool,
    outcome: Option<Result<NodeExecutionOutcome, Arc<ExecutionRunnerError>>>,
    superseded_outcomes: Vec<NodeExecutionOutcome>,
    terminal_replan_cleanup: Option<OriginalTerminalReplanCleanup>,
    original_binding_closed: bool,
    closing_deadline: Option<std::time::Instant>,
    pump_task: Option<Arc<OriginalPumpTask>>,
    // One retained physical cancellation, shared by every original waiter.
    // Its driver survives an abandoned waiter and never replays the work.
    drain: Option<Shared<BoxFuture<'static, Result<OriginalDrainOutcome, String>>>>,
    drain_deadline: Option<std::time::Instant>,
    cleanup_inflight: bool,
    cleanup_action: Option<Shared<BoxFuture<'static, Result<(), String>>>>,
    cleanup_deadline: Option<std::time::Instant>,
    cleanup_nonce: Option<Arc<()>>,
    terminal_release_action: Option<Shared<BoxFuture<'static, Result<(), String>>>>,
    lease_close_action:
        Option<Shared<BoxFuture<'static, Result<Option<LeaseClosureCertification>, String>>>>,
    lease_close_resources: Option<Arc<StdMutex<Option<NodeResourceGuards>>>>,
    callback_action: Option<Shared<BoxFuture<'static, Result<(), String>>>>,
    callback_decision: Option<CallbackDisposition>,
    callback_nonce: Option<Arc<()>>,
    callback_deadline: Option<std::time::Instant>,
    proof_inflight: bool,
}

/// Facts certified by one physical lease release that ran outside the graph
/// decision domain. The durable checkpoint append — the only graph-stream
/// revision this closure produces — waits for coordination and commits
/// inside the same domain every terminal wave, dependency reduction, command
/// apply and start wave already shares, so a slow physical close (scope or
/// worktree IO) cannot serialize unrelated successor dispatch behind the
/// domain lock.
#[derive(Clone)]
struct LeaseClosureCertification {
    early_reads: Vec<crate::conversation::EarlyToolExecutionReceipt>,
    early_sources: Vec<crate::runtime_event_store::ExpectedStreamRevision>,
}

// This is a guard on the same original handoff, not another owner. Dropping
// a cancelled cleanup waiter makes the original cleanup eligible to resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OriginalDrainOutcome { Drained, AlreadyCommitted }

struct OriginalProofInFlight(Arc<CancelOutcomeHandoff>);
impl Drop for OriginalProofInFlight {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() { state.proof_inflight = false; }
        self.0.ready.notify_waiters();
    }
}

struct OriginalCleanupInFlight(Arc<CancelOutcomeHandoff>);
impl Drop for OriginalCleanupInFlight {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() { state.cleanup_inflight = false; }
    }
}

#[derive(Clone)]
struct OriginalTerminalReplanCleanup {
    original: NodeExecutionOutcome,
    candidate: NodeExecutionOutcome,
    handle: super::registry::OriginalSynthesizeReplanHandle,
    retained: crate::runtime_event_store::OriginalRetainedTerminal,
    receipt: Option<crate::AppendTransactionReceipt>,
    request: Option<Arc<crate::CheckedAppendRequest>>,
    transferred: bool,
    retired: bool,
}

struct ActiveNode {
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    resource_kind: Option<ExecutionResourceKind>,
    resource_queue_wait: Duration,
    resource_started: std::time::Instant,
    effect_state: ExecutionEffectState,
    effect_receipt_required: bool,
    cancel_outcome: Arc<CancelOutcomeHandoff>,
    // Identity and callback state remain visible without taking the owner's
    // resource lock. The table retains the Arc across a cancelled waiter.
    binding: ExecutionNodeBinding,
    resources: Arc<StdMutex<NodeResourceGuards>>,
    release_blocked: bool,
    callback_completed: bool,
    drain_completed: bool,
    callback_gate: Arc<Mutex<()>>,
    no_dispatch: Option<Arc<super::executors::agent_tool::NoDispatchObservation>>,
}

/// Exact active owners captured before the supervisor aborts its node tasks.
#[derive(Clone)]
pub(crate) struct PumpCancellationOwner {
    binding: ExecutionNodeBinding,
    ticket: NodeExecutionTicket,
    executor: Arc<dyn NodeExecutor>,
    gate: Arc<Mutex<()>>,
    handoff: Arc<CancelOutcomeHandoff>,
}

struct PendingTerminalCommit {
    transition: ExecutionTerminalCommit,
    expected_binding: Option<ExecutionNodeBinding>,
    response: oneshot::Sender<TerminalCommitDisposition>,
}

#[derive(Default)]
struct TerminalBatchState {
    leader_active: bool,
    pending: Vec<PendingTerminalCommit>,
}

enum TerminalCommitDisposition {
    Committed,
    Superseded,
    Failed(String),
    WaitForDrain(Shared<BoxFuture<'static, Result<OriginalDrainOutcome, String>>>, std::time::Instant),
    WaitForCleanup(Shared<BoxFuture<'static, Result<(), String>>>, std::time::Instant),
}

struct PreparedNodeStart {
    node: harness_contract::execution_graph::ExecutionNodeSpec,
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    resources: NodeResourceGuards,
    resource_kind: Option<ExecutionResourceKind>,
    resource_queue_wait: Duration,
    resource_started: std::time::Instant,
    effect_state: ExecutionEffectState,
    effect_receipt_required: bool,
}

struct NodeResourceGuards {
    resource: Option<ExecutionResourceLease>,
    scope: Option<ScopeLockLease>,
    worktree: Option<WorktreeLease>,
    original_binding: Option<ExecutionNodeBinding>,
    admission_pending: bool,
    owner_fence: Option<super::resources::BindingOwnerFence>,
}

impl NodeResourceGuards {
    fn binding(&self, ticket: &NodeExecutionTicket) -> ExecutionNodeBinding {
        if let Some(binding) = &self.original_binding {
            return binding.clone();
        }
        ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: ticket.attempt,
            resource_lease_refs: self
                .resource
                .as_ref()
                .map(|resource| vec![resource.projection_reference()])
                .unwrap_or_default(),
            scope_lease_ref: self
                .scope
                .as_ref()
                .map(|lease| lease.projection_reference()),
            worktree_lease_ref: self
                .worktree
                .as_ref()
                .map(|lease| lease.record().lease_id.to_string()),
        }
    }

    fn release_worktree(&mut self) {
        if let Some(lease) = self.worktree.take() {
            let _ = lease.release();
        }
    }
}

impl Drop for NodeResourceGuards {
    fn drop(&mut self) {
        if !self.admission_pending {
            self.release_worktree();
        }
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionGraphRunner {
    registry: Arc<NodeExecutorRegistry>,
    state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    resource_manager: Arc<ExecutionResourceManager>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    workspace_id: String,
    workspace_root: PathBuf,
    path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    active: Arc<Mutex<BTreeMap<(String, String), ActiveNode>>>,
    active_changed: Arc<Notify>,
    terminal_batches: Arc<Mutex<BTreeMap<String, TerminalBatchState>>>,
    coordination: Arc<StdMutex<BTreeMap<String, Weak<Mutex<()>>>>>,
    command_intents: Arc<StdMutex<BTreeMap<String, Arc<Notify>>>>,
    shutdown_deadline: Arc<OnceLock<std::time::Instant>>,
    mutation_gate: Arc<RwLock<Option<MutationGate>>>,
    #[cfg(test)]
    closure_probe: Arc<StdMutex<Option<Arc<dyn Fn(&str) + Send + Sync>>>>,
    #[cfg(test)]
    closure_finished_probe: Arc<StdMutex<Option<Arc<dyn Fn(&str) + Send + Sync>>>>,
    #[cfg(test)]
    lease_closure_checkpoint_probe: Arc<StdMutex<Option<Arc<dyn Fn(&str) + Send + Sync>>>>,
}

type MutationGate = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

fn normalize_start_failure(
    expected_node_id: &str,
    admitted: bool,
    error: ExecutionRunnerError,
) -> ExecutionRunnerError {
    match error {
        ExecutionRunnerError::Executor(error @ NodeExecutorError::Start { .. }) => {
            if admitted {
                NodeExecutorError::OriginalCustodyUnknown {
                    node_id: expected_node_id.into(),
                    reason: format!("Start reported after canonical admission: {error}"),
                }.into()
            } else if executor_error_node_id(&error) != expected_node_id {
                NodeExecutorError::Invalid {
                    node_id: expected_node_id.into(),
                    reason: format!("start failure belongs to another node: {error}"),
                }.into()
            } else {
                error.into()
            }
        }
        error => error,
    }
}

struct CommandIntentOwner {
    graph_id: String,
    intent: Arc<Notify>,
    intents: Arc<StdMutex<BTreeMap<String, Arc<Notify>>>>,
}

type ActiveCancellation = (String, Arc<dyn NodeExecutor>, NodeExecutionTicket);

struct CancellationFinalizationOwner {
    active: Vec<ActiveCancellation>,
}

impl CancellationFinalizationOwner {
    fn new(active: Vec<ActiveCancellation>) -> Self {
        Self { active }
    }

    fn active(&self) -> &[ActiveCancellation] {
        &self.active
    }
}

impl Drop for CancellationFinalizationOwner {
    fn drop(&mut self) {
        for (_, executor, ticket) in &self.active {
            executor.cancellation_finalized(ticket);
        }
    }
}

impl CommandIntentOwner {
    fn install(
        graph_id: &str,
        intents: Arc<StdMutex<BTreeMap<String, Arc<Notify>>>>,
    ) -> Result<Self, ExecutionRunnerError> {
        let intent = Arc::new(Notify::new());
        let mut current = intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.contains_key(graph_id) {
            return Err(ExecutionRunnerError::MutationBlocked(format!(
                "graph `{graph_id}` already has a command in flight"
            )));
        }
        current.insert(graph_id.to_string(), Arc::clone(&intent));
        drop(current);
        Ok(Self {
            graph_id: graph_id.to_string(),
            intent,
            intents,
        })
    }
}

impl Drop for CommandIntentOwner {
    fn drop(&mut self) {
        let removed = {
            let mut intents = self
                .intents
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if intents
                .get(&self.graph_id)
                .is_some_and(|current| Arc::ptr_eq(current, &self.intent))
            {
                intents.remove(&self.graph_id);
                true
            } else {
                false
            }
        };
        if removed {
            self.intent.notify_waiters();
        }
    }
}

impl ExecutionGraphRunner {
    pub(crate) async fn retained_original_pumps(&self, graph_id: &str) -> Result<Vec<(String, Arc<OriginalPumpTask>, PumpCancellationOwner)>, ExecutionRunnerError> {
        self.active.lock().await.iter()
            .filter(|((graph, _), _)| graph == graph_id)
            .map(|((_, node), active)| match active.cancel_outcome.state.lock() {
                Ok(state) => Ok(state.pump_task.clone().map(|task| (node.clone(), task, PumpCancellationOwner {
                    binding: active.binding.clone(), ticket: active.ticket.clone(), executor: Arc::clone(&active.executor),
                    gate: Arc::clone(&active.callback_gate), handoff: Arc::clone(&active.cancel_outcome),
                }))),
                Err(_) => Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: node.clone(), reason: "original pump handoff unavailable".into(),
                })),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|tasks| tasks.into_iter().flatten().collect())
    }

    pub(crate) async fn original_pump_matches(&self, graph_id: &str, node: &str, task: &Arc<OriginalPumpTask>, original: &PumpCancellationOwner) -> bool {
        self.active.lock().await.get(&(graph_id.to_string(), node.to_string()))
            .filter(|active| active.binding == original.binding && active.ticket == original.ticket
                && Arc::ptr_eq(&active.executor, &original.executor) && Arc::ptr_eq(&active.callback_gate, &original.gate)
                && Arc::ptr_eq(&active.cancel_outcome, &original.handoff))
            .and_then(|active| active.cancel_outcome.state.lock().ok()
                .and_then(|state| state.pump_task.clone()))
            .is_some_and(|current| Arc::ptr_eq(&current, task))
    }

    pub(crate) fn original_pump_was_closed(&self, task: &Arc<OriginalPumpTask>, original: &PumpCancellationOwner) -> bool {
        original.handoff.state.lock().ok().is_some_and(|state| state.original_binding_closed
            && state.pump_task.as_ref().is_some_and(|current| Arc::ptr_eq(current, task)))
    }
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(
        registry: Arc<NodeExecutorRegistry>,
        state_store: ExecutionGraphStateStore,
        commit_service: ExecutionCommitService,
        resource_manager: Arc<ExecutionResourceManager>,
        scope_locks: Arc<ScopeLockManager>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        let workspace_root = workspace_root.into();
        let path_identity_resolver = Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(&workspace_root)
                .expect("ExecutionGraphRunner requires an existing workspace root"),
        );
        Self::new_with_path_identity_resolver(
            registry,
            state_store,
            commit_service,
            resource_manager,
            scope_locks,
            worktree_leases,
            workspace_id,
            workspace_root,
            path_identity_resolver,
        )
    }

    #[must_use]
    pub(crate) fn new_with_path_identity_resolver(
        registry: Arc<NodeExecutorRegistry>,
        state_store: ExecutionGraphStateStore,
        commit_service: ExecutionCommitService,
        resource_manager: Arc<ExecutionResourceManager>,
        scope_locks: Arc<ScopeLockManager>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
        path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    ) -> Self {
        Self {
            registry,
            state_store,
            commit_service,
            resource_manager,
            scope_locks,
            worktree_leases,
            workspace_id: workspace_id.into(),
            workspace_root: workspace_root.into(),
            path_identity_resolver,
            active: Arc::new(Mutex::new(BTreeMap::new())),
            active_changed: Arc::new(Notify::new()),
            terminal_batches: Arc::new(Mutex::new(BTreeMap::new())),
            coordination: Arc::new(StdMutex::new(BTreeMap::new())),
            command_intents: Arc::new(StdMutex::new(BTreeMap::new())),
            shutdown_deadline: Arc::new(OnceLock::new()),
            mutation_gate: Arc::new(RwLock::new(None)),
            #[cfg(test)]
            closure_probe: Arc::new(StdMutex::new(None)),
            #[cfg(test)]
            closure_finished_probe: Arc::new(StdMutex::new(None)),
            #[cfg(test)]
            lease_closure_checkpoint_probe: Arc::new(StdMutex::new(None)),
        }
    }

    fn command_intent_waiter(&self, graph_id: &str) -> Option<OwnedNotified> {
        self.command_intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(graph_id)
            .cloned()
            .map(Notify::notified_owned)
    }

    pub(crate) fn state_store(&self) -> &ExecutionGraphStateStore {
        &self.state_store
    }

    pub(crate) async fn register_original_pump_task(
        &self, graph_id: &str, prepared: &ExecutionPreparedPumpNode, task: Arc<OriginalPumpTask>,
    ) -> Result<(), ExecutionRunnerError> {
        let node_id = &prepared.node.id;
        let active = self.active.lock().await;
        let owner = active.get(&(graph_id.to_owned(), node_id.clone()))
            .filter(|owner| Some(&owner.binding) == prepared.binding.as_ref())
            .ok_or_else(|| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: node_id.clone(), reason: "original task owner unavailable before spawn".into(),
            }))?;
        let mut state = owner.cancel_outcome.state.lock().map_err(|_| ExecutionRunnerError::Executor(
            NodeExecutorError::OriginalCustodyUnknown {
                node_id: node_id.clone(), reason: "original task handoff unavailable before spawn".into(),
            }))?;
        if state.pump_task.is_some() {
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: node_id.clone(), reason: "original pump task already registered".into(),
            }));
        }
        state.pump_task = Some(task);
        Ok(())
    }

    // The prepared Task result does not contain the runner's elapsed duration.
    // Only the original live handoff can supply the finalized outcome.
    pub(crate) async fn commit_live_prepared_outcome(
        &self,
        ticket: &NodeExecutionTicket,
        prepared: &NodeExecutionOutcome,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        let unknown = |reason: &str| ExecutionRunnerError::Executor(
            NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: reason.into(),
            });
        let _coordination = self.graph_coordination_without_command(&ticket.graph_id).await;
        let graph = self.state_store.load_async(&ticket.graph_id).await?;
        let (binding, handoff) = {
            let active = self.active.lock().await;
            let owner = active.get(&(ticket.graph_id.clone(), ticket.node_id.clone()))
                .filter(|owner| owner.ticket == *ticket)
                .ok_or_else(|| unknown("original prepared outcome owner unavailable"))?;
            (owner.binding.clone(), Arc::clone(&owner.cancel_outcome))
        };
        if graph.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running)
            || graph.recovery_cursor.node_attempts.get(&ticket.node_id) != Some(&binding.attempt) {
            return Err(unknown("original prepared outcome binding changed"));
        }
        let outcome = {
            let state = handoff.state.lock().map_err(|_| unknown("original outcome handoff unavailable"))?;
            if !state.finished || state.drain.is_some() || state.callback_action.is_some()
                || state.cleanup_action.as_ref().is_some_and(|action| !matches!(action.peek(), Some(Ok(())))) {
                return Err(unknown("original outcome or cancellation remains unresolved"));
            }
            let outcome = state.outcome.as_ref().and_then(|outcome| outcome.as_ref().ok()).cloned()
                .ok_or_else(|| unknown("original finalized outcome unavailable"))?;
            let mut expected = prepared.clone();
            expected.result.usage.duration_ms = outcome.result.usage.duration_ms;
            if outcome.result.usage.duration_ms == 0 || expected != outcome {
                return Err(unknown("prepared outcome differs from original finalized outcome"));
            }
            outcome
        };
        let sources = self.commit_service.execution_outcome_sources(&graph, ticket, &outcome)?;
        Ok(self.commit_service.transition_execution_outcome_async(
            graph, ticket.clone(), outcome, sources,
        ).await?.graph)
    }

    pub(crate) async fn confirm_cancelled_bound_child_never_admitted(
        &self,
        expected: &ExecutionGraph,
    ) -> Result<(), ExecutionRunnerError> {
        let _coordination = self.graph_coordination_without_command(&expected.id).await;
        if self
            .active
            .lock()
            .await
            .keys()
            .any(|(graph_id, _)| graph_id == &expected.id)
        {
            return Err(ExecutionRunnerError::AdmissionRaced {
                graph_id: expected.id.clone(),
            });
        }
        let service = self.commit_service.clone();
        let expected = expected.clone();
        tokio::task::spawn_blocking(move || {
            service.confirm_cancelled_bound_child_never_admitted(&expected)
        })
        .await
        .map_err(|error| ExecutionRunnerError::Join(error.to_string()))??;
        Ok(())
    }

    pub(crate) async fn drain_physical_execution(&self) -> Vec<String> {
        self.registry.drain_physical_execution().await
    }

    pub(crate) async fn has_retained_shutdown_owners(&self) -> bool {
        !self.active.lock().await.is_empty()
    }

    pub(crate) async fn wait_retained_shutdown_owners(&self) {
        loop {
            let changed = self.active_changed.notified();
            if self.active.lock().await.is_empty() { return; }
            changed.await;
        }
    }

    pub(crate) fn install_shutdown_deadline(&self, deadline: std::time::Instant) {
        let _ = self.shutdown_deadline.set(deadline);
    }

    pub(crate) async fn recover_graph(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        // A cancelled supervisor waiter may have left the one original
        // physical drain running. Rejoin that exact handoff before deciding
        // whether the retained binding can be recovered; never start it again.
        let draining = self.active.lock().await.iter()
            .filter(|((active_graph, _), active)| active_graph == graph_id
                && active.cancel_outcome.state.lock().ok().is_some_and(|state| state.drain.is_some()))
            .map(|(_, active)| PumpCancellationOwner {
                binding: active.binding.clone(), ticket: active.ticket.clone(),
                executor: Arc::clone(&active.executor), gate: Arc::clone(&active.callback_gate),
                handoff: Arc::clone(&active.cancel_outcome),
            }).collect::<Vec<_>>();
        if !draining.is_empty() {
            self.drain_pump_cancellation_owners(graph_id, draining).await
                .map_err(|error| ExecutionRecoveryError::LeaseReconciliation(error.to_string()))?;
        }
        let retained = self
            .active
            .lock()
            .await
            .iter()
            .filter(|((active_graph, _), _)| active_graph == graph_id)
            .map(|((_, node_id), active)| {
                (node_id.clone(), active.binding.clone())
            })
            .collect::<Vec<_>>();
        let graph = self.state_store.load_async(graph_id).await?;
        for (node_id, binding) in &retained {
            if self
                .commit_service
                .binding_callback_disposition(&graph, node_id, binding)?
                == CallbackDisposition::Pending
            {
                return Err(ExecutionRecoveryError::LeaseReconciliation(
                    "original in-process execution owner is live".into(),
                ));
            }
        }
        for (node_id, binding) in retained {
            self.take_bound_active_node(graph_id, &node_id, Some(&binding))
                .await
                .map_err(|error| ExecutionRecoveryError::LeaseReconciliation(error.to_string()))?;
        }
        if self
            .active
            .lock()
            .await
            .keys()
            .any(|(active_graph, _)| active_graph == graph_id)
        {
            return Err(ExecutionRecoveryError::LeaseReconciliation(
                "original in-process execution owner remains retained".into(),
            ));
        }
        ExecutionGraphRecovery::new(&self.state_store, &self.commit_service, &self.registry)
            .recover_with_leases(
                graph_id,
                &self.scope_locks,
                &self.worktree_leases,
                &self.workspace_id,
            )
            .await
    }

    async fn graph_coordination_without_command(&self, graph_id: &str) -> OwnedMutexGuard<()> {
        loop {
            if let Some(waiter) = self.command_intent_waiter(graph_id) {
                waiter.await;
                continue;
            }
            let coordination = self.graph_coordination(graph_id).await;
            if let Some(waiter) = self.command_intent_waiter(graph_id) {
                drop(coordination);
                waiter.await;
                continue;
            }
            return coordination;
        }
    }

    async fn graph_coordination(&self, graph_id: &str) -> OwnedMutexGuard<()> {
        {
            let mut registry = self
                .coordination
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry.retain(|_, lock| lock.strong_count() > 0);
            registry
                .get(graph_id)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let lock = Arc::new(Mutex::new(()));
                    registry.insert(graph_id.to_string(), Arc::downgrade(&lock));
                    lock
                })
        }
        .lock_owned()
        .await
    }

    async fn coordinate_original_gate(
        &self, graph_id: &str, node_id: &str, gate: &Arc<Mutex<()>>,
        deadline: std::time::Instant,
    ) -> Result<(OwnedMutexGuard<()>, OwnedMutexGuard<()>), ExecutionRunnerError> {
        let unknown = || ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            node_id: node_id.into(), reason: "original owner coordination deadline expired".into(),
        });
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let coordination = tokio::time::timeout(remaining, self.graph_coordination(graph_id))
                .await.map_err(|_| unknown())?;
            if let Ok(owner) = Arc::clone(gate).try_lock_owned() {
                return Ok((coordination, owner));
            }
            drop(coordination);
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let owner = tokio::time::timeout(remaining, Arc::clone(gate).lock_owned())
                .await.map_err(|_| unknown())?;
            drop(owner);
        }
    }

    async fn graph_coordination_for_outcome(
        &self, graph_id: &str, node_id: &str, binding: Option<&ExecutionNodeBinding>,
    ) -> Result<OwnedMutexGuard<()>, ExecutionRunnerError> {
        let unknown = |reason: String| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            node_id: node_id.into(), reason,
        });
        loop {
            let coordination = self.graph_coordination_without_command(graph_id).await;
            let pending = {
                let active = self.active.lock().await;
                let Some(owner) = active.get(&(graph_id.to_owned(), node_id.to_owned()))
                    .filter(|owner| Some(&owner.binding) == binding) else {
                        return Ok(coordination);
                    };
                let state = owner.cancel_outcome.state.lock()
                    .map_err(|_| unknown("original outcome handoff unavailable".into()))?;
                if let Some(drain) = state.drain.as_ref() {
                    if let Some(Err(reason)) = drain.peek() {
                        return Err(unknown(format!("original drain unconfirmed: {reason}")));
                    }
                    if drain.peek().is_none() && state.drain_deadline.is_none() {
                        return Err(unknown("original drain deadline unavailable".into()));
                    }
                }
                (if let Some(cleanup) = state.cleanup_action.as_ref() {
                    match cleanup.peek() {
                        Some(Ok(())) => None,
                        Some(Err(reason)) => return Err(unknown(format!("original cleanup unconfirmed: {reason}"))),
                        None => {
                            let cleanup = cleanup.clone();
                            let deadline = state.cleanup_deadline.ok_or_else(|| unknown("original cleanup deadline unavailable".into()))?;
                            Some((async move { cleanup.await.map(|()| OriginalDrainOutcome::Drained) }.boxed().shared(), deadline))
                        }
                    }
                } else { None }).or_else(|| match state.drain.as_ref() {
                    None => None,
                    Some(drain) => match drain.peek() {
                        Some(Ok(_)) => None,
                        Some(Err(_)) => None,
                        None => state.drain_deadline.map(|deadline| (drain.clone(), deadline)),
                    },
                })
            };
            let Some((drain, deadline)) = pending else { return Ok(coordination); };
            drop(coordination);
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match tokio::time::timeout(remaining, drain).await {
                Ok(Ok(_)) => {},
                Ok(Err(reason)) => return Err(unknown(reason)),
                Err(_) => return Err(unknown("original physical drain remains pending".into())),
            }
        }
    }

    pub(crate) fn install_mutation_gate(
        &self,
        gate: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) {
        *self
            .mutation_gate
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::new(gate));
    }

    fn ensure_mutation_allowed(&self) -> Result<(), ExecutionRunnerError> {
        let gate = self
            .mutation_gate
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        gate.map_or(Ok(()), |gate| {
            gate().map_err(ExecutionRunnerError::MutationBlocked)
        })
    }

    #[cfg(test)]
    pub(crate) async fn start(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let registered = self.register(graph).await?;
        self.run_until_quiescent(&registered.id).await
    }

    /// Persist a valid graph without executing its first node. Callers that
    /// need live surface attachment use this boundary to publish the durable
    /// graph identity before any provider or tool work begins.
    pub(crate) async fn register(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.register_continuation(graph, None, None, None).await
    }

    pub(crate) async fn register_continuation(
        &self,
        graph: ExecutionGraph,
        actor: Option<harness_contract::agent_action::AgentActorBinding>,
        source_goal_cut: Option<crate::ExpectedStreamRevision>,
        original_owner: Option<Arc<crate::execution_core::services::OriginalGraphRegistrationOwner>>,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        validate_execution_graph(&graph)?;
        self.registry.validate_graph(&graph)?;
        match Box::pin(self.commit_service.register_continuation_graph_async(
            graph, actor, source_goal_cut, original_owner,
        )).await
        {
            Ok(receipt) => Ok(receipt.graph),
            Err(ExecutionCommitError::AlreadyAppliedSame { graph_id }) => self
                .state_store
                .load_async(graph_id)
                .await
                .map_err(ExecutionRunnerError::from),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn run_until_quiescent(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        let durable_progress = Arc::new(Notify::new());
        let mut active_nodes = tokio::task::JoinSet::new();
        let mut in_flight = BTreeSet::new();
        loop {
            let snapshot = self.pump_snapshot(graph_id, &in_flight).await?;
            let ready = snapshot
                .ready
                .into_iter()
                .take(64usize.saturating_sub(active_nodes.len()))
                .collect::<Vec<_>>();
            let prepared = match self.prepare_ready_wave(graph_id, ready).await {
                Ok(prepared) => prepared,
                Err(ExecutionRunnerError::ResourceDeferred { .. }) if active_nodes.is_empty() => {
                    self.wait_for_resource_change().await;
                    continue;
                }
                Err(ExecutionRunnerError::ResourceDeferred { .. }) => Vec::new(),
                Err(error) => return Err(error),
            };
            for prepared in prepared {
                let node_id = prepared.node.id.clone();
                if !in_flight.insert(node_id.clone()) {
                    continue;
                }
                let runner = self.clone();
                let graph_id = graph_id.to_string();
                let progress = Arc::clone(&durable_progress);
                active_nodes.spawn(async move {
                    (
                        node_id,
                        runner
                            .execute_prepared_pump_node(&graph_id, prepared, progress)
                            .await,
                    )
                });
            }
            if active_nodes.is_empty() {
                return Ok(snapshot.report);
            }
            tokio::select! {
                () = durable_progress.notified() => {}
                joined = active_nodes.join_next() => {
                    let (node_id, result) = joined
                        .ok_or_else(|| ExecutionRunnerError::Join(
                            "test completion pump closed unexpectedly".to_string(),
                        ))?
                        .map_err(|error| ExecutionRunnerError::Join(error.to_string()))?;
                    in_flight.remove(&node_id);
                    result?;
                }
            }
        }
    }

    pub(crate) async fn pump_snapshot(
        &self,
        graph_id: &str,
        in_flight: &BTreeSet<String>,
    ) -> Result<ExecutionGraphPumpSnapshot, ExecutionRunnerError> {
        self.pump_snapshot_with_unknown(graph_id, in_flight, &BTreeSet::new()).await
    }

    pub(crate) async fn pump_snapshot_with_unknown(
        &self, graph_id: &str, in_flight: &BTreeSet<String>, unknown: &BTreeSet<String>,
    ) -> Result<ExecutionGraphPumpSnapshot, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        let graph = self.state_store.load_async(graph_id).await?;
        let mut graph = self.advance_dependencies(graph).await?;
        for node_id in quorum_tail_cancellations_excluding(&graph, &unknown_dependency_nodes(&graph, unknown)) {
            if unknown.contains(&node_id) { continue; }
            let current = self.state_store.load_async(graph_id).await?;
            if !quorum_tail_cancellations_excluding(&current, &unknown_dependency_nodes(&current, unknown)).contains(&node_id) { continue; }
            if current
                .node_statuses
                .get(&node_id)
                .is_none_or(|status| status.is_terminal())
            {
                continue;
            }
            graph = self
                .command(
                    graph_id,
                    ExecutionGraphCommand::CancelNode {
                        expected_revision: current.revision,
                        node_id,
                        reason: "optional work cancelled after dependency quorum".to_string(),
                    },
                )
                .await?;
        }
        graph = self.advance_dependencies(graph).await?;
        let prohibited = unknown_dependency_nodes(&graph, unknown);
        let mut ready = Vec::new();
        for node in &graph.nodes {
            if graph.node_statuses[&node.id] == ExecutionNodeStatus::Ready
                && !in_flight.contains(&node.id)
                && !prohibited.contains(&node.id)
                && self.dependency_publications_ready(&graph, node)?
            {
                ready.push(node.clone());
            }
        }
        Ok(ExecutionGraphPumpSnapshot {
            ready,
            report: report(&graph),
        })
    }

    /// A terminal node commit is itself the durable wake-up boundary. Reduce
    /// dependencies before executor cleanup so a lost process-local notify or
    /// a slow `after_commit` hook cannot strand Verify/Synthesize behind an
    /// already-terminal Agent wave. This also commits typed orphan failure as
    /// soon as every Agent is terminal and required autonomous work remains.
    async fn converge_after_terminal_commit(
        &self,
        graph_id: &str,
    ) -> Result<(), ExecutionRunnerError> {
        let graph = self.state_store.load_async(graph_id).await?;
        self.advance_dependencies(graph).await?;
        Ok(())
    }

    pub(crate) async fn current_report(
        &self,
        graph_id: &str,
    ) -> Result<(ExecutionRunReport, bool), ExecutionRunnerError> {
        let graph = self.state_store.load_async(graph_id).await?;
        let has_actionable_node = graph.nodes.iter().any(|node| {
            let status = graph.node_statuses[&node.id];
            if matches!(
                status,
                ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running
            ) {
                return true;
            }
            if status != ExecutionNodeStatus::Planned {
                return false;
            }
            let predecessors = dependency_predecessors(&graph, node);
            let dependency = node
                .work
                .as_ref()
                .map(|work| work.dependency.clone())
                .unwrap_or_default();
            dependency_target(&dependency, &predecessors).is_some()
        });
        let quiescent = !has_actionable_node;
        Ok((report(&graph), quiescent))
    }

    pub(crate) fn subscribe_state_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.state_store.subscribe_commits()
    }

    /// Resource pressure is a soft wait, not a terminal graph outcome.  The
    /// completion pump owns the retry loop so a released lease wakes the same
    /// durable Ready node instead of silently ending its graph driver.
    pub(crate) async fn wait_for_resource_change(&self) {
        self.resource_manager.wait_for_change().await;
    }

    pub(crate) async fn terminal_report(
        &self,
        graph_id: &str,
    ) -> Result<Option<ExecutionRunReport>, ExecutionRunnerError> {
        let graph = self.state_store.load_current_async(graph_id).await?;
        Ok(graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
            .then(|| report(&graph)))
    }

    /// Prepares every currently schedulable node and commits the admitted wave
    /// as one durable graph mutation. Executor work starts only after this
    /// returns, so short tasks can reach the configured concurrency instead of
    /// completing while later siblings are still paying one fsync each.
    pub(crate) async fn prepare_ready_wave(
        &self,
        graph_id: &str,
        nodes: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
    ) -> Result<Vec<ExecutionPreparedPumpNode>, ExecutionRunnerError> {
        self.prepare_ready_wave_with_unknown(graph_id, nodes, &BTreeSet::new()).await
    }

    pub(crate) async fn prepare_ready_wave_with_unknown(
        &self, graph_id: &str, nodes: Vec<harness_contract::execution_graph::ExecutionNodeSpec>, unknown: &BTreeSet<String>,
    ) -> Result<Vec<ExecutionPreparedPumpNode>, ExecutionRunnerError> {
        for _retry in 0..8 {
            match self.prepare_ready_wave_once(graph_id, nodes.clone(), unknown).await {
                Err(ExecutionRunnerError::AdmissionRaced { .. }) => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(ExecutionRunnerError::ResourceDeferred {
            node_id: graph_id.into(),
            reason: "fresh unadmitted graph changed during bounded preparation".into(),
        })
    }

    async fn prepare_ready_wave_once(
        &self,
        graph_id: &str,
        nodes: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
        unknown: &BTreeSet<String>,
    ) -> Result<Vec<ExecutionPreparedPumpNode>, ExecutionRunnerError> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
            return Ok(nodes
                .into_iter()
                .map(|node| ExecutionPreparedPumpNode {
                    binding: None,
                    startup_error: Some(ExecutionRunnerError::CommandSuperseded {
                        node_id: node.id.clone(),
                    }),
                    node,
                })
                .collect());
        }
        // Win the graph mutation order before invoking executor.start. A
        // Pause/Cancel command that arrives afterward must observe this
        // wave's single Running revision, matching the established command
        // CAS contract, while all actual executor polling remains outside.
        let coordination = self.graph_coordination(graph_id).await;
        let graph = self.state_store.load_snapshot_async(graph_id).await?;
        let prohibited = unknown_dependency_nodes(&graph, unknown);
        let mut prepared = Vec::with_capacity(nodes.len());
        let mut output = Vec::with_capacity(nodes.len());
        for node in nodes {
            if prohibited.contains(&node.id) { continue; }
            if graph.node_statuses.get(&node.id) != Some(&ExecutionNodeStatus::Ready)
                || !self.dependency_publications_ready(&graph, &node)?
            {
                continue;
            }
            if self
                .active
                .lock()
                .await
                .contains_key(&(graph_id.to_string(), node.id.clone()))
            {
                output.push(ExecutionPreparedPumpNode {
                    binding: None,
                    startup_error: Some(ExecutionRunnerError::ResourceDeferred {
                        node_id: node.id.clone(),
                        reason: "original physical/lease owner still retained".into(),
                    }),
                    node,
                });
                continue;
            }
            let deadline_at_ms = match execution_deadline_at_ms(&node) {
                Ok(value) => value,
                Err(error) => {
                    output.push(ExecutionPreparedPumpNode {
                        binding: None,
                        node,
                        startup_error: Some(error),
                    });
                    continue;
                }
            };
            if deadline_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
                output.push(ExecutionPreparedPumpNode {
                    binding: None,
                    startup_error: Some(ExecutionRunnerError::DeadlineExceeded {
                        node_id: node.id.clone(),
                        deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                    }),
                    node,
                });
                continue;
            }
            let mut resources = match self
                .acquire_node_resources(graph.as_ref(), &node, deadline_at_ms, Some(Duration::ZERO))
                .await
            {
                Ok(resources) => resources,
                Err(error) => {
                    output.push(ExecutionPreparedPumpNode {
                        binding: None,
                        node,
                        startup_error: Some(error),
                    });
                    continue;
                }
            };
            let resource_kind = resources
                .resource
                .as_ref()
                .map(|resource| resource.kind().clone());
            let resource_queue_wait = resources
                .resource
                .as_ref()
                .map_or(Duration::ZERO, ExecutionResourceLease::queue_wait);
            let resource_started = std::time::Instant::now();
            let Some(executor) = self.registry.get(&node.executor_kind) else {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                );
                output.push(ExecutionPreparedPumpNode {
                    binding: None,
                    startup_error: Some(
                        NodeExecutorError::Unavailable {
                            executor_kind: node.executor_kind.clone(),
                            node_id: node.id.clone(),
                        }
                        .into(),
                    ),
                    node,
                });
                continue;
            };
            let attempt = match ExecutionNodeBinding::next_attempt(&graph, &node.id) {
                Ok(attempt) => attempt,
                Err(reason) => {
                    self.record_resource_terminal(
                        resource_kind.as_ref(),
                        resource_queue_wait,
                        resource_started,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    );
                    self.discard_unpolled_wave(&prepared);
                    return Err(NodeExecutorError::Invalid {
                        node_id: node.id,
                        reason,
                    }
                    .into());
                }
            };
            let ticket = match executor
                .start(NodeExecutionContext {
                    graph: Arc::clone(&graph),
                    node: node.clone(),
                    attempt,
                })
                .await
            {
                Ok(ticket) => ticket,
                Err(error) => {
                    self.record_resource_terminal(
                        resource_kind.as_ref(),
                        resource_queue_wait,
                        resource_started,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    );
                    if matches!(&error, NodeExecutorError::Start { node_id, .. } if node_id == &node.id) {
                        // No ticket or binding was admitted. Keep the other
                        // prepared siblings and isolate this node through the
                        // same startup-error path as other preparation failures.
                        output.push(ExecutionPreparedPumpNode {
                            binding: None,
                            node,
                            startup_error: Some(error.into()),
                        });
                        continue;
                    }
                    self.discard_unpolled_wave(&prepared);
                    return Err(error.into());
                }
            };
            if let Err(reason) =
                ExecutionNodeBinding::validate_ticket(&ticket, &graph, &node, attempt)
            {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                );
                self.discard_unpolled_wave(&prepared);
                return Err(NodeExecutorError::Invalid {
                    node_id: node.id,
                    reason,
                }
                .into());
            }
            let leaf_effect_owner = node.kind
                == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                && node.executor_kind == "tool_batch";
            let effect_state = match self
                .commit_service
                .validate_fresh_node_admission(&graph, &node, &ticket)
            {
                Ok(()) => ExecutionEffectState::Fresh,
                Err(error) => {
                    self.record_resource_terminal(
                        resource_kind.as_ref(),
                        resource_queue_wait,
                        resource_started,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    );
                    self.discard_unpolled_wave(&prepared);
                    return Err(error.into());
                }
            };
            let effect_receipt_required =
                !leaf_effect_owner && matches!(effect_state, ExecutionEffectState::Fresh);
            let binding = resources.binding(&ticket);
            if ExecutionBindingLeaseClosure::required(&binding) {
                let key = ExecutionBindingLeaseClosure::fence_key(&graph, &node.id, &binding);
                resources.original_binding = Some(binding.clone());
                let mut admission_error = None;
                let owner = self.scope_locks.acquire_binding_owner(&key, || {
                    self.commit_service
                        .validate_unadmitted_binding_start(&graph, &node, &ticket)
                        .map_err(|error| {
                            let reason = error.to_string();
                            admission_error = Some(error);
                            reason
                        })
                });
                resources.owner_fence = Some(match owner {
                    Ok(owner) => owner,
                    Err(error) => {
                        self.discard_unpolled_wave(&prepared);
                        if matches!(
                            admission_error.as_ref(),
                            Some(ExecutionCommitError::EventStore(
                                crate::RuntimeEventStoreError::StaleRevision { .. }
                            ))
                        ) {
                            return Err(ExecutionRunnerError::AdmissionRaced {
                                graph_id: graph_id.into(),
                            });
                        }
                        if let Some(error) = admission_error {
                            return Err(error.into());
                        }
                        return Err(ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason: error.to_string(),
                        });
                    }
                });
            }
            prepared.push(PreparedNodeStart {
                node,
                executor,
                ticket,
                resources,
                resource_kind,
                resource_queue_wait,
                resource_started,
                effect_state,
                effect_receipt_required,
            });
        }

        if prepared.is_empty() {
            return Ok(output);
        }

        let current = Arc::clone(&graph);
        let mut admitted = Vec::with_capacity(prepared.len());
        let mut superseded = Vec::new();
        for start in prepared {
            if current.node_statuses.get(&start.node.id) == Some(&ExecutionNodeStatus::Ready) {
                admitted.push(start);
            } else {
                superseded.push(start);
            }
        }
        if !admitted.is_empty() {
            let bindings = admitted
                .iter()
                .map(|start| {
                    (
                        start.node.id.clone(),
                        start.resources.binding(&start.ticket),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let effect_tickets = admitted
                .iter()
                .filter(|start| start.effect_receipt_required)
                .map(|start| start.ticket.clone())
                .collect::<Vec<_>>();
            let mut observations = Vec::with_capacity(admitted.len());
            for start in &admitted {
                match self.registry.observe_builtin_binding(&start.ticket) {
                    Ok(observation) => observations.push(observation),
                    Err(error) => {
                        for (original, observation) in admitted.iter().zip(&observations) {
                            if let Some(observation) = observation {
                                self.registry.rollback_unadmitted_builtin_binding(
                                    &original.ticket,
                                    observation,
                                    &self.commit_service,
                                )?;
                            }
                        }
                        self.discard_unpolled_wave(&admitted);
                        return Err(error.into());
                    }
                }
            }
            for (index, start) in admitted.iter().enumerate() {
                if let Err(error) = start.executor.prepare_original_binding(&start.ticket) {
                    for previous in admitted.iter().take(index) {
                        self.commit_service.validate_unadmitted_binding_rollback(&previous.ticket)?;
                        previous.executor.rollback_original_binding(&previous.ticket)?;
                    }
                    for (original, observation) in admitted.iter().zip(&observations) {
                        if let Some(observation) = observation {
                            self.registry.rollback_unadmitted_builtin_binding(&original.ticket, observation, &self.commit_service)?;
                        }
                    }
                    self.discard_unpolled_wave(&admitted);
                    return Err(error.into());
                }
            }
            {
                let mut active = self.active.lock().await;
                for (mut start, no_dispatch) in admitted.drain(..).zip(observations) {
                    start.resources.admission_pending = true;
                    output.push(ExecutionPreparedPumpNode {
                        binding: bindings.get(&start.node.id).cloned(),
                        node: start.node.clone(),
                        startup_error: None,
                    });
                    active.insert(
                        (start.ticket.graph_id.clone(), start.ticket.node_id.clone()),
                        ActiveNode {
                            executor: start.executor,
                            binding: start.resources.binding(&start.ticket),
                            ticket: start.ticket,
                            resource_kind: start.resource_kind,
                            resource_queue_wait: start.resource_queue_wait,
                            resource_started: start.resource_started,
                            effect_state: start.effect_state,
                            effect_receipt_required: start.effect_receipt_required,
                            cancel_outcome: Arc::new(CancelOutcomeHandoff::default()),
                            resources: Arc::new(StdMutex::new(start.resources)),
                            release_blocked: true,
                            callback_completed: false,
                            drain_completed: false,
                            callback_gate: Arc::new(Mutex::new(())),
                            no_dispatch,
                        },
                    );
                }
            }
            if let Err(error) = self
                .commit_service
                .bind_and_start_nodes_async(current.as_ref().clone(), bindings, effect_tickets)
                .await
            {
                let starts = self
                    .active
                    .lock()
                    .await
                    .iter()
                    .filter(|((id, node), _)| {
                        id == graph_id
                            && output
                                .iter()
                                .any(|item| item.startup_error.is_none() && &item.node.id == node)
                    })
                    .map(|(_, active)| {
                        (
                            active.ticket.clone(),
                            active.binding.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                let states = starts
                    .iter()
                    .map(|(ticket, binding)| {
                        self.commit_service
                            .binding_start_was_admitted(graph_id, ticket, binding)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if states.is_empty()
                    || states.iter().any(|value| *value) && states.iter().any(|value| !*value)
                {
                    return Err(ExecutionRunnerError::Driver("atomic start response has inconsistent canonical admission; original owners retained".into()));
                }
                if states.iter().all(|value| !*value) {
                    let mut active = self.active.lock().await;
                    for (ticket, binding) in &starts {
                        let original = active
                            .get(&(graph_id.to_string(), ticket.node_id.clone()))
                            .filter(|original| {
                                original.ticket == *ticket
                                    && original.binding.clone() == *binding
                            })
                            .ok_or_else(|| {
                                ExecutionRunnerError::Driver(
                                    "unadmitted rollback original owner changed".into(),
                                )
                            })?;
                        if let Some(observation) = original.no_dispatch.as_ref() {
                            self.registry.rollback_unadmitted_builtin_binding(
                                ticket,
                                observation,
                                &self.commit_service,
                            )?;
                        }
                        self.commit_service.validate_unadmitted_binding_rollback(ticket)?;
                        original.executor.rollback_original_binding(ticket)?;
                    }
                    let aborted = starts
                        .iter()
                        .filter_map(|(ticket, _)| {
                            active.remove(&(graph_id.to_string(), ticket.node_id.clone()))
                        })
                        .collect::<Vec<_>>();
                    self.active_changed.notify_one();
                    drop(active);
                    drop(coordination);
                    for mut start in aborted {
                        start.resources.lock().unwrap_or_else(std::sync::PoisonError::into_inner).admission_pending = false;
                        self.record_resource_terminal(
                            start.resource_kind.as_ref(),
                            start.resource_queue_wait,
                            start.resource_started,
                            crate::execution_core::graph::ResourceResultClass::Failed,
                        );
                    }
                    if matches!(&error, ExecutionCommitError::EventStore(
                        crate::RuntimeEventStoreError::StaleRevision { stream_id, .. }
                    ) if stream_id == graph_id)
                    {
                        return Err(ExecutionRunnerError::AdmissionRaced {
                            graph_id: graph_id.into(),
                        });
                    }
                    return Err(error.into());
                }
            }
        }
        drop(coordination);
        for start in superseded {
            let installed = self.active.lock().await.get(&(graph_id.to_owned(), start.node.id.clone()))
                .map(|active| active.ticket == start.ticket
                    && Arc::ptr_eq(&active.executor, &start.executor));
            match installed {
                Some(true) => {
                    let deadline = std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
                    if !self.drain_active_original(graph_id, &start.executor, &start.ticket, deadline).await? {
                        return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                            node_id:start.node.id.clone(), reason:"original start committed while superseded".into(),
                        }));
                    }
                }
                Some(false) => return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                    node_id:start.node.id.clone(), reason:"superseded start has a different active owner".into(),
                })),
                None => {
                    self.commit_service.validate_unadmitted_binding_rollback(&start.ticket)?;
                    start.executor.cancel_bounded(&start.ticket, PHYSICAL_CLOSURE_BUDGET).await?;
                }
            }
            self.record_resource_terminal(
                start.resource_kind.as_ref(),
                start.resource_queue_wait,
                start.resource_started,
                crate::execution_core::graph::ResourceResultClass::Cancelled,
            );
            output.push(ExecutionPreparedPumpNode {
                binding: Some(start.resources.binding(&start.ticket)),
                startup_error: Some(ExecutionRunnerError::CommandSuperseded {
                    node_id: start.node.id.clone(),
                }),
                node: start.node,
            });
        }
        Ok(output)
    }

    fn discard_unpolled_wave(&self, prepared: &[PreparedNodeStart]) {
        for start in prepared {
            self.record_resource_terminal(
                start.resource_kind.as_ref(),
                start.resource_queue_wait,
                start.resource_started,
                crate::execution_core::graph::ResourceResultClass::Failed,
            );
        }
    }

    pub(crate) async fn execute_prepared_pump_node(
        &self,
        graph_id: &str,
        prepared: ExecutionPreparedPumpNode,
        durable_progress: Arc<Notify>,
    ) -> Result<(), ExecutionRunnerError> {
        let expected_binding = prepared.binding.clone();
        let node_id = prepared.node.id.clone();
        #[cfg(feature = "test-fixtures")]
        let mut diagnostic_ticket = None;
        let handoff = self.active.lock().await.get(&(graph_id.to_string(), node_id.clone()))
            .filter(|active| Some(&active.binding) == expected_binding.as_ref())
            .map(|active| {
                #[cfg(feature = "test-fixtures")]
                { diagnostic_ticket = Some(active.ticket.clone()); }
                Arc::clone(&active.cancel_outcome)
            });
        #[cfg(feature = "test-fixtures")]
        let diagnostic_ticket = diagnostic_ticket.filter(k05_completion_trace_enabled);
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "start_and_execute_enter", None, None); }
        let result = match prepared.startup_error {
            Some(error) => Err(error),
            None => {
                self.start_and_execute_node(graph_id, prepared.node, expected_binding.as_ref())
                    .await
            }
        };
        // Normalize before publishing the original handoff. An admitted
        // Start is unknown custody, never a finished preparation failure.
        let result = result.map_err(|error| normalize_start_failure(
            &node_id, expected_binding.is_some(), error,
        ));
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "start_and_execute_return", None, Some(result.is_ok())); }
        #[cfg(feature = "test-fixtures")]
        {
            let detail = match &result {
                Ok((_, outcome)) => format!("result=ok status={:?}", outcome.result.status),
                Err(error) => format!("result=error kind={} original_detail=private", build16_runner_error_kind(error)),
            };
            build16_callback_trace(graph_id, &node_id, expected_binding.as_ref().map(|b| b.attempt), "drive_return", &detail);
        }
        let result = if let Some(handoff) = handoff.as_ref() {
            let mut state = handoff.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let result = match result {
                Ok((node, outcome)) => {
                    state.finished = true;
                    state.outcome = Some(Ok(outcome.clone()));
                    Ok((node, outcome))
                }
                Err(error) => {
                    state.finished = !matches!(&error, ExecutionRunnerError::Executor(
                        NodeExecutorError::OriginalCustodyUnknown { .. }));
                    let error = Arc::new(error);
                    state.outcome = Some(Err(Arc::clone(&error)));
                    Err(original_shared_error(&error))
                }
            };
            drop(state);
            handoff.ready.notify_one();
            result
        } else { result };
        let binding_matches = self
            .active_binding_matches(graph_id, &node_id, expected_binding.as_ref())
            .await;
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "binding_checked", None, Some(binding_matches)); }
        if !binding_matches {
            return Ok(());
        }
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "finish_pump_enter", None, None); }
        // Keep the large terminal-transition future out of its caller's
        // async frame. Its ownership, cancellation and fences are unchanged.
        let result = Box::pin(self.finish_pump_node(
            graph_id, &node_id, result, durable_progress, expected_binding, handoff,
        )).await;
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "finish_pump_return", None, Some(result.is_ok())); }
        result
    }

    #[cfg(test)]
    pub(super) async fn original_pump_outcome_for_test(
        &self,
        graph_id: &str,
        node_id: &str,
    ) -> Option<(bool, Option<Result<NodeExecutionOutcome, Arc<ExecutionRunnerError>>>, bool)> {
        let active = self.active.lock().await;
        let owner = active.get(&(graph_id.to_string(), node_id.to_string()))?;
        let state = owner.cancel_outcome.state.lock().unwrap();
        Some((state.finished, state.outcome.clone(), state.original_binding_closed))
    }

    #[cfg(test)]
    async fn drop_injected_empty_bound_owner(
        &self,
        graph_id: &str,
        node: &str,
        binding: Option<&ExecutionNodeBinding>,
        error: &ExecutionCommitError,
    ) -> Result<bool, ExecutionRunnerError> {
        if !matches!(error,ExecutionCommitError::InvalidCommand(reason) if reason=="450 original leaf completed before Graph result append fault")
        {
            return Ok(false);
        }
        let graph = self.state_store.load_async(graph_id).await?;
        if !super::executors::agent_tool::is_bound_agent_tool_graph(&graph)
            || graph.node_statuses.get(node) != Some(&ExecutionNodeStatus::Running)
        {
            return Err(ExecutionRunnerError::Driver(
                "test crash injection lost original bound Running identity".into(),
            ));
        }
        let mut active = self.active.lock().await;
        let key = (graph_id.to_string(), node.to_string());
        let Some(owner) = active.get(&key) else {
            return Ok(false);
        };
        let actual = owner.binding.clone();
        if Some(&actual) != binding
            || !actual.resource_lease_refs.is_empty()
            || actual.scope_lease_ref.is_some()
            || actual.worktree_lease_ref.is_some()
            || owner.callback_completed
        {
            return Err(ExecutionRunnerError::Driver(
                "test crash injection cannot drop nonempty or closed owner".into(),
            ));
        }
        drop(active.remove(&key));
        self.active_changed.notify_one();
        Ok(true)
    }

    async fn active_binding_matches(
        &self,
        graph_id: &str,
        node_id: &str,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> bool {
        self.active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.to_string()))
            .map(|active| active.binding.clone())
            .as_ref()
            == expected_binding
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn active_binding_for_ticket_for_test(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<ExecutionNodeBinding> {
        self.active
            .lock()
            .await
            .get(&(ticket.graph_id.clone(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| active.binding.clone())
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn reject_stale_binding_for_test(
        &self,
        graph: &str,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<(), ExecutionRunnerError> {
        self.isolate_node_failure(
            graph,
            node,
            Some(binding),
            "late recovery owner probe".into(),
        )
        .await
    }

    async fn binding_callback_gate(
        &self,
        graph_id: &str,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<Mutex<()>>> {
        self.active
            .lock()
            .await
            .get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| active.callback_gate.clone())
    }

    /// Finish only the still-live original binding; absence is not a receipt.
    pub(crate) async fn settle_retained_original_callback<B, F>(
        &self,
        graph_id: &str,
        node_id: &str,
        attempt: u32,
        verify_original: B,
        verify_calls: F,
    ) -> Result<Option<NodeExecutionTicket>, ExecutionRunnerError>
    where
        B: FnOnce(&NodeExecutionTicket) -> Result<bool, String> + Send,
        F: FnOnce(&NodeExecutionTicket, Option<&Arc<super::executors::agent_tool::NoDispatchObservation>>) -> Result<bool, String> + Send,
    {
        let key = (graph_id.to_owned(), node_id.to_owned());
        let original = self.active.lock().await.get(&key).filter(|active| {
            active.ticket.attempt == attempt && active.ticket.graph_id == graph_id
        }).map(|active| (
            active.ticket.clone(), active.binding.clone(), Arc::clone(&active.executor),
            Arc::clone(&active.callback_gate), active.no_dispatch.clone(),
        ));
        let Some((ticket, binding, executor, gate, observation)) = original else { return Ok(None) };
        if ticket.executor_kind == "tool_batch" && observation.is_none() { return Ok(None); }
        let _owner = gate.lock().await;
        let matches_original = |active: &ActiveNode| {
            active.ticket == ticket && active.binding == binding
                && Arc::ptr_eq(&active.executor, &executor)
                && Arc::ptr_eq(&active.callback_gate, &gate)
                && match (&active.no_dispatch, &observation) {
                    (Some(current), Some(original)) => Arc::ptr_eq(current, original),
                    (None, None) => true,
                    _ => false,
                }
        };
        if self.command_intent_waiter(graph_id).is_some()
            || !self.active.lock().await.get(&key).is_some_and(|active| {
                matches_original(active)
                    && active.cancel_outcome.state.lock().ok().is_some_and(|state| !state.polled)
            })
        {
            return Ok(None);
        }
        if !verify_original(&ticket).map_err(ExecutionRunnerError::Driver)? {
            return Ok(None);
        }
        let graph = self.state_store.load_async(graph_id).await?;
        if self.commit_service.binding_callback_disposition(&graph, node_id, &binding)?
            != CallbackDisposition::Commit
        {
            return Ok(None);
        }
        drop(_owner);
        self.complete_callback_observed(graph_id, &executor, &ticket, None).await?;
        let _owner = gate.lock().await;
        if !self.active.lock().await.get(&key).is_some_and(|active| matches_original(active)) {
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id:ticket.node_id.clone(), reason:"original retained callback owner changed".into(),
            }));
        }
        if !verify_calls(&ticket, observation.as_ref()).map_err(ExecutionRunnerError::Driver)? {
            return Ok(None);
        }
        if self.command_intent_waiter(graph_id).is_none()
            && self.active.lock().await.get(&key).is_some_and(|active| {
                matches_original(active) && active.callback_completed && !active.release_blocked
                    && active.cancel_outcome.state.lock().ok().is_some_and(|state| !state.polled)
            }) {
            Ok(Some(ticket))
        } else {
            Ok(None)
        }
    }

    async fn replace_original_terminal_handoff(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, binding: &ExecutionNodeBinding,
        executor: &Arc<dyn NodeExecutor>, original: &NodeExecutionOutcome, candidate: &NodeExecutionOutcome,
        handle: &super::registry::OriginalSynthesizeReplanHandle,
        retained: &crate::runtime_event_store::OriginalRetainedTerminal,
    ) -> Result<(), ExecutionRunnerError> {
        let gate = self.binding_callback_gate(graph_id, ticket).await
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal callback gate unavailable".into()))?;
        let _guard = gate.lock().await;
        if self.command_intent_waiter(graph_id).is_some() {
            return Err(ExecutionRunnerError::Driver("original terminal replacement cancelled".into()));
        }
        let handoff = self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket && active.binding == *binding
                && Arc::ptr_eq(&active.executor, executor) && Arc::ptr_eq(&active.callback_gate, &gate)
                && !active.callback_completed)
            .map(|active| Arc::clone(&active.cancel_outcome))
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal replacement owner changed".into()))?;
        let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
        if !state.finished || state.outcome.as_ref().and_then(|value| value.as_ref().ok()) != Some(original) || state.terminal_replan_cleanup.is_some()
            || state.terminal_release_action.is_some() || state.lease_close_action.is_some() {
            return Err(ExecutionRunnerError::Driver("original terminal handoff differs".into()));
        }
        if state.cleanup_action.as_ref().is_some_and(|action| !matches!(action.peek(), Some(Ok(())))) {
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "previous original cleanup remains unconfirmed".into(),
            }));
        }
        state.cleanup_action = None;
        state.cleanup_deadline = None;
        state.cleanup_nonce = None;
        state.superseded_outcomes.push(original.clone());
        state.outcome = Some(Ok(candidate.clone()));
        state.terminal_replan_cleanup = Some(OriginalTerminalReplanCleanup {
            original: original.clone(), candidate: candidate.clone(), handle: handle.clone(), retained: retained.clone(),
            receipt: None, request: None, transferred: false, retired: false,
        });
        Ok(())
    }

    async fn replace_rolled_back_replan_handoff(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, binding: &ExecutionNodeBinding,
        executor: &Arc<dyn NodeExecutor>, original: &NodeExecutionOutcome,
        previous: &NodeExecutionOutcome, candidate: &NodeExecutionOutcome,
    ) -> Result<(), ExecutionRunnerError> {
        let gate = self.binding_callback_gate(graph_id, ticket).await
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal callback gate unavailable".into()))?;
        let _guard = gate.lock().await;
        if self.command_intent_waiter(graph_id).is_some() {
            return Err(ExecutionRunnerError::Driver("original terminal replacement cancelled".into()));
        }
        let handoff = self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket && active.binding == *binding
                && Arc::ptr_eq(&active.executor, executor) && Arc::ptr_eq(&active.callback_gate, &gate)
                && !active.callback_completed)
            .map(|active| Arc::clone(&active.cancel_outcome))
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal replacement owner changed".into()))?;
        let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
        if state.outcome.as_ref().and_then(|value| value.as_ref().ok()) != Some(previous) {
            return Err(ExecutionRunnerError::Driver("original replacement handoff differs".into()));
        }
        let cleanup = state.terminal_replan_cleanup.as_mut().filter(|cleanup|
            &cleanup.original == original && &cleanup.candidate == previous && !cleanup.transferred && !cleanup.retired
                && cleanup.receipt.is_none() && cleanup.request.is_none())
            .ok_or_else(|| ExecutionRunnerError::Driver("original replacement cleanup differs".into()))?;
        if !cleanup.retained.owner.replacement_rollback_confirmed_for_outcome(ticket, previous)
            .map_err(ExecutionRunnerError::Driver)? {
            return Err(ExecutionRunnerError::Driver("original replacement rollback unconfirmed".into()));
        }
        cleanup.candidate = candidate.clone();
        state.superseded_outcomes.push(previous.clone());
        state.outcome = Some(Ok(candidate.clone()));
        Ok(())
    }

    // Capture the exact owner and retain its original handoff before waiting.
    async fn original_terminal_cleanup_handoff(
        &self, graph_id: &str, ticket: &NodeExecutionTicket,
        executor: &Arc<dyn NodeExecutor>,
    ) -> Result<Option<(Arc<Mutex<()>>, Arc<CancelOutcomeHandoff>, OriginalTerminalReplanCleanup, OriginalCleanupInFlight)>, ExecutionRunnerError> {
        let captured = {
            let active = self.active.lock().await;
            let Some(owner) = active.get(&(graph_id.to_owned(), ticket.node_id.clone()))
                .filter(|owner| owner.ticket == *ticket) else { return Ok(None); };
            if !Arc::ptr_eq(&owner.executor, executor) {
                return Err(ExecutionRunnerError::Driver("original terminal cleanup owner changed".into()));
            }
            (owner.binding.clone(), Arc::clone(&owner.callback_gate), Arc::clone(&owner.cancel_outcome))
        };
        self.original_terminal_cleanup_captured(graph_id, ticket, executor, captured).await
    }

    async fn original_terminal_cleanup_captured(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, executor: &Arc<dyn NodeExecutor>,
        (binding, gate, handoff): (ExecutionNodeBinding, Arc<Mutex<()>>, Arc<CancelOutcomeHandoff>),
    ) -> Result<Option<(Arc<Mutex<()>>, Arc<CancelOutcomeHandoff>, OriginalTerminalReplanCleanup, OriginalCleanupInFlight)>, ExecutionRunnerError> {
        let guard = gate.lock().await;
        let active = self.active.lock().await;
        let callback_completed = match active.get(&(graph_id.to_owned(), ticket.node_id.clone())) {
            Some(owner) if owner.ticket == *ticket && owner.binding == binding
                && Arc::ptr_eq(&owner.executor, executor) && Arc::ptr_eq(&owner.callback_gate, &gate)
                && Arc::ptr_eq(&owner.cancel_outcome, &handoff) => owner.callback_completed,
            Some(_) => return Err(ExecutionRunnerError::Driver("original terminal cleanup owner changed".into())),
            None => {
                let state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
                if state.original_binding_closed { return Ok(None); }
                return Err(ExecutionRunnerError::Driver("original terminal cleanup owner changed".into()));
            }
        };
        drop(active);
        let cleanup = {
            let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
            if state.drain.as_ref().is_some_and(|drain| !matches!(drain.peek(), Some(Ok(_)))) || state.cleanup_inflight
                || state.proof_inflight || (!callback_completed && state.callback_action.is_some()) {
                return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(), reason: "original physical drain or terminal cleanup remains unresolved".into(),
                }));
            }
            let cleanup = state.terminal_replan_cleanup.clone();
            if cleanup.as_ref().is_some_and(|cleanup| !cleanup.retired) && !callback_completed {
                state.cleanup_inflight = true;
            }
            cleanup
        };
        if callback_completed {
            if cleanup.as_ref().is_some_and(|cleanup| !cleanup.retired) {
                return Err(ExecutionRunnerError::Driver("original terminal cleanup unconfirmed".into()));
            }
            return Ok(None);
        }
        drop(guard);
        Ok(cleanup.map(|cleanup| {
            let inflight = OriginalCleanupInFlight(Arc::clone(&handoff));
            (gate, handoff, cleanup, inflight)
        }))
    }

    async fn resume_original_terminal_replan_cleanup(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, executor: &Arc<dyn NodeExecutor>,
    ) -> Result<(), ExecutionRunnerError> {
        self.resume_original_terminal_replan_cleanup_until(
            graph_id, ticket, executor, std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET,
        ).await
    }

    async fn resume_original_terminal_replan_cleanup_until(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, executor: &Arc<dyn NodeExecutor>,
        deadline: std::time::Instant,
    ) -> Result<(), ExecutionRunnerError> {
        let unknown = |reason: &str| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: reason.into(),
        });
        let owner = self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone()))
            .filter(|owner| owner.ticket == *ticket && Arc::ptr_eq(&owner.executor, executor))
            .map(|owner| PumpCancellationOwner { binding: owner.binding.clone(), ticket: owner.ticket.clone(),
                executor: Arc::clone(&owner.executor), gate: Arc::clone(&owner.callback_gate),
                handoff: Arc::clone(&owner.cancel_outcome) });
        let Some(owner) = owner else { return Err(unknown("original terminal cleanup owner unavailable")); };
        let deadline = owner.handoff.closing_deadline(deadline)
            .map_err(|_| unknown("original closing deadline unavailable"))?;
        let deadline = owner.handoff.state.lock().map_err(|_| unknown("original terminal cleanup handoff unavailable"))?
            .cleanup_deadline.map_or(deadline, |original| original.min(deadline));
        let (action, original_deadline, nonce) = {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let _gate = tokio::time::timeout(remaining, owner.gate.lock()).await
                .map_err(|_| unknown("original cleanup gate deadline expired"))?;
            if !self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|current|
                current.ticket == owner.ticket && current.binding == owner.binding
                && Arc::ptr_eq(&current.executor, &owner.executor) && Arc::ptr_eq(&current.callback_gate, &owner.gate)
                && Arc::ptr_eq(&current.cancel_outcome, &owner.handoff)) {
                return Err(unknown("original terminal cleanup owner changed before registration"));
            }
            let mut state = owner.handoff.state.lock().map_err(|_| unknown("original terminal cleanup handoff unavailable"))?;
            if state.cleanup_action.is_none()
                && state.terminal_replan_cleanup.as_ref().is_none_or(|cleanup| cleanup.retired) { return Ok(()); }
            if state.cleanup_action.is_none() {
                if std::time::Instant::now() >= deadline {
                    return Err(unknown("original cleanup deadline expired before registration"));
                }
                state.cleanup_deadline = Some(deadline);
                state.cleanup_nonce = Some(Arc::new(()));
                let runner = self.clone();
                let graph = graph_id.to_owned();
                let ticket = ticket.clone();
                let executor = Arc::clone(executor);
                let driver = tokio::spawn(async move {
                    runner.resume_original_terminal_replan_cleanup_inner(&graph, &ticket, &executor)
                        .await.map_err(|error| error.to_string())
                });
                state.cleanup_action = Some(async move {
                    driver.await.map_err(|error| format!("original cleanup driver join failed: {error}"))?
                }.boxed().shared());
            }
            (state.cleanup_action.clone().ok_or_else(|| unknown("original cleanup driver missing"))?,
                state.cleanup_deadline.ok_or_else(|| unknown("original cleanup deadline missing"))?,
                state.cleanup_nonce.clone().ok_or_else(|| unknown("original cleanup identity missing"))?)
        };
        let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
        match tokio::time::timeout(remaining, action).await {
            Ok(Ok(())) => {},
            Ok(Err(reason)) => return Err(unknown(&format!("original cleanup unconfirmed: {reason}"))),
            Err(_) => return Err(unknown("original cleanup remains in flight after deadline")),
        }
        let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
        let _gate = tokio::time::timeout(remaining, owner.gate.lock()).await
            .map_err(|_| unknown("original cleanup gate deadline expired after completion"))?;
        if !self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|current|
            current.ticket == owner.ticket && current.binding == owner.binding
            && Arc::ptr_eq(&current.executor, &owner.executor) && Arc::ptr_eq(&current.callback_gate, &owner.gate)
            && Arc::ptr_eq(&current.cancel_outcome, &owner.handoff)) {
            return Err(unknown("original terminal cleanup owner changed after completion"));
        }
        let state = owner.handoff.state.lock().map_err(|_| unknown("original terminal cleanup handoff unavailable"))?;
        if state.cleanup_nonce.as_ref().is_none_or(|current| !Arc::ptr_eq(current, &nonce))
            || state.cleanup_inflight || state.terminal_replan_cleanup.as_ref().is_some_and(|cleanup| !cleanup.retired) {
            return Err(unknown("original terminal cleanup is not retired"));
        }
        Ok(())
    }

    async fn resume_original_terminal_replan_cleanup_inner(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, executor: &Arc<dyn NodeExecutor>,
    ) -> Result<(), ExecutionRunnerError> {
        let Some((gate, handoff, mut cleanup, _inflight)) = self.original_terminal_cleanup_handoff(graph_id, ticket, executor).await? else { return Ok(()); };
        if cleanup.retired { return Ok(()); }
        if cleanup.receipt.is_none() || cleanup.request.is_none() {
            let commits = self.commit_service.clone();
            let retained = cleanup.retained.clone();
            let candidate = cleanup.candidate.clone();
            let ticket_copy = ticket.clone();
            let winner = tokio::task::spawn_blocking(move || commits.original_terminal_replan_winner(&retained, &ticket_copy, &candidate))
                .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original replan winner read failed: {failure}")))??;
            let Some((request, receipt)) = winner else {
                let commits = self.commit_service.clone();
                let original = cleanup.original.clone();
                let old_winner = tokio::task::spawn_blocking(move || commits.original_terminal_rollback_decision(&original))
                    .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original terminal winner read failed: {failure}")))??;
                if matches!(old_winner, super::commit_service::OriginalTerminalRollbackDecision::Committed(_)) {
                    let binding = self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone()))
                        .filter(|owner| owner.ticket == *ticket && Arc::ptr_eq(&owner.executor, executor)
                            && Arc::ptr_eq(&owner.callback_gate, &gate) && Arc::ptr_eq(&owner.cancel_outcome, &handoff))
                        .map(|owner| owner.binding.clone())
                        .ok_or_else(|| ExecutionRunnerError::Driver("original terminal winner owner changed".into()))?;
                    self.restore_original_terminal_handoff(graph_id, ticket, &binding, executor, &cleanup.original, &cleanup.candidate).await?;
                    return Ok(());
                }
                return Err(ExecutionRunnerError::Driver("original terminal replan outcome unconfirmed".into()));
            };
            cleanup.request = Some(request);
            cleanup.receipt = Some(receipt);
        }
        let request = cleanup.request.clone().ok_or_else(|| ExecutionRunnerError::Driver("original replan request unavailable".into()))?;
        let receipt = cleanup.receipt.clone().ok_or_else(|| ExecutionRunnerError::Driver("original replan receipt unavailable".into()))?;
        let commits = self.commit_service.clone();
        let retained = cleanup.retained.clone();
        let verify_request = Arc::clone(&request);
        let verify_receipt = receipt.clone();
        let graph = graph_id.to_owned();
        tokio::task::spawn_blocking(move || commits.verify_original_terminal_replan(&retained, &verify_receipt, &verify_request, &graph))
            .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original replan proof failed: {failure}")))??;
        {
            let _guard = gate.lock().await;
            let active = self.active.lock().await;
            if !active.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|owner|
                owner.ticket == *ticket && Arc::ptr_eq(&owner.executor, executor)
                    && Arc::ptr_eq(&owner.callback_gate, &gate) && Arc::ptr_eq(&owner.cancel_outcome, &handoff)
                    && !owner.callback_completed) {
                return Err(ExecutionRunnerError::Driver("original terminal cleanup owner changed".into()));
            }
            let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
            if state.outcome.as_ref().and_then(|value| value.as_ref().ok()) != Some(&cleanup.candidate) {
                return Err(ExecutionRunnerError::Driver("original terminal cleanup outcome changed".into()));
            }
            let current = state.terminal_replan_cleanup.as_mut().ok_or_else(|| ExecutionRunnerError::Driver("original terminal cleanup missing".into()))?;
            if current.original != cleanup.original || current.candidate != cleanup.candidate || current.handle != cleanup.handle {
                return Err(ExecutionRunnerError::Driver("original terminal cleanup identity changed".into()));
            }
            current.request = Some(Arc::clone(&request));
            current.receipt = Some(receipt.clone());
            cleanup.transferred = current.transferred;
            cleanup.retired = current.retired;
        }
        if cleanup.retired { return Ok(()); }
        if !cleanup.transferred {
            cleanup.handle.0.committed(ticket, &cleanup.original).await.map_err(ExecutionRunnerError::Driver)?;
            let _guard = gate.lock().await;
            let active = self.active.lock().await;
            if !active.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|owner|
                owner.ticket == *ticket && Arc::ptr_eq(&owner.executor, executor)
                    && Arc::ptr_eq(&owner.callback_gate, &gate) && Arc::ptr_eq(&owner.cancel_outcome, &handoff)
                    && !owner.callback_completed) {
                return Err(ExecutionRunnerError::Driver("original terminal transfer owner changed".into()));
            }
            let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
            let current = state.terminal_replan_cleanup.as_mut().filter(|current| current.original == cleanup.original
                && current.candidate == cleanup.candidate && current.handle == cleanup.handle)
                .ok_or_else(|| ExecutionRunnerError::Driver("original terminal transfer identity changed".into()))?;
            current.transferred = true;
        }
        let commits = self.commit_service.clone();
        let retained = cleanup.retained.clone();
        let graph = graph_id.to_owned();
        tokio::task::spawn_blocking(move || commits.retire_original_terminal_after_replan(&retained, &receipt, &request, &graph))
            .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original terminal retirement failed: {failure}")))??;
        let _guard = gate.lock().await;
        let active = self.active.lock().await;
        if !active.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|owner|
            owner.ticket == *ticket && Arc::ptr_eq(&owner.executor, executor)
                && Arc::ptr_eq(&owner.callback_gate, &gate) && Arc::ptr_eq(&owner.cancel_outcome, &handoff)
                && !owner.callback_completed) {
            return Err(ExecutionRunnerError::Driver("original terminal retirement owner changed".into()));
        }
        let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
        let current = state.terminal_replan_cleanup.as_mut().filter(|current| current.original == cleanup.original
            && current.candidate == cleanup.candidate && current.handle == cleanup.handle && current.transferred)
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal retirement identity changed".into()))?;
        current.retired = true;
        Ok(())
    }

    async fn restore_original_terminal_handoff(
        &self, graph_id: &str, ticket: &NodeExecutionTicket, binding: &ExecutionNodeBinding,
        executor: &Arc<dyn NodeExecutor>, original: &NodeExecutionOutcome, candidate: &NodeExecutionOutcome,
    ) -> Result<(), ExecutionRunnerError> {
        let gate = self.binding_callback_gate(graph_id, ticket).await
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal callback gate unavailable".into()))?;
        let _guard = gate.lock().await;
        let handoff = self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket && active.binding == *binding
                && Arc::ptr_eq(&active.executor, executor) && Arc::ptr_eq(&active.callback_gate, &gate)
                && !active.callback_completed)
            .map(|active| Arc::clone(&active.cancel_outcome))
            .ok_or_else(|| ExecutionRunnerError::Driver("original terminal winner owner changed".into()))?;
        let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
        if state.outcome.as_ref().and_then(|value| value.as_ref().ok()) != Some(candidate) || !state.superseded_outcomes.contains(original) {
            return Err(ExecutionRunnerError::Driver("original terminal winner handoff differs".into()));
        }
        state.outcome = Some(Ok(original.clone()));
        state.terminal_replan_cleanup = None;
        Ok(())
    }

    async fn cancel_physical_until(
        executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), ExecutionRunnerError> {
        let budget = deadline.map_or(PHYSICAL_CLOSURE_BUDGET,
            |deadline| deadline.saturating_duration_since(std::time::Instant::now()));
        if executor.cancellation_requires_quiescence(ticket) {
            executor.cancel_bounded(ticket, budget).await?;
        } else {
            tokio::time::timeout(budget, executor.cancel_bounded(ticket, budget)).await
                .map_err(|_| NodeExecutorError::Cancel { node_id: ticket.node_id.clone(),
                    reason: "original cancellation budget expired; drain unconfirmed".into() })??;
        }
        if executor.cancellation_requires_quiescence(ticket) {
            match executor.cancel(ticket).now_or_never() {
                Some(Ok(())) => {},
                Some(Err(error)) => return Err(error.into()),
                None => return Err(NodeExecutorError::Cancel { node_id: ticket.node_id.clone(),
                    reason: "bounded cancellation returned without physical quiescence".into() }.into()),
            }
        }
        Ok(())
    }

    async fn drive_original_drain(
        &self, owner: PumpCancellationOwner, deadline: std::time::Instant,
    ) -> Result<OriginalDrainOutcome, String> {
        {
            let _gate = owner.gate.lock().await;
            if !self.active.lock().await.get(&(owner.ticket.graph_id.clone(), owner.ticket.node_id.clone()))
                .is_some_and(|active| active.ticket == owner.ticket && active.binding == owner.binding
                    && Arc::ptr_eq(&active.executor, &owner.executor) && Arc::ptr_eq(&active.callback_gate, &owner.gate)
                    && Arc::ptr_eq(&active.cancel_outcome, &owner.handoff)) {
                return Err("original drain owner changed before physical stop".into());
            }
            let graph = self.state_store.load_async(&owner.ticket.graph_id).await.map_err(|error| error.to_string())?;
            if self.commit_service.binding_callback_disposition(&graph, &owner.ticket.node_id, &owner.binding)
                .map_err(|error| error.to_string())? == CallbackDisposition::Commit {
                return Ok(OriginalDrainOutcome::AlreadyCommitted);
            }
        }
        Self::cancel_physical_until(&owner.executor, &owner.ticket, Some(deadline)).await
            .map_err(|error| error.to_string())?;
        Ok(OriginalDrainOutcome::Drained)
    }

    pub(crate) async fn capture_pump_cancellation_owners(
        &self, graph_id: &str, in_flight: &BTreeSet<String>,
    ) -> (Vec<PumpCancellationOwner>, bool) {
        let owners: Vec<_> = self.active.lock().await.iter()
            .filter(|((graph, node), _)| graph == graph_id && in_flight.contains(node))
            .map(|(_, active)| PumpCancellationOwner {
                binding: active.binding.clone(), ticket: active.ticket.clone(),
                executor: Arc::clone(&active.executor), gate: Arc::clone(&active.callback_gate),
                handoff: Arc::clone(&active.cancel_outcome),
            }).collect();
        let missing = owners.len() != in_flight.len();
        (owners, missing)
    }

    pub(crate) async fn drain_pump_cancellation_owners(
        &self, graph_id: &str, owners: Vec<PumpCancellationOwner>,
    ) -> Result<(), ExecutionRunnerError> {
        let results = futures::future::join_all(owners.into_iter().map(|owner| async move {
            let unknown = |reason: &str| ExecutionRunnerError::Executor(
                NodeExecutorError::OriginalCustodyUnknown {
                    node_id: owner.ticket.node_id.clone(), reason: reason.into(),
                });
            let original_closed = || owner.handoff.state.lock().ok()
                .is_some_and(|state| state.original_binding_closed);
            let proposed = std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
            let proposed = self.shutdown_deadline.get().map_or(proposed, |shutdown| proposed.min(*shutdown));
            let deadline = owner.handoff.closing_deadline(proposed)
                .map_err(|_| unknown("original closing deadline unavailable"))?;
            let original_operation_started = owner.handoff.state.lock().ok()
                .is_some_and(|state| state.drain.is_some() || state.callback_action.is_some());
            let cleanup = if original_operation_started { Ok(()) } else {
                self.resume_original_terminal_replan_cleanup_until(graph_id, &owner.ticket, &owner.executor, deadline).await
            };
            let result = async {
                let (drain, cleanup_error, original_deadline) = {
                    let (_coordination, _guard) = self.coordinate_original_gate(
                        graph_id, &owner.ticket.node_id, &owner.gate, deadline).await?;
                    let matches = self.active.lock().await
                        .get(&(graph_id.to_string(), owner.ticket.node_id.clone()))
                        .is_some_and(|active| active.binding == owner.binding && active.ticket == owner.ticket
                            && Arc::ptr_eq(&active.executor, &owner.executor)
                            && Arc::ptr_eq(&active.callback_gate, &owner.gate)
                            && Arc::ptr_eq(&active.cancel_outcome, &owner.handoff));
                    if !matches {
                        return if original_closed() { Ok(()) } else {
                            Err(unknown("original cancellation owner changed before pump drain"))
                        };
                    }
                    let graph = self.state_store.load_async(graph_id).await
                        .map_err(|_| unknown("original graph unavailable before pump drain"))?;
                    let disposition = self.commit_service.binding_callback_disposition(&graph, &owner.ticket.node_id, &owner.binding)
                        .map_err(|_| unknown("original callback decision unavailable before pump drain"))?;
                    let existing = owner.handoff.state.lock().map_err(|_| unknown("original drain handoff unavailable"))?.drain.clone();
                    let cleanup_error = cleanup.err().map(|error| error.to_string());
                    let action_started = owner.handoff.state.lock().map_err(|_| unknown("original callback handoff unavailable"))?.callback_action.is_some();
                    if action_started && disposition == CallbackDisposition::Pending {
                        return Err(unknown("original callback winner became unconfirmed"));
                    }
                    if (disposition == CallbackDisposition::Commit && cleanup_error.is_none() && existing.is_none())
                        || (action_started && existing.is_none() && cleanup_error.is_none()) {
                        // The original callback is registered or rejoined after
                        // the gate is released by the captured-owner take path.
                        (None, None, deadline)
                    } else {
                        // Register exactly one drain while the original gate is
                        // held; the retained driver lives in this same handoff.
                        let mut state = owner.handoff.state.lock().map_err(|_| unknown("original drain handoff unavailable"))?;
                        if state.cleanup_inflight || state.cleanup_action.as_ref().is_some_and(|action| !matches!(action.peek(), Some(Ok(())))) {
                            return Err(unknown("original terminal cleanup remains in flight"));
                        }
                        if state.drain.is_none() {
                            if std::time::Instant::now() >= deadline {
                                return Err(unknown("original drain deadline expired before registration"));
                            }
                            state.drain_deadline = Some(deadline);
                        }
                        let original_deadline = state.drain_deadline.ok_or_else(|| unknown("original drain deadline unavailable"))?;
                        let drain = state.drain.get_or_insert_with(|| {
                            let captured = owner.clone();
                            let runner = self.clone();
                            let driver = tokio::spawn(async move {
                                runner.drive_original_drain(captured, original_deadline).await
                            });
                            async move { driver.await.map_err(|error| format!("original physical driver join failed: {error}"))? }
                                .boxed().shared()
                        }).clone();
                        (Some(drain), cleanup_error, original_deadline)
                    }
                };
                if let Some(drain) = drain {
                    let budget = original_deadline.saturating_duration_since(std::time::Instant::now());
                    match tokio::time::timeout(budget, drain).await {
                        Ok(Ok(OriginalDrainOutcome::Drained | OriginalDrainOutcome::AlreadyCommitted)) => {},
                        Ok(Err(error)) => return Err(unknown(&format!("original physical drain unconfirmed: {error}"))),
                        Err(_) => return Err(unknown("original physical drain remains in flight after deadline")),
                    }
                }
                if let Some(error) = cleanup_error {
                    return Err(unknown(&format!("original terminal cleanup unconfirmed: {error}")));
                }
                // A joined driver is not a durable result. Re-read outside the
                // original gate; the final take re-enters and rechecks it.
                if original_closed() { return Ok(()); }
                let graph = self.state_store.load_async(graph_id).await
                    .map_err(|_| unknown("original graph unavailable after pump drain"))?;
                let mut disposition = self.commit_service.binding_callback_disposition(&graph, &owner.ticket.node_id, &owner.binding)
                    .map_err(|_| unknown("original callback decision unavailable after pump drain"))?;
                if disposition == CallbackDisposition::Pending && !original_closed() {
                    let task = owner.handoff.state.lock().map_err(|_| unknown("original task handoff unavailable"))?.pump_task.clone();
                    if let Some(task) = task {
                        let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
                        match tokio::time::timeout(remaining, task.observe(&owner.ticket.node_id)).await {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => return Err(unknown(&format!("original task remains unconfirmed: {error}"))),
                            Err(_) => return Err(unknown("original task remains in flight after deadline")),
                        }
                        if original_closed() { return Ok(()); }
                        let graph = self.state_store.load_async(graph_id).await
                            .map_err(|_| unknown("original graph unavailable after task completion"))?;
                        disposition = self.commit_service.binding_callback_disposition(&graph, &owner.ticket.node_id, &owner.binding)
                            .map_err(|_| unknown("original callback decision unavailable after task completion"))?;
                    }
                }
                match disposition {
                    CallbackDisposition::Pending => return if original_closed() { Ok(()) } else {
                        Err(unknown("original callback outcome or retirement remains unconfirmed"))
                    },
                    CallbackDisposition::Commit | CallbackDisposition::Abort => {},
                }
                let taken = self.take_bound_active_node_captured(graph_id, &owner.ticket.node_id,
                    Some(&owner.binding), Some(&owner), Some(original_deadline)).await
                    .map_err(|error| unknown(&format!("original callback or lease release unconfirmed: {error}")))?;
                if taken.is_some() || original_closed() { Ok(()) } else {
                    Err(unknown("original cancellation owner changed before lease release"))
                }
            }.await;
            result
        })).await;
        match results.into_iter().find_map(Result::err) { Some(error) => Err(error), None => Ok(()) }
    }

    // Returns false only when the exact original outcome is already Commit;
    // the caller must use the original callback instead of cancelling it.
    async fn drain_active_original(
        &self, graph_id: &str, executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
        deadline: std::time::Instant,
    ) -> Result<bool, ExecutionRunnerError> {
        let unknown = |reason: &str| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: reason.into(),
        });
        let original = self.active.lock().await.get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket && Arc::ptr_eq(&active.executor, executor))
            .map(|active| PumpCancellationOwner { binding:active.binding.clone(), ticket:active.ticket.clone(),
                executor:Arc::clone(&active.executor), gate:Arc::clone(&active.callback_gate),
                handoff:Arc::clone(&active.cancel_outcome) }).ok_or_else(|| unknown("original drain owner missing"))?;
        let deadline = original.handoff.closing_deadline(deadline)
            .map_err(|_| unknown("original closing deadline unavailable"))?;
        let (drain, original_deadline) = {
            let (_coordination, _gate) = self.coordinate_original_gate(
                graph_id, &ticket.node_id, &original.gate, deadline).await?;
            if !self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|active|
                active.binding == original.binding && active.ticket == original.ticket
                && Arc::ptr_eq(&active.executor, &original.executor) && Arc::ptr_eq(&active.callback_gate, &original.gate)
                && Arc::ptr_eq(&active.cancel_outcome, &original.handoff)) {
                return Err(unknown("original drain owner changed"));
            }
            let graph = self.state_store.load_async(graph_id).await
                .map_err(|_| unknown("original graph unavailable before cancellation"))?;
            if self.commit_service.binding_callback_disposition(&graph, &ticket.node_id, &original.binding)
                .map_err(|_| unknown("original callback decision unavailable before cancellation"))? == CallbackDisposition::Commit {
                return Ok(false);
            }
            let mut state = original.handoff.state.lock().map_err(|_| unknown("original drain handoff unavailable"))?;
            if state.cleanup_inflight || state.cleanup_action.as_ref().is_some_and(|action| !matches!(action.peek(), Some(Ok(()))))
                || state.proof_inflight || state.callback_action.is_some() {
                return Err(unknown("original callback or terminal cleanup already owns this ticket"));
            }
            if state.drain.is_none() {
                if std::time::Instant::now() >= deadline {
                    return Err(unknown("original drain deadline expired before registration"));
                }
                state.drain_deadline = Some(deadline);
            }
            let original_deadline = state.drain_deadline.ok_or_else(|| unknown("original drain deadline unavailable"))?;
            let drain = state.drain.get_or_insert_with(|| {
                let captured = original.clone();
                let runner = self.clone();
                let driver = tokio::spawn(async move {
                    runner.drive_original_drain(captured, original_deadline).await
                });
                async move { driver.await.map_err(|error| format!("original physical driver join failed: {error}"))? }.boxed().shared()
            }).clone();
            (drain, original_deadline)
        };
        let budget = original_deadline.saturating_duration_since(std::time::Instant::now());
        match tokio::time::timeout(budget, drain).await {
            Ok(Ok(OriginalDrainOutcome::Drained)) => {},
            Ok(Ok(OriginalDrainOutcome::AlreadyCommitted)) => return Ok(false),
            Ok(Err(error)) => return Err(unknown(&format!("original physical drain unconfirmed: {error}"))),
            Err(_) => return Err(unknown("original physical drain remains in flight after deadline")),
        }
        let _gate = original.gate.lock().await;
        let mut active = self.active.lock().await;
        let owner = active.get_mut(&(graph_id.to_owned(), ticket.node_id.clone())).filter(|active|
            active.binding == original.binding && active.ticket == original.ticket
            && Arc::ptr_eq(&active.executor, &original.executor) && Arc::ptr_eq(&active.callback_gate, &original.gate)
            && Arc::ptr_eq(&active.cancel_outcome, &original.handoff)).ok_or_else(|| unknown("original drain owner changed after completion"))?;
        owner.drain_completed = true;
        Ok(true)
    }

    async fn active_cancel_observed(
        &self, graph_id: &str, executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
    ) -> Result<(), ExecutionRunnerError> {
        let deadline = std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
        let cleanup = self.resume_original_terminal_replan_cleanup_until(graph_id, ticket, executor, deadline).await;
        let drained = self.drain_active_original(graph_id, executor, ticket, deadline).await?;
        if let Err(error) = cleanup { return Err(error); }
        if drained { Ok(()) } else {
            self.complete_callback_observed(graph_id, executor, ticket, None).await
        }
    }

    async fn complete_callback_locked_until(
        &self, graph_id: &str, executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
        requested_abort: Option<&str>, deadline: Option<std::time::Instant>, expected: CallbackDisposition,
    ) -> Result<(), ExecutionRunnerError> {
        let handoff = self.active.lock().await.get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket && Arc::ptr_eq(&active.executor, executor))
            .map(|active| Arc::clone(&active.cancel_outcome));
        if let Some(handoff) = handoff {
            let state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
            if state.cleanup_inflight || state.drain.as_ref().is_some_and(|drain| !matches!(drain.peek(), Some(Ok(_)))) {
                return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(), reason: "original physical drain or terminal cleanup remains unresolved".into(),
                }));
            }
            if state.terminal_replan_cleanup.as_ref().is_some_and(|cleanup| !cleanup.retired) {
                return Err(ExecutionRunnerError::Driver("original terminal cleanup unconfirmed".into()));
            }
        }
        let state = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket)
            .map(|active| {
                (
                    active.binding.clone(),
                    active.callback_completed,
                    active.no_dispatch.clone(),
                )
            });
        let Some((binding, completed, observation)) = state else {
            return Ok(());
        };
        if completed {
            return Ok(());
        }
        let graph = self.state_store.load_async(graph_id).await?;
        let disposition =
            self.commit_service
                .binding_callback_disposition(&graph, &ticket.node_id, &binding)?;
        if disposition != expected {
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id:ticket.node_id.clone(), reason:"original callback winner changed before physical proof".into(),
            }));
        }
        #[cfg(feature = "test-fixtures")]
        {
            let state_name = match disposition {
                CallbackDisposition::Commit => "committed_outcome",
                CallbackDisposition::Abort => "canonical_retirement",
                CallbackDisposition::Pending => "pending_no_outcome_or_retirement",
            };
            let snapshot = self.active.lock().await;
            let live = snapshot.get(&(graph_id.to_string(), ticket.node_id.clone()))
                .filter(|active| active.ticket == *ticket);
            let detail = match live {
                Some(active) => {
                    let driver = active.cancel_outcome.state.lock().ok().map(|state|
                        (state.polled, state.finished, state.outcome.is_some()));
                    format!("disposition={state_name} requested_abort={requested_abort:?} node_status={:?} graph_attempt={:?} callback_completed={} release_blocked={} drain_completed={} driver_polled_finished_has_outcome={driver:?}",
                        graph.node_statuses.get(&ticket.node_id), graph.recovery_cursor.node_attempts.get(&ticket.node_id),
                        active.callback_completed, active.release_blocked, active.drain_completed)
                },
                None => format!("disposition={state_name} requested_abort={requested_abort:?} active=unknown"),
            };
            build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt), "callback_disposition", &detail);
        }
        if disposition == CallbackDisposition::Pending {
            tracing::warn!(graph_id = %graph_id, node_id = %ticket.node_id,
                attempt = ticket.attempt, executor_kind = %ticket.executor_kind,
                requested_abort = ?requested_abort, phase = "callback_pending",
                "original callback has no committed outcome or retirement");
            if requested_abort.is_some() {
                return Ok(());
            }
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(),
                reason: "callback has no canonical original outcome or retirement".into(),
            }));
        };
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=callback_enter node={} executor={} abort={}", ticket.node_id, ticket.executor_kind, disposition == CallbackDisposition::Abort);
        }
        // The original action was already joined by the same handoff; this
        // stage only re-reads physical proof and never invokes it again.
        let result: Result<(), NodeExecutorError> = Ok(());
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=callback_return node={} executor={} ok={}", ticket.node_id, ticket.executor_kind, result.is_ok());
        }
        let result = match result {
            Ok(()) => {
                let mut changes = self.state_store.subscribe_commits();
                #[cfg(test)]
                if std::env::var_os("COWD_R622_TRACE").is_some() {
                    eprintln!("r622_toolbatch phase=physical_wait_enter node={} executor={}", ticket.node_id, ticket.executor_kind);
                }
                let budget = deadline.map_or(PHYSICAL_CLOSURE_BUDGET,
                    |deadline| deadline.saturating_duration_since(std::time::Instant::now()));
                let observed = tokio::time::timeout(budget, async {
                    loop {
                        let current = self.state_store.load_async(graph_id).await?;
                        match self
                            .commit_service
                            .binding_physical_sources_with_original_reads(
                                &current,
                                &ticket.node_id,
                                &binding,
                                observation
                                    .as_ref()
                                    .and_then(|observation| observation.witness())
                                    .as_ref(),
                                &observation.as_ref().map(|owner| owner.original_early_reads()).transpose()
                                    .map_err(ExecutionRunnerError::Driver)?.unwrap_or_default(),
                            )? {
                            super::commit_service::PhysicalClosureProof::Proven(_) => {
                                return Ok::<(), ExecutionRunnerError>(());
                            }
                            super::commit_service::PhysicalClosureProof::Unprovable { ring, reason } => {
                                // A ring that can never hold must not spend the
                                // whole physical-closure budget: name it and
                                // fail closed now.
                                return Err(ExecutionRunnerError::Driver(format!(
                                    "physical closure unprovable: {ring}: {reason}"
                                )));
                            }
                            // A source that moved under the proof is the
                            // same wait here: this observation loop already
                            // re-proves on the next commit inside the
                            // physical-closure budget.
                            // Every wait is the same wait here: this
                            // observation loop already re-proves on the next
                            // commit inside the physical-closure budget. The
                            // asymmetry with the recovery is deliberate and
                            // is registered as R-1 in the patch notes.
                            super::commit_service::PhysicalClosureProof::Pending(_) => {}
                        }
                        changes.changed().await.map_err(|_| {
                            ExecutionRunnerError::Driver(
                                "physical source stream closed before quiescence".into(),
                            )
                        })?;
                    }
                })
                .await
                .map_err(|_| {
                    #[cfg(feature = "test-fixtures")]
                    build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt),
                        "physical_sources", "result=timeout");
                    // A physical closure that cannot be proven must name its own
                    // missing proof in production, not only inside the test
                    // harness: an unproven closure otherwise surfaces as one
                    // opaque terminal and every operator has to reconstruct the
                    // cause from the event store after the fact.
                    let diagnostic = self.commit_service.physical_closure_diagnostic(
                        &graph,
                        &ticket.node_id,
                        &binding,
                    );
                    #[cfg(test)]
                    eprintln!("OWN834_PHYSICAL_CLOSURE_UNPROVED {diagnostic}");
                    tracing::warn!(
                        OWN834_PHYSICAL_CLOSURE_UNPROVED = %diagnostic,
                        graph_id = %graph_id,
                        node_id = %ticket.node_id,
                        attempt = binding.attempt,
                        budget_seconds = PHYSICAL_CLOSURE_BUDGET.as_secs(),
                        "physical closure unconfirmed within original cancellation budget"
                    );
                    ExecutionRunnerError::Driver(
                        "physical closure unconfirmed within original cancellation budget".into(),
                    )
                })?;
                #[cfg(test)]
                if std::env::var_os("COWD_R622_TRACE").is_some() {
                    eprintln!("r622_toolbatch phase=physical_wait_return node={} executor={} ok={}", ticket.node_id, ticket.executor_kind, observed.is_ok());
                }
                observed
            }
            Err(error) => Err(error.into()),
        };
        #[cfg(feature = "test-fixtures")]
        build16_callback_trace(graph_id, &ticket.node_id, Some(ticket.attempt), "callback_and_physical_result",
            &match &result { Ok(()) => "result=ok physical_sources=confirmed".into(),
                Err(error) => format!("result=error physical_sources=unconfirmed kind={} original_detail=private", build16_runner_error_kind(error)) });
        let result = if result.is_ok() && disposition == CallbackDisposition::Commit && ticket.executor_kind == "synthesize" {
            let original_handoff = {
                let active = self.active.lock().await;
                active.get(&(graph_id.to_string(), ticket.node_id.clone()))
                    .filter(|owner| owner.ticket == *ticket && owner.binding == binding)
                    .and_then(|owner| owner.cancel_outcome.state.lock().ok().and_then(|handoff| {
                        if !handoff.finished { return None; }
                        handoff.outcome.clone().map(|outcome| (outcome, Arc::clone(&owner.cancel_outcome)))
                    }))
            };
            match original_handoff {
                Some((Ok(outcome), handoff)) => {
                    match outcome.original_terminal_owner.as_ref() {
                        Some(owner) if requests_original_session_terminal(&outcome) => {
                            let action = {
                                let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Executor(
                                    NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(),
                                        reason: "original terminal release handoff unavailable".into() }))?;
                                if !state.finished || state.outcome.as_ref().and_then(|value| value.as_ref().ok()) != Some(&outcome) {
                                    return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                                        node_id: ticket.node_id.clone(), reason: "original terminal release outcome changed".into(),
                                    }));
                                }
                                if state.terminal_release_action.is_none() {
                                    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                                        return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                                            node_id: ticket.node_id.clone(), reason: "original terminal release deadline expired".into(),
                                        }));
                                    }
                                    let owner = Arc::clone(&owner.0);
                                    let ticket = ticket.clone();
                                    let driver = tokio::task::spawn_blocking(move || owner.release_after_original_physical_close(&ticket));
                                    state.terminal_release_action = Some(async move {
                                        driver.await.map_err(|error| format!("original terminal release task failed: {error}"))?
                                    }.boxed().shared());
                                }
                                state.terminal_release_action.clone().ok_or_else(|| ExecutionRunnerError::Driver("original terminal release driver missing".into()))?
                            };
                            let remaining = deadline.map_or(PHYSICAL_CLOSURE_BUDGET,
                                |deadline| deadline.saturating_duration_since(std::time::Instant::now()));
                            match tokio::time::timeout(remaining, action).await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(error)) => Err(ExecutionRunnerError::Driver(error)),
                                Err(_) => Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                                    node_id: ticket.node_id.clone(), reason: "original terminal release remains in flight".into(),
                                })),
                            }
                        }
                        Some(_) => result,
                        // The original Session terminal was requested but no
                        // live owner carries it: Unknown, and the custody of
                        // the original terminal must be retained.
                        None => match missing_original_terminal_custody(&outcome) {
                            Some(error) => Err(error),
                            // A delegated leaf publishes its own terminal and
                            // never owns the original Session terminal. It has
                            // no original terminal custody to retain.
                            None => result,
                        },
                    }
                }
                Some((Err(error), _)) => Err(original_shared_error(&error)),
                None => {
                    // The live handoff is gone. Without it the original
                    // terminal custody cannot be read, so the original
                    // fail-closed check still applies.
                    let document = self.commit_service.committed_host_transcript(ticket)?;
                    if document.terminal_completion.is_some() {
                        Err(ExecutionRunnerError::Driver("original terminal handoff is unconfirmed".into()))
                    } else { result }
                }
            }
        } else { result };
        result
    }

    /// Supply the missing canonical terminal fact for an override node at the
    /// exact criterion that would otherwise report unknown custody.
    ///
    /// Two durable facts gate the seal, and neither is a fence relaxation: the
    /// node id must belong to the known override family, and the committed Host
    /// transcript must already carry this turn's terminal completion. Only then
    /// can the node never publish its own committed attempt, which is precisely
    /// the precondition for writing its retirement with the existing
    /// `cancel_node` command. Anything else returns `None` and the original
    /// fail-closed report stands.
    async fn seal_pending_override_custody(
        &self,
        graph_id: &str,
        ticket: &NodeExecutionTicket,
        graph: &harness_contract::execution_graph::ExecutionGraph,
    ) -> Option<OverrideCustody> {
        let terminalized = self
            .commit_service
            .committed_host_transcript(ticket)
            .ok()
            .and_then(|document| document.terminal_completion)
            .is_some();
        if !terminalized {
            // 034-5(a) supplementary trigger surface: the silent type. A turn
            // that halts before any terminal exists is invisible to a streak
            // over custody receipts, because an interrupted reconciliation
            // never reaches three. Silence is judged only on durable facts and
            // only after the whole bound, so a legitimately slow turn keeps its
            // terminal presentation untouched.
            let released = release_silent_override_custody(
                &self.state_store,
                &self.commit_service,
                self.commit_service.event_store(),
                graph_id,
                unix_now_ms(),
                OVERRIDE_CUSTODY_SILENCE_RELEASE_MS,
            );
            return released
                .into_iter()
                .find(|(released_id, _)| *released_id == ticket.node_id)
                .map(|(_, custody)| custody);
        }
        seal_override_custody_for_graph(
            &self.state_store,
            &self.commit_service,
            graph,
            &ticket.node_id,
            "callback-pending-override-seal",
        )
    }

    async fn complete_callback_observed(
        &self, graph_id: &str, executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
        requested_abort: Option<&str>,
    ) -> Result<(), ExecutionRunnerError> {
        self.complete_callback_observed_until(graph_id, executor, ticket, requested_abort,
            std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET).await
    }

    async fn complete_callback_observed_until(
        &self, graph_id: &str, executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
        requested_abort: Option<&str>, deadline: std::time::Instant,
    ) -> Result<(), ExecutionRunnerError> {
        let unknown = |reason: &str| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: reason.into(),
        });
        let original = self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone()))
            .filter(|active| active.ticket == *ticket && Arc::ptr_eq(&active.executor, executor))
            .map(|active| PumpCancellationOwner { binding:active.binding.clone(), ticket:active.ticket.clone(),
                executor:Arc::clone(&active.executor), gate:Arc::clone(&active.callback_gate),
                handoff:Arc::clone(&active.cancel_outcome) });
        let Some(original) = original else { return Err(unknown("original callback owner missing")); };
        let deadline = original.handoff.closing_deadline(deadline)
            .map_err(|_| unknown("original closing deadline unavailable"))?;
        let matches_original = |active: &ActiveNode| active.ticket == original.ticket && active.binding == original.binding
            && Arc::ptr_eq(&active.executor, &original.executor) && Arc::ptr_eq(&active.callback_gate, &original.gate)
            && Arc::ptr_eq(&active.cancel_outcome, &original.handoff);
        let has_action = original.handoff.state.lock().map_err(|_| unknown("original callback handoff unavailable"))?.callback_action.is_some();
        if !has_action { self.resume_original_terminal_replan_cleanup_until(graph_id, ticket, executor, deadline).await?; }
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let graph = tokio::time::timeout(remaining, self.state_store.load_async(graph_id)).await
                .map_err(|_| unknown("original callback graph deadline expired"))?
                .map_err(|_| unknown("original callback graph unavailable"))?;
            let decision = self.commit_service.binding_callback_disposition(&graph, &ticket.node_id, &original.binding)
                .map_err(|_| unknown("original callback decision unavailable"))?;
            if decision == CallbackDisposition::Pending {
                // Read every handoff fact in one scope, so no owner lock is
                // held across the awaits below.
                let (action_started, task, published) = {
                    let state = original.handoff.state.lock()
                        .map_err(|_| unknown("original callback handoff unavailable"))?;
                    (state.callback_action.is_some(), state.pump_task.clone(),
                        state.finished || state.outcome.is_some())
                };
                if action_started { return Err(unknown("original callback winner changed")); }
                if requested_abort.is_some() { return Ok(()); }
                // The exact original owner must reach a canonical terminal fact
                // before this criterion judges unknown. The owner-scoped
                // terminal cleanup commits the real retirement receipt of this
                // exact original binding when one is prepared, and the physical
                // drain releases the original effect so its own canonical
                // receipt becomes reachable. Both keep the identity fence: the
                // same binding, ticket, executor and handoff must still own
                // this node, and an unconfirmed cleanup or drain stays unknown
                // custody.
                let cleanup = self.resume_original_terminal_replan_cleanup_until(graph_id, ticket, executor, deadline).await;
                let drained = match self.drain_active_original(graph_id, executor, ticket, deadline).await {
                    Ok(drained) => drained,
                    Err(error) => return Err(unknown(&format!("callback has no canonical original outcome or retirement: {error}"))),
                };
                if let Err(error) = cleanup {
                    return Err(unknown(&format!("callback has no canonical original outcome or retirement: {error}")));
                }
                if drained {
                    // The original was physically released, so its own task may
                    // now publish the canonical receipt. The original task
                    // publishes its outcome before it runs its callback, so an
                    // unpublished original is the in-flight task and the only
                    // one this callback may wait for: the original task must
                    // never wait for its own receipt.
                    if let (Some(task), false) = (task, published) {
                        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                        match tokio::time::timeout(remaining, task.observe(&ticket.node_id)).await {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => return Err(unknown(&format!("callback has no canonical original outcome or retirement: {error}"))),
                            Err(_) => return Err(unknown("callback has no canonical original outcome or retirement: original task remains in flight after deadline")),
                        }
                    }
                }
                // Re-read the canonical fact for the same binding. Only a real
                // committed outcome or a real retirement receipt leaves this
                // criterion; Pending is still unknown custody.
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let graph = tokio::time::timeout(remaining, self.state_store.load_async(graph_id)).await
                    .map_err(|_| unknown("callback has no canonical original outcome or retirement: original callback graph deadline expired"))?
                    .map_err(|_| unknown("callback has no canonical original outcome or retirement: original callback graph unavailable"))?;
                let mut settled = self.commit_service.binding_callback_disposition(&graph, &ticket.node_id, &original.binding)
                    .map_err(|_| unknown("callback has no canonical original outcome or retirement: original callback decision unavailable"))?;
                if settled == CallbackDisposition::Pending {
                    // An override node planned with a terminal intervention is
                    // short-circuited by the override itself, so it can be
                    // started and admitted and still never publish a committed
                    // attempt. Once the turn is durably terminalized that fact
                    // is unreachable, and the existing `cancel_node` retirement
                    // is the canonical receipt it still owes. Pending after the
                    // seal is still unknown custody: nothing is reinterpreted.
                    let custody = self.seal_pending_override_custody(graph_id, ticket, &graph).await;
                    if let Some(custody) = custody {
                        tracing::warn!(
                            target: "cowd::override_custody",
                            graph_id,
                            node_id = %ticket.node_id,
                            outcome = ?custody,
                            "the pending callback criterion ran the override custody seal"
                        );
                        if custody.sealed() {
                            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                            let sealed = tokio::time::timeout(remaining, self.state_store.load_async(graph_id))
                                .await
                                .map_err(|_| unknown("callback has no canonical original outcome or retirement: original callback graph deadline expired"))?
                                .map_err(|_| unknown("callback has no canonical original outcome or retirement: original callback graph unavailable"))?;
                            settled = self.commit_service.binding_callback_disposition(&sealed, &ticket.node_id, &original.binding)
                                .map_err(|_| unknown("callback has no canonical original outcome or retirement: original callback decision unavailable"))?;
                        }
                    }
                }
                if settled == CallbackDisposition::Pending {
                    return Err(unknown("callback has no canonical original outcome or retirement"));
                }
                continue;
            }
            if decision == CallbackDisposition::Abort {
                let action_started = original.handoff.state.lock().map_err(|_| unknown("original callback handoff unavailable"))?.callback_action.is_some();
                if !action_started {
                    let drained = self.drain_active_original(graph_id, executor, ticket, deadline).await?;
                    if !drained { continue; }
                }
            }
            let (action, nonce, original_deadline) = {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let _gate = tokio::time::timeout(remaining, original.gate.lock()).await
                    .map_err(|_| unknown("original callback gate deadline expired"))?;
                let current_graph = self.state_store.load_async(graph_id).await
                    .map_err(|_| unknown("original callback graph unavailable at action registration"))?;
                if self.commit_service.binding_callback_disposition(&current_graph, &ticket.node_id, &original.binding)
                    .map_err(|_| unknown("original callback winner unavailable at action registration"))? != decision {
                    return Err(unknown("original callback winner changed before action"));
                }
                let active = self.active.lock().await;
                let current = active.get(&(graph_id.to_owned(), ticket.node_id.clone()))
                    .filter(|active| matches_original(active)).ok_or_else(|| unknown("original callback owner changed before action"))?;
                if current.callback_completed { return Ok(()); }
                let mut state = original.handoff.state.lock().map_err(|_| unknown("original callback handoff unavailable"))?;
                if state.cleanup_inflight || state.cleanup_action.as_ref().is_some_and(|action| !matches!(action.peek(), Some(Ok(()))))
                    || state.terminal_replan_cleanup.as_ref().is_some_and(|cleanup| !cleanup.retired)
                    || state.drain.as_ref().is_some_and(|drain| !matches!(drain.peek(), Some(Ok(_)))) {
                    return Err(unknown("original cleanup or physical drain remains unresolved"));
                }
                if state.callback_decision.is_some_and(|existing| existing != decision) {
                    return Err(unknown("original callback winner changed"));
                }
                if state.callback_action.is_none() {
                    let action_deadline = state.drain_deadline.map_or(deadline, |original| original.min(deadline));
                    state.callback_deadline = Some(action_deadline);
                    state.callback_decision = Some(decision);
                    state.callback_nonce = Some(Arc::new(()));
                    let action_executor = Arc::clone(executor);
                    let action_ticket = ticket.clone();
                    let action_owner = original.clone();
                    let action_nonce = state.callback_nonce.clone().ok_or_else(|| unknown("original callback identity missing"))?;
                    let action_runner = self.clone();
                    let reason = requested_abort.unwrap_or("canonical_binding_retirement").to_string();
                    let driver = tokio::spawn(async move {
                        // The registered handoff serializes all original
                        // operations; recheck it after acquiring the gate,
                        // then release the gate before executor I/O.
                        {
                            let remaining = action_deadline.saturating_duration_since(std::time::Instant::now());
                            let _gate = tokio::time::timeout(remaining, action_owner.gate.lock()).await
                                .map_err(|_| "original callback action gate deadline expired".to_string())?;
                            if !action_runner.active.lock().await.get(&(action_ticket.graph_id.clone(), action_ticket.node_id.clone()))
                                .is_some_and(|active| active.ticket == action_owner.ticket && active.binding == action_owner.binding
                                    && Arc::ptr_eq(&active.executor, &action_owner.executor)
                                    && Arc::ptr_eq(&active.callback_gate, &action_owner.gate)
                                    && Arc::ptr_eq(&active.cancel_outcome, &action_owner.handoff)) {
                                return Err("original callback owner changed before action".to_string());
                            }
                            let graph = action_runner.state_store.load_async(&action_ticket.graph_id).await.map_err(|error| error.to_string())?;
                            if action_runner.commit_service.binding_callback_disposition(&graph, &action_ticket.node_id, &action_owner.binding)
                                .map_err(|error| error.to_string())? != decision {
                                return Err("original callback winner changed before action".to_string());
                            }
                            let state = action_owner.handoff.state.lock().map_err(|_| "original callback handoff unavailable".to_string())?;
                            if state.callback_decision != Some(decision) || state.callback_nonce.as_ref()
                                .is_none_or(|nonce| !Arc::ptr_eq(nonce, &action_nonce)) {
                                return Err("original callback action identity changed".to_string());
                            }
                        }
                        if std::time::Instant::now() >= action_deadline {
                            return Err("original callback action deadline expired".to_string());
                        }
                        match decision {
                            CallbackDisposition::Commit => run_after_commit_observed(&action_executor, &action_ticket).await,
                            CallbackDisposition::Abort => action_executor.after_abort(&action_ticket, &reason).await,
                            CallbackDisposition::Pending => return Err("original callback became pending".to_string()),
                        }.map_err(|error| error.to_string())
                    });
                    state.callback_action = Some(async move {
                        driver.await.map_err(|error| format!("original callback driver join failed: {error}"))?
                    }.boxed().shared());
                }
                (state.callback_action.clone().ok_or_else(|| unknown("original callback action missing"))?,
                    state.callback_nonce.clone().ok_or_else(|| unknown("original callback identity missing"))?,
                    state.callback_deadline.ok_or_else(|| unknown("original callback deadline missing"))?)
            };
            let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
            match tokio::time::timeout(remaining, action).await {
                Ok(Ok(())) => {},
                Ok(Err(error)) => return Err(unknown(&format!("original callback action unconfirmed: {error}"))),
                Err(_) => return Err(unknown("original callback action remains in flight after deadline")),
            }
            let proof_ready = original.handoff.ready.notified();
            tokio::pin!(proof_ready);
            proof_ready.as_mut().enable();
            let proof_guard = {
                let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
                let _gate = tokio::time::timeout(remaining, original.gate.lock()).await
                    .map_err(|_| unknown("original callback proof gate deadline expired"))?;
                if !self.active.lock().await.get(&(graph_id.to_owned(), ticket.node_id.clone())).is_some_and(|active| matches_original(active)) {
                    return Err(unknown("original callback owner changed before proof"));
                }
                let mut state = original.handoff.state.lock().map_err(|_| unknown("original callback handoff unavailable"))?;
                if state.callback_decision != Some(decision) || state.callback_nonce.as_ref().is_none_or(|value| !Arc::ptr_eq(value, &nonce)) {
                    return Err(unknown("original callback proof owner changed"));
                }
                if state.proof_inflight {
                    None
                } else {
                    state.proof_inflight = true;
                    Some(OriginalProofInFlight(Arc::clone(&original.handoff)))
                }
            };
            let Some(proof_guard) = proof_guard else {
                let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
                tokio::time::timeout(remaining, proof_ready).await
                    .map_err(|_| unknown("original callback proof remains in flight after deadline"))?;
                continue;
            };
            // The original action is never replayed when only its source proof
            // needs another read. No graph, gate, Session or active lock is held.
            let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
            let proof = tokio::time::timeout(remaining,
                self.complete_callback_locked_until(graph_id, executor, ticket, requested_abort, Some(original_deadline), decision))
                .await.map_err(|_| unknown("original callback proof remains unconfirmed after deadline"))?;
            let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
            let graph = tokio::time::timeout(remaining, self.state_store.load_async(graph_id)).await
                .map_err(|_| unknown("original callback graph deadline expired after proof"))?
                .map_err(|_| unknown("original callback graph unavailable after proof"))?;
            let winner = self.commit_service.binding_callback_disposition(&graph, &ticket.node_id, &original.binding)
                .map_err(|_| unknown("original callback decision unavailable after proof"))?;
            let remaining = original_deadline.saturating_duration_since(std::time::Instant::now());
            let _gate = tokio::time::timeout(remaining, original.gate.lock()).await
                .map_err(|_| unknown("original callback gate deadline expired after proof"))?;
            let mut active = self.active.lock().await;
            let current = active.get_mut(&(graph_id.to_owned(), ticket.node_id.clone()))
                .filter(|active| matches_original(active)).ok_or_else(|| unknown("original callback owner changed after proof"))?;
            let state = original.handoff.state.lock().map_err(|_| unknown("original callback handoff unavailable"))?;
            if winner != decision || state.callback_decision != Some(decision)
                || state.callback_nonce.as_ref().is_none_or(|value| !Arc::ptr_eq(value, &nonce)) {
                return Err(unknown("original callback winner changed after proof"));
            }
            current.callback_completed = proof.is_ok();
            current.release_blocked = proof.is_err();
            drop(state);
            drop(proof_guard);
            return proof;
        }
    }

    fn close_active_binding_leases(
        &self,
        graph_id: &str,
        node_id: &str,
        ticket: &NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
        resources: &mut NodeResourceGuards,
        no_dispatch: Option<&Arc<super::executors::agent_tool::NoDispatchObservation>>,
        deadline: std::time::Instant,
    ) -> Result<Option<LeaseClosureCertification>, ExecutionRunnerError> {
        if !ExecutionBindingLeaseClosure::required(binding) {
            return Ok(None);
        }
        #[cfg(test)]
        {
            let probe = self.closure_probe.lock().unwrap().clone();
            if let Some(probe) = probe {
                probe(graph_id);
            }
        }
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(ExecutionRunnerError::Driver("original lease closure deadline expired".into()));
            }
            let graph = self.state_store.load(graph_id)?;
            let position = self
                .state_store
                .binding_start_position(&graph, node_id, binding)?
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver("original lease start missing".into())
                })?;
            let owner = resources.owner_fence.as_mut().ok_or_else(|| {
                ExecutionRunnerError::Driver("original binding owner capability missing".into())
            })?;
            let released = owner
                .release_original(
                    &graph,
                    node_id,
                    binding,
                    position,
                    &mut resources.resource,
                    &mut resources.scope,
                    &mut resources.worktree,
                    &self.worktree_leases,
                )
                .map_err(ExecutionRunnerError::Driver)?;
            if self
                .state_store
                .binding_lease_closure(&graph, node_id, binding, position)?
            {
                if no_dispatch.map(|owner| owner.original_early_reads()).transpose()
                    .map_err(ExecutionRunnerError::Driver)?.is_some_and(|reads| !reads.is_empty()) {
                    return Err(ExecutionRunnerError::Driver("existing lease closure cannot authenticate retained early read".into()));
                }
                self.registry.forget_builtin_binding(ticket);
                return Ok(None);
            }
            let early_reads = no_dispatch.map(|owner| owner.original_early_reads()).transpose()
                .map_err(ExecutionRunnerError::Driver)?.unwrap_or_default();
            let early_sources = if early_reads.is_empty() { Vec::new() } else {
                match self.commit_service.binding_physical_sources_with_original_reads(
                    &graph, node_id, binding,
                    no_dispatch.and_then(|owner| owner.witness()).as_ref(), &early_reads)? {
                    super::commit_service::PhysicalClosureProof::Proven(sources) => sources,
                    super::commit_service::PhysicalClosureProof::Pending(
                        super::commit_service::PhysicalClosurePending::SourceMoved { stream_id },
                    ) => {
                        if std::time::Instant::now() >= deadline {
                            return Err(ExecutionCommitError::PhysicalClosureSourceMoved { stream_id }.into());
                        }
                        std::thread::sleep(super::recovery::PHYSICAL_CLOSURE_RETRY_PAUSE.min(
                            deadline.saturating_duration_since(std::time::Instant::now()),
                        ));
                        continue;
                    },
                    super::commit_service::PhysicalClosureProof::Pending(
                        super::commit_service::PhysicalClosurePending::LeafIncomplete
                        | super::commit_service::PhysicalClosurePending::AgentRunUnsettled,
                    )
                    | super::commit_service::PhysicalClosureProof::Unprovable { .. } =>
                        return Err(ExecutionRunnerError::Driver("original early read sources unconfirmed".into())),
                }
            };
            // The physical release above gave back every held lease and the
            // proof above authenticated the original early reads against the
            // durable receipts. `released` borrows the fence that stays inside
            // these guards; the durable checkpoint is decided in one place by
            // the coordination-side caller below.
            let _ = released;
            return Ok(Some(LeaseClosureCertification { early_reads, early_sources }));
        }
    }

    /// Commit the durable lease-closure checkpoint for a physical release
    /// already certified outside the domain. The caller holds graph
    /// coordination across this call, so this append is one more revision
    /// decision taken inside the single graph decision domain: no
    /// coordination-side consumer can have its load-to-CAS window
    /// invalidated by the checkpoint, while slow physical release IO stays
    /// off the domain lock. `release_original` is idempotent on a released
    /// fence and re-derives the same closure against the fresh same-source
    /// head; only a moved external proof stream retries here.
    async fn commit_certified_lease_closure_checkpoint(
        &self,
        graph_id: &str,
        node_id: &str,
        ticket: &NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
        retained: &Arc<StdMutex<Option<NodeResourceGuards>>>,
        no_dispatch: Option<&Arc<super::executors::agent_tool::NoDispatchObservation>>,
        certification: LeaseClosureCertification,
        deadline: std::time::Instant,
    ) -> Result<(), ExecutionRunnerError> {
        let commits = self.commit_service.clone();
        let stores = self.state_store.clone();
        let registry = Arc::clone(&self.registry);
        let worktrees = Arc::clone(&self.worktree_leases);
        let graph_id = graph_id.to_string();
        let node_id = node_id.to_string();
        let ticket = ticket.clone();
        let binding = binding.clone();
        let retained = Arc::clone(retained);
        let no_dispatch = no_dispatch.cloned();
        let LeaseClosureCertification { early_reads, early_sources } = certification;
        tokio::task::spawn_blocking(move || {
            let mut slot = retained.lock().map_err(|_| ExecutionRunnerError::Driver(
                "original lease closure checkpoint owner unavailable".into(),
            ))?;
            let Some(mut original) = slot.take() else {
                // The original binding owner may already have certified and
                // removed this binding through its own take of the shared
                // closure action. The durable checkpoint either exists, in
                // which case this waiter is complete, or the original owner
                // still holds the guards elsewhere; nothing is re-appended.
                let graph = stores.load(&graph_id)?;
                let durable = match stores
                    .binding_start_position(&graph, &node_id, &binding)?
                {
                    Some(position) => stores
                        .binding_lease_closure(&graph, &node_id, &binding, position)?,
                    None => false,
                };
                if durable {
                    return Ok(());
                }
                return Err(ExecutionRunnerError::Driver(
                    "original lease closure checkpoint resources missing".into(),
                ));
            };
            let outcome = (|| -> Result<(), ExecutionRunnerError> {
                let mut proof_retries = 0;
                loop {
                    if std::time::Instant::now() >= deadline {
                        return Err(ExecutionRunnerError::Driver(
                            "original lease closure checkpoint deadline expired".into(),
                        ));
                    }
                    let graph = stores.load(&graph_id)?;
                    let position = stores
                        .binding_start_position(&graph, &node_id, &binding)?
                        .ok_or_else(|| ExecutionRunnerError::Driver(
                            "original lease start missing".into(),
                        ))?;
                    let owner = original.owner_fence.as_mut().ok_or_else(|| ExecutionRunnerError::Driver(
                        "original binding owner capability missing".into(),
                    ))?;
                    let released = owner
                        .release_original(
                            &graph,
                            &node_id,
                            &binding,
                            position,
                            &mut original.resource,
                            &mut original.scope,
                            &mut original.worktree,
                            &worktrees,
                        )
                        .map_err(ExecutionRunnerError::Driver)?;
                    if stores.binding_lease_closure(&graph, &node_id, &binding, position)? {
                        if !early_reads.is_empty() {
                            return Err(ExecutionRunnerError::Driver(
                                "existing lease closure cannot authenticate retained early read".into(),
                            ));
                        }
                        registry.forget_builtin_binding(&ticket);
                        return Ok(());
                    }
                    match commits.commit_binding_lease_closure_with_original_reads(
                        &graph,
                        &released,
                        no_dispatch
                            .as_ref()
                            .and_then(|observation| observation.witness())
                            .as_ref(),
                        &early_reads,
                        Some(deadline),
                    ) {
                        Ok(receipt) => {
                            if let Some(owner) = no_dispatch.as_ref() {
                                owner
                                    .release_original_early_reads(
                                        &ticket,
                                        &early_reads,
                                        &early_sources,
                                        &receipt,
                                    )
                                    .map_err(ExecutionRunnerError::Driver)?;
                            }
                            registry.forget_builtin_binding(&ticket);
                            return Ok(());
                        }
                        Err(error) => {
                            if matches!(&error, ExecutionCommitError::EventStore(
                                crate::RuntimeEventStoreError::StaleRevision { stream_id, .. })
                                if stream_id == &graph_id)
                            {
                                // The graph stream moved while its decision
                                // domain was held: a domain-external writer
                                // exists again. Surface it, never rebase over it.
                                return Err(error.into());
                            }
                            if matches!(&error, ExecutionCommitError::EventStore(
                                crate::RuntimeEventStoreError::StaleRevision { .. })
                                | ExecutionCommitError::PhysicalClosureSourceMoved { .. })
                            {
                                // Only an external proof stream can move
                                // here; re-decide from the fresh durable head
                                // inside the domain under the shared deadline.
                                if proof_retries >= 7 || std::time::Instant::now() >= deadline {
                                    return Err(error.into());
                                }
                                proof_retries += 1;
                                std::thread::sleep(super::recovery::PHYSICAL_CLOSURE_RETRY_PAUSE.min(
                                    deadline.saturating_duration_since(std::time::Instant::now()),
                                ));
                                continue;
                            }
                            // A lost append response is not a rollback. Re-read
                            // the exact original binding closure before
                            // reporting Unknown.
                            let current = stores.load(&graph_id)?;
                            let exact = stores.binding_lease_closure(
                                &current, &node_id, &binding, position)?;
                            if exact && early_reads.is_empty() {
                                registry.forget_builtin_binding(&ticket);
                                return Ok(());
                            }
                            return Err(error.into());
                        }
                    }
                }
            })();
            *slot = Some(original);
            outcome
        })
        .await
        .map_err(|error| ExecutionRunnerError::Driver(format!(
            "original lease closure checkpoint driver join failed: {error}"
        )))?
    }

    async fn take_bound_active_node(
        &self,
        graph_id: &str,
        node_id: &str,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<Option<ActiveNode>, ExecutionRunnerError> {
        self.take_bound_active_node_captured(graph_id, node_id, expected_binding, None, None).await
    }

    async fn take_bound_active_node_captured(
        &self, graph_id: &str, node_id: &str, expected_binding: Option<&ExecutionNodeBinding>,
        captured: Option<&PumpCancellationOwner>, deadline: Option<std::time::Instant>,
    ) -> Result<Option<ActiveNode>, ExecutionRunnerError> {
        let key = (graph_id.to_string(), node_id.to_string());
        let owner = self
            .active
            .lock()
            .await
            .get(&key)
            .filter(|active| Some(active.binding.clone()).as_ref() == expected_binding
                && captured.is_none_or(|original| active.binding == original.binding && active.ticket == original.ticket
                    && Arc::ptr_eq(&active.executor, &original.executor)
                    && Arc::ptr_eq(&active.callback_gate, &original.gate)
                    && Arc::ptr_eq(&active.cancel_outcome, &original.handoff)))
            .map(|active| {
                (
                    active.callback_gate.clone(),
                    active.executor.clone(),
                    active.ticket.clone(),
                    active.cancel_outcome.clone(),
                )
            });
        let Some((gate, executor, ticket, handoff)) = owner else {
            return Ok(None);
        };
        let close_deadline = handoff.closing_deadline(
            deadline.unwrap_or_else(|| std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET))
            .map_err(|_| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original closing deadline unavailable".into(),
            }))?;
        self.complete_callback_observed_until(graph_id, &executor, &ticket, None, close_deadline).await?;
        // Never wait for either lock while holding the other. The same
        // original closing deadline bounds both acquisition attempts.
        let (_coordination, _owner) = loop {
            let remaining = close_deadline.saturating_duration_since(std::time::Instant::now());
            let coordination = tokio::time::timeout(remaining, self.graph_coordination(graph_id)).await
                .map_err(|_| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(), reason:"original graph coordination deadline expired".into(),
                }))?;
            if let Ok(owner) = gate.try_lock() { break (coordination, owner); }
            drop(coordination);
            let remaining = close_deadline.saturating_duration_since(std::time::Instant::now());
            let owner = tokio::time::timeout(remaining, gate.lock()).await
                .map_err(|_| ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(), reason:"original callback gate deadline expired".into(),
                }))?;
            drop(owner);
        };
        if !self.active.lock().await.get(&key).is_some_and(|active|
            Some(&active.binding) == expected_binding && active.ticket == ticket
                && Arc::ptr_eq(&active.executor, &executor) && Arc::ptr_eq(&active.callback_gate, &gate)
                && Arc::ptr_eq(&active.cancel_outcome, &handoff)) {
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original callback owner changed before gate recheck".into(),
            }));
        }
        let (resources, binding, no_dispatch) = {
            let active = self.active.lock().await;
            let Some(finished) = active.get(&key).filter(|finished| {
                Some(&finished.binding) == expected_binding && finished.ticket == ticket
                    && Arc::ptr_eq(&finished.executor, &executor)
                    && Arc::ptr_eq(&finished.callback_gate, &gate)
                    && Arc::ptr_eq(&finished.cancel_outcome, &handoff)
            }) else {
                return Ok(None);
            };
            if finished.release_blocked || !finished.callback_completed {
                return Err(ExecutionRunnerError::Driver(
                    "original callback has not positively completed; binding release blocked"
                        .into(),
                ));
            }
            (
                Arc::clone(&finished.resources),
                finished.binding.clone(),
                finished.no_dispatch.clone(),
            )
        };
        let checkpoint_no_dispatch = no_dispatch.clone();
        let close_action = {
            let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Executor(
                NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(),
                    reason: "original lease closure handoff unavailable".into() }))?;
            if state.lease_close_action.is_none() {
                if state.lease_close_resources.is_some() {
                    return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                        node_id: ticket.node_id.clone(), reason: "original lease resources already transferred without a driver".into(),
                    }));
                }
                if std::time::Instant::now() >= close_deadline {
                    return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                        node_id: ticket.node_id.clone(), reason: "original lease closure deadline expired".into(),
                    }));
                }
                let runner = self.clone(); let graph = graph_id.to_owned(); let node = node_id.to_owned();
                let original_ticket = ticket.clone(); let original_binding = binding.clone();
                let original_executor = Arc::clone(&executor);
                let retained_resources = {
                    let mut slot = resources.lock().map_err(|_| ExecutionRunnerError::Executor(
                        NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(),
                            reason: "original binding resource owner poisoned".into() }))?;
                    let original = std::mem::replace(&mut *slot, NodeResourceGuards {
                        resource: None, scope: None, worktree: None, original_binding: None,
                        admission_pending: false, owner_fence: None,
                    });
                    Arc::new(StdMutex::new(Some(original)))
                };
                state.lease_close_resources = Some(Arc::clone(&retained_resources));
                let driver = tokio::task::spawn_blocking(move || {
                    let mut original = retained_resources.lock().map_err(|_| "retained resource owner poisoned".to_string())?
                        .take().ok_or_else(|| "retained resource owner missing".to_string())?;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let certification = runner.close_active_binding_leases(&graph, &node, &original_ticket, &original_binding,
                            &mut original, no_dispatch.as_ref(), close_deadline).map_err(|error| error.to_string())?;
                        original_executor.original_binding_released(&original_ticket).map_err(|error| error.to_string())?;
                        Ok(certification)
                    })).unwrap_or_else(|_| Err("original lease closure driver panicked".into()));
                    *retained_resources.lock().map_err(|_| "retained resource owner poisoned".to_string())? = Some(original);
                    result
                });
                state.lease_close_action = Some(async move {
                    driver.await.map_err(|error| format!("original lease closure driver join failed: {error}"))?
                }.boxed().shared());
            }
            state.lease_close_action.clone().ok_or_else(|| ExecutionRunnerError::Driver("original lease closure driver missing".into()))?
        };
        drop(_owner);
        drop(_coordination);
        // The physical lease release and its proof run outside the graph
        // coordination decision domain, so slow scope or worktree IO can
        // never serialize an unrelated successor's commits behind this
        // closure. The durable checkpoint it certifies - the only graph
        // revision this closure produces - is appended below, inside the
        // same domain that the coalesced terminal wave, dependency
        // reduction, command applies and start waves already share. That
        // is where the checkpoint used to slip between another consumer's
        // locked load and its own append and stale its CAS (the
        // StaleRevision expected N actual N+1 terminal wave failures with
        // an unresolved original owner).
        let remaining = close_deadline.saturating_duration_since(std::time::Instant::now());
        let lease_closure_certification = match tokio::time::timeout(remaining, close_action).await {
            Ok(Ok(certification)) => certification,
            Ok(Err(reason)) => return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: format!("original lease closure unconfirmed: {reason}"),
            })),
            Err(_) => return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original lease closure remains in flight".into(),
            })),
        };
        if std::time::Instant::now() >= close_deadline {
            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original lease closure completed after deadline".into(),
            }));
        }
        #[cfg(test)]
        {
            let probe = self.lease_closure_checkpoint_probe.lock().unwrap().clone();
            if let Some(probe) = probe {
                probe(graph_id);
            }
        }
        // The checkpoint takes the graph decision domain only, never the
        // per-node callback gate: a callback winner on the same node must
        // keep being able to register and finish its action while this
        // owner's durable closure is appended. The gate is taken again
        // below for certification exactly as before.
        if let Some(certification) = lease_closure_certification {
            let retained = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver(
                "original lease closure handoff unavailable".into(),
            ))?.lease_close_resources.clone().ok_or_else(|| ExecutionRunnerError::Driver(
                "original retained resources missing".into(),
            ))?;
            let checkpoint_coordination = self.graph_coordination(graph_id).await;
            let checkpoint = self
                .commit_certified_lease_closure_checkpoint(
                    graph_id,
                    node_id,
                    &ticket,
                    &binding,
                    &retained,
                    checkpoint_no_dispatch.as_ref(),
                    certification,
                    close_deadline,
                )
                .await;
            drop(checkpoint_coordination);
            checkpoint.map_err(|error| ExecutionRunnerError::Executor(
                NodeExecutorError::OriginalCustodyUnknown {
                    node_id: ticket.node_id.clone(),
                    reason: format!("original lease closure checkpoint unconfirmed: {error}"),
                },
            ))?;
        }
        let (_coordination, _owner) = self.coordinate_original_gate(graph_id, node_id, &gate, close_deadline).await?;
        #[cfg(test)]
        {
            let probe = self.closure_finished_probe.lock().unwrap().clone();
            if let Some(probe) = probe {
                probe(graph_id);
            }
        }
        let mut active = self.active.lock().await;
        if !active.get(&key).is_some_and(|finished| {
            finished.ticket == ticket
                && finished.binding == binding
                && Arc::ptr_eq(&finished.resources, &resources)
                && Arc::ptr_eq(&finished.executor, &executor)
                && Arc::ptr_eq(&finished.callback_gate, &gate)
                && Arc::ptr_eq(&finished.cancel_outcome, &handoff)
        }) {
            if handoff.state.lock().ok().is_some_and(|state| state.original_binding_closed) {
                return Ok(None);
            }
            return Err(ExecutionRunnerError::Driver(
                "original binding owner changed during closure".into(),
            ));
        }
        let retained_resources = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver(
            "original lease closure handoff unavailable".into()))?.lease_close_resources.clone()
            .ok_or_else(|| ExecutionRunnerError::Driver("original retained resources missing".into()))?;
        let mut original = resources.lock().map_err(|_| {
            ExecutionRunnerError::Driver(
                "original binding resource owner poisoned after closure".into(),
            )
        })?;
        let restored = retained_resources.lock().map_err(|_| ExecutionRunnerError::Driver(
            "original retained resources poisoned".into()))?.take()
            .ok_or_else(|| ExecutionRunnerError::Driver("original retained resources still in use".into()))?;
        *original = restored;
        // The original gate is still held. Only this owner, after callback,
        // lease closure and executor release, may certify its own removal.
        let mut state = handoff.state.lock().map_err(|_| ExecutionRunnerError::Driver("original terminal handoff unavailable".into()))?;
        if state.terminal_replan_cleanup.as_ref().is_some_and(|cleanup| !cleanup.retired) {
            return Err(ExecutionRunnerError::Driver("original terminal cleanup unconfirmed".into()));
        }
        state.original_binding_closed = true;
        let fence = original.owner_fence.take();
        let finished = active.remove(&key);
        self.active_changed.notify_one();
        drop(state);
        drop(active);
        drop(original);
        drop(fence);
        Ok(finished)
    }

    async fn commit_terminal_wave(
        &self,
        graph_id: &str,
        transition: ExecutionTerminalCommit,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<TerminalCommitDisposition, ExecutionRunnerError> {
        let (response, receiver) = oneshot::channel();
        let leader = {
            let mut batches = self.terminal_batches.lock().await;
            let batch = batches.entry(graph_id.to_string()).or_default();
            batch.pending.push(PendingTerminalCommit {
                transition,
                expected_binding: expected_binding.cloned(),
                response,
            });
            if batch.leader_active {
                false
            } else {
                batch.leader_active = true;
                true
            }
        };
        if leader {
            // A tiny coalescing window gathers siblings that finished in the
            // same scheduler wave. It is bounded and does not delay a lone
            // dependency predecessor materially.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let pending = {
                let mut batches = self.terminal_batches.lock().await;
                batches
                    .remove(graph_id)
                    .map_or_else(Vec::new, |batch| batch.pending)
            };
            #[cfg(feature = "test-fixtures")]
            let trace_nodes = pending.iter().map(|item| (item.transition.node_id.clone(),
                item.expected_binding.as_ref().map(|binding| binding.attempt))).collect::<Vec<_>>();
            let coordination = self.graph_coordination_without_command(graph_id).await;
            match self.state_store.load_async(graph_id).await {
                Ok(current) => {
                    let mut admitted = Vec::new();
                    let mut admitted_responses = Vec::new();
                    let mut superseded_responses = Vec::new();
                    let active = self.active.lock().await;
                    for pending in pending {
                        if current.node_statuses.get(&pending.transition.node_id)
                            == Some(&ExecutionNodeStatus::Running)
                            && pending.expected_binding.as_ref().is_some_and(|binding| {
                                current
                                    .recovery_cursor
                                    .node_attempts
                                    .get(&pending.transition.node_id)
                                    == Some(&binding.attempt)
                                    && active
                                        .get(&(
                                            graph_id.to_string(),
                                            pending.transition.node_id.clone(),
                                        ))
                                        .is_some_and(|active| {
                                            active.binding.clone() == *binding
                                        })
                            })
                        {
                            let state = active.get(&(graph_id.to_string(), pending.transition.node_id.clone()))
                                .and_then(|owner| owner.cancel_outcome.state.lock().ok().map(|state| {
                                    (state.drain.clone(), state.drain_deadline,
                                        state.cleanup_action.clone(), state.cleanup_deadline)
                                }));
                            let Some((drain, deadline, cleanup, cleanup_deadline)) = state else {
                                let _ = pending.response.send(TerminalCommitDisposition::Failed(
                                    "original terminal handoff unavailable".into()));
                                continue;
                            };
                            if let Some(cleanup) = cleanup {
                                match cleanup.peek() {
                                    None => {
                                        let disposition = match cleanup_deadline {
                                            Some(deadline) => TerminalCommitDisposition::WaitForCleanup(cleanup.clone(), deadline),
                                            None => TerminalCommitDisposition::Failed("original cleanup deadline unavailable".into()),
                                        };
                                        let _ = pending.response.send(disposition);
                                        continue;
                                    }
                                    Some(Err(error)) => {
                                        let _ = pending.response.send(TerminalCommitDisposition::Failed(
                                            format!("original cleanup unconfirmed: {error}")));
                                        continue;
                                    }
                                    Some(Ok(())) => {}
                                }
                            }
                            if let Some(drain) = drain {
                                match drain.peek() {
                                    None => {
                                        let disposition = match deadline {
                                            Some(deadline) => TerminalCommitDisposition::WaitForDrain(drain.clone(), deadline),
                                            None => TerminalCommitDisposition::Failed("original drain deadline unavailable".into()),
                                        };
                                        let _ = pending.response.send(disposition);
                                        continue;
                                    }
                                    Some(Err(error)) => {
                                        let _ = pending.response.send(TerminalCommitDisposition::Failed(
                                            format!("original physical drain unconfirmed: {error}")));
                                        continue;
                                    }
                                    Some(Ok(_)) => {}
                                }
                            }
                            #[cfg(feature = "test-fixtures")]
                            build16_callback_trace(graph_id, &pending.transition.node_id,
                                pending.expected_binding.as_ref().map(|binding| binding.attempt),
                                "terminal_wave_membership", "member=admitted");
                            admitted.push(pending.transition);
                            admitted_responses.push(pending.response);
                        } else {
                            #[cfg(feature = "test-fixtures")]
                            build16_callback_trace(graph_id, &pending.transition.node_id,
                                pending.expected_binding.as_ref().map(|binding| binding.attempt),
                                "terminal_wave_membership", "member=superseded");
                            superseded_responses.push(pending.response);
                        }
                    }
                    drop(active);
                    #[cfg(feature = "test-fixtures")]
                    let admitted_count = admitted.len();
                    let commit = if admitted.is_empty() {
                        Ok(())
                    } else {
                        self.commit_service
                            .transition_nodes_async(current, admitted)
                            .await
                            .map(|_| ())
                    };
                    #[cfg(feature = "test-fixtures")]
                    for (node, attempt) in &trace_nodes {
                        let detail = match &commit {
                            Ok(()) if admitted_count == 0 => "scope=batch result=no_admitted_transition".to_string(),
                            Ok(()) => format!("scope=batch result=committed admitted={admitted_count}"),
                            Err(_) => format!("scope=batch result=error admitted={admitted_count} original_detail=private"),
                        };
                        build16_callback_trace(graph_id, node, *attempt, "terminal_wave_batch_commit", &detail);
                    }
                    for response in superseded_responses {
                        let _ = response.send(TerminalCommitDisposition::Superseded);
                    }
                    drop(coordination);
                    match commit {
                        Ok(()) => {
                            // One durable terminal wave has one convergence
                            // owner. Running this in every sibling waiter
                            // turns an O(1) atomic commit into O(width) graph
                            // reloads and serial dependency reductions.
                            let convergence = if admitted_responses.is_empty() {
                                Ok(())
                            } else {
                                self.converge_after_terminal_commit(graph_id).await
                            };
                            #[cfg(feature = "test-fixtures")]
                            for (node, attempt) in &trace_nodes {
                                build16_callback_trace(graph_id, node, *attempt, "terminal_wave_batch_converge",
                                    &match &convergence { Ok(()) => "scope=batch result=ok".into(), Err(error) => format!("scope=batch result=error kind={} original_detail=private", build16_runner_error_kind(error)) });
                            }
                            if let Err(error) = convergence {
                                let reason = error.to_string();
                                for response in admitted_responses {
                                    let _ = response
                                        .send(TerminalCommitDisposition::Failed(reason.clone()));
                                }
                                return receiver.await.map_err(|_| {
                                    ExecutionRunnerError::Driver(
                                        "terminal wave convergence failed before returning a disposition"
                                            .to_string(),
                                    )
                                });
                            }
                            for response in admitted_responses {
                                let _ = response.send(TerminalCommitDisposition::Committed);
                            }
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            for response in admitted_responses {
                                let _ = response
                                    .send(TerminalCommitDisposition::Failed(reason.clone()));
                            }
                        }
                    }
                }
                Err(error) => {
                    drop(coordination);
                    let reason = error.to_string();
                    #[cfg(feature = "test-fixtures")]
                    for (node, attempt) in &trace_nodes {
                        build16_callback_trace(graph_id, node, *attempt, "terminal_wave_batch_load", "scope=batch result=error original_detail=private");
                    }
                    for pending in pending {
                        let _ = pending
                            .response
                            .send(TerminalCommitDisposition::Failed(reason.clone()));
                    }
                }
            }
        }
        receiver.await.map_err(|_| {
            ExecutionRunnerError::Driver(
                "terminal wave coordinator closed before returning a disposition".to_string(),
            )
        })
    }

    #[cfg(test)]
    pub(super) async fn pending_terminal_count(&self, graph_id: &str) -> usize {
        self.terminal_batches
            .lock()
            .await
            .get(graph_id)
            .map_or(0, |batch| batch.pending.len())
    }

    async fn finish_pump_node(
        &self,
        graph_id: &str,
        expected_node_id: &str,
        result: Result<(String, NodeExecutionOutcome), ExecutionRunnerError>,
        durable_progress: Arc<Notify>,
        expected_binding: Option<ExecutionNodeBinding>,
        original_handoff: Option<Arc<CancelOutcomeHandoff>>,
    ) -> Result<(), ExecutionRunnerError> {
        let Some((node_id, mut outcome)) = self
            .resolve_pump_result(
                graph_id,
                expected_node_id,
                result,
                &durable_progress,
                expected_binding.as_ref(),
            )
            .await?
        else {
            return Ok(());
        };
        #[cfg(feature = "test-fixtures")]
        let diagnostic_ticket = if std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some() {
            self.active.lock().await.get(&(graph_id.to_string(), node_id.clone()))
                .filter(|active| Some(&active.binding) == expected_binding.as_ref())
                .map(|active| active.ticket.clone()).filter(k05_completion_trace_enabled)
        } else { None };
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "resolve_pump_return", None, None); }
        let deadline_terminal = outcome
            .result
            .failure
            .as_ref()
            .is_some_and(|failure| failure.kind == "execution_deadline_exceeded");
        if let Err(error) = validate_outcome(&node_id, &outcome) {
            self.isolate_node_failure(
                graph_id,
                &node_id,
                expected_binding.as_ref(),
                error.to_string(),
            )
            .await?;
            return Err(error);
        }
        if let Some(replan) = outcome.replan.as_ref() {
            if let Err(error) = self.registry.validate_nodes(&replan.nodes) {
                self.isolate_node_failure(
                    graph_id,
                    &node_id,
                    expected_binding.as_ref(),
                    error.to_string(),
                )
                .await?;
                return Err(error.into());
            }
        }
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "intent_wait_check", None, None); }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "intent_wait_enter", None, None); }
            waiter.await;
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "intent_wait_return", None, None); }
        }
        let active_wave = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.clone()))
            .filter(|active| {
                Some(active.binding.clone()).as_ref()
                    == expected_binding.as_ref()
            })
            .map(|active| {
                (
                    Arc::clone(&active.executor),
                    active.ticket.clone(),
                    active.effect_receipt_required,
                )
            });
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "active_wave_checked", None, Some(active_wave.is_some())); }
        if active_wave.is_none() {
            return Ok(());
        }
        let batchable = outcome.replan.is_none()
            && outcome.delivery_envelope.is_none()
            && outcome.terminal_presentation.is_none();
        if batchable {
            let Some((executor, ticket, effect_receipt_required)) = active_wave.clone() else {
                return Err(ExecutionRunnerError::Driver(
                    "batchable active wave has no execution binding".to_string(),
                ));
            };
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_wave_enter", None, None); }
            let disposition = loop {
                let disposition = self.commit_terminal_wave(
                    graph_id,
                    ExecutionTerminalCommit {
                        node_id: node_id.clone(),
                        outcome: outcome.clone(),
                        effect_ticket: effect_receipt_required.then_some(ticket.clone()),
                    },
                    expected_binding.as_ref(),
                )
                .await?;
                match disposition {
                    TerminalCommitDisposition::WaitForDrain(drain, deadline) => {
                        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                        match tokio::time::timeout(remaining, drain).await {
                            Ok(Ok(_)) => continue,
                            Ok(Err(reason)) => return Err(ExecutionRunnerError::Executor(
                                NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(), reason })),
                            Err(_) => return Err(ExecutionRunnerError::Executor(
                                NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(),
                                    reason: "original physical drain remains pending".into() })),
                        }
                    }
                    TerminalCommitDisposition::WaitForCleanup(cleanup, deadline) => {
                        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                        match tokio::time::timeout(remaining, cleanup).await {
                            Ok(Ok(())) => continue,
                            Ok(Err(reason)) => return Err(ExecutionRunnerError::Executor(
                                NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(), reason })),
                            Err(_) => return Err(ExecutionRunnerError::Executor(
                                NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(),
                                    reason: "original cleanup remains pending".into() })),
                        }
                    }
                    disposition => break disposition,
                }
            };
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_wave_return", None, None); }
            #[cfg(feature = "test-fixtures")]
            build16_callback_trace(graph_id, &node_id, Some(ticket.attempt), "terminal_wave_disposition",
                match &disposition {
                    TerminalCommitDisposition::Committed => "wave=committed",
                    TerminalCommitDisposition::Superseded => "wave=superseded",
                    TerminalCommitDisposition::Failed(_) => "wave=failed source=unknown original_detail=private",
                    TerminalCommitDisposition::WaitForDrain(_, _) => "wave=drain_pending",
                    TerminalCommitDisposition::WaitForCleanup(_, _) => "wave=cleanup_pending",
                });
            match disposition {
                TerminalCommitDisposition::Committed => {
                    durable_progress.notify_one();
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        deadline_terminal.then_some("canonical_deadline_after_atomic_commit"),
                    )
                    .await?;
                    if deadline_terminal {
                        executor.cancellation_finalized(&ticket);
                    }
                    self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                        .await?;
                    // The direct successor of this node is released only once
                    // the original binding's durable lease closure exists:
                    // `published_dependency_predecessors` keeps a terminal
                    // predecessor in Running state until its lease closure is
                    // on disk, so a pump that wakes before that checkpoint
                    // cannot release the successor yet. The wave commit
                    // notification above therefore arrives too early to
                    // release anything, and without a second wake-up the
                    // successor waits for an unrelated event (the slow
                    // sibling's completion) before it is dispatched. Wake the
                    // pump again now that the closure is durable, so a
                    // committed branch releases its successor immediately.
                    durable_progress.notify_one();
                    return Ok(());
                }
                TerminalCommitDisposition::Superseded => {
                    // The captured handoff is the original binding's owner.
                    // It sets this bit only after physical closure, the
                    // original executor's release notification and resource
                    // handback all succeed under that owner's gate.
                    if original_handoff.as_ref().is_some_and(|handoff|
                        handoff.state.lock().ok().is_some_and(|state| state.original_binding_closed)) {
                        return Ok(());
                    }
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("graph_command_superseded_before_terminal_wave"),
                    )
                    .await?;
                    self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                        .await?;
                    return Ok(());
                }
                TerminalCommitDisposition::Failed(reason) => {
                    // A preview-producing outcome that cannot commit must still be
                    // retired. Without a durable retirement the callback resolves to
                    // `Pending` forever, so the executor is never told to abort the
                    // stream it already produced. The cancellation carries no domain
                    // events, so it is not blocked by whatever refused the terminal
                    // commit itself.
                    if let Ok(current) = self.state_store.load_async(graph_id).await {
                        if current.node_statuses.get(&node_id)
                            == Some(&ExecutionNodeStatus::Running)
                        {
                            let _ = self.commit_service.apply_command(
                                &current,
                                &ExecutionGraphCommand::CancelNode {
                                    expected_revision: current.revision,
                                    node_id: node_id.clone(),
                                    reason: "terminal wave commit failed".into(),
                                },
                            );
                        }
                    }
                    if let Err(cleanup) = self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("terminal_wave_commit_failed"),
                    ).await {
                        return Err(ExecutionRunnerError::TerminalWaveCleanup {
                            reason, phase: "original callback", cleanup: Box::new(cleanup),
                        });
                    }
                    if let Err(cleanup) = self.take_bound_active_node(
                        graph_id, &node_id, expected_binding.as_ref(),
                    ).await {
                        return Err(ExecutionRunnerError::TerminalWaveCleanup {
                            reason, phase: "original owner", cleanup: Box::new(cleanup),
                        });
                    }
                    return Err(ExecutionRunnerError::Driver(reason));
                }
                TerminalCommitDisposition::WaitForDrain(_, _) => return Err(ExecutionRunnerError::Executor(
                    NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(),
                        reason: "original wave drain remains pending".into() })),
                TerminalCommitDisposition::WaitForCleanup(_, _) => return Err(ExecutionRunnerError::Executor(
                    NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(),
                        reason: "original wave cleanup remains pending".into() })),
            }
        }

        // Commit terminal graph truth before waking the pump or publishing
        // process-local executor output.
        let mut original_terminal_retry = None;
        let committed_executor = {
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "coordination_enter", None, None); }
            let mut coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "coordination_acquired", None, None); }
            let mut current = self.state_store.load_async(graph_id).await?;
            #[cfg(feature = "test-fixtures")]
            if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "graph_loaded", Some(current.revision), None); }
            if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                || !expected_binding.as_ref().is_some_and(|binding| {
                    current.recovery_cursor.node_attempts.get(&node_id) == Some(&binding.attempt)
                })
                || !self
                    .active_binding_matches(graph_id, &node_id, expected_binding.as_ref())
                    .await
            {
                let aborted = active_wave
                    .as_ref()
                    .map(|(executor, ticket, _)| (Arc::clone(executor), ticket.clone()));
                drop(coordination);
                if let Some((executor, ticket)) = aborted {
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("graph_command_superseded_before_commit"),
                    )
                    .await?;
                }
                self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                    .await?;
                return Ok(());
            }
            let (original_executor, ticket, _) = active_wave.as_ref().ok_or_else(|| {
                ExecutionRunnerError::Driver("atomic outcome binding missing".into())
            })?;
            // Rebuild from durable state after an explicitly retryable source
            // change. Every attempt still validates the original carrier tail;
            // an incompatible tail cannot be made valid by retrying it.
            const MAX_SOURCE_PREPARATION_ATTEMPTS: usize = 3;
            let mut preparation_attempt = 0;
            let source_result = loop {
                preparation_attempt += 1;
                let result = self.commit_service.execution_outcome_sources(&current, ticket, &outcome);
                if !matches!(&result, Err(error) if retryable_proof_source_change(error))
                    || preparation_attempt >= MAX_SOURCE_PREPARATION_ATTEMPTS
                {
                    break result;
                }
                drop(coordination);
                tokio::task::yield_now().await;
                coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                current = self.state_store.load_async(graph_id).await?;
                let original_owner_live = self.active.lock().await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .is_some_and(|owner| {
                        Some(&owner.binding) == expected_binding.as_ref()
                            && owner.ticket == *ticket
                            && Arc::ptr_eq(&owner.executor, original_executor)
                            && !owner.callback_completed
                    });
                if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                    || !expected_binding.as_ref().is_some_and(|binding| {
                        current.recovery_cursor.node_attempts.get(&node_id) == Some(&binding.attempt)
                    })
                    || !original_owner_live
                {
                    return Err(ExecutionRunnerError::Driver(
                        "node transition superseded during source preparation retry".into(),
                    ));
                }
            };
            let mut outcome_sources = match source_result {
                Ok(sources) => sources,
                Err(error) => {
                    // Conflicting cuts cannot establish whether this original
                    // outcome or its physical effect committed. Preserve the
                    // exact owner instead of inventing a cancellation winner.
                    let source_conflict = matches!(&error,
                        ExecutionCommitError::InvalidCommand(reason)
                        if reason == "observation source conflicts with causal read-set");
                    let mut original_pump = false;
                    if source_conflict {
                        let active = self.active.lock().await;
                        let task = active.get(&(graph_id.to_owned(), node_id.clone()))
                            .filter(|owner| Some(&owner.binding) == expected_binding.as_ref()
                                && owner.ticket == *ticket && Arc::ptr_eq(&owner.executor, original_executor))
                            .and_then(|owner| owner.cancel_outcome.state.lock().ok()
                                .and_then(|state| state.pump_task.clone()));
                        if let Some(task) = task {
                            task.source_conflict.store(true, std::sync::atomic::Ordering::Release);
                            original_pump = true;
                        }
                    }
                    drop(coordination);
                    if source_conflict {
                        // A supervised pump drains after its work future returns.
                        // Direct runners have no pump observer, so they must
                        // initiate the same original-ticket drain here.
                        let cleanup = if original_pump { None } else {
                            self.active_cancel_observed(graph_id, original_executor, ticket).await.err()
                        };
                        let reason = match cleanup {
                            Some(cleanup) => format!("original outcome preparation failed: {error}; original drain unconfirmed: {cleanup}"),
                            None => format!("original outcome preparation failed: {error}"),
                        };
                        return Err(ExecutionRunnerError::Executor(
                            NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(), reason },
                        ));
                    } else if let Err(settlement_error) = self
                        .isolate_node_failure(
                            graph_id,
                            &node_id,
                            expected_binding.as_ref(),
                            format!("original outcome preparation failed: {error}"),
                        )
                        .await
                    {
                        return Err(ExecutionRunnerError::Driver(format!(
                            "{error}; original binding settlement failed: {settlement_error}"
                        )));
                    }
                    return Err(error.into());
                }
            };
            // A concurrent writer can advance this graph stream between the
            // load and the atomic outcome commit. Keep the captured outcome and
            // its original source fences while retrying instead of failing the whole
            // execution driver with a raw `revision mismatch`.
            const MAX_TRANSITION_ATTEMPTS: usize = 8;
            let mut attempt = 0;
            let mut owner_superseded = false;
            let transition = loop {
                attempt += 1;
                #[cfg(feature = "test-fixtures")]
                if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_cas_enter", Some(current.revision), None); }
                let result = self
                    .commit_service
                    .transition_execution_outcome_async(
                        current.clone(),
                        ticket.clone(),
                        outcome.clone(),
                        outcome_sources.clone(),
                    )
                    .await;
                #[cfg(feature = "test-fixtures")]
                if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "terminal_cas_return", Some(current.revision), Some(result.is_ok())); }
                #[cfg(feature = "test-fixtures")]
                if let (Some(ticket), Err(error)) = (&diagnostic_ticket, &result) { k05_completion_cas_error(ticket, error); }
                match result {
                    Ok(receipt) => break Ok(receipt),
                    Err(error) if original_terminal_retry.is_none()
                        && original_terminal_retryable_rollback(&error, &outcome) => {
                        let (executor, original_ticket, _) = active_wave.as_ref().ok_or_else(||
                            ExecutionRunnerError::Driver("original terminal retry owner unavailable".into()))?;
                        let binding = expected_binding.as_ref().ok_or_else(||
                            ExecutionRunnerError::Driver("original terminal retry binding unavailable".into()))?;
                        let original = outcome.clone();
                        let handle = original.original_synthesize_replan.clone().ok_or_else(||
                            ExecutionRunnerError::Driver("original Synthesize retry handle unavailable".into()))?;
                        drop(coordination);
                        let commits = self.commit_service.clone();
                        let source = original.clone();
                        let decision = tokio::task::spawn_blocking(move || commits.original_terminal_rollback_decision(&source))
                            .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original terminal winner read failed: {failure}")))??;
                        match decision {
                            super::commit_service::OriginalTerminalRollbackDecision::Committed(winner) => {
                                coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                                current = self.state_store.load_async(graph_id).await?;
                                if current.node_results.get(&node_id) != Some(&original.result)
                                    || current.recovery_cursor.node_attempts.get(&node_id) != Some(&binding.attempt)
                                    || !self.active_binding_matches(graph_id, &node_id, Some(binding)).await
                                { return Err(ExecutionRunnerError::Driver("original terminal winner binding differs".into())); }
                                break Ok(super::commit_service::ExecutionCommitReceipt {
                                    graph: current.clone(), transaction: winner.commit, original_replan_request: None,
                                });
                            }
                            super::commit_service::OriginalTerminalRollbackDecision::RolledBack(retained) => {
                                let candidate = handle.0.prepare(original_ticket, &original).await.map_err(ExecutionRunnerError::Driver)?;
                                validate_outcome(&node_id, &candidate)?;
                                let replan = candidate.replan.as_ref().ok_or_else(|| ExecutionRunnerError::Driver("original terminal replacement has no replan".into()))?;
                                self.registry.validate_nodes(&replan.nodes)?;
                                if candidate.delivery_envelope.is_some() || candidate.terminal_presentation.is_some() {
                                    return Err(ExecutionRunnerError::Driver("original terminal replacement is not nonterminal".into()));
                                }
                                self.replace_original_terminal_handoff(graph_id, original_ticket, binding, executor, &original, &candidate, &handle, &retained).await?;
                                coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                                current = self.state_store.load_async(graph_id).await?;
                                if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                                    || current.recovery_cursor.node_attempts.get(&node_id) != Some(&binding.attempt)
                                    || !self.active_binding_matches(graph_id, &node_id, Some(binding)).await
                                { return Err(ExecutionRunnerError::Driver("original terminal replacement binding changed".into())); }
                                outcome_sources = self.commit_service.execution_outcome_sources(&current, original_ticket, &candidate)?;
                                outcome = candidate;
                                original_terminal_retry = Some((original, handle, retained));
                                continue;
                            }
                        }
                    }
                    Err(ExecutionCommitError::EventStore(crate::RuntimeEventStoreError::CheckedAttemptRolledBack(cause)))
                        if original_terminal_retry.is_some() && attempt < MAX_TRANSITION_ATTEMPTS
                            && matches!(cause.as_ref(), crate::RuntimeEventStoreError::StaleRevision { .. }) => {
                        let (original, handle, retained) = original_terminal_retry.as_ref().ok_or_else(||
                            ExecutionRunnerError::Driver("original terminal retry custody unavailable".into()))?;
                        let (executor, original_ticket, _) = active_wave.as_ref().ok_or_else(||
                            ExecutionRunnerError::Driver("original terminal retry owner unavailable".into()))?;
                        let binding = expected_binding.as_ref().ok_or_else(||
                            ExecutionRunnerError::Driver("original terminal retry binding unavailable".into()))?;
                        if !retained.owner.replacement_rollback_confirmed_for_outcome(original_ticket, &outcome)
                            .map_err(ExecutionRunnerError::Driver)? {
                            return Err(ExecutionRunnerError::Driver("original replacement rollback unconfirmed".into()));
                        }
                        drop(coordination);
                        let commits = self.commit_service.clone();
                        let source = original.clone();
                        let decision = tokio::task::spawn_blocking(move || commits.original_terminal_rollback_decision(&source))
                            .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original terminal winner read failed: {failure}")))??;
                        if let super::commit_service::OriginalTerminalRollbackDecision::Committed(winner) = decision {
                            self.restore_original_terminal_handoff(graph_id, original_ticket, binding, executor, original, &outcome).await?;
                            coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                            current = self.state_store.load_async(graph_id).await?;
                            if current.node_results.get(&node_id) != Some(&original.result)
                                || current.recovery_cursor.node_attempts.get(&node_id) != Some(&binding.attempt)
                                || !self.active_binding_matches(graph_id, &node_id, Some(binding)).await {
                                return Err(ExecutionRunnerError::Driver("original terminal winner binding differs".into()));
                            }
                            outcome = original.clone();
                            original_terminal_retry = None;
                            break Ok(super::commit_service::ExecutionCommitReceipt {
                                graph: current.clone(), transaction: winner.commit, original_replan_request: None,
                            });
                        }
                        let candidate = handle.0.prepare(original_ticket, original).await.map_err(ExecutionRunnerError::Driver)?;
                        validate_outcome(&node_id, &candidate)?;
                        let replan = candidate.replan.as_ref().ok_or_else(|| ExecutionRunnerError::Driver("original terminal replacement has no replan".into()))?;
                        self.registry.validate_nodes(&replan.nodes)?;
                        if candidate.delivery_envelope.is_some() || candidate.terminal_presentation.is_some() {
                            return Err(ExecutionRunnerError::Driver("original terminal replacement is not nonterminal".into()));
                        }
                        self.replace_rolled_back_replan_handoff(graph_id, original_ticket, binding, executor, original, &outcome, &candidate).await?;
                        coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                        current = self.state_store.load_async(graph_id).await?;
                        if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                            || current.recovery_cursor.node_attempts.get(&node_id) != Some(&binding.attempt)
                            || !self.active_binding_matches(graph_id, &node_id, Some(binding)).await {
                            return Err(ExecutionRunnerError::Driver("original terminal replacement binding changed".into()));
                        }
                        outcome_sources = self.commit_service.execution_outcome_sources(&current, original_ticket, &candidate)?;
                        outcome = candidate;
                        continue;
                    }
                    Err(error @ ExecutionCommitError::EventStore(
                        crate::runtime_event_store::RuntimeEventStoreError::StaleRevision { .. },
                    )) if attempt < MAX_TRANSITION_ATTEMPTS => {
                        let (stream, expected, actual) = match &error {
                            ExecutionCommitError::EventStore(crate::runtime_event_store::RuntimeEventStoreError::StaleRevision { stream_id, expected, actual }) => (stream_id.clone(), *expected, *actual),
                            _ => unreachable!(),
                        };
                        if stream != graph_id {
                            let (executor, original_ticket, _) = active_wave.as_ref().expect("checked active binding");
                            if !self.registry.is_builtin_tool_batch(executor)
                                || outcome_sources.get(&stream) != Some(&expected)
                                || !stream.starts_with("goal:")
                            {
                                break Err(error);
                            }
                            // The original owner may do bounded persistent reads;
                            // never hold graph coordination across that work.
                            drop(coordination);
                            let rebuilt = executor.revalidate_goal_source(
                                original_ticket, &outcome, &stream, expected, actual,
                            ).await;
                            coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                            current = self.state_store.load_async(graph_id).await?;
                            let original_owner_live = self.active.lock().await
                                .get(&(graph_id.to_string(), node_id.clone()))
                                .is_some_and(|owner| {
                                    Some(&owner.binding) == expected_binding.as_ref()
                                        && owner.ticket == *original_ticket
                                        && Arc::ptr_eq(&owner.executor, executor)
                                        && !owner.callback_completed
                                });
                            if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running)
                                || !expected_binding.as_ref().is_some_and(|binding| current.recovery_cursor.node_attempts.get(&node_id) == Some(&binding.attempt))
                                || !original_owner_live
                            {
                                owner_superseded = !original_owner_live;
                                break Err(ExecutionCommitError::InvalidCommand("node transition superseded during Goal revalidation".into()));
                            }
                            let Ok(Some(candidate)) = rebuilt else { break Err(error); };
                            if validate_outcome(&node_id, &candidate).is_err()
                                || candidate.replan.as_ref().is_some_and(|plan| self.registry.validate_nodes(&plan.nodes).is_err())
                            {
                                break Err(error);
                            }
                            let candidate_sources = match self.commit_service.execution_outcome_sources(&current, original_ticket, &candidate) {
                                Ok(sources) => sources,
                                Err(source_error) => break Err(source_error),
                            };
                            if !accepts_goal_revalidation(&outcome, &candidate, &outcome_sources, &candidate_sources, &stream, expected, actual) {
                                break Err(error);
                            }
                            outcome_sources = candidate_sources;
                            outcome = candidate;
                            continue;
                        }
                        current = self.state_store.load_async(graph_id).await?;
                        if current.node_statuses.get(&node_id)
                            != Some(&ExecutionNodeStatus::Running)
                            || !expected_binding.as_ref().is_some_and(|binding| {
                                current.recovery_cursor.node_attempts.get(&node_id)
                                    == Some(&binding.attempt)
                            })
                            || !self
                                .active_binding_matches(
                                    graph_id,
                                    &node_id,
                                    expected_binding.as_ref(),
                                )
                                .await
                        {
                            break Err(ExecutionCommitError::InvalidCommand(
                                "node transition superseded by a concurrent writer".to_string(),
                            ));
                        }
                    }
                    Err(error) => break Err(error),
                }
            };
            let receipt = match transition {
                Ok(receipt) => receipt,
                Err(error) if original_terminal_retry.is_some() => {
                    let (original, _handle, _retained) = original_terminal_retry.take().ok_or_else(||
                        ExecutionRunnerError::Driver("original terminal retry custody unavailable".into()))?;
                    let (executor, original_ticket, _) = active_wave.as_ref().ok_or_else(||
                        ExecutionRunnerError::Driver("original terminal winner owner unavailable".into()))?;
                    let binding = expected_binding.as_ref().ok_or_else(||
                        ExecutionRunnerError::Driver("original terminal winner binding unavailable".into()))?;
                    drop(coordination);
                    let commits = self.commit_service.clone();
                    let source = original.clone();
                    let decision = tokio::task::spawn_blocking(move || commits.original_terminal_rollback_decision(&source))
                        .await.map_err(|failure| ExecutionRunnerError::Driver(format!("original terminal winner read failed: {failure}")))??;
                    let super::commit_service::OriginalTerminalRollbackDecision::Committed(winner) = decision else {
                        // Absence of a winner is not proof that one cannot
                        // still commit; retain both original handoff values.
                        return Err(error.into());
                    };
                    self.restore_original_terminal_handoff(graph_id, original_ticket, binding, executor, &original, &outcome).await?;
                    coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding.as_ref()).await?;
                    current = self.state_store.load_async(graph_id).await?;
                    if current.node_results.get(&node_id) != Some(&original.result)
                        || current.recovery_cursor.node_attempts.get(&node_id) != Some(&binding.attempt)
                        || !self.active_binding_matches(graph_id, &node_id, Some(binding)).await
                    { return Err(ExecutionRunnerError::Driver("original terminal winner binding differs".into())); }
                    outcome = original;
                    super::commit_service::ExecutionCommitReceipt {
                        graph: current.clone(), transaction: winner.commit, original_replan_request: None,
                    }
                }
                Err(error) => {
                if owner_superseded {
                    // A same-key replacement is not the original owner. Do
                    // not call its callback or release its leases.
                    return Err(error.into());
                }
                #[cfg(test)]
                if self
                    .drop_injected_empty_bound_owner(
                        graph_id,
                        &node_id,
                        expected_binding.as_ref(),
                        &error,
                    )
                    .await?
                {
                    return Err(error.into());
                }
                let aborted = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .filter(|active| {
                        Some(active.binding.clone()).as_ref()
                            == expected_binding.as_ref()
                    })
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                drop(coordination);
                if let Some((executor, ticket)) = aborted {
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("graph_transition_commit_failed"),
                    )
                    .await?;
                }
                self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                    .await?;
                return Err(error.into());
                }
            };
            if let Some((original, handle, retained)) = original_terminal_retry.take() {
                let (executor, original_ticket, _) = active_wave.as_ref().ok_or_else(||
                    ExecutionRunnerError::Driver("original terminal retry owner unavailable".into()))?;
                let binding = expected_binding.as_ref().ok_or_else(||
                    ExecutionRunnerError::Driver("original terminal retry binding unavailable".into()))?;
                let _ = (original, handle, retained, binding);
                if receipt.original_replan_request.is_none() {
                    return Err(ExecutionRunnerError::Driver("original terminal replan request unavailable".into()));
                }
                drop(coordination);
                self.resume_original_terminal_replan_cleanup(graph_id, original_ticket, executor).await?;
            }
            self.active
                .lock()
                .await
                .get(&(graph_id.to_string(), node_id.clone()))
                .filter(|active| {
                    Some(active.binding.clone()).as_ref()
                        == expected_binding.as_ref()
                })
                .map(|active| (Arc::clone(&active.executor), active.ticket.clone()))
        };
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "post_commit_converge_enter", None, None); }
        let converged = self.converge_after_terminal_commit(graph_id).await;
        #[cfg(feature = "test-fixtures")]
        if let Some(ticket) = &diagnostic_ticket { k05_completion_trace(ticket, "post_commit_converge_return", None, Some(converged.is_ok())); }
        converged?;
        durable_progress.notify_one();
        if let Some((executor, ticket)) = committed_executor {
            self.complete_callback_observed(
                graph_id,
                &executor,
                &ticket,
                deadline_terminal.then_some("canonical_deadline_after_atomic_commit"),
            )
            .await?;
            if deadline_terminal {
                executor.cancellation_finalized(&ticket);
            }
            self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                .await?;
        } else {
            self.take_bound_active_node(graph_id, &node_id, expected_binding.as_ref())
                .await?;
        }
        Ok(())
    }

    async fn resolve_pump_result(
        &self,
        graph_id: &str,
        expected_node_id: &str,
        result: Result<(String, NodeExecutionOutcome), ExecutionRunnerError>,
        durable_progress: &Notify,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<Option<(String, NodeExecutionOutcome)>, ExecutionRunnerError> {
        match result.map_err(|error| normalize_start_failure(expected_node_id, expected_binding.is_some(), error)) {
            Err(ExecutionRunnerError::CommandSuperseded { node_id }) => {
                let running = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .filter(|active| {
                        Some(active.binding.clone()).as_ref() == expected_binding
                    })
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                if let Some((executor, ticket)) = running {
                    self.active_cancel_observed(graph_id, &executor, &ticket)
                        .await?;
                    self.take_bound_active_node(graph_id, &node_id, expected_binding)
                        .await?;
                }
                Ok(None)
            }
            Err(error @ ExecutionRunnerError::ResourceDeferred { .. }) => Err(error),
            Err(ExecutionRunnerError::DeadlineExceeded {
                node_id,
                deadline_at_ms,
            }) => {
                self.terminalize_deadline_node(
                    graph_id,
                    &node_id,
                    deadline_at_ms,
                    expected_binding,
                )
                .await?;
                self.converge_after_terminal_commit(graph_id).await?;
                durable_progress.notify_one();
                Ok(None)
            }
            Err(ExecutionRunnerError::Resource { node_id, reason }) => {
                self.block_unstarted_resource_node(graph_id, &node_id, reason)
                    .await?;
                self.converge_after_terminal_commit(graph_id).await?;
                durable_progress.notify_one();
                Ok(None)
            }
            Err(error @ ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { .. })) => {
                // No original receipt exists: leave the active owner and its
                // handoff intact for the original cancellation/drain path.
                Err(error)
            }
            Ok(value) => Ok(Some(value)),
            Err(ExecutionRunnerError::Executor(error)) => {
                if matches!(error, NodeExecutorError::Start { .. }) {
                    let node_id = executor_error_node_id(&error).to_string();
                    match self
                        .isolate_node_failure(
                            graph_id,
                            &node_id,
                            expected_binding,
                            error.to_string(),
                        )
                        .await
                    {
                        Err(ExecutionRunnerError::CommandSuperseded { .. }) => return Ok(None),
                        result => result?,
                    }
                    self.converge_after_terminal_commit(graph_id).await?;
                    durable_progress.notify_one();
                    return Ok(None);
                }
                let node_id = executor_error_node_id(&error).to_string();
                if let Some(waiter) = self.command_intent_waiter(graph_id) {
                    waiter.await;
                }
                let result = failed_result(&error);
                let _coordination = self.graph_coordination_for_outcome(graph_id, &node_id, expected_binding).await?;
                let current = self.state_store.load_async(graph_id).await?;
                if !self
                    .active_binding_matches(graph_id, &node_id, expected_binding)
                    .await
                    || expected_binding.is_some_and(|binding| {
                        current.recovery_cursor.node_attempts.get(&node_id)
                            != Some(&binding.attempt)
                    })
                {
                    return Ok(None);
                }
                let transitioned =
                    current.node_statuses.get(&node_id) == Some(&ExecutionNodeStatus::Running);
                if transitioned {
                    let ticket = self
                        .active
                        .lock()
                        .await
                        .get(&(graph_id.to_string(), node_id.clone()))
                        .map(|active| active.ticket.clone())
                        .ok_or_else(|| {
                            ExecutionRunnerError::Driver(
                                "failed Running node lost original ticket".into(),
                            )
                        })?;
                    self.commit_service
                        .transition_execution_outcome_async(
                            current,
                            ticket,
                            NodeExecutionOutcome::new(result),
                            BTreeMap::new(),
                        )
                        .await?;
                }
                drop(_coordination);
                if transitioned {
                    self.converge_after_terminal_commit(graph_id).await?;
                    durable_progress.notify_one();
                }
                let pending = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                if let Some((executor, ticket)) = pending {
                    self.active_cancel_observed(graph_id, &executor, &ticket)
                        .await?;
                    self.complete_callback_observed(
                        graph_id,
                        &executor,
                        &ticket,
                        Some("executor_failure_before_resource_release"),
            )
                    .await?;
                }
                self.take_bound_active_node(graph_id, &node_id, expected_binding)
                    .await?;
                // A Pause/Cancel command may have superseded the executor while
                // it returned. The command's durable graph state remains truth.
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn start_and_execute_node(
        &self,
        graph_id: &str,
        node: harness_contract::execution_graph::ExecutionNodeSpec,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<(String, NodeExecutionOutcome), ExecutionRunnerError> {
        let deadline_at_ms = execution_deadline_at_ms(&node)?;
        if deadline_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
            return Err(ExecutionRunnerError::DeadlineExceeded {
                node_id: node.id,
                deadline_at_ms: deadline_at_ms.unwrap_or_default(),
            });
        }
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        }
        let active = self.active.lock().await;
        let prepared = active.get(&(graph_id.to_string(), node.id.clone()));
        if prepared
            .map(|active| active.binding.clone())
            .as_ref()
            != expected_binding
        {
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        }
        let Some(prepared) = prepared else {
            return Err(ExecutionRunnerError::CommandSuperseded { node_id: node.id });
        };
        let (
            executor,
            ticket,
            resource_kind,
            resource_queue_wait,
            resource_started,
            effect_state,
            no_dispatch,
        ) = (
            Arc::clone(&prepared.executor),
            prepared.ticket.clone(),
            prepared.resource_kind.clone(),
            prepared.resource_queue_wait,
            prepared.resource_started,
            prepared.effect_state.clone(),
            prepared.no_dispatch.clone(),
        );
        drop(active);
        if let Some(waiter) = self.command_intent_waiter(graph_id) {
            waiter.await;
        }
        if !self
            .active_binding_matches(graph_id, &ticket.node_id, expected_binding)
            .await
        {
            return Err(ExecutionRunnerError::CommandSuperseded {
                node_id: ticket.node_id.clone(),
            });
        }
        match effect_state {
            ExecutionEffectState::Completed(_) => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                );
                return Err(NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(),
                    reason: "Completed effect must reconcile its original committed attempt, never enter fresh execution".into(),
                }.into());
            }
            ExecutionEffectState::Uncertain => {
                self.record_resource_terminal(
                    resource_kind.as_ref(),
                    resource_queue_wait,
                    resource_started,
                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                );
                return Err(NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(),
                    reason: format!(
                        "effect intent `{}` has no durable receipt",
                        ticket.idempotency_key
                    ),
                }
                .into());
            }
            ExecutionEffectState::Fresh => {}
        }
        {
            let active = self.active.lock().await;
            let Some(owner) = active.get(&(graph_id.to_string(), ticket.node_id.clone()))
                .filter(|owner| owner.ticket == ticket && Some(&owner.binding) == expected_binding) else {
                return Err(ExecutionRunnerError::CommandSuperseded { node_id: ticket.node_id.clone() });
            };
            if self.command_intent_waiter(graph_id).is_some() {
                return Err(ExecutionRunnerError::CommandSuperseded { node_id: ticket.node_id.clone() });
            }
            owner.cancel_outcome.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).polled = true;
        }
        let mut original_unknown = None;
        let mut commits = self.state_store.subscribe_commits();
        let outcome = if let Some(deadline_at_ms) = deadline_at_ms {
            let remaining = Duration::from_millis(deadline_at_ms.saturating_sub(now_ms()));
            let result = tokio::time::timeout(remaining, poll_with_original_custody(
                self, expected_binding, &executor, &ticket, &mut commits, &self.resource_manager, &mut original_unknown,
            )).await;
            match result {
                Ok(outcome) => outcome,
                Err(_) => {
                    if let Some(error) = original_unknown.take() {
                        let error = drain_original_unknown(self, &executor, &ticket, error).await;
                        return Err(ExecutionRunnerError::Executor(error));
                    }
                    let close_deadline = std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
                    if !self.drain_active_original(graph_id, &executor, &ticket, close_deadline).await? {
                        return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                            node_id:ticket.node_id.clone(), reason:"original outcome committed while the deadline was closing".into(),
                        }));
                    }
                    Ok(deadline_exceeded_outcome(&ticket.node_id, deadline_at_ms))
                }
            }
        } else {
            execute_node_under_ceiling(self, expected_binding, &executor, &ticket, &mut commits, &self.resource_manager).await
        };
        let node_duration_ms = resource_started
            .elapsed()
            .saturating_add(resource_queue_wait)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if let Some(resource_kind) = resource_kind {
            let result_class = match &outcome {
                Ok(outcome)
                    if outcome.result.status
                        == harness_contract::execution_graph::ExecutionNodeStatus::Cancelled =>
                {
                    crate::execution_core::graph::ResourceResultClass::Cancelled
                }
                Ok(outcome)
                    if outcome.result.status
                        == harness_contract::execution_graph::ExecutionNodeStatus::Completed =>
                {
                    crate::execution_core::graph::ResourceResultClass::Completed
                }
                Ok(_) | Err(_) => crate::execution_core::graph::ResourceResultClass::Failed,
            };
            let _ = self.resource_manager.record_observation(
                &resource_kind,
                crate::execution_core::graph::ResourceObservation::terminal(
                    resource_queue_wait,
                    resource_started.elapsed(),
                    result_class,
                ),
            );
        }
        let mut outcome = outcome.map_err(ExecutionRunnerError::from)?;
        if let Some(observation) = no_dispatch {
            observation
                .finalize_duration(&ticket, &mut outcome, node_duration_ms)
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        } else if outcome.no_dispatch.is_some() {
            return Err(NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: "NoDispatch duration has no original active observation".into(),
            }
            .into());
        } else {
            outcome.result.usage.duration_ms = outcome
                .result
                .usage
                .duration_ms
                .max(node_duration_ms.max(1));
        }
        Ok((ticket.node_id.clone(), outcome))
    }

    fn record_resource_terminal(
        &self,
        kind: Option<&ExecutionResourceKind>,
        queue_wait: Duration,
        started: std::time::Instant,
        result_class: crate::execution_core::graph::ResourceResultClass,
    ) {
        let Some(kind) = kind else {
            return;
        };
        let _ = self.resource_manager.record_observation(
            kind,
            crate::execution_core::graph::ResourceObservation::terminal(
                queue_wait,
                started.elapsed(),
                result_class,
            ),
        );
    }

    async fn acquire_node_resources(
        &self,
        graph: &ExecutionGraph,
        node: &harness_contract::execution_graph::ExecutionNodeSpec,
        deadline_at_ms: Option<u64>,
        scope_wait: Option<Duration>,
    ) -> Result<NodeResourceGuards, ExecutionRunnerError> {
        if node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            || (node.kind == harness_contract::execution_graph::ExecutionNodeKind::Verify
                && node.executor_kind == super::executors::CompileTargetGuardExecutor::KIND)
        {
            // ToolBatch is a durable orchestration container. Its tool
            // leaves acquire the
            // authoritative resource and scope leases. Holding a path
            // lock here while synchronously awaiting a descendant that
            // needs the same path creates a parent-waits-child / child-
            // waits-parent deadlock. Keep validation at the container
            // boundary, but defer ownership to the actual effect leaf.
            for scope in &node.resource_scopes {
                if let Some(path) = scope.strip_prefix("read:") {
                    let _ = self.scoped_resource_for_path(&node.id, path, false)?;
                } else if let Some(path) = scope.strip_prefix("write:") {
                    let _ = self.scoped_resource_for_path(&node.id, path, true)?;
                } else if let Some(path) = scope.strip_prefix("worktree:") {
                    validate_worktree_path(&self.workspace_root, path).map_err(|reason| {
                        ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason,
                        }
                    })?;
                }
            }
            return Ok(NodeResourceGuards {
                resource: None,
                scope: None,
                worktree: None,
                original_binding: None,
                admission_pending: false,
                owner_fence: None,
            });
        }
        let resource_kind = match node.kind {
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask => {
                Some(ExecutionResourceKind::Agent)
            }
            harness_contract::execution_graph::ExecutionNodeKind::Materialize => {
                Some(ExecutionResourceKind::Tool)
            }
            // InlineModel owns Provider admission in ConversationRuntime.
            // ToolBatch leaves own Tool admission in ToolExecutionPlane.
            // Deterministic control nodes do not consume either family.
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel
            | harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            | harness_contract::execution_graph::ExecutionNodeKind::Verify
            | harness_contract::execution_graph::ExecutionNodeKind::Synthesize
            | harness_contract::execution_graph::ExecutionNodeKind::Approval
            | harness_contract::execution_graph::ExecutionNodeKind::SessionDispatch
            | harness_contract::execution_graph::ExecutionNodeKind::Timer => None,
        };
        let resource = if let Some(resource_kind) = resource_kind {
            let mut demands = vec![(resource_kind, 1)];
            let mut parent_execution_limit = None;
            if node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                let budget = serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(
                    &node.payload_ref,
                )
                .map(|packet| packet.budget_lease)
                .or_else(|packet_error| {
                    serde_json::from_str::<harness_contract::agent::AgentTaskIntent>(
                        &node.payload_ref,
                    )
                    .map(|intent| intent.budget_lease)
                    .map_err(|intent_error| {
                        format!(
                            "neither canonical AgentTaskPacket ({packet_error}) nor AgentTaskIntent ({intent_error}) carries a valid parent execution budget"
                        )
                    })
                })
                .map_err(|reason| ExecutionRunnerError::Resource {
                    node_id: node.id.clone(),
                    reason,
                })?;
                budget
                    .validate()
                    .map_err(|reason| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: reason.to_string(),
                    })?;
                let parent_kind =
                    ExecutionResourceKind::ParentExecution(budget.parent_budget_id.clone());
                demands.push((parent_kind.clone(), 1));
                parent_execution_limit = Some((parent_kind, budget.parent_budget.max_parallel));
            }
            let mut request = ResourceAdmissionRequest::new(graph.service_class, demands)
                .with_fairness_key(format!("graph:{}", graph.id));
            if let Some(work) = node
                .work
                .as_ref()
                .filter(|work| work.scheduling_priority > 0)
            {
                request = request.with_priority(work.scheduling_priority);
            }
            if let Some((kind, limit)) = parent_execution_limit {
                request = request.with_ephemeral_limit(kind, limit);
            }
            if let Some(deadline_at_ms) = deadline_at_ms {
                request = request.with_deadline_at_ms(deadline_at_ms);
            }
            let decision = if scope_wait == Some(Duration::ZERO) {
                self.resource_manager.try_admit(request).await
            } else {
                self.resource_manager.admit(request).await
            };
            let decision = decision.map_err(|error| ExecutionRunnerError::Resource {
                    node_id: node.id.clone(),
                    reason: error.to_string(),
                })?;
            Some(match decision {
                ResourceAdmissionDecision::Granted { lease, .. } => lease,
                ResourceAdmissionDecision::Overloaded { wait_reason, .. } => {
                    if wait_reason == ResourceWaitReason::DeadlineExpired {
                        return Err(ExecutionRunnerError::DeadlineExceeded {
                            node_id: node.id.clone(),
                            deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                        });
                    }
                    return Err(ExecutionRunnerError::ResourceDeferred {
                        node_id: node.id.clone(),
                        reason: format!("resource admission overloaded: {wait_reason:?}"),
                    });
                }
                ResourceAdmissionDecision::Deferred { wait_reason, .. } => {
                    if wait_reason == ResourceWaitReason::DeadlineExpired {
                        return Err(ExecutionRunnerError::DeadlineExceeded {
                            node_id: node.id.clone(),
                            deadline_at_ms: deadline_at_ms.unwrap_or_default(),
                        });
                    }
                    if scope_wait == Some(Duration::ZERO) && matches!(wait_reason,
                        ResourceWaitReason::Capacity | ResourceWaitReason::ClassFifo | ResourceWaitReason::HigherServiceClass)
                    {
                        return Err(ExecutionRunnerError::ResourceDeferred {
                            node_id: node.id.clone(),
                            reason: format!("resource admission waiting: {wait_reason:?}"),
                        });
                    }
                    return Err(ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: format!("resource admission refused: {wait_reason:?}"),
                    });
                }
            })
        } else {
            None
        };
        let mut scope_requests = Vec::new();
        let mut worktree_path = None;
        for scope in &node.resource_scopes {
            if let Some(path) = scope.strip_prefix("read:") {
                if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                    scope_requests.push(ScopeLockRequest {
                        scope: self.scoped_resource_for_path(&node.id, path, false)?,
                        mode: ScopeLockMode::Read,
                    });
                }
            } else if let Some(path) = scope.strip_prefix("write:") {
                if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                    scope_requests.push(ScopeLockRequest {
                        scope: self.scoped_resource_for_path(&node.id, path, true)?,
                        mode: ScopeLockMode::Write,
                    });
                }
            } else if let Some(path) = scope.strip_prefix("worktree:") {
                worktree_path = Some(validate_worktree_path(&self.workspace_root, path).map_err(
                    |reason| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason,
                    },
                )?);
            }
        }
        // AgentTask 是委派/授权边界，不是文件副作用边界。其 in-process
        // 子代理会在同一个 Runner 中创建真正执行 read/write 的 ToolBatch。
        // 如果父 AgentTask 在等待子图期间持有文件锁，子 ToolBatch 对同一路径
        // 的合法读写会永久等待父节点结束，形成父等子、子等父的自死锁。
        // 文件互斥仍由叶子效果节点获取，AgentTask 继续持有 Agent 配额和
        // 可选 worktree 租约，授权范围也仍保留在节点合同中。
        let scope = if scope_requests.is_empty() {
            None
        } else {
            Some(
                match self.scope_locks.acquire(scope_requests, scope_wait).await {
                    Ok(lease) => lease,
                    Err(ScopeLockError::TimedOut { .. }) if scope_wait.is_some() => {
                        return Err(ExecutionRunnerError::ResourceDeferred {
                            node_id: node.id.clone(),
                            reason: "scope lock is currently owned by another effect leaf"
                                .to_string(),
                        });
                    }
                    Err(error) => {
                        return Err(ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason: error.to_string(),
                        });
                    }
                },
            )
        };
        let worktree = worktree_path
            .map(|path| {
                self.worktree_leases.acquire(WorktreeLeaseRequest {
                    workspace_id: self.workspace_id.clone(),
                    task_id: graph.id.clone(),
                    owner_id: node.id.clone(),
                    path,
                    ownership: WorktreeOwnership::UserManaged,
                    ttl: Duration::from_secs(300),
                })
            })
            .transpose()
            .map_err(|error| ExecutionRunnerError::Resource {
                node_id: node.id.clone(),
                reason: error.to_string(),
            })?;
        Ok(NodeResourceGuards {
            resource,
            scope,
            worktree,
            original_binding: None,
            admission_pending: false,
            owner_fence: None,
        })
    }

    async fn terminalize_deadline_node(
        &self,
        graph_id: &str,
        node_id: &str,
        deadline_at_ms: u64,
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<(), ExecutionRunnerError> {
        let running = self
            .active
            .lock()
            .await
            .get(&(graph_id.to_string(), node_id.to_string()))
            .filter(|active| {
                Some(active.binding.clone()).as_ref() == expected_binding
            })
            .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
        if let Some((executor, ticket)) = running.as_ref() {
            self.active_cancel_observed(graph_id, &executor, ticket)
                .await?;
        }
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if !self
            .active_binding_matches(graph_id, node_id, expected_binding)
            .await
            || expected_binding.is_some_and(|binding| {
                current.recovery_cursor.node_attempts.get(node_id) != Some(&binding.attempt)
            })
        {
            return Ok(());
        }
        if !matches!(
            current.node_statuses.get(node_id),
            Some(ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running)
        ) {
            return Ok(());
        }
        let outcome = deadline_exceeded_outcome(node_id, deadline_at_ms);
        if current.node_statuses.get(node_id) == Some(&ExecutionNodeStatus::Running) {
            let ticket = running
                .as_ref()
                .map(|(_, ticket)| ticket.clone())
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver(
                        "deadline Running node lost original ticket".into(),
                    )
                })?;
            self.commit_service
                .transition_execution_outcome_async(current, ticket, outcome, BTreeMap::new())
                .await?;
        } else {
            self.commit_service
                .transition_node_async(
                    current,
                    node_id.to_string(),
                    outcome.result.status,
                    Some(outcome.result),
                    Vec::new(),
                )
                .await?;
        }
        drop(_coordination);
        if let Some(finished) = self
            .take_bound_active_node(graph_id, node_id, expected_binding)
            .await?
        {
            finished.executor.cancellation_finalized(&finished.ticket);
        }
        Ok(())
    }

    fn scoped_resource_for_path(
        &self,
        node_id: &str,
        path: &str,
        write: bool,
    ) -> Result<ScopedResource, ExecutionRunnerError> {
        let requested = Path::new(path.trim());
        let relative = if requested.is_absolute() {
            requested.strip_prefix(&self.workspace_root).map_err(|_| {
                ExecutionRunnerError::Resource {
                    node_id: node_id.to_string(),
                    reason: format!(
                        "absolute resource scope `{}` is outside workspace `{}`",
                        requested.display(),
                        self.workspace_root.display()
                    ),
                }
            })?
        } else {
            requested
        };
        if relative.as_os_str().is_empty() || relative == Path::new(".") {
            return ScopedResource::workspace(self.path_identity_resolver.workspace_id()).map_err(
                |error| ExecutionRunnerError::Resource {
                    node_id: node_id.to_string(),
                    reason: error.to_string(),
                },
            );
        }
        let rendered = relative.to_string_lossy();
        let identity = if write {
            self.path_identity_resolver.resolve_planned_file(&rendered)
        } else {
            self.path_identity_resolver.resolve_existing(&rendered)
        }
        .map_err(|error| ExecutionRunnerError::Resource {
            node_id: node_id.to_string(),
            reason: error.to_string(),
        })?;
        Ok(ScopedResource::workspace_object(identity))
    }

    async fn block_unstarted_resource_node(
        &self,
        graph_id: &str,
        node_id: &str,
        reason: String,
    ) -> Result<(), ExecutionRunnerError> {
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if current.node_statuses.get(node_id) != Some(&ExecutionNodeStatus::Ready) {
            return Ok(());
        }
        self.commit_service
            .transition_node_async(
                current,
                node_id.to_string(),
                ExecutionNodeStatus::Blocked,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Blocked,
                    result_ref: None,
                    wait_class: None,
                    summary: None,
                    evidence_refs: Vec::new(),
                    failure: Some(harness_contract::execution_graph::ExecutionFailure {
                        kind: "resource_acquisition_failed".to_string(),
                        message: reason,
                        retryable: false,
                        evidence_refs: Vec::new(),
                    }),
                    usage: Default::default(),
                    finished_at_ms: now_ms(),
                }),
                Vec::new(),
            )
            .await?;
        Ok(())
    }

    /// Record a node-level failure without terminating unrelated ready nodes.
    ///
    /// Start errors and panic recovery must not propagate to the completion
    /// pump as graph-level failures: the node is the only thing that failed.
    pub(crate) async fn isolate_node_failure(
        &self,
        graph_id: &str,
        node_id: &str,
        expected_binding: Option<&ExecutionNodeBinding>,
        reason: String,
    ) -> Result<(), ExecutionRunnerError> {
        let key = (graph_id.to_string(), node_id.to_string());
        let running = self.active.lock().await.get(&key).map(|active| {
            (
                Arc::clone(&active.executor),
                active.ticket.clone(),
                active.binding.clone(),
            )
        });
        if running.as_ref().map(|(_, _, binding)| binding) != expected_binding {
            return Err(ExecutionRunnerError::CommandSuperseded {
                node_id: node_id.to_owned(),
            });
        }
        if let Some((executor, ticket, _)) = running.as_ref() {
            self.active_cancel_observed(graph_id, &executor, ticket)
                .await?;
            self.complete_callback_observed(
                graph_id,
                &executor,
                ticket,
                Some("node_execution_isolated_failure"),
            )
            .await?;
        }
        let _coordination = self.graph_coordination_without_command(graph_id).await;
        let current = self.state_store.load_async(graph_id).await?;
        if let Some(binding) = expected_binding {
            if current.recovery_cursor.node_attempts.get(node_id) != Some(&binding.attempt)
                || !self
                    .active
                    .lock()
                    .await
                    .get(&key)
                    .is_some_and(|active| active.binding.clone() == *binding)
            {
                return Err(ExecutionRunnerError::CommandSuperseded {
                    node_id: node_id.to_owned(),
                });
            }
        }
        let status = current.node_statuses.get(node_id).copied();
        if !matches!(
            status,
            Some(
                ExecutionNodeStatus::Ready
                    | ExecutionNodeStatus::Running
                    | ExecutionNodeStatus::Planned
            )
        ) {
            drop(_coordination);
            self.release_isolated_node(&key, expected_binding).await?;
            return Ok(());
        }
        // A failed start, executor panic, or node-local failure is a failure of
        // that node, not a safety boundary. It is terminal (`Failed` has no
        // outgoing transition), so it must not advertise retryability
        // (PLAN C3-P-g / I5). Recovery/replan act on the isolated node instead
        // of a `retryable` flag the graph state machine does not honour.
        let terminal_status = ExecutionNodeStatus::Failed;
        let result = ExecutionNodeResult {
            status: terminal_status,
            result_ref: None,
            wait_class: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: Some(harness_contract::execution_graph::ExecutionFailure {
                kind: "node_execution_isolated_failure".to_string(),
                message: reason,
                retryable: false,
                evidence_refs: Vec::new(),
            }),
            usage: Default::default(),
            finished_at_ms: now_ms(),
        };
        if status == Some(ExecutionNodeStatus::Running) {
            let ticket = running
                .as_ref()
                .map(|(_, ticket, _)| ticket.clone())
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver(
                        "isolated Running node lost original ticket".into(),
                    )
                })?;
            self.commit_service
                .transition_execution_outcome_async(
                    current,
                    ticket,
                    NodeExecutionOutcome::new(result),
                    BTreeMap::new(),
                )
                .await?;
        } else {
            self.commit_service
                .transition_node_async(
                    current,
                    node_id.to_string(),
                    terminal_status,
                    Some(result),
                    Vec::new(),
                )
                .await?;
        }
        drop(_coordination);
        self.release_isolated_node(&key, expected_binding).await?;
        Ok(())
    }

    async fn release_isolated_node(
        &self,
        key: &(String, String),
        expected_binding: Option<&ExecutionNodeBinding>,
    ) -> Result<(), ExecutionRunnerError> {
        if let Some(finished) = self
            .take_bound_active_node(&key.0, &key.1, expected_binding)
            .await?
        {
            self.record_resource_terminal(
                finished.resource_kind.as_ref(),
                finished.resource_queue_wait,
                finished.resource_started,
                crate::execution_core::graph::ResourceResultClass::Failed,
            );
            finished.executor.cancellation_finalized(&finished.ticket);
        }
        Ok(())
    }

    pub(crate) async fn command(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        if matches!(
            command,
            ExecutionGraphCommand::Pause { .. }
                | ExecutionGraphCommand::Cancel { .. }
                | ExecutionGraphCommand::CancelNode { .. }
        ) {
            // Publish command intent before waiting for graph coordination so
            // a just-admitted execution wave cannot begin polling in the gap
            // between its Running commit and this command acquiring the lock.
            let intent = CommandIntentOwner::install(graph_id, Arc::clone(&self.command_intents))?;
            let coordination = self.graph_coordination(graph_id).await;
            let graph = self.state_store.load_async(graph_id).await?;
            self.commit_service
                .validate_command_revision(&graph, &command)?;
            let active = self
                .active
                .lock()
                .await
                .iter()
                .filter(|((active_graph_id, node_id), _)| {
                    active_graph_id == graph_id
                        && match &command {
                            ExecutionGraphCommand::CancelNode {
                                node_id: target, ..
                            } => node_id == target,
                            _ => true,
                        }
                })
                .map(|((_, node_id), node)| {
                    (
                        node_id.clone(),
                        Arc::clone(&node.executor),
                        node.ticket.clone(),
                    )
                })
                .collect::<Vec<_>>();
            if matches!(command, ExecutionGraphCommand::Pause { .. })
                && active
                    .iter()
                    .any(|(_, executor, _)| !executor.supports_resumable_pause())
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "an active execution node does not expose a resumable pause capability; cancel or wait for quiescence"
                        .to_string(),
                )
                .into());
            }
            let handoffs = if matches!(command, ExecutionGraphCommand::Cancel { .. } | ExecutionGraphCommand::CancelNode { .. }) {
                let table = self.active.lock().await;
                active.iter().filter_map(|(node_id, _, ticket)| {
                    table.get(&(graph_id.to_string(), node_id.clone()))
                        .filter(|owner| owner.ticket == *ticket)
                        .map(|owner| (node_id.clone(), ticket.clone(), owner.binding.clone(), Arc::clone(&owner.cancel_outcome), owner.effect_receipt_required))
                }).collect::<Vec<_>>()
            } else { Vec::new() };
            let cancellation_finalizer = CancellationFinalizationOwner::new(active);
            drop(coordination);

            // Executor cancellation can wait for a nested child graph to
            // quiesce. It must never run while the global graph coordination
            // gate is held, because that child uses this same Runner.
            if matches!(
                command,
                ExecutionGraphCommand::Pause { .. }
                    | ExecutionGraphCommand::Cancel { .. }
                    | ExecutionGraphCommand::CancelNode { .. }
            ) {
                let results = futures::future::join_all(cancellation_finalizer.active().iter().map(
                    |(_, executor, ticket)| self.active_cancel_observed(graph_id, executor, ticket)
                )).await;
                if let Some(error) = results.into_iter().find_map(Result::err) {
                    return Err(error);
                }
            }

            let outcomes = futures::future::join_all(handoffs.into_iter().map(|(node_id, ticket, binding, handoff, effect_receipt_required)| async move {
                let deadline = handoff.state.lock().map_err(|_| ExecutionRunnerError::Executor(
                    NodeExecutorError::OriginalCustodyUnknown { node_id: node_id.clone(),
                        reason: "original cancellation handoff unavailable".into() }))?
                    .closing_deadline.map(tokio::time::Instant::from_std)
                    .unwrap_or_else(|| tokio::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET);
                let outcome = loop {
                    let ready = handoff.ready.notified();
                    let snapshot = {
                        let state = handoff.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        (state.polled, state.finished, state.outcome.clone())
                    };
                    if !snapshot.0 { break None; }
                    if let Some(Err(error)) = &snapshot.2 {
                        return Err(original_shared_error(error));
                    }
                    if snapshot.1 {
                        let Some(Ok(outcome)) = snapshot.2 else {
                            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                                node_id: node_id.clone(), reason: "original cancelled task finished without an outcome".into(),
                            }));
                        };
                        // The original owner may already have committed this
                        // complete outcome, including its replan. Authenticate
                        // both the binding result and the full same-ticket
                        // receipt before leaving it out of the Cancel batch.
                        if outcome.replan.is_some() && effect_receipt_required {
                            let graph = self.state_store.load_async(graph_id).await?;
                            let committed = self.commit_service.committed_node_attempt(&graph, &node_id, &binding)?;
                            let exact_receipt = self.commit_service.inspect_execution_effect(&ticket)?;
                            if committed.is_some_and(|(result, _)| result == outcome.result)
                                && matches!(exact_receipt, ExecutionEffectState::Completed(ref original) if original == &outcome) {
                                break None;
                            }
                        }
                        // R02 collect-and-close: a finished replan outcome is
                        // stopped from further dispatch, its occurred facts are
                        // aggregated, and the node closes with a named terminal
                        // state carrying that evidence in the same command
                        // transaction. Only transitions the original owner must
                        // commit atomically (delivery envelope, terminal
                        // presentation, NoDispatch with receipt) or a
                        // non-terminal result keep this command unknown.
                        if matches!(outcome.result.status, ExecutionNodeStatus::Cancelled | ExecutionNodeStatus::Completed)
                            && outcome.replan.is_some()
                            && outcome.delivery_envelope.is_none()
                            && outcome.terminal_presentation.is_none() {
                            break Some(collect_cancelled_outcome(&outcome));
                        }
                        // A completed operation remains completed. An error,
                        // nonterminal result or outcome requiring another
                        // domain transition remains unknown to this command.
                        if !matches!(outcome.result.status, ExecutionNodeStatus::Cancelled | ExecutionNodeStatus::Completed)
                            || outcome.replan.is_some() || outcome.delivery_envelope.is_some()
                            || outcome.terminal_presentation.is_some()
                            || (outcome.no_dispatch.is_some() && effect_receipt_required) {
                            return Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
                                node_id: node_id.clone(), reason: "original cancelled task outcome requires its original owner".into(),
                            }));
                        }
                        break Some(outcome);
                    }
                    if tokio::time::timeout_at(deadline, ready).await.is_err() {
                        return Err(ExecutionRunnerError::Driver("original cancelled executor outcome remains pending".into()));
                    }
                };
                Ok::<_, ExecutionRunnerError>(outcome.map(|outcome| ExecutionTerminalCommit {
                    node_id, outcome, effect_ticket:Some(ticket),
                }))
            })).await;
            let cancelled_outcomes = outcomes.into_iter().collect::<Result<Vec<_>, _>>()?
                .into_iter().flatten().collect();
            let coordination = self.graph_coordination(graph_id).await;
            let current = self.state_store.load_async(graph_id).await?;
            // The intent gate prevents node transitions while cancellation is
            // in flight. Re-validating the original caller revision here is
            // the CAS that prevents a delayed command from overwriting a
            // concurrently committed graph mutation.
            if let Err(error) = self
                .commit_service
                .validate_command_revision(&current, &command)
            {
                drop(coordination);
                return Err(error.into());
            }
            let result = self
                .commit_service
                .apply_command_with_cancelled_outcomes_async(current, command.clone(), cancelled_outcomes)
                .await;
            drop(coordination);
            if result.is_ok() {
                let node_ids = cancellation_finalizer
                    .active()
                    .iter()
                    .map(|(node_id, _, _)| node_id.clone())
                    .collect::<Vec<_>>();
                for node_id in node_ids {
                    let binding = self
                        .active
                        .lock()
                        .await
                        .get(&(graph_id.to_string(), node_id.clone()))
                        .map(|active| active.binding.clone());
                    self.take_bound_active_node(graph_id, &node_id, binding.as_ref())
                        .await?;
                }
            }
            drop(intent);
            drop(cancellation_finalizer);
            result?;
            return self
                .state_store
                .load_async(graph_id)
                .await
                .map_err(Into::into);
        }

        let coordination = self.graph_coordination_without_command(graph_id).await;
        let graph = self.state_store.load_async(graph_id).await?;
        self.commit_service
            .validate_command_revision(&graph, &command)?;
        let graph = self
            .commit_service
            .apply_command_async(graph, command.clone())
            .await?
            .graph;
        drop(coordination);
        Ok(graph)
    }

    pub(crate) async fn projection(
        &self,
        graph_id: &str,
    ) -> Result<harness_contract::execution_graph::ExecutionGraphProjection, ExecutionRunnerError>
    {
        self.state_store
            .projection_async(graph_id)
            .await
            .map_err(ExecutionRunnerError::from)
    }

    fn published_dependency_predecessors<'graph>(
        &self,
        graph: &'graph ExecutionGraph,
        node: &'graph harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<Vec<DependencyPredecessor<'graph>>, ExecutionRunnerError> {
        let mut predecessors = dependency_predecessors(graph, node);
        let edges = graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency() && edge.to == node.id);
        for (predecessor, edge) in predecessors.iter_mut().zip(edges) {
            let attempt = graph
                .recovery_cursor
                .node_attempts
                .get(&edge.from)
                .copied()
                .unwrap_or(0);
            if !predecessor.status.is_terminal() || attempt == 0 {
                continue;
            }
            let binding = self
                .state_store
                .current_binding(graph, &edge.from, attempt)?;
            if !ExecutionBindingLeaseClosure::required(&binding) {
                continue;
            }
            let start = self
                .state_store
                .binding_start_position(graph, &edge.from, &binding)?
                .ok_or_else(|| {
                    ExecutionRunnerError::Driver("dependency original binding start missing".into())
                })?;
            if !self
                .state_store
                .binding_lease_closure(graph, &edge.from, &binding, start)?
            {
                predecessor.status = ExecutionNodeStatus::Running;
                predecessor.result = None;
            }
        }
        Ok(predecessors)
    }

    fn dependency_publications_ready(
        &self,
        graph: &ExecutionGraph,
        node: &harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<bool, ExecutionRunnerError> {
        let predecessors = self.published_dependency_predecessors(graph, node)?;
        let policy = node
            .work
            .as_ref()
            .map(|work| work.dependency.clone())
            .unwrap_or_default();
        Ok(dependency_target(&policy, &predecessors) == Some(ExecutionNodeStatus::Ready))
    }

    async fn advance_dependencies(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        let graph_id = graph.id;
        let coordination = self.graph_coordination_without_command(&graph_id).await;
        let mut graph = self.state_store.load_async(&graph_id).await?;
        loop {
            let mut changed = false;
            let planned = graph
                .nodes
                .iter()
                .filter(|node| graph.node_statuses[&node.id] == ExecutionNodeStatus::Planned)
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            for node_id in planned {
                let node = graph.nodes.iter().find(|node| node.id == node_id);
                let predecessors = match node {
                    Some(node) => self.published_dependency_predecessors(&graph, node)?,
                    None => Vec::new(),
                };
                let dependency = node
                    .and_then(|node| node.work.as_ref())
                    .map(|work| work.dependency.clone())
                    .unwrap_or_default();
                let target = dependency_target(&dependency, &predecessors);
                if let Some(target) = target {
                    graph = self
                        .commit_service
                        .transition_node_async(graph, node_id, target, None, Vec::new())
                        .await?
                        .graph;
                    changed = true;
                }
            }
            if !changed {
                drop(coordination);
                return Ok(graph);
            }
        }
    }
}

/// One predecessor lane carrying both its durable lifecycle status and its
/// committed terminal result. `EvidenceReady` consumes the result facts;
/// every other policy only needs the lifecycle status.
#[derive(Debug, Clone, Copy)]
struct DependencyPredecessor<'a> {
    status: ExecutionNodeStatus,
    result: Option<&'a ExecutionNodeResult>,
}

impl DependencyPredecessor<'_> {
    #[cfg(test)]
    fn status_only(status: ExecutionNodeStatus) -> Self {
        Self {
            status,
            result: None,
        }
    }
}

/// Build the exact `DependsOn` predecessor lanes for one node. The same
/// helper feeds `current_report`, `advance_dependencies` and every recovery
/// pump, so a lost notify is repaired by the next durable repump instead of
/// by a poll loop.
fn dependency_predecessors<'a>(
    graph: &'a ExecutionGraph,
    node: &'a harness_contract::execution_graph::ExecutionNodeSpec,
) -> Vec<DependencyPredecessor<'a>> {
    let required_evidence_refs = node
        .work
        .as_ref()
        .map(|work| work.required_evidence_refs.as_slice())
        .unwrap_or_default();
    graph
        .edges
        .iter()
        .filter(|edge| edge.kind.is_dependency() && edge.to == node.id)
        .map(|edge| {
            let status = verified_predecessor_status(graph, &edge.from, required_evidence_refs);
            DependencyPredecessor {
                status,
                result: graph.node_results.get(&edge.from),
            }
        })
        .collect()
}

fn dependency_target(
    policy: &harness_contract::execution_graph::ExecutionDependencyPolicy,
    predecessors: &[DependencyPredecessor<'_>],
) -> Option<ExecutionNodeStatus> {
    use harness_contract::execution_graph::ExecutionDependencyPolicy;

    if matches!(policy, ExecutionDependencyPolicy::Finally) {
        return predecessors
            .iter()
            .all(|predecessor| predecessor.status.is_terminal())
            .then_some(ExecutionNodeStatus::Ready);
    }

    if let ExecutionDependencyPolicy::EvidenceReady { predicate, .. } = policy {
        return evidence_ready_target(predicate, predecessors);
    }

    let completed = predecessors
        .iter()
        .filter(|predecessor| predecessor.status == ExecutionNodeStatus::Completed)
        .count();
    let possible = completed
        + predecessors
            .iter()
            .filter(|predecessor| !predecessor.status.is_terminal())
            .count();
    let required = match policy {
        ExecutionDependencyPolicy::All | ExecutionDependencyPolicy::Finally => predecessors.len(),
        ExecutionDependencyPolicy::Any { .. } => 1,
        ExecutionDependencyPolicy::Quorum { minimum, .. } => usize::from(*minimum),
        ExecutionDependencyPolicy::EvidenceReady { predicate, .. } => {
            return evidence_ready_target(predicate, predecessors);
        }
    };
    if completed >= required {
        Some(ExecutionNodeStatus::Ready)
    } else if possible < required {
        Some(ExecutionNodeStatus::Blocked)
    } else {
        None
    }
}

/// Ready, wait or permanently block one `EvidenceReady` edge.
///
/// The predicate consumes only Runtime-attested terminal facts: lifecycle
/// status, committed result facts and the acceptance verdict derived from
/// the persisted `required_acceptance`/`observed_acceptance` pair. It never
/// consults the filesystem, never re-runs an evaluator, and never rewrites a
/// predecessor's terminal status.
fn evidence_ready_target(
    predicate: &harness_contract::execution_graph::DependencyPredicate,
    predecessors: &[DependencyPredecessor<'_>],
) -> Option<ExecutionNodeStatus> {
    let harness_contract::execution_graph::DependencyPredicate::EvidenceReady {
        minimum,
        required_fact_kinds,
        accepted_execution_statuses,
        accepted_acceptance_verdicts,
        require_committed_effect,
    } = predicate;
    let required = usize::from(*minimum);
    if required == 0 {
        return Some(ExecutionNodeStatus::Ready);
    }
    let mut satisfied = 0usize;
    let mut waiting = 0usize;
    for predecessor in predecessors {
        match evidence_ready_satisfaction(
            predecessor,
            required_fact_kinds,
            accepted_execution_statuses,
            accepted_acceptance_verdicts,
            *require_committed_effect,
        ) {
            EvidenceReadyOutcome::Satisfied => satisfied += 1,
            EvidenceReadyOutcome::Waiting => waiting += 1,
            EvidenceReadyOutcome::Blocked => {}
        }
    }
    if satisfied >= required {
        Some(ExecutionNodeStatus::Ready)
    } else if satisfied + waiting >= required {
        None
    } else {
        Some(ExecutionNodeStatus::Blocked)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvidenceReadyOutcome {
    Satisfied,
    Waiting,
    Blocked,
}

fn evidence_ready_satisfaction(
    predecessor: &DependencyPredecessor<'_>,
    required_fact_kinds: &[TerminalFactKind],
    accepted_execution_statuses: &[ExecutionNodeStatus],
    accepted_acceptance_verdicts: &[AcceptanceVerdict],
    require_committed_effect: bool,
) -> EvidenceReadyOutcome {
    let status = predecessor.status;
    if !status.is_terminal() {
        return EvidenceReadyOutcome::Waiting;
    }
    // A terminal predecessor without a committed result can never acquire
    // facts. It is a permanent blocker, never a waiting lane.
    let Some(result) = predecessor.result else {
        return EvidenceReadyOutcome::Blocked;
    };
    if !accepted_execution_statuses.contains(&status) {
        return EvidenceReadyOutcome::Blocked;
    }
    let verdict = derived_acceptance_verdict(result);
    if !accepted_acceptance_verdicts.contains(&verdict) {
        return EvidenceReadyOutcome::Blocked;
    }
    if require_committed_effect
        && !has_terminal_fact_kind(result, TerminalFactKind::CommittedEffect)
    {
        return EvidenceReadyOutcome::Blocked;
    }
    if !required_fact_kinds
        .iter()
        .all(|kind| has_terminal_fact_kind(result, *kind))
    {
        return EvidenceReadyOutcome::Blocked;
    }
    EvidenceReadyOutcome::Satisfied
}

/// Read the single terminal acceptance verdict from persisted node-result
/// facts.  The evaluator owns matching and digest construction at the
/// terminal producer; Runner only consumes that immutable result.
fn derived_acceptance_verdict(result: &ExecutionNodeResult) -> AcceptanceVerdict {
    // The Runtime contract-rejection terminal is the canonical
    // FrameworkInvalid marker: it preserves committed facts while reporting
    // that the Runtime acceptance/evaluation machinery could not validate
    // the result. Business failures never carry this marker.
    let framework_invalid = result.failure.as_ref().is_some_and(|failure| {
        failure.kind == "agent_backend"
            && failure
                .message
                .contains("Runtime rejected Agent terminal result")
    });
    if framework_invalid {
        return AcceptanceVerdict::FrameworkInvalid;
    }
    result
        .usage
        .acceptance_evaluation
        .as_ref()
        .filter(|evaluation| {
            evaluation.evaluator_revision
                == crate::acceptance_evaluator::AcceptanceEvaluator::REVISION
        })
        .map_or(AcceptanceVerdict::Unresolved, |evaluation| {
            evaluation.verdict
        })
}

fn has_terminal_fact_kind(result: &ExecutionNodeResult, kind: TerminalFactKind) -> bool {
    match kind {
        TerminalFactKind::CommittedEffect => {
            result
                .usage
                .observed_acceptance
                .observed_evidence
                .iter()
                .any(|evidence| evidence.workspace_prior_state.is_some())
                || result
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.evidence_ref.ref_type == "runtime_change")
        }
        TerminalFactKind::ObservedEvidence => {
            !result
                .usage
                .observed_acceptance
                .observed_evidence
                .is_empty()
                || result
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.is_durable())
        }
        TerminalFactKind::Artifact => result.evidence_refs.iter().any(|reference| {
            reference.evidence_ref.ref_type == "artifact" || reference.is_durable()
        }),
        TerminalFactKind::AcceptanceVerdict => {
            derived_acceptance_verdict(result) != AcceptanceVerdict::Unresolved
        }
    }
}

/// Bounded ceiling for a node that carries no durable deadline of its own.
///
/// Only `AgentTask` nodes read an absolute deadline from their payload, so every
/// other kind — `inline_model`, `tool_batch`, `synthesize`, `model` — used to be
/// awaited without any bound at all. That is not a theoretical gap: a paid run
/// lost ten minutes to one blocked `inline_model` node and ended in a forced
/// abort at shutdown, because nothing in the Runtime would ever stop it.
///
/// The ceiling is deliberately far above any real step (a root model step in that
/// same run took 23 seconds, a delegated Agent minutes), so it never truncates
/// work that is genuinely progressing. It only converts "hangs forever" into
/// "cancelled with a summary".
fn node_execution_ceiling() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("COWD_NODE_EXECUTION_CEILING_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1_800),
    )
}

/// Await a node that carries no durable deadline of its own, under a bounded
/// ceiling. A node that is genuinely working is untouched; a node whose executor
/// stopped making progress is cancelled with an explicit summary instead of
/// hanging the turn forever.
async fn execute_node_under_ceiling(
    runner: &ExecutionGraphRunner,
    expected_binding: Option<&ExecutionNodeBinding>,
    executor: &Arc<dyn NodeExecutor>,
    ticket: &NodeExecutionTicket,
    commits: &mut tokio::sync::watch::Receiver<u64>,
    resources: &ExecutionResourceManager,
) -> Result<NodeExecutionOutcome, NodeExecutorError> {
    let ceiling = node_execution_ceiling();
    let started = std::time::Instant::now();
    let ceiling_ms = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
    // Capture the deadline once. Re-reading an environment-backed ceiling after
    // expiry could report a deadline different from the one that actually
    // governed this attempt.
    let deadline_at_ms = now_ms().saturating_add(ceiling_ms);
    tracing::debug!(
        target: "cowd::graph_runner",
        graph_id = ticket.graph_id,
        node_id = ticket.node_id,
        executor_kind = ticket.executor_kind,
        attempt = ticket.attempt,
        ceiling_ms,
        "node execution entered bounded ceiling"
    );
    let mut original_unknown = None;
    match tokio::time::timeout(ceiling, poll_with_original_custody(
        runner, expected_binding, executor, ticket, commits, resources, &mut original_unknown,
    )).await {
        Ok(outcome) => outcome,
        Err(_) => {
            if let Some(error) = original_unknown {
                return Err(drain_original_unknown(runner, executor, ticket, error).await);
            }
            tracing::warn!(
                graph_id = ticket.graph_id,
                node_id = ticket.node_id,
                executor_kind = ticket.executor_kind,
                attempt = ticket.attempt,
                ceiling_ms,
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "node execution exceeded its bounded ceiling; cancelling with a summary"
            );
            let deadline = std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
            match runner.drain_active_original(&ticket.graph_id, executor, ticket, deadline).await {
                Ok(true) => Ok(deadline_exceeded_outcome(&ticket.node_id, deadline_at_ms)),
                Ok(false) => Err(NodeExecutorError::OriginalCustodyUnknown { node_id:ticket.node_id.clone(),
                    reason:"original outcome committed while the deadline was closing".into() }),
                Err(error) => Err(NodeExecutorError::OriginalCustodyUnknown { node_id:ticket.node_id.clone(),
                    reason:format!("original deadline drain unconfirmed: {error}") }),
            }
        }
    }
}

async fn drain_original_unknown(
    runner: &ExecutionGraphRunner, executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
    original: NodeExecutorError,
) -> NodeExecutorError {
    let deadline = std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET;
    match runner.drain_active_original(&ticket.graph_id, executor, ticket, deadline).await {
        Ok(true) => original,
        Ok(false) => NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: format!("{original}; original commit requires callback reconciliation"),
        },
        Err(error) => NodeExecutorError::OriginalCustodyUnknown {
            node_id: ticket.node_id.clone(), reason: format!("{original}; original cancellation drain unconfirmed: {error}"),
        },
    }
}

async fn poll_with_original_custody(
    runner: &ExecutionGraphRunner, expected_binding: Option<&ExecutionNodeBinding>,
    executor: &Arc<dyn NodeExecutor>, ticket: &NodeExecutionTicket,
    commits: &mut tokio::sync::watch::Receiver<u64>, resources: &ExecutionResourceManager,
    original_unknown: &mut Option<NodeExecutorError>,
) -> Result<NodeExecutionOutcome, NodeExecutorError> {
    match executor.poll_or_await(ticket).await {
        Err(error @ NodeExecutorError::OriginalCustodyUnknown { .. }) => *original_unknown = Some(error),
        result => return result,
    }
    loop {
        if runner.command_intent_waiter(&ticket.graph_id).is_some()
            || !runner.active_binding_matches(&ticket.graph_id, &ticket.node_id, expected_binding).await
        {
            return Err(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: "original custody handed to cancellation or replaced binding".into(),
            });
        }
        match executor.reconcile_original_custody(ticket).await {
            Ok(Some(outcome)) => return Ok(outcome),
            Ok(None) => {},
            Err(error) => *original_unknown = Some(NodeExecutorError::OriginalCustodyUnknown {
                node_id: ticket.node_id.clone(), reason: format!("original custody reconciliation unavailable: {error}"),
            }),
        }
        tokio::select! {
            changed = commits.changed() => { if changed.is_err() { resources.wait_for_change().await; } },
            () = resources.wait_for_change() => {},
        }
    }
}

async fn run_after_commit_observed(
    executor: &Arc<dyn NodeExecutor>,
    ticket: &NodeExecutionTicket,
) -> Result<(), NodeExecutorError> {
    let started = std::time::Instant::now();
    #[cfg(feature = "test-fixtures")]
    let k05_trace = k05_completion_trace_enabled(ticket);
    #[cfg(feature = "test-fixtures")]
    if k05_trace { k05_completion_trace(ticket, "after_commit_enter", None, None); }
    tracing::info!(
        target: "cowd::graph_runner",
        graph_id = ticket.graph_id,
        node_id = ticket.node_id,
        executor_kind = ticket.executor_kind,
        attempt = ticket.attempt,
        phase = "after_commit_started",
        "node post-commit hook entered"
    );
    let result = executor.after_commit(ticket).await;
    #[cfg(feature = "test-fixtures")]
    if k05_trace { k05_completion_trace(ticket, "after_commit_return", None, Some(result.is_ok())); }
    tracing::info!(
        target: "cowd::graph_runner",
        graph_id = ticket.graph_id,
        node_id = ticket.node_id,
        executor_kind = ticket.executor_kind,
        attempt = ticket.attempt,
        phase = "after_commit_finished",
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        failed = result.is_err(),
        "node post-commit hook finished"
    );
    result
}

fn execution_deadline_at_ms(
    node: &harness_contract::execution_graph::ExecutionNodeSpec,
) -> Result<Option<u64>, ExecutionRunnerError> {
    if !matches!(
        node.kind,
        harness_contract::execution_graph::ExecutionNodeKind::AgentTask
    ) {
        return Ok(None);
    }
    let payload =
        serde_json::from_str::<serde_json::Value>(&node.payload_ref).map_err(|error| {
            ExecutionRunnerError::Resource {
                node_id: node.id.clone(),
                reason: format!("durable execution payload is invalid: {error}"),
            }
        })?;
    let deadline_at_ms = payload
        .get("deadline_at_ms")
        .and_then(serde_json::Value::as_u64)
        .filter(|deadline| *deadline != 0)
        .ok_or_else(|| ExecutionRunnerError::Resource {
            node_id: node.id.clone(),
            reason: "AgentTask has no Runtime-issued absolute deadline".to_string(),
        })?;
    // `u64::MAX` is the explicit no-wall-deadline sentinel used by durable
    // Agent packets. Do not convert it into an enormous Tokio timer: Runtime
    // still governs cancellation, resource admission and rolling claims.
    if deadline_at_ms == u64::MAX {
        return Ok(None);
    }
    Ok(Some(deadline_at_ms))
}

fn deadline_exceeded_outcome(node_id: &str, deadline_at_ms: u64) -> NodeExecutionOutcome {
    NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Cancelled,
        result_ref: Some(format!("execution-deadline:{node_id}:{deadline_at_ms}")),
        wait_class: None,
        summary: Some("Execution branch stopped at its durable wall-clock deadline".to_string()),
        evidence_refs: Vec::new(),
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: "execution_deadline_exceeded".to_string(),
            message: format!(
                "execution node `{node_id}` exceeded absolute deadline `{deadline_at_ms}`"
            ),
            retryable: false,
            evidence_refs: Vec::new(),
        }),
        usage: Default::default(),
        finished_at_ms: now_ms(),
    })
}

fn verified_predecessor_status(
    graph: &ExecutionGraph,
    predecessor_id: &str,
    required_evidence_refs: &[String],
) -> ExecutionNodeStatus {
    let status = graph.node_statuses[predecessor_id];
    if status != ExecutionNodeStatus::Completed || required_evidence_refs.is_empty() {
        return status;
    }
    let satisfied = graph
        .node_results
        .get(predecessor_id)
        .is_some_and(|result| {
            required_evidence_refs.iter().all(|required| {
                result.evidence_refs.iter().any(|reference| {
                    reference.evidence_ref.id == *required
                        || reference.retrieval_selector == *required
                        || format!(
                            "{}:{}",
                            reference.evidence_ref.ref_type, reference.evidence_ref.id
                        ) == *required
                })
            })
        });
    if satisfied {
        status
    } else {
        ExecutionNodeStatus::Failed
    }
}

fn quorum_tail_cancellations(graph: &ExecutionGraph) -> Vec<String> {
    quorum_tail_cancellations_excluding(graph, &BTreeSet::new())
}

fn quorum_tail_cancellations_excluding(graph: &ExecutionGraph, prohibited: &BTreeSet<String>) -> Vec<String> {
    let mut cancellations = BTreeSet::new();
    let protected = graph.edges.iter().filter(|edge| edge.kind.is_dependency() && prohibited.contains(&edge.to))
        .map(|edge| edge.from.as_str()).collect::<BTreeSet<_>>();
    for consumer in &graph.nodes {
        if prohibited.contains(&consumer.id) { continue; }
        if graph.node_statuses.get(&consumer.id) != Some(&ExecutionNodeStatus::Ready) {
            continue;
        }
        let Some(work) = consumer.work.as_ref() else {
            continue;
        };
        if !work.dependency.cancel_remaining() {
            continue;
        }
        let Some(group) = work.cancellation_group.as_deref() else {
            continue;
        };
        for predecessor_id in graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency() && edge.to == consumer.id)
            .map(|edge| edge.from.as_str())
        {
            if protected.contains(predecessor_id) { continue; }
            let Some(predecessor) = graph.nodes.iter().find(|node| node.id == predecessor_id)
            else {
                continue;
            };
            let cancellable = predecessor.work.as_ref().is_some_and(|predecessor_work| {
                !predecessor_work.required
                    && predecessor_work.cancellation_group.as_deref() == Some(group)
            });
            if cancellable
                && graph
                    .node_statuses
                    .get(predecessor_id)
                    .is_some_and(|status| !status.is_terminal())
            {
                cancellations.insert(predecessor_id.to_string());
            }
        }
    }
    cancellations.into_iter().collect()
}

pub(crate) fn validate_worktree_path(
    workspace_root: &Path,
    requested: &str,
) -> Result<PathBuf, String> {
    let relative = Path::new(requested);
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err("worktree scope must be a non-empty relative path".to_string());
    }
    if relative.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err("worktree scope may not contain parent or root components".to_string());
    }
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace root cannot be canonicalized: {error}"))?;
    let mut candidate = canonical_root.clone();
    for component in relative.components() {
        candidate.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&candidate)
            .map_err(|error| format!("worktree path cannot be inspected: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("worktree scope may not traverse symbolic links".to_string());
        }
    }
    let canonical_candidate = candidate
        .canonicalize()
        .map_err(|error| format!("worktree path cannot be canonicalized: {error}"))?;
    if canonical_candidate == canonical_root || !canonical_candidate.starts_with(&canonical_root) {
        return Err("worktree scope must resolve to a workspace descendant".to_string());
    }
    Ok(canonical_candidate)
}

pub(super) fn validate_outcome(
    node_id: &str,
    outcome: &NodeExecutionOutcome,
) -> Result<(), ExecutionRunnerError> {
    if matches!(
        outcome.result.status,
        ExecutionNodeStatus::Completed
            | ExecutionNodeStatus::Failed
            | ExecutionNodeStatus::Blocked
            | ExecutionNodeStatus::Cancelled
            | ExecutionNodeStatus::WaitingInput
            | ExecutionNodeStatus::WaitingApproval
            | ExecutionNodeStatus::WaitingExternal
    ) {
        Ok(())
    } else {
        Err(ExecutionRunnerError::IllegalOutcome {
            node_id: node_id.to_string(),
            status: outcome.result.status,
        })
    }
}

fn executor_error_node_id(error: &NodeExecutorError) -> &str {
    match error {
        NodeExecutorError::DuplicateExecutor(kind) => kind,
        NodeExecutorError::Unavailable { node_id, .. }
        | NodeExecutorError::Invalid { node_id, .. }
        | NodeExecutorError::Start { node_id, .. }
        | NodeExecutorError::Poll { node_id, .. }
        | NodeExecutorError::Cancel { node_id, .. }
        | NodeExecutorError::Recover { node_id, .. }
        | NodeExecutorError::Uncertain { node_id, .. }
        | NodeExecutorError::OriginalCustodyUnknown { node_id, .. } => node_id,
    }
}


/// R02 collect-and-close: a cancelled node whose original task already
/// finished with a dynamic-replan outcome is closed by this command with a
/// named terminal state carrying the collected facts. Everything that had
/// happened by that point - the computed result reference, the original
/// domain facts and source reads, and the same-ticket effect receipt that
/// this closure commits atomically - is aggregated into the Cancelled
/// result's summary; the replan itself is collected as evidence and never
/// dispatched, so the command leaves no partial graph mutation and the
/// graph keeps converging. Outcomes whose remaining transitions belong to
/// the original owner's atomic domain (delivery envelope, terminal
/// presentation, NoDispatch-with-receipt) stay unknown to this command:
/// per the ruling that unknown is a pre-collection staging state, and they
/// are not expressible as one graph decision without taking the single
/// decision away from that owner.
fn collect_cancelled_outcome(outcome: &NodeExecutionOutcome) -> NodeExecutionOutcome {
    let mut closure = outcome.clone();
    let replan = outcome.replan.as_ref().expect("collected closure requires a replan");
    let collected_nodes = replan
        .nodes
        .iter()
        .map(|spec| spec.id.as_str())
        .collect::<Vec<_>>()
        .join("+");
    let mut collected = format!(
        "cancelled with collected original facts: outcome {:?}{}; dynamic replan {} node(s) [{collected_nodes}] {} edge(s) reason `{}` collected unapplied",
        outcome.result.status,
        outcome
            .result
            .result_ref
            .as_deref()
            .map(|reference| format!(" result_ref `{reference}`"))
            .unwrap_or_default(),
        replan.nodes.len(),
        replan.edges.len(),
        replan.reason,
    );
    if let Some(summary) = outcome.result.summary.as_deref() {
        collected.push_str(&format!("; original summary `{summary}`"));
    }
    closure.result.status = ExecutionNodeStatus::Cancelled;
    closure.result.summary = Some(collected);
    closure.replan = None;
    closure
}

fn failed_result(error: &NodeExecutorError) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: if matches!(error, NodeExecutorError::Uncertain { .. }) {
            ExecutionNodeStatus::Blocked
        } else {
            ExecutionNodeStatus::Failed
        },
        result_ref: None,
        wait_class: None,
        summary: None,
        evidence_refs: Vec::new(),
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: if matches!(error, NodeExecutorError::Uncertain { .. }) {
                "effect_completion_uncertain".to_string()
            } else {
                "executor_error".to_string()
            },
            message: error.to_string(),
            retryable: false,
            evidence_refs: Vec::new(),
        }),
        usage: Default::default(),
        finished_at_ms: now_ms(),
    }
}

fn report(graph: &ExecutionGraph) -> ExecutionRunReport {
    let count = |status| {
        graph
            .node_statuses
            .values()
            .filter(|current| **current == status)
            .count()
    };
    ExecutionRunReport {
        graph_id: graph.id.clone(),
        revision: graph.revision,
        completed: count(ExecutionNodeStatus::Completed),
        failed: count(ExecutionNodeStatus::Failed),
        blocked: count(ExecutionNodeStatus::Blocked),
        cancelled: count(ExecutionNodeStatus::Cancelled),
        waiting: count(ExecutionNodeStatus::WaitingInput)
            + count(ExecutionNodeStatus::WaitingApproval)
            + count(ExecutionNodeStatus::WaitingExternal)
            + count(ExecutionNodeStatus::Paused),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod goal_revalidation_fence_tests {
    use super::*;

    #[test]
    fn terminal_wave_cleanup_keeps_original_reason_and_typed_error_source() {
        let cleanup = ExecutionRunnerError::Executor(NodeExecutorError::Cancel {
            node_id: "node".into(), reason: "physical close pending".into(),
        });
        let error = ExecutionRunnerError::TerminalWaveCleanup {
            reason: "terminal commit failed".into(), phase: "original owner", cleanup: Box::new(cleanup),
        };
        assert!(error.to_string().contains("terminal commit failed"));
        let source = std::error::Error::source(&error).expect("original cleanup source");
        let source = source.downcast_ref::<ExecutionRunnerError>().or_else(|| {
            source.downcast_ref::<Box<ExecutionRunnerError>>().map(|boxed| boxed.as_ref())
        }).expect("the source retains the typed original Runner error");
        assert!(matches!(source, ExecutionRunnerError::Executor(NodeExecutorError::Cancel {
            node_id, reason,
        }) if node_id == "node" && reason == "physical close pending"));
    }

    #[test]
    fn proof_source_retry_classifies_all_six_branches_without_accepting_other_errors() {
        for message in [
            "proof source changed while paging; retry current inputs",
            "proof source changed at final page boundary; retry current inputs",
            "proof Program source changed; retry current inputs",
            "proof Program source changed; retry current inputs; advanced=6 head=24 [event]",
            "proof physical lineage changed during discovery; retry current inputs",
            "proof source changed during validation; retry current inputs; source=goal:x expected=1 actual=2 phase=final_source_fence; advanced=1 head=2 []",
        ] {
            assert!(retryable_proof_source_change(&ExecutionCommitError::InvalidCommand(message.into())), "{message}");
        }
        for message in ["proof Goal spec changed", "observation source conflicts with causal read-set", "proof Program source changed; retry current inputs; forged"] {
            assert!(!retryable_proof_source_change(&ExecutionCommitError::InvalidCommand(message.into())), "{message}");
        }
    }

    #[test]
    fn goal_revalidation_preserves_physical_fact_and_every_other_source() {
        let original = deadline_exceeded_outcome("node", 42);
        let old = BTreeMap::from([("goal:one".to_owned(), 62), ("effect:one".to_owned(), 7)]);
        let next = BTreeMap::from([("goal:one".to_owned(), 63), ("effect:one".to_owned(), 7)]);
        let candidate = original.clone();
        assert!(accepts_goal_revalidation(&original, &candidate, &old, &next, "goal:one", 62, 63));
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &next, "goal:foreign", 62, 63));
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &next, "goal:one", 62, 64));
        let mut changed = candidate.clone();
        changed.result.summary = Some("rewritten physical result".into());
        assert!(!accepts_goal_revalidation(&original, &changed, &old, &next, "goal:one", 62, 63));
        let mut changed_sources = next.clone();
        changed_sources.remove("effect:one");
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &changed_sources, "goal:one", 62, 63));
        changed_sources.insert("effect:one".into(), 8);
        assert!(!accepts_goal_revalidation(&original, &candidate, &old, &changed_sources, "goal:one", 62, 63));
    }

    // The completion classifier sits behind the test-fixtures feature, so
    // the release build does not type-check it. The arm for the moved
    // source is asserted here, where the feature is on: the error is
    // classified by its type and keeps the stream that moved, instead of
    // falling into a bucket that names neither.
    #[cfg(feature = "test-fixtures")]
    #[test]
    fn the_moved_closure_source_is_classified_by_type_and_keeps_its_stream() {
        let error = ExecutionCommitError::PhysicalClosureSourceMoved {
            stream_id: "execution-effect:shape-moved".into(),
        };
        let (category, stream, expected, actual) = k05_completion_cas_class(&error);
        assert_eq!(category, "physical_closure_source_moved");
        assert_eq!(stream, Some("execution-effect:shape-moved"));
        assert_eq!(expected, None);
        assert_eq!(actual, None);
    }

    #[test]
    fn every_executor_error_that_carries_a_node_gives_the_node_back() {
        // `DuplicateExecutor` carries no node: it names the executor kind
        // that was registered twice. Every other variant carries the node
        // it failed on, and the attribution must hand it back unchanged, so
        // a failure stays attributable to its node.
        for error in [
            NodeExecutorError::Unavailable {
                executor_kind: "executor".into(),
                node_id: "node-a".into(),
            },
            NodeExecutorError::Invalid {
                node_id: "node-a".into(),
                reason: "refused".into(),
            },
            NodeExecutorError::Start {
                node_id: "node-a".into(),
                reason: "refused".into(),
            },
            NodeExecutorError::Poll {
                node_id: "node-a".into(),
                reason: "refused".into(),
            },
            NodeExecutorError::Cancel {
                node_id: "node-a".into(),
                reason: "refused".into(),
            },
            NodeExecutorError::Recover {
                node_id: "node-a".into(),
                reason: "refused".into(),
            },
            NodeExecutorError::Uncertain {
                node_id: "node-a".into(),
                reason: "refused".into(),
            },
        ] {
            assert_eq!(executor_error_node_id(&error), "node-a", "{error}");
        }
    }
}

#[cfg(test)]
mod nodispatch_duration_617_tests {
    use sha2::{Digest, Sha256};
    use super::super::executors::{
        ScopedNodeBackend, ScopedNodeBackendResolver, ScopedNodeExecutor,
    };
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RefusedBackend617 {
        owner: Weak<ScopedNodeExecutor>,
        calls: AtomicUsize,
        revalidation: std::sync::Mutex<Option<Arc<GoalRevalidationProbe>>>,
        execution_pause: std::sync::Mutex<Option<Arc<GoalRevalidationProbe>>>,
        callback_pause: std::sync::Mutex<Option<Arc<GoalRevalidationProbe>>>,
        callback_calls: AtomicUsize,
        stop_calls: AtomicUsize,
    }

    struct GoalRevalidationProbe {
        entered: Notify,
        resume: Notify,
        calls: AtomicUsize,
        accept: bool,
    }

    #[async_trait::async_trait]
    impl ScopedNodeBackend for RefusedBackend617 {
        async fn after_commit(&self, _: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.callback_calls.fetch_add(1, Ordering::SeqCst);
            let pause = self.callback_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
            Ok(())
        }
        fn retains_physical_execution(&self) -> bool {
            self.execution_pause.lock().unwrap().is_some()
        }
        fn request_stop(&self, _: &NodeExecutionTicket) {
            if let Some(probe) = self.execution_pause.lock().unwrap().as_ref() {
                self.stop_calls.fetch_add(1, Ordering::SeqCst);
                probe.entered.notify_one();
            }
        }
        async fn revalidate_goal_source(
            &self, _ticket: &NodeExecutionTicket, outcome: &NodeExecutionOutcome,
            stream: &str, _expected: u64, actual: u64,
        ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
            let probe = self.revalidation.lock().unwrap().clone();
            let Some(probe) = probe else { return Ok(None); };
            if probe.calls.fetch_add(1, Ordering::SeqCst) != 0 { return Ok(None); }
            probe.entered.notify_one();
            probe.resume.notified().await;
            if !probe.accept { return Ok(None); }
            let mut rebuilt = outcome.clone();
            rebuilt.source_revisions.insert(stream.into(), actual);
            Ok(Some(rebuilt))
        }
        async fn execute(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let owner = self.owner.upgrade().unwrap();
            owner
                .binding_observation(ticket)
                .unwrap()
                .refuse("denied-617", "original pre-effect scope refusal".into());
            let pause = self.execution_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("original refused batch".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }

    struct Resolver617(Arc<RefusedBackend617>);

    struct ClosureProbeExecutor;

    #[async_trait::async_trait]
    impl NodeExecutor for ClosureProbeExecutor {
        fn kind(&self) -> &str {
            "k05-closure-probe"
        }
        fn validate(
            &self,
            _: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
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
                payload_ref: context.node.payload_ref.clone(),
            })
        }
        async fn poll_or_await(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("k05 closure probe".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }

    struct CancelOutcomeProbeK05 {
        entered: Notify,
        stop: Notify,
        finished: Notify,
        returned: std::sync::atomic::AtomicBool,
        completed: bool,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for CancelOutcomeProbeK05 {
        fn kind(&self) -> &str { "k05-cancel-outcome-probe" }
        fn validate(&self, _: &harness_contract::execution_graph::ExecutionNodeSpec) -> Result<(), NodeExecutorError> { Ok(()) }
        async fn start(&self, context: NodeExecutionContext) -> Result<NodeExecutionTicket, NodeExecutorError> {
            Ok(NodeExecutionTicket { graph_id:context.graph.id.clone(), node_id:context.node.id.clone(),
                executor_kind:self.kind().into(), service_class:context.graph.service_class,
                attempt:context.attempt, idempotency_key:context.node.idempotency_key.clone(),
                payload_ref:context.node.payload_ref.clone() })
        }
        async fn poll_or_await(&self, _: &NodeExecutionTicket) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            self.entered.notify_one();
            self.stop.notified().await;
            self.returned.store(true, Ordering::SeqCst);
            self.finished.notify_one();
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status:if self.completed { ExecutionNodeStatus::Completed } else { ExecutionNodeStatus::Cancelled }, result_ref:None, wait_class:None,
                summary:Some("original executor cancellation outcome".into()), evidence_refs:Vec::new(),
                failure:None, usage:Default::default(), finished_at_ms:1,
            }))
        }
        async fn cancel(&self, _: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.stop.notify_one();
            self.finished.notified().await;
            Ok(())
        }
    }

    #[test]
    fn k05_host_transcript_command_requires_original_node_attempt_status_and_transaction() {
        use super::super::commit_service::host_transcript_cancel_command_result as extract;
        use super::super::events::{ExecutionGraphDelta, ExecutionGraphEvent};
        let mut before = ExecutionGraph::new("cancel transcript source");
        let node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel, "inline_model", "model");
        let node_id = node.id.clone();
        before.nodes.push(node);
        before.revision = 4;
        before.node_statuses.insert(node_id.clone(), ExecutionNodeStatus::Running);
        before.recovery_cursor.node_attempts.insert(node_id.clone(), 1);
        let ticket = NodeExecutionTicket { graph_id:before.id.clone(), node_id:node_id.clone(),
            executor_kind:"inline_model".into(), service_class:before.service_class,
            attempt:1, idempotency_key:"original-ticket".into(), payload_ref:"original-payload".into() };
        let mut after = before.clone();
        after.revision = 5;
        after.node_statuses.insert(node_id.clone(), ExecutionNodeStatus::Cancelled);
        after.node_results.insert(node_id.clone(), ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
            result_ref:None, wait_class:None, summary:None, evidence_refs:Vec::new(), failure:None,
            usage:Default::default(), finished_at_ms:1 });
        let delta = ExecutionGraphDelta::between(&before, &after);
        let transaction = format!("{}:command:cancel:5", before.id);
        let event = ExecutionGraphEvent::CommandApplied { command:"cancel".into(), reason:None, delta:delta.clone() };
        assert_eq!(extract(&event, &ticket, &transaction).unwrap().status, ExecutionNodeStatus::Cancelled);
        let mut foreign = ticket.clone(); foreign.attempt = 2;
        assert!(extract(&event, &foreign, &transaction).is_none());
        foreign = ticket.clone(); foreign.node_id = "foreign".into();
        assert!(extract(&event, &foreign, &transaction).is_none());
        assert!(extract(&event, &ticket, &format!("{}:command:cancel:6", before.id)).is_none());
        let wrong_command = ExecutionGraphEvent::CommandApplied { command:"pause".into(), reason:None, delta:delta.clone() };
        assert!(extract(&wrong_command, &ticket, &transaction).is_none());
        let mut wrong_delta = delta.clone();
        wrong_delta.node_status_updates.insert(node_id.clone(), ExecutionNodeStatus::Completed);
        let wrong_status = ExecutionGraphEvent::CommandApplied { command:"cancel".into(), reason:None, delta:wrong_delta };
        assert!(extract(&wrong_status, &ticket, &transaction).is_none());
        let checkpoint = ExecutionGraphEvent::Checkpoint { cause:"execution_graph.command_applied".into(),
            graph:after, bindings:Default::default(), lease_closures:Vec::new() };
        assert!(extract(&checkpoint, &ticket, &transaction).is_some());
        assert!(extract(&checkpoint, &ticket, "foreign:command:cancel:5").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn k05_host_transcript_original_cancel_command_is_atomic_and_foreign_rejected() {
        let (_workspace, services, runner, initial_graph, initial_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&initial_graph, initial_node, Arc::new(Notify::new())).await.unwrap();
        let mut source = ExecutionGraph::new("original inline model transcript cancellation");
        source.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id:"session-617".into(), turn_id:"turn-617".into(),
            root_task_id:"task-617".into(), task_id:"task-617".into(), generation:1 });
        source.nodes.push(harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel, "inline_model", "original-model"));
        let graph_id = runner.register(source).await.unwrap().id;
        let snapshot = runner.pump_snapshot(&graph_id, &Default::default()).await.unwrap();
        let prepared = runner.prepare_ready_wave(&graph_id, snapshot.ready).await.unwrap().remove(0);
        let node_id = prepared.node.id.clone();
        let ticket = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().ticket.clone();
        let graph = runner.state_store.load(&graph_id).unwrap();
        let event = services.commit_service().prepare_host_transcript(&ticket, "session-617", "turn-617", 0,
            format!("sha256:{}", "0".repeat(64)), Vec::new(), None).unwrap();
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
            result_ref:None, wait_class:None, summary:None, evidence_refs:Vec::new(), failure:None,
            usage:Default::default(), finished_at_ms:1 });
        let mut missing_session_proof = outcome.result.clone();
        missing_session_proof.summary = Some("original local provider response observed authorized cancellation".into());
        missing_session_proof.result_ref = Some(format!("session:session-617:physical:5:sha256:{}", "0".repeat(64)));
        assert!(services.commit_service().validate_cold_provider_cancellation(&ticket, &missing_session_proof).is_err());
        super::super::commit_service::ExecutionCommitService::attach_host_transcript(&mut outcome, event).unwrap();
        let command = ExecutionGraphCommand::Cancel { expected_revision:graph.revision, reason:"original cancellation".into() };
        let mut foreign = outcome.clone();
        foreign.domain_events[0].event.payload["attempt"] = serde_json::json!(ticket.attempt + 1);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome:foreign, effect_ticket:Some(ticket.clone())}]).is_err());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome, effect_ticket:Some(ticket.clone())}]).unwrap();
        assert!(services.commit_service().committed_host_transcript(&ticket).unwrap().messages.is_empty());
        let effects = services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap();
        let receipt = effects.iter().find(|event| event.kind == "execution.effect.receipt").unwrap();
        let graph_events = services.event_store().list_stream(&graph_id).unwrap();
        assert!(graph_events.iter().any(|event| event.transaction_id == receipt.transaction_id && event.commit_cursor == receipt.commit_cursor));
    }

    // Original runner/cancel command counterexample: the physical executor
    // returned its actual outcome, but cancel removed the original binding
    // before the pump could publish the effect receipt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_original_cancel_drops_actual_executor_outcome_before_effect_receipt() {
        let (_workspace, services, runner, initial_graph, initial_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&initial_graph, initial_node, Arc::new(Notify::new())).await.unwrap();
        let executor = Arc::new(CancelOutcomeProbeK05 { entered:Notify::new(), stop:Notify::new(),
            finished:Notify::new(), returned:std::sync::atomic::AtomicBool::new(false), completed:false });
        services.executor_registry().register(executor.clone()).unwrap();
        let mut graph = ExecutionGraph::new("k05 real cancellation effect handoff");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id:"k05-cancel-session".into(), turn_id:"k05-cancel-turn".into(),
            root_task_id:"k05-cancel-task".into(), task_id:"k05-cancel-task".into(), generation:1 });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify, "k05-cancel-outcome-probe", "probe");
        node.resource_scopes.push("write:k05-cancel".into());
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner.pump_snapshot(&registered.id, &Default::default()).await.unwrap();
        let prepared = runner.prepare_ready_wave(&registered.id, snapshot.ready).await.unwrap().remove(0);
        let node_id = prepared.node.id.clone();
        let binding = prepared.binding.clone().unwrap();
        let ticket = runner.active.lock().await.get(&(registered.id.clone(), node_id.clone())).unwrap().ticket.clone();
        let worker_runner = runner.clone(); let worker_graph = registered.id.clone();
        let worker = tokio::spawn(async move { worker_runner.execute_prepared_pump_node(&worker_graph, prepared, Arc::new(Notify::new())).await });
        tokio::time::timeout(Duration::from_secs(3), executor.entered.notified()).await.unwrap();
        let current = runner.state_store.load_async(&registered.id).await.unwrap();
        let command = ExecutionGraphCommand::Cancel { expected_revision:current.revision, reason:"controlled original cancellation".into() };
        let fixture_outcome = NodeExecutionOutcome::new(ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
            result_ref:None, wait_class:None, summary:Some("original executor cancellation outcome".into()),
            evidence_refs:Vec::new(), failure:None, usage:Default::default(), finished_at_ms:1 });
        let mut foreign = ticket.clone(); foreign.attempt += 1;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(foreign) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        let mut noncancel = fixture_outcome.clone(); noncancel.result.status = ExecutionNodeStatus::Failed;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:noncancel, effect_ticket:Some(ticket.clone()) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        runner.command(&registered.id, command.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker).await.unwrap().unwrap().unwrap();
        assert!(executor.returned.load(Ordering::SeqCst));
        let graph = runner.state_store.load(&registered.id).unwrap();
        let start = runner.state_store.binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(runner.state_store.binding_lease_closure(&graph, &node_id, &binding, start).unwrap());
        let actual_outcome = match services.commit_service().inspect_execution_effect(&ticket).unwrap() {
            ExecutionEffectState::Completed(outcome) => outcome,
            _ => panic!("the original executor outcome must be atomically recorded with cancellation"),
        };
        assert_eq!(actual_outcome.result.status, ExecutionNodeStatus::Cancelled);
        assert_eq!(actual_outcome.result.summary, fixture_outcome.result.summary);
        assert!(actual_outcome.result.usage.duration_ms >= 1);
        let effects = services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap();
        assert_eq!(effects.len(), 2);
        let receipt = effects.iter().find(|event| event.kind == "execution.effect.receipt").unwrap();
        let graph_events = services.event_store().list_stream(&registered.id).unwrap();
        let command_event = graph_events.iter().find(|event| event.transaction_id == receipt.transaction_id).unwrap();
        assert_eq!(command_event.commit_cursor, receipt.commit_cursor);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(ticket.clone())}]).is_err());
        assert_eq!(services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap().len(), 2);
        let cold_store = ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
        let replayed = cold_store.load(&registered.id).unwrap();
        assert_eq!(replayed.node_statuses.get(&node_id), Some(&ExecutionNodeStatus::Cancelled));
        assert_eq!(replayed.node_results.get(&node_id), Some(&actual_outcome.result));
        let cold_commits = super::super::commit_service::ExecutionCommitService::new(Arc::clone(services.event_store()));
        assert!(matches!(cold_commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Completed(outcome) if outcome == actual_outcome));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_cancel_preserves_actual_completed_outcome_in_atomic_receipt() {
        let (_workspace, services, runner, initial_graph, initial_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&initial_graph, initial_node, Arc::new(Notify::new())).await.unwrap();
        let executor = Arc::new(CancelOutcomeProbeK05 { entered:Notify::new(), stop:Notify::new(),
            finished:Notify::new(), returned:std::sync::atomic::AtomicBool::new(false), completed:true });
        services.executor_registry().register(executor.clone()).unwrap();
        let mut graph = ExecutionGraph::new("k05 real cancellation effect handoff");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id:"k05-cancel-session".into(), turn_id:"k05-cancel-turn".into(),
            root_task_id:"k05-cancel-task".into(), task_id:"k05-cancel-task".into(), generation:1 });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify, "k05-cancel-outcome-probe", "probe");
        node.resource_scopes.push("write:k05-cancel".into());
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner.pump_snapshot(&registered.id, &Default::default()).await.unwrap();
        let prepared = runner.prepare_ready_wave(&registered.id, snapshot.ready).await.unwrap().remove(0);
        let node_id = prepared.node.id.clone();
        let binding = prepared.binding.clone().unwrap();
        let ticket = runner.active.lock().await.get(&(registered.id.clone(), node_id.clone())).unwrap().ticket.clone();
        let worker_runner = runner.clone(); let worker_graph = registered.id.clone();
        let worker = tokio::spawn(async move { worker_runner.execute_prepared_pump_node(&worker_graph, prepared, Arc::new(Notify::new())).await });
        tokio::time::timeout(Duration::from_secs(3), executor.entered.notified()).await.unwrap();
        let current = runner.state_store.load_async(&registered.id).await.unwrap();
        let command = ExecutionGraphCommand::Cancel { expected_revision:current.revision, reason:"controlled original cancellation".into() };
        let fixture_outcome = NodeExecutionOutcome::new(ExecutionNodeResult { status:ExecutionNodeStatus::Completed,
            result_ref:None, wait_class:None, summary:Some("original executor cancellation outcome".into()),
            evidence_refs:Vec::new(), failure:None, usage:Default::default(), finished_at_ms:1 });
        let mut foreign = ticket.clone(); foreign.attempt += 1;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(foreign) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        let mut noncancel = fixture_outcome.clone(); noncancel.result.status = ExecutionNodeStatus::Failed;
        let rejected = ExecutionTerminalCommit { node_id:node_id.clone(), outcome:noncancel, effect_ticket:Some(ticket.clone()) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&current, &command, vec![rejected]).is_err());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Uncertain));
        runner.command(&registered.id, command.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker).await.unwrap().unwrap().unwrap();
        assert!(executor.returned.load(Ordering::SeqCst));
        let graph = runner.state_store.load(&registered.id).unwrap();
        let start = runner.state_store.binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(runner.state_store.binding_lease_closure(&graph, &node_id, &binding, start).unwrap());
        let actual_outcome = match services.commit_service().inspect_execution_effect(&ticket).unwrap() {
            ExecutionEffectState::Completed(outcome) => outcome,
            _ => panic!("the original executor outcome must be atomically recorded with cancellation"),
        };
        assert_eq!(actual_outcome.result.status, ExecutionNodeStatus::Completed);
        assert_eq!(actual_outcome.result.summary, fixture_outcome.result.summary);
        assert!(actual_outcome.result.usage.duration_ms >= 1);
        let effects = services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap();
        assert_eq!(effects.len(), 2);
        let receipt = effects.iter().find(|event| event.kind == "execution.effect.receipt").unwrap();
        let graph_events = services.event_store().list_stream(&registered.id).unwrap();
        let command_event = graph_events.iter().find(|event| event.transaction_id == receipt.transaction_id).unwrap();
        assert_eq!(command_event.commit_cursor, receipt.commit_cursor);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(&graph, &command,
            vec![ExecutionTerminalCommit {node_id:node_id.clone(), outcome:fixture_outcome.clone(), effect_ticket:Some(ticket.clone())}]).is_err());
        assert_eq!(services.event_store().list_stream(&ExecutionNodeBinding::effect_stream_id(&ticket)).unwrap().len(), 2);
        let cold_store = ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
        let replayed = cold_store.load(&registered.id).unwrap();
        assert_eq!(replayed.node_statuses.get(&node_id), Some(&ExecutionNodeStatus::Completed));
        assert_eq!(replayed.node_results.get(&node_id), Some(&actual_outcome.result));
        let cold_commits = super::super::commit_service::ExecutionCommitService::new(Arc::clone(services.event_store()));
        assert!(matches!(cold_commits.inspect_execution_effect(&ticket).unwrap(),
            ExecutionEffectState::Completed(outcome) if outcome == actual_outcome));
    }

    async fn prepare_scoped_k05(
        runner: &ExecutionGraphRunner,
        session: &str,
        path: &str,
    ) -> (String, ExecutionPreparedPumpNode) {
        let mut graph = ExecutionGraph::new("k05 scoped closure");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: session.into(),
            turn_id: format!("{session}-turn"),
            root_task_id: format!("{session}-task"),
            task_id: format!("{session}-task"),
            generation: 1,
        });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Verify,
            "k05-closure-probe",
            "k05",
        );
        node.resource_scopes.push(format!("write:{path}"));
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        let mut ready = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(ready.len(), 1);
        let prepared = ready.remove(0);
        assert!(prepared.binding.as_ref().unwrap().scope_lease_ref.is_some());
        (registered.id, prepared)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_shared_runner_slow_original_closure_does_not_block_other_session() {
        let (_workspace, services, runner, graph_a, prepared_a, _) = original_prepared617().await;
        // Retire the fixture's original unpolled owner before creating two scoped
        // leaf graphs on the very same runner and resource managers.
        runner
            .execute_prepared_pump_node(&graph_a, prepared_a, Arc::new(Notify::new()))
            .await
            .unwrap();
        services
            .executor_registry()
            .register(Arc::new(ClosureProbeExecutor))
            .unwrap();
        let (a, a_node) = prepare_scoped_k05(&runner, "k05-session-a", "a").await;
        let (b, b_node) = prepare_scoped_k05(&runner, "k05-session-b", "b").await;
        let a_binding = a_node.binding.clone().unwrap();
        let b_binding = b_node.binding.clone().unwrap();
        let a_id = a_node.node.id.clone();
        let b_id = b_node.node.id.clone();
        let entered = Arc::new(Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = StdMutex::new(release_rx);
        let probe_graph = a.clone();
        let probe_entered = entered.clone();
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |graph| {
            if graph == probe_graph {
                probe_entered.notify_one();
                // A bounded fallback guarantees that a broken assertion cannot
                // leave the original owner holding the shared lock indefinitely.
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15));
            }
        }));
        let a_runner = runner.clone();
        let a_graph = a.clone();
        let task_a = tokio::spawn(async move {
            a_runner
                .execute_prepared_pump_node(&a_graph, a_node, Arc::new(Notify::new()))
                .await
        });
        let hit = tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .is_ok();
        let b_runner = runner.clone();
        let b_graph = b.clone();
        let mut task_b = tokio::spawn(async move {
            b_runner
                .execute_prepared_pump_node(&b_graph, b_node, Arc::new(Notify::new()))
                .await
        });
        let progress = if hit {
            tokio::time::timeout(Duration::from_secs(2), &mut task_b)
                .await
                .ok()
        } else {
            None
        };
        let b_progressed = progress.is_some();
        let _ = release_tx.send(());
        let result_a = task_a.await.unwrap();
        let result_b = match progress {
            Some(result) => result.unwrap(),
            None => task_b.await.unwrap(),
        };
        *runner.closure_probe.lock().unwrap() = None;
        result_a.unwrap();
        result_b.unwrap();
        for (graph, node, binding) in [(&a, &a_id, &a_binding), (&b, &b_id, &b_binding)] {
            let state = runner.state_store.load(graph).unwrap();
            let position = runner
                .state_store
                .binding_start_position(&state, node, binding)
                .unwrap()
                .unwrap();
            assert!(runner
                .state_store
                .binding_lease_closure(&state, node, binding, position)
                .unwrap());
            assert!(
                !runner
                    .active_binding_matches(graph, node, Some(binding))
                    .await
            );
        }
        assert!(runner.drain_physical_execution().await.is_empty());
        assert!(hit, "original A closure barrier was not reached");
        assert!(
            b_progressed,
            "independent B could not finish while A held the original closure barrier"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k05_cancelled_closure_waiter_retains_original_owner_until_retry() {
        let (_workspace, services, runner, original_graph, original_node, _) =
            original_prepared617().await;
        runner
            .execute_prepared_pump_node(&original_graph, original_node, Arc::new(Notify::new()))
            .await
            .unwrap();
        services
            .executor_registry()
            .register(Arc::new(ClosureProbeExecutor))
            .unwrap();
        let (graph, node) = prepare_scoped_k05(&runner, "k05-cancel-session", "cancel").await;
        let binding = node.binding.clone().unwrap();
        let node_id = node.node.id.clone();
        let mut foreign = binding.clone();
        foreign.attempt += 1;
        assert!(runner
            .take_bound_active_node(&graph, &node_id, Some(&foreign))
            .await
            .unwrap()
            .is_none());
        assert!(
            runner
                .active_binding_matches(&graph, &node_id, Some(&binding))
                .await
        );
        let entered = Arc::new(Notify::new());
        let completed = Arc::new(Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = StdMutex::new(release_rx);
        let probe_graph = graph.clone();
        let probe_entered = entered.clone();
        let close_calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = Arc::clone(&close_calls);
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == probe_graph {
                probe_calls.fetch_add(1, Ordering::SeqCst);
                probe_entered.notify_one();
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15));
            }
        }));
        let completed_graph = graph.clone();
        let completed_notify = completed.clone();
        *runner.closure_finished_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == completed_graph { completed_notify.notify_one(); }
        }));
        let task_runner = runner.clone();
        let task_graph = graph.clone();
        let task = tokio::spawn(async move {
            task_runner
                .execute_prepared_pump_node(&task_graph, node, Arc::new(Notify::new()))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        // The original closure is durable, but its active owner has not yet
        // been removed when the observer is cancelled.
        let active = runner.active.lock().await;
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), completed.notified()).await.unwrap();
        task.abort();
        assert!(tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
        let retained = active.get(&(graph.clone(), node_id.clone())).unwrap();
        assert_eq!(retained.binding, binding);
        {
            let state = retained.cancel_outcome.state.lock().unwrap();
            assert!(state.lease_close_action.is_some());
            assert!(state.lease_close_resources.is_some());
            assert!(!state.original_binding_closed);
            assert!(state.lease_close_resources.as_ref().unwrap().lock().unwrap()
                .as_ref().unwrap().owner_fence.is_some());
        }
        assert!(retained.callback_completed);
        let ticket = retained.ticket.clone();
        let executor = Arc::clone(&retained.executor);
        drop(active);
        assert!(runner.original_terminal_cleanup_handoff(&graph, &ticket, &executor)
            .await.unwrap().is_none());
        let foreign_executor: Arc<dyn NodeExecutor> = Arc::new(ClosureProbeExecutor);
        assert!(runner.original_terminal_cleanup_handoff(&graph, &ticket, &foreign_executor)
            .await.is_err());
        runner.complete_callback_observed(&graph, &executor, &ticket, None)
            .await.unwrap();
        assert!(runner.original_terminal_cleanup_handoff(&graph, &ticket, &executor)
            .await.unwrap().is_none());
        *runner.closure_probe.lock().unwrap() = None;
        *runner.closure_finished_probe.lock().unwrap() = None;
        let captured = {
            let active = runner.active.lock().await;
            let owner = active.get(&(graph.clone(), node_id.clone())).unwrap();
            (owner.binding.clone(), Arc::clone(&owner.callback_gate), Arc::clone(&owner.cancel_outcome))
        };
        let unconfirmed = Arc::new(CancelOutcomeHandoff::default());
        assert!(runner.original_terminal_cleanup_captured(
            &graph, &ticket, &executor, (binding.clone(), Arc::clone(&captured.1), Arc::clone(&unconfirmed)),
        ).await.is_err());
        assert!(runner
            .take_bound_active_node(&graph, &node_id, Some(&binding))
            .await
            .unwrap()
            .is_some());
        assert!(runner.original_terminal_cleanup_captured(
            &graph, &ticket, &executor, (binding.clone(), Arc::clone(&captured.1), unconfirmed),
        ).await.is_err());
        assert!(runner.original_terminal_cleanup_captured(&graph, &ticket, &executor, captured)
            .await.unwrap().is_none());
        assert!(runner
            .take_bound_active_node(&graph, &node_id, Some(&binding))
            .await
            .unwrap()
            .is_none());
        let state = runner.state_store.load(&graph).unwrap();
        let position = runner
            .state_store
            .binding_start_position(&state, &node_id, &binding)
            .unwrap()
            .unwrap();
        assert!(runner
            .state_store
            .binding_lease_closure(&state, &node_id, &binding, position)
            .unwrap());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert!(runner.drain_physical_execution().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a8_lease_driver_panic_keeps_the_same_resources_and_is_not_replayed() {
        let (_workspace, services, runner, original_graph, original_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&original_graph, original_node, Arc::new(Notify::new())).await.unwrap();
        services.executor_registry().register(Arc::new(ClosureProbeExecutor)).unwrap();
        let (graph, node) = prepare_scoped_k05(&runner, "a8-lease-panic", "panic").await;
        let binding = node.binding.clone().unwrap();
        let node_id = node.node.id.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = Arc::clone(&calls);
        let probe_graph = graph.clone();
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == probe_graph { probe_calls.fetch_add(1, Ordering::SeqCst); panic!("lease probe panic"); }
        }));
        assert!(runner.execute_prepared_pump_node(&graph, node, Arc::new(Notify::new())).await.is_err());
        let holder = {
            let active = runner.active.lock().await;
            let owner = active.get(&(graph.clone(), node_id.clone())).expect("original owner retained");
            assert_eq!(owner.binding, binding);
            let state = owner.cancel_outcome.state.lock().unwrap();
            assert!(!state.original_binding_closed);
            state.lease_close_resources.clone().unwrap()
        };
        assert!(holder.lock().unwrap().as_ref().unwrap().owner_fence.is_some());
        assert!(runner.take_bound_active_node(&graph, &node_id, Some(&binding)).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let state = runner.state_store.load(&graph).unwrap();
        let position = runner.state_store.binding_start_position(&state, &node_id, &binding).unwrap().unwrap();
        assert!(!runner.state_store.binding_lease_closure(&state, &node_id, &binding, position).unwrap());
        *runner.closure_probe.lock().unwrap() = None;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a8_lease_driver_survives_observer_cancel_before_commit() {
        let (_workspace, services, runner, original_graph, original_node, _) = original_prepared617().await;
        runner.execute_prepared_pump_node(&original_graph, original_node, Arc::new(Notify::new())).await.unwrap();
        services.executor_registry().register(Arc::new(ClosureProbeExecutor)).unwrap();
        let (graph, node) = prepare_scoped_k05(&runner, "a8-lease-waiter", "waiter").await;
        let binding = node.binding.clone().unwrap(); let node_id = node.node.id.clone();
        let entered = Arc::new(Notify::new()); let calls = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = StdMutex::new(release_rx);
        let probe_graph = graph.clone(); let signal = Arc::clone(&entered); let probe_calls = Arc::clone(&calls);
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == probe_graph {
                probe_calls.fetch_add(1, Ordering::SeqCst); signal.notify_one();
                let _ = release_rx.lock().unwrap().recv_timeout(Duration::from_secs(15));
            }
        }));
        let first = {
            let runner = runner.clone(); let graph = graph.clone();
            tokio::spawn(async move { runner.execute_prepared_pump_node(&graph, node, Arc::new(Notify::new())).await })
        };
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await.unwrap();
        first.abort(); assert!(first.await.unwrap_err().is_cancelled());
        {
            let active = runner.active.lock().await;
            let owner = active.get(&(graph.clone(), node_id.clone())).unwrap();
            let state = owner.cancel_outcome.state.lock().unwrap();
            assert!(state.lease_close_action.is_some() && state.lease_close_resources.is_some());
            assert!(!state.original_binding_closed);
        }
        let second = {
            let runner = runner.clone(); let graph = graph.clone(); let node = node_id.clone(); let binding = binding.clone();
            tokio::spawn(async move { runner.take_bound_active_node(&graph, &node, Some(&binding)).await })
        };
        release_tx.send(()).unwrap();
        assert!(second.await.unwrap().unwrap().is_some());
        let state = runner.state_store.load(&graph).unwrap();
        let position = runner.state_store.binding_start_position(&state, &node_id, &binding).unwrap().unwrap();
        assert!(runner.state_store.binding_lease_closure(&state, &node_id, &binding, position).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        *runner.closure_probe.lock().unwrap() = None;
    }

    // The sealed terminal-wave races both came from the second checkpoint of
    // a wave terminal: the lease-closure `Recovered` append ran while its
    // owner held no graph coordination, so any coordination-side consumer
    // (wave leader load-then-CAS, dependency reduction, command) could see
    // the graph stream head move underneath the revision it had decided
    // from. The follow-up failure mode is the opposite coupling: owning the
    // domain across the whole physical release serializes unrelated
    // successor dispatch behind slow lease IO. This test fixes the schedule
    // for both edges: while the physical release runs, the domain must be
    // acquirable; once the release is certified and the checkpoint attempt
    // has begun, a competitor holding the domain must keep the graph stream
    // durable-closure uncommitted until the domain is given back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn e1_lease_closure_checkpoint_is_decided_inside_graph_coordination() {
        let (_workspace, services, runner, original_graph, original_node, _) =
            original_prepared617().await;
        runner
            .execute_prepared_pump_node(&original_graph, original_node, Arc::new(Notify::new()))
            .await
            .unwrap();
        services
            .executor_registry()
            .register(Arc::new(ClosureProbeExecutor))
            .unwrap();
        let (graph, node) = prepare_scoped_k05(&runner, "e1-closure-decision-domain", "domain").await;
        let binding = node.binding.clone().unwrap();
        let node_id = node.node.id.clone();
        let entered = Arc::new(Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = StdMutex::new(release_rx);
        let probe_graph = graph.clone();
        let signal = Arc::clone(&entered);
        let calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = Arc::clone(&calls);
        *runner.closure_probe.lock().unwrap() = Some(Arc::new(move |id| {
            if id == probe_graph {
                probe_calls.fetch_add(1, Ordering::SeqCst);
                signal.notify_one();
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15));
            }
        }));
        let checkpoint_reached = Arc::new(Notify::new());
        let checkpoint_graph = graph.clone();
        let checkpoint_signal = Arc::clone(&checkpoint_reached);
        *runner.lease_closure_checkpoint_probe.lock().unwrap() =
            Some(Arc::new(move |id| {
                if id == checkpoint_graph {
                    checkpoint_signal.notify_one();
                }
            }));
        let closer = {
            let runner = runner.clone();
            let graph = graph.clone();
            tokio::spawn(async move {
                runner
                    .execute_prepared_pump_node(&graph, node, Arc::new(Notify::new()))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("the original lease closure barrier must be reached");
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Edge one: the parked physical release must not own the domain, or
        // every successor commit of this graph would serialize behind lease
        // IO - the failure mode the first shape of this fix introduced.
        let domain = tokio::time::timeout(
            Duration::from_millis(500),
            runner.graph_coordination_without_command(&graph),
        )
        .await
        .expect("the physical lease release must run outside the graph decision domain");
        let _ = release_tx.send(());
        // Edge two: once certified, the checkpoint attempt parks on the
        // domain the test now holds. While the domain is held, the durable
        // closure cannot exist: the append is decided in one place, inside
        // the same domain every other graph-stream writer loads under. The
        // pre-fix shape appended here without the domain and this assertion
        // goes red deterministically.
        tokio::time::timeout(Duration::from_secs(5), checkpoint_reached.notified())
            .await
            .expect("the certified lease closure checkpoint must reach its decision domain");
        let pending = runner.state_store.load(&graph).unwrap();
        let pending_position = runner
            .state_store
            .binding_start_position(&pending, &node_id, &binding)
            .unwrap()
            .unwrap();
        assert!(
            !runner
                .state_store
                .binding_lease_closure(&pending, &node_id, &binding, pending_position)
                .unwrap(),
            "the lease closure checkpoint must not advance the graph stream outside its decision domain"
        );
        drop(domain);
        closer.await.unwrap().unwrap();
        *runner.closure_probe.lock().unwrap() = None;
        *runner.lease_closure_checkpoint_probe.lock().unwrap() = None;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let state = runner.state_store.load(&graph).unwrap();
        let position = runner
            .state_store
            .binding_start_position(&state, &node_id, &binding)
            .unwrap()
            .unwrap();
        assert!(runner
            .state_store
            .binding_lease_closure(&state, &node_id, &binding, position)
            .unwrap());
        assert!(!runner.has_retained_shutdown_owners().await);
    }

    impl ScopedNodeBackendResolver for Resolver617 {
        fn resolve(&self, _: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
            Some(self.0.clone())
        }
    }

    struct TamperedExecutor617 {
        inner: Arc<dyn NodeExecutor>,
        drop_witness: bool,
        observed: Option<Arc<std::sync::Mutex<Option<NodeExecutionOutcome>>>>,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for TamperedExecutor617 {
        fn kind(&self) -> &str {
            self.inner.kind()
        }

        fn supports_resumable_pause(&self) -> bool {
            self.inner.supports_resumable_pause()
        }

        fn validate(
            &self,
            node: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
            self.inner.validate(node)
        }

        async fn start(
            &self,
            context: NodeExecutionContext,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            self.inner.start(context).await
        }

        fn prepare_original_binding(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.inner.prepare_original_binding(ticket)
        }
        fn rollback_original_binding(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.inner.rollback_original_binding(ticket)
        }

        async fn poll_or_await(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            let mut outcome = self.inner.poll_or_await(ticket).await?;
            if let Some(observed) = &self.observed {
                *observed.lock().unwrap() = Some(outcome.clone());
                return Ok(outcome);
            }
            if self.drop_witness {
                outcome.no_dispatch = None;
            } else {
                outcome.result.summary = Some("tampered after original seal".into());
            }
            Ok(outcome)
        }

        async fn after_commit(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<(), NodeExecutorError> {
            self.inner.after_commit(ticket).await
        }

        async fn after_abort(
            &self,
            ticket: &NodeExecutionTicket,
            reason: &str,
        ) -> Result<(), NodeExecutorError> {
            self.inner.after_abort(ticket, reason).await
        }

        async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            self.inner.cancel(ticket).await
        }

        async fn cancel_bounded(
            &self,
            ticket: &NodeExecutionTicket,
            budget: std::time::Duration,
        ) -> Result<(), NodeExecutorError> {
            self.inner.cancel_bounded(ticket, budget).await
        }

        fn cancellation_requires_quiescence(&self, ticket: &NodeExecutionTicket) -> bool {
            self.inner.cancellation_requires_quiescence(ticket)
        }

        fn cancellation_finalized(&self, ticket: &NodeExecutionTicket) {
            self.inner.cancellation_finalized(ticket)
        }

        async fn recover(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            self.inner.recover(ticket).await
        }

        async fn drain_physical_execution(&self) -> Result<(), NodeExecutorError> {
            self.inner.drain_physical_execution().await
        }
    }

    #[derive(Default)]
    struct LifecycleProbe660 {
        budgets: std::sync::Mutex<Vec<std::time::Duration>>,
        finalized: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for LifecycleProbe660 {
        fn kind(&self) -> &str {
            "lifecycle-660"
        }

        fn supports_resumable_pause(&self) -> bool {
            false
        }

        fn validate(
            &self,
            _: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
            Ok(())
        }

        async fn start(
            &self,
            _: NodeExecutionContext,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            unreachable!()
        }

        async fn poll_or_await(
            &self,
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            unreachable!()
        }

        async fn cancel(&self, _: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
            panic!("bounded cancellation must not fall back to unbounded cancellation")
        }

        async fn cancel_bounded(
            &self,
            _: &NodeExecutionTicket,
            budget: std::time::Duration,
        ) -> Result<(), NodeExecutorError> {
            self.budgets.lock().unwrap().push(budget);
            Ok(())
        }

        fn cancellation_requires_quiescence(&self, _: &NodeExecutionTicket) -> bool {
            true
        }

        fn cancellation_finalized(&self, _: &NodeExecutionTicket) {
            self.finalized.fetch_add(1, Ordering::SeqCst);
        }

        async fn recover(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionTicket, NodeExecutorError> {
            Err(NodeExecutorError::Recover {
                node_id: ticket.node_id.clone(),
                reason: "original recovery result".into(),
            })
        }
    }

    #[tokio::test]
    async fn r1_660_duration_observer_forwards_original_lifecycle_contract() {
        let inner = Arc::new(LifecycleProbe660::default());
        let wrapper = TamperedExecutor617 {
            inner: inner.clone(),
            drop_witness: false,
            observed: Some(Arc::new(std::sync::Mutex::new(None))),
        };
        let ticket = NodeExecutionTicket {
            graph_id: "graph-660".into(),
            node_id: "node-660".into(),
            executor_kind: "lifecycle-660".into(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "attempt-660".into(),
            payload_ref: "payload-660".into(),
        };
        assert_eq!(wrapper.kind(), inner.kind());
        assert!(!wrapper.supports_resumable_pause());
        assert!(wrapper.cancellation_requires_quiescence(&ticket));
        let budget = std::time::Duration::from_millis(37);
        wrapper.cancel_bounded(&ticket, budget).await.unwrap();
        assert_eq!(*inner.budgets.lock().unwrap(), vec![budget]);
        assert_eq!(inner.finalized.load(Ordering::SeqCst), 0);
        wrapper.cancellation_finalized(&ticket);
        assert_eq!(inner.finalized.load(Ordering::SeqCst), 1);
        assert_eq!(wrapper.recover(&ticket).await, inner.recover(&ticket).await);
    }

    #[tokio::test]
    async fn retained_original_callback_does_not_accept_a_missing_owner() {
        let (_workspace, _services, runner, graph_id, _prepared, _backend) =
            original_prepared617().await;
        assert!(runner.settle_retained_original_callback(&graph_id, "missing-node", 1, |_| Ok(true), |_, _| Ok(true))
            .await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a8_original_pump_task_survives_observer_cancellation() {
        let (task, sender) = OriginalPumpTask::new();
        assert!(task.classify("node-a").is_none());
        let observer = {
            let task = Arc::clone(&task);
            tokio::spawn(async move { task.observe("node-a").await })
        };
        tokio::task::yield_now().await;
        observer.abort();
        let _ = observer.await;
        sender.send(Ok(())).unwrap();
        task.observe("node-a").await.unwrap();
        task.observe("node-a").await.unwrap();
        assert!(matches!(task.classify("node-a"), Some(Ok(()))));
        let (pending, sender) = OriginalPumpTask::new();
        let first = { let pending = Arc::clone(&pending); tokio::spawn(async move { pending.observe("node-b").await }) };
        let second = { let pending = Arc::clone(&pending); tokio::spawn(async move { pending.observe("node-b").await }) };
        sender.send(Ok(())).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a8_original_pump_error_remains_typed_for_each_observer() {
        let (task, sender) = OriginalPumpTask::new();
        sender.send(Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            node_id: "node-a".into(), reason: "original unresolved".into(),
        }))).unwrap();
        assert!(matches!(task.classify("node-a"), Some(Err(ExecutionRunnerError::Executor(
            NodeExecutorError::OriginalCustodyUnknown { .. })))));
        for _ in 0..2 {
            assert!(matches!(task.observe("node-a").await,
                Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { node_id, reason }))
                if node_id == "node-a" && reason == "original unresolved"));
        }
        let (deferred, sender) = OriginalPumpTask::new();
        sender.send(Err(ExecutionRunnerError::ResourceDeferred {
            node_id: "node-b".into(), reason: "original resource wait".into(),
        })).unwrap();
        for _ in 0..2 {
            assert!(matches!(deferred.observe("node-b").await,
                Err(ExecutionRunnerError::ResourceDeferred { node_id, reason })
                if node_id == "node-b" && reason == "original resource wait"));
        }
        let (stale, sender) = OriginalPumpTask::new();
        sender.send(Err(ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision {
            graph_id: "graph".into(), expected: 3, actual: 4,
        }))).unwrap();
        for _ in 0..2 {
            assert!(matches!(stale.observe("node-c").await,
                Err(ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision { graph_id, expected: 3, actual: 4 }))
                if graph_id == "graph"));
        }
    }

    #[tokio::test]
    async fn a8_original_pump_task_closed_sender_is_unknown_for_its_node() {
        let (task, sender) = OriginalPumpTask::new();
        drop(sender);
        assert!(matches!(task.classify("node-b"), Some(Err(ExecutionRunnerError::Executor(
            NodeExecutorError::OriginalCustodyUnknown { .. })))));
        assert!(matches!(task.observe("node-b").await,
            Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { node_id, .. }))
            if node_id == "node-b"));
    }

    #[tokio::test]
    async fn a8_original_pump_tasks_have_independent_observers() {
        let (first, _first_sender) = OriginalPumpTask::new();
        let (second, second_sender) = OriginalPumpTask::new();
        second_sender.send(Ok(())).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(5), first.observe("node-a")).await.is_err());
        tokio::time::timeout(Duration::from_secs(1), second.observe("node-b")).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a8_cleanup_rejoins_same_registered_driver_after_waiter_cancel() {
        let (_workspace, _services, runner, graph_id, prepared, _) = original_prepared617().await;
        let node_id = prepared.node.id.clone();
        let (ticket, executor, handoff) = {
            let active = runner.active.lock().await;
            let owner = active.get(&(graph_id.clone(), node_id)).unwrap();
            (owner.ticket.clone(), Arc::clone(&owner.executor), Arc::clone(&owner.cancel_outcome))
        };
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let worker = {
            let entered = Arc::clone(&entered); let release = Arc::clone(&release); let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                calls.fetch_add(1, Ordering::SeqCst); entered.notify_one(); release.notified().await;
                Ok::<(), String>(())
            })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        {
            let mut state = handoff.state.lock().unwrap();
            state.cleanup_deadline = Some(deadline);
            state.cleanup_nonce = Some(Arc::new(()));
            state.cleanup_action = Some(async move { worker.await.map_err(|error| error.to_string())? }.boxed().shared());
        }
        let first = {
            let runner = runner.clone(); let graph = graph_id.clone(); let ticket = ticket.clone(); let executor = Arc::clone(&executor);
            tokio::spawn(async move { runner.resume_original_terminal_replan_cleanup_until(&graph, &ticket, &executor, deadline).await })
        };
        entered.notified().await;
        first.abort(); assert!(first.await.unwrap_err().is_cancelled());
        let second = {
            let runner = runner.clone(); let graph = graph_id.clone(); let ticket = ticket.clone(); let executor = Arc::clone(&executor);
            tokio::spawn(async move { runner.resume_original_terminal_replan_cleanup_until(&graph, &ticket, &executor, deadline).await })
        };
        release.notify_one(); second.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a8_original_closing_deadline_does_not_reset_on_reentry() {
        let (_workspace, _services, runner, graph_id, prepared, _) = original_prepared617().await;
        let handoff = runner.active.lock().await.get(&(graph_id, prepared.node.id)).unwrap().cancel_outcome.clone();
        let first = std::time::Instant::now() + Duration::from_secs(3);
        let later = first + Duration::from_secs(30);
        let earlier = first - Duration::from_secs(1);
        assert_eq!(handoff.closing_deadline(first).unwrap(), first);
        assert_eq!(handoff.closing_deadline(later).unwrap(), first);
        assert_eq!(handoff.closing_deadline(earlier).unwrap(), earlier);
        assert_eq!(handoff.closing_deadline(later).unwrap(), earlier);
    }

    #[tokio::test]
    async fn retained_original_callback_does_not_accept_an_uncommitted_outcome() {
        let (_workspace, _services, runner, graph_id, prepared, _backend) =
            original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, _outcome) = runner.start_and_execute_node(
            &graph_id, prepared.node, Some(&binding),
        ).await.unwrap();
        let ticket = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone()))
            .unwrap().ticket.clone();
        assert!(runner.settle_retained_original_callback(&graph_id, &node_id, ticket.attempt, |_| Ok(true), |_, _| Ok(true))
            .await.unwrap().is_none());
        assert!(!runner.active.lock().await.get(&(graph_id, node_id)).unwrap().callback_completed);
    }

    #[tokio::test]
    async fn a8_live_prepared_uses_the_exact_original_finalized_outcome() {
        let (_workspace, services, runner, graph_id, prepared, _) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, outcome) = runner.start_and_execute_node(
            &graph_id, prepared.node, Some(&binding),
        ).await.unwrap();
        let (ticket, handoff) = {
            let active = runner.active.lock().await;
            let owner = active.get(&(graph_id.clone(), node_id.clone())).unwrap();
            (owner.ticket.clone(), Arc::clone(&owner.cancel_outcome))
        };
        let mut expected = outcome.clone();
        expected.result.usage.duration_ms = 0;
        assert!(runner.commit_live_prepared_outcome(&ticket, &expected).await.is_err());
        {
            let mut state = handoff.state.lock().unwrap();
            state.finished = true;
            state.outcome = Some(Ok(outcome.clone()));
        }
        let mut foreign = expected.clone();
        foreign.result.finished_at_ms = foreign.result.finished_at_ms.saturating_add(1);
        assert!(runner.commit_live_prepared_outcome(&ticket, &foreign).await.is_err());
        let committed = runner.commit_live_prepared_outcome(&ticket, &expected).await.unwrap();
        assert_eq!(committed.node_results.get(&node_id), Some(&outcome.result));
        assert_eq!(services.graph_state_store().load(&graph_id).unwrap().node_results.get(&node_id), Some(&outcome.result));
    }

    #[tokio::test]
    async fn a8_pump_drain_does_not_accept_missing_or_replaced_owner() {
        let (_workspace, _services, runner, graph_id, prepared, _) = original_prepared617().await;
        let node_id = prepared.node.id.clone();
        let missing = BTreeSet::from([node_id.clone(), "missing-original-node".to_string()]);
        let (owners, absent) = runner.capture_pump_cancellation_owners(&graph_id, &missing).await;
        assert!(absent);
        assert_eq!(owners.len(), 1);
        let original = {
            let mut active = runner.active.lock().await;
            let owner = active.get_mut(&(graph_id.clone(), node_id.clone())).unwrap();
            let original = owner.binding.clone();
            owner.binding.attempt = owner.binding.attempt.saturating_add(1);
            original
        };
        let error = runner.drain_pump_cancellation_owners(&graph_id, owners).await.unwrap_err();
        assert!(error.to_string().contains("original cancellation owner changed"));
        runner.active.lock().await.get_mut(&(graph_id, node_id)).unwrap().binding = original;
    }

    #[tokio::test]
    async fn a8_pump_drain_keeps_uncommitted_original_owner_unknown() {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, _outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        let error = crate::execution_core::supervisor::abort_pump_nodes(&runner, &graph_id,
            &BTreeSet::from([node_id.clone()]), &mut tasks).await.unwrap_err();
        assert!(matches!(error, ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { .. })), "{error:?}");
        let current = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(current.node_statuses[&node_id], ExecutionNodeStatus::Running);
        assert!(services.graph_state_store().binding_retirement_record(&current, &node_id, &binding).unwrap().is_none());
        let active = runner.active.lock().await;
        let retained = active.get(&(graph_id.clone(), node_id.clone())).expect("the original custody stays live");
        assert_eq!(retained.binding, binding);
        assert!(!retained.callback_completed && retained.release_blocked);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        drop(active);
        // Recovery cannot adopt a still-live, undecided original owner.
        assert!(runner.recover_graph(&graph_id).await.is_err());
        // A later original command is a real durable winner; the existing
        // recovery caller can then re-enter and close the exact same owner.
        let current = services.graph_state_store().load(&graph_id).unwrap();
        let command = ExecutionGraphCommand::CancelNode { expected_revision: current.revision,
            node_id: node_id.clone(), reason: "original later cancellation".into() };
        services.commit_service().apply_command(&current, &command).unwrap();
        runner.recover_graph(&graph_id).await.unwrap();
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a8_pump_drain_closes_only_the_committed_original_outcome() {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
        let ticket = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().ticket.clone();
        let current = services.graph_state_store().load(&graph_id).unwrap();
        let sources = services.commit_service().execution_outcome_sources(&current, &ticket, &outcome).unwrap();
        services.commit_service().transition_execution_outcome(&current, &ticket, &outcome, &sources).unwrap();
        *backend.execution_pause.lock().unwrap() = Some(Arc::new(GoalRevalidationProbe {
            entered: Notify::new(), resume: Notify::new(), calls: AtomicUsize::new(0), accept: true,
        }));
        let mut tasks = tokio::task::JoinSet::new();
        crate::execution_core::supervisor::abort_pump_nodes(&runner, &graph_id,
            &BTreeSet::from([node_id.clone()]), &mut tasks).await.unwrap();
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let position = services.graph_state_store().binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(services.graph_state_store().binding_lease_closure(&graph, &node_id, &binding, position).unwrap());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.stop_calls.load(Ordering::SeqCst), 0, "committed winner must not start physical cancellation");
    }

    #[tokio::test]
    async fn a8_wave_waits_for_its_original_drain_before_committing() {
        let (_workspace, services, runner, graph_id, prepared, _) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
        let (ticket, handoff, effect_receipt_required) = {
            let active = runner.active.lock().await;
            let owner = active.get(&(graph_id.clone(), node_id.clone())).unwrap();
            (owner.ticket.clone(), Arc::clone(&owner.cancel_outcome), owner.effect_receipt_required)
        };
        let (release, wait) = oneshot::channel();
        let drain = async move { wait.await.map_err(|error| error.to_string())?; Ok(OriginalDrainOutcome::Drained) }.boxed().shared();
        {
            let mut state = handoff.state.lock().unwrap();
            state.drain = Some(drain);
            state.drain_deadline = Some(std::time::Instant::now() + Duration::from_secs(5));
        }
        let transition = || ExecutionTerminalCommit { node_id: node_id.clone(), outcome: outcome.clone(),
            effect_ticket: effect_receipt_required.then_some(ticket.clone()) };
        let pending = runner.commit_terminal_wave(&graph_id, transition(), Some(&binding)).await.unwrap();
        let TerminalCommitDisposition::WaitForDrain(drain, _) = pending else { panic!("wave did not retain the original drain") };
        assert_eq!(services.graph_state_store().load(&graph_id).unwrap().node_statuses[&node_id], ExecutionNodeStatus::Running);
        release.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), OriginalDrainOutcome::Drained);
        assert!(matches!(runner.commit_terminal_wave(&graph_id, transition(), Some(&binding)).await.unwrap(), TerminalCommitDisposition::Committed));
        assert_eq!(services.graph_state_store().load(&graph_id).unwrap().node_results.get(&node_id), Some(&outcome.result));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a8_cancel_first_preserves_the_original_task_outcome_and_callback() {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let node_id = prepared.node.id.clone();
        let pause = Arc::new(GoalRevalidationProbe { entered:Notify::new(), resume:Notify::new(),
            calls:AtomicUsize::new(0), accept:true });
        *backend.execution_pause.lock().unwrap() = Some(Arc::clone(&pause));
        let (task, sender) = OriginalPumpTask::new();
        runner.register_original_pump_task(&graph_id, &prepared, Arc::clone(&task)).await.unwrap();
        let worker_runner = runner.clone();
        let worker_graph = graph_id.clone();
        tokio::spawn(async move {
            let result = worker_runner.execute_prepared_pump_node(&worker_graph, prepared, Arc::new(Notify::new())).await;
            let _ = sender.send(result);
        });
        let mut observers = tokio::task::JoinSet::new();
        let observer_node = node_id.clone();
        observers.spawn(async move { let result = task.observe(&observer_node).await; (observer_node, result) });
        pause.entered.notified().await;
        let abort_runner = runner.clone();
        let abort_graph = graph_id.clone();
        let abort_node = node_id.clone();
        let abort = tokio::spawn(async move {
            crate::execution_core::supervisor::abort_pump_nodes(&abort_runner, &abort_graph,
                &BTreeSet::from([abort_node]), &mut observers).await
        });
        pause.entered.notified().await;
        assert_eq!(backend.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(services.graph_state_store().load(&graph_id).unwrap().node_statuses[&node_id], ExecutionNodeStatus::Running);
        pause.resume.notify_one();
        abort.await.unwrap().unwrap();
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(graph.node_statuses[&node_id], ExecutionNodeStatus::Completed);
        let position = services.graph_state_store().binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(services.graph_state_store().binding_lease_closure(&graph, &node_id, &binding, position).unwrap());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.callback_calls.load(Ordering::SeqCst), 1);
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a8_callback_action_survives_cancelled_waiter_and_runs_once() {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
        let ticket = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().ticket.clone();
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let sources = services.commit_service().execution_outcome_sources(&graph, &ticket, &outcome).unwrap();
        services.commit_service().transition_execution_outcome(&graph, &ticket, &outcome, &sources).unwrap();
        let pause = Arc::new(GoalRevalidationProbe { entered:Notify::new(), resume:Notify::new(),
            calls:AtomicUsize::new(0), accept:true });
        *backend.callback_pause.lock().unwrap() = Some(pause.clone());
        let first_runner = runner.clone(); let first_graph = graph_id.clone(); let first_node = node_id.clone();
        let first = tokio::spawn(async move { let mut tasks = tokio::task::JoinSet::new();
            crate::execution_core::supervisor::abort_pump_nodes(&first_runner, &first_graph,
                &BTreeSet::from([first_node]), &mut tasks).await });
        pause.entered.notified().await;
        let gate = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().callback_gate.clone();
        let guard = tokio::time::timeout(Duration::from_secs(1), gate.lock()).await.expect("callback action must not hold the gate");
        drop(guard);
        first.abort(); assert!(first.await.unwrap_err().is_cancelled());
        let second_runner = runner.clone(); let second_graph = graph_id.clone(); let second_node = node_id.clone();
        let second = tokio::spawn(async move { let mut tasks = tokio::task::JoinSet::new();
            crate::execution_core::supervisor::abort_pump_nodes(&second_runner, &second_graph,
                &BTreeSet::from([second_node]), &mut tasks).await });
        pause.resume.notify_one();
        second.await.unwrap().unwrap();
        assert_eq!(backend.callback_calls.load(Ordering::SeqCst), 1);
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
    }

    /// Same scaffold as `original_prepared617`, but the node carries one of the
    /// durable override node ids, so the seal is exercised on the exact id
    /// family the production override sites plan.
    async fn override_prepared(node_class: &str) -> (
        tempfile::TempDir,
        Arc<crate::RuntimeServices>,
        ExecutionGraphRunner,
        String,
        ExecutionPreparedPumpNode,
    ) {
        use crate::execution_core::graph::override_custody::OVERRIDE_TERMINAL_NODE_SUFFIXES;
        assert!(OVERRIDE_TERMINAL_NODE_SUFFIXES
            .into_iter()
            .chain(std::iter::once("model"))
            .any(|known| known == node_class));
        let workspace = tempfile::tempdir().unwrap();
        let services = crate::RuntimeServices::in_memory().unwrap();
        let backend = Arc::new(RefusedBackend617 {
            owner: Arc::downgrade(services.tool_batch_executor()),
            calls: AtomicUsize::new(0),
            revalidation: std::sync::Mutex::new(None),
            execution_pause: std::sync::Mutex::new(None),
            callback_pause: std::sync::Mutex::new(None),
            callback_calls: AtomicUsize::new(0),
            stop_calls: AtomicUsize::new(0),
        });
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(Resolver617(backend.clone())));
        let runner = ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            Arc::new(ExecutionResourceManager::new([])),
            Arc::new(ScopeLockManager::persistent(workspace.path().join("scopes")).unwrap()),
            Arc::new(WorktreeLeaseManager::open(workspace.path().join("leases.json")).unwrap()),
            "duration-override",
            workspace.path(),
        );
        let mut graph = ExecutionGraph::new("override custody seal");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-override".into(),
            turn_id: "turn-override".into(),
            root_task_id: "task-override".into(),
            task_id: "task-override".into(),
            generation: 1,
        });
        let payload = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"session-override", "step_snapshots":{},
            "calls":[{"id":"override-617","name":"read_file","input":"{}","depends_on":[]}]
        }))
        .unwrap();
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
            "tool_batch",
            payload,
        );
        node.id = format!("{}:0:{}", graph.id, node_class);
        node.idempotency_key = node.id.clone();
        graph.nodes.push(node);
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let mut prepared = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        (workspace, services, runner, registered.id, prepared.remove(0))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn override_custody_seal_lands_one_canonical_retirement_per_override_class() {
        use crate::execution_core::graph::override_custody::seal_override_node_terminal;
        for class in crate::execution_core::graph::override_custody::OVERRIDE_TERMINAL_NODE_SUFFIXES {
            let (_workspace, services, runner, graph_id, prepared) = override_prepared(class).await;
            let binding = prepared.binding.clone().unwrap();
            let node = prepared.node.clone();
            let node_id = node.id.clone();
            let (executed, _outcome) = runner
                .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
                .await
                .unwrap();
            assert_eq!(executed, node_id);
            let store = services.graph_state_store();
            let commits = services.commit_service();
            let graph = store.load(&graph_id).unwrap();
            // The defect itself: a started override binding with neither a
            // committed attempt nor a retirement stays Pending forever, which is
            // what the runner reports as unknown original call custody.
            assert_eq!(
                commits.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
                CallbackDisposition::Pending,
                "class {class} must start as the unresolved custody defect"
            );
            let revision = graph.revision;
            let custody = seal_override_node_terminal(store, commits, &graph_id, &node, "override seal");
            assert_eq!(custody, OverrideCustody::Sealed, "class {class} must seal");
            let graph = store.load(&graph_id).unwrap();
            assert_eq!(
                graph.revision - revision,
                1,
                "class {class} retirement must append exactly one durable event"
            );
            assert!(store.binding_retirement_record(&graph, &node_id, &binding).unwrap().is_some());
            // Acceptance 1: the disposition leaves Pending for a canonical fact.
            assert_eq!(
                commits.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
                CallbackDisposition::Abort,
                "class {class} custody must be decidable after the seal"
            );
            // A second seal may not rewrite or duplicate the fact.
            let revision = graph.revision;
            assert_eq!(
                seal_override_node_terminal(store, commits, &graph_id, &node, "second seal"),
                OverrideCustody::AlreadySettled
            );
            assert_eq!(store.load(&graph_id).unwrap().revision, revision, "class {class} re-seal must not advance the stream");
        }
    }

    /// Acceptance negative: a node the graph only *planned* has no binding to
    /// retire, so the seal refuses it and leaves the stream untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn override_custody_seal_refuses_a_planned_override_node() {
        use crate::execution_core::graph::override_custody::seal_override_node_terminal;
        let services = crate::RuntimeServices::in_memory().expect("services");
        let mut graph = ExecutionGraph::new("planned override node");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let graph_id = graph.id.clone();
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Synthesize,
            "synthesize",
            "{}",
        );
        node.id = format!("{graph_id}:0:policy-block-synthesize");
        node.idempotency_key = node.id.clone();
        graph.nodes.push(node.clone());
        let registered = services
            .commit_service()
            .register_graph(graph)
            .expect("register a planned override graph")
            .graph;
        let store = services.graph_state_store();
        let loaded = store.load(&graph_id).expect("load the planned graph");
        assert_eq!(loaded.revision, registered.revision);
        let custody = seal_override_node_terminal(store, services.commit_service(), &graph_id, &node, "planned only");
        assert!(
            matches!(custody, OverrideCustody::RefusedNotStarted | OverrideCustody::RefusedNotRetirable),
            "a planned override node that was never admitted and started can own no retirement: \
             custody={custody:?} status={:?} attempt={:?}",
            loaded.node_statuses.get(&node.id).copied(),
            loaded.recovery_cursor.node_attempts.get(&node.id).copied(),
        );
        assert_eq!(
            store.load(&graph_id).unwrap().revision,
            loaded.revision,
            "a refused seal must not touch the stream"
        );
    }

    /// Acceptance negative: a node outside the override family is never even
    /// addressed, so its own unknown-custody report stays exactly as before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn override_custody_seal_refuses_non_override_nodes() {
        let (_workspace, services, runner, graph_id, prepared) = override_prepared("model").await;
        let binding = prepared.binding.clone().unwrap();
        let store = services.graph_state_store();
        let commits = services.commit_service();
        let (node_id, _outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        let graph = store.load(&graph_id).unwrap();
        let revision = graph.revision;
        assert_eq!(seal_override_custody_for_graph(store, commits, &graph, &node_id, "not an override node"), None);
        assert_eq!(store.load(&graph_id).unwrap().revision, revision);
        assert_eq!(
            commits.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
            CallbackDisposition::Pending,
            "a non-override binding must keep the original fail-closed report"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn override_custody_seal_grants_nothing_to_a_foreign_or_forged_binding() {
        use crate::execution_core::graph::override_custody::seal_override_node_terminal;
        let (_workspace, services, runner, graph_id, prepared) =
            override_prepared("provider-block-synthesize").await;
        let binding = prepared.binding.clone().unwrap();
        let node = prepared.node.clone();
        let node_id = node.id.clone();
        let store = services.graph_state_store();
        let commits = services.commit_service();
        let (executed, _outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        assert_eq!(executed, node_id);
        // The seal derives the canonical binding from the durable graph, so an
        // older attempt can never borrow the fact it writes.
        let mut stale = binding.clone();
        stale.attempt += 1;
        let mut foreign_key = binding.clone();
        foreign_key.ticket_idempotency_key = "forged-idempotency-key".into();
        let mut foreign_executor = binding.clone();
        foreign_executor.executor_kind = "inline_model".into();
        assert_eq!(
            seal_override_node_terminal(store, commits, &graph_id, &node, "canonical seal"),
            OverrideCustody::Sealed
        );
        let graph = store.load(&graph_id).unwrap();
        assert!(store.binding_retirement_record(&graph, &node_id, &binding).unwrap().is_some());
        for borrowed in [stale.clone(), foreign_key, foreign_executor] {
            assert!(
                !store.binding_retirement_record(&graph, &node_id, &borrowed).is_ok_and(|record| record.is_some()),
                "a stale attempt or a changed binding identity must own no retirement"
            );
        }
        // A forged retirement writer either is refused by the canonical stream
        // outright, or is refused by the reader: it must never grant custody.
        let forged = commits.event_store().append(crate::RuntimeEventInput {
            stream_id: graph_id.clone(),
            scope: crate::RuntimeEventScope::ExecutionGraph,
            kind: "execution_graph.command_applied".into(),
            status: Some("applied".into()),
            actor: Some("forged-retirement-writer".into()),
            refs: Vec::new(),
            payload: serde_json::json!({
                "event": "command_applied",
                "command": "cancel_node",
                "node_id": node_id,
                "graph": graph,
            }),
        });
        let granted = store
            .load(&graph_id)
            .ok()
            .and_then(|graph| store.binding_retirement_record(&graph, &node_id, &stale).ok())
            .is_some_and(|record| record.is_some());
        assert!(
            forged.is_err() || !granted,
            "a non-canonical retirement source must not become a custody fact"
        );
        assert_ne!(
            seal_override_node_terminal(store, commits, &graph_id, &node, "after forge"),
            OverrideCustody::Sealed,
            "the seal must not write a second fact onto a stream the reader rejects"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn override_custody_silence_releases_only_after_the_whole_bound() {
        use crate::execution_core::graph::override_custody::{
            graph_last_durable_ms, override_custody_silence,
        };
        let (_workspace, services, runner, graph_id, prepared) =
            override_prepared("policy-block-synthesize").await;
        let binding = prepared.binding.clone().unwrap();
        let node = prepared.node.clone();
        let node_id = node.id.clone();
        let store = services.graph_state_store();
        let commits = services.commit_service();
        let events = commits.event_store();
        let (_executed, _outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        let graph = store.load(&graph_id).unwrap();
        let head = graph_last_durable_ms(events, &graph_id, graph.revision).expect("durable head");
        let unfinalized = override_custody_silence(store, commits, events, &graph_id, head + 60_000)
            .expect("silence measurement");
        assert_eq!(unfinalized.unfinalized_node_ids, vec![node_id.clone()]);
        assert_eq!(unfinalized.idle_ms, 60_000);
        assert!(!unfinalized.is_stall(OVERRIDE_CUSTODY_SILENCE_RELEASE_MS));
        // One millisecond short of the bound releases nothing and rewrites nothing.
        let revision = graph.revision;
        assert!(release_silent_override_custody(
            store, commits, events, &graph_id, head + OVERRIDE_CUSTODY_SILENCE_RELEASE_MS - 1,
            OVERRIDE_CUSTODY_SILENCE_RELEASE_MS
        )
        .is_empty());
        assert_eq!(store.load(&graph_id).unwrap().revision, revision);
        // At the bound the silent custody stall is judged and released, which is
        // the taxo certv3 case: an interrupted streak never reaches three.
        let released = release_silent_override_custody(
            store, commits, events, &graph_id, head + 3_816_000, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS,
        );
        assert_eq!(
            released,
            vec![(node_id.clone(), OverrideCustody::Sealed)],
            "a 63.6 minute silent stall with an unfinalized override binding must release"
        );
        let graph = store.load(&graph_id).unwrap();
        assert!(store.binding_retirement_record(&graph, &node_id, &binding).unwrap().is_some());
        assert_eq!(
            commits.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
            CallbackDisposition::Abort
        );
        // The bound is durable, not a counter: an unrelated admission cannot
        // reset it, so a second pass has nothing left to do.
        assert!(release_silent_override_custody(
            store, commits, events, &graph_id, head + 3_816_000, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS,
        )
        .is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn override_custody_seal_is_parallel_safe_across_executions() {
        use crate::execution_core::graph::override_custody::seal_override_node_terminal;
        let mut tasks = tokio::task::JoinSet::new();
        for class in crate::execution_core::graph::override_custody::OVERRIDE_TERMINAL_NODE_SUFFIXES {
            tasks.spawn(async move {
                let (_workspace, services, runner, graph_id, prepared) = override_prepared(class).await;
                let binding = prepared.binding.clone().unwrap();
                let node = prepared.node.clone();
                let node_id = node.id.clone();
                let _started = runner
                    .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
                    .await
                    .unwrap();
                let custody = seal_override_node_terminal(
                    services.graph_state_store(),
                    services.commit_service(),
                    &graph_id,
                    &node,
                    "parallel seal",
                );
                (class, node_id, custody)
            });
        }
        let mut sealed = 0;
        while let Some(joined) = tasks.join_next().await {
            let (class, _node_id, custody) = joined.expect("a concurrent override seal must not disturb another");
            assert_eq!(custody, OverrideCustody::Sealed, "class {class}");
            sealed += 1;
        }
        assert_eq!(sealed, crate::execution_core::graph::override_custody::OVERRIDE_TERMINAL_NODE_SUFFIXES.len());
    }

    #[tokio::test]
    async fn a8_pump_drain_closes_only_the_canonical_retirement() {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, _outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let command = ExecutionGraphCommand::CancelNode { expected_revision: graph.revision,
            node_id: node_id.clone(), reason: "original cancellation".into() };
        services.commit_service().apply_command(&graph, &command).unwrap();
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(services.graph_state_store().binding_retirement_record(&graph, &node_id, &binding).unwrap().is_some());
        let mut tasks = tokio::task::JoinSet::new();
        crate::execution_core::supervisor::abort_pump_nodes(&runner, &graph_id,
            &BTreeSet::from([node_id.clone()]), &mut tasks).await.unwrap();
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let position = services.graph_state_store().binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(services.graph_state_store().binding_lease_closure(&graph, &node_id, &binding, position).unwrap());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a8_pump_drain_rereads_original_winner_after_physical_stop() {
        for (replace_owner, cancel_waiter) in [(false, false), (false, true), (true, false)] {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let node_id = prepared.node.id.clone();
        let pause = Arc::new(GoalRevalidationProbe { entered:Notify::new(), resume:Notify::new(),
            calls:AtomicUsize::new(0), accept:true });
        *backend.execution_pause.lock().unwrap() = Some(pause.clone());
        let worker_runner = runner.clone();
        let worker_graph = graph_id.clone();
        let worker_binding = binding.clone();
        let worker = tokio::spawn(async move {
            worker_runner.start_and_execute_node(&worker_graph, prepared.node, Some(&worker_binding)).await
        });
        pause.entered.notified().await;
        let drain_runner = runner.clone();
        let drain_graph = graph_id.clone();
        let drain_node = node_id.clone();
        let mut drain = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            crate::execution_core::supervisor::abort_pump_nodes(&drain_runner, &drain_graph,
                &BTreeSet::from([drain_node]), &mut tasks).await
        });
        // The existing backend is now physically stopping, while its work is
        // still retained and the original cancellation owner holds the gate.
        pause.entered.notified().await;
        let gate = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().callback_gate.clone();
        let guard = tokio::time::timeout(Duration::from_secs(1), gate.lock()).await.expect("physical drain must not hold the callback gate");
        drop(guard);
        if cancel_waiter {
            drain.abort();
            assert!(drain.await.unwrap_err().is_cancelled());
            let retry_runner = runner.clone();
            let retry_graph = graph_id.clone();
            let retry_node = node_id.clone();
            drain = tokio::spawn(async move {
                let mut tasks = tokio::task::JoinSet::new();
                crate::execution_core::supervisor::abort_pump_nodes(&retry_runner, &retry_graph,
                    &BTreeSet::from([retry_node]), &mut tasks).await
            });
        }
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        services.commit_service().apply_command(&graph, &ExecutionGraphCommand::CancelNode {
            expected_revision: graph.revision, node_id: node_id.clone(), reason:"racing original cancellation".into(),
        }).unwrap();
        if replace_owner {
            let mut active = runner.active.lock().await;
            active.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().binding.attempt += 1;
        }
        pause.resume.notify_one();
        worker.await.unwrap().unwrap();
        let result = drain.await.unwrap();
        if replace_owner {
            assert!(matches!(result, Err(ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { .. }))));
            let mut active = runner.active.lock().await;
            let retained = active.get_mut(&(graph_id.clone(), node_id.clone())).expect("foreign owner was not released");
            assert_ne!(retained.binding, binding);
            retained.binding = binding.clone();
            drop(active);
            runner.recover_graph(&graph_id).await.unwrap();
        } else { result.unwrap(); }
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(services.graph_state_store().binding_retirement_record(&graph, &node_id, &binding).unwrap().is_some());
        let position = services.graph_state_store().binding_start_position(&graph, &node_id, &binding).unwrap().unwrap();
        assert!(services.graph_state_store().binding_lease_closure(&graph, &node_id, &binding, position).unwrap());
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.stop_calls.load(Ordering::SeqCst), 1, "all waiters share the original physical stop");
        }
    }

    async fn original_prepared617() -> (
        tempfile::TempDir,
        Arc<crate::RuntimeServices>,
        ExecutionGraphRunner,
        String,
        ExecutionPreparedPumpNode,
        Arc<RefusedBackend617>,
    ) {
        let workspace = tempfile::tempdir().unwrap();
        let services = crate::RuntimeServices::in_memory().unwrap();
        let backend = Arc::new(RefusedBackend617 {
            owner: Arc::downgrade(services.tool_batch_executor()),
            calls: AtomicUsize::new(0),
            revalidation: std::sync::Mutex::new(None),
            execution_pause: std::sync::Mutex::new(None),
            callback_pause: std::sync::Mutex::new(None),
            callback_calls: AtomicUsize::new(0),
            stop_calls: AtomicUsize::new(0),
        });
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(Resolver617(backend.clone())));
        let runner = ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            Arc::new(ExecutionResourceManager::new([])),
            Arc::new(ScopeLockManager::persistent(workspace.path().join("scopes")).unwrap()),
            Arc::new(WorktreeLeaseManager::open(workspace.path().join("leases.json")).unwrap()),
            "duration-617",
            workspace.path(),
        );
        let mut graph = ExecutionGraph::new("original refused batch duration");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-617".into(),
            turn_id: "turn-617".into(),
            root_task_id: "task-617".into(),
            task_id: "task-617".into(),
            generation: 1,
        });
        let payload = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"session-617", "step_snapshots":{},
            "calls":[{"id":"denied-617","name":"read_file","input":"{}","depends_on":[]}]
        }))
        .unwrap();
        graph
            .nodes
            .push(harness_contract::execution_graph::ExecutionNodeSpec::new(
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
                "tool_batch",
                payload,
            ));
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let mut prepared = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        (
            workspace,
            services,
            runner,
            registered.id,
            prepared.remove(0),
            backend,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn k05_original_runner_goal_revalidation_releases_coordination_and_keeps_fences() {
        for mode in ["accept", "unknown", "source_advances", "binding_changes", "ticket_changes", "executor_changes"] {
            let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
            let probe = Arc::new(GoalRevalidationProbe {
                entered: Notify::new(), resume: Notify::new(), calls: AtomicUsize::new(0),
                accept: mode != "unknown",
            });
            *backend.revalidation.lock().unwrap() = Some(probe.clone());
            let binding = prepared.binding.clone().unwrap();
            let (node_id, mut outcome) = runner.start_and_execute_node(&graph_id, prepared.node, Some(&binding)).await.unwrap();
            outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
                "envelope_id":"k05-envelope","revision":1,"objective_id":"k05-objective","created_at_ms":1
            })).unwrap());
            let goal = format!("goal:k05-revalidation-{mode}");
            outcome.source_revisions.insert(goal.clone(), 0);
            let append_goal = || services.event_store().append(crate::RuntimeEventInput {
                stream_id: goal.clone(), scope: crate::RuntimeEventScope::Goal,
                kind: "test.k05_goal_advance".into(), status: None, actor: None,
                refs: Vec::new(), payload: serde_json::json!({}),
            }).unwrap();
            append_goal();
            let task_runner = runner.clone();
            let task_graph = graph_id.clone();
            let task_node = node_id.clone();
            let task = tokio::spawn(async move {
                task_runner.finish_pump_node(&task_graph, &task_node, Ok((task_node.clone(), outcome)), Arc::new(Notify::new()), Some(binding), None).await
            });
            tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await.unwrap();
            // A competing graph operation must be able to acquire the actual
            // coordination guard while the original backend is held here.
            let guard = tokio::time::timeout(Duration::from_secs(5), runner.graph_coordination_without_command(&graph_id)).await.unwrap();
            assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&prepared.binding.unwrap())).await);
            if mode == "source_advances" { append_goal(); }
            if mode == "binding_changes" {
                runner.active.lock().await.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().binding.attempt += 1;
            }
            if mode == "ticket_changes" {
                runner.active.lock().await.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().ticket.payload_ref = "foreign".into();
            }
            if mode == "executor_changes" {
                runner.active.lock().await.get_mut(&(graph_id.clone(), node_id.clone())).unwrap().executor = Arc::new(ClosureProbeExecutor);
            }
            drop(guard);
            probe.resume.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
            if mode == "accept" { assert!(result.is_ok(), "{result:?}"); }
            else { assert!(result.is_err(), "{mode} unexpectedly committed"); }
            if matches!(mode, "binding_changes" | "ticket_changes" | "executor_changes") {
                assert!(runner.active.lock().await.contains_key(&(graph_id.clone(), node_id.clone())), "foreign owner must not be released");
            }
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1, "physical tool must not replay");
            let graph = services.graph_state_store().load(&graph_id).unwrap();
            if mode == "accept" { assert_eq!(graph.node_statuses[&node_id], ExecutionNodeStatus::Completed); }
            else { assert_ne!(graph.node_statuses[&node_id], ExecutionNodeStatus::Completed); }
        }
    }

    #[tokio::test]
    async fn never_admitted_rejects_original_active_prepared_owner_without_clearing_it() {
        let (_workspace, _services, runner, graph_id, prepared, backend) =
            original_prepared617().await;
        let expected = runner.state_store.load_async(&graph_id).await.unwrap();
        let before = runner.active.lock().await.len();
        let error = runner
            .confirm_cancelled_bound_child_never_admitted(&expected)
            .await
            .unwrap_err();
        assert!(
            matches!(error, ExecutionRunnerError::AdmissionRaced { .. }),
            "{error}"
        );
        assert_eq!(runner.active.lock().await.len(), before);
        assert!(runner
            .active
            .lock()
            .await
            .contains_key(&(graph_id, prepared.node.id)));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn r1_617_real_runner_duration_commits_original_refusal_and_rejects_old_seal() {
        let (_workspace, services, runner, graph_id, prepared, backend) =
            original_prepared617().await;
        let observed = Arc::new(std::sync::Mutex::new(None));
        let mut active = runner.active.lock().await;
        let original = active
            .get_mut(&(graph_id.clone(), prepared.node.id.clone()))
            .unwrap();
        let ticket = original.ticket.clone();
        let observation = original.no_dispatch.clone().unwrap();
        original.executor = Arc::new(TamperedExecutor617 {
            inner: original.executor.clone(),
            drop_witness: false,
            observed: Some(observed.clone()),
        });
        drop(active);
        runner
            .execute_prepared_pump_node(&graph_id, prepared, Arc::new(Notify::new()))
            .await
            .unwrap();
        let sealed = observed.lock().unwrap().take().unwrap();
        let old_witness = sealed.no_dispatch.clone().unwrap();
        old_witness
            .authenticate(&ticket, Some(&sealed.result))
            .unwrap();
        assert_eq!(sealed.result.usage.duration_ms, 0);
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let committed = &graph.node_results[&ticket.node_id];
        assert_eq!(committed.status, ExecutionNodeStatus::Completed);
        let events = services.event_store().list_stream(&format!("{}:node:{}", graph.id, ticket.node_id)).unwrap();
        let transition = events.iter().find(|event| event.kind == "execution_node.transitioned"
            && !event.payload["no_dispatch"].is_null()).expect("original NoDispatch result");
        let digest = format!("sha256:{:x}", Sha256::digest(serde_json::to_vec(committed).unwrap()));
        assert_eq!(transition.payload["no_dispatch"]["result_sha256"], digest);
        assert_eq!(transition.payload["no_dispatch"]["durable_result_sha256"], digest);
        assert!(committed.usage.duration_ms >= 1);
        let mut expected = sealed.result;
        expected.usage.duration_ms = committed.usage.duration_ms;
        assert_eq!(*committed, expected);
        observation
            .witness()
            .unwrap()
            .authenticate(&ticket, Some(committed))
            .unwrap();
        assert!(old_witness.authenticate(&ticket, Some(committed)).is_err());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert!(!runner
            .active
            .lock()
            .await
            .contains_key(&(graph_id, ticket.node_id)));
        assert!(runner.drain_physical_execution().await.is_empty());
    }

    #[tokio::test]
    async fn r601_mixed_goal_cut_keeps_original_binding_until_outcome_is_proven() {
        let (_workspace, services, runner, graph_id, prepared, _) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, mut outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
            "envelope_id":"601-envelope","revision":1,"objective_id":"601-objective","created_at_ms":1
        })).unwrap());
        outcome.source_revisions.insert("goal:601-conflict".into(), 1);
        outcome.domain_events.push(crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: "goal:601-conflict".into(),
                scope: crate::RuntimeEventScope::Goal,
                kind: "goal.observation".into(),
                status: None, actor: None, refs: Vec::new(),
                payload: serde_json::json!({"goal_source":{"stream_id":"goal:601-conflict","sequence":2}}),
            },
            schema_version: 2,
            idempotency_key: Some("601-conflict".into()),
        });
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        let stale = runner.command(&graph_id, ExecutionGraphCommand::CancelNode {
            expected_revision: graph.revision + 1,
            node_id: node_id.clone(),
            reason: "r601 stale owner must not retire".into(),
        }).await;
        assert!(stale.is_err());
        assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        let error = runner.finish_pump_node(&graph_id, &node_id, Ok((node_id.clone(), outcome)), Arc::new(Notify::new()), Some(binding.clone()), None).await.unwrap_err();
        assert!(error.to_string().contains("observation source conflicts with causal read-set"), "{error}");
        assert!(matches!(error, ExecutionRunnerError::Executor(
            NodeExecutorError::OriginalCustodyUnknown { .. }
        )));
        let after = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(after, graph);
        assert!(services.graph_state_store().binding_retirement_record(&after, &node_id, &binding).unwrap().is_none());
        assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert!(services.event_store().event_by_idempotency_key("goal:601-conflict", "601-conflict").unwrap().is_none());
    }

    #[tokio::test]
    async fn original_cancel_commits_only_same_ticket_no_dispatch_atomically() {
        let (_workspace, services, runner, graph_id, prepared, backend) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, outcome) = runner.start_and_execute_node(
            &graph_id, prepared.node, Some(&binding),
        ).await.unwrap();
        assert!(outcome.no_dispatch.is_some());
        let ticket = runner.active.lock().await.get(&(graph_id.clone(), node_id.clone())).unwrap().ticket.clone();
        let before = services.graph_state_store().load(&graph_id).unwrap();
        let command = ExecutionGraphCommand::CancelNode {
            expected_revision: before.revision, node_id: node_id.clone(), reason: "original cancellation".into(),
        };
        let mut foreign = ticket.clone();
        foreign.attempt += 1;
        let make = |ticket, outcome| ExecutionTerminalCommit { node_id: node_id.clone(), outcome, effect_ticket: Some(ticket) };
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(
            &before, &command, vec![make(foreign, outcome.clone())],
        ).is_err());
        let mut missing = outcome.clone();
        missing.no_dispatch = None;
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(
            &before, &command, vec![make(ticket.clone(), missing)],
        ).is_err());
        let mut stale = outcome.clone();
        stale.source_revisions.insert("goal:cancel-no-dispatch-stale".into(), 1);
        assert!(services.commit_service().apply_command_with_cancelled_outcomes(
            &before, &command, vec![make(ticket.clone(), stale)],
        ).is_err());
        assert_eq!(services.graph_state_store().load(&graph_id).unwrap(), before);
        let calls_before = backend.calls.load(Ordering::SeqCst);
        let receipt = services.commit_service().apply_command_with_cancelled_outcomes(
            &before, &command, vec![make(ticket.clone(), outcome.clone())],
        ).unwrap();
        assert_eq!(receipt.graph.node_results[&node_id], outcome.result);
        assert_eq!(backend.calls.load(Ordering::SeqCst), calls_before);
        let events = services.event_store().list_stream(&format!("{}:node:{}", graph_id, node_id)).unwrap();
        let event = events.iter().find(|event| event.kind == "execution_node.transitioned"
            && !event.payload["no_dispatch"].is_null()).expect("same-ticket NoDispatch transition");
        assert_eq!(event.transaction_id, receipt.transaction.transaction_id);
        let expected_digest = format!("sha256:{:x}", Sha256::digest(serde_json::to_vec(&receipt.graph.node_results[&node_id]).unwrap()));
        assert_eq!(event.payload["no_dispatch"]["durable_result_sha256"], expected_digest);
        assert_eq!(event.payload["no_dispatch"]["result_sha256"], expected_digest);
        assert!(services.event_store().event_by_idempotency_key(
            &ExecutionNodeBinding::effect_stream_id(&ticket), &format!("{}:receipt", ticket.idempotency_key)
        ).unwrap().is_none());
    }

    #[tokio::test]
    async fn r623_foreign_conflicting_outcome_cannot_retire_original_binding() {
        let (_workspace, services, runner, graph_id, prepared, _) = original_prepared617().await;
        let binding = prepared.binding.clone().unwrap();
        let (node_id, mut outcome) = runner
            .start_and_execute_node(&graph_id, prepared.node, Some(&binding))
            .await
            .unwrap();
        let original_outcome = outcome.clone();
        outcome.delivery_envelope = Some(serde_json::from_value(serde_json::json!({
            "envelope_id":"623-envelope","revision":1,"objective_id":"623-objective","created_at_ms":1
        })).unwrap());
        outcome.source_revisions.insert("goal:623-conflict".into(), 1);
        outcome.domain_events.push(crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: "goal:623-conflict".into(), scope: crate::RuntimeEventScope::Goal,
                kind: "goal.observation".into(), status: None, actor: None, refs: Vec::new(),
                payload: serde_json::json!({"goal_source":{"stream_id":"goal:623-conflict","sequence":2}}),
            },
            schema_version: 2, idempotency_key: Some("623-conflict".into()),
        });
        let before = services.graph_state_store().load(&graph_id).unwrap();
        let mut foreign = binding.clone();
        foreign.attempt += 1;
        runner.finish_pump_node(&graph_id, &node_id, Ok((node_id.clone(), outcome)), Arc::new(Notify::new()), Some(foreign), None).await.unwrap();
        let after = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(after, before);
        assert!(runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert!(services.graph_state_store().binding_retirement_record(&after, &node_id, &binding).unwrap().is_none());
        // The actual original outcome was already produced; let its original
        // owner commit it instead of inventing a cancelled result for cleanup.
        runner.finish_pump_node(&graph_id, &node_id, Ok((node_id.clone(), original_outcome.clone())),
            Arc::new(Notify::new()), Some(binding.clone()), None).await.unwrap();
        let completed = services.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(completed.node_results[&node_id], original_outcome.result);
        assert!(!runner.active_binding_matches(&graph_id, &node_id, Some(&binding)).await);
        assert!(runner.drain_physical_execution().await.is_empty());
    }

    #[tokio::test]
    async fn r1_617_real_runner_rejects_post_seal_tampering_or_lost_witness_before_commit() {
        for mode in ["summary", "lost-witness", "missing-observation"] {
            let (_workspace, services, runner, graph_id, prepared, backend) =
                original_prepared617().await;
            let mut active = runner.active.lock().await;
            let original = active
                .get_mut(&(graph_id.clone(), prepared.node.id.clone()))
                .unwrap();
            let ticket = original.ticket.clone();
            let observation = original.no_dispatch.clone().unwrap();
            if mode == "missing-observation" {
                original.no_dispatch = None;
            } else {
                original.executor = Arc::new(TamperedExecutor617 {
                    inner: original.executor.clone(),
                    drop_witness: mode == "lost-witness",
                    observed: None,
                });
            }
            drop(active);
            let before = services.event_store().durable_commit_cursor().unwrap();
            let error = runner
                .start_and_execute_node(&graph_id, prepared.node, prepared.binding.as_ref())
                .await
                .unwrap_err();
            if mode == "missing-observation" {
                assert!(matches!(error,
                    ExecutionRunnerError::Executor(NodeExecutorError::Invalid { reason, .. })
                        if reason == "NoDispatch duration has no original active observation"));
            } else {
                let expected = if mode == "lost-witness" {
                    "NoDispatch duration lost original witness"
                } else {
                    "NoDispatch sealed result mismatch"
                };
                assert!(matches!(error,
                    ExecutionRunnerError::Executor(NodeExecutorError::Poll { reason, .. })
                        if reason == expected));
            }
            assert_eq!(
                services.event_store().durable_commit_cursor().unwrap(),
                before
            );
            let graph = services.graph_state_store().load(&graph_id).unwrap();
            assert_eq!(
                graph.node_statuses[&ticket.node_id],
                ExecutionNodeStatus::Running
            );
            assert!(!graph.node_results.contains_key(&ticket.node_id));
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            let original = services
                .tool_batch_executor()
                .poll_or_await(&ticket)
                .await
                .unwrap();
            observation
                .witness()
                .unwrap()
                .authenticate(&ticket, Some(&original.result))
                .unwrap();
            assert!(runner.drain_physical_execution().await.is_empty());
        }
    }
}

#[cfg(test)]
mod dependency_policy_tests {
    use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};
    use harness_contract::context::{
        EvidenceAccessRef, EvidenceRef, ObservedAcceptance, RequiredAcceptance,
    };
    use harness_contract::execution_graph::{
        DependencyPredicate, ExecutionFailure, ExecutionUsage,
    };
    use harness_contract::execution_graph::{
        ExecutionDependencyPolicy, ExecutionNodeResult, ExecutionNodeStatus,
    };
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionWorkContract, ExecutionWorkRole,
    };

    use super::{
        dependency_predecessors, dependency_target, quorum_tail_cancellations,
        quorum_tail_cancellations_excluding, unknown_dependency_nodes,
        verified_predecessor_status, DependencyPredecessor,
    };

    fn predecessor(status: ExecutionNodeStatus) -> DependencyPredecessor<'static> {
        DependencyPredecessor::status_only(status)
    }

    #[test]
    fn any_and_quorum_become_ready_without_waiting_for_every_lane() {
        let running = [
            predecessor(ExecutionNodeStatus::Completed),
            predecessor(ExecutionNodeStatus::Running),
            predecessor(ExecutionNodeStatus::Planned),
        ];
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Any {
                    cancel_remaining: true,
                },
                &running,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: true,
                },
                &running,
            ),
            None
        );
        let quorum = [
            predecessor(ExecutionNodeStatus::Completed),
            predecessor(ExecutionNodeStatus::Completed),
            predecessor(ExecutionNodeStatus::Running),
        ];
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: true,
                },
                &quorum,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    #[test]
    fn impossible_quorum_is_blocked() {
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Quorum {
                    minimum: 2,
                    cancel_remaining: false,
                },
                &[
                    predecessor(ExecutionNodeStatus::Completed),
                    predecessor(ExecutionNodeStatus::Failed),
                    predecessor(ExecutionNodeStatus::Cancelled),
                ],
            ),
            Some(ExecutionNodeStatus::Blocked)
        );
    }

    #[test]
    fn finally_waits_for_every_lane_terminal_but_never_requires_success() {
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Finally,
                &[
                    predecessor(ExecutionNodeStatus::Completed),
                    predecessor(ExecutionNodeStatus::Failed),
                    predecessor(ExecutionNodeStatus::Running),
                ],
            ),
            None
        );
        assert_eq!(
            dependency_target(
                &ExecutionDependencyPolicy::Finally,
                &[
                    predecessor(ExecutionNodeStatus::Completed),
                    predecessor(ExecutionNodeStatus::Failed),
                    predecessor(ExecutionNodeStatus::Cancelled),
                ],
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    fn durable_evidence(ref_type: &str, id: &str) -> EvidenceAccessRef {
        EvidenceAccessRef::durable(
            EvidenceRef::observed(ref_type, id),
            "sha",
            1,
            "text/plain",
            format!("evidence://{id}"),
            "workspace",
        )
    }

    fn framework_failed_result() -> ExecutionNodeResult {
        ExecutionNodeResult {
            status: ExecutionNodeStatus::Failed,
            result_ref: Some("agent-return:run".to_string()),
            wait_class: None,
            summary: Some("partial".to_string()),
            evidence_refs: vec![
                durable_evidence("runtime_change", "receipt-1"),
                durable_evidence("evidence", "proof-1"),
            ],
            failure: Some(ExecutionFailure {
                kind: "agent_backend".to_string(),
                message: "Runtime rejected Agent terminal result: fixture".to_string(),
                retryable: false,
                evidence_refs: Vec::new(),
            }),
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
                acceptance_evaluation: Some(harness_contract::acceptance::AcceptanceEvaluation {
                    evaluator_revision: crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
                    contract_digest: "fixture-contract".to_string(),
                    receipt_set_digest: "fixture-receipts".to_string(),
                    derived_obligations: Vec::new(),
                    verdict: AcceptanceVerdict::Unsatisfied,
                }),
                ..ExecutionUsage::default()
            },
            finished_at_ms: 1,
        }
    }

    fn evidence_ready_graph(
        source_status: ExecutionNodeStatus,
        source_result: Option<ExecutionNodeResult>,
        required_fact_kinds: Vec<TerminalFactKind>,
        accepted_execution_statuses: Vec<ExecutionNodeStatus>,
        accepted_acceptance_verdicts: Vec<AcceptanceVerdict>,
        require_committed_effect: bool,
    ) -> ExecutionGraph {
        let mut graph = ExecutionGraph::new("evidence ready reviewer");
        let mut source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        source.id = "source".to_string();
        let mut reviewer =
            ExecutionNodeSpec::new(ExecutionNodeKind::Verify, "verify", "team:fixture");
        reviewer.id = "reviewer".to_string();
        reviewer.work = Some(ExecutionWorkContract {
            role: ExecutionWorkRole::CrossCheck,
            dependency: ExecutionDependencyPolicy::EvidenceReady {
                predicate: DependencyPredicate::EvidenceReady {
                    minimum: 1,
                    required_fact_kinds,
                    accepted_execution_statuses,
                    accepted_acceptance_verdicts,
                    require_committed_effect,
                },
                cancel_remaining: false,
            },
            ..ExecutionWorkContract::new(ExecutionWorkRole::CrossCheck)
        });
        graph.nodes = vec![source, reviewer];
        graph.edges = vec![ExecutionEdge {
            from: "source".to_string(),
            to: "reviewer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        }];
        graph
            .node_statuses
            .insert("source".to_string(), source_status);
        if let Some(result) = source_result {
            graph.node_results.insert("source".to_string(), result);
        }
        graph
    }

    #[test]
    fn evidence_ready_accepts_failed_predecessor_with_durable_facts() {
        let graph = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            Some(framework_failed_result()),
            vec![
                TerminalFactKind::CommittedEffect,
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed, ExecutionNodeStatus::Completed],
            vec![
                AcceptanceVerdict::FrameworkInvalid,
                AcceptanceVerdict::Unsatisfied,
            ],
            true,
        );
        let predecessors = dependency_predecessors(&graph, &graph.nodes[1]);
        assert_eq!(
            dependency_target(
                &graph.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    #[test]
    fn evidence_ready_waits_for_non_terminal_predecessor() {
        let graph = evidence_ready_graph(
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
            vec![ExecutionNodeStatus::Completed],
            vec![AcceptanceVerdict::Satisfied],
            false,
        );
        let predecessors = dependency_predecessors(&graph, &graph.nodes[1]);
        assert_eq!(
            dependency_target(
                &graph.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            None
        );
    }

    #[test]
    fn evidence_ready_blocks_terminal_predecessor_without_committed_facts() {
        let graph = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            None,
            Vec::new(),
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            false,
        );
        let predecessors = dependency_predecessors(&graph, &graph.nodes[1]);
        assert_eq!(
            dependency_target(
                &graph.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Blocked)
        );
    }

    #[test]
    fn evidence_ready_cancelled_partial_facts_follow_the_contract_opt_in() {
        let result = ExecutionNodeResult {
            status: ExecutionNodeStatus::Cancelled,
            result_ref: Some("agent-return:cancel".to_string()),
            wait_class: None,
            summary: Some("partial evidence".to_string()),
            evidence_refs: vec![durable_evidence("evidence", "partial-1")],
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
                acceptance_evaluation: Some(harness_contract::acceptance::AcceptanceEvaluation {
                    evaluator_revision: crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
                    contract_digest: "fixture-contract".to_string(),
                    receipt_set_digest: "fixture-receipts".to_string(),
                    derived_obligations: Vec::new(),
                    verdict: AcceptanceVerdict::Unsatisfied,
                }),
                ..ExecutionUsage::default()
            },
            finished_at_ms: 1,
        };
        let opted_in = evidence_ready_graph(
            ExecutionNodeStatus::Cancelled,
            Some(result.clone()),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Cancelled],
            vec![AcceptanceVerdict::Unsatisfied],
            false,
        );
        let predecessors = dependency_predecessors(&opted_in, &opted_in.nodes[1]);
        assert_eq!(
            dependency_target(
                &opted_in.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Ready)
        );

        let excluded = evidence_ready_graph(
            ExecutionNodeStatus::Cancelled,
            Some(result),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Completed],
            vec![AcceptanceVerdict::Unsatisfied],
            false,
        );
        let predecessors = dependency_predecessors(&excluded, &excluded.nodes[1]);
        assert_eq!(
            dependency_target(
                &excluded.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Blocked)
        );
    }

    #[test]
    fn evidence_ready_committed_effect_is_required_only_when_declared() {
        let mut result = framework_failed_result();
        result
            .evidence_refs
            .retain(|reference| reference.evidence_ref.ref_type != "runtime_change");
        let required = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            Some(result.clone()),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            true,
        );
        let predecessors = dependency_predecessors(&required, &required.nodes[1]);
        assert_eq!(
            dependency_target(
                &required.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Blocked)
        );

        let optional = evidence_ready_graph(
            ExecutionNodeStatus::Failed,
            Some(result),
            vec![
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            false,
        );
        let predecessors = dependency_predecessors(&optional, &optional.nodes[1]);
        assert_eq!(
            dependency_target(
                &optional.nodes[1].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            Some(ExecutionNodeStatus::Ready)
        );
    }

    #[test]
    fn evidence_ready_minimum_keeps_waiting_while_any_lane_can_still_satisfy() {
        let mut graph = evidence_ready_graph(
            ExecutionNodeStatus::Running,
            None,
            vec![
                TerminalFactKind::CommittedEffect,
                TerminalFactKind::ObservedEvidence,
                TerminalFactKind::AcceptanceVerdict,
            ],
            vec![ExecutionNodeStatus::Failed],
            vec![AcceptanceVerdict::FrameworkInvalid],
            false,
        );
        let mut second_source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        second_source.id = "second".to_string();
        graph.nodes.insert(1, second_source);
        graph
            .node_statuses
            .insert("second".to_string(), ExecutionNodeStatus::Failed);
        graph
            .node_results
            .insert("second".to_string(), framework_failed_result());
        graph.edges.push(ExecutionEdge {
            from: "second".to_string(),
            to: "reviewer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        let dependency = &mut graph.nodes[2].work.as_mut().unwrap().dependency;
        if let ExecutionDependencyPolicy::EvidenceReady { predicate, .. } = dependency {
            let DependencyPredicate::EvidenceReady { minimum, .. } = predicate;
            *minimum = 2;
        }
        let predecessors = dependency_predecessors(&graph, &graph.nodes[2]);
        assert_eq!(
            dependency_target(
                &graph.nodes[2].work.as_ref().unwrap().dependency,
                &predecessors,
            ),
            None
        );
    }

    #[test]
    fn completed_predecessor_without_required_evidence_does_not_satisfy_quorum() {
        let mut graph = ExecutionGraph::new("verified quorum");
        let mut source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
        source.id = "source".to_string();
        graph.nodes.push(source);
        graph
            .node_statuses
            .insert("source".to_string(), ExecutionNodeStatus::Completed);
        graph.node_results.insert(
            "source".to_string(),
            harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("answer".to_string()),
                wait_class: None,
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            },
        );

        assert_eq!(
            verified_predecessor_status(&graph, "source", &["proof".to_string()]),
            ExecutionNodeStatus::Failed
        );
    }

    #[test]
    fn ready_quorum_cancels_only_optional_tail_in_the_same_group() {
        let mut graph = ExecutionGraph::new("cancel redundant evidence lane");
        let mut completed =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "completed");
        completed.id = "completed".to_string();
        let mut completed_work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        completed_work.required = false;
        completed_work.cancellation_group = Some("evidence".to_string());
        completed.work = Some(completed_work);
        let mut running = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "running");
        running.id = "running".to_string();
        let mut running_work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        running_work.required = false;
        running_work.cancellation_group = Some("evidence".to_string());
        running.work = Some(running_work);
        let mut merge = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "merge");
        merge.id = "merge".to_string();
        let mut merge_work = ExecutionWorkContract::new(ExecutionWorkRole::Synthesize);
        merge_work.dependency = ExecutionDependencyPolicy::Quorum {
            minimum: 1,
            cancel_remaining: true,
        };
        merge_work.cancellation_group = Some("evidence".to_string());
        merge.work = Some(merge_work);
        graph.nodes = vec![completed, running, merge];
        graph.edges = vec![
            ExecutionEdge {
                from: "completed".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "running".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];
        graph
            .node_statuses
            .insert("completed".to_string(), ExecutionNodeStatus::Completed);
        graph
            .node_statuses
            .insert("running".to_string(), ExecutionNodeStatus::Running);
        graph
            .node_statuses
            .insert("merge".to_string(), ExecutionNodeStatus::Ready);
        assert_eq!(quorum_tail_cancellations(&graph), vec!["running"]);
        let mut unknown = graph.nodes.iter().find(|node| node.id == "running").unwrap().clone();
        unknown.id = "unknown".into();
        graph.nodes.push(unknown);
        graph.node_statuses.insert("unknown".into(), ExecutionNodeStatus::Running);
        graph.edges.push(ExecutionEdge { from: "unknown".into(), to: "merge".into(), kind: ExecutionEdgeKind::DependsOn });
        let prohibited = unknown_dependency_nodes(&graph, &std::collections::BTreeSet::from(["unknown".to_string()]));
        assert!(prohibited.contains("merge"));
        assert!(quorum_tail_cancellations_excluding(&graph, &prohibited).is_empty());
    }
}

/// The custody closure examples for the original call custody criterion.
///
/// The Pending callback branch must reach a canonical original fact through
/// the reused owner-scoped cleanup and the reused physical drain before it
/// judges the custody unknown. A real retirement receipt closes it, an
/// unconfirmed original stays refused, and a foreign owner stays refused.
#[cfg(test)]
mod custody_closure_tests {
    use super::*;
    use super::super::executors::{ScopedNodeBackend, ScopedNodeBackendResolver};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// One original tool-batch backend. Its physical stop is the point where
    /// the canonical retirement receipt of its own node can land.
    struct CustodyBackend {
        stops: AtomicUsize,
        retired: AtomicBool,
        retire_on_stop: bool,
        services: Arc<crate::RuntimeServices>,
        identity: StdMutex<Option<(String, String)>>,
        stopped: Notify,
        release: Notify,
    }

    impl CustodyBackend {
        fn retire(&self) {
            if self.retired.swap(true, Ordering::SeqCst) { return; }
            let identity = self.identity.lock().unwrap().clone();
            let Some((graph_id, node_id)) = identity else { return; };
            let Ok(graph) = self.services.graph_state_store().load(&graph_id) else { return; };
            let command = ExecutionGraphCommand::CancelNode {
                expected_revision: graph.revision,
                node_id,
                reason: "original custody closure receipt".into(),
            };
            let _ = self.services.commit_service().apply_command(&graph, &command);
        }
    }

    #[async_trait::async_trait]
    impl ScopedNodeBackend for CustodyBackend {
        fn request_stop(&self, _ticket: &NodeExecutionTicket) {
            self.stops.fetch_add(1, Ordering::SeqCst);
            if self.retire_on_stop { self.retire(); }
            self.stopped.notify_one();
        }
        async fn execute(
            &self,
            _ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            // The original physical effect stays in flight until the test
            // releases it, so the drain really has to wait for it.
            self.release.notified().await;
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("custody closure original".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }

    /// A foreign executor never owns the original custody.
    struct ForeignCustodyExecutor;

    #[async_trait::async_trait]
    impl NodeExecutor for ForeignCustodyExecutor {
        fn kind(&self) -> &str {
            "foreign-custody-executor"
        }
        fn validate(
            &self,
            _: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), NodeExecutorError> {
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
            _: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("foreign custody executor".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }))
        }
    }

    struct CustodyResolver {
        backend: Arc<CustodyBackend>,
    }

    impl ScopedNodeBackendResolver for CustodyResolver {
        fn resolve(&self, _ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
            Some(Arc::clone(&self.backend) as Arc<dyn ScopedNodeBackend>)
        }
    }

    async fn custody_prepared(retire_on_stop: bool) -> (
        tempfile::TempDir,
        Arc<crate::RuntimeServices>,
        ExecutionGraphRunner,
        String,
        ExecutionPreparedPumpNode,
        Arc<CustodyBackend>,
    ) {
        let workspace = tempfile::tempdir().unwrap();
        let services = crate::RuntimeServices::in_memory().unwrap();
        let backend = Arc::new(CustodyBackend {
            stops: AtomicUsize::new(0),
            retired: AtomicBool::new(false),
            retire_on_stop,
            services: Arc::clone(&services),
            identity: StdMutex::new(None),
            stopped: Notify::new(),
            release: Notify::new(),
        });
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(CustodyResolver { backend: Arc::clone(&backend) }));
        let runner = ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            Arc::new(ExecutionResourceManager::new([])),
            Arc::new(ScopeLockManager::persistent(workspace.path().join("scopes")).unwrap()),
            Arc::new(WorktreeLeaseManager::open(workspace.path().join("leases.json")).unwrap()),
            "custody-closure",
            workspace.path(),
        );
        let mut graph = ExecutionGraph::new("original custody closure");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-custody".into(),
            turn_id: "turn-custody".into(),
            root_task_id: "task-custody".into(),
            task_id: "task-custody".into(),
            generation: 1,
        });
        let payload = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"session-custody","step_snapshots":{},
            "calls":[{"id":"custody-call","name":"read_file","input":"{}","depends_on":[]}]
        }))
        .unwrap();
        graph
            .nodes
            .push(harness_contract::execution_graph::ExecutionNodeSpec::new(
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
                "tool_batch",
                payload,
            ));
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let mut prepared = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        let prepared = prepared.remove(0);
        *backend.identity.lock().unwrap() =
            Some((registered.id.clone(), prepared.node.id.clone()));
        (workspace, services, runner, registered.id, prepared, backend)
    }

    fn pending_unknown(error: &ExecutionRunnerError) -> bool {
        matches!(error, ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown {
            reason, ..
        }) if reason == "callback has no canonical original outcome or retirement")
    }

    /// Positive example: a real retirement receipt closes the custody.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn custody_pending_callback_closes_on_the_real_retirement_receipt() {
        let (_workspace, services, runner, graph_id, prepared, backend) =
            custody_prepared(true).await;
        let binding = prepared.binding.clone().unwrap();
        let node_id = prepared.node.id.clone();
        let original = {
            let runner = runner.clone();
            let graph = graph_id.clone();
            let node = prepared.node.clone();
            let binding = binding.clone();
            tokio::spawn(async move { runner.start_and_execute_node(&graph, node, Some(&binding)).await })
        };
        // The original owner is live and its physical effect is in flight.
        let (ticket, executor) = loop {
            let active = runner.active.lock().await;
            if let Some(owner) = active.get(&(graph_id.clone(), node_id.clone())) {
                break (owner.ticket.clone(), Arc::clone(&owner.executor));
            }
            drop(active);
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        // Neither a committed outcome nor a retirement record exists yet.
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(services
            .graph_state_store()
            .binding_retirement_record(&graph, &node_id, &binding)
            .unwrap()
            .is_none());
        assert_eq!(
            runner.commit_service.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
            CallbackDisposition::Pending
        );
        // The Pending criterion must try the reused cleanup and drain before
        // it judges the custody unknown.
        let callback = {
            let runner = runner.clone();
            let graph = graph_id.clone();
            let ticket = ticket.clone();
            let executor = Arc::clone(&executor);
            tokio::spawn(async move {
                runner.complete_callback_observed_until(
                    &graph,
                    &executor,
                    &ticket,
                    None,
                    std::time::Instant::now() + Duration::from_secs(30),
                ).await
            })
        };
        // The drain asks the original physical effect to stop.
        tokio::time::timeout(Duration::from_secs(10), backend.stopped.notified())
            .await
            .unwrap();
        assert!(backend.stops.load(Ordering::SeqCst) >= 1);
        // The canonical retirement receipt is durable now.
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(services
            .graph_state_store()
            .binding_retirement_record(&graph, &node_id, &binding)
            .unwrap()
            .is_some());
        assert_eq!(
            runner.commit_service.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
            CallbackDisposition::Abort
        );
        // The original physical effect may finish now.
        backend.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(20), callback).await.unwrap().unwrap();
        assert!(!result.as_ref().err().is_some_and(pending_unknown),
            "the real receipt must close the custody: {result:?}");
        let _ = original.await;
    }

    /// Negative example: an unconfirmed original stays refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn custody_pending_callback_still_refuses_an_unconfirmed_original() {
        let (_workspace, services, runner, graph_id, prepared, backend) =
            custody_prepared(false).await;
        let binding = prepared.binding.clone().unwrap();
        let node_id = prepared.node.id.clone();
        let original = {
            let runner = runner.clone();
            let graph = graph_id.clone();
            let node = prepared.node.clone();
            let binding = binding.clone();
            tokio::spawn(async move { runner.start_and_execute_node(&graph, node, Some(&binding)).await })
        };
        let (ticket, executor) = loop {
            let active = runner.active.lock().await;
            if let Some(owner) = active.get(&(graph_id.clone(), node_id.clone())) {
                break (owner.ticket.clone(), Arc::clone(&owner.executor));
            }
            drop(active);
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let callback = {
            let runner = runner.clone();
            let graph = graph_id.clone();
            let ticket = ticket.clone();
            let executor = Arc::clone(&executor);
            tokio::spawn(async move {
                runner.complete_callback_observed_until(
                    &graph,
                    &executor,
                    &ticket,
                    None,
                    std::time::Instant::now() + Duration::from_secs(30),
                ).await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), backend.stopped.notified())
            .await
            .unwrap();
        // The original never reaches a canonical fact.
        backend.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(20), callback).await.unwrap().unwrap();
        let error = result.expect_err("an unconfirmed original stays unknown custody");
        assert!(pending_unknown(&error), "the unconfirmed original must stay refused: {error:?}");
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(services
            .graph_state_store()
            .binding_retirement_record(&graph, &node_id, &binding)
            .unwrap()
            .is_none());
        assert_eq!(
            runner.commit_service.binding_callback_disposition(&graph, &node_id, &binding).unwrap(),
            CallbackDisposition::Pending
        );
        let _ = original.await;
    }

    /// Negative example: a foreign owner stays refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn custody_pending_callback_still_refuses_a_foreign_owner() {
        let (_workspace, _services, runner, graph_id, prepared, backend) =
            custody_prepared(true).await;
        let binding = prepared.binding.clone().unwrap();
        let node_id = prepared.node.id.clone();
        let original = {
            let runner = runner.clone();
            let graph = graph_id.clone();
            let node = prepared.node.clone();
            let binding = binding.clone();
            tokio::spawn(async move { runner.start_and_execute_node(&graph, node, Some(&binding)).await })
        };
        let (ticket, executor) = loop {
            let active = runner.active.lock().await;
            if let Some(owner) = active.get(&(graph_id.clone(), node_id.clone())) {
                break (owner.ticket.clone(), Arc::clone(&owner.executor));
            }
            drop(active);
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        // A foreign ticket is not the admitted original owner.
        let mut foreign = ticket.clone();
        foreign.attempt = foreign.attempt.saturating_add(1);
        let error = runner
            .complete_callback_observed_until(
                &graph_id,
                &executor,
                &foreign,
                None,
                std::time::Instant::now() + Duration::from_secs(30),
            )
            .await
            .expect_err("a foreign ticket never owns the original custody");
        assert!(matches!(error, ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { .. })),
            "the foreign owner must stay refused: {error:?}");
        // A foreign executor is refused the same way.
        let foreign_executor: Arc<dyn NodeExecutor> = Arc::new(ForeignCustodyExecutor);
        let error = runner
            .complete_callback_observed_until(
                &graph_id,
                &foreign_executor,
                &ticket,
                None,
                std::time::Instant::now() + Duration::from_secs(30),
            )
            .await
            .expect_err("a foreign executor never owns the original custody");
        assert!(matches!(error, ExecutionRunnerError::Executor(NodeExecutorError::OriginalCustodyUnknown { .. })),
            "the foreign executor must stay refused: {error:?}");
        assert_eq!(backend.stops.load(Ordering::SeqCst), 0);
        backend.release.notify_one();
        let _ = original.await;
    }
}
