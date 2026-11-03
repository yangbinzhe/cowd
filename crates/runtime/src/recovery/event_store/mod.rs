//! Durable, transactional runtime lifecycle event store.
//!
//! A committed transaction is the only externally visible write unit. Graph,
//! node, goal, agent, team, and mission projections therefore observe one
//! monotonic commit cursor and never a partially appended multi-stream update.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex, OnceLock,
};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const SCOPE_REPLAY_PAGE_SIZE: usize = 1_024;
const EVENT_SCHEMA_VERSION: u32 = 1;
const MAX_TRANSACTION_EVENTS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;
const SESSION_TERMINAL_ARTIFACT_REF_PREFIX: &str = "terminal_artifact_v1:";
/// Latest Session terminal artifact payload schema emitted by Runtime.
///
/// Gateway consumers accept every positive version through this value so a
/// writer/reader rollout cannot drift through duplicated numeric literals.
pub const SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION: u64 = 3;

thread_local! {
    static PROJECTION_WORK_CLASS: Cell<Option<RuntimeProjectionWorkClass>> =
        const { Cell::new(None) };
}

pub(crate) mod content;
mod ports;
pub use content::QuiescedContentRoots;
pub use ports::*;
mod domain;
pub use domain::{
    request_hash as runtime_event_request_hash,
    request_hash_with_terminal as runtime_event_request_hash_with_terminal,
    validate_decision_lease_claims as validate_runtime_decision_lease_claims,
    validate_event as validate_runtime_event,
    validate_fenced_terminal as validate_runtime_fenced_terminal,
    validate_transaction as validate_runtime_event_transaction,
};
mod ephemeral;
pub use ephemeral::EphemeralRuntimeEventStore;

/// The sole Runtime-facing durable event-store API. Runtime callers depend on
/// lifecycle semantics rather than a concrete database, path, pragma, or SQL
/// schema. Backend adapters are composed explicitly at the trusted host root.
#[derive(Debug)]
pub struct RuntimeEventStore {
    repetition_namespace: String,
    session_recovery_exclusions: std::sync::RwLock<BTreeSet<String>>,
    artifacts: OnceLock<Arc<crate::ArtifactStore>>,
    backend: Arc<dyn RuntimeEventStoreBackend>,
    commit_signal: tokio::sync::watch::Sender<u64>,
    /// Monotonic time of the latest non-projection commit. Maintenance lanes
    /// use this signal to yield during a foreground burst without reducing
    /// their idle catch-up throughput or starving indefinitely.
    last_foreground_commit_ms: AtomicU64,
    /// Per-stream serialization for the read-revision-then-append window.
    /// One stream (for example `session:<id>`) is written by many Runtime
    /// tasks (model events, early-tool authorizations, approval grants), so
    /// the optimistic-CAS append can race with itself. A per-stream
    /// in-process lock removes the TOCTOU window while keeping different
    /// streams fully parallel. Guards are acquired in sorted stream order so
    /// multi-stream transactions can never deadlock.
    stream_locks: StdMutex<std::collections::HashMap<String, Arc<StdMutex<()>>>>,
    retained_checked_members: StdMutex<std::collections::BTreeMap<String, RetainedMemberEntry>>,
    #[cfg(any(test, feature = "test-fixtures"))]
    read_fault: StdMutex<Option<TestReadFault>>,
    #[cfg(feature = "test-fixtures")]
    conclusion_probe: StdMutex<Option<FixtureConclusionProbe>>,
    #[cfg(test)]
    append_fault: StdMutex<Option<TestAppendFault>>,
}

#[cfg(feature = "test-fixtures")]
pub(crate) struct FixtureConclusionProbe {
    program_id: String,
    goal_id: String,
    pub(crate) hook: Box<dyn FnOnce(&AppendTransactionRequest) + Send>,
    pub(crate) observe: Box<dyn FnOnce(bool, Option<crate::execution_core::services::ConclusionAttemptObservation>) + Send>,
}

#[cfg(feature = "test-fixtures")]
impl std::fmt::Debug for FixtureConclusionProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixtureConclusionProbe").field("program_id", &self.program_id)
            .field("goal_id", &self.goal_id).finish_non_exhaustive()
    }
}

/// An in-process reservation by the original member owner. This is not a
/// cross-process admission decision and grants no policy or execution authority.
pub(crate) struct OriginalMemberReservation {
    owner: std::sync::Weak<RuntimeEventStore>,
    outer: String,
    index: u64,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    nonce: Arc<()>,
}

impl OriginalMemberReservation {
    /// Authenticate only the still-held in-process reservation. This does not
    /// establish durable first-execution eligibility or grant execution rights.
    pub(crate) fn matches_original(
        &self, store: &Arc<RuntimeEventStore>, outer: &str, index: u64,
        ticket: &crate::execution_core::graph::NodeExecutionTicket,
    ) -> bool {
        if !std::ptr::eq(self.owner.as_ptr(), Arc::as_ptr(store))
            || self.outer != outer || self.index != index || &self.ticket != ticket {
            return false;
        }
        let Ok(retained) = store.retained_checked_members.lock() else { return false; };
        matches!(retained.get(&original_member_reservation_key(outer, index)),
            Some(RetainedMemberEntry::Reserved { outer: current, index: current_index, nonce, .. })
                if current == outer && *current_index == index && Arc::ptr_eq(nonce, &self.nonce))
    }
}

/// Original effect-stream preparation append, retained even if its reply is
/// unknown. A decoded event or receipt cannot construct this owner handle.
#[derive(Debug)]
pub(crate) struct OriginalMemberEffectFence {
    owner: std::sync::Weak<RuntimeEventStore>,
    nonce: Arc<()>,
    outer: String,
    index: u64,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    request: Arc<AppendTransactionRequest>,
    receipt: StdMutex<Option<AppendTransactionReceipt>>,
    hook_entered: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
enum RetainedMemberEntry {
    Reserved { outer: String, index: u64, nonce: Arc<()>, effect: Option<Arc<OriginalMemberEffectFence>> },
    Checked(RetainedCheckedMember),
    FreshEffect { transaction: Arc<AppendTransactionRequest>, original: Arc<crate::RuntimeToolExecutionRequest>,
        owner: Option<Arc<crate::execution_core::graph::OriginalFreshEffectOwner>> },
}

fn original_member_reservation_key(outer: &str, index: u64) -> String {
    format!("original-member-reservation:{}:{outer}:{index}", outer.len())
}

fn original_member_identity(checked: &CheckedAppendRequest) -> RuntimeEventStoreResult<Option<(String, u64)>> {
    let mut markers = checked.transaction.events.iter().filter(|event| event.event.kind == "agentic.batch_member.index");
    let Some(marker) = markers.next() else { return Ok(None); };
    if markers.next().is_some() { return Err(RuntimeEventStoreError::InvalidTransaction("ambiguous member index".into())); }
    let outer = marker.event.payload["outer_key"].as_str().filter(|value| !value.is_empty())
        .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("member outer missing".into()))?;
    let index = marker.event.payload["member_index"].as_u64()
        .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("member index missing".into()))?;
    Ok(Some((outer.to_owned(), index)))
}

/// Original transaction and control remain owned while its commit outcome is unknown.
struct RetainedCheckedMember {
    request: Arc<CheckedAppendRequest>,
    control: Arc<crate::conversation::MemberCommitControl>,
    authority: Option<Arc<crate::authorization_negotiator::PreparedMemberAuthority>>,
    effect_fence: Option<Arc<OriginalMemberEffectFence>>,
    conclusion_proof: Option<Arc<crate::execution_core::goal::OriginalConclusionProofCustody>>,
    conclusion_receipt: Option<CheckedAppendReceipt>,
    conclusion_published: bool,
    terminal: Option<SessionTerminalInput>,
    terminal_owner: Option<Arc<crate::execution_core::services::OriginalTerminalCommitOwner>>,
}
impl std::fmt::Debug for RetainedCheckedMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedCheckedMember").field("request", &self.request)
            .field("has_authority", &self.authority.is_some()).finish_non_exhaustive()
    }
}

/// The exact post-validation command and receipt returned by this store's
/// original checked append. Private fields prevent a decoded receipt from
/// manufacturing same-owner committed authority.
pub(crate) struct OriginalCheckedAppend {
    request: Arc<CheckedAppendRequest>,
    receipt: CheckedAppendReceipt,
    authority: Option<Arc<crate::authorization_negotiator::PreparedMemberAuthority>>,
}
impl OriginalCheckedAppend {
    pub(crate) fn into_parts(self) -> (Arc<CheckedAppendRequest>, CheckedAppendReceipt, Option<Arc<crate::authorization_negotiator::PreparedMemberAuthority>>) {
        (self.request, self.receipt, self.authority)
    }
}

/// A retained checked request whose exact commit was verified by the original
/// store owner after an unknown return. No checked validation evidence is
/// synthesized by this receipt read.
pub(crate) struct RecoveredOriginalCheckedAppend {
    pub(crate) request: Arc<CheckedAppendRequest>,
    pub(crate) receipt: AppendTransactionReceipt,
    pub(crate) authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
}

/// Proof scoped to the synchronous owner closure that holds these stream locks.
/// Fields are private so a caller cannot manufacture lock ownership.
pub(crate) struct HeldStreamLocks<'a> {
    owner: &'a RuntimeEventStore,
    streams: BTreeSet<String>,
}

#[cfg(test)]
#[derive(Debug)]
struct TestAppendFault { transaction_id: String, after_commit: bool }

#[cfg(any(test, feature = "test-fixtures"))]
struct TestReadFault {
    stream: String,
    operation: &'static str,
    remaining: usize,
    #[cfg(feature = "test-fixtures")]
    selector: Option<FixtureGoalReadSelector>,
    #[cfg(feature = "test-fixtures")]
    predicate: Option<Arc<dyn Fn(&RuntimeEventStore, &str, &str, u32) -> bool + Send + Sync>>,
    #[cfg(feature = "test-fixtures")]
    phase_action: Option<Box<dyn FnOnce(&RuntimeEventStore, &str, &str, u32) + Send>>,
    action: Option<Box<dyn FnOnce(&RuntimeEventStore) + Send>>,
}

// A thread-local marker is valid only for the synchronous original projection.
#[cfg(feature = "test-fixtures")]
#[derive(Clone, PartialEq, Eq)]
struct FixtureGoalReadPhase { graph: String, node: String, attempt: u32, stream: String, call: String }
#[cfg(feature = "test-fixtures")]
#[derive(Clone, PartialEq, Eq)]
struct FixtureGoalReadSelector { graph: String, stream: String, call: String }
#[cfg(feature = "test-fixtures")]
thread_local! {
    static FIXTURE_GOAL_READ_PHASE: std::cell::RefCell<Option<FixtureGoalReadPhase>> = const { std::cell::RefCell::new(None) };
}
#[cfg(feature = "test-fixtures")]
struct FixtureGoalReadGuard(Option<FixtureGoalReadPhase>);
#[cfg(feature = "test-fixtures")]
impl Drop for FixtureGoalReadGuard {
    fn drop(&mut self) { FIXTURE_GOAL_READ_PHASE.with(|phase| *phase.borrow_mut() = self.0.take()); }
}
#[cfg(feature = "test-fixtures")]
fn fixture_goal_read_scope<T>(phase: Option<FixtureGoalReadPhase>, f: impl FnOnce() -> T) -> T {
    let old = FIXTURE_GOAL_READ_PHASE.with(|slot| slot.replace(phase));
    let _guard = FixtureGoalReadGuard(old);
    f()
}

#[cfg(any(test, feature = "test-fixtures"))]
impl std::fmt::Debug for TestReadFault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("TestReadFault")
            .field("stream", &self.stream)
            .field("operation", &self.operation)
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

impl RuntimeEventStore {
    /// Inject an I/O failure at this store's transaction boundary. After-commit
    /// injection runs the real backend first and deliberately loses its reply.
    #[cfg(test)]
    pub(crate) fn inject_append_fault_once(&self, transaction_id: String, after_commit: bool) {
        let mut fault = self.append_fault.lock().unwrap();
        assert!(fault.is_none(), "append fault already installed");
        *fault = Some(TestAppendFault { transaction_id, after_commit });
    }

    #[cfg(test)]
    pub(crate) fn append_fault_is_pending(&self, transaction_id: &str) -> bool {
        self.append_fault.lock().unwrap().as_ref().is_some_and(|fault| fault.transaction_id == transaction_id)
    }

    #[cfg(test)]
    fn take_append_fault(&self, transaction_id: &str, after_commit: bool) -> RuntimeEventStoreResult<()> {
        let mut fault = self.append_fault.lock().unwrap();
        if fault.as_ref().is_some_and(|fault| fault.transaction_id == transaction_id && fault.after_commit == after_commit) {
            fault.take();
            return Err(RuntimeEventStoreError::Io(std::io::Error::other("injected transaction reply failure")));
        }
        Ok(())
    }

    /// Inspect the actual backend instance in fixture tests, not its configured name.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn fixture_backend_is<T: RuntimeEventStoreBackend + 'static>(&self) -> bool {
        (&*self.backend as &dyn std::any::Any).is::<T>()
    }
    #[cfg(feature = "test-fixtures")]
    pub(crate) fn fixture_arm_original_conclusion(
        &self, program_id: String, goal_id: String,
        hook: impl FnOnce(&AppendTransactionRequest) + Send + 'static,
        observe: impl FnOnce(bool, Option<crate::execution_core::services::ConclusionAttemptObservation>) + Send + 'static,
    ) -> Result<(), String> {
        if program_id.trim().is_empty() || goal_id.trim().is_empty() {
            return Err("original conclusion fixture identity missing".into());
        }
        let mut slot = self.conclusion_probe.lock().map_err(|error| error.to_string())?;
        if slot.is_some() { return Err("original conclusion fixture already armed".into()); }
        *slot = Some(FixtureConclusionProbe { program_id, goal_id, hook: Box::new(hook), observe: Box::new(observe) });
        Ok(())
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn fixture_take_original_conclusion(&self, program_id: &str, goal_id: &str)
        -> Result<Option<FixtureConclusionProbe>, String> {
        let mut slot = self.conclusion_probe.lock().map_err(|error| error.to_string())?;
        if slot.as_ref().is_some_and(|probe| probe.program_id == program_id && probe.goal_id == goal_id) {
            Ok(slot.take())
        } else { Ok(None) }
    }

    /// One-shot fault at the actual store read boundary, scoped to one store
    /// instance, stream, operation and ordinal. The action runs outside the
    /// hook lock and may append durable events through the original backend.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn inject_read_fault_once(
        &self, stream: String, operation: &'static str, ordinal: usize,
        action: impl FnOnce(&RuntimeEventStore) + Send + 'static,
    ) {
        assert!(ordinal > 0);
        #[cfg(not(test))]
        assert_eq!(operation, "stream_revision", "fixture hook only supports stream_revision");
        let mut fault = self.read_fault.lock().unwrap();
        assert!(fault.is_none(), "read fault already installed");
        *fault = Some(TestReadFault { stream, operation, remaining: ordinal,
            #[cfg(feature = "test-fixtures")] selector: None,
            #[cfg(feature = "test-fixtures")] predicate: None,
            #[cfg(feature = "test-fixtures")] phase_action: None,
            action: Some(Box::new(action)) });
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn fixture_arm_original_goal_read(
        &self, graph: String, call: String, stream: String,
        predicate: impl Fn(&RuntimeEventStore, &str, &str, u32) -> bool + Send + Sync + 'static,
        action: impl FnOnce(&RuntimeEventStore, &str, &str, u32) + Send + 'static,
    ) {
        let mut fault = self.read_fault.lock().unwrap();
        assert!(fault.is_none(), "read fault already installed");
        let selector = FixtureGoalReadSelector { graph, stream: stream.clone(), call };
        *fault = Some(TestReadFault { stream, operation: "stream_revision", remaining: 1,
            selector: Some(selector), predicate: Some(Arc::new(predicate)), phase_action: Some(Box::new(action)), action: None });
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn fixture_scope_original_goal_read<T>(
        &self, ticket: &crate::execution_core::graph::NodeExecutionTicket, stream: &str, f: impl FnOnce() -> T,
    ) -> T {
        let selector = self.read_fault.lock().unwrap().as_ref().and_then(|fault| fault.selector.clone());
        let phase = selector.and_then(|selector| {
            if selector.graph != ticket.graph_id || selector.stream != stream || ticket.node_id.is_empty() || ticket.attempt == 0 { return None; }
            let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref).ok()?;
            if calls.iter().filter(|call| call.id == selector.call).count() != 1 { return None; }
            Some(FixtureGoalReadPhase { graph: ticket.graph_id.clone(), node: ticket.node_id.clone(), attempt: ticket.attempt,
                stream: stream.into(), call: selector.call })
        });
        fixture_goal_read_scope(phase, f)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn after_test_read(&self, stream: &str, operation: &'static str) {
        #[cfg(feature = "test-fixtures")]
        let conditional = {
            let phase = FIXTURE_GOAL_READ_PHASE.with(|slot| slot.borrow().clone());
            let guard = self.read_fault.lock().unwrap();
            guard.as_ref().and_then(|fault| {
                let selector = fault.selector.as_ref()?;
                let phase = phase.as_ref()?;
                if fault.stream == stream && fault.operation == operation && selector.graph == phase.graph
                    && selector.stream == phase.stream && selector.call == phase.call {
                    fault.predicate.clone().map(|predicate| (predicate, phase.clone()))
                } else { None }
            })
        };
        #[cfg(feature = "test-fixtures")]
        if let Some((predicate, phase)) = conditional {
            fixture_goal_read_scope(None, || {
                if !predicate(self, &phase.graph, &phase.node, phase.attempt) { return; }
                let action = {
                    let mut guard = self.read_fault.lock().unwrap();
                    if guard.as_ref().and_then(|fault| fault.predicate.as_ref()).is_some_and(|actual| Arc::ptr_eq(actual, &predicate)) {
                        guard.take().and_then(|fault| fault.phase_action)
                    } else { None }
                };
                if let Some(action) = action { action(self, &phase.graph, &phase.node, phase.attempt); }
            });
            return;
        }
        let action = {
            let mut guard = self.read_fault.lock().unwrap();
            match guard.as_mut() {
                Some(fault) if fault.stream == stream && fault.operation == operation
                    && {
                        #[cfg(feature = "test-fixtures")]
                        { fault.selector.is_none() }
                        #[cfg(not(feature = "test-fixtures"))]
                        { true }
                    } => {
                    fault.remaining -= 1;
                    if fault.remaining == 0 { guard.take().and_then(|fault| fault.action) } else { None }
                }
                _ => None,
            }
        };
        if let Some(action) = action { action(self); }
    }
    pub(crate) fn repetition_scope(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> crate::repetition_guard::RepetitionScope {
        crate::repetition_guard::RepetitionScope::turn(session_id, turn_id)
            .in_namespace(&self.repetition_namespace)
    }

    pub(crate) fn repetition_execution_scope(
        &self,
        session_id: &str,
        execution_id: &str,
    ) -> crate::repetition_guard::RepetitionScope {
        crate::repetition_guard::RepetitionScope::execution(session_id, execution_id)
            .in_namespace(&self.repetition_namespace)
    }

    /// Construct an explicit process-local test backend.
    #[must_use]
    pub fn for_test() -> Self {
        Self::from_backend(Arc::new(EphemeralRuntimeEventStore::new()))
    }

    pub fn run_projection_work<T>(
        &self,
        class: RuntimeProjectionWorkClass,
        work: impl FnOnce() -> T,
    ) -> T {
        PROJECTION_WORK_CLASS.with(|slot| {
            let previous = slot.replace(Some(class));
            struct Restore<'a> {
                slot: &'a Cell<Option<RuntimeProjectionWorkClass>>,
                previous: Option<RuntimeProjectionWorkClass>,
            }
            impl Drop for Restore<'_> {
                fn drop(&mut self) {
                    self.slot.set(self.previous);
                }
            }
            let _restore = Restore { slot, previous };
            work()
        })
    }

    #[must_use]
    pub fn current_projection_work_class() -> Option<RuntimeProjectionWorkClass> {
        PROJECTION_WORK_CLASS.with(Cell::get)
    }

    #[must_use]
    pub fn from_backend(backend: Arc<dyn RuntimeEventStoreBackend>) -> Self {
        let latest_cursor = backend
            .all_events(1)
            .ok()
            .and_then(|events| events.first().map(|event| event.commit_cursor))
            .unwrap_or_default();
        let (commit_signal, _) = tokio::sync::watch::channel(latest_cursor);
        Self {
            repetition_namespace: uuid::Uuid::new_v4().to_string(),
            backend,
            session_recovery_exclusions: std::sync::RwLock::new(BTreeSet::new()),
            artifacts: OnceLock::new(),
            commit_signal,
            last_foreground_commit_ms: AtomicU64::new(0),
            stream_locks: StdMutex::new(std::collections::HashMap::new()),
            retained_checked_members: StdMutex::new(std::collections::BTreeMap::new()),
            #[cfg(any(test, feature = "test-fixtures"))]
            read_fault: StdMutex::new(None),
            #[cfg(feature = "test-fixtures")]
            conclusion_probe: StdMutex::new(None),
            #[cfg(test)]
            append_fault: StdMutex::new(None),
        }
    }

    pub(crate) fn has_session_recovery_exclusions(&self) -> bool {
        !self
            .session_recovery_exclusions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    pub(crate) fn session_recovery_exclusions(&self) -> Vec<String> {
        self.session_recovery_exclusions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    pub(crate) fn replace_session_recovery_exclusions(&self, excluded: BTreeSet<String>) {
        *self
            .session_recovery_exclusions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = excluded;
    }

    pub(crate) fn is_session_recovery_excluded(&self, session_id: &str) -> bool {
        self.session_recovery_exclusions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(session_id)
    }

    pub(crate) fn ensure_session_recovery_admitted(&self, session_id: &str) -> Result<(), String> {
        if self.is_session_recovery_excluded(session_id) {
            return Err(format!("session_recovery_quarantined:{session_id}"));
        }
        Ok(())
    }

    fn ensure_graph_transaction_recovery_admitted(
        &self,
        request: &AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<()> {
        if !self.has_session_recovery_exclusions() {
            return Ok(());
        }
        for input in &request.events {
            if !matches!(
                input.event.scope,
                RuntimeEventScope::ExecutionGraph | RuntimeEventScope::ExecutionNode
            ) {
                continue;
            }
            let activity = input.event.activity_binding();
            if let Some(binding) = &activity {
                binding.validate().map_err(|error| {
                    RuntimeEventStoreError::InvalidTransaction(error.to_string())
                })?;
                self.ensure_session_recovery_admitted(&binding.session_id)
                    .map_err(RuntimeEventStoreError::InvalidTransaction)?;
            }
            if input.event.scope == RuntimeEventScope::ExecutionGraph {
                if activity.is_none() {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "session_recovery_graph_activity_binding_missing".into(),
                    ));
                }
                continue;
            }
            let mut graphs = input
                .event
                .refs
                .iter()
                .filter(|reference| reference.kind == "execution_graph")
                .map(|reference| reference.id.clone())
                .collect::<BTreeSet<_>>();
            if let Some(binding) = &activity {
                graphs.insert(binding.root_execution_id.clone());
            }
            let governed_session = if input.event.actor.as_deref() == Some("governed_tool")
                && input
                    .event
                    .payload
                    .get("replay")
                    .is_some_and(|replay| !replay.is_null())
            {
                let replay = &input.event.payload["replay"];
                let origin: harness_contract::step::CallOrigin =
                    serde_json::from_value(replay["origin"].clone())?;
                let invocation: harness_contract::tool::GovernedToolInvocation =
                    serde_json::from_value(replay["invocation"].clone())?;
                let session = replay["session_id"]
                    .as_str()
                    .filter(|session| !session.trim().is_empty())
                    .ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction(
                            "session_recovery_tool_session_missing".into(),
                        )
                    })?;
                if invocation.origin.as_ref() != Some(&origin)
                    || invocation.invocation_id != origin.call_id
                    || replay["call_id"].as_str() != Some(origin.call_id.as_str())
                    || origin.origin_step_id.is_empty()
                    || origin.catalog_digest.is_empty()
                {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "session_recovery_tool_replay_mismatch".into(),
                    ));
                }
                self.ensure_session_recovery_admitted(session)
                    .map_err(RuntimeEventStoreError::InvalidTransaction)?;
                true
            } else {
                false
            };
            let original_intent;
            let owner = if input.event.kind == "execution.effect.receipt"
                && graphs.is_empty()
                && input.event.actor.as_deref() != Some("governed_tool")
            {
                let call = input
                    .idempotency_key
                    .as_deref()
                    .and_then(|key| key.strip_suffix(":receipt"))
                    .ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction(
                            "session_recovery_receipt_key_missing".into(),
                        )
                    })?;
                original_intent = self
                    .event_by_idempotency_key(&input.event.stream_id, &format!("{call}:intent"))?;
                let original = original_intent.as_ref().ok_or_else(|| {
                    RuntimeEventStoreError::InvalidTransaction(
                        "session_recovery_receipt_intent_missing".into(),
                    )
                })?;
                if original.stream_id != input.event.stream_id
                    || original.scope != RuntimeEventScope::ExecutionNode
                    || original.kind != "execution.effect.intent"
                    || original.schema_version != 1
                    || original.idempotency_key.as_deref()
                        != Some(format!("{call}:intent").as_str())
                    || original.actor != input.event.actor
                {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "session_recovery_receipt_intent_invalid".into(),
                    ));
                }
                &original.payload
            } else {
                &input.event.payload
            };
            if graphs.is_empty()
                && matches!(
                    input.event.kind.as_str(),
                    "execution.effect.intent" | "execution.effect.receipt"
                )
                && input.event.actor.as_deref() != Some("governed_tool")
            {
                let ticket: crate::execution_core::NodeExecutionTicket =
                    serde_json::from_value(owner.clone())?;
                graphs.insert(ticket.graph_id);
            }
            if graphs.is_empty() && !governed_session {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "session_recovery_node_owner_unknown".into(),
                ));
            }
            for graph_id in graphs {
                let in_transaction = request.events.iter().find(|event| {
                    event.event.scope == RuntimeEventScope::ExecutionGraph
                        && event.event.stream_id == graph_id
                });
                let original = if in_transaction.is_none() {
                    self.event_by_idempotency_key(&graph_id, &format!("{graph_id}:revision:1"))?
                } else {
                    None
                };
                let binding = match in_transaction {
                    Some(event) => event.event.activity_binding(),
                    None => original
                        .as_ref()
                        .filter(|event| {
                            event.stream_id == graph_id
                                && event.scope == RuntimeEventScope::ExecutionGraph
                                && event.sequence == 1
                                && event.kind == "execution_graph.planned"
                                && event.schema_version == 2
                        })
                        .and_then(|event| event.activity_binding()),
                }
                .ok_or_else(|| {
                    RuntimeEventStoreError::InvalidTransaction(
                        "session_recovery_original_graph_owner_missing".into(),
                    )
                })?;
                binding.validate().map_err(|error| {
                    RuntimeEventStoreError::InvalidTransaction(error.to_string())
                })?;
                self.ensure_session_recovery_admitted(&binding.session_id)
                    .map_err(RuntimeEventStoreError::InvalidTransaction)?;
            }
        }
        Ok(())
    }

    pub(crate) fn with_stream_locks<T>(
        &self,
        stream_ids: &[String],
        work: impl FnOnce() -> T,
    ) -> T {
        let unique = stream_ids
            .iter()
            .filter(|stream_id| !stream_id.is_empty())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut locks = self
            .stream_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        let arcs = unique
            .into_iter()
            .map(|stream_id| {
                Arc::clone(
                    locks
                        .entry(stream_id)
                        .or_insert_with(|| Arc::new(StdMutex::new(()))),
                )
            })
            .collect::<Vec<_>>();
        drop(locks);
        let _guards = arcs
            .iter()
            .map(|arc| {
                arc.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            })
            .collect::<Vec<_>>();
        work()
    }

    pub(crate) fn with_authenticated_stream_locks<T>(
        &self,
        stream_ids: &[String],
        work: impl FnOnce(&HeldStreamLocks<'_>) -> T,
    ) -> T {
        self.with_stream_locks(stream_ids, || {
            let locks = HeldStreamLocks {
                owner: self,
                streams: stream_ids.iter().filter(|id| !id.is_empty()).cloned().collect(),
            };
            work(&locks)
        })
    }

    /// Runs `work` while holding the per-stream in-process write lock. The
    /// lock covers the whole read-revision-then-append window, so callers that
    /// first read `stream_revision` and then append can never be interrupted
    /// by another in-process writer on the same stream.
    pub fn with_stream_lock<T>(&self, stream_id: &str, work: impl FnOnce() -> T) -> T {
        self.with_stream_locks(&[stream_id.to_string()], work)
    }

    pub fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        let mut validation = AppendTransactionRequest {
            transaction_id: "append-validation".into(),
            expected_streams: Vec::new(),
            events: vec![RuntimeTransactionEventInput {
                event: input,
                idempotency_key: None,
                schema_version: 1,
            }],
        };
        self.ensure_graph_transaction_recovery_admitted(&validation)
            .map_err(|error| error.to_string())?;
        self.validate_content_publication(&mut validation)
            .map_err(|error| error.to_string())?;
        let input = validation.events.remove(0).event;
        let stream_id = input.stream_id.clone();
        let event = self.with_stream_lock(&stream_id, || self.backend.append(input))?;
        self.publish_commit(event.commit_cursor);
        Ok(event)
    }

    pub fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.ensure_graph_transaction_recovery_admitted(&request)?;
        let mut request = request;
        self.validate_content_publication(&mut request)?;
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        #[cfg(test)]
        let transaction_id = request.transaction_id.clone();
        #[cfg(test)]
        self.take_append_fault(&transaction_id, false)?;
        let receipt =
            self.with_stream_locks(&stream_ids, || self.backend.append_transaction(request))?;
        #[cfg(test)]
        self.take_append_fault(&transaction_id, true)?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    /// Uses the same admission/content validation and stream locks as the
    /// original transaction owner; unsupported backends fail closed.
    pub fn append_transaction_cancellable(
        &self, mut request: AppendTransactionRequest, control: &dyn RuntimeTransactionCommitControl,
    ) -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        self.ensure_graph_transaction_recovery_admitted(&request)?;
        self.validate_content_publication(&mut request)?;
        let streams = request.expected_streams.iter().map(|entry| entry.stream_id.clone()).collect::<Vec<_>>();
        let receipt = self.with_stream_locks(&streams, || self.backend.append_transaction_cancellable(request, control))?;
        if let Some(receipt) = &receipt { self.publish_commit(receipt.commit_cursor); }
        Ok(receipt)
    }

    pub fn append_checked_transaction(
        &self,
        mut request: CheckedAppendRequest,
        validator: &dyn CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<CheckedAppendReceipt> {
        self.ensure_graph_transaction_recovery_admitted(&request.transaction)?;
        self.validate_content_publication(&mut request.transaction)?;
        request.identity_hash()?;
        if request.frame_digest != validator.frame_digest() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "source validator frame mismatch".into(),
            ));
        }
        let streams = request
            .transaction
            .expected_streams
            .iter()
            .map(|source| source.stream_id.clone())
            .collect::<Vec<_>>();
        #[cfg(test)]
        let transaction_id = request.transaction.transaction_id.clone();
        #[cfg(test)]
        self.take_append_fault(&transaction_id, false)?;
        let receipt = self.with_stream_locks(&streams, || {
            self.backend.append_checked_transaction(request, validator)
        })?;
        #[cfg(test)]
        self.take_append_fault(&transaction_id, true)?;
        self.publish_commit(receipt.commit.commit_cursor);
        Ok(receipt)
    }

    /// A retained member authority is an unresolved original checked attempt.
    pub(crate) fn has_unresolved_original_member_custody(&self) -> RuntimeEventStoreResult<bool> {
        let retained = self.retained_checked_members.lock().map_err(|_|
            RuntimeEventStoreError::Corrupt("original member custody unavailable".into()))?;
        Ok(retained.values().any(|entry| match entry {
            RetainedMemberEntry::Checked(owner) => owner.authority.is_some(),
            RetainedMemberEntry::Reserved { effect, .. } => effect.is_some(),
            RetainedMemberEntry::FreshEffect { original, .. } => original.tool_name == "agent_action_batch",
        }))
    }

    /// Keep the original effect append and call before entering the backend;
    /// a lost reply or failed carrier installation is not a rollback.
    pub(crate) fn retain_original_first_effect(
        &self, transaction: Arc<AppendTransactionRequest>, original: Arc<crate::RuntimeToolExecutionRequest>,
    ) -> RuntimeEventStoreResult<()> {
        let key = format!("original-first-effect:{}", transaction.transaction_id);
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original effect owner unavailable".into()))?;
        if retained.contains_key(&key) || retained.len() >= 256 {
            return Err(RuntimeEventStoreError::InvalidTransaction("original effect owner already retained or full".into()));
        }
        retained.insert(key, RetainedMemberEntry::FreshEffect { transaction, original, owner: None });
        Ok(())
    }

    /// Borrow the original in-process custody after an uncertain effect append.
    /// Absence is unknown and never proves that the append rolled back.
    pub(crate) fn retained_original_first_effect(
        &self, transaction_id: &str,
    ) -> RuntimeEventStoreResult<Option<(Arc<AppendTransactionRequest>, Arc<crate::RuntimeToolExecutionRequest>, Option<Arc<crate::execution_core::graph::OriginalFreshEffectOwner>>)>> {
        let retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original effect owner unavailable".into()))?;
        Ok(match retained.get(&format!("original-first-effect:{transaction_id}")) {
            Some(RetainedMemberEntry::FreshEffect { transaction, original, owner }) => Some((Arc::clone(transaction), Arc::clone(original), owner.clone())),
            _ => None,
        })
    }

    pub(crate) fn publish_original_first_effect_owner(
        &self, transaction: &Arc<AppendTransactionRequest>, original: &Arc<crate::RuntimeToolExecutionRequest>,
        owner: Arc<crate::execution_core::graph::OriginalFreshEffectOwner>,
    ) -> RuntimeEventStoreResult<()> {
        let key = format!("original-first-effect:{}", transaction.transaction_id);
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original effect owner unavailable".into()))?;
        match retained.get_mut(&key) {
            Some(RetainedMemberEntry::FreshEffect { transaction: current, original: call, owner: slot })
                if Arc::ptr_eq(current, transaction) && Arc::ptr_eq(call, original) && slot.is_none() => { *slot = Some(owner); Ok(()) }
            _ => Err(RuntimeEventStoreError::InvalidTransaction("original first effect owner changed".into())),
        }
    }

    pub(crate) fn release_original_first_effect_owner(
        &self, transaction: &Arc<AppendTransactionRequest>, owner: &Arc<crate::execution_core::graph::OriginalFreshEffectOwner>,
    ) -> RuntimeEventStoreResult<()> {
        let key = format!("original-first-effect:{}", transaction.transaction_id);
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original effect owner unavailable".into()))?;
        match retained.get(&key) {
            Some(RetainedMemberEntry::FreshEffect { transaction: current, owner: Some(stored), .. })
                if Arc::ptr_eq(current, transaction) && Arc::ptr_eq(stored, owner) => { retained.remove(&key); Ok(()) }
            _ => Err(RuntimeEventStoreError::InvalidTransaction("original first effect owner changed".into())),
        }
    }

    /// Reserve one original member before invoking any hook or approval. A
    /// missing durable index does not itself establish that the member is new.
    /// The original producer must authenticate the outer call and use the
    /// backend/effect owner to establish cross-process admission separately.
    pub(crate) fn reserve_original_member(
        self: &Arc<Self>, outer: &str, index: u64, ticket: &crate::execution_core::graph::NodeExecutionTicket,
    ) -> RuntimeEventStoreResult<OriginalMemberReservation> {
        if outer.is_empty() { return Err(RuntimeEventStoreError::InvalidTransaction("original member outer missing".into())); }
        let mut retained = self.retained_checked_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in retained.values() {
            let matches = match entry {
                RetainedMemberEntry::Reserved { outer: current, index: current_index, .. } => current == outer && *current_index == index,
                RetainedMemberEntry::Checked(owner) => original_member_identity(&owner.request)?.is_some_and(|identity| identity == (outer.to_owned(), index)),
                RetainedMemberEntry::FreshEffect { .. } => false,
            };
            if matches { return Err(RuntimeEventStoreError::InvalidTransaction("original member owner already retained".into())); }
        }
        if retained.len() >= 256 { return Err(RuntimeEventStoreError::InvalidTransaction("checked member transaction custody capacity exhausted".into())); }
        let nonce = Arc::new(());
        let key = original_member_reservation_key(outer, index);
        if retained.contains_key(&key) { return Err(RuntimeEventStoreError::InvalidTransaction("original member reservation key conflict".into())); }
        retained.insert(key, RetainedMemberEntry::Reserved { outer: outer.to_owned(), index, nonce: Arc::clone(&nonce), effect: None });
        Ok(OriginalMemberReservation { owner: Arc::downgrade(self), outer: outer.to_owned(), index, ticket: ticket.clone(), nonce })
    }

    /// Release only a reservation which has not entered the backend. The
    /// original caller may use this after confirming cancellation before submit.
    pub(crate) fn cancel_original_member_reservation(&self, reservation: &OriginalMemberReservation) -> RuntimeEventStoreResult<()> {
        if !std::ptr::eq(reservation.owner.as_ptr(), self) { return Err(RuntimeEventStoreError::InvalidTransaction("original member store mismatch".into())); }
        let key = original_member_reservation_key(&reservation.outer, reservation.index);
        let mut retained = self.retained_checked_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match retained.get(&key) {
            Some(RetainedMemberEntry::Reserved { nonce, effect: None, .. }) if Arc::ptr_eq(nonce, &reservation.nonce) => { retained.remove(&key); Ok(()) }
            _ => Err(RuntimeEventStoreError::InvalidTransaction("original member reservation unavailable or already submitted".into())),
        }
    }

    /// Claim the original member in the existing effect stream before any
    /// member hook. An uncertain append remains attached to the reservation.
    pub(crate) fn acquire_original_member_preparation_fence(
        self: &Arc<Self>, reservation: &OriginalMemberReservation,
        original: &crate::RuntimeToolExecutionRequest,
        ticket: &crate::execution_core::graph::NodeExecutionTicket, index: usize,
        first_effect: &crate::execution_core::graph::OriginalFreshEffectOwner,
    ) -> RuntimeEventStoreResult<Arc<OriginalMemberEffectFence>> {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        let index = u64::try_from(index).map_err(|_| RuntimeEventStoreError::InvalidTransaction("member index overflow".into()))?;
        if !first_effect.matches_original(self, original) {
            return Err(RuntimeEventStoreError::InvalidTransaction("original first effect owner unavailable".into()));
        }
        if !reservation.matches_original(self, &original.idempotency_key, index, ticket) {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member reservation mismatch".into()));
        }
        crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, original)
            .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
        let stream = format!("execution-effect:{}", original.idempotency_key);
        let graph_stream = ticket.graph_id.clone();
        let locks = vec![stream.clone(), graph_stream.clone()];
        self.with_authenticated_stream_locks(&locks, |_| {
            let state = crate::execution_core::graph::ExecutionGraphStateStore::new(Arc::clone(self));
            let graph = state.load(&graph_stream).map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
            if graph.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running)
                || graph.recovery_cursor.node_attempts.get(&ticket.node_id) != Some(&ticket.attempt) {
                return Err(RuntimeEventStoreError::InvalidTransaction("original member node is not running".into()));
            }
            let binding = state.current_binding(&graph, &ticket.node_id, ticket.attempt)
                .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
            let node = graph.nodes.iter().find(|node| node.id == ticket.node_id)
                .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("original member node missing".into()))?;
            crate::execution_core::graph::ExecutionNodeBinding::validate_ticket(ticket, &graph, node, binding.attempt)
                .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
            let invocation = original.compiled_invocation.as_ref().ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("original effect missing".into()))?;
            let read = invocation.effect.effect_kind == harness_contract::tool::ToolEffectKind::Read;
            let intent_key = format!("{}:{}", original.idempotency_key, if read { "read-intent" } else { "intent" });
            let intent = self.event_by_idempotency_key(&stream, &intent_key)?
                .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("original effect intent missing".into()))?;
            let receipt = self.original_transaction_receipt(&intent.transaction_id)?
                .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("original effect intent receipt unavailable".into()))?;
            let replay = crate::execution_core::graph::receipt_replay_contract(original)
                .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
            if intent.sequence != 1 || intent.stream_id != stream || intent.scope != RuntimeEventScope::ExecutionNode
                || intent.actor.as_deref() != Some("governed_tool") || intent.status.as_deref() != Some("inflight")
                || intent.kind != if read { "execution.read.intent" } else { "execution.effect.intent" }
                || intent.payload["idempotency_key"] != original.idempotency_key
                || intent.payload["tool_use_id"] != original.tool_use_id || intent.payload["tool_name"] != original.tool_name
                || intent.payload["input_sha256"] != format!("sha256:{:x}", Sha256::digest(original.input.as_bytes()))
                || intent.payload["effect"] != serde_json::to_value(&invocation.effect).map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?
                || intent.payload["replay"] != replay
                || receipt.commit_cursor != intent.commit_cursor || receipt.transaction_id != intent.transaction_id
                || receipt.event_ids.get(intent.transaction_index as usize) != Some(&intent.event_id)
                || !receipt.stream_revisions.iter().any(|source| source.stream_id == stream && source.expected_revision == 0 && source.committed_revision == 1) {
                return Err(RuntimeEventStoreError::InvalidTransaction("original effect intent mismatch".into()));
            }
            let key = format!("agentic-member-preparation:{index}");
            if self.event_by_idempotency_key(&stream, &key)?.is_some()
                || self.event_by_idempotency_key(&stream, &format!("agentic-member:{index}"))?.is_some() {
                return Err(RuntimeEventStoreError::InvalidTransaction("original member already prepared or committed; original outcome required".into()));
            }
            let revision = self.stream_revision(&stream)?;
            let transaction = AppendTransactionRequest {
                transaction_id: format!("member-preparation:{}:{}:{index}", original.idempotency_key.len(), original.idempotency_key),
                expected_streams: vec![ExpectedStreamRevision { stream_id: graph_stream.clone(), expected_revision: graph.revision },
                    ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: revision }],
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput { stream_id: stream.clone(), scope: RuntimeEventScope::ExecutionNode,
                        kind: "execution.effect.member_preparation".into(), status: Some("inflight".into()),
                        actor: Some("execution_commit_service".into()), refs: vec![RuntimeEventRef { kind: "execution_graph".into(), id: graph_stream.clone() }],
                        payload: serde_json::json!({"ticket":ticket,"outer_key":original.idempotency_key,"member_index":index,
                            "tool_use_id":original.tool_use_id,"input_sha256":format!("sha256:{:x}", Sha256::digest(original.input.as_bytes())),
                            "intent_event_id":intent.event_id,"intent_transaction_id":intent.transaction_id}) },
                    idempotency_key: Some(key), schema_version: 1,
                }],
            };
            let fence = Arc::new(OriginalMemberEffectFence { owner: Arc::downgrade(self), nonce: Arc::clone(&reservation.nonce),
                outer: original.idempotency_key.clone(), index, ticket: ticket.clone(), request: Arc::new(transaction), receipt: StdMutex::new(None),
                hook_entered: std::sync::atomic::AtomicBool::new(false) });
            {
                let mut owners = self.retained_checked_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                match owners.get_mut(&original_member_reservation_key(&reservation.outer, reservation.index)) {
                    Some(RetainedMemberEntry::Reserved { nonce, effect, .. }) if Arc::ptr_eq(nonce, &reservation.nonce) && effect.is_none() => *effect = Some(Arc::clone(&fence)),
                    _ => return Err(RuntimeEventStoreError::InvalidTransaction("original member owner changed before effect append".into())),
                }
            }
            let committed = self.append_transaction_locked((*fence.request).clone())?;
            *fence.receipt.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(committed);
            Ok(fence)
        })
    }

    /// Re-read only the exact retained original append after an unknown reply.
    /// None is unknown, never proof of rollback or permission to prepare again.
    pub(crate) fn recover_original_member_preparation_fence(
        &self, reservation: &OriginalMemberReservation,
    ) -> RuntimeEventStoreResult<Option<Arc<OriginalMemberEffectFence>>> {
        if !std::ptr::eq(reservation.owner.as_ptr(), self) {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member store mismatch".into()));
        }
        let fence = {
            let owners = self.retained_checked_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match owners.get(&original_member_reservation_key(&reservation.outer, reservation.index)) {
                Some(RetainedMemberEntry::Reserved { nonce, effect: Some(effect), .. }) if Arc::ptr_eq(nonce, &reservation.nonce) => Arc::clone(effect),
                _ => return Err(RuntimeEventStoreError::InvalidTransaction("original member effect owner unavailable".into())),
            }
        };
        let Some(receipt) = self.original_transaction_receipt(&fence.request.transaction_id)? else { return Ok(None); };
        let event = self.event_by_idempotency_key(&fence.request.events[0].event.stream_id,
            fence.request.events[0].idempotency_key.as_deref().unwrap_or_default())?
            .ok_or_else(|| RuntimeEventStoreError::Corrupt("original member effect marker missing".into()))?;
        if receipt.request_hash != domain::request_hash(&fence.request)? || receipt.transaction_id != fence.request.transaction_id
            || receipt.commit_cursor == 0 || receipt.event_ids.len() != fence.request.events.len()
            || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
            || event.transaction_id != receipt.transaction_id || event.commit_cursor != receipt.commit_cursor
            || event.scope != RuntimeEventScope::ExecutionNode || event.kind != fence.request.events[0].event.kind
            || event.payload != fence.request.events[0].event.payload {
            return Err(RuntimeEventStoreError::Corrupt("original member effect receipt mismatch".into()));
        }
        *fence.receipt.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(receipt);
        Ok(Some(fence))
    }

    /// A hook claim is only a non-replay guard, never proof of completion or authorization.
    pub(crate) fn claim_original_member_hook(
        self: &Arc<Self>, fence: &Arc<OriginalMemberEffectFence>, reservation: &OriginalMemberReservation,
    ) -> RuntimeEventStoreResult<()> {
        if !std::ptr::eq(fence.owner.as_ptr(), Arc::as_ptr(self))
            || !std::ptr::eq(reservation.owner.as_ptr(), Arc::as_ptr(self))
            || !Arc::ptr_eq(&fence.nonce, &reservation.nonce)
            || fence.outer != reservation.outer || fence.index != reservation.index || fence.ticket != reservation.ticket {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member hook owner differs".into()));
        }
        let receipt_guard = fence.receipt.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original member fence receipt unavailable".into()))?;
        let Some(receipt) = receipt_guard.as_ref() else {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member fence receipt unknown".into()));
        };
        if receipt.transaction_id != fence.request.transaction_id || receipt.request_hash != domain::request_hash(&fence.request)?
            || receipt.commit_cursor == 0 || receipt.event_ids.len() != fence.request.events.len() {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member fence receipt differs".into()));
        }
        drop(receipt_guard);
        let owners = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original member custody unavailable".into()))?;
        match owners.get(&original_member_reservation_key(&reservation.outer, reservation.index)) {
            Some(RetainedMemberEntry::Reserved { nonce, effect: Some(current), .. })
                if Arc::ptr_eq(nonce, &reservation.nonce) && Arc::ptr_eq(current, fence) => {},
            _ => return Err(RuntimeEventStoreError::InvalidTransaction("original member fence custody changed".into())),
        }
        fence.hook_entered.compare_exchange(false, true, std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire).map_err(|_| RuntimeEventStoreError::InvalidTransaction(
                "original member hook outcome remains unknown".into()))?;
        Ok(())
    }

    pub(crate) fn verify_original_member_preparation_fence(
        self: &Arc<Self>, fence: &OriginalMemberEffectFence, reservation: &OriginalMemberReservation,
        original: &crate::RuntimeToolExecutionRequest,
        ticket: &crate::execution_core::graph::NodeExecutionTicket, index: usize,
    ) -> RuntimeEventStoreResult<()> {
        let index = u64::try_from(index).map_err(|_| RuntimeEventStoreError::InvalidTransaction("member index overflow".into()))?;
        if !reservation.matches_original(self, &original.idempotency_key, index, ticket)
            || fence.request.events[0].event.payload["input_sha256"] != format!("sha256:{:x}", Sha256::digest(original.input.as_bytes()))
            || fence.request.events[0].event.payload["tool_use_id"] != original.tool_use_id {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member effective call mismatch".into()));
        }
        let streams = vec![ticket.graph_id.clone(), fence.request.events[0].event.stream_id.clone()];
        self.with_authenticated_stream_locks(&streams, |held| self.original_member_effect_source_locked(fence, reservation, held).map(|_| ()))
    }

    /// Verify the same producer's marker and return its current effect source
    /// for the original checked action. Call while that source is locked.
    pub(crate) fn original_member_effect_source_locked(
        &self, fence: &OriginalMemberEffectFence, reservation: &OriginalMemberReservation,
        locks: &HeldStreamLocks<'_>,
    ) -> RuntimeEventStoreResult<ExpectedStreamRevision> {
        if !std::ptr::eq(locks.owner, self) || !std::ptr::eq(fence.owner.as_ptr(), self)
            || !Arc::ptr_eq(&fence.nonce, &reservation.nonce) || fence.outer != reservation.outer
            || fence.index != reservation.index || fence.ticket != reservation.ticket {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member effect fence mismatch".into()));
        }
        let stream = &fence.request.events[0].event.stream_id;
        if !locks.streams.contains(stream) { return Err(RuntimeEventStoreError::InvalidTransaction("original member effect source is not locked".into())); }
        let receipt = fence.receipt.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
            .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("original member effect commit outcome unknown".into()))?;
        let event = self.event_by_idempotency_key(stream, fence.request.events[0].idempotency_key.as_deref().unwrap_or_default())?
            .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("original member effect marker unavailable".into()))?;
        if receipt.request_hash != domain::request_hash(&fence.request)? || event.transaction_id != fence.request.transaction_id || event.commit_cursor != receipt.commit_cursor
            || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
            || event.payload != fence.request.events[0].event.payload || event.kind != fence.request.events[0].event.kind {
            return Err(RuntimeEventStoreError::InvalidTransaction("original member effect marker mismatch".into()));
        }
        Ok(ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: self.stream_revision(stream)? })
    }

    /// A backend-confirmed non-commit must not discard the durable effect
    /// claim. Restore its same original reservation until the effect owner has
    /// durably resolved cancellation; it is not a new preparation grant.
    fn restore_original_member_after_confirmed_noncommit(&self, transaction_id: &str) -> RuntimeEventStoreResult<()> {
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original member owner unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.remove(transaction_id) else {
            return Err(RuntimeEventStoreError::Corrupt("original checked member owner disappeared".into()));
        };
        let Some(effect) = owner.effect_fence.as_ref().cloned() else {
            if owner.authority.is_none() { return Ok(()); }
            retained.insert(transaction_id.to_owned(), RetainedMemberEntry::Checked(owner));
            return Err(RuntimeEventStoreError::Corrupt("original member effect owner missing".into()));
        };
        let key = original_member_reservation_key(&effect.outer, effect.index);
        if retained.contains_key(&key) {
            retained.insert(transaction_id.to_owned(), RetainedMemberEntry::Checked(owner));
            return Err(RuntimeEventStoreError::Corrupt("original member reservation conflict".into()));
        }
        retained.insert(key, RetainedMemberEntry::Reserved { outer: effect.outer.clone(), index: effect.index,
            nonce: Arc::clone(&effect.nonce), effect: Some(effect) });
        Ok(())
    }

    /// Caller must hold the complete, ordered source-stream lock set. The
    /// backend, not this wrapper, arbitrates cancellation with the commit.
    pub(crate) fn append_checked_transaction_cancellable_locked(
        &self,
        mut request: CheckedAppendRequest,
        validator: &dyn CheckedSourceValidator,
        control: Arc<crate::conversation::MemberCommitControl>,
        claim: &RuntimeOriginalSessionClaim<'_>,
        locks: &HeldStreamLocks<'_>,
        authority: Option<Arc<crate::authorization_negotiator::PreparedMemberAuthority>>,
        reservation: Option<&OriginalMemberReservation>,
        effect_fence: Option<&OriginalMemberEffectFence>,
        conclusion_proof: Option<Arc<crate::execution_core::goal::OriginalConclusionProofCustody>>,
    ) -> RuntimeEventStoreResult<Option<OriginalCheckedAppend>> {
        if conclusion_proof.is_some() && (authority.is_some() || reservation.is_some() || effect_fence.is_some()
            || !request.transaction.transaction_id.starts_with("program-conclusion:")) {
            return Err(RuntimeEventStoreError::InvalidTransaction("original conclusion proof owner differs".into()));
        }
        if !std::ptr::eq(locks.owner, self)
            || request.transaction.expected_streams.iter().any(|source| {
                source.stream_id.is_empty() || !locks.streams.contains(&source.stream_id)
            })
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "checked transaction sources are not held by this store owner".into(),
            ));
        }
        if authority.is_some() {
            let reservation = reservation.ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("original member reservation required".into())
            })?;
            let fence = effect_fence.ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("original member effect fence required".into())
            })?;
            let source = self.original_member_effect_source_locked(fence, reservation, locks).map_err(|error| {
                error
            })?;
            if !request.transaction.expected_streams.iter().any(|expected| expected == &source) {
                return Err(RuntimeEventStoreError::InvalidTransaction("original member effect source is not checked".into()));
            }
            if !locks.streams.contains(&fence.ticket.graph_id) || !request.transaction.expected_streams.iter().any(|expected|
                expected.stream_id == fence.ticket.graph_id && self.stream_revision(&expected.stream_id).ok() == Some(expected.expected_revision)) {
                return Err(RuntimeEventStoreError::InvalidTransaction("original member current graph source is not checked".into()));
            }
            let owner = fence.owner.upgrade().ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("original member effect owner unavailable".into())
            })?;
            let state = crate::execution_core::graph::ExecutionGraphStateStore::new(owner);
            let graph = state.load(&fence.ticket.graph_id).map_err(|error| {
                RuntimeEventStoreError::InvalidTransaction(error.to_string())
            })?;
            if graph.node_statuses.get(&fence.ticket.node_id) != Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
                || graph.recovery_cursor.node_attempts.get(&fence.ticket.node_id) != Some(&fence.ticket.attempt) {
                return Err(RuntimeEventStoreError::InvalidTransaction("original member graph owner is no longer running".into()));
            }
            let binding = state.current_binding(&graph, &fence.ticket.node_id, fence.ticket.attempt)
                .map_err(|error| {
                    RuntimeEventStoreError::InvalidTransaction(error.to_string())
                })?;
            let node = graph.nodes.iter().find(|node| node.id == fence.ticket.node_id)
                .ok_or_else(|| {
                    RuntimeEventStoreError::InvalidTransaction("original member graph node missing".into())
                })?;
            crate::execution_core::graph::ExecutionNodeBinding::validate_ticket(&fence.ticket, &graph, node, binding.attempt)
                .map_err(|error| {
                    RuntimeEventStoreError::InvalidTransaction(error.to_string())
                })?;
        } else if effect_fence.is_some() {
            return Err(RuntimeEventStoreError::InvalidTransaction("effect fence without original member authority".into()));
        }
        self.ensure_graph_transaction_recovery_admitted(&request.transaction).map_err(|error| {
            error
        })?;
        self.validate_content_publication(&mut request.transaction).map_err(|error| {
            error
        })?;
        request.identity_hash().map_err(|error| {
            error
        })?;
        if request.frame_digest != validator.frame_digest() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "source validator frame mismatch".into(),
            ));
        }
        let transaction_id = request.transaction.transaction_id.clone();
        let submitted = Arc::new(request.clone());
        {
            let mut retained = self.retained_checked_members.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if retained.contains_key(&transaction_id) {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "original checked member transaction outcome remains unresolved".into(),
                ));
            }
            let identity = original_member_identity(&submitted).map_err(|error| {
                error
            })?;
            let retained_fence = if authority.is_some() {
                let reservation = reservation.ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("original member reservation required".into())
            })?;
                if !std::ptr::eq(reservation.owner.as_ptr(), self)
                    || identity.as_ref() != Some(&(reservation.outer.clone(), reservation.index))
                    || submitted.transaction.events.iter().find(|event| event.event.kind == "agentic.batch_member.index")
                        .is_none_or(|event| serde_json::to_value(&reservation.ticket).ok().as_ref() != event.event.payload.get("ticket")) {
                    return Err(RuntimeEventStoreError::InvalidTransaction("original member reservation identity mismatch".into()));
                }
                let key = original_member_reservation_key(&reservation.outer, reservation.index);
                let retained_fence = match retained.get(&key) {
                    Some(RetainedMemberEntry::Reserved { nonce, effect: Some(effect), .. })
                        if Arc::ptr_eq(nonce, &reservation.nonce) && effect_fence.is_some_and(|provided| std::ptr::eq(Arc::as_ptr(effect), provided)) => Arc::clone(effect),
                    _ => {
                        return Err(RuntimeEventStoreError::InvalidTransaction("original member reservation or effect owner unavailable".into()));
                    },
                };
                // The same mutex changes the original reservation into the
                // complete request; no await or gap permits a second owner.
                retained.remove(&key);
                Some(retained_fence)
            } else {
                if reservation.is_some() || identity.is_some() {
                    return Err(RuntimeEventStoreError::InvalidTransaction("member reservation without original authority".into()));
                }
                if retained.len() >= 256 { return Err(RuntimeEventStoreError::InvalidTransaction("checked member transaction custody capacity exhausted".into())); }
                None
            };
            retained.insert(transaction_id.clone(), RetainedMemberEntry::Checked(RetainedCheckedMember {
                request: Arc::clone(&submitted), control: Arc::clone(&control), authority: authority.clone(), effect_fence: retained_fence,
                conclusion_proof: conclusion_proof.clone(), conclusion_receipt: None, conclusion_published: false, terminal: None, terminal_owner: None,
            }));
        }
        let result = self.backend.append_checked_transaction_cancellable(
            request, validator, control.as_ref(), claim,
        );
        match result {
            Ok(receipt) => {
                if let Some(receipt) = &receipt {
                    if conclusion_proof.is_some() {
                        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
                        let Some(RetainedMemberEntry::Checked(owner)) = retained.get_mut(&transaction_id) else {
                            return Err(RuntimeEventStoreError::Corrupt("original conclusion custody disappeared".into()));
                        };
                        if !Arc::ptr_eq(&owner.request, &submitted) { return Err(RuntimeEventStoreError::Corrupt("original conclusion request changed".into())); }
                        owner.conclusion_receipt = Some(receipt.clone());
                    }
                    self.publish_commit(receipt.commit.commit_cursor);
                }
                // A committed member remains owned until its original lease/
                // approval owner has applied the committed transition. A
                // missing receipt here is the backend's confirmed cancellation.
                if receipt.is_none() {
                    self.restore_original_member_after_confirmed_noncommit(&transaction_id)?;
                } else if authority.is_none() && conclusion_proof.is_none() {
                    self.retained_checked_members.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner).remove(&transaction_id);
                }
                Ok(receipt.map(|receipt| OriginalCheckedAppend { request: submitted, receipt, authority }))
            }
            Err(RuntimeEventStoreError::CheckedAttemptRolledBack(cause)) => {
                self.restore_original_member_after_confirmed_noncommit(&transaction_id)?;
                // Keep the backend's proof of rollback in the error type so
                // the action owner can distinguish a retryable non-commit
                // from an unknown transaction result.
                Err(RuntimeEventStoreError::CheckedAttemptRolledBack(cause))
            }
            // Commit result unknown: retain the original request and control.
            Err(error) => {
                Err(error)
            },
        }
    }
    /// A missing commit is still Unknown. A present commit must match the
    /// exact retained checked request before it can be used for reconciliation.
    pub(crate) fn read_retained_checked_member_commit(
        &self,
        transaction_id: &str,
    ) -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        let (expected_hash, event_count) = {
            let retained = self.retained_checked_members.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(RetainedMemberEntry::Checked(original)) = retained.get(transaction_id) else {
                return Err(RuntimeEventStoreError::InvalidTransaction("original checked member owner unavailable".into()));
            };
            // Read the Arc so reconciliation is visibly tied to the original
            // owner; it remains retained until that owner settles it.
            let _original_control = Arc::clone(&original.control);
            (original.request.identity_hash()?, original.request.transaction.events.len())
        };
        let Some(receipt) = self.backend.original_transaction_receipt(transaction_id)? else {
            return Ok(None);
        };
        if receipt.transaction_id != transaction_id || receipt.request_hash != expected_hash
            || receipt.commit_cursor == 0 || receipt.event_ids.len() != event_count
        {
            return Err(RuntimeEventStoreError::Corrupt(
                "original checked member receipt identity mismatch".into(),
            ));
        }
        Ok(Some(receipt))
    }

    /// Locate only a retained checked request owned by this exact Process call.
    /// Absence says nothing about whether any durable transaction committed.
    pub(crate) fn retained_original_process_member_transaction(
        &self, process: &Arc<crate::agent_in_process_worker::OriginalProcessBatchCall>,
        original: &crate::RuntimeToolExecutionRequest, index: usize,
    ) -> RuntimeEventStoreResult<Option<(String, crate::authorization_negotiator::PreparedMemberBinding)>> {
        if !std::ptr::eq(Arc::as_ptr(process.store()), self) {
            return Err(RuntimeEventStoreError::InvalidTransaction("original Process store mismatch".into()));
        }
        let index = u64::try_from(index).map_err(|_| RuntimeEventStoreError::InvalidTransaction("original member index overflow".into()))?;
        let retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::InvalidTransaction("original member owner unavailable".into()))?;
        let mut found = None;
        for entry in retained.values() {
            let RetainedMemberEntry::Checked(owner) = entry else { continue; };
            if original_member_identity(&owner.request)?.as_ref() != Some(&(original.idempotency_key.clone(), index)) { continue; }
            let authority = owner.authority.as_ref().ok_or_else(|| RuntimeEventStoreError::Corrupt("retained member authority missing".into()))?;
            let member_guard = authority.member().upgrade().ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("retained member owner unavailable".into()))?;
            let member = member_guard.process().ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("retained member owner is not Process".into()))?;
            if !Arc::ptr_eq(member.owner(), process) || member.ticket() != process.ticket()
                || member.original_request() != original
                || !member.original_request().tool_progress.same_original_channels(&original.tool_progress) {
                return Err(RuntimeEventStoreError::InvalidTransaction("retained Process call identity mismatch".into()));
            }
            if found.replace((owner.request.transaction.transaction_id.clone(), member_guard)).is_some() {
                return Err(RuntimeEventStoreError::Corrupt("multiple retained original member transactions".into()));
            }
        }
        Ok(found)
    }

    /// Release only the exact checked bounded-decision request after its same
    /// physical node owner authenticated the winner and completed publication.
    pub(crate) fn release_original_bounded_decision(
        &self, proof: &crate::execution_core::graph::CommittedCallRefusal,
    ) -> RuntimeEventStoreResult<()> {
        let transaction_id = proof.transaction_id();
        let receipt = self.backend.original_transaction_receipt(transaction_id)?;
        let mut retained = self.retained_checked_members.lock().map_err(|_|
            RuntimeEventStoreError::Corrupt("bounded decision custody unavailable".into()))?;
        let Some(entry) = retained.get(transaction_id) else { return Ok(()); };
        let RetainedMemberEntry::Checked(owner) = entry else {
            return Err(RuntimeEventStoreError::Corrupt("bounded decision custody type differs".into()));
        };
        let receipt = receipt.ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("bounded decision receipt unavailable".into()))?;
        let events = &owner.request.transaction.events;
        if owner.authority.is_some() || owner.effect_fence.is_some() || owner.conclusion_proof.is_some()
            || owner.terminal.is_some() || owner.terminal_owner.is_some()
            || owner.request.transaction.transaction_id != transaction_id
            || owner.request.identity_hash()? != receipt.request_hash || receipt.transaction_id != transaction_id
            || receipt.event_ids.as_slice() != [proof.assessment_event_id().to_string(), proof.event_id().to_string()]
            || events.len() != 2 || events[0].event.kind != "execution.bounded_acquisition.failed"
            || events[1].event.kind != "execution.call.refused"
            || events[0].event.payload["request"]["tool_use_id"] != proof.call_id()
            || events[0].event.payload["request"]["idempotency_key"] != proof.effect_key() {
            return Err(RuntimeEventStoreError::Corrupt("bounded decision custody identity differs".into()));
        }
        retained.remove(transaction_id);
        Ok(())
    }

    pub(crate) fn recover_retained_member(
        &self, transaction_id: &str,
    ) -> RuntimeEventStoreResult<Option<RecoveredOriginalCheckedAppend>> {
        let Some(receipt) = self.read_retained_checked_member_commit(transaction_id)? else { return Ok(None); };
        let retained = self.retained_checked_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get(transaction_id) else { return Err(RuntimeEventStoreError::Corrupt("retained original member owner disappeared".into())); };
        if owner.request.identity_hash()? != receipt.request_hash || owner.request.transaction.transaction_id != receipt.transaction_id {
            return Err(RuntimeEventStoreError::Corrupt("retained original member changed".into()));
        }
        let authority = owner.authority.clone().ok_or_else(|| RuntimeEventStoreError::Corrupt(
            "retained original member authority missing".into()))?;
        self.publish_commit(receipt.commit_cursor);
        Ok(Some(RecoveredOriginalCheckedAppend { request: Arc::clone(&owner.request), receipt, authority }))
    }

    pub(crate) fn retain_original_checked_terminal(
        &self, request: Arc<CheckedAppendRequest>, terminal: SessionTerminalInput,
        control: Arc<crate::conversation::MemberCommitControl>,
        terminal_owner: Arc<crate::execution_core::services::OriginalTerminalCommitOwner>,
    ) -> RuntimeEventStoreResult<()> {
        let id = &request.transaction.transaction_id;
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("terminal custody unavailable".into()))?;
        if retained.contains_key(id) { return Err(RuntimeEventStoreError::InvalidTransaction("original terminal outcome remains unknown".into())); }
        retained.insert(id.clone(), RetainedMemberEntry::Checked(RetainedCheckedMember {
            request, control, authority: None, effect_fence: None, conclusion_proof: None,
            conclusion_receipt: None, conclusion_published: false, terminal: Some(terminal), terminal_owner: Some(terminal_owner),
        }));
        Ok(())
    }

    pub(crate) fn finish_original_checked_terminal(
        &self, request: &Arc<CheckedAppendRequest>, result: &RuntimeEventStoreResult<Option<CheckedAppendReceipt>>,
    ) -> RuntimeEventStoreResult<()> {
        let id = &request.transaction.transaction_id;
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("terminal custody unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get_mut(id) else { return Err(RuntimeEventStoreError::Corrupt("terminal custody disappeared".into())); };
        if !Arc::ptr_eq(&owner.request, request) || owner.terminal.is_none() || owner.terminal_owner.is_none() { return Err(RuntimeEventStoreError::Corrupt("terminal custody differs".into())); }
        match result {
            Ok(Some(receipt)) => owner.conclusion_receipt = Some(receipt.clone()),
            Ok(None) | Err(RuntimeEventStoreError::CheckedAttemptRolledBack(_)) => { retained.remove(id); },
            Err(_) => {}, // Unknown retains the original request and control.
        }
        Ok(())
    }

    pub(crate) fn record_original_checked_terminal_winner(
        &self, request: &CheckedAppendRequest, terminal: &SessionTerminalInput, receipt: &CheckedAppendReceipt,
    ) -> RuntimeEventStoreResult<()> {
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("terminal custody unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get_mut(&request.transaction.transaction_id) else {
            return Err(RuntimeEventStoreError::Corrupt("original terminal custody unavailable".into()));
        };
        if owner.request.as_ref() != request || owner.terminal.as_ref() != Some(terminal)
            || receipt.commit.transaction_id != request.transaction.transaction_id
            || receipt.commit.request_hash != request.identity_hash()? {
            return Err(RuntimeEventStoreError::Corrupt("terminal winner identity differs".into()));
        }
        if let Some(old) = owner.conclusion_receipt.as_ref() {
            let mut normalized = receipt.clone();
            normalized.commit.duplicate = old.commit.duplicate;
            if old != &normalized { return Err(RuntimeEventStoreError::Corrupt("terminal winner receipt differs".into())); }
        } else {
            owner.conclusion_receipt = Some(receipt.clone());
        }
        Ok(())
    }

    /// Borrow only the same-store checked winner and its original proof guards.
    /// An uncertain append without a checked receipt remains unknown.
    pub(crate) fn original_conclusion_proof_custody(
        self: &Arc<Self>, transaction_id: &str, program_id: &str,
        goal: &harness_contract::goal::GoalContract,
    ) -> RuntimeEventStoreResult<Option<(Arc<crate::execution_core::goal::OriginalConclusionProofCustody>, CheckedAppendReceipt)>> {
        let (proof, original_request, known_receipt) = {
            let retained = self.retained_checked_members.lock()
                .map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
            let Some(RetainedMemberEntry::Checked(owner)) = retained.get(transaction_id) else { return Ok(None); };
            let Some(proof) = owner.conclusion_proof.as_ref() else { return Ok(None); };
            if !proof.matches(self, program_id, goal) || owner.authority.is_some() || owner.effect_fence.is_some()
                || owner.request.transaction.transaction_id != transaction_id {
                return Err(RuntimeEventStoreError::Corrupt("original conclusion proof custody differs".into()));
            }
            (Arc::clone(proof), Arc::clone(&owner.request), owner.conclusion_receipt.clone())
        };
        let receipt = match known_receipt {
            Some(receipt) => receipt,
            None => match self.backend.original_checked_transaction_receipt(transaction_id)? {
                Some(receipt) => receipt,
                None => return Ok(None), // Absence never proves rollback.
            },
        };
        if receipt.commit.transaction_id != transaction_id
            || original_request.identity_hash()? != receipt.commit.request_hash
            || original_request.frame_digest != receipt.validation.frame_digest {
            return Err(RuntimeEventStoreError::Corrupt("original conclusion checked receipt differs".into()));
        }
        let mut retained = self.retained_checked_members.lock()
            .map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get_mut(transaction_id) else {
            return Err(RuntimeEventStoreError::Corrupt("original conclusion custody disappeared".into()));
        };
        if !Arc::ptr_eq(&owner.request, &original_request)
            || owner.conclusion_proof.as_ref().is_none_or(|actual| !Arc::ptr_eq(actual, &proof))
            || owner.conclusion_receipt.as_ref().is_some_and(|known| known != &receipt) {
            return Err(RuntimeEventStoreError::Corrupt("original conclusion custody changed".into()));
        }
        owner.conclusion_receipt = Some(receipt.clone());
        Ok(Some((proof, receipt)))
    }

    /// Release only the exact original terminal transaction after its original
    /// runner has confirmed publication and physical close.
    pub(crate) fn release_original_checked_terminal(&self, event: &DurableRuntimeEvent) -> RuntimeEventStoreResult<()> {
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("terminal custody unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get(&event.transaction_id) else {
            return Err(RuntimeEventStoreError::Corrupt("terminal custody missing".into()));
        };
        let invalid = || RuntimeEventStoreError::Corrupt("terminal custody identity differs".into());
        let receipt = owner.conclusion_receipt.as_ref().ok_or_else(invalid)?;
        let terminal = owner.terminal.as_ref().ok_or_else(invalid)?;
        let index = event.transaction_index as usize;
        let input = owner.request.transaction.events.get(index).ok_or_else(invalid)?;
        if receipt.commit.transaction_id != event.transaction_id || receipt.commit.commit_cursor != event.commit_cursor
            || receipt.commit.event_ids.get(index) != Some(&event.event_id) || owner.request.identity_hash()? != receipt.commit.request_hash
            || input.idempotency_key != event.idempotency_key || input.schema_version != event.schema_version
            || input.event.stream_id != event.stream_id || input.event.kind != event.kind || input.event.scope != event.scope
            || input.event.actor != event.actor || input.event.status != event.status || input.event.refs != event.refs
            || input.event.payload != event.payload || event.payload["terminal_id"] != terminal.terminal_id {
            return Err(invalid());
        }
        retained.remove(&event.transaction_id);
        Ok(())
    }

    /// Called only by the original terminal owner after the terminal commit
    /// and physical close are authenticated. Unknown results keep custody.
    pub(crate) fn release_original_conclusion_proof(
        &self, transaction_id: &str, proof: &Arc<crate::execution_core::goal::OriginalConclusionProofCustody>,
        receipt: &CheckedAppendReceipt, verdict_event: &DurableRuntimeEvent,
        goal: &harness_contract::goal::GoalContract, terminal_event: &DurableRuntimeEvent,
    ) -> RuntimeEventStoreResult<()> {
        let request = {
            let retained = self.retained_checked_members.lock()
                .map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
            let Some(RetainedMemberEntry::Checked(owner)) = retained.get(transaction_id) else {
                return Err(RuntimeEventStoreError::Corrupt("original conclusion custody disappeared".into()));
            };
            if owner.conclusion_proof.as_ref().is_none_or(|actual| !Arc::ptr_eq(actual, proof))
                || owner.conclusion_receipt.as_ref() != Some(receipt)
                || receipt.commit.transaction_id != transaction_id {
                return Err(RuntimeEventStoreError::Corrupt("original conclusion release owner differs".into()));
            }
            Arc::clone(&owner.request)
        };
        let corrupt = || RuntimeEventStoreError::Corrupt("original conclusion transaction events differ".into());
        let index = usize::try_from(verdict_event.transaction_index).map_err(|_| corrupt())?;
        let program = request.transaction.events.get(index).ok_or_else(corrupt)?;
        if verdict_event.transaction_id != transaction_id || verdict_event.commit_cursor != receipt.commit.commit_cursor
            || receipt.commit.event_ids.get(index) != Some(&verdict_event.event_id)
            || program.idempotency_key != verdict_event.idempotency_key
            || program.schema_version != verdict_event.schema_version
            || program.event.stream_id != verdict_event.stream_id || program.event.kind != verdict_event.kind
            || program.event.scope != verdict_event.scope || program.event.status != verdict_event.status
            || program.event.actor != verdict_event.actor || program.event.refs != verdict_event.refs
            || program.event.payload != verdict_event.payload {
            return Err(corrupt());
        }
        let goal_stream = format!("goal:{}", goal.id);
        let goal_value = serde_json::to_value(goal)?;
        let matching: Vec<_> = request.transaction.events.iter().enumerate().filter(|(_, input)|
            input.event.stream_id == goal_stream && input.event.payload["goal"] == goal_value
        ).collect();
        let [(goal_index, goal_input)] = matching.as_slice() else { return Err(corrupt()); };
        let key = goal_input.idempotency_key.as_deref().ok_or_else(corrupt)?;
        let goal_event = self.event_by_idempotency_key(&goal_stream, key)?.ok_or_else(corrupt)?;
        if goal_event.transaction_id != transaction_id || goal_event.commit_cursor != receipt.commit.commit_cursor
            || goal_event.transaction_index as usize != *goal_index
            || receipt.commit.event_ids.get(*goal_index) != Some(&goal_event.event_id)
            || goal_event.kind != goal_input.event.kind || goal_event.scope != goal_input.event.scope
            || goal_event.status != goal_input.event.status || goal_event.actor != goal_input.event.actor
            || goal_event.refs != goal_input.event.refs || goal_event.payload != goal_input.event.payload
            || goal_event.schema_version != goal_input.schema_version {
            return Err(corrupt());
        }
        let mut retained = self.retained_checked_members.lock()
            .map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get(transaction_id) else { return Err(corrupt()); };
        if !Arc::ptr_eq(&owner.request, &request)
            || owner.conclusion_proof.as_ref().is_none_or(|actual| !Arc::ptr_eq(actual, proof))
            || owner.conclusion_receipt.as_ref() != Some(receipt) { return Err(corrupt()); }
        let Some(RetainedMemberEntry::Checked(terminal_owner)) = retained.get(&terminal_event.transaction_id) else { return Err(corrupt()); };
        let terminal_receipt = terminal_owner.conclusion_receipt.as_ref().ok_or_else(corrupt)?;
        let terminal_input = terminal_owner.terminal.as_ref().ok_or_else(corrupt)?;
        let position = terminal_event.transaction_index as usize;
        let input = terminal_owner.request.transaction.events.get(position).ok_or_else(corrupt)?;
        if terminal_receipt.commit.transaction_id != terminal_event.transaction_id
            || terminal_receipt.commit.commit_cursor != terminal_event.commit_cursor
            || terminal_receipt.commit.event_ids.get(position) != Some(&terminal_event.event_id)
            || terminal_owner.request.identity_hash()? != terminal_receipt.commit.request_hash
            || terminal_input.session_id != goal.session_id
            || input.idempotency_key != terminal_event.idempotency_key
            || input.event.stream_id != terminal_event.stream_id || input.event.kind != terminal_event.kind
            || input.event.scope != terminal_event.scope || input.event.actor != terminal_event.actor
            || input.event.status != terminal_event.status || input.event.refs != terminal_event.refs
            || input.event.payload != terminal_event.payload || input.schema_version != terminal_event.schema_version {
            return Err(corrupt());
        }
        if terminal_event.transaction_id == transaction_id { return Err(corrupt()); }
        retained.remove(&terminal_event.transaction_id);
        retained.remove(transaction_id);
        Ok(())
    }

    pub(crate) fn has_retained_conclusion(&self, transaction_id: &str) -> RuntimeEventStoreResult<bool> {
        if !transaction_id.starts_with("program-conclusion:") {
            return Err(RuntimeEventStoreError::InvalidTransaction("not an original conclusion transaction".into()));
        }
        let retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
        match retained.get(transaction_id) {
            None => Ok(false),
            Some(RetainedMemberEntry::Checked(owner)) if owner.authority.is_none() && owner.effect_fence.is_none() =>
                Ok(owner.conclusion_receipt.is_none() && !owner.conclusion_published),
            Some(_) => Err(RuntimeEventStoreError::Corrupt("original conclusion custody differs".into())),
        }
    }

    /// Read the exact retained checked conclusion after an uncertain reply.
    /// A missing backend receipt remains Unknown; no validation evidence is invented.
    pub(crate) fn recover_retained_conclusion(
        &self, transaction_id: &str,
    ) -> RuntimeEventStoreResult<Option<(Arc<CheckedAppendRequest>, AppendTransactionReceipt)>> {
        if !transaction_id.starts_with("program-conclusion:") {
            return Err(RuntimeEventStoreError::InvalidTransaction("not an original conclusion transaction".into()));
        }
        let Some(receipt) = self.read_retained_checked_member_commit(transaction_id)? else { return Ok(None); };
        let retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get(transaction_id) else {
            return Err(RuntimeEventStoreError::Corrupt("original conclusion custody disappeared".into()));
        };
        let events = &owner.request.transaction.events;
        if owner.authority.is_some() || owner.effect_fence.is_some() || events.len() != 2
            || events[0].event.scope != RuntimeEventScope::Goal || events[1].event.scope != RuntimeEventScope::Program
            || owner.request.identity_hash()? != receipt.request_hash
            || owner.request.transaction.transaction_id != receipt.transaction_id {
            return Err(RuntimeEventStoreError::Corrupt("retained conclusion identity differs".into()));
        }
        let request = Arc::clone(&owner.request);
        drop(retained);
        self.publish_commit(receipt.commit_cursor);
        Ok(Some((request, receipt)))
    }

    /// Release only the original conclusion custody after its same-call result
    /// has actually been published to the original request's sink.
    pub(crate) fn release_reconciled_conclusion(
        &self, request: &Arc<CheckedAppendRequest>, receipt: &AppendTransactionReceipt,
    ) -> RuntimeEventStoreResult<()> {
        let transaction_id = &request.transaction.transaction_id;
        let mut retained = self.retained_checked_members.lock().map_err(|_| RuntimeEventStoreError::Corrupt("original conclusion custody unavailable".into()))?;
        if !transaction_id.starts_with("program-conclusion:") || receipt.transaction_id != *transaction_id
            || request.identity_hash()? != receipt.request_hash {
            return Err(RuntimeEventStoreError::Corrupt("retained conclusion identity differs".into()));
        }
        match retained.get(transaction_id) {
            Some(RetainedMemberEntry::Checked(owner)) if owner.authority.is_none()
                && Arc::ptr_eq(&owner.request, request) => {},
            // A concurrent recovery may already have published this exact
            // owner result and released it. The caller has just confirmed its
            // same-call sink; this does not grant a new attempt.
            None => return Ok(()),
            _ => return Err(RuntimeEventStoreError::Corrupt("original conclusion custody changed".into())),
        }
        if let Some(RetainedMemberEntry::Checked(owner)) = retained.get_mut(transaction_id) {
            if owner.conclusion_proof.is_some() {
                owner.conclusion_published = true;
                return Ok(());
            }
        }
        retained.remove(transaction_id);
        Ok(())
    }

    /// Called only after the original authorization owner has applied the
    /// verified committed transition to its hot projection.
    pub(crate) fn release_reconciled_member(
        &self, transaction_id: &str, request: &Arc<CheckedAppendRequest>,
        authority: &Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        receipt: &AppendTransactionReceipt,
    ) -> RuntimeEventStoreResult<()> {
        let mut retained = self.retained_checked_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(RetainedMemberEntry::Checked(owner)) = retained.get(transaction_id) else { return Err(RuntimeEventStoreError::Corrupt("retained member owner missing".into())); };
        if !Arc::ptr_eq(&owner.request, request)
            || owner.authority.as_ref().is_none_or(|original| !Arc::ptr_eq(original, authority))
            || receipt.transaction_id != transaction_id || request.identity_hash()? != receipt.request_hash
        { return Err(RuntimeEventStoreError::Corrupt("retained member owner changed".into())); }
        retained.remove(transaction_id);
        Ok(())
    }

    pub(crate) fn original_transaction_receipt(&self, transaction_id: &str)
        -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        self.backend.original_transaction_receipt(transaction_id)
    }

    pub fn list_stream_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let result = self.backend.list_stream_after(
            stream_id,
            after_sequence,
            through_sequence,
            max_events,
            max_bytes,
        );
        #[cfg(test)]
        if result.is_ok() { self.after_test_read(stream_id, "list_stream_after"); }
        result
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn fixture_evidence_page(
        &self,
        request: &FixtureEvidencePageRequest<'_>,
    ) -> Result<FixtureEvidencePage, &'static str> {
        if Instant::now() >= request.deadline {
            return Err("Unavailable: deadline exhausted before read");
        }
        if request.max_events == 0 || request.max_bytes == 0 {
            return Err("Unavailable: budget exhausted before read");
        }
        self.backend.fixture_evidence_page(request)
    }

    pub fn position_page_classified(
        &self, scope: RuntimeEventScope, identity: &str, kind: Option<&str>,
        after: Option<(u64, u32)>, through_cursor: u64, max_events: usize, max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, PositionPageError> {
        self.backend.position_page_classified(scope, identity, kind, after, through_cursor, max_events, max_bytes)
    }

    pub fn position_page(
        &self,
        scope: RuntimeEventScope,
        identity: &str,
        kind: Option<&str>,
        after: Option<(u64, u32)>,
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.position_page(
            scope,
            identity,
            kind,
            after,
            through_cursor,
            max_events,
            max_bytes,
        )
    }

    pub fn list_stream_before(
        &self,
        stream_id: &str,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let result = self.backend
            .list_stream_before(stream_id, through_sequence, max_events, max_bytes);
        #[cfg(test)]
        if result.is_ok() { self.after_test_read(stream_id, "list_stream_before"); }
        result
    }

    pub fn count_stream_kinds(
        &self,
        stream_id: &str,
        kinds: &[&str],
        through_sequence: u64,
    ) -> Result<u64, String> {
        self.backend
            .count_stream_kinds(stream_id, kinds, through_sequence)
    }

    /// Appends without acquiring the per-stream lock. Only callers that
    /// already hold [`Self::with_stream_lock`] for every expected stream may
    /// use this; it avoids a non-reentrant deadlock on the same stream.
    pub(crate) fn append_transaction_locked(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.ensure_graph_transaction_recovery_admitted(&request)?;
        let mut request = request;
        self.validate_content_publication(&mut request)?;
        #[cfg(test)]
        let transaction_id = request.transaction_id.clone();
        #[cfg(test)]
        self.take_append_fault(&transaction_id, false)?;
        let receipt = self.backend.append_transaction(request)?;
        #[cfg(test)]
        self.take_append_fault(&transaction_id, true)?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    #[cfg(test)]
    pub(crate) fn inject_content_transaction_fault(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.backend.append_transaction(request)
    }

    #[cfg(test)]
    pub(crate) fn inject_content_checkpoint_fault(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.backend.compare_and_repair_projection_checkpoint(
            projection_id,
            source_cursor,
            expected_revision,
            payload,
            0,
        )
    }

    pub fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.ensure_graph_transaction_recovery_admitted(&request)?;
        self.ensure_session_recovery_admitted(&terminal.session_id)
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        let mut request = request;
        self.validate_content_publication(&mut request)?;
        self.validate_terminal_publication(&terminal, &request)?;
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        let receipt = self.with_stream_locks(&stream_ids, || {
            self.backend
                .append_transaction_with_terminal(request, terminal)
        })?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn consume_verified_decision_lease(
        &self,
        lease_id: &str,
        principal_id: &str,
        review_id: &str,
        action: &str,
        scope: &str,
        evidence_digest: &str,
        credential_epoch: u64,
        consumed_at_ms: u64,
    ) -> RuntimeEventStoreResult<()> {
        self.backend.consume_verified_decision_lease(
            lease_id,
            principal_id,
            review_id,
            action,
            scope,
            evidence_digest,
            credential_epoch,
            consumed_at_ms,
        )
    }

    pub(crate) fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.ensure_graph_transaction_recovery_admitted(&request)?;
        let mut request = request;
        self.validate_content_publication(&mut request)?;
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        let receipt = self.with_stream_locks(&stream_ids, || {
            self.backend
                .append_transaction_with_verified_decision_lease(request, lease)
        })?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    pub fn append_batch_if_revision(
        &self,
        stream_id: impl Into<String>,
        expected_revision: u64,
        transaction_id: impl Into<String>,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let stream_id = stream_id.into();
        self.append_transaction(AppendTransactionRequest {
            transaction_id: transaction_id.into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id,
                expected_revision,
            }],
            events,
        })
    }

    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.commit_signal.subscribe()
    }

    fn publish_commit(&self, cursor: u64) {
        if Self::current_projection_work_class().is_none() {
            self.last_foreground_commit_ms
                .store(monotonic_elapsed_ms(), Ordering::Release);
        }
        if cursor > *self.commit_signal.borrow() {
            self.commit_signal.send_replace(cursor);
        }
    }

    pub(crate) fn foreground_quiet_remaining(&self, quiet_period: Duration) -> Duration {
        let last = self.last_foreground_commit_ms.load(Ordering::Acquire);
        if last == 0 {
            return Duration::ZERO;
        }
        let quiet_ms = u64::try_from(quiet_period.as_millis()).unwrap_or(u64::MAX);
        let elapsed_ms = monotonic_elapsed_ms().saturating_sub(last);
        Duration::from_millis(quiet_ms.saturating_sub(elapsed_ms))
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        self.backend.events_after_cursor(cursor, max_commits)
    }

    pub fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        self.backend
            .projection_scan_page(cursor, interest, max_commits, max_events, max_bytes)
    }

    #[must_use]
    pub fn background_projection_capacity_hint(&self) -> usize {
        self.backend.background_projection_capacity_hint().max(1)
    }

    pub fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        self.backend.projection_checkpoint(projection_id)
    }

    pub fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        self.backend.projection_checkpoints_with_prefix(prefix)
    }

    pub fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.validate_checkpoint_content(projection_id, source_cursor, payload)?;
        self.backend
            .put_projection_checkpoint(projection_id, source_cursor, payload, updated_at_ms)
    }

    pub fn source_mutation_fence(&self) -> RuntimeEventStoreResult<RuntimeSourceMutationFence> {
        self.backend.source_mutation_fence()
    }

    pub fn compare_and_put_projection_checkpoint_fenced(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
        source_fence: &RuntimeSourceMutationFence,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.validate_checkpoint_content(projection_id, source_cursor, payload)?;
        self.backend.compare_and_put_projection_checkpoint_fenced(
            projection_id, source_cursor, expected_revision, payload, updated_at_ms, source_fence,
        )
    }

    pub fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.validate_checkpoint_content(projection_id, source_cursor, payload)?;
        self.backend.compare_and_put_projection_checkpoint(
            projection_id,
            source_cursor,
            expected_revision,
            payload,
            updated_at_ms,
        )
    }

    /// Compare-and-put a projection checkpoint, re-reading the authoritative row
    /// revision and retrying on a stale revision. Projection writers race other
    /// passes over the same row; callers that read the expected revision in a
    /// separate step must use this to avoid dropping the write.
    pub fn put_projection_checkpoint_retrying(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let mut attempt = 0;
        loop {
            let expected_revision = self
                .projection_checkpoint(projection_id)?
                .map_or(0, |checkpoint| checkpoint.revision);
            match self.compare_and_put_projection_checkpoint(
                projection_id,
                source_cursor,
                expected_revision,
                payload,
                updated_at_ms,
            ) {
                Ok(checkpoint) => return Ok(checkpoint),
                Err(RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => {
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Fenced repair of an existing, damaged derived checkpoint from journal truth.
    pub fn compare_and_repair_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.validate_checkpoint_content(projection_id, source_cursor, payload)?;
        self.backend.compare_and_repair_projection_checkpoint(
            projection_id,
            source_cursor,
            expected_revision,
            payload,
            updated_at_ms,
        )
    }

    pub fn delete_projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        self.backend.delete_projection_checkpoint(projection_id)
    }

    #[must_use]
    pub fn current_commit_cursor(&self) -> u64 {
        *self.commit_signal.borrow()
    }

    pub fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64> {
        self.backend.durable_commit_cursor()
    }

    pub fn recent_event_lines(
        &self,
        stream_id: &str,
        references: &[RuntimeEventRef],
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String> {
        self.backend.recent_event_lines(
            stream_id,
            references,
            through_cursor,
            max_events,
            max_bytes,
        )
    }

    pub fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        self.backend
            .event_by_idempotency_key(stream_id, idempotency_key)
    }

    pub fn bounded_event_by_idempotency_key(
        &self,
        stream: &str,
        key: &str,
        through: u64,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeBoundedEventLookup> {
        self.backend.bounded_event_by_idempotency_key(stream, key, through, max_bytes)
    }

    pub fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        let result = self.backend.stream_revision(stream_id);
        #[cfg(any(test, feature = "test-fixtures"))]
        if result.is_ok() { self.after_test_read(stream_id, "stream_revision"); }
        result
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        tracing::trace!(stream_id, "reading complete Runtime event stream");
        self.backend.list_stream(stream_id)
    }

    pub fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.list_stream_page_desc(stream_id, limit, offset)
    }

    pub fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        self.backend.stream_event_count(stream_id)
    }

    pub fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .execution_events_for_session(session_id, after_position, limit)
    }

    pub fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .events_for_root_execution(root_execution_id, after_position, limit)
    }

    pub fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .events_for_root_execution_kind(root_execution_id, kind, after_position, limit)
    }

    pub fn events_for_activity(
        &self,
        activity_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .events_for_activity(activity_id, after_position, limit)
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.list_scope(scope, limit)
    }

    pub fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .list_scope_page_asc(scope, after_position, limit)
    }

    pub fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .list_scope_stream_prefix_page_asc(scope, stream_prefix, after_position, limit)
    }

    pub fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .list_scope_kind_page_asc(scope, kind, after_position, limit)
    }

    /// Replay a complete scope in durable commit order without a hidden
    /// cardinality ceiling. Projectors use this API; bounded UI views keep
    /// using [`Self::list_scope`].
    pub fn replay_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut events = Vec::new();
        let mut after_position = None;
        loop {
            let page = self.list_scope_page_asc(scope, after_position, SCOPE_REPLAY_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after_position = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let complete = page.len() < SCOPE_REPLAY_PAGE_SIZE;
            events.extend(page);
            if complete {
                break;
            }
        }
        Ok(events)
    }

    /// Replay one aggregate family without materialising unrelated events in
    /// the same domain. Prefixes are exact text prefixes, not SQL patterns.
    pub fn replay_scope_stream_prefix(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if stream_prefix.is_empty() {
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
        let mut after_position = None;
        loop {
            let page = self.list_scope_stream_prefix_page_asc(
                scope,
                stream_prefix,
                after_position,
                SCOPE_REPLAY_PAGE_SIZE,
            )?;
            if page.is_empty() {
                break;
            }
            after_position = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let complete = page.len() < SCOPE_REPLAY_PAGE_SIZE;
            events.extend(page);
            if complete {
                break;
            }
        }
        Ok(events)
    }

    /// Replay one event kind in durable commit order using the backend's
    /// `(scope, kind, commit_cursor)` index.
    pub fn replay_scope_kind(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut events = Vec::new();
        let mut after_position = None;
        loop {
            let page =
                self.list_scope_kind_page_asc(scope, kind, after_position, SCOPE_REPLAY_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after_position = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let complete = page.len() < SCOPE_REPLAY_PAGE_SIZE;
            events.extend(page);
            if complete {
                break;
            }
        }
        Ok(events)
    }

    pub fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        self.backend.stream_ids_for_scope(scope)
    }

    pub fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        self.backend
            .stream_ids_for_scope_kind_at_sequence(scope, kind, sequence)
    }

    pub fn stream_ids_for_scope_kind_at_sequence_page(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
        after: Option<(u64, String)>,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<(String, u64)>> {
        self.backend
            .stream_ids_for_scope_kind_at_sequence_page(scope, kind, sequence, after, limit)
    }

    pub fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        self.backend
            .latest_stream_statuses_for_scope_kind_at_sequence(scope, kind, sequence)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.all_events(limit)
    }

    pub fn latest_for_stream(
        &self,
        stream_id: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        self.backend.latest_for_stream(stream_id)
    }

    pub fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        self.backend.latest_for_stream_kind(stream_id, kind)
    }

    pub fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        #[cfg(any(test, feature = "test-fixtures"))]
        {
            self.backend.enqueue_session_terminal(
                terminal_id,
                message_id,
                session_id,
                commit_cursor,
                payload_ref,
            )
        }
        #[cfg(not(any(test, feature = "test-fixtures")))]
        {
            let _ = (
                terminal_id,
                message_id,
                session_id,
                commit_cursor,
                payload_ref,
            );
            Err(RuntimeEventStoreError::InvalidTransaction(
                "unfenced terminal enqueue is test-only; use append_transaction_with_terminal"
                    .to_string(),
            ))
        }
    }

    pub fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        self.backend
            .claim_session_terminals(worker_id, now_ms, lease_ms, limit)
    }

    pub fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        self.backend.session_terminal(terminal_id)
    }

    pub fn has_unsettled_session_terminals(
        &self,
        session_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        self.backend.has_unsettled_session_terminals(session_id)
    }

    pub fn validate_terminal_execution_selector(
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> RuntimeEventStoreResult<()> {
        for (name, value) in [
            ("session_id", session_id),
            ("execution_id", execution_id),
            ("turn_id", turn_id),
        ] {
            if value.is_empty() || value.trim() != value || value.contains('\0') {
                return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                    "invalid terminal selector {name}"
                )));
            }
        }
        Ok(())
    }

    pub fn session_terminal_for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        Self::validate_terminal_execution_selector(session_id, execution_id, turn_id)?;
        self.backend
            .session_terminal_for_execution(session_id, execution_id, turn_id)
    }

    pub fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        self.backend
            .materialized_session_terminals_after(session_id, after_commit_cursor, limit)
    }

    pub fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        self.backend.session_terminal_health()
    }

    pub fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        self.backend.blocked_session_terminals(limit)
    }

    pub fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend
            .retry_session_terminal(terminal_id, actor, reason, now_ms)
    }

    pub fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend.adopt_session_terminal_fence(request)
    }

    pub fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend
            .ack_session_terminal(terminal_id, worker_id, expected_revision, now_ms)
    }

    /// Settle a claimed terminal that lost the durable execution fence to a
    /// different terminal outcome (most commonly user cancellation). Unlike
    /// `materialized`, this state never asserts that an assistant transcript
    /// was written.
    pub fn suppress_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend.suppress_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            reason,
            now_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend.fail_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            class,
            error,
            retry_at_ms,
            max_attempts,
            now_ms,
        )
    }
}

fn monotonic_elapsed_ms() -> u64 {
    static PROCESS_CLOCK: OnceLock<Instant> = OnceLock::new();
    u64::try_from(
        PROCESS_CLOCK
            .get_or_init(Instant::now)
            .elapsed()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(1)
}

#[cfg(test)]
mod read_fault_tests {
    use super::*;

    #[test]
    fn one_shot_read_fault_advances_only_selected_stream_at_selected_read() {
        let store = RuntimeEventStore::for_test();
        let append = |store: &RuntimeEventStore| store.append(RuntimeEventInput {
            stream_id: "fault-stream".into(),
            scope: RuntimeEventScope::Program,
            kind: "test.fault-advance".into(),
            status: None, actor: None, refs: vec![], payload: serde_json::json!({}),
        }).unwrap();
        store.inject_read_fault_once("fault-stream".into(), "stream_revision", 2, move |store| { append(store); });
        assert_eq!(store.stream_revision("other-stream").unwrap(), 0);
        assert_eq!(store.stream_revision("fault-stream").unwrap(), 0);
        assert_eq!(store.stream_revision("fault-stream").unwrap(), 0);
        assert_eq!(store.stream_revision("fault-stream").unwrap(), 1);
        assert_eq!(store.stream_revision("fault-stream").unwrap(), 1);
    }
}
