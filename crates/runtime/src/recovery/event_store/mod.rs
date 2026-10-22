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
}

impl RuntimeEventStore {
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
        let receipt =
            self.with_stream_locks(&stream_ids, || self.backend.append_transaction(request))?;
        self.publish_commit(receipt.commit_cursor);
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
        let receipt = self.with_stream_locks(&streams, || {
            self.backend.append_checked_transaction(request, validator)
        })?;
        self.publish_commit(receipt.commit.commit_cursor);
        Ok(receipt)
    }

    pub fn list_stream_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.list_stream_after(
            stream_id,
            after_sequence,
            through_sequence,
            max_events,
            max_bytes,
        )
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
        self.backend
            .list_stream_before(stream_id, through_sequence, max_events, max_bytes)
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
        let receipt = self.backend.append_transaction(request)?;
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
        self.backend.stream_revision(stream_id)
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
