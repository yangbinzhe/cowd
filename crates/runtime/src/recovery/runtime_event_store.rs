//! Durable, transactional runtime lifecycle event store.
//!
//! A committed transaction is the only externally visible write unit. Graph,
//! node, goal, agent, team, and mission projections therefore observe one
//! monotonic commit cursor and never a partially appended multi-stream update.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{SqliteExecutor, StorageHandle};
use thiserror::Error;

const STORE_SCHEMA_VERSION: i64 = 6;
const SCOPE_REPLAY_PAGE_SIZE: usize = 1_024;
const EVENT_SCHEMA_VERSION: u32 = 1;
const MAX_TRANSACTION_EVENTS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventScope {
    ExecutionGraph,
    ExecutionNode,
    /// Durable hot-state snapshots for a provider-backed execution.  This
    /// must remain distinct from the canonical `ExecutionGraph` stream: graph
    /// registration starts at revision zero while the live reducer may emit
    /// progress before the graph itself is committed.
    ExecutionLive,
    Goal,
    Mission,
    Session,
    SessionInput,
    /// Historical durable record for a session command dispatch.  These
    /// records remain replay-only evidence; command execution is now owned by
    /// the typed SessionInput/ExecutionGraph path.
    SessionCommand,
    Team,
    Agent,
    AgentDefinition,
    TeamTemplate,
    Approval,
    Evolution,
    Knowledge,
    Relation,
    Steward,
    Task,
    Worker,
    Schedule,
    /// Durable definitions, invocations, dispatcher fences and effect-outbox
    /// receipts for Runtime-managed Agent automation.
    ManagedAgent,
    Skill,
    Tool,
    Recovery,
    CrossPlane,
}

impl RuntimeEventScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionGraph => "execution_graph",
            Self::ExecutionNode => "execution_node",
            Self::ExecutionLive => "execution_live",
            Self::Goal => "goal",
            Self::Mission => "mission",
            Self::Session => "session",
            Self::SessionInput => "session_input",
            Self::SessionCommand => "session_command",
            Self::Team => "team",
            Self::Agent => "agent",
            Self::AgentDefinition => "agent_definition",
            Self::TeamTemplate => "team_template",
            Self::Approval => "approval",
            Self::Evolution => "evolution",
            Self::Knowledge => "knowledge",
            Self::Relation => "relation",
            Self::Steward => "steward",
            Self::Task => "task",
            Self::Worker => "worker",
            Self::Schedule => "schedule",
            Self::ManagedAgent => "managed_agent",
            Self::Skill => "skill",
            Self::Tool => "tool",
            Self::Recovery => "recovery",
            Self::CrossPlane => "cross_plane",
        }
    }

    pub fn parse(value: &str) -> Result<Self, RuntimeEventStoreError> {
        match value {
            "execution_graph" => Ok(Self::ExecutionGraph),
            "execution_node" => Ok(Self::ExecutionNode),
            "execution_live" => Ok(Self::ExecutionLive),
            "goal" => Ok(Self::Goal),
            "mission" => Ok(Self::Mission),
            "session" => Ok(Self::Session),
            "session_input" => Ok(Self::SessionInput),
            "session_command" => Ok(Self::SessionCommand),
            "team" => Ok(Self::Team),
            "agent" => Ok(Self::Agent),
            "agent_definition" => Ok(Self::AgentDefinition),
            "team_template" => Ok(Self::TeamTemplate),
            "approval" => Ok(Self::Approval),
            "evolution" => Ok(Self::Evolution),
            "knowledge" => Ok(Self::Knowledge),
            "relation" => Ok(Self::Relation),
            "steward" => Ok(Self::Steward),
            "task" => Ok(Self::Task),
            "worker" => Ok(Self::Worker),
            "schedule" => Ok(Self::Schedule),
            "managed_agent" => Ok(Self::ManagedAgent),
            "skill" => Ok(Self::Skill),
            "tool" => Ok(Self::Tool),
            "recovery" => Ok(Self::Recovery),
            "cross_plane" => Ok(Self::CrossPlane),
            unknown => Err(RuntimeEventStoreError::UnknownScope(unknown.to_string())),
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeEventStoreError {
    #[error("runtime event scope `{0}` is unknown or requires migration")]
    UnknownScope(String),
    #[error("runtime event store is corrupt: {0}")]
    Corrupt(String),
    #[error("runtime event transaction `{transaction_id}` conflicts with its committed hash")]
    TransactionConflict { transaction_id: String },
    #[error(
        "runtime stream `{stream_id}` revision mismatch: expected {expected}, actual {actual}"
    )]
    StaleRevision {
        stream_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid runtime event transaction: {0}")]
    InvalidTransaction(String),
    #[error("decision lease `{lease_id}` has already been consumed")]
    DecisionLeaseAlreadyConsumed { lease_id: String },
    #[error("runtime event store SQL failure: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("runtime event serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("runtime event store I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("runtime event store storage failure: {0}")]
    Storage(#[from] storage::StorageError),
}

pub type RuntimeEventStoreResult<T> = Result<T, RuntimeEventStoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventRef {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableRuntimeEvent {
    pub event_id: String,
    pub stream_id: String,
    pub sequence: u64,
    pub scope: RuntimeEventScope,
    pub kind: String,
    pub status: Option<String>,
    pub actor: Option<String>,
    pub refs: Vec<RuntimeEventRef>,
    pub payload: serde_json::Value,
    pub created_at_ms: u64,
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub transaction_index: u32,
    pub schema_version: u32,
    pub idempotency_key: Option<String>,
}

pub type RuntimeEventRecord = DurableRuntimeEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventInput {
    pub stream_id: String,
    pub scope: RuntimeEventScope,
    pub kind: String,
    pub status: Option<String>,
    pub actor: Option<String>,
    pub refs: Vec<RuntimeEventRef>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTransactionEventInput {
    pub event: RuntimeEventInput,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "default_event_schema_version")]
    pub schema_version: u32,
}

impl From<RuntimeEventInput> for RuntimeTransactionEventInput {
    fn from(event: RuntimeEventInput) -> Self {
        Self {
            event,
            idempotency_key: None,
            schema_version: EVENT_SCHEMA_VERSION,
        }
    }
}

const fn default_event_schema_version() -> u32 {
    EVENT_SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedStreamRevision {
    pub stream_id: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendTransactionRequest {
    pub transaction_id: String,
    pub expected_streams: Vec<ExpectedStreamRevision>,
    pub events: Vec<RuntimeTransactionEventInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTerminalInput {
    pub terminal_id: String,
    pub message_id: String,
    pub session_id: String,
    /// Canonical graph relation captured in the same commit as the terminal.
    /// Older terminal requests predate this field and intentionally remain
    /// uncorrelated rather than being guessed from their identifier.
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub session_generation: Option<u64>,
    #[serde(default)]
    pub input_sequence: Option<u64>,
    #[serde(default)]
    pub input_claim_owner: Option<String>,
    #[serde(default)]
    pub input_claim_token: Option<String>,
    #[serde(default)]
    pub input_claim_revision: Option<u64>,
    pub payload_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionTerminalFenceAdoption {
    pub terminal_id: String,
    pub expected_terminal_revision: u64,
    pub request_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub session_generation: u64,
    pub input_sequence: u64,
    pub claim_owner: String,
    pub claim_token: String,
    pub claim_revision: u64,
    pub claim_expires_at_ms: u64,
    pub adopted_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedStreamRevision {
    pub stream_id: String,
    pub expected_revision: u64,
    pub committed_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendTransactionReceipt {
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub request_hash: String,
    pub stream_revisions: Vec<CommittedStreamRevision>,
    pub event_ids: Vec<String>,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedEventBatch {
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub events: Vec<RuntimeEventRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSessionOutboxFailureClass {
    Retryable,
    Permanent,
    AuthorizationBlocked,
    CorruptPayload,
}

impl RuntimeSessionOutboxFailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::AuthorizationBlocked => "authorization_blocked",
            Self::CorruptPayload => "corrupt_payload",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionOutboxRecord {
    pub terminal_id: String,
    pub message_id: String,
    pub session_id: String,
    pub commit_cursor: u64,
    pub payload_ref: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub session_generation: Option<u64>,
    #[serde(default)]
    pub input_sequence: Option<u64>,
    #[serde(default)]
    pub input_claim_owner: Option<String>,
    #[serde(default)]
    pub input_claim_token: Option<String>,
    #[serde(default)]
    pub input_claim_revision: Option<u64>,
    pub status: String,
    pub attempts: u32,
    pub next_attempt_at_ms: Option<u64>,
    pub claim_owner: Option<String>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<String>,
    pub last_error: Option<String>,
    pub materialized_at_ms: Option<u64>,
    pub revision: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionOutboxHealth {
    pub pending: u64,
    pub claimed: u64,
    pub retry_scheduled: u64,
    pub materialized: u64,
    pub blocked: u64,
}

/// One committed Runtime transaction preserved for a backend migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventCommitSnapshot {
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub request_hash: String,
    pub created_at_ms: u64,
}

/// A stream revision captured as part of a committed Runtime transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventTransactionStreamSnapshot {
    pub transaction_id: String,
    pub stream_id: String,
    pub expected_revision: u64,
    pub committed_revision: u64,
}

/// The current revision of one Runtime event stream at migration cutover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventStreamHeadSnapshot {
    pub stream_id: String,
    pub revision: u64,
}

/// The durable replay fence for a verified human decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeDecisionLeaseSnapshot {
    pub lease_id: String,
    pub principal_id: String,
    pub review_id: String,
    pub action: String,
    pub scope: String,
    pub evidence_digest: String,
    pub credential_epoch: u64,
    pub consumed_at_ms: u64,
}

/// Complete, ordered RuntimeEvent migration payload. It is domain-owned and
/// is valid only for a quiesced source and an empty verified target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuntimeEventStoreSnapshot {
    pub commits: Vec<RuntimeEventCommitSnapshot>,
    pub events: Vec<DurableRuntimeEvent>,
    pub transaction_streams: Vec<RuntimeEventTransactionStreamSnapshot>,
    pub stream_heads: Vec<RuntimeEventStreamHeadSnapshot>,
    pub session_outbox: Vec<RuntimeSessionOutboxRecord>,
    pub decision_leases: Vec<RuntimeDecisionLeaseSnapshot>,
}

impl RuntimeEventStoreSnapshot {
    /// Stable digest used to prove source/target migration equivalence. The
    /// digest does not include a database path, URL, pool identity, or secret.
    pub fn canonical_digest(&self) -> RuntimeEventStoreResult<String> {
        let mut canonical = self.clone();
        canonical.canonicalize();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&canonical)?)
        ))
    }

    /// Reject malformed cross-table linkages before a target backend accepts a
    /// copy. Operational source quiescence remains the migration
    /// coordinator's responsibility.
    pub fn validate(&self) -> RuntimeEventStoreResult<()> {
        validate_migration_snapshot(self)
    }

    fn canonicalize(&mut self) {
        self.commits.sort_by_key(|commit| commit.commit_cursor);
        self.events
            .sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        self.transaction_streams.sort_by(|left, right| {
            (&left.transaction_id, &left.stream_id).cmp(&(&right.transaction_id, &right.stream_id))
        });
        self.stream_heads
            .sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
        self.session_outbox
            .sort_by(|left, right| left.terminal_id.cmp(&right.terminal_id));
        self.decision_leases
            .sort_by(|left, right| left.lease_id.cmp(&right.lease_id));
    }
}

/// Backend contract for the durable Runtime lifecycle ledger.
///
/// Runtime business callers must use [`RuntimeEventStore`], never this port.
/// It is public solely so an isolated infrastructure crate can provide a
/// backend without pulling a database driver into the Runtime dependency tree.
/// Every method preserves event transaction, revision, lease, and outbox
/// semantics; a backend must not provide a partial implementation.
pub trait RuntimeEventStoreBackend: std::fmt::Debug + Send + Sync {
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String>;
    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
    fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
    #[allow(clippy::too_many_arguments)]
    fn consume_verified_decision_lease(
        &self,
        lease_id: &str,
        principal_id: &str,
        review_id: &str,
        action: &str,
        scope: &str,
        evidence_digest: &str,
        credential_epoch: u64,
        consumed_at_ms: u64,
    ) -> RuntimeEventStoreResult<()>;
    fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
    fn append_batch_if_revision(
        &self,
        stream_id: String,
        expected_revision: u64,
        transaction_id: String,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
    fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>>;
    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>>;
    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64>;
    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String>;
    /// Return one newest-first page without materialising the whole stream.
    ///
    /// Administrative timelines (for example the cross-plane audit ledger)
    /// must keep their paging boundary in the durable backend.  Pulling an
    /// ever-growing immutable stream into a process and paging it afterwards
    /// turns a read-only UI request into an unbounded allocation.
    fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn stream_event_count(&self, stream_id: &str) -> Result<usize, String>;
    fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>>;
    /// Return streams whose event at `sequence` has the requested scope and kind.
    ///
    /// Canonical aggregate discovery must use this bounded predicate instead of
    /// loading every stream merely to inspect its first event.
    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>>;
    /// Return the latest status for canonical streams identified by one exact
    /// first-event predicate. Backends must answer this without loading event
    /// payloads or replaying the streams.
    fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>>;
    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String>;
    fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String>;
    fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
    fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
    fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
    /// True while this Session still owns a Runtime terminal that has not
    /// reached the durable `materialized` state.
    fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool>;
    fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
    fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth>;
    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
    fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
    fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
    fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
    #[allow(clippy::too_many_arguments)]
    fn fail_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
    fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot>;
    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()>;
}

/// The sole Runtime-facing durable event-store API. Runtime callers depend on
/// lifecycle semantics rather than a concrete database, path, pragma, or SQL
/// schema. Backend adapters are composed explicitly at the trusted host root.
#[derive(Debug)]
pub struct RuntimeEventStore {
    backend: Arc<dyn RuntimeEventStoreBackend>,
    commit_signal: tokio::sync::watch::Sender<u64>,
}

impl RuntimeEventStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::try_open(path).map_err(|error| error.to_string())
    }

    pub fn try_open(path: impl AsRef<Path>) -> RuntimeEventStoreResult<Self> {
        SqliteRuntimeEventStore::try_open(path).map(|store| Self::from_backend(Arc::new(store)))
    }

    pub fn open_in_memory() -> Result<Self, String> {
        Self::try_open_in_memory().map_err(|error| error.to_string())
    }

    pub fn try_open_in_memory() -> RuntimeEventStoreResult<Self> {
        SqliteRuntimeEventStore::try_open_in_memory()
            .map(|store| Self::from_backend(Arc::new(store)))
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
            backend,
            commit_signal,
        }
    }

    pub fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        let event = self.backend.append(input)?;
        self.publish_commit(event.commit_cursor);
        Ok(event)
    }

    pub fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let receipt = self.backend.append_transaction(request)?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    pub fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let receipt = self
            .backend
            .append_transaction_with_terminal(request, terminal)?;
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
        let receipt = self
            .backend
            .append_transaction_with_verified_decision_lease(request, lease)?;
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
        let receipt = self.backend.append_batch_if_revision(
            stream_id.into(),
            expected_revision,
            transaction_id.into(),
            events,
        )?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.commit_signal.subscribe()
    }

    fn publish_commit(&self, cursor: u64) {
        if cursor > *self.commit_signal.borrow() {
            self.commit_signal.send_replace(cursor);
        }
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        self.backend.events_after_cursor(cursor, max_commits)
    }

    pub fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        self.backend
            .event_by_idempotency_key(stream_id, idempotency_key)
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

    /// Export a canonical, read-only migration payload from a quiesced source.
    pub fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
        self.backend.export_migration_snapshot()
    }

    /// Import a migration payload into an empty, already verified target.
    /// Normal Runtime execution must never call this API.
    pub fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        self.backend.import_migration_snapshot(snapshot)?;
        if let Some(commit) = snapshot.commits.last() {
            self.publish_commit(commit.commit_cursor);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SqliteRuntimeEventStore {
    executor: SqliteExecutor,
}

impl SqliteRuntimeEventStore {
    fn try_open(path: impl AsRef<Path>) -> RuntimeEventStoreResult<Self> {
        let path = path.as_ref().to_path_buf();
        let handle = StorageHandle::sqlite(
            "runtime_events",
            path.clone(),
            "runtime",
            "runtime_event_executor",
        );
        let executor = SqliteExecutor::for_handle(&handle)?;
        let mut conn = executor.checkout()?;
        configure_connection(&conn, false)?;
        migrate_schema(&mut conn)?;
        Ok(Self { executor })
    }

    fn try_open_in_memory() -> RuntimeEventStoreResult<Self> {
        let executor = SqliteExecutor::in_memory("runtime-event-store")?;
        let mut conn = executor.checkout()?;
        configure_connection(&conn, true)?;
        migrate_schema(&mut conn)?;
        Ok(Self { executor })
    }

    /// Compatibility convenience for existing single-stream producers.
    ///
    /// New graph/goal lifecycle code must use `append_transaction` with an
    /// explicit expected revision and stable transaction id.
    pub fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        self.append_single(input).map_err(|error| error.to_string())
    }

    fn append_single(
        &self,
        input: RuntimeEventInput,
    ) -> RuntimeEventStoreResult<DurableRuntimeEvent> {
        validate_event(&input)?;
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let expected_revision = stream_head(&tx, &input.stream_id)?;
        let request = AppendTransactionRequest {
            transaction_id: format!("runtime-tx-{}", uuid::Uuid::new_v4()),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: input.stream_id.clone(),
                expected_revision,
            }],
            events: vec![input.into()],
        };
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        tx.commit()?;
        load_transaction_events(&conn, &receipt.transaction_id)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                RuntimeEventStoreError::Corrupt("committed transaction has no event".to_string())
            })
    }

    pub fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_transaction(&request)?;
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        tx.commit()?;
        Ok(receipt)
    }

    pub fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let receipt = append_transaction_in_tx(&tx, &request, Some(&terminal))?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Atomically records a previously verified human decision lease.  This
    /// is deliberately narrower than the generic event transaction API: it
    /// cannot create lifecycle events and a duplicate lease is rejected.
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
        if lease_id.trim().is_empty()
            || principal_id.trim().is_empty()
            || review_id.trim().is_empty()
            || action.trim().is_empty()
            || scope.trim().is_empty()
            || evidence_digest.trim().is_empty()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "decision lease consumption requires non-empty bound claims".to_string(),
            ));
        }
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO runtime_consumed_decision_leases \
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease_id,
                principal_id,
                review_id,
                action,
                scope,
                evidence_digest,
                credential_epoch as i64,
                consumed_at_ms as i64,
            ],
        )?;
        if inserted == 0 {
            return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                lease_id: lease_id.to_string(),
            });
        }
        tx.commit()?;
        Ok(())
    }

    /// Commit lifecycle events and consume one already-verified human lease in
    /// the same SQLite transaction.  A release decision is never allowed to
    /// consume authorization first and mutate a projection later: either both
    /// durable effects become visible or neither does.
    pub(crate) fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_decision_lease_claims(
            lease.lease_id(),
            lease.principal_id(),
            lease.review_id(),
            lease.action(),
            lease.scope(),
            lease.evidence_digest(),
        )?;
        validate_transaction(&request)?;
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO runtime_consumed_decision_leases \
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease.lease_id(),
                lease.principal_id(),
                lease.review_id(),
                lease.action(),
                lease.scope(),
                lease.evidence_digest(),
                lease.credential_epoch() as i64,
                now_ms() as i64,
            ],
        )?;
        if inserted == 0 {
            // A retry of the exact committed transaction is safe only when
            // the stored lease claims are identical. Any other replay is an
            // authorization error, even if the event transaction happens to
            // have an idempotent key collision.
            let existing = tx.query_row(
                "SELECT principal_id, review_id, action, scope, evidence_digest, credential_epoch \
                     FROM runtime_consumed_decision_leases WHERE lease_id = ?1",
                params![lease.lease_id()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )?;
            let matches = existing.0 == lease.principal_id()
                && existing.1 == lease.review_id()
                && existing.2 == lease.action()
                && existing.3 == lease.scope()
                && existing.4 == lease.evidence_digest()
                && existing.5 == lease.credential_epoch() as i64;
            if !receipt.duplicate || !matches {
                return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                    lease_id: lease.lease_id().to_string(),
                });
            }
        }
        tx.commit()?;
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
        if events
            .iter()
            .any(|event| event.event.stream_id != stream_id)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "single-stream batch contains an event for another stream".to_string(),
            ));
        }
        self.append_transaction(AppendTransactionRequest {
            transaction_id: transaction_id.into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id,
                expected_revision,
            }],
            events,
        })
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        if max_commits == 0 {
            return Ok(Vec::new());
        }
        let conn = self.executor.checkout()?;
        let mut stmt = conn.prepare(
            "SELECT commit_cursor, transaction_id FROM runtime_commits \
             WHERE commit_cursor > ?1 ORDER BY commit_cursor ASC LIMIT ?2",
        )?;
        let commits = stmt
            .query_map(params![cursor as i64, max_commits as i64], |row| {
                Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        commits
            .into_iter()
            .map(|(commit_cursor, transaction_id)| {
                Ok(CommittedEventBatch {
                    commit_cursor,
                    events: load_transaction_events(&conn, &transaction_id)?,
                    transaction_id,
                })
            })
            .collect()
    }

    pub fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let conn = self.executor.checkout()?;
        conn.query_row(
            &format!(
                "{} WHERE stream_id = ?1 AND idempotency_key = ?2",
                event_select()
            ),
            params![stream_id, idempotency_key],
            row_to_event,
        )
        .optional()
        .map_err(RuntimeEventStoreError::from)
    }

    pub fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        let conn = self.executor.checkout()?;
        stream_head(&conn, stream_id)
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            &format!(
                "{} WHERE stream_id = ?1 ORDER BY sequence ASC",
                event_select()
            ),
            params![stream_id],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE scope = ?1
                 AND (?2 = 0 OR commit_cursor > ?2
                      OR (commit_cursor = ?2 AND transaction_index > ?3))
                 ORDER BY commit_cursor ASC, transaction_index ASC
                 LIMIT ?4",
                event_select()
            ),
            params![
                scope.as_str(),
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if stream_prefix.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE scope = ?1
                 AND substr(stream_id, 1, length(?2)) = ?2
                 AND (?3 = 0 OR commit_cursor > ?3
                      OR (commit_cursor = ?3 AND transaction_index > ?4))
                 ORDER BY commit_cursor ASC, transaction_index ASC
                 LIMIT ?5",
                event_select()
            ),
            params![
                scope.as_str(),
                stream_prefix,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.query_events(
            &format!(
                "{} WHERE stream_id = ?1 ORDER BY sequence DESC LIMIT ?2 OFFSET ?3",
                event_select()
            ),
            params![stream_id, limit as i64, offset as i64],
        )
        .map_err(|error| error.to_string())
    }

    pub fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        let conn = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM runtime_events WHERE stream_id = ?1",
                params![stream_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        usize::try_from(count).map_err(|_| "runtime stream event count overflow".to_string())
    }

    /// Resolve the canonical graph streams that produced terminal work for a
    /// session. The terminal request is the durable bridge from a session
    /// input to its graph; callers must not reconstruct this relation from
    /// transcript text or a client-side naming convention.
    pub fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if session_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let direct_refs = self.events_for_ref("session", session_id, after_position, limit)?;
        let terminal_requests = self.events_for_ref_kind(
            "session",
            session_id,
            "runtime.session.terminal_requested",
            after_position,
            limit,
        )?;
        let graph_ids = terminal_requests
            .iter()
            .flat_map(|event| event.refs.iter())
            .filter(|reference| reference.kind == "execution_graph")
            .map(|reference| reference.id.clone())
            .collect::<BTreeSet<_>>();
        let mut related = direct_refs;
        related.extend(terminal_requests);
        let mut pending = graph_ids.into_iter().collect::<VecDeque<_>>();
        let mut visited = BTreeSet::new();
        while let Some(graph_id) = pending.pop_front() {
            if visited.len() >= limit || !visited.insert(graph_id.clone()) {
                continue;
            }
            related.extend(self.list_stream(&graph_id)?);
            // Live status is persisted on an isolated stream so early
            // progress cannot collide with canonical graph revisions.  Once
            // a graph is related to this session, retain those snapshots in
            // the durable session timeline as well.
            related.extend(self.list_stream(&format!("execution-live:{graph_id}"))?);
            let lineage_stream = format!("execution-lineage:{graph_id}");
            let lineage_events = self.list_stream(&lineage_stream)?;
            for event in &lineage_events {
                if event.kind != "execution.lineage.child_registered.v1" {
                    continue;
                }
                if let Some(child_id) = event
                    .payload
                    .get("child_execution_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    pending.push_back(child_id.to_string());
                }
            }
            related.extend(lineage_events);
        }
        related.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        related.dedup_by(|left, right| left.event_id == right.event_id);
        Ok(related
            .into_iter()
            .filter(|event| {
                after_position.is_none_or(|position| {
                    (event.commit_cursor, event.transaction_index) > position
                })
            })
            .take(limit)
            .collect())
    }

    fn events_for_ref(
        &self,
        ref_kind: &str,
        ref_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if ref_kind.trim().is_empty() || ref_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE event_id IN (
                    SELECT event_id FROM runtime_event_refs
                    WHERE ref_kind = ?1 AND ref_id = ?2
                )
                AND (?3 = 0 OR commit_cursor > ?3
                     OR (commit_cursor = ?3 AND transaction_index > ?4))
                ORDER BY commit_cursor ASC, transaction_index ASC
                LIMIT ?5",
                event_select()
            ),
            params![
                ref_kind,
                ref_id,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    fn events_for_ref_kind(
        &self,
        ref_kind: &str,
        ref_id: &str,
        event_kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if ref_kind.trim().is_empty()
            || ref_id.trim().is_empty()
            || event_kind.trim().is_empty()
            || limit == 0
        {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE event_id IN (
                    SELECT event_id FROM runtime_event_refs
                    WHERE ref_kind = ?1 AND ref_id = ?2
                )
                AND kind = ?3
                AND (?4 = 0 OR commit_cursor > ?4
                     OR (commit_cursor = ?4 AND transaction_index > ?5))
                ORDER BY commit_cursor ASC, transaction_index ASC
                LIMIT ?6",
                event_select()
            ),
            params![
                ref_kind,
                ref_id,
                event_kind,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            &format!(
                "{} WHERE scope = ?1 ORDER BY commit_cursor DESC, transaction_index DESC LIMIT ?2",
                event_select()
            ),
            params![scope.as_str(), limit as i64],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE scope = ?1 AND kind = ?2
                 AND (?3 = 0 OR commit_cursor > ?3
                      OR (commit_cursor = ?3 AND transaction_index > ?4))
                 ORDER BY commit_cursor ASC, transaction_index ASC LIMIT ?5",
                event_select()
            ),
            params![
                scope.as_str(),
                kind,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let conn = self.executor.checkout()?;
        let mut statement = conn.prepare(
            "SELECT stream_id FROM runtime_events
             WHERE scope = ?1
             GROUP BY stream_id
             ORDER BY MAX(commit_cursor) ASC, stream_id ASC",
        )?;
        let stream_ids = statement
            .query_map(params![scope.as_str()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(stream_ids)
    }

    pub fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let sequence = i64::try_from(sequence).map_err(|_| {
            RuntimeEventStoreError::Corrupt(format!(
                "runtime event sequence `{sequence}` exceeds SQLite range"
            ))
        })?;
        let conn = self.executor.checkout()?;
        let mut statement = conn.prepare(
            "SELECT stream_id FROM runtime_events
             WHERE scope = ?1 AND kind = ?2 AND sequence = ?3
             ORDER BY commit_cursor ASC, stream_id ASC",
        )?;
        let stream_ids = statement
            .query_map(params![scope.as_str(), kind, sequence], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(stream_ids)
    }

    pub fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        let sequence = i64::try_from(sequence).map_err(|_| {
            RuntimeEventStoreError::Corrupt(format!(
                "runtime event sequence `{sequence}` exceeds SQLite range"
            ))
        })?;
        let conn = self.executor.checkout()?;
        let mut statement = conn.prepare(
            "WITH candidates AS (
                 SELECT stream_id FROM runtime_events
                  WHERE scope=?1 AND kind=?2 AND sequence=?3
             ),
             latest AS (
                 SELECT event.stream_id, event.status,
                        ROW_NUMBER() OVER (
                            PARTITION BY event.stream_id
                            ORDER BY event.sequence DESC
                        ) AS rank
                   FROM runtime_events AS event
                   JOIN candidates USING(stream_id)
             )
             SELECT stream_id, status FROM latest
              WHERE rank=1 ORDER BY stream_id ASC",
        )?;
        let statuses = statement
            .query_map(params![scope.as_str(), kind, sequence], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(statuses)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            &format!(
                "{} ORDER BY commit_cursor DESC, transaction_index DESC LIMIT ?1",
                event_select()
            ),
            params![limit as i64],
        )
        .map_err(|error| error.to_string())
    }

    pub fn latest_for_stream(
        &self,
        stream_id: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let conn = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        conn.query_row(
            &format!(
                "{} WHERE stream_id = ?1 ORDER BY sequence DESC LIMIT 1",
                event_select()
            ),
            params![stream_id],
            row_to_event,
        )
        .optional()
        .map_err(|error| error.to_string())
    }

    pub fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let conn = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        conn.query_row(
            &format!(
                "{} WHERE stream_id = ?1 AND kind = ?2 ORDER BY sequence DESC LIMIT 1",
                event_select()
            ),
            params![stream_id, kind],
            row_to_event,
        )
        .optional()
        .map_err(|error| error.to_string())
    }

    fn query_events<P>(
        &self,
        sql: &str,
        params: P,
    ) -> RuntimeEventStoreResult<Vec<DurableRuntimeEvent>>
    where
        P: rusqlite::Params,
    {
        let conn = self.executor.checkout()?;
        let mut stmt = conn.prepare(sql)?;
        let events = stmt
            .query_map(params, row_to_event)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events)
    }

    /// Insert one terminal delivery exactly once. A duplicate terminal ID is
    /// accepted only when every immutable field matches the committed row.
    #[cfg(any(test, feature = "test-fixtures"))]
    fn enqueue_unfenced_session_terminal_for_test(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let conn = self.executor.checkout()?;
        conn.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
              request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
              input_claim_revision, status, revision)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'pending', 0)
             ON CONFLICT(terminal_id) DO NOTHING",
            params![
                terminal_id,
                message_id,
                session_id,
                commit_cursor as i64,
                payload_ref
            ],
        )?;
        let record = query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!(
                "terminal outbox `{terminal_id}` disappeared after enqueue"
            ))
        })?;
        if record.message_id != message_id
            || record.session_id != session_id
            || record.commit_cursor != commit_cursor
            || record.payload_ref != payload_ref
        {
            return Err(RuntimeEventStoreError::TransactionConflict {
                transaction_id: terminal_id.to_string(),
            });
        }
        Ok(record)
    }

    pub fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal claim requires worker, lease and limit".to_string(),
            ));
        }
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ids = {
            let mut statement = tx.prepare(
                "SELECT terminal_id FROM runtime_session_outbox
                 WHERE (status IN ('pending','retry_scheduled') AND COALESCE(next_attempt_at, 0) <= ?1)
                    OR (status = 'claimed' AND claim_expires_at <= ?1)
                 ORDER BY commit_cursor, terminal_id LIMIT ?2",
            )?;
            let ids = statement
                .query_map(params![now_ms as i64, limit as i64], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            ids
        };
        let expires = now_ms.saturating_add(lease_ms);
        let mut claimed = Vec::new();
        for id in ids {
            let changed = tx.execute(
                "UPDATE runtime_session_outbox SET status='claimed', attempts=attempts+1,
                 claim_owner=?1, claim_expires_at=?2, revision=revision+1
                 WHERE terminal_id=?3 AND ((status IN ('pending','retry_scheduled') AND
                 COALESCE(next_attempt_at,0)<=?4) OR (status='claimed' AND claim_expires_at<=?4))",
                params![worker_id, expires as i64, id, now_ms as i64],
            )?;
            if changed == 1 {
                claimed.push(query_runtime_session_outbox(&tx, &id)?.ok_or_else(|| {
                    RuntimeEventStoreError::Corrupt(format!("claimed terminal `{id}` vanished"))
                })?);
            }
        }
        tx.commit()?;
        Ok(claimed)
    }

    pub fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        let connection = self.executor.checkout()?;
        query_runtime_session_outbox(&connection, terminal_id)
    }

    pub fn has_unsettled_session_terminals(
        &self,
        session_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        let connection = self.executor.checkout()?;
        connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM runtime_session_outbox
                      WHERE session_id=?1 AND status!='materialized'
                 )",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(RuntimeEventStoreError::from)
    }

    /// Return already materialized terminal commits after a durable runtime
    /// cursor. Gateway uses this for resumable surface streams; the transient
    /// session bus is deliberately not the source of truth for final replies.
    pub fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let conn = self.executor.checkout()?;
        let mut statement = conn.prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox
              WHERE session_id=?1
                AND status='materialized'
                AND commit_cursor>?2
              ORDER BY commit_cursor, terminal_id
              LIMIT ?3",
        )?;
        let records = statement
            .query_map(
                params![
                    session_id,
                    after_commit_cursor as i64,
                    limit.clamp(1, 500) as i64,
                ],
                row_to_runtime_session_outbox,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(records)
    }

    pub fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        let conn = self.executor.checkout()?;
        let mut health = RuntimeSessionOutboxHealth::default();
        let mut statement =
            conn.prepare("SELECT status, COUNT(*) FROM runtime_session_outbox GROUP BY status")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        for row in rows {
            let (status, count) = row?;
            match status.as_str() {
                "pending" => health.pending = count,
                "claimed" => health.claimed = count,
                "retry_scheduled" => health.retry_scheduled = count,
                "materialized" => health.materialized = count,
                "blocked" => health.blocked = count,
                _ => {}
            }
        }
        Ok(health)
    }

    pub fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let conn = self.executor.checkout()?;
        let mut statement = conn.prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE status='blocked'
               ORDER BY COALESCE(next_attempt_at, 0), commit_cursor, terminal_id LIMIT ?1",
        )?;
        let records = statement
            .query_map(
                params![limit.clamp(1, 500) as i64],
                row_to_runtime_session_outbox,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(records)
    }

    pub fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "manual terminal retry requires actor and reason".to_string(),
            ));
        }
        let conn = self.executor.checkout()?;
        let changed = conn.execute(
            "UPDATE runtime_session_outbox SET status='retry_scheduled', next_attempt_at=?1,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=NULL,
             last_error=?2, revision=revision+1 WHERE terminal_id=?3 AND status='blocked'",
            params![
                now_ms as i64,
                format!("manual retry by {actor}: {reason}"),
                terminal_id
            ],
        )?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{terminal_id}` is not blocked"
            )));
        }
        query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }

    pub fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        if request.terminal_id.trim().is_empty()
            || request.request_id.trim().is_empty()
            || request.session_id.trim().is_empty()
            || request.turn_id.trim().is_empty()
            || request.claim_owner.trim().is_empty()
            || request.claim_token.trim().is_empty()
            || request.session_generation == 0
            || request.claim_revision == 0
            || request.claim_expires_at_ms <= request.adopted_at_ms
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal fence adoption requires live terminal, request, session, turn and claim identities"
                    .to_string(),
            ));
        }
        let mut conn = self.executor.checkout()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current =
            query_runtime_session_outbox(&tx, &request.terminal_id)?.ok_or_else(|| {
                RuntimeEventStoreError::Corrupt(format!(
                    "terminal `{}` is missing",
                    request.terminal_id
                ))
            })?;
        if current.request_id.as_deref() != Some(request.request_id.as_str())
            || current.session_id != request.session_id
            || current.turn_id.as_deref() != Some(request.turn_id.as_str())
            || current.session_generation != Some(request.session_generation)
            || current.input_sequence != Some(request.input_sequence)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` identity does not match the current Session claim",
                request.terminal_id
            )));
        }
        let already_adopted = current.input_claim_owner.as_deref()
            == Some(request.claim_owner.as_str())
            && current.input_claim_token.as_deref() == Some(request.claim_token.as_str())
            && current.input_claim_revision == Some(request.claim_revision)
            && current.input_sequence == Some(request.input_sequence);
        if already_adopted {
            return Ok(current);
        }
        if current.revision != request.expected_terminal_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        if current.status == "materialized" {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "materialized terminal `{}` cannot adopt a different Session claim",
                request.terminal_id
            )));
        }
        if !matches!(
            current.status.as_str(),
            "pending" | "retry_scheduled" | "blocked" | "claimed"
        ) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` in state `{}` cannot adopt a Session claim",
                request.terminal_id, current.status
            )));
        }
        if current
            .input_claim_revision
            .is_some_and(|revision| request.claim_revision <= revision)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` cannot regress Session claim revision",
                request.terminal_id
            )));
        }
        if current.status == "claimed"
            && !current
                .claim_expires_at_ms
                .is_some_and(|expires| expires <= request.adopted_at_ms)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` has an active delivery claim",
                request.terminal_id
            )));
        }
        let changed = tx.execute(
            "UPDATE runtime_session_outbox
                SET input_sequence=?1, input_claim_owner=?2, input_claim_token=?3, input_claim_revision=?4,
                    status='pending', next_attempt_at=0, claim_owner=NULL,
                    claim_expires_at=NULL, failure_class=NULL, last_error=NULL,
                    materialized_at=NULL, revision=revision+1
              WHERE terminal_id=?5 AND revision=?6",
            params![
                request.input_sequence as i64,
                request.claim_owner,
                request.claim_token,
                request.claim_revision as i64,
                request.terminal_id,
                request.expected_terminal_revision as i64,
            ],
        )?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        let adopted =
            query_runtime_session_outbox(&tx, &request.terminal_id)?.ok_or_else(|| {
                RuntimeEventStoreError::Corrupt(format!(
                    "terminal `{}` vanished after fence adoption",
                    request.terminal_id
                ))
            })?;
        tx.commit()?;
        Ok(adopted)
    }

    pub fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            "materialized",
            None,
            None,
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
        let current = {
            let conn = self.executor.checkout()?;
            query_runtime_session_outbox(&conn, terminal_id)?
        }
        .ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` missing"))
        })?;
        let retry = class == RuntimeSessionOutboxFailureClass::Retryable
            && current.attempts < max_attempts.max(1);
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            if retry { "retry_scheduled" } else { "blocked" },
            Some((class.as_str(), error)),
            retry.then_some(retry_at_ms),
            now_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn transition_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        status: &str,
        failure: Option<(&str, &str)>,
        retry_at_ms: Option<u64>,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let conn = self.executor.checkout()?;
        let (failure_class, last_error) = failure.unzip();
        let changed = conn.execute(
            "UPDATE runtime_session_outbox SET status=?1, next_attempt_at=?2,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=?3, last_error=?4,
             materialized_at=CASE WHEN ?1='materialized' THEN ?5 ELSE materialized_at END,
             revision=revision+1 WHERE terminal_id=?6 AND status='claimed'
             AND claim_owner=?7 AND revision=?8",
            params![
                status,
                retry_at_ms.map(|value| value as i64),
                failure_class,
                last_error,
                now_ms as i64,
                terminal_id,
                worker_id,
                expected_revision as i64,
            ],
        )?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("terminal:{terminal_id}"),
                expected: expected_revision,
                actual: query_runtime_session_outbox(&conn, terminal_id)?
                    .map_or(0, |record| record.revision),
            });
        }
        query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }
}

impl RuntimeEventStoreBackend for SqliteRuntimeEventStore {
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        Self::append(self, input)
    }

    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_transaction(self, request)
    }

    fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_transaction_with_terminal(self, request, terminal)
    }

    fn consume_verified_decision_lease(
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
        Self::consume_verified_decision_lease(
            self,
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

    fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_transaction_with_verified_decision_lease(self, request, lease)
    }

    fn append_batch_if_revision(
        &self,
        stream_id: String,
        expected_revision: u64,
        transaction_id: String,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_batch_if_revision(self, stream_id, expected_revision, transaction_id, events)
    }

    fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        Self::events_after_cursor(self, cursor, max_commits)
    }

    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        Self::event_by_idempotency_key(self, stream_id, idempotency_key)
    }

    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        Self::stream_revision(self, stream_id)
    }

    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_stream(self, stream_id)
    }

    fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_stream_page_desc(self, stream_id, limit, offset)
    }

    fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        Self::stream_event_count(self, stream_id)
    }

    fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::execution_events_for_session(self, session_id, after_position, limit)
    }

    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope(self, scope, limit)
    }

    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope_page_asc(self, scope, after_position, limit)
    }

    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope_stream_prefix_page_asc(self, scope, stream_prefix, after_position, limit)
    }

    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope_kind_page_asc(self, scope, kind, after_position, limit)
    }

    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        Self::stream_ids_for_scope(self, scope)
    }

    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        Self::stream_ids_for_scope_kind_at_sequence(self, scope, kind, sequence)
    }

    fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        Self::latest_stream_statuses_for_scope_kind_at_sequence(self, scope, kind, sequence)
    }

    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::all_events(self, limit)
    }

    fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String> {
        Self::latest_for_stream(self, stream_id)
    }

    fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        Self::latest_for_stream_kind(self, stream_id, kind)
    }

    fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        #[cfg(any(test, feature = "test-fixtures"))]
        {
            Self::enqueue_unfenced_session_terminal_for_test(
                self,
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

    fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Self::claim_session_terminals(self, worker_id, now_ms, lease_ms, limit)
    }

    fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        Self::session_terminal(self, terminal_id)
    }

    fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool> {
        Self::has_unsettled_session_terminals(self, session_id)
    }

    fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Self::materialized_session_terminals_after(self, session_id, after_commit_cursor, limit)
    }

    fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        Self::session_terminal_health(self)
    }

    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Self::blocked_session_terminals(self, limit)
    }

    fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::retry_session_terminal(self, terminal_id, actor, reason, now_ms)
    }

    fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::adopt_session_terminal_fence(self, request)
    }

    fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::ack_session_terminal(self, terminal_id, worker_id, expected_revision, now_ms)
    }

    fn fail_session_terminal(
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
        Self::fail_session_terminal(
            self,
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

    fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
        let conn = self.executor.checkout()?;
        export_sqlite_migration_snapshot(&conn)
    }

    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        let mut conn = self.executor.checkout()?;
        import_sqlite_migration_snapshot(&mut conn, snapshot)
    }
}

fn export_sqlite_migration_snapshot(
    conn: &Connection,
) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
    let commits = conn
        .prepare(
            "SELECT commit_cursor, transaction_id, request_hash, created_at_ms
               FROM runtime_commits ORDER BY commit_cursor ASC",
        )?
        .query_map([], |row| {
            Ok(RuntimeEventCommitSnapshot {
                commit_cursor: row.get::<_, i64>(0)? as u64,
                transaction_id: row.get(1)?,
                request_hash: row.get(2)?,
                created_at_ms: row.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let events = conn
        .prepare(&format!(
            "{} ORDER BY commit_cursor ASC, transaction_index ASC",
            event_select()
        ))?
        .query_map([], row_to_event)?
        .collect::<Result<Vec<_>, _>>()?;
    let transaction_streams = conn
        .prepare(
            "SELECT transaction_id, stream_id, expected_revision, committed_revision
               FROM runtime_transaction_streams ORDER BY transaction_id ASC, stream_id ASC",
        )?
        .query_map([], |row| {
            Ok(RuntimeEventTransactionStreamSnapshot {
                transaction_id: row.get(0)?,
                stream_id: row.get(1)?,
                expected_revision: row.get::<_, i64>(2)? as u64,
                committed_revision: row.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let stream_heads = conn
        .prepare("SELECT stream_id, revision FROM runtime_stream_heads ORDER BY stream_id ASC")?
        .query_map([], |row| {
            Ok(RuntimeEventStreamHeadSnapshot {
                stream_id: row.get(0)?,
                revision: row.get::<_, i64>(1)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let session_outbox = conn
        .prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox ORDER BY terminal_id ASC",
        )?
        .query_map([], row_to_runtime_session_outbox)?
        .collect::<Result<Vec<_>, _>>()?;
    let decision_leases = conn
        .prepare(
            "SELECT lease_id, principal_id, review_id, action, scope, evidence_digest,
                    credential_epoch, consumed_at_ms
               FROM runtime_consumed_decision_leases ORDER BY lease_id ASC",
        )?
        .query_map([], |row| {
            Ok(RuntimeDecisionLeaseSnapshot {
                lease_id: row.get(0)?,
                principal_id: row.get(1)?,
                review_id: row.get(2)?,
                action: row.get(3)?,
                scope: row.get(4)?,
                evidence_digest: row.get(5)?,
                credential_epoch: row.get::<_, i64>(6)? as u64,
                consumed_at_ms: row.get::<_, i64>(7)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut snapshot = RuntimeEventStoreSnapshot {
        commits,
        events,
        transaction_streams,
        stream_heads,
        session_outbox,
        decision_leases,
    };
    snapshot.canonicalize();
    Ok(snapshot)
}

fn import_sqlite_migration_snapshot(
    conn: &mut Connection,
    snapshot: &RuntimeEventStoreSnapshot,
) -> RuntimeEventStoreResult<()> {
    validate_migration_snapshot(snapshot)?;
    let mut snapshot = snapshot.clone();
    snapshot.canonicalize();
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    for table in [
        "runtime_commits",
        "runtime_events",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_event_refs",
        "runtime_session_outbox",
        "runtime_consumed_decision_leases",
    ] {
        let count = tx.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })?;
        if count != 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "runtime event migration target table `{table}` is not empty"
            )));
        }
    }
    for commit in &snapshot.commits {
        tx.execute(
            "INSERT INTO runtime_commits(commit_cursor, transaction_id, request_hash, created_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                snapshot_i64(commit.commit_cursor, "commit_cursor")?,
                commit.transaction_id,
                commit.request_hash,
                snapshot_i64(commit.created_at_ms, "created_at_ms")?,
            ],
        )?;
    }
    for event in &snapshot.events {
        tx.execute(
            "INSERT INTO runtime_events
             (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms,
              commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                event.event_id,
                event.stream_id,
                snapshot_i64(event.sequence, "sequence")?,
                event.scope.as_str(),
                event.kind,
                event.status,
                event.actor,
                serde_json::to_string(&event.payload)?,
                serde_json::to_string(&event.refs)?,
                snapshot_i64(event.created_at_ms, "created_at_ms")?,
                snapshot_i64(event.commit_cursor, "commit_cursor")?,
                event.transaction_id,
                i64::from(event.transaction_index),
                i64::from(event.schema_version),
                event.idempotency_key,
            ],
        )?;
        insert_event_refs(&tx, &event.event_id, &event.refs)?;
    }
    for stream in &snapshot.transaction_streams {
        tx.execute(
            "INSERT INTO runtime_transaction_streams
             (transaction_id, stream_id, expected_revision, committed_revision)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                stream.transaction_id,
                stream.stream_id,
                snapshot_i64(stream.expected_revision, "expected_revision")?,
                snapshot_i64(stream.committed_revision, "committed_revision")?,
            ],
        )?;
    }
    for head in &snapshot.stream_heads {
        tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES (?1, ?2)",
            params![head.stream_id, snapshot_i64(head.revision, "revision")?,],
        )?;
    }
    for terminal in &snapshot.session_outbox {
        tx.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
              request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
              input_claim_revision, status, attempts,
              next_attempt_at, claim_owner, claim_expires_at, failure_class, last_error,
              materialized_at, revision)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                     ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                terminal.terminal_id,
                terminal.message_id,
                terminal.session_id,
                snapshot_i64(terminal.commit_cursor, "commit_cursor")?,
                terminal.payload_ref,
                terminal.execution_id,
                terminal.turn_id,
                terminal.request_id,
                terminal
                    .session_generation
                    .map(|value| snapshot_i64(value, "session_generation"))
                    .transpose()?,
                terminal
                    .input_sequence
                    .map(|value| snapshot_i64(value, "input_sequence"))
                    .transpose()?,
                terminal.input_claim_owner,
                terminal.input_claim_token,
                terminal
                    .input_claim_revision
                    .map(|value| snapshot_i64(value, "input_claim_revision"))
                    .transpose()?,
                terminal.status,
                i64::from(terminal.attempts),
                terminal
                    .next_attempt_at_ms
                    .map(|value| snapshot_i64(value, "next_attempt_at"))
                    .transpose()?,
                terminal.claim_owner,
                terminal
                    .claim_expires_at_ms
                    .map(|value| snapshot_i64(value, "claim_expires_at"))
                    .transpose()?,
                terminal.failure_class,
                terminal.last_error,
                terminal
                    .materialized_at_ms
                    .map(|value| snapshot_i64(value, "materialized_at"))
                    .transpose()?,
                snapshot_i64(terminal.revision, "revision")?,
            ],
        )?;
    }
    for lease in &snapshot.decision_leases {
        tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease.lease_id,
                lease.principal_id,
                lease.review_id,
                lease.action,
                lease.scope,
                lease.evidence_digest,
                snapshot_i64(lease.credential_epoch, "credential_epoch")?,
                snapshot_i64(lease.consumed_at_ms, "consumed_at_ms")?,
            ],
        )?;
    }
    if let Some(max_cursor) = snapshot.commits.last().map(|commit| commit.commit_cursor) {
        tx.execute(
            "DELETE FROM sqlite_sequence WHERE name='runtime_commits'",
            [],
        )?;
        tx.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES ('runtime_commits', ?1)",
            params![snapshot_i64(max_cursor, "commit_cursor")?],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn validate_migration_snapshot(
    snapshot: &RuntimeEventStoreSnapshot,
) -> RuntimeEventStoreResult<()> {
    let mut commits = BTreeMap::new();
    let mut commit_cursors = BTreeSet::new();
    for commit in &snapshot.commits {
        if commit.commit_cursor == 0
            || commit.transaction_id.trim().is_empty()
            || commit.request_hash.trim().is_empty()
            || commits
                .insert(commit.transaction_id.as_str(), commit.commit_cursor)
                .is_some()
            || !commit_cursors.insert(commit.commit_cursor)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid commit".to_string(),
            ));
        }
    }
    let mut event_ids = BTreeSet::new();
    let mut event_sequences = BTreeSet::new();
    let mut event_indexes = BTreeSet::new();
    let mut events_per_transaction_stream = BTreeMap::<(&str, &str), u64>::new();
    for event in &snapshot.events {
        if !event_ids.insert(event.event_id.as_str())
            || commits.get(event.transaction_id.as_str()) != Some(&event.commit_cursor)
            || !commit_cursors.contains(&event.commit_cursor)
            || event.sequence == 0
            || event.schema_version == 0
            || !event_sequences.insert((event.stream_id.as_str(), event.sequence))
            || !event_indexes.insert((event.transaction_id.as_str(), event.transaction_index))
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid event linkage".to_string(),
            ));
        }
        *events_per_transaction_stream
            .entry((event.transaction_id.as_str(), event.stream_id.as_str()))
            .or_default() += 1;
    }
    let mut transaction_streams = BTreeSet::new();
    let mut heads_from_transactions = BTreeMap::<&str, (u64, u64)>::new();
    for stream in &snapshot.transaction_streams {
        if !commits.contains_key(stream.transaction_id.as_str())
            || stream.stream_id.trim().is_empty()
            || !transaction_streams
                .insert((stream.transaction_id.as_str(), stream.stream_id.as_str()))
            || stream.committed_revision
                != stream.expected_revision
                    + events_per_transaction_stream
                        .get(&(stream.transaction_id.as_str(), stream.stream_id.as_str()))
                        .copied()
                        .unwrap_or_default()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid transaction stream".to_string(),
            ));
        }
        let commit_cursor = commits[stream.transaction_id.as_str()];
        heads_from_transactions
            .entry(stream.stream_id.as_str())
            .and_modify(|current| {
                if commit_cursor > current.0 {
                    *current = (commit_cursor, stream.committed_revision);
                }
            })
            .or_insert((commit_cursor, stream.committed_revision));
    }
    if events_per_transaction_stream
        .keys()
        .any(|stream| !transaction_streams.contains(stream))
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "runtime event migration snapshot is missing a transaction stream".to_string(),
        ));
    }
    let mut stream_heads = BTreeMap::new();
    for head in &snapshot.stream_heads {
        if head.stream_id.trim().is_empty()
            || stream_heads
                .insert(head.stream_id.as_str(), head.revision)
                .is_some()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid stream head".to_string(),
            ));
        }
    }
    if stream_heads.len() != heads_from_transactions.len()
        || heads_from_transactions
            .iter()
            .any(|(stream_id, (_, revision))| stream_heads.get(stream_id) != Some(revision))
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "runtime event migration snapshot stream heads do not match events".to_string(),
        ));
    }
    let mut terminal_ids = BTreeSet::new();
    let mut message_ids = BTreeSet::new();
    for terminal in &snapshot.session_outbox {
        if terminal.terminal_id.trim().is_empty()
            || terminal.message_id.trim().is_empty()
            || !terminal_ids.insert(terminal.terminal_id.as_str())
            || !message_ids.insert(terminal.message_id.as_str())
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid terminal outbox row".to_string(),
            ));
        }
    }
    let mut lease_ids = BTreeSet::new();
    for lease in &snapshot.decision_leases {
        if lease.lease_id.trim().is_empty()
            || lease.principal_id.trim().is_empty()
            || lease.review_id.trim().is_empty()
            || lease.action.trim().is_empty()
            || lease.scope.trim().is_empty()
            || lease.evidence_digest.trim().is_empty()
            || !lease_ids.insert(lease.lease_id.as_str())
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid decision lease".to_string(),
            ));
        }
    }
    Ok(())
}

fn snapshot_i64(value: u64, field: &str) -> RuntimeEventStoreResult<i64> {
    i64::try_from(value).map_err(|_| {
        RuntimeEventStoreError::InvalidTransaction(format!(
            "runtime event migration `{field}` exceeds i64"
        ))
    })
}

fn configure_connection(conn: &Connection, in_memory: bool) -> RuntimeEventStoreResult<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    if !in_memory {
        let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            let activated: String =
                conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
            if !activated.eq_ignore_ascii_case("wal") {
                return Err(RuntimeEventStoreError::Corrupt(format!(
                    "failed to activate WAL journal mode; SQLite selected `{activated}`"
                )));
            }
        }
    }
    Ok(())
}

fn migrate_schema(conn: &mut Connection) -> RuntimeEventStoreResult<()> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current > STORE_SCHEMA_VERSION {
        return Err(RuntimeEventStoreError::Corrupt(format!(
            "database schema version {current} is newer than supported {STORE_SCHEMA_VERSION}"
        )));
    }
    if current == STORE_SCHEMA_VERSION {
        return validate_schema(conn);
    }
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    create_current_tables(&tx)?;
    migrate_legacy_runtime_events(&tx)?;
    backfill_terminal_session_refs(&tx)?;
    tx.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
    tx.commit()?;
    validate_schema(conn)
}

fn create_current_tables(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS runtime_events (
            event_id TEXT PRIMARY KEY,
            stream_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            scope TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT,
            actor TEXT,
            payload TEXT NOT NULL,
            refs TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            commit_cursor INTEGER,
            transaction_id TEXT,
            transaction_index INTEGER,
            schema_version INTEGER NOT NULL DEFAULT 1,
            idempotency_key TEXT
        );
        CREATE TABLE IF NOT EXISTS runtime_commits (
            commit_cursor INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT NOT NULL UNIQUE,
            request_hash TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_transaction_streams (
            transaction_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            expected_revision INTEGER NOT NULL,
            committed_revision INTEGER NOT NULL,
            PRIMARY KEY(transaction_id, stream_id),
            FOREIGN KEY(transaction_id) REFERENCES runtime_commits(transaction_id)
        );
        CREATE TABLE IF NOT EXISTS runtime_stream_heads (
            stream_id TEXT PRIMARY KEY,
            revision INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_event_refs (
            event_id TEXT NOT NULL,
            ref_kind TEXT NOT NULL,
            ref_id TEXT NOT NULL,
            PRIMARY KEY(event_id, ref_kind, ref_id),
            FOREIGN KEY(event_id) REFERENCES runtime_events(event_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS runtime_session_outbox (
            terminal_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            commit_cursor INTEGER NOT NULL,
            payload_ref TEXT NOT NULL,
            execution_id TEXT,
            turn_id TEXT,
            request_id TEXT,
            session_generation INTEGER,
            input_sequence INTEGER,
            input_claim_owner TEXT,
            input_claim_token TEXT,
            input_claim_revision INTEGER,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER,
            claim_owner TEXT,
            claim_expires_at INTEGER,
            failure_class TEXT,
            last_error TEXT,
            materialized_at INTEGER,
            revision INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS runtime_consumed_decision_leases (
            lease_id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            review_id TEXT NOT NULL,
            action TEXT NOT NULL,
            scope TEXT NOT NULL,
            evidence_digest TEXT NOT NULL,
            credential_epoch INTEGER NOT NULL,
            consumed_at_ms INTEGER NOT NULL
        );",
    )?;

    for (column, definition) in [
        ("commit_cursor", "INTEGER"),
        ("transaction_id", "TEXT"),
        ("transaction_index", "INTEGER"),
        ("schema_version", "INTEGER NOT NULL DEFAULT 1"),
        ("idempotency_key", "TEXT"),
    ] {
        if !table_has_column(tx, "runtime_events", column)? {
            tx.execute(
                &format!("ALTER TABLE runtime_events ADD COLUMN {column} {definition}"),
                [],
            )?;
        }
    }
    for (column, definition) in [
        ("execution_id", "TEXT"),
        ("turn_id", "TEXT"),
        ("request_id", "TEXT"),
        ("session_generation", "INTEGER"),
        ("input_sequence", "INTEGER"),
        ("input_claim_owner", "TEXT"),
        ("input_claim_token", "TEXT"),
        ("input_claim_revision", "INTEGER"),
    ] {
        if !table_has_column(tx, "runtime_session_outbox", column)? {
            tx.execute(
                &format!("ALTER TABLE runtime_session_outbox ADD COLUMN {column} {definition}"),
                [],
            )?;
        }
    }
    Ok(())
}

fn query_runtime_session_outbox(
    conn: &Connection,
    terminal_id: &str,
) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
    conn.query_row(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status,
                attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                last_error, materialized_at, revision
         FROM runtime_session_outbox WHERE terminal_id=?1",
        params![terminal_id],
        row_to_runtime_session_outbox,
    )
    .optional()
    .map_err(Into::into)
}

fn row_to_runtime_session_outbox(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeSessionOutboxRecord> {
    Ok(RuntimeSessionOutboxRecord {
        terminal_id: row.get(0)?,
        message_id: row.get(1)?,
        session_id: row.get(2)?,
        commit_cursor: row.get::<_, i64>(3)? as u64,
        payload_ref: row.get(4)?,
        execution_id: row.get(5)?,
        turn_id: row.get(6)?,
        request_id: row.get(7)?,
        session_generation: row.get::<_, Option<i64>>(8)?.map(|value| value as u64),
        input_sequence: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
        input_claim_owner: row.get(10)?,
        input_claim_token: row.get(11)?,
        input_claim_revision: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
        status: row.get(13)?,
        attempts: row.get::<_, i64>(14)? as u32,
        next_attempt_at_ms: row.get::<_, Option<i64>>(15)?.map(|value| value as u64),
        claim_owner: row.get(16)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(17)?.map(|value| value as u64),
        failure_class: row.get(18)?,
        last_error: row.get(19)?,
        materialized_at_ms: row.get::<_, Option<i64>>(20)?.map(|value| value as u64),
        revision: row.get::<_, i64>(21)? as u64,
    })
}

fn migrate_legacy_runtime_events(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    let mut stmt = tx.prepare(
        "SELECT event_id, stream_id, sequence, scope, created_at_ms FROM runtime_events \
         WHERE commit_cursor IS NULL OR transaction_id IS NULL OR transaction_index IS NULL \
         ORDER BY created_at_ms ASC, event_id ASC",
    )?;
    let legacy = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)? as u64,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    for (event_id, stream_id, sequence, scope, created_at_ms) in legacy {
        RuntimeEventScope::parse(&scope)?;
        let transaction_id = format!("legacy:{event_id}");
        let request_hash =
            hash_bytes(format!("legacy:{event_id}:{stream_id}:{sequence}").as_bytes());
        tx.execute(
            "INSERT INTO runtime_commits(transaction_id, request_hash, created_at_ms) VALUES (?1, ?2, ?3)",
            params![transaction_id, request_hash, created_at_ms as i64],
        )?;
        let cursor = tx.last_insert_rowid() as u64;
        tx.execute(
            "UPDATE runtime_events SET commit_cursor = ?1, transaction_id = ?2, \
             transaction_index = 0, schema_version = COALESCE(schema_version, 1) WHERE event_id = ?3",
            params![cursor as i64, transaction_id, event_id],
        )?;
        tx.execute(
            "INSERT INTO runtime_transaction_streams \
             (transaction_id, stream_id, expected_revision, committed_revision) VALUES (?1, ?2, ?3, ?4)",
            params![transaction_id, stream_id, sequence.saturating_sub(1) as i64, sequence as i64],
        )?;
    }
    tx.execute(
        "INSERT INTO runtime_stream_heads(stream_id, revision) \
         SELECT stream_id, MAX(sequence) FROM runtime_events GROUP BY stream_id \
         ON CONFLICT(stream_id) DO UPDATE SET revision = MAX(revision, excluded.revision)",
        [],
    )?;
    tx.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_sequence
             ON runtime_events(stream_id, sequence);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_stream_kind_sequence
             ON runtime_events(stream_id, kind, sequence DESC);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_commit
            ON runtime_events(scope, commit_cursor, transaction_index);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_kind_commit
            ON runtime_events(scope, kind, commit_cursor, transaction_index);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_stream_commit
            ON runtime_events(scope, stream_id, commit_cursor, transaction_index);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_stream_sequence
            ON runtime_events(scope, stream_id, sequence DESC);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_commit_index
            ON runtime_events(commit_cursor, transaction_index);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_transaction_index
            ON runtime_events(transaction_id, transaction_index);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_idempotency
            ON runtime_events(stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_runtime_commits_cursor
            ON runtime_commits(commit_cursor);
         CREATE INDEX IF NOT EXISTS idx_runtime_event_refs_lookup
            ON runtime_event_refs(ref_kind, ref_id, event_id);
         CREATE INDEX IF NOT EXISTS idx_runtime_consumed_decision_leases_review
            ON runtime_consumed_decision_leases(review_id, action);",
    )?;
    let existing_refs = {
        let mut statement = tx.prepare("SELECT event_id, refs FROM runtime_events")?;
        let refs = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        refs
    };
    for (event_id, refs) in existing_refs {
        let refs = serde_json::from_str::<Vec<RuntimeEventRef>>(&refs)?;
        insert_event_refs(tx, &event_id, &refs)?;
    }
    Ok(())
}

fn backfill_terminal_session_refs(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    let terminal_events = {
        let mut statement = tx.prepare(
            "SELECT event_id, refs, payload FROM runtime_events
             WHERE kind = 'runtime.session.terminal_requested'",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    for (event_id, refs_json, payload_json) in terminal_events {
        let payload = serde_json::from_str::<serde_json::Value>(&payload_json)?;
        let Some(session_id) = payload
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let mut refs = serde_json::from_str::<Vec<RuntimeEventRef>>(&refs_json)?;
        if !refs
            .iter()
            .any(|reference| reference.kind == "session" && reference.id == session_id)
        {
            refs.push(RuntimeEventRef {
                kind: "session".to_string(),
                id: session_id.to_string(),
            });
            tx.execute(
                "UPDATE runtime_events SET refs = ?1 WHERE event_id = ?2",
                params![serde_json::to_string(&refs)?, event_id],
            )?;
        }
        insert_event_refs(tx, &event_id, &refs)?;
    }
    Ok(())
}

fn validate_schema(conn: &Connection) -> RuntimeEventStoreResult<()> {
    for table in [
        "runtime_events",
        "runtime_commits",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_event_refs",
        "runtime_session_outbox",
        "runtime_consumed_decision_leases",
    ] {
        if !table_exists(conn, table)? {
            return Err(RuntimeEventStoreError::Corrupt(format!(
                "required table `{table}` is missing"
            )));
        }
    }
    let mut stmt = conn.prepare("SELECT DISTINCT scope FROM runtime_events")?;
    let scopes = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for scope in scopes {
        RuntimeEventScope::parse(&scope)?;
    }
    Ok(())
}

fn append_transaction_in_tx(
    tx: &Transaction<'_>,
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    validate_transaction(request)?;
    if let Some(terminal) = terminal {
        validate_fenced_terminal(terminal)?;
        if serde_json::to_vec(&(request, terminal))?.len() > MAX_TRANSACTION_BYTES {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "serialized terminal transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
            )));
        }
    }
    let request_hash = request_hash_with_terminal(request, terminal)?;
    if let Some(committed_hash) = tx
        .query_row(
            "SELECT request_hash FROM runtime_commits WHERE transaction_id = ?1",
            params![request.transaction_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    {
        if committed_hash != request_hash {
            return Err(RuntimeEventStoreError::TransactionConflict {
                transaction_id: request.transaction_id.clone(),
            });
        }
        let receipt = load_receipt(tx, &request.transaction_id, true)?;
        if let Some(terminal) = terminal {
            verify_terminal_for_commit(tx, terminal, receipt.commit_cursor)?;
        }
        return Ok(receipt);
    }

    let expected = request
        .expected_streams
        .iter()
        .map(|stream| (stream.stream_id.as_str(), stream.expected_revision))
        .collect::<BTreeMap<_, _>>();
    for stream in &request.expected_streams {
        let actual = stream_head(tx, &stream.stream_id)?;
        if actual != stream.expected_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: stream.stream_id.clone(),
                expected: stream.expected_revision,
                actual,
            });
        }
    }

    let created_at_ms = now_ms();
    tx.execute(
        "INSERT INTO runtime_commits(transaction_id, request_hash, created_at_ms) VALUES (?1, ?2, ?3)",
        params![request.transaction_id, request_hash, created_at_ms as i64],
    )?;
    let commit_cursor = tx.last_insert_rowid() as u64;
    if let Some(terminal) = terminal {
        insert_terminal_in_tx(tx, terminal, commit_cursor)?;
    }
    let mut increments = BTreeMap::<&str, u64>::new();
    let mut event_ids = Vec::with_capacity(request.events.len());
    for (transaction_index, input) in request.events.iter().enumerate() {
        let stream_id = input.event.stream_id.as_str();
        let offset = increments.entry(stream_id).or_default();
        *offset += 1;
        let sequence = expected[stream_id] + *offset;
        let event_id = format!("runtime-event-{}", uuid::Uuid::new_v4());
        tx.execute(
            "INSERT INTO runtime_events \
             (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
              commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                event_id,
                input.event.stream_id,
                sequence as i64,
                input.event.scope.as_str(),
                input.event.kind,
                input.event.status,
                input.event.actor,
                serde_json::to_string(&input.event.payload)?,
                serde_json::to_string(&input.event.refs)?,
                created_at_ms as i64,
                commit_cursor as i64,
                request.transaction_id,
                transaction_index as i64,
                input.schema_version as i64,
                input.idempotency_key,
            ],
        )?;
        insert_event_refs(tx, &event_id, &input.event.refs)?;
        event_ids.push(event_id);
    }

    let mut stream_revisions = Vec::with_capacity(request.expected_streams.len());
    for stream in &request.expected_streams {
        let committed_revision = stream.expected_revision
            + increments
                .get(stream.stream_id.as_str())
                .copied()
                .unwrap_or_default();
        tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES (?1, ?2) \
             ON CONFLICT(stream_id) DO UPDATE SET revision = excluded.revision",
            params![stream.stream_id, committed_revision as i64],
        )?;
        tx.execute(
            "INSERT INTO runtime_transaction_streams \
             (transaction_id, stream_id, expected_revision, committed_revision) VALUES (?1, ?2, ?3, ?4)",
            params![
                request.transaction_id,
                stream.stream_id,
                stream.expected_revision as i64,
                committed_revision as i64,
            ],
        )?;
        stream_revisions.push(CommittedStreamRevision {
            stream_id: stream.stream_id.clone(),
            expected_revision: stream.expected_revision,
            committed_revision,
        });
    }
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: request.transaction_id.clone(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate: false,
    })
}

fn validate_decision_lease_claims(
    lease_id: &str,
    principal_id: &str,
    review_id: &str,
    action: &str,
    scope: &str,
    evidence_digest: &str,
) -> RuntimeEventStoreResult<()> {
    if lease_id.trim().is_empty()
        || principal_id.trim().is_empty()
        || review_id.trim().is_empty()
        || action.trim().is_empty()
        || scope.trim().is_empty()
        || evidence_digest.trim().is_empty()
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "decision lease consumption requires non-empty bound claims".to_string(),
        ));
    }
    Ok(())
}

fn insert_terminal_in_tx(
    tx: &Transaction<'_>,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    tx.execute(
        "INSERT INTO runtime_session_outbox
         (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
          request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
          input_claim_revision, status, revision)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 'pending', 0)
         ON CONFLICT(terminal_id) DO NOTHING",
        params![
            terminal.terminal_id,
            terminal.message_id,
            terminal.session_id,
            commit_cursor as i64,
            terminal.payload_ref,
            terminal.execution_id,
            terminal.turn_id,
            terminal.request_id,
            terminal.session_generation.map(|value| value as i64),
            terminal.input_sequence.map(|value| value as i64),
            terminal.input_claim_owner,
            terminal.input_claim_token,
            terminal.input_claim_revision.map(|value| value as i64),
        ],
    )?;
    let stored = query_runtime_session_outbox(tx, &terminal.terminal_id)?.ok_or_else(|| {
        RuntimeEventStoreError::Corrupt(format!(
            "terminal outbox `{}` disappeared during commit",
            terminal.terminal_id
        ))
    })?;
    if stored.message_id != terminal.message_id
        || stored.session_id != terminal.session_id
        || stored.commit_cursor != commit_cursor
        || stored.payload_ref != terminal.payload_ref
        || stored.execution_id != terminal.execution_id
        || stored.turn_id != terminal.turn_id
        || stored.request_id != terminal.request_id
        || stored.session_generation != terminal.session_generation
        || stored.input_sequence != terminal.input_sequence
        || stored.input_claim_owner != terminal.input_claim_owner
        || stored.input_claim_token != terminal.input_claim_token
        || stored.input_claim_revision != terminal.input_claim_revision
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
}

fn verify_terminal_for_commit(
    conn: &Connection,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    let mut statement = conn.prepare(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                claim_expires_at, failure_class, last_error, materialized_at, revision
           FROM runtime_session_outbox WHERE commit_cursor=?1
           ORDER BY terminal_id LIMIT 2",
    )?;
    let records = statement
        .query_map(params![commit_cursor as i64], row_to_runtime_session_outbox)?
        .collect::<Result<Vec<_>, _>>()?;
    if records.len() != 1 {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    let stored = &records[0];
    if stored.terminal_id != terminal.terminal_id
        || stored.message_id != terminal.message_id
        || stored.session_id != terminal.session_id
        || stored.payload_ref != terminal.payload_ref
        || stored.execution_id != terminal.execution_id
        || stored.turn_id != terminal.turn_id
        || stored.request_id != terminal.request_id
        || stored.session_generation != terminal.session_generation
        || stored.input_sequence != terminal.input_sequence
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
}

fn validate_fenced_terminal(terminal: &SessionTerminalInput) -> RuntimeEventStoreResult<()> {
    let required = [
        terminal.terminal_id.as_str(),
        terminal.message_id.as_str(),
        terminal.session_id.as_str(),
        terminal.payload_ref.as_str(),
        terminal.execution_id.as_deref().unwrap_or_default(),
        terminal.turn_id.as_deref().unwrap_or_default(),
        terminal.request_id.as_deref().unwrap_or_default(),
        terminal.input_claim_owner.as_deref().unwrap_or_default(),
        terminal.input_claim_token.as_deref().unwrap_or_default(),
    ];
    if required.iter().any(|value| value.trim().is_empty())
        || terminal
            .session_generation
            .is_none_or(|generation| generation == 0)
        || terminal.input_sequence.is_none()
        || terminal
            .input_claim_revision
            .is_none_or(|revision| revision == 0)
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "terminal transaction requires complete execution, turn and Session claim fences"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_transaction(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<()> {
    if request.transaction_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "transaction_id must not be empty".to_string(),
        ));
    }
    if request.events.is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "events must not be empty".to_string(),
        ));
    }
    if request.events.len() > MAX_TRANSACTION_EVENTS {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "event count exceeds hard limit {MAX_TRANSACTION_EVENTS}"
        )));
    }
    let bytes = serde_json::to_vec(request)?.len();
    if bytes > MAX_TRANSACTION_BYTES {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "serialized transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
        )));
    }
    let mut expected = BTreeSet::new();
    for stream in &request.expected_streams {
        if stream.stream_id.trim().is_empty() || !expected.insert(stream.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "expected streams must be non-empty and unique".to_string(),
            ));
        }
    }
    for event in &request.events {
        validate_event(&event.event)?;
        if event.schema_version == 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "event schema_version must be positive".to_string(),
            ));
        }
        if !expected.contains(event.event.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "event stream `{}` has no expected revision",
                event.event.stream_id
            )));
        }
    }
    Ok(())
}

fn validate_event(input: &RuntimeEventInput) -> RuntimeEventStoreResult<()> {
    if input.stream_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event stream_id must not be empty".to_string(),
        ));
    }
    if input.kind.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event kind must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn request_hash(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<String> {
    Ok(hash_bytes(&serde_json::to_vec(request)?))
}

fn request_hash_with_terminal(
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<String> {
    terminal.map_or_else(
        || request_hash(request),
        |terminal| Ok(hash_bytes(&serde_json::to_vec(&(request, terminal))?)),
    )
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn load_receipt(
    conn: &Connection,
    transaction_id: &str,
    duplicate: bool,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    let (commit_cursor, request_hash) = conn.query_row(
        "SELECT commit_cursor, request_hash FROM runtime_commits WHERE transaction_id = ?1",
        params![transaction_id],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?)),
    )?;
    let mut stream_stmt = conn.prepare(
        "SELECT stream_id, expected_revision, committed_revision FROM runtime_transaction_streams \
         WHERE transaction_id = ?1 ORDER BY stream_id ASC",
    )?;
    let stream_revisions = stream_stmt
        .query_map(params![transaction_id], |row| {
            Ok(CommittedStreamRevision {
                stream_id: row.get(0)?,
                expected_revision: row.get::<_, i64>(1)? as u64,
                committed_revision: row.get::<_, i64>(2)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut event_stmt = conn.prepare(
        "SELECT event_id FROM runtime_events WHERE transaction_id = ?1 ORDER BY transaction_index ASC",
    )?;
    let event_ids = event_stmt
        .query_map(params![transaction_id], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: transaction_id.to_string(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate,
    })
}

fn insert_event_refs(
    tx: &Transaction<'_>,
    event_id: &str,
    refs: &[RuntimeEventRef],
) -> RuntimeEventStoreResult<()> {
    for reference in refs {
        tx.execute(
            "INSERT OR IGNORE INTO runtime_event_refs(event_id, ref_kind, ref_id)
             VALUES (?1, ?2, ?3)",
            params![event_id, reference.kind, reference.id],
        )?;
    }
    Ok(())
}

fn load_transaction_events(
    conn: &Connection,
    transaction_id: &str,
) -> RuntimeEventStoreResult<Vec<RuntimeEventRecord>> {
    let mut stmt = conn.prepare(&format!(
        "{} WHERE transaction_id = ?1 ORDER BY transaction_index ASC",
        event_select()
    ))?;
    let events = stmt
        .query_map(params![transaction_id], row_to_event)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(events)
}

fn event_select() -> &'static str {
    "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
     commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key FROM runtime_events"
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableRuntimeEvent> {
    let scope: String = row.get(3)?;
    let payload: String = row.get(7)?;
    let refs: String = row.get(8)?;
    let scope = RuntimeEventScope::parse(&scope).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(DurableRuntimeEvent {
        event_id: row.get(0)?,
        stream_id: row.get(1)?,
        sequence: row.get::<_, i64>(2)? as u64,
        scope,
        kind: row.get(4)?,
        status: row.get(5)?,
        actor: row.get(6)?,
        payload: serde_json::from_str(&payload).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        refs: serde_json::from_str(&refs).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        created_at_ms: row.get::<_, i64>(9)? as u64,
        commit_cursor: row.get::<_, i64>(10)? as u64,
        transaction_id: row.get(11)?,
        transaction_index: row.get::<_, i64>(12)? as u32,
        schema_version: row.get::<_, i64>(13)? as u32,
        idempotency_key: row.get(14)?,
    })
}

fn stream_head(conn: &Connection, stream_id: &str) -> RuntimeEventStoreResult<u64> {
    Ok(conn
        .query_row(
            "SELECT revision FROM runtime_stream_heads WHERE stream_id = ?1",
            params![stream_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or_default() as u64)
}

fn table_exists(conn: &Connection, table: &str) -> RuntimeEventStoreResult<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> RuntimeEventStoreResult<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|candidate| candidate == column))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
        RuntimeEventInput {
            stream_id: stream_id.to_string(),
            scope,
            kind: kind.to_string(),
            status: Some("running".to_string()),
            actor: Some("test".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"kind": kind}),
        }
    }

    fn transaction(id: &str) -> AppendTransactionRequest {
        AppendTransactionRequest {
            transaction_id: id.to_string(),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: "graph:g1".to_string(),
                    expected_revision: 0,
                },
                ExpectedStreamRevision {
                    stream_id: "node:n1".to_string(),
                    expected_revision: 0,
                },
            ],
            events: vec![
                RuntimeTransactionEventInput {
                    event: input(
                        "graph:g1",
                        RuntimeEventScope::ExecutionGraph,
                        "graph.started",
                    ),
                    idempotency_key: Some("graph-start".to_string()),
                    schema_version: 1,
                },
                RuntimeTransactionEventInput {
                    event: input("node:n1", RuntimeEventScope::ExecutionNode, "node.running"),
                    idempotency_key: Some("node-run".to_string()),
                    schema_version: 1,
                },
            ],
        }
    }

    fn fenced_terminal(id: &str, claim_revision: u64) -> SessionTerminalInput {
        SessionTerminalInput {
            terminal_id: format!("terminal-{id}"),
            message_id: format!("assistant-{id}"),
            session_id: format!("session-{id}"),
            execution_id: Some(format!("execution-{id}")),
            turn_id: Some(format!("turn-{id}")),
            request_id: Some(format!("request-{id}")),
            session_generation: Some(1),
            input_sequence: Some(1),
            input_claim_owner: Some("session-worker-old".to_string()),
            input_claim_token: Some(format!("claim-old-{id}")),
            input_claim_revision: Some(claim_revision),
            payload_ref: format!("assistant_json:\"{id}\""),
        }
    }

    #[test]
    fn migration_snapshot_round_trip_preserves_canonical_digest_and_rejects_nonempty_target() {
        let source = RuntimeEventStore::try_open_in_memory().expect("source store");
        source
            .append_transaction_with_terminal(
                AppendTransactionRequest {
                    transaction_id: "migration-round-trip".to_string(),
                    expected_streams: vec![
                        ExpectedStreamRevision {
                            stream_id: "migration:stream".to_string(),
                            expected_revision: 0,
                        },
                        ExpectedStreamRevision {
                            stream_id: "migration:empty-stream".to_string(),
                            expected_revision: 0,
                        },
                    ],
                    events: vec![input(
                        "migration:stream",
                        RuntimeEventScope::Recovery,
                        "migration.seeded",
                    )
                    .into()],
                },
                SessionTerminalInput {
                    terminal_id: "migration-terminal".to_string(),
                    message_id: "migration-message".to_string(),
                    session_id: "migration-session".to_string(),
                    execution_id: Some("migration-execution".to_string()),
                    turn_id: Some("migration-turn".to_string()),
                    request_id: Some("migration-request".to_string()),
                    session_generation: Some(1),
                    input_sequence: Some(1),
                    input_claim_owner: Some("migration-worker".to_string()),
                    input_claim_token: Some("migration-claim".to_string()),
                    input_claim_revision: Some(3),
                    payload_ref: "assistant_json:\"done\"".to_string(),
                },
            )
            .expect("source event");
        let snapshot = source
            .export_migration_snapshot()
            .expect("export source snapshot");
        assert_eq!(snapshot.session_outbox.len(), 1);
        assert_eq!(
            snapshot.session_outbox[0].execution_id.as_deref(),
            Some("migration-execution")
        );
        assert_eq!(
            snapshot.session_outbox[0].turn_id.as_deref(),
            Some("migration-turn")
        );
        let digest = snapshot.canonical_digest().expect("source digest");
        let target = RuntimeEventStore::try_open_in_memory().expect("target store");
        target
            .import_migration_snapshot(&snapshot)
            .expect("import snapshot");
        assert_eq!(
            target
                .export_migration_snapshot()
                .expect("export target snapshot")
                .canonical_digest()
                .expect("target digest"),
            digest
        );
        assert!(target.import_migration_snapshot(&snapshot).is_err());
    }

    #[test]
    fn multi_stream_transaction_is_atomic_and_idempotent() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let request = transaction("tx-1");
        let first = store
            .append_transaction(request.clone())
            .expect("first commit");
        let duplicate = store.append_transaction(request).expect("idempotent retry");
        assert_eq!(first.commit_cursor, duplicate.commit_cursor);
        assert!(!first.duplicate);
        assert!(duplicate.duplicate);
        assert_eq!(store.stream_revision("graph:g1").unwrap(), 1);
        assert_eq!(store.stream_revision("node:n1").unwrap(), 1);
        assert_eq!(store.all_events(100).unwrap().len(), 2);
    }

    #[test]
    fn latest_stream_kind_uses_the_exact_kind_cursor_without_reading_the_stream() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let stream_id = "projector:cursor";
        store
            .append(input(
                stream_id,
                RuntimeEventScope::Recovery,
                "projector.checkpoint",
            ))
            .expect("first checkpoint");
        store
            .append(input(
                stream_id,
                RuntimeEventScope::Recovery,
                "projector.diagnostic",
            ))
            .expect("diagnostic");
        store
            .append(input(
                stream_id,
                RuntimeEventScope::Recovery,
                "projector.checkpoint",
            ))
            .expect("second checkpoint");

        let checkpoint = store
            .latest_for_stream_kind(stream_id, "projector.checkpoint")
            .expect("checkpoint query")
            .expect("checkpoint");
        let diagnostic = store
            .latest_for_stream_kind(stream_id, "projector.diagnostic")
            .expect("diagnostic query")
            .expect("diagnostic");

        assert_eq!(checkpoint.sequence, 3);
        assert_eq!(checkpoint.kind, "projector.checkpoint");
        assert_eq!(diagnostic.sequence, 2);
        assert_eq!(diagnostic.kind, "projector.diagnostic");
        assert!(store
            .latest_for_stream_kind(stream_id, "projector.missing")
            .expect("missing query")
            .is_none());
    }

    #[test]
    fn session_execution_events_follow_durable_terminal_graph_reference() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let graph_id = "graph:session-a";
        let child_graph_id = "graph:session-a:team";
        store
            .append(input(
                graph_id,
                RuntimeEventScope::ExecutionGraph,
                "execution_graph.planned",
            ))
            .unwrap();
        let mut child = input(
            child_graph_id,
            RuntimeEventScope::ExecutionGraph,
            "execution_graph.planned",
        );
        child.payload = serde_json::json!({
            "event": "planned",
            "graph": {"id": child_graph_id, "nodes": [{"kind": "agent_task"}]},
        });
        store.append(child).unwrap();
        let mut lineage = input(
            &format!("execution-lineage:{graph_id}"),
            RuntimeEventScope::Relation,
            "execution.lineage.child_registered.v1",
        );
        lineage.payload = serde_json::json!({
            "parent_execution_id": graph_id,
            "parent_node_id": "model",
            "child_execution_id": child_graph_id,
            "child_objective": "parallel review",
        });
        store.append(lineage).unwrap();
        let mut terminal = input(
            "session-terminal:request-a",
            RuntimeEventScope::SessionInput,
            "runtime.session.terminal_requested",
        );
        terminal.payload = serde_json::json!({"session_id": "session-a"});
        terminal.refs = vec![
            RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: graph_id.to_string(),
            },
            RuntimeEventRef {
                kind: "session".to_string(),
                id: "session-a".to_string(),
            },
        ];
        store.append(terminal).unwrap();
        let mut task = input("task:task-a", RuntimeEventScope::Task, "task.created");
        task.refs = vec![RuntimeEventRef {
            kind: "session".to_string(),
            id: "session-a".to_string(),
        }];
        store.append(task).unwrap();

        let related = store
            .execution_events_for_session("session-a", None, 20)
            .unwrap();
        assert!(related
            .iter()
            .any(|event| event.stream_id == graph_id && event.kind == "execution_graph.planned"));
        assert!(related.iter().any(|event| {
            event.stream_id == child_graph_id && event.kind == "execution_graph.planned"
        }));
        assert!(related.iter().any(|event| {
            event.stream_id == format!("execution-lineage:{graph_id}")
                && event.kind == "execution.lineage.child_registered.v1"
        }));
        assert!(related
            .iter()
            .any(|event| event.kind == "runtime.session.terminal_requested"));
        assert!(related
            .iter()
            .any(|event| event.stream_id == "task:task-a" && event.kind == "task.created"));
        assert!(store
            .execution_events_for_session("session-b", None, 20)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn schema_v6_backfills_terminal_session_reference() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-events.sqlite");
        {
            let store = RuntimeEventStore::try_open(&path).expect("event store");
            let mut terminal = input(
                "session-terminal:legacy-request",
                RuntimeEventScope::SessionInput,
                "runtime.session.terminal_requested",
            );
            terminal.payload = serde_json::json!({"session_id": "legacy-session"});
            terminal.refs = vec![RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: "legacy-graph".to_string(),
            }];
            store.append(terminal).expect("legacy terminal");
        }
        {
            let connection = Connection::open(&path).unwrap();
            connection.pragma_update(None, "user_version", 5).unwrap();
        }

        let migrated = RuntimeEventStore::try_open(&path).expect("migrated store");
        let events = migrated
            .execution_events_for_session("legacy-session", None, 10)
            .expect("session events");
        assert!(events
            .iter()
            .any(|event| event.kind == "runtime.session.terminal_requested"));
        let refs = migrated
            .all_events(10)
            .unwrap()
            .into_iter()
            .find(|event| event.kind == "runtime.session.terminal_requested")
            .unwrap()
            .refs;
        assert!(refs
            .iter()
            .any(|reference| { reference.kind == "session" && reference.id == "legacy-session" }));
    }

    #[test]
    fn scope_replay_crosses_backend_page_boundaries_without_truncation() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let event_count = SCOPE_REPLAY_PAGE_SIZE + 3;
        for index in 0..event_count {
            store
                .append(input(
                    &format!("approval:{index}"),
                    RuntimeEventScope::Approval,
                    "approval.seeded",
                ))
                .expect("scope event");
        }

        let replayed = store
            .replay_scope(RuntimeEventScope::Approval)
            .expect("scope replay");
        assert_eq!(replayed.len(), event_count);
        assert!(replayed.windows(2).all(|events| {
            (events[0].commit_cursor, events[0].transaction_index)
                < (events[1].commit_cursor, events[1].transaction_index)
        }));
    }

    #[test]
    fn scope_kind_replay_uses_kind_boundary_without_losing_commit_order() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        for index in 0..(SCOPE_REPLAY_PAGE_SIZE + 3) {
            let kind = if index % 2 == 0 {
                "evolution.release.assignment_authorized"
            } else {
                "evolution.signal.projector.checkpoint.v1"
            };
            store
                .append(input(
                    &format!("evolution:{index}"),
                    RuntimeEventScope::Evolution,
                    kind,
                ))
                .expect("scope event");
        }

        let replayed = store
            .replay_scope_kind(
                RuntimeEventScope::Evolution,
                "evolution.release.assignment_authorized",
            )
            .expect("scope-kind replay");
        assert_eq!(replayed.len(), (SCOPE_REPLAY_PAGE_SIZE + 4) / 2);
        assert!(replayed
            .iter()
            .all(|event| event.kind == "evolution.release.assignment_authorized"));
        assert!(replayed.windows(2).all(|events| {
            (events[0].commit_cursor, events[0].transaction_index)
                < (events[1].commit_cursor, events[1].transaction_index)
        }));
    }

    #[test]
    fn scope_stream_prefix_replay_excludes_unrelated_aggregate_families() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        for index in 0..(SCOPE_REPLAY_PAGE_SIZE + 3) {
            let stream = if index % 2 == 0 {
                format!("evolution:candidate:{index}")
            } else {
                format!("evolution:signal:{index}")
            };
            store
                .append(input(
                    &stream,
                    RuntimeEventScope::Evolution,
                    "evolution.test",
                ))
                .expect("scope event");
        }

        let replayed = store
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, "evolution:candidate:")
            .expect("scope-prefix replay");
        assert_eq!(replayed.len(), (SCOPE_REPLAY_PAGE_SIZE + 4) / 2);
        assert!(replayed
            .iter()
            .all(|event| event.stream_id.starts_with("evolution:candidate:")));
        assert!(replayed.windows(2).all(|events| {
            (events[0].commit_cursor, events[0].transaction_index)
                < (events[1].commit_cursor, events[1].transaction_index)
        }));
    }

    #[test]
    fn transaction_id_reuse_with_different_request_is_rejected() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let request = transaction("tx-conflict");
        store.append_transaction(request.clone()).expect("commit");
        let mut changed = request;
        changed.events[0].event.kind = "graph.changed".to_string();
        assert!(matches!(
            store.append_transaction(changed),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
    }

    #[test]
    fn stale_revision_rolls_back_entire_transaction_without_visible_cursor() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        store
            .append(input(
                "node:n1",
                RuntimeEventScope::ExecutionNode,
                "node.created",
            ))
            .expect("seed");
        let before = store.events_after_cursor(0, 100).unwrap();
        let request = transaction("tx-stale");
        assert!(matches!(
            store.append_transaction(request),
            Err(RuntimeEventStoreError::StaleRevision { .. })
        ));
        let after = store.events_after_cursor(0, 100).unwrap();
        assert_eq!(before, after);
        assert!(store.list_stream("graph:g1").unwrap().is_empty());
    }

    #[test]
    fn cursor_pagination_never_splits_a_transaction() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let first = store.append_transaction(transaction("tx-page-1")).unwrap();
        let second = store
            .append_batch_if_revision(
                "graph:g1",
                1,
                "tx-page-2",
                vec![input(
                    "graph:g1",
                    RuntimeEventScope::ExecutionGraph,
                    "graph.completed",
                )
                .into()],
            )
            .unwrap();
        let page = store.events_after_cursor(0, 1).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].commit_cursor, first.commit_cursor);
        assert_eq!(page[0].events.len(), 2);
        let next = store.events_after_cursor(first.commit_cursor, 1).unwrap();
        assert_eq!(next[0].commit_cursor, second.commit_cursor);
        assert_eq!(next[0].events.len(), 1);
    }

    #[test]
    fn legacy_version_zero_database_is_migrated_without_losing_events() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE runtime_events (
                event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                scope TEXT NOT NULL, kind TEXT NOT NULL, status TEXT, actor TEXT,
                payload TEXT NOT NULL, refs TEXT NOT NULL, created_at_ms INTEGER NOT NULL
             );
             INSERT INTO runtime_events VALUES
                ('old-1', 'mission:m1', 1, 'mission', 'mission.started', 'running', NULL, '{}', '[]', 1);",
        )
        .unwrap();
        drop(conn);

        let store = RuntimeEventStore::try_open(&path).expect("legacy migration");
        let events = store.list_stream("mission:m1").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "old-1");
        assert!(events[0].commit_cursor > 0);
        let conn = Connection::open(path).unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, STORE_SCHEMA_VERSION);
    }

    #[test]
    fn historical_session_command_scope_remains_replayable_after_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let store = RuntimeEventStore::try_open(&path).expect("event store opens");
        store
            .append(input(
                "session-command:legacy-1",
                RuntimeEventScope::SessionCommand,
                "session_execution.dispatched",
            ))
            .expect("historical command event persists");
        drop(store);

        let reopened = RuntimeEventStore::try_open(&path)
            .expect("historical session command scope remains readable");
        let events = reopened
            .list_scope(RuntimeEventScope::SessionCommand, 10)
            .expect("historical command scope lists");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].scope, RuntimeEventScope::SessionCommand);
        assert_eq!(events[0].kind, "session_execution.dispatched");
    }

    #[test]
    fn unknown_legacy_scope_aborts_migration_and_preserves_version_zero() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE runtime_events (
                event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                scope TEXT NOT NULL, kind TEXT NOT NULL, status TEXT, actor TEXT,
                payload TEXT NOT NULL, refs TEXT NOT NULL, created_at_ms INTEGER NOT NULL
             );
             INSERT INTO runtime_events VALUES
                ('bad-1', 'bad:x', 1, 'unknown_scope', 'bad', NULL, NULL, '{}', '[]', 1);",
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            RuntimeEventStore::try_open(&path),
            Err(RuntimeEventStoreError::UnknownScope(_))
        ));
        let conn = Connection::open(path).unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 0);
        assert!(!table_has_column(&conn, "runtime_events", "commit_cursor").unwrap());
    }

    #[test]
    fn legacy_terminal_outbox_schema_gains_nullable_execution_relation() {
        let mut conn = Connection::open_in_memory().expect("sqlite opens");
        conn.execute_batch(
            "CREATE TABLE runtime_session_outbox (
                terminal_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL UNIQUE,
                session_id TEXT NOT NULL,
                commit_cursor INTEGER NOT NULL,
                payload_ref TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER,
                claim_owner TEXT,
                claim_expires_at INTEGER,
                failure_class TEXT,
                last_error TEXT,
                materialized_at INTEGER,
                revision INTEGER NOT NULL DEFAULT 0
            );",
        )
        .expect("legacy outbox schema");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("migration transaction");
        create_current_tables(&tx).expect("additive schema migration");
        tx.commit().expect("migration commits");
        assert!(table_has_column(&conn, "runtime_session_outbox", "execution_id").unwrap());
        assert!(table_has_column(&conn, "runtime_session_outbox", "turn_id").unwrap());
    }

    #[test]
    fn idempotency_key_can_resolve_the_committed_side_effect() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        store.append_transaction(transaction("tx-idem")).unwrap();
        let event = store
            .event_by_idempotency_key("node:n1", "node-run")
            .unwrap()
            .expect("idempotent event");
        assert_eq!(event.kind, "node.running");
    }

    #[test]
    fn decision_lease_consumption_is_durable_and_replay_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-events.db");
        let store = RuntimeEventStore::try_open(&path).expect("event store");
        store
            .consume_verified_decision_lease(
                "lease-1",
                "human-1",
                "candidate:c-1",
                "promote",
                "evolution.candidate:c-1",
                "sha256:evidence",
                2,
                10,
            )
            .expect("first consumption");
        assert!(matches!(
            store.consume_verified_decision_lease(
                "lease-1",
                "human-1",
                "candidate:c-1",
                "promote",
                "evolution.candidate:c-1",
                "sha256:evidence",
                2,
                11,
            ),
            Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed { .. })
        ));
        drop(store);
        let reopened = RuntimeEventStore::try_open(&path).expect("reopen");
        assert!(matches!(
            reopened.consume_verified_decision_lease(
                "lease-1",
                "human-1",
                "candidate:c-1",
                "promote",
                "evolution.candidate:c-1",
                "sha256:evidence",
                2,
                12,
            ),
            Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed { .. })
        ));
    }

    #[test]
    fn terminal_outbox_reclaims_expired_lease_and_materializes_once() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        store
            .enqueue_session_terminal("t1", "m1", "s1", 7, "e:1")
            .unwrap();
        let first = store.claim_session_terminals("a", 100, 50, 8).unwrap();
        assert_eq!(first.len(), 1);
        assert!(store
            .claim_session_terminals("b", 149, 50, 8)
            .unwrap()
            .is_empty());
        let reclaimed = store.claim_session_terminals("b", 150, 50, 8).unwrap();
        let done = store
            .ack_session_terminal("t1", "b", reclaimed[0].revision, 151)
            .unwrap();
        assert_eq!(done.status, "materialized");
        assert!(store
            .claim_session_terminals("c", 1_000, 50, 8)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn terminal_drain_probe_tracks_unmaterialized_session_work() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        assert!(!store
            .has_unsettled_session_terminals("drain-session")
            .unwrap());
        store
            .enqueue_session_terminal(
                "drain-terminal",
                "drain-message",
                "drain-session",
                7,
                "assistant_json:\"done\"",
            )
            .unwrap();
        assert!(store
            .has_unsettled_session_terminals("drain-session")
            .unwrap());
        assert!(!store
            .has_unsettled_session_terminals("other-session")
            .unwrap());

        let claim = store
            .claim_session_terminals("drain-worker", 100, 50, 1)
            .unwrap()
            .remove(0);
        assert!(store
            .has_unsettled_session_terminals("drain-session")
            .unwrap());
        store
            .ack_session_terminal(&claim.terminal_id, "drain-worker", claim.revision, 101)
            .unwrap();
        assert!(!store
            .has_unsettled_session_terminals("drain-session")
            .unwrap());
    }

    #[test]
    fn materialized_terminal_replay_is_scoped_cursor_ordered_and_excludes_pending() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        for (terminal, message, session, cursor) in [
            ("t-old", "m-old", "s-a", 4_u64),
            ("t-new", "m-new", "s-a", 9_u64),
            ("t-other", "m-other", "s-b", 12_u64),
            ("t-pending", "m-pending", "s-a", 15_u64),
        ] {
            store
                .enqueue_session_terminal(
                    terminal,
                    message,
                    session,
                    cursor,
                    "assistant_json:\"ok\"",
                )
                .unwrap();
        }
        let claims = store
            .claim_session_terminals("worker", 100, 50, 10)
            .unwrap();
        for terminal in ["t-old", "t-new", "t-other"] {
            let claimed = claims
                .iter()
                .find(|record| record.terminal_id == terminal)
                .unwrap();
            store
                .ack_session_terminal(terminal, "worker", claimed.revision, 101)
                .unwrap();
        }

        let replay = store
            .materialized_session_terminals_after("s-a", 4, 10)
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].terminal_id, "t-new");
        assert_eq!(replay[0].commit_cursor, 9);
    }

    #[test]
    fn terminal_outbox_retries_then_blocks_and_rejects_conflict() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        store
            .enqueue_session_terminal("t2", "m2", "s2", 8, "e:2")
            .unwrap();
        assert!(matches!(
            store.enqueue_session_terminal("t2", "different", "s2", 8, "e:2"),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
        let first = store
            .claim_session_terminals("w", 200, 50, 1)
            .unwrap()
            .pop()
            .unwrap();
        let retry = store
            .fail_session_terminal(
                "t2",
                "w",
                first.revision,
                RuntimeSessionOutboxFailureClass::Retryable,
                "temporary",
                300,
                2,
                201,
            )
            .unwrap();
        assert_eq!(retry.status, "retry_scheduled");
        let second = store
            .claim_session_terminals("w", 300, 50, 1)
            .unwrap()
            .pop()
            .unwrap();
        let blocked = store
            .fail_session_terminal(
                "t2",
                "w",
                second.revision,
                RuntimeSessionOutboxFailureClass::Retryable,
                "still unavailable",
                400,
                2,
                301,
            )
            .unwrap();
        assert_eq!(blocked.status, "blocked");
    }

    #[test]
    fn terminal_outbox_permanent_failure_never_retries() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        store
            .enqueue_session_terminal("t3", "m3", "s3", 9, "e:3")
            .unwrap();
        let claim = store
            .claim_session_terminals("w", 400, 50, 1)
            .unwrap()
            .pop()
            .unwrap();
        let blocked = store
            .fail_session_terminal(
                "t3",
                "w",
                claim.revision,
                RuntimeSessionOutboxFailureClass::CorruptPayload,
                "invalid payload",
                500,
                10,
                401,
            )
            .unwrap();
        assert_eq!(blocked.status, "blocked");
        assert!(store
            .claim_session_terminals("w", 10_000, 50, 1)
            .unwrap()
            .is_empty());
        assert_eq!(store.blocked_session_terminals(10).unwrap().len(), 1);
        let retried = store
            .retry_session_terminal("t3", "operator", "payload repaired", 10_001)
            .unwrap();
        assert_eq!(retried.status, "retry_scheduled");
    }

    #[test]
    fn terminal_outbox_survives_restart_and_two_workers_claim_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-events.db");
        {
            let store = RuntimeEventStore::open(&path).unwrap();
            store
                .enqueue_session_terminal("restart", "m4", "s4", 10, "assistant_json:\"ok\"")
                .unwrap();
        }
        let store = Arc::new(RuntimeEventStore::open(&path).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut handles = Vec::new();
        for worker in ["worker-a", "worker-b"] {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                store.claim_session_terminals(worker, 100, 50, 1).unwrap()
            }));
        }
        barrier.wait();
        let claimed = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            claimed.len(),
            1,
            "concurrent workers must claim exactly once"
        );
    }

    #[test]
    fn terminal_transaction_requires_a_complete_fence_and_binds_replay_identity() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        let request = transaction("terminal-fence-identity");
        let terminal = fenced_terminal("identity", 2);
        let first = store
            .append_transaction_with_terminal(request.clone(), terminal.clone())
            .expect("fenced terminal commits");
        assert!(!first.duplicate);
        let replay = store
            .append_transaction_with_terminal(request.clone(), terminal.clone())
            .expect("exact terminal replay is idempotent");
        assert!(replay.duplicate);

        let mut conflicting = terminal.clone();
        conflicting.terminal_id = "terminal-conflict".to_string();
        conflicting.message_id = "assistant-conflict".to_string();
        assert!(matches!(
            store.append_transaction_with_terminal(request, conflicting),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
        assert!(store
            .session_terminal("terminal-conflict")
            .unwrap()
            .is_none());

        let mut unfenced = terminal;
        unfenced.input_claim_token = None;
        assert!(matches!(
            RuntimeEventStore::try_open_in_memory()
                .unwrap()
                .append_transaction_with_terminal(transaction("terminal-unfenced"), unfenced),
            Err(RuntimeEventStoreError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn expired_terminal_delivery_adopts_the_reclaimed_session_fence_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-terminal-adoption.db");
        let terminal = fenced_terminal("adoption", 2);
        {
            let store = RuntimeEventStore::open(&path).unwrap();
            store
                .append_transaction_with_terminal(
                    transaction("terminal-adoption"),
                    terminal.clone(),
                )
                .unwrap();
            let delivery = store
                .claim_session_terminals("delivery-before-crash", 100, 50, 1)
                .unwrap()
                .remove(0);
            assert_eq!(delivery.revision, 1);
        }

        let store = RuntimeEventStore::open(&path).unwrap();
        let current = store
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .unwrap();
        let adoption = RuntimeSessionTerminalFenceAdoption {
            terminal_id: terminal.terminal_id.clone(),
            expected_terminal_revision: current.revision,
            request_id: terminal.request_id.clone().unwrap(),
            session_id: terminal.session_id.clone(),
            turn_id: terminal.turn_id.clone().unwrap(),
            session_generation: 1,
            input_sequence: terminal.input_sequence.unwrap(),
            claim_owner: "session-worker-reclaimed".to_string(),
            claim_token: "claim-reclaimed".to_string(),
            claim_revision: 5,
            claim_expires_at_ms: 1_000,
            adopted_at_ms: 150,
        };
        let adopted = store
            .adopt_session_terminal_fence(&adoption)
            .expect("expired delivery adopts live Session fence");
        assert_eq!(adopted.status, "pending");
        assert_eq!(
            adopted.input_claim_owner.as_deref(),
            Some("session-worker-reclaimed")
        );
        assert_eq!(
            adopted.input_claim_token.as_deref(),
            Some("claim-reclaimed")
        );
        assert_eq!(adopted.input_claim_revision, Some(5));
        assert_eq!(adopted.claim_owner, None);
        assert_eq!(adopted.claim_expires_at_ms, None);
        let replay = store
            .append_transaction_with_terminal(transaction("terminal-adoption"), terminal.clone())
            .expect("initial transaction remains idempotent after fence adoption");
        assert!(replay.duplicate);

        let duplicate = store
            .adopt_session_terminal_fence(&adoption)
            .expect("same desired fence is idempotent despite old CAS revision");
        assert_eq!(duplicate.revision, adopted.revision);

        let claimed = store
            .claim_session_terminals("delivery-after-adoption", 151, 50, 1)
            .unwrap()
            .remove(0);
        assert_eq!(claimed.input_claim_revision, Some(5));
        let mut stale = adoption.clone();
        stale.expected_terminal_revision = claimed.revision;
        stale.claim_token = "claim-stale".to_string();
        stale.claim_revision = 4;
        assert!(matches!(
            store.adopt_session_terminal_fence(&stale),
            Err(RuntimeEventStoreError::InvalidTransaction(_))
        ));

        let materialized = store
            .ack_session_terminal(
                &claimed.terminal_id,
                claimed.claim_owner.as_deref().unwrap(),
                claimed.revision,
                152,
            )
            .unwrap();
        let mut after_materialized = adoption;
        after_materialized.expected_terminal_revision = materialized.revision;
        after_materialized.claim_token = "claim-newer".to_string();
        after_materialized.claim_revision = 6;
        assert!(matches!(
            store.adopt_session_terminal_fence(&after_materialized),
            Err(RuntimeEventStoreError::InvalidTransaction(_))
        ));
    }

    #[tokio::test]
    async fn commit_subscription_wakes_after_a_new_durable_commit() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let mut commits = store.subscribe_commits();
        let event = store
            .append(RuntimeEventInput {
                stream_id: "commit-watch".to_string(),
                scope: RuntimeEventScope::Mission,
                kind: "mission.commit_watch.v1".to_string(),
                status: Some("committed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::Value::Null,
            })
            .expect("append");
        tokio::time::timeout(std::time::Duration::from_secs(1), commits.changed())
            .await
            .expect("commit notification")
            .expect("watch remains open");
        assert_eq!(*commits.borrow(), event.commit_cursor);
    }
}
