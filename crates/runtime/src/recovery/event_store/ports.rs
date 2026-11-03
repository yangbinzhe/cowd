//! Runtime event contracts, migration snapshots, and backend ports.

use super::*;

/// Logical resource intent for synchronous projection work.
///
/// Backends may map Background to a separate low-priority pool while Recovery
/// preserves startup catch-up priority. The scope is thread-local and bounded
/// to one synchronous closure, so ordinary callers retain their existing lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeProjectionWorkClass {
    Background,
    Recovery,
}

/// Original Session claim passed to the backend transaction owner. The
/// Session backend still authenticates all fields while holding its row locks.
pub struct RuntimeOriginalSessionClaim<'a> {
    pub session_id: &'a str,
    pub ingress_message_id: &'a str,
    pub turn_id: &'a str,
    pub fence: &'a session::SessionTerminalExecutionFence,
    pub consumed_input_sequence: usize,
}

/// Backend-owned decision for one cancellable event transaction. A cancel
/// request is not an acknowledgement until the backend confirms rollback.
pub trait RuntimeTransactionCommitControl: Send + Sync {
    /// Requests cancellation; true means cancellation won the decision race,
    /// not that rollback has completed or the database has acknowledged it.
    fn request_cancel(&self) -> bool;
    fn begin(&self) -> Result<bool, String>;
    fn before_commit(&self);
    fn commit_wins(&self) -> Result<bool, String>;
    fn finish(&self, outcome: RuntimeTransactionOutcome);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeTransactionOutcome { Committed, RolledBack, Unknown }

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
    /// Agent-first collaboration Program journal. Program events are the
    /// canonical source for Team roster, work-market, topic and completion
    /// projections; model-authored execution graphs never write this scope.
    Program,
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
            Self::Program => "program",
            Self::Goal => "goal",
            Self::Mission => "mission",
            Self::Session => "session",
            Self::SessionInput => "session_input",
            Self::SessionCommand => "session_command",
            Self::Team => "team",
            Self::Agent => "agent",
            Self::AgentDefinition => "agent_definition",
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
            "program" => Ok(Self::Program),
            "goal" => Ok(Self::Goal),
            "mission" => Ok(Self::Mission),
            "session" => Ok(Self::Session),
            "session_input" => Ok(Self::SessionInput),
            "session_command" => Ok(Self::SessionCommand),
            "team" => Ok(Self::Team),
            "agent" => Ok(Self::Agent),
            "agent_definition" => Ok(Self::AgentDefinition),
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
    #[error("HistoryBudgetExceeded: {0}")]
    HistoryBudgetExceeded(String),
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
    /// Backend-confirmed rollback of the current checked attempt. This is not
    /// a claim that no competing transaction with the same ID can exist.
    #[error("checked transaction attempt rolled back: {0}")]
    CheckedAttemptRolledBack(Box<RuntimeEventStoreError>),
    #[error("original member authority expired before checked commit")]
    CurrentMemberAuthorityExpired,
    #[error("invalid runtime event transaction: {0}")]
    InvalidTransaction(String),
    #[error("decision lease `{lease_id}` has already been consumed")]
    DecisionLeaseAlreadyConsumed { lease_id: String },
    #[error("runtime event serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("runtime event store I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("runtime event store storage failure: {0}")]
    Storage(#[from] storage::StorageError),
}

pub type RuntimeEventStoreResult<T> = Result<T, RuntimeEventStoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PreparedContentRoot {
    Absent,
    Canonical,
    Checkpoint,
    Terminal,
    Blocked,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PreparedReconciliationReport {
    pub examined: u64,
    pub retired_records: u64,
    pub referenced_canonical: u64,
    pub referenced_checkpoint: u64,
    pub referenced_terminal: u64,
    pub retained_other_pin_or_changed: u64,
    pub blocked: u64,
    pub released_attempts: u64,
    pub inventory_pass_complete: bool,
    pub attempt_pass_complete: bool,
    pub eligible_prepared_reconciled: bool,
}

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

const ACTIVITY_BINDING_PAYLOAD_KEY: &str = "_runtime_activity_binding";

impl RuntimeEventInput {
    pub fn with_activity_binding(
        mut self,
        binding: harness_contract::projection::RuntimeActivityBinding,
    ) -> RuntimeEventStoreResult<Self> {
        binding
            .validate()
            .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
        let payload = self.payload.as_object_mut().ok_or_else(|| {
            RuntimeEventStoreError::InvalidTransaction(
                "activity-bound Runtime event payload must be an object".to_string(),
            )
        })?;
        payload.insert(
            ACTIVITY_BINDING_PAYLOAD_KEY.to_string(),
            serde_json::to_value(binding)?,
        );
        Ok(self)
    }

    #[must_use]
    pub fn activity_binding(&self) -> Option<harness_contract::projection::RuntimeActivityBinding> {
        activity_binding_from_payload(&self.payload)
    }
}

impl DurableRuntimeEvent {
    #[must_use]
    pub fn activity_binding(&self) -> Option<harness_contract::projection::RuntimeActivityBinding> {
        activity_binding_from_payload(&self.payload)
    }
}

fn activity_binding_from_payload(
    payload: &serde_json::Value,
) -> Option<harness_contract::projection::RuntimeActivityBinding> {
    payload
        .get(ACTIVITY_BINDING_PAYLOAD_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .filter(
            |binding: &harness_contract::projection::RuntimeActivityBinding| {
                binding.validate().is_ok()
            },
        )
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

/// Cumulative limits for one source check, not a fresh allowance for each page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCheckBudget {
    pub max_events: usize,
    pub max_bytes: usize,
    pub max_pages: usize,
    pub max_sources: usize,
    pub max_lock_millis: u64,
}

impl Default for SourceCheckBudget {
    fn default() -> Self {
        Self {
            max_events: 1024,
            max_bytes: 4 * 1024 * 1024,
            max_pages: 64,
            max_sources: 128,
            max_lock_millis: 1000,
        }
    }
}

impl SourceCheckBudget {
    pub fn validate(&self) -> RuntimeEventStoreResult<()> {
        let ceiling = Self::default();
        if self.max_events == 0
            || self.max_events > ceiling.max_events
            || self.max_bytes == 0
            || self.max_bytes > ceiling.max_bytes
            || self.max_pages == 0
            || self.max_pages > ceiling.max_pages
            || self.max_sources == 0
            || self.max_sources > ceiling.max_sources
            || self.max_lock_millis == 0
            || self.max_lock_millis > ceiling.max_lock_millis
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "invalid cumulative source-check budget".into(),
            ));
        }
        Ok(())
    }
}

/// Original immutable command and dependency cut. Neutral tails never rewrite
/// this request or its idempotency hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckedAppendRequest {
    pub transaction: AppendTransactionRequest,
    pub frame_digest: String,
    pub budget: SourceCheckBudget,
}

impl CheckedAppendRequest {
    pub fn identity_hash(&self) -> RuntimeEventStoreResult<String> {
        self.budget.validate()?;
        if self.frame_digest.is_empty()
            || self.transaction.expected_streams.len() > self.budget.max_sources
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "missing frame or source-check source budget exceeded".into(),
            ));
        }
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                "checked-source-v1",
                &self.transaction,
                &self.frame_digest
            ),)?)
        ))
    }
}

/// Only the original domain owner implements this trusted Rust interface.
/// It must use its immutable frame and these bounded events only: no store
/// access, locks, model/tool calls, or other IO inside validation.
pub trait CheckedSourceValidator: Send + Sync {
    fn frame_digest(&self) -> &str;
    fn validate_tail(
        &self,
        source: &ExpectedStreamRevision,
        observed_revision: u64,
        events: &[DurableRuntimeEvent],
    ) -> RuntimeEventStoreResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckedSourceEvidence {
    pub frame_digest: String,
    pub sources: Vec<CommittedStreamRevision>,
    pub events_read: usize,
    pub bytes_read: usize,
    pub pages_read: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckedAppendReceipt {
    pub commit: AppendTransactionReceipt,
    pub validation: CheckedSourceEvidence,
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
    /// Exact controlled-recovery claims settled by this turn terminal. The
    /// graph transition, this carrier and the Session outbox row commit in one
    /// transaction; older durable terminals intentionally decode as empty.
    #[serde(default)]
    pub controlled_recovery_claim_fingerprints: Vec<String>,
    pub payload_ref: String,
}

pub fn encode_session_terminal_artifact_ref(
    artifact: &harness_contract::context::ArtifactRef,
) -> Result<String, String> {
    serde_json::to_string(artifact)
        .map(|encoded| format!("{SESSION_TERMINAL_ARTIFACT_REF_PREFIX}{encoded}"))
        .map_err(|error| error.to_string())
}

pub fn decode_session_terminal_artifact_ref(
    payload_ref: &str,
) -> Result<harness_contract::context::ArtifactRef, String> {
    let encoded = payload_ref
        .strip_prefix(SESSION_TERMINAL_ARTIFACT_REF_PREFIX)
        .ok_or_else(|| "terminal payload is not an artifact reference".to_string())?;
    serde_json::from_str(encoded).map_err(|error| error.to_string())
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProjectionEventInterest {
    pub scope: RuntimeEventScope,
    pub kind: String,
}

impl RuntimeProjectionEventInterest {
    #[must_use]
    pub fn new(scope: RuntimeEventScope, kind: impl Into<String>) -> Self {
        Self {
            scope,
            kind: kind.into(),
        }
    }

    #[must_use]
    pub fn matches(&self, event: &RuntimeEventRecord) -> bool {
        self.scope == event.scope && self.kind == event.kind
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProjectionInterest {
    pub events: Vec<RuntimeProjectionEventInterest>,
}

impl RuntimeProjectionInterest {
    #[must_use]
    pub fn new(events: impl IntoIterator<Item = RuntimeProjectionEventInterest>) -> Self {
        let mut events = events
            .into_iter()
            .filter(|event| !event.kind.trim().is_empty())
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            left.scope
                .as_str()
                .cmp(right.scope.as_str())
                .then_with(|| left.kind.cmp(&right.kind))
        });
        events.dedup();
        Self { events }
    }

    #[must_use]
    pub fn matches(&self, event: &RuntimeEventRecord) -> bool {
        self.events.iter().any(|interest| interest.matches(event))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProjectionScanPage {
    pub scanned_through_cursor: u64,
    pub scanned_commits: usize,
    pub matched_events: usize,
    pub batches: Vec<CommittedEventBatch>,
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
    pub(super) const fn as_str(self) -> &'static str {
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
    #[serde(default)]
    pub suppressed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSourceMutationFence {
    pub incarnation: String,
    pub mutation_epoch: u64,
}

/// Latest durable state for one rebuildable Runtime projection.
///
/// `source_cursor` is the projection fence: writers may only move it forward.
/// `revision` is the mutable-row revision and is not part of the immutable
/// Runtime event cursor. Updating a projection therefore never wakes source
/// event subscribers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProjectionCheckpoint {
    pub projection_id: String,
    pub source_cursor: u64,
    pub revision: u64,
    pub payload: serde_json::Value,
    pub updated_at_ms: u64,
}

/// Backend contract for the durable Runtime lifecycle ledger.
///
/// Runtime business callers must use [`RuntimeEventStore`], never this port.
/// It is public solely so an isolated infrastructure crate can provide a
/// backend without pulling a database driver into the Runtime dependency tree.
/// Every method preserves event transaction, revision, lease, and outbox
/// semantics; a backend must not provide a partial implementation.
#[derive(Debug, Clone)]
pub enum RuntimeBoundedEventLookup {
    Absent,
    Deferred { required_bytes: usize },
    Record { record: Box<RuntimeEventRecord>, encoded_bytes: usize },
}

#[cfg(any(test, feature = "test-fixtures"))]
pub struct FixtureEvidencePageRequest<'a> {
    pub stream_id: &'a str,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub max_events: usize,
    pub max_bytes: usize,
    pub deadline: std::time::Instant,
}

#[cfg(any(test, feature = "test-fixtures"))]
pub struct FixtureEvidencePage {
    pub head: u64,
    pub next_sequence: u64,
    pub encoded_bytes: usize,
    pub records: Vec<DurableRuntimeEvent>,
    pub stop: Option<&'static str>,
}

/// Classification local to bounded position-page reads. Backends that cannot
/// prove the first-row condition retain the original error as Other.
#[derive(Debug, thiserror::Error)]
pub enum PositionPageError {
    #[error("HistoryBudgetExceeded: {0}")]
    FirstRowExceedsBudget(String),
    #[error("{0}")]
    Other(String),
}

impl From<String> for PositionPageError {
    fn from(value: String) -> Self { Self::Other(value) }
}
impl From<&str> for PositionPageError {
    fn from(value: &str) -> Self { Self::Other(value.to_owned()) }
}

pub trait RuntimeEventStoreBackend: std::fmt::Debug + Send + Sync + std::any::Any {
    #[cfg(any(test, feature = "test-fixtures"))]
    fn fixture_evidence_page(
        &self,
        _request: &FixtureEvidencePageRequest<'_>,
    ) -> Result<FixtureEvidencePage, &'static str> {
        Err("Unavailable: backend has no deadline-bounded fixture reader")
    }
    fn bounded_event_by_idempotency_key(
        &self,
        stream: &str,
        key: &str,
        through: u64,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeBoundedEventLookup> {
        let _ = (stream, key, through, max_bytes);
        Err(RuntimeEventStoreError::Corrupt("bounded exact source lookup unsupported".into()))
    }

    fn source_mutation_fence(&self) -> RuntimeEventStoreResult<RuntimeSourceMutationFence> {
        Err(RuntimeEventStoreError::Corrupt("source mutation fence unsupported".into()))
    }

    fn compare_and_put_projection_checkpoint_fenced(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
        source_fence: &RuntimeSourceMutationFence,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let _ = (projection_id, source_cursor, expected_revision, payload, updated_at_ms, source_fence);
        Err(RuntimeEventStoreError::Corrupt("atomic source-fenced checkpoint unsupported".into()))
    }

    fn recent_event_lines(
        &self,
        stream_id: &str,
        references: &[RuntimeEventRef],
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String>;
    fn prepared_content_root(
        &self,
        origin: &crate::PreparedArtifactOrigin,
        artifact: &harness_contract::context::ArtifactRef,
    ) -> RuntimeEventStoreResult<PreparedContentRoot> {
        let _ = (origin, artifact);
        Err(RuntimeEventStoreError::Corrupt(
            "prepared root probe unsupported".into(),
        ))
    }
    /// Retrieve the durable original transaction; unsupported is not absence.
    fn original_transaction_receipt(&self, _transaction_id: &str)
        -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        Err(RuntimeEventStoreError::InvalidTransaction("original transaction receipt probe unsupported".into()))
    }
    /// Read the source validation recorded by the original checked transaction.
    /// A missing record is not proof that the transaction rolled back.
    fn original_checked_transaction_receipt(&self, _transaction_id: &str)
        -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        Err(RuntimeEventStoreError::InvalidTransaction("original checked transaction receipt probe unsupported".into()))
    }
    fn transaction_stream_revision_exact(
        &self,
        transaction_id: &str,
        stream_id: &str,
    ) -> RuntimeEventStoreResult<Option<CommittedStreamRevision>> {
        let _ = (transaction_id, stream_id);
        Err(RuntimeEventStoreError::Corrupt(
            "exact transaction stream probe unsupported".into(),
        ))
    }
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String>;
    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
    fn append_transaction_cancellable(
        &self, _request: AppendTransactionRequest, _control: &dyn RuntimeTransactionCommitControl,
    ) -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        Err(RuntimeEventStoreError::InvalidTransaction("backend has no cancellable transaction owner".into()))
    }
    fn append_checked_transaction(
        &self,
        _request: CheckedAppendRequest,
        _validator: &dyn CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<CheckedAppendReceipt> {
        Err(RuntimeEventStoreError::InvalidTransaction(
            "backend does not support checked-source transactions".into(),
        ))
    }
    /// Checked source validation and cancellation must share the backend's
    /// physical transaction and original commit-control decision.
    fn append_checked_transaction_cancellable(
        &self,
        _request: CheckedAppendRequest,
        _validator: &dyn CheckedSourceValidator,
        _control: &dyn RuntimeTransactionCommitControl,
        _claim: &RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        Err(RuntimeEventStoreError::InvalidTransaction(
            "backend has no checked cancellable transaction owner".into(),
        ))
    }
    /// Authenticate an already committed checked terminal. Absence means only
    /// that no winner was observed, never that an earlier attempt rolled back.
    fn original_checked_terminal_winner(
        &self, _request: &CheckedAppendRequest, _terminal: &SessionTerminalInput,
        _validator: &dyn CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        Err(RuntimeEventStoreError::InvalidTransaction("backend has no checked terminal winner owner".into()))
    }
    /// Terminal, checked sources, original Session claim and commit control
    /// share the same backend transaction. Unsupported owners fail closed.
    fn append_checked_terminal_cancellable(
        &self, _request: CheckedAppendRequest, _terminal: SessionTerminalInput,
        _validator: &dyn CheckedSourceValidator, _control: &dyn RuntimeTransactionCommitControl,
        _claim: &RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        Err(RuntimeEventStoreError::InvalidTransaction("backend has no checked terminal transaction owner".into()))
    }
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
    fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage>;
    fn background_projection_capacity_hint(&self) -> usize {
        1
    }
    fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>>;
    fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>>;
    fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
    fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
    /// Replace a damaged derived checkpoint with a journal-rebuilt value.
    /// Unlike ordinary CAS this permits source-cursor correction, but MUST
    /// fence the existing revision and increment it (never delete/recreate).
    fn compare_and_repair_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
    fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>>;
    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64>;
    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn position_page_classified(
        &self, scope: RuntimeEventScope, identity: &str, kind: Option<&str>,
        after: Option<(u64, u32)>, through_cursor: u64, max_events: usize, max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, PositionPageError> {
        self.position_page(scope, identity, kind, after, through_cursor, max_events, max_bytes)
            .map_err(PositionPageError::Other)
    }
    fn position_page(
        &self,
        scope: RuntimeEventScope,
        identity: &str,
        kind: Option<&str>,
        after: Option<(u64, u32)>,
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn list_stream_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn list_stream_before(
        &self,
        stream_id: &str,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    fn count_stream_kinds(
        &self,
        stream_id: &str,
        kinds: &[&str],
        through_sequence: u64,
    ) -> Result<u64, String>;
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
    /// Read activity events by the immutable Runtime-owned root identity.
    fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    /// Read one event kind inside an immutable Runtime-owned root identity.
    fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String>;
    /// Read one activity lifecycle without materialising the whole execution.
    fn events_for_activity(
        &self,
        activity_id: &str,
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
    /// Page canonical stream identifiers without materialising the complete
    /// aggregate catalogue. The cursor is the last `(commit_cursor, stream_id)`
    /// returned by the previous page.
    fn stream_ids_for_scope_kind_at_sequence_page(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
        after: Option<(u64, String)>,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<(String, u64)>>;
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
    fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64> {
        let events = self
            .all_events(1)
            .map_err(|error| RuntimeEventStoreError::Io(std::io::Error::other(error)))?;
        Ok(events
            .into_iter()
            .map(|event| event.commit_cursor)
            .max()
            .unwrap_or_default())
    }
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
    fn session_terminal_for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
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
    fn suppress_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
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
}
