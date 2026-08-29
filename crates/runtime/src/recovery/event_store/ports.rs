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

    pub(super) fn canonicalize(&mut self) {
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
    fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        let batches = self.events_after_cursor(cursor, max_commits)?;
        let mut scanned_through_cursor = cursor;
        let mut scanned_commits = 0_usize;
        let mut matched_events = 0_usize;
        let mut matched_bytes = 0_usize;
        let mut filtered = Vec::new();
        for batch in batches {
            let events = batch
                .events
                .into_iter()
                .filter(|event| interest.matches(event))
                .collect::<Vec<_>>();
            if events.is_empty() {
                scanned_through_cursor = batch.commit_cursor;
                scanned_commits = scanned_commits.saturating_add(1);
                continue;
            }
            let batch_bytes = events.iter().fold(0_usize, |total, event| {
                total.saturating_add(serde_json::to_vec(event).map_or(0, |bytes| bytes.len()))
            });
            if !filtered.is_empty()
                && (matched_events.saturating_add(events.len()) > max_events.max(1)
                    || matched_bytes.saturating_add(batch_bytes) > max_bytes.max(1))
            {
                break;
            }
            matched_events = matched_events.saturating_add(events.len());
            matched_bytes = matched_bytes.saturating_add(batch_bytes);
            filtered.push(CommittedEventBatch {
                commit_cursor: batch.commit_cursor,
                transaction_id: batch.transaction_id,
                events,
            });
            scanned_through_cursor = batch.commit_cursor;
            scanned_commits = scanned_commits.saturating_add(1);
        }
        Ok(RuntimeProjectionScanPage {
            scanned_through_cursor,
            scanned_commits,
            matched_events,
            batches: filtered,
        })
    }
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
    fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
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
    fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot>;
    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()>;
}
