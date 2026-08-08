//! Runtime-owned Session application port.
//!
//! Runtime defines the use cases it consumes; the production implementation
//! lives in Gateway's `SessionService`. Runtime must never receive a Session
//! repository, backend, or `UnifiedSessionStore` handle.

use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::{task::TaskRouteHint, turn::InputRoutingDecision};
use serde::{Deserialize, Serialize};
use session::SessionError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionRecord {
    pub session_id: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionInputAdmission {
    pub session_id: String,
    pub generation: u64,
    pub open: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSessionInputStatus {
    Accepted,
    Classified,
    Queued,
    RejectedDuplicate,
    RejectedPolicy,
    Claimed,
    Running,
    Reclassified,
    Completed,
    Supplemented,
    Failed,
    Blocked,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionInputRecord {
    pub input_id: String,
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub session_generation: u64,
    pub decision: InputRoutingDecision,
    pub target_turn_id: Option<String>,
    pub classification_json: Option<String>,
    pub task_route_hint: Option<TaskRouteHint>,
    pub status: RuntimeSessionInputStatus,
    pub runtime_commit_cursor: Option<u64>,
    pub attempts: u32,
    pub next_attempt_at_ms: u64,
    pub claim_owner: Option<String>,
    pub claim_token: Option<String>,
    pub claim_fence_epoch: Option<u64>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<String>,
    pub last_error: Option<String>,
    pub revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub terminal_at_ms: Option<u64>,
    pub runtime_options_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionIngressCommand {
    pub input_id: String,
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub session_generation: u64,
    pub decision: InputRoutingDecision,
    pub target_turn_id: Option<String>,
    pub classification_json: Option<String>,
    pub task_route_hint: Option<TaskRouteHint>,
    pub created_at_ms: u64,
    pub runtime_options_json: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSessionEventKind {
    SkillCandidates,
    SkillMemoryCandidate,
    ContextTurnReport,
    ProviderRequestPacked,
    ContextGovernanceReport,
    ContextFactCandidateReview,
    ContextSessionCompacted,
    MemorySemanticCheckpointCreated,
    RuntimePolicyDecided,
    EvidenceRawPersisted,
    ToolInvocationStarted,
    ToolInvocationCompleted,
    ToolInvocationFailed,
    ToolExecutionPlanCreated,
}

impl RuntimeSessionEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SkillCandidates => "skill_candidates",
            Self::SkillMemoryCandidate => "skill_memory_candidate",
            Self::ContextTurnReport => "context.turn_report",
            Self::ProviderRequestPacked => "context.provider_request_packed",
            Self::ContextGovernanceReport => "context.governance_report",
            Self::ContextFactCandidateReview => "context.fact_candidate_review",
            Self::ContextSessionCompacted => "context.session_compacted",
            Self::MemorySemanticCheckpointCreated => "memory.semantic_checkpoint.created",
            Self::RuntimePolicyDecided => "runtime.policy.decided",
            Self::EvidenceRawPersisted => "evidence.raw.persisted",
            Self::ToolInvocationStarted => "tool.invocation.started",
            Self::ToolInvocationCompleted => "tool.invocation.completed",
            Self::ToolInvocationFailed => "tool.invocation.failed",
            Self::ToolExecutionPlanCreated => "tool.execution_plan.created",
        }
    }

    #[must_use]
    pub const fn scope(self) -> session::SessionDomainScope {
        match self {
            Self::MemorySemanticCheckpointCreated => session::SessionDomainScope::Memory,
            Self::RuntimePolicyDecided => session::SessionDomainScope::Policy,
            Self::EvidenceRawPersisted
            | Self::ToolInvocationStarted
            | Self::ToolInvocationCompleted
            | Self::ToolInvocationFailed
            | Self::ToolExecutionPlanCreated => session::SessionDomainScope::Tool,
            Self::SkillCandidates
            | Self::SkillMemoryCandidate
            | Self::ContextTurnReport
            | Self::ProviderRequestPacked
            | Self::ContextGovernanceReport
            | Self::ContextFactCandidateReview
            | Self::ContextSessionCompacted => session::SessionDomainScope::Context,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionEventRef {
    pub ref_type: String,
    pub id: String,
    pub label: Option<String>,
}

/// Runtime-owned semantic write command. The event kind and scope are closed
/// enums, so Runtime cannot turn this port into an arbitrary Session writer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSessionEvent {
    pub session_id: String,
    pub sequence_hint: usize,
    pub kind: RuntimeSessionEventKind,
    pub payload: serde_json::Value,
    pub occurred_at_ms: u64,
    pub span_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub correlation_id: Option<String>,
    pub status: Option<String>,
    pub refs: Vec<RuntimeSessionEventRef>,
}

impl RuntimeSessionEvent {
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        sequence_hint: usize,
        kind: RuntimeSessionEventKind,
        payload: serde_json::Value,
        occurred_at_ms: u64,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            sequence_hint,
            kind,
            payload,
            occurred_at_ms,
            span_id: None,
            parent_span_id: None,
            correlation_id: None,
            status: None,
            refs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = Some(status.into());
        self
    }

    #[must_use]
    pub fn with_ref(mut self, reference: RuntimeSessionEventRef) -> Self {
        self.refs.push(reference);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeContextEnvelopeRecord {
    pub session_id: String,
    pub payload: serde_json::Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionEventReceipt {
    pub sequence: usize,
}

#[async_trait]
pub trait SessionRuntimeQueryPort: Send + Sync {
    fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>>;

    async fn session_record(
        &self,
        session_id: &str,
    ) -> Result<Option<RuntimeSessionRecord>, SessionError>;

    async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<RuntimeSessionInputRecord>, SessionError>;

    async fn input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<RuntimeSessionInputAdmission>, SessionError>;
}

#[async_trait]
pub trait SessionRuntimeIngressPort: Send + Sync {
    async fn append_ingress(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &RuntimeSessionIngressCommand,
    ) -> Result<RuntimeSessionInputRecord, SessionError>;
}

#[async_trait]
pub trait SessionRuntimeJournalPort: Send + Sync {
    async fn append_event(
        &self,
        event: &RuntimeSessionEvent,
    ) -> Result<RuntimeSessionEventReceipt, SessionError>;

    async fn append_context_envelope_if_absent(
        &self,
        record: &RuntimeContextEnvelopeRecord,
    ) -> Result<Option<RuntimeSessionEventReceipt>, SessionError>;

    async fn append_compaction_bundle_if_absent(
        &self,
        events: &[RuntimeSessionEvent],
        checkpoint_id: &str,
    ) -> Result<bool, SessionError>;
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestSessionPortAdapter {
    store: Arc<session::UnifiedSessionStore>,
}

#[cfg(test)]
impl TestSessionPortAdapter {
    pub(crate) fn new(store: Arc<session::UnifiedSessionStore>) -> Arc<Self> {
        Arc::new(Self { store })
    }
}

#[cfg(test)]
#[async_trait]
impl SessionRuntimeQueryPort for TestSessionPortAdapter {
    fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        Some(Arc::new(self.store.history_reader()))
    }

    async fn session_record(
        &self,
        session_id: &str,
    ) -> Result<Option<RuntimeSessionRecord>, SessionError> {
        self.store
            .get_session(session_id)
            .await
            .map(|record| record.map(to_runtime_session_record))
    }

    async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<RuntimeSessionInputRecord>, SessionError> {
        self.store
            .get_session_runtime_outbox(request_id)
            .await
            .map(|record| record.map(to_runtime_input_record))
    }

    async fn input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<RuntimeSessionInputAdmission>, SessionError> {
        self.store
            .get_session_input_admission(session_id)
            .await
            .map(|admission| {
                admission.map(|admission| RuntimeSessionInputAdmission {
                    session_id: admission.session_id,
                    generation: admission.generation,
                    open: admission.open,
                })
            })
    }
}

#[cfg(test)]
#[async_trait]
impl SessionRuntimeIngressPort for TestSessionPortAdapter {
    async fn append_ingress(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &RuntimeSessionIngressCommand,
    ) -> Result<RuntimeSessionInputRecord, SessionError> {
        let request = to_session_ingress_request(request);
        self.store
            .append_ingress_with_runtime_outbox(
                session_id,
                role,
                content_json,
                created_at_ms,
                &request,
            )
            .await
            .map(to_runtime_input_record)
    }
}

#[cfg(test)]
#[async_trait]
impl SessionRuntimeJournalPort for TestSessionPortAdapter {
    async fn append_event(
        &self,
        event: &RuntimeSessionEvent,
    ) -> Result<RuntimeSessionEventReceipt, SessionError> {
        self.store
            .append_session_domain_event_allocating_sequence(&to_session_event(event))
            .await
            .map(|event| RuntimeSessionEventReceipt {
                sequence: event.sequence,
            })
    }

    async fn append_context_envelope_if_absent(
        &self,
        record: &RuntimeContextEnvelopeRecord,
    ) -> Result<Option<RuntimeSessionEventReceipt>, SessionError> {
        let event = session::SessionEvent {
            session_id: record.session_id.clone(),
            event_type: "ContextEnvelope".to_string(),
            event_json: record.payload.to_string(),
            sequence: 0,
            created_at_ms: record.created_at_ms,
        };
        self.store
            .append_context_envelope_event_if_absent_allocating_sequence(&event)
            .await
            .map(|event| {
                event.map(|event| RuntimeSessionEventReceipt {
                    sequence: event.sequence,
                })
            })
    }

    async fn append_compaction_bundle_if_absent(
        &self,
        events: &[RuntimeSessionEvent],
        checkpoint_id: &str,
    ) -> Result<bool, SessionError> {
        let events = events.iter().map(to_session_event).collect::<Vec<_>>();
        self.store
            .append_session_domain_events_if_checkpoint_absent(&events, checkpoint_id)
            .await
    }
}

#[cfg(test)]
fn to_session_event(event: &RuntimeSessionEvent) -> session::SessionDomainEvent {
    let mut domain_event = session::SessionDomainEvent::new(
        event.session_id.clone(),
        event.sequence_hint,
        event.kind.scope(),
        event.kind.as_str(),
        event.payload.clone(),
        event.occurred_at_ms,
    );
    domain_event.status.clone_from(&event.status);
    domain_event.span_id.clone_from(&event.span_id);
    domain_event
        .parent_span_id
        .clone_from(&event.parent_span_id);
    domain_event
        .correlation_id
        .clone_from(&event.correlation_id);
    domain_event.refs = event
        .refs
        .iter()
        .map(|reference| session::SessionDomainRef {
            ref_type: reference.ref_type.clone(),
            id: reference.id.clone(),
            label: reference.label.clone(),
        })
        .collect();
    domain_event
}

#[cfg(test)]
fn to_runtime_session_record(record: session::SessionRecord) -> RuntimeSessionRecord {
    RuntimeSessionRecord {
        session_id: record.session_id,
        status: record.status,
    }
}

#[cfg(test)]
fn to_session_ingress_request(
    request: &RuntimeSessionIngressCommand,
) -> session::SessionRuntimeOutboxRequest {
    session::SessionRuntimeOutboxRequest {
        input_id: request.input_id.clone(),
        request_id: request.request_id.clone(),
        turn_id: request.turn_id.clone(),
        message_id: request.message_id.clone(),
        session_generation: request.session_generation,
        decision: request.decision,
        target_turn_id: request.target_turn_id.clone(),
        classification_json: request.classification_json.clone(),
        task_route_hint: request.task_route_hint.clone(),
        created_at_ms: request.created_at_ms,
        runtime_options_json: request.runtime_options_json.clone(),
    }
}

#[cfg(test)]
pub(crate) fn to_runtime_input_record(
    record: session::SessionRuntimeOutboxRecord,
) -> RuntimeSessionInputRecord {
    RuntimeSessionInputRecord {
        input_id: record.input_id,
        request_id: record.request_id,
        turn_id: record.turn_id,
        message_id: record.message_id,
        session_id: record.session_id,
        sequence: record.sequence,
        session_generation: record.session_generation,
        decision: record.decision,
        target_turn_id: record.target_turn_id,
        classification_json: record.classification_json,
        task_route_hint: record.task_route_hint,
        status: match record.status {
            session::SessionRuntimeInputStatus::Accepted => RuntimeSessionInputStatus::Accepted,
            session::SessionRuntimeInputStatus::Classified => RuntimeSessionInputStatus::Classified,
            session::SessionRuntimeInputStatus::Queued => RuntimeSessionInputStatus::Queued,
            session::SessionRuntimeInputStatus::RejectedDuplicate => {
                RuntimeSessionInputStatus::RejectedDuplicate
            }
            session::SessionRuntimeInputStatus::RejectedPolicy => {
                RuntimeSessionInputStatus::RejectedPolicy
            }
            session::SessionRuntimeInputStatus::Claimed => RuntimeSessionInputStatus::Claimed,
            session::SessionRuntimeInputStatus::Running => RuntimeSessionInputStatus::Running,
            session::SessionRuntimeInputStatus::Reclassified => {
                RuntimeSessionInputStatus::Reclassified
            }
            session::SessionRuntimeInputStatus::Completed => RuntimeSessionInputStatus::Completed,
            session::SessionRuntimeInputStatus::Supplemented => {
                RuntimeSessionInputStatus::Supplemented
            }
            session::SessionRuntimeInputStatus::Failed => RuntimeSessionInputStatus::Failed,
            session::SessionRuntimeInputStatus::Blocked => RuntimeSessionInputStatus::Blocked,
            session::SessionRuntimeInputStatus::Cancelled => RuntimeSessionInputStatus::Cancelled,
            session::SessionRuntimeInputStatus::Expired => RuntimeSessionInputStatus::Expired,
        },
        runtime_commit_cursor: record.runtime_commit_cursor,
        attempts: record.attempts,
        next_attempt_at_ms: record.next_attempt_at_ms,
        claim_owner: record.claim_owner,
        claim_token: record.claim_token,
        claim_fence_epoch: record.claim_fence_epoch,
        claim_expires_at_ms: record.claim_expires_at_ms,
        failure_class: record
            .failure_class
            .map(|failure| failure.as_str().to_string()),
        last_error: record.last_error,
        revision: record.revision,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
        terminal_at_ms: record.terminal_at_ms,
        runtime_options_json: record.runtime_options_json,
    }
}
