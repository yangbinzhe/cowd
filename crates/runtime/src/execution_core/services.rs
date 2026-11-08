//! Workspace-owned runtime service graph.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{FutureExt, StreamExt};
use harness_contract::agent::{
    AgentEvaluationBinding, AgentReleaseBinding, AgentTaskIntent, AgentTaskPacket,
    AgentTerminalStatus, ReleaseChannel, RevisionSelector,
};
use harness_contract::context::ChildExecutionBudgetReservation;
use harness_contract::evaluation::{EvaluationScenarioObservation, EvaluationScenarioSpec};
use harness_contract::execution::ExecutionIdentity;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::cross_plane::{CrossPlaneRuntimeError, CrossPlaneRuntimeService};
use super::goal::{GoalStore, ObjectiveSupervisor};
use super::graph::{
    executors::{
        AgentTaskExecutor, AgenticProgramWaitExecutor, ApprovalNodeExecutor,
        CompileTargetGuardExecutor, MaterializeNodeExecutor, ScopedNodeExecutor,
        SynthesizeNodeExecutor, VerifyNodeExecutor,
    },
    ExecutionCommitService, ExecutionGraphRunner, ExecutionGraphStateStore, ExecutionRecoveryError,
    ExecutionResourceKind, ExecutionResourceManager, ExecutionRunnerError, ExecutionServiceClass,
    ExecutionStateStoreError, NodeExecutor, NodeExecutorError, NodeExecutorRegistry,
    ResourceAdmissionDecision, ResourceAdmissionRequest, ResourceObservation, ResourceQuota,
    ResourceResultClass, ScopeLockError, ScopeLockManager, WorktreeLeaseError,
    WorktreeLeaseManager,
};
use crate::agent::binding::request_for_intent;
use crate::agent::definition::ExplicitTomlAgentImport;
use crate::managed_agent::ManagedAgentRestartDisposition;
use crate::runtime_event_store::RuntimeEventStoreError;
#[cfg(feature = "test-fixtures")]
use crate::RuntimeEventInput;
use crate::{
    AgentBindingCompiler, AgentBindingRequest, AgentDefinitionDraftReceipt, AgentRuntime,
    AgentRuntimeResolver, ApprovalConfig, ApprovalCoordinator, ApprovalQueue, CompiledAgentBinding,
    ConflictArbiter, DefinitionRegistryError, DurableRuntimeEvent, ExecutionGraphHost,
    InProcessAgentWorker, ManagedAgentRuntimeDispatchReport, MissionEvidenceBus, MissionRuntime,
    MissionScheduleStore, ProcessJsonlAdapter, ProcessJsonlSpec, RealityRecallPort,
    RuntimeDefinitionRegistry, RuntimeEventReplayer, RuntimeEventScope, RuntimeEventStore,
    RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord,
    SessionInputRouter, SessionRelationGraph,
};

#[path = "composition.rs"]
mod composition;
use composition::normalize_provider_fallbacks;

#[path = "lifecycle_services.rs"]
mod lifecycle_services;

#[derive(Debug, Error)]
pub enum RuntimeServicesError {
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error(transparent)]
    DefinitionRegistry(#[from] DefinitionRegistryError),
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
    #[error(transparent)]
    Artifact(#[from] crate::ArtifactError),
    #[error(transparent)]
    WorktreeLease(#[from] WorktreeLeaseError),
    #[error(transparent)]
    ScopeLock(#[from] ScopeLockError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    CrossPlane(#[from] CrossPlaneRuntimeError),
    #[error(transparent)]
    NodeExecutor(#[from] NodeExecutorError),
    #[error(transparent)]
    GraphState(#[from] ExecutionStateStoreError),
    #[error(transparent)]
    GraphRecovery(#[from] ExecutionRecoveryError),
    #[error(transparent)]
    GraphRunner(#[from] ExecutionRunnerError),
    #[error("runtime service root cannot be empty")]
    EmptyRoot,
    #[error("mission runtime initialization failed: {0}")]
    Mission(String),
    #[error("task runtime initialization failed: {0}")]
    Task(String),
    #[error("runtime composition requires injected PostgreSQL backend `{0}`")]
    MissingBackend(&'static str),
    #[error("agent runtime initialization failed: {0}")]
    AgentRuntime(String),
    #[error("session input router was concurrently installed")]
    DuplicateSessionRouter,
    #[error("session integration requires query, ingress, journal and application ports together")]
    IncompleteSessionPorts,
    #[error("workspace mutation is blocked because upgrade recovery is required")]
    UpgradeRecoveryRequired,
    #[error("durable session handoff recovery failed: {0}")]
    SessionHandoffRecovery(String),
    #[error("execution projection access is denied by the current scoped authorization")]
    ProjectionAccessDenied,
    #[error("execution projection invariant failed: {0}")]
    Invariant(String),
}

pub(crate) fn read_original_requested_event(
    store: &RuntimeEventStore,
    receipt: &harness_contract::turn::CancellationReceipt,
) -> Result<
    (
        DurableRuntimeEvent,
        harness_contract::turn::CancellationReceipt,
    ),
    RuntimeServicesError,
> {
    let stream_id = format!("cancellation:{}", receipt.cancellation_id);
    let events = store
        .list_stream_after(&stream_id, 0, 1, 1, 256 * 1024)
        .map_err(RuntimeServicesError::Invariant)?;
    let event = events.into_iter().next().ok_or_else(|| {
        RuntimeServicesError::Invariant("original Requested cancellation event is missing".into())
    })?;
    let source = authenticate_requested_event(&event, receipt)?;
    Ok((event, source))
}

fn authenticate_requested_event(
    event: &DurableRuntimeEvent,
    expected: &harness_contract::turn::CancellationReceipt,
) -> Result<harness_contract::turn::CancellationReceipt, RuntimeServicesError> {
    use harness_contract::turn::{CancellationCause, CancellationReceipt, CancellationStatus};
    let reject =
        || RuntimeServicesError::Invariant("invalid original Requested cancellation event".into());
    let mut source: CancellationReceipt =
        serde_json::from_value(event.payload.clone()).map_err(|_| reject())?;
    let mut normalized = expected.clone();
    normalized.journal_sequence = 0;
    normalized.projection_revision = 0;
    let refs = vec![
        crate::RuntimeEventRef {
            kind: "session".into(),
            id: source.session_id.clone(),
        },
        crate::RuntimeEventRef {
            kind: "execution".into(),
            id: source.execution_id.clone(),
        },
        crate::RuntimeEventRef {
            kind: "turn".into(),
            id: source.turn_id.clone(),
        },
    ];
    if source != normalized
        || source.status != CancellationStatus::Requested
        || source.cause != CancellationCause::UserRequested
        || source.effective_at_ms.is_some()
        || source.requested_at_ms == 0
        || source.cancellation_id.is_empty()
        || !source.actor_id.starts_with("principal:")
        || source.actor_id.len() <= "principal:".len()
        || source.session_id.is_empty()
        || source.turn_id.is_empty()
        || source.execution_id.is_empty()
        || source.journal_sequence != 0
        || source.projection_revision != 0
        || event.event_id.is_empty()
        || event.commit_cursor == 0
        || event.sequence != 1
        || event.stream_id != format!("cancellation:{}", source.cancellation_id)
        || event.scope != RuntimeEventScope::Session
        || event.kind != "session.cancellation_committed"
        || event.status.as_deref() != Some("requested")
        || event.actor.as_deref() != Some(source.actor_id.as_str())
        || event.refs != refs
        || event.schema_version != 1
        || event.transaction_index != 0
        || event.transaction_id
            != format!("session-cancellation:{}:requested", source.cancellation_id)
        || event.idempotency_key.as_deref()
            != Some(format!("{}:requested", source.cancellation_id).as_str())
        || (expected.journal_sequence != 0 && expected.journal_sequence != event.commit_cursor)
        || (expected.projection_revision != 0 && expected.projection_revision != event.sequence)
    {
        return Err(reject());
    }
    source.journal_sequence = event.commit_cursor;
    source.projection_revision = event.sequence;
    Ok(source)
}

fn publish_requested_source(
    control: &Arc<TurnCancellationControl>,
    source: AuthorizedRequestedCancellation,
) -> Result<CancellationNotificationDisposition, RuntimeServicesError> {
    if !source
        .source
        .original_control
        .ptr_eq(&Arc::downgrade(control))
        || !control.matches_target(
            &source.source.receipt.session_id,
            &source.source.receipt.turn_id,
            &source.source.receipt.execution_id,
        )
        || control.ingress.is_none()
    {
        return Err(RuntimeServicesError::Invariant(
            "Requested source belongs to a different original control".into(),
        ));
    }
    let mut cause = control
        .first_cause
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match &*cause {
        TurnCancellationCause::None => {
            // Publish under the same cause lock before notifying. A raw token
            // signal racing publication wins as unattributed/operational; it
            // must never acquire the later user's identity retroactively.
            *cause = TurnCancellationCause::AuthorizedRequested(source);
            if !control.token.cancel_first_signal() {
                *cause = TurnCancellationCause::Operational;
                return Ok(CancellationNotificationDisposition::OperationalFirst);
            }
            return Ok(CancellationNotificationDisposition::Delivered);
        }
        TurnCancellationCause::Operational => {
            return Ok(CancellationNotificationDisposition::OperationalFirst)
        }
        TurnCancellationCause::AuthorizedRequested(existing) => {
            if existing.event_id() != source.event_id()
                || existing.event_reference() != source.event_reference()
                || existing.receipt() != source.receipt()
                || existing.authorization != source.authorization
            {
                return Err(RuntimeServicesError::Invariant(
                    "foreign Requested cancellation source".into(),
                ));
            }
        }
    }
    control.token.cancel();
    Ok(CancellationNotificationDisposition::Delivered)
}

/// Original cancellation source for one installed root turn. The token only wakes
/// work; first-cause attribution is protected independently by this short lock.
/// Last operational turn-cancellation signal, kept so a later abort can name
/// the scope that escalated it.  Test-fixture only: it carries no production
/// behaviour and exists because a captured child trace keeps only its tail.
#[cfg(feature = "test-fixtures")]
static LAST_OPERATIONAL_CANCEL: std::sync::OnceLock<
    std::sync::Mutex<Option<std::string::String>>,
> = std::sync::OnceLock::new();

#[cfg(feature = "test-fixtures")]
pub(crate) fn record_last_operational_cancel(site: &str) {
    let slot = LAST_OPERATIONAL_CANCEL.get_or_init(|| std::sync::Mutex::new(None));
    if let Ok(mut guard) = slot.lock() {
        *guard = Some(site.to_string());
    }
}

#[cfg(feature = "test-fixtures")]
pub(crate) fn last_operational_cancel() -> Option<std::string::String> {
    let slot = LAST_OPERATIONAL_CANCEL.get_or_init(|| std::sync::Mutex::new(None));
    slot.lock().ok().and_then(|guard| guard.clone())
}

pub struct TurnCancellationControl {
    session_id: String,
    turn_id: String,
    execution_id: String,
    ingress: Option<crate::TurnIngressRef>,
    token: crate::CancellationToken,
    first_cause: Mutex<TurnCancellationCause>,
    /// Step-level wakeup on the shared home. A provider step stop wakes the
    /// waiters here without touching the turn-level token or first cause.
    step_wakeup: tokio::sync::Notify,
}

enum TurnCancellationCause {
    None,
    Operational,
    AuthorizedRequested(AuthorizedRequestedCancellation),
}

/// Authenticated Runtime intent and frozen target, not Session authorization.
#[derive(Debug, Clone)]
pub struct AuthenticatedRequestedCancellation {
    receipt: harness_contract::turn::CancellationReceipt,
    event_id: String,
    stream_id: String,
    sequence: u64,
    commit_cursor: u64,
    digest: String,
    original_control: std::sync::Weak<TurnCancellationControl>,
    live_binding: Option<Arc<RequestedCancellationBinding>>,
}

/// Original Session authorization, minted only after complete committed-event
/// validation. It is not serializable or constructible from a client payload.
#[derive(Debug, Clone)]
pub struct AuthorizedRequestedCancellation {
    source: AuthenticatedRequestedCancellation,
    authorization: session::SessionDomainEvent,
}

#[derive(Debug)]
pub enum CancellationAuthorizationOutcome {
    Authorized(AuthorizedRequestedCancellation),
    RejectedStale {
        authorization_id: String,
    },
    Unknown {
        authorization_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationNotificationDisposition {
    Delivered,
    NotPresent,
    OperationalFirst,
}

/// Internal delivery result. NotActive is absence of the original local target,
/// not proof that an authorization committed or that an execution terminated.
#[derive(Debug)]
pub enum RequestedCancellationDeliveryOutcome {
    NotActive {
        cancellation_id: String,
    },
    Authorized {
        authorization: AuthorizedRequestedCancellation,
        notification: CancellationNotificationDisposition,
    },
    RejectedStale {
        authorization_id: String,
    },
    Unknown {
        authorization_id: String,
        reason: String,
    },
}

impl AuthorizedRequestedCancellation {
    pub fn receipt(&self) -> &harness_contract::turn::CancellationReceipt {
        self.source.receipt()
    }

    pub fn event_reference(&self) -> (&str, u64, u64, &str) {
        self.source.event_reference()
    }

    pub fn event_id(&self) -> &str {
        self.source.event_id()
    }
    pub fn authorization_id(&self) -> &str {
        &self.authorization.event_id
    }
    pub fn authorization_sequence(&self) -> usize {
        self.authorization.sequence
    }

    pub(crate) fn claim_event_append(
        &self,
        event: &crate::RuntimeSessionEvent,
        observation: &crate::provider_runtime_client::ProviderPhysicalObservation,
    ) -> Result<crate::session_runtime_port::RuntimeSessionClaimEventAppend, String> {
        self.source.claim_event_append(event, observation)
    }

    pub async fn verify_current(&self) -> Result<(), String> {
        let original = self
            .source
            .original_control
            .upgrade()
            .ok_or("original Requested cancellation control was released")?;
        let current = original
            .requested_source()
            .ok_or("original control has no published Requested cancellation")?;
        if current.authorization != self.authorization
            || current.source.event_id != self.source.event_id
            || current.event_reference() != self.event_reference()
            || current.receipt() != self.receipt()
        {
            return Err("Requested cancellation publication changed".into());
        }
        self.source
            .live_binding
            .as_ref()
            .ok_or("Requested cancellation has no original live claim binding")?
            .verify_current()
            .await
    }
}

/// Never serialized or formatted: contains the original live claim capability.
struct RequestedCancellationBinding {
    query: Arc<dyn crate::SessionRuntimeQueryPort>,
    fence: crate::SessionExecutionFence,
    ingress: crate::TurnIngressRef,
}

impl std::fmt::Debug for RequestedCancellationBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RequestedCancellationBinding { .. }")
    }
}

impl RequestedCancellationBinding {
    async fn verify_current(&self) -> Result<(), String> {
        let reject = || "original Requested cancellation claim is no longer current".to_string();
        let snapshot = self
            .fence
            .verify(crate::SessionExecutionFencePhase::ProviderRequest)
            .await?;
        let ingress = &self.ingress;
        if snapshot.claim_fence_epoch != ingress.claim_revision {
            return Err(reject());
        }
        let current = self
            .query
            .runtime_input(&ingress.request_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(reject)?;
        let admission = self
            .query
            .input_admission(&ingress.session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(reject)?;
        if current.request_id != ingress.request_id
            || current.turn_id != ingress.turn_id
            || current.message_id != ingress.message_id
            || current.claim_fence_epoch != Some(ingress.claim_revision)
            || current.claim_owner.as_deref() != Some(ingress.claim_owner.as_str())
            || current.claim_token.as_deref() != Some(ingress.claim_token.as_str())
            || current.session_id != ingress.session_id
            || current.session_generation != ingress.session_generation
            || current.sequence as u64 != ingress.input_sequence
            || !matches!(
                current.status,
                crate::RuntimeSessionInputStatus::Claimed
                    | crate::RuntimeSessionInputStatus::Running
            )
            || !current
                .claim_expires_at_ms
                .is_some_and(|deadline| deadline > now_ms())
            || admission.session_id != ingress.session_id
            || admission.generation != ingress.session_generation
            || !admission.open
        {
            return Err(reject());
        }
        Ok(())
    }
}

impl AuthenticatedRequestedCancellation {
    fn authorization_request(
        &self,
    ) -> Result<
        crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
        RuntimeServicesError,
    > {
        let binding = self.live_binding.as_ref().ok_or_else(|| {
            RuntimeServicesError::Invariant("Requested source has no frozen original claim".into())
        })?;
        let ingress = &binding.ingress;
        let owner = session::SessionClaimEventAppend::cancellation_authorization(
            ingress.session_id.clone(),
            ingress.message_id.clone(),
            ingress.turn_id.clone(),
            session::SessionTerminalExecutionFence {
                request_id: ingress.request_id.clone(),
                input_sequence: usize::try_from(ingress.input_sequence).map_err(|_| {
                    RuntimeServicesError::Invariant(
                        "claim input sequence is not representable".into(),
                    )
                })?,
                session_generation: ingress.session_generation,
                claim_owner: ingress.claim_owner.clone(),
                claim_token: ingress.claim_token.clone(),
                claim_fence_epoch: ingress.claim_revision,
            },
            self.receipt.clone(),
            session::SessionCancellationRequestedReference {
                event_id: self.event_id.clone(),
                stream_id: self.stream_id.clone(),
                event_sequence: self.sequence,
                commit_cursor: self.commit_cursor,
                payload_digest: self.digest.clone(),
            },
        )
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest::new(owner))
    }

    fn bind_authorization(
        self,
        request: &crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
        committed: session::SessionDomainEvent,
    ) -> Result<AuthorizedRequestedCancellation, RuntimeServicesError> {
        let expected = &request.owner_request().event;
        let actual_row = committed.to_session_event().map_err(|error| {
            RuntimeServicesError::Invariant(format!("invalid Session authorization event: {error}"))
        })?;
        let expected_row = expected.to_session_event().map_err(|error| {
            RuntimeServicesError::Invariant(format!("invalid authorization candidate: {error}"))
        })?;
        let matches =
            session::SessionDomainEvent::semantically_equivalent(&actual_row, &expected_row)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        if !matches {
            return Err(RuntimeServicesError::Invariant(
                "committed Session authorization conflicts with its frozen candidate".into(),
            ));
        }
        Ok(AuthorizedRequestedCancellation {
            source: self,
            authorization: committed,
        })
    }

    /// Build the owner transaction request from this opaque authenticated source.
    /// The serialized observation alone cannot supply its claim capability.
    fn claim_event_append(
        &self,
        event: &crate::RuntimeSessionEvent,
        observation: &crate::provider_runtime_client::ProviderPhysicalObservation,
    ) -> Result<crate::session_runtime_port::RuntimeSessionClaimEventAppend, String> {
        use crate::provider_runtime_client::ProviderPhysicalObservation;
        let ProviderPhysicalObservation::CancellationObserved {
            identity,
            requested,
            ..
        } = observation
        else {
            return Err("only cancellation observations consume a Requested claim".into());
        };
        let binding = self
            .live_binding
            .as_ref()
            .ok_or("Requested cancellation has no original live claim binding")?;
        let ingress = &binding.ingress;
        let (stream, sequence, cursor, digest) = self.event_reference();
        if identity.session_id != self.receipt.session_id
            || identity.execution_id != self.receipt.execution_id
            || identity.turn_id != self.receipt.turn_id
            || ingress.session_id != identity.session_id
            || ingress.turn_id != identity.turn_id
            || event.session_id != identity.session_id
            || event.kind != crate::RuntimeSessionEventKind::ProviderPhysicalObservation
            || event.status.as_deref() != Some("cancel_observed")
            || event.occurred_at_ms != observation.occurred_at_ms()
            || event.payload
                != serde_json::to_value(observation).map_err(|error| error.to_string())?
            || requested.cancellation_id != self.receipt.cancellation_id
            || requested.stream_id != stream
            || requested.event_sequence != sequence
            || requested.commit_cursor != cursor
            || requested.payload_digest != digest
        {
            return Err("physical observation does not match its original Requested source".into());
        }
        let mut domain = crate::session_runtime_port::to_session_event(event);
        domain.event_id = format!(
            "provider-physical:{}",
            observation.digest().map_err(|error| error.to_string())?
        );
        Ok(
            crate::session_runtime_port::RuntimeSessionClaimEventAppend::new(
                ingress.session_id.clone(),
                ingress.message_id.clone(),
                ingress.turn_id.clone(),
                session::SessionTerminalExecutionFence {
                    request_id: ingress.request_id.clone(),
                    input_sequence: usize::try_from(ingress.input_sequence)
                        .map_err(|_| "claim input sequence is not representable")?,
                    session_generation: ingress.session_generation,
                    claim_owner: ingress.claim_owner.clone(),
                    claim_token: ingress.claim_token.clone(),
                    claim_fence_epoch: ingress.claim_revision,
                },
                domain,
            ),
        )
    }

    pub fn receipt(&self) -> &harness_contract::turn::CancellationReceipt {
        &self.receipt
    }

    pub fn event_reference(&self) -> (&str, u64, u64, &str) {
        (
            &self.stream_id,
            self.sequence,
            self.commit_cursor,
            &self.digest,
        )
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }
}

impl std::fmt::Debug for TurnCancellationControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnCancellationControl")
            .field("session_id", &self.session_id)
            .field("turn_id", &self.turn_id)
            .field("execution_id", &self.execution_id)
            .finish_non_exhaustive()
    }
}

impl TurnCancellationControl {
    /// Unbound helpers can stop work, but can never carry Requested evidence.
    pub fn operational(session_id: &str, turn_id: &str, execution_id: &str) -> Arc<Self> {
        Self::operational_with_token(session_id, turn_id, execution_id, crate::CancellationToken::new())
    }

    /// Bind an unprivileged delegated turn to its existing physical run owner.
    /// This control cannot authorize a Requested cancellation without ingress.
    pub(crate) fn operational_with_token(
        session_id: &str,
        turn_id: &str,
        execution_id: &str,
        token: crate::CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id: session_id.into(),
            turn_id: turn_id.into(),
            execution_id: execution_id.into(),
            ingress: None,
            token,
            first_cause: Mutex::new(TurnCancellationCause::None),
            step_wakeup: tokio::sync::Notify::new(),
        })
    }

    pub fn token(&self) -> crate::CancellationToken {
        self.token.clone()
    }

    pub fn matches_target(&self, session_id: &str, turn_id: &str, execution_id: &str) -> bool {
        self.session_id == session_id
            && self.turn_id == turn_id
            && self.execution_id == execution_id
    }

    pub fn requested_source(&self) -> Option<AuthorizedRequestedCancellation> {
        match &*self
            .first_cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            TurnCancellationCause::AuthorizedRequested(source) => Some(source.clone()),
            _ => None,
        }
    }

    pub fn signal_operational(&self) {
        #[cfg(feature = "test-fixtures")]
        {
            // A turn-scoped operational cancellation can be triggered by work
            // that does not own this turn.  Record who signalled it so the
            // captured child trace names the escalating scope instead of
            // leaving "the turn was cancelled" as an unexplained fact.
            let site = format!(
                "site={} session={} turn={} execution={}",
                std::panic::Location::caller(),
                self.session_id,
                self.turn_id,
                self.execution_id
            );
            eprintln!("k05_operational_cancel {site}");
            crate::execution_core::services::record_last_operational_cancel(&site);
        }
        let mut cause = self
            .first_cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*cause, TurnCancellationCause::None) {
            *cause = TurnCancellationCause::Operational;
        }
        self.token.cancel();
    }

    /// Turn-level cancellation state lives here and only here: every instance
    /// that holds this Arc answers the same question, so no caller may depend
    /// on which runtime instance it happens to hold.
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// A provider step stopped. This wakes step-level waiters through the
    /// shared home; it is NOT a turn cancellation and never sets first_cause.
    pub fn notify_step_stop(&self) {
        self.step_wakeup.notify_waiters();
    }

    /// Wait until either this turn is cancelled or a provider step stopped.
    pub async fn cancelled_or_step_stop(&self) {
        tokio::select! {
            () = self.token.cancelled() => {}
            () = self.step_wakeup.notified() => {}
        }
    }
}

#[cfg(test)]
#[path = "tests/services.rs"]
mod tests;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartupRecoveryReport {
    pub examined_graphs: usize,
    /// Graphs deliberately left untouched because their owning Session did
    /// not finish hydration in this restoration pass.
    #[serde(default)]
    pub deferred_graphs: usize,
    pub recovered_graphs: usize,
    pub notified_graphs: usize,
    pub advanced_graphs: usize,
    pub terminal_graphs: usize,
    pub waiting_graphs: usize,
    pub blocked_graphs: usize,
    pub resolved_handoff_results: usize,
    pub errors: Vec<ExecutionStartupRecoveryError>,
    pub records: Vec<ExecutionStartupRecoveryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartupRecoveryRecord {
    pub graph_id: String,
    pub objective: String,
    pub before_revision: u64,
    pub after_revision: u64,
    pub before_status: String,
    pub after_status: String,
    pub action: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartupRecoveryError {
    pub graph_id: String,
    pub error: String,
}

pub struct RuntimeServicesBuilder {
    cowd_home: PathBuf,
    workspace_root: PathBuf,
    runtime_build_identity: harness_contract::outcome::RuntimeBuildIdentity,
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    task_aggregate_service: Option<Arc<crate::TaskAggregateService>>,
    builtin_definitions_root: Option<PathBuf>,
    resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
    provider_resource_config: crate::ProviderResourceConfig,
    provider_registry: Arc<crate::ProviderRegistry>,
    provider_fallbacks: Vec<String>,
    provider_transport_pool: Arc<crate::ProviderTransportPool>,
    provider_template_cache: Arc<crate::ProviderClientTemplateCache>,
    process_jsonl_commands: Vec<ProcessJsonlSpec>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    session_query_port: Option<Arc<dyn crate::SessionRuntimeQueryPort>>,
    session_ingress_port: Option<Arc<dyn crate::SessionRuntimeIngressPort>>,
    session_journal_port: Option<Arc<dyn crate::SessionRuntimeJournalPort>>,
    session_application_port: Option<Arc<dyn crate::SessionRuntimeApplicationPort>>,
    artifact_store: Option<Arc<crate::ArtifactStore>>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    reality_recall_port: Option<Arc<RealityRecallPort>>,
    knowledge_activation: Option<crate::knowledge_activation::KnowledgeActivationRuntime>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    background_work: crate::BackgroundWorkConfig,
    skill_catalog: crate::RuntimeSkillCatalog,
    mission_schedule_policy: crate::MissionSchedulePolicy,
    hot_state_config: crate::execution_core::hot_state::HotStateConfig,
    approval_config: ApprovalConfig,
    collaboration_capacity: crate::CollaborationCapacityPolicy,
    collaboration_max_parallel_agents: usize,
    projection_lanes: Vec<crate::RuntimeProjectionLane>,
}

/// Runtime-owned supervisor for non-critical-path maintenance.
///
/// Tasks are serialized per logical owner, retained until completion, and
/// drained explicitly during process shutdown. This keeps post-turn work off
/// the response path without creating detached tasks.
type MaintenanceWork = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

// Maintenance is intentionally off the response-critical path, but it still
// needs a hard memory bound. Backpressure preserves every completed turn's
// work instead of dropping or coalescing semantically distinct memory updates.
const MAX_MAINTENANCE_OWNERS: usize = 1_024;
const MAX_QUEUED_MAINTENANCE_PER_OWNER: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaintenanceLifecycle {
    Open,
    Closing,
    Closed,
}

struct MaintenanceOwner {
    generation: u64,
    queued: VecDeque<MaintenanceWork>,
    handle: tokio::task::JoinHandle<()>,
}

struct MaintenanceState {
    lifecycle: MaintenanceLifecycle,
    next_generation: u64,
    owners: HashMap<String, MaintenanceOwner>,
    reaping: HashMap<String, usize>,
}

struct MaintenanceCompletion {
    owner_key: String,
    owner: MaintenanceOwner,
}

pub(crate) struct RuntimeMaintenanceSupervisor {
    state: Arc<Mutex<MaintenanceState>>,
    changed: Arc<tokio::sync::Notify>,
    completion_tx: tokio::sync::mpsc::Sender<MaintenanceCompletion>,
    completion_rx: Mutex<Option<tokio::sync::mpsc::Receiver<MaintenanceCompletion>>>,
    reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    reaper_cancellation: crate::CancellationToken,
    shutdown_lock: tokio::sync::Mutex<()>,
    shutdown_timeout: Duration,
}

/// Per-Runtime scheduling control for the original self-retirement oracle.
/// This never supplies an action, exit, receipt or binding closure.
#[cfg(test)]
pub(crate) struct RetirementOwnerTestGate {
    pub(crate) packet: harness_contract::agent::AgentTaskPacket,
    pub(crate) action_id: String,
    pub(crate) semantic_committed: std::sync::atomic::AtomicBool,
    pub(crate) postcommit_returned: std::sync::atomic::AtomicBool,
    pub(crate) cancel_hits: std::sync::atomic::AtomicUsize,
    cancel_released: std::sync::atomic::AtomicBool,
    response_released: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}

#[cfg(test)]
impl RetirementOwnerTestGate {
    pub(crate) fn new(
        packet: harness_contract::agent::AgentTaskPacket,
        action_id: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            packet,
            action_id,
            semantic_committed: false.into(),
            postcommit_returned: false.into(),
            cancel_hits: 0.into(),
            cancel_released: false.into(),
            response_released: false.into(),
            changed: tokio::sync::Notify::new(),
        })
    }

    pub(crate) async fn until(&self, predicate: impl Fn() -> bool) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if predicate() {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn release_response(&self) {
        self.response_released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub(crate) fn release_cancel(&self) {
        self.cancel_released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceSubmitError {
    Full,
    Closing,
}

impl RuntimeMaintenanceSupervisor {
    fn new() -> Self {
        Self::with_shutdown_timeout(Duration::from_secs(10))
    }

    fn with_shutdown_timeout(shutdown_timeout: Duration) -> Self {
        let (completion_tx, completion_rx) = tokio::sync::mpsc::channel(MAX_MAINTENANCE_OWNERS);
        Self {
            state: Arc::new(Mutex::new(MaintenanceState {
                lifecycle: MaintenanceLifecycle::Open,
                next_generation: 0,
                owners: HashMap::new(),
                reaping: HashMap::new(),
            })),
            changed: Arc::new(tokio::sync::Notify::new()),
            completion_tx,
            completion_rx: Mutex::new(Some(completion_rx)),
            reaper: Mutex::new(None),
            reaper_cancellation: crate::CancellationToken::new(),
            shutdown_lock: tokio::sync::Mutex::new(()),
            shutdown_timeout,
        }
    }

    /// Non-waiting admission into the same retained worker/reaper as `submit`.
    /// The future is returned internally on Full so the waiting API can retry;
    /// callers of try_submit drop it (and its original in-flight token).
    pub(crate) fn try_submit<F>(&self, owner: String, work: F) -> Result<(), MaintenanceSubmitError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.try_submit_work(owner, Box::pin(work))
            .map_err(|(error, _work)| error)
    }

    pub(crate) async fn submit<F>(&self, owner: String, work: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut work: MaintenanceWork = Box::pin(work);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_submit_work(owner.clone(), work) {
                Ok(()) => return true,
                Err((MaintenanceSubmitError::Closing, _)) => return false,
                Err((MaintenanceSubmitError::Full, returned)) => {
                    work = returned;
                    changed.await;
                }
            }
        }
    }

    fn try_submit_work(
        &self,
        owner: String,
        work: MaintenanceWork,
    ) -> Result<(), (MaintenanceSubmitError, MaintenanceWork)> {
        if !self.ensure_reaper() {
            return Err((MaintenanceSubmitError::Closing, work));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.lifecycle != MaintenanceLifecycle::Open {
            return Err((MaintenanceSubmitError::Closing, work));
        }
        if let Some(existing) = state.owners.get_mut(&owner) {
            if existing.queued.len() >= MAX_QUEUED_MAINTENANCE_PER_OWNER {
                return Err((MaintenanceSubmitError::Full, work));
            }
            existing.queued.push_back(work);
            return Ok(());
        }
        if state.owners.len() >= MAX_MAINTENANCE_OWNERS {
            return Err((MaintenanceSubmitError::Full, work));
        }
        state.next_generation = state.next_generation.saturating_add(1);
        let generation = state.next_generation;
        let worker_state = Arc::downgrade(&self.state);
        let worker_changed = Arc::clone(&self.changed);
        let completion_tx = self.completion_tx.clone();
        let worker_owner = owner.clone();
        let initial_work = work;
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return;
            }
            let mut current = initial_work;
            loop {
                let _ = std::panic::AssertUnwindSafe(current).catch_unwind().await;
                let Some(state) = worker_state.upgrade() else {
                    return;
                };
                let (next, completed) = {
                    let mut state = state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = state.owners.get_mut(&worker_owner) else {
                        return;
                    };
                    if entry.generation != generation {
                        return;
                    }
                    if let Some(next) = entry.queued.pop_front() {
                        (Some(next), None)
                    } else {
                        let Some(completed) = state.owners.remove(&worker_owner) else {
                            return;
                        };
                        let count = state.reaping.entry(worker_owner.clone()).or_default();
                        *count = count.saturating_add(1);
                        (None, Some(completed))
                    }
                };
                worker_changed.notify_waiters();
                if let Some(completed) = completed {
                    if completion_tx
                        .send(MaintenanceCompletion {
                            owner_key: worker_owner.clone(),
                            owner: completed,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!(
                            owner = %worker_owner,
                            "maintenance completion reaper is closed"
                        );
                    }
                    return;
                }
                match next {
                    Some(next) => current = next,
                    None => return,
                }
            }
        });
        state.owners.insert(
            owner.clone(),
            MaintenanceOwner {
                generation,
                queued: VecDeque::new(),
                handle,
            },
        );
        let _ = start_tx.send(());
        Ok(())
    }

    fn ensure_reaper(&self) -> bool {
        let mut reaper = self
            .reaper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reaper.is_some() {
            return true;
        }
        let Some(mut receiver) = self
            .completion_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return false;
        };
        let state = Arc::downgrade(&self.state);
        let changed = Arc::clone(&self.changed);
        let cancellation = self.reaper_cancellation.clone();
        *reaper = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    completion = receiver.recv() => {
                        let Some(mut completion) = completion else {
                            break;
                        };
                        let _ = (&mut completion.owner.handle).await;
                        let Some(state) = state.upgrade() else {
                            break;
                        };
                        let mut state = state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if let Some(count) = state.reaping.get_mut(&completion.owner_key) {
                            *count = count.saturating_sub(1);
                            if *count == 0 { state.reaping.remove(&completion.owner_key); }
                        }
                        drop(state);
                        changed.notify_waiters();
                    }
                }
            }
        }));
        true
    }

    fn pending_for_owner_prefix(&self, prefix: &str) -> usize {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let running = state
            .owners
            .iter()
            .filter(|(owner, _)| owner.starts_with(prefix))
            .map(|(_, owner)| 1usize.saturating_add(owner.queued.len()))
            .sum::<usize>();
        let reaping = state
            .reaping
            .iter()
            .filter(|(owner, _)| owner.starts_with(prefix))
            .map(|(_, count)| *count)
            .sum::<usize>();
        running.saturating_add(reaping)
    }

    /// Observe physical joins for this domain in the existing reaper. Other
    /// domains (including Memory/Skill maintenance) retain their own lifecycle.
    async fn drain_owner_prefix(
        &self,
        prefix: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.pending_for_owner_prefix(prefix) == 0 {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| format!("maintenance domain `{prefix}` is still draining"))?;
        }
    }

    async fn shutdown_and_drain(&self) {
        let _shutdown = self.shutdown_lock.lock().await;
        let wait_for_existing_shutdown = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match state.lifecycle {
                MaintenanceLifecycle::Open => {
                    state.lifecycle = MaintenanceLifecycle::Closing;
                    false
                }
                MaintenanceLifecycle::Closing => true,
                MaintenanceLifecycle::Closed => return,
            }
        };
        // Wake submissions currently applying bounded-queue backpressure so
        // they observe Closing and return instead of waiting through shutdown.
        self.changed.notify_waiters();

        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        loop {
            let notified = self.changed.notified();
            let lifecycle = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.owners.is_empty() && state.reaping.is_empty() {
                    Some(state.lifecycle)
                } else {
                    None
                }
            };
            if matches!(lifecycle, Some(MaintenanceLifecycle::Closed)) {
                return;
            }
            if lifecycle.is_some() {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.lifecycle = MaintenanceLifecycle::Closed;
                self.changed.notify_waiters();
                break;
            }
            if wait_for_existing_shutdown && tokio::time::Instant::now() >= deadline {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }

        let owners = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.lifecycle = MaintenanceLifecycle::Closed;
            std::mem::take(&mut state.owners)
        };
        for (_, owner) in owners {
            owner.handle.abort();
            let _ = owner.handle.await;
        }
        self.reaper_cancellation.cancel();
        let reaper = self
            .reaper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut reaper) = reaper {
            if tokio::time::timeout(self.shutdown_timeout, &mut reaper)
                .await
                .is_err()
            {
                reaper.abort();
                let _ = reaper.await;
            }
        }
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    fn tracked_task_count(&self) -> usize {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .owners
            .len()
            .saturating_add(state.reaping.values().copied().sum::<usize>())
    }
}

/// Read-only projection port for the durable runtime ledger.
///
/// Gateway and surfaces receive this port rather than the SQLite-backed event
/// store.  Keeping the store private prevents a projection consumer from
/// silently becoming another lifecycle event writer.
#[derive(Clone)]
pub struct RuntimeEventReader {
    store: Arc<RuntimeEventStore>,
}

/// One bounded page of complete, authenticated Session lifecycle events.
pub struct RuntimeTimelinePage {
    pub events: Vec<DurableRuntimeEvent>,
    pub has_more: bool,
    /// Integer-cursor consumers may only advance through complete commits.
    /// Tuple-position consumers can also consume a partial commit's events.
    pub complete_through_cursor: u64,
}

impl RuntimeEventReader {
    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.store.subscribe_commits()
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> Result<Vec<crate::CommittedEventBatch>, String> {
        self.store
            .events_after_cursor(cursor, max_commits)
            .map_err(|error| error.to_string())
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.store.list_stream(stream_id)
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.store.list_scope(scope, limit)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.store.all_events(limit)
    }

    pub fn replay_report(&self, limit: usize) -> Result<crate::RuntimeReplayReport, String> {
        RuntimeEventReplayer::report(&self.store, limit)
    }

    pub fn session_timeline_events(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<RuntimeTimelinePage, String> {
        let head = self
            .store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        const PAGE_BYTES: usize = 1024 * 1024;
        let events = self.store.position_page(
            RuntimeEventScope::Session,
            session_id,
            None,
            after_position,
            head,
            limit.min(100),
            PAGE_BYTES,
        )?;
        let stored_bytes = events.iter().try_fold(0usize, |total, event| {
            serde_json::to_vec(event)
                .map_err(|error| error.to_string())
                .and_then(|bytes| {
                    total
                        .checked_add(bytes.len())
                        .ok_or_else(|| "timeline byte budget overflow".to_string())
                })
        })?;
        let mut total = 0usize;
        let mut resolved = Vec::new();
        let mut next_position = None;
        for event in events {
            let metadata = serde_json::to_vec(&event)
                .map_err(|error| error.to_string())?
                .len();
            let envelope = if event.scope == RuntimeEventScope::ExecutionGraph {
                Some(&event.payload)
            } else if event.kind == "goal.observation" {
                Some(&event.payload["observation_body"])
            } else {
                None
            };
            // Metadata already contains inline content and the graph binding.
            // Reserve external content before its authenticated resolver does IO.
            let external = envelope
                .filter(|value| value["body"]["kind"] == "artifact_ref")
                .map(|value| {
                    value["bytes"]
                        .as_u64()
                        .ok_or("timeline body extent missing")
                        .and_then(|bytes| {
                            usize::try_from(bytes).map_err(|_| "timeline body extent overflow")
                        })
                })
                .transpose()?
                .unwrap_or(0);
            let extent = metadata
                .checked_add(external)
                .ok_or("timeline byte budget overflow")?;
            if !resolved.is_empty() && extent > PAGE_BYTES.saturating_sub(total) {
                next_position = Some((event.commit_cursor, event.transaction_index));
                break;
            }
            // A single record still has to fit. Resolve it once to measure its
            // exact public representation; envelope overhead is only a bound.
            // One oversized record must not fail the whole page: a record whose
            // content is already addressed by a durable reference stays in that
            // reference form and its body can be fetched on demand.
            let event = self
                .store
                .resolve_read_record_within(event, crate::RuntimeEventStore::MAX_HYDRATED_CONTENT_BYTES)?;
            let bytes = serde_json::to_vec(&event)
                .map_err(|error| error.to_string())?
                .len();
            total = total
                .checked_add(bytes)
                .ok_or("timeline byte budget overflow")?;
            if total > PAGE_BYTES {
                return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                    "timeline resolved content".into(),
                )
                .to_string());
            }
            resolved.push(event);
        }
        let mut has_more = next_position.is_some();
        if !has_more && !resolved.is_empty() && limit > 0 {
            let last = resolved.last().expect("nonempty page");
            let remaining = PAGE_BYTES.saturating_sub(stored_bytes);
            if remaining == 0 {
                has_more = true;
            } else {
                match self.store.position_page(
                    RuntimeEventScope::Session,
                    session_id,
                    None,
                    Some((last.commit_cursor, last.transaction_index)),
                    head,
                    1,
                    remaining,
                ) {
                    Ok(next) => {
                        next_position = next
                            .first()
                            .map(|event| (event.commit_cursor, event.transaction_index));
                        has_more = next_position.is_some();
                    }
                    Err(error) if error.starts_with("HistoryBudgetExceeded:") => has_more = true,
                    Err(error) => return Err(error),
                }
            }
        }
        let complete_through_cursor = match resolved.last() {
            Some(last) if has_more && next_position.is_none_or(|next| next.0 == last.commit_cursor) => {
                last.commit_cursor.saturating_sub(1)
            }
            Some(last) => last.commit_cursor,
            None if limit == 0 => after_position.map_or(0, |position| position.0),
            None => head,
        };
        Ok(RuntimeTimelinePage {
            events: resolved,
            has_more,
            complete_through_cursor,
        })
    }
}

/// Fixture-only ledger writer used by downstream unit tests to seed durable
/// historical state. Production Gateway paths never receive this capability.
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
#[derive(Clone)]
pub struct RuntimeFixtureEventPort {
    store: Arc<RuntimeEventStore>,
}

#[cfg(feature = "test-fixtures")]
impl RuntimeFixtureEventPort {
    pub fn append_for_test(&self, event: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        self.store.append(event)
    }
}

/// Narrow delivery port for the Gateway-owned surface delivery worker.
///
/// It deliberately exposes only the terminal-outbox state machine.  It cannot
/// append arbitrary runtime events or access graph/approval transactions.
#[derive(Clone)]
pub struct SessionTerminalDeliveryPort {
    store: Arc<RuntimeEventStore>,
}

impl SessionTerminalDeliveryPort {
    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.store.subscribe_commits()
    }

    pub fn enqueue(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.enqueue_session_terminal(
            terminal_id,
            message_id,
            session_id,
            commit_cursor,
            payload_ref,
        )
    }

    pub fn get(
        &self,
        terminal_id: &str,
    ) -> Result<Option<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store.session_terminal(terminal_id)
    }

    pub fn for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> Result<Option<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store
            .session_terminal_for_execution(session_id, execution_id, turn_id)
    }

    pub fn claim(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store
            .claim_session_terminals(worker_id, now_ms, lease_ms, limit)
    }

    pub fn acknowledge(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store
            .ack_session_terminal(terminal_id, worker_id, expected_revision, now_ms)
    }

    pub fn suppress(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.suppress_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            reason,
            now_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fail(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.fail_session_terminal(
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

    pub fn health(&self) -> Result<RuntimeSessionOutboxHealth, RuntimeEventStoreError> {
        self.store.session_terminal_health()
    }

    pub fn has_unsettled_for_session(
        &self,
        session_id: &str,
    ) -> Result<bool, RuntimeEventStoreError> {
        self.store.has_unsettled_session_terminals(session_id)
    }

    pub fn blocked(
        &self,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store.blocked_session_terminals(limit)
    }

    pub fn materialized_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store
            .materialized_session_terminals_after(session_id, after_commit_cursor, limit)
    }

    pub fn retry_blocked(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store
            .retry_session_terminal(terminal_id, actor, reason, now_ms)
    }

    pub fn adopt_fence(
        &self,
        request: &crate::RuntimeSessionTerminalFenceAdoption,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.adopt_session_terminal_fence(request)
    }
}

pub struct RuntimeServices {
    workspace_root: PathBuf,
    workspace_key: String,
    path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    event_store: Arc<RuntimeEventStore>,
    live_execution_store: Arc<crate::execution_live::ExecutionLiveStore>,
    hot_state: Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>,
    executor_registry: Arc<NodeExecutorRegistry>,
    model_step_executor: Arc<ScopedNodeExecutor>,
    tool_batch_executor: Arc<ScopedNodeExecutor>,
    cross_plane_connector_executor: Arc<ScopedNodeExecutor>,
    agent_task_executor: Arc<AgentTaskExecutor>,
    agent_runtime: Arc<AgentRuntime>,
    agentic_actions: crate::AgentActionService,
    l4_promotion_service: Arc<crate::L4PromotionService>,
    knowledge_candidate_projector: Arc<crate::KnowledgeCandidateProjector>,
    outcome_service: Arc<crate::execution_core::OutcomeService>,
    outcome_projector: Arc<crate::OutcomeProjector>,
    verify_executor: Arc<VerifyNodeExecutor>,
    synthesize_executor: Arc<SynthesizeNodeExecutor>,
    graph_state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    execution_supervisor: Arc<crate::RuntimeExecutionSupervisor>,
    approval_queue: Arc<ApprovalQueue>,
    approval_coordinator: Arc<ApprovalCoordinator>,
    execution_capacity_profile: Arc<crate::ExecutionCapacityProfile>,
    evolution_governance: Arc<crate::EvolutionGovernanceService>,
    evolution_discovery: Arc<crate::evolution::EvolutionDiscoveryService>,
    evolution_analyst: RwLock<Option<Arc<crate::evolution::analyst::EvolutionAnalystService>>>,
    background_work: Mutex<crate::BackgroundWorkConfig>,
    evolution_signal_projector: Arc<crate::evolution::EvolutionSignalProjector>,
    skill_maintenance_projector: Arc<crate::SkillMaintenanceProjector>,
    event_reactor: Arc<crate::RuntimeEventReactor>,
    skill_revision_governance: Arc<crate::SkillRevisionGovernanceService>,
    // One lazy Runtime-owned worker, never one worker per in-memory Runtime.
    skill_usage_recorder: std::sync::Mutex<(bool, Option<Arc<crate::RuntimeSkillUsageRecorder>>)>,
    mission_evidence: Arc<MissionEvidenceBus>,
    conflict_resolver: Arc<ConflictArbiter>,
    resource_manager: Arc<ExecutionResourceManager>,
    tool_execution_plane: Arc<crate::ToolExecutionPlane>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    definition_registry: Arc<RuntimeDefinitionRegistry>,
    cross_plane: Arc<CrossPlaneRuntimeService>,
    mission_runtime: Arc<MissionRuntime>,
    task_aggregate_service: Arc<crate::TaskAggregateService>,
    mission_schedules: Arc<MissionScheduleStore>,
    managed_agents: Arc<crate::ManagedAgentDispatcher>,
    mission_schedule_policy: Arc<RwLock<crate::MissionSchedulePolicy>>,
    session_relations: Arc<SessionRelationGraph>,
    goal_store: Arc<GoalStore>,
    objective_supervisor: Arc<ObjectiveSupervisor>,
    provider_registry: Arc<crate::ProviderRegistry>,
    provider_resource_config: Arc<RwLock<crate::ProviderResourceConfig>>,
    provider_fallbacks: Arc<RwLock<Vec<String>>>,
    provider_transport_pool: Arc<crate::ProviderTransportPool>,
    provider_template_cache: Arc<crate::ProviderClientTemplateCache>,
    evaluation_provider_token_leases:
        Arc<crate::conversation::EvaluationProviderTokenLeaseRegistry>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    artifact_store: Arc<crate::ArtifactStore>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    evolution_eval_runner: RwLock<Option<Arc<dyn crate::EvolutionEvalRunner>>>,
    evolution_evaluation_flights: Arc<Mutex<BTreeSet<String>>>,
    /// Runtime-instance single-flight for Agent-first physical dispatch. The
    /// durable task claim remains the cross-process owner; this local fence
    /// only suppresses duplicate reconcilers inside this service graph.
    agentic_dispatch_flights: Arc<Mutex<BTreeSet<String>>>,
    agentic_read_model: Arc<crate::agentic::AgenticReadModel>,
    skill_catalog: Arc<RwLock<crate::RuntimeSkillCatalog>>,
    reality_recall_port: Arc<RealityRecallPort>,
    knowledge_activation: crate::knowledge_activation::KnowledgeActivationRuntime,
    session_dispatch_executor: Arc<crate::session_execution::SessionDispatchNodeExecutor>,
    session_input_router: OnceLock<Arc<SessionInputRouter>>,
    session_query_port: Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>,
    session_ingress_port: OnceLock<Arc<dyn crate::SessionRuntimeIngressPort>>,
    session_journal_port: OnceLock<Arc<dyn crate::SessionRuntimeJournalPort>>,
    session_application_port: OnceLock<Arc<dyn crate::SessionRuntimeApplicationPort>>,
    active_execution_buses: Arc<Mutex<BTreeMap<String, ActiveExecutionBus>>>,
    /// The original Session owner parts per Session, retained after the root
    /// execution bus lease is retired. This index holds the very claim and
    /// control that `install_session_fence` took from the original ingress;
    /// it is not a second authority source and it never mints an owner.
    original_session_owners: Arc<Mutex<BTreeMap<String, OriginalSessionOwnerParts>>>,
    session_execution_policy_controls:
        Arc<RwLock<BTreeMap<String, crate::permissions::SessionExecutionPolicyControl>>>,
    session_execution_policy_admission_blocks: Arc<RwLock<BTreeMap<String, String>>>,
    maintenance_supervisor: Arc<RuntimeMaintenanceSupervisor>,
    #[cfg(test)]
    retirement_owner_test_gate: Mutex<Option<Arc<RetirementOwnerTestGate>>>,
    resource_evidence_writer: Arc<super::evidence_writer::ResourceEvidenceWriter>,
    execution_projection_cache: Mutex<crate::execution_projection::ExecutionProjectionCache>,
    // Keep this field last so filesystem-backed components are dropped before
    // the temporary root removes their files.
    _ephemeral_root: Option<tempfile::TempDir>,
}

#[derive(Clone)]
struct ActiveExecutionBus {
    generation: u64,
    bus: crate::CowdEventBus,
    session_fence: Option<ActiveSessionFence>,
}

/// Original ingress-derived Session identity, created only by the active
/// root execution lease after checking its original turn control.
pub(crate) struct OriginalSessionClaim { ingress: crate::TurnIngressRef }
impl std::fmt::Debug for OriginalSessionClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OriginalSessionClaim { .. }")
    }
}
impl OriginalSessionClaim {
    /// The claim of one original Session turn, for an isolated fixture.
    ///
    /// A claim is otherwise installed by the ingress that owns the turn. A
    /// fixture that drives the original member flow needs the same claim
    /// shape, and it must not rebuild the turn's own lineage.
    #[cfg(test)]
    pub(crate) fn for_isolated_turn(ingress: crate::TurnIngressRef) -> Arc<OriginalSessionClaim> {
        Arc::new(OriginalSessionClaim { ingress })
    }
    pub(crate) fn session_id(&self) -> &str { &self.ingress.session_id }
    pub(crate) fn message_id(&self) -> &str { &self.ingress.message_id }
    pub(crate) fn turn_id(&self) -> &str { &self.ingress.turn_id }
    pub(crate) fn request_id(&self) -> &str { &self.ingress.request_id }
    /// The identity of one original Session claim: the Session, the turn and
    /// the ingress request that carried it.
    pub(crate) fn identity(&self) -> (&str, &str, &str) {
        (&self.ingress.session_id, &self.ingress.turn_id, &self.ingress.request_id)
    }
    pub(crate) fn fence(&self) -> Result<session::SessionTerminalExecutionFence, String> {
        Ok(session::SessionTerminalExecutionFence {
            request_id: self.ingress.request_id.clone(),
            input_sequence: usize::try_from(self.ingress.input_sequence).map_err(|_| "original input sequence overflow")?,
            session_generation: self.ingress.session_generation,
            claim_owner: self.ingress.claim_owner.clone(), claim_token: self.ingress.claim_token.clone(),
            claim_fence_epoch: self.ingress.claim_revision,
        })
    }
}


#[derive(Clone)]
pub(crate) struct ActiveSessionFence {
    session_id: String,
    turn_id: String,
    fence: crate::SessionExecutionFence,
    root_control: Arc<TurnCancellationControl>,
    claim: Arc<OriginalSessionClaim>,
    input: crate::SessionInputStream,
    control_requested: Arc<crate::conversation::OriginalTurnInputControl>,
    original_policy: crate::permissions::PermissionPolicy,
    original_policy_owner: Option<Arc<crate::conversation::SharedPolicyOwner>>,
}

impl ActiveSessionFence {
    pub(crate) fn for_lineage(&self, session_id: &str, turn_id: &str) -> Option<(crate::SessionExecutionFence, Arc<TurnCancellationControl>, Arc<OriginalSessionClaim>, crate::SessionInputStream, Arc<crate::conversation::OriginalTurnInputControl>)> {
        (self.session_id == session_id && self.turn_id == turn_id)
            .then(|| (self.fence.clone(), Arc::clone(&self.root_control), Arc::clone(&self.claim), self.input.clone(), Arc::clone(&self.control_requested)))
    }

    pub(crate) fn policy_owner_for_lineage(
        &self,
        session_id: &str,
        turn_id: &str,
        store: &Arc<crate::RuntimeEventStore>,
    ) -> Option<Arc<crate::conversation::SharedPolicyOwner>> {
        let owner = self.original_policy_owner.as_ref()?;
        (self.session_id == session_id && self.turn_id == turn_id
            && owner.matches_original_lineage(&self.claim, &self.original_policy.execution_policy_control(),
                &self.control_requested, store)).then(|| Arc::clone(owner))
    }
}

/// The original Session owner handles that one root ingress installed.
///
/// These are the very handles the original ingress took: the same durable
/// claim (with its claim token) and the same original turn control. Keeping
/// them reachable after the root execution bus lease is retired lets a member
/// that outlives the root graph recover the equivalent owner instead of
/// losing it. Nothing here is minted: a missing or cancelled original turn
/// simply has no owner to recover.
#[derive(Clone)]
struct OriginalSessionOwnerParts {
    fence: crate::SessionExecutionFence,
    root_control: Arc<TurnCancellationControl>,
    claim: Arc<OriginalSessionClaim>,
    input: crate::SessionInputStream,
    requested: Arc<crate::conversation::OriginalTurnInputControl>,
    policy: crate::permissions::PermissionPolicy,
}

/// Original active turn owners used by a Program conclusion. These handles
/// are taken from the active ingress registry, never reconstructed from a
/// Program projection. The commit control is registered with both original
/// cancellation paths and must be retained through the checked transaction.
pub(crate) struct OriginalConclusionOwner {
    pub(crate) claim: Arc<OriginalSessionClaim>,
    pub(crate) input: crate::SessionInputStream,
    pub(crate) control: Arc<crate::conversation::MemberCommitControl>,
    policy: crate::permissions::PermissionPolicy,
}

/// One Program action by the member that made it, reduced to what the
/// recovery predicate needs: whether it opens that member's attempt, closes
/// it, or neither.
struct MemberAttemptRecord {
    execution_id: String,
    /// Opens the member's attempt.
    dispatch: bool,
    /// Closes the member's attempt.
    settles: bool,
}

/// The durable Program journal of one Session turn, told apart by state.
enum SessionProgramJournal {
    /// The journal exists but could not be read.
    Unknown,
    /// This Session turn has no Program journal.
    NoProgram,
    /// The journal of this turn.
    Journal(Vec<crate::DurableRuntimeEvent>),
}

/// Original live Session owner carried by the Synthesize outcome. Keep only
/// its original store and policy-owner handles, not the Runtime service graph.
pub(crate) struct OriginalTerminalCommitOwner {
    store: std::sync::Weak<RuntimeEventStore>,
    policy_controls: Arc<RwLock<BTreeMap<String, crate::permissions::SessionExecutionPolicyControl>>>,
    admission_blocks: Arc<RwLock<BTreeMap<String, String>>>,
    /// Close the recovery window for one Session turn, if the members of that
    /// turn have all settled.
    ///
    /// The action is held as an opaque closure on purpose: this owner lives
    /// inside the runner's own type graph, and naming the Runtime services
    /// here would put the whole service graph into every proof about it. The
    /// action asks the same per-member fact the recovery gate asks, and it
    /// removes the retained owner under the same identity rule.
    release_after_terminal: Arc<dyn Fn(&str, &OriginalSessionClaim) -> bool + Send + Sync>,
    owner: OriginalConclusionOwner,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    terminal: Mutex<OriginalTerminalClosureState>,
}

#[derive(Default)]
struct OriginalTerminalClosureState {
    event: Option<crate::DurableRuntimeEvent>,
    released: bool,
}

impl OriginalTerminalCommitOwner {
    pub(crate) fn record_terminal(&self, ticket: &crate::execution_core::graph::NodeExecutionTicket, event: crate::DurableRuntimeEvent) -> Result<(), String> {
        if self.ticket != *ticket { return Err("original terminal ticket differs".into()); }
        let mut terminal = self.terminal.lock().map_err(|_| "original terminal owner poisoned")?;
        if terminal.released { return Err("original terminal owner already released".into()); }
        if terminal.event.as_ref().is_some_and(|old| old != &event) { return Err("original terminal event differs".into()); }
        terminal.event = Some(event);
        Ok(())
    }

    /// Called by the original runner only after the exact binding's callback,
    /// driver return and physical-source witness have all completed.
    pub(crate) fn release_after_original_physical_close(&self, ticket: &crate::execution_core::graph::NodeExecutionTicket) -> Result<(), String> {
        if self.ticket != *ticket { return Err("original terminal release ticket differs".into()); }
        let mut terminal = self.terminal.lock().map_err(|_| "original terminal owner poisoned")?;
        if terminal.released { return Ok(()); }
        let event = terminal.event.as_ref().ok_or("original authenticated terminal event unavailable")?.clone();
        let store = self.store.upgrade().ok_or("original terminal store unavailable")?;
        let metadata = event.payload.get("objective_verdict");
        if let Some(metadata) = metadata {
            let program_id = metadata["program_id"].as_str().ok_or("terminal Program identity missing")?;
            let verdict = &metadata["verdict"];
            let goal_id = verdict["goal_id"].as_str().ok_or("terminal Goal identity missing")?;
            let authority = verdict["authority_revision"].as_u64().ok_or("terminal authority missing")?;
            let a_cursor = metadata["a_commit_cursor"].as_u64().ok_or("terminal conclusion cursor missing")?;
            let terminal_receipt = store.original_transaction_receipt(&event.transaction_id)
                .map_err(|error| error.to_string())?.ok_or("terminal transaction receipt missing")?;
            if terminal_receipt.commit_cursor != event.commit_cursor
                || terminal_receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id) {
                return Err("terminal transaction event differs".into());
            }
            let program_stream = format!("agentic-program:{program_id}");
            let conclusion = store.event_by_idempotency_key(&program_stream, &format!("program-conclusion:{authority}"))
                .map_err(|error| error.to_string())?;
            let fence = verdict["terminal_fence"].as_str().ok_or("terminal fence missing")?;
            let bound = store.event_by_idempotency_key(&program_stream, &format!("agentic-objective-verdict:{fence}"))
                .map_err(|error| error.to_string())?;
            let original = match (conclusion, bound) {
                (Some(first), Some(second)) if first == second => first,
                (Some(first), None) | (None, Some(first)) => first,
                (None, None) => return Err("original Program conclusion event missing".into()),
                _ => return Err("original Program conclusion identities conflict".into()),
            };
            if original.kind != "agentic.objective_verdict_bound" || original.payload["program_id"] != program_id
                || &original.payload["verdict"] != verdict || original.commit_cursor != a_cursor {
                return Err("terminal Program conclusion differs".into());
            }
            let goal = GoalStore::new(Arc::clone(&store)).projection(goal_id)?
                .ok_or("terminal Goal unavailable")?.goal;
            let binding = goal.execution_binding.as_ref().ok_or("terminal Goal owner missing")?;
            if binding.agentic_program_id != program_id || binding.root_execution_id != ticket.graph_id
                || goal.revision != verdict["goal_revision"].as_u64().ok_or("terminal Goal revision missing")? {
                return Err("terminal Goal binding differs".into());
            }
            match store.original_conclusion_proof_custody(&original.transaction_id, program_id, &goal)
                .map_err(|error| error.to_string())? {
                Some((proof, receipt)) => store.release_original_conclusion_proof(
                    &original.transaction_id, &proof, &receipt, &original, &goal, &event,
                ).map_err(|error| error.to_string())?,
                None if goal.completion != harness_contract::goal::GoalCompletion::Satisfied => store.release_original_checked_terminal(&event).map_err(|error| error.to_string())?,
                None => return Err("original conclusion proof custody unavailable".into()),
            }
        } else {
            store.release_original_checked_terminal(&event).map_err(|error| error.to_string())?;
        }
        terminal.event = None;
        terminal.released = true;
        drop(terminal);
        // The original turn has closed its Session terminal: the recovery
        // window for that Session ends with it. It does not end while a
        // member of that turn is still unsettled. Such a member can still be
        // owed an owner, and an owner removed under it could never be
        // recovered again. This is the same fact the recovery gate asks, in
        // the Session-wide form.
        (self.release_after_terminal)(&self.owner.claim.session_id(), &self.owner.claim);
        Ok(())
    }

    pub(crate) fn with_current<T>(&self, f: impl FnOnce(&crate::RuntimeOriginalSessionClaim<'_>, &Arc<crate::conversation::MemberCommitControl>) -> Result<T, String>) -> Result<T, String> {
        let original = self.owner.policy.execution_policy_control();
        with_original_session_policy_handles(&self.admission_blocks, &self.policy_controls, self.owner.claim.session_id(), &original, |_| {
            let turn = harness_contract::turn::TurnId::from_string(self.owner.claim.turn_id());
            if self.owner.input.active_turn_id().as_ref() != Some(&turn) { return Err("original terminal turn changed".into()); }
            let fence = self.owner.claim.fence()?;
            let cursor = self.owner.input.highest_consumed_cursor(&turn);
            if self.owner.input.active_turn_id().as_ref() != Some(&turn) { return Err("original terminal turn changed while reading cursor".into()); }
            let consumed_input_sequence = match cursor {
                Some(cursor) if cursor.generation == fence.session_generation =>
                    usize::try_from(cursor.sequence).map_err(|_| "terminal consumed cursor overflow")?.max(fence.input_sequence),
                Some(_) => return Err("terminal consumed cursor generation mismatch".into()),
                None => fence.input_sequence,
            };
            let claim = crate::RuntimeOriginalSessionClaim {
                session_id: self.owner.claim.session_id(), ingress_message_id: self.owner.claim.message_id(),
                turn_id: self.owner.claim.turn_id(), fence: &fence, consumed_input_sequence,
            };
            f(&claim, &self.owner.control)
        })
    }
}

/// Use the original shared policy-owner locks in the same order as admission.
fn with_original_session_policy_handles<T>(
    admission_blocks: &RwLock<BTreeMap<String, String>>,
    policy_controls: &RwLock<BTreeMap<String, crate::permissions::SessionExecutionPolicyControl>>,
    session_id: &str, original: &crate::permissions::SessionExecutionPolicyControl,
    f: impl FnOnce(&harness_contract::policy::SessionExecutionPolicy) -> Result<T, String>,
) -> Result<T, String> {
    let blocked = admission_blocks.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    if blocked.contains_key(session_id) { return Err("Session policy admission is frozen".into()); }
    let controls = policy_controls.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    let current = controls.get(session_id).ok_or("original Session policy is unavailable")?;
    if !current.same_owner(original) { return Err("original Session policy owner was replaced".into()); }
    current.with_snapshot(f)
}

/// Process-local fail-fast single-flight for paid candidate evaluation. It
/// never queues behind another evaluation, so duplicate UI/API requests do
/// not consume another provider budget or delay foreground execution.
struct EvolutionEvaluationFlight {
    candidate_id: String,
    active: Arc<Mutex<BTreeSet<String>>>,
}

impl EvolutionEvaluationFlight {
    fn try_acquire(
        active: Arc<Mutex<BTreeSet<String>>>,
        candidate_id: &str,
    ) -> Result<Self, RuntimeServicesError> {
        let inserted = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(candidate_id.to_string());
        if !inserted {
            return Err(RuntimeServicesError::Invariant(
                "evolution_evaluation_in_progress".to_string(),
            ));
        }
        Ok(Self {
            candidate_id: candidate_id.to_string(),
            active,
        })
    }
}

impl Drop for EvolutionEvaluationFlight {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.candidate_id);
    }
}

/// Removes one request-local root event bus without allowing an older turn to
/// tear down a newer binding for the same deterministic execution identity.
pub(crate) struct ActiveExecutionBusLease {
    execution_id: String,
    generation: u64,
    bus: crate::CowdEventBus,
    buses: Arc<Mutex<BTreeMap<String, ActiveExecutionBus>>>,
    /// Session-scoped index of the original Session owner parts this Runtime
    /// installed. It keeps the original claim reachable after the root bus
    /// lease is retired, without keeping the root bus itself alive.
    session_owners: Arc<Mutex<BTreeMap<String, OriginalSessionOwnerParts>>>,
}

impl ActiveExecutionBusLease {
    pub(crate) fn install_session_fence(
        &self,
        ingress: &crate::TurnIngressRef,
        fence: crate::SessionExecutionFence,
        root_control: Arc<TurnCancellationControl>,
        input: crate::SessionInputStream,
        control_requested: Arc<crate::conversation::OriginalTurnInputControl>,
        original_policy: crate::permissions::PermissionPolicy,
    ) -> Result<Arc<OriginalSessionClaim>, String> {
        if root_control.ingress.as_ref() != Some(ingress)
            || !root_control.matches_target(&ingress.session_id, &ingress.turn_id, &self.execution_id)
        { return Err("original Session turn control does not match ingress".into()); }
        if self.execution_id != crate::session_ingress_graph_id(
            &ingress.session_id, &ingress.request_id, &ingress.turn_id,
        ) {
            return Err("Session ingress does not own the active execution bus".into());
        }
        let mut buses = self.buses.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let active = buses.get_mut(&self.execution_id)
            .filter(|active| active.generation == self.generation)
            .ok_or_else(|| "Session execution bus binding is no longer current".to_string())?;
        if active.session_fence.is_some() {
            return Err("Session execution fence already installed".into());
        }
        let claim = Arc::new(OriginalSessionClaim { ingress: ingress.clone() });
        claim.fence()?;
        active.session_fence = Some(ActiveSessionFence {
            session_id: ingress.session_id.clone(), turn_id: ingress.turn_id.clone(), fence: fence.clone(),
            root_control: Arc::clone(&root_control),
            claim: Arc::clone(&claim), input: input.clone(), control_requested: Arc::clone(&control_requested),
            original_policy: original_policy.clone(), original_policy_owner: None,
        });
        // The root bus lease is retired when the root graph ends, but the
        // original Session turn may still owe its members an owner. Keep the
        // very handles this ingress installed reachable by Session, so the
        // owner can be recovered instead of being minted again.
        self.session_owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                ingress.session_id.clone(),
                OriginalSessionOwnerParts {
                    fence: fence.clone(),
                    root_control: Arc::clone(&root_control),
                    claim: Arc::clone(&claim),
                    input: input.clone(),
                    requested: Arc::clone(&control_requested),
                    policy: original_policy.clone(),
                },
            );
        Ok(claim)
    }

    pub(crate) fn install_session_policy_owner(
        &self,
        owner: Arc<crate::conversation::SharedPolicyOwner>,
    ) -> Result<(), String> {
        let mut buses = self.buses.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let active = buses.get_mut(&self.execution_id)
            .filter(|active| active.generation == self.generation)
            .and_then(|active| active.session_fence.as_mut())
            .ok_or_else(|| "original Session execution owner is no longer current".to_string())?;
        if owner.session_id() != active.session_id || active.original_policy_owner.is_some() {
            return Err("original Session policy owner mismatch or already installed".into());
        }
        active.original_policy_owner = Some(owner);
        Ok(())
    }
}

impl Drop for ActiveExecutionBusLease {
    fn drop(&mut self) {
        self.bus.unbind_live_telemetry_observer(self.generation);
        let mut buses = self
            .buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buses
            .get(&self.execution_id)
            .is_some_and(|active| active.generation == self.generation)
        {
            buses.remove(&self.execution_id);
        }
    }
}

/// Read-only observation of the first targeted checked append, captured before
/// the original reconciliation loop can retry. No observation grants authority.
#[cfg(feature = "test-fixtures")]
#[derive(Debug, Clone)]
pub struct ConclusionAttemptObservation {
    pub transaction_id: String,
    pub outcome: ConclusionAttemptOutcome,
    /// Point-in-time original transaction lookup after the attempt and before
    /// retry. An error remains unknown; absence is not a future-state claim.
    pub original_receipt: Result<Option<crate::AppendTransactionReceipt>, String>,
    /// Individual intended events, read before a retry; callers must check
    /// transaction identity on any existing event and treat errors as unknown.
    pub events: Vec<ConclusionAttemptEvent>,
}

#[cfg(feature = "test-fixtures")]
#[derive(Debug, Clone)]
pub struct ConclusionAttemptEvent {
    pub stream_id: String,
    pub idempotency_key: Option<String>,
    pub observed: Result<Option<crate::DurableRuntimeEvent>, String>,
}

#[cfg(feature = "test-fixtures")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConclusionAttemptOutcome {
    Committed,
    Cancelled,
    Stale { stream_id: String, expected: u64, actual: u64 },
    Conflict,
    Error(String),
}

impl RuntimeServices {
    #[must_use]
    pub fn builder(
        cowd_home: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> RuntimeServicesBuilder {
        RuntimeServicesBuilder {
            cowd_home: cowd_home.into(),
            workspace_root: workspace_root.into(),
            runtime_build_identity:
                harness_contract::outcome::RuntimeBuildIdentity::unresolved_development(env!(
                    "CARGO_PKG_VERSION"
                )),
            runtime_event_store: None,
            task_aggregate_service: None,
            builtin_definitions_root: None,
            resource_quotas: default_resource_quotas(),
            provider_resource_config: crate::ProviderResourceConfig::default(),
            provider_registry: Arc::new(crate::ProviderRegistry::empty()),
            provider_fallbacks: Vec::new(),
            provider_transport_pool: Arc::new(crate::ProviderTransportPool::default()),
            provider_template_cache: Arc::new(crate::ProviderClientTemplateCache::default()),
            process_jsonl_commands: Vec::new(),
            tool_execution_host: None,
            session_query_port: None,
            session_ingress_port: None,
            session_journal_port: None,
            session_application_port: None,
            artifact_store: None,
            memory_manager: None,
            reality_recall_port: None,
            knowledge_activation: None,
            evolution_eval_runner: None,
            background_work: crate::BackgroundWorkConfig::default(),
            skill_catalog: crate::RuntimeSkillCatalog::default(),
            mission_schedule_policy: crate::MissionSchedulePolicy::default(),
            hot_state_config: crate::execution_core::hot_state::HotStateConfig::default(),
            approval_config: ApprovalConfig::default(),
            collaboration_capacity: crate::CollaborationCapacityPolicy::default(),
            collaboration_max_parallel_agents: crate::AgentControlPolicy::default()
                .max_parallel_agents,
            projection_lanes: Vec::new(),
        }
    }

    /// Unit-test composition root with all non-durable ports selected
    /// explicitly. Production callers continue to use [`Self::builder`] and
    /// must inject PostgreSQL-backed owners before `build` can succeed.
    #[cfg(test)]
    pub(crate) fn test_builder(
        cowd_home: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> RuntimeServicesBuilder {
        let cowd_home = cowd_home.into();
        let workspace_root = workspace_root.into();
        Self::builder(&cowd_home, workspace_root)
            .runtime_event_store(Arc::new(RuntimeEventStore::for_test()))
            .artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
                cowd_home.join("test-artifacts"),
            )))
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
            .background_work(crate::BackgroundWorkConfig {
                evolution: false,
                growth: false,
            })
    }

    pub fn in_memory() -> Result<Arc<Self>, RuntimeServicesError> {
        Self::in_memory_composed(None)
    }

    #[cfg(test)]
    pub(crate) fn in_memory_with_tool_host(
        host: Arc<dyn crate::RuntimeExecutionHost>,
    ) -> Result<Arc<Self>, RuntimeServicesError> {
        Self::in_memory_composed(Some(host))
    }

    fn in_memory_composed(
        tool_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    ) -> Result<Arc<Self>, RuntimeServicesError> {
        let workspace_key = format!("in-memory-{}", uuid::Uuid::new_v4());
        let ephemeral_root = tempfile::Builder::new()
            .prefix("cowd-runtime-services-")
            .tempdir()?;
        let workspace_root = ephemeral_root.path().join(&workspace_key);
        std::fs::create_dir_all(&workspace_root)?;
        let definition_root = ephemeral_root.path().join("definitions");
        let config_home = definition_root.join("config-home");
        let storage_registry = storage::StorageRegistry::default_for_config_home(&config_home)
            .with_workspace(&workspace_root)?;
        let definition_registry = Arc::new(RuntimeDefinitionRegistry::from_storage_registry(
            &storage_registry,
            definition_root.join("builtin"),
            &workspace_root,
        )?);
        let task_aggregate_service = Arc::new(crate::TaskAggregateService::for_test());
        let services = Arc::new(Self::assemble(
            config_home,
            workspace_root,
            workspace_key.clone(),
            Arc::new(RuntimeEventStore::for_test()),
            harness_contract::outcome::RuntimeBuildIdentity::unresolved_development(env!(
                "CARGO_PKG_VERSION"
            )),
            Arc::new(WorktreeLeaseManager::open(
                ephemeral_root.path().join("worktree-leases.json"),
            )?),
            Arc::new(ScopeLockManager::persistent(
                ephemeral_root.path().join("scope-locks"),
            )?),
            default_resource_quotas(),
            crate::ProviderResourceConfig::default(),
            Arc::new(crate::ProviderRegistry::empty()),
            Vec::new(),
            Arc::new(crate::ProviderTransportPool::default()),
            Arc::new(crate::ProviderClientTemplateCache::default()),
            tool_host,
            Arc::new(crate::ArtifactStore::for_test_default(
                storage_registry
                    .endpoint(&storage::StorageDomainId::Blobs)?
                    .path
                    .clone(),
            )),
            None,
            None,
            None,
            None,
            crate::RuntimeSkillCatalog::default(),
            crate::BackgroundWorkConfig::default(),
            crate::MissionSchedulePolicy::default(),
            crate::execution_core::hot_state::HotStateConfig::default(),
            ApprovalConfig::default(),
            crate::CollaborationCapacityPolicy::default(),
            crate::AgentControlPolicy::default().max_parallel_agents,
            definition_registry,
            task_aggregate_service,
            Vec::new(),
            Some(ephemeral_root),
            false,
        )?);
        services.install_graph_settled_observer()?;
        crate::task::structured::install(&services);
        crate::execution_core::graph::executors::agent_tool::AgentToolBatchResolver::install(
            &services,
        );
        services.agent_runtime.bind_services(Arc::clone(&services));
        services
            .agent_runtime
            .register_observation_authority_backend(Arc::new(InProcessAgentWorker::new(
                Arc::downgrade(&services),
            )));
        services
            .agent_runtime
            .register_runtime_process_jsonl_backend(Arc::new(ProcessJsonlAdapter::for_runtime(
                Arc::downgrade(&services),
                services.workspace_root(),
            )));
        services
            .agent_runtime
            .block_unrecoverable_replayed_runs()
            .map_err(RuntimeServicesError::AgentRuntime)?;
        services.materialize_evolution_release_assignments()?;
        services.start_background_reactors()?;
        Ok(services)
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        cowd_home: PathBuf,
        workspace_root: PathBuf,
        workspace_key: String,
        event_store: Arc<RuntimeEventStore>,
        runtime_build_identity: harness_contract::outcome::RuntimeBuildIdentity,
        worktree_leases: Arc<WorktreeLeaseManager>,
        scope_locks: Arc<ScopeLockManager>,
        mut resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
        provider_resource_config: crate::ProviderResourceConfig,
        provider_registry: Arc<crate::ProviderRegistry>,
        provider_fallbacks: Vec<String>,
        provider_transport_pool: Arc<crate::ProviderTransportPool>,
        provider_template_cache: Arc<crate::ProviderClientTemplateCache>,
        tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
        artifact_store: Arc<crate::ArtifactStore>,
        memory_manager: Option<Arc<memory::CognitiveContextManager>>,
        reality_recall_port: Option<Arc<RealityRecallPort>>,
        knowledge_activation: Option<crate::knowledge_activation::KnowledgeActivationRuntime>,
        evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
        skill_catalog: crate::RuntimeSkillCatalog,
        background_work: crate::BackgroundWorkConfig,
        mission_schedule_policy: crate::MissionSchedulePolicy,
        hot_state_config: crate::execution_core::hot_state::HotStateConfig,
        approval_config: ApprovalConfig,
        collaboration_capacity: crate::CollaborationCapacityPolicy,
        collaboration_max_parallel_agents: usize,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
        task_aggregate_service: Arc<crate::TaskAggregateService>,
        mut projection_lanes: Vec<crate::RuntimeProjectionLane>,
        ephemeral_root: Option<tempfile::TempDir>,
        defer_graph_producers: bool,
    ) -> Result<Self, RuntimeServicesError> {
        let assembly_started_at = Instant::now();
        event_store
            .bind_artifact_store(Arc::clone(&artifact_store))
            .map_err(RuntimeServicesError::Invariant)?;
        let path_identity_resolver = Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(&workspace_root)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?,
        );
        let executor_registry = Arc::new(NodeExecutorRegistry::new());
        let hot_state = Arc::new(crate::execution_core::hot_state::RuntimeHotStatePlane::new(
            hot_state_config,
        ));
        let graph_state_store = ExecutionGraphStateStore::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&hot_state),
        );
        let agentic_read_model = Arc::new(
            crate::agentic::AgenticReadModel::default()
                .with_residency(Arc::clone(hot_state.residency())),
        );
        let model_step_executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
        let tool_batch_executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        let cross_plane_connector_executor =
            Arc::new(ScopedNodeExecutor::new("cross_plane_connector"));
        let agent_task_executor =
            Arc::new(AgentTaskExecutor::new().with_state_store(graph_state_store.clone()));
        agent_task_executor
            .bind_path_identity_resolver(Arc::clone(&path_identity_resolver))
            .map_err(RuntimeServicesError::Invariant)?;
        let agent_runtime = Arc::new(AgentRuntime::new(
            Arc::clone(&event_store),
            Arc::clone(&provider_registry),
        ));
        agent_runtime
            .catalog()
            .replace_all(definition_registry.runnable_agent_catalog()?);
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime Agent graph assembled"
        );
        agent_task_executor.install_resolver(Arc::new(AgentRuntimeResolver::new(Arc::clone(
            &agent_runtime,
        ))));
        let verify_executor = Arc::new(VerifyNodeExecutor::new(graph_state_store.clone()));
        let materialize_executor = Arc::new(MaterializeNodeExecutor::new(
            graph_state_store.clone(),
            workspace_root.clone(),
        ));
        let synthesize_executor = Arc::new(SynthesizeNodeExecutor::new());
        let session_dispatch_executor =
            Arc::new(crate::session_execution::SessionDispatchNodeExecutor::new());
        let session_execution_policy_controls = Arc::new(RwLock::new(BTreeMap::<
            String,
            crate::permissions::SessionExecutionPolicyControl,
        >::new()));
        let session_execution_policy_admission_blocks =
            Arc::new(RwLock::new(BTreeMap::<String, String>::new()));
        let approval_queue = Arc::new(ApprovalQueue::new(Arc::clone(&event_store)));
        let approval_coordinator = Arc::new(ApprovalCoordinator::new(
            Arc::clone(&approval_queue),
            approval_config,
        ));
        let evolution_governance = Arc::new(crate::EvolutionGovernanceService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
        ));
        let evolution_discovery = Arc::new(crate::evolution::EvolutionDiscoveryService::new(
            Arc::clone(&event_store),
        ));
        let evolution_analyst = RwLock::new(background_work.evolution.then(|| {
            Arc::new(crate::evolution::analyst::EvolutionAnalystService::new(
                Arc::clone(&event_store),
                Arc::clone(&evolution_discovery),
            ))
        }));
        let evolution_signal_projector = Arc::new(
            crate::evolution::EvolutionSignalProjector::new(
                Arc::clone(&event_store),
                Arc::clone(&evolution_discovery),
            )
            .with_workspace_identity(workspace_key.clone()),
        );
        if background_work.evolution && evolution_signal_projector.ensure_source_epoch().is_err() {
            // Background bookkeeping failure must not prevent foreground
            // Runtime creation. Its original projection lane retries under
            // the same durable source owner; controls report unknown/error.
            tracing::warn!("Evolution source epoch initialization remains pending");
        }
        let skill_maintenance_projector = Arc::new(crate::SkillMaintenanceProjector::new(
            Arc::clone(&event_store),
        ));
        let skill_catalog = Arc::new(RwLock::new(skill_catalog));
        let skill_revision_governance = Arc::new(crate::SkillRevisionGovernanceService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
            Arc::clone(&skill_catalog),
        ));
        let approval_policy_controls = Arc::clone(&session_execution_policy_controls);
        install_builtin_executors(
            &executor_registry,
            vec![
                Arc::clone(&model_step_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&tool_batch_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&cross_plane_connector_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&agent_task_executor) as Arc<dyn NodeExecutor>,
                Arc::new(CompileTargetGuardExecutor) as Arc<dyn NodeExecutor>,
                Arc::new(ApprovalNodeExecutor::with_session_policy_lookup(
                    Arc::clone(&approval_queue),
                    Arc::new(move |session_id| {
                        approval_policy_controls
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get(session_id)
                            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
                    }),
                )),
                Arc::clone(&verify_executor) as Arc<dyn NodeExecutor>,
                materialize_executor as Arc<dyn NodeExecutor>,
                Arc::clone(&synthesize_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&session_dispatch_executor) as Arc<dyn NodeExecutor>,
            ],
        )?;
        executor_registry.install_builtin_tool_batch(&tool_batch_executor)?;
        let process_input_verifier =
            crate::agentic::review_evidence::ProcessInputVerifier::with_workspace(Arc::clone(&scope_locks), workspace_root.clone());
        let session_query_port = Arc::new(OnceLock::new());
        let commit_service = ExecutionCommitService::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&hot_state),
        )
        .with_process_input_verifier(process_input_verifier.clone())
        .with_artifact_store(Arc::clone(&artifact_store))
        .with_session_query_slot(Arc::clone(&session_query_port));
        let provider_generation = provider_resource_config.materialize(&provider_registry.pin());
        resource_quotas.retain(|(kind, _)| {
            !matches!(
                kind,
                ExecutionResourceKind::Provider
                    | ExecutionResourceKind::ProviderAccount(_)
                    | ExecutionResourceKind::ProviderModel(_)
                    | ExecutionResourceKind::ProviderTokenPool(_)
            )
        });
        resource_quotas.extend(provider_generation.quotas.iter().cloned());
        let execution_capacity_profile = crate::ExecutionCapacityProfile::resolve(
            &collaboration_capacity,
            collaboration_max_parallel_agents,
        )
        .map_err(RuntimeServicesError::Invariant)?;
        let resource_manager = Arc::new(
            ExecutionResourceManager::with_admission_policy(
                resource_quotas.clone(),
                crate::execution_core::graph::ExecutionAdmissionPolicy {
                    revision: execution_capacity_profile.revision,
                    max_pending_instance: execution_capacity_profile.max_pending_instance,
                    max_pending_per_class: execution_capacity_profile.max_pending_per_class,
                    max_pending_per_key: execution_capacity_profile.max_pending_per_key,
                    aging_interval: std::time::Duration::from_millis(
                        execution_capacity_profile.admission_aging_interval_ms,
                    ),
                },
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?,
        );
        resource_manager
            .reconcile_quotas(resource_quotas, provider_generation.reserves)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let resource_evidence_writer =
            super::evidence_writer::ResourceEvidenceWriter::start(Arc::clone(&event_store));
        let observer_writer = Arc::clone(&resource_evidence_writer);
        resource_manager
            .install_admission_observer(move |observation| {
                observer_writer.try_publish(observation);
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let tool_execution_plane = Arc::new(crate::ToolExecutionPlane::new(
            Arc::clone(&resource_manager),
            Arc::clone(&scope_locks),
        ));
        let graph_runner = Arc::new(ExecutionGraphRunner::new_with_path_identity_resolver(
            Arc::clone(&executor_registry),
            graph_state_store.clone(),
            commit_service.clone(),
            Arc::clone(&resource_manager),
            Arc::clone(&scope_locks),
            Arc::clone(&worktree_leases),
            workspace_key.clone(),
            workspace_root.clone(),
            Arc::clone(&path_identity_resolver),
        ));
        let execution_supervisor =
            Arc::new(crate::RuntimeExecutionSupervisor::with_capacity_profile(
                graph_runner,
                &execution_capacity_profile,
                defer_graph_producers,
            ));
        execution_supervisor.install_session_recovery_owner(Arc::clone(&event_store))?;
        let agentic_actions = crate::AgentActionService::new(Arc::clone(&event_store))
            .with_graph_store(graph_state_store.clone())
            .with_read_model(Arc::clone(&agentic_read_model))
            .with_process_input_verifier(process_input_verifier.clone())
            .with_artifact_store(Arc::clone(&artifact_store))
            .with_session_query_slot(Arc::clone(&session_query_port));
        let goal_store = Arc::new(
            GoalStore::new(Arc::clone(&event_store))
                .with_process_input_verifier(process_input_verifier.clone())
                .with_artifact_store(Arc::clone(&artifact_store))
                .with_session_query_slot(Arc::clone(&session_query_port)),
        );
        let objective_supervisor = Arc::new(ObjectiveSupervisor::new(Arc::clone(&goal_store)));
        executor_registry.register(Arc::new(AgenticProgramWaitExecutor::new(
            agentic_actions.clone(),
            graph_state_store.clone(),
            Arc::downgrade(&execution_supervisor),
        )))?;
        tool_execution_plane.bind_supervisor(&execution_supervisor);
        let mission_runtime = Arc::new(
            MissionRuntime::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        let l4_session_policy_lookup = {
            let controls = Arc::clone(&session_execution_policy_controls);
            Arc::new(move |session_id: &str| {
                controls
                    .read()
                    .ok()
                    .and_then(|map| map.get(session_id).cloned())
                    .map(|control| control.snapshot())
            })
                as Arc<
                    dyn Fn(&str) -> Option<harness_contract::policy::SessionExecutionPolicy>
                        + Send
                        + Sync,
                >
        };
        let l4_promotion_service = Arc::new(crate::L4PromotionService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
            memory_manager.clone(),
            Some(l4_session_policy_lookup),
        ));
        let knowledge_candidate_projector = Arc::new(crate::KnowledgeCandidateProjector::new(
            Arc::clone(&event_store),
            Arc::clone(&l4_promotion_service),
        ));
        let outcome_service = Arc::new(crate::execution_core::OutcomeService::with_build_identity(
            Arc::clone(&event_store),
            runtime_build_identity,
        ));
        let outcome_projector = Arc::new(crate::OutcomeProjector::new(Arc::clone(&event_store)));
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime outcome projection assembled"
        );
        let gate_store = Arc::clone(&event_store);
        let gate_workspace = workspace_key.clone();
        let gate_root = workspace_root.clone();
        execution_supervisor.install_mutation_gate(move || {
            let importer = crate::upgrade::LegacyExecutionImporter::new(
                Arc::clone(&gate_store),
                &gate_workspace,
                &gate_root,
                "",
            );
            importer
                .mutation_allowed()
                .map_err(|error| error.to_string())?
                .then_some(())
                .ok_or_else(|| "upgrade_recovery_required".to_string())
        });
        let mission_evidence = Arc::new(MissionEvidenceBus::new(Arc::clone(&event_store)));
        if background_work.evolution {
            projection_lanes.push(
            crate::evolution::collaboration_experience::CollaborationExperienceProjector::new(
                Arc::clone(&event_store),
                graph_state_store.clone(),
                workspace_key.clone(),
            )
            .projection_lane()
            .map_err(RuntimeServicesError::Invariant)?,
            );
            projection_lanes.push(
                evolution_signal_projector
                    .projection_lane()
                    .map_err(RuntimeServicesError::Invariant)?,
            );
        }
        projection_lanes.extend([
            knowledge_candidate_projector
                .projection_lane()
                .map_err(RuntimeServicesError::Invariant)?,
            outcome_projector
                .projection_lane()
                .map_err(RuntimeServicesError::Invariant)?,
            mission_evidence
                .projection_lane()
                .map_err(RuntimeServicesError::Invariant)?,
            skill_maintenance_projector
                .projection_lane()
                .map_err(RuntimeServicesError::Invariant)?,
        ]);
        projection_lanes.push(agentic_settled_graph_resolution_lane(
            Arc::clone(&event_store),
            graph_state_store.clone(),
            Arc::clone(&execution_supervisor),
            agentic_actions.clone(),
            Arc::clone(&agent_runtime),
        )?);
        projection_lanes.push(agentic_program_wait_resolution_lane(
            Arc::clone(&event_store),
            graph_state_store.clone(),
            Arc::clone(&execution_supervisor),
            agentic_actions.clone(),
            Arc::clone(&agent_runtime),
        )?);
        let event_reactor = Arc::new(
            crate::RuntimeEventReactor::sealed(Arc::clone(&event_store), projection_lanes)
                .map_err(RuntimeServicesError::Invariant)?,
        );
        let conflict_resolver = Arc::new(ConflictArbiter::new(
            Arc::clone(&mission_evidence),
            Arc::clone(&event_store),
        ));
        let mission_schedules = Arc::new(
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        let managed_agents = Arc::new(
            crate::ManagedAgentDispatcher::event_sourced(
                Arc::clone(&event_store),
                workspace_key.clone(),
            )
            .map_err(RuntimeServicesError::Mission)?,
        );
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime mission and Managed Agent projections assembled"
        );
        let session_relations = Arc::new(
            SessionRelationGraph::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime Session relation projection assembled"
        );
        let reality_recall_port = reality_recall_port.unwrap_or_else(|| {
            Arc::new(RealityRecallPort::for_config_home_and_workspace(
                &cowd_home,
                &workspace_root,
            ))
        });
        Ok(Self {
            workspace_root,
            workspace_key: workspace_key.clone(),
            path_identity_resolver,
            event_store: Arc::clone(&event_store),
            live_execution_store: Arc::new(
                crate::execution_live::ExecutionLiveStore::with_hot_state(
                    Arc::clone(&event_store),
                    Arc::clone(&hot_state),
                ),
            ),
            hot_state,
            executor_registry,
            model_step_executor,
            tool_batch_executor,
            cross_plane_connector_executor,
            agent_task_executor,
            agent_runtime,
            agentic_actions,
            l4_promotion_service,
            knowledge_candidate_projector,
            outcome_service,
            outcome_projector,
            verify_executor,
            synthesize_executor,
            graph_state_store,
            commit_service,
            execution_supervisor,
            approval_queue,
            approval_coordinator,
            execution_capacity_profile: Arc::new(execution_capacity_profile),
            evolution_governance,
            evolution_discovery,
            evolution_analyst,
            background_work: Mutex::new(background_work),
            evolution_signal_projector,
            skill_maintenance_projector,
            event_reactor,
            skill_revision_governance,
            skill_usage_recorder: std::sync::Mutex::new((false, None)),
            mission_evidence,
            conflict_resolver,
            resource_manager,
            tool_execution_plane,
            scope_locks,
            worktree_leases,
            definition_registry,
            cross_plane: Arc::new(CrossPlaneRuntimeService::open(Arc::clone(&event_store))?),
            mission_runtime,
            task_aggregate_service,
            mission_schedules,
            managed_agents,
            mission_schedule_policy: Arc::new(RwLock::new(mission_schedule_policy)),
            session_relations,
            goal_store,
            objective_supervisor,
            provider_registry,
            provider_resource_config: Arc::new(RwLock::new(provider_resource_config)),
            provider_fallbacks: Arc::new(RwLock::new(normalize_provider_fallbacks(
                provider_fallbacks,
            ))),
            provider_transport_pool,
            provider_template_cache,
            evaluation_provider_token_leases: Arc::new(
                crate::conversation::EvaluationProviderTokenLeaseRegistry::default(),
            ),
            tool_execution_host,
            artifact_store,
            memory_manager,
            evolution_eval_runner: RwLock::new(evolution_eval_runner),
            evolution_evaluation_flights: Arc::new(Mutex::new(BTreeSet::new())),
            agentic_dispatch_flights: Arc::new(Mutex::new(BTreeSet::new())),
            agentic_read_model,
            skill_catalog,
            reality_recall_port,
            knowledge_activation: knowledge_activation
                .unwrap_or_else(crate::knowledge_activation::KnowledgeActivationRuntime::new),
            session_dispatch_executor,
            session_input_router: OnceLock::new(),
            session_query_port,
            session_ingress_port: OnceLock::new(),
            session_journal_port: OnceLock::new(),
            session_application_port: OnceLock::new(),
            active_execution_buses: Arc::new(Mutex::new(BTreeMap::new())),
            original_session_owners: Arc::new(Mutex::new(BTreeMap::new())),
            session_execution_policy_controls,
            session_execution_policy_admission_blocks,
            maintenance_supervisor: Arc::new(RuntimeMaintenanceSupervisor::new()),
            #[cfg(test)]
            retirement_owner_test_gate: Mutex::new(None),
            resource_evidence_writer,
            execution_projection_cache: Mutex::new(
                crate::execution_projection::ExecutionProjectionCache::default(),
            ),
            _ephemeral_root: ephemeral_root,
        })
    }

    /// A settled callback only wakes the original durable consumer. The reactor
    /// owns its work and join; no callback can retain or independently drive the
    /// Runtime after shutdown.
    pub(crate) fn install_graph_settled_observer(
        self: &Arc<Self>,
    ) -> Result<(), RuntimeServicesError> {
        let reactor = Arc::downgrade(&self.event_reactor);
        self.execution_supervisor
            .install_graph_settled_observer(move |_graph_id| {
                if let Some(reactor) = reactor.upgrade() {
                    reactor.wake_lane(AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID);
                }
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub(crate) fn notify_agentic_followups(&self) {
        self.event_reactor
            .wake_lane(AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID);
    }

    /// Release every Runtime-owned autonomous writer only after the launcher
    /// has hydrated Sessions and recovered Graphs and Agentic Programs in
    /// dependency order. Keeping the event reactor and approval deadline
    /// scheduler behind one gate prevents either subsystem from advancing
    /// durable business state during crash recovery.
    pub(super) fn start_background_reactors(self: &Arc<Self>) -> Result<(), RuntimeServicesError> {
        // ApprovalQueue retains this wake callback and is itself retained by
        // the supervisor's approval executor/coordinator. Neither back-edge
        // may keep its owner alive after maintenance shutdown.
        let deadline_supervisor = Arc::downgrade(&self.execution_supervisor);
        let deadline_approval_coordinator = Arc::downgrade(&self.approval_coordinator);
        self.approval_queue
            .install_deadline_scheduler(Arc::new(move |approval_id| {
                let supervisor = deadline_supervisor.clone();
                let approval_coordinator = deadline_approval_coordinator.clone();
                Box::pin(async move {
                    let (Some(supervisor), Some(approval_coordinator)) =
                        (supervisor.upgrade(), approval_coordinator.upgrade())
                    else {
                        return;
                    };
                    approval_coordinator.notify_decision(&approval_id);
                    if let Some((graph_id, _)) =
                        crate::execution_core::graph::executors::parse_graph_approval_id(
                            &approval_id,
                        )
                    {
                        if let Err(error) = supervisor.notify_graph(&graph_id).await {
                            tracing::warn!(
                                graph_id,
                                %error,
                                "approval deadline could not wake graph"
                            );
                        }
                    }
                })
            }));
        self.event_reactor
            .start()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn session_recovery_exclusions(&self) -> Vec<String> {
        self.event_store.session_recovery_exclusions()
    }

    pub fn ensure_session_recovery_admitted(
        &self,
        session_id: &str,
    ) -> Result<(), RuntimeServicesError> {
        self.event_store
            .ensure_session_recovery_admitted(session_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn install_startup_recovery_exclusions(
        &self,
        excluded: BTreeSet<String>,
    ) -> Result<(), RuntimeServicesError> {
        if excluded.iter().any(|session| session.trim().is_empty()) {
            return Err(RuntimeServicesError::Invariant(
                "startup_recovery_session_id_empty".into(),
            ));
        }
        self.execution_supervisor
            .install_startup_recovery_exclusions(&self.event_store, excluded)?;
        Ok(())
    }

    pub async fn release_recovered_producers(
        self: &Arc<Self>,
    ) -> Result<usize, RuntimeServicesError> {
        // Restore original Task registration/result obligations before taking
        // the deferred Graph snapshot and validating executor bindings.
        let mut structured_after = String::new();
        while let Some(after) = self
            .recover_structured_task_page(&structured_after, 256)
            .await
            .map_err(RuntimeServicesError::Task)?
        {
            structured_after = after;
        }
        let Some(deferred) = self.execution_supervisor.deferred_recovered_graphs() else {
            return Ok(0);
        };
        self.execution_supervisor.prepare_recovered_producers()?;
        let mut prepared = Vec::new();
        for graph_id in &deferred {
            // Wake only a recovered graph whose turn-scoped executors are bound.
            // A conversation-owned graph (inline_model/tool_batch/synthesize) is
            // bound by the Session host after it restores the turn context;
            // waking it here races that binding and fails the node as
            // `executor ... is unavailable`. Unbound graphs stay deferred and are
            // driven by the Session host itself once it restores the context.
            let graph = self.graph_state_store().load_async(graph_id).await?;
            if graph.lineage.as_ref().is_some_and(|lineage| {
                self.event_store
                    .is_session_recovery_excluded(&lineage.session_id)
            }) {
                continue;
            }
            self.execution_supervisor
                .ensure_graph_session_recovery_admitted(&graph)?;
            if !self.graph_executors_are_bound(&graph)? {
                continue;
            }
            prepared.push(graph_id.clone());
        }
        self.start_background_reactors()?;
        self.execution_supervisor
            .release_prepared_recovered_graphs(&deferred)?;
        let mut released = 0usize;
        for graph_id in prepared {
            self.execution_supervisor.notify_graph(&graph_id).await?;
            released += 1;
        }
        tracing::info!(
            released,
            "ordered Runtime recovery released bound autonomous producers"
        );
        Ok(released)
    }

    pub fn install_session_ports(
        self: &Arc<Self>,
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
        journal: Arc<dyn crate::SessionRuntimeJournalPort>,
        application: Arc<dyn crate::SessionRuntimeApplicationPort>,
    ) -> Result<Arc<SessionInputRouter>, RuntimeServicesError> {
        if let Some(router) = self.session_input_router.get() {
            return Ok(Arc::clone(router));
        }
        let router = SessionInputRouter::install(
            Arc::clone(&query),
            Arc::clone(&ingress),
            &self.workspace_key,
            Arc::clone(&self.event_store),
        )?;
        self.session_dispatch_executor
            .install_router(Arc::clone(&router))
            .map_err(RuntimeServicesError::Mission)?;
        self.session_query_port
            .set(query)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_ingress_port
            .set(ingress)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_journal_port
            .set(journal)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_application_port
            .set(application)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_input_router
            .set(Arc::clone(&router))
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        Ok(router)
    }

    #[cfg(test)]
    pub(crate) fn install_test_session_store(
        self: &Arc<Self>,
        store: Arc<session::UnifiedSessionStore>,
    ) -> Result<Arc<SessionInputRouter>, RuntimeServicesError> {
        if let Some(router) = self.session_input_router.get() {
            return Ok(Arc::clone(router));
        }
        let port = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let query: Arc<dyn crate::SessionRuntimeQueryPort> = port.clone();
        let ingress: Arc<dyn crate::SessionRuntimeIngressPort> = port.clone();
        let journal: Arc<dyn crate::SessionRuntimeJournalPort> = port;
        let application: Arc<dyn crate::SessionRuntimeApplicationPort> =
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let router = SessionInputRouter::install_for_test(
            Arc::clone(&query),
            Arc::clone(&ingress),
            store,
            &self.workspace_key,
            Arc::clone(&self.event_store),
        )?;
        self.session_dispatch_executor
            .install_router(Arc::clone(&router))
            .map_err(RuntimeServicesError::Mission)?;
        self.session_query_port
            .set(query)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_ingress_port
            .set(ingress)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_journal_port
            .set(journal)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_application_port
            .set(application)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_input_router
            .set(Arc::clone(&router))
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        Ok(router)
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
    pub fn path_identity_resolver(
        &self,
    ) -> &Arc<crate::path_identity::WorkspacePathIdentityResolver> {
        &self.path_identity_resolver
    }
    pub fn workspace_key(&self) -> &str {
        &self.workspace_key
    }

    pub(crate) fn cached_execution_projection(
        &self,
        key: &crate::execution_projection::ExecutionProjectionCacheKey,
    ) -> Option<harness_contract::projection::ExecutionProjection> {
        self.execution_projection_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
    }

    pub(crate) fn cache_execution_projection(
        &self,
        key: crate::execution_projection::ExecutionProjectionCacheKey,
        projection: harness_contract::projection::ExecutionProjection,
    ) {
        self.execution_projection_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .put(key, projection);
    }

    #[cfg(test)]
    pub(crate) fn execution_projection_cache_stats(&self) -> (u64, u64, usize) {
        self.execution_projection_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stats()
    }

    /// Runtime-owned access to the shared cognitive manager. Callers can use
    /// it only through Runtime host construction; Gateway does not receive a
    /// prompt-assembly capability from this accessor.
    #[must_use]
    pub fn memory_manager(&self) -> Option<Arc<memory::CognitiveContextManager>> {
        self.memory_manager.clone()
    }

    pub(crate) fn maintenance_supervisor(&self) -> Arc<RuntimeMaintenanceSupervisor> {
        Arc::clone(&self.maintenance_supervisor)
    }

    #[cfg(test)]
    pub(crate) fn retirement_lane_notifications_for_test(
        &self,
        source: tokio::sync::watch::Receiver<u64>,
    ) -> Arc<std::sync::atomic::AtomicU64> {
        self.event_reactor
            .use_lane_notifications_for_test(AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID, source)
            .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn install_retirement_owner_test_gate(&self, gate: Arc<RetirementOwnerTestGate>) {
        let mut slot = self.retirement_owner_test_gate.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(gate);
    }

    #[cfg(test)]
    pub(crate) async fn retirement_cancel_gate_for_test(
        &self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
    ) {
        let gate = self.retirement_owner_test_gate.lock().unwrap().clone();
        let Some(gate) = gate.filter(|gate| gate.packet.graph_id() == graph.id) else {
            return;
        };
        let original = graph
            .nodes
            .iter()
            .find(|node| node.id == gate.packet.node_id())
            .and_then(|node| {
                serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref)
                    .ok()
            });
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(Some(&gate.packet)).unwrap()
        );
        gate.cancel_hits.fetch_add(1, Ordering::SeqCst);
        gate.changed.notify_waiters();
        gate.until(|| gate.cancel_released.load(Ordering::Acquire))
            .await;
    }

    #[cfg(test)]
    pub(crate) fn retirement_semantic_for_test(
        &self,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
    ) {
        let gate = self.retirement_owner_test_gate.lock().unwrap().clone();
        if let Some(gate) = gate.filter(|gate| gate.action_id == envelope.action_id) {
            assert_eq!(
                envelope.actor.execution_id.as_deref(),
                Some(gate.packet.graph_id())
            );
            gate.semantic_committed.store(true, Ordering::Release);
            gate.changed.notify_waiters();
        }
    }

    #[cfg(test)]
    pub(crate) async fn retirement_response_for_test(
        &self,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
    ) {
        let gate = self.retirement_owner_test_gate.lock().unwrap().clone();
        if let Some(gate) = gate.filter(|gate| gate.action_id == envelope.action_id) {
            gate.postcommit_returned.store(true, Ordering::Release);
            gate.changed.notify_waiters();
            gate.until(|| gate.response_released.load(Ordering::Acquire))
                .await;
        }
    }

    /// Stop accepting detached maintenance and await every retained task.
    pub async fn shutdown_maintenance(&self) {
        if let Some(report) = self.shutdown_skill_usage(Duration::from_secs(5)).await {
            if !report.clean {
                tracing::warn!(?report, "Runtime Skill usage shutdown incomplete");
            }
        }
        self.approval_queue.shutdown_deadline_scheduler().await;
        let report = self.event_reactor.shutdown().await;
        if !report.timed_out_lanes.is_empty() || !report.join_errors.is_empty() {
            tracing::warn!(
                ?report,
                "Runtime event reactor shutdown was not fully drained"
            );
        }
        self.maintenance_supervisor.shutdown_and_drain().await;
        self.resource_evidence_writer.shutdown_and_drain();
    }

    /// The process composition root receives an observe port to this same
    /// retained worker. Startup rollback and normal shutdown share its owner.
    pub fn skill_usage_recorder(&self) -> Result<Arc<crate::RuntimeSkillUsageRecorder>, String> {
        let mut slot = self
            .skill_usage_recorder
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.0 {
            return Err("Runtime Skill usage intake is closed".into());
        }
        if slot.1.is_none() {
            slot.1 = Some(Arc::new(
                crate::RuntimeSkillUsageRecorder::new(Arc::clone(&self.event_store))
                    .map_err(|e| format!("Skill usage worker failed to start: {e}"))?,
            ));
        }
        Ok(Arc::clone(slot.1.as_ref().unwrap()))
    }

    pub async fn shutdown_skill_usage(
        &self,
        timeout: Duration,
    ) -> Option<crate::SkillUsageShutdownReport> {
        let recorder = {
            let mut slot = self
                .skill_usage_recorder
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.0 = true;
            slot.1.clone()
        };
        match recorder {
            Some(recorder) => Some(recorder.shutdown_and_drain(timeout).await),
            None => None,
        }
    }

    /// Stop the one Runtime execution owner and return auditable per-owner
    /// queue/abort/error evidence to the process host.
    pub async fn shutdown_execution(&self) -> crate::RuntimeExecutionShutdownReport {
        self.agent_runtime.fence_run_admission();
        let mut report = self.execution_supervisor.shutdown().await;
        if let Err(error) = self.agent_runtime.drain_retained_runs().await {
            report.errors.push(error);
        }
        // Original Graph and retained Agent producers must physically drain
        // before the observation intake is fenced and its writer is joined.
        if let Some(usage) = self.shutdown_skill_usage(Duration::from_secs(5)).await {
            if !usage.clean {
                report
                    .errors
                    .push(format!("Skill usage shutdown incomplete: {usage:?}"));
            }
        }
        match self.event_store.has_unresolved_original_member_custody() {
            Ok(false) => {}
            Ok(true) => report.errors.push("original member custody remains unresolved".into()),
            Err(_) => report.errors.push("original member custody could not be inspected".into()),
        }
        report
    }

    #[must_use]
    pub fn execution_health(&self) -> crate::RuntimeExecutionHealth {
        self.execution_supervisor.health()
    }

    /// Return a stable copy for primary or delegated turn construction.
    /// Replacing the catalog later cannot alter an already-created turn.
    #[must_use]
    pub fn skill_catalog(&self) -> crate::RuntimeSkillCatalog {
        self.skill_catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Gateway's composition boundary may refresh an inspected package
    /// snapshot. Runtime retains sole ownership of activation and injection.
    pub fn replace_skill_catalog(&self, catalog: crate::RuntimeSkillCatalog) {
        *self
            .skill_catalog
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = catalog;
    }

    #[must_use]
    pub fn l4_promotion_service(&self) -> &Arc<crate::L4PromotionService> {
        &self.l4_promotion_service
    }

    #[must_use]
    pub fn knowledge_candidate_projector(&self) -> &Arc<crate::KnowledgeCandidateProjector> {
        &self.knowledge_candidate_projector
    }

    #[must_use]
    pub fn outcome_service(&self) -> &Arc<crate::execution_core::OutcomeService> {
        &self.outcome_service
    }

    #[must_use]
    pub fn outcome_projector(&self) -> &Arc<crate::OutcomeProjector> {
        &self.outcome_projector
    }

    /// Unified operational view for every sealed Runtime event projection.
    pub fn event_reactor_health(
        &self,
    ) -> Result<crate::RuntimeEventReactorHealth, RuntimeServicesError> {
        self.event_reactor
            .health()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn import_legacy_strategy_outcomes(
        &self,
        path: &Path,
    ) -> Result<crate::execution_core::LegacyOutcomeImportReceipt, RuntimeServicesError> {
        self.outcome_service
            .import_legacy_strategy_file(path)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn import_calibration_outcomes(
        &self,
        path: &Path,
    ) -> Result<crate::execution_core::CalibrationOutcomeImportReceipt, RuntimeServicesError> {
        let receipt = self
            .outcome_service
            .import_calibration_file(path)
            .map_err(RuntimeServicesError::Invariant)?;
        Ok(receipt)
    }

    /// Runtime-owned read port for Fact and Matrix context. Each call requires
    /// a Binding and verifies its data lease before exposing model context.
    #[must_use]
    pub fn reality_recall_port(&self) -> &Arc<RealityRecallPort> {
        &self.reality_recall_port
    }

    #[must_use]
    pub fn knowledge_activation(&self) -> crate::knowledge_activation::KnowledgeActivationRuntime {
        self.knowledge_activation.clone()
    }

    /// Runtime-owned, scope-qualified Agent and Team Definition registry.
    /// Gateway and surfaces consume this projection; they do not scan
    /// arbitrary workspace directories to construct runnable identities.
    #[must_use]
    pub fn definition_registry(&self) -> &Arc<RuntimeDefinitionRegistry> {
        &self.definition_registry
    }

    /// Rebuild the executable Agent index after a Definition release state
    /// changes. The operation replaces, rather than merges, cached entries so
    /// stopped or revoked Definitions cannot remain selectable.
    pub fn refresh_definition_catalog(&self) -> Result<(), RuntimeServicesError> {
        self.agent_runtime
            .catalog()
            .replace_all(self.definition_registry.runnable_agent_catalog()?);
        Ok(())
    }

    /// Import one caller-selected external Agent TOML document as a local
    /// Draft through the Runtime ownership boundary.
    ///
    /// This command deliberately does not release the Definition, update a
    /// default pointer, or refresh the runnable catalog. Those are separate
    /// human-authorized Runtime commands, so an import can never become an
    /// executable identity merely by reaching the Gateway.
    pub fn import_agent_toml_draft(
        &self,
        import: ExplicitTomlAgentImport,
    ) -> Result<AgentDefinitionDraftReceipt, RuntimeServicesError> {
        self.definition_registry
            .import_agent_toml_draft(import)
            .map_err(RuntimeServicesError::from)
    }

    /// Compile one exact Agent Definition execution Binding through the
    /// Runtime composition root. Gateway, Teams and Surfaces submit only a
    /// restricted request; they never resolve Definition paths or construct
    /// executable snapshots themselves.
    pub(crate) fn prepare_skill_binding_request(
        &self,
        request: &mut AgentBindingRequest,
        captured: Option<&(
            Vec<harness_contract::skill::SkillInstructionPin>,
            Vec<String>,
        )>,
    ) -> Result<(), RuntimeServicesError> {
        if request.allowed_skill_refs.is_empty() {
            if captured.is_some_and(|(pins, tools)| !pins.is_empty() || !tools.is_empty()) {
                return Err(RuntimeServicesError::Invariant(
                    "Skill pins outside the Binding ceiling".into(),
                ));
            }
            request.skill_tool_pins = None;
            request.skill_tool_refs = None;
            return Ok(());
        }
        let source = self.skill_catalog().instruction_source().ok_or_else(|| {
            RuntimeServicesError::Invariant("Skill Binding has no original approval source".into())
        })?;
        let pins = match captured {
            Some((pins, _)) => pins.clone(),
            None => source
                .required_tool_pins(&request.allowed_skill_refs)
                .map_err(RuntimeServicesError::Invariant)?,
        };
        if request
            .allowed_skill_refs
            .iter()
            .any(|reference| !pins.iter().any(|pin| pin.matches_reference(reference)))
            || pins.iter().any(|pin| {
                !request
                    .allowed_skill_refs
                    .iter()
                    .any(|reference| pin.matches_reference(reference))
            })
        {
            return Err(RuntimeServicesError::Invariant(
                "Skill Binding does not match its original source read cut".into(),
            ));
        }
        source
            .validate_pins(&pins)
            .map_err(RuntimeServicesError::Invariant)?;
        // Generic callers already supply independently authorized tools. Only
        // original Agentic admission may contribute an additional Skill set.
        let tools = captured.map(|(_, tools)| tools.clone()).unwrap_or_default();
        if tools.iter().any(|tool| {
            !request.allowed_tool_contract_refs.contains(tool)
                || !pins.iter().any(|pin| pin.tool_refs.contains(tool))
        }) {
            return Err(RuntimeServicesError::Invariant(
                "foreign Skill tool contribution".into(),
            ));
        }
        request.skill_tool_refs = Some(tools);
        request.skill_tool_pins = Some(pins);
        Ok(())
    }

    pub fn compile_agent_binding(
        &self,
        mut request: AgentBindingRequest,
    ) -> Result<CompiledAgentBinding, RuntimeServicesError> {
        self.prepare_skill_binding_request(&mut request, None)?;
        self.compile_agent_binding_captured(request)
    }

    fn compile_agent_binding_captured(
        &self,
        request: AgentBindingRequest,
    ) -> Result<CompiledAgentBinding, RuntimeServicesError> {
        let routing_identity = format!(
            "{}|{}|{}|{}",
            request.session_id,
            request.task_id,
            request.instance_id,
            request.team_id.as_deref().unwrap_or("direct")
        );
        let compiler = AgentBindingCompiler::new(Arc::clone(&self.definition_registry));
        let selected_canary = self
            .evolution_governance
            .select_agent_canary_assignment(
                &request.definition_id,
                &request.selector,
                &routing_identity,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        if let Some(assignment) = selected_canary {
            let crate::EvolutionCandidateSubject::AgentDefinition { revision_ref } =
                &assignment.subject;
            let resolved = self
                .definition_registry
                .resolve_agent_canary(revision_ref)
                .map_err(RuntimeServicesError::from)?;
            return compiler
                .compile_resolved(
                    request,
                    resolved,
                    Some(AgentReleaseBinding {
                        assignment_id: assignment.assignment_id,
                        generation: assignment.generation,
                        channel: ReleaseChannel::Canary,
                    }),
                )
                .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()));
        }
        compiler
            .compile(request)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))
    }

    /// Turn one non-executable planning intent into the immutable packet that
    /// a graph runner may persist.  This is the sole Runtime-owned boundary
    /// where a catalog choice becomes an instance identity and data lease.
    pub fn compile_agent_task_intent(
        &self,
        intent: AgentTaskIntent,
    ) -> Result<AgentTaskPacket, RuntimeServicesError> {
        self.compile_agent_task_intent_captured(intent, None)
    }

    fn compile_agent_task_intent_captured(
        &self,
        intent: AgentTaskIntent,
        pins: Option<&(
            Vec<harness_contract::skill::SkillInstructionPin>,
            Vec<String>,
        )>,
    ) -> Result<AgentTaskPacket, RuntimeServicesError> {
        let execution_identity = self.prepare_agent_task_intent(&intent)?;
        let policy_revision = self.canonical_task_policy_revision(&intent.task_id)?;
        let selected = intent
            .selected_agent_id
            .as_deref()
            .and_then(|agent_id| self.agent_runtime.catalog().get(agent_id));
        let mut request = request_for_intent(&intent, selected)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        self.prepare_skill_binding_request(&mut request, pins)?;
        let compiled = self.compile_agent_binding_captured(request)?;
        let mut packet = compiled
            .snapshot
            .compile_task_packet(intent, execution_identity)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        packet.policy_revision = policy_revision;
        Ok(packet)
    }

    fn canonical_task_policy_revision(&self, task_id: &str) -> Result<u64, RuntimeServicesError> {
        let task = crate::TaskRuntimePort::new(self)
            .get(task_id)
            .map_err(RuntimeServicesError::Task)?
            .ok_or_else(|| {
                RuntimeServicesError::Invariant(format!(
                    "Agent task `{task_id}` has no canonical Task aggregate"
                ))
            })?;
        let binding = task.execution_policy.binding.as_ref().ok_or_else(|| {
            RuntimeServicesError::Invariant(format!(
                "Agent task `{task_id}` has no canonical execution-policy binding"
            ))
        })?;
        binding
            .validate()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(binding.execution.policy_revision)
    }

    fn ensure_evolution_execution_policy(
        &self,
        session_id: &str,
    ) -> Result<(), RuntimeServicesError> {
        let expected = harness_contract::policy::SessionExecutionPolicy::from_profile(
            harness_contract::policy::AutonomyProfileId::Cautious,
            1,
            harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
        );
        match self.session_execution_policy(session_id) {
            Some(current) if current == expected => Ok(()),
            Some(_) => Err(RuntimeServicesError::Invariant(format!(
                "evolution evaluation Session `{session_id}` has a non-canonical execution policy"
            ))),
            None => {
                self.publish_session_execution_policy(
                    session_id,
                    crate::permissions::SessionExecutionPolicyControl::from_policy(expected),
                );
                Ok(())
            }
        }
    }

    fn prepare_agent_task_intent(
        &self,
        intent: &AgentTaskIntent,
    ) -> Result<ExecutionIdentity, RuntimeServicesError> {
        let task_port = crate::TaskRuntimePort::new(self);
        match task_port
            .get(&intent.task_id)
            .map_err(RuntimeServicesError::Task)?
        {
            Some(task)
                if task.mission_id == intent.mission_id
                    && task.origin_session_id == intent.session_id
                    && task.origin_turn_id == intent.source_turn_id
                    && task.root_task_id == intent.root_task_id
                    && task.parent_task_id == intent.parent_task_id => {}
            Some(_) => {
                return Err(RuntimeServicesError::Invariant(format!(
                    "Agent task `{}` conflicts with its canonical Task aggregate lineage",
                    intent.task_id
                )));
            }
            None => {
                let mut spec = harness_contract::task::TaskSpec::new(intent.objective.clone());
                spec.execution_policy.max_failures_before_block = 3;
                let spec = if let Some(parent_task_id) = intent.parent_task_id.as_deref() {
                    task_port
                        .bind_inherited_task_spec(parent_task_id, intent.permission_ceiling, spec)
                        .map_err(RuntimeServicesError::Task)?
                } else {
                    task_port
                        .bind_task_spec(&intent.session_id, None, spec)
                        .map_err(RuntimeServicesError::Task)?
                };
                let command = harness_contract::task::TaskCreateCommand {
                    task_id: intent.task_id.clone(),
                    mission_id: intent.mission_id.clone(),
                    kind: if intent.task_id == intent.root_task_id {
                        harness_contract::task::TaskKind::Root
                    } else {
                        harness_contract::task::TaskKind::Delegated
                    },
                    origin: if intent.task_id == intent.root_task_id {
                        harness_contract::task::TaskOrigin::User
                    } else {
                        harness_contract::task::TaskOrigin::Delegated
                    },
                    origin_session_id: intent.session_id.clone(),
                    origin_turn_id: intent.source_turn_id.clone(),
                    root_task_id: intent.root_task_id.clone(),
                    parent_task_id: intent.parent_task_id.clone(),
                    predecessor_task_id: None,
                    mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
                    mission_assigned_by: "runtime.agent".to_string(),
                    spec,
                    evidence_refs: Vec::new(),
                };
                if command.parent_task_id.is_some() {
                    task_port
                        .create_inherited(command)
                        .map_err(RuntimeServicesError::Task)?;
                } else {
                    task_port
                        .create(command)
                        .map_err(RuntimeServicesError::Task)?;
                }
            }
        }
        let graph_identity = ExecutionIdentity::for_task_graph(
            intent.principal_id.clone(),
            self.workspace_key.clone(),
            intent.mission_id.clone(),
            intent.task_id.clone(),
            intent.session_id.clone(),
            intent.source_turn_id.clone(),
            intent.graph_id.clone(),
        )
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let parent_identity = if let Some(team_id) = &intent.team_id {
            ExecutionIdentity::for_team_node(&graph_identity, team_id, &intent.node_id)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
        } else {
            graph_identity
        };
        let execution_identity =
            ExecutionIdentity::for_agent_node(&parent_identity, &intent.run_id, &intent.node_id)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(execution_identity)
    }

    /// Validate release provenance immediately before a compiled Agent packet
    /// is admitted. Canary packets are deliberately rechecked because a
    /// human StopCanary decision may occur after planning but before worker
    /// start; Stable packets continue through the normal Definition resolver.
    pub(crate) fn validate_agent_binding_release(
        &self,
        binding: &harness_contract::agent::AgentBindingSnapshot,
    ) -> Result<(), RuntimeServicesError> {
        binding
            .validate()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let pins = binding
            .skill_tool_pins
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|pin| {
                pin.tool_refs.iter().any(|tool| {
                    binding
                        .skill_tool_refs
                        .as_deref()
                        .unwrap_or_default()
                        .contains(tool)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if !pins.is_empty() {
            self.skill_catalog()
                .instruction_source()
                .ok_or_else(|| {
                    RuntimeServicesError::Invariant(
                        "Agent Skill tools have no original approval source".into(),
                    )
                })?
                .validate_pins(&pins)
                .map_err(RuntimeServicesError::Invariant)?;
        }
        let Some(release) = &binding.release else {
            return Ok(());
        };
        match release.channel {
            ReleaseChannel::Canary => self
                .evolution_governance
                .validate_agent_canary_binding(
                    &binding.definition_ref,
                    &release.assignment_id,
                    release.generation,
                )
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string())),
            ReleaseChannel::Stable | ReleaseChannel::Shadow => {
                Err(RuntimeServicesError::Invariant(
                    "Runtime only accepts explicit release provenance for active Canary Bindings"
                        .to_string(),
                ))
            }
        }
    }

    /// Compile every Agent intent before graph registration.  A graph that
    /// already contains an executable packet is rejected instead of being
    /// silently re-bound, preventing recovery or a surface from replacing an
    /// immutable Binding after planning.
    pub fn compile_graph_agent_intents(
        &self,
        mut graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, RuntimeServicesError> {
        self.compile_agent_task_nodes_captured(
            &mut graph.nodes,
            &std::collections::BTreeMap::new(),
        )?;
        Ok(graph)
    }

    pub(crate) fn compile_graph_agent_intents_with_skill_pins(
        &self,
        mut graph: ExecutionGraph,
        pins: &std::collections::BTreeMap<
            String,
            (
                Vec<harness_contract::skill::SkillInstructionPin>,
                Vec<String>,
            ),
        >,
    ) -> Result<ExecutionGraph, RuntimeServicesError> {
        if pins.keys().any(|id| {
            !graph
                .nodes
                .iter()
                .any(|node| &node.id == id && node.kind == ExecutionNodeKind::AgentTask)
        }) {
            return Err(RuntimeServicesError::Invariant(
                "unconsumed Skill admission source".into(),
            ));
        }
        self.compile_agent_task_nodes_captured(&mut graph.nodes, pins)?;
        Ok(graph)
    }

    /// Resolve only newly-added semantic Agent nodes during a graph revision.
    /// Existing immutable packets are deliberately not rebound.
    pub fn compile_agent_task_nodes(
        &self,
        nodes: &mut [harness_contract::execution_graph::ExecutionNodeSpec],
    ) -> Result<(), RuntimeServicesError> {
        self.compile_agent_task_nodes_captured(nodes, &std::collections::BTreeMap::new())
    }

    fn compile_agent_task_nodes_captured(
        &self,
        nodes: &mut [harness_contract::execution_graph::ExecutionNodeSpec],
        pins: &std::collections::BTreeMap<
            String,
            (
                Vec<harness_contract::skill::SkillInstructionPin>,
                Vec<String>,
            ),
        >,
    ) -> Result<(), RuntimeServicesError> {
        for node in nodes {
            if node.kind != ExecutionNodeKind::AgentTask {
                continue;
            }
            let intent: AgentTaskIntent = serde_json::from_str(&node.payload_ref).map_err(|_| {
                RuntimeServicesError::AgentRuntime(format!(
                    "AgentTask node `{}` must contain an unbound AgentTaskIntent before registration",
                    node.id
                ))
            })?;
            if intent.node_id != node.id {
                return Err(RuntimeServicesError::AgentRuntime(format!(
                    "AgentTask intent node identity `{}` does not match graph node `{}`",
                    intent.node_id, node.id
                )));
            }
            let packet = self.compile_agent_task_intent_captured(intent, pins.get(&node.id))?;
            if let Some(work) = node.work.as_mut() {
                work.model_profile = packet
                    .binding
                    .as_ref()
                    .map(|binding| binding.model_policy.profile.clone())
                    .or_else(|| Some(packet.model_lease.clone()));
                work.context_view_ref = Some(packet.budget_lease.lease_id.clone());
            }
            node.payload_ref = serde_json::to_string(&packet).map_err(|error| {
                RuntimeServicesError::AgentRuntime(format!(
                    "encode Runtime-bound AgentTask node `{}`: {error}",
                    node.id
                ))
            })?;
        }
        Ok(())
    }
    pub(crate) fn event_store(&self) -> &Arc<RuntimeEventStore> {
        &self.event_store
    }

    /// Test-only registration on this Runtime's original event store.
    #[cfg(feature = "test-fixtures")]
    pub fn fixture_arm_original_goal_read(
        &self, graph: String, call: String, goal_stream: String,
        predicate: impl Fn(&RuntimeEventStore, &str, &str, u32) -> bool + Send + Sync + 'static,
        action: impl FnOnce(&RuntimeEventStore, &str, &str, u32) + Send + 'static,
    ) {
        self.event_store.fixture_arm_original_goal_read(graph, call, goal_stream, predicate, action);
    }

    /// Returns the single Agent-first collaboration mutation owner. The
    /// service is a cheap handle over the workspace event journal; callers do
    /// not receive direct event-store access.
    #[must_use]
    pub fn agent_action_service(&self) -> crate::AgentActionService {
        self.agentic_actions.clone()
    }

    /// Ask the sole ObjectiveSupervisor to reduce one durable Agentic
    /// completion request, then bind its GoalStore terminal back to Program.
    /// Model/Gateway callers cannot supply or infer the verdict.
    pub fn reconcile_agentic_completion_request(
        &self,
        program_id: &str,
    ) -> Result<Option<crate::AgenticProgramProjection>, String> {
        crate::agentic::supervision::reconcile_completion_request(
            self,
            &self.agent_action_service(),
            self.objective_supervisor.as_ref(),
            program_id,
        )
    }

    /// Synchronously drive the original conclusion owner with a test-only
    /// hook for exactly one Program transaction. The boolean is true only if
    /// the original guarded commit point was reached on this same thread.
    #[cfg(feature = "test-fixtures")]
    pub fn reconcile_with_conclusion_append_hook_for_test(
        &self, program_id: &str, transaction_id: &str,
        hook: impl FnOnce(&crate::AppendTransactionRequest) + 'static,
    ) -> (Result<Option<crate::AgenticProgramProjection>, String>, bool, Option<ConclusionAttemptObservation>) {
        let prefix = format!("program-conclusion:{program_id}:");
        if transaction_id.strip_prefix(&prefix).and_then(|revision| revision.parse::<u64>().ok()).is_none() {
            return (Err("conclusion hook transaction does not match Program".into()), false, None);
        }
        crate::AgentActionService::with_targeted_conclusion_append_hook_for_test(
            transaction_id.to_owned(), hook, || self.reconcile_agentic_completion_request(program_id),
        )
    }

    /// Arm the existing original conclusion hook before the live owner reaches
    /// its guarded commit. No authority is created by this fixture selector.
    #[cfg(feature = "test-fixtures")]
    pub fn fixture_arm_original_conclusion(
        &self, program_id: String, goal_id: String,
        hook: impl FnOnce(&crate::AppendTransactionRequest) + Send + 'static,
        observe: impl FnOnce(bool, Option<ConclusionAttemptObservation>) + Send + 'static,
    ) -> Result<(), String> {
        self.event_store.fixture_arm_original_conclusion(program_id, goal_id, hook, observe)
    }

    /// Resolve an actor from the request's physical attempt and its original
    /// graph-start ticket. The caller must also fence publication on the graph.
    pub async fn resolve_agent_action_actor_for_attempt(
        &self,
        parent: &harness_contract::execution_graph::ExecutionParentBinding,
        attempt: u32,
        trusted_root: Option<harness_contract::agent_action::AgentActorBinding>,
    ) -> Result<harness_contract::agent_action::AgentActorBinding, String> {
        use harness_contract::execution_graph::ExecutionNodeStatus;
        if attempt == 0 {
            return Err("agent_actor_attempt_missing".into());
        }
        let graph = self.graph_state_store.load_async(&parent.execution_id).await
            .map_err(|e| format!("agent_actor_graph_load_failed:{e}"))?;
        if graph.id != parent.execution_id
            || graph.node_statuses.get(&parent.node_id) != Some(&ExecutionNodeStatus::Running)
            || graph.recovery_cursor.node_attempts.get(&parent.node_id) != Some(&attempt)
        {
            return Err("agent_actor_attempt_not_running".into());
        }
        let binding = self.graph_state_store.current_binding(&graph, &parent.node_id, attempt)
            .map_err(|e| format!("agent_actor_original_binding_invalid:{e}"))?;
        let (ticket, original_node) = self.graph_state_store
            .binding_start_ticket(&graph, &parent.node_id, &binding)
            .map_err(|e| format!("agent_actor_original_ticket_invalid:{e}"))?;
        let start = crate::execution_core::graph::validate_node_ticket_start(
            &self.event_store, &ticket, None,
        ).map_err(|e| format!("agent_actor_original_start_invalid:{e}"))?;
        if ticket.graph_id != parent.execution_id || ticket.node_id != parent.node_id
            || ticket.attempt != attempt
            || start.nodes.iter().find(|n| n.id == parent.node_id) != Some(&original_node)
            || graph.nodes.iter().find(|n| n.id == parent.node_id) != Some(&original_node)
        {
            return Err("agent_actor_original_ticket_mismatch".into());
        }
        let mut actor = self.resolve_agent_action_actor(parent, trusted_root).await?;
        if actor.original_execution.is_some() {
            return Err("agent_actor_unexpected_original_execution".into());
        }
        actor.original_execution = Some(harness_contract::agent_action::OriginalAgentExecution {
            graph_id: parent.execution_id.clone(), node_id: parent.node_id.clone(), attempt,
        });
        Ok(actor)
    }

    /// Resolve a delegated model's immutable Agent-first identity from the
    /// graph packet supplied by Runtime. No identity or scope is accepted from
    /// model-authored tool input.
    pub async fn resolve_agent_action_actor(
        &self,
        parent: &harness_contract::execution_graph::ExecutionParentBinding,
        trusted_root: Option<harness_contract::agent_action::AgentActorBinding>,
    ) -> Result<harness_contract::agent_action::AgentActorBinding, String> {
        if trusted_root.as_ref().is_some_and(|root| root.original_execution.is_some()) {
            return Err("agent_actor_untrusted_original_execution".into());
        }
        let graph = self
            .graph_state_store
            .load_async(&parent.execution_id)
            .await
            .map_err(|error| format!("agent_actor_graph_load_failed:{error}"))?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == parent.node_id)
            .ok_or_else(|| "agent_actor_node_not_found".to_string())?;
        if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
            let mut root =
                trusted_root.ok_or_else(|| "agent_actor_node_is_not_agent_task".to_string())?;
            let lineage = graph
                .lineage
                .as_ref()
                .ok_or_else(|| "agent_actor_root_graph_has_no_lineage".to_string())?;
            let objective_id = harness_contract::agent_action::root_objective_id(
                &lineage.session_id,
                &lineage.turn_id,
            );
            let program_id =
                harness_contract::agent_action::program_id_for_objective(&objective_id);
            if graph.parent_execution.is_some()
                || root.kind != harness_contract::agent_action::AgentActorKind::Root
                || root.session_id != lineage.session_id
                || root.turn_id != lineage.turn_id
                || root.objective_id != objective_id
                || root.program_id != program_id
                || root.root_execution_id.as_deref() != Some(parent.execution_id.as_str())
                || root.actor_id != format!("root:{}", lineage.session_id)
                || root.execution_id.is_some()
                || root.team_id.is_some()
                || root.agent_id.is_some()
            {
                return Err("agent_actor_root_binding_mismatch".to_string());
            }

            // The current Agent Action node is an internal Runtime mutation
            // and therefore has no file scope of its own. Delegation authority
            // belongs to the immutable root graph resource contract compiled
            // at turn admission. Copy that trusted contract into the first
            // Program action; later actions inherit the frozen Program below.
            root.resource_scopes.extend(
                graph
                    .nodes
                    .iter()
                    .flat_map(|candidate| candidate.resource_scopes.iter().cloned()),
            );
            root.resource_scopes.sort();
            root.resource_scopes.dedup();
            root.objective_summary.clone_from(&graph.objective);

            // The first accepted root action freezes the Program's semantic
            // and authorization binding. Dynamic model/replan nodes may carry
            // a refreshed decision snapshot, but they must never become a
            // second source of truth for the same Program. Keep validating the
            // immutable graph/session/turn identity above, then inherit the
            // durable Program authority for every continuation action.
            if let Some(program) = self
                .agent_action_service()
                .project_if_exists(&program_id)
                .map_err(|error| format!("agent_actor_program_load_failed:{error}"))?
            {
                if program.objective_id != objective_id
                    || program.session_id != lineage.session_id
                    || program.turn_id != lineage.turn_id
                    || program.root_execution_id.as_deref() != Some(parent.execution_id.as_str())
                {
                    return Err("agent_actor_program_binding_mismatch".to_string());
                }
                root.required_team_count = program.required_team_count;
                root.exact_team_count = program.exact_team_count;
                root.objective_summary = program.objective_summary;
                root.model_lease = program.model_lease;
                root.permission_ceiling = Some(program.permission_ceiling);
                root.resource_scopes = program.resource_scopes;
            }
            return Ok(root);
        }
        let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
            .map_err(|error| format!("agent_actor_packet_invalid:{error}"))?;
        if graph.id != parent.execution_id
            || packet.graph_id() != parent.execution_id
            || packet.node_id() != parent.node_id
        {
            return Err("agent_actor_packet_parent_binding_mismatch".to_string());
        }
        let lineage = graph
            .lineage
            .as_ref()
            .ok_or_else(|| "agent_actor_graph_has_no_lineage".to_string())?;
        if packet.session_id() != lineage.session_id
            || packet.task_id() != lineage.task_id
            || packet.assignment.root_task_id != lineage.root_task_id
            || packet.assignment.execution_identity.turn_id() != Some(lineage.turn_id.as_str())
        {
            return Err("agent_actor_packet_graph_lineage_mismatch".to_string());
        }
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "agent_actor_packet_has_no_runtime_binding".to_string())?;
        binding
            .validate()
            .map_err(|error| format!("agent_actor_packet_binding_invalid:{error}"))?;
        if packet.agent_id() != binding.instance.instance_id
            || binding.data_lease.session_id != packet.session_id()
            || binding.data_lease.task_id != packet.task_id()
            || binding.data_lease.team_id.as_deref() != packet.team_id()
            || crate::agent::binding::recompute_binding_digest(binding)
                .map_err(|error| format!("agent_actor_packet_binding_invalid:{error}"))?
                != binding.binding_digest
        {
            return Err("agent_actor_packet_binding_mismatch".to_string());
        }
        packet
            .validate_cohort_prompt_package()
            .map_err(|error| format!("agent_actor_packet_cohort_mismatch:{error}"))?;

        let agentic = packet
            .agentic_binding
            .as_ref()
            .ok_or_else(|| "agent_actor_packet_has_no_typed_agentic_binding".to_string())?;
        agentic
            .validate()
            .map_err(|error| format!("agent_actor_packet_agentic_binding_invalid:{error}"))?;
        let projection = self
            .agent_action_service()
            .project(&agentic.program_id)
            .map_err(|error| format!("agent_actor_program_load_failed:{error}"))?;
        if projection.execution_is_coordinator(Some(packet.graph_id()))
            && !matches!(
                agentic.focus,
                harness_contract::agent::AgenticExecutionFocus::Coordination { .. }
            )
        {
            return Err("agent_actor_coordination_focus_mismatch".into());
        }
        let (task_id, mode) = match &agentic.focus {
            harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => {
                (task_ref.as_str(), "execute")
            }
            harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
                (task_ref.as_str(), "review")
            }
            harness_contract::agent::AgenticExecutionFocus::Coordination { wake_ref } => {
                let (_, wake) = projection
                    .coordination_wake_ref(wake_ref)
                    .ok_or("agent_actor_coordination_wake_missing")?;
                let consumption = wake
                    .coordination
                    .as_ref()
                    .ok_or("agent_actor_coordination_not_registered")?;
                if consumption.settled
                    || consumption.execution_id != packet.graph_id()
                    || consumption.agent_id != agentic.agent_id
                    || consumption.membership_id != agentic.membership_id
                    || projection.coordination_attempt(packet.graph_id()).is_none()
                    || projection.coordination_team_id_for(&agentic.agent_id, wake.revision)
                        != Some(agentic.team_id.as_str())
                {
                    return Err("agent_actor_coordination_binding_mismatch".into());
                }
                (
                    wake.intent
                        .as_ref()
                        .ok_or("agent_actor_coordination_intent_missing")?
                        .task_ref
                        .as_str(),
                    "coordination",
                )
            }
            _ => return Err("agent_actor_packet_focus_cannot_mutate_task".to_string()),
        };
        let program_id = agentic.program_id.as_str();
        let team_id = agentic.team_id.as_str();
        let task_team_id = agentic.task_team_id.as_str();
        let agent_id = agentic.agent_id.as_str();

        let parent_execution_id = graph
            .parent_execution
            .as_ref()
            .map(|binding| binding.execution_id.as_str());
        if projection.session_id != lineage.session_id
            || projection.turn_id != lineage.turn_id
            || projection.root_execution_id.as_deref() != parent_execution_id
        {
            return Err("agent_actor_program_graph_lineage_mismatch".to_string());
        }
        let _member = projection
            .agents
            .get(agent_id)
            .ok_or_else(|| "agent_actor_is_not_in_program_roster".to_string())?;
        if !projection.agent_is_active_in(agent_id, team_id) {
            return Err("agent_actor_agentic_team_mismatch".to_string());
        }
        if projection
            .membership_for(agent_id, team_id)
            .is_none_or(|membership| membership.membership_id != agentic.membership_id)
        {
            return Err("agent_actor_agentic_membership_mismatch".to_string());
        }
        let task = projection
            .tasks
            .get(task_id)
            .ok_or_else(|| "agent_actor_task_is_not_in_program".to_string())?;
        if task.team_id != task_team_id
            || packet.task_id() != projection.execution_task_id(task_id)?
            || (mode == "execute" && !projection.agent_is_active_in(agent_id, &task.team_id))
        {
            return Err("agent_actor_agentic_task_mismatch".to_string());
        }
        let display = binding
            .display
            .as_ref()
            .ok_or_else(|| "agent_actor_packet_has_no_display_identity".to_string())?;
        if display.agent_id != agent_id
            || display.provenance != format!("runtime.agentic:{program_id}")
        {
            return Err("agent_actor_display_identity_mismatch".to_string());
        }
        return Ok(harness_contract::agent_action::AgentActorBinding {
            original_execution: None,
            objective_id: projection.objective_id,
            program_id: program_id.to_string(),
            session_id: projection.session_id,
            turn_id: projection.turn_id,
            root_execution_id: projection.root_execution_id,
            required_team_count: projection.required_team_count,
            exact_team_count: projection.exact_team_count,
            objective_summary: projection.objective_summary,
            model_lease: projection.model_lease,
            permission_ceiling: Some(projection.permission_ceiling),
            resource_scopes: projection.resource_scopes,
            actor_id: agent_id.to_string(),
            kind: harness_contract::agent_action::AgentActorKind::Agent,
            execution_id: Some(parent.execution_id.clone()),
            team_id: Some(team_id.to_string()),
            agent_id: Some(agent_id.to_string()),
        });
    }

    /// Bind the live bus owned by one root Session execution. Nested Agents
    /// resolve this request-local registry through their immutable graph
    /// parent binding; no event transport handles enter serialized contracts.
    pub(crate) fn bind_active_execution_bus(
        &self,
        execution_id: impl Into<String>,
        bus: crate::CowdEventBus,
    ) -> Result<ActiveExecutionBusLease, String> {
        let execution_id = execution_id.into();
        let generation = self
            .live_execution_store
            .allocate_model_usage_generation(&execution_id)?;
        let mut buses = self
            .active_execution_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buses
            .get(&execution_id)
            .is_some_and(|active| active.generation >= generation)
        {
            return Err(format!(
                "stale execution bus generation {generation} cannot replace the active binding for `{execution_id}`"
            ));
        }
        let live_execution_store = Arc::clone(&self.live_execution_store);
        bus.bind_live_telemetry_observer(generation, move |event: &crate::CowdEvent| {
            if let Some(context) = event.execution_context() {
                live_execution_store.observe_event_with_generation(
                    &context.session_id,
                    event,
                    generation,
                );
            }
        });
        buses.insert(
            execution_id.clone(),
            ActiveExecutionBus {
                generation,
                bus: bus.clone(),
                session_fence: None,
            },
        );
        drop(buses);
        Ok(ActiveExecutionBusLease {
            execution_id,
            generation,
            bus,
            buses: Arc::clone(&self.active_execution_buses),
            session_owners: Arc::clone(&self.original_session_owners),
        })
    }

    #[must_use]
    pub(crate) fn active_execution_bus(&self, execution_id: &str) -> Option<crate::CowdEventBus> {
        self.active_execution_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .map(|active| active.bus.clone())
    }

    /// Resolve the root Session event bus for any nested execution graph.
    ///
    /// Team graphs are normally two levels below SessionIngress
    /// (Mission -> Team), while Agent graphs add another level. The live bus
    /// registry intentionally contains only root Session executions, so a
    /// direct lookup is insufficient for nested Agents.
    #[must_use]
    pub(crate) fn resolve_active_execution_bus(
        &self,
        execution_id: &str,
    ) -> Option<(String, crate::CowdEventBus)> {
        self.resolve_active_execution_bus_with_fence(execution_id)
            .map(|(id, bus, _)| (id, bus))
    }

    pub(crate) fn resolve_active_execution_bus_with_fence(
        &self, execution_id: &str,
    ) -> Option<(String, crate::CowdEventBus, Option<ActiveSessionFence>)> {
        const MAX_LINEAGE_DEPTH: usize = 32;
        let mut candidate = execution_id.to_string();
        let mut visited = std::collections::BTreeSet::new();
        for _ in 0..MAX_LINEAGE_DEPTH {
            if !visited.insert(candidate.clone()) {
                return None;
            }
            let active = self.active_execution_buses.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&candidate).cloned();
            if let Some(active) = active {
                return Some((candidate, active.bus, active.session_fence));
            }
            candidate = self
                .graph_state_store
                .load(&candidate)
                .ok()?
                .parent_execution?
                .execution_id;
        }
        None
    }

    pub(crate) fn commit_owned_program_conclusion(
        &self, program: &crate::AgenticProgramProjection,
        prepared: crate::execution_core::goal::PreparedProgramConclusion,
    ) -> Result<crate::AgenticProgramProjection, crate::AgentActionServiceError> {
        let actions = self.agent_action_service();
        if program.completion_request.is_some()
            && actions.authenticated_existing_request_conclusion(program)?
        { return actions.project(&program.program_id); }
        let owner = self.original_conclusion_owner(program)
            .ok_or_else(|| crate::AgentActionServiceError::EventStore("original active Session conclusion owner unavailable".into()))?;
        self.with_conclusion_policy(&owner, |_| {
            let turn = harness_contract::turn::TurnId::from_string(owner.claim.turn_id());
            if owner.input.active_turn_id().as_ref() != Some(&turn) {
                return Err("original Session active turn changed".into());
            }
            let fence = owner.claim.fence()?;
            let cursor = owner.input.highest_consumed_cursor(&turn);
            if owner.input.active_turn_id().as_ref() != Some(&turn) {
                return Err("original Session active turn changed while reading cursor".into());
            }
            let consumed = match cursor {
                Some(cursor) if cursor.generation == fence.session_generation =>
                    usize::try_from(cursor.sequence).map_err(|_| "original consumed input sequence overflow")?.max(fence.input_sequence),
                Some(_) => return Err("original consumed cursor generation mismatch".into()),
                None => fence.input_sequence,
            };
            #[cfg(feature = "test-fixtures")]
            if prepared.goal.completion == harness_contract::goal::GoalCompletion::Satisfied {
                if let Some(request) = program.completion_request.as_ref() {
                    if let Some(probe) = self.event_store.fixture_take_original_conclusion(&program.program_id, &prepared.goal.id)? {
                        let transaction_id = format!("program-conclusion:{}:{}", program.program_id, request.program_revision);
                        let (result, reached, observation) = crate::AgentActionService::with_targeted_conclusion_append_hook_for_test(
                            transaction_id, probe.hook,
                            || actions.commit_program_conclusion_guarded(program, prepared, &owner, consumed));
                        (probe.observe)(reached, observation);
                        return Ok(result);
                    }
                }
            }
            Ok(actions.commit_program_conclusion_guarded(program, prepared, &owner, consumed))
        }).map_err(crate::AgentActionServiceError::EventStore)?
    }

    pub(crate) fn original_terminal_owner(self: &Arc<Self>, ticket: &crate::execution_core::graph::NodeExecutionTicket, session: &str, turn: &str) -> Option<Arc<OriginalTerminalCommitOwner>> {
        let (_, _, active) = self.resolve_active_execution_bus_with_fence(&ticket.graph_id)?;
        let active = active?;
        let (_, root_control, claim, input, requested) = active.for_lineage(session, turn)?;
        if claim.session_id() != session || claim.turn_id() != turn { return None; }
        let control = root_control.token().member_commit_control();
        requested.register(&control);
        let owner = OriginalConclusionOwner { claim, input, control, policy: active.original_policy.clone() };
        Some(Arc::new(OriginalTerminalCommitOwner {
            store: Arc::downgrade(&self.event_store),
            policy_controls: Arc::clone(&self.session_execution_policy_controls),
            admission_blocks: Arc::clone(&self.session_execution_policy_admission_blocks),
            release_after_terminal: Arc::new({
                let services = Arc::downgrade(self);
                move |session_id: &str, claim: &OriginalSessionClaim| -> bool {
                    services.upgrade().is_some_and(|services| {
                        services.release_original_session_owner_after_terminal(session_id, claim)
                    })
                }
            }),
            owner, ticket: ticket.clone(), terminal: Mutex::new(OriginalTerminalClosureState::default()),
        }))
    }

    /// Borrow the active ingress's original policy, input and cancellation
    /// control for the synchronous bounded decision transaction. No serialized
    /// request or event can construct this owner.
    pub(crate) fn with_original_bounded_decision<T>(
        &self, ticket: &crate::execution_core::graph::NodeExecutionTicket,
        request: &crate::RuntimeToolExecutionRequest,
        stop: Option<&crate::bound_tool_batch::BoundToolStop>,
        parent_stop: Option<&crate::bound_tool_batch::BoundToolStop>,
        observation: &crate::execution_core::graph::executors::agent_tool::NoDispatchObservation,
        f: impl FnOnce(&crate::RuntimeOriginalSessionClaim<'_>, &Arc<crate::conversation::MemberCommitControl>) -> Result<T, String>,
    ) -> Result<T, String> {
        let step = request.step_binding.as_ref().ok_or("bounded decision original Step unavailable")?;
        let session = request.session_id.as_deref().ok_or("bounded decision original Session unavailable")?;
        let turn = &step.snapshot.identity.turn_id;
        if step.snapshot.identity.session_id != session || step.origin.call_id != request.tool_use_id {
            return Err("bounded decision original Step differs".into());
        }
        let (_, _, active) = self.resolve_active_execution_bus_with_fence(&ticket.graph_id)
            .ok_or("bounded decision active execution unavailable")?;
        let active = active.ok_or("bounded decision active Session fence unavailable")?;
        let policy_owner = active.policy_owner_for_lineage(session, turn, &self.event_store)
            .ok_or("bounded decision original policy owner unavailable")?;
        let (_, root_control, claim, input, requested) = active.for_lineage(session, turn)
            .ok_or("bounded decision original Session turn differs")?;
        let control = root_control.token().member_commit_control();
        observation.register_bounded_control(ticket, &control)?;
        requested.register(&control);
        if let Some(stop) = stop { stop.register_commit_control(&control); }
        if let Some(stop) = parent_stop { stop.register_commit_control(&control); }
        let policy = policy_owner.original_policy_control();
        self.with_original_session_policy(session, &policy, |snapshot| {
            if snapshot.revision != request.policy_revision { return Err("bounded decision policy revision changed".into()); }
            let turn_id = harness_contract::turn::TurnId::from_string(turn);
            if input.active_turn_id().as_ref() != Some(&turn_id) { return Err("bounded decision original turn changed".into()); }
            let fence = claim.fence()?;
            let cursor = input.highest_consumed_cursor(&turn_id);
            if input.active_turn_id().as_ref() != Some(&turn_id) { return Err("bounded decision original turn changed while reading cursor".into()); }
            let consumed_input_sequence = match cursor {
                Some(cursor) if cursor.generation == fence.session_generation => usize::try_from(cursor.sequence)
                    .map_err(|_| "bounded decision consumed cursor overflow")?.max(fence.input_sequence),
                Some(_) => return Err("bounded decision consumed cursor generation differs".into()),
                None => fence.input_sequence,
            };
            let claim = crate::RuntimeOriginalSessionClaim { session_id:claim.session_id(), ingress_message_id:claim.message_id(),
                turn_id:claim.turn_id(), fence:&fence, consumed_input_sequence };
            f(&claim, &control)
        })
    }

    /// Resolve the registered ingress owner for this exact Program lineage.
    /// Missing or displaced ingress is not permission to write a conclusion.
    pub(crate) fn original_conclusion_owner(
        &self, program: &crate::AgenticProgramProjection,
    ) -> Option<OriginalConclusionOwner> {
        let execution_id = program.root_execution_id.as_deref()?;
        let (_, _, active) = self.resolve_active_execution_bus_with_fence(execution_id)?;
        let active = active?;
        let (_, root_control, claim, input, requested) =
            active.for_lineage(&program.session_id, &program.turn_id)?;
        if claim.session_id() != program.session_id || claim.turn_id() != program.turn_id {
            return None;
        }
        let control = root_control.token().member_commit_control();
        requested.register(&control);
        Some(OriginalConclusionOwner { claim, input, control, policy: active.original_policy.clone() })
    }

    /// The durable Program journal of one Session turn.
    ///
    /// One Session turn owns exactly one Program: the objective id is derived
    /// from the Session and the turn, and the Program id from that objective.
    ///
    /// The three states are not the same fact, and the two callers below need
    /// them told apart:
    ///
    /// * `Unknown` - the journal exists but could not be read. That is no
    ///   fact at all, and it is never read as "nothing is running": the
    ///   recovery refuses, and the release keeps the retained owner rather
    ///   than removing an owner it cannot reason about.
    /// * `NoProgram` - this Session turn has no Program journal. The turn
    ///   registered no member work, so the recovery refuses and the release
    ///   removes.
    /// * `Journal` - the journal of this turn.
    fn session_program_journal(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> SessionProgramJournal {
        let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
        let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
        match self.event_store.list_stream(&format!("agentic-program:{program_id}")) {
            Err(_) => SessionProgramJournal::Unknown,
            Ok(events) if events.is_empty() => SessionProgramJournal::NoProgram,
            Ok(events) => SessionProgramJournal::Journal(events),
        }
    }

    /// One Program action by the member that made it, reduced to what the
    /// recovery predicate needs.
    ///
    /// This is the single fold both predicates use, so the read side and the
    /// write side can never disagree about one member: a dispatch opens that
    /// member's attempt, a settlement closes it, and the last record wins.
    fn member_attempt_records(
        events: &[crate::DurableRuntimeEvent],
    ) -> Vec<MemberAttemptRecord> {
        let mut records = Vec::new();
        for event in events {
            if event.kind != "agentic.action_applied" {
                continue;
            }
            let Some(envelope) = event
                .payload
                .get("envelope")
                .and_then(|value| serde_json::from_value::<
                    harness_contract::agent_action::AgentActionEnvelope,
                >(value.clone())
                .ok())
            else {
                continue;
            };
            let Some(execution_id) = envelope.actor.execution_id else {
                continue;
            };
            let settles = Self::program_action_settles_attempt(&envelope.action);
            let dispatch = matches!(
                envelope.action,
                harness_contract::agent_action::AgentAction::TaskAttemptDispatch(_)
            );
            if settles || dispatch {
                records.push(MemberAttemptRecord { execution_id, settles, dispatch });
            }
        }
        records
    }

    /// Whether one member's attempt is still unsettled: it was dispatched and
    /// nothing has settled it since.
    fn member_attempt_record_is_unsettled(
        records: &[MemberAttemptRecord],
        member_execution_id: &str,
    ) -> bool {
        let mut unsettled = false;
        for record in records.iter().filter(|record| record.execution_id == member_execution_id) {
            if record.settles {
                unsettled = false;
            }
            if record.dispatch {
                unsettled = true;
            }
        }
        unsettled
    }

    /// Whether any member's attempt is still unsettled, by the same fold.
    fn any_member_attempt_is_unsettled(records: &[MemberAttemptRecord]) -> bool {
        let members: std::collections::BTreeSet<&str> =
            records.iter().map(|record| record.execution_id.as_str()).collect();
        members
            .into_iter()
            .any(|member| Self::member_attempt_record_is_unsettled(records, member))
    }

    /// Whether one Program action ends the attempt of the member that made it.
    ///
    /// These are the durable settlement records of a member attempt, named
    /// one by one. A failed attempt, a delivered Task, a review decision, a
    /// released claim and a declined opportunity each end the attempt that
    /// produced them: the member has nothing left to delegate. The attempt
    /// registration itself (`TaskAttemptDispatch`) is deliberately absent,
    /// because it opens an attempt instead of closing one.
    fn program_action_settles_attempt(action: &harness_contract::agent_action::AgentAction) -> bool {
        use harness_contract::agent_action::AgentAction;
        match action {
            AgentAction::TaskAttemptFail(_)
            | AgentAction::TaskSubmit(_)
            | AgentAction::TaskReview(_)
            | AgentAction::TaskRelease(_) => true,
            AgentAction::MessagePublish(publish) => publish
                .intent
                .as_ref()
                .is_some_and(|intent| intent.kind == harness_contract::agent_action::TaskIntentKind::Decline),
            _ => false,
        }
    }

    /// Per-member recovery liveness, read from the durable Program journal.
    ///
    /// The original Session's durable claim is a Session-wide fact: the
    /// ingress execution acks that record to a terminal status and it ends
    /// for every member at once, even while members are still running. The
    /// Program journal keeps the per-member fact instead: one member's
    /// attempt is registered by its dispatch and closed by its own
    /// settlement, and no other member's record can close it.
    ///
    /// The fold of that journal (`AgenticTaskProjection.active_attempts`) is
    /// not used here. It drops an attempt the moment the attempt fails, so
    /// "absent from the fold" cannot distinguish a settled attempt from one
    /// that is still running. The journal itself can, because it names the
    /// settlement instead of inferring it from absence.
    ///
    /// Both absent states refuse: an unreadable journal is no fact, and a
    /// turn with no Program registered no member work at all.
    pub(crate) fn member_attempt_is_unsettled(
        &self,
        session_id: &str,
        turn_id: &str,
        member_execution_id: &str,
    ) -> bool {
        match self.session_program_journal(session_id, turn_id) {
            SessionProgramJournal::Journal(events) => {
                let records = Self::member_attempt_records(&events);
                Self::member_attempt_record_is_unsettled(&records, member_execution_id)
            }
            SessionProgramJournal::Unknown | SessionProgramJournal::NoProgram => false,
        }
    }

    /// The Session-wide counterpart of the per-member fact: some member of
    /// this turn still has an attempt that has not settled.
    ///
    /// The recovery window may not close while it does. A member that is
    /// still unsettled may still owe the Session an owner, and an owner that
    /// was removed under it would be an owner that member can never recover.
    ///
    /// This reads the same journal through the same fold as the per-member
    /// predicate above, so the write side can never keep an owner for a turn
    /// whose members the read side already refuses - or the other way round.
    /// An unreadable journal keeps the owner: removing it would be a decision
    /// made on a fact nobody could read.
    pub(crate) fn session_has_unsettled_attempts(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> bool {
        match self.session_program_journal(session_id, turn_id) {
            SessionProgramJournal::Journal(events) => {
                Self::any_member_attempt_is_unsettled(&Self::member_attempt_records(&events))
            }
            SessionProgramJournal::Unknown => true,
            SessionProgramJournal::NoProgram => false,
        }
    }

    /// Recover the equivalent original Session owner after the root graph has
    /// ended and its active bus lease has been retired.
    ///
    /// The recovered owner is the one the original ingress installed: the same
    /// durable claim (with its claim token) and the same original turn
    /// control. Nothing is minted here, so the recovered owner keeps its
    /// commit_wins participation and its cancellation participation. The
    /// original turn's own cancellation still decides: a cancelled Session
    /// turn never regains a delegated owner.
    ///
    /// The recovery is granted per member. The original Session's live claim
    /// stays a sufficient reason on its own, but it ends for every member at
    /// once when the ingress execution reaches its terminal. The member's own
    /// attempt is the per-member fact: while that attempt is still unsettled,
    /// this member can still be owed an owner. A member whose attempt already
    /// settled gets no owner, and an unreadable Program is no permission.
    pub(crate) async fn recovered_original_conclusion_owner(
        &self,
        session_id: &str,
        turn_id: &str,
        root_execution_id: &str,
        member_execution_id: &str,
    ) -> Option<OriginalConclusionOwner> {
        let parts = self
            .original_session_owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()?;
        let OriginalSessionOwnerParts { fence, root_control, claim, input, requested, policy } = parts;
        if claim.session_id() != session_id || claim.turn_id() != turn_id {
            return None;
        }
        // The Program's root execution must be the very root ingress that
        // installed this fence. A recovered owner never widens the lineage.
        let fence_root_execution_id = crate::session_ingress_graph_id(
            claim.session_id(),
            claim.request_id(),
            claim.turn_id(),
        );
        if root_execution_id != fence_root_execution_id {
            return None;
        }
        // The original turn's own cancellation still decides: a cancelled
        // Session turn never regains a delegated owner, and that turn is over.
        if root_control.is_cancelled() {
            self.release_original_session_owner(session_id, &claim);
            return None;
        }
        // Named exception to the live Session claim, per member.
        //
        // The live claim says the original turn is still active: the Session
        // keeps its root claim Running until the terminal ACK, and a
        // finished, cancelled or superseded turn no longer owns it. That
        // claim ends for every member at once, the moment the ingress
        // execution reaches its terminal. A member whose own attempt is
        // still unsettled is still running at that point and is still owed
        // an owner, so the claim's end does not end the member's work.
        //
        // Every other gate below stays exactly as it was: the Program's root
        // execution must be the very root ingress that installed this fence,
        // a cancelled Session turn never regains a delegated owner, the
        // claim must name this same Session and turn, and the recovered
        // owner is the very claim, turn control and original policy the
        // ingress installed - never a second one. A journal that cannot be
        // read is no fact: this member is refused, exactly as it would be
        // without any attempt at all.
        let session_claim_is_live = fence
            .verify(crate::SessionExecutionFencePhase::ToolCommit)
            .await
            .is_ok();
        if !session_claim_is_live {
            if !self.member_attempt_is_unsettled(session_id, turn_id, member_execution_id) {
                // The read side and the write side ask the same fact in the
                // same direction: nobody in this turn is unsettled any more,
                // so the window this member asked about is over.
                self.release_original_session_owner_after_terminal(session_id, &claim);
                return None;
            }
        }
        let control = root_control.token().member_commit_control();
        requested.register(&control);
        Some(OriginalConclusionOwner {
            claim,
            input,
            control,
            policy,
        })
    }

    /// Drop the retained original Session owner parts for one Session.
    ///
    /// The recovery window ends with the original turn: a finished turn keeps
    /// no owner that a later member could recover.
    ///
    /// The removal is identity qualified. The next turn for the same Session
    /// replaces the retained entry, and a release that belongs to the
    /// previous turn must not remove the new turn's owner.
    ///
    /// This is the cancellation path, and it does not wait for the turn's
    /// members to settle. A cancelled Session turn is a terminal fact, not a
    /// liveness question, and the recovery gate refuses a cancelled turn on
    /// its own: the retained owner would authorize nothing.
    pub(crate) fn release_original_session_owner(
        &self,
        session_id: &str,
        claim: &OriginalSessionClaim,
    ) {
        self.remove_retained_session_owner(session_id, claim);
    }

    /// Close the recovery window for one Session turn at its terminal close.
    ///
    /// The window ends with the original turn. It does not end while a member
    /// of that turn is still unsettled: such a member can still be owed an
    /// owner, and an owner removed under it could never be recovered again.
    /// This is the same fact the recovery gate asks, in the Session-wide
    /// form, so the read side and the write side can never disagree about a
    /// member that is still running.
    ///
    /// The boundary is the attempt settlement, which is an event boundary:
    /// nothing re-runs this release when the last member settles. The
    /// retained owner can therefore outlive the turn. It authorizes nothing
    /// on its own - it only ever hands out the owner the ingress installed,
    /// to a member whose own attempt the journal still shows as unsettled -
    /// and the next turn for the same Session replaces the entry.
    ///
    /// Returns whether the retained owner was removed.
    pub(crate) fn release_original_session_owner_after_terminal(
        &self,
        session_id: &str,
        claim: &OriginalSessionClaim,
    ) -> bool {
        if self.session_has_unsettled_attempts(session_id, claim.turn_id()) {
            return false;
        }
        self.remove_retained_session_owner(session_id, claim)
    }

    /// Remove the retained original Session owner parts for one Session, if
    /// the retained entry is still the one this claim identifies.
    fn remove_retained_session_owner(
        &self,
        session_id: &str,
        claim: &OriginalSessionClaim,
    ) -> bool {
        let mut owners = self
            .original_session_owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if owners
            .get(session_id)
            .is_some_and(|parts| parts.claim.identity() == claim.identity())
        {
            owners.remove(session_id);
            true
        } else {
            false
        }
    }

    pub(crate) fn with_conclusion_policy<T>(
        &self, owner: &OriginalConclusionOwner,
        f: impl FnOnce(&crate::permissions::PermissionPolicy) -> Result<T, String>,
    ) -> Result<T, String> {
        self.with_original_session_policy(owner.claim.session_id(), &owner.policy.execution_policy_control(), |snapshot| {
            f(&owner.policy.bound_to_snapshot(snapshot))
        })
    }

    /// Read-only durable event projection for Gateway and Surface consumers.
    #[must_use]
    pub fn event_reader(&self) -> RuntimeEventReader {
        RuntimeEventReader {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Freeze the actual root ingress binding before installing its control.
    pub fn create_turn_cancellation_control(
        &self,
        execution_id: &str,
        ingress: &crate::TurnIngressRef,
    ) -> Result<Arc<TurnCancellationControl>, RuntimeServicesError> {
        if execution_id
            != crate::session_ingress_graph_id(
                &ingress.session_id,
                &ingress.request_id,
                &ingress.turn_id,
            )
            || ingress.request_id.is_empty()
            || ingress.session_id.is_empty()
            || ingress.turn_id.is_empty()
            || ingress.message_id.is_empty()
            || ingress.primary_task_id.is_empty()
            || ingress.root_task_id.is_empty()
            || ingress.claim_owner.is_empty()
            || ingress.claim_token.is_empty()
        {
            return Err(RuntimeServicesError::Invariant(
                "cancellation control requires the original root ingress binding".into(),
            ));
        }
        Ok(Arc::new(TurnCancellationControl {
            session_id: ingress.session_id.clone(),
            turn_id: ingress.turn_id.clone(),
            execution_id: execution_id.into(),
            ingress: Some(ingress.clone()),
            token: crate::CancellationToken::new(),
            first_cause: Mutex::new(TurnCancellationCause::None),
            step_wakeup: tokio::sync::Notify::new(),
        }))
    }

    /// Authenticate one specific stream, never a latest Session projection.
    pub async fn authenticate_requested_cancellation(
        &self,
        receipt: &harness_contract::turn::CancellationReceipt,
        control: &Arc<TurnCancellationControl>,
    ) -> Result<AuthenticatedRequestedCancellation, RuntimeServicesError> {
        let reject = || {
            RuntimeServicesError::Invariant(
                "Requested cancellation source or frozen binding mismatch".into(),
            )
        };
        let ingress = control.ingress.as_ref().ok_or_else(reject)?;
        if !control.matches_target(&receipt.session_id, &receipt.turn_id, &receipt.execution_id) {
            return Err(reject());
        }
        let (event, source) = read_original_requested_event(&self.event_store, receipt)?;
        let query = self.session_query_port.get().ok_or_else(reject)?;
        let fence = crate::SessionExecutionFence::from_claim(
            Arc::clone(query),
            ingress.request_id.clone(),
            ingress.session_id.clone(),
            ingress.session_generation,
            ingress.input_sequence as usize,
            ingress.claim_owner.clone(),
            ingress.claim_token.clone(),
        )
        .map_err(RuntimeServicesError::Invariant)?;
        let live_binding = Arc::new(RequestedCancellationBinding {
            query: Arc::clone(query),
            fence,
            ingress: ingress.clone(),
        });
        // This phase authenticates the immutable Runtime intent/frozen binding.
        // Current ownership is decided only by the Session write transaction;
        // a historical committed authorization must remain confirmable.
        Ok(AuthenticatedRequestedCancellation {
            receipt: source,
            event_id: event.event_id.clone(),
            stream_id: event.stream_id.clone(),
            sequence: event.sequence,
            commit_cursor: event.commit_cursor,
            digest: format!(
                "sha256:{:x}",
                Sha256::digest(serde_json::to_vec(&event).map_err(|_| reject())?)
            ),
            original_control: Arc::downgrade(control),
            live_binding: Some(live_binding),
        })
    }

    pub async fn authorize_requested_cancellation(
        &self,
        source: AuthenticatedRequestedCancellation,
    ) -> Result<CancellationAuthorizationOutcome, RuntimeServicesError> {
        let request = source.authorization_request()?;
        let authorization_id = request.owner_request().event.event_id.clone();
        let Some(journal) = self.session_journal_port.get() else {
            return Ok(CancellationAuthorizationOutcome::Unknown {
                authorization_id,
                reason: "original Session authorization writer unavailable".into(),
            });
        };
        match journal.authorize_requested_cancellation(&request).await {
            Ok(committed) => source
                .bind_authorization(&request, committed)
                .map(CancellationAuthorizationOutcome::Authorized),
            Err(write_error) => {
                // One exact read after an unsuccessful/unknown write. None is
                // not evidence that a different connection can never commit.
                let Some(query) = self.session_query_port.get() else {
                    return Ok(CancellationAuthorizationOutcome::Unknown {
                        authorization_id,
                        reason: "authorization confirmation reader unavailable".into(),
                    });
                };
                match query.cancellation_authorization_by_id(&request).await {
                    Ok(Some(committed)) => source.bind_authorization(&request, committed)
                        .map(CancellationAuthorizationOutcome::Authorized),
                    Ok(None) if matches!(write_error, session::SessionError::StaleExecutionFence(_)) => {
                        Ok(CancellationAuthorizationOutcome::RejectedStale { authorization_id })
                    }
                    Ok(None) => Ok(CancellationAuthorizationOutcome::Unknown {
                        authorization_id, reason: write_error.to_string(),
                    }),
                    Err(read_error) => Ok(CancellationAuthorizationOutcome::Unknown {
                        authorization_id,
                        reason: format!("authorization write/confirmation unresolved: {write_error}; {read_error}"),
                    }),
                }
            }
        }
    }

    /// Notify only after original-owner authorization, under registry revalidation.
    pub fn publish_requested_cancellation(
        &self,
        control: &Arc<TurnCancellationControl>,
        source: AuthorizedRequestedCancellation,
    ) -> Result<CancellationNotificationDisposition, RuntimeServicesError> {
        publish_requested_source(control, source)
    }

    /// Commit the one canonical cancellation activity receipt. The receipt is
    /// keyed by `cancellation_id`, so HTTP retries and live delivery reuse the
    /// same durable fact; a changed payload under the same id is rejected by
    /// the event-store transaction hash.
    pub fn commit_cancellation_receipt(
        &self,
        mut receipt: harness_contract::turn::CancellationReceipt,
    ) -> Result<harness_contract::turn::CancellationReceipt, RuntimeServicesError> {
        if receipt.cancellation_id.trim().is_empty()
            || receipt.session_id.trim().is_empty()
            || receipt.requested_at_ms == 0
        {
            return Err(RuntimeServicesError::Invariant(
                "cancellation receipt requires id, session and requested_at_ms".to_string(),
            ));
        }
        let stream_id = format!("cancellation:{}", receipt.cancellation_id);
        let expected_revision = self.event_store.stream_revision(&stream_id)?;
        // Commit metadata is assigned by the ledger, never trusted from an
        // HTTP caller. Normalization keeps an idempotent retry byte-identical.
        receipt.journal_sequence = 0;
        receipt.projection_revision = 0;
        if let Some(existing) = self
            .event_store
            .list_stream(&stream_id)
            .map_err(RuntimeServicesError::Invariant)?
            .last()
        {
            let mut persisted =
                serde_json::from_value::<harness_contract::turn::CancellationReceipt>(
                    existing.payload.clone(),
                )
                .map_err(|error| {
                    RuntimeServicesError::Invariant(format!(
                        "decode durable cancellation receipt: {error}"
                    ))
                })?;
            if persisted == receipt {
                persisted.journal_sequence = existing.commit_cursor;
                persisted.projection_revision = existing.sequence;
                return Ok(persisted);
            }
            let same_identity = persisted.cancellation_id == receipt.cancellation_id
                && persisted.session_id == receipt.session_id
                && persisted.turn_id == receipt.turn_id
                && persisted.execution_id == receipt.execution_id
                && persisted.actor_id == receipt.actor_id
                && persisted.cause == receipt.cause
                && persisted.reason == receipt.reason
                && persisted.requested_at_ms == receipt.requested_at_ms;
            // HTTP, the ingress worker, and the recovery reconciler can all
            // finalize the same Requested intent. Once a final receipt exists,
            // its status and effective timestamp are the durable winner; a
            // concurrent writer with the same immutable request identity must
            // return it rather than treating timestamp drift as ID reuse.
            if same_identity
                && persisted.status != harness_contract::turn::CancellationStatus::Requested
            {
                persisted.journal_sequence = existing.commit_cursor;
                persisted.projection_revision = existing.sequence;
                return Ok(persisted);
            }
            let valid_requested_transition = persisted.status
                == harness_contract::turn::CancellationStatus::Requested
                && receipt.status != harness_contract::turn::CancellationStatus::Requested
                && same_identity;
            if !valid_requested_transition {
                return Err(RuntimeServicesError::Invariant(format!(
                    "cancellation id `{}` was reused with a different receipt",
                    receipt.cancellation_id
                )));
            }
        }
        let status_key = match receipt.status {
            harness_contract::turn::CancellationStatus::Requested => "requested",
            harness_contract::turn::CancellationStatus::Cancelled => "cancelled",
            harness_contract::turn::CancellationStatus::AlreadyTerminal => "already-terminal",
        };
        let transaction_id = format!(
            "session-cancellation:{}:{status_key}",
            receipt.cancellation_id
        );
        let event = crate::RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: crate::RuntimeEventScope::Session,
            kind: "session.cancellation_committed".to_string(),
            status: Some(
                match receipt.status {
                    harness_contract::turn::CancellationStatus::Requested => "requested",
                    harness_contract::turn::CancellationStatus::Cancelled => "cancelled",
                    harness_contract::turn::CancellationStatus::AlreadyTerminal => {
                        "already_terminal"
                    }
                }
                .to_string(),
            ),
            actor: Some(receipt.actor_id.clone()),
            refs: [
                (!receipt.session_id.is_empty()).then(|| crate::RuntimeEventRef {
                    kind: "session".to_string(),
                    id: receipt.session_id.clone(),
                }),
                (!receipt.execution_id.is_empty()).then(|| crate::RuntimeEventRef {
                    kind: "execution".to_string(),
                    id: receipt.execution_id.clone(),
                }),
                (!receipt.turn_id.is_empty()).then(|| crate::RuntimeEventRef {
                    kind: "turn".to_string(),
                    id: receipt.turn_id.clone(),
                }),
            ]
            .into_iter()
            .flatten()
            .collect(),
            payload: serde_json::to_value(&receipt).map_err(|error| {
                RuntimeServicesError::Invariant(format!("encode cancellation receipt: {error}"))
            })?,
        };
        let committed = match self
            .event_store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id,
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event,
                    idempotency_key: Some(format!("{}:{status_key}", receipt.cancellation_id)),
                    schema_version: 1,
                }],
            }) {
            Ok(committed) => committed,
            Err(error) => {
                // HTTP, ingress execution, and the restart reconciler may all
                // race to finalize the same durable Requested intent. The CAS
                // loser must observe the already committed winner rather than
                // turn a successful user cancellation into an execution
                // failure merely because its effective timestamp differed.
                if let Some(winner) = self.cancellation_receipt(&receipt.cancellation_id)? {
                    let same_identity = winner.cancellation_id == receipt.cancellation_id
                        && winner.session_id == receipt.session_id
                        && winner.turn_id == receipt.turn_id
                        && winner.execution_id == receipt.execution_id
                        && winner.actor_id == receipt.actor_id
                        && winner.cause == receipt.cause
                        && winner.reason == receipt.reason
                        && winner.requested_at_ms == receipt.requested_at_ms;
                    if same_identity {
                        if receipt.status == harness_contract::turn::CancellationStatus::Requested
                            || winner.status
                                != harness_contract::turn::CancellationStatus::Requested
                        {
                            return Ok(winner);
                        }
                        // The CAS lost to the first identical Requested
                        // append, not to a finalizer. Retry against the stable
                        // new revision; if a finalizer wins meanwhile, the
                        // next read returns that durable winner.
                        return self.commit_cancellation_receipt(receipt);
                    }
                }
                return Err(error.into());
            }
        };
        receipt.journal_sequence = committed.commit_cursor;
        receipt.projection_revision = committed
            .stream_revisions
            .first()
            .map_or(expected_revision.saturating_add(1), |revision| {
                revision.committed_revision
            });
        Ok(receipt)
    }

    pub fn cancellation_receipt(
        &self,
        cancellation_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let stream_id = format!("cancellation:{cancellation_id}");
        let Some(event) = self
            .event_store
            .list_stream(&stream_id)
            .map_err(RuntimeServicesError::Invariant)?
            .last()
            .cloned()
        else {
            return Ok(None);
        };
        let mut receipt =
            serde_json::from_value::<harness_contract::turn::CancellationReceipt>(event.payload)
                .map_err(|error| {
                    RuntimeServicesError::Invariant(format!(
                        "decode durable cancellation receipt: {error}"
                    ))
                })?;
        receipt.journal_sequence = event.commit_cursor;
        receipt.projection_revision = event.sequence;
        Ok(Some(receipt))
    }

    /// Resolve durable cancellation intents and replay a committed winner's
    /// derived live projection after a process crash. Missing executions stay
    /// Requested; absence of a process-local token is never proof of terminal.
    pub fn reconcile_requested_cancellations(
        &self,
        limit: usize,
        authorized: &[AuthorizedRequestedCancellation],
    ) -> Result<Vec<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let mut finalized = Vec::new();
        let candidates = self
            .latest_cancellation_receipts()?
            .into_iter()
            .filter(|receipt| {
                if receipt.execution_id.is_empty() {
                    return false;
                }
                match receipt.status {
                    harness_contract::turn::CancellationStatus::Requested => authorized
                        .iter()
                        .any(|grant| grant.receipt().cancellation_id == receipt.cancellation_id),
                    harness_contract::turn::CancellationStatus::Cancelled => {
                        let terminal_ref = format!("cancellation:{}", receipt.cancellation_id);
                        self.execution_live(&receipt.execution_id)
                            .is_none_or(|live| {
                                live.status
                                    != harness_contract::projection::ExecutionLiveStatus::Cancelled
                                    || live.terminal_ref.as_deref() != Some(terminal_ref.as_str())
                            })
                    }
                    harness_contract::turn::CancellationStatus::AlreadyTerminal => false,
                }
            })
            .take(limit)
            .collect::<Vec<_>>();
        for receipt in candidates {
            let resolved =
                if receipt.status == harness_contract::turn::CancellationStatus::Requested {
                    let Some(grant) = authorized
                        .iter()
                        .find(|grant| grant.receipt().cancellation_id == receipt.cancellation_id)
                    else {
                        continue;
                    };
                    self.resolve_authorized_requested_cancellation(grant)?
                } else {
                    self.replay_or_resolve_inactive_cancellation(&receipt.cancellation_id)?
                };
            if let Some(receipt) = resolved {
                finalized.push(receipt);
            }
        }
        Ok(finalized)
    }

    /// Existing final carriers may replay, and a truly empty target is known
    /// inactive. Missing local control is not an inactive execution proof.
    pub fn replay_or_resolve_inactive_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let Some(receipt) = self.cancellation_receipt(cancellation_id)? else {
            return Ok(None);
        };
        if receipt.status == harness_contract::turn::CancellationStatus::Requested
            && !receipt.execution_id.is_empty()
            && !receipt.turn_id.is_empty()
        {
            return Ok(None);
        }
        self.resolve_requested_cancellation(cancellation_id)
    }

    pub fn resolve_authorized_requested_cancellation(
        &self,
        authorization: &AuthorizedRequestedCancellation,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let request = authorization.source.authorization_request()?;
        authorization
            .source
            .clone()
            .bind_authorization(&request, authorization.authorization.clone())?;
        let original = authorization.receipt();
        let (event, receipt) = read_original_requested_event(&self.event_store, original)?;
        let digest = format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&event)
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
            )
        );
        if receipt != *original
            || event.event_id != authorization.source.event_id
            || event.stream_id != authorization.source.stream_id
            || event.sequence != authorization.source.sequence
            || event.commit_cursor != authorization.source.commit_cursor
            || digest != authorization.source.digest
        {
            return Err(RuntimeServicesError::Invariant(
                "authorization belongs to a different Runtime Requested source".into(),
            ));
        }
        self.resolve_requested_cancellation(&original.cancellation_id)
    }

    /// Recognize only an original, atomically committed provider cancellation.
    /// A raw token, cancelled status, missing effect, foreign Session or any
    /// failed/blocked graph node is insufficient to attribute this outcome.
    pub(crate) fn authenticated_provider_graph_cancellation(
        &self,
        graph: &ExecutionGraph,
    ) -> Result<Option<(String, super::graph::NodeExecutionTicket)>, RuntimeServicesError> {
        if graph.nodes.iter().any(|node| graph.node_statuses.get(&node.id).is_none_or(|status|
            !status.is_terminal() || matches!(status, ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked))) {
            return Ok(None);
        }
        let mut matching = graph.nodes.iter().filter(|node| {
            node.kind == ExecutionNodeKind::InlineModel && node.executor_kind == "inline_model"
                && graph.node_results.get(&node.id).is_some_and(|result|
                    result.status == ExecutionNodeStatus::Cancelled
                    && result.summary.as_deref() == Some("original local provider response observed authorized cancellation"))
        });
        let Some(node) = matching.next() else { return Ok(None) };
        if matching.next().is_some() { return Ok(None); }
        let attempt = *graph.recovery_cursor.node_attempts.get(&node.id)
            .ok_or_else(|| RuntimeServicesError::Invariant("cancelled model attempt missing".into()))?;
        let binding = self.graph_state_store.current_binding(graph, &node.id, attempt)?;
        let (ticket, _) = self.graph_state_store.binding_start_ticket(graph, &node.id, &binding)?;
        let Some((result, _)) = self.commit_service.committed_node_attempt(graph, &node.id, &binding)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))? else { return Ok(None) };
        if graph.node_results.get(&node.id) != Some(&result) { return Ok(None); }
        let cancellation_id = self.commit_service.validate_cold_provider_cancellation(&ticket, &result)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(cancellation_id.map(|id| (id, ticket)))
    }

    /// Hand a committed Session cancellation to its original ordinary Goal.
    /// The Goal write is fenced against the exact Graph, effect and receipt
    /// heads; a Team Program retains its own independent terminal owner.
    fn settle_authenticated_provider_cancel_goal(
        &self,
        receipt: &harness_contract::turn::CancellationReceipt,
    ) -> Result<bool, RuntimeServicesError> {
        use harness_contract::goal::GoalCompletion;
        if receipt.status != harness_contract::turn::CancellationStatus::Cancelled {
            return Ok(true);
        }
        let graph = match self.graph_state_store.load(&receipt.execution_id) {
            Ok(graph) => graph,
            Err(ExecutionStateStoreError::NotFound { .. }) => return Ok(true),
            Err(error) => return Err(error.into()),
        };
        if graph.lineage.as_ref().is_none_or(|lineage|
            lineage.session_id != receipt.session_id || lineage.turn_id != receipt.turn_id) {
            return Ok(true);
        }
        let Some((cancellation_id, ticket)) = self.authenticated_provider_graph_cancellation(&graph)? else { return Ok(true) };
        if cancellation_id != receipt.cancellation_id { return Ok(true); }
        let goal_id = format!("goal:{}", graph.id);
        let Some(projection) = self.goal_store.projection(&goal_id).map_err(RuntimeServicesError::Invariant)? else { return Ok(true) };
        let goal = projection.goal;
        let Some(binding) = goal.execution_binding.as_ref() else { return Ok(true) };
        if binding.root_execution_id != graph.id || binding.session_id != receipt.session_id || binding.turn_id != receipt.turn_id {
            return Ok(true);
        }
        if self.agent_action_service().project_if_exists(&binding.agentic_program_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?.is_some() {
            return Ok(true);
        }
        // Never overwrite a prior winning terminal or turn a cancellation
        // into an answer. Existing terminal facts retain their own owner.
        if goal.completion != GoalCompletion::Open { return Ok(true); }
        let goal_stream = format!("goal:{goal_id}");
        let cancellation_stream = format!("cancellation:{}", receipt.cancellation_id);
        let effect_stream = super::graph::ExecutionNodeBinding::effect_stream_id(&ticket);
        // TurnStrategy owns a distinct `turn:<decision_id>` Outcome; its
        // original Session strategy event supplies the Graph-to-Outcome link.
        let strategy_stream = format!("session:{}", receipt.session_id);
        let strategy_head = self.event_store.stream_revision(&strategy_stream)?;
        if strategy_head == 0 { return Ok(false); }
        if strategy_head > 4096 {
            return Err(RuntimeServicesError::Invariant("cancelled Goal strategy evidence budget exhausted".into()));
        }
        let mut after = 0;
        let mut decisions = BTreeSet::new();
        let mut bytes = 0usize;
        while after < strategy_head {
            let page = self.event_store.list_stream_after(&strategy_stream, after, strategy_head, 100, 1024 * 1024)
                .map_err(RuntimeServicesError::Invariant)?;
            if page.is_empty() { return Err(RuntimeServicesError::Invariant("original strategy history gap".into())); }
            for source in page {
                bytes = bytes.saturating_add(serde_json::to_vec(&source)
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?.len());
                if bytes > 8 * 1024 * 1024 {
                    return Err(RuntimeServicesError::Invariant("cancelled Goal strategy byte budget exhausted".into()));
                }
                if source.sequence != after + 1 || source.stream_id != strategy_stream {
                    return Err(RuntimeServicesError::Invariant("original strategy history position mismatch".into()));
                }
                after = source.sequence;
                if source.kind == "runtime.strategy.outcome"
                    && source.payload["session_ref"] == receipt.session_id
                    && source.payload["turn_ref"] == receipt.turn_id
                    && source.payload["execution_graph_ref"] == graph.id {
                    if source.scope != RuntimeEventScope::Session
                        || source.actor.as_deref() != Some("conversation_runtime.strategy_owner") {
                        return Err(RuntimeServicesError::Invariant("original strategy outcome owner mismatch".into()));
                    }
                    let decision = source.payload["decision_id"].as_str()
                        .filter(|value| !value.is_empty()).ok_or_else(|| RuntimeServicesError::Invariant("original strategy decision missing".into()))?;
                    let status = source.status.as_deref().filter(|value| !value.is_empty())
                        .ok_or_else(|| RuntimeServicesError::Invariant("original strategy terminal status missing".into()))?;
                    decisions.insert((decision.to_owned(), status.to_owned()));
                }
            }
        }
        if decisions.is_empty() { return Ok(false); }
        // Multiple durable owners are a terminal conflict, not missing work
        // to retry forever. Preserve their original facts for their owner.
        if decisions.len() != 1 { return Ok(true); }
        let (decision, strategy_status) = decisions.into_iter().next().expect("one decision checked");
        let outcome_stream = format!("outcome:turn:{decision}");
        let outcome_head = self.event_store.stream_revision(&outcome_stream)?;
        if outcome_head == 0 { return Ok(false); }
        if outcome_head != 1 {
            return Ok(true);
        }
        let outcome_event = self.event_store.list_stream_after(&outcome_stream, 0, outcome_head, 1, 1024 * 1024)
            .map_err(RuntimeServicesError::Invariant)?.into_iter().next()
            .ok_or_else(|| RuntimeServicesError::Invariant("original execution outcome missing".into()))?;
        let outcome: harness_contract::outcome::ExecutionOutcome = serde_json::from_value(outcome_event.payload.clone())
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        if outcome_event.kind != crate::execution_core::outcome_service::OUTCOME_EVENT_KIND
            || outcome_event.actor.as_deref() != Some("runtime.outcome_service")
            || outcome_event.status.as_deref() != Some(outcome.terminal.class_name())
            || outcome.identity.execution_scope != harness_contract::outcome::OutcomeExecutionScope::Session
            || outcome.identity.execution_id != format!("turn:{decision}")
            || outcome.identity.execution_graph_ref.as_deref() != Some(graph.id.as_str())
            || outcome.strategy.decision_id != decision || outcome.identity.session_id != receipt.session_id
            || outcome.identity.turn_id != receipt.turn_id {
            return Err(RuntimeServicesError::Invariant("original execution outcome identity mismatch".into()));
        }
        if strategy_status != outcome.terminal.class_name() {
            // Both incompatible terminal facts are durable; neither can be
            // made true by polling them repeatedly or overwriting the Goal.
            return Ok(true);
        }
        let (completion, reason) = match outcome.terminal {
            harness_contract::outcome::OutcomeTerminalClass::Cancelled(_) => (GoalCompletion::Cancelled, "original Session-authorized provider cancellation"),
            harness_contract::outcome::OutcomeTerminalClass::Failed(_) => (GoalCompletion::Failed, "original execution outcome failed"),
            harness_contract::outcome::OutcomeTerminalClass::Blocked(_) => (GoalCompletion::Blocked, "original execution outcome blocked"),
            // A durable success/partial belongs to its original terminal
            // owner. Do not fabricate cancellation or keep a retry alive for
            // an already committed conflicting outcome.
            _ => return Ok(true),
        };
        let goal_head = self.event_store.stream_revision(&goal_stream)?;
        let cancellation_head = self.event_store.stream_revision(&cancellation_stream)?;
        let effect_head = self.event_store.stream_revision(&effect_stream)?;
        if cancellation_head != receipt.projection_revision || effect_head != 2 {
            return Err(RuntimeServicesError::Invariant("cancelled Goal source heads changed".into()));
        }
        let fence = format!("session-cancellation:{}", receipt.cancellation_id);
        let already_applied = || -> Result<bool, RuntimeServicesError> {
            let existing = self.event_store.event_by_idempotency_key(&goal_stream, &fence)?;
            Ok(existing.is_some_and(|event| event.kind == "goal.completed"
                && event.actor.as_deref() == Some("runtime.session_cancellation")
                && event.payload["goal"]["terminal"]["terminal_fence"] == fence
                && event.payload["goal"]["completion"] == serde_json::to_value(completion).unwrap_or_default()))
        };
        let mut event = match self.goal_store.terminal_event(&goal_id, completion,
            vec![format!("cancellation:{}", receipt.cancellation_id), effect_stream.clone(), outcome_stream.clone()],
            reason.into(), fence.clone()) {
                Ok(event) => event,
                Err(_error) if already_applied()? => return Ok(true),
                Err(error) => return Err(RuntimeServicesError::Invariant(error)),
            };
        if event.event.payload["goal"]["revision"].as_u64() != goal.revision.checked_add(1)
            || event.event.payload["goal"]["execution_binding"] != serde_json::to_value(binding)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))? {
            return Err(RuntimeServicesError::Invariant("cancelled Goal owner or revision changed".into()));
        }
        event.event.actor = Some("runtime.session_cancellation".into());
        let applied = self.event_store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("goal-cancellation:{goal_id}:{}", receipt.cancellation_id),
            expected_streams: vec![
                crate::ExpectedStreamRevision { stream_id:goal_stream.clone(), expected_revision:goal_head },
                crate::ExpectedStreamRevision { stream_id:graph.id.clone(), expected_revision:graph.revision },
                crate::ExpectedStreamRevision { stream_id:cancellation_stream, expected_revision:cancellation_head },
                crate::ExpectedStreamRevision { stream_id:effect_stream, expected_revision:effect_head },
                crate::ExpectedStreamRevision { stream_id:strategy_stream, expected_revision:strategy_head },
                crate::ExpectedStreamRevision { stream_id:outcome_stream, expected_revision:outcome_head },
            ],
            events:vec![event],
        });
        if let Err(error) = applied {
            if already_applied()? { return Ok(true); }
            return Err(error.into());
        }
        Ok(true)
    }

    // The existing settled-graph lane owns retry and its durable per-graph
    // pending cursor. Either the Graph, the Session receipt, the Outcome or
    // the Goal event can arrive last; each carries this same graph identity.
    fn reconcile_provider_cancel_goal(&self, graph_id: &str) -> Result<(), String> {
        let graph = match self.graph_state_store.load(graph_id) {
            Ok(graph) => graph,
            Err(ExecutionStateStoreError::NotFound { .. }) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        let Some((cancellation_id, _)) = self.authenticated_provider_graph_cancellation(&graph)
            .map_err(|error| error.to_string())? else { return Ok(()) };
        let Some(receipt) = self.cancellation_receipt(&cancellation_id).map_err(|error| error.to_string())? else {
            return Err("original Session cancellation receipt pending".into());
        };
        match receipt.status {
            harness_contract::turn::CancellationStatus::Requested => Err("original Session cancellation receipt pending".into()),
            harness_contract::turn::CancellationStatus::AlreadyTerminal => Ok(()),
            harness_contract::turn::CancellationStatus::Cancelled => {
                if self.settle_authenticated_provider_cancel_goal(&receipt).map_err(|error| error.to_string())? {
                    Ok(())
                } else {
                    Err("original execution Outcome pending".into())
                }
            }
        }
    }

    // Internal terminal-arbitration kernel. Cross-crate callers must present
    // Session authorization or restrict themselves to already-final replay.
    pub(crate) fn resolve_requested_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let Some(mut receipt) = self.cancellation_receipt(cancellation_id)? else {
            return Ok(None);
        };
        if receipt.status == harness_contract::turn::CancellationStatus::Requested
            && (receipt.execution_id.is_empty() || receipt.turn_id.is_empty())
        {
            // A cancellation against a Session with no execution target has
            // a deterministic durable outcome. It never needs a synthetic
            // live record or process-local cancellation side effect.
            receipt.status = harness_contract::turn::CancellationStatus::AlreadyTerminal;
            receipt.effective_at_ms = Some(now_ms());
            receipt.journal_sequence = 0;
            receipt.projection_revision = 0;
            return self.commit_cancellation_receipt(receipt).map(Some);
        }
        if receipt.status != harness_contract::turn::CancellationStatus::Requested {
            if receipt.status == harness_contract::turn::CancellationStatus::Cancelled {
                self.finalize_committed_cancellation(&receipt)?;
                self.settle_authenticated_provider_cancel_goal(&receipt)?;
            }
            return Ok(Some(receipt));
        }
        let Some(_live) = self.execution_live(&receipt.execution_id) else {
            // A concurrent finalizer releases the live winner checkpoint only
            // after committing the final receipt. Re-read that durable stream
            // before declaring the intent unresolved.
            let winner = self.cancellation_receipt(cancellation_id)?;
            return Ok(winner.filter(|winner| {
                winner.status != harness_contract::turn::CancellationStatus::Requested
            }));
        };
        let cancellation_ref = format!("cancellation:{cancellation_id}");
        // The request timestamp is immutable across Requested -> Cancelled
        // receipt revisions, so a post-commit crash can recover the exact
        // pending claim. A ledger cursor changes at final commit and therefore
        // cannot serve as the cancellation claim epoch.
        let claim_generation = receipt.requested_at_ms.max(1);
        let (status, finalize_cancellation) = match self
            .live_execution_store
            .claim_terminal(
                &receipt.execution_id,
                cancellation_ref.clone(),
                harness_contract::projection::ExecutionLiveStatus::Cancelled,
                claim_generation,
            )
            .map_err(RuntimeServicesError::Invariant)?
        {
            crate::execution_live::TerminalFenceClaim::Claimed
            | crate::execution_live::TerminalFenceClaim::SamePending => {
                (harness_contract::turn::CancellationStatus::Cancelled, true)
            }
            crate::execution_live::TerminalFenceClaim::SameTerminal => {
                (harness_contract::turn::CancellationStatus::Cancelled, false)
            }
            crate::execution_live::TerminalFenceClaim::ConflictingTerminal => (
                harness_contract::turn::CancellationStatus::AlreadyTerminal,
                false,
            ),
            crate::execution_live::TerminalFenceClaim::ConflictingPending => {
                // Completion has prepared its reversible claim. Keep the
                // durable Requested receipt for reconciliation; whichever
                // durable carrier commits first will finalize the shared
                // live arbitration slot.
                return Ok(None);
            }
            crate::execution_live::TerminalFenceClaim::MissingExecution => {
                let winner = self.cancellation_receipt(cancellation_id)?;
                return Ok(winner.filter(|winner| {
                    winner.status != harness_contract::turn::CancellationStatus::Requested
                }));
            }
        };
        receipt.status = status;
        receipt.effective_at_ms = Some(now_ms());
        receipt.journal_sequence = 0;
        receipt.projection_revision = 0;
        let receipt = self.commit_cancellation_receipt(receipt)?;
        if finalize_cancellation {
            self.finalize_committed_cancellation(&receipt)?;
        }
        self.settle_authenticated_provider_cancel_goal(&receipt)?;
        Ok(Some(receipt))
    }

    fn finalize_committed_cancellation(
        &self,
        receipt: &harness_contract::turn::CancellationReceipt,
    ) -> Result<(), RuntimeServicesError> {
        if receipt.status != harness_contract::turn::CancellationStatus::Cancelled {
            return Ok(());
        }
        if self.execution_live(&receipt.execution_id).is_none() {
            self.record_live_execution(
                &receipt.session_id,
                receipt.execution_id.clone(),
                receipt.turn_id.clone(),
            );
        }
        let cancellation_ref = format!("cancellation:{}", receipt.cancellation_id);
        let claim_generation = receipt.requested_at_ms.max(1);
        match self
            .live_execution_store
            .claim_terminal(
                &receipt.execution_id,
                cancellation_ref.clone(),
                harness_contract::projection::ExecutionLiveStatus::Cancelled,
                claim_generation,
            )
            .map_err(RuntimeServicesError::Invariant)?
        {
            crate::execution_live::TerminalFenceClaim::Claimed
            | crate::execution_live::TerminalFenceClaim::SamePending => match self
                .live_execution_store
                .finalize_terminal(
                    &receipt.execution_id,
                    &cancellation_ref,
                    harness_contract::projection::ExecutionLiveStatus::Cancelled,
                    claim_generation,
                )
                .map_err(RuntimeServicesError::Invariant)?
            {
                crate::execution_live::TerminalFenceClaim::Claimed
                | crate::execution_live::TerminalFenceClaim::SameTerminal => Ok(()),
                other => Err(RuntimeServicesError::Invariant(format!(
                    "durable cancellation committed but live finalization returned {other:?}"
                ))),
            },
            crate::execution_live::TerminalFenceClaim::SameTerminal => Ok(()),
            other => Err(RuntimeServicesError::Invariant(format!(
                "durable cancellation conflicts with live terminal arbitration: {other:?}"
            ))),
        }
    }

    pub fn pending_cancellation_receipts(
        &self,
        limit: usize,
    ) -> Result<Vec<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        Ok(self
            .latest_cancellation_receipts()?
            .into_iter()
            .filter(|receipt| {
                receipt.status == harness_contract::turn::CancellationStatus::Requested
                    && !receipt.execution_id.is_empty()
            })
            .take(limit)
            .collect())
    }

    fn latest_cancellation_receipts(
        &self,
    ) -> Result<Vec<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        const PAGE_SIZE: usize = 256;
        let mut latest =
            std::collections::BTreeMap::<String, harness_contract::turn::CancellationReceipt>::new(
            );
        let mut after = None;
        loop {
            let page = self
                .event_store
                .list_scope_kind_page_asc(
                    crate::RuntimeEventScope::Session,
                    "session.cancellation_committed",
                    after,
                    PAGE_SIZE,
                )
                .map_err(RuntimeServicesError::Invariant)?;
            if page.is_empty() {
                break;
            }
            for event in &page {
                let Ok(mut receipt) = serde_json::from_value::<
                    harness_contract::turn::CancellationReceipt,
                >(event.payload.clone()) else {
                    continue;
                };
                receipt.journal_sequence = event.commit_cursor;
                receipt.projection_revision = event.sequence;
                latest.insert(receipt.cancellation_id.clone(), receipt);
            }
            after = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(latest.into_values().collect())
    }

    pub fn latest_cancellation_receipt_for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let head = self.event_store.durable_commit_cursor()?;
        let mut after = None;
        let mut found = None;
        loop {
            let page = self
                .event_store
                .position_page(
                    RuntimeEventScope::Session,
                    session_id,
                    Some("session.cancellation_committed"),
                    after,
                    head,
                    100,
                    1024 * 1024,
                )
                .map_err(RuntimeServicesError::Invariant)?;
            if page.is_empty() {
                break;
            }
            for event in page {
                let position = (event.commit_cursor, event.transaction_index);
                if after.is_some_and(|after| position <= after) {
                    return Err(RuntimeServicesError::Invariant(
                        "cancellation page did not advance".into(),
                    ));
                }
                after = Some(position);
                let mut receipt: harness_contract::turn::CancellationReceipt =
                    serde_json::from_value(event.payload).map_err(|_| {
                        RuntimeServicesError::Invariant("invalid current cancellation fact".into())
                    })?;
                if receipt.session_id == session_id
                    && receipt.execution_id == execution_id
                    && receipt.turn_id == turn_id
                {
                    receipt.journal_sequence = event.commit_cursor;
                    receipt.projection_revision = event.sequence;
                    found = Some(receipt);
                }
            }
        }
        Ok(found)
    }

    /// Register the durable SessionIngress identity before a provider-backed
    /// turn starts. Runtime owns every subsequent lifecycle transition.
    pub fn record_live_execution(&self, session_id: &str, execution_id: String, turn_id: String) {
        self.live_execution_store
            .record_queued(session_id, execution_id, turn_id);
    }

    /// Reduce an execution-scoped Runtime event. Events without an execution
    /// context are intentionally ignored instead of being assigned to the
    /// session's most recently active turn.
    pub fn observe_live_execution_event(&self, session_id: &str, event: &crate::CowdEvent) {
        self.live_execution_store.observe_event(session_id, event);
    }

    /// Direct lifecycle completion for delegated Agent executions. Session
    /// roots must use the durable delivery claim/finalize protocol instead.
    pub(crate) fn complete_agent_live_execution(
        &self,
        execution_id: &str,
        report: &harness_contract::context::ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
    ) {
        self.live_execution_store
            .complete(execution_id, report, write_attempt_paths, terminal_ref);
    }

    pub fn claim_live_terminal_fence(
        &self,
        execution_id: &str,
        terminal_ref: String,
        status: harness_contract::projection::ExecutionLiveStatus,
        session_generation: u64,
    ) -> Result<crate::execution_live::TerminalFenceClaim, String> {
        self.live_execution_store.claim_terminal(
            execution_id,
            terminal_ref,
            status,
            session_generation,
        )
    }

    pub fn finalize_live_terminal_fence(
        &self,
        execution_id: &str,
        terminal_ref: &str,
        status: harness_contract::projection::ExecutionLiveStatus,
        session_generation: u64,
    ) -> Result<crate::execution_live::TerminalFenceClaim, String> {
        self.live_execution_store.finalize_terminal(
            execution_id,
            terminal_ref,
            status,
            session_generation,
        )
    }

    /// Add the full Runtime report to an already-finalized Session terminal.
    /// The store verifies status and terminal identity and cannot perform a
    /// lifecycle transition through this API.
    pub fn enrich_finalized_session_live(
        &self,
        execution_id: &str,
        expected_status: harness_contract::projection::ExecutionLiveStatus,
        terminal_ref: String,
        report: &harness_contract::context::ContextTurnReport,
        write_attempt_paths: &[String],
        error: Option<String>,
    ) -> Result<(), String> {
        self.live_execution_store.enrich_finalized_session_terminal(
            execution_id,
            expected_status,
            terminal_ref,
            report,
            write_attempt_paths,
            error,
        )
    }

    pub fn abort_live_terminal_fence(
        &self,
        execution_id: &str,
        terminal_ref: &str,
        session_generation: u64,
    ) -> Result<bool, String> {
        self.live_execution_store.abort_terminal_claim(
            execution_id,
            terminal_ref,
            session_generation,
        )
    }

    /// Release the derived live checkpoint only after its canonical Session
    /// terminal has committed and the live projection has finalized.
    pub fn release_live_terminal_fence(&self, execution_id: &str) {
        if !execution_id.trim().is_empty() {
            self.live_execution_store
                .release_terminal_checkpoint(execution_id);
        }
    }

    /// Direct lifecycle failure for delegated Agent executions only.
    pub(crate) fn fail_agent_live_execution(&self, execution_id: &str, error: String) {
        self.live_execution_store.fail(execution_id, error);
    }

    /// Direct lifecycle blocking for delegated Agent executions only.
    pub(crate) fn block_agent_live_execution(
        &self,
        execution_id: &str,
        report: &harness_contract::context::ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
        reason: String,
    ) {
        self.live_execution_store.block(
            execution_id,
            report,
            write_attempt_paths,
            terminal_ref,
            reason,
        );
    }

    /// Atomically claim cancellation as the live terminal winner. A concurrent
    /// normal terminal transition uses the same sharded record lock; exactly
    /// one transition can leave a non-terminal state.
    pub(crate) fn try_cancel_agent_live_execution(
        &self,
        execution_id: &str,
        detail: String,
    ) -> Result<bool, String> {
        self.live_execution_store.cancel(execution_id, detail)
    }

    /// Direct lifecycle cancellation for delegated Agent executions only.
    pub(crate) fn cancel_agent_live_execution(&self, execution_id: &str, detail: String) {
        let _ = self.try_cancel_agent_live_execution(execution_id, detail);
    }

    #[must_use]
    pub fn execution_live(
        &self,
        execution_id: &str,
    ) -> Option<harness_contract::projection::ExecutionLiveState> {
        self.live_execution_store.execution_live(execution_id)
    }

    #[must_use]
    pub(crate) fn execution_model_usage_snapshot(
        &self,
        execution_id: &str,
    ) -> Option<crate::execution_live::ExecutionModelUsageSnapshot> {
        self.live_execution_store.model_usage_snapshot(execution_id)
    }

    #[must_use]
    pub fn session_execution_index(
        &self,
        session_id: &str,
    ) -> harness_contract::projection::SessionExecutionIndexProjection {
        self.live_execution_store
            .session_execution_index(session_id)
    }

    #[must_use]
    pub fn running_session_execution_indices(
        &self,
    ) -> Vec<harness_contract::projection::SessionExecutionIndexProjection> {
        self.live_execution_store
            .running_session_execution_indices()
    }

    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    #[must_use]
    pub fn fixture_event_port(&self) -> RuntimeFixtureEventPort {
        RuntimeFixtureEventPort {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Delivery-only terminal outbox port for the Gateway session bridge.
    #[must_use]
    pub fn session_terminal_delivery(&self) -> SessionTerminalDeliveryPort {
        SessionTerminalDeliveryPort {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Consume an already verified human decision lease exactly once.  The
    /// signed lease is verified before it reaches this method; this Runtime
    /// command persists the replay fence alongside the workspace ledger so a
    /// process restart cannot resurrect a release decision.
    pub fn consume_verified_decision_lease(
        &self,
        lease: crate::VerifiedDecisionLease,
    ) -> Result<(), String> {
        let consumed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.event_store
            .consume_verified_decision_lease(
                lease.lease_id(),
                lease.principal_id(),
                lease.review_id(),
                lease.action(),
                lease.scope(),
                lease.evidence_digest(),
                lease.credential_epoch(),
                consumed_at_ms,
            )
            .map_err(|error| error.to_string())
    }

    /// Runtime-owned startup migration command.  Gateway may trigger the
    /// command at startup but never obtains the raw event-store committer.
    pub fn import_legacy_execution_receipt(
        &self,
        receipt_path: &Path,
        version: &str,
    ) -> Result<(), String> {
        crate::upgrade::LegacyExecutionImporter::new(
            Arc::clone(&self.event_store),
            &self.workspace_key,
            &self.workspace_root,
            version,
        )
        .import_receipt_file(receipt_path)
        .map(|_| ())
        .map_err(|error| error.to_string())
    }
    pub fn executor_registry(&self) -> &Arc<NodeExecutorRegistry> {
        &self.executor_registry
    }
    pub fn model_step_executor(&self) -> &Arc<ScopedNodeExecutor> {
        &self.model_step_executor
    }
    pub fn tool_batch_executor(&self) -> &Arc<ScopedNodeExecutor> {
        &self.tool_batch_executor
    }
    pub fn cross_plane_connector_executor(&self) -> &Arc<ScopedNodeExecutor> {
        &self.cross_plane_connector_executor
    }
    pub fn agent_task_executor(&self) -> &Arc<AgentTaskExecutor> {
        &self.agent_task_executor
    }
    pub fn agent_runtime(&self) -> &Arc<AgentRuntime> {
        &self.agent_runtime
    }

    /// Install an immutable ProcessJsonl command manifest before compiling
    /// Agent work that references it. The manifest digest is checked again
    /// against the frozen Agent Binding at execution time.
    pub(crate) fn register_process_jsonl_command(
        &self,
        spec: ProcessJsonlSpec,
    ) -> Result<(), RuntimeServicesError> {
        self.agent_runtime
            .register_process_jsonl_command(spec)
            .map_err(RuntimeServicesError::AgentRuntime)
    }

    pub(crate) fn agentic_dispatch_flights(&self) -> Arc<Mutex<BTreeSet<String>>> {
        Arc::clone(&self.agentic_dispatch_flights)
    }

    /// Runtime-owned, immutable terminal run evidence. Consumers receive
    /// projections only and cannot amend a Definition's self-model directly.
    #[must_use]
    pub fn agent_run_evaluations(&self) -> Vec<crate::AgentRunEvaluation> {
        self.agent_runtime.evaluations()
    }

    #[must_use]
    pub fn agent_self_models(&self) -> Vec<crate::AgentSelfModel> {
        self.agent_runtime.self_models()
    }

    /// Recompute all active Canary observations from the canonical terminal
    /// Agent evidence. The method is intentionally idempotent and does not
    /// authorize Stable promotion; it only refreshes the evidence consumed by
    /// the separate typed Stable-review gate.
    pub fn refresh_evolution_canary_observations(
        &self,
    ) -> Result<Vec<crate::EvolutionGovernanceCandidate>, RuntimeServicesError> {
        self.evolution_governance
            .refresh_canary_observations_from_agent_runs(&self.agent_runtime.evaluations())
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }
    pub fn verify_executor(&self) -> &Arc<VerifyNodeExecutor> {
        &self.verify_executor
    }
    pub fn synthesize_executor(&self) -> &Arc<SynthesizeNodeExecutor> {
        &self.synthesize_executor
    }
    pub fn graph_state_store(&self) -> &ExecutionGraphStateStore {
        &self.graph_state_store
    }
    pub fn hot_state(&self) -> &Arc<crate::execution_core::hot_state::RuntimeHotStatePlane> {
        &self.hot_state
    }

    #[must_use]
    pub fn hot_session_snapshot(
        &self,
        session_id: &str,
    ) -> Option<Arc<crate::execution_core::hot_state::HotSessionSnapshot>> {
        let snapshot = self.hot_state.sessions().get(session_id)?;
        let mut snapshot = (*snapshot).clone();
        snapshot.pending_approvals = self
            .approval_queue
            .pending()
            .into_iter()
            .filter(|request| request.source.session_id.as_deref() == Some(session_id))
            .count();
        Some(Arc::new(snapshot))
    }

    #[must_use]
    pub fn hot_session_snapshots(
        &self,
        session_ids: &[String],
    ) -> Vec<Arc<crate::execution_core::hot_state::HotSessionSnapshot>> {
        let mut pending_approvals = HashMap::<String, usize>::new();
        for request in self.approval_queue.pending() {
            if let Some(session_id) = request.source.session_id {
                *pending_approvals.entry(session_id).or_default() += 1;
            }
        }
        self.hot_state
            .sessions()
            .get_many(session_ids)
            .into_iter()
            .map(|snapshot| {
                let mut snapshot = (*snapshot).clone();
                snapshot.pending_approvals = pending_approvals
                    .get(&snapshot.session_id)
                    .copied()
                    .unwrap_or_default();
                Arc::new(snapshot)
            })
            .collect()
    }

    pub fn update_hot_session_input(
        &self,
        projection: &harness_contract::turn::SessionInputProjection,
        inbox: &harness_contract::turn::TurnInboxSnapshot,
    ) {
        if projection.session_id != inbox.session_id {
            tracing::warn!(
                projection_session_id = %projection.session_id,
                inbox_session_id = %inbox.session_id,
                "refused mismatched hot Session input projection"
            );
            return;
        }
        self.hot_state
            .sessions()
            .update(&projection.session_id, |snapshot| {
                if let Some(cursor) = projection.admitted_cursor {
                    snapshot.generation = snapshot.generation.max(cursor.generation);
                    snapshot.accepted_cursor = snapshot.accepted_cursor.max(cursor.sequence);
                    snapshot.durable_cursor = Some(
                        snapshot
                            .durable_cursor
                            .map_or(cursor.sequence, |current| current.max(cursor.sequence)),
                    );
                }
                if let Some(cursor) = projection.consumed_cursor {
                    snapshot.generation = snapshot.generation.max(cursor.generation);
                    snapshot.runtime_cursor = snapshot.runtime_cursor.max(cursor.sequence);
                }
                snapshot.current_turn_id =
                    projection.active_turn_id.as_ref().map(ToString::to_string);
                snapshot.pending_inputs = projection.pending_count;
                snapshot.inbox_refs = inbox
                    .items
                    .iter()
                    .filter(|item| item.consumed_at.is_none())
                    .map(|item| format!("session-input:{}", item.input_id))
                    .collect();
            });
    }

    pub fn record_hot_session_durable_ingress(&self, session_id: &str, durable_cursor: u64) {
        self.hot_state.sessions().update(session_id, |snapshot| {
            snapshot.durable_cursor = Some(
                snapshot
                    .durable_cursor
                    .map_or(durable_cursor, |current| current.max(durable_cursor)),
            );
        });
    }

    #[must_use]
    pub fn hot_state_health(&self) -> crate::execution_core::hot_state::HotStateHealth {
        self.hot_state.health()
    }

    pub fn update_hot_state_config(
        &self,
        config: &crate::execution_core::hot_state::HotStateConfig,
    ) -> Result<crate::execution_core::hot_state::HotStateHealth, String> {
        self.hot_state.reconfigure(config)?;
        Ok(self.hot_state.health())
    }
    /// Recover original user-input receipt facts after Session hydration. This
    /// never consumes a historical input as a fresh model request.
    pub async fn recover_user_input_applications(&self, session_id: &str) -> Result<usize, String> {
        let Some(query) = self.session_query_port() else {
            return Ok(0);
        };
        let Some(admission) = query
            .input_admission(session_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(0);
        };
        let mut after = None;
        let mut graphs = BTreeSet::new();
        loop {
            let page = query
                .pending_session_input_applications(
                    session_id,
                    admission.generation,
                    after.clone(),
                    32,
                )
                .await
                .map_err(|e| e.to_string())?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let Some(receipt) = &row.application_receipt else {
                    continue;
                };
                let Some(identity) = &receipt.prepared_application else {
                    continue;
                };
                let known_goal = self
                    .event_store()
                    .event_by_idempotency_key(
                        &format!("goal:{}", identity.goal_id),
                        &format!(
                            "goal-user-input:{}:{}",
                            identity.goal_id, identity.source_digest
                        ),
                    )
                    .map_err(|e| e.to_string())?
                    .is_some();
                let known_graph_result = self
                    .graph_state_store()
                    .load(&identity.graph_id)
                    .ok()
                    .is_some_and(|graph| {
                        graph
                            .node_results
                            .get(&identity.node_id)
                            .and_then(|r| r.result_ref.as_deref())
                            == Some(
                                format!("user_input_application:{}", identity.artifact_ref)
                                    .as_str(),
                            )
                    });
                if known_goal || known_graph_result || receipt.cancellation.is_some() {
                    graphs.insert(identity.graph_id.clone());
                }
            }
            let last = page.last().unwrap();
            let next = (last.sequence, last.input_id.clone());
            if after.as_ref().is_some_and(|old| old >= &next) {
                return Err("user application discovery cursor did not advance".into());
            }
            after = Some(next);
            if page.len() < 32 {
                break;
            }
        }
        for graph in &graphs {
            super::goal::reconcile_user_goal_applications(self, graph).await?;
        }
        Ok(graphs.len())
    }

    pub fn commit_service(&self) -> &ExecutionCommitService {
        &self.commit_service
    }
    /// Re-evaluate the root barrier for one Program, synchronously.
    ///
    /// Every Program mutation used to reach the barrier only through a wake: the
    /// node's own `after_commit`, a graph-settled observer, or a projection lane. All
    /// three missed the decisive moment in paid runs - one run recorded `wait` at
    /// revision 38, the final accept landed 23 seconds later, and the next resolver
    /// pass was ten minutes after that, one second after the evaluator had cancelled
    /// the barrier. The barrier now also re-arms itself, but a synchronous call in the
    /// same path as the mutation removes the race entirely for the common case.
    pub async fn resolve_agentic_program_barrier(&self, program_id: &str) -> Result<usize, String> {
        crate::execution_core::graph::executors::resolve_agentic_program_wait(
            program_id,
            &self.agent_action_service(),
            self.graph_state_store(),
            self.execution_supervisor().as_ref(),
            true,
        )
        .await
    }

    pub fn execution_supervisor(&self) -> &Arc<crate::RuntimeExecutionSupervisor> {
        &self.execution_supervisor
    }

    /// Durably cancel an execution graph and every graph registered beneath it.
    ///
    /// Session cancellation cannot stop at the process-local provider token or
    /// the live projection: Team/Subgraph work may already have been admitted
    /// into independent supervisor slots. The lineage stream is the canonical
    /// ownership relation, so walk it breadth-first and terminalize every
    /// non-terminal descendant. A graph that is already terminal is still
    /// traversed because its children may outlive a failed or cancelled parent.
    pub async fn cancel_execution_tree(
        &self,
        root_execution_id: &str,
        reason: &str,
    ) -> Result<Vec<String>, RuntimeServicesError> {
        if root_execution_id.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut pending = VecDeque::from([root_execution_id.to_string()]);
        let mut seen = BTreeSet::new();
        let mut cancelled = Vec::new();
        while let Some(graph_id) = pending.pop_front() {
            if !seen.insert(graph_id.clone()) {
                continue;
            }

            // Discover children even when the parent is already terminal. This
            // is the exact crash/race shape that previously left Team
            // synthesizers running after a Session cancellation won.
            for link in self
                .graph_state_store
                .child_links_async(graph_id.clone())
                .await?
            {
                pending.push_back(link.child_execution_id);
            }

            let mut terminalized = false;
            for _ in 0..4 {
                let graph = match self.graph_state_store.load_async(&graph_id).await {
                    Ok(graph) => graph,
                    Err(ExecutionStateStoreError::NotFound { .. }) => break,
                    Err(error) => return Err(error.into()),
                };
                if graph.nodes.iter().all(|node| {
                    graph
                        .node_statuses
                        .get(&node.id)
                        .is_some_and(|status| status.is_terminal())
                }) {
                    break;
                }
                match self
                    .execution_supervisor
                    .command_graph(
                        &graph_id,
                        ExecutionGraphCommand::Cancel {
                            expected_revision: graph.revision,
                            reason: reason.to_string(),
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        terminalized = true;
                        break;
                    }
                    Err(ExecutionRunnerError::Commit(
                        super::graph::ExecutionCommitError::StaleRevision { .. },
                    )) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            if terminalized {
                cancelled.push(graph_id.clone());
            }

            // A running parent can register a child concurrently with the
            // first observation. Re-read after cancellation and enqueue only
            // newly committed lineage; `seen` keeps replay idempotent.
            for link in self.graph_state_store.child_links_async(graph_id).await? {
                if !seen.contains(&link.child_execution_id) {
                    pending.push_back(link.child_execution_id);
                }
            }
        }
        Ok(cancelled)
    }

    pub fn approval_queue(&self) -> &Arc<ApprovalQueue> {
        &self.approval_queue
    }

    /// Materialize the current decision for the ordinary evidence reader.
    /// The approval journal remains the authority; the blob is derived data.
    pub async fn approval_result_content(
        &self,
        session_id: &str,
        reference: &str,
    ) -> Result<
        (
            harness_contract::context::ArtifactRef,
            harness_contract::policy::ApprovalRequest,
            crate::ExpectedStreamRevision,
        ),
        String,
    > {
        let (graph_id, node_id) =
            crate::execution_core::graph::executors::parse_graph_approval_id(reference)
                .ok_or("external result requires a canonical graph approval reference")?;
        let (request, source) = self
            .approval_queue
            .decision_result_snapshot(session_id, reference)?;
        let graph = self
            .graph_state_store()
            .load(&graph_id)
            .map_err(|error| error.to_string())?;
        if !graph
            .lineage
            .as_ref()
            .is_some_and(|lineage| lineage.session_id == session_id)
            || !graph.nodes.iter().any(|node| {
                node.id == node_id
                    && node.kind == harness_contract::execution_graph::ExecutionNodeKind::Approval
            })
        {
            return Err("external decision has no matching Session graph approval source".into());
        }
        let bytes = serde_json::to_vec(&serde_json::json!({
            "kind":"external_decision", "approval":request, "source_revision":source.expected_revision,
            "scope":"approval_decision_only"
        })).map_err(|error| error.to_string())?;
        // The source revision and exact bytes identify this derived immutable
        // content. Repeated authorized pages must not mint different selectors.
        use sha2::{Digest, Sha256};
        let identity = format!(
            "approval_result_{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "cowd.approval-result/v1",
                    session_id,
                    reference,
                    &source.stream_id,
                    source.expected_revision,
                    format!("{:x}", Sha256::digest(&bytes)),
                ))
                .map_err(|error| error.to_string())?
            )
        );
        let content = self
            .artifact_store()
            .write_bytes_with_identity(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/json".into(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                &bytes,
                identity,
            )
            .await
            .map_err(|error| error.to_string())?;
        if self
            .event_store()
            .stream_revision(&source.stream_id)
            .map_err(|error| error.to_string())?
            != source.expected_revision
        {
            return Err("external decision changed while materializing its content; retry".into());
        }
        Ok((content, request, source))
    }
    pub fn approval_coordinator(&self) -> &Arc<ApprovalCoordinator> {
        &self.approval_coordinator
    }
    pub fn execution_capacity_profile(&self) -> &Arc<crate::ExecutionCapacityProfile> {
        &self.execution_capacity_profile
    }

    pub fn publish_session_execution_policy(
        &self,
        session_id: impl Into<String>,
        control: crate::permissions::SessionExecutionPolicyControl,
    ) {
        let session_id = session_id.into();
        self.session_execution_policy_controls
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id, control);
    }

    #[must_use]
    pub fn session_execution_policy(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::policy::SessionExecutionPolicy> {
        self.session_execution_policy_controls
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
    }

    #[must_use]
    pub fn session_execution_policy_control(
        &self,
        session_id: &str,
    ) -> Option<crate::permissions::SessionExecutionPolicyControl> {
        self.session_execution_policy_controls
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
    }

    /// Pin the original published policy owner and its live revision for a
    /// synchronous caller. This does not authenticate durable Session claim
    /// or replace the original transaction's checked source contract.
    pub(crate) fn with_original_session_policy<T>(
        &self, session_id: &str, original: &crate::permissions::SessionExecutionPolicyControl,
        f: impl FnOnce(&harness_contract::policy::SessionExecutionPolicy) -> Result<T, String>,
    ) -> Result<T, String> {
        with_original_session_policy_handles(
            &self.session_execution_policy_admission_blocks,
            &self.session_execution_policy_controls, session_id, original, f,
        )
    }

    pub(crate) fn session_execution_policy_controls_handle(
        &self,
    ) -> Arc<RwLock<BTreeMap<String, crate::permissions::SessionExecutionPolicyControl>>> {
        Arc::clone(&self.session_execution_policy_controls)
    }

    pub(crate) fn session_execution_policy_admission_blocks_handle(
        &self,
    ) -> Arc<RwLock<BTreeMap<String, String>>> {
        Arc::clone(&self.session_execution_policy_admission_blocks)
    }

    /// Fence every new Task admission for one Session while Gateway drains a
    /// policy revision. Existing Tasks retain their immutable binding.
    pub fn freeze_session_execution_policy_admission(
        &self,
        session_id: impl Into<String>,
        transition_id: impl Into<String>,
    ) {
        self.session_execution_policy_admission_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.into(), transition_id.into());
    }

    pub fn unfreeze_session_execution_policy_admission(
        &self,
        session_id: &str,
        transition_id: &str,
    ) -> bool {
        let mut blocks = self
            .session_execution_policy_admission_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if blocks.get(session_id).map(String::as_str) != Some(transition_id) {
            return false;
        }
        blocks.remove(session_id);
        true
    }

    /// Returns every non-terminal durable graph whose canonical Task remains
    /// bound to one exact Session policy revision. This is the Runtime-owned
    /// drain truth for background/Mission work that has no Gateway turn guard.
    pub async fn active_graphs_for_session_policy_revision(
        &self,
        session_id: &str,
        policy_revision: u64,
    ) -> Result<Vec<(String, u64)>, RuntimeServicesError> {
        let mut active = Vec::new();
        for graph_id in self.graph_state_store.nonterminal_graph_ids_async().await? {
            let graph = self.graph_state_store.load_async(&graph_id).await?;
            let Some(task_id) = graph
                .lineage
                .as_ref()
                .map(|lineage| lineage.task_id.as_str())
            else {
                continue;
            };
            let Some(task) = self
                .task_aggregate_service
                .get(task_id)
                .map_err(RuntimeServicesError::Invariant)?
            else {
                continue;
            };
            let Some(binding) = task.execution_policy.binding.as_ref() else {
                continue;
            };
            if binding.execution.session_id == session_id
                && binding.execution.policy_revision == policy_revision
            {
                active.push((graph.id, graph.revision));
            }
        }
        active.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(active)
    }

    /// Returns already-admitted Task attempts even during the narrow window
    /// before their graph is submitted. Counting the Task binding closes the
    /// freeze-versus-submit race that a graph-only drain cannot observe.
    pub async fn active_tasks_for_session_policy_revision(
        &self,
        session_id: &str,
        policy_revision: u64,
    ) -> Result<Vec<(String, u64)>, RuntimeServicesError> {
        let candidates = self
            .task_aggregate_service
            .list()
            .map_err(RuntimeServicesError::Invariant)?
            .into_iter()
            .filter(|task| !task.status.is_terminal())
            .filter(|task| {
                task.execution_policy
                    .binding
                    .as_ref()
                    .is_some_and(|binding| {
                        binding.execution.session_id == session_id
                            && binding.execution.policy_revision == policy_revision
                    })
            })
            .collect::<Vec<_>>();
        let mut active = Vec::new();
        for task in candidates {
            let mut has_live_graph = task.graph_refs.is_empty();
            for graph_ref in &task.graph_refs {
                let graph = self
                    .graph_state_store
                    .load_async(&graph_ref.graph_id)
                    .await?;
                if graph
                    .node_statuses
                    .values()
                    .any(|status| !status.is_terminal())
                {
                    has_live_graph = true;
                    break;
                }
            }
            if has_live_graph {
                active.push((task.task_id, task.revision));
            }
        }
        active.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(active)
    }

    /// Requests exact durable cancellation for old-revision graphs and their
    /// admitted Tasks after the transition drain grace expires.
    /// Revision races are harmless: the next drain observation reloads
    /// canonical state and retries only live attempts.
    pub async fn cancel_attempts_for_session_policy_revision(
        self: &Arc<Self>,
        session_id: &str,
        policy_revision: u64,
        reason: &str,
    ) -> Result<usize, RuntimeServicesError> {
        // Freeze the Task set before graph cancellation. Once a graph becomes
        // terminal it no longer appears in the active-task projection, but its
        // owning Task must still be terminalized under the old policy revision.
        let tasks = self
            .active_tasks_for_session_policy_revision(session_id, policy_revision)
            .await?;
        let graphs = self
            .active_graphs_for_session_policy_revision(session_id, policy_revision)
            .await?;
        let mut cancelled = 0usize;
        for (graph_id, revision) in graphs {
            match self
                .execution_supervisor
                .command_graph(
                    &graph_id,
                    harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                        expected_revision: revision,
                        reason: reason.to_string(),
                    },
                )
                .await
            {
                Ok(_) => cancelled = cancelled.saturating_add(1),
                Err(crate::execution_core::ExecutionRunnerError::Commit(
                    crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
                )) => {}
                Err(error) => return Err(RuntimeServicesError::GraphRunner(error)),
            }
        }
        for (task_id, _) in tasks {
            let Some(task) = self
                .task_aggregate_service
                .get(&task_id)
                .map_err(RuntimeServicesError::Invariant)?
            else {
                continue;
            };
            let still_bound_to_old_revision =
                task.execution_policy
                    .binding
                    .as_ref()
                    .is_some_and(|binding| {
                        binding.execution.session_id == session_id
                            && binding.execution.policy_revision == policy_revision
                    });
            if task.status.is_terminal() || !still_bound_to_old_revision {
                continue;
            }
            if task.structured_registration.is_some() {
                self.cancel_structured_task(&task.task_id, task.revision)
                    .await
                    .map_err(RuntimeServicesError::Task)?;
                cancelled = cancelled.saturating_add(1);
                continue;
            }
            match self.task_runtime_port().transition(
                &task.task_id,
                task.revision,
                harness_contract::task::TaskStatus::Cancelled,
                vec![harness_contract::reality::EvidenceRef::observed(
                    "session_policy_transition",
                    format!("{session_id}:{policy_revision}"),
                )],
                reason,
            ) {
                Ok(_) => cancelled = cancelled.saturating_add(1),
                Err(error) if error.contains("stale") || error.contains("terminal") => {}
                Err(error) => return Err(RuntimeServicesError::Invariant(error)),
            }
        }
        Ok(cancelled)
    }

    /// Attach the current Session policy revision to an approval context.
    /// Non-Session governance domains remain independent and are returned
    /// unchanged.
    #[must_use]
    pub fn bind_session_policy_to_approval_context(
        &self,
        context: harness_contract::policy::ApprovalContext,
    ) -> harness_contract::policy::ApprovalContext {
        let Some(session_id) = context.session_id.as_deref() else {
            return context;
        };
        match self.session_execution_policy(session_id) {
            Some(policy) => context.with_execution_policy(&policy),
            None => context,
        }
    }

    pub fn remove_session_execution_policy(&self, session_id: &str) {
        self.session_execution_policy_controls
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_execution_policy_admission_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
    }
    /// Runtime is the single owner of evolution candidates, evaluation
    /// eligibility and release-change review projections. Gateway and
    /// surfaces consume this service rather than keeping a second registry.
    pub fn record_evolution_signal(
        &self,
        signal: crate::EvolutionSignal,
    ) -> Result<crate::EvolutionSignal, RuntimeServicesError> {
        // Stop and this synchronous derived-work admission share the original
        // policy fence. No Provider future or domain drain holds this lock.
        let policy = self
            .background_work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if policy.evolution {
            self.evolution_signal_projector
                .ensure_source_epoch()
                .map_err(RuntimeServicesError::Invariant)?;
        }
        self.evolution_discovery
            .record_signal_with_automatic_discovery(signal, policy.evolution)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_signals(&self) -> Result<Vec<crate::EvolutionSignal>, RuntimeServicesError> {
        self.evolution_discovery
            .list_signals()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_signal(
        &self,
        signal_id: &str,
    ) -> Result<Option<crate::EvolutionSignal>, RuntimeServicesError> {
        self.evolution_discovery
            .signal(signal_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_cases(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::EvolutionCase>, RuntimeServicesError> {
        self.evolution_discovery
            .list_cases(limit)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_case(
        &self,
        case_id: &str,
    ) -> Result<Option<crate::EvolutionCase>, RuntimeServicesError> {
        self.evolution_discovery
            .case(case_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_case_page(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<crate::EvolutionCasePage, RuntimeServicesError> {
        self.evolution_discovery
            .case_page(cursor, limit)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_case_index(&self) -> Result<crate::EvolutionCaseIndex, RuntimeServicesError> {
        self.evolution_discovery
            .case_index()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_analysis(
        &self,
        case_id: &str,
    ) -> Result<Option<harness_contract::evolution::EvolutionAnalysisDraft>, RuntimeServicesError>
    {
        crate::evolution::analyst::EvolutionAnalystService::read_draft_for_case(
            &self.event_store,
            &self.evolution_discovery,
            case_id,
        )
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn skill_maintenance_drafts(
        &self,
        limit: usize,
    ) -> Result<Vec<harness_contract::skill::SkillMaintenanceDraft>, RuntimeServicesError> {
        Ok(self.skill_maintenance_projector.drafts(limit))
    }

    pub fn skill_maintenance_draft(
        &self,
        draft_id: &str,
    ) -> Result<Option<harness_contract::skill::SkillMaintenanceDraft>, RuntimeServicesError> {
        Ok(self.skill_maintenance_projector.draft(draft_id))
    }

    pub fn skill_maintenance_health(&self) -> crate::SkillMaintenanceProjectionHealth {
        let worker_running = self
            .event_reactor
            .lane_health(crate::skill::maintenance::PROJECTOR_ID)
            .ok()
            .flatten()
            .is_some_and(|health| health.worker_running);
        self.skill_maintenance_projector
            .health_with_worker(worker_running)
    }

    pub fn request_skill_revision_activation(
        &self,
        principal: &crate::VerifiedPrincipal,
        request_id: &str,
        draft_id: &str,
        target_revision: &str,
        receipt_id: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        let draft = self
            .skill_maintenance_draft(draft_id)?
            .ok_or_else(|| RuntimeServicesError::Invariant("skill Draft not found".to_string()))?;
        self.skill_revision_governance
            .request_activation(principal, request_id, &draft, target_revision, receipt_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_skill_first_install(
        &self,
        principal: &crate::VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        target_revision: &str,
        receipt_id: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        self.skill_revision_governance
            .request_first_install(principal, request_id, skill_id, target_revision, receipt_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_skill_revision_rollback(
        &self,
        principal: &crate::VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        target_revision: &str,
        evidence_digest: &str,
        receipt_id: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        self.skill_revision_governance
            .request_rollback(
                principal,
                request_id,
                skill_id,
                target_revision,
                evidence_digest,
                receipt_id,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_skill_deactivation(
        &self,
        principal: &crate::VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        expected_generation: u64,
        reason: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        self.skill_revision_governance
            .request_deactivation(principal, request_id, skill_id, expected_generation, reason)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn skill_revision_review(
        &self,
        review_id: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        self.skill_revision_governance
            .review(review_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn decide_skill_revision_review(
        &self,
        principal: &crate::VerifiedPrincipal,
        lease: &crate::VerifiedDecisionLease,
        review_id: &str,
        decision: harness_contract::skill::SkillRevisionReviewDecision,
        reason: &str,
    ) -> Result<Option<harness_contract::skill::SkillActivePointer>, RuntimeServicesError> {
        self.skill_revision_governance
            .decide_review(principal, lease, review_id, decision, reason)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn skill_active_pointer(
        &self,
        skill_id: &str,
    ) -> Result<Option<harness_contract::skill::SkillActivePointer>, RuntimeServicesError> {
        self.skill_revision_governance
            .pointer(skill_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn mission_evidence(&self) -> &Arc<MissionEvidenceBus> {
        &self.mission_evidence
    }
    pub fn goal_store(&self) -> &Arc<GoalStore> {
        &self.goal_store
    }
    pub fn objective_supervisor(&self) -> &Arc<ObjectiveSupervisor> {
        &self.objective_supervisor
    }
    pub fn conflict_resolver(&self) -> &Arc<ConflictArbiter> {
        &self.conflict_resolver
    }
    pub fn resource_manager(&self) -> &Arc<ExecutionResourceManager> {
        &self.resource_manager
    }
    pub(crate) fn evaluation_provider_token_leases(
        &self,
    ) -> &Arc<crate::conversation::EvaluationProviderTokenLeaseRegistry> {
        &self.evaluation_provider_token_leases
    }
    pub fn session_turn_admission(&self) -> crate::SessionTurnAdmissionPort {
        crate::SessionTurnAdmissionPort::new(Arc::clone(&self.resource_manager))
    }
    pub fn tool_execution_plane(&self) -> &Arc<crate::ToolExecutionPlane> {
        &self.tool_execution_plane
    }
    pub fn scope_locks(&self) -> &Arc<ScopeLockManager> {
        &self.scope_locks
    }
    pub fn worktree_leases(&self) -> &Arc<WorktreeLeaseManager> {
        &self.worktree_leases
    }
    pub fn ensure_mutation_allowed(&self) -> Result<(), RuntimeServicesError> {
        let importer = crate::upgrade::LegacyExecutionImporter::new(
            Arc::clone(&self.event_store),
            &self.workspace_key,
            &self.workspace_root,
            "",
        );
        if importer
            .mutation_allowed()
            .map_err(|error| RuntimeServicesError::Mission(error.to_string()))?
        {
            Ok(())
        } else {
            Err(RuntimeServicesError::UpgradeRecoveryRequired)
        }
    }

    #[must_use]
    pub fn mission_schedule_policy(&self) -> crate::MissionSchedulePolicy {
        self.mission_schedule_policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub fn update_mission_schedule_policy(
        &self,
        policy: crate::MissionSchedulePolicy,
    ) -> Result<(), String> {
        policy.validate()?;
        *self
            .mission_schedule_policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
        Ok(())
    }

    /// Validate the Runtime-only candidate/scenario provenance carried by an
    /// isolated evaluation Binding immediately before execution.
    pub(crate) fn validate_agent_evaluation_binding(
        &self,
        binding: &harness_contract::agent::AgentBindingSnapshot,
    ) -> Result<(), RuntimeServicesError> {
        let Some(evaluation) = &binding.evaluation else {
            return Ok(());
        };
        self.evolution_governance
            .validate_agent_evaluation_binding(
                &binding.definition_ref,
                &evaluation.candidate_id,
                &evaluation.scenario_ref,
                evaluation.role,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }
    pub fn session_relations(&self) -> &Arc<SessionRelationGraph> {
        &self.session_relations
    }
    pub fn provider_registry(&self) -> &Arc<crate::ProviderRegistry> {
        &self.provider_registry
    }
    #[must_use]
    pub fn resource_evidence_writer_health(
        &self,
    ) -> crate::execution_core::ResourceEvidenceWriterHealth {
        self.resource_evidence_writer.health()
    }
    #[must_use]
    pub fn provider_resource_config(&self) -> Arc<RwLock<crate::ProviderResourceConfig>> {
        Arc::clone(&self.provider_resource_config)
    }
    pub fn replace_provider_resource_config(
        &self,
        config: crate::ProviderResourceConfig,
    ) -> Result<Vec<crate::execution_core::graph::ExecutionResourceSnapshot>, String> {
        config.validate()?;
        let generation = config.materialize(&self.provider_registry.pin());
        let mut quotas = default_resource_quotas();
        quotas.retain(|(kind, _)| {
            !matches!(
                kind,
                ExecutionResourceKind::Provider
                    | ExecutionResourceKind::ProviderAccount(_)
                    | ExecutionResourceKind::ProviderModel(_)
                    | ExecutionResourceKind::ProviderTokenPool(_)
            )
        });
        // Provider reload does not own the Context byte budget. Preserve its
        // configured ceiling (including explicit absence), rather than resetting
        // a narrowed content budget to the process default.
        quotas.retain(|(kind, _)| *kind != ExecutionResourceKind::ContextBytes);
        for current in self
            .resource_manager
            .snapshots()
            .map_err(|e| e.to_string())?
        {
            if current.kind == ExecutionResourceKind::ContextBytes {
                quotas.push((
                    current.kind,
                    ResourceQuota {
                        minimum: current.minimum,
                        target: current.target,
                        maximum: current.maximum,
                    },
                ));
            }
        }
        quotas.extend(generation.quotas);
        let snapshots = self
            .resource_manager
            .reconcile_quotas(quotas, generation.reserves)
            .map_err(|error| error.to_string())?;
        *self
            .provider_resource_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = config;
        Ok(snapshots)
    }
    /// Return the current ordered fallback policy. Each model request reads
    /// this snapshot so Gateway config reloads affect already-open Sessions
    /// and their child Agents without mutating an in-flight request.
    #[must_use]
    pub fn provider_fallbacks(&self) -> Vec<String> {
        self.provider_fallbacks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    #[must_use]
    pub(crate) fn provider_fallback_policy(&self) -> Arc<RwLock<Vec<String>>> {
        Arc::clone(&self.provider_fallbacks)
    }
    pub fn replace_provider_fallbacks(
        &self,
        fallbacks: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
        let normalized = normalize_provider_fallbacks(fallbacks);
        *self
            .provider_fallbacks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = normalized.clone();
        normalized
    }
    pub fn provider_transport_pool(&self) -> &Arc<crate::ProviderTransportPool> {
        &self.provider_transport_pool
    }
    pub fn provider_template_cache(&self) -> &Arc<crate::ProviderClientTemplateCache> {
        &self.provider_template_cache
    }
    pub fn artifact_store(&self) -> &Arc<crate::ArtifactStore> {
        &self.artifact_store
    }

    /// Resolve a model-visible logical evidence id through the Session owner.
    pub async fn session_evidence_access(
        &self,
        session_id: &str,
        evidence_id: &str,
    ) -> Result<Option<harness_contract::context::EvidenceAccessRef>, session::SessionError> {
        let Some(query) = self.session_query_port.get() else {
            return Ok(None);
        };
        query.evidence_access(session_id, evidence_id).await
    }
    pub fn tool_execution_host(&self) -> Option<&Arc<dyn crate::RuntimeExecutionHost>> {
        self.tool_execution_host.as_ref()
    }
    pub fn session_input_router(&self) -> Option<&Arc<SessionInputRouter>> {
        self.session_input_router.get()
    }

    #[must_use]
    pub(crate) fn session_query_port(&self) -> Option<Arc<dyn crate::SessionRuntimeQueryPort>> {
        self.session_query_port.get().cloned()
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub(crate) fn session_ingress_port(&self) -> Option<Arc<dyn crate::SessionRuntimeIngressPort>> {
        self.session_ingress_port.get().cloned()
    }

    #[must_use]
    pub(crate) fn session_journal_port(&self) -> Option<Arc<dyn crate::SessionRuntimeJournalPort>> {
        self.session_journal_port.get().cloned()
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub(crate) fn session_application_port(
        &self,
    ) -> Option<Arc<dyn crate::SessionRuntimeApplicationPort>> {
        self.session_application_port.get().cloned()
    }

    #[must_use]
    pub fn session_history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        self.session_query_port
            .get()
            .and_then(|port| port.history_reader())
    }
}

/// Evaluation work uses the normal tool/Skill execution plane, but only after
/// Runtime has frozen an explicit resource ceiling. Mutating tools are safe
/// here because each side receives a distinct output lease and every effect
/// still crosses the scoped ToolHost receipt/fence boundary.
fn validate_evolution_scenario_isolation(
    scenario: &EvaluationScenarioSpec,
    tool_host: Option<&dyn crate::RuntimeExecutionHost>,
    skill_catalog: &crate::RuntimeSkillCatalog,
) -> Result<(), RuntimeServicesError> {
    let shared_write_scopes = scenario
        .resource_scopes
        .iter()
        .filter(|scope| scope.starts_with("write:") || scope.starts_with("workspace:"))
        .cloned()
        .collect::<Vec<_>>();
    if !shared_write_scopes.is_empty() {
        return Err(RuntimeServicesError::Invariant(format!(
            "paired evolution scenario cannot share writable input scopes: {}",
            shared_write_scopes.join(", ")
        )));
    }
    let installed_skills = skill_catalog
        .profiles()
        .map_err(RuntimeServicesError::Invariant)?
        .into_iter()
        .map(|profile| profile.skill_id)
        .collect::<BTreeSet<_>>();
    let missing_skills = scenario
        .allowed_skills
        .iter()
        .filter(|skill| !installed_skills.contains(*skill))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_skills.is_empty() {
        return Err(RuntimeServicesError::Invariant(format!(
            "paired evolution evaluation has unavailable Skills: {}",
            missing_skills.join(", ")
        )));
    }
    let lease = tool_host.and_then(crate::RuntimeExecutionHost::pin_step_catalog);
    let metadata = lease.as_ref().map(tools::ToolHostLease::catalog_receipt);
    let requested = scenario
        .allowed_tools
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let definitions = tool_host
        .map(|host| host.delegated_tool_definitions(&scenario.allowed_tools))
        .unwrap_or_default();
    let projection = lease
        .as_ref()
        .zip(metadata.as_ref())
        .map(|(lease, metadata)| {
            crate::tool_host::DelegatedCatalogProjection::new(lease, metadata, &requested)
        });
    let registered = |tool: &str| {
        metadata
            .as_ref()
            .zip(projection.as_ref())
            .is_some_and(|(metadata, projection)| {
                metadata.descriptors.iter().any(|descriptor| {
                    descriptor.canonical_id == tool
                        && descriptor.health
                            == harness_contract::tool::ToolDescriptorHealth::Healthy
                        && scenario
                            .permission_ceiling
                            .permits(descriptor.required_permission)
                        && definitions.iter().any(|definition| {
                            definition.name == tool && projection.registered(definition)
                        })
                })
            })
    };
    let unknown_tools = scenario
        .allowed_tools
        .iter()
        .filter(|tool| !registered(tool))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown_tools.is_empty() {
        return Err(RuntimeServicesError::Invariant(format!(
            "paired evolution evaluation has tools without healthy pinned catalog metadata: {}",
            unknown_tools.join(", ")
        )));
    }
    for requirement in &scenario.acceptance_checks {
        use harness_contract::{evaluation::EvaluationAcceptanceCheck, tool::ToolEffectKind};
        let effect = match requirement.check {
            EvaluationAcceptanceCheck::IsolatedWorkspaceChange { .. } => ToolEffectKind::Write,
            EvaluationAcceptanceCheck::IsolatedWorkspaceRead => ToolEffectKind::Read,
            EvaluationAcceptanceCheck::Output { .. } => continue,
        };
        let available = scenario.allowed_tools.iter().any(|tool| {
            let Some((lease, metadata)) = lease.as_ref().zip(metadata.as_ref()) else {
                return false;
            };
            let resolver = lease.snapshot().catalog.effect_resolver(tool);
            if resolver.resolver_id == "builtin.orchestrate" || !registered(tool) {
                return false;
            }
            if effect == ToolEffectKind::Write {
                return tool == "edit"
                    && resolver == tools::builtin_effect_resolver_spec(tool)
                    && metadata.descriptors.iter().any(|descriptor| {
                        descriptor.canonical_id == *tool && descriptor.source == "builtin"
                    })
                    && scenario
                        .permission_ceiling
                        .permits(harness_contract::policy::PermissionMode::WorkspaceWrite);
            }
            definitions
                .iter()
                .find(|definition| definition.name == *tool)
                .and_then(|definition| {
                    projection.as_ref()?.discover(
                        definition,
                        &requested,
                        scenario.permission_ceiling,
                        Some(&scenario.resource_scopes),
                    )
                })
                .is_some_and(|_| {
                    (tool == "read_file" && resolver == tools::builtin_effect_resolver_spec(tool))
                        || {
                            let probe = lease.describe_effect(tool, &serde_json::json!({}));
                            probe.effect_kind == effect
                                && probe.scopes.iter().any(|scope| {
                                    scope.resource
                                        == harness_contract::policy::PermissionResource::File
                                })
                        }
                })
        });
        if !available {
            return Err(RuntimeServicesError::Invariant(format!(
                "paired evaluation acceptance `{}` has no authorized file {effect:?} tool",
                requirement.criterion
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod discovery_inventory_tests {
    use super::*;

    struct CatalogHost(tools::ToolHostLease);

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for CatalogHost {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.0.clone())
        }

        fn delegated_tool_definitions(
            &self,
            names: &[String],
        ) -> Vec<crate::ProviderToolDefinition> {
            self.0
                .snapshot()
                .catalog
                .definitions(None)
                .into_iter()
                .filter(|definition| names.contains(&definition.name))
                .map(|definition| crate::ProviderToolDefinition {
                    name: definition.name,
                    description: definition.description,
                    input_schema: definition.input_schema,
                })
                .collect()
        }

        async fn execute_runtime_tool(
            &self,
            _: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            panic!("inventory must not execute a producer")
        }
    }

    #[test]
    fn r1_discovery_evolution_inventory_requires_real_leaf_producers() {
        use harness_contract::evaluation::{
            EvaluationAcceptanceCheck, EvaluationAcceptanceRequirement,
        };
        let root = tempfile::tempdir().unwrap();
        let host = CatalogHost(
            tools::ToolHost::builtin("evolution-discovery", root.path()).pin_snapshot(),
        );
        let skills = crate::RuntimeSkillCatalog::default();
        let mut scenario = EvaluationScenarioSpec {
            scenario_ref: "discovery/read".into(),
            objective: "read narrow input".into(),
            acceptance: vec!["read proof".into()],
            acceptance_checks: vec![EvaluationAcceptanceRequirement {
                criterion: "read proof".into(),
                check: EvaluationAcceptanceCheck::IsolatedWorkspaceRead,
            }],
            allowed_tools: vec!["read_file".into(), "tool_orchestrate".into()],
            allowed_skills: Vec::new(),
            resource_scopes: vec!["read:src/a.rs".into()],
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "local".into(),
            replay_manifest: None,
        };
        assert!(validate_evolution_scenario_isolation(&scenario, Some(&host), &skills).is_ok());
        for scopes in [
            vec![],
            vec!["network:*".into()],
            vec!["read:src/../secret".into()],
            vec!["write:shared".into()],
        ] {
            let mut denied = scenario.clone();
            denied.resource_scopes = scopes;
            assert!(validate_evolution_scenario_isolation(&denied, Some(&host), &skills).is_err());
        }
        scenario.allowed_tools = vec!["tool_orchestrate".into()];
        assert!(validate_evolution_scenario_isolation(&scenario, Some(&host), &skills).is_err());
        scenario.acceptance_checks[0].check = EvaluationAcceptanceCheck::IsolatedWorkspaceChange {
            field: harness_contract::agent::StructuredOutputField::Implementation,
        };
        scenario.permission_ceiling = harness_contract::policy::PermissionMode::WorkspaceWrite;
        assert!(validate_evolution_scenario_isolation(&scenario, Some(&host), &skills).is_err());
        scenario.allowed_tools.push("edit".into());
        assert!(validate_evolution_scenario_isolation(&scenario, Some(&host), &skills).is_ok());
        scenario.allowed_tools.push("mcp_tool".into());
        assert!(validate_evolution_scenario_isolation(&scenario, Some(&host), &skills).is_err());
        assert!(validate_evolution_scenario_isolation(&scenario, None, &skills).is_err());
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

const AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID: &str =
    "runtime:agentic-settled-graph-resolver:v1";
const AGENTIC_PROGRAM_WAIT_RESOLVER_PROJECTION_ID: &str =
    "runtime:agentic-program-wait-resolver:v1";

fn agentic_program_wait_resolution_lane(
    event_store: Arc<RuntimeEventStore>,
    graph_store: ExecutionGraphStateStore,
    supervisor: Arc<crate::RuntimeExecutionSupervisor>,
    actions: crate::AgentActionService,
    agent_runtime: Arc<AgentRuntime>,
) -> Result<crate::RuntimeProjectionLane, RuntimeServicesError> {
    let descriptor = crate::RuntimeProjectionDescriptor::new(
        AGENTIC_PROGRAM_WAIT_RESOLVER_PROJECTION_ID,
        crate::RuntimeProjectionInterest::new([
            crate::RuntimeProjectionEventInterest::new(
                RuntimeEventScope::Program,
                "agentic.action_applied",
            ),
            crate::RuntimeProjectionEventInterest::new(
                RuntimeEventScope::Program,
                "agentic.objective_verdict_bound",
            ),
            crate::RuntimeProjectionEventInterest::new(
                RuntimeEventScope::Program,
                "agentic.completion_rejected",
            ),
        ]),
        256,
        Duration::from_secs(5),
    )
    .map_err(RuntimeServicesError::Invariant)?;
    Ok(crate::RuntimeProjectionLane::asynchronous(
        descriptor,
        move |batch_size| {
            let event_store = Arc::clone(&event_store);
            let graph_store = graph_store.clone();
            let supervisor = Arc::clone(&supervisor);
            let actions = actions.clone();
            let agent_runtime = Arc::clone(&agent_runtime);
            Box::pin(async move {
                let services = agent_runtime.services().ok_or("Program reconciliation lane has no bound RuntimeServices")?;
                let checkpoint = event_store
                    .projection_checkpoint(AGENTIC_PROGRAM_WAIT_RESOLVER_PROJECTION_ID)
                    .map_err(|error| error.to_string())?;
                let source_cursor = checkpoint
                    .as_ref()
                    .map_or(0, |checkpoint| checkpoint.source_cursor);
                let interest = crate::RuntimeProjectionInterest::new([
                    crate::RuntimeProjectionEventInterest::new(
                        RuntimeEventScope::Program,
                        "agentic.action_applied",
                    ),
                    crate::RuntimeProjectionEventInterest::new(
                        RuntimeEventScope::Program,
                        "agentic.objective_verdict_bound",
                    ),
                    crate::RuntimeProjectionEventInterest::new(
                        RuntimeEventScope::Program,
                        "agentic.completion_rejected",
                    ),
                ]);
                let page = event_store
                    .projection_scan_page(
                        source_cursor,
                        &interest,
                        batch_size.max(1),
                        10_000,
                        16 * 1024 * 1024,
                    )
                    .map_err(|error| error.to_string())?;
                if page.scanned_commits == 0 {
                    return Ok(crate::RuntimeProjectionPass::default());
                }
                let mut program_ids = BTreeSet::new();
                for batch in &page.batches {
                    for event in &batch.events {
                        if let Some(program_id) = event
                            .payload
                            .get("envelope")
                            .and_then(|envelope| envelope.get("actor"))
                            .and_then(|actor| actor.get("program_id"))
                            .and_then(serde_json::Value::as_str)
                            .or_else(|| {
                                event
                                    .payload
                                    .get("program_id")
                                    .and_then(serde_json::Value::as_str)
                            })
                            .filter(|program_id| !program_id.trim().is_empty())
                        {
                            program_ids.insert(program_id.to_string());
                        }
                    }
                }
                let mut resolved = 0usize;
                for program_id in &program_ids {
                    if actions.program_recovery_is_excluded(program_id)? {
                        continue;
                    }
                    services.reconcile_agentic_completion_request(program_id)?;
                    resolved = resolved.saturating_add(
                        crate::execution_core::graph::executors::resolve_agentic_program_wait(
                            program_id,
                            &actions,
                            &graph_store,
                            supervisor.as_ref(),
                            false,
                        )
                        .await?,
                    );
                }
                event_store
                    .put_projection_checkpoint_retrying(
                        AGENTIC_PROGRAM_WAIT_RESOLVER_PROJECTION_ID,
                        page.scanned_through_cursor,
                        &serde_json::json!({
                            "source_cursor": page.scanned_through_cursor,
                            "programs": program_ids.len(),
                            "resolved_waits": resolved,
                        }),
                        now_ms(),
                    )
                    .map_err(|error| error.to_string())?;
                Ok(
                    crate::RuntimeProjectionPass::scanned(page.scanned_commits, batch_size)
                        .with_matches(page.matched_events),
                )
            })
        },
    ))
}

fn agentic_settled_graph_resolution_interest() -> crate::RuntimeProjectionInterest {
    crate::RuntimeProjectionInterest::new([
        crate::RuntimeProjectionEventInterest::new(RuntimeEventScope::Goal, "goal.revised"),
        crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::ExecutionGraph,
            "execution_graph.recovered",
        ),
        crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::Program,
            "agentic.action_applied",
        ),
        crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::ManagedAgent,
            "managed_agent.invocation.started.v1",
        ),
        crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::ExecutionNode,
            "execution_node.transitioned",
        ),
        crate::RuntimeProjectionEventInterest::new(RuntimeEventScope::Agent, "agent.local_exit"),
        crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::ExecutionNode,
            "execution.effect.receipt",
        ),
    ])
}

// Failed event-derived graphs must survive advancing the event cursor. A
// rotating bounded retry window prevents a permanently failing key from
// monopolising the pending directory. The directory is durable checkpoint
// data, not an assertion that a graph has settled.
fn settled_pending_window(
    pending: &BTreeMap<String, String>,
    after: &str,
    limit: usize,
) -> Vec<String> {
    use std::ops::Bound::{Excluded, Unbounded};
    pending
        .range::<str, _>((Excluded(after), Unbounded))
        .chain(pending.range::<str, _>((Unbounded, std::ops::Bound::Included(after))))
        .take(limit.max(1))
        .map(|(key, _)| key.clone())
        .collect()
}

fn settled_record_graph_result(
    pending: &mut BTreeMap<String, String>,
    graph_id: &str,
    errors: Vec<String>,
) -> Option<String> {
    if errors.is_empty() {
        pending.remove(graph_id);
        None
    } else {
        let error = errors.join("; ");
        pending.insert(graph_id.to_owned(), error.clone());
        Some(format!("graph {graph_id}: {error}"))
    }
}

// A rejection can be consumed while a child is still active. Revisit the
// original supervisor when that graph changes, before the existing barrier
// resolver advances its checkpoint; otherwise the final work-cut may never be
// assessed after the rejection event has already been checkpointed.
pub(crate) async fn reconcile_rejections_for_settled_graph(
    graph_id: &str,
    services: &RuntimeServices,
    actions: &crate::AgentActionService,
    graph_store: &ExecutionGraphStateStore,
) -> Result<(), String> {
    let graph = match graph_store.load_current_async(graph_id.to_string()).await {
        Ok(graph) => graph,
        Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    let mut program_ids = crate::execution_core::graph::executors::agentic_program_ids(&graph)
        .into_iter().collect::<BTreeSet<_>>();
    if let Some(binding) = services.goal_store().get(&format!("goal:{graph_id}"))?
        .and_then(|goal| goal.execution_binding)
    {
        program_ids.insert(binding.agentic_program_id);
    }
    for program_id in program_ids {
        if actions.program_recovery_is_excluded(&program_id)? {
            continue;
        }
        if actions.project_if_exists(&program_id).map_err(|error| error.to_string())?.is_some() {
            services.reconcile_agentic_completion_request(&program_id)?;
        }
    }
    Ok(())
}

pub(crate) fn agentic_settled_graph_resolution_lane(
    event_store: Arc<RuntimeEventStore>,
    graph_store: ExecutionGraphStateStore,
    supervisor: Arc<crate::RuntimeExecutionSupervisor>,
    actions: crate::AgentActionService,
    agent_runtime: Arc<AgentRuntime>,
) -> Result<crate::RuntimeProjectionLane, RuntimeServicesError> {
    let descriptor = crate::RuntimeProjectionDescriptor::new(
        AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID,
        agentic_settled_graph_resolution_interest(),
        256,
        Duration::from_secs(5),
    )
    .map_err(RuntimeServicesError::Invariant)?;
    Ok(crate::RuntimeProjectionLane::asynchronous(
        descriptor,
        move |batch_size| {
            let event_store = Arc::clone(&event_store);
            let graph_store = graph_store.clone();
            let supervisor = Arc::clone(&supervisor);
            let actions = actions.clone();
            let agent_runtime = Arc::clone(&agent_runtime);
            Box::pin(async move {
                let checkpoint = event_store
                    .projection_checkpoint(AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID)
                    .map_err(|error| error.to_string())?;
                let source_cursor = checkpoint
                    .as_ref()
                    .map_or(0, |checkpoint| checkpoint.source_cursor);
                let interest = agentic_settled_graph_resolution_interest();
                let page = event_store
                    .projection_scan_page(
                        source_cursor,
                        &interest,
                        batch_size.max(1),
                        10_000,
                        16 * 1024 * 1024,
                    )
                    .map_err(|error| error.to_string())?;
                let services = agent_runtime
                    .services()
                    .ok_or("Agent exit reconciliation lane has no bound RuntimeServices")?;
                let mut touched_graphs = BTreeSet::new();
                let mut pending_graphs: BTreeMap<String, String> = checkpoint
                    .as_ref()
                    .and_then(|c| c.payload.get("pending_graphs"))
                    .map(|value| {
                        serde_json::from_value(value.clone()).map_err(|error| error.to_string())
                    })
                    .transpose()?
                    .unwrap_or_default();
                let pending_after = checkpoint
                    .as_ref()
                    .and_then(|c| c.payload.get("pending_after"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let retry_graphs =
                    settled_pending_window(&pending_graphs, pending_after, batch_size);
                let next_pending_after = retry_graphs.last().cloned();
                touched_graphs.extend(retry_graphs);
                for batch in &page.batches {
                    for event in &batch.events {
                        if event.scope == RuntimeEventScope::ExecutionGraph {
                            touched_graphs.insert(event.stream_id.clone());
                        }
                        if let Some(id) = event
                            .payload
                            .get("input_application_ticket")
                            .and_then(|t| t.get("graph_id"))
                            .and_then(serde_json::Value::as_str)
                        {
                            touched_graphs.insert(id.to_string());
                        }
                        if let Some(graph_id) = event
                            .payload
                            .get("graph_id")
                            .and_then(serde_json::Value::as_str)
                        {
                            touched_graphs.insert(graph_id.to_string());
                        }
                        // Terminal owners can commit in either order. Feed the
                        // original linked Graph back into the existing durable,
                        // rotating per-Graph pending lane for either arrival.
                        let terminal_graph = match event.kind.as_str() {
                            "session.cancellation_committed" => event.payload.get("execution_id"),
                            "runtime.strategy.outcome" => event.payload.get("execution_graph_ref"),
                            "runtime.outcome.recorded.v1" => event.payload.pointer("/identity/execution_graph_ref"),
                            "goal.created" | "goal.revised" | "goal.completed" => event.payload.pointer("/goal/execution_binding/root_execution_id"),
                            _ => None,
                        };
                        if let Some(graph_id) = terminal_graph.and_then(serde_json::Value::as_str) {
                            touched_graphs.insert(graph_id.to_string());
                        }
                    }
                }
                // Running invocations are durable pending work, independently
                // of the event cursor. Rotate a bounded page so a long-running
                // prefix cannot hide later terminal graphs after a restart.
                let managed_after = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.payload.get("managed_after"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let mut pending_managed = services
                    .managed_agents
                    .running_invocation_page(managed_after, batch_size);
                let managed_backlog = pending_managed.len() > batch_size.max(1);
                pending_managed.truncate(batch_size.max(1));
                let next_managed_after = if managed_backlog {
                    pending_managed
                        .last()
                        .map(|invocation| invocation.invocation_id.clone())
                } else {
                    None
                };
                let mut managed_errors = Vec::new();
                for invocation in pending_managed {
                    if let Err(error) = services
                        .reconcile_managed_agent_invocation(invocation)
                        .await
                    {
                        managed_errors.push(error.to_string());
                    }
                }
                // A graph may settle before or after its Program root commits the
                // durable wait. Reconcile both the transitioned graph and its
                // linked Agent children so either event order closes the barrier.
                let mut exit_errors = managed_errors;
                for graph_id in &touched_graphs {
                    let mut graph_errors = Vec::new();
                    if let Err(error) = services.reconcile_provider_cancel_goal(graph_id) {
                        graph_errors.push(error);
                    }
                    if let Err(error) =
                        super::goal::reconcile_user_goal_applications(services.as_ref(), graph_id)
                            .await
                    {
                        graph_errors.push(error);
                    }
                    if let Err(error) = reconcile_rejections_for_settled_graph(
                        graph_id, services.as_ref(), &actions, &graph_store,
                    ).await {
                        graph_errors.push(error);
                    }
                    if let Err(error) = crate::execution_core::graph::executors::reconcile_agentic_program_wait_for_settled_graph(
                        graph_id, &actions, &graph_store, supervisor.as_ref(),
                    ).await {
                        graph_errors.push(error);
                    }
                    match graph_store.child_links(graph_id) {
                        Ok(links) => {
                            for link in links {
                                if let Err(error) = reconcile_rejections_for_settled_graph(
                                    &link.child_execution_id, services.as_ref(), &actions, &graph_store,
                                ).await {
                                    graph_errors.push(format!("child {}: {error}", link.child_execution_id));
                                }
                                if let Err(error) = crate::execution_core::graph::executors::reconcile_agentic_program_wait_for_settled_graph(
                                &link.child_execution_id, &actions, &graph_store, supervisor.as_ref(),
                            ).await {
                                graph_errors.push(format!("child {}: {error}", link.child_execution_id));
                            }
                            }
                        }
                        Err(error) => graph_errors.push(error.to_string()),
                    }
                    if let Some(error) =
                        settled_record_graph_result(&mut pending_graphs, graph_id, graph_errors)
                    {
                        exit_errors.push(error);
                    }
                }
                let mut recovery_cursor: crate::agentic::execution::dispatch::AgenticRecoveryCursor =
                    match checkpoint.as_ref().and_then(|checkpoint| checkpoint.payload.get("agentic_recovery")) {
                        Some(value) => serde_json::from_value(value.clone()).map_err(|error| error.to_string())?,
                        None => Default::default(),
                    };
                let recovery_backlog =
                    match services.admit_agentic_recovery_page(&mut recovery_cursor) {
                        Ok(backlog) => backlog,
                        Err(error) => {
                            exit_errors.push(error);
                            false
                        }
                    };
                let last_exit_retry_ms = now_ms();
                let structured_after = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.payload.get("structured_after"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let next_structured_after = match services
                    .task_aggregate_service()
                    .structured_recovery_candidates(
                        structured_after,
                        batch_size.max(1).saturating_add(1),
                    ) {
                    Ok(tasks) => {
                        let backlog = tasks.len() > batch_size.max(1);
                        let mut last = None;
                        for task in tasks.into_iter().take(batch_size.max(1)) {
                            last = Some(task.task_id.clone());
                            if event_store.is_session_recovery_excluded(&task.origin_session_id) {
                                continue;
                            }
                            let result = match services
                                .register_structured_task(&task.task_id)
                                .await
                            {
                                Ok(_) => services.reconcile_structured_task(&task.task_id).await,
                                Err(error) => Err(format!(
                                    "structured Task {} registration: {error}",
                                    task.task_id
                                )),
                            };
                            if let Err(error) = result {
                                // The original structured candidate remains durable;
                                // rotate this directory even when one item fails.
                                exit_errors
                                    .push(format!("structured Task {}: {error}", task.task_id));
                            }
                        }
                        if backlog {
                            last
                        } else {
                            None
                        }
                    }
                    Err(error) => {
                        exit_errors.push(error);
                        if structured_after.is_empty() {
                            None
                        } else {
                            Some(structured_after.to_owned())
                        }
                    }
                };
                // Event-derived failures and the cursor are committed in the
                // same checkpoint. A failed checkpoint leaves the old cursor
                // replayable, so no graph obligation is lost.
                let committed_cursor = page.scanned_through_cursor;
                let mut payload = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.payload.clone())
                    .unwrap_or_else(|| serde_json::json!({}));
                let fields = payload
                    .as_object_mut()
                    .ok_or("invalid settled lane checkpoint payload")?;
                fields.insert("source_cursor".into(), serde_json::json!(committed_cursor));
                fields.insert(
                    "resolved_graphs".into(),
                    serde_json::json!(touched_graphs.len()),
                );
                fields.insert(
                    "managed_after".into(),
                    serde_json::json!(next_managed_after),
                );
                fields.insert(
                    "structured_after".into(),
                    serde_json::json!(next_structured_after),
                );
                fields.insert(
                    "last_exit_retry_ms".into(),
                    serde_json::json!(last_exit_retry_ms),
                );
                fields.insert(
                    "pending_graphs".into(),
                    serde_json::to_value(&pending_graphs).map_err(|error| error.to_string())?,
                );
                fields.insert(
                    "pending_after".into(),
                    serde_json::json!(next_pending_after),
                );
                fields.insert(
                    "agentic_recovery".into(),
                    serde_json::to_value(recovery_cursor).map_err(|error| error.to_string())?,
                );
                event_store
                    .compare_and_put_projection_checkpoint(
                        AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID,
                        committed_cursor,
                        checkpoint
                            .as_ref()
                            .map_or(0, |checkpoint| checkpoint.revision),
                        &payload,
                        now_ms(),
                    )
                    .map_err(|error| error.to_string())?;
                if !exit_errors.is_empty() {
                    return Err(exit_errors.join("; "));
                }
                let mut pass =
                    crate::RuntimeProjectionPass::scanned(page.scanned_commits, batch_size)
                        .with_matches(page.matched_events);
                pass.backlog |= managed_backlog
                    || next_structured_after.is_some()
                    || recovery_backlog
                    || !pending_graphs.is_empty();
                Ok(pass)
            })
        },
    ))
}

#[cfg(test)]
mod exact_exit_lane_tests {
    use super::*;

    #[tokio::test]
    async fn k05_failed_graph_is_checkpointed_and_healthy_keys_rotate_then_retry() {
        let services = RuntimeServices::in_memory().unwrap();
        let store = services.event_store();
        let projection = "test:k05:settled-pending";
        let mut pending = BTreeMap::new();
        assert!(
            settled_record_graph_result(&mut pending, "a", vec!["original failure".into()])
                .is_some()
        );
        assert!(settled_record_graph_result(&mut pending, "b", vec![]).is_none());
        assert_eq!(
            pending.get("a").map(String::as_str),
            Some("original failure")
        );
        // The cursor and the failed identity are one durable checkpoint.
        store
            .put_projection_checkpoint_retrying(
                projection,
                42,
                &serde_json::json!({"pending_graphs":pending}),
                now_ms(),
            )
            .unwrap();
        let checkpoint = store.projection_checkpoint(projection).unwrap().unwrap();
        assert_eq!(checkpoint.source_cursor, 42);
        let mut reopened: BTreeMap<String, String> =
            serde_json::from_value(checkpoint.payload["pending_graphs"].clone()).unwrap();
        reopened.insert("c".into(), "another failure".into());
        assert_eq!(settled_pending_window(&reopened, "", 1), vec!["a"]);
        assert_eq!(settled_pending_window(&reopened, "a", 1), vec!["c"]);
        assert_eq!(settled_pending_window(&reopened, "c", 1), vec!["a"]);
        assert!(
            settled_record_graph_result(&mut reopened, "a", vec!["still failing".into()]).is_some()
        );
        assert_eq!(reopened["a"], "still failing");
        assert!(settled_record_graph_result(&mut reopened, "a", vec![]).is_none());
        assert!(!reopened.contains_key("a"));
        assert!(reopened.contains_key("c"));
    }

    #[tokio::test]
    async fn k05_original_lane_keeps_bad_graph_and_processes_healthy_graph() {
        let services = RuntimeServices::in_memory().unwrap();
        let stopped = services.event_reactor.shutdown().await;
        assert!(stopped.timed_out_lanes.is_empty());
        assert!(stopped.join_errors.is_empty());
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("k05 healthy graph");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph
            .nodes
            .push(harness_contract::execution_graph::ExecutionNodeSpec::new(
                harness_contract::execution_graph::ExecutionNodeKind::Verify,
                "verify",
                "k05",
            ));
        let graph = services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .unwrap();
        let store = services.event_store();
        let id = AGENTIC_SETTLED_GRAPH_RESOLVER_PROJECTION_ID;
        let before = store.projection_checkpoint(id).unwrap();
        let cursor = before
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.source_cursor);
        let mut payload = before
            .as_ref()
            .map(|checkpoint| checkpoint.payload.clone())
            .unwrap_or_else(|| serde_json::json!({}));
        payload["pending_graphs"] = serde_json::json!({ "k05-missing-graph": "injected failure" });
        payload["pending_graphs"][&graph.id] = serde_json::json!("retry healthy graph");
        store
            .compare_and_put_projection_checkpoint(
                id,
                cursor,
                before.as_ref().map_or(0, |c| c.revision),
                &payload,
                now_ms(),
            )
            .unwrap();
        let lane = agentic_settled_graph_resolution_lane(
            Arc::clone(store),
            services.graph_state_store().clone(),
            Arc::clone(services.execution_supervisor()),
            services.agent_action_service(),
            Arc::clone(services.agent_runtime()),
        )
        .unwrap();
        let reactor =
            Arc::new(crate::RuntimeEventReactor::sealed(Arc::clone(store), [lane]).unwrap());
        reactor.start().unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let checkpoint = store.projection_checkpoint(id).unwrap().unwrap();
                let pending = &checkpoint.payload["pending_graphs"];
                if pending.get("k05-missing-graph").is_some()
                    && pending.get(&graph.id).is_none()
                    && checkpoint.source_cursor > cursor
                {
                    break checkpoint;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let stopped = reactor.shutdown().await;
        assert!(stopped.timed_out_lanes.is_empty());
        assert!(stopped.join_errors.is_empty());
        let checkpoint =
            observed.expect("healthy graph did not progress while the bad graph was retained");
        assert!(checkpoint.payload["pending_graphs"]["k05-missing-graph"]
            .as_str()
            .unwrap()
            .contains("k05-missing-graph"));
        assert!(services.shutdown_execution().await.errors.is_empty());
    }

    #[test]
    fn exact_exit_lane_declares_both_orders_and_excludes_command_ack() {
        let interest = agentic_settled_graph_resolution_interest();
        let exit = crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::Agent,
            "agent.local_exit",
        );
        let graph = crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::ExecutionNode,
            "execution_node.transitioned",
        );
        for order in [[exit.clone(), graph.clone()], [graph, exit]] {
            assert!(order.iter().all(|event| interest.events.contains(event)));
        }
        assert!(!interest
            .events
            .contains(&crate::RuntimeProjectionEventInterest::new(
                RuntimeEventScope::Agent,
                "agent.command"
            )));
        assert!(!interest
            .events
            .contains(&crate::RuntimeProjectionEventInterest::new(
                RuntimeEventScope::Agent,
                "agent.terminal"
            )));
        assert!(interest
            .events
            .contains(&crate::RuntimeProjectionEventInterest::new(
                RuntimeEventScope::ExecutionNode,
                "execution.effect.receipt"
            )));
    }

    #[tokio::test]
    async fn retirement_maintenance_nonwaiting_full_closing_drops_rejected_work() {
        struct Rejected(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Rejected {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let supervisor = RuntimeMaintenanceSupervisor::new();
        let release = Arc::new(tokio::sync::Notify::new());
        let started = Arc::new(tokio::sync::Notify::new());
        let (worker_release, worker_started) = (release.clone(), started.clone());
        supervisor
            .try_submit("capacity".into(), async move {
                worker_started.notify_one();
                worker_release.notified().await;
            })
            .unwrap();
        started.notified().await;
        for _ in 0..MAX_QUEUED_MAINTENANCE_PER_OWNER {
            supervisor.try_submit("capacity".into(), async {}).unwrap();
        }
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe = Rejected(dropped.clone());
        assert_eq!(
            supervisor.try_submit("capacity".into(), async move {
                let _probe = probe;
                panic!("a rejected maintenance future must never be polled");
            }),
            Err(MaintenanceSubmitError::Full)
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        // The waiting API shares that admission step and must still wait.
        let waiting = supervisor.submit("capacity".into(), async {});
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        release.notify_one();
        assert!(waiting.await);
        supervisor.shutdown_and_drain().await;
        let probe = Rejected(dropped.clone());
        assert_eq!(
            supervisor.try_submit("closed".into(), async move {
                let _probe = probe;
                panic!("a closing owner cannot poll rejected work");
            }),
            Err(MaintenanceSubmitError::Closing)
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        let state = supervisor.state.lock().unwrap();
        assert!(state.owners.is_empty());
        assert!(state.reaping.is_empty());
    }

    #[tokio::test]
    async fn retirement_directory_busy_full_preserves_first_unadmitted_work() {
        use crate::agentic::execution::dispatch::AgenticRecoveryCursor;
        use harness_contract::agent_action::{
            AgentAction, AgentActionStatus, TaskPublishInput, TeamCreateInput,
        };
        let services = RuntimeServices::in_memory().unwrap();
        // This is an admission/cursor contract, independent of reactor timing.
        let reactor = services.event_reactor.shutdown().await;
        assert!(reactor.timed_out_lanes.is_empty());
        assert!(reactor.join_errors.is_empty());
        let mut tasks = Vec::new();
        for id in ["first", "second"] {
            let envelope = |suffix: &str, action| {
                let mut envelope = harness_contract::agent_action::AgentActionEnvelope {
                    action_id: format!("1145:{id}:{suffix}"),
                    action,
                    actor: harness_contract::agent_action::AgentActorBinding {
                        original_execution: None,
                        objective_id: String::new(),
                        program_id: String::new(),
                        session_id: "session-1145-directory".into(),
                        turn_id: "turn-1145-directory".into(),
                        root_execution_id: None,
                        required_team_count: 0,
                        exact_team_count: None,
                        objective_summary: "bounded admission".into(),
                        model_lease: "test".into(),
                        permission_ceiling: Some(
                            harness_contract::policy::PermissionMode::ReadOnly,
                        ),
                        resource_scopes: Vec::new(),
                        actor_id: "root:session-1145-directory".into(),
                        kind: harness_contract::agent_action::AgentActorKind::Root,
                        execution_id: None,
                        team_id: None,
                        agent_id: None,
                    },
                    expected_revision: None,
                };
                envelope.actor.program_id = format!("1145-{id}");
                envelope.actor.objective_id = format!("1145-objective-{id}");
                envelope
            };
            let team = services
                .agent_action_service()
                .apply(&envelope(
                    "team",
                    AgentAction::TeamCreate(TeamCreateInput {
                        name: id.into(),
                        mission: "directory fairness".into(),
                        objective: None,
                    }),
                ))
                .unwrap();
            assert_eq!(team.status, AgentActionStatus::Applied);
            let task = services
                .agent_action_service()
                .apply(&envelope(
                    "task",
                    AgentAction::TaskPublish(TaskPublishInput {
                        team_ref: team.changed_refs[0].clone(),
                        title: id.into(),
                        objective: "independent ready work".into(),
                        acceptance: "actual admission remains pending without a member".into(),
                        criterion_source: Default::default(),
                        acceptance_checks: Vec::new(),
                        required_capabilities: Vec::new(),
                        depends_on: Vec::new(),
                        obligation_refs: Vec::new(),
                        purpose: harness_contract::agent_action::TaskPurpose::Exploration,
                        execution_requirements: Vec::new(),
                        expertise_hints: Vec::new(),
                    }),
                ))
                .unwrap();
            assert_eq!(task.status, AgentActionStatus::Applied);
            tasks.push(task.changed_refs[0].clone());
        }
        let owner = "runtime:agentic-program-recovery:v1";
        let flights = services.agentic_dispatch_flights();
        assert!(flights.lock().unwrap().insert(owner.into()));
        let mut cursor = AgenticRecoveryCursor::default();
        assert!(!services.admit_agentic_recovery_page(&mut cursor).unwrap());
        let blocked_first = serde_json::to_value(&cursor).unwrap();
        assert_eq!(blocked_first["program"][1], "agentic-program:1145-first");
        assert!(blocked_first["after_work"].is_null());
        assert!(!services.admit_agentic_recovery_page(&mut cursor).unwrap());
        assert_eq!(serde_json::to_value(&cursor).unwrap(), blocked_first);
        flights.lock().unwrap().remove(owner);
        assert!(services.admit_agentic_recovery_page(&mut cursor).unwrap());
        assert_eq!(
            serde_json::to_value(&cursor).unwrap()["after_work"],
            serde_json::json!([tasks[0], ""])
        );
        // A still-pending first Task has already received its bounded turn.
        assert!(services.admit_agentic_recovery_page(&mut cursor).unwrap());
        assert!(flights.lock().unwrap().contains(owner));
        assert!(!services.admit_agentic_recovery_page(&mut cursor).unwrap());
        let blocked_second = serde_json::to_value(&cursor).unwrap();
        assert_eq!(blocked_second["program"][1], "agentic-program:1145-second");
        // Let the retained original worker return, then saturate the SAME
        // supervisor queue. Rejection must also drop its DispatchFlight token.
        tokio::time::timeout(Duration::from_secs(5), async {
            while flights.lock().unwrap().contains(owner) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let worker_release = release.clone();
        let maintenance = services.maintenance_supervisor();
        maintenance
            .try_submit(owner.into(), async move {
                worker_release.notified().await;
            })
            .unwrap();
        for _ in 0..MAX_QUEUED_MAINTENANCE_PER_OWNER {
            maintenance.try_submit(owner.into(), async {}).unwrap();
        }
        assert!(!services.admit_agentic_recovery_page(&mut cursor).unwrap());
        assert_eq!(serde_json::to_value(&cursor).unwrap(), blocked_second);
        assert!(!flights.lock().unwrap().contains(owner));
        release.notify_one();
        maintenance.shutdown_and_drain().await;
        assert!(!services.admit_agentic_recovery_page(&mut cursor).unwrap());
        assert_eq!(serde_json::to_value(&cursor).unwrap(), blocked_second);
        assert!(!flights.lock().unwrap().contains(owner));
        services.shutdown_maintenance().await;
        assert!(services.shutdown_execution().await.errors.is_empty());
    }

    #[tokio::test]
    async fn n1_managed_terminal_projection_handles_both_durable_orders() {
        use harness_contract::agent::{AgentDefinitionId, DefinitionScope, RevisionSelector};
        use harness_contract::execution_graph::{ExecutionNodeKind, ExecutionNodeSpec};
        use harness_contract::managed_agent::*;
        for graph_first in [false, true] {
            let services = RuntimeServices::in_memory().unwrap();
            let dispatcher = services.managed_agents.clone();
            let definition = dispatcher
                .register_definition(
                    ManagedAgentDefinition {
                        managed_agent_id: "workspace/n1/managed".into(),
                        revision: 1,
                        target: ManagedAgentTarget::Agent {
                            definition_id: AgentDefinitionId::new(
                                DefinitionScope::Workspace,
                                "n1/managed",
                            )
                            .unwrap(),
                            selector: RevisionSelector::LatestApprovedStable,
                        },
                        trigger: ManagedAgentTrigger::Manual,
                        session_id: "n1-managed-session".into(),
                        objective: "project canonical execution terminal".into(),
                        acceptance: vec![],
                        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                        model_lease: "default".into(),
                        granted_capabilities: vec![harness_contract::agent::AgentCapability::Read],
                        allowed_tool_contract_refs: vec![],
                        allowed_skill_refs: vec![],
                        resource_scopes: vec!["workspace".into()],
                        overlap_policy: ManagedAgentOverlapPolicy::Forbid,
                        retry_policy: ManagedAgentRetryPolicy::default(),
                        health_policy: ManagedAgentHealthPolicy::default(),
                        enabled: true,
                    },
                    1,
                )
                .unwrap();
            let mut graph = ExecutionGraph::new("N1 managed terminal projection");
            graph.id = "n1-managed-graph".into();
            crate::test_support::attach_execution_graph_lineage(&mut graph);
            graph.nodes.push(ExecutionNodeSpec::new(
                ExecutionNodeKind::Verify,
                "verify",
                "n1-no-required-input",
            ));
            if graph_first {
                services
                    .execution_supervisor()
                    .register_graph(graph.clone())
                    .await
                    .unwrap();
                services
                    .execution_supervisor()
                    .drive_registered(&graph.id)
                    .await
                    .unwrap();
            }
            dispatcher
                .trigger_manual(&definition.managed_agent_id, "n1-request", 2)
                .unwrap();
            let claim = dispatcher
                .claim_ready("n1-dispatcher", 3, 120000, 1)
                .unwrap()
                .pop()
                .unwrap();
            let token = claim.claim_token.as_deref().unwrap();
            dispatcher
                .begin_graph_registration(
                    &claim.invocation_id,
                    "n1-dispatcher",
                    claim.fence_generation,
                    token,
                    graph.id.clone(),
                )
                .unwrap();
            dispatcher
                .materialize_invocation(
                    &claim.invocation_id,
                    "n1-dispatcher",
                    claim.fence_generation,
                    token,
                    graph.id.clone(),
                    "n1-registration".into(),
                )
                .unwrap();
            dispatcher
                .start_invocation(
                    &claim.invocation_id,
                    "n1-dispatcher",
                    claim.fence_generation,
                    token,
                    graph.id.clone(),
                    4,
                )
                .unwrap();
            if !graph_first {
                // A missing graph must not manufacture a completed invocation.
                assert!(services
                    .reconcile_managed_agent_invocation(
                        dispatcher.invocations().unwrap().pop().unwrap()
                    )
                    .await
                    .unwrap()
                    .is_none());
                assert_eq!(
                    dispatcher.invocations().unwrap()[0].status,
                    crate::ManagedAgentInvocationStatus::Running
                );
                services
                    .execution_supervisor()
                    .register_graph(graph.clone())
                    .await
                    .unwrap();
                services
                    .execution_supervisor()
                    .drive_registered(&graph.id)
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let invocation = dispatcher.invocations().unwrap().pop().unwrap();
                    if !invocation.status.is_active() {
                        assert_eq!(
                            invocation.status,
                            crate::ManagedAgentInvocationStatus::Completed
                        );
                        assert_eq!(invocation.execution_ref.as_deref(), Some(graph.id.as_str()));
                        assert_eq!(invocation.fence_generation, claim.fence_generation);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("original reactor must project terminal without dispatch/retry");
            services.shutdown_maintenance().await;
            assert!(services.shutdown_execution().await.errors.is_empty());
        }
    }

    #[tokio::test]
    async fn exact_exit_original_services_shutdown_is_repeatable_without_runs() {
        let services = RuntimeServices::in_memory().unwrap();
        let first = services.shutdown_execution().await;
        let second = services.shutdown_execution().await;
        assert!(first.errors.is_empty(), "{first:?}");
        assert!(second.errors.is_empty(), "{second:?}");
        assert_eq!(first.forced_aborts, 0);
        assert_eq!(second.forced_aborts, 0);
        services.shutdown_maintenance().await;
    }
}

fn scenario_observation(
    packet: &AgentTaskPacket,
    returned: &harness_contract::agent::AgentReturnPacket,
    scenario: &EvaluationScenarioSpec,
    elapsed_ms: u64,
) -> EvaluationScenarioObservation {
    let acceptance_satisfied = scenario
        .acceptance
        .iter()
        .filter(|criterion| returned.acceptance.contains(*criterion))
        .count()
        .min(u32::MAX as usize) as u32;
    let mut evidence_refs = returned
        .evidence_refs
        .iter()
        .filter(|evidence| evidence.is_durable())
        .map(|evidence| evidence.evidence_ref.clone())
        .collect::<Vec<_>>();
    dedupe_evolution_evidence(&mut evidence_refs);
    let provider_model_refs = (!returned.provider.trim().is_empty()
        || !returned.model.trim().is_empty())
    .then(|| format!("{}/{}", returned.provider, returned.model))
    .into_iter()
    .collect::<Vec<_>>();
    let environment_fingerprint = format!(
        "sha256:{:x}",
        Sha256::digest(
            serde_json::to_vec(&serde_json::json!({
                "provider": returned.provider,
                "model": returned.model,
                "permission_ceiling": packet.permission_ceiling,
                "allowed_tools": scenario.allowed_tools,
                "allowed_skills": scenario.allowed_skills,
                "resource_scopes": scenario.resource_scopes,
            }))
            .unwrap_or_default()
        )
    );
    EvaluationScenarioObservation {
        scenario_ref: scenario.scenario_ref.clone(),
        definition_revision: packet
            .binding
            .as_ref()
            .map(|binding| binding.definition_ref.revision)
            .unwrap_or_default(),
        run_ref: format!("agent-run:{}", packet.run_id()),
        succeeded: returned.status == AgentTerminalStatus::Completed,
        acceptance_total: scenario.acceptance.len().min(u32::MAX as usize) as u32,
        acceptance_satisfied,
        evidence_refs,
        input_tokens: returned.input_tokens,
        output_tokens: returned.output_tokens,
        tool_calls: returned.tool_calls,
        elapsed_ms,
        provider_model_refs,
        environment_fingerprint,
    }
}

fn dedupe_evolution_evidence(evidence_refs: &mut Vec<harness_contract::reality::EvidenceRef>) {
    evidence_refs
        .sort_by(|left, right| (&left.ref_type, &left.id).cmp(&(&right.ref_type, &right.id)));
    evidence_refs.dedup_by(|left, right| left.ref_type == right.ref_type && left.id == right.id);
}

fn install_builtin_executors(
    registry: &NodeExecutorRegistry,
    executors: Vec<Arc<dyn NodeExecutor>>,
) -> Result<(), NodeExecutorError> {
    for executor in executors {
        registry.register(executor)?;
    }
    Ok(())
}

fn default_resource_quotas() -> Vec<(ExecutionResourceKind, ResourceQuota)> {
    vec![
        (
            ExecutionResourceKind::ContextBytes,
            ResourceQuota {
                minimum: 1,
                target: crate::content_publication::PUBLICATION_RESIDENT_BYTES,
                maximum: crate::content_publication::PUBLICATION_RESIDENT_BYTES,
            },
        ),
        (
            ExecutionResourceKind::SessionTurn,
            ResourceQuota {
                minimum: 2,
                target: 32,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Provider,
            ResourceQuota {
                minimum: 8,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Agent,
            ResourceQuota {
                minimum: 4,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Tool,
            ResourceQuota {
                minimum: 4,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.process".to_string()),
            ResourceQuota {
                minimum: 2,
                target: 16,
                maximum: 64,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.network".to_string()),
            ResourceQuota {
                minimum: 2,
                target: 32,
                maximum: 128,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.cpu".to_string()),
            ResourceQuota {
                minimum: 2,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.memory_mib".to_string()),
            ResourceQuota {
                minimum: 64,
                target: 2_048,
                maximum: 16_384,
            },
        ),
    ]
}

fn graph_is_terminal(graph: &ExecutionGraph) -> bool {
    !graph.node_statuses.is_empty()
        && graph
            .node_statuses
            .values()
            .copied()
            .all(ExecutionNodeStatus::is_terminal)
}

fn managed_invocation_fences(
    graph: &ExecutionGraph,
) -> Vec<harness_contract::managed_agent::ManagedAgentInvocationFence> {
    graph
        .nodes
        .iter()
        .filter_map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok())
        .filter_map(|packet| packet.managed_invocation)
        .collect()
}

fn graph_can_advance(graph: &ExecutionGraph) -> bool {
    graph.node_statuses.values().any(|status| {
        matches!(
            status,
            ExecutionNodeStatus::Planned | ExecutionNodeStatus::Ready
        )
    })
}

fn graph_is_waiting(graph: &ExecutionGraph) -> bool {
    graph.node_statuses.values().any(|status| {
        matches!(
            status,
            ExecutionNodeStatus::WaitingInput
                | ExecutionNodeStatus::WaitingApproval
                | ExecutionNodeStatus::WaitingExternal
                | ExecutionNodeStatus::Paused
        )
    })
}

fn graph_has_status(graph: &ExecutionGraph, target: ExecutionNodeStatus) -> bool {
    graph
        .node_statuses
        .values()
        .copied()
        .any(|status| status == target)
}

fn graph_status_label(graph: &ExecutionGraph) -> String {
    if graph.node_statuses.is_empty() {
        return "planned".to_string();
    }
    if graph_is_terminal(graph) {
        if graph_has_status(graph, ExecutionNodeStatus::Failed) {
            "failed"
        } else if graph_has_status(graph, ExecutionNodeStatus::Blocked) {
            "partial"
        } else if graph_has_status(graph, ExecutionNodeStatus::Cancelled) {
            "cancelled"
        } else {
            "completed"
        }
    } else if graph_is_waiting(graph) {
        "waiting"
    } else if graph_has_status(graph, ExecutionNodeStatus::Running) {
        "running"
    } else {
        "ready"
    }
    .to_string()
}

fn workspace_key(workspace_root: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workspace_root.as_os_str().as_encoded_bytes());
    let digest = format!("{:x}", hasher.finalize());
    digest[..24].to_string()
}

pub fn canonical_workspace_root(workspace_root: &Path) -> Result<PathBuf, RuntimeServicesError> {
    if workspace_root.as_os_str().is_empty() {
        return Err(RuntimeServicesError::EmptyRoot);
    }
    std::fs::canonicalize(workspace_root).map_err(|error| {
        RuntimeServicesError::Mission(format!(
            "failed to canonicalize workspace `{}`: {error}",
            workspace_root.display()
        ))
    })
}

pub fn canonical_workspace_identity(workspace_root: &Path) -> Result<String, RuntimeServicesError> {
    canonical_workspace_root(workspace_root).map(|root| workspace_key(&root))
}

#[cfg(test)]
pub(crate) mod requested_cancellation_source_tests {
    use super::*;
    use harness_contract::turn::{CancellationCause, CancellationReceipt, CancellationStatus};

    /// Real Session ingress/claim plus a committed Requested receipt. The control
    /// is unsignalled so provider tests can publish at their own IO barrier.
    pub(crate) async fn live_requested_fixture(
        root: &Path,
    ) -> (
        Arc<RuntimeServices>,
        Arc<TurnCancellationControl>,
        CancellationReceipt,
        crate::TurnIngressRef,
    ) {
        let (services, control, receipt, ingress, _store) =
            live_requested_fixture_with_store(root).await;
        (services, control, receipt, ingress)
    }

    pub(crate) async fn live_requested_fixture_with_store(
        root: &Path,
    ) -> (
        Arc<RuntimeServices>,
        Arc<TurnCancellationControl>,
        CancellationReceipt,
        crate::TurnIngressRef,
        Arc<session::UnifiedSessionStore>,
    ) {
        let store = Arc::new(crate::test_support::session_store());
        let timestamp = chrono::Utc::now().to_rfc3339();
        let session_id = "requested-live-session";
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.into(),
                platform: "test".into(),
                chat_id: "requested-live-chat".into(),
                user_id: None,
                model: None,
                created_at: timestamp.clone(),
                last_activity: timestamp,
                message_count: 0,
                reset_policy: "manual".into(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".into(),
            })
            .await
            .unwrap();
        let workspace = root.join("requested-live-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let services = RuntimeServices::test_builder(root, &workspace)
            .build()
            .unwrap();
        let router = services
            .install_test_session_store(Arc::clone(&store))
            .unwrap();
        let admission = store
            .get_session_input_admission(session_id)
            .await
            .unwrap()
            .unwrap();
        let request = crate::RuntimeSessionIngressCommand {
            input_id: "requested-live-input".into(),
            request_id: "requested-live-request".into(),
            turn_id: "requested-live-turn".into(),
            message_id: "requested-live-message".into(),
            session_generation: admission.generation,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            created_at_ms: now_ms(),
            runtime_options_json: None,
        };
        router
            .persist_input(session_id, "cancel this original turn", &request)
            .await
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("requested-live-worker", now_ms(), 120_000, 1, &[])
            .await
            .unwrap()
            .pop()
            .expect("original Session claim");
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "requested-live-worker",
                claimed.session_generation,
                claimed
                    .claim_token
                    .as_deref()
                    .expect("original claim token"),
                claimed.revision,
                now_ms(),
            )
            .await
            .unwrap();
        let ingress = crate::TurnIngressRef {
            request_id: running.request_id.clone(),
            turn_id: running.turn_id.clone(),
            message_id: running.message_id.clone(),
            session_id: running.session_id.clone(),
            primary_task_id: "requested-live-task".into(),
            root_task_id: "requested-live-task".into(),
            session_generation: running.session_generation,
            input_sequence: running.sequence as u64,
            claim_owner: running.claim_owner.clone().expect("original claim owner"),
            claim_token: running.claim_token.clone().expect("original claim token"),
            claim_revision: running
                .claim_fence_epoch
                .expect("original frozen claim epoch"),
            handoff_id: None,
        };
        let execution_id = crate::session_ingress_graph_id(
            &ingress.session_id,
            &ingress.request_id,
            &ingress.turn_id,
        );
        let control = services
            .create_turn_cancellation_control(&execution_id, &ingress)
            .unwrap();
        let receipt = services
            .commit_cancellation_receipt(CancellationReceipt {
                cancellation_id: "requested-live-cancellation".into(),
                session_id: running.session_id,
                turn_id: running.turn_id,
                execution_id,
                actor_id: "principal:requested-live-owner".into(),
                cause: CancellationCause::UserRequested,
                reason: Some("original user request".into()),
                requested_at_ms: now_ms(),
                effective_at_ms: None,
                status: CancellationStatus::Requested,
                journal_sequence: 0,
                projection_revision: 0,
            })
            .unwrap();
        (services, control, receipt, ingress, store)
    }

    /// Use the original Session owner path; never mint a capability in fixtures.
    pub(crate) async fn authorized_fixture_source(
        services: &RuntimeServices,
        control: &Arc<TurnCancellationControl>,
        receipt: &CancellationReceipt,
    ) -> AuthorizedRequestedCancellation {
        let authenticated = services
            .authenticate_requested_cancellation(receipt, control)
            .await
            .unwrap();
        match services
            .authorize_requested_cancellation(authenticated)
            .await
            .unwrap()
        {
            CancellationAuthorizationOutcome::Authorized(source) => source,
            outcome => panic!("expected original Session authorization, got {outcome:?}"),
        }
    }

    async fn revoke_fixture_claim(
        store: &session::UnifiedSessionStore,
        ingress: &crate::TurnIngressRef,
    ) {
        let running = store
            .get_session_runtime_outbox(&ingress.request_id)
            .await
            .unwrap()
            .unwrap();
        store
            .requeue_claimed_session_runtime_outbox(
                &ingress.request_id,
                &ingress.claim_owner,
                ingress.session_generation,
                &ingress.claim_token,
                running.revision,
                harness_contract::turn::InputRoutingDecision::StartNewTurn,
                None,
                None,
                "revoke original claim for source fence test",
                now_ms(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn production_requested_source_confirms_historical_authorization_but_rejects_revoked_physical_claim(
    ) {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, ingress, store) =
            live_requested_fixture_with_store(root.path()).await;
        assert!(control.requested_source().is_none());
        assert!(!control.token().is_cancelled());
        let source = authorized_fixture_source(&services, &control, &receipt).await;
        assert_eq!(
            services
                .publish_requested_cancellation(&control, source)
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        let published = control
            .requested_source()
            .expect("published original Requested source");
        assert_eq!(published.receipt(), &receipt);
        assert_eq!(published.event_reference().1, 1);
        assert_eq!(published.event_reference().2, receipt.journal_sequence);
        assert!(control.token().is_cancelled());
        published.verify_current().await.unwrap();

        revoke_fixture_claim(&store, &ingress).await;
        assert!(published.verify_current().await.is_err());
        let historical = authorized_fixture_source(&services, &control, &receipt).await;
        assert_eq!(historical.authorization, published.authorization);
        assert_eq!(historical.event_reference(), published.event_reference());
        let replacement = store
            .claim_session_runtime_outbox("requested-replacement-worker", now_ms(), 120_000, 1, &[])
            .await
            .unwrap()
            .pop()
            .expect("replacement claim");
        assert!(replacement.claim_token.as_deref() != Some(ingress.claim_token.as_str()));
        assert_ne!(replacement.claim_fence_epoch, Some(ingress.claim_revision));
        assert!(published.verify_current().await.is_err());
        let historical = authorized_fixture_source(&services, &control, &receipt).await;
        assert_eq!(historical.authorization_id(), published.authorization_id());
        assert_eq!(historical.authorization, published.authorization);
        assert!(historical.verify_current().await.is_err());
        let mut replacement_ingress = ingress.clone();
        replacement_ingress.claim_owner = replacement.claim_owner.unwrap();
        replacement_ingress.claim_token = replacement.claim_token.unwrap();
        replacement_ingress.claim_revision = replacement.claim_fence_epoch.unwrap();
        let replacement_control = services
            .create_turn_cancellation_control(&receipt.execution_id, &replacement_ingress)
            .unwrap();
        assert!(services
            .publish_requested_cancellation(&replacement_control, historical)
            .is_err());
        assert!(!replacement_control.token().is_cancelled());
        assert!(replacement_control.requested_source().is_none());
        assert_eq!(
            control.requested_source().unwrap().event_id(),
            published.event_id()
        );
    }

    #[tokio::test]
    async fn revoke_before_authorization_commits_no_authorization_and_signals_no_control() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, ingress, store) =
            live_requested_fixture_with_store(root.path()).await;
        revoke_fixture_claim(&store, &ingress).await;
        // Immutable Runtime intent still authenticates after revocation.
        let authenticated = services
            .authenticate_requested_cancellation(&receipt, &control)
            .await
            .unwrap();
        let request = authenticated.authorization_request().unwrap();
        let expected_id = request.owner_request().event.event_id.clone();
        match services
            .authorize_requested_cancellation(authenticated)
            .await
            .unwrap()
        {
            CancellationAuthorizationOutcome::RejectedStale { authorization_id } => {
                assert_eq!(authorization_id, expected_id);
            }
            outcome => panic!("revocation must reject before authorization: {outcome:?}"),
        }
        assert!(store
            .get_session_domain_event_by_id(&receipt.session_id, &expected_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .count_session_domain_events_by_kind_from(
                    &receipt.session_id,
                    "session.claim.cancellation_authorized.v1",
                    0,
                )
                .await
                .unwrap(),
            0
        );
        assert!(!control.token().is_cancelled());
        assert!(control.requested_source().is_none());
        assert!(matches!(
            &*control.first_cause.lock().unwrap(),
            TurnCancellationCause::None
        ));
    }

    #[tokio::test]
    async fn authorization_commit_before_revoke_delays_notification_to_only_original_control() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, ingress, store) =
            live_requested_fixture_with_store(root.path()).await;
        // Commit authorization, deliberately leaving notification pending.
        let authorized = authorized_fixture_source(&services, &control, &receipt).await;
        assert!(!control.token().is_cancelled());
        assert!(control.requested_source().is_none());
        let committed = store
            .get_session_domain_event_by_id(&receipt.session_id, authorized.authorization_id())
            .await
            .unwrap()
            .expect("original committed Session authorization");
        assert_eq!(committed, authorized.authorization);
        revoke_fixture_claim(&store, &ingress).await;
        let replacement = store
            .claim_session_runtime_outbox("requested-delay-replacement", now_ms(), 120_000, 1, &[])
            .await
            .unwrap()
            .pop()
            .expect("new claim before old notification");
        let mut replacement_ingress = ingress.clone();
        replacement_ingress.claim_owner = replacement.claim_owner.unwrap();
        replacement_ingress.claim_token = replacement.claim_token.unwrap();
        replacement_ingress.claim_revision = replacement.claim_fence_epoch.unwrap();
        assert_ne!(replacement_ingress.claim_token, ingress.claim_token);
        assert_ne!(replacement_ingress.claim_revision, ingress.claim_revision);
        let new_control = services
            .create_turn_cancellation_control(&receipt.execution_id, &replacement_ingress)
            .unwrap();
        let historical = authorized_fixture_source(&services, &control, &receipt).await;
        assert_eq!(historical.authorization, committed);
        assert_eq!(historical.event_reference(), authorized.event_reference());
        assert!(services
            .publish_requested_cancellation(&new_control, historical.clone())
            .is_err());
        assert_eq!(
            services
                .publish_requested_cancellation(&control, historical)
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        assert!(control.token().is_cancelled());
        let published = control.requested_source().unwrap();
        assert_eq!(published.authorization, committed);
        assert_eq!(published.receipt(), &receipt);
        assert!(published.verify_current().await.is_err());
        assert!(!new_control.token().is_cancelled());
        assert!(new_control.requested_source().is_none());
        assert!(matches!(
            &*new_control.first_cause.lock().unwrap(),
            TurnCancellationCause::None
        ));
        assert_eq!(
            store
                .count_session_domain_events_by_kind_from(
                    &receipt.session_id,
                    "session.claim.cancellation_authorized.v1",
                    0,
                )
                .await
                .unwrap(),
            1
        );
        // Notification alone does not finalize the original Requested receipt.
        let (_, original) = read_original_requested_event(&services.event_store, &receipt).unwrap();
        assert_eq!(original, receipt);
        assert_eq!(
            services
                .event_store
                .list_stream(&format!("cancellation:{}", receipt.cancellation_id))
                .unwrap()
                .len(),
            1
        );
    }

    fn receipt() -> CancellationReceipt {
        CancellationReceipt {
            cancellation_id: "cancel-original".into(),
            session_id: "session-original".into(),
            turn_id: "turn-original".into(),
            execution_id: "execution-original".into(),
            actor_id: "principal:owner".into(),
            cause: CancellationCause::UserRequested,
            reason: Some("stop".into()),
            requested_at_ms: 10,
            effective_at_ms: None,
            status: CancellationStatus::Requested,
            journal_sequence: 0,
            projection_revision: 0,
        }
    }

    fn event(receipt: &CancellationReceipt) -> DurableRuntimeEvent {
        DurableRuntimeEvent {
            event_id: "requested-event-original".into(),
            stream_id: format!("cancellation:{}", receipt.cancellation_id),
            sequence: 1,
            scope: RuntimeEventScope::Session,
            kind: "session.cancellation_committed".into(),
            status: Some("requested".into()),
            actor: Some(receipt.actor_id.clone()),
            refs: vec![
                crate::RuntimeEventRef {
                    kind: "session".into(),
                    id: receipt.session_id.clone(),
                },
                crate::RuntimeEventRef {
                    kind: "execution".into(),
                    id: receipt.execution_id.clone(),
                },
                crate::RuntimeEventRef {
                    kind: "turn".into(),
                    id: receipt.turn_id.clone(),
                },
            ],
            payload: serde_json::to_value(receipt).unwrap(),
            created_at_ms: 11,
            commit_cursor: 41,
            transaction_id: format!("session-cancellation:{}:requested", receipt.cancellation_id),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: Some(format!("{}:requested", receipt.cancellation_id)),
        }
    }

    #[test]
    fn missing_requested_and_final_first_cannot_authenticate() {
        let store = RuntimeEventStore::for_test();
        assert!(read_original_requested_event(&store, &receipt()).is_err());
        let mut final_receipt = receipt();
        final_receipt.status = CancellationStatus::Cancelled;
        final_receipt.effective_at_ms = Some(20);
        let mut final_event = event(&final_receipt);
        final_event.status = Some("cancelled".into());
        assert!(authenticate_requested_event(&final_event, &final_receipt).is_err());
        assert!(authenticate_requested_event(&final_event, &receipt()).is_err());
    }

    #[test]
    fn bounded_stream_authentication_keeps_original_after_final_commit() {
        let store = RuntimeEventStore::for_test();
        let requested = receipt();
        let original_event = event(&requested);
        let append = |payload: CancellationReceipt, revision: u64, status: &str| {
            store
                .append_transaction(crate::AppendTransactionRequest {
                    transaction_id: format!(
                        "session-cancellation:{}:{status}",
                        payload.cancellation_id
                    ),
                    expected_streams: vec![crate::ExpectedStreamRevision {
                        stream_id: original_event.stream_id.clone(),
                        expected_revision: revision,
                    }],
                    events: vec![crate::RuntimeTransactionEventInput {
                        event: crate::RuntimeEventInput {
                            stream_id: original_event.stream_id.clone(),
                            scope: original_event.scope,
                            kind: original_event.kind.clone(),
                            status: Some(status.into()),
                            actor: original_event.actor.clone(),
                            refs: original_event.refs.clone(),
                            payload: serde_json::to_value(&payload).unwrap(),
                        },
                        idempotency_key: Some(format!("{}:{status}", payload.cancellation_id)),
                        schema_version: 1,
                    }],
                })
                .unwrap()
        };
        let original_commit = append(requested.clone(), 0, "requested");
        let mut final_receipt = requested.clone();
        final_receipt.status = CancellationStatus::Cancelled;
        final_receipt.effective_at_ms = Some(20);
        let final_commit = append(final_receipt.clone(), 1, "cancelled");
        let (original, authenticated) = read_original_requested_event(&store, &requested).unwrap();
        assert_eq!(original.sequence, 1);
        assert_eq!(authenticated.status, CancellationStatus::Requested);
        assert_eq!(
            authenticated.journal_sequence,
            original_commit.commit_cursor
        );
        assert_ne!(authenticated.journal_sequence, final_commit.commit_cursor);
        assert!(read_original_requested_event(&store, &final_receipt).is_err());
    }

    #[test]
    fn original_requested_checks_actor_cause_refs_target_and_position() {
        let requested = receipt();
        let original = event(&requested);
        assert!(authenticate_requested_event(&original, &requested).is_ok());
        for field in [
            "actor", "cause", "refs", "target", "cursor", "stream", "status",
        ] {
            let mut candidate = original.clone();
            match field {
                "actor" => candidate.actor = Some("principal:foreign".into()),
                "cause" => candidate.payload["cause"] = serde_json::json!("system"),
                "refs" => candidate.refs[1].id = "foreign-execution".into(),
                "target" => {
                    candidate.payload["execution_id"] = serde_json::json!("foreign-execution")
                }
                "cursor" => candidate.commit_cursor = 0,
                "stream" => candidate.stream_id = "cancellation:foreign".into(),
                "status" => candidate.status = Some("cancelled".into()),
                _ => unreachable!(),
            }
            assert!(
                authenticate_requested_event(&candidate, &requested).is_err(),
                "{field}"
            );
        }
        let mut wrong_reference = requested;
        wrong_reference.journal_sequence = original.commit_cursor + 1;
        assert!(authenticate_requested_event(&original, &wrong_reference).is_err());
    }

    #[tokio::test]
    async fn replay_preserves_original_source_and_foreign_id_is_rejected() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, _) = live_requested_fixture(root.path()).await;
        let original = authorized_fixture_source(&services, &control, &receipt).await;
        assert_eq!(
            services
                .publish_requested_cancellation(&control, original.clone())
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        let replay = authorized_fixture_source(&services, &control, &receipt).await;
        assert_eq!(replay.authorization, original.authorization);
        assert_eq!(
            services
                .publish_requested_cancellation(&control, replay)
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        control.signal_operational();
        let observed = control.requested_source().unwrap();
        assert_eq!(observed.event_id(), original.event_id());
        assert_eq!(observed.event_reference(), original.event_reference());
        assert_eq!(observed.receipt().status, CancellationStatus::Requested);
        assert_eq!(
            observed.receipt().journal_sequence,
            receipt.journal_sequence
        );
        assert_eq!(observed.authorization, original.authorization);
        let mut foreign = original.clone();
        foreign.source.receipt.cancellation_id = "foreign-request".into();
        assert!(services
            .publish_requested_cancellation(&control, foreign)
            .is_err());
        let mut foreign_event = original;
        foreign_event.source.event_id = "foreign-event".into();
        assert!(services
            .publish_requested_cancellation(&control, foreign_event)
            .is_err());
        assert_eq!(
            control.requested_source().unwrap().authorization,
            observed.authorization
        );
    }

    #[tokio::test]
    async fn operational_first_never_becomes_requested() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, _) = live_requested_fixture(root.path()).await;
        control.signal_operational();
        assert!(control.requested_source().is_none());
        let original = authorized_fixture_source(&services, &control, &receipt).await;
        for source in [original.clone(), original] {
            assert_eq!(
                services
                    .publish_requested_cancellation(&control, source)
                    .unwrap(),
                CancellationNotificationDisposition::OperationalFirst
            );
        }
        assert!(control.token().is_cancelled());
        assert!(control.requested_source().is_none());
    }

    #[tokio::test]
    async fn released_original_control_rejects_source_reverification() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, _) = live_requested_fixture(root.path()).await;
        let source = {
            let original = authorized_fixture_source(&services, &control, &receipt).await;
            assert_eq!(
                services
                    .publish_requested_cancellation(&control, original.clone())
                    .unwrap(),
                CancellationNotificationDisposition::Delivered
            );
            original
        };
        source.verify_current().await.unwrap();
        drop(control);
        assert!(source.source.original_control.upgrade().is_none());
        let error = source.verify_current().await.unwrap_err();
        assert_eq!(
            error,
            "original Requested cancellation control was released"
        );
    }

    #[test]
    fn delegated_control_shares_owner_token_but_not_sibling_or_step_stop() {
        let owner = crate::CancellationToken::new();
        let sibling = crate::CancellationToken::new();
        let control = TurnCancellationControl::operational_with_token(
            "child-session", "child-turn", "child-execution", owner.clone(),
        );
        let sibling_control = TurnCancellationControl::operational_with_token(
            "child-session", "other-turn", "other-execution", sibling.clone(),
        );
        control.notify_step_stop();
        assert!(!control.is_cancelled());
        owner.cancel();
        assert!(control.is_cancelled());
        assert!(!sibling_control.is_cancelled());
        assert!(control.requested_source().is_none());
    }

    #[tokio::test]
    async fn replacement_control_and_unbound_control_reject_original_authorization() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, original, receipt, ingress) = live_requested_fixture(root.path()).await;
        let source = authorized_fixture_source(&services, &original, &receipt).await;
        let replacement = services
            .create_turn_cancellation_control(&receipt.execution_id, &ingress)
            .unwrap();
        assert!(services
            .publish_requested_cancellation(&replacement, source.clone())
            .is_err());
        assert!(!replacement.token().is_cancelled());
        assert!(replacement.requested_source().is_none());
        let unbound = TurnCancellationControl::operational(
            &receipt.session_id,
            &receipt.turn_id,
            &receipt.execution_id,
        );
        // Keep the exact pointer predicate valid to isolate the missing ingress.
        let mut unbound_source = source;
        unbound_source.source.original_control = Arc::downgrade(&unbound);
        assert!(services
            .publish_requested_cancellation(&unbound, unbound_source)
            .is_err());
        assert!(!unbound.token().is_cancelled());
        assert!(unbound.requested_source().is_none());
    }
}

#[cfg(test)]
mod requested_cancellation_unknown_recovery_tests {
    use super::*;
    use crate::session_runtime_port::{
        RuntimeSessionCancellationAuthorizationRequest, RuntimeSessionClaimEventAppend,
    };
    use harness_contract::projection::ExecutionLiveStatus;
    use harness_contract::turn::{CancellationReceipt, CancellationStatus};
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;

    const AUTHORIZATION_KIND: &str = "session.claim.cancellation_authorized.v1";
    const BOUND: Duration = Duration::from_secs(5);

    enum JournalFault {
        BeforeAppend,
        // Injected result loss after the real owner returns; not a TCP cut.
        LostResult,
        Deferred {
            queued: Arc<Notify>,
            release: Arc<Notify>,
            task: Mutex<
                Option<JoinHandle<Result<session::SessionDomainEvent, session::SessionError>>>,
            >,
        },
    }

    struct FaultJournal {
        inner: Arc<dyn crate::SessionRuntimeJournalPort>,
        fault: JournalFault,
        deferred_started: AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::SessionRuntimeJournalPort for FaultJournal {
        async fn commit_input_application_receipt(
            &self,
            input_ids: &[String],
            expected_revisions: &[u64],
            receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
            now_ms: u64,
        ) -> Result<Vec<crate::RuntimeSessionInputRecord>, session::SessionError> {
            self.inner
                .commit_input_application_receipt(input_ids, expected_revisions, receipt, now_ms)
                .await
        }

        async fn authorize_requested_cancellation(
            &self,
            request: &RuntimeSessionCancellationAuthorizationRequest,
        ) -> Result<session::SessionDomainEvent, session::SessionError> {
            match &self.fault {
                JournalFault::BeforeAppend => Err(session::SessionError::Store(
                    "injected generic error before append".into(),
                )),
                JournalFault::LostResult => {
                    self.inner.authorize_requested_cancellation(request).await?;
                    Err(session::SessionError::Store(
                        "injected owner result loss".into(),
                    ))
                }
                JournalFault::Deferred {
                    queued,
                    release,
                    task,
                } if !self.deferred_started.swap(true, Ordering::SeqCst) => {
                    let owned = RuntimeSessionCancellationAuthorizationRequest::new(
                        request.owner_request().clone(),
                    );
                    let inner = Arc::clone(&self.inner);
                    let queued = Arc::clone(queued);
                    let release = Arc::clone(release);
                    let handle = tokio::spawn(async move {
                        queued.notify_one();
                        release.notified().await;
                        inner.authorize_requested_cancellation(&owned).await
                    });
                    *task.lock().unwrap() = Some(handle);
                    Err(session::SessionError::Store(
                        "injected result loss with prepared write queued".into(),
                    ))
                }
                JournalFault::Deferred { .. } => {
                    self.inner.authorize_requested_cancellation(request).await
                }
            }
        }

        async fn append_event_if_claim_current(
            &self,
            request: &RuntimeSessionClaimEventAppend,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            self.inner.append_event_if_claim_current(request).await
        }

        async fn append_event(
            &self,
            event: &crate::RuntimeSessionEvent,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            self.inner.append_event(event).await
        }

        async fn append_context_envelope_if_absent(
            &self,
            record: &crate::RuntimeContextEnvelopeRecord,
        ) -> Result<Option<crate::RuntimeSessionEventReceipt>, session::SessionError> {
            self.inner.append_context_envelope_if_absent(record).await
        }

        async fn append_compaction_bundle_if_absent(
            &self,
            events: &[crate::RuntimeSessionEvent],
            checkpoint_id: &str,
        ) -> Result<bool, session::SessionError> {
            self.inner
                .append_compaction_bundle_if_absent(events, checkpoint_id)
                .await
        }
    }

    async fn fault_fixture(
        root: &Path,
        fault: JournalFault,
    ) -> (
        Arc<RuntimeServices>,
        Arc<TurnCancellationControl>,
        CancellationReceipt,
        Arc<session::UnifiedSessionStore>,
        Arc<FaultJournal>,
    ) {
        let original_root = root.join("original");
        std::fs::create_dir_all(&original_root).unwrap();
        let (original, original_control, mut receipt, ingress, store) =
            requested_cancellation_source_tests::live_requested_fixture_with_store(&original_root)
                .await;
        assert!(!original_control.token().is_cancelled());
        let fresh_root = root.join("fault-consumer");
        let workspace = fresh_root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let services = RuntimeServices::test_builder(&fresh_root, &workspace)
            .build()
            .unwrap();
        let journal = Arc::new(FaultJournal {
            inner: Arc::clone(original.session_journal_port.get().unwrap()),
            fault,
            deferred_started: AtomicBool::new(false),
        });
        services
            .install_session_ports(
                Arc::clone(original.session_query_port.get().unwrap()),
                Arc::clone(original.session_ingress_port.get().unwrap()),
                journal.clone(),
                Arc::clone(original.session_application_port.get().unwrap()),
            )
            .unwrap();
        receipt.journal_sequence = 0;
        receipt.projection_revision = 0;
        let receipt = services.commit_cancellation_receipt(receipt).unwrap();
        let control = services
            .create_turn_cancellation_control(&receipt.execution_id, &ingress)
            .unwrap();
        (services, control, receipt, store, journal)
    }

    async fn authorization_attempt(
        services: &RuntimeServices,
        control: &Arc<TurnCancellationControl>,
        receipt: &CancellationReceipt,
    ) -> (String, CancellationAuthorizationOutcome) {
        let authenticated = services
            .authenticate_requested_cancellation(receipt, control)
            .await
            .unwrap();
        let request = authenticated.authorization_request().unwrap();
        let id = request.owner_request().event.event_id.clone();
        let outcome = tokio::time::timeout(
            BOUND,
            services.authorize_requested_cancellation(authenticated),
        )
        .await
        .expect("bounded authorization")
        .unwrap();
        (id, outcome)
    }

    fn assert_unsignalled(control: &TurnCancellationControl) {
        assert!(!control.token().is_cancelled());
        assert!(control.requested_source().is_none());
        assert!(matches!(
            &*control.first_cause.lock().unwrap(),
            TurnCancellationCause::None
        ));
    }

    fn assert_requested(services: &RuntimeServices, receipt: &CancellationReceipt) {
        assert_eq!(
            services
                .cancellation_receipt(&receipt.cancellation_id)
                .unwrap(),
            Some(receipt.clone())
        );
        let rows = services
            .event_store
            .list_stream(&format!("cancellation:{}", receipt.cancellation_id))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, "session.cancellation_committed");
        assert_eq!(rows[0].status.as_deref(), Some("requested"));
        let stored: CancellationReceipt = serde_json::from_value(rows[0].payload.clone()).unwrap();
        assert_eq!(stored.status, CancellationStatus::Requested);
        let (_, original) = read_original_requested_event(&services.event_store, receipt).unwrap();
        assert_eq!(&original, receipt);
    }

    fn assert_live_pending(services: &RuntimeServices, receipt: &CancellationReceipt) {
        let live = services
            .execution_live(&receipt.execution_id)
            .expect("actual live execution");
        assert_eq!(live.status, ExecutionLiveStatus::Queued);
        assert_eq!(live.terminal_ref, None);
    }

    async fn assert_authorization_count(
        store: &session::UnifiedSessionStore,
        receipt: &CancellationReceipt,
        expected: usize,
    ) {
        let count = store
            .count_session_domain_events_by_kind_from(&receipt.session_id, AUTHORIZATION_KIND, 0)
            .await
            .unwrap();
        assert_eq!(count, expected);
    }

    #[tokio::test]
    async fn generic_write_error_and_real_none_remain_requested_in_same_tick_reconcile() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, store, _journal) =
            fault_fixture(root.path(), JournalFault::BeforeAppend).await;
        assert_requested(&services, &receipt);
        assert_unsignalled(&control);
        assert_authorization_count(&store, &receipt, 0).await;
        let (expected_id, outcome) = authorization_attempt(&services, &control, &receipt).await;
        match outcome {
            CancellationAuthorizationOutcome::Unknown {
                authorization_id,
                reason,
            } => {
                assert_eq!(authorization_id, expected_id);
                assert!(reason.contains("injected generic error before append"));
            }
            other => panic!("generic error plus real None must stay Unknown: {other:?}"),
        }
        assert!(store
            .get_session_domain_event_by_id(&receipt.session_id, &expected_id)
            .await
            .unwrap()
            .is_none());
        // Actual ROUND197 consumer: a live target exists before this same-tick reconcile.
        services.record_live_execution(
            &receipt.session_id,
            receipt.execution_id.clone(),
            receipt.turn_id.clone(),
        );
        let before = services.execution_live(&receipt.execution_id).unwrap();
        assert_live_pending(&services, &receipt);
        assert!(services
            .reconcile_requested_cancellations(16, &[])
            .unwrap()
            .is_empty());
        assert_eq!(
            services.execution_live(&receipt.execution_id).unwrap(),
            before
        );
        assert_live_pending(&services, &receipt);
        assert_requested(&services, &receipt);
        assert_unsignalled(&control);
        assert_eq!(
            services
                .replay_or_resolve_inactive_cancellation(&receipt.cancellation_id)
                .unwrap(),
            None
        );
        assert_requested(&services, &receipt);
        let (retry_id, retry) = authorization_attempt(&services, &control, &receipt).await;
        assert_eq!(retry_id, expected_id);
        assert!(
            matches!(retry, CancellationAuthorizationOutcome::Unknown { authorization_id, .. } if authorization_id == expected_id)
        );
        assert_authorization_count(&store, &receipt, 0).await;
        assert_requested(&services, &receipt);
        assert_unsignalled(&control);
        assert_live_pending(&services, &receipt);
    }

    #[tokio::test]
    async fn injected_owner_result_loss_confirms_one_row_and_protected_final_replay() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (services, control, receipt, store, _journal) =
            fault_fixture(root.path(), JournalFault::LostResult).await;
        services.record_live_execution(
            &receipt.session_id,
            receipt.execution_id.clone(),
            receipt.turn_id.clone(),
        );
        assert_live_pending(&services, &receipt);
        assert_authorization_count(&store, &receipt, 0).await;
        let (id, outcome) = authorization_attempt(&services, &control, &receipt).await;
        let CancellationAuthorizationOutcome::Authorized(cap) = outcome else {
            panic!("real exact read must confirm injected owner result loss: {outcome:?}");
        };
        assert_eq!(cap.authorization_id(), id);
        let stored = store
            .get_session_domain_event_by_id(&receipt.session_id, &id)
            .await
            .unwrap()
            .expect("real committed authorization");
        assert_eq!(stored.kind, AUTHORIZATION_KIND);
        assert_eq!(stored, cap.authorization);
        assert_authorization_count(&store, &receipt, 1).await;
        assert_unsignalled(&control);
        assert_requested(&services, &receipt);
        assert_live_pending(&services, &receipt);
        let (retry_id, retry) = authorization_attempt(&services, &control, &receipt).await;
        let CancellationAuthorizationOutcome::Authorized(replayed) = retry else {
            panic!("duplicate must confirm the original row: {retry:?}");
        };
        assert_eq!(retry_id, id);
        assert_eq!(replayed.authorization, stored);
        assert_eq!(replayed.event_reference(), cap.event_reference());
        assert_eq!(
            services
                .publish_requested_cancellation(&control, cap.clone())
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        assert_eq!(
            services
                .publish_requested_cancellation(&control, replayed)
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        assert!(control.token().is_cancelled());
        assert_eq!(control.requested_source().unwrap().authorization, stored);
        assert_requested(&services, &receipt);
        assert_live_pending(&services, &receipt);
        let finalized = services
            .reconcile_requested_cancellations(16, &[cap.clone()])
            .unwrap();
        assert_eq!(finalized.len(), 1);
        let winner = &finalized[0];
        assert_eq!(winner.status, CancellationStatus::Cancelled);
        let live = services.execution_live(&receipt.execution_id).unwrap();
        assert_eq!(live.status, ExecutionLiveStatus::Cancelled);
        let terminal_ref = format!("cancellation:{}", receipt.cancellation_id);
        assert_eq!(live.terminal_ref.as_deref(), Some(terminal_ref.as_str()));
        // The real terminal slot is owned by cancellation, so a competing completion loses.
        assert_eq!(
            services
                .claim_live_terminal_fence(
                    &receipt.execution_id,
                    "competing-completion".into(),
                    ExecutionLiveStatus::Complete,
                    receipt.requested_at_ms,
                )
                .unwrap(),
            crate::execution_live::TerminalFenceClaim::ConflictingTerminal
        );
        let rows = services
            .event_store
            .list_stream(&format!("cancellation:{}", receipt.cancellation_id))
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].status.as_deref(), Some("cancelled"));
        for _ in 0..2 {
            assert_eq!(
                services
                    .replay_or_resolve_inactive_cancellation(&receipt.cancellation_id)
                    .unwrap(),
                Some(winner.clone())
            );
            assert_eq!(
                services
                    .resolve_authorized_requested_cancellation(&cap)
                    .unwrap(),
                Some(winner.clone())
            );
            assert!(services
                .reconcile_requested_cancellations(16, &[])
                .unwrap()
                .is_empty());
        }
        assert_eq!(
            services
                .event_store
                .list_stream(&format!("cancellation:{}", receipt.cancellation_id))
                .unwrap(),
            rows
        );
        assert_eq!(
            services.execution_live(&receipt.execution_id).unwrap(),
            live
        );
        assert_eq!(
            store
                .get_session_domain_event_by_id(&receipt.session_id, &id)
                .await
                .unwrap(),
            Some(stored)
        );
        assert_authorization_count(&store, &receipt, 1).await;
    }

    #[tokio::test]
    async fn queued_original_write_real_none_is_unknown_until_owned_task_commits() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let queued = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (services, control, receipt, store, journal) = fault_fixture(
            root.path(),
            JournalFault::Deferred {
                queued: Arc::clone(&queued),
                release: Arc::clone(&release),
                task: Mutex::new(None),
            },
        )
        .await;
        services.record_live_execution(
            &receipt.session_id,
            receipt.execution_id.clone(),
            receipt.turn_id.clone(),
        );
        // Catch assertion panics so every spawned writer is released and reaped even on failure.
        let pending_phase = std::panic::AssertUnwindSafe(async {
            assert_authorization_count(&store, &receipt, 0).await;
            let (id, outcome) = authorization_attempt(&services, &control, &receipt).await;
            tokio::time::timeout(BOUND, queued.notified()).await.expect("writer queued barrier");
            assert!(matches!(outcome, CancellationAuthorizationOutcome::Unknown { authorization_id, .. } if authorization_id == id));
            assert!(store.get_session_domain_event_by_id(&receipt.session_id, &id)
                .await.unwrap().is_none());
            assert_authorization_count(&store, &receipt, 0).await;
            assert_unsignalled(&control);
            assert_requested(&services, &receipt);
            let before = services.execution_live(&receipt.execution_id).unwrap();
            assert_live_pending(&services, &receipt);
            assert!(services.reconcile_requested_cancellations(16, &[]).unwrap().is_empty());
            assert_eq!(services.replay_or_resolve_inactive_cancellation(&receipt.cancellation_id).unwrap(), None);
            assert_eq!(services.execution_live(&receipt.execution_id).unwrap(), before);
            assert_requested(&services, &receipt);
            assert_unsignalled(&control);
            id
        }).catch_unwind().await;
        release.notify_one();
        let JournalFault::Deferred { task, .. } = &journal.fault else {
            unreachable!()
        };
        let handle = task.lock().unwrap().take();
        let committed = if let Some(mut handle) = handle {
            match tokio::time::timeout(BOUND, &mut handle).await {
                Ok(result) => Some(
                    result
                        .expect("owned writer joined")
                        .expect("real owner commit"),
                ),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    panic!("owned writer exceeded bounded commit; aborted and reaped");
                }
            }
        } else {
            None
        };
        let id = match pending_phase {
            Ok(id) => id,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        let committed = committed.expect("owned deferred write was retained and reaped");
        assert_eq!(committed.event_id, id);
        assert_eq!(committed.kind, AUTHORIZATION_KIND);
        assert_eq!(
            store
                .get_session_domain_event_by_id(&receipt.session_id, &id)
                .await
                .unwrap(),
            Some(committed.clone())
        );
        assert_authorization_count(&store, &receipt, 1).await;
        // Commit alone still cannot notify or finalize: Runtime has not confirmed it yet.
        assert_unsignalled(&control);
        assert_requested(&services, &receipt);
        assert!(services
            .reconcile_requested_cancellations(16, &[])
            .unwrap()
            .is_empty());
        assert_live_pending(&services, &receipt);
        let (retry_id, outcome) = authorization_attempt(&services, &control, &receipt).await;
        let CancellationAuthorizationOutcome::Authorized(cap) = outcome else {
            panic!("same-key reentry must confirm the actual owner commit: {outcome:?}");
        };
        assert_eq!(retry_id, id);
        assert_eq!(cap.authorization_id(), id);
        assert_eq!(cap.authorization, committed);
        assert_unsignalled(&control);
        assert_requested(&services, &receipt);
        assert_eq!(
            services
                .publish_requested_cancellation(&control, cap.clone())
                .unwrap(),
            CancellationNotificationDisposition::Delivered
        );
        assert!(control.token().is_cancelled());
        let final_receipts = services
            .reconcile_requested_cancellations(16, &[cap])
            .unwrap();
        assert_eq!(final_receipts.len(), 1);
        assert_eq!(final_receipts[0].status, CancellationStatus::Cancelled);
        assert_eq!(
            services
                .execution_live(&receipt.execution_id)
                .unwrap()
                .status,
            ExecutionLiveStatus::Cancelled
        );
        assert_authorization_count(&store, &receipt, 1).await;
    }
}
