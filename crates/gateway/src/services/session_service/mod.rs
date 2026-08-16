use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

pub(crate) mod activation;
mod journal_event;
pub(crate) mod presence;
pub(crate) mod repository;

pub(crate) use self::activation::{
    EnsureSessionOutcome, EnsureSessionRequest, SessionRecoverySummary, SessionSource,
    SessionWorkingSetProjection,
};
use self::activation::{SessionActivationCoordinator, SessionActivationIntent};
pub(crate) use self::journal_event::{
    ContextSessionJournalEvent, SurfaceRegisteredResourceEvidence, SurfaceResourceEvidence,
    SurfaceResourceRegistrationStatus, SurfaceSessionJournalEvent,
};
use self::presence::{SessionActor, SessionPresenceLedger};
use self::repository::SessionRepository;
use super::ServiceEnvelope;
use crate::runtime_service::RuntimeService;
use chrono::{DateTime, Utc};
use cowd_app_protocol::{
    ApplicationExecutionSummaryIdempotencyV1, ApplicationExecutionSummaryReceiptV1,
    ApplicationExecutionSummaryStatusV1, ApplicationExecutionSummaryV1, ProtocolValidate,
};
use harness_contract::task::{
    SessionFocusMutation, SessionFocusReceipt, SessionMissionFocus, SessionRoutingFocus,
    SessionTaskFocus,
};
use harness_contract::turn::{
    InputPayloadKind, InputRelationProposal, InputRoutingDecision, InputRoutingReason,
    InputSourceKind, SessionInputCursor, SessionInputEnvelope, SessionInputId,
    SessionInputProjection, SessionInputReceipt, SessionInputStatus, TurnId, TurnInboxItem,
    TurnInboxSnapshot,
};
use serde::{Deserialize, Serialize};
use session::{
    OutboxFailureClass, SessionDomainEventPage, SessionDomainScope, SessionError, SessionEvent,
    SessionListOptions, SessionListPage, SessionMessage, SessionRecord, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageSummary,
};
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct SessionService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Arc<SessionRepository>,
    presence_ledger: Arc<SessionPresenceLedger>,
    runtime: Option<Arc<RuntimeService>>,
    coordinator: Option<Arc<SessionActivationCoordinator>>,
    supervisor: Arc<OnceLock<Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SessionUpdateRequest {
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) metadata: Option<serde_json::Value>,
}

const ROUTING_FOCUS_METADATA_KEY: &str = "routing_focus";

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionCompactResult {
    pub(crate) session_id: String,
    pub(crate) compacted: bool,
    pub(crate) removed_message_count: usize,
    pub(crate) summary: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionBranchOutcome {
    pub(crate) session: EnsureSessionOutcome,
    pub(crate) operation_id: String,
    pub(crate) replayed: bool,
    pub(crate) copied_message_count: usize,
    pub(crate) source_message_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionStatsSnapshot {
    pub(crate) session_id: String,
    pub(crate) message_count: usize,
    pub(crate) message_counts: SessionMessageCounts,
    pub(crate) tokens: SessionTokenCounts,
    pub(crate) tool_usage: HashMap<String, usize>,
    pub(crate) duration_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionMessageCounts {
    pub(crate) user: usize,
    pub(crate) assistant: usize,
    pub(crate) tool: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionTokenCounts {
    pub(crate) input: u32,
    pub(crate) output: u32,
    pub(crate) total: u32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActiveMessagesPage {
    pub(crate) session_id: String,
    pub(crate) messages: Vec<serde_json::Value>,
    pub(crate) total: usize,
    pub(crate) offset: usize,
    pub(crate) from_seq: Option<usize>,
    pub(crate) next_seq: Option<usize>,
    pub(crate) limit: usize,
    pub(crate) has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableInputClassification {
    reason: InputRoutingReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relation_proposal: Option<InputRelationProposal>,
    source_kind: InputSourceKind,
    payload_kind: InputPayloadKind,
    content_preview: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_message_id: Option<String>,
    idempotency_key: String,
    metadata: serde_json::Value,
    created_at: DateTime<Utc>,
}

const fn close_transitional_status(disposition: session::SessionCloseDisposition) -> &'static str {
    match disposition {
        session::SessionCloseDisposition::Archive => "archiving",
        session::SessionCloseDisposition::Delete => "deleting",
    }
}

const fn close_terminal_status(disposition: session::SessionCloseDisposition) -> &'static str {
    match disposition {
        session::SessionCloseDisposition::Archive => "archived",
        session::SessionCloseDisposition::Delete => "deleted",
    }
}

const fn close_fence_reason(disposition: session::SessionCloseDisposition) -> &'static str {
    match disposition {
        session::SessionCloseDisposition::Archive => "session archived",
        session::SessionCloseDisposition::Delete => "session deleted",
    }
}

const fn close_started_event(disposition: session::SessionCloseDisposition) -> &'static str {
    match disposition {
        session::SessionCloseDisposition::Archive => "session.archive_started",
        session::SessionCloseDisposition::Delete => "session.delete_started",
    }
}

const fn close_completed_event(disposition: session::SessionCloseDisposition) -> &'static str {
    match disposition {
        session::SessionCloseDisposition::Archive => "session.archived",
        session::SessionCloseDisposition::Delete => "session.deleted",
    }
}

const fn close_metadata_key(disposition: session::SessionCloseDisposition) -> &'static str {
    match disposition {
        session::SessionCloseDisposition::Archive => "archived_at",
        session::SessionCloseDisposition::Delete => "deleted_at",
    }
}

fn lifecycle_event(
    session_id: &str,
    kind: &str,
    payload: serde_json::Value,
    occurred_at_ms: u64,
) -> Result<SessionEvent, String> {
    let mut event = session::SessionDomainEvent::new(
        session_id,
        0,
        SessionDomainScope::Context,
        kind,
        payload,
        occurred_at_ms,
    );
    event.event_id = format!("{kind}:{session_id}:{occurred_at_ms}");
    event
        .to_session_event()
        .map_err(|error| format!("encode lifecycle event: {error}"))
}

fn runtime_domain_event(event: &runtime::RuntimeSessionEvent) -> session::SessionDomainEvent {
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

impl SessionService {
    #[must_use]
    pub(crate) fn lifecycle_work_wake(&self) -> Arc<Notify> {
        self.kernel.lifecycle_work_wake()
    }

    #[must_use]
    pub(crate) fn branch_work_wake(&self) -> Arc<Notify> {
        self.kernel.branch_work_wake()
    }

    pub(crate) fn new(
        runtime: Arc<RuntimeService>,
        coordinator: Arc<SessionActivationCoordinator>,
        supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
    ) -> Self {
        let service = Self::new_unbound(runtime, coordinator);
        service
            .install_supervisor(supervisor)
            .expect("new SessionService has no installed supervisor");
        service
    }

    pub(crate) fn new_unbound(
        runtime: Arc<RuntimeService>,
        coordinator: Arc<SessionActivationCoordinator>,
    ) -> Self {
        let kernel = coordinator.repository();
        let presence_ledger = coordinator.presence_ledger();
        Self {
            label: "session",
            owner: "Gateway Session application owner",
            kernel,
            presence_ledger,
            runtime: Some(runtime),
            coordinator: Some(coordinator),
            supervisor: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn install_supervisor(
        &self,
        supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
    ) -> Result<(), String> {
        self.supervisor
            .set(supervisor)
            .map_err(|_| "Session worker supervisor was already installed".to_string())
    }

    #[cfg(test)]
    pub(crate) fn for_tests(
        kernel: Arc<SessionRepository>,
        presence_ledger: Arc<SessionPresenceLedger>,
    ) -> Self {
        Self {
            label: "session",
            owner: "Gateway Session test owner",
            kernel,
            presence_ledger,
            runtime: None,
            coordinator: None,
            supervisor: Arc::new(OnceLock::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_for_tests(
        runtime: Arc<RuntimeService>,
        coordinator: Arc<SessionActivationCoordinator>,
    ) -> Self {
        Self {
            label: "session",
            owner: "Gateway Session runtime test owner",
            kernel: coordinator.repository(),
            presence_ledger: coordinator.presence_ledger(),
            runtime: Some(runtime),
            coordinator: Some(coordinator),
            supervisor: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_ready",
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    fn kernel(&self) -> &Arc<SessionRepository> {
        &self.kernel
    }

    fn presence_ledger(&self) -> &Arc<SessionPresenceLedger> {
        &self.presence_ledger
    }

    fn runtime(&self) -> Result<&Arc<RuntimeService>, String> {
        self.runtime
            .as_ref()
            .ok_or_else(|| "Session Runtime port is unavailable in this test service".to_string())
    }

    fn coordinator(&self) -> Result<&Arc<SessionActivationCoordinator>, String> {
        self.coordinator.as_ref().ok_or_else(|| {
            "Session activation coordinator is unavailable in this test service".to_string()
        })
    }

    fn ensure_accepting(&self) -> Result<(), String> {
        if self
            .supervisor
            .get()
            .is_some_and(|supervisor| !supervisor.is_accepting())
        {
            return Err("Session admission is shutting down".to_string());
        }
        Ok(())
    }

    pub(crate) fn worker_health(
        &self,
    ) -> Result<crate::session_runtime_bridge::SessionWorkerHealth, String> {
        self.supervisor
            .get()
            .map(|supervisor| supervisor.health())
            .ok_or_else(|| "Session worker supervisor is unavailable".to_string())
    }

    pub(crate) async fn create_user_session(
        &self,
        request: EnsureSessionRequest,
    ) -> Result<EnsureSessionOutcome, String> {
        self.ensure_accepting()?;
        if !matches!(&request.source, SessionSource::WebUi | SessionSource::Tui) {
            return Err("create_user_session requires a user-facing Session source".to_string());
        }
        self.coordinator()?
            .activate(request, SessionActivationIntent::CreateNew)
            .await
    }

    pub(crate) async fn ensure_surface_session(
        &self,
        request: EnsureSessionRequest,
    ) -> Result<EnsureSessionOutcome, String> {
        self.ensure_accepting()?;
        if matches!(&request.source, SessionSource::Internal) {
            return Err("ensure_surface_session does not accept an internal source".to_string());
        }
        self.coordinator()?
            .activate(request, SessionActivationIntent::Ensure)
            .await
    }

    pub(crate) async fn activate_existing_session(
        &self,
        request: EnsureSessionRequest,
    ) -> Result<EnsureSessionOutcome, String> {
        self.coordinator()?
            .activate(request, SessionActivationIntent::ExistingOnly)
            .await
    }

    pub(crate) async fn activate_worker_session(
        &self,
        session_id: &str,
    ) -> Result<EnsureSessionOutcome, String> {
        self.activate_existing_session(EnsureSessionRequest::new(
            session_id,
            None,
            SessionSource::Internal,
        ))
        .await
    }

    /// Resolve a model-selected semantic Session target through the one
    /// Gateway Session owner. Existing targets must share the exact principal
    /// and workspace boundary; isolated targets are deterministic and
    /// idempotent for the durable disposition.
    pub(crate) async fn resolve_input_disposition_session_target(
        &self,
        request: &runtime::RuntimeSessionTargetRequest,
    ) -> Result<runtime::RuntimeSessionTargetResolution, String> {
        self.ensure_accepting()?;
        let source = self
            .stored_session(&request.source_session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("source Session {} not found", request.source_session_id))?;
        let source_metadata = session_metadata_object(&source);
        let source_owner = source_metadata
            .get("owner_principal_id")
            .and_then(serde_json::Value::as_str);
        let source_workspace = source_metadata
            .get("workspace_root")
            .and_then(serde_json::Value::as_str);

        match request.mode {
            harness_contract::input_disposition::InputDispositionSessionTargetMode::ExistingAuthorized => {
                let target_session_id = request
                    .target_ref
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.strip_prefix("@session:").unwrap_or(value))
                    .ok_or_else(|| "existing Session target_ref is required".to_string())?;
                if target_session_id == request.source_session_id {
                    return Err("Session dispatch target must differ from its source".to_string());
                }
                let target = self
                    .stored_session(target_session_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("target Session {target_session_id} not found"))?;
                let target_metadata = session_metadata_object(&target);
                let target_owner = target_metadata
                    .get("owner_principal_id")
                    .and_then(serde_json::Value::as_str);
                let target_workspace = target_metadata
                    .get("workspace_root")
                    .and_then(serde_json::Value::as_str);
                if source_owner.is_none()
                    || source_owner != target_owner
                    || source_workspace.is_none()
                    || source_workspace != target_workspace
                {
                    return Err(
                        "target Session is outside the source principal/workspace authority"
                            .to_string(),
                    );
                }
                let mut activation = EnsureSessionRequest::new(
                    target_session_id,
                    target.model.clone(),
                    SessionSource::Internal,
                );
                activation.owner_principal_id = source_owner.map(str::to_string);
                self.coordinator()?
                    .activate(activation, SessionActivationIntent::ExistingOnly)
                    .await?;
                Ok(runtime::RuntimeSessionTargetResolution {
                    target_session_id: target_session_id.to_string(),
                    created: false,
                })
            }
            harness_contract::input_disposition::InputDispositionSessionTargetMode::CreateIsolated => {
                if request.target_ref.is_some() {
                    return Err(
                        "create_isolated must not carry a model-selected Session reference"
                            .to_string(),
                    );
                }
                let suffix = request
                    .disposition_id
                    .strip_prefix("disposition-")
                    .unwrap_or(request.disposition_id.as_str());
                let target_session_id = format!("session-isolated-{suffix}");
                let mut activation = EnsureSessionRequest::new(
                    &target_session_id,
                    source.model.clone(),
                    SessionSource::Internal,
                );
                activation.title = Some(request.objective.chars().take(80).collect());
                activation.user_id = source.user_id.clone();
                activation.owner_principal_id = source_owner.map(str::to_string);
                activation.metadata = serde_json::json!({
                    "runtime_handoff_source_session_id": request.source_session_id,
                    "runtime_handoff_disposition_id": request.disposition_id,
                    "runtime_handoff_isolated": true,
                });
                let outcome = self
                    .coordinator()?
                    .activate(activation, SessionActivationIntent::Ensure)
                    .await?;
                let metadata = session_metadata_object(&outcome.record);
                if metadata
                    .get("runtime_handoff_disposition_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(request.disposition_id.as_str())
                    || metadata
                        .get("runtime_handoff_source_session_id")
                        .and_then(serde_json::Value::as_str)
                        != Some(request.source_session_id.as_str())
                {
                    return Err(
                        "deterministic isolated Session identity is already bound elsewhere"
                            .to_string(),
                    );
                }
                Ok(runtime::RuntimeSessionTargetResolution {
                    target_session_id: outcome.session_id,
                    created: outcome.created,
                })
            }
        }
    }

    pub(crate) async fn ensure_internal_context(
        &self,
        session_id: &str,
        platform: &str,
        metadata: serde_json::Value,
    ) -> Result<SessionRecord, String> {
        let session_id = session_id.trim();
        let platform = platform.trim();
        if session_id.is_empty() {
            return Err("internal context session id is required".to_string());
        }
        if platform.is_empty() {
            return Err("internal context platform is required".to_string());
        }
        let _guard = self.coordinator()?.acquire_exclusive(session_id).await;
        let now = Utc::now().to_rfc3339();
        if let Some(mut record) = self
            .kernel()
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
        {
            if matches!(
                record.status.as_str(),
                "archiving" | "archived" | "deleting" | "deleted"
            ) {
                return Err(format!(
                    "session {session_id} rejects internal context writes in lifecycle state {}",
                    record.status
                ));
            }
            record.last_activity = now;
            self.kernel()
                .update_stored_session(&record)
                .await
                .map_err(|error| error.to_string())?;
            return Ok(record);
        }

        let mut metadata = metadata.as_object().cloned().unwrap_or_default();
        metadata.insert(
            "internal_context".to_string(),
            serde_json::Value::Bool(true),
        );
        metadata.insert(
            "workspace_root".to_string(),
            serde_json::Value::String(
                self.runtime()?
                    .runtime_services()
                    .workspace_root()
                    .display()
                    .to_string(),
            ),
        );
        let record = SessionRecord {
            session_id: session_id.to_string(),
            platform: platform.to_string(),
            chat_id: session_id.to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "none".to_string(),
            metadata_json: Some(serde_json::Value::Object(metadata).to_string()),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        self.kernel()
            .upsert_stored_session(&record)
            .await
            .map_err(|error| error.to_string())?;
        Ok(record)
    }

    pub(crate) async fn append_application_execution_summary(
        &self,
        session_id: &str,
        summary: &ApplicationExecutionSummaryV1,
    ) -> Result<ApplicationExecutionSummaryReceiptV1, String> {
        self.append_application_execution_summary_for_producer(
            session_id,
            "gateway.matrix",
            summary,
        )
        .await
        .map_err(|error| error.to_string())
    }

    /// The only APP execution-outcome writer.
    ///
    /// `producer_id` must be supplied by a trusted Gateway/Host binding, never
    /// decoded from the APP intent payload. The durable idempotency key is
    /// producer + contract version + outcome id.
    pub(crate) async fn append_application_execution_summary_for_producer(
        &self,
        session_id: &str,
        producer_id: &str,
        summary: &ApplicationExecutionSummaryV1,
    ) -> Result<ApplicationExecutionSummaryReceiptV1, SessionError> {
        let normalized = summary.normalized().map_err(|error| {
            SessionError::InvalidArgument(format!("invalid application execution summary: {error}"))
        })?;
        normalized.validate().map_err(|error| {
            SessionError::InvalidArgument(format!("invalid application execution summary: {error}"))
        })?;
        let idempotency_key =
            ApplicationExecutionSummaryIdempotencyV1::bind(producer_id, &normalized).map_err(
                |error| {
                    SessionError::InvalidArgument(format!(
                        "invalid application execution producer identity: {error}"
                    ))
                },
            )?;
        self.ensure_internal_context(
            session_id,
            "app",
            serde_json::json!({"kind": "cowd.work_context.session"}),
        )
        .await
        .map_err(SessionError::Other)?;
        let mut event = session::SessionDomainEvent::new(
            session_id,
            0,
            SessionDomainScope::ApplicationTask,
            "application.execution_summary",
            serde_json::to_value(&normalized).map_err(SessionError::Serialization)?,
            normalized.occurred_at_ms,
        );
        event.event_id = idempotency_key.event_id();
        event.status =
            Some(application_execution_summary_status_label(normalized.status).to_string());
        let mut refs = vec![session::SessionDomainRef {
            ref_type: "producer".to_string(),
            id: idempotency_key.producer_id.clone(),
            label: None,
        }];
        refs.extend(
            normalized
                .refs
                .iter()
                .map(|reference| session::SessionDomainRef {
                    ref_type: reference.ref_type.clone(),
                    id: reference.id.clone(),
                    label: reference.label.clone(),
                }),
        );
        refs.extend(
            normalized
                .evidence_refs
                .iter()
                .map(|id| session::SessionDomainRef {
                    ref_type: "evidence".to_string(),
                    id: id.clone(),
                    label: None,
                }),
        );
        refs.extend(
            normalized
                .metric_refs
                .iter()
                .map(|id| session::SessionDomainRef {
                    ref_type: "metric".to_string(),
                    id: id.clone(),
                    label: None,
                }),
        );
        event.refs = refs;
        self.append_runtime_domain_event_if_absent(&event)
            .await
            .and_then(|(stored, replayed)| {
                u64::try_from(stored.sequence)
                    .map(|sequence| ApplicationExecutionSummaryReceiptV1 {
                        schema_version: 1,
                        producer_id: idempotency_key.producer_id.clone(),
                        summary_id: idempotency_key.summary_id.clone(),
                        sequence,
                        replayed,
                    })
                    .map_err(|_| {
                        SessionError::Store(
                            "application execution outcome sequence exceeds u64".to_string(),
                        )
                    })
            })
    }

    pub(crate) async fn branch_session(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        operation_id: String,
        title: String,
        model: Option<String>,
        owner_principal_id: String,
    ) -> Result<SessionBranchOutcome, String> {
        let runtime = self.runtime()?;
        if source_session_id == target_session_id {
            return Err("branch target must differ from its source".to_string());
        }
        let coordinator = self.coordinator()?;
        let (first_session_id, second_session_id) = if source_session_id < target_session_id {
            (source_session_id, target_session_id)
        } else {
            (target_session_id, source_session_id)
        };
        let first_guard = coordinator.acquire_exclusive(first_session_id).await;
        let second_guard = coordinator.acquire_exclusive(second_session_id).await;
        let source = self
            .stored_session(source_session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {source_session_id} not found"))?;
        if matches!(
            source.status.as_str(),
            "archiving" | "archived" | "deleting" | "deleted"
        ) {
            return Err(format!(
                "session {source_session_id} cannot branch from lifecycle state {}",
                source.status
            ));
        }
        let model = runtime.resolve_session_model(
            model
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .or(source.model.as_deref()),
        )?;
        let now = Utc::now();
        let runtime_services = runtime.runtime_services();
        let workspace_root = runtime_services.workspace_root();
        let mut metadata = serde_json::json!({
            "title": title,
            "branched_from": source_session_id,
            "owner_principal_id": owner_principal_id,
            "workspace_root": workspace_root.display().to_string(),
        });
        let mut inherited_focus = routing_focus_from_record(&source)?;
        if inherited_focus.task.is_some() || inherited_focus.mission.is_some() {
            inherited_focus.revision = 1;
            let inherited_at_ms = now.timestamp_millis().max(0) as u64;
            if let Some(task) = inherited_focus.task.as_mut() {
                task.revision = 1;
                task.actor = "session.branch".to_string();
                task.updated_at_ms = inherited_at_ms;
                task.inherited_from_session_id = Some(source_session_id.to_string());
            }
            if let Some(mission) = inherited_focus.mission.as_mut() {
                mission.revision = 1;
                mission.actor = "session.branch".to_string();
                mission.updated_at_ms = inherited_at_ms;
                mission.inherited_from_session_id = Some(source_session_id.to_string());
            }
            metadata[ROUTING_FOCUS_METADATA_KEY] = serde_json::to_value(inherited_focus)
                .map_err(|error| format!("serialize inherited Session routing focus: {error}"))?;
        }
        // P0: branch inherits the source execution policy (revision reset,
        // origin marked explicit) so a branch never silently falls back to
        // the global default.
        if let Some(source_policy) = source
            .metadata_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .and_then(|value| value.get("execution_policy").cloned())
            .and_then(|policy| policy.as_object().cloned())
        {
            let mut inherited_policy = source_policy;
            inherited_policy.insert("revision".to_string(), serde_json::json!(1));
            inherited_policy.insert("origin".to_string(), serde_json::json!("session_explicit"));
            metadata["execution_policy"] = serde_json::Value::Object(inherited_policy);
        }
        let target = SessionRecord {
            session_id: target_session_id.to_string(),
            platform: "webui".to_string(),
            chat_id: target_session_id.to_string(),
            user_id: source.user_id.clone(),
            model: Some(model.clone()),
            created_at: now.to_rfc3339(),
            last_activity: now.to_rfc3339(),
            message_count: 0,
            reset_policy: source.reset_policy.clone(),
            metadata_json: Some(metadata.to_string()),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let created_at_ms = now.timestamp_millis().max(0) as u64;
        let operation_replay;
        let source_message_count = match self
            .kernel()
            .session_branch_activation(&operation_id)
            .await
            .map_err(|error| error.to_string())?
        {
            Some(existing) => {
                operation_replay = true;
                if existing.source_session_id != source_session_id
                    || existing.target_session_id != target_session_id
                {
                    return Err(format!(
                        "branch operation {operation_id} is bound to another source or target"
                    ));
                }
                existing.source_message_count
            }
            None => {
                operation_replay = false;
                self.kernel()
                    .stored_message_count(source_session_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        format!("durable Session store is unavailable for {source_session_id}")
                    })?
            }
        };
        let branch_operation_id = operation_id.clone();
        let result = self
            .kernel()
            .branch_session_at_cutoff(&session::SessionBranchRequest {
                operation_id,
                source_session_id: source_session_id.to_string(),
                source_message_count,
                target: target.clone(),
                source_event_json: serde_json::json!({
                    "source_session_id": source_session_id,
                    "branch_session_id": target_session_id,
                    "status": "created",
                })
                .to_string(),
                target_event_json: serde_json::json!({
                    "source_session_id": source_session_id,
                    "branch_session_id": target_session_id,
                    "status": "created",
                })
                .to_string(),
                created_at_ms,
            })
            .await
            .map_err(|error| error.to_string())?;
        drop(second_guard);
        drop(first_guard);
        let mut session = self
            .activate_branch_receipt(result.activation.clone())
            .await?;
        session.created = !operation_replay;
        session.record = result.target;
        Ok(SessionBranchOutcome {
            session,
            operation_id: branch_operation_id,
            replayed: operation_replay,
            copied_message_count: result.copied_message_count,
            source_message_count: result.source_message_count,
        })
    }

    async fn activate_branch_receipt(
        &self,
        mut receipt: session::SessionBranchActivation,
    ) -> Result<EnsureSessionOutcome, String> {
        if receipt.phase == session::SessionBranchActivationPhase::Activated {
            let target = self
                .stored_session(&receipt.target_session_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "branch operation {} lost target {}",
                        receipt.operation_id, receipt.target_session_id
                    )
                })?;
            let model = self
                .runtime()?
                .resolve_session_model(target.model.as_deref())?;
            return Ok(EnsureSessionOutcome {
                session_id: target.session_id.clone(),
                model,
                created: false,
                restored: self.has_active_session(&target.session_id),
                record: target,
            });
        }
        if matches!(
            receipt.phase,
            session::SessionBranchActivationPhase::BranchCommitted
                | session::SessionBranchActivationPhase::Failed
        ) {
            let expected_phase = receipt.phase;
            receipt = self
                .kernel()
                .transition_session_branch_activation(&session::SessionBranchActivationTransition {
                    operation_id: receipt.operation_id.clone(),
                    expected_revision: receipt.revision,
                    expected_phase,
                    next_phase: session::SessionBranchActivationPhase::ActivationPending,
                    updated_at_ms: now_ms(),
                    error: None,
                })
                .await
                .map_err(|error| error.to_string())?;
        }
        let target = self
            .stored_session(&receipt.target_session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                format!(
                    "branch operation {} lost target {}",
                    receipt.operation_id, receipt.target_session_id
                )
            })?;
        let metadata = target
            .metadata_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let mut activation = EnsureSessionRequest::new(
            &target.session_id,
            target.model.clone(),
            SessionSource::Internal,
        );
        activation.owner_principal_id = metadata
            .get("owner_principal_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        activation.title = Some(session_title(&target));
        activation.metadata = metadata;
        match self
            .coordinator()?
            .activate(activation, SessionActivationIntent::ExistingOnly)
            .await
        {
            Ok(outcome) => {
                self.kernel()
                    .transition_session_branch_activation(
                        &session::SessionBranchActivationTransition {
                            operation_id: receipt.operation_id,
                            expected_revision: receipt.revision,
                            expected_phase:
                                session::SessionBranchActivationPhase::ActivationPending,
                            next_phase: session::SessionBranchActivationPhase::Activated,
                            updated_at_ms: now_ms(),
                            error: None,
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = self
                    .kernel()
                    .transition_session_branch_activation(
                        &session::SessionBranchActivationTransition {
                            operation_id: receipt.operation_id,
                            expected_revision: receipt.revision,
                            expected_phase:
                                session::SessionBranchActivationPhase::ActivationPending,
                            next_phase: session::SessionBranchActivationPhase::Failed,
                            updated_at_ms: now_ms(),
                            error: Some(error.clone()),
                        },
                    )
                    .await;
                Err(error)
            }
        }
    }

    pub(crate) async fn list_pending_branch_activations(
        &self,
        limit: usize,
    ) -> Result<Vec<session::SessionBranchActivation>, String> {
        self.kernel()
            .recoverable_session_branch_activations(limit)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn reconcile_branch_once(&self, operation_id: &str) -> Result<bool, String> {
        let Some(receipt) = self
            .kernel()
            .session_branch_activation(operation_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        self.activate_branch_receipt(receipt).await?;
        Ok(true)
    }

    pub(crate) async fn archive_session(&self, session_id: &str) -> Result<bool, String> {
        self.close_session(session_id, session::SessionCloseDisposition::Archive)
            .await
    }

    pub(crate) async fn delete_session(&self, session_id: &str) -> Result<bool, String> {
        self.close_session(session_id, session::SessionCloseDisposition::Delete)
            .await
    }

    async fn close_session(
        &self,
        session_id: &str,
        disposition: session::SessionCloseDisposition,
    ) -> Result<bool, String> {
        let coordinator = self.coordinator()?;
        let _guard = coordinator.acquire_exclusive(session_id).await;
        let Some(record) = self
            .kernel()
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        match record.status.as_str() {
            "deleted" if disposition == session::SessionCloseDisposition::Delete => {
                return Ok(true);
            }
            "deleted" => {
                return Err(format!(
                    "session {session_id} is deleted and cannot transition to archived"
                ));
            }
            "archived" if disposition == session::SessionCloseDisposition::Archive => {
                return Ok(true);
            }
            "archived" => {
                return Err(format!(
                    "session {session_id} is archived; archive and delete are distinct terminal operations"
                ));
            }
            "deleting" if disposition == session::SessionCloseDisposition::Archive => {
                return Err(format!(
                    "session {session_id} is already transitioning to deleted"
                ));
            }
            "archiving" if disposition == session::SessionCloseDisposition::Delete => {
                return Err(format!(
                    "session {session_id} is already transitioning to archived"
                ));
            }
            _ => {}
        }

        let operation_id = format!("session-lifecycle:{}:{session_id}", disposition.as_str());
        let intent = match self
            .kernel()
            .session_lifecycle_intent(&operation_id)
            .await
            .map_err(|error| error.to_string())?
        {
            Some(intent) => intent,
            None => {
                let admission = self
                    .kernel()
                    .session_input_admission(session_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        format!("session {session_id} has no durable input admission")
                    })?;
                self.kernel()
                    .plan_session_lifecycle(&session::SessionLifecyclePlan {
                        operation_id,
                        session_id: session_id.to_string(),
                        disposition,
                        expected_generation: admission.generation,
                        created_at_ms: now_ms(),
                    })
                    .await
                    .map_err(|error| error.to_string())?
            }
        };
        self.reconcile_lifecycle_under_guard(intent).await?;
        Ok(true)
    }

    async fn reconcile_lifecycle_under_guard(
        &self,
        mut intent: session::SessionLifecycleIntent,
    ) -> Result<(), String> {
        loop {
            intent = match intent.phase {
                session::SessionLifecyclePhase::Unloaded => return Ok(()),
                session::SessionLifecyclePhase::Failed => self
                    .kernel()
                    .transition_session_lifecycle(&session::SessionLifecycleTransition {
                        operation_id: intent.operation_id.clone(),
                        expected_revision: intent.revision,
                        expected_phase: session::SessionLifecyclePhase::Failed,
                        next_phase: intent.last_stable_phase,
                        updated_at_ms: now_ms(),
                        error: None,
                    })
                    .await
                    .map_err(|error| error.to_string())?,
                session::SessionLifecyclePhase::Planned => {
                    let at_ms = now_ms();
                    self.kernel()
                        .fence_session_lifecycle(&session::SessionLifecycleFenceRequest {
                            transition: session::SessionLifecycleTransition {
                                operation_id: intent.operation_id.clone(),
                                expected_revision: intent.revision,
                                expected_phase: session::SessionLifecyclePhase::Planned,
                                next_phase: session::SessionLifecyclePhase::AdmissionFenced,
                                updated_at_ms: at_ms,
                                error: None,
                            },
                            actor: "gateway-session-service".to_string(),
                            reason: close_fence_reason(intent.disposition).to_string(),
                            transitional_status: close_transitional_status(intent.disposition)
                                .to_string(),
                            event: lifecycle_event(
                                &intent.session_id,
                                close_started_event(intent.disposition),
                                serde_json::json!({
                                    "operation_id": intent.operation_id,
                                    "status": close_transitional_status(intent.disposition),
                                    "generation": intent.expected_generation.saturating_add(1),
                                }),
                                at_ms,
                            )?,
                        })
                        .await
                        .map_err(|error| error.to_string())?
                }
                session::SessionLifecyclePhase::AdmissionFenced => {
                    if let Err(error) = self
                        .cancel_and_drain(
                            &intent.session_id,
                            close_fence_reason(intent.disposition),
                        )
                        .await
                    {
                        self.fail_lifecycle_intent(&intent, &error).await;
                        return Err(error);
                    }
                    self.kernel()
                        .transition_session_lifecycle(&session::SessionLifecycleTransition {
                            operation_id: intent.operation_id.clone(),
                            expected_revision: intent.revision,
                            expected_phase: session::SessionLifecyclePhase::AdmissionFenced,
                            next_phase: session::SessionLifecyclePhase::RuntimeDrained,
                            updated_at_ms: now_ms(),
                            error: None,
                        })
                        .await
                        .map_err(|error| error.to_string())?
                }
                session::SessionLifecyclePhase::RuntimeDrained => {
                    let mut record = self
                        .kernel()
                        .stored_session(&intent.session_id)
                        .await
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            format!(
                                "lifecycle operation {} lost Session {}",
                                intent.operation_id, intent.session_id
                            )
                        })?;
                    let closed_at = Utc::now();
                    let closed_at_ms = closed_at.timestamp_millis().max(0) as u64;
                    let mut metadata = record
                        .metadata_json
                        .as_deref()
                        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                        .and_then(|value| value.as_object().cloned())
                        .unwrap_or_default();
                    metadata.insert(
                        close_metadata_key(intent.disposition).to_string(),
                        serde_json::Value::String(closed_at.to_rfc3339()),
                    );
                    metadata.insert(
                        "tombstone".to_string(),
                        serde_json::json!({
                            "kind": close_terminal_status(intent.disposition),
                            "closed_at": closed_at.to_rfc3339(),
                            "physical_delete": false,
                            "operation_id": intent.operation_id,
                        }),
                    );
                    record.status = close_terminal_status(intent.disposition).to_string();
                    record.last_activity = closed_at.to_rfc3339();
                    record.metadata_json = Some(serde_json::Value::Object(metadata).to_string());
                    self.kernel()
                        .commit_session_lifecycle_tombstone(
                            &session::SessionLifecycleTombstoneRequest {
                                transition: session::SessionLifecycleTransition {
                                    operation_id: intent.operation_id.clone(),
                                    expected_revision: intent.revision,
                                    expected_phase: session::SessionLifecyclePhase::RuntimeDrained,
                                    next_phase: session::SessionLifecyclePhase::TombstoneCommitted,
                                    updated_at_ms: closed_at_ms,
                                    error: None,
                                },
                                record,
                                event: lifecycle_event(
                                    &intent.session_id,
                                    close_completed_event(intent.disposition),
                                    serde_json::json!({
                                        "operation_id": intent.operation_id,
                                        "status": close_terminal_status(intent.disposition),
                                        "physical_delete": false,
                                    }),
                                    closed_at_ms,
                                )?,
                            },
                        )
                        .await
                        .map_err(|error| error.to_string())?
                }
                session::SessionLifecyclePhase::TombstoneCommitted => {
                    self.coordinator()?
                        .unload_runtime_under_guard(&intent.session_id)
                        .await;
                    self.kernel()
                        .transition_session_lifecycle(&session::SessionLifecycleTransition {
                            operation_id: intent.operation_id.clone(),
                            expected_revision: intent.revision,
                            expected_phase: session::SessionLifecyclePhase::TombstoneCommitted,
                            next_phase: session::SessionLifecyclePhase::Unloaded,
                            updated_at_ms: now_ms(),
                            error: None,
                        })
                        .await
                        .map_err(|error| error.to_string())?
                }
            };
        }
    }

    async fn fail_lifecycle_intent(&self, intent: &session::SessionLifecycleIntent, error: &str) {
        if intent.phase.is_stable() && !intent.phase.is_terminal() {
            let _ = self
                .kernel()
                .transition_session_lifecycle(&session::SessionLifecycleTransition {
                    operation_id: intent.operation_id.clone(),
                    expected_revision: intent.revision,
                    expected_phase: intent.phase,
                    next_phase: session::SessionLifecyclePhase::Failed,
                    updated_at_ms: now_ms(),
                    error: Some(error.to_string()),
                })
                .await;
        }
    }

    pub(crate) async fn list_pending_lifecycle_operations(
        &self,
        limit: usize,
    ) -> Result<Vec<session::SessionLifecycleIntent>, String> {
        self.kernel()
            .recoverable_session_lifecycle_intents(limit)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn reconcile_lifecycle_once(
        &self,
        operation_id: &str,
    ) -> Result<bool, String> {
        let Some(intent) = self
            .kernel()
            .session_lifecycle_intent(operation_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        let _guard = self
            .coordinator()?
            .acquire_exclusive(&intent.session_id)
            .await;
        self.reconcile_lifecycle_under_guard(intent).await?;
        Ok(true)
    }

    async fn cancel_and_drain(&self, session_id: &str, reason: &str) -> Result<(), String> {
        const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        let runtime = self.runtime()?;
        runtime.cancel_active_session(session_id, reason);
        let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
            loop {
                let active =
                    runtime
                        .running_session_execution_indices()
                        .into_iter()
                        .any(|execution| {
                            execution.session_id == session_id
                                && !execution.active_execution_ids.is_empty()
                        });
                let inputs = self
                    .kernel()
                    .runtime_inputs(session_id, 500)
                    .await
                    .map_err(|error| error.to_string())?;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                // D7 rule 2: the delete fence force-cancels claimed/running/
                // attached durable inputs with a generation-scoped CAS so a
                // vanished worker cannot keep the Session alive forever.
                for input in &inputs {
                    if Self::runtime_input_blocks_drain(&input.status) {
                        if let Err(error) = self
                            .kernel()
                            .cancel_runtime_input(
                                &input.input_id,
                                input.session_generation,
                                input.revision,
                                "session-lifecycle:delete",
                                reason,
                                now_ms,
                            )
                            .await
                        {
                            tracing::warn!(
                                input_id = %input.input_id,
                                %error,
                                "delete fence input cancel CAS rejected; retrying"
                            );
                        }
                    }
                }
                let durable_active = inputs
                    .into_iter()
                    .any(|input| Self::runtime_input_blocks_drain(&input.status));
                let terminal_pending = runtime
                    .runtime_services()
                    .session_terminal_delivery()
                    .has_unsettled_for_session(session_id)
                    .map_err(|error| {
                        format!("inspect Runtime terminal drain for session {session_id}: {error}")
                    })?;
                if Self::session_runtime_is_drained(active, durable_active, terminal_pending) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Ok::<(), String>(())
        })
        .await;
        match drained {
            Ok(result) => result?,
            Err(_) => {
                return Err(format!(
                    "session {session_id} remained active after {}ms drain timeout",
                    DRAIN_TIMEOUT.as_millis()
                ));
            }
        }
        Ok(())
    }

    fn session_runtime_is_drained(
        active_execution: bool,
        active_input: bool,
        unsettled_terminal: bool,
    ) -> bool {
        !active_execution && !active_input && !unsettled_terminal
    }

    /// D7: a terminal status is drained even when `terminal_at_ms` is missing
    /// (legacy/recovered records). Only Claimed/Running and non-terminal
    /// statuses keep the Session alive.
    fn runtime_input_blocks_drain(status: &SessionRuntimeInputStatus) -> bool {
        matches!(
            status,
            SessionRuntimeInputStatus::Claimed | SessionRuntimeInputStatus::Running
        ) || !status.is_terminal()
    }

    pub(crate) async fn unload_runtime(&self, session_id: &str) -> Result<bool, String> {
        Ok(self.coordinator()?.unload_runtime(session_id).await)
    }

    pub(crate) async fn recover_active_sessions(&self) -> Result<SessionRecoverySummary, String> {
        Ok(self.coordinator()?.recover_active_sessions().await)
    }

    pub(crate) async fn recover_required_sessions(&self) -> Result<SessionRecoverySummary, String> {
        Ok(self.coordinator()?.recover_required_sessions().await)
    }

    pub(crate) async fn run_resource_cleanup(&self) -> Result<usize, String> {
        Ok(self.coordinator()?.run_resource_cleanup().await)
    }

    pub(crate) async fn working_set_projection(
        &self,
    ) -> Result<SessionWorkingSetProjection, String> {
        match self.coordinator.as_ref() {
            Some(coordinator) => Ok(coordinator.working_set_projection().await),
            None if cfg!(test) => Ok(SessionWorkingSetProjection::default()),
            None => Err("Session activation coordinator is unavailable".to_string()),
        }
    }

    pub(crate) fn has_active_session(&self, session_id: &str) -> bool {
        self.runtime
            .as_ref()
            .is_some_and(|runtime| runtime.has_active_session(session_id))
    }

    pub(crate) fn cancel_active_turns(
        &self,
        session_id: &str,
        reason: &str,
    ) -> Result<Vec<String>, String> {
        Ok(self.runtime()?.cancel_active_session(session_id, reason))
    }

    pub(crate) fn cancel_active_execution(
        &self,
        session_id: &str,
        turn_id: &str,
        execution_id: &str,
        reason: &str,
    ) -> Result<bool, String> {
        Ok(self
            .runtime()?
            .cancel_active_execution(session_id, turn_id, execution_id, reason))
    }

    pub(crate) async fn admit_input(
        &self,
        mut envelope: SessionInputEnvelope,
    ) -> Result<crate::runtime_service::SessionInputAdmission, String> {
        self.ensure_accepting()?;
        let runtime = self.runtime()?;
        let admission = self
            .kernel()
            .session_input_admission(&envelope.session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {} does not exist", envelope.session_id))?;
        if !admission.open {
            return Err(format!(
                "session {} no longer accepts input at generation {}",
                envelope.session_id, admission.generation
            ));
        }
        if envelope.task_route_hint.is_none() {
            let record = self
                .stored_session(&envelope.session_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("session {} does not exist", envelope.session_id))?;
            envelope.task_route_hint = self.validated_routing_focus(&record).await?.route_hint();
        }

        let runtime_state = runtime.session_input_runtime_state(&envelope.session_id);
        let (decision, reason) = runtime::classify_session_input(&envelope, &runtime_state);
        let relation_proposal = runtime::input_classifier::propose_input_relation(&envelope);
        let target_turn_id = match decision {
            InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval => runtime_state
                .active_turn_id
                .as_ref()
                .map(ToString::to_string),
            _ => None,
        };
        let classification = DurableInputClassification {
            reason,
            relation_proposal: relation_proposal.clone(),
            source_kind: envelope.source_kind,
            payload_kind: envelope.payload_kind,
            content_preview: envelope.content_preview.clone(),
            source_ref: envelope.source_ref.clone(),
            source_message_id: envelope.source_message_id.clone(),
            idempotency_key: envelope.idempotency_key.clone(),
            metadata: envelope.metadata.clone(),
            created_at: envelope.created_at,
        };
        let turn_id = input_execution_turn_id(&envelope);
        let request = SessionRuntimeOutboxRequest {
            input_id: envelope.input_id.to_string(),
            request_id: envelope.idempotency_key.clone(),
            turn_id,
            message_id: envelope
                .source_message_id
                .clone()
                .unwrap_or_else(|| envelope.input_id.to_string()),
            session_generation: admission.generation,
            decision,
            target_turn_id,
            classification_json: Some(
                serde_json::to_string(&classification).map_err(|error| error.to_string())?,
            ),
            task_route_hint: envelope.task_route_hint.clone(),
            created_at_ms: envelope.created_at.timestamp_millis().max(0) as u64,
            runtime_options_json: envelope
                .metadata
                .get("runtime_options")
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| error.to_string())?,
        };
        let content_json = serde_json::to_string(&serde_json::json!([{
            "type": "text",
            "text": envelope.content,
            "cowd_turn_id": request.turn_id,
            "cowd_turn_ingress_message_id": request.message_id,
        }]))
        .map_err(|error| error.to_string())?;
        let mut record = self
            .kernel()
            .append_runtime_ingress(
                &envelope.session_id,
                "user",
                Some(&content_json),
                request.created_at_ms,
                &request,
            )
            .await
            .map_err(|error| error.to_string())?;
        runtime.notify_session_input_scheduler();

        if runtime.has_active_session(&record.session_id) {
            let receipt = receipt_from_durable_input(&record);
            runtime
                .project_durable_session_input(envelope.clone(), receipt.clone())
                .await
                .map_err(|error| error.message())?;
            if record.decision == InputRoutingDecision::SupplementCurrentTurn {
                if let Some(target_turn_id) = record.target_turn_id.as_deref() {
                    match self
                        .kernel()
                        .attach_runtime_input(
                            &record.input_id,
                            record.session_generation,
                            record.revision,
                            target_turn_id,
                            "gateway-session-service",
                            "direct delivery to active Runtime turn",
                            now_ms(),
                        )
                        .await
                    {
                        Ok(attached) => {
                            record = attached;
                            runtime.project_durable_session_receipt(
                                &record.session_id,
                                receipt_from_durable_input(&record),
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                input_id = %record.input_id,
                                session_id = %record.session_id,
                                %error,
                                "direct Session input delivery succeeded but its Attached projection was fenced"
                            );
                            // A terminal commit may have won the same revision
                            // race and already proved application. Re-read the
                            // canonical row instead of returning stale queued
                            // state to the Surface.
                            if let Ok(Some(current)) =
                                self.kernel().runtime_input(&record.request_id).await
                            {
                                record = current;
                                runtime.project_durable_session_receipt(
                                    &record.session_id,
                                    receipt_from_durable_input(&record),
                                );
                            }
                        }
                    }
                }
            }
        }
        let receipt = receipt_from_durable_input(&record);
        let (execution_graph_id, projection_turn_id, _supplemental) = runtime
            .publish_user_message_committed(&record, &envelope.content)
            .await;
        let materialized =
            runtime.responsive_input_projection(&record.session_id, relation_proposal.as_ref());
        Ok(crate::runtime_service::SessionInputAdmission {
            execution_graph_id,
            receipt,
            materialized,
            terminal_id: format!("turn-terminal:{}", record.request_id),
            turn_id: projection_turn_id,
            message_id: record.message_id,
            message_sequence: record.sequence,
        })
    }

    pub(crate) async fn input_projection(
        &self,
        session_id: &str,
    ) -> Result<SessionInputProjection, String> {
        let records = self
            .kernel()
            .runtime_inputs(session_id, 500)
            .await
            .map_err(|error| error.to_string())?;
        let active_turn_id = self
            .runtime()?
            .session_input_runtime_state(session_id)
            .active_turn_id;
        Ok(projection_from_durable_inputs(
            session_id,
            active_turn_id,
            records,
        ))
    }

    pub(crate) async fn turn_inbox(
        &self,
        session_id: &str,
        turn_id: Option<TurnId>,
    ) -> Result<TurnInboxSnapshot, String> {
        let records = self
            .kernel()
            .runtime_inputs(session_id, 500)
            .await
            .map_err(|error| error.to_string())?;
        let selected_turn_id = turn_id.or_else(|| {
            self.runtime().ok().and_then(|runtime| {
                runtime
                    .session_input_runtime_state(session_id)
                    .active_turn_id
            })
        });
        let mut items = records
            .into_iter()
            .filter(|record| {
                selected_turn_id.as_ref().is_none_or(|turn_id| {
                    record.target_turn_id.as_deref() == Some(turn_id.as_str())
                        || record.turn_id == turn_id.as_str()
                })
            })
            .map(|record| inbox_item_from_durable_input(&record))
            .collect::<Vec<_>>();
        items.sort_by_key(|item| item.created_at);
        let consumed_count = items
            .iter()
            .filter(|item| item.status == SessionInputStatus::Consumed)
            .count();
        let admitted_cursor = items.iter().filter_map(|item| item.cursor).max();
        let consumed_cursor = items
            .iter()
            .filter(|item| item.consumed_at.is_some())
            .filter_map(|item| item.cursor)
            .max();
        Ok(TurnInboxSnapshot {
            session_id: session_id.to_string(),
            turn_id: selected_turn_id,
            pending_count: items.len().saturating_sub(consumed_count),
            consumed_count,
            admitted_cursor,
            consumed_cursor,
            items,
            updated_at: Utc::now(),
        })
    }

    pub(crate) async fn cancel_input(
        &self,
        session_id: &str,
        input_id: SessionInputId,
        reason: &str,
    ) -> Result<SessionInputReceipt, String> {
        let current = self
            .kernel()
            .runtime_input_by_input_id(input_id.as_str())
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session input {} not found", input_id))?;
        if current.session_id != session_id {
            return Err(format!(
                "session input {} does not belong to session {session_id}",
                input_id
            ));
        }
        let cancelled = self
            .kernel()
            .cancel_runtime_input(
                input_id.as_str(),
                current.session_generation,
                current.revision,
                "gateway-session-service",
                reason,
                now_ms(),
            )
            .await
            .map_err(|error| error.to_string())?;
        if current.status.holds_claim() {
            self.runtime()?
                .cancel_active_session(session_id, "durable Session input cancelled");
        }
        let receipt = receipt_from_durable_input(&cancelled);
        self.runtime()?
            .project_durable_session_receipt(session_id, receipt.clone());
        Ok(receipt)
    }

    pub(crate) async fn reclassify_input(
        &self,
        session_id: &str,
        input_id: SessionInputId,
        decision: InputRoutingDecision,
        reason: &str,
    ) -> Result<SessionInputReceipt, String> {
        let current = self
            .kernel()
            .runtime_input_by_input_id(input_id.as_str())
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session input {} not found", input_id))?;
        if current.session_id != session_id {
            return Err(format!(
                "session input {} does not belong to session {session_id}",
                input_id
            ));
        }
        let target_turn_id = match decision {
            InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval => self
                .runtime()?
                .session_input_runtime_state(session_id)
                .active_turn_id
                .as_ref()
                .map(ToString::to_string),
            _ => None,
        };
        let updated = self
            .kernel()
            .reclassify_runtime_input(
                input_id.as_str(),
                current.session_generation,
                current.revision,
                decision,
                target_turn_id.as_deref(),
                current.classification_json.as_deref(),
                "gateway-session-service",
                reason,
                now_ms(),
            )
            .await
            .map_err(|error| error.to_string())?;
        let receipt = receipt_from_durable_input(&updated);
        if let Ok(runtime) = self.runtime() {
            runtime.notify_session_input_scheduler();
            runtime.project_durable_session_receipt(session_id, receipt.clone());
        }
        Ok(receipt)
    }

    pub(crate) async fn runtime_input_by_input_id(
        &self,
        input_id: &str,
    ) -> Result<Option<session::SessionRuntimeOutboxRecord>, session::SessionError> {
        self.kernel().runtime_input_by_input_id(input_id).await
    }

    pub(crate) async fn commit_input_application_receipt(
        &self,
        input_ids: &[String],
        expected_revisions: &[u64],
        receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
        now_ms: u64,
    ) -> Result<Vec<session::SessionRuntimeOutboxRecord>, session::SessionError> {
        self.kernel()
            .commit_input_application_receipt(input_ids, expected_revisions, receipt, now_ms)
            .await
    }

    pub(crate) fn event_bus(&self) -> Arc<crate::event_bus::SessionProjectionHub> {
        self.kernel().event_bus()
    }

    pub(crate) fn has_unified_store(&self) -> bool {
        self.kernel().has_unified_store()
    }

    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.kernel().list_active_session_ids()
    }

    pub(crate) async fn session_exists(&self, session_id: &str) -> Result<bool, SessionError> {
        if self
            .list_active_session_ids()
            .iter()
            .any(|id| id == session_id)
        {
            return Ok(true);
        }
        self.stored_session(session_id)
            .await
            .map(|record| record.is_some())
    }

    pub(crate) async fn attach_session_value(
        &self,
        session_id: &str,
        actor_id: &str,
        surface: &str,
        role: Option<&str>,
    ) -> serde_json::Value {
        let mut actor = SessionActor::new(actor_id, surface);
        actor.role = role.map(ToOwned::to_owned);
        match self.presence_ledger().attach(session_id, actor).await {
            Ok(event) => {
                let snapshot = self.presence_ledger().snapshot(session_id).await;
                serde_json::json!({
                    "ok": true,
                    "session_id": session_id,
                    "presence_ttl_ms": self.presence_ledger().presence_ttl_ms(),
                    "event": event,
                    "snapshot": snapshot,
                })
            }
            Err(error) => serde_json::json!({
                "ok": false,
                "error": error,
            }),
        }
    }

    pub(crate) async fn detach_session_value(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> serde_json::Value {
        match self.presence_ledger().detach(session_id, actor_id).await {
            Ok(event) => {
                let snapshot = self.presence_ledger().snapshot(session_id).await;
                serde_json::json!({
                    "ok": true,
                    "session_id": session_id,
                    "event": event,
                    "snapshot": snapshot,
                })
            }
            Err(error) => serde_json::json!({
                "ok": false,
                "error": error,
            }),
        }
    }

    pub(crate) async fn lifecycle_snapshot_value(
        &self,
        session_id: Option<&str>,
    ) -> serde_json::Value {
        match session_id {
            Some(session_id) => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "snapshot": self.presence_ledger().snapshot(session_id).await,
            }),
            None => serde_json::json!({
                "ok": true,
                "sessions": self.presence_ledger().snapshots().await,
            }),
        }
    }

    pub(crate) async fn lifecycle_attachment_role(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> Option<String> {
        self.presence_ledger()
            .attachment_role(session_id, actor_id)
            .await
    }

    pub(crate) async fn principal_has_lifecycle_attachment(
        &self,
        session_id: &str,
        principal_actor_id: &str,
    ) -> bool {
        let actor_prefix = format!("{principal_actor_id}:surface:");
        self.presence_ledger()
            .snapshot(session_id)
            .await
            .is_some_and(|snapshot| {
                snapshot
                    .attachments
                    .iter()
                    .any(|attachment| attachment.actor.id.starts_with(&actor_prefix))
            })
    }

    pub(crate) async fn replay_session_value(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> serde_json::Value {
        if session_id.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "session_id is required",
            });
        }
        let capped_limit = limit.clamp(1, 500);
        match self
            .stored_events_page(session_id, from_sequence, capped_limit)
            .await
        {
            Ok(Some((total, events))) => {
                let next_sequence = events
                    .last()
                    .map(|event| event.sequence + 1)
                    .unwrap_or(from_sequence);
                let projected_events: Vec<_> = events
                    .into_iter()
                    .map(|event| {
                        serde_json::json!({
                            "session_id": event.session_id,
                            "event_type": event.event_type,
                            "event_json": event.event_json,
                            "sequence": event.sequence,
                            "created_at_ms": event.created_at_ms,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "session_id": session_id,
                    "from_sequence": from_sequence,
                    "limit": capped_limit,
                    "total": total,
                    "next_sequence": next_sequence,
                    "events": projected_events,
                })
            }
            Ok(None) => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "from_sequence": from_sequence,
                "limit": capped_limit,
                "total": 0,
                "next_sequence": from_sequence,
                "events": [],
                "degraded": "unified session store unavailable",
            }),
            Err(error) => serde_json::json!({
                "ok": false,
                "error": error.to_string(),
            }),
        }
    }

    pub(crate) async fn list_stored_sessions_page(
        &self,
        options: &SessionListOptions<'_>,
    ) -> Result<Option<SessionListPage>, SessionError> {
        self.kernel().list_stored_sessions_page(options).await
    }

    pub(crate) async fn session_usage_summary(
        &self,
        recent_limit: usize,
    ) -> Result<Option<SessionUsageSummary>, SessionError> {
        self.kernel().session_usage_summary(recent_limit).await
    }

    pub(crate) async fn list_stored_sessions(
        &self,
    ) -> Result<Option<Vec<SessionRecord>>, SessionError> {
        self.kernel().list_stored_sessions().await
    }

    pub(crate) async fn search_stored_messages_visible(
        &self,
        query: &str,
        owner_principal_id: Option<&str>,
        visible_session_ids: &[String],
        unrestricted: bool,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        self.kernel()
            .search_stored_messages_visible(
                query,
                owner_principal_id,
                visible_session_ids,
                unrestricted,
                limit,
            )
            .await
    }

    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, SessionError> {
        self.kernel().stored_session(session_id).await
    }

    pub(crate) async fn has_domain_event_kind(
        &self,
        kind: &str,
    ) -> Result<Option<bool>, SessionError> {
        self.kernel().has_domain_event_kind(kind).await
    }

    pub(crate) async fn has_session_with_domain_event_kinds(
        &self,
        kinds: &[String],
    ) -> Result<Option<bool>, SessionError> {
        self.kernel()
            .has_session_with_domain_event_kinds(kinds)
            .await
    }

    pub(crate) async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>, SessionError> {
        self.kernel().runtime_input(request_id).await
    }

    pub(crate) async fn session_input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<session::SessionInputAdmission>, SessionError> {
        self.kernel().session_input_admission(session_id).await
    }

    pub(crate) async fn append_runtime_ingress(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.ensure_accepting()
            .map_err(SessionError::InvalidArgument)?;
        self.kernel()
            .append_runtime_ingress(session_id, role, content_json, created_at_ms, request)
            .await
    }

    async fn append_runtime_domain_event(
        &self,
        event: &session::SessionDomainEvent,
    ) -> Result<SessionEvent, SessionError> {
        let coordinator = self.coordinator().map_err(SessionError::InvalidArgument)?;
        let _guard = coordinator.acquire_exclusive(&event.session_id).await;
        let record = self
            .kernel()
            .stored_session(&event.session_id)
            .await?
            .ok_or_else(|| {
                SessionError::Store(format!("session `{}` not found", event.session_id))
            })?;
        if matches!(
            record.status.as_str(),
            "archiving" | "archived" | "deleting" | "deleted"
        ) {
            return Err(SessionError::StaleExecutionFence(format!(
                "session `{}` rejects Runtime events in lifecycle state {}",
                event.session_id, record.status
            )));
        }
        self.kernel().append_runtime_domain_event(event).await
    }

    async fn append_runtime_domain_event_if_absent(
        &self,
        event: &session::SessionDomainEvent,
    ) -> Result<(SessionEvent, bool), SessionError> {
        let coordinator = self.coordinator().map_err(SessionError::InvalidArgument)?;
        let _guard = coordinator.acquire_exclusive(&event.session_id).await;
        let record = self
            .kernel()
            .stored_session(&event.session_id)
            .await?
            .ok_or_else(|| {
                SessionError::Store(format!("session `{}` not found", event.session_id))
            })?;
        if matches!(
            record.status.as_str(),
            "archiving" | "archived" | "deleting" | "deleted"
        ) {
            return Err(SessionError::StaleExecutionFence(format!(
                "session `{}` rejects Runtime events in lifecycle state {}",
                event.session_id, record.status
            )));
        }
        self.kernel()
            .append_runtime_domain_event_if_absent(event)
            .await
    }

    pub(crate) async fn append_control_domain_event_if_absent(
        &self,
        event: &session::SessionDomainEvent,
    ) -> Result<bool, SessionError> {
        self.append_runtime_domain_event_if_absent(event)
            .await
            .map(|(_, replayed)| !replayed)
    }

    pub(crate) async fn append_runtime_journal_event(
        &self,
        event: &runtime::RuntimeSessionEvent,
    ) -> Result<SessionEvent, SessionError> {
        let event = runtime_domain_event(event);
        self.append_runtime_domain_event(&event).await
    }

    pub(crate) async fn append_session_input_journal(
        &self,
        session_id: &str,
        kind: crate::session_runtime_data_port::SessionInputJournalKind,
        payload: serde_json::Value,
        occurred_at_ms: u64,
        event_id: &str,
    ) -> Result<SessionEvent, SessionError> {
        let mut event = session::SessionDomainEvent::new(
            session_id.to_string(),
            0,
            session::SessionDomainScope::Turn,
            kind.as_str(),
            payload,
            occurred_at_ms,
        );
        event.event_id = event_id.to_string();
        let (stored, _replayed) = self.append_runtime_domain_event_if_absent(&event).await?;
        Ok(stored)
    }

    pub(crate) async fn append_runtime_context_envelope_if_absent(
        &self,
        record: &runtime::RuntimeContextEnvelopeRecord,
    ) -> Result<Option<SessionEvent>, SessionError> {
        let event = SessionEvent {
            session_id: record.session_id.clone(),
            event_type: "ContextEnvelope".to_string(),
            event_json: record.payload.to_string(),
            sequence: 0,
            created_at_ms: record.created_at_ms,
        };
        self.kernel()
            .append_runtime_context_envelope_if_absent(&event)
            .await
    }

    pub(crate) async fn append_runtime_compaction_bundle_if_absent(
        &self,
        events: &[runtime::RuntimeSessionEvent],
        checkpoint_id: &str,
    ) -> Result<bool, SessionError> {
        let session_id = events.first().map(|event| event.session_id.clone());
        let events = events.iter().map(runtime_domain_event).collect::<Vec<_>>();
        let inserted = self
            .kernel()
            .append_runtime_compaction_bundle_if_absent(&events, checkpoint_id)
            .await?;
        if inserted {
            if let Some(session_id) = session_id {
                self.schedule_context_index_reconciliation(&session_id);
            }
        }
        Ok(inserted)
    }

    pub(crate) async fn presence_snapshots(&self) -> Vec<session::SessionLifecycleSnapshot> {
        self.presence_ledger().snapshots().await
    }

    pub(crate) fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        self.kernel().history_reader()
    }

    pub(crate) fn schedule_context_index_reconciliation(&self, session_id: &str) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.schedule_context_index_reconciliation(session_id.to_string());
        }
    }

    pub(crate) async fn append_turn_journal_event(
        &self,
        session_id: &str,
        envelope: harness_contract::turn::TurnJournalEnvelope,
    ) -> Result<Option<usize>, SessionError> {
        self.kernel()
            .append_turn_journal_event(session_id, envelope)
            .await
    }

    async fn update_stored_session(&self, record: &SessionRecord) -> Result<bool, SessionError> {
        self.kernel().update_stored_session(record).await
    }

    pub(crate) async fn stored_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, SessionError> {
        self.kernel()
            .stored_events_page(session_id, from_sequence, limit)
            .await
    }

    pub(crate) async fn stored_events_by_type_page(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, SessionError> {
        self.kernel()
            .stored_events_by_type_page(session_id, event_type, from_sequence, limit)
            .await
    }

    pub(crate) async fn stored_domain_events_by_kind_page(
        &self,
        session_id: &str,
        kind: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, SessionError> {
        self.kernel()
            .stored_domain_events_by_kind_page(session_id, kind, from_sequence, limit)
            .await
    }

    pub(crate) async fn search_stored_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        self.kernel()
            .search_stored_messages_in_sessions(query, session_ids, limit)
            .await
    }

    pub(crate) async fn stored_message_count(
        &self,
        session_id: &str,
    ) -> Result<Option<usize>, SessionError> {
        self.kernel().stored_message_count(session_id).await
    }

    pub(crate) async fn stored_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        self.kernel()
            .stored_messages(session_id, offset, limit)
            .await
    }

    pub(crate) async fn stored_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        self.kernel()
            .stored_messages_from_sequence(session_id, from_sequence, limit)
            .await
    }

    pub(crate) async fn runtime_inputs(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.kernel().runtime_inputs(session_id, limit).await
    }

    pub(crate) async fn runtime_inputs_for_turn_relation(
        &self,
        session_id: &str,
        session_generation: u64,
        turn_id: &str,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.kernel()
            .runtime_inputs_for_turn_relation(session_id, session_generation, turn_id)
            .await
    }

    pub(crate) async fn runtime_inputs_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.kernel()
            .runtime_inputs_for_sessions(session_ids, per_session_limit)
            .await
    }

    pub(crate) async fn active_runtime_inputs(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.kernel().active_runtime_inputs(limit).await
    }

    pub(crate) async fn runtime_outbox_health(
        &self,
    ) -> Result<SessionRuntimeOutboxHealth, SessionError> {
        self.kernel().runtime_outbox_health().await
    }

    pub(crate) async fn blocked_runtime_inputs(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.kernel().blocked_runtime_inputs(limit).await
    }

    pub(crate) async fn retry_blocked_runtime_input(
        &self,
        request_id: &str,
        expected_revision: Option<u64>,
        actor: &str,
        reason: &str,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        let current = self
            .kernel()
            .runtime_input(request_id)
            .await?
            .ok_or_else(|| {
                SessionError::InvalidArgument(format!(
                    "session runtime outbox item {request_id} was not found"
                ))
            })?;
        self.kernel()
            .retry_blocked_runtime_input(
                request_id,
                current.session_generation,
                expected_revision.unwrap_or(current.revision),
                actor,
                reason,
                now_ms(),
            )
            .await
    }

    pub(crate) async fn claim_ingress_work(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.ensure_accepting()
            .map_err(SessionError::InvalidArgument)?;
        self.kernel()
            .claim_ingress_work(worker_id, now_ms, lease_ms, limit)
            .await
    }

    pub(crate) async fn load_ingress_content(
        &self,
        record: &SessionRuntimeOutboxRecord,
    ) -> Result<String, SessionError> {
        let messages = self
            .kernel()
            .stored_messages_from_sequence(&record.session_id, record.sequence, 1)
            .await?
            .ok_or_else(|| {
                SessionError::Store("durable Session store is unavailable".to_string())
            })?;
        let message = messages.into_iter().next().ok_or_else(|| {
            SessionError::InvalidArgument(format!(
                "Session ingress message {} at sequence {} is missing",
                record.message_id, record.sequence
            ))
        })?;
        let blocks = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
            .map_err(|error| {
                SessionError::InvalidArgument(format!(
                    "Session ingress message {} is not a block array: {error}",
                    record.message_id
                ))
            })?;
        blocks
            .into_iter()
            .find_map(|block| {
                block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                SessionError::InvalidArgument(format!(
                    "Session ingress message {} has no text block",
                    record.message_id
                ))
            })
    }

    pub(crate) async fn durable_target_turn_is_terminal(
        &self,
        record: &SessionRuntimeOutboxRecord,
    ) -> Result<bool, SessionError> {
        let Some(target_turn_id) = record.target_turn_id.as_deref() else {
            return Ok(true);
        };
        Ok(self
            .kernel()
            .runtime_inputs_for_turn_relation(
                &record.session_id,
                record.session_generation,
                target_turn_id,
            )
            .await?
            .into_iter()
            .filter(|candidate| candidate.turn_id == target_turn_id)
            .max_by_key(|candidate| (candidate.sequence, candidate.updated_at_ms))
            .is_some_and(|target| target.status.is_terminal()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn requeue_ingress_work(
        &self,
        record: &SessionRuntimeOutboxRecord,
        worker_id: &str,
        claim_token: &str,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.kernel()
            .requeue_ingress_work(
                &record.request_id,
                worker_id,
                record.session_generation,
                claim_token,
                expected_revision,
                decision,
                target_turn_id,
                record.classification_json.as_deref(),
                reason,
                now_ms,
            )
            .await
    }

    pub(crate) async fn mark_ingress_running(
        &self,
        record: &SessionRuntimeOutboxRecord,
        worker_id: &str,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.kernel()
            .mark_ingress_running(
                &record.request_id,
                worker_id,
                record.session_generation,
                claim_token,
                expected_revision,
                now_ms,
            )
            .await
    }

    pub(crate) async fn renew_ingress_lease(
        &self,
        record: &SessionRuntimeOutboxRecord,
        worker_id: &str,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.kernel()
            .renew_ingress_lease(
                &record.request_id,
                worker_id,
                record.session_generation,
                claim_token,
                expected_revision,
                now_ms,
                lease_ms,
            )
            .await
    }

    pub(crate) async fn ingress_completed_at(
        &self,
        request_id: &str,
        runtime_commit_cursor: u64,
    ) -> Result<bool, SessionError> {
        Ok(self
            .kernel()
            .runtime_input(request_id)
            .await?
            .is_some_and(|current| {
                current.status == SessionRuntimeInputStatus::Completed
                    && current.runtime_commit_cursor == Some(runtime_commit_cursor)
            }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn complete_ingress_work(
        &self,
        record: &SessionRuntimeOutboxRecord,
        worker_id: &str,
        claim_token: &str,
        expected_revision: u64,
        terminal_status: SessionRuntimeInputStatus,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        let updated = self
            .kernel()
            .complete_ingress_work(
                &record.request_id,
                worker_id,
                record.session_generation,
                claim_token,
                expected_revision,
                terminal_status,
                runtime_commit_cursor,
                now_ms,
            )
            .await?;
        self.project_runtime_input_state(&updated);
        Ok(updated)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fail_ingress_work(
        &self,
        record: &SessionRuntimeOutboxRecord,
        worker_id: &str,
        claim_token: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        let updated = self
            .kernel()
            .fail_ingress_work(
                &record.request_id,
                worker_id,
                record.session_generation,
                claim_token,
                expected_revision,
                failure_class,
                error,
                retry_at_ms,
                max_attempts,
                now_ms,
            )
            .await?;
        self.project_runtime_input_state(&updated);
        Ok(updated)
    }

    fn project_runtime_input_state(&self, record: &SessionRuntimeOutboxRecord) {
        if let Ok(runtime) = self.runtime() {
            runtime.project_durable_session_receipt(
                &record.session_id,
                receipt_from_durable_input(record),
            );
        }
    }

    pub(crate) async fn commit_terminal_transcript(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> Result<SessionTerminalTranscriptReceipt, SessionError> {
        let receipt = self.kernel().commit_terminal_transcript(request).await?;
        self.project_runtime_input_state(&receipt.input);
        if let Ok(runtime) = self.runtime() {
            runtime.acknowledge_durable_session_inputs_through(
                &request.session_id,
                &request.turn_id,
                request.fence.session_generation,
                request.consumed_input_sequence,
            );
        }
        Ok(receipt)
    }

    #[cfg(test)]
    pub(crate) async fn create_stored_session_for_tests(
        &self,
        record: &SessionRecord,
    ) -> Result<(), SessionError> {
        self.kernel().create_stored_session_for_tests(record).await
    }

    pub(crate) async fn append_context_event(
        &self,
        event: &ContextSessionJournalEvent,
    ) -> Result<bool, SessionError> {
        let event = session::SessionDomainEvent::new(
            event.session_id(),
            0,
            event.scope(),
            event.kind(),
            serde_json::to_value(event).map_err(|error| {
                SessionError::InvalidArgument(format!(
                    "failed to serialize typed Context Session event: {error}"
                ))
            })?,
            now_ms(),
        );
        self.append_runtime_domain_event(&event).await.map(|_| true)
    }

    pub(crate) async fn append_surface_journal_projection(
        &self,
        inbox_key: &str,
        projection: &surface::SurfaceSessionProjectionRecord,
    ) -> Result<bool, SessionError> {
        let scope = match projection.scope.as_str() {
            "session" => SessionDomainScope::Session,
            "message" => SessionDomainScope::Message,
            "turn" => SessionDomainScope::Turn,
            "tool" => SessionDomainScope::Tool,
            other => {
                return Err(SessionError::InvalidArgument(format!(
                    "unsupported Surface Session projection scope `{other}`"
                )))
            }
        };
        let mut domain_event = session::SessionDomainEvent::new(
            &projection.session_id,
            0,
            scope,
            &projection.kind,
            projection.payload_json.clone(),
            projection.created_at_ms,
        );
        domain_event.event_id = projection.event_id.clone();
        domain_event.correlation_id = Some(inbox_key.to_string());
        domain_event.status = Some(projection.status.clone());
        domain_event.refs.push(session::SessionDomainRef {
            ref_type: "surface_inbox".to_string(),
            id: format!("surface-inbox:{inbox_key}"),
            label: Some(projection.phase.clone()),
        });
        let coordinator = self.coordinator().map_err(SessionError::InvalidArgument)?;
        let _guard = coordinator.acquire_exclusive(&projection.session_id).await;
        let record = self
            .kernel()
            .stored_session(&projection.session_id)
            .await?
            .ok_or_else(|| {
                SessionError::Store(format!("session `{}` not found", projection.session_id))
            })?;
        if matches!(
            record.status.as_str(),
            "archiving" | "archived" | "deleting" | "deleted"
        ) {
            return Err(SessionError::StaleExecutionFence(format!(
                "session `{}` rejects Surface projections in lifecycle state {}",
                projection.session_id, record.status
            )));
        }
        match self
            .kernel()
            .append_runtime_domain_event_if_absent(&domain_event)
            .await
        {
            Ok((_, replayed)) => Ok(!replayed),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>, SessionError> {
        self.kernel()
            .context_event_by_envelope_id(envelope_id)
            .await
    }

    pub(crate) async fn stored_session_domain_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<SessionDomainEventPage>, SessionError> {
        self.kernel()
            .stored_session_domain_events_page(session_id, from_sequence, limit)
            .await
    }

    pub(crate) async fn stored_timeline_runtime_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<SessionDomainEventPage>, SessionError> {
        self.kernel()
            .stored_timeline_runtime_page(session_id, from_sequence, limit)
            .await
    }

    pub(crate) async fn update_session(
        &self,
        session_id: &str,
        update: SessionUpdateRequest,
    ) -> Result<bool, SessionError> {
        let coordinator = self.coordinator().map_err(SessionError::InvalidArgument)?;
        let _guard = coordinator.acquire_exclusive(session_id).await;
        let mut found = false;
        let resolved_model = match update.model.as_deref() {
            Some(model) => Some(
                self.runtime()
                    .map_err(SessionError::InvalidArgument)?
                    .resolve_session_model(Some(model))
                    .map_err(SessionError::InvalidArgument)?,
            ),
            None => None,
        };

        if let Some(mut record) = self.stored_session(session_id).await? {
            found = true;
            if let Some(model) = resolved_model {
                record.model = Some(model.clone());
            }
            if let Some(ref title) = update.title {
                let mut meta: serde_json::Value = record
                    .metadata_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str(value).ok())
                    .unwrap_or(serde_json::json!({}));
                meta["title"] = serde_json::Value::String(title.clone());
                record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
            }
            if let Some(ref metadata) = update.metadata {
                let mut meta: serde_json::Value = record
                    .metadata_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str(value).ok())
                    .unwrap_or(serde_json::json!({}));
                if let Some(obj) = meta.as_object_mut() {
                    if let Some(new_obj) = metadata.as_object() {
                        for (key, value) in new_obj {
                            obj.insert(key.clone(), value.clone());
                        }
                    }
                }
                record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
            }
            self.update_stored_session(&record).await?;
        }

        Ok(found)
    }

    pub(crate) async fn routing_focus(
        &self,
        session_id: &str,
    ) -> Result<SessionRoutingFocus, String> {
        let record = self
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session `{session_id}` not found"))?;
        routing_focus_from_record(&record)
    }

    pub(crate) async fn set_task_focus(
        &self,
        session_id: &str,
        task_id: &str,
        expected_revision: u64,
        actor: &str,
    ) -> Result<SessionFocusReceipt, String> {
        let task = self
            .runtime()?
            .runtime_services()
            .task_runtime_port()
            .get(task_id)?
            .ok_or_else(|| format!("task `{task_id}` not found"))?;
        let root = self
            .runtime()?
            .runtime_services()
            .task_runtime_port()
            .get(&task.root_task_id)?
            .ok_or_else(|| format!("root task `{}` not found", task.root_task_id))?;
        self.mutate_routing_focus(
            session_id,
            expected_revision,
            actor,
            SessionFocusMutation::TaskSet,
            |focus, revision, now| {
                focus.task = Some(SessionTaskFocus {
                    task_id: root.task_id.clone(),
                    actor: actor.to_string(),
                    revision,
                    updated_at_ms: now,
                    inherited_from_session_id: None,
                });
            },
        )
        .await
    }

    pub(crate) async fn clear_task_focus(
        &self,
        session_id: &str,
        expected_revision: u64,
        actor: &str,
    ) -> Result<SessionFocusReceipt, String> {
        self.mutate_routing_focus(
            session_id,
            expected_revision,
            actor,
            SessionFocusMutation::TaskCleared,
            |focus, _, _| focus.task = None,
        )
        .await
    }

    pub(crate) async fn set_mission_focus(
        &self,
        session_id: &str,
        mission_id: &str,
        expected_revision: u64,
        actor: &str,
    ) -> Result<SessionFocusReceipt, String> {
        let mission = self
            .runtime()?
            .runtime_services()
            .mission_runtime()
            .aggregate(mission_id)
            .ok_or_else(|| format!("mission `{mission_id}` not found"))?;
        if mission.status.is_terminal() {
            return Err(format!("mission `{mission_id}` is terminal"));
        }
        self.mutate_routing_focus(
            session_id,
            expected_revision,
            actor,
            SessionFocusMutation::MissionSet,
            |focus, revision, now| {
                focus.mission = Some(SessionMissionFocus {
                    mission_id: mission_id.to_string(),
                    actor: actor.to_string(),
                    revision,
                    updated_at_ms: now,
                    inherited_from_session_id: None,
                });
            },
        )
        .await
    }

    pub(crate) async fn clear_mission_focus(
        &self,
        session_id: &str,
        expected_revision: u64,
        actor: &str,
    ) -> Result<SessionFocusReceipt, String> {
        self.mutate_routing_focus(
            session_id,
            expected_revision,
            actor,
            SessionFocusMutation::MissionCleared,
            |focus, _, _| focus.mission = None,
        )
        .await
    }

    async fn mutate_routing_focus<F>(
        &self,
        session_id: &str,
        expected_revision: u64,
        actor: &str,
        mutation: SessionFocusMutation,
        mutate: F,
    ) -> Result<SessionFocusReceipt, String>
    where
        F: FnOnce(&mut SessionRoutingFocus, u64, u64),
    {
        if actor.trim().is_empty() {
            return Err("Session focus actor is required".to_string());
        }
        let coordinator = self.coordinator()?;
        let _guard = coordinator.acquire_exclusive(session_id).await;
        let mut record = self
            .kernel()
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session `{session_id}` not found"))?;
        if matches!(
            record.status.as_str(),
            "archiving" | "archived" | "deleting" | "deleted"
        ) {
            return Err(format!(
                "session `{session_id}` rejects focus changes in lifecycle state {}",
                record.status
            ));
        }
        let mut focus = routing_focus_from_record(&record)?;
        if focus.revision != expected_revision {
            return Err(format!(
                "session `{session_id}` focus revision conflict: expected {expected_revision}, actual {}",
                focus.revision
            ));
        }
        let now = now_ms();
        let revision = focus.revision.saturating_add(1);
        mutate(&mut focus, revision, now);
        focus.revision = revision;
        let mut metadata = session_metadata_object(&record);
        metadata.insert(
            ROUTING_FOCUS_METADATA_KEY.to_string(),
            serde_json::to_value(&focus).map_err(|error| error.to_string())?,
        );
        record.metadata_json = Some(serde_json::Value::Object(metadata).to_string());
        self.kernel()
            .update_stored_session(&record)
            .await
            .map_err(|error| error.to_string())?;
        let receipt = SessionFocusReceipt {
            session_id: session_id.to_string(),
            mutation,
            accepted_revision: revision,
            actor: actor.to_string(),
            updated_at_ms: now,
            focus,
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "session_focus",
                format!("{session_id}:{revision}"),
            )],
        };
        let mut event = session::SessionDomainEvent::new(
            session_id,
            0,
            SessionDomainScope::Session,
            "session.routing_focus.changed",
            serde_json::to_value(&receipt).map_err(|error| error.to_string())?,
            now,
        );
        event.event_id = format!("session-focus:{session_id}:{revision}");
        event.correlation_id = Some(format!("session-focus:{session_id}"));
        self.kernel()
            .append_runtime_domain_event_if_absent(&event)
            .await
            .map_err(|error| error.to_string())?;
        Ok(receipt)
    }

    async fn validated_routing_focus(
        &self,
        record: &SessionRecord,
    ) -> Result<SessionRoutingFocus, String> {
        let focus = routing_focus_from_record(record)?;
        let task_missing = match focus.task.as_ref() {
            Some(task) => self
                .runtime()?
                .runtime_services()
                .task_runtime_port()
                .get(&task.task_id)?
                .is_none(),
            None => false,
        };
        let mission_unavailable = match focus.mission.as_ref() {
            Some(mission) => self
                .runtime()?
                .runtime_services()
                .mission_runtime()
                .aggregate(&mission.mission_id)
                .is_none_or(|aggregate| aggregate.status.is_terminal()),
            None => false,
        };
        if !task_missing && !mission_unavailable {
            return Ok(focus);
        }

        let expected_revision = focus.revision;
        let mutation = match (task_missing, mission_unavailable) {
            (true, true) => SessionFocusMutation::FocusInvalidated,
            (true, false) => SessionFocusMutation::TaskInvalidated,
            (false, true) => SessionFocusMutation::MissionInvalidated,
            (false, false) => unreachable!(),
        };
        self.mutate_routing_focus(
            &record.session_id,
            expected_revision,
            "runtime.focus_validator",
            mutation,
            move |current, _, _| {
                if task_missing {
                    current.task = None;
                }
                if mission_unavailable {
                    current.mission = None;
                }
            },
        )
        .await
        .map(|receipt| receipt.focus)
    }
}

fn session_metadata_object(record: &SessionRecord) -> serde_json::Map<String, serde_json::Value> {
    record
        .metadata_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}

fn routing_focus_from_record(record: &SessionRecord) -> Result<SessionRoutingFocus, String> {
    let metadata = session_metadata_object(record);
    let Some(value) = metadata.get(ROUTING_FOCUS_METADATA_KEY).cloned() else {
        return Ok(SessionRoutingFocus::default());
    };
    serde_json::from_value(value).map_err(|error| {
        format!(
            "session `{}` has invalid routing_focus metadata: {error}",
            record.session_id
        )
    })
}

fn classification_from_durable_input(
    record: &SessionRuntimeOutboxRecord,
) -> Option<DurableInputClassification> {
    record
        .classification_json
        .as_deref()
        .and_then(|value| serde_json::from_str(value).ok())
}

fn status_from_durable_input(record: &SessionRuntimeOutboxRecord) -> SessionInputStatus {
    match record.status {
        SessionRuntimeInputStatus::Completed => match record.decision {
            InputRoutingDecision::SpawnSubtask => SessionInputStatus::DispatchedSubtask,
            InputRoutingDecision::RouteCrossSession => SessionInputStatus::DispatchedSession,
            InputRoutingDecision::CreateNewSession => SessionInputStatus::NewSessionCreated,
            InputRoutingDecision::ControlOrApproval => SessionInputStatus::ControlResolved,
            _ => SessionInputStatus::Consumed,
        },
        SessionRuntimeInputStatus::Supplemented => match record.decision {
            InputRoutingDecision::ControlOrApproval => SessionInputStatus::ControlResolved,
            _ => SessionInputStatus::Consumed,
        },
        SessionRuntimeInputStatus::Attached => SessionInputStatus::AttachedToTurn,
        SessionRuntimeInputStatus::Failed | SessionRuntimeInputStatus::Blocked => {
            SessionInputStatus::Failed
        }
        SessionRuntimeInputStatus::Cancelled => SessionInputStatus::Cancelled,
        SessionRuntimeInputStatus::Expired => SessionInputStatus::Superseded,
        SessionRuntimeInputStatus::RejectedDuplicate => SessionInputStatus::RejectedDuplicate,
        SessionRuntimeInputStatus::RejectedPolicy => SessionInputStatus::RejectedPolicy,
        SessionRuntimeInputStatus::Accepted => SessionInputStatus::Received,
        SessionRuntimeInputStatus::Classified => SessionInputStatus::Classified,
        SessionRuntimeInputStatus::Queued
        | SessionRuntimeInputStatus::Claimed
        | SessionRuntimeInputStatus::Running
        | SessionRuntimeInputStatus::Reclassified => match record.decision {
            InputRoutingDecision::StartNewTurn | InputRoutingDecision::EnqueueNextStep => {
                SessionInputStatus::QueuedNext
            }
            InputRoutingDecision::SupplementCurrentTurn => SessionInputStatus::Persisted,
            InputRoutingDecision::InterruptAndReplan => SessionInputStatus::InterruptRequested,
            InputRoutingDecision::SpawnSubtask => SessionInputStatus::DispatchedSubtask,
            InputRoutingDecision::RouteCrossSession => SessionInputStatus::DispatchedSession,
            InputRoutingDecision::CreateNewSession => SessionInputStatus::NewSessionCreated,
            InputRoutingDecision::ControlOrApproval => SessionInputStatus::Persisted,
            InputRoutingDecision::RejectDuplicate => SessionInputStatus::RejectedDuplicate,
            InputRoutingDecision::RejectPolicy => SessionInputStatus::RejectedPolicy,
        },
    }
}

fn created_at_from_millis(created_at_ms: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(created_at_ms.min(i64::MAX as u64) as i64)
        .unwrap_or_else(Utc::now)
}

fn receipt_from_durable_input(record: &SessionRuntimeOutboxRecord) -> SessionInputReceipt {
    let classification = classification_from_durable_input(record);
    SessionInputReceipt {
        input_id: SessionInputId::from_string(record.input_id.clone()),
        session_id: record.session_id.clone(),
        status: status_from_durable_input(record),
        decision: record.decision,
        relation_proposal: classification
            .as_ref()
            .and_then(|item| item.relation_proposal.clone()),
        reason: classification
            .as_ref()
            .map(|item| item.reason.clone())
            .or_else(|| {
                record.last_error.as_ref().map(|error| {
                    InputRoutingReason::new("durable_transition", error.clone(), 10_000)
                })
            }),
        active_turn_id: record
            .target_turn_id
            .as_ref()
            .map(|turn_id| TurnId::from_string(turn_id.clone())),
        evidence_refs: vec![format!("session-input:{}", record.input_id)],
        cursor: Some(SessionInputCursor::new(
            record.session_generation,
            u64::try_from(record.sequence).unwrap_or(u64::MAX),
        )),
        created_at: classification.map_or_else(
            || created_at_from_millis(record.created_at_ms),
            |item| item.created_at,
        ),
    }
}

fn inbox_item_from_durable_input(record: &SessionRuntimeOutboxRecord) -> TurnInboxItem {
    let classification = classification_from_durable_input(record);
    TurnInboxItem {
        input_id: SessionInputId::from_string(record.input_id.clone()),
        session_id: record.session_id.clone(),
        status: status_from_durable_input(record),
        decision: record.decision,
        relation_proposal: classification
            .as_ref()
            .and_then(|item| item.relation_proposal.clone()),
        content_preview: classification
            .as_ref()
            .map_or_else(String::new, |item| item.content_preview.clone()),
        checkpoint: None,
        created_at: classification.map_or_else(
            || created_at_from_millis(record.created_at_ms),
            |item| item.created_at,
        ),
        consumed_at: record.terminal_at_ms.map(created_at_from_millis),
        cursor: Some(SessionInputCursor::new(
            record.session_generation,
            u64::try_from(record.sequence).unwrap_or(u64::MAX),
        )),
        failure_class: record.failure_class.map(|class| class.as_str().to_string()),
        last_error: record.last_error.clone(),
        application_receipt: record.application_receipt.clone(),
    }
}

fn projection_from_durable_inputs(
    session_id: &str,
    active_turn_id: Option<TurnId>,
    mut records: Vec<SessionRuntimeOutboxRecord>,
) -> SessionInputProjection {
    records.sort_by_key(|record| (record.created_at_ms, record.sequence));
    let inputs = records
        .iter()
        .map(inbox_item_from_durable_input)
        .collect::<Vec<_>>();
    let consumed_count = inputs
        .iter()
        .filter(|item| item.status == SessionInputStatus::Consumed)
        .count();
    let pending_count = records
        .iter()
        .filter(|record| !record.status.is_terminal())
        .count();
    let queued_next_count = inputs
        .iter()
        .filter(|item| item.status == SessionInputStatus::QueuedNext)
        .count();
    let admitted_cursor = inputs.iter().filter_map(|item| item.cursor).max();
    let consumed_cursor = inputs
        .iter()
        .filter(|item| item.consumed_at.is_some())
        .filter_map(|item| item.cursor)
        .max();
    SessionInputProjection {
        session_id: session_id.to_string(),
        active_turn_id,
        total: inputs.len(),
        pending_count,
        queued_next_count,
        consumed_count,
        admitted_cursor,
        consumed_cursor,
        last_decision: records.last().map(|record| record.decision),
        inputs,
        updated_at: Utc::now(),
    }
}

fn now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

fn input_execution_turn_id(envelope: &SessionInputEnvelope) -> String {
    envelope
        .metadata
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| envelope.input_id.to_string())
}

fn application_execution_summary_status_label(
    status: ApplicationExecutionSummaryStatusV1,
) -> &'static str {
    match status {
        ApplicationExecutionSummaryStatusV1::Planned => "planned",
        ApplicationExecutionSummaryStatusV1::Running => "running",
        ApplicationExecutionSummaryStatusV1::Succeeded => "succeeded",
        ApplicationExecutionSummaryStatusV1::Failed => "failed",
        ApplicationExecutionSummaryStatusV1::Blocked => "blocked",
        ApplicationExecutionSummaryStatusV1::Partial => "partial",
    }
}

fn session_title(record: &SessionRecord) -> String {
    record
        .metadata_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .and_then(|value| {
            value
                .get("title")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            format!(
                "Session {}",
                record.session_id.chars().take(8).collect::<String>()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::HotSessionPool;
    use crate::services::session_service::presence::SessionPresenceLedger;

    fn focus_record(metadata: serde_json::Value) -> SessionRecord {
        SessionRecord {
            session_id: "session-focus".to_string(),
            platform: "webui".to_string(),
            chat_id: "session-focus".to_string(),
            user_id: Some("user".to_string()),
            model: None,
            created_at: "2026-08-08T00:00:00Z".to_string(),
            last_activity: "2026-08-08T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "never".to_string(),
            metadata_json: Some(metadata.to_string()),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    #[test]
    fn input_turn_identity_is_independent_from_an_active_target_turn() {
        let envelope =
            SessionInputEnvelope::text("session-1", InputSourceKind::Webui, "supplement");
        let input_id = envelope.input_id.to_string();

        assert_eq!(input_execution_turn_id(&envelope), input_id);

        let explicit = envelope.with_metadata(serde_json::json!({
            "turn_id": "surface-turn-1"
        }));
        assert_eq!(input_execution_turn_id(&explicit), "surface-turn-1");
    }

    #[test]
    fn runtime_drained_requires_execution_input_and_terminal_closure() {
        assert!(SessionService::session_runtime_is_drained(
            false, false, false
        ));
        for (execution, input, terminal) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (true, true, true),
        ] {
            assert!(!SessionService::session_runtime_is_drained(
                execution, input, terminal
            ));
        }
    }

    #[test]
    fn terminal_runtime_inputs_never_block_drain_even_without_terminal_at_ms() {
        for status in [
            SessionRuntimeInputStatus::Completed,
            SessionRuntimeInputStatus::Supplemented,
            SessionRuntimeInputStatus::Failed,
            SessionRuntimeInputStatus::Cancelled,
            SessionRuntimeInputStatus::Expired,
        ] {
            assert!(
                !SessionService::runtime_input_blocks_drain(&status),
                "{status:?} is terminal and must not block drain"
            );
        }
        assert!(SessionService::runtime_input_blocks_drain(
            &SessionRuntimeInputStatus::Running
        ));
    }

    #[test]
    fn routing_focus_metadata_is_typed_and_never_silently_discarded() {
        let valid = focus_record(serde_json::json!({
            ROUTING_FOCUS_METADATA_KEY: {
                "revision": 3,
                "task": {
                    "task_id": "task-1",
                    "actor": "user",
                    "revision": 3,
                    "updated_at_ms": 42
                }
            }
        }));
        assert_eq!(
            routing_focus_from_record(&valid)
                .expect("valid focus")
                .task
                .expect("task focus")
                .task_id,
            "task-1"
        );

        let invalid = focus_record(serde_json::json!({
            ROUTING_FOCUS_METADATA_KEY: {"revision": "not-a-number"}
        }));
        assert!(routing_focus_from_record(&invalid)
            .is_err_and(|error| error.contains("invalid routing_focus metadata")));
    }

    #[tokio::test]
    async fn session_service_owns_attach_detach_lifecycle_projection() {
        let sessions = Arc::new(HotSessionPool::default());
        let service = SessionService::for_tests(
            Arc::new(SessionRepository::new(
                sessions,
                None,
                crate::event_bus::SessionProjectionHub::new(),
            )),
            Arc::new(SessionPresenceLedger::new()),
        );

        let attached = service
            .attach_session_value("session-1", "tui-1", "tui", Some("reader"))
            .await;
        assert_eq!(attached["ok"], true);
        assert_eq!(attached["session_id"], "session-1");
        assert_eq!(attached["presence_ttl_ms"], 3_600_000);
        assert_eq!(attached["event"]["sequence"], 0);
        assert_eq!(attached["snapshot"]["state"], "attached");

        let lifecycle = service.lifecycle_snapshot_value(Some("session-1")).await;
        assert_eq!(lifecycle["ok"], true);
        assert_eq!(lifecycle["snapshot"]["state"], "attached");

        let detached = service.detach_session_value("session-1", "tui-1").await;
        assert_eq!(detached["ok"], true);
        assert_eq!(detached["session_id"], "session-1");
        assert_eq!(detached["snapshot"]["state"], "detached");
    }
}
