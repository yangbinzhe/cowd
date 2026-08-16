use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use harness_contract::turn::TurnJournalEnvelope;

use crate::services::SessionService;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionInputJournalKind {
    Received,
    IngressBound,
    IngressSettled,
    IngressFailed,
    Cancelled,
    Reclassified,
    TaskRouted,
}

impl SessionInputJournalKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "SessionInputReceived",
            Self::IngressBound => "SessionInputIngressBound",
            Self::IngressSettled => "SessionInputIngressSettled",
            Self::IngressFailed => "SessionInputIngressFailed",
            Self::Cancelled => "SessionInputCancelled",
            Self::Reclassified => "SessionInputReclassified",
            Self::TaskRouted => "SessionInputTaskRouted",
        }
    }
}

/// Deferred Session application port used to break the composition-root cycle.
///
/// Runtime is assembled before `SessionService`, but no Session operation is
/// accepted until the one Gateway-owned service is bound. The weak reference
/// prevents `RuntimeService -> RuntimeServices -> port -> SessionService ->
/// RuntimeService` from becoming a process-lifetime ownership cycle.
pub(crate) struct GatewaySessionRuntimePort {
    service: OnceLock<Weak<SessionService>>,
    #[cfg(test)]
    _test_service: Option<Arc<SessionService>>,
}

impl GatewaySessionRuntimePort {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            service: OnceLock::new(),
            #[cfg(test)]
            _test_service: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        repository: Arc<crate::services::session_service::repository::SessionRepository>,
        presence: Arc<crate::services::session_service::presence::SessionPresenceLedger>,
    ) -> Arc<Self> {
        let service = Arc::new(SessionService::for_tests(repository, presence));
        let port = Arc::new(Self {
            service: OnceLock::new(),
            _test_service: Some(Arc::clone(&service)),
        });
        port.service
            .set(Arc::downgrade(&service))
            .expect("new test Session port has no binding");
        port
    }

    pub(crate) fn bind(&self, service: &Arc<SessionService>) -> Result<(), String> {
        self.service
            .set(Arc::downgrade(service))
            .map_err(|_| "Gateway Session runtime port was already bound".to_string())
    }

    fn service(&self) -> Result<Arc<SessionService>, session::SessionError> {
        self.service.get().and_then(Weak::upgrade).ok_or_else(|| {
            session::SessionError::Other(
                "Gateway SessionService is not available to Runtime".to_string(),
            )
        })
    }
}

#[async_trait]
impl runtime::SessionRuntimeQueryPort for GatewaySessionRuntimePort {
    fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        self.service()
            .ok()
            .and_then(|service| service.history_reader())
    }

    async fn session_record(
        &self,
        session_id: &str,
    ) -> Result<Option<runtime::RuntimeSessionRecord>, session::SessionError> {
        self.service()?
            .stored_session(session_id)
            .await
            .map(|record| {
                record.map(|record| runtime::RuntimeSessionRecord {
                    session_id: record.session_id,
                    status: record.status,
                })
            })
    }

    async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<runtime::RuntimeSessionInputRecord>, session::SessionError> {
        self.service()?
            .runtime_input(request_id)
            .await
            .map(|record| record.map(to_runtime_input_record))
    }

    async fn runtime_input_by_input_id(
        &self,
        input_id: &str,
    ) -> Result<Option<runtime::RuntimeSessionInputRecord>, session::SessionError> {
        self.service()?
            .runtime_input_by_input_id(input_id)
            .await
            .map(|record| record.map(to_runtime_input_record))
    }

    async fn input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<runtime::RuntimeSessionInputAdmission>, session::SessionError> {
        self.service()?
            .session_input_admission(session_id)
            .await
            .map(|admission| {
                admission.map(|admission| runtime::RuntimeSessionInputAdmission {
                    session_id: admission.session_id,
                    generation: admission.generation,
                    open: admission.open,
                })
            })
    }
}

#[async_trait]
impl runtime::SessionRuntimeApplicationPort for GatewaySessionRuntimePort {
    async fn resolve_input_disposition_session_target(
        &self,
        request: &runtime::RuntimeSessionTargetRequest,
    ) -> Result<runtime::RuntimeSessionTargetResolution, session::SessionError> {
        self.service()?
            .resolve_input_disposition_session_target(request)
            .await
            .map_err(session::SessionError::Other)
    }

    async fn commit_input_application_receipt(
        &self,
        input_ids: &[String],
        expected_revisions: &[u64],
        receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
        now_ms: u64,
    ) -> Result<Vec<runtime::RuntimeSessionInputRecord>, session::SessionError> {
        self.service()?
            .commit_input_application_receipt(input_ids, expected_revisions, receipt, now_ms)
            .await
            .map(|records| records.into_iter().map(to_runtime_input_record).collect())
    }
}

#[async_trait]
impl runtime::SessionRuntimeIngressPort for GatewaySessionRuntimePort {
    async fn append_ingress(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &runtime::RuntimeSessionIngressCommand,
    ) -> Result<runtime::RuntimeSessionInputRecord, session::SessionError> {
        let request = session::SessionRuntimeOutboxRequest {
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
        };
        self.service()?
            .append_runtime_ingress(session_id, role, content_json, created_at_ms, &request)
            .await
            .map(to_runtime_input_record)
    }
}

#[async_trait]
impl runtime::SessionRuntimeJournalPort for GatewaySessionRuntimePort {
    async fn append_event(
        &self,
        event: &runtime::RuntimeSessionEvent,
    ) -> Result<runtime::RuntimeSessionEventReceipt, session::SessionError> {
        self.service()?
            .append_runtime_journal_event(event)
            .await
            .map(|event| runtime::RuntimeSessionEventReceipt {
                sequence: event.sequence,
            })
    }

    async fn append_context_envelope_if_absent(
        &self,
        record: &runtime::RuntimeContextEnvelopeRecord,
    ) -> Result<Option<runtime::RuntimeSessionEventReceipt>, session::SessionError> {
        self.service()?
            .append_runtime_context_envelope_if_absent(record)
            .await
            .map(|event| {
                event.map(|event| runtime::RuntimeSessionEventReceipt {
                    sequence: event.sequence,
                })
            })
    }

    async fn append_compaction_bundle_if_absent(
        &self,
        events: &[runtime::RuntimeSessionEvent],
        checkpoint_id: &str,
    ) -> Result<bool, session::SessionError> {
        self.service()?
            .append_runtime_compaction_bundle_if_absent(events, checkpoint_id)
            .await
    }
}

pub(crate) fn to_runtime_input_record(
    record: session::SessionRuntimeOutboxRecord,
) -> runtime::RuntimeSessionInputRecord {
    runtime::RuntimeSessionInputRecord {
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
            session::SessionRuntimeInputStatus::Accepted => {
                runtime::RuntimeSessionInputStatus::Accepted
            }
            session::SessionRuntimeInputStatus::Classified => {
                runtime::RuntimeSessionInputStatus::Classified
            }
            session::SessionRuntimeInputStatus::Queued => {
                runtime::RuntimeSessionInputStatus::Queued
            }
            session::SessionRuntimeInputStatus::RejectedDuplicate => {
                runtime::RuntimeSessionInputStatus::RejectedDuplicate
            }
            session::SessionRuntimeInputStatus::RejectedPolicy => {
                runtime::RuntimeSessionInputStatus::RejectedPolicy
            }
            session::SessionRuntimeInputStatus::Claimed => {
                runtime::RuntimeSessionInputStatus::Claimed
            }
            session::SessionRuntimeInputStatus::Running => {
                runtime::RuntimeSessionInputStatus::Running
            }
            session::SessionRuntimeInputStatus::Reclassified => {
                runtime::RuntimeSessionInputStatus::Reclassified
            }
            session::SessionRuntimeInputStatus::Attached => {
                runtime::RuntimeSessionInputStatus::Attached
            }
            session::SessionRuntimeInputStatus::Completed => {
                runtime::RuntimeSessionInputStatus::Completed
            }
            session::SessionRuntimeInputStatus::Supplemented => {
                runtime::RuntimeSessionInputStatus::Supplemented
            }
            session::SessionRuntimeInputStatus::Failed => {
                runtime::RuntimeSessionInputStatus::Failed
            }
            session::SessionRuntimeInputStatus::Blocked => {
                runtime::RuntimeSessionInputStatus::Blocked
            }
            session::SessionRuntimeInputStatus::Cancelled => {
                runtime::RuntimeSessionInputStatus::Cancelled
            }
            session::SessionRuntimeInputStatus::Expired => {
                runtime::RuntimeSessionInputStatus::Expired
            }
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
        application_receipt: record.application_receipt,
    }
}

impl GatewaySessionRuntimePort {
    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<session::SessionRecord>, session::SessionError> {
        self.service()?.stored_session(session_id).await
    }

    pub(crate) async fn update_session_metadata(
        &self,
        session_id: &str,
        metadata: serde_json::Value,
    ) -> Result<bool, session::SessionError> {
        self.service()?
            .update_session(
                session_id,
                crate::services::SessionUpdateRequest {
                    model: None,
                    title: None,
                    metadata: Some(metadata),
                },
            )
            .await
    }

    pub(crate) async fn append_control_domain_event_if_absent(
        &self,
        event: &session::SessionDomainEvent,
    ) -> Result<bool, session::SessionError> {
        self.service()?
            .append_control_domain_event_if_absent(event)
            .await
    }

    pub(crate) async fn stored_message_count(
        &self,
        session_id: &str,
    ) -> Result<usize, session::SessionError> {
        self.service()?
            .stored_message_count(session_id)
            .await
            .map(Option::unwrap_or_default)
    }

    pub(crate) async fn stored_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<session::SessionMessage>, session::SessionError> {
        self.service()?
            .stored_messages(session_id, offset, limit)
            .await
            .map(Option::unwrap_or_default)
    }

    pub(crate) async fn append_turn_journal(
        &self,
        session_id: &str,
        envelope: TurnJournalEnvelope,
    ) -> Result<Option<usize>, session::SessionError> {
        self.service()?
            .append_turn_journal_event(session_id, envelope)
            .await
    }

    pub(crate) async fn presence_snapshots(&self) -> Vec<session::SessionLifecycleSnapshot> {
        match self.service() {
            Ok(service) => service.presence_snapshots().await,
            Err(_) => Vec::new(),
        }
    }

    pub(crate) async fn append_session_input_journal(
        &self,
        session_id: &str,
        kind: SessionInputJournalKind,
        payload: serde_json::Value,
        occurred_at_ms: u64,
        event_id: &str,
    ) -> Result<Option<usize>, session::SessionError> {
        self.service()?
            .append_session_input_journal(session_id, kind, payload, occurred_at_ms, event_id)
            .await
            .map(|event| Some(event.sequence))
    }

    pub(crate) async fn runtime_inputs(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<session::SessionRuntimeOutboxRecord>, session::SessionError> {
        self.service()?.runtime_inputs(session_id, limit).await
    }

    pub(crate) async fn runtime_inputs_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> Result<Vec<session::SessionRuntimeOutboxRecord>, session::SessionError> {
        self.service()?
            .runtime_inputs_for_sessions(session_ids, per_session_limit)
            .await
    }

    pub(crate) async fn active_runtime_inputs(
        &self,
        limit: usize,
    ) -> Result<Vec<session::SessionRuntimeOutboxRecord>, session::SessionError> {
        self.service()?.active_runtime_inputs(limit).await
    }

    pub(crate) async fn session_input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<session::SessionInputAdmission>, session::SessionError> {
        self.service()?.session_input_admission(session_id).await
    }
}
