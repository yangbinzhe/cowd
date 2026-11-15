//! Integration fixture forwarding Runtime's Session ports to a real PG owner.
use async_trait::async_trait;
use harness_contract::input_disposition::{
    InputDispositionSessionTargetMode, SessionInputApplicationReceipt,
};
use runtime::session_runtime_port::*;
use session::{SessionError, UnifiedSessionStore};
use std::sync::Arc;

pub struct CanonicalSessionPort(pub Arc<UnifiedSessionStore>);

pub async fn create_session_port(session_id: &str) -> Arc<CanonicalSessionPort> {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated PostgreSQL owner required");
    let mut config = storage::PostgresConnectionConfig::new(
        "managed-agent-fixture",
        "managed-agent-fixture.pg",
        "cowd-managed-agent-test",
    );
    config.max_connections =
        storage::test_fixture_pool_budget(1, None).expect("fixture connection budget");
    config.min_idle_connections = None;
    config.checkout_timeout_ms = storage::test_fixture_checkout_timeout_ms();
    let (_, resolver) = storage::test_fixture_postgres_url_snapshot(
        "managed-agent-fixture.pg",
        &storage::ResolvedPostgresUrl::new(url),
    );
    let executor = storage::PostgresExecutor::connect(config, &resolver)
        .expect("isolated PostgreSQL executor");
    let schema = format!("cowdrt_{}", uuid::Uuid::new_v4().simple());
    executor
        .checkout_critical()
        .expect("test connection")
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .expect("isolated Session schema");
    let backend = session_postgres::PostgresSessionStore::new(
        executor
            .scoped_namespace(&schema)
            .expect("isolated Session namespace"),
    )
    .expect("Session backend");
    let session_store = Arc::new(session::UnifiedSessionStore::from_backend(Arc::new(
        backend,
    )));
    session_store
        .create_session(&session::SessionRecord {
            session_id: session_id.into(),
            platform: "test".into(),
            chat_id: "managed-agent".into(),
            user_id: None,
            model: None,
            created_at: "2026-11-15T00:00:00Z".into(),
            last_activity: "2026-11-15T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .expect("original Session owner");
    Arc::new(CanonicalSessionPort(session_store))
}

fn convert(
    record: session::SessionRuntimeOutboxRecord,
) -> Result<RuntimeSessionInputRecord, SessionError> {
    serde_json::to_value(record)
        .and_then(serde_json::from_value)
        .map_err(|error| SessionError::Store(error.to_string()))
}

fn event_to_session(event: &RuntimeSessionEvent) -> session::SessionDomainEvent {
    let mut value = session::SessionDomainEvent::new(
        event.session_id.clone(),
        event.sequence_hint,
        event.kind.scope(),
        event.kind.as_str(),
        event.payload.clone(),
        event.occurred_at_ms,
    );
    value.status.clone_from(&event.status);
    value.span_id.clone_from(&event.span_id);
    value.parent_span_id.clone_from(&event.parent_span_id);
    value.correlation_id.clone_from(&event.correlation_id);
    value.refs = event
        .refs
        .iter()
        .map(|r| session::SessionDomainRef {
            ref_type: r.ref_type.clone(),
            id: r.id.clone(),
            label: r.label.clone(),
        })
        .collect();
    value
}

#[async_trait]
impl SessionRuntimeQueryPort for CanonicalSessionPort {
    async fn cancellation_authorization_by_id(
        &self,
        request: &RuntimeSessionCancellationAuthorizationRequest,
    ) -> Result<Option<session::SessionDomainEvent>, SessionError> {
        let owner = request.owner_request();
        self.0
            .get_session_domain_event_by_id(&owner.session_id, &owner.event.event_id)
            .await
    }
    fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        Some(Arc::new(self.0.history_reader()))
    }
    async fn session_record(&self, id: &str) -> Result<Option<RuntimeSessionRecord>, SessionError> {
        Ok(self.0.get_session(id).await?.map(|r| RuntimeSessionRecord {
            session_id: r.session_id,
            status: r.status,
        }))
    }
    async fn runtime_input(
        &self,
        id: &str,
    ) -> Result<Option<RuntimeSessionInputRecord>, SessionError> {
        self.0
            .get_session_runtime_outbox(id)
            .await?
            .map(convert)
            .transpose()
    }
    async fn runtime_input_by_input_id(
        &self,
        id: &str,
    ) -> Result<Option<RuntimeSessionInputRecord>, SessionError> {
        self.0
            .get_session_runtime_outbox_by_input_id(id)
            .await?
            .map(convert)
            .transpose()
    }
    async fn pending_session_input_applications(
        &self,
        id: &str,
        generation: u64,
        after: Option<(usize, String)>,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionInputRecord>, SessionError> {
        self.0
            .pending_session_input_applications(id, generation, after, limit)
            .await?
            .into_iter()
            .map(convert)
            .collect()
    }
    async fn input_admission(
        &self,
        id: &str,
    ) -> Result<Option<RuntimeSessionInputAdmission>, SessionError> {
        Ok(self
            .0
            .get_session_input_admission(id)
            .await?
            .map(|r| RuntimeSessionInputAdmission {
                session_id: r.session_id,
                generation: r.generation,
                open: r.open,
            }))
    }
    async fn evidence_access(
        &self,
        session_id: &str,
        evidence_id: &str,
    ) -> Result<Option<harness_contract::context::EvidenceAccessRef>, SessionError> {
        let mut prefix = None;
        let mut from = 0;
        loop {
            let events = self
                .0
                .get_session_domain_events_by_kind_limited(
                    session_id,
                    RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
                    from,
                    128,
                )
                .await?;
            if events.is_empty() {
                break;
            }
            let mut next = from;
            for stored in events {
                next = next.max(stored.sequence.saturating_add(1));
                let event = session::SessionDomainEvent::from_session_event(&stored)
                    .map_err(|error| SessionError::Store(error.to_string()))?;
                if let Some(access) = runtime::context_evidence::raw::access_from_persisted_payload(
                    evidence_id,
                    &event.payload,
                ) {
                    return Ok(Some(access));
                }
                if let Some(recorded) = event
                    .payload
                    .get("evidence_id")
                    .and_then(serde_json::Value::as_str)
                {
                    if recorded.starts_with(evidence_id) {
                        if prefix.is_some() {
                            prefix = None;
                            break;
                        }
                        prefix = runtime::context_evidence::raw::access_from_persisted_payload(
                            recorded,
                            &event.payload,
                        );
                    }
                }
            }
            if next <= from {
                return Err(SessionError::Store(
                    "evidence pagination did not advance".into(),
                ));
            }
            from = next;
        }
        Ok(prefix)
    }
    async fn session_evidence_receipt_ids(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<String>, SessionError> {
        let mut from = 0;
        let mut ids = Vec::new();
        while ids.len() < limit {
            let events = self
                .0
                .get_session_domain_events_by_kind_limited(
                    session_id,
                    RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
                    from,
                    128.min(limit - ids.len()).max(1),
                )
                .await?;
            if events.is_empty() {
                break;
            }
            let mut next = from;
            for stored in events {
                next = next.max(stored.sequence.saturating_add(1));
                let event = session::SessionDomainEvent::from_session_event(&stored)
                    .map_err(|error| SessionError::Store(error.to_string()))?;
                if let Some(id) =
                    runtime::context_evidence::raw::persisted_evidence_id(&event.payload)
                {
                    ids.push(id.to_string());
                    if ids.len() == limit {
                        return Ok(ids);
                    }
                }
            }
            if next <= from {
                return Err(SessionError::Store(
                    "evidence pagination did not advance".into(),
                ));
            }
            from = next;
        }
        Ok(ids)
    }
}

#[async_trait]
impl SessionRuntimeApplicationPort for CanonicalSessionPort {
    async fn resolve_input_disposition_session_target(
        &self,
        request: &RuntimeSessionTargetRequest,
    ) -> Result<RuntimeSessionTargetResolution, SessionError> {
        match request.mode {
            InputDispositionSessionTargetMode::ExistingAuthorized => {
                let id = request
                    .target_ref
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.strip_prefix("@session:").unwrap_or(s))
                    .ok_or_else(|| {
                        SessionError::Other("existing Session target_ref is required".into())
                    })?;
                if self.0.get_session(id).await?.is_none() {
                    return Err(SessionError::NotFound(id.to_string()));
                }
                Ok(RuntimeSessionTargetResolution {
                    target_session_id: id.into(),
                    created: false,
                })
            }
            InputDispositionSessionTargetMode::CreateIsolated => Err(SessionError::Other(
                "isolated Session creation requires production owner".into(),
            )),
        }
    }
}

#[async_trait]
impl SessionRuntimeIngressPort for CanonicalSessionPort {
    async fn append_ingress(
        &self,
        id: &str,
        role: &str,
        content: Option<&str>,
        now: u64,
        request: &RuntimeSessionIngressCommand,
    ) -> Result<RuntimeSessionInputRecord, SessionError> {
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
        convert(
            self.0
                .append_ingress_with_runtime_outbox(id, role, content, now, &request)
                .await?,
        )
    }
}

#[async_trait]
impl SessionRuntimeJournalPort for CanonicalSessionPort {
    async fn commit_input_application_receipt(
        &self,
        ids: &[String],
        revisions: &[u64],
        receipt: &SessionInputApplicationReceipt,
        now: u64,
    ) -> Result<Vec<RuntimeSessionInputRecord>, SessionError> {
        self.0
            .set_session_input_application_receipt(ids, revisions, receipt, now)
            .await?
            .into_iter()
            .map(convert)
            .collect()
    }
    async fn authorize_requested_cancellation(
        &self,
        request: &RuntimeSessionCancellationAuthorizationRequest,
    ) -> Result<session::SessionDomainEvent, SessionError> {
        let event = self
            .0
            .append_session_domain_event_if_claim_current(request.owner_request())
            .await?;
        session::SessionDomainEvent::from_session_event(&event)
            .map_err(|e| SessionError::Store(e.to_string()))
    }
    async fn append_event_if_claim_current(
        &self,
        request: &RuntimeSessionClaimEventAppend,
    ) -> Result<RuntimeSessionEventReceipt, SessionError> {
        Ok(RuntimeSessionEventReceipt {
            sequence: self
                .0
                .append_session_domain_event_if_claim_current(request.owner_request())
                .await?
                .sequence,
        })
    }
    async fn append_event(
        &self,
        event: &RuntimeSessionEvent,
    ) -> Result<RuntimeSessionEventReceipt, SessionError> {
        Ok(RuntimeSessionEventReceipt {
            sequence: self
                .0
                .append_session_domain_event_allocating_sequence(&event_to_session(event))
                .await?
                .sequence,
        })
    }
    async fn append_context_envelope_if_absent(
        &self,
        record: &RuntimeContextEnvelopeRecord,
    ) -> Result<Option<RuntimeSessionEventReceipt>, SessionError> {
        let event = session::SessionEvent {
            session_id: record.session_id.clone(),
            event_type: "ContextEnvelope".into(),
            event_json: record.payload.to_string(),
            sequence: 0,
            created_at_ms: record.created_at_ms,
        };
        Ok(self
            .0
            .append_context_envelope_event_if_absent_allocating_sequence(&event)
            .await?
            .map(|event| RuntimeSessionEventReceipt {
                sequence: event.sequence,
            }))
    }
    async fn append_compaction_bundle_if_absent(
        &self,
        events: &[RuntimeSessionEvent],
        checkpoint: &str,
    ) -> Result<bool, SessionError> {
        self.0
            .append_session_domain_events_if_checkpoint_absent(
                &events.iter().map(event_to_session).collect::<Vec<_>>(),
                checkpoint,
            )
            .await
    }
}

#[tokio::test]
async fn real_session_journal_persists_provider_event() {
    let id = format!("managed-fixture-journal-{}", uuid::Uuid::new_v4());
    let port = create_session_port(&id).await;
    let payload = serde_json::json!({"fixture":"original-provider-wire","sequence":7});
    let event = RuntimeSessionEvent::new(
        &id,
        0,
        RuntimeSessionEventKind::ProviderRequestPacked,
        payload.clone(),
        1,
    );
    let receipt = port
        .append_event(&event)
        .await
        .expect("durable Session append");
    let records = port
        .0
        .get_session_domain_events_by_kind_limited(
            &id,
            RuntimeSessionEventKind::ProviderRequestPacked.as_str(),
            0,
            8,
        )
        .await
        .expect("durable Session read");
    let stored = records
        .iter()
        .find(|record| record.sequence == receipt.sequence)
        .expect("original journal receipt");
    let actual = session::SessionDomainEvent::from_session_event(stored).expect("domain event");
    assert_eq!(actual.session_id, id);
    assert_eq!(actual.payload, payload);
}
