use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use harness_contract::turn::TurnJournalEnvelope;
use session::{
    OutboxFailureClass, SessionDomainEvent, SessionDomainEventPage, SessionError, SessionEvent,
    SessionInputAdmission, SessionListOptions, SessionListPage, SessionMessage,
    SessionMissionOutboxRecord, SessionMissionOutboxRequest, SessionRecord,
    SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, UnifiedSessionStore,
};
use tokio::sync::Mutex;

use crate::event_bus::SessionProjectionHub;
use crate::gateway::HotSessionPool;
use crate::runtime_entry::GatewayRuntimeEntry;

type RuntimeEntry = Arc<Mutex<GatewayRuntimeEntry>>;

/// Unified session capability boundary for hot runtimes, durable session data,
/// and frontend event fan-out.
///
/// `UnifiedSessionStore` remains the durable source of truth. `HotSessionPool`
/// is the hot runtime cache, and `SessionProjectionHub` is the cross-frontend event
/// transport.
pub(crate) struct SessionRepository {
    active_sessions: Arc<HotSessionPool>,
    unified_store: Option<Arc<UnifiedSessionStore>>,
    event_bus: Arc<SessionProjectionHub>,
}

impl SessionRepository {
    #[must_use]
    pub(crate) fn new(
        active_sessions: Arc<HotSessionPool>,
        unified_store: Option<Arc<UnifiedSessionStore>>,
        event_bus: Arc<SessionProjectionHub>,
    ) -> Self {
        Self {
            active_sessions,
            unified_store,
            event_bus,
        }
    }

    #[must_use]
    pub(super) fn active_sessions(&self) -> Arc<HotSessionPool> {
        self.active_sessions.clone()
    }

    fn durable_store(&self) -> Result<&Arc<UnifiedSessionStore>, SessionError> {
        self.unified_store
            .as_ref()
            .ok_or_else(|| SessionError::Store("durable Session store is unavailable".to_string()))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn test_unified_store(&self) -> Option<Arc<UnifiedSessionStore>> {
        self.unified_store.clone()
    }

    #[must_use]
    pub(crate) fn has_unified_store(&self) -> bool {
        self.unified_store.is_some()
    }

    #[must_use]
    pub(super) fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        self.unified_store
            .as_ref()
            .map(|store| Arc::new(store.history_reader()))
    }

    #[must_use]
    pub(super) fn event_bus(&self) -> Arc<SessionProjectionHub> {
        self.event_bus.clone()
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn test_event_bus(&self) -> Arc<SessionProjectionHub> {
        self.event_bus()
    }

    #[must_use]
    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.active_sessions.list()
    }

    #[must_use]
    pub(super) fn active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.active_sessions.get(session_id)
    }

    pub(super) fn register_runtime(
        &self,
        session_id: String,
        runtime: GatewayRuntimeEntry,
    ) -> Result<Option<RuntimeEntry>, String> {
        self.active_sessions.register(session_id, runtime)
    }

    pub(super) fn remove_active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.active_sessions.remove(session_id)
    }

    pub(crate) async fn list_stored_sessions_page(
        &self,
        options: &SessionListOptions<'_>,
    ) -> Result<Option<SessionListPage>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.list_sessions_page(options).await.map(Some)
    }

    pub(crate) async fn list_stored_sessions(
        &self,
    ) -> Result<Option<Vec<SessionRecord>>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.list_sessions().await.map(Some)
    }

    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_session(session_id).await
    }

    pub(crate) async fn stored_recovery_manifest(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecoveryManifest>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_session_recovery_manifest(session_id).await
    }

    pub(crate) async fn active_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionRecoveryManifest>>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .list_active_session_recovery_manifests(offset, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn set_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> Result<Option<SessionRecoveryManifest>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .set_session_recovery_signal(session_id, signal, active, observed_at_ms)
            .await
            .map(Some)
    }

    /// Persist the Session authority record and its one-way Mission lifecycle
    /// intent in the same SQLite transaction. Runtime bridge workers, not API
    /// routes, materialize that intent into the Mission event stream.
    pub(crate) async fn upsert_stored_session_with_mission_outbox(
        &self,
        record: &SessionRecord,
        request: &SessionMissionOutboxRequest,
    ) -> Result<bool, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        store
            .upsert_session_with_mission_outbox(record, request)
            .await?;
        Ok(true)
    }

    pub(crate) async fn update_stored_session(
        &self,
        record: &SessionRecord,
    ) -> Result<bool, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        store.update_session(record).await?;
        Ok(true)
    }

    pub(crate) async fn plan_session_lifecycle(
        &self,
        plan: &session::SessionLifecyclePlan,
    ) -> Result<session::SessionLifecycleIntent, SessionError> {
        self.durable_store()?.plan_session_lifecycle(plan).await
    }

    pub(crate) async fn session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> Result<Option<session::SessionLifecycleIntent>, SessionError> {
        self.durable_store()?
            .get_session_lifecycle_intent(operation_id)
            .await
    }

    pub(crate) async fn recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> Result<Vec<session::SessionLifecycleIntent>, SessionError> {
        self.durable_store()?
            .list_recoverable_session_lifecycle_intents(limit)
            .await
    }

    pub(crate) async fn fence_session_lifecycle(
        &self,
        request: &session::SessionLifecycleFenceRequest,
    ) -> Result<session::SessionLifecycleIntent, SessionError> {
        self.durable_store()?.fence_session_lifecycle(request).await
    }

    pub(crate) async fn transition_session_lifecycle(
        &self,
        transition: &session::SessionLifecycleTransition,
    ) -> Result<session::SessionLifecycleIntent, SessionError> {
        self.durable_store()?
            .transition_session_lifecycle(transition)
            .await
    }

    pub(crate) async fn commit_session_lifecycle_tombstone(
        &self,
        request: &session::SessionLifecycleTombstoneRequest,
    ) -> Result<session::SessionLifecycleIntent, SessionError> {
        self.durable_store()?
            .commit_session_lifecycle_tombstone(request)
            .await
    }

    pub(crate) async fn delete_stored_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> Result<bool, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        store.delete_session_with_mission_outbox(request).await
    }

    pub(crate) async fn stored_message_count(
        &self,
        session_id: &str,
    ) -> Result<Option<usize>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_message_count(session_id).await.map(Some)
    }

    pub(crate) async fn stored_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .get_messages(session_id, offset, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn stored_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .get_messages_from_sequence(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn branch_session_at_cutoff(
        &self,
        request: &session::SessionBranchRequest,
    ) -> Result<session::SessionBranchResult, SessionError> {
        let store = self.unified_store.as_ref().ok_or_else(|| {
            SessionError::Store("durable Session store is unavailable".to_string())
        })?;
        store.branch_session_at_cutoff(request).await
    }

    pub(crate) async fn session_branch_activation(
        &self,
        operation_id: &str,
    ) -> Result<Option<session::SessionBranchActivation>, SessionError> {
        self.durable_store()?
            .get_session_branch_activation(operation_id)
            .await
    }

    pub(crate) async fn recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> Result<Vec<session::SessionBranchActivation>, SessionError> {
        self.durable_store()?
            .list_recoverable_session_branch_activations(limit)
            .await
    }

    pub(crate) async fn transition_session_branch_activation(
        &self,
        transition: &session::SessionBranchActivationTransition,
    ) -> Result<session::SessionBranchActivation, SessionError> {
        self.durable_store()?
            .transition_session_branch_activation(transition)
            .await
    }

    pub(crate) async fn session_input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionInputAdmission>, SessionError> {
        let store = self.unified_store.as_ref().ok_or_else(|| {
            SessionError::Store("durable Session store is unavailable".to_string())
        })?;
        store.get_session_input_admission(session_id).await
    }

    pub(crate) async fn advance_input_generation(
        &self,
        session_id: &str,
        expected_generation: u64,
        open: bool,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission, SessionError> {
        let store = self.unified_store.as_ref().ok_or_else(|| {
            SessionError::Store("durable Session store is unavailable".to_string())
        })?;
        store
            .advance_session_input_generation(
                session_id,
                expected_generation,
                open,
                actor,
                reason,
                now_ms,
            )
            .await
    }

    pub(crate) async fn runtime_inputs(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        let store = self.unified_store.as_ref().ok_or_else(|| {
            SessionError::Store("durable Session store is unavailable".to_string())
        })?;
        store
            .session_runtime_outbox_for_session(session_id, limit)
            .await
    }

    pub(super) async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>, SessionError> {
        self.durable_store()?
            .get_session_runtime_outbox(request_id)
            .await
    }

    pub(super) async fn runtime_input_by_input_id(
        &self,
        input_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>, SessionError> {
        self.durable_store()?
            .get_session_runtime_outbox_by_input_id(input_id)
            .await
    }

    pub(super) async fn append_runtime_ingress(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .append_ingress_with_runtime_outbox(
                session_id,
                role,
                content_json,
                created_at_ms,
                request,
            )
            .await
    }

    pub(super) async fn append_runtime_domain_event(
        &self,
        event: &SessionDomainEvent,
    ) -> Result<SessionEvent, SessionError> {
        self.durable_store()?
            .append_session_domain_event_allocating_sequence(event)
            .await
    }

    pub(super) async fn append_runtime_domain_event_if_absent(
        &self,
        event: &SessionDomainEvent,
    ) -> Result<(SessionEvent, bool), SessionError> {
        self.durable_store()?
            .append_session_domain_event_if_absent_allocating_sequence(event)
            .await
    }

    pub(super) async fn stored_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> Result<Option<SessionDomainEvent>, SessionError> {
        self.durable_store()?
            .get_session_domain_event_by_id(session_id, event_id)
            .await
    }

    pub(super) async fn append_runtime_context_envelope_if_absent(
        &self,
        event: &SessionEvent,
    ) -> Result<Option<SessionEvent>, SessionError> {
        self.durable_store()?
            .append_context_envelope_event_if_absent_allocating_sequence(event)
            .await
    }

    pub(super) async fn append_runtime_compaction_bundle_if_absent(
        &self,
        events: &[SessionDomainEvent],
        checkpoint_id: &str,
    ) -> Result<bool, SessionError> {
        self.durable_store()?
            .append_session_domain_events_if_checkpoint_absent(events, checkpoint_id)
            .await
    }

    pub(super) async fn cancel_runtime_input(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .cancel_session_runtime_outbox(
                input_id,
                session_generation,
                expected_revision,
                actor,
                reason,
                now_ms,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn reclassify_runtime_input(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        decision: harness_contract::turn::InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .reclassify_session_runtime_outbox(
                input_id,
                session_generation,
                expected_revision,
                decision,
                target_turn_id,
                classification_json,
                actor,
                reason,
                now_ms,
            )
            .await
    }

    pub(super) async fn runtime_outbox_health(
        &self,
    ) -> Result<SessionRuntimeOutboxHealth, SessionError> {
        self.durable_store()?.session_runtime_outbox_health().await
    }

    pub(super) async fn blocked_runtime_inputs(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.durable_store()?
            .blocked_session_runtime_outbox(limit)
            .await
    }

    pub(super) async fn retry_blocked_runtime_input(
        &self,
        request_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .retry_blocked_session_runtime_outbox(
                request_id,
                session_generation,
                expected_revision,
                actor,
                reason,
                now_ms,
            )
            .await
    }

    pub(super) async fn claim_ingress_work(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        self.durable_store()?
            .claim_session_runtime_outbox(worker_id, now_ms, lease_ms, limit)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn mark_ingress_running(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .mark_session_runtime_outbox_running(
                request_id,
                worker_id,
                session_generation,
                claim_token,
                expected_revision,
                now_ms,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn renew_ingress_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .renew_session_runtime_outbox_lease(
                request_id,
                worker_id,
                session_generation,
                claim_token,
                expected_revision,
                now_ms,
                lease_ms,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn requeue_ingress_work(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        decision: harness_contract::turn::InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .requeue_claimed_session_runtime_outbox(
                request_id,
                worker_id,
                session_generation,
                claim_token,
                expected_revision,
                decision,
                target_turn_id,
                classification_json,
                reason,
                now_ms,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn complete_ingress_work(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        terminal_status: SessionRuntimeInputStatus,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .ack_session_runtime_outbox(
                request_id,
                worker_id,
                session_generation,
                claim_token,
                expected_revision,
                terminal_status,
                runtime_commit_cursor,
                now_ms,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fail_ingress_work(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord, SessionError> {
        self.durable_store()?
            .fail_session_runtime_outbox(
                request_id,
                worker_id,
                session_generation,
                claim_token,
                expected_revision,
                failure_class,
                error,
                retry_at_ms,
                max_attempts,
                now_ms,
            )
            .await
    }

    pub(super) async fn claim_mission_work(
        &self,
        workspace_key: &str,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionMissionOutboxRecord>, SessionError> {
        self.durable_store()?
            .claim_session_mission_outbox(workspace_key, worker_id, now_ms, lease_ms, limit)
            .await
    }

    pub(super) async fn complete_mission_work(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord, SessionError> {
        self.durable_store()?
            .ack_session_mission_outbox(request_id, worker_id, expected_revision, now_ms)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fail_mission_work(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord, SessionError> {
        self.durable_store()?
            .fail_session_mission_outbox(
                request_id,
                worker_id,
                expected_revision,
                failure_class,
                error,
                retry_at_ms,
                max_attempts,
                now_ms,
            )
            .await
    }

    pub(super) async fn commit_terminal_transcript(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> Result<SessionTerminalTranscriptReceipt, SessionError> {
        self.durable_store()?
            .commit_terminal_transcript_if_fenced(request)
            .await
    }

    pub(crate) async fn active_runtime_inputs(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(Vec::new());
        };
        store.active_session_runtime_outbox(limit).await
    }

    pub(crate) async fn stored_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        let total = store.count_events_from(session_id, from_sequence).await?;
        let events = store
            .get_events_limited(session_id, from_sequence, limit)
            .await?;
        Ok(Some((total, events)))
    }

    pub(crate) async fn stored_events_by_type_page(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        let total = store
            .count_events_by_type_from(session_id, event_type, from_sequence)
            .await?;
        let events = store
            .get_events_by_type_limited(session_id, event_type, from_sequence, limit)
            .await?;
        Ok(Some((total, events)))
    }

    pub(crate) async fn stored_domain_events_by_kind_page(
        &self,
        session_id: &str,
        kind: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        let total = store
            .count_session_domain_events_by_kind_from(session_id, kind, from_sequence)
            .await?;
        let events = store
            .get_session_domain_events_by_kind_limited(session_id, kind, from_sequence, limit)
            .await?;
        Ok(Some((total, events)))
    }

    pub(crate) async fn context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_context_event_by_envelope_id(envelope_id).await
    }

    pub(crate) async fn search_stored_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .search_messages_in_sessions(query, session_ids, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn append_turn_journal_event(
        &self,
        session_id: &str,
        envelope: TurnJournalEnvelope,
    ) -> Result<Option<usize>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        if store.get_session(session_id).await?.is_none() {
            let record = new_api_session_record(session_id, None);
            store.create_session(&record).await?;
        }
        let mut envelope = envelope.with_sequence(0);
        envelope.session_id = session_id.to_string();
        let created_at_ms = current_time_ms();
        let event = SessionEvent {
            session_id: session_id.to_string(),
            event_type: "TurnJournal".to_string(),
            event_json: serde_json::to_string(&envelope)
                .map_err(|error| SessionError::Store(error.to_string()))?,
            sequence: 0,
            created_at_ms,
        };
        let stored = store.append_event_allocating_sequence(&event).await?;
        Ok(Some(stored.sequence))
    }

    pub(crate) async fn stored_session_domain_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<SessionDomainEventPage>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .session_domain_events_page(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn stored_timeline_runtime_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<SessionDomainEventPage>, SessionError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .timeline_events_page(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    #[cfg(test)]
    pub(super) async fn create_stored_session_for_tests(
        &self,
        record: &SessionRecord,
    ) -> Result<(), SessionError> {
        self.durable_store()?.create_session(record).await
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn new_api_session_record(session_id: &str, model: Option<String>) -> SessionRecord {
    let now = chrono::Utc::now().to_rfc3339();
    let title = format!("Session {}", session_id.chars().take(8).collect::<String>());
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "api_server".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(serde_json::json!({ "title": title }).to_string()),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::SessionRepository;
    use crate::event_bus::SessionProjectionHub;
    use crate::gateway::HotSessionPool;
    #[test]
    fn kernel_shares_session_runtime_store_and_event_bus_handles() {
        let active_sessions = Arc::new(HotSessionPool::new());
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let event_bus = SessionProjectionHub::new();

        let kernel = SessionRepository::new(
            active_sessions.clone(),
            Some(store.clone()),
            event_bus.clone(),
        );

        assert!(Arc::ptr_eq(&kernel.active_sessions(), &active_sessions));
        assert!(Arc::ptr_eq(
            &kernel.test_unified_store().expect("store should exist"),
            &store
        ));
        assert!(Arc::ptr_eq(&kernel.event_bus(), &event_bus));
    }

    #[test]
    fn kernel_exposes_active_runtime_registry_queries() {
        let kernel = SessionRepository::new(
            Arc::new(HotSessionPool::new()),
            None,
            SessionProjectionHub::new(),
        );

        assert!(kernel.list_active_session_ids().is_empty());
        assert!(kernel.active_runtime("missing").is_none());
        assert!(kernel.remove_active_runtime("missing").is_none());
    }

    #[tokio::test]
    async fn kernel_queries_stored_session_records() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let kernel = SessionRepository::new(
            Arc::new(HotSessionPool::new()),
            Some(store.clone()),
            SessionProjectionHub::new(),
        );
        let record = session::SessionRecord {
            session_id: "stored-session".to_string(),
            platform: "api".to_string(),
            chat_id: "stored-session".to_string(),
            user_id: None,
            model: Some("claude-sonnet-4-6".to_string()),
            created_at: "2026-06-05T00:00:00Z".to_string(),
            last_activity: "2026-06-05T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        store.upsert_session(&record).await.unwrap();

        assert!(kernel
            .stored_session("stored-session")
            .await
            .unwrap()
            .is_some());
    }
}
