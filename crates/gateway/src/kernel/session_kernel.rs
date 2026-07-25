use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use harness_contract::turn::TurnJournalEnvelope;
use memory::store::session::{
    SessionEvent, SessionListOptions, SessionListPage, SessionMessage, SessionMissionOutboxRequest,
};
use memory::{
    MemoryError, SessionDomainEvent, SessionDomainEventPage, SessionDomainScope, SessionRecord,
    SessionRecoveryManifest, SessionRecoverySignal, UnifiedSessionStore,
};
use tokio::sync::Mutex;

use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use crate::runtime_entry::GatewayRuntimeEntry;

type RuntimeEntry = Arc<Mutex<GatewayRuntimeEntry>>;

/// Unified session capability boundary for hot runtimes, durable session data,
/// and frontend event fan-out.
///
/// `UnifiedSessionStore` remains the durable source of truth. `ActiveSessions`
/// is the hot runtime cache, and `SessionEventBus` is the cross-frontend event
/// transport.
pub(crate) struct SessionKernel {
    active_sessions: Arc<ActiveSessions>,
    unified_store: Option<Arc<UnifiedSessionStore>>,
    event_bus: Arc<SessionEventBus>,
}

impl SessionKernel {
    #[must_use]
    pub(crate) fn new(
        active_sessions: Arc<ActiveSessions>,
        unified_store: Option<Arc<UnifiedSessionStore>>,
        event_bus: Arc<SessionEventBus>,
    ) -> Self {
        Self {
            active_sessions,
            unified_store,
            event_bus,
        }
    }

    #[must_use]
    pub(crate) fn active_sessions(&self) -> Arc<ActiveSessions> {
        self.active_sessions.clone()
    }

    #[must_use]
    pub(crate) fn unified_store(&self) -> Option<Arc<UnifiedSessionStore>> {
        self.unified_store.clone()
    }

    #[must_use]
    pub(crate) fn has_unified_store(&self) -> bool {
        self.unified_store.is_some()
    }

    #[must_use]
    pub(crate) fn event_bus(&self) -> Arc<SessionEventBus> {
        self.event_bus.clone()
    }

    #[must_use]
    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.active_sessions.list()
    }

    #[must_use]
    pub(crate) fn active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.active_sessions.get(session_id)
    }

    pub(crate) fn register_runtime(
        &self,
        session_id: String,
        runtime: GatewayRuntimeEntry,
    ) -> Result<Option<RuntimeEntry>, String> {
        self.active_sessions.register(session_id, runtime)
    }

    pub(crate) fn remove_active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.active_sessions.remove(session_id)
    }

    pub(crate) async fn list_stored_sessions_page(
        &self,
        options: &SessionListOptions<'_>,
    ) -> Result<Option<SessionListPage>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.list_sessions_page(options).await.map(Some)
    }

    pub(crate) async fn list_stored_sessions(
        &self,
    ) -> Result<Option<Vec<SessionRecord>>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.list_sessions().await.map(Some)
    }

    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_session(session_id).await
    }

    pub(crate) async fn stored_recovery_manifest(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecoveryManifest>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_session_recovery_manifest(session_id).await
    }

    pub(crate) async fn active_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionRecoveryManifest>>, MemoryError> {
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
    ) -> Result<Option<SessionRecoveryManifest>, MemoryError> {
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
    ) -> Result<bool, MemoryError> {
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
    ) -> Result<bool, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        store.update_session(record).await?;
        Ok(true)
    }

    pub(crate) async fn delete_stored_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> Result<bool, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        store.delete_session_with_mission_outbox(request).await
    }

    pub(crate) async fn stored_message_count(
        &self,
        session_id: &str,
    ) -> Result<Option<usize>, MemoryError> {
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
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
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
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .get_messages_from_sequence(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn copy_stored_messages(
        &self,
        source_session_id: &str,
        target_session_id: &str,
    ) -> Result<Option<usize>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        let total = store.get_message_count(source_session_id).await?;
        if total == 0 {
            return Ok(Some(0));
        }
        let mut messages = store
            .get_messages(source_session_id, 0, total)
            .await?
            .into_iter()
            .enumerate()
            .map(|(sequence, mut message)| {
                let source_message_id = std::mem::take(&mut message.stable_message_id);
                // `stable_message_id` is globally unique, not scoped by
                // session. A branch retains the source provenance while
                // receiving a deterministic identity in its own transcript.
                message.stable_message_id =
                    format!("branch:{target_session_id}:{source_message_id}");
                message.session_id = target_session_id.to_string();
                message.sequence = sequence;
                message
            })
            .collect::<Vec<_>>();
        let copied = messages.len();
        store.insert_messages_batch(&messages).await?;
        messages.clear();
        Ok(Some(copied))
    }

    pub(crate) async fn stored_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, MemoryError> {
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
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, MemoryError> {
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

    pub(crate) async fn context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>, MemoryError> {
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
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .search_messages_in_sessions(query, session_ids, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn append_timeline_event(
        &self,
        session_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<bool, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        let created_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let event = SessionEvent {
            session_id: session_id.to_string(),
            event_type: event_type.to_string(),
            event_json: payload.to_string(),
            sequence: 0,
            created_at_ms,
        };
        store.append_event_allocating_sequence(&event).await?;
        Ok(true)
    }

    pub(crate) async fn append_turn_journal_event(
        &self,
        session_id: &str,
        envelope: TurnJournalEnvelope,
    ) -> Result<Option<usize>, MemoryError> {
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
                .map_err(|error| MemoryError::Store(error.to_string()))?,
            sequence: 0,
            created_at_ms,
        };
        let stored = store.append_event_allocating_sequence(&event).await?;
        Ok(Some(stored.sequence))
    }

    pub(crate) async fn append_session_domain_event(
        &self,
        session_id: &str,
        scope: SessionDomainScope,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<Option<usize>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        let created_at_ms = current_time_ms();
        let event = SessionDomainEvent::new(session_id, 0, scope, kind, payload, created_at_ms);
        let stored = store
            .append_session_domain_event_allocating_sequence(&event)
            .await?;
        Ok(Some(stored.sequence))
    }

    pub(crate) async fn stored_session_domain_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<SessionDomainEventPage>, MemoryError> {
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
    ) -> Result<Option<SessionDomainEventPage>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .timeline_events_page(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn sync_runtime_session_snapshot(
        &self,
        session_id: &str,
        session: &runtime::Session,
    ) -> Result<bool, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        let now = chrono::Utc::now().to_rfc3339();
        let existing_record = store.get_session(session_id).await?;
        let mut record = existing_record
            .clone()
            .unwrap_or_else(|| new_api_session_record(session_id, session.model.clone()));

        record.model = session.model.clone().or(record.model);
        record.last_activity = now;
        record.message_count = session.message_count() as i64;
        record.input_tokens = session
            .messages()
            .filter_map(|m| m.usage.as_ref())
            .map(|u| i64::from(u.input_tokens))
            .sum();
        record.output_tokens = session
            .messages()
            .filter_map(|m| m.usage.as_ref())
            .map(|u| i64::from(u.output_tokens))
            .sum();

        if existing_record.is_some() {
            store.update_session(&record).await?;
        } else {
            store.create_session(&record).await?;
        }

        store.delete_messages_from(session_id, 0).await?;
        store
            .delete_events_by_type_from(session_id, "message_appended", 0)
            .await?;

        let mut message_events = Vec::with_capacity(session.message_count());
        for (sequence, message) in session.messages().enumerate() {
            let message_record = message.to_session_message(session_id, sequence);
            store.insert_message(&message_record).await?;

            let message_json =
                serde_json::from_str::<serde_json::Value>(&message.to_json().render())
                    .unwrap_or(serde_json::Value::Null);
            message_events.push(SessionEvent {
                session_id: session_id.to_string(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({
                    "type": "message_appended",
                    "sequence": sequence,
                    "role": message.role.role_str(),
                    "message": message_json,
                })
                .to_string(),
                sequence,
                created_at_ms: message_record.created_at_ms,
            });
        }
        store
            .append_events_allocating_sequence(&message_events)
            .await?;
        Ok(true)
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

    use super::SessionKernel;
    use crate::event_bus::SessionEventBus;
    use crate::gateway::ActiveSessions;
    #[test]
    fn kernel_shares_session_runtime_store_and_event_bus_handles() {
        let active_sessions = Arc::new(ActiveSessions::new());
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let event_bus = SessionEventBus::new();

        let kernel = SessionKernel::new(
            active_sessions.clone(),
            Some(store.clone()),
            event_bus.clone(),
        );

        assert!(Arc::ptr_eq(&kernel.active_sessions(), &active_sessions));
        assert!(Arc::ptr_eq(
            &kernel.unified_store().expect("store should exist"),
            &store
        ));
        assert!(Arc::ptr_eq(&kernel.event_bus(), &event_bus));
    }

    #[test]
    fn kernel_exposes_active_runtime_registry_queries() {
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            None,
            SessionEventBus::new(),
        );

        assert!(kernel.list_active_session_ids().is_empty());
        assert!(kernel.active_runtime("missing").is_none());
        assert!(kernel.remove_active_runtime("missing").is_none());
    }

    #[tokio::test]
    async fn kernel_queries_stored_session_records() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            Some(store.clone()),
            SessionEventBus::new(),
        );
        let record = memory::SessionRecord {
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
