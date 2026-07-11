use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use harness_contract::turn::TurnJournalEnvelope;
use memory::store::session::{SessionEvent, SessionListOptions, SessionListPage, SessionMessage};
use memory::{
    CognitiveContextManager, MemoryError, MemoryPulseReport, RuntimeEvent, RuntimeEventPage,
    RuntimeEventScope, SessionRecord, UnifiedSessionStore,
};
use runtime::{AgentWorkGraph, CollaborationReviewPacket};
use tokio::sync::Mutex;

use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use crate::runtime_entry::GatewayRuntimeEntry;

type RuntimeEntry = Arc<Mutex<GatewayRuntimeEntry>>;

#[derive(Debug, Clone)]
pub(crate) enum RuntimeCommand {
    CreateSession {
        session_id: String,
        model: Option<String>,
    },
    ActivateSession {
        session_id: String,
    },
    ArchiveSession {
        session_id: String,
    },
    DeleteSession {
        session_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCommandResult {
    pub session_id: String,
    pub kind: &'static str,
    pub persisted: bool,
    pub runtime_event_sequence: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RuntimeClosedLoopResult {
    pub session_id: String,
    pub persisted: bool,
    pub runtime_event_sequence: Option<usize>,
    pub memory_pulse: Option<MemoryPulseReport>,
    pub degraded_reason: Option<String>,
}

impl RuntimeCommand {
    fn session_id(&self) -> &str {
        match self {
            Self::CreateSession { session_id, .. }
            | Self::ActivateSession { session_id }
            | Self::ArchiveSession { session_id }
            | Self::DeleteSession { session_id } => session_id,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::CreateSession { .. } => "session.create",
            Self::ActivateSession { .. } => "session.activate",
            Self::ArchiveSession { .. } => "session.archive",
            Self::DeleteSession { .. } => "session.delete",
        }
    }
}

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

    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.get_session(session_id).await
    }

    pub(crate) async fn upsert_stored_session(
        &self,
        record: &SessionRecord,
    ) -> Result<bool, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        store.upsert_session(record).await?;
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

    pub(crate) async fn delete_stored_session(
        &self,
        session_id: &str,
    ) -> Result<bool, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(false);
        };
        if store.get_session(session_id).await?.is_some() {
            store.delete_session(session_id).await?;
            Ok(true)
        } else {
            Ok(false)
        }
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

    pub(crate) async fn search_stored_messages(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store.search_messages(query, None, limit).await.map(Some)
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

    pub(crate) async fn append_runtime_event(
        &self,
        session_id: &str,
        scope: RuntimeEventScope,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<Option<usize>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        let created_at_ms = current_time_ms();
        let event = RuntimeEvent::new(session_id, 0, scope, kind, payload, created_at_ms);
        let stored = store
            .append_runtime_event_allocating_sequence(&event)
            .await?;
        Ok(Some(stored.sequence))
    }

    pub(crate) async fn persist_workgraph_review(
        &self,
        graph: &AgentWorkGraph,
        packet: &CollaborationReviewPacket,
        memory_manager: Option<&Arc<CognitiveContextManager>>,
    ) -> Result<RuntimeClosedLoopResult, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(RuntimeClosedLoopResult {
                session_id: graph.session_id.clone(),
                persisted: false,
                runtime_event_sequence: None,
                memory_pulse: None,
                degraded_reason: Some("session store not available".to_string()),
            });
        };

        let mut event = graph.reviewed_runtime_event(0, packet);
        let stored = store
            .append_runtime_event_allocating_sequence(&event)
            .await?;
        event.sequence = stored.sequence;

        let memory_pulse = memory_manager
            .map(|manager| manager.process_memory_pulse_runtime_event(&event))
            .transpose()?
            .flatten();

        Ok(RuntimeClosedLoopResult {
            session_id: graph.session_id.clone(),
            persisted: true,
            runtime_event_sequence: Some(stored.sequence),
            memory_pulse,
            degraded_reason: None,
        })
    }

    pub(crate) async fn stored_runtime_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<RuntimeEventPage>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .runtime_events_page(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn stored_timeline_runtime_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<RuntimeEventPage>, MemoryError> {
        let Some(store) = self.unified_store.as_ref() else {
            return Ok(None);
        };
        store
            .timeline_events_page(session_id, from_sequence, limit)
            .await
            .map(Some)
    }

    pub(crate) async fn execute_runtime_command(
        &self,
        command: RuntimeCommand,
    ) -> Result<RuntimeCommandResult, MemoryError> {
        let session_id = command.session_id().to_string();
        let kind = command.kind();
        let Some(store) = self.unified_store.as_ref() else {
            if matches!(command, RuntimeCommand::DeleteSession { .. }) {
                self.remove_active_runtime(&session_id);
            }
            return Ok(RuntimeCommandResult {
                session_id,
                kind,
                persisted: false,
                runtime_event_sequence: None,
            });
        };

        match &command {
            RuntimeCommand::CreateSession { model, .. } => {
                let mut record = new_api_session_record(&session_id, model.clone());
                record.status = "active".to_string();
                store.create_session(&record).await?;
            }
            RuntimeCommand::ActivateSession { .. } => {
                if let Some(mut record) = store.get_session(&session_id).await? {
                    record.status = "active".to_string();
                    record.last_activity = chrono::Utc::now().to_rfc3339();
                    store.update_session(&record).await?;
                }
            }
            RuntimeCommand::ArchiveSession { .. } => {
                if let Some(mut record) = store.get_session(&session_id).await? {
                    record.status = "archived".to_string();
                    record.last_activity = chrono::Utc::now().to_rfc3339();
                    store.update_session(&record).await?;
                }
            }
            RuntimeCommand::DeleteSession { .. } => {
                self.remove_active_runtime(&session_id);
                if store.get_session(&session_id).await?.is_some() {
                    store.delete_session(&session_id).await?;
                }
                return Ok(RuntimeCommandResult {
                    session_id,
                    kind,
                    persisted: true,
                    runtime_event_sequence: None,
                });
            }
        }

        let runtime_event_sequence = self
            .append_runtime_event(
                &session_id,
                RuntimeEventScope::Session,
                kind,
                serde_json::json!({
                    "command": kind,
                    "session_id": session_id,
                    "persisted": true,
                }),
            )
            .await?;

        Ok(RuntimeCommandResult {
            session_id,
            kind,
            persisted: true,
            runtime_event_sequence,
        })
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
        record.message_count = session.messages.len() as i64;
        record.input_tokens = session
            .messages
            .iter()
            .filter_map(|m| m.usage.as_ref())
            .map(|u| i64::from(u.input_tokens))
            .sum();
        record.output_tokens = session
            .messages
            .iter()
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

        let mut message_events = Vec::with_capacity(session.messages.len());
        for (sequence, message) in session.messages.iter().enumerate() {
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

    use super::{RuntimeCommand, SessionKernel};
    use crate::event_bus::SessionEventBus;
    use crate::gateway::ActiveSessions;
    use memory::{MaintenanceCandidate, MaintenanceCandidateKind, MaintenanceCandidateStatus};
    use runtime::{
        AgentTaskTrace, AgentWorkGraph, CollaborationReviewPacket, CollaborationScorecard,
        CollaborationTask,
    };

    fn scorecard() -> CollaborationScorecard {
        CollaborationScorecard {
            completion_rate: 1.0,
            synthesis_lift: 1.2,
            complementarity_score: 0.7,
            active_memory_score: 0.5,
            conflict_count: 0,
            memory_pulse_count: 1,
            surfaced_conflicts: Vec::new(),
        }
    }

    fn review_packet() -> CollaborationReviewPacket {
        let now = chrono::Utc::now();
        CollaborationReviewPacket {
            board_id: "board-closed-loop".to_string(),
            parent_run_id: Some("parent-run".to_string()),
            scorecard: scorecard(),
            agent_tasks: vec![AgentTaskTrace {
                task_id: "agent-review".to_string(),
                parent_run_id: Some("parent-run".to_string()),
                agent_run_id: Some("agent-run".to_string()),
                role: "reviewer".to_string(),
                objective: "review implementation".to_string(),
                status: "completed".to_string(),
                context_envelope_id: Some("ctx-1".to_string()),
                result_summary: "accepted".to_string(),
                evidence_refs: Vec::new(),
                collaboration_board_id: "board-closed-loop".to_string(),
                confidence: 0.9,
                conflicts: Vec::new(),
                created_at_ms: 1,
                updated_at_ms: 2,
            }],
            maintenance_candidates: vec![MaintenanceCandidate {
                id: "candidate-closed-loop".to_string(),
                kind: MaintenanceCandidateKind::RelationshipRefresh,
                status: MaintenanceCandidateStatus::Open,
                entry_ids: Vec::new(),
                summary: "refresh discovered relationship".to_string(),
                reason: "agent review".to_string(),
                confidence: 0.8,
                source: Some("test".to_string()),
                source_ref: Some("board-closed-loop".to_string()),
                created_at: now,
                updated_at: now,
            }],
        }
    }

    fn reviewed_graph(packet: &CollaborationReviewPacket) -> AgentWorkGraph {
        let task = CollaborationTask {
            description: "review implementation".to_string(),
            required_capabilities: vec!["review".to_string()],
            subtasks: Vec::new(),
            review_criteria: None,
            collaboration_decision: None,
        };
        AgentWorkGraph::from_collaboration_task("closed-loop-session", &task)
            .with_review_packet(packet)
    }

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
    async fn kernel_queries_stored_session_records_and_deletes_existing_rows() {
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
        assert!(kernel
            .delete_stored_session("stored-session")
            .await
            .unwrap());
        assert!(kernel
            .stored_session("stored-session")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn kernel_upserts_and_updates_stored_session_records() {
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            Some(Arc::new(
                memory::UnifiedSessionStore::open_in_memory().unwrap(),
            )),
            SessionEventBus::new(),
        );
        let mut record = memory::SessionRecord {
            session_id: "upsert-session".to_string(),
            platform: "api".to_string(),
            chat_id: "upsert-session".to_string(),
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

        assert!(kernel.upsert_stored_session(&record).await.unwrap());
        record.model = Some("claude-opus-4-6".to_string());
        assert!(kernel.update_stored_session(&record).await.unwrap());

        let stored = kernel
            .stored_session("upsert-session")
            .await
            .unwrap()
            .expect("record should exist");
        assert_eq!(stored.model.as_deref(), Some("claude-opus-4-6"));
    }

    #[tokio::test]
    async fn session_lifecycle_create_activate_archive_emits_runtime_events() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            Some(store.clone()),
            SessionEventBus::new(),
        );

        let created = kernel
            .execute_runtime_command(RuntimeCommand::CreateSession {
                session_id: "lifecycle-session".to_string(),
                model: Some("claude-sonnet-4-6".to_string()),
            })
            .await
            .unwrap();
        assert!(created.persisted);
        assert_eq!(created.runtime_event_sequence, Some(0));

        let activated = kernel
            .execute_runtime_command(RuntimeCommand::ActivateSession {
                session_id: "lifecycle-session".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(activated.runtime_event_sequence, Some(1));

        let archived = kernel
            .execute_runtime_command(RuntimeCommand::ArchiveSession {
                session_id: "lifecycle-session".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(archived.runtime_event_sequence, Some(2));

        let record = store
            .get_session("lifecycle-session")
            .await
            .unwrap()
            .expect("session should exist");
        assert_eq!(record.status, "archived");

        let page = kernel
            .stored_runtime_events_page("lifecycle-session", 0, 10)
            .await
            .unwrap()
            .expect("runtime events page");
        assert_eq!(page.total, 3);
        assert_eq!(page.events[0].kind, "session.create");
        assert_eq!(page.events[1].kind, "session.activate");
        assert_eq!(page.events[2].kind, "session.archive");
    }

    #[tokio::test]
    async fn session_lifecycle_delete_cleans_hot_and_cold_state() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            Some(store.clone()),
            SessionEventBus::new(),
        );
        kernel
            .execute_runtime_command(RuntimeCommand::CreateSession {
                session_id: "delete-session".to_string(),
                model: None,
            })
            .await
            .unwrap();

        let deleted = kernel
            .execute_runtime_command(RuntimeCommand::DeleteSession {
                session_id: "delete-session".to_string(),
            })
            .await
            .unwrap();

        assert!(deleted.persisted);
        assert_eq!(deleted.kind, "session.delete");
        assert!(store.get_session("delete-session").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn session_lifecycle_without_store_degrades_without_error() {
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            None,
            SessionEventBus::new(),
        );

        let result = kernel
            .execute_runtime_command(RuntimeCommand::ActivateSession {
                session_id: "missing-store-session".to_string(),
            })
            .await
            .unwrap();

        assert!(!result.persisted);
        assert_eq!(result.runtime_event_sequence, None);
    }

    #[tokio::test]
    async fn runtime_closed_loop_persists_workgraph_review_event() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            Some(store.clone()),
            SessionEventBus::new(),
        );
        kernel
            .execute_runtime_command(RuntimeCommand::CreateSession {
                session_id: "closed-loop-session".to_string(),
                model: None,
            })
            .await
            .unwrap();
        let packet = review_packet();
        let graph = reviewed_graph(&packet);

        let result = kernel
            .persist_workgraph_review(&graph, &packet, None)
            .await
            .unwrap();

        assert!(result.persisted);
        assert_eq!(result.runtime_event_sequence, Some(1));
        assert!(result.memory_pulse.is_none());
        let page = kernel
            .stored_runtime_events_page("closed-loop-session", 0, 10)
            .await
            .unwrap()
            .expect("runtime page");
        assert_eq!(page.total, 2);
        let review_event = page
            .events
            .iter()
            .find(|event| event.kind == "agent.workgraph.reviewed")
            .expect("review event");
        assert_eq!(review_event.scope, memory::RuntimeEventScope::Workgraph);
        assert_eq!(
            review_event.payload["maintenance_candidates"][0]["id"],
            "candidate-closed-loop"
        );
    }

    #[tokio::test]
    async fn runtime_closed_loop_without_store_degrades_without_error() {
        let kernel = SessionKernel::new(
            Arc::new(ActiveSessions::new()),
            None,
            SessionEventBus::new(),
        );
        let packet = review_packet();
        let graph = reviewed_graph(&packet);

        let result = kernel
            .persist_workgraph_review(&graph, &packet, None)
            .await
            .unwrap();

        assert!(!result.persisted);
        assert_eq!(result.runtime_event_sequence, None);
        assert_eq!(
            result.degraded_reason.as_deref(),
            Some("session store not available")
        );
    }
}
