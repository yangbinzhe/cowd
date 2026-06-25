use std::{collections::HashMap, sync::Arc};

use memory::store::session::{
    SessionEvent, SessionListOptions, SessionListPage, SessionMessage, SessionRecord,
};
use memory::{
    CognitiveContextManager, MemoryError, RuntimeEventPage, RuntimeEventScope, UnifiedSessionStore,
};
use runtime::{AgentWorkGraph, CollaborationReviewPacket};
use serde::{Deserialize, Serialize};
use session::SessionLifecycleKernel;

use super::ServiceEnvelope;
use crate::session_kernel::SessionKernel;
use crate::session_lifecycle_kernel::SessionActor;

#[derive(Clone)]
pub(crate) struct SessionService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Option<Arc<SessionKernel>>,
    lifecycle_kernel: Option<Arc<SessionLifecycleKernel>>,
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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionCompactResult {
    pub(crate) session_id: String,
    pub(crate) compacted: bool,
    pub(crate) removed_message_count: usize,
    pub(crate) summary: String,
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

impl SessionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "session",
            owner: "0.9.296 Session service boundary",
            kernel: None,
            lifecycle_kernel: None,
        }
    }

    pub(crate) fn with_kernel(kernel: Arc<SessionKernel>) -> Self {
        Self {
            kernel: Some(kernel),
            ..Self::new()
        }
    }

    pub(crate) fn with_runtime_boundaries(
        kernel: Arc<SessionKernel>,
        lifecycle_kernel: Arc<SessionLifecycleKernel>,
    ) -> Self {
        Self {
            kernel: Some(kernel),
            lifecycle_kernel: Some(lifecycle_kernel),
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.kernel.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    fn kernel(&self) -> Option<&Arc<SessionKernel>> {
        self.kernel.as_ref()
    }

    fn lifecycle_kernel(&self) -> Option<&Arc<SessionLifecycleKernel>> {
        self.lifecycle_kernel.as_ref()
    }

    pub(crate) fn unified_store(&self) -> Option<Arc<UnifiedSessionStore>> {
        self.kernel().and_then(|kernel| kernel.unified_store())
    }

    pub(crate) fn event_bus(&self) -> Option<Arc<crate::event_bus::SessionEventBus>> {
        self.kernel().map(|kernel| kernel.event_bus())
    }

    pub(crate) fn has_unified_store(&self) -> bool {
        self.kernel()
            .is_some_and(|kernel| kernel.has_unified_store())
    }

    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.kernel()
            .map_or_else(Vec::new, |kernel| kernel.list_active_session_ids())
    }

    pub(crate) async fn session_exists(&self, session_id: &str) -> Result<bool, MemoryError> {
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
        let Some(lifecycle_kernel) = self.lifecycle_kernel() else {
            return serde_json::json!({
                "ok": false,
                "error": "session lifecycle service unavailable",
            });
        };
        let mut actor = SessionActor::new(actor_id, surface);
        actor.role = role.map(ToOwned::to_owned);
        match lifecycle_kernel.attach(session_id, actor).await {
            Ok(event) => {
                let snapshot = lifecycle_kernel.snapshot(session_id).await;
                serde_json::json!({
                    "ok": true,
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
        let Some(lifecycle_kernel) = self.lifecycle_kernel() else {
            return serde_json::json!({
                "ok": false,
                "error": "session lifecycle service unavailable",
            });
        };
        match lifecycle_kernel.detach(session_id, actor_id).await {
            Ok(event) => {
                let snapshot = lifecycle_kernel.snapshot(session_id).await;
                serde_json::json!({
                    "ok": true,
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
        let Some(lifecycle_kernel) = self.lifecycle_kernel() else {
            return serde_json::json!({
                "ok": false,
                "error": "session lifecycle service unavailable",
            });
        };
        match session_id {
            Some(session_id) => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "snapshot": lifecycle_kernel.snapshot(session_id).await,
            }),
            None => serde_json::json!({
                "ok": true,
                "sessions": lifecycle_kernel.snapshots().await,
            }),
        }
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
    ) -> Result<Option<SessionListPage>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.list_stored_sessions_page(options).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.stored_session(session_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn upsert_stored_session(
        &self,
        record: &SessionRecord,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.upsert_stored_session(record).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn update_stored_session(
        &self,
        record: &SessionRecord,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.update_stored_session(record).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn delete_stored_session(
        &self,
        session_id: &str,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.delete_stored_session(session_id).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn stored_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_events_page(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_events_by_type_page(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_events_by_type_page(session_id, event_type, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn search_stored_messages(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.search_stored_messages(query, limit).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_message_count(
        &self,
        session_id: &str,
    ) -> Result<Option<usize>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.stored_message_count(session_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.stored_messages(session_id, offset, limit).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_messages_from_sequence(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn append_timeline_event(
        &self,
        session_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .append_timeline_event(session_id, event_type, payload)
                    .await
            }
            None => Ok(false),
        }
    }

    pub(crate) async fn append_runtime_event(
        &self,
        session_id: &str,
        scope: RuntimeEventScope,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<Option<usize>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .append_runtime_event(session_id, scope, kind, payload)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn persist_workgraph_review(
        &self,
        graph: &AgentWorkGraph,
        packet: &CollaborationReviewPacket,
        memory_manager: Option<&Arc<CognitiveContextManager>>,
    ) -> Result<crate::session_kernel::RuntimeClosedLoopResult, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .persist_workgraph_review(graph, packet, memory_manager)
                    .await
            }
            None => Ok(crate::session_kernel::RuntimeClosedLoopResult {
                session_id: graph.session_id.clone(),
                persisted: false,
                runtime_event_sequence: None,
                memory_pulse: None,
                degraded_reason: Some("session service not configured".to_string()),
            }),
        }
    }

    pub(crate) async fn context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.context_event_by_envelope_id(envelope_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_runtime_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<RuntimeEventPage>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_runtime_events_page(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_timeline_runtime_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<RuntimeEventPage>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_timeline_runtime_page(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn update_session(
        &self,
        session_id: &str,
        update: SessionUpdateRequest,
    ) -> Result<bool, MemoryError> {
        let mut found = false;

        if let Some(mut record) = self.stored_session(session_id).await? {
            found = true;
            if let Some(ref model) = update.model {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::ActiveSessions;
    use crate::session_lifecycle_kernel::SessionLifecycleKernel;

    #[tokio::test]
    async fn session_service_owns_attach_detach_lifecycle_projection() {
        let sessions = Arc::new(ActiveSessions::default());
        let service = SessionService::with_runtime_boundaries(
            Arc::new(SessionKernel::new(
                sessions,
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
        );

        let attached = service
            .attach_session_value("session-1", "tui-1", "tui", Some("reader"))
            .await;
        assert_eq!(attached["ok"], true);
        assert_eq!(attached["event"]["sequence"], 0);
        assert_eq!(attached["snapshot"]["state"], "attached");

        let lifecycle = service.lifecycle_snapshot_value(Some("session-1")).await;
        assert_eq!(lifecycle["ok"], true);
        assert_eq!(lifecycle["snapshot"]["state"], "attached");

        let detached = service.detach_session_value("session-1", "tui-1").await;
        assert_eq!(detached["ok"], true);
        assert_eq!(detached["snapshot"]["state"], "detached");
    }
}
