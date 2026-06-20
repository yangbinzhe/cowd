use std::sync::Arc;

use memory::store::session::{
    SessionEvent, SessionListOptions, SessionListPage, SessionMessage, SessionRecord,
};
use memory::{
    CognitiveContextManager, MemoryError, RuntimeEventPage, RuntimeEventScope, UnifiedSessionStore,
};
use runtime::{AgentWorkGraph, CollaborationReviewPacket};

use super::ServiceEnvelope;
use crate::session_kernel::SessionKernel;

#[derive(Clone)]
pub(crate) struct SessionService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Option<Arc<SessionKernel>>,
}

impl SessionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "session",
            owner: "0.9.296 Session service boundary",
            kernel: None,
        }
    }

    pub(crate) fn with_kernel(kernel: Arc<SessionKernel>) -> Self {
        Self {
            kernel: Some(kernel),
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

    pub(crate) fn active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::BuiltRuntime>>> {
        self.kernel()
            .and_then(|kernel| kernel.active_runtime(session_id))
    }

    pub(crate) fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::BuiltRuntime,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::BuiltRuntime>>>, String> {
        self.kernel()
            .ok_or_else(|| "session service not configured".to_string())?
            .register_runtime(session_id, runtime)
    }

    pub(crate) fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::BuiltRuntime>>> {
        self.kernel()
            .and_then(|kernel| kernel.remove_active_runtime(session_id))
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

    pub(crate) async fn sync_runtime_session_snapshot(
        &self,
        session_id: &str,
        session: &runtime::Session,
    ) -> Result<(), MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel
                .sync_runtime_session_snapshot(session_id, session)
                .await
                .map(|_| ()),
            None => Ok(()),
        }
    }
}
