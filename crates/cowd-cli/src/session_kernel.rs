use std::sync::Arc;

use memory::UnifiedSessionStore;
use tokio::sync::Mutex;

use crate::BuiltRuntime;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;

type RuntimeEntry = Arc<Mutex<BuiltRuntime>>;

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
        runtime: BuiltRuntime,
    ) -> Result<Option<RuntimeEntry>, String> {
        self.active_sessions.register(session_id, runtime)
    }

    pub(crate) fn remove_active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.active_sessions.remove(session_id)
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
}
