use std::sync::Arc;

use memory::UnifiedSessionStore;

use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;

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
}
