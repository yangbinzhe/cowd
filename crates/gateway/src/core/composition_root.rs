use std::{sync::Arc, time::Duration};

use crate::{
    active_session::ActiveSessionDirectory, runtime_host::task_set::GatewayRuntimeTaskSet,
};

/// Process-local composition owner for lifecycle-sensitive Gateway resources.
///
/// Durable stores and domain services are composed after configuration is
/// loaded, but the task authority and active Session directory must share one
/// process lifetime. Keeping their creation here prevents route/test adapters
/// from silently constructing competing production authorities.
pub(crate) struct GatewayCompositionRoot {
    active_sessions: Arc<ActiveSessionDirectory>,
    gateway_tasks: Arc<GatewayRuntimeTaskSet>,
}

impl GatewayCompositionRoot {
    #[must_use]
    pub(crate) fn new(task_shutdown_timeout: Duration) -> Self {
        Self {
            active_sessions: Arc::new(ActiveSessionDirectory::default()),
            gateway_tasks: GatewayRuntimeTaskSet::new(task_shutdown_timeout),
        }
    }

    #[must_use]
    pub(crate) fn active_sessions(&self) -> Arc<ActiveSessionDirectory> {
        Arc::clone(&self.active_sessions)
    }

    #[must_use]
    pub(crate) fn gateway_tasks(&self) -> Arc<GatewayRuntimeTaskSet> {
        Arc::clone(&self.gateway_tasks)
    }
}
