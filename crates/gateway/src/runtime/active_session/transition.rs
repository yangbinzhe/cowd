use std::sync::Arc;

use super::aggregate::ActiveSessionAggregate;

/// Result of one atomic directory publication.
pub(crate) struct ActiveSessionTransition {
    pub(crate) current: Arc<ActiveSessionAggregate>,
    pub(crate) replaced: Option<Arc<ActiveSessionAggregate>>,
}

impl ActiveSessionTransition {
    #[must_use]
    pub(crate) fn replaced_carrier(&self) -> Option<super::aggregate::RuntimeCarrier> {
        self.replaced.as_ref().map(|session| session.carrier())
    }
}
