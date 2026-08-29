use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
};

use super::{
    aggregate::{ActiveSessionAggregate, PreparedActiveSession, RuntimeCarrier},
    transition::ActiveSessionTransition,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveSessionObservations {
    pub(crate) registered: u64,
    pub(crate) unregistered: u64,
}

/// Atomic publication directory for complete active Session aggregates.
///
/// It owns neither scheduling nor durable Session state. The write guard is
/// held only for capacity validation and a single map mutation, so unrelated
/// Session materialization and draining remain parallel.
pub(crate) struct ActiveSessionDirectory {
    sessions: RwLock<HashMap<String, Arc<ActiveSessionAggregate>>>,
    max_sessions: Option<usize>,
    next_generation: AtomicU64,
    registered: AtomicU64,
    unregistered: AtomicU64,
    transitions: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

impl ActiveSessionDirectory {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            max_sessions: None,
            next_generation: AtomicU64::new(1),
            registered: AtomicU64::new(0),
            unregistered: AtomicU64::new(0),
            transitions: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub(crate) fn with_max_sessions(max: usize) -> Self {
        Self {
            max_sessions: Some(max.max(1)),
            ..Self::new()
        }
    }

    #[must_use]
    pub(crate) const fn max_sessions(&self) -> Option<usize> {
        self.max_sessions
    }

    pub(crate) fn publish(
        &self,
        session_id: String,
        prepared: PreparedActiveSession,
    ) -> Result<ActiveSessionTransition, String> {
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .max_sessions
            .is_some_and(|maximum| sessions.len() >= maximum)
            && !sessions.contains_key(&session_id)
        {
            return Err(format!(
                "max sessions limit ({}) reached",
                self.max_sessions.unwrap_or_default()
            ));
        }
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let current = Arc::new(ActiveSessionAggregate::activate(
            session_id.clone(),
            generation,
            prepared,
        ));
        let replaced = sessions.insert(session_id, Arc::clone(&current));
        self.registered.fetch_add(1, Ordering::Relaxed);
        Ok(ActiveSessionTransition { current, replaced })
    }

    /// Serialize materialize/publish/drain only for the same Session key.
    /// Weak entries disappear after the transition, so historical Session IDs
    /// cannot grow an unbounded lock registry.
    pub(crate) fn transition_lock(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut transitions = self
            .transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        transitions.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = transitions.get(session_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        transitions.insert(session_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    #[must_use]
    pub(crate) fn session(&self, session_id: &str) -> Option<Arc<ActiveSessionAggregate>> {
        self.sessions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
    }

    #[must_use]
    pub(crate) fn get(&self, session_id: &str) -> Option<RuntimeCarrier> {
        self.session(session_id).map(|session| session.carrier())
    }

    #[must_use]
    pub(crate) fn list(&self) -> Vec<String> {
        let mut ids = self
            .sessions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub(crate) fn remove_aggregate(&self, session_id: &str) -> Option<Arc<ActiveSessionAggregate>> {
        let removed = self
            .sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        if removed.is_some() {
            self.unregistered.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    pub(crate) fn remove(&self, session_id: &str) -> Option<RuntimeCarrier> {
        self.remove_aggregate(session_id)
            .map(|session| session.carrier())
    }

    #[must_use]
    pub(crate) fn observations(&self) -> ActiveSessionObservations {
        ActiveSessionObservations {
            registered: self.registered.load(Ordering::Relaxed),
            unregistered: self.unregistered.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn register(
        &self,
        session_id: String,
        runtime: crate::runtime_entry::GatewayRuntimeEntry,
    ) -> Result<Option<RuntimeCarrier>, String> {
        self.publish(session_id, PreparedActiveSession::carrier_only(runtime))
            .map(|transition| transition.replaced_carrier())
    }

    #[cfg(test)]
    pub(crate) fn install_policy_fixture(
        &self,
        session_id: &str,
        policy: runtime::permissions::SessionExecutionPolicyControl,
    ) {
        if let Some(session) = self.session(session_id) {
            session.set_policy_control_for_test(policy);
            return;
        }
        let transition = self
            .publish(
                session_id.to_string(),
                PreparedActiveSession::carrier_only(
                    crate::runtime_entry::GatewayRuntimeEntry::test_runtime_entry(),
                ),
            )
            .expect("test Session policy fixture must fit directory capacity");
        transition.current.set_policy_control_for_test(policy);
    }

    #[cfg(test)]
    pub(crate) fn install_input_fixture(
        &self,
        session_id: &str,
        input: runtime::SessionInputStream,
    ) {
        if let Some(session) = self.session(session_id) {
            session.set_input_for_test(input);
            return;
        }
        let transition = self
            .publish(
                session_id.to_string(),
                PreparedActiveSession::carrier_only(
                    crate::runtime_entry::GatewayRuntimeEntry::test_runtime_entry(),
                ),
            )
            .expect("test Session input fixture must fit directory capacity");
        transition.current.set_input_for_test(input);
    }
}

impl Default for ActiveSessionDirectory {
    fn default() -> Self {
        Self::new()
    }
}
