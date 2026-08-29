use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use crate::runtime_entry::GatewayRuntimeEntry;

pub(crate) type RuntimeCarrier = Arc<Mutex<GatewayRuntimeEntry>>;

/// Identity of the task-set lease which relays Runtime events to Surfaces.
///
/// The task itself remains owned by `GatewayRuntimeTaskSet`; keeping only its
/// identity here makes relay presence part of the atomically published Session
/// state without creating a second task scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionRelayLease {
    task_id: u64,
}

impl SessionRelayLease {
    #[must_use]
    pub(crate) const fn new(task_id: u64) -> Self {
        Self { task_id }
    }

    #[must_use]
    pub(crate) const fn task_id(self) -> u64 {
        self.task_id
    }
}

/// A fully materialized active Session awaiting one directory publication.
pub(crate) struct PreparedActiveSession {
    pub(super) carrier: RuntimeCarrier,
    pub(super) input: Option<runtime::SessionInputStream>,
    pub(super) event_bus: Option<runtime::CowdEventBus>,
    pub(super) model: Option<String>,
    pub(super) policy: Option<runtime::permissions::SessionExecutionPolicyControl>,
    pub(super) relay: Option<SessionRelayLease>,
}

impl PreparedActiveSession {
    #[must_use]
    pub(crate) fn complete(
        carrier: GatewayRuntimeEntry,
        input: runtime::SessionInputStream,
        event_bus: Option<runtime::CowdEventBus>,
        model: Option<String>,
        policy: runtime::permissions::SessionExecutionPolicyControl,
        relay: Option<SessionRelayLease>,
    ) -> Self {
        Self {
            carrier: Arc::new(Mutex::new(carrier)),
            input: Some(input),
            event_bus,
            model,
            policy: Some(policy),
            relay,
        }
    }

    /// Compatibility fixture for tests which exercise only directory carrier
    /// semantics. Production activation always uses `complete`.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn carrier_only(carrier: GatewayRuntimeEntry) -> Self {
        Self {
            carrier: Arc::new(Mutex::new(carrier)),
            input: None,
            event_bus: None,
            model: None,
            policy: None,
            relay: None,
        }
    }
}

/// One immutable lifecycle publication for an active Session.
///
/// Carrier-adjacent capabilities are installed together. The selected model
/// is the sole mutable projection because model switching is an in-place
/// operation on the carrier; policy mutation goes through Runtime's canonical
/// `SessionExecutionPolicyControl` handle.
pub(crate) struct ActiveSessionAggregate {
    session_id: String,
    generation: u64,
    carrier: RuntimeCarrier,
    input: RwLock<Option<runtime::SessionInputStream>>,
    event_bus: Option<runtime::CowdEventBus>,
    model: RwLock<Option<String>>,
    policy: RwLock<Option<runtime::permissions::SessionExecutionPolicyControl>>,
    policy_transition: Arc<Mutex<()>>,
    relay: Option<SessionRelayLease>,
}

impl ActiveSessionAggregate {
    pub(super) fn activate(
        session_id: String,
        generation: u64,
        prepared: PreparedActiveSession,
    ) -> Self {
        Self {
            session_id,
            generation,
            carrier: prepared.carrier,
            input: RwLock::new(prepared.input),
            event_bus: prepared.event_bus,
            model: RwLock::new(prepared.model),
            policy: RwLock::new(prepared.policy),
            policy_transition: Arc::new(Mutex::new(())),
            relay: prepared.relay,
        }
    }

    #[must_use]
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub(crate) fn carrier(&self) -> RuntimeCarrier {
        Arc::clone(&self.carrier)
    }

    #[must_use]
    pub(crate) fn input(&self) -> Option<runtime::SessionInputStream> {
        self.input
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub(crate) fn event_bus(&self) -> Option<runtime::CowdEventBus> {
        self.event_bus.clone()
    }

    #[must_use]
    pub(crate) fn model(&self) -> Option<String> {
        self.model
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_model(&self, model: Option<String>) {
        *self
            .model
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = model;
    }

    #[must_use]
    pub(crate) fn policy_control(
        &self,
    ) -> Option<runtime::permissions::SessionExecutionPolicyControl> {
        self.policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub(crate) fn policy(&self) -> Option<runtime::SessionExecutionPolicy> {
        self.policy_control().map(|control| control.snapshot())
    }

    #[must_use]
    pub(crate) fn policy_transition_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.policy_transition)
    }

    #[must_use]
    pub(crate) const fn relay(&self) -> Option<SessionRelayLease> {
        self.relay
    }

    #[cfg(test)]
    pub(crate) fn set_policy_control_for_test(
        &self,
        policy: runtime::permissions::SessionExecutionPolicyControl,
    ) {
        *self
            .policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(policy);
    }

    #[cfg(test)]
    pub(crate) fn set_input_for_test(&self, input: runtime::SessionInputStream) {
        *self
            .input
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(input);
    }
}
