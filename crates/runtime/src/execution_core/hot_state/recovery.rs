use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
pub(super) struct RecoveryState {
    active: bool,
    generation: u64,
}

/// Per-key singleflight permit used only around cold durable recovery.
pub struct RecoveryPermit {
    state: Arc<(Mutex<RecoveryState>, Condvar)>,
    leader: bool,
}

impl RecoveryPermit {
    pub(super) fn acquire(
        flights: &Mutex<HashMap<String, Arc<(Mutex<RecoveryState>, Condvar)>>>,
        key: &str,
    ) -> Self {
        let state = {
            let mut flights = flights
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(flights.entry(key.to_string()).or_insert_with(|| {
                Arc::new((Mutex::new(RecoveryState::default()), Condvar::new()))
            }))
        };
        let (lock, changed) = &*state;
        let mut guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !guard.active {
            guard.active = true;
            drop(guard);
            return Self {
                state,
                leader: true,
            };
        }
        let generation = guard.generation;
        while guard.active && guard.generation == generation {
            guard = changed
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(guard);
        Self {
            state,
            leader: false,
        }
    }

    #[must_use]
    pub const fn is_leader(&self) -> bool {
        self.leader
    }
}

impl Drop for RecoveryPermit {
    fn drop(&mut self) {
        if !self.leader {
            return;
        }
        let (lock, changed) = &*self.state;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        state.generation = state.generation.saturating_add(1);
        changed.notify_all();
    }
}
