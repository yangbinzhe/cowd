use std::collections::HashMap;
use std::sync::Mutex;

use super::AgenticProgramProjection;

const DEFAULT_MAX_PROGRAMS: usize = 1_024;

#[derive(Debug, Clone)]
struct CachedProgram {
    projection: AgenticProgramProjection,
    last_access: u64,
}

/// Rebuildable Program read acceleration. The event journal remains the only
/// owner; every hit is checked against the durable stream head before use.
#[derive(Debug)]
pub(crate) struct AgenticReadModel {
    state: Mutex<ReadModelState>,
    max_programs: usize,
}

#[derive(Debug, Default)]
struct ReadModelState {
    access_clock: u64,
    programs: HashMap<String, CachedProgram>,
}

impl Default for AgenticReadModel {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PROGRAMS)
    }
}

impl AgenticReadModel {
    pub(crate) fn new(max_programs: usize) -> Self {
        Self {
            state: Mutex::new(ReadModelState::default()),
            max_programs: max_programs.max(1),
        }
    }

    pub(crate) fn get(&self, program_id: &str) -> Option<AgenticProgramProjection> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.access_clock = state.access_clock.saturating_add(1);
        let access = state.access_clock;
        let cached = state.programs.get_mut(program_id)?;
        cached.last_access = access;
        Some(cached.projection.clone())
    }

    pub(crate) fn put(&self, projection: AgenticProgramProjection) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.access_clock = state.access_clock.saturating_add(1);
        let access = state.access_clock;
        state.programs.insert(
            projection.program_id.clone(),
            CachedProgram {
                projection,
                last_access: access,
            },
        );
        while state.programs.len() > self.max_programs {
            let Some(oldest) = state
                .programs
                .iter()
                .min_by_key(|(_, cached)| cached.last_access)
                .map(|(program_id, _)| program_id.clone())
            else {
                break;
            };
            state.programs.remove(&oldest);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .programs
            .len()
    }
}
