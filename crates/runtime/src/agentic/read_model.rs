use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::AgenticProgramProjection;
use crate::execution_core::hot_state::{HotResidencyRegistry, HotResidentClass};

const DEFAULT_MAX_PROGRAMS: usize = 1_024;

#[derive(Debug, Clone)]
struct CachedProgram {
    projection: Arc<AgenticProgramProjection>,
    last_access: u64,
}

/// Rebuildable Program read acceleration. The event journal remains the only
/// owner; every hit is checked against the durable stream head before use.
pub(crate) struct AgenticReadModel {
    state: Mutex<ReadModelState>,
    max_programs: usize,
    residency: Option<Arc<HotResidencyRegistry>>,
}

impl std::fmt::Debug for AgenticReadModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgenticReadModel")
            .field("max_programs", &self.max_programs)
            .field("shared_memory_budget", &self.residency.is_some())
            .finish_non_exhaustive()
    }
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
            residency: None,
        }
    }

    pub(crate) fn with_residency(mut self, residency: Arc<HotResidencyRegistry>) -> Self {
        self.residency = Some(residency);
        self
    }

    pub(crate) fn get(&self, program_id: &str) -> Option<Arc<AgenticProgramProjection>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.trim_to_budget(&mut state);
        state.access_clock = state.access_clock.saturating_add(1);
        let access = state.access_clock;
        let cached = state.programs.get_mut(program_id)?;
        cached.last_access = access;
        if let Some(residency) = &self.residency {
            residency.touch(&resident_id(program_id));
        }
        Some(Arc::clone(&cached.projection))
    }

    pub(crate) fn put(&self, projection: Arc<AgenticProgramProjection>) {
        // Serialize into a counter rather than allocating a whole JSON body.
        // Estimation must never hold the cross-Program cache mutex.
        let estimated_bytes = if self.residency.is_some() {
            let mut counter = ByteCounter(0);
            if serde_json::to_writer(&mut counter, projection.as_ref()).is_err() {
                tracing::warn!(program_id = %projection.program_id, "declining unmeasurable Agentic cache entry");
                return;
            }
            counter
                .0
                .saturating_mul(2)
                .saturating_add(std::mem::size_of::<AgenticProgramProjection>() as u64)
        } else {
            0
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .programs
            .get(&projection.program_id)
            .is_some_and(|cached| cached.projection.revision >= projection.revision)
        {
            return;
        }
        state.access_clock = state.access_clock.saturating_add(1);
        let access = state.access_clock;
        if let Some(residency) = &self.residency {
            residency.upsert(
                resident_id(&projection.program_id),
                HotResidentClass::DerivedProjection,
                projection.program_id.clone(),
                estimated_bytes,
                Some(projection.revision),
            );
        }
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
            if let Some(residency) = &self.residency {
                residency.remove(&resident_id(&oldest));
            }
        }
        self.trim_to_budget(&mut state);
    }

    fn trim_to_budget(&self, state: &mut ReadModelState) {
        if let Some(residency) = &self.residency {
            if residency.pressure_high() {
                while residency.resident_bytes() > residency.target_low_watermark() {
                    let Some(oldest) = state
                        .programs
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_access)
                        .map(|(program_id, _)| program_id.clone())
                    else {
                        break;
                    };
                    state.programs.remove(&oldest);
                    residency.remove(&resident_id(&oldest));
                }
            }
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

fn resident_id(program_id: &str) -> String {
    format!("agentic-read-model:{program_id}")
}

struct ByteCounter(u64);

impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for AgenticReadModel {
    fn drop(&mut self) {
        if let Some(residency) = &self.residency {
            let state = self
                .state
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for program_id in state.programs.keys() {
                residency.remove(&resident_id(program_id));
            }
        }
    }
}
