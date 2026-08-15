use std::sync::Arc;

use crate::{ManagedWorkerError, ManagedWorkerResult};

/// Immutable generation fence shared by a process and every channel derived from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationFence(Arc<str>);

impl GenerationFence {
    pub fn new(generation: impl Into<String>) -> ManagedWorkerResult<Self> {
        let generation = generation.into();
        if generation.trim().is_empty() || generation.len() > 256 {
            return Err(ManagedWorkerError::InvalidSpec(
                "generation must contain 1..=256 non-blank bytes".to_string(),
            ));
        }
        Ok(Self(Arc::from(generation)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn ensure(&self, observed: &str) -> ManagedWorkerResult<()> {
        if self.as_str() == observed {
            Ok(())
        } else {
            Err(ManagedWorkerError::StaleGeneration {
                expected: self.as_str().to_string(),
                observed: observed.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_generation_is_rejected() {
        let fence = GenerationFence::new("generation-a").expect("fence");
        fence.ensure("generation-a").expect("current");
        assert!(matches!(
            fence.ensure("generation-b"),
            Err(ManagedWorkerError::StaleGeneration { .. })
        ));
    }
}
